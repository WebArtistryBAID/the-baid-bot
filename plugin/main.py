"""WeKit plugin that forwards text and image messages between two WeChat group chats."""

from __future__ import annotations

import os
import random
import threading
import time

from dev.ujhhgtg.wekit.features.api.core import (  # ty: ignore[unresolved-import]
    WeDatabaseApi,
    WeDatabaseListenerApi,
    WeMessageApi,
)
from java import dynamic_proxy  # ty: ignore[unresolved-import]

from .relay_core import (
    BAD_IMAGE,
    BLOCKED,
    IMAGE_MESSAGE_TYPE,
    IMAGE_RATE_LIMITED,
    QUEUE_FULL,
    STALE,
    USER_RATE_LIMITED,
    Forward,
    Intake,
    MessageRow,
    Outbox,
    RateLimiter,
    load_blocked_words,
    run_sender,
)

# ---------------------------- Configuration ----------------------------

# The two group IDs, each ending in "@chatroom", are private, so they live in
# plugin/local_config.py, which git ignores. That file contains:
#
#     GROUP_A = "111111111@chatroom"
#     GROUP_B = "222222222@chatroom"
#
# While either is empty or the file is missing, the plugin forwards nothing and logs
# the ID and name of each group that receives a message.
try:
    from .local_config import GROUP_A, GROUP_B
except ModuleNotFoundError:
    GROUP_A = ""
    GROUP_B = ""

# Blocked words are private too, so they live in this file next to the plugin, which
# git ignores. It has one word or phrase per line; blank lines are skipped. Text
# messages containing any of them are not forwarded. Matching ignores case and
# full-width characters, and finds words inside longer text, so "广告" also blocks
# "这是广告". Images are not checked. Without this file, nothing is blocked.
BLOCKED_WORDS_FILE = "blocked_words.txt"

# The most messages the bot may send per minute, across both groups. Messages over
# this limit wait in the queue. Because waiting messages are combined, one send can
# carry many people's messages.
GLOBAL_MAX_MESSAGES_PER_MINUTE = 5 # 20

# The most messages forwarded per minute for one person, across both groups. Messages
# over this limit are dropped.
PER_USER_MAX_MESSAGES_PER_MINUTE = 12

# The most messages that can wait to be sent, counting each original message. New
# messages are dropped once it is full.
MAX_QUEUED_MESSAGES = 100 

# Messages waiting for the same group are sent together as one message, one line each.
# After a message arrives, wait this long for others to join it before sending.
COMBINE_WAIT_SECONDS = 5

# The longest a combined message can be, in characters. Messages that don't fit wait
# for the next send. A single message longer than this is still sent on its own.
MAX_COMBINED_CHARACTERS = 1000

# Skip messages older than this, such as a backlog that arrives after a reconnect.
MAX_MESSAGE_AGE_SECONDS = 300

# A random pause between sends, in seconds.
MIN_SEND_PAUSE_SECONDS = 1.0
MAX_SEND_PAUSE_SECONDS = 2.5

# How each forwarded message appears, as one line of a combined message. Available
# fields: {name} and {text}.
FORWARD_FORMAT = "{name}: {text}"

# Whether to forward images. Each image is its own send and counts toward
# GLOBAL_MAX_MESSAGES_PER_MINUTE. Stickers, videos and files are never forwarded.
FORWARD_IMAGES = True

# The most images forwarded per minute for one person, across both groups. Images
# over this limit are dropped. They also count toward PER_USER_MAX_MESSAGES_PER_MINUTE.
MAX_IMAGES_PER_USER_PER_MINUTE = 3

# The line that credits an image's sender. It ends the text sent just before the
# image. Available field: {name}.
IMAGE_CREDIT_FORMAT = "{name}: [图片] ↓"

# After uploading an image, how long to watch WeChat's database for the bot's copy to
# finish sending, so the result can be logged.
IMAGE_SEND_CHECK_SECONDS = 30

# To forward an image, the bot downloads it, saves a normal image file to the phone's
# Download/WeKit folder, and uploads that file. Whether to delete that file once the
# forwarded copy is confirmed sent, and how many seconds to wait first. Files whose
# send failed or didn't finish are kept, so the problem can be looked into.
DELETE_FORWARDED_IMAGES = True
DELETE_IMAGES_AFTER_SECONDS = 600

# -----------------------------------------------------------------------


def _member_name(group: str, wxid: str) -> str:
    database = WeDatabaseApi.INSTANCE
    try:
        return (
            database.getGroupMemberDisplayName(group, wxid)
            or database.getDisplayName(wxid)
            or wxid
        )
    except Exception:  # noqa: BLE001 - a failed lookup shouldn't cost the message
        return wxid


def _format_line(forward: Forward) -> str:
    # This runs on the sender thread, so database reads never block WeChat's insert.
    name = _member_name(forward.source_group, forward.sender)
    if forward.is_image:
        return IMAGE_CREDIT_FORMAT.format(name=name)
    return FORWARD_FORMAT.format(name=name, text=forward.text)


def _deliver(ctx, destination: str, text: str) -> None:
    if not WeMessageApi.INSTANCE.sendText(destination, text):
        ctx.log.error(f"Could not send to {destination}")


# WeChat's `status` values for a message the bot sent. These come from observing
# WeChat, not from documentation, so the log always shows the raw value too.
_STATUS_SENT = 2
_STATUS_FAILED = 5


def _as_int(value) -> int | None:
    # Database values may arrive as Java numbers, and NULL arrives as an empty string.
    try:
        return int(str(value))
    except ValueError:
        return None


def _query_first_value(statement: str) -> int | None:
    rows = WeDatabaseApi.INSTANCE.executeQuery(statement, None)
    return None if rows.isEmpty() else _as_int(rows.get(0).get("value"))


def _latest_message_id() -> int | None:
    return _query_first_value("SELECT MAX(msgId) AS value FROM message")


def _wait_for_sent_image(destination: str, after_message_id: int, stop: threading.Event) -> str:
    """Watches for the bot's next image in the group, returning how the send went.

    Returns "sent" or "failed" once WeChat reports either, "missing" if no image
    appeared at all, or "pending (status N)" if one appeared but hadn't finished.
    """
    talker = destination.replace("'", "''")
    statement = (
        "SELECT status AS value FROM message "
        f"WHERE talker='{talker}' AND isSend=1 AND type={IMAGE_MESSAGE_TYPE} "
        f"AND msgId>{after_message_id} ORDER BY msgId ASC LIMIT 1"
    )
    status = None
    deadline = time.monotonic() + IMAGE_SEND_CHECK_SECONDS
    while time.monotonic() < deadline:
        status = _query_first_value(statement)
        if status == _STATUS_SENT:
            return "sent"
        if status == _STATUS_FAILED:
            return "failed"
        if stop.wait(0.5):
            break
    return "missing" if status is None else f"pending (status {status})"


def _send_and_check(ctx, destination: str, send, stop: threading.Event) -> str:
    """Runs one send, returning its outcome, or "unchecked" if it can't be checked.

    A send that returns False was refused by WeKit outright, so there's nothing to wait for.
    """
    latest = _latest_message_id()
    if send() is False:
        return "rejected by WeKit"
    if latest is None:
        ctx.log.warning("Could not read the latest message ID, so the image send can't be checked")
        return "unchecked"
    return _wait_for_sent_image(destination, latest, stop)


def _delete_later(ctx, path: str, timers: list[threading.Timer]) -> None:
    def delete() -> None:
        try:
            os.remove(path)
            ctx.log.info(f"Deleted forwarded image file {os.path.basename(path)}")
        except FileNotFoundError:
            pass
        except OSError:
            ctx.log.exception(f"Could not delete forwarded image file {path}")

    timer = threading.Timer(DELETE_IMAGES_AFTER_SECONDS, delete)
    timer.daemon = True
    timers.append(timer)
    timer.start()


def _download_image(ctx, forward: Forward) -> str | None:
    # WeChat only keeps a thumbnail until an image is opened. downloadImage downloads
    # the image (waiting up to two minutes), converts it from WeChat's own format to a
    # normal image file, and saves that to Download/WeKit. The image's credit line
    # isn't sent until this finishes.
    path = WeMessageApi.INSTANCE.downloadImage(forward.image_server_id)
    if not path:
        ctx.log.error(f"Could not download image {forward.image_server_id}; skipping it")
        return None
    return str(path)


def _deliver_image(
    ctx,
    destination: str,
    forward: Forward,
    path: str,
    stop: threading.Event,
    timers: list[threading.Timer],
) -> None:
    server_id = forward.image_server_id
    outcome = _send_and_check(
        ctx, destination, lambda: WeMessageApi.INSTANCE.sendImage(destination, path), stop
    )
    ctx.log.info(f"Image {server_id} to {destination}: {outcome}")

    if outcome == "sent":
        if DELETE_FORWARDED_IMAGES:
            _delete_later(ctx, path, timers)
    elif not outcome.startswith("pending"):
        ctx.log.error(f"Could not forward image {server_id} to {destination}; kept {path}")


def _log_discovered_group(ctx, group: str) -> None:
    try:
        name = WeDatabaseApi.INSTANCE.getDisplayName(group)
        ctx.log.info(f"Found group: {group} ({name})")
    except Exception:
        ctx.log.exception(f"Found group: {group} (name lookup failed)")


def setup(ctx) -> None:
    routes = {}
    if GROUP_A and GROUP_B and GROUP_A != GROUP_B:
        routes = {GROUP_A: GROUP_B, GROUP_B: GROUP_A}
    else:
        ctx.log.warning("GROUP_A and GROUP_B are not set up; logging group IDs only")

    # Read once at start-up. A file that exists but can't be read stops the plugin here,
    # rather than letting it run without the filter.
    blocked_words = load_blocked_words(ctx.root / BLOCKED_WORDS_FILE)
    if blocked_words is None:
        ctx.log.info(f"Blocked words: off ({BLOCKED_WORDS_FILE} not found)")
    else:
        ctx.log.info(f"Blocked words: on ({blocked_words.count} loaded)")

    stop = threading.Event()
    # Deletions still waiting when the plugin stops are cancelled, keeping those files.
    deletion_timers: list[threading.Timer] = []
    ctx.defer(lambda: [timer.cancel() for timer in deletion_timers])
    outbox = Outbox(MAX_QUEUED_MESSAGES)
    global_limiter = RateLimiter(GLOBAL_MAX_MESSAGES_PER_MINUTE)
    intake = Intake(
        routes,
        RateLimiter(PER_USER_MAX_MESSAGES_PER_MINUTE),
        outbox,
        MAX_MESSAGE_AGE_SECONDS,
        image_limiter=RateLimiter(MAX_IMAGES_PER_USER_PER_MINUTE) if FORWARD_IMAGES else None,
        blocked_words=blocked_words,
    )
    discovered_groups: set[str] = set()

    class Listener(dynamic_proxy(WeDatabaseListenerApi.IInsertListener)):
        def onInsert(self, table, values):
            # WeKit calls this for every database insert, so return quickly.
            if table != "message":
                return
            try:
                talker = values.getAsString("talker")

                if not routes:
                    if talker and talker.endswith("@chatroom") and talker not in discovered_groups:
                        discovered_groups.add(talker)
                        threading.Thread(
                            target=_log_discovered_group,
                            args=(ctx, talker),
                            daemon=True,
                        ).start()
                    return

                result = intake.accept(
                    MessageRow(
                        talker=talker,
                        is_send=values.getAsInteger("isSend"),
                        msg_type=values.getAsInteger("type"),
                        content=values.getAsString("content"),
                        server_id=values.getAsLong("msgSvrId"),
                        create_time_ms=values.getAsLong("createTime"),
                    )
                )
                if result in (
                    USER_RATE_LIMITED, IMAGE_RATE_LIMITED, QUEUE_FULL, STALE, BAD_IMAGE, BLOCKED
                ):
                    ctx.log.warning(f"Dropped a message from {talker}: {result}")
            except Exception:
                ctx.log.exception("Could not process a new message")

    sender = threading.Thread(
        target=run_sender,
        args=(
            outbox,
            global_limiter,
            _format_line,
            lambda destination, text: _deliver(ctx, destination, text),
            lambda forward: _download_image(ctx, forward),
            lambda destination, forward, path: _deliver_image(
                ctx, destination, forward, path, stop, deletion_timers
            ),
            stop,
            COMBINE_WAIT_SECONDS,
            MAX_COMBINED_CHARACTERS,
            lambda: random.uniform(MIN_SEND_PAUSE_SECONDS, MAX_SEND_PAUSE_SECONDS),
            lambda error: ctx.log.exception(message="Could not forward a message", error=error),
        ),
        name="GroupRelaySender",
        daemon=True,
    )
    sender.start()
    ctx.defer(stop.set)

    listener = ctx.track_reference(Listener())
    WeDatabaseListenerApi.INSTANCE.addListener(listener)
    ctx.defer(lambda: WeDatabaseListenerApi.INSTANCE.removeListener(listener))

    ctx.log.info(f"Group Relay started; forwarding {'on' if routes else 'off'}")
