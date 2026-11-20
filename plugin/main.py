"""WeKit plugin that forwards text, image and sticker messages between two WeChat group chats."""

from __future__ import annotations

import os
import random
import threading
import time

from dev.ujhhgtg.wekit.features.api.core import (  # ty: ignore[unresolved-import]
    WeApi,
    WeDatabaseApi,
    WeDatabaseListenerApi,
    WeMessageApi,
    WeServiceApi,
)
from java import dynamic_proxy  # ty: ignore[unresolved-import]

from .relay_core import (
    IMAGE_MESSAGE_TYPE,
    STICKER_MESSAGE_TYPE,
    Forward,
    Intake,
    MentionReply,
    MessageRow,
    Outbox,
    RateLimiter,
    UserBlocker,
    expand_replies,
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

# When someone @mentions the bot, it replies in both groups with one of these, picked
# at random (the same one in both). An entry containing {person} counts once per name
# in MENTION_REPLY_NAMES, so each name is as likely as each other entry. With no
# entries at all, the bot doesn't reply to mentions.
MENTION_REPLY_MESSAGES = [
    "喵喵喵，我猜你是{person}",
    "你所热爱的，就是你的生活。",
    "996 是我们这些人修来的福报。",
    "京东永远不会开除任何一个兄弟。",
    "先定一个能达到的小目标，比方说我先挣它一个亿。",
    "原来你也玩原神",
    "用心创造快乐。",
    "钱有什么用？我对钱没有兴趣。",
    "没事，你们就偷偷乐吧",
    "B 站可能会倒闭，但绝对不会变质。",
]

# The names for {person}. They may be real people, so they live in
# plugin/local_config.py too:
#
#     MENTION_REPLY_NAMES = ["Name 1", "Name 2"]
#
# Without names, entries containing {person} are left out.
try:
    from .local_config import MENTION_REPLY_NAMES
except ImportError:
    MENTION_REPLY_NAMES: list[str] = []

# People whose messages are never forwarded, by raw WeChat ID (wxid_…), not by name.
# The IDs identify real people, so they live in plugin/local_config.py too:
#
#     BLOCKED_USERS = ["wxid_abc123"]
#
# They also get no mention replies. The list is optional.
try:
    from .local_config import BLOCKED_USERS
except ImportError:
    BLOCKED_USERS: list[str] = []

# Anyone not on BLOCKED_USERS who sends more than this many messages in an hour, of any
# type, is blocked for AUTO_BLOCK_HOURS. Messages of theirs that are still waiting are
# dropped too. Blocks are kept in memory, so they're cleared when the plugin restarts.
# Set AUTO_BLOCK_MESSAGES_PER_HOUR to None to turn this off.
AUTO_BLOCK_MESSAGES_PER_HOUR = 100
AUTO_BLOCK_HOURS = 2

# Each person gets at most one reply per this many seconds. Replies also count toward
# GLOBAL_MAX_MESSAGES_PER_MINUTE, and are always sent on their own.
MENTION_REPLY_COOLDOWN_SECONDS = 30

# Blocked words are private too, so they live in this file next to the plugin, which
# git ignores. It has one word or phrase per line; blank lines are skipped. Text
# messages containing any of them are not forwarded. Matching ignores case and
# full-width characters, and finds words inside longer text, so "广告" also blocks
# "这是广告". Images are not checked. Without this file, nothing is blocked.
BLOCKED_WORDS_FILE = "blocked_words.txt"

# The most messages the bot may send per minute, across both groups. Messages over
# this limit wait in the queue. Because waiting messages are combined, one send can
# carry many people's messages.
GLOBAL_MAX_MESSAGES_PER_MINUTE = 40

# The most messages forwarded per minute for one person, across both groups. Messages
# over this limit are dropped.
PER_USER_MAX_MESSAGES_PER_MINUTE = 20

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
# GLOBAL_MAX_MESSAGES_PER_MINUTE. Videos and files are never forwarded.
FORWARD_IMAGES = True

# The most images forwarded per minute for one person, across both groups. Images
# over this limit are dropped. They also count toward PER_USER_MAX_MESSAGES_PER_MINUTE.
MAX_IMAGES_PER_USER_PER_MINUTE = 5

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

# Whether to forward stickers (表情). Like images, each sticker is its own send, sent
# straight after a credit line, and counts toward GLOBAL_MAX_MESSAGES_PER_MINUTE.
# Stickers from the sticker store and dice or rock-paper-scissors games are skipped.
FORWARD_STICKERS = True

# The most stickers forwarded per minute for one person, across both groups. Stickers
# over this limit are dropped. They also count toward PER_USER_MAX_MESSAGES_PER_MINUTE.
MAX_STICKERS_PER_USER_PER_MINUTE = 5

# The line that credits a sticker's sender. Available field: {name}.
STICKER_CREDIT_FORMAT = "{name}: [表情] ↓"

# A sticker is forwarded by its MD5, the way WeChat itself forwards stickers; no file
# is saved. WeChat registers a new sticker in the background after its message
# arrives, which has taken around 10 seconds, so the bot checks every
# STICKER_RETRY_SECONDS for up to STICKER_WAIT_SECONDS. Messages behind the sticker
# wait meanwhile, to keep their order.
STICKER_WAIT_SECONDS = 60
STICKER_RETRY_SECONDS = 3

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
    if forward.is_sticker:
        return STICKER_CREDIT_FORMAT.format(name=name)
    if forward.is_media:
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


def _wait_for_sent_media(
    destination: str, msg_type: int, after_message_id: int, stop: threading.Event
) -> str:
    """Watches for the bot's next image or sticker in the group, returning how it went.

    Returns "sent" or "failed" once WeChat reports either, "missing" if nothing
    appeared at all, or "pending (status N)" if it appeared but hadn't finished.
    """
    talker = destination.replace("'", "''")
    statement = (
        "SELECT status AS value FROM message "
        f"WHERE talker='{talker}' AND isSend=1 AND type={msg_type} "
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


def _send_and_check(ctx, destination: str, msg_type: int, send, stop: threading.Event) -> str:
    """Runs one send, returning its outcome, or "unchecked" if it can't be checked.

    A send that returns False was refused by WeKit outright, so there's nothing to wait for.
    """
    latest = _latest_message_id()
    if send() is False:
        return "rejected by WeKit"
    if latest is None:
        ctx.log.warning("Could not read the latest message ID, so the send can't be checked")
        return "unchecked"
    return _wait_for_sent_media(destination, msg_type, latest, stop)


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


def _download_media(ctx, forward: Forward, stop: threading.Event) -> str | None:
    """Prepares an image or sticker, returning what's needed to send it, or None."""
    if forward.is_sticker:
        return _wait_for_sticker(ctx, forward, stop)
    return _download_image(ctx, forward)


def _is_registered(md5: str) -> bool:
    try:
        return WeServiceApi.INSTANCE.getEmojiInfoByMd5(md5) is not None
    except Exception:  # noqa: BLE001 - treated as not registered yet
        return False


def _wait_for_sticker(ctx, forward: Forward, stop: threading.Event) -> str | None:
    """Waits for WeChat to register the sticker, returning its MD5, or None."""
    server_id = forward.media_server_id
    md5 = forward.sticker_md5
    started = time.monotonic()
    tries = 0
    while True:
        tries += 1
        waited = time.monotonic() - started
        if _is_registered(md5):
            if tries > 1:
                ctx.log.info(f"WeChat registered sticker {server_id} after {waited:.0f}s")
            return md5
        if tries == 1:
            ctx.log.info(
                f"Waiting up to {STICKER_WAIT_SECONDS}s for WeChat to register sticker {server_id}"
            )
        if waited >= STICKER_WAIT_SECONDS or stop.wait(STICKER_RETRY_SECONDS):
            ctx.log.error(
                f"WeChat didn't register sticker {server_id} within {waited:.0f}s; skipping it"
            )
            return None


def _download_image(ctx, forward: Forward) -> str | None:
    # WeChat only keeps a thumbnail until an image is opened. downloadImage downloads
    # the image (waiting up to two minutes), converts it from WeChat's own format to a
    # normal image file, and saves that to Download/WeKit. The image's credit line
    # isn't sent until this finishes.
    path = WeMessageApi.INSTANCE.downloadImage(forward.media_server_id)
    if not path:
        ctx.log.error(f"Could not download image {forward.media_server_id}; skipping it")
        return None
    return str(path)


def _deliver_media(
    ctx,
    destination: str,
    forward: Forward,
    prepared: str,
    stop: threading.Event,
    timers: list[threading.Timer],
) -> None:
    if forward.is_sticker:
        _deliver_sticker(ctx, destination, forward, prepared, stop)
    else:
        _deliver_image(ctx, destination, forward, prepared, stop, timers)


def _deliver_sticker(
    ctx, destination: str, forward: Forward, md5: str, stop: threading.Event
) -> None:
    server_id = forward.media_server_id
    outcome = _send_and_check(
        ctx,
        destination,
        STICKER_MESSAGE_TYPE,
        lambda: WeMessageApi.INSTANCE.sendEmojiByMd5(destination, md5),
        stop,
    )
    ctx.log.info(f"Sticker {server_id} to {destination}: {outcome}")
    if outcome not in ("sent", "unchecked") and not outcome.startswith("pending"):
        ctx.log.error(f"Could not forward sticker {server_id} to {destination}")


def _deliver_image(
    ctx,
    destination: str,
    forward: Forward,
    path: str,
    stop: threading.Event,
    timers: list[threading.Timer],
) -> None:
    server_id = forward.media_server_id
    outcome = _send_and_check(
        ctx,
        destination,
        IMAGE_MESSAGE_TYPE,
        lambda: WeMessageApi.INSTANCE.sendImage(destination, path),
        stop,
    )
    ctx.log.info(f"Image {server_id} to {destination}: {outcome}")

    if outcome == "sent":
        if DELETE_FORWARDED_IMAGES:
            _delete_later(ctx, path, timers)
    elif not outcome.startswith("pending"):
        ctx.log.error(f"Could not forward image {server_id} to {destination}; kept {path}")


def _message_source(values) -> bytes | None:
    """Reads a new message's hidden metadata, used to recognise group announcements.

    WeChat normally includes it in the inserted row. If it doesn't, the row is read
    back from the database, since it has already been written by this point.
    """
    raw = values.getAsByteArray("lvbuffer")
    if raw is None:
        server_id = _as_int(values.getAsLong("msgSvrId"))
        if not server_id:
            return None
        rows = WeDatabaseApi.INSTANCE.executeQuery(
            f"SELECT lvbuffer AS value FROM message WHERE msgSvrId={server_id}", None
        )
        raw = None if rows.isEmpty() else rows.get(0).get("value")
    # NULL arrives as an empty string; a real value arrives as a Java byte array.
    if raw is None or isinstance(raw, str):
        return None
    return bytes(raw)


def _own_wechat_id() -> str | None:
    try:
        return str(WeApi.INSTANCE.getSelfWxId()) or None
    except Exception:  # noqa: BLE001 - without it, mentions just go unanswered
        return None


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

    mention_reply = None
    replies = expand_replies(MENTION_REPLY_MESSAGES, MENTION_REPLY_NAMES)
    if replies:
        mention_reply = MentionReply(_own_wechat_id, replies, MENTION_REPLY_COOLDOWN_SECONDS)
        ctx.log.info(
            f"Mention replies: on ({len(replies)} possible replies, "
            f"{len(MENTION_REPLY_NAMES)} names)"
        )
    else:
        ctx.log.info("Mention replies: off (no possible replies)")

    user_blocker = UserBlocker(
        BLOCKED_USERS,
        AUTO_BLOCK_MESSAGES_PER_HOUR,
        block_seconds=AUTO_BLOCK_HOURS * 3600,
    )
    auto_block = (
        f"over {AUTO_BLOCK_MESSAGES_PER_HOUR} messages an hour blocks for {AUTO_BLOCK_HOURS}h"
        if AUTO_BLOCK_MESSAGES_PER_HOUR is not None
        else "automatic blocking off"
    )
    ctx.log.info(f"User blocklist: {user_blocker.listed_count} listed; {auto_block}")

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
        sticker_limiter=(
            RateLimiter(MAX_STICKERS_PER_USER_PER_MINUTE) if FORWARD_STICKERS else None
        ),
        blocked_words=blocked_words,
        mention_reply=mention_reply,
        user_blocker=user_blocker,
        log=ctx.log,
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

                intake.accept(
                    MessageRow(
                        talker=talker,
                        is_send=values.getAsInteger("isSend"),
                        msg_type=values.getAsInteger("type"),
                        content=values.getAsString("content"),
                        server_id=values.getAsLong("msgSvrId"),
                        create_time_ms=values.getAsLong("createTime"),
                        msg_source=_message_source(values),
                        img_path=values.getAsString("imgPath"),
                    )
                )
            except Exception:
                ctx.log.exception("Could not process a new message")

    sender = threading.Thread(
        target=run_sender,
        args=(
            outbox,
            global_limiter,
            _format_line,
            lambda destination, text: _deliver(ctx, destination, text),
            lambda forward: _download_media(ctx, forward, stop),
            lambda destination, forward, path: _deliver_media(
                ctx, destination, forward, path, stop, deletion_timers
            ),
            stop,
            COMBINE_WAIT_SECONDS,
            MAX_COMBINED_CHARACTERS,
            lambda: random.uniform(MIN_SEND_PAUSE_SECONDS, MAX_SEND_PAUSE_SECONDS),
            lambda error: ctx.log.exception(message="Could not forward a message", error=error),
            ctx.log,
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
