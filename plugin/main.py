"""WeKit plugin that forwards text messages between two WeChat group chats."""

from __future__ import annotations

import random
import threading

from dev.ujhhgtg.wekit.features.api.core import (  # ty: ignore[unresolved-import]
    WeDatabaseApi,
    WeDatabaseListenerApi,
    WeMessageApi,
)
from java import dynamic_proxy  # ty: ignore[unresolved-import]

from .relay_core import (
    QUEUE_FULL,
    STALE,
    USER_RATE_LIMITED,
    Forward,
    Intake,
    MessageRow,
    Outbox,
    RateLimiter,
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
    return FORWARD_FORMAT.format(name=name, text=forward.text)


def _deliver(ctx, destination: str, text: str) -> None:
    if not WeMessageApi.INSTANCE.sendText(destination, text):
        ctx.log.error(f"Could not send to {destination}")


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

    stop = threading.Event()
    outbox = Outbox(MAX_QUEUED_MESSAGES)
    global_limiter = RateLimiter(GLOBAL_MAX_MESSAGES_PER_MINUTE)
    intake = Intake(
        routes,
        RateLimiter(PER_USER_MAX_MESSAGES_PER_MINUTE),
        outbox,
        MAX_MESSAGE_AGE_SECONDS,
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
                if result in (USER_RATE_LIMITED, QUEUE_FULL, STALE):
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
