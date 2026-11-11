"""Relay logic with no Android or WeChat imports, so it can be tested on a desktop."""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable, Hashable, Iterable, Mapping
from dataclasses import dataclass

TEXT_MESSAGE_TYPE = 1

# Results returned by Intake.accept.
QUEUED = "queued"
IGNORED = "ignored"
DUPLICATE = "duplicate"
STALE = "stale"
USER_RATE_LIMITED = "user_rate_limited"
QUEUE_FULL = "queue_full"


class RateLimiter:
    """Allows at most `limit` events per key within a sliding time window."""

    def __init__(
        self,
        limit: int,
        window_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if limit < 1:
            raise ValueError("rate limit must be at least 1")
        self._limit = limit
        self._window = window_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._events: dict[Hashable, deque[float]] = {}

    def _prune(self, now: float) -> None:
        for key, events in list(self._events.items()):
            while events and now - events[0] >= self._window:
                events.popleft()
            if not events:
                del self._events[key]

    def try_acquire(self, key: Hashable = "global") -> bool:
        with self._lock:
            now = self._clock()
            self._prune(now)
            events = self._events.get(key)
            if events is not None and len(events) >= self._limit:
                return False
            self._events.setdefault(key, deque()).append(now)
            return True

    def seconds_until_available(self, key: Hashable = "global") -> float:
        with self._lock:
            now = self._clock()
            self._prune(now)
            events = self._events.get(key)
            if events is None or len(events) < self._limit:
                return 0.0
            return events[0] + self._window - now


@dataclass(frozen=True)
class MessageRow:
    """The columns of a new row in WeChat's `message` table that the relay uses."""

    talker: str | None
    is_send: int | None
    msg_type: int | None
    content: str | None
    server_id: int | None
    create_time_ms: int | None


@dataclass(frozen=True)
class Forward:
    source_group: str
    destination_group: str
    sender: str
    text: str


def parse_group_content(content: str | None) -> tuple[str, str] | None:
    """Splits a received group message, stored as "wxid:\\ntext", into its two parts."""
    sender, separator, text = (content or "").partition(":\n")
    if not separator or not sender or not text:
        return None
    return sender, text


class Outbox:
    """Messages waiting to be sent, in arrival order, across both destination groups."""

    def __init__(self, max_messages: int) -> None:
        self._max_messages = max_messages
        self._forwards: deque[Forward] = deque()
        self._condition = threading.Condition()

    def __len__(self) -> int:
        with self._condition:
            return len(self._forwards)

    def put(self, forward: Forward) -> bool:
        """Adds a message, returning False if the outbox is full."""
        with self._condition:
            if len(self._forwards) >= self._max_messages:
                return False
            self._forwards.append(forward)
            self._condition.notify()
            return True

    def next_destination(self, timeout: float) -> str | None:
        """Waits for a message, then returns the group the oldest message is going to."""
        with self._condition:
            if not self._forwards:
                self._condition.wait(timeout)
            return self._forwards[0].destination_group if self._forwards else None

    def waiting_for(self, destination: str) -> list[Forward]:
        """Returns the messages going to one group, oldest first, without removing them."""
        with self._condition:
            return [f for f in self._forwards if f.destination_group == destination]

    def remove(self, forwards: Iterable[Forward]) -> None:
        # Compare by identity, because two people can send identical messages.
        removed = {id(forward) for forward in forwards}
        with self._condition:
            self._forwards = deque(f for f in self._forwards if id(f) not in removed)


class Intake:
    """Decides whether a new message row should be queued for forwarding."""

    def __init__(
        self,
        routes: Mapping[str, str],
        user_limiter: RateLimiter,
        outbox: Outbox,
        max_age_seconds: float,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._routes = routes
        self._user_limiter = user_limiter
        self._outbox = outbox
        self._max_age_seconds = max_age_seconds
        self._wall_clock = wall_clock
        self._seen_ids: deque[int] = deque(maxlen=1000)

    def accept(self, row: MessageRow) -> str:
        destination = self._routes.get(row.talker or "")
        # is_send == 1 covers the relay's own forwards, which is what prevents a loop.
        if destination is None or row.is_send == 1 or row.msg_type != TEXT_MESSAGE_TYPE:
            return IGNORED

        parsed = parse_group_content(row.content)
        if parsed is None:
            return IGNORED
        sender, text = parsed

        if row.server_id:
            if row.server_id in self._seen_ids:
                return DUPLICATE
            self._seen_ids.append(row.server_id)

        if row.create_time_ms is not None:
            age_seconds = self._wall_clock() - row.create_time_ms / 1000
            if age_seconds > self._max_age_seconds:
                return STALE

        if not self._user_limiter.try_acquire(sender):
            return USER_RATE_LIMITED

        if not self._outbox.put(Forward(row.talker, destination, sender, text)):
            return QUEUE_FULL
        return QUEUED


def combine(
    forwards: Iterable[Forward],
    format_line: Callable[[Forward], str],
    max_characters: int,
    on_error: Callable[[Exception], object],
) -> tuple[str, list[Forward]]:
    """Joins messages into one, one line each, stopping before it gets too long.

    Returns the combined text and the messages it used up. The first message is always
    used, even if it is longer than the limit. A message that can't be formatted is
    reported and used up, so it can't block the ones behind it.
    """
    lines: list[str] = []
    used: list[Forward] = []
    length = 0
    for forward in forwards:
        try:
            line = format_line(forward)
        except Exception as error:  # noqa: BLE001 - one bad message must not stop the rest
            on_error(error)
            used.append(forward)
            continue
        added = len(line) + (1 if lines else 0)
        if lines and length + added > max_characters:
            break
        lines.append(line)
        used.append(forward)
        length += added
    return "\n".join(lines), used


def run_sender(
    outbox: Outbox,
    global_limiter: RateLimiter,
    format_line: Callable[[Forward], str],
    deliver: Callable[[str, str], object],
    stop: threading.Event,
    collect_seconds: float,
    max_characters: int,
    pause_seconds: Callable[[], float],
    on_error: Callable[[Exception], object],
) -> None:
    """Sends waiting messages, combining those going to the same group into one send.

    After the first message arrives, it waits `collect_seconds` so more can join it.
    While the global limit holds a send back, new messages keep joining it too.
    """
    while not stop.is_set():
        destination = outbox.next_destination(timeout=0.5)
        if destination is None:
            continue
        if stop.wait(collect_seconds):
            return

        while not global_limiter.try_acquire():
            if stop.wait(global_limiter.seconds_until_available()):
                return

        text, used = combine(outbox.waiting_for(destination), format_line, max_characters, on_error)
        outbox.remove(used)
        if text:
            try:
                deliver(destination, text)
            except Exception as error:  # noqa: BLE001 - one failed send must not stop the relay
                on_error(error)
        stop.wait(pause_seconds())
