"""Relay logic with no Android or WeChat imports, so it can be tested on a desktop."""

from __future__ import annotations

import queue
import threading
import time
from collections import deque
from collections.abc import Callable, Hashable, Mapping
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


class Intake:
    """Decides whether a new message row should be queued for forwarding."""

    def __init__(
        self,
        routes: Mapping[str, str],
        user_limiter: RateLimiter,
        outbox: queue.Queue[Forward],
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

        try:
            self._outbox.put_nowait(Forward(row.talker, destination, sender, text))
        except queue.Full:
            return QUEUE_FULL
        return QUEUED


def run_sender(
    outbox: queue.Queue[Forward],
    global_limiter: RateLimiter,
    deliver: Callable[[Forward], object],
    stop: threading.Event,
    pause_seconds: Callable[[], float],
    on_error: Callable[[Exception], object],
) -> None:
    """Sends queued forwards in order, waiting whenever the global limit is reached."""
    while not stop.is_set():
        try:
            forward = outbox.get(timeout=0.5)
        except queue.Empty:
            continue

        while not global_limiter.try_acquire():
            if stop.wait(global_limiter.seconds_until_available()):
                return

        try:
            deliver(forward)
        except Exception as error:  # noqa: BLE001 - one failed send must not stop the relay
            on_error(error)
        stop.wait(pause_seconds())
