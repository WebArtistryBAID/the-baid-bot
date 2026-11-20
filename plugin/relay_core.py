"""Relay logic with no Android or WeChat imports, so it can be tested on a desktop."""

from __future__ import annotations

import random
import re
import threading
import time
import unicodedata
from collections import deque
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree

TEXT_MESSAGE_TYPE = 1
IMAGE_MESSAGE_TYPE = 3
STICKER_MESSAGE_TYPE = 47
# A message that quotes another one. Its content is XML: the reply's own text is in
# <appmsg><title>, and the quoted message is in <appmsg><refermsg>.
QUOTE_MESSAGE_TYPE = 822083633
# Some WeChat versions store a quote as a general app message (links, files and so on)
# whose <appmsg><type> is 57.
APP_MESSAGE_TYPE = 49
QUOTE_APP_MESSAGE_SUBTYPE = "57"

# Results returned by Intake.accept.
QUEUED = "queued"
IGNORED = "ignored"
DUPLICATE = "duplicate"
STALE = "stale"
BAD_IMAGE = "bad_image"
BAD_STICKER = "bad_sticker"
BLOCKED = "blocked"
BLOCKED_USER = "blocked_user"
ANNOUNCEMENT = "announcement"
IMAGE_RATE_LIMITED = "image_rate_limited"
STICKER_RATE_LIMITED = "sticker_rate_limited"
USER_RATE_LIMITED = "user_rate_limited"
QUEUE_FULL = "queue_full"


class SilentLog:
    """Accepts log messages and discards them, for when no logger is given."""

    def debug(self, message: object) -> None: ...
    def info(self, message: object) -> None: ...
    def warning(self, message: object) -> None: ...


def _plural(count: int, word: str) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


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

    @property
    def limit(self) -> int:
        return self._limit

    def describe(self) -> str:
        """The limit in words, such as "5 per minute"."""
        if self._window == 60:
            return f"{self._limit} per minute"
        return f"{self._limit} per {self._window:g} seconds"

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
    # The message's hidden metadata (WeChat's `lvbuffer` column), which lists who it
    # mentions. None when WeChat didn't provide it.
    msg_source: bytes | None = None


@dataclass(frozen=True)
class Forward:
    """A message waiting to be sent.

    An image or sticker ("media") has its server ID in `media_server_id` and an empty
    `text`. Media is sent on its own, straight after a credit line naming its sender.
    """

    source_group: str
    destination_group: str
    sender: str
    text: str
    media_server_id: int | None = None
    # A reply from the bot itself, sent as its own message rather than forwarded. For a
    # reply, `text` is the whole message and both groups are the group it goes to.
    is_reply: bool = False
    # For media: a sticker rather than an image.
    is_sticker: bool = False
    # A sticker's MD5, which is the same every time anyone sends that sticker. None
    # when it couldn't be read.
    sticker_md5: str | None = None

    @property
    def is_media(self) -> bool:
        return self.media_server_id is not None


_AT_USER_LIST = re.compile(
    rb"<atuserlist>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</atuserlist>", re.DOTALL
)


def mentioned_users(msg_source: bytes | None) -> set[str]:
    """The WeChat IDs a message mentions, read from its hidden metadata."""
    if not msg_source:
        return set()
    found = _AT_USER_LIST.search(msg_source)
    if found is None:
        return set()
    ids = found.group(1).decode("utf-8", errors="ignore").split(",")
    return {wxid.strip() for wxid in ids if wxid.strip()}


def is_announcement(msg_source: bytes | None) -> bool:
    """Whether a message is a group announcement (群公告).

    WeChat marks an announcement by listing "announcement@all" among the users it
    mentions. A plain @所有人 message lists "notify@all" instead, so it isn't matched.
    """
    return msg_source is not None and b"announcement@all" in msg_source


def parse_group_content(content: str | None) -> tuple[str, str] | None:
    """Splits a received group message, stored as "wxid:\\ntext", into its two parts."""
    sender, separator, text = (content or "").partition(":\n")
    if not separator or not sender or not text:
        return None
    return sender, text


# A sticker that can't be forwarded: one from the sticker store, or a dice or
# rock-paper-scissors game whose result is random.
STORE_STICKER = "store"
GAME_STICKER = "game"


def sticker_kind(xml: str) -> str | None:
    """Returns STORE_STICKER or GAME_STICKER for stickers that aren't forwarded, else None.

    A sticker whose XML can't be read is treated as forwardable, since downloading it
    only needs its server ID.
    """
    try:
        root = ElementTree.fromstring(xml.strip())
    except ElementTree.ParseError:
        return None
    emoji = root if root.tag == "emoji" else root.find("emoji")
    if emoji is None:
        return None
    if root.find(".//gameext") is not None or (emoji.get("gameext") or "").strip():
        return GAME_STICKER
    if (emoji.get("productid") or "").strip():
        return STORE_STICKER
    return None


def sticker_md5(xml: str) -> str | None:
    """Returns a sticker's MD5 from its XML, or None if it can't be read."""
    try:
        root = ElementTree.fromstring(xml.strip())
    except ElementTree.ParseError:
        return None
    emoji = root if root.tag == "emoji" else root.find("emoji")
    md5 = (emoji.get("md5") or "").strip().lower() if emoji is not None else ""
    return md5 or None


def parse_quote_text(xml: str) -> str | None:
    """Returns the reply's own text from a quote message, leaving out what it quotes.

    Returns None when the XML can't be read, isn't a quote, or has no text.
    """
    try:
        root = ElementTree.fromstring(xml.strip())
    except ElementTree.ParseError:
        return None
    appmsg = root if root.tag == "appmsg" else root.find("appmsg")
    if appmsg is None or appmsg.find("refermsg") is None:
        return None
    subtype = (appmsg.findtext("type") or "").strip()
    if subtype and subtype != QUOTE_APP_MESSAGE_SUBTYPE:
        return None
    text = (appmsg.findtext("title") or "").strip()
    return text or None


class Outbox:
    """Messages waiting to be sent, in arrival order, across both destination groups."""

    def __init__(self, max_messages: int) -> None:
        self._max_messages = max_messages
        self._forwards: deque[Forward] = deque()
        self._condition = threading.Condition()

    def __len__(self) -> int:
        with self._condition:
            return len(self._forwards)

    @property
    def max_messages(self) -> int:
        return self._max_messages

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

    def drop_sender(self, sender: str) -> int:
        """Removes everything waiting from one sender, returning how many were removed."""
        with self._condition:
            kept = deque(f for f in self._forwards if f.sender != sender)
            dropped = len(self._forwards) - len(kept)
            self._forwards = kept
            return dropped


def normalise_for_matching(text: str) -> str:
    """Folds case and character width, so "ＡＢＣ", "abc" and "ABC" all match."""
    return unicodedata.normalize("NFKC", text).casefold()


class BlockedWords:
    """Finds messages containing any of a list of words or phrases.

    Uses the Aho-Corasick algorithm: the words are built into a tree once, and each
    message is then checked in a single pass, however many words there are.
    """

    def __init__(self, words: Iterable[str]) -> None:
        # A single string would otherwise be read as a list of its characters.
        if isinstance(words, str):
            words = [words]
        # Blank entries are skipped, since an empty word would match every message.
        unique = {normalise_for_matching(word).strip() for word in words} - {""}
        self.count = len(unique)

        # Node 0 is the root. For each node: the next node for each character, the
        # node to fall back to when the next character doesn't continue the current
        # word, and whether a whole word ends here (or at any fallback node).
        self._next: list[dict[str, int]] = [{}]
        self._fallback: list[int] = [0]
        self._ends_word: list[bool] = [False]
        for word in unique:
            node = 0
            for character in word:
                child = self._next[node].get(character)
                if child is None:
                    child = len(self._next)
                    self._next[node][character] = child
                    self._next.append({})
                    self._fallback.append(0)
                    self._ends_word.append(False)
                node = child
            self._ends_word[node] = True

        # Breadth first, so every node's fallback is final before its children use it.
        queue = deque(self._next[0].values())
        while queue:
            node = queue.popleft()
            for character, child in self._next[node].items():
                queue.append(child)
                fallback = self._fallback[node]
                while fallback and character not in self._next[fallback]:
                    fallback = self._fallback[fallback]
                self._fallback[child] = self._next[fallback].get(character, 0)
                if self._ends_word[self._fallback[child]]:
                    self._ends_word[child] = True

    def matches(self, text: str) -> bool:
        if not self.count:
            return False
        next_node, fallback, ends_word = self._next, self._fallback, self._ends_word
        node = 0
        for character in normalise_for_matching(text):
            while node and character not in next_node[node]:
                node = fallback[node]
            node = next_node[node].get(character, 0)
            if ends_word[node]:
                return True
        return False


def load_blocked_words(path: Path) -> BlockedWords | None:
    """Reads one word or phrase per line, returning None if the file doesn't exist.

    Blank lines are skipped and surrounding spaces are ignored. A file that exists
    but can't be read raises an error, so the relay never quietly runs unfiltered.
    """
    try:
        text = path.read_text(encoding="utf-8-sig")
    except FileNotFoundError:
        return None
    return BlockedWords(text.splitlines())


def expand_replies(templates: Sequence[str], people: Sequence[str]) -> list[str]:
    """Turns reply templates into the full list of possible replies.

    A template containing {person} becomes one reply per person; with no people, it's
    left out. Other templates are used as they are.
    """
    if isinstance(people, str):
        people = [people]
    names = [person.strip() for person in people if person.strip()]
    replies: list[str] = []
    for template in templates:
        if "{person}" in template:
            replies.extend(template.replace("{person}", name) for name in names)
        elif template.strip():
            replies.append(template)
    return replies


class MentionReply:
    """Decides the bot's reply when someone @mentions it.

    The reply is picked at random from `replies`. Each sender gets at most one reply per
    `cooldown_seconds`, so mentions can't be used to make the bot spam the groups.
    """

    COOLING_DOWN = object()

    def __init__(
        self,
        own_id: Callable[[], str | None],
        replies: Sequence[str],
        cooldown_seconds: float,
        choose: Callable[[Sequence[str]], str] = random.choice,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._replies = list(replies)
        if not self._replies:
            raise ValueError("a mention reply needs at least one possible reply")
        self._own_id = own_id
        self._choose = choose
        self._cooldown = RateLimiter(1, window_seconds=cooldown_seconds, clock=clock)

    @property
    def reply_count(self) -> int:
        return len(self._replies)

    def reply_to(self, sender: str, msg_source: bytes | None):
        """The reply text, COOLING_DOWN, or None if the message doesn't mention the bot."""
        own_id = self._own_id()
        if not own_id or own_id not in mentioned_users(msg_source):
            return None
        if not self._cooldown.try_acquire(sender):
            return MentionReply.COOLING_DOWN
        return self._choose(self._replies)


class UserBlocker:
    """Decides whose messages are never forwarded.

    People in `listed` are always blocked. Anyone else who sends more than
    `max_messages` within `window_seconds` is blocked for `block_seconds`, counted from
    the message that went over the limit. Every message counts, whatever its type and
    whether or not it was forwarded. Set `max_messages` to None to turn this off.
    """

    LISTED = "listed"
    STARTED = "started"
    ONGOING = "ongoing"
    ENDED = "ended"

    def __init__(
        self,
        listed: Iterable[str],
        max_messages: int | None,
        window_seconds: float = 3600,
        block_seconds: float = 7200,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if isinstance(listed, str):
            listed = [listed]
        self._listed = {wxid.strip() for wxid in listed if wxid.strip()}
        self._max_messages = max_messages
        self._window = window_seconds
        self._block = block_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._sent: dict[str, deque[float]] = {}
        self._blocked_until: dict[str, float] = {}

    @property
    def listed_count(self) -> int:
        return len(self._listed)

    def _prune(self, now: float) -> None:
        for sender, times in list(self._sent.items()):
            while times and now - times[0] >= self._window:
                times.popleft()
            if not times:
                del self._sent[sender]

    def check(self, sender: str) -> str | None:
        """Records a message from `sender` and returns their block status.

        Returns LISTED, STARTED (this message put them over the limit), ONGOING, ENDED
        (an earlier block has just run out, and this message isn't blocked), or None.
        """
        if sender in self._listed:
            return self.LISTED
        if self._max_messages is None:
            return None
        with self._lock:
            now = self._clock()
            self._prune(now)
            times = self._sent.setdefault(sender, deque())
            times.append(now)

            until = self._blocked_until.get(sender)
            if until is not None:
                if now < until:
                    return self.ONGOING
                del self._blocked_until[sender]
                ended = True
            else:
                ended = False

            if len(times) > self._max_messages:
                self._blocked_until[sender] = now + self._block
                return self.STARTED
            return self.ENDED if ended else None


class Intake:
    """Decides whether a new message row should be queued for forwarding."""

    def __init__(
        self,
        routes: Mapping[str, str],
        user_limiter: RateLimiter,
        outbox: Outbox,
        max_age_seconds: float,
        *,
        image_limiter: RateLimiter | None = None,
        sticker_limiter: RateLimiter | None = None,
        blocked_words: BlockedWords | None = None,
        mention_reply: MentionReply | None = None,
        user_blocker: UserBlocker | None = None,
        log: object | None = None,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        """Images are forwarded only when `image_limiter` is given, and stickers only
        when `sticker_limiter` is given.

        `log` receives a line for each decision about a message from one of the two
        groups. Lines name the sender's WeChat ID and the group, never the message text.
        """
        self._routes = routes
        self._mention_reply = mention_reply
        self._user_blocker = user_blocker
        self._log = log or SilentLog()
        self._blocked_words = blocked_words or BlockedWords([])
        self._user_limiter = user_limiter
        self._image_limiter = image_limiter
        self._sticker_limiter = sticker_limiter
        self._outbox = outbox
        self._max_age_seconds = max_age_seconds
        self._wall_clock = wall_clock
        self._seen_ids: deque[int] = deque(maxlen=1000)

    def accept(self, row: MessageRow) -> str:
        destination = self._routes.get(row.talker or "")
        accepted_types = {TEXT_MESSAGE_TYPE, QUOTE_MESSAGE_TYPE, APP_MESSAGE_TYPE}
        if self._image_limiter is not None:
            accepted_types.add(IMAGE_MESSAGE_TYPE)
        if self._sticker_limiter is not None:
            accepted_types.add(STICKER_MESSAGE_TYPE)
        # is_send == 1 covers the relay's own forwards, which is what prevents a loop.
        if destination is None or row.is_send == 1:
            return IGNORED

        parsed = parse_group_content(row.content)
        if parsed is None:
            return IGNORED
        sender, text = parsed
        group = row.talker

        # Checked before the message type, so stickers and other messages that are never
        # forwarded still count toward the automatic block.
        if self._user_blocker is not None and self._is_blocked(sender, group):
            return BLOCKED_USER

        if row.msg_type not in accepted_types:
            return IGNORED
        # A quote is forwarded as plain text: just the reply, without the quoted message.
        # Other app messages (links, files, mini programs) are not forwarded.
        if row.msg_type in (QUOTE_MESSAGE_TYPE, APP_MESSAGE_TYPE):
            quote_text = parse_quote_text(text)
            if quote_text is None:
                if row.msg_type == QUOTE_MESSAGE_TYPE:
                    self._log.warning(
                        f"Skipped a quote from {sender} in {group}: could not read its text"
                    )
                return IGNORED
            text = quote_text
        is_image = row.msg_type == IMAGE_MESSAGE_TYPE
        is_sticker = row.msg_type == STICKER_MESSAGE_TYPE
        is_media = is_image or is_sticker
        kind = "image" if is_image else "sticker" if is_sticker else "message"
        a_kind = f"an {kind}" if is_image else f"a {kind}"
        source = f"from {sender} in {group}"

        # Checked before the rate limits, so an announcement doesn't use up any quota.
        if is_announcement(row.msg_source):
            self._log.info(f"Skipped a group announcement {source}")
            return ANNOUNCEMENT

        md5 = None
        if is_sticker:
            md5 = sticker_md5(text)
            unsupported = sticker_kind(text)
            if unsupported is not None:
                self._log.info(f"Skipped a {unsupported} sticker {source}: can't be forwarded")
                return IGNORED
        if is_media:
            # The server ID is how the sender later asks WeKit for the file.
            if not row.server_id:
                self._log.warning(f"Skipped {a_kind} {source}: it has no server ID")
                return BAD_IMAGE if is_image else BAD_STICKER
            text = ""
        # Checked before the rate limits, so a blocked message doesn't use up any quota.
        elif self._blocked_words.matches(text):
            self._log.info(f"Blocked a message {source}: it contains a blocked word")
            return BLOCKED

        if row.server_id:
            if row.server_id in self._seen_ids:
                self._log.debug(f"Skipped a repeat of {kind} {row.server_id} {source}")
                return DUPLICATE
            self._seen_ids.append(row.server_id)

        if row.create_time_ms is not None:
            age_seconds = self._wall_clock() - row.create_time_ms / 1000
            if age_seconds > self._max_age_seconds:
                self._log.warning(
                    f"Skipped an old {kind} {source}: sent {age_seconds:.0f}s ago, "
                    f"over the {self._max_age_seconds:g}s limit"
                )
                return STALE

        # Check the image or sticker limit first without using it, so refused media
        # doesn't use up the sender's general quota.
        media_limiter = (
            self._image_limiter if is_image else self._sticker_limiter if is_sticker else None
        )
        if media_limiter is not None and media_limiter.seconds_until_available(sender) > 0:
            self._log.warning(
                f"{kind.capitalize()} limit reached for {sender} ({media_limiter.describe()}); "
                f"dropped their {kind} in {group}"
            )
            return IMAGE_RATE_LIMITED if is_image else STICKER_RATE_LIMITED
        if not self._user_limiter.try_acquire(sender):
            self._log.warning(
                f"Message limit reached for {sender} ({self._user_limiter.describe()}); "
                f"dropped their {kind} in {group}"
            )
            return USER_RATE_LIMITED
        if media_limiter is not None:
            media_limiter.try_acquire(sender)

        forward = Forward(
            row.talker,
            destination,
            sender,
            text,
            media_server_id=row.server_id if is_media else None,
            is_sticker=is_sticker,
            sticker_md5=md5,
        )
        if not self._outbox.put(forward):
            self._log.warning(
                f"Queue full ({self._outbox.max_messages} waiting); dropped {a_kind} {source}"
            )
            return QUEUE_FULL
        self._log.info(
            f"Queued {a_kind} {source} for {destination} ({len(self._outbox)} waiting)"
        )
        if self._mention_reply is not None:
            self._queue_mention_reply(row, sender, group)
        return QUEUED

    def _is_blocked(self, sender: str, group: str) -> bool:
        status = self._user_blocker.check(sender)
        if status == UserBlocker.LISTED:
            self._log.debug(f"Skipped a message from {sender} in {group}: on the blocklist")
            return True
        if status == UserBlocker.STARTED:
            dropped = self._outbox.drop_sender(sender)
            self._log.warning(
                f"Blocked {sender} for sending too many messages (last seen in {group}); "
                f"dropped {_plural(dropped, 'waiting message')}"
            )
            return True
        if status == UserBlocker.ONGOING:
            self._log.debug(f"Skipped a message from {sender} in {group}: temporarily blocked")
            return True
        if status == UserBlocker.ENDED:
            self._log.info(f"Temporary block on {sender} has ended")
        return False

    def _queue_mention_reply(self, row: MessageRow, sender: str, group: str) -> None:
        reply = self._mention_reply.reply_to(sender, row.msg_source)
        if reply is None:
            return
        if reply == MentionReply.COOLING_DOWN:
            self._log.info(f"Not replying to a mention from {sender} in {group}: on cooldown")
            return
        # The reply goes to every relay group, starting with the one the mention was in.
        targets = [group, *(other for other in self._routes if other != group)]
        queued = []
        for target in targets:
            if self._outbox.put(Forward(group, target, sender, reply, is_reply=True)):
                queued.append(target)
            else:
                self._log.warning(
                    f"Queue full; not sending the mention reply for {sender} to {target}"
                )
        if queued:
            self._log.info(
                f"Queued a mention reply to {sender} (mentioned in {group}) "
                f"for {' and '.join(queued)}"
            )


def combine(
    forwards: Iterable[Forward],
    format_line: Callable[[Forward], str],
    max_characters: int,
    on_error: Callable[[Exception], object],
) -> tuple[str, list[Forward], Forward | None, str | None]:
    """Joins messages into one, one line each, stopping before it gets too long.

    An image or sticker can't be part of the text, so combining stops at the first
    one. Its line (the credit, such as "Alice: [图片]") is returned separately from the
    text, so the caller can add it only once the file is ready to send.

    Returns the combined text, the messages it used up, and the next image or sticker
    with its
    credit line (or None for both). The first message is always used, even if it is
    longer than the limit. A message that can't be formatted is reported and used up,
    so it can't block the ones behind it.
    """
    lines: list[str] = []
    used: list[Forward] = []
    length = 0
    for forward in forwards:
        # A reply is a message of its own, so it never shares a send with forwards.
        if forward.is_reply:
            if lines:
                break
            return forward.text, [*used, forward], None, None
        try:
            line = format_line(forward)
        except Exception as error:  # noqa: BLE001 - one bad message must not stop the rest
            on_error(error)
            used.append(forward)
            continue
        added = len(line) + (1 if lines else 0)
        if lines and length + added > max_characters:
            break
        used.append(forward)
        if forward.is_media:
            return "\n".join(lines), used, forward, line
        lines.append(line)
        length += added
    return "\n".join(lines), used, None, None


def _acquire(
    limiter: RateLimiter,
    stop: threading.Event,
    on_wait: Callable[[float], object] = lambda seconds: None,
) -> tuple[bool, bool]:
    """Waits for the limiter.

    Returns whether to go ahead (False if the relay stopped first), and whether it had
    to wait. `on_wait` is called once, with the expected wait, when waiting starts.
    """
    waited = False
    while not limiter.try_acquire():
        seconds = limiter.seconds_until_available()
        if not waited:
            on_wait(seconds)
            waited = True
        if stop.wait(seconds):
            return False, waited
    return True, waited


def run_sender(
    outbox: Outbox,
    global_limiter: RateLimiter,
    format_line: Callable[[Forward], str],
    deliver: Callable[[str, str], object],
    prepare_media: Callable[[Forward], object | None],
    deliver_media: Callable[[str, Forward, object], object],
    stop: threading.Event,
    collect_seconds: float,
    max_characters: int,
    pause_seconds: Callable[[], float],
    on_error: Callable[[Exception], object],
    log: object | None = None,
) -> None:
    """Sends waiting messages, combining those going to the same group into one send.

    After the first message arrives, it waits `collect_seconds` so more can join it.
    While the global limit holds a send back, new messages keep joining it too.

    An image or sticker is first prepared with `prepare_media`, which downloads it and
    returns what `deliver_media` needs, or None if it failed. Only then is its credit
    line added to the text. A failed one is dropped along with its credit line. The file
    itself is its own send, made straight after the text, and it counts toward the
    global limit like any other send.

    `log` receives a line when the global limit holds sends back, and for each send.
    """
    log = log or SilentLog()

    def on_wait(seconds: float) -> None:
        log.warning(
            f"Global limit reached ({_plural(global_limiter.limit, 'send')} per minute); waiting "
            f"{seconds:.0f}s with {_plural(len(outbox), 'message')} queued"
        )

    while not stop.is_set():
        destination = outbox.next_destination(timeout=0.5)
        if destination is None:
            continue
        if stop.wait(collect_seconds):
            return
        go, waited = _acquire(global_limiter, stop, on_wait)
        if not go:
            return

        text, used, media, credit = combine(
            outbox.waiting_for(destination), format_line, max_characters, on_error
        )
        outbox.remove(used)

        prepared = None
        if media is not None:
            try:
                prepared = prepare_media(media)
            except Exception as error:  # noqa: BLE001 - one bad file must not stop the relay
                on_error(error)
            if prepared is None:
                media = None
            else:
                text = f"{text}\n{credit}" if text else credit

        if text:
            try:
                deliver(destination, text)
            except Exception as error:  # noqa: BLE001 - one failed send must not stop the relay
                on_error(error)
            else:
                count = sum(1 for f in used if not f.is_media and not f.is_reply)
                parts = [_plural(count, "message")] if count else []
                if any(f.is_reply for f in used):
                    parts.append("a mention reply")
                if media is not None:
                    parts.append("a sticker credit" if media.is_sticker else "an image credit")
                after_wait = " after waiting for the global limit" if waited else ""
                log.info(
                    f"Sent {' and '.join(parts)} to {destination} in one send "
                    f"({len(text)} characters){after_wait}; "
                    f"{_plural(len(outbox), 'message')} still queued"
                )
        if media is not None:
            stop.wait(pause_seconds())
            go, _ = _acquire(global_limiter, stop, on_wait)
            if not go:
                return
            try:
                deliver_media(destination, media, prepared)
            except Exception as error:  # noqa: BLE001 - one failed send must not stop the relay
                on_error(error)
        stop.wait(pause_seconds())
