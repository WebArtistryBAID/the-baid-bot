import random
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "plugin"))

from relay_core import (
    ANNOUNCEMENT,
    BAD_IMAGE,
    BLOCKED,
    DUPLICATE,
    IGNORED,
    IMAGE_MESSAGE_TYPE,
    IMAGE_RATE_LIMITED,
    QUEUE_FULL,
    QUEUED,
    STALE,
    USER_RATE_LIMITED,
    BlockedWords,
    Forward,
    Intake,
    MentionReply,
    MessageRow,
    Outbox,
    RateLimiter,
    combine,
    is_announcement,
    load_blocked_words,
    mentioned_users,
    normalise_for_matching,
    parse_group_content,
    run_sender,
)

A = "a@chatroom"
B = "b@chatroom"
NOW = 1_000_000.0


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def row(**overrides):
    values = {
        "talker": A,
        "is_send": 0,
        "msg_type": 1,
        "content": "wxid_x:\nhello",
        "server_id": None,
        "create_time_ms": int(NOW * 1000),
    }
    return MessageRow(**{**values, **overrides})


IMAGE_XML = '<?xml version="1.0"?>\n<msg>\n\t<img aeskey="k" length="2048" />\n</msg>\n'


def image_row(sender="wxid_x", server_id=500, content=None, **overrides):
    return row(
        msg_type=IMAGE_MESSAGE_TYPE,
        content=content if content is not None else f"{sender}:\n{IMAGE_XML}",
        server_id=server_id,
        **overrides,
    )


class RecordingLog:
    def __init__(self):
        self.lines = []

    def debug(self, message):
        self.lines.append(("debug", message))

    def info(self, message):
        self.lines.append(("info", message))

    def warning(self, message):
        self.lines.append(("warning", message))

    def messages(self):
        return [message for _, message in self.lines]


def make_intake(user_limit=100, max_queue=100, image_limit=None, blocked=(), log=None):
    outbox = Outbox(max_queue)
    clock = FakeClock()
    limiter = RateLimiter(user_limit, clock=clock)
    image_limiter = RateLimiter(image_limit, clock=clock) if image_limit else None
    intake = Intake(
        {A: B, B: A},
        limiter,
        outbox,
        300,
        image_limiter=image_limiter,
        blocked_words=BlockedWords(blocked),
        log=log,
        wall_clock=lambda: NOW,
    )
    return intake, outbox, clock


class RateLimiterTest(unittest.TestCase):
    def test_blocks_at_limit_and_recovers_after_window(self):
        clock = FakeClock()
        limiter = RateLimiter(2, clock=clock)
        self.assertTrue(limiter.try_acquire())
        clock.now = 10
        self.assertTrue(limiter.try_acquire())
        self.assertFalse(limiter.try_acquire())
        self.assertEqual(limiter.seconds_until_available(), 50)

        clock.now = 59.9
        self.assertFalse(limiter.try_acquire())
        clock.now = 60
        self.assertEqual(limiter.seconds_until_available(), 0)
        self.assertTrue(limiter.try_acquire())
        self.assertFalse(limiter.try_acquire())

    def test_window_slides_instead_of_resetting(self):
        clock = FakeClock()
        limiter = RateLimiter(2, clock=clock)
        limiter.try_acquire()
        clock.now = 59
        limiter.try_acquire()
        clock.now = 61
        self.assertTrue(limiter.try_acquire())
        self.assertFalse(limiter.try_acquire())

    def test_keys_are_independent(self):
        limiter = RateLimiter(1, clock=FakeClock())
        self.assertTrue(limiter.try_acquire("alice"))
        self.assertFalse(limiter.try_acquire("alice"))
        self.assertTrue(limiter.try_acquire("bob"))

    def test_expired_keys_are_forgotten(self):
        clock = FakeClock()
        limiter = RateLimiter(1, clock=clock)
        limiter.try_acquire("alice")
        clock.now = 60
        limiter.try_acquire("bob")
        self.assertEqual(list(limiter._events), ["bob"])

    def test_rejects_limit_below_one(self):
        with self.assertRaises(ValueError):
            RateLimiter(0)


class ParseTest(unittest.TestCase):
    def test_splits_sender_from_text(self):
        self.assertEqual(parse_group_content("wxid_x:\nhello"), ("wxid_x", "hello"))

    def test_keeps_later_separators_in_text(self):
        self.assertEqual(parse_group_content("wxid_x:\nsee:\nthis"), ("wxid_x", "see:\nthis"))

    def test_rejects_unexpected_content(self):
        for content in (None, "", "no separator", ":\nno sender", "wxid_x:\n"):
            self.assertIsNone(parse_group_content(content))


class BlockedWordsTest(unittest.TestCase):
    def test_matches_words_inside_longer_text(self):
        words = BlockedWords(["广告", "spam"])
        self.assertTrue(words.matches("这是广告，欢迎购买"))
        self.assertTrue(words.matches("no spamming please"))
        self.assertFalse(words.matches("一条普通的消息"))

    def test_ignores_case_and_full_width_characters(self):
        words = BlockedWords(["Spam", "ＶＰＮ"])
        self.assertTrue(words.matches("SPAM here"))
        self.assertTrue(words.matches("ｓｐａｍ"))
        self.assertTrue(words.matches("cheap vpn"))

    def test_blank_entries_never_block_everything(self):
        words = BlockedWords(["", "   "])
        self.assertFalse(words.matches("hello"))
        self.assertFalse(BlockedWords([]).matches("hello"))

    def test_a_single_string_is_one_word_not_its_letters(self):
        words = BlockedWords("spam")
        self.assertTrue(words.matches("spam"))
        self.assertFalse(words.matches("a plain message"))

    def test_overlapping_and_nested_words(self):
        # Classic Aho-Corasick cases: words inside words, and a failed partial match
        # ("hers" broken off) that must fall back to a shorter word ("she", "he").
        words = BlockedWords(["he", "she", "his", "hers"])
        for text, expected in {
            "ushers": True,
            "shx": False,
            "ahishers": True,
            "this": True,
            "hx": False,
            "sh": False,
        }.items():
            with self.subTest(text):
                self.assertEqual(words.matches(text), expected)

        self.assertTrue(BlockedWords(["abcd", "bc"]).matches("abce"))
        self.assertFalse(BlockedWords(["abcd", "bce"]).matches("abcf"))
        self.assertTrue(BlockedWords(["aab"]).matches("aaab"))

    def test_agrees_with_a_simple_search_on_random_text(self):
        rng = random.Random(7)
        alphabet = "ab广告"
        for _ in range(300):
            words = ["".join(rng.choice(alphabet) for _ in range(rng.randint(1, 4)))
                     for _ in range(rng.randint(1, 6))]
            text = "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12)))
            expected = any(normalise_for_matching(w) in normalise_for_matching(text) for w in words)
            with self.subTest(words=words, text=text):
                self.assertEqual(BlockedWords(words).matches(text), expected)

    def test_count_ignores_duplicates_and_blanks(self):
        self.assertEqual(BlockedWords(["a", "A", " a ", "", "b"]).count, 2)

    def test_surrounding_spaces_in_a_word_are_ignored(self):
        self.assertTrue(BlockedWords([" spam "]).matches("spam!"))


class LoadBlockedWordsTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "blocked_words.txt"

    def test_missing_file_turns_the_feature_off(self):
        self.assertIsNone(load_blocked_words(self.path))

    def test_reads_one_item_per_line(self):
        self.path.write_text("广告\r\n\n  free money  \n代购", encoding="utf-8")
        words = load_blocked_words(self.path)
        self.assertEqual(words.count, 3)
        self.assertTrue(words.matches("get FREE MONEY now"))
        self.assertTrue(words.matches("海外代购"))
        self.assertFalse(words.matches("free"))

    def test_byte_order_mark_is_ignored(self):
        self.path.write_bytes("\ufeff广告\n".encode())
        self.assertTrue(load_blocked_words(self.path).matches("广告"))

    def test_empty_file_blocks_nothing(self):
        self.path.write_text("", encoding="utf-8")
        words = load_blocked_words(self.path)
        self.assertEqual(words.count, 0)
        self.assertFalse(words.matches("anything"))

    def test_unreadable_file_raises(self):
        self.path.write_bytes(b"\xff\xfe\xfa")
        with self.assertRaises(UnicodeDecodeError):
            load_blocked_words(self.path)


class AnnouncementTest(unittest.TestCase):
    def test_recognises_announcements_only(self):
        self.assertTrue(is_announcement(b"<atuserlist>announcement@all</atuserlist>"))
        self.assertTrue(is_announcement(b"<atuserlist>wxid_a,announcement@all</atuserlist>"))
        self.assertFalse(is_announcement(b"<atuserlist>notify@all</atuserlist>"))
        self.assertFalse(is_announcement(b""))
        self.assertFalse(is_announcement(None))

    def test_announcement_is_not_queued_and_uses_no_quota(self):
        intake, outbox, _ = make_intake(user_limit=1)
        source = b"<msgsource><atuserlist>announcement@all</atuserlist></msgsource>"
        self.assertEqual(intake.accept(row(msg_source=source)), ANNOUNCEMENT)
        self.assertEqual(len(outbox), 0)
        self.assertEqual(intake.accept(row()), QUEUED)

    def test_announcements_are_dropped_in_both_directions(self):
        intake, _, _ = make_intake()
        source = b"announcement@all"
        self.assertEqual(intake.accept(row(talker=B, msg_source=source)), ANNOUNCEMENT)


class IntakeLogTest(unittest.TestCase):
    def accept(self, row_to_accept, **intake_options):
        log = RecordingLog()
        intake, _outbox, clock = make_intake(log=log, **intake_options)
        return intake, log, clock

    def test_queued_message_names_sender_groups_and_queue_length(self):
        intake, log, _ = self.accept(None)
        intake.accept(row())
        intake.accept(row(content="wxid_y:\nhi"))
        self.assertEqual(
            log.lines,
            [
                ("info", f"Queued a message from wxid_x in {A} for {B} (1 waiting)"),
                ("info", f"Queued a message from wxid_y in {A} for {B} (2 waiting)"),
            ],
        )

    def test_each_reason_for_not_forwarding_is_explained(self):
        log = RecordingLog()
        intake, _, _ = make_intake(user_limit=1, max_queue=2, image_limit=1, blocked=["spam"], log=log)
        source = b"announcement@all"
        cases = [
            (row(msg_source=source), "info", f"Skipped a group announcement from wxid_x in {A}"),
            (row(content="wxid_x:\nspam"), "info",
             f"Blocked a message from wxid_x in {A}: it contains a blocked word"),
            (image_row(server_id=None), "warning", f"Skipped an image from wxid_x in {A}: it has no server ID"),
            (row(create_time_ms=int((NOW - 400) * 1000)), "warning",
             f"Skipped an old message from wxid_x in {A}: sent 400s ago, over the 300s limit"),
            (row(server_id=9), "info", f"Queued a message from wxid_x in {A} for {B} (1 waiting)"),
            (row(server_id=9), "debug", f"Skipped a repeat of message 9 from wxid_x in {A}"),
            (row(server_id=10), "warning",
             f"Message limit reached for wxid_x (1 per minute); dropped their message in {A}"),
            (image_row(sender="wxid_y", server_id=11), "info",
             f"Queued an image from wxid_y in {A} for {B} (2 waiting)"),
            (image_row(sender="wxid_y", server_id=12), "warning",
             f"Image limit reached for wxid_y (1 per minute); dropped their image in {A}"),
            (row(content="wxid_z:\nhello", server_id=13), "warning",
             f"Queue full (2 waiting); dropped a message from wxid_z in {A}"),
        ]
        for message_row, level, expected in cases:
            with self.subTest(expected):
                intake.accept(message_row)
                self.assertEqual(log.lines[-1], (level, expected))

    def test_ignored_rows_are_not_logged(self):
        log = RecordingLog()
        intake, _, _ = make_intake(log=log)
        intake.accept(row(talker="c@chatroom"))
        intake.accept(row(is_send=1))
        self.assertEqual(log.lines, [])


MENTIONS_BOT = b"<msgsource><atuserlist><![CDATA[,wxid_a, wxid_bot]]></atuserlist></msgsource>"


def mention_reply(people=("甲", "乙"), cooldown=60, clock=None, own_id="wxid_bot"):
    return MentionReply(
        lambda: own_id,
        "喵喵喵，我猜你是{person}",
        list(people),
        cooldown,
        choose=lambda names: names[-1],
        clock=clock or FakeClock(),
    )


class MentionedUsersTest(unittest.TestCase):
    def test_reads_plain_and_cdata_lists(self):
        self.assertEqual(mentioned_users(b"<atuserlist>wxid_a,wxid_b</atuserlist>"), {"wxid_a", "wxid_b"})
        self.assertEqual(mentioned_users(MENTIONS_BOT), {"wxid_a", "wxid_bot"})

    def test_handles_missing_or_empty_lists(self):
        for source in (None, b"", b"<msgsource></msgsource>", b"<atuserlist></atuserlist>"):
            with self.subTest(source):
                self.assertEqual(mentioned_users(source), set())


class MentionReplyTest(unittest.TestCase):
    def test_replies_only_when_the_bot_is_mentioned(self):
        reply = mention_reply()
        self.assertEqual(reply.reply_to("wxid_x", MENTIONS_BOT), "喵喵喵，我猜你是乙")
        self.assertIsNone(reply.reply_to("wxid_y", b"<atuserlist>wxid_a</atuserlist>"))
        self.assertIsNone(reply.reply_to("wxid_y", None))

    def test_no_reply_when_the_bot_id_is_unknown(self):
        self.assertIsNone(mention_reply(own_id=None).reply_to("wxid_x", MENTIONS_BOT))

    def test_cooldown_is_per_person(self):
        clock = FakeClock()
        reply = mention_reply(clock=clock)
        self.assertIsInstance(reply.reply_to("wxid_x", MENTIONS_BOT), str)
        self.assertIs(reply.reply_to("wxid_x", MENTIONS_BOT), MentionReply.COOLING_DOWN)
        self.assertIsInstance(reply.reply_to("wxid_y", MENTIONS_BOT), str)
        clock.now = 60
        self.assertIsInstance(reply.reply_to("wxid_x", MENTIONS_BOT), str)

    def test_picks_from_the_people_given(self):
        reply = MentionReply(lambda: "wxid_bot", "{person}", ["甲", "乙", "丙"], 0.0)
        seen = {reply.reply_to(f"wxid_{n}", MENTIONS_BOT) for n in range(200)}
        self.assertEqual(seen, {"甲", "乙", "丙"})

    def test_needs_at_least_one_name(self):
        with self.assertRaises(ValueError):
            mention_reply(people=["", "  "])
        self.assertEqual(mention_reply(people="甲").people_count, 1)


class MentionIntakeTest(unittest.TestCase):
    def make(self, **reply_options):
        log = RecordingLog()
        outbox = Outbox(10)
        intake = Intake(
            {A: B, B: A}, RateLimiter(100, clock=FakeClock()), outbox, 300,
            mention_reply=mention_reply(**reply_options), log=log, wall_clock=lambda: NOW,
        )
        return intake, outbox, log

    def test_mention_queues_the_forward_and_a_reply_to_the_same_group(self):
        intake, outbox, log = self.make()
        self.assertEqual(intake.accept(row(msg_source=MENTIONS_BOT)), QUEUED)
        self.assertEqual(outbox.waiting_for(B), [Forward(A, B, "wxid_x", "hello")])
        self.assertEqual(
            outbox.waiting_for(A), [Forward(A, A, "wxid_x", "喵喵喵，我猜你是乙", is_reply=True)]
        )
        self.assertEqual(log.lines[-1], ("info", f"Queued a mention reply to wxid_x in {A}"))

    def test_second_mention_within_the_cooldown_is_forwarded_without_a_reply(self):
        intake, outbox, log = self.make()
        intake.accept(row(msg_source=MENTIONS_BOT))
        intake.accept(row(msg_source=MENTIONS_BOT))
        self.assertEqual(len(outbox.waiting_for(A)), 1)
        self.assertEqual(len(outbox.waiting_for(B)), 2)
        self.assertEqual(log.lines[-1], ("info", f"Not replying to a mention from wxid_x in {A}: on cooldown"))

    def test_blocked_or_announcement_mentions_get_no_reply(self):
        intake, outbox, _ = self.make()
        intake.accept(row(msg_source=MENTIONS_BOT + b"announcement@all"))
        self.assertEqual(len(outbox), 0)


class BlockedIntakeTest(unittest.TestCase):
    def test_message_with_a_blocked_word_is_not_queued(self):
        intake, outbox, _ = make_intake(blocked=["广告"])
        self.assertEqual(intake.accept(row(content="wxid_x:\n免费广告")), BLOCKED)
        self.assertEqual(len(outbox), 0)
        self.assertEqual(intake.accept(row(content="wxid_x:\n你好")), QUEUED)

    def test_blocked_message_does_not_use_the_sender_quota(self):
        intake, _, _ = make_intake(user_limit=1, blocked=["spam"])
        self.assertEqual(intake.accept(row(content="wxid_x:\nspam")), BLOCKED)
        self.assertEqual(intake.accept(row(content="wxid_x:\nhello")), QUEUED)

    def test_blocking_applies_in_both_directions(self):
        intake, _, _ = make_intake(blocked=["spam"])
        self.assertEqual(intake.accept(row(talker=B, content="wxid_x:\nspam")), BLOCKED)

    def test_images_are_not_checked(self):
        # An image's content is XML, so a blocked word inside it must not block the image.
        intake, _, _ = make_intake(image_limit=3, blocked=["img"])
        self.assertEqual(intake.accept(image_row()), QUEUED)


class ImageIntakeTest(unittest.TestCase):
    def test_images_are_ignored_unless_enabled(self):
        intake, outbox, _ = make_intake()
        self.assertEqual(intake.accept(image_row()), IGNORED)
        self.assertEqual(len(outbox), 0)

    def test_image_is_queued_with_its_server_id(self):
        intake, outbox, _ = make_intake(image_limit=3)
        self.assertEqual(intake.accept(image_row(server_id=500)), QUEUED)
        forward = outbox.waiting_for(B)[0]
        self.assertTrue(forward.is_image)
        self.assertEqual((forward.sender, forward.text), ("wxid_x", ""))
        self.assertEqual(forward.image_server_id, 500)

    def test_image_without_a_server_id_is_dropped(self):
        intake, outbox, _ = make_intake(image_limit=3)
        self.assertEqual(intake.accept(image_row(server_id=None)), BAD_IMAGE)
        self.assertEqual(len(outbox), 0)

    def test_own_image_is_ignored(self):
        intake, _, _ = make_intake(image_limit=3)
        self.assertEqual(intake.accept(image_row(is_send=1)), IGNORED)

    def test_image_limit_is_per_person_and_resets(self):
        intake, _, clock = make_intake(image_limit=2)
        self.assertEqual(intake.accept(image_row(server_id=1)), QUEUED)
        self.assertEqual(intake.accept(image_row(server_id=2)), QUEUED)
        self.assertEqual(intake.accept(image_row(server_id=3)), IMAGE_RATE_LIMITED)
        self.assertEqual(intake.accept(image_row(sender="wxid_y", server_id=4)), QUEUED)
        clock.now = 60
        self.assertEqual(intake.accept(image_row(server_id=5)), QUEUED)

    def test_refused_image_does_not_use_the_message_quota(self):
        intake, _, _ = make_intake(user_limit=2, image_limit=1)
        self.assertEqual(intake.accept(image_row(server_id=1)), QUEUED)
        self.assertEqual(intake.accept(image_row(server_id=2)), IMAGE_RATE_LIMITED)
        self.assertEqual(intake.accept(row()), QUEUED)

    def test_images_count_toward_the_message_quota(self):
        intake, _, _ = make_intake(user_limit=1, image_limit=5)
        self.assertEqual(intake.accept(image_row()), QUEUED)
        self.assertEqual(intake.accept(row()), USER_RATE_LIMITED)


class IntakeTest(unittest.TestCase):
    def test_queues_for_the_other_group_in_both_directions(self):
        intake, outbox, _ = make_intake()
        self.assertEqual(intake.accept(row(talker=A)), QUEUED)
        self.assertEqual(intake.accept(row(talker=B)), QUEUED)
        self.assertEqual(outbox.waiting_for(B), [Forward(A, B, "wxid_x", "hello")])
        self.assertEqual(outbox.waiting_for(A), [Forward(B, A, "wxid_x", "hello")])

    def test_ignores_what_should_not_be_forwarded(self):
        intake, outbox, _ = make_intake()
        cases = {
            "own message": row(is_send=1),
            "other chat": row(talker="c@chatroom"),
            "image": row(msg_type=3),
            "system notice": row(msg_type=10000),
            "malformed": row(content="no separator"),
        }
        for name, message in cases.items():
            with self.subTest(name):
                self.assertEqual(intake.accept(message), IGNORED)
        self.assertEqual(len(outbox), 0)

    def test_duplicate_is_skipped_without_using_quota(self):
        intake, outbox, _ = make_intake(user_limit=2)
        self.assertEqual(intake.accept(row(server_id=7)), QUEUED)
        self.assertEqual(intake.accept(row(server_id=7)), DUPLICATE)
        self.assertEqual(intake.accept(row(server_id=8)), QUEUED)
        self.assertEqual(len(outbox), 2)

    def test_stale_message_is_skipped(self):
        intake, _, _ = make_intake()
        old = int((NOW - 301) * 1000)
        recent = int((NOW - 299) * 1000)
        self.assertEqual(intake.accept(row(create_time_ms=old)), STALE)
        self.assertEqual(intake.accept(row(create_time_ms=recent)), QUEUED)

    def test_per_user_limit_applies_per_person_across_both_groups(self):
        intake, outbox, clock = make_intake(user_limit=2)
        self.assertEqual(intake.accept(row(talker=A)), QUEUED)
        self.assertEqual(intake.accept(row(talker=B)), QUEUED)
        self.assertEqual(intake.accept(row(talker=A)), USER_RATE_LIMITED)
        self.assertEqual(intake.accept(row(content="wxid_y:\nhi")), QUEUED)
        self.assertEqual(len(outbox), 3)

        clock.now = 60
        self.assertEqual(intake.accept(row(talker=A)), QUEUED)

    def test_full_queue_drops_the_new_message(self):
        intake, outbox, _ = make_intake(max_queue=1)
        self.assertEqual(intake.accept(row()), QUEUED)
        self.assertEqual(intake.accept(row(content="wxid_y:\nhi")), QUEUE_FULL)
        self.assertEqual(len(outbox), 1)


class TimeAdvancingStop:
    """A stop event whose wait moves a fake clock forward instead of sleeping."""

    def __init__(self, clock):
        self.clock = clock
        self.stopped = False
        self.waits = []

    def is_set(self):
        return self.stopped

    def wait(self, seconds):
        self.waits.append(seconds)
        self.clock.now += seconds
        return self.stopped


def line(forward):
    if forward.is_image:
        return f"{forward.sender}: [image]"
    return f"{forward.sender}: {forward.text}"


def image(sender, server_id, destination=B):
    source = A if destination == B else B
    return Forward(source, destination, sender, "", image_server_id=server_id)


class CombineReplyTest(unittest.TestCase):
    def test_reply_is_never_combined_with_other_messages(self):
        reply = Forward(A, A, "bob", "喵", is_reply=True)
        forwards = [Forward(B, A, "x", "1"), reply, Forward(B, A, "y", "2")]
        self.assertEqual(combine(forwards, line, 1000, self.fail), ("x: 1", forwards[:1], None, None))
        self.assertEqual(combine(forwards[1:], line, 1000, self.fail), ("喵", [reply], None, None))


class CombineImageTest(unittest.TestCase):
    def test_stops_at_an_image_and_ends_with_its_credit(self):
        photo = image("alice", 7)
        forwards = [Forward(A, B, "bob", "hi"), photo, Forward(A, B, "carol", "after")]
        text, used, next_image, credit = combine(forwards, line, 1000, self.fail)
        self.assertEqual(text, "bob: hi")
        self.assertEqual(credit, "alice: [image]")
        self.assertEqual(used, forwards[:2])
        self.assertIs(next_image, photo)

    def test_image_whose_credit_does_not_fit_waits_for_the_next_send(self):
        forwards = [Forward(A, B, "bob", "hi"), image("alice", 7)]
        result = combine(forwards, line, len("bob: hi"), self.fail)
        self.assertEqual(result, ("bob: hi", forwards[:1], None, None))

    def test_image_first_gets_its_credit_alone(self):
        photo = image("alice", 7)
        result = combine([photo], line, 1000, self.fail)
        self.assertEqual(result, ("", [photo], photo, "alice: [image]"))


class OutboxTest(unittest.TestCase):
    def test_refuses_messages_once_full(self):
        outbox = Outbox(1)
        self.assertTrue(outbox.put(Forward(A, B, "wxid_x", "1")))
        self.assertFalse(outbox.put(Forward(A, B, "wxid_x", "2")))
        self.assertEqual(len(outbox), 1)

    def test_next_destination_is_the_oldest_message_group(self):
        outbox = Outbox(10)
        outbox.put(Forward(B, A, "wxid_x", "1"))
        outbox.put(Forward(A, B, "wxid_x", "2"))
        self.assertEqual(outbox.next_destination(timeout=0), A)

    def test_next_destination_is_none_when_empty(self):
        self.assertIsNone(Outbox(10).next_destination(timeout=0.01))

    def test_next_destination_wakes_when_a_message_arrives(self):
        outbox = Outbox(10)
        threading.Timer(0.05, outbox.put, [Forward(A, B, "wxid_x", "1")]).start()
        self.assertEqual(outbox.next_destination(timeout=5), B)

    def test_remove_only_takes_the_given_message_even_if_another_is_identical(self):
        outbox = Outbox(10)
        first = Forward(A, B, "wxid_x", "same")
        second = Forward(A, B, "wxid_x", "same")
        outbox.put(first)
        outbox.put(second)
        outbox.remove([first])
        remaining = outbox.waiting_for(B)
        self.assertEqual(len(remaining), 1)
        self.assertIs(remaining[0], second)


class CombineTest(unittest.TestCase):
    def test_joins_lines_until_the_limit(self):
        forwards = [Forward(A, B, "x", str(number)) for number in range(3)]
        # Each line is "x: N" (4 characters), so two lines with a newline take 9.
        text, used, _, _ = combine(forwards, line, 9, self.fail)
        self.assertEqual(text, "x: 0\nx: 1")
        self.assertEqual(used, forwards[:2])

    def test_first_message_is_used_even_if_too_long(self):
        forwards = [Forward(A, B, "x", "a long message"), Forward(A, B, "x", "short")]
        text, used, _, _ = combine(forwards, line, 5, self.fail)
        self.assertEqual(text, "x: a long message")
        self.assertEqual(used, forwards[:1])

    def test_message_that_cannot_be_formatted_is_reported_and_used_up(self):
        bad = Forward(A, B, "x", "bad")
        good = Forward(A, B, "x", "good")
        errors = []

        def format_line(forward):
            if forward is bad:
                raise RuntimeError("boom")
            return line(forward)

        text, used, _, _ = combine([bad, good], format_line, 100, errors.append)
        self.assertEqual(text, "x: good")
        self.assertEqual(used, [bad, good])
        self.assertEqual(str(errors[0]), "boom")


class RunSenderTest(unittest.TestCase):
    def run_until_sends(
        self, outbox, limiter, count, *, max_characters=1000, on_send=None, prepare=None,
        events=None, on_error=None,
    ):
        events = events if events is not None else []
        clock = limiter._clock
        stop = TimeAdvancingStop(clock)
        sends = []

        def deliver(destination, text):
            events.append(f"send {text}")
            sends.append((clock.now, destination, text))
            if on_send:
                on_send(len(sends))
            stop.stopped = len(sends) == count

        def prepare_image(forward):
            events.append(f"prepare {forward.image_server_id}")
            return prepare(forward) if prepare else f"file {forward.image_server_id}"

        def deliver_image(destination, forward, prepared):
            deliver(destination, f"<image {forward.image_server_id} from {prepared}>")

        run_sender(
            outbox, limiter, line, deliver, prepare_image, deliver_image,
            stop, 5, max_characters, lambda: 1.0, on_error or self.fail,
        )
        return sends

    def test_waiting_messages_for_a_group_go_out_as_one_send(self):
        outbox = Outbox(10)
        for number in range(3):
            outbox.put(Forward(A, B, "wxid_x", str(number)))

        sends = self.run_until_sends(outbox, RateLimiter(5, clock=FakeClock()), 1)

        self.assertEqual(sends, [(5, B, "wxid_x: 0\nwxid_x: 1\nwxid_x: 2")])
        self.assertEqual(len(outbox), 0)

    def test_each_group_gets_its_own_send_starting_with_the_oldest(self):
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "alice", "1"))
        outbox.put(Forward(B, A, "bob", "2"))
        outbox.put(Forward(A, B, "carol", "3"))

        sends = self.run_until_sends(outbox, RateLimiter(5, clock=FakeClock()), 2)

        self.assertEqual(
            [(destination, text) for _, destination, text in sends],
            [(B, "alice: 1\ncarol: 3"), (A, "bob: 2")],
        )

    def test_messages_over_the_character_limit_wait_for_the_next_send(self):
        outbox = Outbox(10)
        for number in range(3):
            outbox.put(Forward(A, B, "x", str(number)))

        sends = self.run_until_sends(outbox, RateLimiter(5, clock=FakeClock()), 2, max_characters=9)

        self.assertEqual([text for _, _, text in sends], ["x: 0\nx: 1", "x: 2"])

    def test_messages_arriving_while_the_global_limit_waits_join_the_next_send(self):
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "x", "a"))

        def add_more(send_count):
            if send_count == 1:
                outbox.put(Forward(A, B, "x", "b"))
                outbox.put(Forward(A, B, "x", "c"))

        sends = self.run_until_sends(outbox, RateLimiter(1, clock=FakeClock()), 2, on_send=add_more)

        self.assertEqual(sends, [(5, B, "x: a"), (65, B, "x: b\nx: c")])

    def test_global_limit_counts_sends_not_original_messages(self):
        outbox = Outbox(100)
        for number in range(50):
            outbox.put(Forward(A, B, "x", str(number)))

        sends = self.run_until_sends(outbox, RateLimiter(1, clock=FakeClock()), 1)

        self.assertEqual(len(sends[0][2].splitlines()), 50)

    def test_image_is_sent_right_after_its_credit_and_counts_as_a_send(self):
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "bob", "hi"))
        outbox.put(image("alice", 7))
        outbox.put(Forward(A, B, "carol", "after"))

        sends = self.run_until_sends(outbox, RateLimiter(2, clock=FakeClock()), 3)

        # Two sends fit in the first minute; the third waits for the limit to reset.
        self.assertEqual(
            sends,
            [
                (5, B, "bob: hi\nalice: [image]"),
                (6, B, "<image 7 from file 7>"),
                (65, B, "carol: after"),
            ],
        )

    def test_image_waits_for_the_global_limit_after_its_credit(self):
        outbox = Outbox(10)
        outbox.put(image("alice", 7))

        sends = self.run_until_sends(outbox, RateLimiter(1, clock=FakeClock()), 2)

        self.assertEqual(sends, [(5, B, "alice: [image]"), (65, B, "<image 7 from file 7>")])

    def test_image_is_prepared_before_its_credit_is_sent(self):
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "bob", "hi"))
        outbox.put(image("alice", 7))
        events = []

        self.run_until_sends(outbox, RateLimiter(5, clock=FakeClock()), 2, events=events)

        self.assertEqual(
            events,
            ["prepare 7", "send bob: hi\nalice: [image]", "send <image 7 from file 7>"],
        )

    def test_image_that_cannot_be_prepared_is_dropped_with_its_credit(self):
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "bob", "hi"))
        outbox.put(image("alice", 7))
        outbox.put(Forward(A, B, "carol", "after"))

        sends = self.run_until_sends(
            outbox, RateLimiter(5, clock=FakeClock()), 2, prepare=lambda forward: None
        )

        self.assertEqual([text for _, _, text in sends], ["bob: hi", "carol: after"])

    def test_image_whose_preparation_raises_is_reported_and_dropped(self):
        outbox = Outbox(10)
        outbox.put(image("alice", 7))
        outbox.put(Forward(A, B, "carol", "after"))
        errors = []

        def explode(forward):
            raise RuntimeError("download broke")

        sends = self.run_until_sends(
            outbox, RateLimiter(5, clock=FakeClock()), 1, prepare=explode, on_error=errors.append
        )

        # Nothing is sent for the image, not even its credit line.
        self.assertEqual([text for _, _, text in sends], ["carol: after"])
        self.assertEqual(str(errors[0]), "download broke")

    def run_logged(self, outbox, limiter, count):
        log = RecordingLog()
        clock = limiter._clock
        stop = TimeAdvancingStop(clock)
        sends = []

        def deliver(destination, text):
            sends.append(text)
            stop.stopped = len(sends) == count

        run_sender(
            outbox, limiter, line, deliver, lambda forward: "file", lambda *args: None,
            stop, 5, 1000, lambda: 1.0, self.fail, log,
        )
        return log

    def test_each_send_is_logged_with_its_size_and_the_queue(self):
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "bob", "hi"))
        outbox.put(Forward(A, B, "carol", "yo"))

        log = self.run_logged(outbox, RateLimiter(5, clock=FakeClock()), 1)

        self.assertEqual(
            log.lines,
            # "bob: hi" + newline + "carol: yo" is 17 characters.
            [("info", f"Sent 2 messages to {B} in one send (17 characters); 0 messages still queued")],
        )

    def test_global_limit_wait_and_the_send_after_it_are_logged(self):
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "bob", "1"))

        def add_more(send_count):
            if send_count == 1:
                outbox.put(Forward(A, B, "carol", "2"))
                outbox.put(Forward(A, B, "dave", "3"))

        clock = FakeClock()
        log = RecordingLog()
        stop = TimeAdvancingStop(clock)
        sends = []

        def deliver(destination, text):
            sends.append(text)
            add_more(len(sends))
            stop.stopped = len(sends) == 2

        run_sender(
            outbox, RateLimiter(1, clock=clock), line, deliver, self.fail, self.fail,
            stop, 5, 1000, lambda: 1.0, self.fail, log,
        )

        self.assertEqual(
            log.messages(),
            [
                # The next two arrive during the first send, so they're already queued.
                f"Sent 1 message to {B} in one send (6 characters); 2 messages still queued",
                # The first send was at 5s; the next slot opens at 65s, 59s after the
                # 6s point where the second round's collect wait ended (5 + 1 pause + 5).
                "Global limit reached (1 send per minute); waiting 54s with 2 messages queued",
                (f"Sent 2 messages to {B} in one send (16 characters) after waiting for the "
                "global limit; 0 messages still queued"),
            ],
        )
        self.assertEqual(log.lines[1][0], "warning")

    def test_image_credit_is_mentioned_in_the_send_log(self):
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "bob", "hi"))
        outbox.put(image("alice", 7))

        log = self.run_logged(outbox, RateLimiter(5, clock=FakeClock()), 1)

        self.assertIn("Sent 1 message and an image credit to", log.messages()[0])

    def test_failed_image_send_is_reported_and_sending_continues(self):
        clock = FakeClock()
        outbox = Outbox(10)
        outbox.put(image("alice", 7))
        stop = TimeAdvancingStop(clock)
        errors = []
        texts = []

        def deliver(destination, text):
            texts.append(text)
            stop.stopped = len(texts) == 2

        def deliver_image(destination, forward, prepared):
            outbox.put(Forward(A, B, "bob", "next"))
            raise RuntimeError("image failed")

        run_sender(
            outbox, RateLimiter(5, clock=clock), line, deliver, lambda forward: "file",
            deliver_image, stop, 0, 1000, lambda: 0, errors.append,
        )

        self.assertEqual(texts, ["alice: [image]", "bob: next"])
        self.assertEqual(str(errors[0]), "image failed")

    def test_failed_send_is_reported_and_sending_continues(self):
        clock = FakeClock()
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "x", "bad"))
        stop = TimeAdvancingStop(clock)
        errors = []
        delivered = []

        def deliver(destination, text):
            if text == "x: bad":
                outbox.put(Forward(A, B, "x", "good"))
                raise RuntimeError("boom")
            delivered.append(text)
            stop.stopped = True

        run_sender(
            outbox, RateLimiter(5, clock=clock), line, deliver, self.fail, self.fail,
            stop, 0, 1000, lambda: 0, errors.append,
        )

        self.assertEqual(delivered, ["x: good"])
        self.assertEqual(str(errors[0]), "boom")

    def test_stops_promptly_while_waiting_for_the_global_limit(self):
        clock = FakeClock()
        limiter = RateLimiter(1, clock=clock)
        limiter.try_acquire()
        outbox = Outbox(10)
        outbox.put(Forward(A, B, "x", "waiting"))
        stop = threading.Event()
        waits = []

        def wait(seconds):
            waits.append(seconds)
            return len(waits) > 1  # The collect wait passes; the limit wait sees the stop.

        stop.wait = wait

        run_sender(
            outbox, limiter, line, self.fail, self.fail, self.fail, stop, 0, 1000, lambda: 0, self.fail
        )
        self.assertEqual(len(outbox), 1)


if __name__ == "__main__":
    unittest.main()
