import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "plugin"))

from relay_core import (
    DUPLICATE,
    IGNORED,
    QUEUE_FULL,
    QUEUED,
    STALE,
    USER_RATE_LIMITED,
    Forward,
    Intake,
    MessageRow,
    Outbox,
    RateLimiter,
    combine,
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


def make_intake(user_limit=100, max_queue=100):
    outbox = Outbox(max_queue)
    clock = FakeClock()
    limiter = RateLimiter(user_limit, clock=clock)
    intake = Intake({A: B, B: A}, limiter, outbox, 300, wall_clock=lambda: NOW)
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
    return f"{forward.sender}: {forward.text}"


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
        text, used = combine(forwards, line, 9, self.fail)
        self.assertEqual(text, "x: 0\nx: 1")
        self.assertEqual(used, forwards[:2])

    def test_first_message_is_used_even_if_too_long(self):
        forwards = [Forward(A, B, "x", "a long message"), Forward(A, B, "x", "short")]
        text, used = combine(forwards, line, 5, self.fail)
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

        text, used = combine([bad, good], format_line, 100, errors.append)
        self.assertEqual(text, "x: good")
        self.assertEqual(used, [bad, good])
        self.assertEqual(str(errors[0]), "boom")


class RunSenderTest(unittest.TestCase):
    def run_until_sends(self, outbox, limiter, count, *, max_characters=1000, on_send=None):
        clock = limiter._clock
        stop = TimeAdvancingStop(clock)
        sends = []

        def deliver(destination, text):
            sends.append((clock.now, destination, text))
            if on_send:
                on_send(len(sends))
            stop.stopped = len(sends) == count

        run_sender(outbox, limiter, line, deliver, stop, 5, max_characters, lambda: 1.0, self.fail)
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
            outbox, RateLimiter(5, clock=clock), line, deliver, stop, 0, 1000, lambda: 0, errors.append
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

        run_sender(outbox, limiter, line, self.fail, stop, 0, 1000, lambda: 0, self.fail)
        self.assertEqual(len(outbox), 1)


if __name__ == "__main__":
    unittest.main()
