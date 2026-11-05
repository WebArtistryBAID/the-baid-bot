import queue
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
    RateLimiter,
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
    outbox = queue.Queue(maxsize=max_queue)
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
        self.assertEqual(outbox.get_nowait(), Forward(A, B, "wxid_x", "hello"))
        self.assertEqual(outbox.get_nowait(), Forward(B, A, "wxid_x", "hello"))

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
        self.assertTrue(outbox.empty())

    def test_duplicate_is_skipped_without_using_quota(self):
        intake, outbox, _ = make_intake(user_limit=2)
        self.assertEqual(intake.accept(row(server_id=7)), QUEUED)
        self.assertEqual(intake.accept(row(server_id=7)), DUPLICATE)
        self.assertEqual(intake.accept(row(server_id=8)), QUEUED)
        self.assertEqual(outbox.qsize(), 2)

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
        self.assertEqual(outbox.qsize(), 3)

        clock.now = 60
        self.assertEqual(intake.accept(row(talker=A)), QUEUED)

    def test_full_queue_drops_the_new_message(self):
        intake, outbox, _ = make_intake(max_queue=1)
        self.assertEqual(intake.accept(row()), QUEUED)
        self.assertEqual(intake.accept(row(content="wxid_y:\nhi")), QUEUE_FULL)
        self.assertEqual(outbox.qsize(), 1)


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


class RunSenderTest(unittest.TestCase):
    def test_global_limit_delays_but_keeps_every_message_in_order(self):
        clock = FakeClock()
        outbox = queue.Queue()
        forwards = [Forward(A, B, "wxid_x", str(number)) for number in range(5)]
        for forward in forwards:
            outbox.put(forward)

        stop = TimeAdvancingStop(clock)
        sends = []

        def deliver(forward):
            sends.append((clock.now, forward))
            stop.stopped = len(sends) == len(forwards)

        run_sender(outbox, RateLimiter(2, clock=clock), deliver, stop, lambda: 1.0, self.fail)

        self.assertEqual([forward for _, forward in sends], forwards)
        times = [sent_at for sent_at, _ in sends]
        self.assertEqual(times, [0, 1, 60, 61, 120])
        for index in range(len(times) - 2):
            self.assertGreaterEqual(times[index + 2] - times[index], 60)

    def test_failed_send_is_reported_and_sending_continues(self):
        clock = FakeClock()
        outbox = queue.Queue()
        outbox.put(Forward(A, B, "wxid_x", "bad"))
        outbox.put(Forward(A, B, "wxid_x", "good"))
        stop = TimeAdvancingStop(clock)
        errors = []
        delivered = []

        def deliver(forward):
            if forward.text == "bad":
                raise RuntimeError("boom")
            delivered.append(forward.text)
            stop.stopped = True

        run_sender(outbox, RateLimiter(5, clock=clock), deliver, stop, lambda: 0, errors.append)

        self.assertEqual(delivered, ["good"])
        self.assertEqual(str(errors[0]), "boom")

    def test_stops_promptly_while_waiting_for_the_global_limit(self):
        clock = FakeClock()
        limiter = RateLimiter(1, clock=clock)
        limiter.try_acquire()
        outbox = queue.Queue()
        outbox.put(Forward(A, B, "wxid_x", "waiting"))
        stop = threading.Event()
        stop.wait = lambda seconds: True

        run_sender(outbox, limiter, self.fail, stop, lambda: 0, self.fail)


if __name__ == "__main__":
    unittest.main()
