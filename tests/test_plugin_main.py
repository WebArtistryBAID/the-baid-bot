"""Runs plugin/main.py against fake WeKit APIs, loaded the way WeKit's bootstrap loads it."""

import importlib
import importlib.machinery
import re
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

PLUGIN_ROOT = Path(__file__).resolve().parent.parent / "plugin"
NAMESPACE = "_wekit_test_plugin"
A = "a@chatroom"
B = "b@chatroom"


class FakeValues:
    def __init__(self, **values):
        self.values = values

    def getAsString(self, key):
        return self.values.get(key)

    getAsInteger = getAsString
    getAsLong = getAsString


class FakeRows(list):
    """Behaves like the java.util.List that WeDatabaseApi.executeQuery returns."""

    def isEmpty(self):
        return len(self) == 0

    def get(self, index):
        return self[index]


class FakeDatabase:
    def __init__(self):
        self.reader_threads = []
        # (msgId, talker, isSend, type, status). A real table already holds messages.
        self.messages = [(1, A, 0, 1, 3)]

    def add_sent_image(self, talker, status):
        self.messages.append((self.messages[-1][0] + 1, talker, 1, 3, status))

    def executeQuery(self, statement, args):
        if "MAX(msgId)" in statement:
            return FakeRows([{"value": self.messages[-1][0] if self.messages else ""}])
        talker = re.search(r"talker='([^']*)'", statement).group(1)
        after = int(re.search(r"msgId>(\d+)", statement).group(1))
        for message_id, row_talker, is_send, message_type, status in self.messages:
            if (row_talker, is_send, message_type) == (talker, 1, 3) and message_id > after:
                return FakeRows([{"value": status}])
        return FakeRows()

    def getGroupMemberDisplayName(self, group, wxid):
        self.reader_threads.append(threading.current_thread())
        return {(A, "wxid_nick"): "Group Nick"}.get((group, wxid), "")

    def getDisplayName(self, wxid):
        self.reader_threads.append(threading.current_thread())
        return {"wxid_plain": "Plain Name", A: "Group A"}.get(wxid, "")


class FakeLog:
    def __init__(self):
        self.lines = []

    def info(self, message):
        self.lines.append(message)

    warning = error = info

    def exception(self, message, error=None):
        self.lines.append(f"{message}: {error}")


class FakeContext:
    def __init__(self, root):
        self.root = root
        self.log = FakeLog()
        self.cleanup = []

    def defer(self, action):
        self.cleanup.append(action)
        return action

    def track_reference(self, reference):
        return reference

    def close(self):
        for action in reversed(self.cleanup):
            action()


def wait_until(condition):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return False


def message(talker=A, sender="wxid_nick", text="hello", **overrides):
    values = {
        "talker": talker,
        "isSend": 0,
        "type": 1,
        "content": f"{sender}:\n{text}",
        "msgSvrId": None,
        "createTime": int(time.time() * 1000),
    }
    return FakeValues(**{**values, **overrides})


def image_message(talker=A, sender="wxid_nick", server_id=500):
    xml = '<?xml version="1.0"?>\n<msg><img length="1"/></msg>'
    return message(talker=talker, type=3, msgSvrId=server_id, content=f"{sender}:\n{xml}")


class PluginMainTest(unittest.TestCase):
    def setUp(self):
        self.sent = []
        self.contexts = []
        self.listeners = []
        self.database = FakeDatabase()

        core_api = types.ModuleType("dev.ujhhgtg.wekit.features.api.core")
        core_api.WeDatabaseApi = SimpleNamespace(INSTANCE=self.database)
        core_api.WeDatabaseListenerApi = SimpleNamespace(
            IInsertListener=object,
            INSTANCE=SimpleNamespace(
                addListener=self.listeners.append,
                removeListener=self.listeners.remove,
            ),
        )
        core_api.WeMessageApi = SimpleNamespace(
            INSTANCE=SimpleNamespace(
                sendText=self.send_text,
                sendImage=self.send_image_file,
                downloadImage=self.download_image,
            )
        )
        java = types.ModuleType("java")
        java.dynamic_proxy = lambda interface: interface

        modules = {"java": java, core_api.__name__: core_api}
        parts = core_api.__name__.split(".")
        for end in range(1, len(parts)):
            modules[".".join(parts[:end])] = types.ModuleType(".".join(parts[:end]))

        package = types.ModuleType(NAMESPACE)
        package.__package__ = NAMESPACE
        package.__path__ = [str(PLUGIN_ROOT)]
        package.__spec__ = importlib.machinery.ModuleSpec(NAMESPACE, None, is_package=True)
        modules[NAMESPACE] = package

        patcher = mock.patch.dict(sys.modules, modules)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.main = importlib.import_module(".main", NAMESPACE)
        self.main.MIN_SEND_PAUSE_SECONDS = 0
        self.main.MAX_SEND_PAUSE_SECONDS = 0
        # Long enough for a test's messages to arrive before the first send.
        self.main.COMBINE_WAIT_SECONDS = 0.3
        self.main.IMAGE_SEND_CHECK_SECONDS = 1
        self.main.DELETE_IMAGES_AFTER_SECONDS = 0.05

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.plugin_root = Path(directory.name) / "plugin"
        self.plugin_root.mkdir()
        self.saved_file = Path(directory.name) / "Download" / "WeKit" / "photo.jpg"
        self.saved_file.parent.mkdir(parents=True)
        self.saved_file.write_bytes(b"image bytes")
        self.download_result = str(self.saved_file)
        self.events = []
        # WeChat's status for the uploaded image: 2 sent, 5 failed, None for no message.
        self.upload_status = 2
        self.upload_accepted = True
        self.uploads = []

    def send_text(self, destination, text):
        self.events.append(f"text {text}")
        self.sent.append((destination, text))
        return True

    def send_image_file(self, destination, path):
        self.events.append(f"image {path}")
        self.uploads.append((destination, path))
        self.sent.append((destination, f"<image {Path(path).name}>"))
        if self.upload_accepted and self.upload_status is not None:
            self.database.add_sent_image(destination, self.upload_status)
        return self.upload_accepted

    def download_image(self, server_id):
        self.events.append(f"download {server_id}")
        return self.download_result

    def sent_log(self, context, outcome, server_id=500, destination=B):
        line = f"Image {server_id} to {destination}: {outcome}"
        return wait_until(lambda: line in context.log.lines)

    def test_image_is_downloaded_then_credited_then_uploaded(self):
        context, listener = self.start()
        listener.onInsert("message", message(text="look"))
        listener.onInsert("message", image_message(server_id=500))

        self.assertTrue(self.sent_log(context, "sent"))
        self.assertEqual(
            self.events,
            [
                "download 500",
                "text Group Nick: look\nGroup Nick: [图片] ↓",
                f"image {self.saved_file}",
            ],
        )
        self.assertEqual(self.uploads, [(B, str(self.saved_file))])

    def test_saved_file_is_deleted_after_a_confirmed_send(self):
        context, listener = self.start()
        listener.onInsert("message", image_message(server_id=500))

        self.assertTrue(self.sent_log(context, "sent"))
        self.assertTrue(wait_until(lambda: not self.saved_file.exists()))
        self.assertIn("Deleted forwarded image file photo.jpg", context.log.lines)

    def test_image_that_never_appears_is_logged_and_its_file_kept(self):
        self.upload_status = None
        context, listener = self.start()
        listener.onInsert("message", image_message(server_id=500))

        self.assertTrue(self.sent_log(context, "missing"))
        expected = f"Could not forward image 500 to {B}; kept {self.saved_file}"
        self.assertTrue(wait_until(lambda: expected in context.log.lines))
        time.sleep(0.2)
        self.assertTrue(self.saved_file.exists())

    def test_failed_image_is_logged_and_its_file_kept(self):
        self.upload_status = 5
        context, listener = self.start()
        listener.onInsert("message", image_message(server_id=500))

        self.assertTrue(self.sent_log(context, "failed"))
        time.sleep(0.2)
        self.assertTrue(self.saved_file.exists())

    def test_image_wekit_refuses_is_logged(self):
        self.upload_accepted = False
        context, listener = self.start()
        listener.onInsert("message", image_message(server_id=500))

        self.assertTrue(self.sent_log(context, "rejected by WeKit"))
        self.assertTrue(self.saved_file.exists())

    def test_unfinished_send_is_logged_without_an_error_and_its_file_kept(self):
        self.upload_status = 1
        context, listener = self.start()
        listener.onInsert("message", image_message(server_id=500))

        self.assertTrue(self.sent_log(context, "pending (status 1)"))
        time.sleep(0.2)
        self.assertTrue(self.saved_file.exists())
        self.assertFalse(any("Could not forward image" in line for line in context.log.lines))

    def test_failed_download_skips_the_image_and_its_credit(self):
        self.download_result = None
        context, listener = self.start()
        listener.onInsert("message", message(text="look"))
        listener.onInsert("message", image_message(server_id=500))

        expected = "Could not download image 500; skipping it"
        self.assertTrue(wait_until(lambda: expected in context.log.lines))
        self.assertTrue(wait_until(lambda: self.sent))
        time.sleep(0.1)
        self.assertEqual(self.sent, [(B, "Group Nick: look")])
        self.assertEqual(self.uploads, [])

    def test_deleting_can_be_turned_off(self):
        self.main.DELETE_FORWARDED_IMAGES = False
        context, listener = self.start()
        listener.onInsert("message", image_message(server_id=500))

        self.assertTrue(self.sent_log(context, "sent"))
        time.sleep(0.2)
        self.assertTrue(self.saved_file.exists())

    def test_disabling_cancels_pending_deletions(self):
        self.main.DELETE_IMAGES_AFTER_SECONDS = 0.5
        context, listener = self.start()
        listener.onInsert("message", image_message(server_id=500))

        self.assertTrue(self.sent_log(context, "sent"))
        context.close()
        context.cleanup.clear()
        time.sleep(0.8)
        self.assertTrue(self.saved_file.exists())

    def write_blocked_words(self, text):
        (self.plugin_root / "blocked_words.txt").write_text(text, encoding="utf-8")

    def test_without_a_blocked_words_file_nothing_is_blocked(self):
        context, listener = self.start()
        listener.onInsert("message", message(text="免费广告"))

        self.assertTrue(wait_until(lambda: self.sent))
        self.assertEqual(self.sent, [(B, "Group Nick: 免费广告")])
        self.assertIn("Blocked words: off (blocked_words.txt not found)", context.log.lines)

    def test_blocked_words_file_is_loaded_and_counted(self):
        self.write_blocked_words("广告\n\n  spam  \n广告\n")
        context, _ = self.start()
        self.assertIn("Blocked words: on (2 loaded)", context.log.lines)

    def test_unreadable_blocked_words_file_stops_the_plugin(self):
        (self.plugin_root / "blocked_words.txt").write_bytes(b"\xff\xfe\xfa not utf-8")
        with self.assertRaises(UnicodeDecodeError):
            self.start()
        self.assertEqual(self.listeners, [])

    def test_blocked_words_file_is_applied_and_logged(self):
        self.write_blocked_words("广告\n")
        context, listener = self.start()
        listener.onInsert("message", message(text="免费广告"))
        listener.onInsert("message", message(text="你好"))

        self.assertTrue(wait_until(lambda: self.sent))
        time.sleep(0.05)
        self.assertEqual(self.sent, [(B, "Group Nick: 你好")])
        self.assertIn(f"Dropped a message from {A}: blocked", context.log.lines)
        # The log never contains the blocked message itself.
        self.assertFalse(any("免费广告" in line for line in context.log.lines))

    def test_images_are_ignored_when_turned_off(self):
        self.main.FORWARD_IMAGES = False
        _, listener = self.start()
        listener.onInsert("message", image_message(server_id=500))
        listener.onInsert("message", message(text="text still works"))

        self.assertTrue(wait_until(lambda: self.sent))
        time.sleep(0.05)
        self.assertEqual(self.sent, [(B, "Group Nick: text still works")])
        self.assertNotIn("download 500", self.events)

    def test_image_limit_constant_is_applied_and_logged(self):
        self.main.MAX_IMAGES_PER_USER_PER_MINUTE = 1
        context, listener = self.start()
        listener.onInsert("message", image_message(server_id=500))
        listener.onInsert("message", image_message(server_id=501))

        self.assertTrue(wait_until(lambda: self.uploads))
        time.sleep(0.05)
        self.assertEqual(len(self.uploads), 1)
        self.assertTrue(any("image_rate_limited" in line for line in context.log.lines))

    def reload_main_with_local_config(self, local_config):
        sys.modules.pop(f"{NAMESPACE}.main")
        sys.modules[f"{NAMESPACE}.local_config"] = local_config
        return importlib.import_module(".main", NAMESPACE)

    def test_group_ids_come_from_local_config(self):
        local_config = types.ModuleType(f"{NAMESPACE}.local_config")
        local_config.GROUP_A = A
        local_config.GROUP_B = B
        main = self.reload_main_with_local_config(local_config)
        self.assertEqual((main.GROUP_A, main.GROUP_B), (A, B))

    def test_missing_local_config_leaves_forwarding_off(self):
        # A None entry in sys.modules makes the import raise ModuleNotFoundError.
        main = self.reload_main_with_local_config(None)
        self.assertEqual((main.GROUP_A, main.GROUP_B), ("", ""))

    def test_local_config_missing_a_group_is_an_error(self):
        local_config = types.ModuleType(f"{NAMESPACE}.local_config")
        local_config.GROUP_A = A
        with self.assertRaises(ImportError):
            self.reload_main_with_local_config(local_config)

    def start(self, group_a=A, group_b=B):
        self.main.GROUP_A = group_a
        self.main.GROUP_B = group_b
        context = FakeContext(self.plugin_root)
        self.main.setup(context)
        self.addCleanup(context.close)
        self.contexts.append(context)
        return context, self.listeners[0]

    def test_forwards_both_ways_with_group_nickname_then_regular_name(self):
        _, listener = self.start()
        listener.onInsert("message", message(talker=A, sender="wxid_nick", text="one"))
        listener.onInsert("message", message(talker=B, sender="wxid_plain", text="two"))
        listener.onInsert("message", message(talker=B, sender="wxid_unknown", text="three"))

        self.assertTrue(wait_until(lambda: len(self.sent) == 2))
        self.assertEqual(
            self.sent,
            [
                (B, "Group Nick: one"),
                (A, "Plain Name: two\nwxid_unknown: three"),
            ],
        )
        self.assertNotIn(threading.current_thread(), self.database.reader_threads)

    def test_per_user_limit_constant_is_applied_and_logged(self):
        self.main.PER_USER_MAX_MESSAGES_PER_MINUTE = 2
        context, listener = self.start()
        for number in range(4):
            listener.onInsert("message", message(text=str(number)))

        self.assertTrue(wait_until(lambda: self.sent))
        time.sleep(0.05)
        self.assertEqual(self.sent, [(B, "Group Nick: 0\nGroup Nick: 1")])
        drops = [line for line in context.log.lines if "user_rate_limited" in line]
        self.assertEqual(len(drops), 2)

    def test_global_limit_constant_holds_back_the_next_send(self):
        self.main.GLOBAL_MAX_MESSAGES_PER_MINUTE = 1
        _, listener = self.start()
        for sender in ("wxid_1", "wxid_2", "wxid_3"):
            listener.onInsert("message", message(sender=sender))

        self.assertTrue(wait_until(lambda: self.sent))
        self.assertEqual(self.sent, [(B, "wxid_1: hello\nwxid_2: hello\nwxid_3: hello")])

        listener.onInsert("message", message(sender="wxid_4"))
        time.sleep(0.6)
        self.assertEqual(len(self.sent), 1)

    def test_combined_character_limit_constant_splits_sends(self):
        self.main.MAX_COMBINED_CHARACTERS = len("wxid_1: hello\nwxid_2: hello")
        _, listener = self.start()
        for sender in ("wxid_1", "wxid_2", "wxid_3"):
            listener.onInsert("message", message(sender=sender))

        self.assertTrue(wait_until(lambda: len(self.sent) == 2))
        self.assertEqual(
            self.sent,
            [(B, "wxid_1: hello\nwxid_2: hello"), (B, "wxid_3: hello")],
        )

    def test_failed_name_lookup_falls_back_to_the_wechat_id(self):
        self.database.getGroupMemberDisplayName = mock.Mock(side_effect=RuntimeError("db"))
        _, listener = self.start()
        listener.onInsert("message", message(sender="wxid_nick"))

        self.assertTrue(wait_until(lambda: self.sent))
        self.assertEqual(self.sent, [(B, "wxid_nick: hello")])

    def test_failed_send_is_logged(self):
        _, listener = self.start()
        self.main.WeMessageApi.INSTANCE.sendText = lambda destination, text: False
        context = self.contexts[-1]
        listener.onInsert("message", message())

        self.assertTrue(wait_until(lambda: f"Could not send to {B}" in context.log.lines))

    def test_without_groups_it_logs_group_ids_and_sends_nothing(self):
        context, listener = self.start(group_a="", group_b="")
        listener.onInsert("message", message(talker=A))
        listener.onInsert("message", message(talker=A))
        listener.onInsert("message", message(talker="wxid_private_chat"))

        expected = f"Found group: {A} (Group A)"
        self.assertTrue(wait_until(lambda: expected in context.log.lines))
        time.sleep(0.05)
        self.assertEqual(context.log.lines.count(expected), 1)
        self.assertFalse(any("wxid_private_chat" in line for line in context.log.lines))
        self.assertEqual(self.sent, [])

    def test_other_tables_are_ignored(self):
        values = mock.Mock()
        _, listener = self.start()
        listener.onInsert("rconversation", values)
        values.getAsString.assert_not_called()

    def test_disabling_removes_listener_and_stops_sender(self):
        context, _ = self.start()
        before = [t for t in threading.enumerate() if t.name == "GroupRelaySender"]
        self.assertTrue(before)
        context.close()
        context.cleanup.clear()
        self.assertEqual(self.listeners, [])
        self.assertTrue(wait_until(lambda: not any(t.is_alive() for t in before)))


if __name__ == "__main__":
    unittest.main()
