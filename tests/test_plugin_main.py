"""Runs plugin/main.py against fake WeKit APIs, loaded the way WeKit's bootstrap loads it."""

import importlib
import importlib.machinery
import sys
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


class FakeDatabase:
    def __init__(self):
        self.reader_threads = []

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
    def __init__(self):
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


class PluginMainTest(unittest.TestCase):
    def setUp(self):
        self.sent = []
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
            INSTANCE=SimpleNamespace(sendText=self.send_text)
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

    def send_text(self, destination, text):
        self.sent.append((destination, text))
        return True

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
        context = FakeContext()
        self.main.setup(context)
        self.addCleanup(context.close)
        return context, self.listeners[0]

    def test_forwards_both_ways_with_group_nickname_then_regular_name(self):
        _, listener = self.start()
        listener.onInsert("message", message(talker=A, sender="wxid_nick", text="one"))
        listener.onInsert("message", message(talker=B, sender="wxid_plain", text="two"))
        listener.onInsert("message", message(talker=B, sender="wxid_unknown", text="three"))

        self.assertTrue(wait_until(lambda: len(self.sent) == 3))
        self.assertEqual(
            self.sent,
            [
                (B, "Group Nick: one"),
                (A, "Plain Name: two"),
                (A, "wxid_unknown: three"),
            ],
        )
        self.assertNotIn(threading.current_thread(), self.database.reader_threads)

    def test_per_user_limit_constant_is_applied_and_logged(self):
        self.main.PER_USER_MAX_MESSAGES_PER_MINUTE = 2
        context, listener = self.start()
        for number in range(4):
            listener.onInsert("message", message(text=str(number)))

        self.assertTrue(wait_until(lambda: len(self.sent) == 2))
        time.sleep(0.05)
        self.assertEqual(self.sent, [(B, "Group Nick: 0"), (B, "Group Nick: 1")])
        drops = [line for line in context.log.lines if "user_rate_limited" in line]
        self.assertEqual(len(drops), 2)

    def test_global_limit_constant_holds_back_extra_messages(self):
        self.main.GLOBAL_MAX_MESSAGES_PER_MINUTE = 2
        _, listener = self.start()
        for sender in ("wxid_1", "wxid_2", "wxid_3"):
            listener.onInsert("message", message(sender=sender))

        self.assertTrue(wait_until(lambda: len(self.sent) == 2))
        time.sleep(0.05)
        self.assertEqual(len(self.sent), 2)

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
