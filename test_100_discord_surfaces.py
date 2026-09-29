from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from discord_remote import DiscordConfig, DiscordControlRemote, ThreadBindingStore, _DiscordOutboundSender


CONTROL, QA, ALERTS, REPORTS = 1551458191261302815, 1552345410553520339, 1552345465654218812, 1552345550806982756
GUILD, USER = 1551458190401736798, 123456789012345678


class StubTransport:
    def __init__(self):
        self.sent = []

    def send_message(self, channel_id, content):
        self.sent.append((channel_id, content))
        return {"delivered": True, "message_id": str(len(self.sent))}


class StubControl:
    def __init__(self):
        self.calls = 0

    def get_queue(self):
        self.calls += 1
        return {"queue": {"progress": {"schema_version": 1, "progress_revision": 1,
                                       "job": {"id": "", "status": "IDLE"}}}}


class DiscordFourSurfaceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = DiscordConfig("fixture", {USER}, GUILD, CONTROL, True, self.root / "none",
                                    qa_channel_id=QA, alerts_channel_id=ALERTS,
                                    reports_channel_id=REPORTS)
        self.transport = StubTransport()
        self.control = StubControl()
        self.remote = DiscordControlRemote(self.control, self.config, transport=self.transport,
                                           bindings=ThreadBindingStore(self.root / "bindings.json"))

    def message(self, channel, content, *, user=USER, guild=GUILD):
        return {"id": "100", "channel_id": str(channel), "guild_id": str(guild),
                "author": {"id": str(user), "bot": False}, "content": content}

    def test_alerts_and_reports_cannot_issue_state_commands(self):
        for channel in (ALERTS, REPORTS):
            for command in ("continue", "approve", "reject", "bind"):
                with self.subTest(channel=channel, command=command):
                    result = self.remote.handle(self.message(channel, "!h " + command + " JOB-X"))
                    self.assertEqual("WRONG_LOCATION", result["outcome"])
        self.assertEqual(0, self.control.calls)

    def test_foreign_user_and_guild_are_rejected(self):
        for message in (self.message(CONTROL, "!h status", user=USER + 1),
                        self.message(CONTROL, "!h status", guild=GUILD + 1)):
            self.assertEqual("UNAUTHORIZED", self.remote.handle(message)["outcome"])
        self.assertEqual(0, self.control.calls)

    def test_outbound_alert_and_report_use_distinct_channels(self):
        transport = StubTransport()
        with patch("discord_remote.DiscordConfig.load", return_value=self.config), \
             patch("discord_remote.DiscordTransport", return_value=transport):
            sender = _DiscordOutboundSender()
            self.assertTrue(sender.send("failure", surface="alerts")["delivered"])
            self.assertTrue(sender.send("success", surface="reports")["delivered"])
        self.assertEqual([ALERTS, REPORTS], [channel for channel, _ in transport.sent])


if __name__ == "__main__":
    unittest.main()
