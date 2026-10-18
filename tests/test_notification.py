"""Meaningful summary and delivery checks without a live notification service."""
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from bridge import notification  # noqa: E402


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.settings = {"notifications": {"summary_webhook": {
            "secret_scope": "ops", "url_secret_key": "url"}}}
        self.dbutils = Mock()
        self.dbutils.secrets.get.return_value = "https://example.org/notify?token=secret"

    def test_summary_includes_optional_failure_even_when_job_is_green(self):
        plan = [{"mapping_id": "a", "business_date": "2026-09-26"},
                {"mapping_id": "b", "business_date": "2026-09-26"}]
        summary = {"job_run_id": "run1", "release_id": "night1", "ok": True,
                   "counts": {"PUBLISHED": 1, "FAILED_EXPORT": 1},
                   "mandatory_failures": [],
                   "other_failures": [{"mapping_id": "b", "status": "FAILED_EXPORT"}]}
        payload = notification.summary_payload(plan, summary, 0, "11:00", "Australia/Sydney")
        self.assertEqual((payload["successful_tables"], payload["failed_tables"]), (1, 1))
        self.assertEqual(payload["failures"], [{"mapping_id": "b", "status": "FAILED_EXPORT"}])
        self.assertEqual(payload["result"], "NEEDS_ATTENTION")
        self.assertEqual(payload["notification_id"], "run1:0")

        response = Mock(status=202)
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        opener = Mock(return_value=response)
        notification.send_summary(self.dbutils, self.settings, payload, opener=opener)
        request = opener.call_args.args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertIn(b'"failed_tables":1', request.data)
        self.assertEqual(request.get_header("Idempotency-key"), "run1:0")

    def test_delivery_failure_does_not_expose_secret_url(self):
        def failing(request, timeout):
            raise OSError(request.full_url)
        with self.assertRaises(RuntimeError) as error:
            notification.send_summary(self.dbutils, self.settings, {"notification_id": "r:0"}, opener=failing)
        self.assertNotIn("token=secret", str(error.exception))

    def test_insecure_endpoint_is_rejected(self):
        self.dbutils.secrets.get.return_value = "http://example.org/notify"
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            notification.send_summary(self.dbutils, self.settings, {"notification_id": "r:0"})


if __name__ == "__main__":
    unittest.main()
