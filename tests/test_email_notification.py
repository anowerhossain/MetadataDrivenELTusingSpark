"""
Unit Tests for EmailNotification Helper Module and EmailNotificationSection TOML Parsing.
"""

import os
import unittest
from unittest.mock import patch, MagicMock
import smtplib

from src.core.config import EmailNotificationSection, ConfigParser, JobConfig
from src.helpers.email_notification import EmailNotification


class TestEmailNotification(unittest.TestCase):

    def test_disabled_email_notification_skips_sending(self):
        """Verify that when enabled=False, send_error returns False without opening SMTP connections."""
        sec = EmailNotificationSection(enabled=False, to=["admin@company.com"])
        err = RuntimeError("Pipeline failure")

        with patch("smtplib.SMTP") as mock_smtp:
            result = EmailNotification.send_error(
                job_id="test_job",
                error=err,
                config=sec,
                run_id="run_12345",
                config_path="config/test.toml"
            )
            self.assertFalse(result)
            mock_smtp.assert_not_called()

    def test_enabled_email_notification_sends_email(self):
        """Verify that when enabled=True, send_error sends email to to, cc, and bcc recipients."""
        sec = EmailNotificationSection(
            enabled=True,
            sender="noreply@company.com",
            to=["user@company.com"],
            cc=["team@company.com"],
            bcc=["audit@company.com"],
            subject_prefix="[CUSTOM ERROR]"
        )
        err = ValueError("Data quality check failed")

        mock_server = MagicMock()
        with patch("smtplib.SMTP", return_value=mock_server) as mock_smtp:
            result = EmailNotification.send_error(
                job_id="customer_etl",
                error=err,
                config=sec,
                run_id="run_99999",
                config_path="config/jobs/customer.toml"
            )

            self.assertTrue(result)
            mock_smtp.assert_called_once_with("localhost", 25, timeout=15)
            mock_server.sendmail.assert_called_once()

            args, _ = mock_server.sendmail.call_args
            from_addr, recipients, msg_str = args
            self.assertEqual(from_addr, "noreply@company.com")
            self.assertIn("user@company.com", recipients)
            self.assertIn("team@company.com", recipients)
            self.assertIn("audit@company.com", recipients)
            self.assertIn("Subject: [CUSTOM ERROR] Job 'customer_etl' Status: FAILED", msg_str)
            self.assertIn("From: noreply@company.com", msg_str)
            self.assertIn("To: user@company.com", msg_str)
            self.assertIn("Cc: team@company.com", msg_str)
            
            # Decode MIME message body parts
            import email
            parsed_msg = email.message_from_string(msg_str)
            payload = ""
            for part in parsed_msg.walk():
                if part.get_content_type() in ("text/plain", "text/html"):
                    payload += part.get_payload(decode=True).decode("utf-8")

            self.assertIn("ValueError", payload)
            self.assertIn("Data quality check failed", payload)
            mock_server.quit.assert_called_once()

    def test_missing_to_recipients_skips_sending(self):
        """Verify that enabled=True with empty 'to' list logs warning and skips sending."""
        sec = EmailNotificationSection(enabled=True, to=[])
        err = RuntimeError("Source system unreachable")

        with patch("smtplib.SMTP") as mock_smtp:
            result = EmailNotification.send_error(
                job_id="test_job",
                error=err,
                config=sec
            )
            self.assertFalse(result)
            mock_smtp.assert_not_called()

    def test_smtp_failure_is_caught_and_logged_without_raising(self):
        """Verify that SMTP connection failure is caught, logged, and returns False without raising exception."""
        sec = EmailNotificationSection(enabled=True, to=["user@company.com"])
        err = RuntimeError("Database timeout")

        with patch("smtplib.SMTP", side_effect=smtplib.SMTPException("SMTP Server Connection Refused")):
            result = EmailNotification.send_error(
                job_id="test_job",
                error=err,
                config=sec
            )
            self.assertFalse(result)

    def test_email_notification_section_parsing(self):
        """Verify TOML section parsing into EmailNotificationSection."""
        data = {
            "enabled": True,
            "from": "alert@bank.com",
            "to": ["ops1@bank.com", "ops2@bank.com"],
            "cc": "manager@bank.com",
            "bcc": [],
            "subject_prefix": "[CRITICAL FAILURE]"
        }
        sec = EmailNotificationSection.from_dict(data)
        self.assertTrue(sec.enabled)
        self.assertEqual(sec.sender, "alert@bank.com")
        self.assertEqual(sec.to, ["ops1@bank.com", "ops2@bank.com"])
        self.assertEqual(sec.cc, ["manager@bank.com"])
        self.assertEqual(sec.bcc, [])
        self.assertEqual(sec.subject_prefix, "[CRITICAL FAILURE]")

    def test_multi_event_routing_success_and_failure(self):
        """Verify routing separate email recipient lists for on_success vs on_failure status."""
        data = {
            "enabled": True,
            "from": "noreply@company.com",
            "to": ["devops@company.com"],
            "subject_prefix": "[FAIL]",
            "events": [
                {
                    "event": "on_success",
                    "enabled": True,
                    "to": ["business-analyst@company.com"],
                    "template": "job_success",
                    "subject_prefix": "[SUCCESS]"
                },
                {
                    "event": "on_quality_failure",
                    "enabled": True,
                    "to": ["data-qa@company.com"],
                    "template": "data_quality_failed",
                    "subject_prefix": "[QUALITY WARN]"
                }
            ]
        }
        sec = EmailNotificationSection.from_dict(data)

        mock_server = MagicMock()
        with patch("smtplib.SMTP", return_value=mock_server) as mock_smtp:
            # Send SUCCESS notification
            res_succ = EmailNotification.send_notification(
                job_id="orders_etl",
                status="SUCCESS",
                config=sec,
                job_name="Orders Processing"
            )
            self.assertTrue(res_succ)
            args, _ = mock_server.sendmail.call_args
            _, rec_succ, msg_succ = args
            self.assertIn("business-analyst@company.com", rec_succ)
            self.assertIn("Subject: [SUCCESS] Job 'Orders Processing' Status: SUCCESS", msg_succ)

            # Send FAILED notification (falls back to top-level default event)
            mock_server.reset_mock()
            res_fail = EmailNotification.send_notification(
                job_id="orders_etl",
                status="FAILED",
                error=RuntimeError("Connection Timeout"),
                config=sec,
                job_name="Orders Processing"
            )
            self.assertTrue(res_fail)
            args_f, _ = mock_server.sendmail.call_args
            _, rec_fail, msg_fail = args_f
            self.assertIn("devops@company.com", rec_fail)
            self.assertIn("Subject: [FAIL] Job 'Orders Processing' Status: FAILED", msg_fail)

            # Send DATA_QUALITY_FAILED notification
            mock_server.reset_mock()
            res_dq = EmailNotification.send_notification(
                job_id="orders_etl",
                status="DATA_QUALITY_FAILED",
                config=sec,
                job_name="Orders Processing"
            )
            self.assertTrue(res_dq)
            args_dq, _ = mock_server.sendmail.call_args
            _, rec_dq, msg_dq = args_dq
            self.assertIn("data-qa@company.com", rec_dq)
            self.assertIn("Subject: [QUALITY WARN] Job 'Orders Processing' Status: DATA_QUALITY_FAILED", msg_dq)

    def test_record_email_audit_telemetry(self):
        """Verify record_email_audit creates structured audit telemetry dictionary."""
        audit_entry = EmailNotification.record_email_audit(
            notification_id="notif_test123",
            job_id="customer_load",
            job_name="Customer Load",
            run_id="run_7777",
            event_type="on_failure",
            pipeline_status="FAILED",
            sender="noreply@company.com",
            recipients_to=["ops@company.com"],
            recipients_cc=["lead@company.com"],
            subject="[FAIL] Job Customer Load Failed",
            template_used="job_failed",
            email_status="SENT",
            error_message=None
        )

        self.assertEqual(audit_entry["notification_id"], "notif_test123")
        self.assertEqual(audit_entry["job_id"], "customer_load")
        self.assertEqual(audit_entry["email_status"], "SENT")
        self.assertEqual(audit_entry["recipients_to"], "ops@company.com")
        self.assertEqual(audit_entry["recipients_cc"], "lead@company.com")
        self.assertIn("sent_timestamp", audit_entry)


if __name__ == "__main__":
    unittest.main()
