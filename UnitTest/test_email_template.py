"""
Unit Tests for EmailTemplateManager and EmailNotification Template Engine.
"""

import email
import smtplib
import unittest
from unittest.mock import patch, MagicMock

try:
    import pandas as pd
except ImportError:
    pd = None

from src.core.config import EmailNotificationSection, JobConfig
from src.helpers.email_template import EmailTemplateManager
from src.helpers.email_notification import EmailNotification


class TestEmailTemplateManager(unittest.TestCase):

    def test_render_html_table_pandas_df(self):
        """Verify converting Pandas DataFrame to HTML table markup."""
        if pd is None:
            self.skipTest("Pandas not installed in test environment")

        df = pd.DataFrame({
            "ORDER_ID": [101, 102],
            "AMOUNT": [250.50, 499.00],
            "STATUS": ["COMPLETED", "PENDING"]
        })
        table_html = EmailTemplateManager.render_html_table(df)
        self.assertIn("<table", table_html)
        self.assertIn("ORDER_ID", table_html)
        self.assertIn("250.5", table_html)
        self.assertIn("COMPLETED", table_html)

    def test_render_html_table_sql_results(self):
        """Verify converting SQL query list of dicts to HTML table markup."""
        sql_rows = [
            {"INVOICE_ID": "INV-001", "TOTAL": 1500.00, "CUSTOMER": "BRAC Bank"},
            {"INVOICE_ID": "INV-002", "TOTAL": 2300.50, "CUSTOMER": "edw_bronze"}
        ]
        table_html = EmailTemplateManager.render_html_table(sql_rows)
        self.assertIn("<table", table_html)
        self.assertIn("INVOICE_ID", table_html)
        self.assertIn("INV-001", table_html)
        self.assertIn("BRAC Bank", table_html)

    def test_render_html_table_dict(self):
        """Verify converting key-value dict to HTML table markup."""
        data_dict = {"Target Table": "edw_bronze.customer", "Rows Loaded": 5000}
        table_html = EmailTemplateManager.render_html_table(data_dict)
        self.assertIn("Target Table", table_html)
        self.assertIn("edw_bronze.customer", table_html)

    def test_render_builtin_templates(self):
        """Verify rendering all 6 built-in preset HTML templates."""
        presets = ["job_failed", "job_success", "data_quality_failed", "sla_breached", "missing_file", "data_anomaly"]
        context = {
            "job_id": "cust_job",
            "job_name": "Customer ETL",
            "status": "FAILED",
            "error_message": "Network timeout connecting to database",
            "file_path": "/sftp/invoices/",
            "file_pattern": "*.csv",
            "quality_rule": "null_check",
            "expected_sla_time": "08:00 AM"
        }

        for preset in presets:
            rendered = EmailTemplateManager.render_template(preset, context)
            self.assertIn("<!DOCTYPE html>", rendered)
            self.assertIn("Customer ETL", rendered)

    def test_safe_placeholder_replacement(self):
        """Verify safe dict handling of missing or extra template placeholders."""
        raw_tmpl = "Job '{job_id}' status is {status}. Extra key: {missing_key}"
        context = {"job_id": "job_99", "status": "SUCCESS"}

        rendered = EmailTemplateManager.render_template(raw_tmpl, context)
        self.assertIn("Job 'job_99' status is SUCCESS.", rendered)
        self.assertIn("{missing_key}", rendered)

    def test_custom_subject_pattern(self):
        """Verify custom subject pattern rendering."""
        pattern = "{subject_prefix} '{job_name}' Status: {status}"
        context = {
            "subject_prefix": "[CRITICAL ALERT]",
            "job_name": "Invoices Pipeline",
            "status": "DATA_QUALITY_FAILED"
        }
        subject = EmailTemplateManager.render_subject(pattern, context)
        self.assertEqual(subject, "[CRITICAL ALERT] 'Invoices Pipeline' Status: DATA_QUALITY_FAILED")

    def test_send_notification_integration_with_html_table(self):
        """Integration test for EmailNotification.send_notification with HTML DataFrame table via mock SMTP."""
        sec = EmailNotificationSection(
            enabled=True,
            template="job_failed",
            subject="{subject_prefix} {job_name} ({status})",
            sender="alert@bank.com",
            to=["ops@bank.com"]
        )

        sql_rows = [{"RULE": "null_check", "COLUMN": "CUSTOMER_ID", "VIOLATIONS": 5}]

        mock_server = MagicMock()
        with patch("smtplib.SMTP", return_value=mock_server) as mock_smtp:
            res = EmailNotification.send_notification(
                job_id="customer_etl",
                status="DATA_QUALITY_FAILED",
                error=ValueError("Null customer IDs detected"),
                config=sec,
                sql_results=sql_rows,
                job_name="Customer Ingestion"
            )

            self.assertTrue(res)
            mock_smtp.assert_called_once()
            mock_server.sendmail.assert_called_once()

            args, _ = mock_server.sendmail.call_args
            sender, recipients, msg_str = args
            self.assertEqual(sender, "alert@bank.com")
            self.assertIn("ops@bank.com", recipients)
            self.assertIn("Subject: [ETL JOB FAILURE] Customer Ingestion (DATA_QUALITY_FAILED)", msg_str)

            parsed = email.message_from_string(msg_str)
            self.assertTrue(parsed.is_multipart())

            # Find HTML part
            html_payload = ""
            for part in parsed.walk():
                if part.get_content_type() == "text/html":
                    html_payload = part.get_payload(decode=True).decode("utf-8")
                    break

            self.assertIn("Customer Ingestion", html_payload)
            self.assertIn("CUSTOMER_ID", html_payload)
            self.assertIn("null_check", html_payload)


if __name__ == "__main__":
    unittest.main()
