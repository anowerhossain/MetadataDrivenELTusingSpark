"""
Reusable Email Notification Helper Module for Job Alerts & Failures.
Supports HTML template rendering via EmailTemplateManager, dynamic DataFrames/SQL results tables,
TOML [email_notification] configuration, recipient lists (to, cc, bcc), secure SMTP environment credentials,
and fail-safe exception handling.
"""

import os
import smtplib
import traceback
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any
from src.helpers.logger import setup_logger
from src.core.config import EmailNotificationSection
from src.helpers.email_template import EmailTemplateManager

logger = setup_logger("EmailNotification")


class EmailNotification:
    """Manages email notifications for ETL pipeline alerts and execution status."""

    @classmethod
    def send_notification(
        cls,
        job_id: str,
        status: str = "FAILED",
        error: Optional[Exception] = None,
        config: Optional[EmailNotificationSection] = None,
        data_context: Optional[Dict[str, Any]] = None,
        df: Any = None,
        sql_results: Any = None,
        run_id: Optional[str] = None,
        config_path: Optional[str] = None,
        job_name: Optional[str] = None,
    ) -> bool:
        """
        Sends an automated notification email using EmailTemplateManager HTML template presets or custom patterns.

        :param job_id: Unique identifier for the job.
        :param status: Pipeline status string (e.g. 'FAILED', 'SUCCESS', 'DATA_QUALITY_FAILED', 'SLA_BREACHED', 'MISSING_FILE', 'DATA_ANOMALY').
        :param error: Optional Exception instance thrown by the pipeline.
        :param config: EmailNotificationSection configuration instance.
        :param data_context: Optional key-value dictionary context data.
        :param df: Optional Pandas or PySpark DataFrame to render as HTML table.
        :param sql_results: Optional SQL query results (list of dicts) to render as HTML table.
        :param run_id: Optional unique run execution ID.
        :param config_path: Optional path to job configuration file.
        :param job_name: Optional descriptive job name.
        :return: True if email was successfully sent, False otherwise.
        """
        # 1. Resolve event-specific configuration for status (e.g., 'FAILED' -> 'on_failure', 'SUCCESS' -> 'on_success')
        if not config or not config.enabled:
            logger.info(f"Email notifications disabled for job '{job_id}'. Skipping notification.")
            return False

        event_cfg = config.get_event_config(status)
        if not event_cfg or not event_cfg.enabled:
            logger.info(f"Email notifications for event status '{status}' disabled for job '{job_id}'. Skipping.")
            return False

        # 2. Check if primary recipients (to) are specified
        if not event_cfg.to:
            logger.warning(
                f"Email notification enabled for job '{job_id}' event '{status}', but no primary recipients ('to') "
                f"were configured. Skipping notification."
            )
            return False

        # 3. Build HTML Table from DataFrame or SQL results
        table_html = ""
        if df is not None:
            table_html = EmailTemplateManager.render_html_table(df)
        elif sql_results is not None:
            table_html = EmailTemplateManager.render_html_table(sql_results)
        elif data_context and ("table" in data_context or "data" in data_context):
            table_html = EmailTemplateManager.render_html_table(data_context.get("table") or data_context.get("data"))

        # 4. Assemble Context Dictionary
        now_str = datetime.now(timezone.utc).isoformat()
        resolved_job_name = job_name or (data_context.get("job_name") if data_context else None) or job_id

        error_type = type(error).__name__ if error else (data_context.get("error_type", "None") if data_context else "None")
        error_msg = str(error) if error else (data_context.get("error_message", "None") if data_context else "None")
        tb_text = "".join(traceback.format_exception(type(error), error, error.__traceback__)) if error else (data_context.get("traceback", "") if data_context else "")

        context: Dict[str, Any] = {
            "job_id": job_id,
            "job_name": resolved_job_name,
            "status": status,
            "subject_prefix": event_cfg.subject_prefix or "[ETL ALERT]",
            "run_id": run_id or (data_context.get("run_id") if data_context else "N/A"),
            "config_path": config_path or (data_context.get("config_path") if data_context else "N/A"),
            "date": now_str,
            "error_type": error_type,
            "error_message": error_msg,
            "traceback": tb_text,
            "table_html": table_html,
        }

        # Merge additional custom data_context values
        if data_context:
            for k, v in data_context.items():
                if k not in context and v is not None:
                    context[k] = v

        # 5. Render Subject and Body
        subject = EmailTemplateManager.render_subject(event_cfg.subject, context)
        template_choice = event_cfg.body_template if event_cfg.body_template else event_cfg.template
        html_body = EmailTemplateManager.render_template(template_choice, context)

        plaintext_body = (
            f"Job Alert: {subject}\n"
            f"Job ID: {job_id}\n"
            f"Status: {status}\n"
            f"Timestamp: {now_str}\n"
            f"Error Details: {error_msg}\n"
        )

        # 6. Resolve SMTP Credentials securely from Environment Variables
        smtp_server = os.getenv("SMTP_SERVER", "localhost")
        try:
            smtp_port = int(os.getenv("SMTP_PORT", "25"))
        except (ValueError, TypeError):
            smtp_port = 25

        smtp_user = os.getenv("SMTP_USERNAME", "")
        smtp_password = os.getenv("SMTP_PASSWORD", "")
        use_tls = os.getenv("SMTP_USE_TLS", "false").lower() in ("true", "1", "yes")

        # 7. Construct Multipart MIME Message (HTML + Plaintext)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = event_cfg.sender
        msg["To"] = ", ".join(event_cfg.to)

        if event_cfg.cc:
            msg["Cc"] = ", ".join(event_cfg.cc)

        msg.attach(MIMEText(plaintext_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        all_recipients: List[str] = list(set(event_cfg.to + event_cfg.cc + event_cfg.bcc))

        # 8. Send Email via SMTP with Fail-Safe Exception Catching
        try:
            logger.info(f"Connecting to SMTP server '{smtp_server}:{smtp_port}' to send '{status}' alert notification...")
            server = smtplib.SMTP(smtp_server, smtp_port, timeout=15)

            if use_tls:
                server.starttls()

            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)

            server.sendmail(config.sender, all_recipients, msg.as_string())
            server.quit()

            logger.info(f"Notification email ('{subject}') successfully sent to recipients: {all_recipients}")
            return True

        except Exception as smtp_err:
            logger.error(f"Failed to send error notification email for job '{job_id}': {smtp_err}")
            return False

    @classmethod
    def send_error(
        cls,
        job_id: str,
        error: Exception,
        config: Optional[EmailNotificationSection] = None,
        run_id: Optional[str] = None,
        config_path: Optional[str] = None
    ) -> bool:
        """
        Backward-compatible error notification helper method forwarding to send_notification().
        """
        template_choice = config.template if config and config.template != "job_failed" else "job_failed"
        return cls.send_notification(
            job_id=job_id,
            status="FAILED",
            error=error,
            config=config,
            run_id=run_id,
            config_path=config_path
        )
