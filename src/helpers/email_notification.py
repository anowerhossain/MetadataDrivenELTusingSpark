"""
Reusable Email Notification Helper Module for Job Alerts & Failures.
Supports HTML template rendering via EmailTemplateManager, dynamic DataFrames/SQL results tables,
TOML [email_notification] configuration, recipient lists (to, cc, bcc), secure SMTP environment credentials,
multi-event routing (on_failure, on_success, on_quality_failure), and Iceberg audit table persistence (etl_audit.etl_email_audit).
"""

import os
import uuid
import json
import smtplib
import traceback
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any
from src.helpers.logger import setup_logger
from src.core.config import EmailNotificationSection, EmailEventSection
from src.helpers.email_template import EmailTemplateManager

logger = setup_logger("EmailNotification")


class EmailNotification:
    """Manages email notifications for ETL pipeline alerts and execution status."""

    @classmethod
    def record_email_audit(
        cls,
        notification_id: str,
        job_id: str,
        job_name: str,
        run_id: str,
        event_type: str,
        pipeline_status: str,
        sender: str,
        recipients_to: List[str],
        recipients_cc: List[str],
        subject: str,
        template_used: str,
        email_status: str,
        error_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Records structured email audit telemetry entry for persistence into etl_audit.etl_email_audit Iceberg table
        and structured JSON logging.
        """
        sent_ts = datetime.now(timezone.utc).isoformat()
        to_str = ", ".join(recipients_to) if isinstance(recipients_to, list) else str(recipients_to or "")
        cc_str = ", ".join(recipients_cc) if isinstance(recipients_cc, list) else str(recipients_cc or "")

        audit_record = {
            "notification_id": notification_id,
            "job_id": job_id,
            "job_name": job_name,
            "run_id": run_id or "N/A",
            "event_type": event_type,
            "pipeline_status": pipeline_status,
            "sender": sender,
            "recipients_to": to_str,
            "recipients_cc": cc_str,
            "subject": subject,
            "template_used": template_used,
            "email_status": email_status,
            "error_message": error_message or "None",
            "sent_timestamp": sent_ts
        }

        # Log structured JSON telemetry
        logger.info(f"[EmailAudit] Telemetry Entry [{email_status}]:\n{json.dumps(audit_record, indent=2)}")

        # Attempt PySpark Iceberg table persistence if active SparkSession is available
        try:
            from src.helpers.spark import SparkSessionFactory
            spark = SparkSessionFactory.get_active_session()
            if spark:
                df = spark.createDataFrame([audit_record])
                df.write.format("iceberg").mode("append").save("etl_audit.etl_email_audit")
                logger.info(f"[EmailAudit] Successfully committed audit record '{notification_id}' to Iceberg table 'etl_audit.etl_email_audit'")
        except Exception as spark_err:
            logger.debug(f"[EmailAudit] Iceberg audit persistence skipped (non-Spark environment): {spark_err}")

        return audit_record

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
        Sends an automated notification email using EmailTemplateManager HTML template presets or custom patterns
        and records structured audit telemetry into etl_audit.etl_email_audit.
        """
        notif_id = f"notif_{uuid.uuid4().hex[:12]}"
        resolved_job_name = job_name or (data_context.get("job_name") if data_context else None) or job_id
        resolved_run_id = run_id or (data_context.get("run_id") if data_context else "N/A")

        # 1. Check if top-level email notifications are enabled
        if not config or not config.enabled:
            logger.info(f"Email notifications disabled for job '{job_id}'. Skipping notification.")
            cls.record_email_audit(
                notification_id=notif_id,
                job_id=job_id,
                job_name=resolved_job_name,
                run_id=resolved_run_id,
                event_type="disabled",
                pipeline_status=status,
                sender="N/A",
                recipients_to=[],
                recipients_cc=[],
                subject="N/A",
                template_used="none",
                email_status="DISABLED",
                error_message="Email notifications disabled in TOML configuration"
            )
            return False

        # 2. Resolve event-specific configuration for status (e.g. 'FAILED' -> 'on_failure')
        event_cfg = config.get_event_config(status)
        if not event_cfg or not event_cfg.enabled:
            logger.info(f"Email notifications for event status '{status}' disabled for job '{job_id}'. Skipping.")
            cls.record_email_audit(
                notification_id=notif_id,
                job_id=job_id,
                job_name=resolved_job_name,
                run_id=resolved_run_id,
                event_type=status.lower(),
                pipeline_status=status,
                sender=event_cfg.sender if event_cfg else "N/A",
                recipients_to=[],
                recipients_cc=[],
                subject="N/A",
                template_used="none",
                email_status="DISABLED",
                error_message=f"Event notification '{status}' disabled in configuration"
            )
            return False

        # 3. Check if primary recipients (to) are specified
        if not event_cfg.to:
            logger.warning(
                f"Email notification enabled for job '{job_id}' event '{status}', but no primary recipients ('to') "
                f"were configured. Skipping notification."
            )
            cls.record_email_audit(
                notification_id=notif_id,
                job_id=job_id,
                job_name=resolved_job_name,
                run_id=resolved_run_id,
                event_type=event_cfg.event,
                pipeline_status=status,
                sender=event_cfg.sender,
                recipients_to=[],
                recipients_cc=event_cfg.cc,
                subject="N/A",
                template_used=event_cfg.template,
                email_status="NO_RECIPIENTS",
                error_message="No primary recipients ('to') specified"
            )
            return False

        # 4. Build HTML Table from DataFrame or SQL results
        table_html = ""
        if df is not None:
            table_html = EmailTemplateManager.render_html_table(df)
        elif sql_results is not None:
            table_html = EmailTemplateManager.render_html_table(sql_results)
        elif data_context and ("table" in data_context or "data" in data_context):
            table_html = EmailTemplateManager.render_html_table(data_context.get("table") or data_context.get("data"))

        # 5. Assemble Context Dictionary
        now_str = datetime.now(timezone.utc).isoformat()
        error_type = type(error).__name__ if error else (data_context.get("error_type", "None") if data_context else "None")
        error_msg = str(error) if error else (data_context.get("error_message", "None") if data_context else "None")
        tb_text = "".join(traceback.format_exception(type(error), error, error.__traceback__)) if error else (data_context.get("traceback", "") if data_context else "")

        context: Dict[str, Any] = {
            "job_id": job_id,
            "job_name": resolved_job_name,
            "status": status,
            "subject_prefix": event_cfg.subject_prefix or "[ETL ALERT]",
            "run_id": resolved_run_id,
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

        # 6. Render Subject and Body
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

        # 7. Resolve SMTP Credentials securely from Environment Variables
        smtp_server = os.getenv("SMTP_SERVER", "localhost")
        try:
            smtp_port = int(os.getenv("SMTP_PORT", "25"))
        except (ValueError, TypeError):
            smtp_port = 25

        smtp_user = os.getenv("SMTP_USERNAME", "")
        smtp_password = os.getenv("SMTP_PASSWORD", "")
        use_tls = os.getenv("SMTP_USE_TLS", "false").lower() in ("true", "1", "yes")

        # 8. Construct Multipart MIME Message (HTML + Plaintext)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = event_cfg.sender
        msg["To"] = ", ".join(event_cfg.to)

        if event_cfg.cc:
            msg["Cc"] = ", ".join(event_cfg.cc)

        msg.attach(MIMEText(plaintext_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        all_recipients: List[str] = list(set(event_cfg.to + event_cfg.cc + event_cfg.bcc))

        # 9. Send Email via SMTP with Fail-Safe Exception Catching & Audit Telemetry Recording
        try:
            logger.info(f"Connecting to SMTP server '{smtp_server}:{smtp_port}' to send '{status}' alert notification...")
            server = smtplib.SMTP(smtp_server, smtp_port, timeout=15)

            if use_tls:
                server.starttls()

            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)

            server.sendmail(event_cfg.sender, all_recipients, msg.as_string())
            server.quit()

            logger.info(f"Notification email ('{subject}') successfully sent to recipients: {all_recipients}")

            cls.record_email_audit(
                notification_id=notif_id,
                job_id=job_id,
                job_name=resolved_job_name,
                run_id=resolved_run_id,
                event_type=event_cfg.event,
                pipeline_status=status,
                sender=event_cfg.sender,
                recipients_to=event_cfg.to,
                recipients_cc=event_cfg.cc,
                subject=subject,
                template_used=template_choice,
                email_status="SENT",
                error_message=None
            )
            return True

        except Exception as smtp_err:
            logger.error(f"Failed to send error notification email for job '{job_id}': {smtp_err}")
            cls.record_email_audit(
                notification_id=notif_id,
                job_id=job_id,
                job_name=resolved_job_name,
                run_id=resolved_run_id,
                event_type=event_cfg.event,
                pipeline_status=status,
                sender=event_cfg.sender,
                recipients_to=event_cfg.to,
                recipients_cc=event_cfg.cc,
                subject=subject,
                template_used=template_choice,
                email_status="FAILED",
                error_message=str(smtp_err)
            )
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
        return cls.send_notification(
            job_id=job_id,
            status="FAILED",
            error=error,
            config=config,
            run_id=run_id,
            config_path=config_path
        )
