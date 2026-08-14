"""
Reusable Email Template Engine Module.
Provides HTML table rendering for DataFrames & SQL results, safe placeholder substitution,
and built-in responsive HTML email alert presets (job_failed, job_success, data_quality_failed,
sla_breached, missing_file, data_anomaly).
"""

import html
from typing import Any, Dict, List, Optional, Union
from src.helpers.logger import setup_logger

logger = setup_logger("EmailTemplateManager")


class SafeDict(dict):
    """Fallback dictionary that preserves missing placeholder keys as {key} instead of raising KeyError."""
    def __missing__(self, key: str) -> str:
        return f"{{{key}}}"


class EmailTemplateManager:
    """Manages HTML email template rendering and dynamic data formatting."""

    BUILTIN_TEMPLATES = {
        "job_failed": """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; color: #333; }}
        .card {{ background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); max-width: 800px; margin: 0 auto; overflow: hidden; border: 1px solid #e1e4e8; }}
        .header {{ background-color: #d9534f; color: #ffffff; padding: 20px 28px; font-size: 20px; font-weight: bold; }}
        .content {{ padding: 28px; }}
        .table-meta {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; }}
        .table-meta td {{ padding: 8px 12px; border-bottom: 1px solid #edf2f7; font-size: 14px; }}
        .table-meta td.label {{ font-weight: 600; color: #4a5568; width: 30%; }}
        .traceback-box {{ background-color: #1e1e1e; color: #f8f8f2; padding: 16px; border-radius: 6px; font-family: 'Courier New', Courier, monospace; font-size: 13px; overflow-x: auto; white-space: pre-wrap; margin-top: 16px; }}
        .data-section {{ margin-top: 24px; }}
        .footer {{ background-color: #f8f9fa; border-top: 1px solid #e9ecef; padding: 14px 28px; font-size: 12px; color: #6c757d; text-align: center; }}
    </style>
</head>
<body>
    <div class="card">
        <div class="header">🚨 {subject_prefix} Job '{job_name}' Failed</div>
        <div class="content">
            <table class="table-meta">
                <tr><td class="label">Job ID</td><td>{job_id}</td></tr>
                <tr><td class="label">Job Name</td><td>{job_name}</td></tr>
                <tr><td class="label">Status</td><td><span style="color: #d9534f; font-weight: bold;">{status}</span></td></tr>
                <tr><td class="label">Execution Run ID</td><td>{run_id}</td></tr>
                <tr><td class="label">Timestamp (UTC)</td><td>{date}</td></tr>
                <tr><td class="label">Config Path</td><td>{config_path}</td></tr>
                <tr><td class="label">Error Type</td><td>{error_type}</td></tr>
                <tr><td class="label">Error Details</td><td>{error_message}</td></tr>
            </table>

            {table_html}

            <div style="font-weight: 600; margin-top: 20px; color: #2d3748;">Python Exception Traceback:</div>
            <div class="traceback-box">{traceback}</div>
        </div>
        <div class="footer">Cloudera CDP PySpark Iceberg ETL Framework • Automated Failure Alert</div>
    </div>
</body>
</html>
""",
        "job_success": """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; color: #333; }}
        .card {{ background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); max-width: 800px; margin: 0 auto; overflow: hidden; border: 1px solid #e1e4e8; }}
        .header {{ background-color: #28a745; color: #ffffff; padding: 20px 28px; font-size: 20px; font-weight: bold; }}
        .content {{ padding: 28px; }}
        .table-meta {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; }}
        .table-meta td {{ padding: 8px 12px; border-bottom: 1px solid #edf2f7; font-size: 14px; }}
        .table-meta td.label {{ font-weight: 600; color: #4a5568; width: 30%; }}
        .footer {{ background-color: #f8f9fa; border-top: 1px solid #e9ecef; padding: 14px 28px; font-size: 12px; color: #6c757d; text-align: center; }}
    </style>
</head>
<body>
    <div class="card">
        <div class="header">✅ Job '{job_name}' Completed Successfully</div>
        <div class="content">
            <table class="table-meta">
                <tr><td class="label">Job ID</td><td>{job_id}</td></tr>
                <tr><td class="label">Job Name</td><td>{job_name}</td></tr>
                <tr><td class="label">Status</td><td><span style="color: #28a745; font-weight: bold;">{status}</span></td></tr>
                <tr><td class="label">Execution Run ID</td><td>{run_id}</td></tr>
                <tr><td class="label">Timestamp (UTC)</td><td>{date}</td></tr>
                <tr><td class="label">Rows Read</td><td>{rows_read}</td></tr>
                <tr><td class="label">Rows Written</td><td>{rows_written}</td></tr>
                <tr><td class="label">Execution Duration</td><td>{duration_seconds} sec</td></tr>
            </table>

            {table_html}
        </div>
        <div class="footer">Cloudera CDP PySpark Iceberg ETL Framework • Success Notification</div>
    </div>
</body>
</html>
""",
        "data_quality_failed": """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; color: #333; }}
        .card {{ background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); max-width: 800px; margin: 0 auto; overflow: hidden; border: 1px solid #e1e4e8; }}
        .header {{ background-color: #fd7e14; color: #ffffff; padding: 20px 28px; font-size: 20px; font-weight: bold; }}
        .content {{ padding: 28px; }}
        .table-meta {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; }}
        .table-meta td {{ padding: 8px 12px; border-bottom: 1px solid #edf2f7; font-size: 14px; }}
        .table-meta td.label {{ font-weight: 600; color: #4a5568; width: 30%; }}
        .footer {{ background-color: #f8f9fa; border-top: 1px solid #e9ecef; padding: 14px 28px; font-size: 12px; color: #6c757d; text-align: center; }}
    </style>
</head>
<body>
    <div class="card">
        <div class="header">⚠️ Data Quality Check Failed: '{job_name}'</div>
        <div class="content">
            <table class="table-meta">
                <tr><td class="label">Job ID</td><td>{job_id}</td></tr>
                <tr><td class="label">Job Name</td><td>{job_name}</td></tr>
                <tr><td class="label">Failed Check Type</td><td>{quality_rule}</td></tr>
                <tr><td class="label">Validation Error</td><td>{error_message}</td></tr>
                <tr><td class="label">Timestamp (UTC)</td><td>{date}</td></tr>
            </table>

            <div style="font-weight: 600; margin-bottom: 10px; color: #2d3748;">Sample Violating Records:</div>
            {table_html}
        </div>
        <div class="footer">Cloudera CDP PySpark Iceberg ETL Framework • Data Quality Alert</div>
    </div>
</body>
</html>
""",
        "sla_breached": """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; color: #333; }}
        .card {{ background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); max-width: 800px; margin: 0 auto; overflow: hidden; border: 1px solid #e1e4e8; }}
        .header {{ background-color: #ffc107; color: #212529; padding: 20px 28px; font-size: 20px; font-weight: bold; }}
        .content {{ padding: 28px; }}
        .table-meta {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; }}
        .table-meta td {{ padding: 8px 12px; border-bottom: 1px solid #edf2f7; font-size: 14px; }}
        .table-meta td.label {{ font-weight: 600; color: #4a5568; width: 30%; }}
        .footer {{ background-color: #f8f9fa; border-top: 1px solid #e9ecef; padding: 14px 28px; font-size: 12px; color: #6c757d; text-align: center; }}
    </style>
</head>
<body>
    <div class="card">
        <div class="header">⏱️ SLA Delay Alert: Job '{job_name}' Exceeded SLA Target</div>
        <div class="content">
            <table class="table-meta">
                <tr><td class="label">Job ID</td><td>{job_id}</td></tr>
                <tr><td class="label">Job Name</td><td>{job_name}</td></tr>
                <tr><td class="label">Target SLA Time</td><td>{expected_sla_time}</td></tr>
                <tr><td class="label">Actual Run Duration</td><td>{duration_seconds} sec</td></tr>
                <tr><td class="label">Timestamp (UTC)</td><td>{date}</td></tr>
            </table>

            {table_html}
        </div>
        <div class="footer">Cloudera CDP PySpark Iceberg ETL Framework • SLA Monitoring Alert</div>
    </div>
</body>
</html>
""",
        "missing_file": """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; color: #333; }}
        .card {{ background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); max-width: 800px; margin: 0 auto; overflow: hidden; border: 1px solid #e1e4e8; }}
        .header {{ background-color: #dc3545; color: #ffffff; padding: 20px 28px; font-size: 20px; font-weight: bold; }}
        .content {{ padding: 28px; }}
        .table-meta {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; }}
        .table-meta td {{ padding: 8px 12px; border-bottom: 1px solid #edf2f7; font-size: 14px; }}
        .table-meta td.label {{ font-weight: 600; color: #4a5568; width: 30%; }}
        .footer {{ background-color: #f8f9fa; border-top: 1px solid #e9ecef; padding: 14px 28px; font-size: 12px; color: #6c757d; text-align: center; }}
    </style>
</head>
<body>
    <div class="card">
        <div class="header">📁 Missing Source File Alert: '{job_name}'</div>
        <div class="content">
            <table class="table-meta">
                <tr><td class="label">Job ID</td><td>{job_id}</td></tr>
                <tr><td class="label">Job Name</td><td>{job_name}</td></tr>
                <tr><td class="label">SFTP / File Directory</td><td>{file_path}</td></tr>
                <tr><td class="label">Expected File Pattern</td><td>{file_pattern}</td></tr>
                <tr><td class="label">Timestamp (UTC)</td><td>{date}</td></tr>
            </table>

            {table_html}
        </div>
        <div class="footer">Cloudera CDP PySpark Iceberg ETL Framework • SFTP File Monitor Alert</div>
    </div>
</body>
</html>
""",
        "data_anomaly": """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; color: #333; }}
        .card {{ background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); max-width: 800px; margin: 0 auto; overflow: hidden; border: 1px solid #e1e4e8; }}
        .header {{ background-color: #6f42c1; color: #ffffff; padding: 20px 28px; font-size: 20px; font-weight: bold; }}
        .content {{ padding: 28px; }}
        .table-meta {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; }}
        .table-meta td {{ padding: 8px 12px; border-bottom: 1px solid #edf2f7; font-size: 14px; }}
        .table-meta td.label {{ font-weight: 600; color: #4a5568; width: 30%; }}
        .footer {{ background-color: #f8f9fa; border-top: 1px solid #e9ecef; padding: 14px 28px; font-size: 12px; color: #6c757d; text-align: center; }}
    </style>
</head>
<body>
    <div class="card">
        <div class="header">🔮 Data Anomaly Detected: '{job_name}'</div>
        <div class="content">
            <table class="table-meta">
                <tr><td class="label">Job ID</td><td>{job_id}</td></tr>
                <tr><td class="label">Job Name</td><td>{job_name}</td></tr>
                <tr><td class="label">Anomaly Summary</td><td>{error_message}</td></tr>
                <tr><td class="label">Timestamp (UTC)</td><td>{date}</td></tr>
            </table>

            <div style="font-weight: 600; margin-bottom: 10px; color: #2d3748;">Anomalous Sample Data:</div>
            {table_html}
        </div>
        <div class="footer">Cloudera CDP PySpark Iceberg ETL Framework • Data Anomaly Alert</div>
    </div>
</body>
</html>
"""
    }

    @classmethod
    def render_html_table(cls, data: Any, max_rows: int = 50) -> str:
        """
        Renders Pandas DataFrames, PySpark DataFrames, lists of dicts (SQL query results),
        or dicts into clean, inline-styled HTML table markup.
        """
        if data is None:
            return ""

        # Handle Pandas DataFrame
        if hasattr(data, "to_html") and callable(getattr(data, "to_html")):
            try:
                limited_df = data.head(max_rows)
                styled_html = limited_df.to_html(
                    index=False,
                    classes="custom-data-table",
                    border=0
                )
                return f"""
                <style>
                    .custom-data-table {{ width: 100%; border-collapse: collapse; font-family: sans-serif; font-size: 13px; margin-top: 12px; }}
                    .custom-data-table th {{ background-color: #4a5568; color: white; padding: 8px; text-align: left; }}
                    .custom-data-table td {{ padding: 8px; border-bottom: 1px solid #e2e8f0; }}
                    .custom-data-table tr:nth-child(even) {{ background-color: #f7fafc; }}
                </style>
                <div style="overflow-x: auto;">{styled_html}</div>
                """
            except Exception as err:
                logger.warning(f"Failed to render DataFrame to HTML: {err}")
                return ""

        # Handle PySpark DataFrame (convert to Pandas)
        if hasattr(data, "limit") and hasattr(data, "toPandas"):
            try:
                pdf = data.limit(max_rows).toPandas()
                return cls.render_html_table(pdf, max_rows=max_rows)
            except Exception as err:
                logger.warning(f"Failed to convert PySpark DataFrame to Pandas: {err}")
                return ""

        # Handle List of Dicts (SQL Query Results / Tabular Rows)
        if isinstance(data, list) and data and isinstance(data[0], dict):
            headers = list(data[0].keys())
            rows_html = []
            for row in data[:max_rows]:
                cells = "".join(f"<td style='padding: 8px; border-bottom: 1px solid #e2e8f0;'>{html.escape(str(row.get(h, '')))}</td>" for h in headers)
                rows_html.append(f"<tr>{cells}</tr>")

            headers_html = "".join(f"<th style='background-color: #4a5568; color: white; padding: 8px; text-align: left;'>{html.escape(str(h))}</th>" for h in headers)

            return f"""
            <div style="overflow-x: auto; margin-top: 12px;">
                <table style="width: 100%; border-collapse: collapse; font-family: sans-serif; font-size: 13px;">
                    <thead><tr>{headers_html}</tr></thead>
                    <tbody>{"".join(rows_html)}</tbody>
                </table>
            </div>
            """

        # Handle Dict (Key-Value summary data)
        if isinstance(data, dict) and data:
            rows = "".join(
                f"<tr><td style='font-weight: 600; padding: 6px 12px; border-bottom: 1px solid #edf2f7; width: 35%;'>{html.escape(str(k))}</td>"
                f"<td style='padding: 6px 12px; border-bottom: 1px solid #edf2f7;'>{html.escape(str(v))}</td></tr>"
                for k, v in data.items()
            )
            return f"""
            <table style="width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px;">
                <tbody>{rows}</tbody>
            </table>
            """

        return ""

    @classmethod
    def render_subject(cls, pattern: Optional[str], context: Dict[str, Any]) -> str:
        """Evaluates custom or default subject pattern with safe placeholder formatting."""
        if not pattern:
            prefix = context.get("subject_prefix", "[ETL JOB FAILURE]")
            job_name = context.get("job_name", context.get("job_id", "Job"))
            status = context.get("status", "FAILED")
            return f"{prefix} Job '{job_name}' Status: {status}"

        safe_ctx = SafeDict({k: str(v) for k, v in context.items()})
        try:
            return pattern.format_map(safe_ctx)
        except Exception as err:
            logger.warning(f"Failed to evaluate subject pattern '{pattern}': {err}")
            return f"{context.get('subject_prefix', '[ETL JOB FAILURE]')} Job '{context.get('job_id', 'Job')}'"

    @classmethod
    def render_template(
        cls,
        template_name_or_raw: str,
        context: Dict[str, Any]
    ) -> str:
        """
        Renders HTML email template by looking up preset or raw HTML string with safe placeholder evaluation.
        """
        key = template_name_or_raw.lower().strip()
        template_str = cls.BUILTIN_TEMPLATES.get(key, template_name_or_raw)

        # Prepare default context placeholders
        defaults = {
            "subject_prefix": "[ETL JOB FAILURE]",
            "job_id": "UNKNOWN",
            "job_name": "ETL Job",
            "status": "UNKNOWN",
            "run_id": "N/A",
            "config_path": "N/A",
            "date": "N/A",
            "error_type": "None",
            "error_message": "None",
            "traceback": "",
            "rows_read": 0,
            "rows_written": 0,
            "duration_seconds": 0.0,
            "table_html": "",
            "quality_rule": "N/A",
            "expected_sla_time": "N/A",
            "file_path": "N/A",
            "file_pattern": "N/A",
        }

        # Merge defaults with provided context
        merged = dict(defaults)
        for k, v in context.items():
            if v is not None:
                merged[k] = v

        safe_context = SafeDict({k: str(v) for k, v in merged.items()})

        try:
            return template_str.format_map(safe_context)
        except Exception as err:
            logger.error(f"Error rendering template '{template_name_or_raw}': {err}")
            return f"<p>Notification for Job '{merged.get('job_id')}' (Status: {merged.get('status')})</p><pre>{merged.get('error_message')}</pre>"
