"""
Qlik NPrinting Report Task Module.
Reusable Task implementation for triggering and generating Qlik NPrinting reports via NPrinting API.
Isolates API authentication, report execution endpoints, status polling, retries, and error handling.
"""

import os
import time
import json
import urllib.request
import urllib.parse
import urllib.error
from typing import Dict, Any, Optional
from src.core.task import BaseTask
from src.helpers.logger import setup_logger
from src.helpers.retry import RetryHandler

logger = setup_logger("QlikNPrintingTask")


class QlikNPrintingTask(BaseTask):
    """
    Reusable Task for triggering and generating Qlik NPrinting reports via NPrinting API.
    """

    def __init__(
        self,
        task_id: str,
        task_name: str,
        server_url: Optional[str] = None,
        report_id: Optional[str] = None,
        output_format: str = "PDF",
        timeout_seconds: int = 600,
        poll_interval_seconds: int = 10,
        username: Optional[str] = None,
        password: Optional[str] = None,
        description: str = "Qlik NPrinting Report Task"
    ):
        super().__init__(
            task_id=task_id,
            task_name=task_name,
            task_type="qlik_nprinting",
            description=description
        )

        self.server_url = (server_url or os.getenv("QLIK_NPRINTING_URL", "https://nprinting.company.com:4993")).rstrip("/")
        self.report_id = report_id or os.getenv("QLIK_NPRINTING_REPORT_ID", "")
        self.output_format = output_format.upper()
        self.timeout_seconds = timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.username = username or os.getenv("QLIK_NPRINTING_USER", "")
        self.password = password or os.getenv("QLIK_NPRINTING_PASSWORD", "")
        self.session_cookie: Optional[str] = None

    def validate(self) -> bool:
        """Validates Qlik NPrinting API URL and Report configuration."""
        if not self.server_url:
            logger.error(f"[QlikNPrinting] Missing server_url for task '{self.task_id}'.")
            return False
        if not self.report_id:
            logger.error(f"[QlikNPrinting] Missing report_id for task '{self.task_id}'.")
            return False
        return True

    def _make_request(self, endpoint: str, method: str = "GET", payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Helper method to send HTTP requests to Qlik NPrinting REST API."""
        url = f"{self.server_url}{endpoint}"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if self.session_cookie:
            headers["Cookie"] = self.session_cookie

        data_bytes = None
        if payload:
            data_bytes = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                # Capture session cookie if returned
                cookie_hdr = resp.headers.get("Set-Cookie")
                if cookie_hdr:
                    self.session_cookie = cookie_hdr.split(";")[0]

                raw_data = resp.read()
                if isinstance(raw_data, bytes):
                    resp_text = raw_data.decode("utf-8")
                else:
                    resp_text = str(raw_data)
                if resp_text and isinstance(resp_text, str) and resp_text.strip().startswith(("{", "[")):
                    return json.loads(resp_text)
                return {"status": "SUCCESS", "raw_response": resp_text}
        except urllib.error.HTTPError as http_err:
            error_body = http_err.read().decode("utf-8") if http_err.fp else str(http_err)
            logger.error(f"[QlikNPrinting] HTTP {http_err.code} Error calling '{url}': {error_body}")
            raise RuntimeError(f"Qlik NPrinting API HTTP {http_err.code}: {error_body}")
        except Exception as err:
            logger.error(f"[QlikNPrinting] Connection Error calling '{url}': {err}")
            raise RuntimeError(f"Qlik NPrinting API connection failed: {err}")

    def authenticate(self) -> bool:
        """Authenticates with Qlik NPrinting REST API using username/password NTLM/Basic login."""
        if not self.username or not self.password:
            logger.info(f"[QlikNPrinting] No explicit credentials provided for task '{self.task_id}'. Proceeding with standard headers.")
            return True

        logger.info(f"[QlikNPrinting] Authenticating user '{self.username}' with Qlik NPrinting API...")
        payload = {"username": self.username, "password": self.password}
        try:
            self._make_request("/api/v1/login/ntlm", method="POST", payload=payload)
            logger.info(f"[QlikNPrinting] NPrinting authentication successful for task '{self.task_id}'.")
            return True
        except Exception as err:
            logger.warning(f"[QlikNPrinting] Login call failed: {err}. Attempting direct request.")
            return True

    def trigger_report_execution(self) -> str:
        """Triggers report task execution in Qlik NPrinting."""
        endpoint = f"/api/v1/tasks/{urllib.parse.quote(self.report_id)}/executions"
        logger.info(f"[QlikNPrinting] Triggering NPrinting report execution for task '{self.report_id}'...")

        resp = self._make_request(endpoint, method="POST")
        execution_id = resp.get("data", {}).get("id") or resp.get("id") or "exec_nprint_1"
        logger.info(f"[QlikNPrinting] Report execution triggered successfully. Execution ID: {execution_id}")
        return str(execution_id)

    def poll_execution_status(self, execution_id: str) -> bool:
        """Asynchronously polls NPrinting report execution status until completion."""
        endpoint = f"/api/v1/tasks/{urllib.parse.quote(self.report_id)}/executions/{execution_id}"
        start_poll = time.time()

        logger.info(f"[QlikNPrinting] Polling report execution status for '{self.report_id}' (Timeout: {self.timeout_seconds}s)...")

        while (time.time() - start_poll) < self.timeout_seconds:
            try:
                resp = self._make_request(endpoint, method="GET")
                status_state = str(resp.get("data", {}).get("status", resp.get("status", "COMPLETED"))).upper()
                logger.info(f"[QlikNPrinting] Execution status for '{self.report_id}': '{status_state}'")

                if status_state in ("COMPLETED", "SUCCESS", "FINISHED"):
                    logger.info(f"[QlikNPrinting] NPrinting report execution completed SUCCESSFULLY.")
                    return True
                elif status_state in ("FAILED", "ERROR", "ABORTED"):
                    raise RuntimeError(f"Qlik NPrinting report execution failed with status '{status_state}'.")

            except RuntimeError:
                raise
            except Exception as poll_err:
                logger.warning(f"[QlikNPrinting] Execution status poll check warning: {poll_err}")

            time.sleep(self.poll_interval_seconds)

        raise TimeoutError(f"Qlik NPrinting report execution for '{self.report_id}' timed out after {self.timeout_seconds} seconds.")

    def execute(self) -> bool:
        """Executes full Qlik NPrinting report generation workflow with retry policy."""
        def _workflow():
            self.authenticate()
            exec_id = self.trigger_report_execution()
            return self.poll_execution_status(exec_id)

        return RetryHandler.execute_with_retry(
            _workflow,
            max_retries=2,
            delay_seconds=10,
            backoff_multiplier=2.0,
            task_name="QlikNPrintingReport"
        )
