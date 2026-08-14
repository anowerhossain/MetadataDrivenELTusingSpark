"""
Qlik Replicate Task Refresh Helper Module.
Reusable Task implementation for triggering, monitoring, and refreshing Qlik Replicate tasks via REST API.
Isolates API authentication, task actions (RELOAD_TARGET, RESUME, RUN), status polling, retries, and error handling.
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

logger = setup_logger("QlikReplicateTask")


class QlikReplicateRefreshTask(BaseTask):
    """
    Reusable Task for triggering and monitoring Qlik Replicate data replication task refreshes via REST API.
    """

    def __init__(
        self,
        task_id: str,
        task_name: str,
        server_url: Optional[str] = None,
        qlik_task_name: Optional[str] = None,
        action: str = "RELOAD_TARGET",
        timeout_seconds: int = 300,
        poll_interval_seconds: int = 5,
        username: Optional[str] = None,
        password: Optional[str] = None,
        description: str = "Qlik Replicate Task Refresh"
    ):
        super().__init__(
            task_id=task_id,
            task_name=task_name,
            task_type="qlik_replicate",
            description=description
        )

        self.server_url = (server_url or os.getenv("QLIK_REPLICATE_URL", "http://localhost:3552")).rstrip("/")
        self.qlik_task_name = qlik_task_name or task_name
        self.action = action.upper()  # RELOAD_TARGET, RESUME, RUN
        self.timeout_seconds = timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.username = username or os.getenv("QLIK_REPLICATE_USER", "")
        self.password = password or os.getenv("QLIK_REPLICATE_PASSWORD", "")
        self.auth_token: Optional[str] = None

    def validate(self) -> bool:
        """Validates Qlik Replicate API URL and task configuration."""
        if not self.server_url:
            logger.error(f"[QlikReplicate] Missing server_url for task '{self.task_id}'.")
            return False
        if not self.qlik_task_name:
            logger.error(f"[QlikReplicate] Missing qlik_task_name for task '{self.task_id}'.")
            return False
        return True

    def _make_request(self, endpoint: str, method: str = "GET", payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Helper method to send HTTP requests to Qlik Replicate REST API."""
        url = f"{self.server_url}{endpoint}"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        data_bytes = None
        if payload:
            data_bytes = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
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
            logger.error(f"[QlikReplicate] HTTP {http_err.code} Error calling '{url}': {error_body}")
            raise RuntimeError(f"Qlik Replicate API HTTP {http_err.code}: {error_body}")
        except Exception as err:
            logger.error(f"[QlikReplicate] Connection Error calling '{url}': {err}")
            raise RuntimeError(f"Qlik Replicate API connection failed: {err}")

    def authenticate(self) -> bool:
        """Authenticates with Qlik Replicate Enterprise Manager REST API if credentials are provided."""
        if not self.username or not self.password:
            logger.info(f"[QlikReplicate] No explicit credentials provided for task '{self.task_id}'. Proceeding with standard headers.")
            return True

        logger.info(f"[QlikReplicate] Authenticating user '{self.username}' with Qlik Replicate API at '{self.server_url}'...")
        payload = {"username": self.username, "password": self.password}
        try:
            resp = self._make_request("/attunityreplicate/api/v1/login", method="POST", payload=payload)
            self.auth_token = resp.get("token") or resp.get("session_id")
            logger.info(f"[QlikReplicate] Authentication successful for task '{self.task_id}'.")
            return True
        except Exception as err:
            logger.warning(f"[QlikReplicate] Authentication call failed: {err}. Attempting direct request.")
            return True

    def trigger_task_action(self) -> bool:
        """Triggers task control action (RELOAD_TARGET, RESUME, RUN) on Qlik Replicate."""
        endpoint = f"/attunityreplicate/api/v1/tasks/{urllib.parse.quote(self.qlik_task_name)}/actions/{self.action.lower()}"
        logger.info(f"[QlikReplicate] Triggering action '{self.action}' for Qlik task '{self.qlik_task_name}'...")
        
        resp = self._make_request(endpoint, method="POST")
        logger.info(f"[QlikReplicate] Action '{self.action}' triggered successfully for Qlik task '{self.qlik_task_name}'. Response: {resp}")
        return True

    def poll_task_status(self) -> bool:
        """Asynchronously polls Qlik Replicate task status until completion or timeout."""
        endpoint = f"/attunityreplicate/api/v1/tasks/{urllib.parse.quote(self.qlik_task_name)}"
        start_poll = time.time()

        logger.info(f"[QlikReplicate] Polling task status for '{self.qlik_task_name}' (Timeout: {self.timeout_seconds}s)...")

        while (time.time() - start_poll) < self.timeout_seconds:
            try:
                resp = self._make_request(endpoint, method="GET")
                task_state = str(resp.get("state", resp.get("status", "RUNNING"))).upper()
                logger.info(f"[QlikReplicate] Task '{self.qlik_task_name}' status: '{task_state}'")

                if task_state in ("RUNNING", "STOPPED_AFTER_FULL_LOAD", "COMPLETED", "SUCCESS"):
                    logger.info(f"[QlikReplicate] Task '{self.qlik_task_name}' reached target completion state '{task_state}'.")
                    return True
                elif task_state in ("ERROR", "FAILED", "STOPPED_WITH_ERROR"):
                    err_msg = resp.get("error_message", f"Qlik task state reached '{task_state}'")
                    raise RuntimeError(f"Qlik Replicate task failed: {err_msg}")

            except RuntimeError:
                raise
            except Exception as poll_err:
                logger.warning(f"[QlikReplicate] Status poll check warning: {poll_err}")

            time.sleep(self.poll_interval_seconds)

        raise TimeoutError(f"Qlik Replicate task '{self.qlik_task_name}' timed out after {self.timeout_seconds} seconds.")

    def execute(self) -> bool:
        """Executes full Qlik Replicate task refresh workflow with retry policy."""
        def _workflow():
            self.authenticate()
            self.trigger_task_action()
            return self.poll_task_status()

        return RetryHandler.execute_with_retry(
            _workflow,
            max_retries=2,
            delay_seconds=5,
            backoff_multiplier=2.0
        )
