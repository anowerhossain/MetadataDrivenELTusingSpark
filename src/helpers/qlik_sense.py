"""
Qlik Sense Report / App Reload Task Module.
Reusable Task implementation for triggering and monitoring Qlik Sense App / Report reloads via QRS API.
Isolates API authentication, reload endpoints, status polling, retries, and error handling.
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

logger = setup_logger("QlikSenseTask")


class QlikSenseRefreshTask(BaseTask):
    """
    Reusable Task for triggering and monitoring Qlik Sense App / Report reloads via QRS API.
    """

    def __init__(
        self,
        task_id: str,
        task_name: str,
        server_url: Optional[str] = None,
        app_id: Optional[str] = None,
        qlik_sense_task_id: Optional[str] = None,
        timeout_seconds: int = 600,
        poll_interval_seconds: int = 10,
        api_key: Optional[str] = None,
        description: str = "Qlik Sense App Reload Task"
    ):
        super().__init__(
            task_id=task_id,
            task_name=task_name,
            task_type="qlik_sense",
            description=description
        )

        self.server_url = (server_url or os.getenv("QLIK_SENSE_URL", "https://qlik-sense.company.com")).rstrip("/")
        self.app_id = app_id or os.getenv("QLIK_SENSE_APP_ID", "")
        self.qlik_sense_task_id = qlik_sense_task_id
        self.timeout_seconds = timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.api_key = api_key or os.getenv("QLIK_SENSE_API_KEY", "")

    def validate(self) -> bool:
        """Validates Qlik Sense API URL and App/Task configuration."""
        if not self.server_url:
            logger.error(f"[QlikSense] Missing server_url for task '{self.task_id}'.")
            return False
        if not self.app_id and not self.qlik_sense_task_id:
            logger.error(f"[QlikSense] Must specify either app_id or qlik_sense_task_id for task '{self.task_id}'.")
            return False
        return True

    def _make_request(self, endpoint: str, method: str = "POST", payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Helper method to send HTTP requests to Qlik Sense QRS REST API."""
        url = f"{self.server_url}{endpoint}"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if self.api_key:
            headers["X-Qlik-Xrfkey"] = "1234567890abcdef"
            headers["Authorization"] = f"Bearer {self.api_key}"

        data_bytes = None
        if payload:
            data_bytes = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
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
            logger.error(f"[QlikSense] HTTP {http_err.code} Error calling '{url}': {error_body}")
            raise RuntimeError(f"Qlik Sense QRS API HTTP {http_err.code}: {error_body}")
        except Exception as err:
            logger.error(f"[QlikSense] Connection Error calling '{url}': {err}")
            raise RuntimeError(f"Qlik Sense QRS API connection failed: {err}")

    def trigger_app_reload(self) -> str:
        """Triggers asynchronous app reload in Qlik Sense via QRS API."""
        if self.qlik_sense_task_id:
            endpoint = f"/qrs/task/{self.qlik_sense_task_id}/start"
            logger.info(f"[QlikSense] Triggering Qlik Sense reload task '{self.qlik_sense_task_id}'...")
        else:
            endpoint = f"/qrs/app/{self.app_id}/reload"
            logger.info(f"[QlikSense] Triggering direct reload for Qlik Sense App '{self.app_id}'...")

        resp = self._make_request(endpoint, method="POST")
        execution_id = resp.get("value") or resp.get("id") or "exec_started"
        logger.info(f"[QlikSense] Reload triggered successfully. Execution ID: {execution_id}")
        return str(execution_id)

    def poll_reload_status(self, execution_id: str) -> bool:
        """Asynchronously polls Qlik Sense reload execution status until completion."""
        endpoint = f"/qrs/reloadtask/progress/{execution_id}" if self.qlik_sense_task_id else f"/qrs/app/{self.app_id}/state"
        start_poll = time.time()

        logger.info(f"[QlikSense] Polling reload status for App '{self.app_id}' (Timeout: {self.timeout_seconds}s)...")

        while (time.time() - start_poll) < self.timeout_seconds:
            try:
                resp = self._make_request(endpoint, method="GET")
                status_state = str(resp.get("status", resp.get("state", "7"))).upper()
                logger.info(f"[QlikSense] Reload status for '{self.app_id}': '{status_state}'")

                # State 7 = Finished Successfully in QRS API
                if status_state in ("7", "FINISHED", "COMPLETED", "SUCCESS"):
                    logger.info(f"[QlikSense] Qlik Sense reload completed SUCCESSFULLY.")
                    return True
                elif status_state in ("8", "FAILED", "ERROR", "ABORTED"):
                    raise RuntimeError(f"Qlik Sense reload failed with state '{status_state}'.")

            except RuntimeError:
                raise
            except Exception as poll_err:
                logger.warning(f"[QlikSense] Reload status poll check warning: {poll_err}")

            time.sleep(self.poll_interval_seconds)

        raise TimeoutError(f"Qlik Sense reload for App '{self.app_id}' timed out after {self.timeout_seconds} seconds.")

    def execute(self) -> bool:
        """Executes full Qlik Sense App reload workflow with retry policy."""
        def _workflow():
            exec_id = self.trigger_app_reload()
            return self.poll_reload_status(exec_id)

        return RetryHandler.execute_with_retry(
            _workflow,
            max_retries=2,
            delay_seconds=10,
            backoff_multiplier=2.0,
            task_name="QlikSenseReload"
        )
