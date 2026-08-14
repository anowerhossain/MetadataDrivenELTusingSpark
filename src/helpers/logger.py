"""
Logging & Telemetry Helper Module.
Provides structured framework logger initialization and ETLLogger telemetry tracking.
"""

import json
import logging
import sys
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any


def setup_logger(name: str = "ETL_Framework", level: int = logging.INFO) -> logging.Logger:
    """Initializes and returns a structured logger following standard formatting."""
    log_obj = logging.getLogger(name)
    if not log_obj.handlers:
        log_obj.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        log_obj.addHandler(handler)
    return log_obj


logger = setup_logger("ETLAuditLogger")


@dataclass
class JobMetrics:
    job_id: str
    job_name: str
    run_id: str
    source: str
    target: str
    load_type: str
    start_time: str
    end_time: Optional[str] = None
    status: str = "RUNNING"
    rows_read: int = 0
    rows_written: int = 0
    duration: float = 0.0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        def json_default(val: Any) -> Any:
            if isinstance(val, (int, float, str, bool, type(None))):
                return val
            try:
                return int(val)
            except (ValueError, TypeError):
                return str(val)

        return json.dumps(self.to_dict(), default=json_default, indent=2)


class ETLLogger:
    """Manages structured job telemetry logging."""

    def __init__(
        self,
        job_id: str,
        job_name: str,
        source: str,
        target: str,
        load_type: str,
        run_id: Optional[str] = None,
    ):
        self.run_id = run_id or f"run_{uuid.uuid4().hex[:12]}"
        self.start_dt = datetime.now(timezone.utc)
        self.metrics = JobMetrics(
            job_id=job_id,
            job_name=job_name,
            run_id=self.run_id,
            source=source,
            target=target,
            load_type=load_type,
            start_time=self.start_dt.isoformat(),
        )
        logger.info(
            f"Initialized ETL Run '{self.run_id}' for Job '{job_id}' ({job_name}). "
            f"Source='{source}', Target='{target}', LoadType='{load_type}'"
        )

    def record_rows_read(self, count: Any) -> None:
        try:
            self.metrics.rows_read = int(count)
        except (ValueError, TypeError):
            self.metrics.rows_read = 0
        logger.info(f"[{self.run_id}] Rows read from source: {self.metrics.rows_read}")

    def record_rows_written(self, count: Any) -> None:
        try:
            self.metrics.rows_written = int(count)
        except (ValueError, TypeError):
            self.metrics.rows_written = 0
        logger.info(f"[{self.run_id}] Rows written to target: {self.metrics.rows_written}")

    def complete_success(self, rows_read: Any = 0, rows_written: Any = 0) -> JobMetrics:
        end_dt = datetime.now(timezone.utc)
        duration = round((end_dt - self.start_dt).total_seconds(), 3)

        if rows_read:
            self.record_rows_read(rows_read)
        if rows_written:
            self.record_rows_written(rows_written)

        self.metrics.end_time = end_dt.isoformat()
        self.metrics.status = "SUCCESS"
        self.metrics.duration = duration

        logger.info(f"[{self.run_id}] ETL Job Completed SUCCESSFULLY.")
        logger.info(f"[{self.run_id}] Structured Telemetry Summary:\n{self.metrics.to_json()}")
        return self.metrics

    def complete_failure(self, error: Exception) -> JobMetrics:
        end_dt = datetime.now(timezone.utc)
        duration = round((end_dt - self.start_dt).total_seconds(), 3)

        self.metrics.end_time = end_dt.isoformat()
        self.metrics.status = "FAILED"
        self.metrics.duration = duration
        self.metrics.error_message = str(error)

        logger.error(f"[{self.run_id}] ETL Job FAILED with error: {self.metrics.error_message}")
        logger.error(f"[{self.run_id}] Structured Telemetry Summary:\n{self.metrics.to_json()}")
        return self.metrics
