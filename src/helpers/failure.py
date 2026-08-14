"""
Failure Audit Tracking and Rerun Recovery Helper Module.
Records failed jobs in timestamped marker files under failed_jobs/YYYYMMDD/
and manages deduplicated rerun recovery and marker cleanup upon successful execution.
"""

import os
import glob
import traceback
from datetime import datetime, timezone
from typing import Dict, List
from src.helpers.logger import setup_logger

logger = setup_logger("FailureHandler")
DEFAULT_FAILED_JOBS_DIR = "failed_jobs"


class FailureHandler:
    """Manages failure marker file creation, deduplication, and cleanup."""

    @classmethod
    def get_failure_dir(cls, date_str: str, base_dir: str = DEFAULT_FAILED_JOBS_DIR) -> str:
        """Returns directory path for given date string YYYYMMDD."""
        return os.path.join(base_dir, date_str)

    @classmethod
    def record_failure(
        cls,
        config_path: str,
        job_id: str,
        error: Exception,
        base_dir: str = DEFAULT_FAILED_JOBS_DIR
    ) -> str:
        """
        Creates a timestamped failure marker file under base_dir/YYYYMMDD/.
        Filename format: {toml_basename}_{time24H}.txt
        """
        now = datetime.now(timezone.utc)
        date_folder = now.strftime("%Y%m%d")
        time_24h = now.strftime("%H%M%S")

        target_dir = os.path.join(base_dir, date_folder)
        os.makedirs(target_dir, exist_ok=True)

        toml_basename = os.path.basename(config_path)
        marker_filename = f"{toml_basename}_{time_24h}.txt"
        marker_path = os.path.join(target_dir, marker_filename)

        abs_config_path = os.path.abspath(config_path)
        formatted_tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))

        try:
            with open(marker_path, "w", encoding="utf-8") as f:
                f.write(f"FAILURE RECORD TIMESTAMP: {now.isoformat()}\n")
                f.write(f"JOB ID                 : {job_id}\n")
                f.write(f"CONFIG PATH            : {abs_config_path}\n")
                f.write(f"ERROR TYPE             : {type(error).__name__}\n")
                f.write(f"ERROR MESSAGE          : {str(error)}\n")
                f.write("--------------------------------------------------------------------------------\n")
                f.write("TRACEBACK:\n")
                f.write(formatted_tb)
                f.write("--------------------------------------------------------------------------------\n")

            logger.info(f"Recorded failure marker file: {marker_path}")
            return marker_path
        except Exception as err:
            logger.error(f"Failed to write failure marker file '{marker_path}': {err}")
            return ""

    @classmethod
    def get_failed_jobs(cls, date_str: str, base_dir: str = DEFAULT_FAILED_JOBS_DIR) -> Dict[str, List[str]]:
        """
        Scans base_dir/date_str/ and returns a deduplicated dictionary mapping:
        { config_path: [list_of_marker_file_paths] }
        """
        target_dir = cls.get_failure_dir(date_str, base_dir=base_dir)
        if not os.path.exists(target_dir):
            logger.warning(f"No failure directory found for date '{date_str}' at '{target_dir}'.")
            return {}

        search_pattern = os.path.join(target_dir, "*.txt")
        marker_files = sorted(glob.glob(search_pattern))

        job_markers: Dict[str, List[str]] = {}

        for marker in marker_files:
            cfg_path = None
            try:
                with open(marker, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("CONFIG PATH"):
                            parts = line.split(":", 1)
                            if len(parts) == 2:
                                cfg_path = parts[1].strip()
                                break
            except Exception as err:
                logger.warning(f"Failed to read marker file '{marker}': {err}")

            if cfg_path:
                if cfg_path not in job_markers:
                    job_markers[cfg_path] = []
                job_markers[cfg_path].append(marker)

        logger.info(
            f"Found {len(marker_files)} failure marker file(s) for date '{date_str}', "
            f"deduplicated to {len(job_markers)} unique failed job(s)."
        )
        return job_markers

    @classmethod
    def clear_job_failure_markers(
        cls,
        date_str: str,
        config_path: str,
        base_dir: str = DEFAULT_FAILED_JOBS_DIR
    ) -> int:
        """
        Deletes all failure marker files associated with config_path under base_dir/date_str/.
        """
        job_markers = cls.get_failed_jobs(date_str, base_dir=base_dir)
        abs_config_path = os.path.abspath(config_path)

        matching_markers = []
        for cfg, markers in job_markers.items():
            if os.path.abspath(cfg) == abs_config_path or cfg == config_path:
                matching_markers.extend(markers)

        deleted_count = 0
        for marker in matching_markers:
            try:
                if os.path.exists(marker):
                    os.remove(marker)
                    deleted_count += 1
                    logger.info(f"Cleared failure marker file: {marker}")
            except Exception as err:
                logger.error(f"Failed to delete failure marker file '{marker}': {err}")

        target_dir = cls.get_failure_dir(date_str, base_dir=base_dir)
        if os.path.exists(target_dir) and not os.listdir(target_dir):
            try:
                os.rmdir(target_dir)
                logger.info(f"Cleaned up empty failure directory: {target_dir}")
            except Exception:
                pass

        return deleted_count
