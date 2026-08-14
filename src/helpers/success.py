"""
Success Audit Tracking Helper Module.
Records successful jobs in timestamped marker files under success_jobs/YYYYMMDD/
Filename format: {toml_basename}_{HHMMSS}.txt
"""

import os
import glob
from datetime import datetime, timezone
from typing import Dict, List, Optional
from src.helpers.logger import setup_logger

logger = setup_logger("SuccessHandler")
DEFAULT_SUCCESS_JOBS_DIR = "success_jobs"


class SuccessHandler:
    """Manages success marker file creation and catalog scanning under success_jobs/YYYYMMDD/."""

    @classmethod
    def get_success_dir(cls, date_str: str, base_dir: str = DEFAULT_SUCCESS_JOBS_DIR) -> str:
        """Returns directory path for given date string YYYYMMDD."""
        return os.path.join(base_dir, date_str)

    @classmethod
    def record_success(
        cls,
        config_path: str,
        job_id: str,
        run_id: str = "",
        rows_read: int = 0,
        rows_written: int = 0,
        duration_seconds: float = 0.0,
        base_dir: str = DEFAULT_SUCCESS_JOBS_DIR
    ) -> str:
        """
        Creates a timestamped success marker file under base_dir/YYYYMMDD/.
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

        try:
            with open(marker_path, "w", encoding="utf-8") as f:
                f.write(f"SUCCESS RECORD TIMESTAMP: {now.isoformat()}\n")
                f.write(f"JOB ID                 : {job_id}\n")
                f.write(f"CONFIG PATH            : {abs_config_path}\n")
                f.write(f"EXECUTION RUN ID       : {run_id}\n")
                f.write(f"STATUS                 : SUCCESS\n")
                f.write(f"ROWS READ              : {rows_read}\n")
                f.write(f"ROWS WRITTEN           : {rows_written}\n")
                f.write(f"DURATION (SECONDS)     : {duration_seconds:.2f}\n")
                f.write("--------------------------------------------------------------------------------\n")
                f.write("PIPELINE SUMMARY:\n")
                f.write(f"ETL Job '{job_id}' completed successfully without errors.\n")
                f.write("--------------------------------------------------------------------------------\n")

            logger.info(f"Recorded success marker file: {marker_path}")
            return marker_path
        except Exception as err:
            logger.error(f"Failed to write success marker file '{marker_path}': {err}")
            return ""

    @classmethod
    def get_success_jobs(cls, date_str: str, base_dir: str = DEFAULT_SUCCESS_JOBS_DIR) -> Dict[str, List[str]]:
        """
        Scans base_dir/date_str/ and returns a deduplicated dictionary mapping:
        { config_path: [list_of_success_marker_file_paths] }
        """
        target_dir = cls.get_success_dir(date_str, base_dir=base_dir)
        if not os.path.exists(target_dir):
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
                logger.warning(f"Failed to read success marker file '{marker}': {err}")

            if cfg_path:
                if cfg_path not in job_markers:
                    job_markers[cfg_path] = []
                job_markers[cfg_path].append(marker)

        return job_markers

    @classmethod
    def is_job_succeeded_today(
        cls,
        config_path: str,
        date_str: Optional[str] = None,
        base_dir: str = DEFAULT_SUCCESS_JOBS_DIR
    ) -> bool:
        """
        Checks if a job matching config_path has already completed successfully today (or date_str).
        """
        if not date_str:
            date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

        success_jobs = cls.get_success_jobs(date_str, base_dir=base_dir)
        abs_config_path = os.path.abspath(config_path)

        for cfg, markers in success_jobs.items():
            if os.path.abspath(cfg) == abs_config_path or cfg == config_path:
                if len(markers) > 0:
                    return True

        # Also check matching by basename in the date directory (e.g. sftp_invoices_csv.toml_*.txt)
        target_dir = cls.get_success_dir(date_str, base_dir=base_dir)
        if os.path.exists(target_dir):
            toml_base = os.path.basename(config_path)
            pattern = os.path.join(target_dir, f"{toml_base}_*.txt")
            if len(glob.glob(pattern)) > 0:
                return True

        return False
