"""
Core Luigi Task Wrapper Module.
Provides BaseLuigiTask / FrameworkLuigiTask inheriting luigi.Task for workflow orchestration.
Enables TOML-driven DAG dependency resolution, output target state persistence, and Luigi scheduler integration
while keeping existing BaseTask, TableLoadTask, QlikReplicate, QlikSense, and QlikNPrinting tasks 100% unchanged.
"""

import os
import sys
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

try:
    import luigi
    LUIGI_AVAILABLE = True
except ImportError:
    LUIGI_AVAILABLE = False
    luigi = None

from src.core.config import ConfigParser, JobConfig
from src.core.task import BaseTask, TableLoadTask
from src.helpers.qlik_replicate import QlikReplicateRefreshTask
from src.helpers.qlik_sense import QlikSenseRefreshTask
from src.helpers.qlik_nprinting import QlikNPrintingTask
from src.helpers.logger import setup_logger

logger = setup_logger("LuigiTask")


def create_framework_task_instance(config: JobConfig) -> BaseTask:
    """
    Instantiates the appropriate BaseTask subclass from JobConfig metadata.
    Reuses existing task executors without duplicating ETL logic.
    """
    raw_dict = config.raw_config
    task_sec = raw_dict.get("task", {}) or raw_dict.get("job", {})
    task_type = str(task_sec.get("type", "table_load")).lower()

    if task_type == "qlik_replicate":
        qr_sec = raw_dict.get("qlik_replicate", {})
        return QlikReplicateRefreshTask(
            task_id=config.job.task_id,
            task_name=config.job.task_name,
            server_url=qr_sec.get("server_url"),
            qlik_task_name=qr_sec.get("task_name"),
            action=qr_sec.get("action", "RELOAD_TARGET"),
            timeout_seconds=qr_sec.get("timeout_seconds", 300),
            poll_interval_seconds=qr_sec.get("poll_interval_seconds", 5),
            description=config.job.description
        )
    elif task_type == "qlik_sense":
        qs_sec = raw_dict.get("qlik_sense", {})
        return QlikSenseRefreshTask(
            task_id=config.job.task_id,
            task_name=config.job.task_name,
            server_url=qs_sec.get("server_url"),
            app_id=qs_sec.get("app_id"),
            qlik_sense_task_id=qs_sec.get("task_id"),
            timeout_seconds=qs_sec.get("timeout_seconds", 600),
            poll_interval_seconds=qs_sec.get("poll_interval_seconds", 10),
            description=config.job.description
        )
    elif task_type == "qlik_nprinting":
        np_sec = raw_dict.get("qlik_nprinting", {})
        return QlikNPrintingTask(
            task_id=config.job.task_id,
            task_name=config.job.task_name,
            server_url=np_sec.get("server_url"),
            report_id=np_sec.get("report_id"),
            output_format=np_sec.get("output_format", "PDF"),
            username=np_sec.get("username"),
            password=np_sec.get("password"),
            timeout_seconds=np_sec.get("timeout_seconds", 600),
            poll_interval_seconds=np_sec.get("poll_interval_seconds", 10),
            description=config.job.description
        )
    else:
        # Default TableLoadTask
        return TableLoadTask(config)


if LUIGI_AVAILABLE:
    class FrameworkLuigiTask(luigi.Task):
        """
        Luigi Task wrapper that dynamically resolves dependencies from TOML 'depends_on' field
        and executes framework BaseTask workflows natively.
        """
        config_path = luigi.Parameter()
        all_task_map = luigi.DictParameter(default={})

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._parsed_config: Optional[JobConfig] = None

        @property
        def parsed_config(self) -> JobConfig:
            if self._parsed_config is None:
                self._parsed_config = ConfigParser.load_toml(str(self.config_path))
            return self._parsed_config

        def requires(self):
            """
            Dynamically resolves required upstream task dependencies from TOML 'depends_on' list.
            If depends_on is empty, returns empty list [] (Root Task).
            """
            deps = self.parsed_config.job.depends_on
            required_tasks = []
            for dep_id in deps:
                if dep_id in self.all_task_map:
                    dep_path = self.all_task_map[dep_id]
                    required_tasks.append(
                        FrameworkLuigiTask(config_path=dep_path, all_task_map=self.all_task_map)
                    )
                else:
                    logger.warning(f"[LuigiTask] Upstream dependency '{dep_id}' not found in task map.")
            return required_tasks

        def output(self):
            """
            Luigi Target output marker in success_jobs/luigi_markers/ for state persistence.
            """
            task_id = self.parsed_config.job.task_id
            today_date = datetime.now(timezone.utc).strftime("%Y%m%d")
            target_path = os.path.join("success_jobs", "luigi_markers", today_date, f"{task_id}.done")
            return luigi.LocalTarget(target_path)

        def run(self):
            """
            Executes framework BaseTask workflow natively with real-time RUNNING marker tracking.
            """
            config = self.parsed_config
            task_instance = create_framework_task_instance(config)

            today_date = datetime.now(timezone.utc).strftime("%Y%m%d")
            running_marker = os.path.join("running_jobs", today_date, f"{task_instance.task_id}.running")
            os.makedirs(os.path.dirname(running_marker), exist_ok=True)

            logger.info(f"[LuigiTask] Starting Luigi task wrapper for '{task_instance.task_id}' ({task_instance.task_name})...")
            
            # Write RUNNING marker
            with open(running_marker, "w", encoding="utf-8") as rf:
                rf.write(f"TASK_ID={task_instance.task_id}\n")
                rf.write(f"STATUS=RUNNING\n")
                rf.write(f"STARTED_AT={datetime.now(timezone.utc).isoformat()}\n")

            try:
                success = task_instance.run()

                if success:
                    output_target = self.output()
                    os.makedirs(os.path.dirname(output_target.path), exist_ok=True)
                    with output_target.open("w") as f:
                        f.write(f"TASK_ID={task_instance.task_id}\n")
                        f.write(f"TASK_NAME={task_instance.task_name}\n")
                        f.write(f"TASK_TYPE={task_instance.task_type}\n")
                        f.write(f"STATUS=SUCCESS\n")
                        f.write(f"COMPLETED_AT={datetime.now(timezone.utc).isoformat()}\n")
                    logger.info(f"[LuigiTask] Luigi task '{task_instance.task_id}' completed SUCCESSFULLY. Target written.")
                else:
                    # Write failure marker
                    failed_dir = os.path.join("failed_jobs", today_date)
                    os.makedirs(failed_dir, exist_ok=True)
                    failed_file = os.path.join(failed_dir, f"{task_instance.task_id}_failed.txt")
                    with open(failed_file, "w", encoding="utf-8") as ff:
                        ff.write(f"TASK_ID={task_instance.task_id}\nSTATUS=FAILED\nFAILED_AT={datetime.now(timezone.utc).isoformat()}\n")
                    raise RuntimeError(f"Framework task '{task_instance.task_id}' execution returned FAILED status.")
            finally:
                if os.path.exists(running_marker):
                    try:
                        os.remove(running_marker)
                    except Exception:
                        pass
else:
    class FrameworkLuigiTask:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("Luigi is not installed in the Python environment. Please run 'pip install luigi'.")
