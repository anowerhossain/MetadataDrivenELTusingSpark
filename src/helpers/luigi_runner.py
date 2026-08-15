"""
Luigi Workflow Runner Helper Module.
Loads TOML task pipeline configurations from config/tasks/, builds dependency DAG,
generates Mermaid.js / ASCII graph representations, and executes via luigi.build().
"""

import os
import glob
import logging
from typing import Dict, List, Any, Optional

try:
    import luigi
    LUIGI_AVAILABLE = True
except ImportError:
    LUIGI_AVAILABLE = False
    luigi = None

from src.core.config import ConfigParser, JobConfig
from src.core.luigi_task import FrameworkLuigiTask, create_framework_task_instance
from src.helpers.logger import setup_logger

logger = setup_logger("LuigiRunner")


class LuigiRunner:
    """
    Manager class for discovering TOML task configurations, constructing task dependency graphs,
    and invoking Luigi orchestration builds.
    """

    def __init__(self, config_dir: str = "config/tasks"):
        self.config_dir = config_dir
        self.task_map: Dict[str, str] = {}  # task_id -> file_path
        self.config_map: Dict[str, JobConfig] = {}  # task_id -> JobConfig
        self._discover_tasks()

    def _discover_tasks(self):
        """Discovers all .toml task configurations in config_dir and builds task_id map."""
        if not os.path.exists(self.config_dir):
            logger.warning(f"[LuigiRunner] Task configuration directory '{self.config_dir}' does not exist.")
            return

        toml_files = sorted(glob.glob(os.path.join(self.config_dir, "*.toml")))
        for filepath in toml_files:
            try:
                config = ConfigParser.load_toml(filepath)
                task_id = config.job.task_id
                if config.job.enabled:
                    self.task_map[task_id] = filepath
                    self.config_map[task_id] = config
                    logger.debug(f"[LuigiRunner] Discovered active task '{task_id}' at {filepath}")
                else:
                    logger.debug(f"[LuigiRunner] Skipping disabled task '{task_id}' at {filepath}")
            except Exception as err:
                logger.warning(f"[LuigiRunner] Could not parse task file '{filepath}': {err}")

    def build_mermaid_dag(self) -> str:
        """Generates Mermaid.js syntax flowchart for visual DAG rendering in Web UI."""
        lines = ["graph TD"]
        if not self.config_map:
            lines.append("    EmptyCatalog[\"No Active Tasks Discovered\"]")
            return "\n".join(lines)

        for task_id, config in self.config_map.items():
            task_name = config.job.task_name
            task_type = str(config.raw_config.get("task", {}).get("type", "table_load")).upper()
            label = f"{task_id}[\"{task_name} ({task_type})\"]"
            lines.append(f"    {label}")

            deps = config.job.depends_on
            for dep in deps:
                if dep in self.config_map:
                    lines.append(f"    {dep} --> {task_id}")
                else:
                    lines.append(f"    {dep}_Missing[\"{dep} (Missing)\"] --> {task_id}")

        return "\n".join(lines)

    def generate_ascii_dag_summary(self) -> str:
        """Generates clean ASCII text representation of task dependency tree."""
        lines = ["============================================================", "LUIGI TASK DEPENDENCY GRAPH (DAG)", "============================================================"]
        if not self.config_map:
            lines.append("No active tasks found.")
            return "\n".join(lines)

        for task_id, config in self.config_map.items():
            deps = config.job.depends_on
            dep_str = ", ".join(deps) if deps else "None (Root Task)"
            lines.append(f"• Task ID: '{task_id}' ({config.job.task_name})")
            lines.append(f"  ├── File: {self.task_map.get(task_id)}")
            lines.append(f"  └── Depends On: {dep_str}")

        lines.append("============================================================")
        return "\n".join(lines)

    def run_pipeline_dag(
        self,
        target_task_ids: Optional[List[str]] = None,
        workers: int = 4,
        local_scheduler: bool = True
    ) -> bool:
        """
        Builds and executes Luigi DAG workflow via luigi.build().

        :param target_task_ids: Optional list of target task_ids to execute. If None, executes all leaf/discovered tasks.
        :param workers: Number of concurrent Luigi worker threads.
        :param local_scheduler: If True, uses local scheduler; if False, connects to central luigid daemon.
        :return: True if all tasks succeeded, False otherwise.
        """
        if not LUIGI_AVAILABLE:
            logger.error("[LuigiRunner] Luigi package is not installed. Please run 'pip install luigi'.")
            return False

        if not self.task_map:
            logger.warning("[LuigiRunner] No active tasks discovered to execute.")
            return True

        logger.info(self.generate_ascii_dag_summary())

        # Identify target tasks to build
        if target_task_ids:
            selected_ids = [tid for tid in target_task_ids if tid in self.task_map]
        else:
            selected_ids = list(self.task_map.keys())

        luigi_tasks = []
        for tid in selected_ids:
            filepath = self.task_map[tid]
            luigi_tasks.append(
                FrameworkLuigiTask(
                    config_path=filepath,
                    all_task_map=self.task_map
                )
            )

        logger.info(f"[LuigiRunner] Invoking luigi.build() for {len(luigi_tasks)} tasks (workers={workers}, local_scheduler={local_scheduler})...")

        result = luigi.build(
            luigi_tasks,
            workers=workers,
            local_scheduler=local_scheduler,
            detailed_summary=True
        )

        success = getattr(result, "scheduling_succeeded", True)
        if success:
            logger.info("[LuigiRunner] Luigi DAG pipeline execution completed SUCCESSFULLY.")
        else:
            logger.error("[LuigiRunner] Luigi DAG pipeline execution FAILED.")

        return success
