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
from src.core.job_pipeline import JobPipelineParser, JobPipelineConfig
from src.core.luigi_task import FrameworkLuigiTask, create_framework_task_instance
from src.helpers.logger import setup_logger

logger = setup_logger("LuigiRunner")


class LuigiRunner:
    """
    Manager class for discovering TOML task and composite job configurations,
    constructing task dependency graphs, and invoking Luigi orchestration builds.
    """

    def __init__(self, config_dir: str = "config/tasks", jobs_dir: str = "config/jobs", job_file: Optional[str] = None, **kwargs):
        self.config_dir = config_dir
        self.jobs_dir = jobs_dir
        self.task_map: Dict[str, str] = {}  # task_id -> file_path
        self.config_map: Dict[str, JobConfig] = {}  # task_id -> JobConfig
        self.active_job: Optional[JobPipelineConfig] = None

        if job_file and os.path.exists(job_file):
            self.load_job_pipeline(job_file)
        else:
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

    def load_job_pipeline(self, job_filepath: str):
        """Loads a composite Job pipeline configuration from config/jobs/ and builds task mapping for that specific job."""
        try:
            # First discover all task files in config_dir
            all_discovered: Dict[str, str] = {}
            if os.path.exists(self.config_dir):
                for fp in glob.glob(os.path.join(self.config_dir, "*.toml")):
                    try:
                        cfg_tmp = ConfigParser.load_toml(fp)
                        all_discovered[cfg_tmp.job.task_id] = fp
                    except Exception:
                        pass

            job_cfg = JobPipelineParser.load_job_toml(job_filepath)
            self.active_job = job_cfg
            self.task_map = {}
            self.config_map = {}
            logger.info(f"[LuigiRunner] Loaded composite Job Pipeline '{job_cfg.job_id}' ({job_cfg.job_name}) with {len(job_cfg.tasks)} tasks.")

            # Load ONLY tasks defined in this Job pipeline mapping
            for t_map in job_cfg.tasks:
                target_file = None
                # Resolution candidate 1: direct task_file path
                if t_map.task_file and os.path.exists(t_map.task_file):
                    target_file = t_map.task_file
                # Resolution candidate 2: task_file relative to config_dir
                elif t_map.task_file and os.path.exists(os.path.join(self.config_dir, os.path.basename(t_map.task_file))):
                    target_file = os.path.join(self.config_dir, os.path.basename(t_map.task_file))
                # Resolution candidate 3: task_id.toml in config_dir
                elif os.path.exists(os.path.join(self.config_dir, f"{t_map.task_id}.toml")):
                    target_file = os.path.join(self.config_dir, f"{t_map.task_id}.toml")
                # Resolution candidate 4: discovered task map
                elif t_map.task_id in all_discovered:
                    target_file = all_discovered[t_map.task_id]

                if target_file and os.path.exists(target_file):
                    self.task_map[t_map.task_id] = target_file
                    cfg = ConfigParser.load_toml(target_file)

                    # Override task depends_on from Job definition if explicitly defined in Job TOML
                    if t_map.depends_on:
                        cfg_dict = dict(cfg.raw_config)
                        task_sec = dict(cfg_dict.get("task", {}) or cfg_dict.get("job", {}))
                        task_sec["depends_on"] = t_map.depends_on
                        cfg_dict["task"] = task_sec
                        cfg = JobConfig.from_dict(cfg_dict)

                    self.config_map[t_map.task_id] = cfg
                    logger.debug(f"[LuigiRunner] Successfully mapped job task '{t_map.task_id}' -> {target_file}")
                else:
                    logger.warning(f"[LuigiRunner] Task file for task_id '{t_map.task_id}' could not be resolved from path '{t_map.task_file}'.")
        except Exception as err:
            logger.error(f"[LuigiRunner] Failed to load Job pipeline '{job_filepath}': {err}")
            raise err

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
