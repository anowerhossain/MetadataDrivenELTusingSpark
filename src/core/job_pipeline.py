"""
Job Pipeline Configuration Module.
Defines higher-level Job workflow schemas (JobPipelineConfig, JobTaskMapping)
that compose multiple tasks from config/tasks/ into orchestrated pipeline jobs in config/jobs/.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from src.core.config import ConfigParser, ConfigError

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib  # type: ignore
    except ImportError:
        tomllib = None  # type: ignore


@dataclass(frozen=True)
class JobTaskMapping:
    """Represents a single task reference within a higher-level Job pipeline."""
    task_id: str
    task_file: str
    depends_on: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobTaskMapping":
        if not isinstance(data, dict):
            raise ConfigError("Job task mapping entry must be a dictionary.")

        task_id = str(data.get("task_id", "")).strip()
        if not task_id:
            raise ConfigError("Missing required field 'task_id' in [[job.tasks]] entry.")

        task_file = str(data.get("task_file") or data.get("config", "")).strip()

        raw_deps = data.get("depends_on", [])
        if isinstance(raw_deps, str):
            depends_on = [d.strip() for d in raw_deps.split(",") if d.strip()]
        elif isinstance(raw_deps, list):
            depends_on = [str(d).strip() for d in raw_deps if str(d).strip()]
        else:
            depends_on = []

        return cls(
            task_id=task_id,
            task_file=task_file,
            depends_on=depends_on
        )


@dataclass(frozen=True)
class JobPipelineConfig:
    """Represents an enterprise Job Pipeline composing multiple task configurations."""
    job_id: str
    job_name: str
    enabled: bool
    description: str = ""
    tasks: List[JobTaskMapping] = field(default_factory=list)
    raw_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobPipelineConfig":
        if not isinstance(data, dict):
            raise ConfigError("TOML configuration root must be a dictionary.")

        job_raw = data.get("job") or data.get("pipeline")
        if not job_raw or not isinstance(job_raw, dict):
            raise ConfigError("Missing required section '[job]' or '[pipeline]' in TOML configuration.")

        job_id = str(job_raw.get("job_id") or job_raw.get("pipeline_id", "")).strip()
        if not job_id:
            raise ConfigError("Missing required field 'job_id' in section '[job]'.")

        job_name = str(job_raw.get("job_name") or job_raw.get("pipeline_name", "")).strip()
        if not job_name:
            raise ConfigError("Missing required field 'job_name' in section '[job]'.")

        enabled = bool(job_raw.get("enabled", True))
        description = str(job_raw.get("description", "")).strip()

        raw_tasks = job_raw.get("tasks", [])
        parsed_tasks: List[JobTaskMapping] = []
        if isinstance(raw_tasks, list):
            for tentry in raw_tasks:
                if isinstance(tentry, dict):
                    parsed_tasks.append(JobTaskMapping.from_dict(tentry))

        return cls(
            job_id=job_id,
            job_name=job_name,
            enabled=enabled,
            description=description,
            tasks=parsed_tasks,
            raw_config=data
        )


class JobPipelineParser:
    """Parses and validates higher-level Job workflow TOML files from config/jobs/."""

    @classmethod
    def load_job_toml(cls, filepath: str) -> JobPipelineConfig:
        """Loads and parses a Job pipeline TOML configuration file."""
        if not os.path.exists(filepath):
            raise ConfigError(f"Job configuration file not found at: '{filepath}'")

        if tomllib is None:
            raise ConfigError("No TOML parser available (install 'tomli' for Python < 3.11).")

        with open(filepath, "rb") as f:
            data = tomllib.load(f)

        return JobPipelineConfig.from_dict(data)
