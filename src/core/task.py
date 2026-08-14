"""
Core Task Abstraction Module.
Defines BaseTask abstract base class and TableLoadTask execution unit.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from src.helpers.logger import setup_logger

logger = setup_logger("Task")


class BaseTask(ABC):
    """Abstract Base Class defining standard lifecycle and interface for all tasks."""

    def __init__(
        self,
        task_id: str,
        task_name: str,
        task_type: str = "generic",
        description: str = ""
    ):
        self.task_id = task_id
        self.task_name = task_name
        self.task_type = task_type
        self.description = description
        self.status = "IDLE"  # IDLE, RUNNING, SUCCESS, FAILED
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.error_message: Optional[str] = None

    @property
    def job_id(self) -> str:
        """Backward-compatible alias for task_id."""
        return self.task_id

    @property
    def job_name(self) -> str:
        """Backward-compatible alias for task_name."""
        return self.task_name

    @abstractmethod
    def validate(self) -> bool:
        """Validates task configuration, credentials, and prerequisites before execution."""
        pass

    @abstractmethod
    def execute(self) -> bool:
        """Executes main task logic. Returns True on success, False or raises Exception on failure."""
        pass

    def run(self) -> bool:
        """Standard task execution wrapper managing validation, lifecycle state, timing, and logging."""
        self.status = "RUNNING"
        self.start_time = datetime.now(timezone.utc)
        logger.info(f"[{self.task_type.upper()}] Starting Task '{self.task_id}' ({self.task_name})...")

        try:
            if not self.validate():
                self.status = "FAILED"
                self.error_message = "Task validation check failed"
                logger.error(f"[{self.task_type.upper()}] Task '{self.task_id}' validation check failed.")
                return False

            success = self.execute()
            self.end_time = datetime.now(timezone.utc)

            if success:
                self.status = "SUCCESS"
                logger.info(f"[{self.task_type.upper()}] Task '{self.task_id}' completed SUCCESSFULLY.")
                return True
            else:
                self.status = "FAILED"
                logger.error(f"[{self.task_type.upper()}] Task '{self.task_id}' execution returned FAILED status.")
                return False

        except Exception as err:
            self.status = "FAILED"
            self.end_time = datetime.now(timezone.utc)
            self.error_message = str(err)
            logger.error(f"[{self.task_type.upper()}] Task '{self.task_id}' FAILED with exception: {err}")
            raise err

    def get_summary(self) -> Dict[str, Any]:
        """Returns structured dictionary summary of task execution lifecycle."""
        duration_sec = 0.0
        if self.start_time and self.end_time:
            duration_sec = (self.end_time - self.start_time).total_seconds()

        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "task_type": self.task_type,
            "status": self.status,
            "duration_seconds": round(duration_sec, 2),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "error_message": self.error_message
        }


class TableLoadTask(BaseTask):
    """
    Task implementation for extracting data from a source (Oracle, SQL Server, MySQL, Postgres, SFTP)
    and loading it into an Apache Iceberg target table.
    """

    def __init__(self, config_or_path: Any):
        from src.core.config import ConfigParser, JobConfig
        if isinstance(config_or_path, str):
            self.config: JobConfig = ConfigParser.load_toml(config_or_path)
            self.config_path: Optional[str] = config_or_path
        elif isinstance(config_or_path, JobConfig):
            self.config = config_or_path
            self.config_path = getattr(config_or_path, "config_path", None)
        else:
            raise ValueError(f"Invalid config object provided to TableLoadTask: {type(config_or_path)}")

        super().__init__(
            task_id=self.config.job.job_id,
            task_name=self.config.job.job_name,
            task_type="table_load",
            description=self.config.job.description or ""
        )

    def validate(self) -> bool:
        """Validates configuration settings for source and target table loading."""
        if not self.config or not self.config.job or not self.config.job.job_id:
            return False
        return True

    def execute(self) -> bool:
        """Executes table loading pipeline using core pipeline components."""
        from src.helpers.spark import SparkSessionFactory
        from src.connectors.factory import ReaderFactory
        from src.core.transformer import DataTransformer
        from src.core.writer import IcebergWriter
        from src.core.quality import DataQualityValidator
        from src.core.hooks import PreloadHandler, PostloadHandler

        spark = SparkSessionFactory.get_session(self.config)

        # Preload Hooks
        if self.config.preload.enabled:
            preload_h = PreloadHandler(spark, self.config)
            preload_h.execute_preload_hooks()

        # Source Extraction
        reader = ReaderFactory.get_reader(spark, self.config.source, self.config.jdbc)
        df = reader.read()

        # Transformation
        transformed_df = DataTransformer.transform(df, self.config)

        # Data Quality Validation
        if self.config.data_quality.enabled:
            DataQualityValidator.validate(transformed_df, self.config)

        # Target Iceberg Write
        IcebergWriter.write(transformed_df, self.config)

        # Postload Hooks
        if self.config.postload.enabled:
            postload_h = PostloadHandler(spark, self.config)
            postload_h.execute_postload_hooks(transformed_df)

        return True
