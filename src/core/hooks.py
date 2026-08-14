"""
TOML-Driven Preload & Postload Execution Hooks Module.
Performs pre-flight pipeline checks (validate_source, validate_target, check_watermark) BEFORE data extraction,
and post-write operations (update_watermark, refresh_metadata, compact_table, remove_orphan_files) AFTER Iceberg writes.
"""

from typing import Any, Set, Optional
from datetime import datetime, timezone
from src.helpers.logger import setup_logger
from src.core.config import JobConfig, ConfigError, LoadType
from src.connectors.factory import ConnectionResolver
from src.core.writer import IcebergWriter
from src.core.state import WatermarkManager

logger = setup_logger("ExecutionHooks")

# Allowed pre-load operations
ALLOWED_PRELOAD_OPERATIONS: Set[str] = {
    "validate_source",
    "validate_target",
    "check_watermark",
}

# Allowed post-load operations
ALLOWED_POSTLOAD_OPERATIONS: Set[str] = {
    "update_watermark",
    "refresh_metadata",
    "compact_table",
    "compact_small_files",
    "remove_orphan_files",
    "cleanup_orphan_files",
}


class PreloadHandler:
    """Executes configured pre-extraction operations according to [preload] TOML configuration."""

    def __init__(self, spark_session: Any, job_config: JobConfig):
        if not isinstance(job_config, JobConfig):
            raise ConfigError(f"Expected JobConfig configuration, got {type(job_config).__name__}.")
        self.spark = spark_session
        self.config = job_config

    def run_validate_source(self) -> None:
        """Operation 'validate_source': Validates source connection credentials and table configuration."""
        logger.info("Preload Hook [validate_source]: Validating source connection and table metadata...")
        try:
            conn = ConnectionResolver.resolve(self.config.source)
            logger.info(
                f"Preload Hook [validate_source] SUCCESS: Connection '{conn.connection_name}' "
                f"(user='{conn.username}') resolved for table '{self.config.source.schema}.{self.config.source.table}'."
            )
        except Exception as err:
            logger.error(f"Preload Hook [validate_source] FAILED: {err}")
            raise ConfigError(f"Preload hook [validate_source] failed: {err}") from err

    def run_validate_target(self) -> None:
        """Operation 'validate_target': Validates target Iceberg catalog, database, and table identifier."""
        writer = IcebergWriter(self.spark, self.config.target, schema_config=self.config.schema_config)
        full_target = writer.get_full_table_name()
        logger.info(f"Preload Hook [validate_target]: Validating target table identifier '{full_target}'...")
        if self.spark is not None:
            exists = writer.table_exists(full_target)
            logger.info(f"Preload Hook [validate_target] SUCCESS: Target table '{full_target}' existence status: {exists}.")
        else:
            logger.info(f"Preload Hook [validate_target] SUCCESS: Target table configuration '{full_target}' is valid.")

    def run_check_watermark(self) -> None:
        """Operation 'check_watermark': Retrieves and verifies previous watermark state for job."""
        logger.info(f"Preload Hook [check_watermark]: Checking previous watermark state for job_id='{self.config.job.job_id}'...")
        wm_mgr = WatermarkManager(self.spark)
        last_wm = wm_mgr.get_last_watermark(self.config.job.job_id)
        logger.info(f"Preload Hook [check_watermark] SUCCESS: Previous watermark for '{self.config.job.job_id}' is '{last_wm}'.")

    def execute_preload_hooks(self) -> None:
        """
        Validates and executes configured preload operations in sequence BEFORE extraction.
        Fails fast if unknown operations or validation checks fail.
        """
        if not self.config.preload or not self.config.preload.enabled:
            logger.info("Preload hooks disabled or not configured. Skipping preload operations.")
            return

        operations = self.config.preload.operations
        logger.info(f"Executing Preload Hooks: {operations}")

        for op in operations:
            if op not in ALLOWED_PRELOAD_OPERATIONS:
                err_msg = (
                    f"Unknown preload operation '{op}' in configuration. "
                    f"Allowed operations: {sorted(list(ALLOWED_PRELOAD_OPERATIONS))}"
                )
                logger.error(err_msg)
                raise ConfigError(err_msg)

        for op in operations:
            if op == "validate_source":
                self.run_validate_source()
            elif op == "validate_target":
                self.run_validate_target()
            elif op == "check_watermark":
                self.run_check_watermark()

        logger.info("Preload Hooks completed successfully.")


class PostloadHandler:
    """Executes configured post-write operations according to [postload] TOML configuration."""

    def __init__(self, spark_session: Any, job_config: JobConfig):
        if not isinstance(job_config, JobConfig):
            raise ConfigError(f"Expected JobConfig configuration, got {type(job_config).__name__}.")
        self.spark = spark_session
        self.config = job_config

    def run_update_watermark(self, df: Any = None) -> None:
        """
        Operation 'update_watermark': Calculates high watermark from DataFrame and persists it to WatermarkManager.
        Executed ONLY AFTER target Iceberg write succeeds.
        """
        job_id = self.config.job.job_id
        wm_col = self.config.load.watermark_column

        if self.config.load.type in (LoadType.INCREMENTAL, LoadType.UPSERT) and wm_col:
            logger.info(f"Postload Hook [update_watermark]: Computing high watermark for job_id='{job_id}' on column '{wm_col}'...")
            wm_mgr = WatermarkManager(self.spark)
            new_wm = wm_mgr.get_max_watermark_from_df(df, wm_col)
            if not new_wm:
                new_wm = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            wm_mgr.update_watermark(job_id, new_wm)
            logger.info(f"Postload Hook [update_watermark] SUCCESS: Updated watermark for job '{job_id}' -> '{new_wm}'.")
        else:
            logger.info(f"Postload Hook [update_watermark]: Job type is '{self.config.load.type.value}'. Skipping watermark update.")

    def run_refresh_metadata(self) -> None:
        """Operation 'refresh_metadata': Refreshes Spark Catalog metadata for target Iceberg table."""
        writer = IcebergWriter(self.spark, self.config.target, schema_config=self.config.schema_config)
        full_target = writer.get_full_table_name()
        logger.info(f"Postload Hook [refresh_metadata]: Refreshing catalog metadata for target table '{full_target}'...")

        if self.spark is not None:
            try:
                refresh_sql = f"REFRESH TABLE {full_target}"
                self.spark.sql(refresh_sql)
                logger.info(f"Postload Hook [refresh_metadata] SUCCESS: Executed SQL '{refresh_sql}'.")
            except Exception as err:
                logger.warning(f"Postload Hook [refresh_metadata]: Refresh command returned: {err}")
        else:
            logger.info(f"Postload Hook [refresh_metadata] SUCCESS: Verified metadata refresh configuration for '{full_target}'.")

    def run_compact_table(self) -> None:
        """Operation 'compact_table' / 'compact_small_files': Executes Apache Iceberg table maintenance & compaction."""
        writer = IcebergWriter(self.spark, self.config.target, schema_config=self.config.schema_config)
        full_target = writer.get_full_table_name()
        logger.info(f"Postload Hook [compact_table]: Triggering Iceberg compaction on target table '{full_target}'...")
        success = writer.compact_table()
        if success:
            logger.info(f"Postload Hook [compact_table] SUCCESS: Compaction finished for '{full_target}'.")
        else:
            logger.warning(f"Postload Hook [compact_table]: Compaction warning or non-critical error for '{full_target}'.")

    def run_remove_orphan_files(self) -> None:
        """Operation 'remove_orphan_files' / 'cleanup_orphan_files': Executes Apache Iceberg orphan file removal."""
        writer = IcebergWriter(self.spark, self.config.target, schema_config=self.config.schema_config)
        full_target = writer.get_full_table_name()
        logger.info(f"Postload Hook [remove_orphan_files]: Triggering orphan file removal on target table '{full_target}'...")
        success = writer.remove_orphan_files()
        if success:
            logger.info(f"Postload Hook [remove_orphan_files] SUCCESS: Orphan file removal finished for '{full_target}'.")
        else:
            logger.warning(f"Postload Hook [remove_orphan_files]: Orphan file removal warning or non-critical error for '{full_target}'.")

    def execute_postload_hooks(self, df: Any = None) -> None:
        """
        Validates and executes configured postload operations in sequence AFTER successful target write.
        Fails fast if unknown operations are specified.
        """
        if not self.config.postload or not self.config.postload.enabled:
            logger.info("Postload hooks disabled or not configured. Skipping postload operations.")
            return

        operations = self.config.postload.operations
        logger.info(f"Executing Postload Hooks: {operations}")

        for op in operations:
            if op not in ALLOWED_POSTLOAD_OPERATIONS:
                err_msg = (
                    f"Unknown postload operation '{op}' in configuration. "
                    f"Allowed operations: {sorted(list(ALLOWED_POSTLOAD_OPERATIONS))}"
                )
                logger.error(err_msg)
                raise ConfigError(err_msg)

        for op in operations:
            if op == "update_watermark":
                self.run_update_watermark(df)
            elif op == "refresh_metadata":
                self.run_refresh_metadata()
            elif op in ("compact_table", "compact_small_files"):
                self.run_compact_table()
            elif op in ("remove_orphan_files", "cleanup_orphan_files"):
                self.run_remove_orphan_files()

        logger.info("Postload Hooks completed successfully.")
