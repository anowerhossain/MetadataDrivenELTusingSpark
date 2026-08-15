"""
Main Driver CLI Entry Point for Metadata-Driven PySpark ETL Framework (TOML Configured) on CDP.

Usage:
  # Single Table Execution:
  spark-submit main.py --config config/tasks/customer.toml
  spark-submit main.py --config config/tasks/customer.toml --validate

  # Concurrent Multi-Table Batch Execution:
  spark-submit main.py --config-dir config/tasks/ --parallel 4
  spark-submit main.py --config-dir config/tasks/ --parallel 4 --validate
"""

import argparse
import glob
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List

from src.helpers.logger import setup_logger, ETLLogger
from src.core.config import ConfigParser, JobConfig, LoadType, ConfigError, TransformSection
from src.helpers.spark import get_cdp_spark_session
from src.core.hooks import PreloadHandler, PostloadHandler
from src.core.transformer import DataTransformer
from src.core.writer import IcebergWriter
from src.core.state import WatermarkManager
from src.core.quality import DataQualityValidator
from src.helpers.retry import RetryHandler
from src.connectors.factory import ConnectionResolver, ReaderFactory
from src.helpers.failure import FailureHandler
from src.helpers.success import SuccessHandler
from src.helpers.email_notification import EmailNotification

logger = setup_logger("MainDriver")


def validate_config_file(config_path: str) -> JobConfig:
    """Loads TOML configuration and validates database connection credentials before Spark initialization."""
    logger.info(f"Validating pipeline configuration file: {config_path}")
    try:
        config: JobConfig = ConfigParser.load_toml(config_path)
    except Exception as err:
        logger.error(f"Configuration file validation FAILED for '{config_path}': {err}")
        raise ConfigError(f"Configuration validation failed: {err}") from err

    # Validate database connection environment variables without logging passwords
    try:
        conn = ConnectionResolver.resolve(config.source)
        logger.info(f"Connection validation SUCCESS: resolved '{conn.connection_name}' (username='{conn.username}')")
    except Exception as err:
        logger.error(f"Connection credential validation FAILED for section [source]: {err}")
        raise ConfigError(f"Connection validation failed: {err}") from err

    logger.info(
        f"Configuration VALIDATED: job_id='{config.job.job_id}', name='{config.job.job_name}', "
        f"enabled={config.job.enabled}, load_type='{config.load.type.value}'"
    )
    return config


def run_pipeline(config_path: str, validate_only: bool = False, force: bool = False) -> int:
    """
    Executes an ETL job pipeline lifecycle based on TOML configuration file.

    :param config_path: Path to TOML configuration file.
    :param validate_only: If True, validates configuration and DB connection without executing Spark ETL.
    :param force: If True, forces execution even if job already completed successfully today.
    :return: Exit code (0 for success/skip, 1 for error).
    """
    logger.info("============================================================")
    logger.info(f"ETL Job Pipeline Execution Lifecycle - Config: {config_path}")
    logger.info("============================================================")

    # 0. Check if job already completed successfully today (unless force=True or validate_only=True)
    today_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    if not validate_only and not force and SuccessHandler.is_job_succeeded_today(config_path, today_date):
        logger.info(
            f"Job configuration '{config_path}' ALREADY COMPLETED SUCCESSFULLY TODAY ({today_date}). "
            f"Skipping execution (Pass --force to override)."
        )
        return 0

    # 1. Validate TOML configuration before starting Spark
    try:
        config: JobConfig = validate_config_file(config_path)
    except Exception as err:
        logger.error(f"Aborting execution due to configuration failure: {err}")
        if not validate_only:
            FailureHandler.record_failure(config_path, "CONFIG_VALIDATION_FAILURE", err)
        return 1

    full_source = f"{config.source.schema}.{config.source.table}" if config.source.schema else config.source.table
    full_target = f"{config.target.catalog}.{config.target.database}.{config.target.table}" if config.target.catalog else f"{config.target.database}.{config.target.table}"

    # Handle dry-run validation mode (--validate)
    if validate_only:
        print("\n============================================================")
        print("CONFIGURATION VALIDATION SUMMARY")
        print("============================================================")
        print(f"Job ID       : {config.job.job_id}")
        print(f"Job Name     : {config.job.job_name}")
        print(f"Enabled      : {config.job.enabled}")
        print(f"Load Type    : {config.load.type.value}")
        print(f"Source Table : {full_source}")
        print(f"Target Table : {full_target}")
        print(f"Status       : VALID (Dry-run check completed successfully)")
        print("============================================================\n")
        logger.info(f"Dry-run validation completed successfully for '{config_path}'. Exiting (code 0).")
        return 0

    # Check if job is enabled
    if not config.job.enabled:
        logger.warning(f"Job '{config.job.job_id}' is marked disabled (enabled=false). Skipping execution.")
        return 0

    # Initialize structured telemetry logger with unique run_id
    etl_logger = ETLLogger(
        job_id=config.job.job_id,
        job_name=config.job.job_name,
        source=full_source,
        target=full_target,
        load_type=config.load.type.value,
    )

    retry_handler = RetryHandler(config.retry)

    def execute_etl_work() -> None:
        # Check load.type support (FULL, INCREMENTAL, UPSERT supported)
        if config.load.type not in (LoadType.FULL, LoadType.INCREMENTAL, LoadType.UPSERT):
            raise NotImplementedError(
                f"Unsupported load.type '{config.load.type.value}' for job '{config.job.job_id}'."
            )

        # 2. Initialize CDP SparkSession via factory
        logger.info(f"[{etl_logger.run_id}] Initializing CDP SparkSession...")
        spark = get_cdp_spark_session(config=config)
        logger.info(f"[{etl_logger.run_id}] CDP SparkSession initialized successfully.")

        # 2b. Execute TOML-driven Preload Hooks BEFORE extraction
        if config.preload and config.preload.enabled:
            logger.info(f"[{etl_logger.run_id}] Executing Preload Hooks...")
            preload_handler = PreloadHandler(spark, config)
            preload_handler.execute_preload_hooks()

        # 3. Retrieve previous watermark state for INCREMENTAL/UPSERT jobs
        watermark_mgr = WatermarkManager(spark)
        last_watermark = None

        if config.load.type in (LoadType.INCREMENTAL, LoadType.UPSERT) and config.load.watermark_column:
            last_watermark = watermark_mgr.get_last_watermark(config.job.job_id)
            if not last_watermark:
                last_watermark = config.load.options.get("initial_watermark", "1970-01-01 00:00:00")

            logger.info(
                f"[{etl_logger.run_id}] Executing INCREMENTAL extraction using watermark column "
                f"'{config.load.watermark_column}' with previous watermark='{last_watermark}'"
            )

        # 4. Extract data from source table via ReaderFactory
        logger.info(f"[{etl_logger.run_id}] Extracting data from {config.source.type.value.upper()} source table ({full_source})...")
        reader = ReaderFactory.get_reader(spark, config.source, jdbc_config=config.jdbc)
        df = reader.read(load_config=config.load, last_watermark=last_watermark)

        rows_read: int = 0
        if df is not None:
            try:
                rows_read = df.count()
            except Exception:
                rows_read = 0
        etl_logger.record_rows_read(rows_read)

        # 4b. Apply TOML-driven Data Transformations (Exclude -> Rename -> Cast -> Derived -> DWH Audit)
        transform_sec = config.transform if config.transform else TransformSection()
        transformer = DataTransformer(transform_sec)
        source_excl = config.source.extraction.exclude_columns if config.source.extraction else None
        df = transformer.transform(
            df,
            source_exclude=source_excl,
            audit_config=config.audit_columns,
            job_id=config.job.job_id,
            source_connection=config.source.connection,
            run_id=etl_logger.run_id
        )

        # 5. Execute Data Quality Validation BEFORE writing to target Iceberg table
        logger.info(f"[{etl_logger.run_id}] Running Data Quality Validation checks...")
        validator = DataQualityValidator(config.quality)
        quality_res = validator.validate(df, merge_keys=config.load.merge_keys)

        # 6. Write data to Iceberg table via IcebergWriter with Schema Evolution
        logger.info(f"[{etl_logger.run_id}] Writing data to Iceberg target table ({full_target})...")
        writer = IcebergWriter(spark, config.target, schema_config=config.schema_config)

        if config.load.merge_keys:
            logger.info(f"[{etl_logger.run_id}] Performing Iceberg MERGE INTO on keys: {config.load.merge_keys}")
            writer.merge(df, merge_keys=config.load.merge_keys)
        elif config.load.type == LoadType.FULL:
            logger.info(f"[{etl_logger.run_id}] Performing Iceberg FULL overwrite")
            writer.write(df, mode="overwrite")
        else:
            logger.info(f"[{etl_logger.run_id}] Performing Iceberg INCREMENTAL append")
            writer.write(df, mode="append")

        rows_written: int = quality_res.processed_rows if quality_res.processed_rows else rows_read
        etl_logger.record_rows_written(rows_written)

        # 7. Execute Postload Hooks ONLY AFTER target Iceberg write succeeds
        if config.postload and config.postload.enabled:
            logger.info(f"[{etl_logger.run_id}] Executing Postload Hooks...")
            postload_handler = PostloadHandler(spark, config)
            postload_handler.execute_postload_hooks(df)
        elif config.load.type in (LoadType.INCREMENTAL, LoadType.UPSERT) and config.load.watermark_column:
            new_watermark = watermark_mgr.get_max_watermark_from_df(df, config.load.watermark_column)
            if not new_watermark:
                new_watermark = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            watermark_mgr.update_watermark(config.job.job_id, new_watermark)

        # 8. Complete run telemetry logging upon success
        etl_logger.complete_success(rows_read=rows_read, rows_written=rows_written)

    try:
        retry_handler.execute(execute_etl_work, task_name=f"ETL_Job_{config.job.job_id}")
        today_date = datetime.now(timezone.utc).strftime("%Y%m%d")
        FailureHandler.clear_job_failure_markers(today_date, config_path)
        SuccessHandler.record_success(
            config_path=config_path,
            job_id=config.job.job_id,
            run_id=etl_logger.run_id,
            rows_read=getattr(etl_logger, "rows_read", 0),
            rows_written=getattr(etl_logger, "rows_written", 0),
            duration_seconds=etl_logger.get_duration_seconds() if hasattr(etl_logger, "get_duration_seconds") else 0.0
        )
        return 0
    except Exception as err:
        etl_logger.complete_failure(err)
        FailureHandler.record_failure(config_path, config.job.job_id, err)
        EmailNotification.send_error(
            job_id=config.job.job_id,
            error=err,
            config=config.email_notification,
            run_id=etl_logger.run_id if 'etl_logger' in locals() else None,
            config_path=config_path,
        )
        return 1


def run_batch_pipeline(config_dir: str, max_workers: int = 4, validate_only: bool = False, force: bool = False) -> int:
    """
    Discovers all TOML job configurations in config_dir and executes them concurrently using a thread pool.

    :param config_dir: Directory containing TOML job config files.
    :param max_workers: Maximum number of concurrent thread workers.
    :param validate_only: If True, performs dry-run validation on all job configs concurrently.
    :param force: If True, forces execution of all jobs even if they succeeded today.
    :return: Exit code (0 if all jobs succeed/skip, 1 if any job fails).
    """
    logger.info("============================================================")
    logger.info(f"CONCURRENT MULTI-TABLE BATCH RUNNER - Directory: {config_dir} (parallel={max_workers}, force={force})")
    logger.info("============================================================")

    search_pattern = os.path.join(config_dir, "*.toml")
    config_files = sorted(glob.glob(search_pattern))

    if not config_files:
        logger.error(f"No TOML configuration files found matching pattern '{search_pattern}'.")
        return 1

    logger.info(f"Discovered {len(config_files)} TOML job configuration(s) for batch execution.")

    results: Dict[str, int] = {}
    all_success = True

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_pipeline, config_path=cfg, validate_only=validate_only, force=force): cfg
            for cfg in config_files
        }

        for future in as_completed(futures):
            cfg_path = futures[future]
            try:
                code = future.result()
                results[cfg_path] = code
                if code != 0:
                    all_success = False
            except Exception as err:
                logger.error(f"Unhandled exception during batch execution of '{cfg_path}': {err}")
                results[cfg_path] = 1
                all_success = False

    print("\n====================================================================================")
    print("CONCURRENT MULTI-TABLE BATCH EXECUTION SUMMARY")
    print("====================================================================================")
    print(f"{'Job Config File':<50} | {'Status':<15} | {'Exit Code':<10}")
    print("-" * 82)

    for cfg_path in config_files:
        code = results.get(cfg_path, 1)
        status_str = "SUCCESS/SKIP" if code == 0 else "FAILED"
        filename = os.path.basename(cfg_path)
        print(f"{filename:<50} | {status_str:<15} | {code:<10}")

    print("====================================================================================")
    total = len(config_files)
    succ = sum(1 for c in results.values() if c == 0)
    fail = sum(1 for c in results.values() if c != 0)
    print(f"Total Jobs: {total} | Succeeded/Skipped: {succ} | Failed: {fail}")
    print("====================================================================================\n")

    return 0 if all_success else 1


def run_rerun_failed_pipeline(date_str: str, max_workers: int = 4, validate_only: bool = False) -> int:
    """
    Scans failed_jobs/date_str/, deduplicates unique failed TOML configs, and reruns them concurrently.
    Clears failure markers upon successful rerun execution.

    :param date_str: Date string in YYYYMMDD format.
    :param max_workers: Maximum parallel thread workers.
    :param validate_only: If True, performs dry-run validation on failed jobs.
    :return: Exit code (0 if all rerun jobs succeed, 1 if any job fails).
    """
    logger.info("============================================================")
    logger.info(f"FAILED JOBS RERUN RECOVERY RUNNER - Date: {date_str} (parallel={max_workers})")
    logger.info("============================================================")

    failed_jobs = FailureHandler.get_failed_jobs(date_str)
    if not failed_jobs:
        logger.info(f"No failed jobs found for date '{date_str}'. Nothing to rerun.")
        return 0

    config_files = list(failed_jobs.keys())
    logger.info(
        f"Deduplicated {len(failed_jobs)} unique failed job(s) for rerun recovery: {config_files}"
    )

    results: Dict[str, int] = {}
    all_success = True

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_pipeline, config_path=cfg, validate_only=validate_only, force=True): cfg
            for cfg in config_files
        }

        for future in as_completed(futures):
            cfg_path = futures[future]
            try:
                code = future.result()
                results[cfg_path] = code
                if code == 0:
                    FailureHandler.clear_job_failure_markers(date_str, cfg_path)
                else:
                    all_success = False
            except Exception as err:
                logger.error(f"Unhandled exception during rerun recovery of '{cfg_path}': {err}")
                results[cfg_path] = 1
                all_success = False

    print("\n====================================================================================")
    print("FAILED JOBS RERUN RECOVERY EXECUTION SUMMARY")
    print("====================================================================================")
    print(f"{'Job Config File':<50} | {'Status':<15} | {'Exit Code':<10}")
    print("-" * 82)

    for cfg_path in config_files:
        code = results.get(cfg_path, 1)
        status_str = "RECOVERED" if code == 0 else "FAILED AGAIN"
        filename = os.path.basename(cfg_path)
        print(f"{filename:<50} | {status_str:<15} | {code:<10}")

    print("====================================================================================")
    total = len(config_files)
    succ = sum(1 for c in results.values() if c == 0)
    fail = sum(1 for c in results.values() if c != 0)
    print(f"Total Jobs Rerun: {total} | Recovered: {succ} | Remaining Failed: {fail}")
    print("====================================================================================\n")

    return 0 if all_success else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Production PySpark Metadata-Driven Multi-Source Iceberg ETL Runner for CDP",
        prog="spark-submit main.py"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--config", help="Path to single pipeline TOML configuration file")
    group.add_argument("--config-dir", help="Directory containing multiple TOML job configuration files for concurrent execution")
    group.add_argument("--rerun-failed", help="Date in YYYYMMDD format to rerun failed jobs from failed_jobs/YYYYMMDD/")
    parser.add_argument("--parallel", type=int, default=4, help="Maximum number of parallel workers for batch or rerun mode (default: 4)")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate TOML configuration and connection credentials without executing Spark ETL job"
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force job execution even if job already succeeded today"
    )
    parser.add_argument(
        "--use-luigi",
        action="store_true",
        help="Use Spotify Luigi orchestration engine to execute metadata-driven DAG task workflows"
    )
    args = parser.parse_args()

    if args.use_luigi:
        from src.helpers.luigi_runner import LuigiRunner
        dir_to_use = args.config_dir if args.config_dir else "config/tasks"
        runner = LuigiRunner(config_dir=dir_to_use)

        target_ids = None
        if args.config:
            try:
                cfg = ConfigParser.load_toml(args.config)
                target_ids = [cfg.job.task_id]
            except Exception as err:
                logger.error(f"Error reading config '{args.config}': {err}")
                sys.exit(1)

        success = runner.run_pipeline_dag(target_task_ids=target_ids, workers=args.parallel, local_scheduler=True)
        exit_code = 0 if success else 1
    elif args.config:
        exit_code = run_pipeline(config_path=args.config, validate_only=args.validate, force=args.force)
    elif args.rerun_failed:
        exit_code = run_rerun_failed_pipeline(date_str=args.rerun_failed, max_workers=args.parallel, validate_only=args.validate)
    else:
        exit_code = run_batch_pipeline(config_dir=args.config_dir, max_workers=args.parallel, validate_only=args.validate, force=args.force)

    sys.exit(exit_code)
