"""
TOML-Driven Data Transformation Module.
Applies schema column renaming, data type casting, and derived column calculations using PySpark DataFrame APIs.
Transformation Pipeline: Source DataFrame -> Exclude -> Rename -> Cast -> Derived Columns -> DWH Audit Columns -> Output DataFrame.
"""

import os
import getpass
from typing import Any, Dict, Optional, List
from src.helpers.logger import setup_logger
from src.core.config import TransformSection, ConfigError

logger = setup_logger("DataTransformer")


class DataTransformer:
    """Applies TOML-configured data transformations (exclude, rename, cast, derived, audit) to PySpark DataFrames."""

    def __init__(self, transform_config: TransformSection):
        if not isinstance(transform_config, TransformSection):
            raise ConfigError(f"Expected TransformSection configuration, got {type(transform_config).__name__}.")
        self.config = transform_config

    def apply_exclusions(
        self,
        df: Any,
        source_exclude: Optional[List[str]] = None,
        v_lst_source_exclude: Optional[List[str]] = None
    ) -> Any:
        """Step 0: Drops columns specified in [transform.exclude] or [source.extraction.exclude_columns]."""
        effective_exclude = source_exclude if source_exclude is not None else v_lst_source_exclude
        all_exclude = list(getattr(self.config, "exclude", None) or [])
        if effective_exclude:
            all_exclude.extend(effective_exclude)

        if not all_exclude or df is None:
            return df

        existing_cols = getattr(df, "columns", [])
        if not existing_cols:
            return df

        to_drop = set()
        for ex in all_exclude:
            ex_clean = str(ex).strip()
            for col_name in existing_cols:
                if col_name == ex_clean or col_name.upper() == ex_clean.upper():
                    to_drop.add(col_name)

        if to_drop:
            logger.info(f"Step 0: Dropping excluded column(s): {list(to_drop)}")
            try:
                df = df.drop(*list(to_drop))
            except Exception as err:
                logger.warning(f"Failed to drop column(s) {to_drop}: {err}")

        return df

    def apply_renames(self, df: Any, renamed_map: Dict[str, str]) -> Any:
        """Step 1: Renames DataFrame columns according to [transform.rename] mapping."""
        if not self.config.rename:
            return df

        logger.info(f"Step 1: Applying column renames: {self.config.rename}")
        existing_cols = getattr(df, "columns", [])

        for old_col, new_col in self.config.rename.items():
            target_old = None
            if existing_cols:
                for c in existing_cols:
                    if c == old_col or c.upper() == old_col.upper():
                        target_old = c
                        break

            if target_old:
                df = df.withColumnRenamed(target_old, new_col)
                renamed_map[old_col] = new_col
                renamed_map[target_old] = new_col
                logger.info(f"Renamed column '{target_old}' -> '{new_col}'")
            else:
                logger.warning(f"Column '{old_col}' not found in DataFrame columns {existing_cols}. Skipping rename.")

        return df

    def apply_casts(self, df: Any, renamed_map: Dict[str, str]) -> Any:
        """Step 2: Casts DataFrame column data types according to [transform.cast] mapping."""
        if not self.config.cast:
            return df

        logger.info(f"Step 2: Applying column type casts: {self.config.cast}")
        current_cols = getattr(df, "columns", [])

        try:
            from pyspark.sql import functions as F
            from pyspark.context import SparkContext
            if SparkContext._active_spark_context is None:
                logger.warning("Active SparkContext not found. Skipping actual cast execution.")
                return df
        except (ImportError, Exception):
            logger.warning("PySpark is not installed or SparkContext is unavailable. Skipping actual cast execution.")
            return df

        for col_name, target_type in self.config.cast.items():
            actual_col = renamed_map.get(col_name, col_name)

            matched_col = None
            if current_cols:
                for c in current_cols:
                    if c == actual_col or c.upper() == actual_col.upper():
                        matched_col = c
                        break

            if matched_col:
                try:
                    df = df.withColumn(matched_col, F.col(matched_col).cast(target_type))
                    logger.info(f"Casted column '{matched_col}' -> {target_type}")
                except (Exception, AssertionError) as err:
                    logger.warning(f"Could not cast column '{matched_col}' -> {target_type}: {err}")
            else:
                logger.warning(f"Column '{col_name}' (resolved as '{actual_col}') not found in DataFrame for casting. Skipping.")

        return df

    def apply_derived(self, df: Any) -> Any:
        """Step 3: Calculates and appends derived columns according to [transform.derived] expressions."""
        if not self.config.derived:
            return df

        logger.info(f"Step 3: Calculating derived columns: {self.config.derived}")

        try:
            from pyspark.sql import functions as F
            from pyspark.context import SparkContext
            if SparkContext._active_spark_context is None:
                logger.warning("Active SparkContext not found. Skipping actual derived column execution.")
                return df
        except (ImportError, Exception):
            logger.warning("PySpark is not installed or SparkContext is unavailable. Skipping actual derived column execution.")
            return df

        for new_col, expr_str in self.config.derived.items():
            try:
                df = df.withColumn(new_col, F.expr(expr_str))
                logger.info(f"Created derived column '{new_col}' = F.expr('{expr_str}')")
            except (Exception, AssertionError) as err:
                logger.warning(f"Could not calculate derived column '{new_col}' = F.expr('{expr_str}'): {err}")

        return df

    def apply_audit_columns(
        self,
        df: Any,
        audit_config: Optional[Any] = None,
        job_id: Optional[str] = None,
        source_connection: Optional[str] = None,
        run_id: Optional[str] = None,
        job_user: Optional[str] = None,
        v_obj_audit_config: Optional[Any] = None
    ) -> Any:
        """Step 4: Appends standard Data Warehouse (DWH) audit metadata columns to output DataFrame."""
        effective_audit_config = audit_config if audit_config is not None else v_obj_audit_config
        if effective_audit_config is not None and hasattr(effective_audit_config, "enabled") and not effective_audit_config.enabled:
            return df

        try:
            from pyspark.sql import functions as F
            from pyspark.context import SparkContext
            if SparkContext._active_spark_context is None:
                logger.warning("Active SparkContext not found. Skipping DWH audit columns execution.")
                return df
        except (ImportError, Exception):
            logger.warning("PySpark is not installed or SparkContext is unavailable. Skipping DWH audit columns execution.")
            return df

        ins_ts_col = getattr(audit_config, "insert_ts_column", "dwh_insert_ts") if audit_config else "dwh_insert_ts"
        upd_ts_col = getattr(audit_config, "updated_ts_column", "dwh_updated_ts") if audit_config else "dwh_updated_ts"
        run_id_col = getattr(audit_config, "run_id_column", getattr(audit_config, "job_id_column", "dwh_etl_run_id")) if audit_config else "dwh_etl_run_id"
        job_usr_col = getattr(audit_config, "job_user_column", getattr(audit_config, "source_system_column", "dwh_job_user")) if audit_config else "dwh_job_user"
        tz = getattr(audit_config, "timezone", "Asia/Dhaka") if audit_config else "Asia/Dhaka"

        val_run_id = run_id or job_id or "ETL_RUN_UNKNOWN"
        try:
            default_user = getpass.getuser()
        except Exception:
            default_user = "cdp_etl_user"
        val_job_usr = job_user or os.environ.get("USER", os.environ.get("USERNAME", default_user))

        logger.info(
            f"Step 4: Injecting DWH Audit Columns ({tz} BST): [{ins_ts_col}, {upd_ts_col}, "
            f"{run_id_col}='{val_run_id}', {job_usr_col}='{val_job_usr}']"
        )

        try:
            df = df.withColumn(ins_ts_col, F.from_utc_timestamp(F.current_timestamp(), tz))
            df = df.withColumn(upd_ts_col, F.from_utc_timestamp(F.current_timestamp(), tz))
            df = df.withColumn(run_id_col, F.lit(val_run_id))
            df = df.withColumn(job_usr_col, F.lit(val_job_usr))
        except (Exception, AssertionError) as err:
            logger.warning(f"Could not inject DWH audit columns: {err}")

        return df

    def transform(
        self,
        df: Any,
        source_exclude: Optional[List[str]] = None,
        audit_config: Optional[Any] = None,
        job_id: Optional[str] = None,
        source_connection: Optional[str] = None,
        run_id: Optional[str] = None,
        job_user: Optional[str] = None
    ) -> Any:
        """
        Executes full transformation pipeline in required order:
        Source DataFrame -> Column Exclusions -> Rename -> Cast -> Derived Columns -> DWH Audit Columns -> Output DataFrame
        """
        if df is None:
            logger.warning("DataFrame is None/stub. Skipping transformation pipeline.")
            return None

        logger.info("Executing TOML-driven Data Transformation Pipeline...")
        renamed_map: Dict[str, str] = {}

        # 0. Exclude Columns
        df = self.apply_exclusions(df, source_exclude)

        # 1. Rename
        df = self.apply_renames(df, renamed_map)

        # 2. Cast
        df = self.apply_casts(df, renamed_map)

        # 3. Derived Columns
        df = self.apply_derived(df)

        # 4. DWH Audit Columns
        df = self.apply_audit_columns(
            df,
            audit_config=audit_config,
            job_id=job_id,
            source_connection=source_connection,
            run_id=run_id,
            job_user=job_user
        )

        logger.info("TOML-driven Data Transformation Pipeline completed successfully.")
        return df
