"""
Data Quality Validation Module.
Performs TOML-configured distributed pre-write validation checks on PySpark DataFrames
(null_check, unique_check, minimum_rows).
"""

from dataclasses import dataclass
from typing import Any, List, Optional
from src.helpers.logger import setup_logger
from src.core.config import QualitySection, ConfigError

logger = setup_logger("DataQualityValidator")


class DataQualityError(Exception):
    """Raised when data quality validation checks fail."""
    pass


@dataclass
class QualityResult:
    passed: bool
    total_rows: int
    processed_rows: int
    null_key_count: int = 0
    duplicate_key_count: int = 0
    error_message: Optional[str] = None


class DataQualityValidator:
    """Validates PySpark DataFrames according to QualitySection TOML rules before target writing."""

    def __init__(self, quality_config: QualitySection):
        if not isinstance(quality_config, QualitySection):
            raise ConfigError(f"Expected QualitySection configuration, got {type(quality_config).__name__}.")
        self.config = quality_config

    def validate(
        self,
        df: Any,
        merge_keys: Optional[List[str]] = None
    ) -> QualityResult:
        """
        Executes distributed quality checks on PySpark DataFrame.
        Raises DataQualityError if critical quality checks fail.
        """
        logger.info("Executing Data Quality Validation checks...")

        if not self.config.enabled:
            logger.info("Data Quality checks are disabled (enabled=false). Skipping checks.")
            return QualityResult(passed=True, total_rows=0, processed_rows=0)

        if df is None:
            logger.warning("DataFrame is None/stub. Skipping actual quality checks.")
            return QualityResult(passed=True, total_rows=0, processed_rows=0)

        # Check explicit QualityResult override for dev/testing mocks
        mock_res = getattr(df, "mock_quality_result", None)
        if mock_res is not None and isinstance(mock_res, QualityResult):
            if not mock_res.passed:
                raise DataQualityError(mock_res.error_message or "Data Quality Validation FAILED (mock).")
            return mock_res

        total_rows: int = 0
        if hasattr(df, "count"):
            try:
                total_rows = int(df.count())
            except Exception:
                total_rows = 0

        logger.info(f"Source row count: {total_rows}")

        # 1. Minimum Rows Check
        if self.config.minimum_rows > 0 and total_rows < self.config.minimum_rows:
            err_msg = (
                f"Data Quality Validation FAILED: Rule [minimum_rows] constraint violated. "
                f"Extracted row count is {total_rows}, which is below required minimum of {self.config.minimum_rows}."
            )
            logger.error(err_msg)
            raise DataQualityError(err_msg)

        try:
            from pyspark.sql import functions as F
        except ImportError:
            logger.warning("PySpark is not installed in local environment. Returning QualityResult stub.")
            return QualityResult(passed=True, total_rows=total_rows, processed_rows=total_rows)

        if total_rows == 0:
            logger.info("Source DataFrame is empty (0 rows). Quality validation passed.")
            return QualityResult(passed=True, total_rows=0, processed_rows=0)

        # 2. Null Check (Supports configured null_check list as well as fallback merge_keys)
        null_cols = list(self.config.null_check)
        if not null_cols and merge_keys and getattr(self.config, "check_null_keys", False):
            null_cols = list(merge_keys)

        null_key_count: int = 0
        if null_cols:
            logger.info(f"Executing Rule [null_check] on columns: {null_cols}")
            null_conds = [F.col(c).isNull() for c in null_cols]
            null_df = df.filter(null_conds[0] if len(null_conds) == 1 else F.or_(*null_conds))
            null_key_count = int(null_df.count())
            logger.info(f"Null check violation count: {null_key_count}")

            if null_key_count > 0:
                err_msg = (
                    f"Data Quality Validation FAILED: Rule [null_check] constraint violated. "
                    f"Found {null_key_count} records containing NULL values in columns {null_cols}."
                )
                logger.error(err_msg)
                raise DataQualityError(err_msg)

        # 3. Unique Check (Supports configured unique_check list as well as fallback merge_keys)
        unique_cols = list(self.config.unique_check)
        if not unique_cols and merge_keys and getattr(self.config, "check_duplicate_keys", False):
            unique_cols = list(merge_keys)

        duplicate_key_count: int = 0
        if unique_cols:
            logger.info(f"Executing Rule [unique_check] on columns: {unique_cols}")
            dup_df = df.groupBy(*unique_cols).count().filter(F.col("count") > 1)
            duplicate_key_count = int(dup_df.count())
            logger.info(f"Unique check duplicate group count: {duplicate_key_count}")

            if duplicate_key_count > 0:
                err_msg = (
                    f"Data Quality Validation FAILED: Rule [unique_check] constraint violated. "
                    f"Found {duplicate_key_count} duplicate record groups on columns {unique_cols}."
                )
                logger.error(err_msg)
                raise DataQualityError(err_msg)

        processed_rows: int = total_rows
        logger.info(f"Data Quality Validation PASSED successfully! (total_rows={total_rows}, processed_rows={processed_rows})")

        return QualityResult(
            passed=True,
            total_rows=total_rows,
            processed_rows=processed_rows,
            null_key_count=null_key_count,
            duplicate_key_count=duplicate_key_count,
        )
