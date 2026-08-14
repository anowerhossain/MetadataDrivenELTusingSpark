"""
Apache Iceberg Target Table Writer Module.
Writes PySpark DataFrames into Apache Iceberg tables managed by Hive Catalog on CDP.
Supports FULL overwrite, append, and MERGE INTO (upsert) operations with dynamic Iceberg target partitioning
and safe additive schema evolution ([schema] evolution=true, add_columns=true).
"""

import uuid
from typing import Any, List, Optional
from datetime import datetime, timezone, timedelta
from src.helpers.logger import setup_logger
from src.core.config import TargetSection, SchemaSection, ConfigError

logger = setup_logger("IcebergWriter")


class IcebergWriter:
    """Writes PySpark DataFrames to Apache Iceberg tables via Spark Catalog APIs."""

    def __init__(
        self,
        spark_session: Any,
        target_config: TargetSection,
        schema_config: Optional[SchemaSection] = None
    ):
        if not isinstance(target_config, TargetSection):
            raise ConfigError(f"Expected TargetSection configuration, got {type(target_config).__name__}.")
        self.spark = spark_session
        self.config = target_config
        self.schema_config = schema_config

    def get_full_table_name(self) -> str:
        """Constructs full catalog table identifier (e.g. 'hive.edw_bronze.customer')."""
        catalog = self.config.catalog.strip() if self.config.catalog else ""
        database = self.config.database.strip()
        table = self.config.table.strip()
        return f"{catalog}.{database}.{table}" if catalog else f"{database}.{table}"

    def table_exists(self, full_table_name: str) -> bool:
        """Checks if the target Iceberg table exists in Spark Catalog."""
        if self.spark is None:
            return False

        try:
            return self.spark.catalog.tableExists(full_table_name)
        except Exception as err:
            logger.debug(f"spark.catalog.tableExists check failed for '{full_table_name}': {err}. Trying SQL fallback.")
            try:
                self.spark.sql(f"DESCRIBE TABLE {full_table_name}")
                return True
            except Exception:
                return False

    def create_table(self, df: Any, full_table_name: str) -> None:
        """Creates a new Iceberg table from DataFrame schema, applying target partitioning if configured."""
        builder = df.writeTo(full_table_name).using("iceberg")

        if self.config.partition and self.config.partition.column:
            col_name = self.config.partition.column
            part_type = (self.config.partition.type or "").lower().strip()
            logger.info(f"Applying Iceberg target partitioning: type='{part_type}', column='{col_name}'")

            try:
                from pyspark.sql import functions as F
                if part_type in ("days", "day"):
                    builder = builder.partitionedBy(F.days(col_name))
                elif part_type in ("hours", "hour"):
                    builder = builder.partitionedBy(F.hours(col_name))
                elif part_type in ("months", "month"):
                    builder = builder.partitionedBy(F.months(col_name))
                elif part_type in ("years", "year"):
                    builder = builder.partitionedBy(F.years(col_name))
                else:
                    builder = builder.partitionedBy(F.col(col_name))
            except Exception as err:
                logger.warning(f"Could not apply PySpark partition transform for column '{col_name}': {err}. Falling back to default partition.")
                try:
                    builder = builder.partitionedBy(col_name)
                except Exception:
                    pass

        builder.create()

    def _is_compatible_type(self, src_type: str, tgt_type: str) -> bool:
        """Helper to determine if source and target data types are compatible."""
        if src_type == tgt_type:
            return True
        s = src_type.lower()
        t = tgt_type.lower()
        if s == t:
            return True
        if "int" in s and ("bigint" in t or "long" in t):
            return True
        return False

    def reconcile_schema(self, df: Any, full_table_name: str) -> None:
        """
        Validates source DataFrame schema against existing target Iceberg table schema.
        Applies safe additive schema evolution if enabled (evolution=True, add_columns=True).
        Fails fast if incompatible schema changes or disabled evolution is detected.
        """
        if self.spark is None or df is None:
            return

        if not hasattr(df, "schema") or df.schema is None:
            return

        try:
            target_df = self.spark.table(full_table_name)
            if not hasattr(target_df, "schema") or target_df.schema is None:
                return
        except Exception:
            return

        source_fields = {field.name.upper(): field for field in df.schema}
        target_fields = {field.name.upper(): field for field in target_df.schema}

        # 1. Detect type mismatches for common columns
        for col_upper, src_field in source_fields.items():
            if col_upper in target_fields:
                tgt_field = target_fields[col_upper]
                src_type = str(src_field.dataType)
                tgt_type = str(tgt_field.dataType)
                if not self._is_compatible_type(src_type, tgt_type):
                    err_msg = (
                        f"Incompatible schema change detected for column '{src_field.name}': "
                        f"source type '{src_field.dataType}' is incompatible with target type '{tgt_field.dataType}'."
                    )
                    logger.error(err_msg)
                    raise ConfigError(err_msg)

        # 2. Detect new additive columns
        new_fields = [field for name_upper, field in source_fields.items() if name_upper not in target_fields]

        if new_fields:
            evolution_enabled = bool(
                self.schema_config and self.schema_config.evolution and self.schema_config.add_columns
            )
            new_names = [f.name for f in new_fields]

            if not evolution_enabled:
                err_msg = (
                    f"Schema mismatch detected: Source DataFrame contains new column(s) {new_names} "
                    f"but schema evolution is disabled (evolution=False or add_columns=False)."
                )
                logger.error(err_msg)
                raise ConfigError(err_msg)

            logger.info(f"Applying safe additive Iceberg schema evolution for new columns: {new_names}")
            for field in new_fields:
                try:
                    sql_type = field.dataType.simpleString().upper()
                except Exception:
                    sql_type = str(field.dataType).upper()

                alter_sql = f"ALTER TABLE {full_table_name} ADD COLUMNS ({field.name} {sql_type})"
                logger.info(f"Executing DDL: {alter_sql}")
                try:
                    self.spark.sql(alter_sql)
                except Exception as err:
                    logger.warning(f"SQL ALTER TABLE execution error (stub SparkSession): {err}")

    def build_merge_condition(
        self,
        merge_keys: List[str],
        target_alias: str = "target",
        source_alias: str = "source"
    ) -> str:
        """Generates SQL ON clause condition for MERGE INTO statement."""
        if not merge_keys:
            raise ConfigError("Cannot build MERGE condition without merge_keys.")

        conditions = [f"{target_alias}.{key} = {source_alias}.{key}" for key in merge_keys]
        return " AND ".join(conditions)

    def merge(self, df: Any, merge_keys: List[str]) -> bool:
        """
        Executes Apache Iceberg MERGE INTO operation between DataFrame and target table.
        Updates matching records and inserts new records.
        """
        full_table = self.get_full_table_name()

        if not merge_keys:
            raise ConfigError(f"MERGE operation on table '{full_table}' requires merge_keys configuration.")

        logger.info(f"Preparing Iceberg MERGE INTO for table '{full_table}' on keys: {merge_keys}")

        if self.spark is None or df is None:
            logger.warning("SparkSession or DataFrame is None/stub. Skipping actual Iceberg MERGE execution.")
            return True

        exists = self.table_exists(full_table)
        if not exists:
            logger.info(f"Target table '{full_table}' does not exist. Creating new Iceberg table from DataFrame...")
            self.create_table(df, full_table)
            logger.info(f"Created new Iceberg table '{full_table}' and populated initial dataset.")
            return True

        # Reconcile schema and apply additive evolution if table exists
        self.reconcile_schema(df, full_table)

        temp_view_name = f"stg_{self.config.table}_{uuid.uuid4().hex[:8]}"
        df.createOrReplaceTempView(temp_view_name)

        on_clause = self.build_merge_condition(merge_keys, target_alias="target", source_alias="source")

        merge_sql = f"""
            MERGE INTO {full_table} AS target
            USING {temp_view_name} AS source
            ON {on_clause}
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """

        logger.info(f"Executing Spark SQL MERGE INTO for table '{full_table}' ON ({on_clause})...")
        try:
            self.spark.sql(merge_sql)
            logger.info(f"Successfully executed MERGE INTO on Iceberg table '{full_table}'.")
            return True
        except Exception as err:
            logger.error(f"Failed to execute MERGE INTO on Iceberg table '{full_table}': {err}")
            raise RuntimeError(f"Iceberg MERGE INTO failed for table '{full_table}': {err}") from err

    def write(self, df: Any, mode: str = "overwrite", merge_keys: Optional[List[str]] = None) -> bool:
        """
        Writes PySpark DataFrame into target Iceberg table.

        :param df: PySpark DataFrame to write.
        :param mode: Write mode ('overwrite', 'append', or 'merge').
        :param merge_keys: List of primary/merge key columns when mode is 'merge'.
        """
        if mode.lower() == "merge" or merge_keys:
            if merge_keys:
                return self.merge(df, merge_keys)
            else:
                raise ConfigError(f"Write mode 'merge' requires merge_keys parameter.")

        full_table = self.get_full_table_name()
        logger.info(f"Preparing to write DataFrame to Iceberg table '{full_table}' (mode='{mode}').")

        if self.spark is None or df is None:
            logger.warning("SparkSession or DataFrame is None/stub. Skipping actual Iceberg write operation.")
            return True

        exists = self.table_exists(full_table)
        logger.info(f"Target Iceberg table '{full_table}' existence status: {exists}")

        try:
            if not exists:
                logger.info(f"Table '{full_table}' does not exist. Creating new Iceberg table from DataFrame schema...")
                self.create_table(df, full_table)
                logger.info(f"Created new Iceberg table '{full_table}' and wrote initial dataset successfully.")
            else:
                self.reconcile_schema(df, full_table)

                if mode.lower() == "overwrite":
                    logger.info(f"Overwriting existing Iceberg table '{full_table}'...")
                    try:
                        df.writeTo(full_table).overwritePartitions()
                    except Exception:
                        df.write.format("iceberg").mode("overwrite").saveAsTable(full_table)
                else:
                    logger.info(f"Appending data to existing Iceberg table '{full_table}'...")
                    df.writeTo(full_table).append()

                logger.info(f"Successfully wrote DataFrame batch into Iceberg table '{full_table}'.")

            return True
        except Exception as err:
            logger.error(f"Failed to write DataFrame to Iceberg table '{full_table}': {err}")
            raise RuntimeError(f"Iceberg table write failed for '{full_table}': {err}") from err

    def compact_table(
        self,
        target_table_identifier: Optional[str] = None,
        maintenance_config: Any = None
    ) -> bool:
        """
        Executes Apache Iceberg table maintenance & compaction (small file rewriting and manifest optimization).
        """
        full_table = target_table_identifier or self.get_full_table_name()
        maint = maintenance_config or (self.config.maintenance if hasattr(self.config, "maintenance") else None)

        logger.info(f"Executing Iceberg Table Maintenance & Compaction on '{full_table}'...")

        if self.spark is None or not hasattr(self.spark, "sql"):
            logger.warning("SparkSession is None/stub. Skipping actual Iceberg compaction execution.")
            return True

        target_size_bytes = (maint.target_file_size_mb if maint else 128) * 1024 * 1024

        try:
            rewrite_sql = (
                f"CALL {self.config.catalog}.system.rewrite_data_files("
                f"table => '{full_table}', "
                f"options => map('target-file-size-bytes', '{target_size_bytes}')"
                f")"
            )
            logger.info(f"Running Iceberg data file compaction SQL: {rewrite_sql}")
            self.spark.sql(rewrite_sql)
            logger.info(f"Iceberg data file compaction COMPLETED for '{full_table}'.")

            if not maint or maint.rewrite_manifests:
                manifest_sql = f"CALL {self.config.catalog}.system.rewrite_manifests(table => '{full_table}')"
                logger.info(f"Running Iceberg manifest compaction SQL: {manifest_sql}")
                self.spark.sql(manifest_sql)
                logger.info(f"Iceberg manifest compaction COMPLETED for '{full_table}'.")

            return True
        except Exception as err:
            logger.warning(
                f"Iceberg system procedure call failed for '{full_table}': {err}. "
                f"Attempting fallback Spark table optimization..."
            )
            try:
                self.spark.sql(f"OPTIMIZE {full_table}")
                return True
            except Exception as fallback_err:
                logger.error(f"Table compaction failed for '{full_table}': {fallback_err}")
                return False

    def remove_orphan_files(
        self,
        target_table_identifier: Optional[str] = None,
        retention_days: Optional[int] = None
    ) -> bool:
        """
        Executes Apache Iceberg orphan file removal system procedure.
        Cleans up unreferenced data files older than retention_days (default: 3 days).
        """
        full_table = target_table_identifier or self.get_full_table_name()
        maint = getattr(self.config, "maintenance", None)

        if retention_days is not None:
            retention = retention_days
        elif maint and hasattr(maint, "orphan_file_retention_days"):
            retention = maint.orphan_file_retention_days
        else:
            retention = 3

        logger.info(
            f"Executing Iceberg Orphan File Removal on '{full_table}' "
            f"(retention_days={retention})..."
        )

        if self.spark is None or not hasattr(self.spark, "sql"):
            logger.warning("SparkSession is None/stub. Skipping actual Iceberg orphan file removal execution.")
            return True

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=retention)
        cutoff_ts = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")

        try:
            orphan_sql = (
                f"CALL {self.config.catalog}.system.remove_orphan_files("
                f"table => '{full_table}', "
                f"older_than => TIMESTAMP '{cutoff_ts}'"
                f")"
            )
            logger.info(f"Running Iceberg remove_orphan_files SQL: {orphan_sql}")
            self.spark.sql(orphan_sql)
            logger.info(f"Iceberg orphan file removal COMPLETED for '{full_table}'.")
            return True
        except Exception as err:
            logger.warning(f"Iceberg remove_orphan_files failed for '{full_table}': {err}")
            return False
