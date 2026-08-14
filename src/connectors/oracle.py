"""
Oracle Connection and JDBC Data Reader Module.
Resolves credentials from environment variables and reads data from Oracle into PySpark DataFrames.
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from src.helpers.logger import setup_logger
from src.core.config import SourceSection, LoadSection, LoadType, JDBCSection, ConfigError
from src.helpers.tuner import SparkResourceTuner

logger = setup_logger("OracleConnector")


@dataclass
class OracleConnectionConfig:
    """Holds validated Oracle connection properties safely without exposing secrets in logs."""
    connection_name: str
    jdbc_url: str
    username: str
    password: str = field(repr=False)
    driver: str = "oracle.jdbc.OracleDriver"
    fetch_size: int = 10000

    def __repr__(self) -> str:
        return (
            f"OracleConnectionConfig(connection_name='{self.connection_name}', "
            f"jdbc_url='{self.jdbc_url}', username='{self.username}', "
            f"password='***', driver='{self.driver}')"
        )

    def __str__(self) -> str:
        return self.__repr__()

    def to_jdbc_options(self) -> Dict[str, str]:
        """Returns a dictionary of JDBC options for PySpark DataFrameReader."""
        return {
            "url": self.jdbc_url,
            "user": self.username,
            "password": self.password,
            "driver": self.driver,
            "fetchSize": str(self.fetch_size),
        }


class OracleConnectionResolver:
    """Resolves connection details from environment variables for a named connection."""

    @classmethod
    def resolve(cls, source_config: SourceSection) -> OracleConnectionConfig:
        """Resolves Oracle connection credentials for the given SourceSection."""
        connection_name = source_config.connection
        if not connection_name:
            raise ConfigError("Source section is missing 'connection' name.")

        prefix = connection_name.strip().upper().replace("-", "_")

        # 1. Resolve JDBC URL
        jdbc_url = os.environ.get(f"{prefix}_JDBC_URL")
        if not jdbc_url:
            host = os.environ.get(f"{prefix}_HOST")
            port = os.environ.get(f"{prefix}_PORT", "1521")
            service_name = os.environ.get(f"{prefix}_SERVICE_NAME") or os.environ.get(f"{prefix}_SID")
            if host and service_name:
                jdbc_url = f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"

        if not jdbc_url or not jdbc_url.strip():
            raise ConfigError(
                f"Oracle connection '{connection_name}' error: Missing environment variable '{prefix}_JDBC_URL' "
                f"(or '{prefix}_HOST' and '{prefix}_SERVICE_NAME')."
            )

        # 2. Resolve Username
        username = os.environ.get(f"{prefix}_USERNAME") or os.environ.get(f"{prefix}_USER")
        if not username or not username.strip():
            raise ConfigError(
                f"Oracle connection '{connection_name}' error: Missing environment variable '{prefix}_USERNAME' "
                f"(or '{prefix}_USER')."
            )

        # 3. Resolve Password
        password = os.environ.get(f"{prefix}_PASSWORD")
        if not password or not password.strip():
            raise ConfigError(
                f"Oracle connection '{connection_name}' error: Missing environment variable '{prefix}_PASSWORD'."
            )

        # 4. Resolve Driver & Fetch Size
        driver = os.environ.get(f"{prefix}_DRIVER", "oracle.jdbc.OracleDriver")
        
        # Safe fetch size check
        raw_fetch = 10000
        if hasattr(source_config, "jdbc") and source_config.jdbc:
            raw_fetch = source_config.jdbc.fetch_size
        try:
            fetch_size = int(raw_fetch)
        except (ValueError, TypeError):
            fetch_size = 10000

        return OracleConnectionConfig(
            connection_name=connection_name,
            jdbc_url=jdbc_url.strip(),
            username=username.strip(),
            password=password.strip(),
            driver=driver.strip(),
            fetch_size=fetch_size,
        )


class OracleReader:
    """Reads data from Oracle tables into PySpark DataFrames via JDBC."""

    def __init__(
        self,
        spark_session: Any,
        source_config: SourceSection,
        jdbc_config: Optional[JDBCSection] = None
    ):
        if not isinstance(source_config, SourceSection):
            raise ConfigError(f"Expected SourceSection configuration, got {type(source_config).__name__}.")
        self.spark = spark_session
        self.config = source_config
        self.jdbc_config = jdbc_config or source_config.jdbc

    def get_full_table_name(self) -> str:
        """Formats full table name (e.g. 'BANK.CUSTOMER' or 'CUSTOMER')."""
        schema_str = self.config.schema.strip() if self.config.schema else ""
        table_str = self.config.table.strip()
        return f"{schema_str}.{table_str}" if schema_str else table_str

    def get_columns_projection(self) -> str:
        """Constructs column selection string for SQL query."""
        if self.config.extraction and self.config.extraction.columns:
            cols = [str(col).strip() for col in self.config.extraction.columns if str(col).strip()]
            if cols:
                return ", ".join(cols)
        return "*"

    def get_connection_config(self) -> OracleConnectionConfig:
        """Resolves secure connection credentials."""
        return OracleConnectionResolver.resolve(self.config)

    def build_incremental_query(
        self,
        full_table: str,
        watermark_column: str,
        last_watermark: Optional[Any] = None
    ) -> str:
        """Generates subquery string for incremental extraction."""
        cols = self.get_columns_projection()
        if last_watermark is not None:
            val_str = f"'{last_watermark}'" if isinstance(last_watermark, str) else str(last_watermark)
            sql_str = f"SELECT {cols} FROM {full_table} WHERE {watermark_column} > {val_str}"
        else:
            sql_str = f"SELECT {cols} FROM {full_table}"

        return f"({sql_str}) AS incremental_src"

    def read(
        self,
        load_config: Optional[LoadSection] = None,
        last_watermark: Optional[Any] = None
    ) -> Any:
        """Executes extraction from Oracle database table based on load configuration."""
        full_table = self.get_full_table_name()
        conn_config = self.get_connection_config()

        load_type = load_config.type if load_config else LoadType.FULL
        cols = self.get_columns_projection()

        if load_type == LoadType.INCREMENTAL:
            if not load_config or not load_config.watermark_column:
                raise ConfigError("Incremental load requires 'watermark_column' in load configuration.")

            watermark_col = load_config.watermark_column
            dbtable_target = self.build_incremental_query(full_table, watermark_col, last_watermark)
            logger.info(
                f"Initializing Oracle JDBC INCREMENTAL extraction for table '{full_table}' "
                f"on column '{watermark_col}' (last_watermark={last_watermark}) using connection '{conn_config.connection_name}'."
            )
        else:
            if cols != "*":
                dbtable_target = f"(SELECT {cols} FROM {full_table}) AS full_src"
            else:
                dbtable_target = full_table

            logger.info(
                f"Initializing Oracle JDBC FULL extraction for table '{full_table}' "
                f"using connection '{conn_config.connection_name}'."
            )

        if self.spark is None:
            logger.warning("SparkSession is None/stub. Skipping actual JDBC execution and returning None.")
            return None

        jdbc_options = conn_config.to_jdbc_options()
        jdbc_options["dbtable"] = dbtable_target

        effective_jdbc = self.jdbc_config or self.config.jdbc

        tuned_fetch = SparkResourceTuner.get_tuned_fetch_size(self.config) if hasattr(self, 'config') and hasattr(self.config, 'load') else (effective_jdbc.fetch_size if effective_jdbc and effective_jdbc.fetch_size else 10000)
        jdbc_options["fetchSize"] = str(tuned_fetch)

        if effective_jdbc and effective_jdbc.partition_column:
            logger.info(
                f"Configuring parallel JDBC extraction: partition_column='{effective_jdbc.partition_column}', "
                f"num_partitions={effective_jdbc.num_partitions}"
            )
            jdbc_options["partitionColumn"] = effective_jdbc.partition_column
            jdbc_options["numPartitions"] = str(effective_jdbc.num_partitions)
            if effective_jdbc.lower_bound is not None:
                jdbc_options["lowerBound"] = str(effective_jdbc.lower_bound)
            if effective_jdbc.upper_bound is not None:
                jdbc_options["upperBound"] = str(effective_jdbc.upper_bound)
        else:
            logger.info("Executing standard single-partition JDBC extraction (no JDBC partitioning configured).")

        logger.info(f"Executing Spark JDBC load for target: {dbtable_target}...")
        try:
            df = self.spark.read.format("jdbc").options(**jdbc_options).load()
            logger.info(f"Oracle JDBC load completed successfully for table '{full_table}'.")
            return df
        except Exception as err:
            logger.error(f"Failed to extract data from Oracle table '{full_table}': {err}")
            raise RuntimeError(f"Oracle JDBC extraction failed for table '{full_table}': {err}") from err
