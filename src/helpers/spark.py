"""
SparkSession Factory Helper for Cloudera CDP Platform with Apache Iceberg & Hive Metastore integration.
"""

from typing import Any, Dict, Optional
from src.helpers.logger import setup_logger
from src.core.config import JobConfig
from src.helpers.tuner import SparkResourceTuner

logger = setup_logger("SparkSessionFactory")


class SparkSessionFactory:
    """Reusable factory to build and manage PySpark SparkSessions on CDP."""

    @classmethod
    def get_session(
        cls,
        config: Optional[JobConfig] = None,
        app_name: Optional[str] = None,
        extra_configs: Optional[Dict[str, str]] = None,
        enable_hive_support: bool = True
    ) -> Any:
        """Creates or retrieves an existing SparkSession configured for CDP."""
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            logger.warning("PySpark is not installed in the current environment. Returning SparkSession stub.")
            return None

        final_app_name = app_name
        if not final_app_name and config:
            final_app_name = f"ETL_{config.job.job_id}"
        if not final_app_name:
            final_app_name = "PySpark_Iceberg_ETL"

        catalog_name = config.target.catalog if config else "spark_catalog"

        builder = SparkSession.builder.appName(final_app_name)

        default_configs = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog_name}.type": "hive",
        }

        for k, v in default_configs.items():
            builder = builder.config(k, v)

        if config:
            tuned_configs = SparkResourceTuner.get_spark_options(config)
            logger.info(f"Applying {len(tuned_configs)} auto-tuned Spark configurations for job '{config.job.job_id}'...")
            for k, v in tuned_configs.items():
                builder = builder.config(k, v)

        if extra_configs:
            logger.info(f"Applying {len(extra_configs)} extra runtime Spark configurations...")
            for k, v in extra_configs.items():
                builder = builder.config(k, v)

        if enable_hive_support:
            try:
                builder = builder.enableHiveSupport()
            except Exception as err:
                logger.warning(f"Could not enable Hive support on builder: {err}")

        spark = builder.getOrCreate()
        spark_app_name = getattr(getattr(spark, "sparkContext", None), "appName", final_app_name)
        spark_ver = getattr(spark, "version", "unknown")
        logger.info(
            f"SparkSession active: app_name='{spark_app_name}', "
            f"catalog='{catalog_name}', spark_version='{spark_ver}'"
        )
        return spark


def get_cdp_spark_session(
    config: Optional[JobConfig] = None,
    app_name: Optional[str] = None,
    extra_configs: Optional[Dict[str, str]] = None
) -> Any:
    """Convenience helper function to retrieve CDP SparkSession."""
    return SparkSessionFactory.get_session(
        config=config,
        app_name=app_name,
        extra_configs=extra_configs
    )
