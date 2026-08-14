"""
Dynamic Spark Resource & Partition Auto-Tuning Helper Module for CDP.
Calculates optimal PySpark executor memory, cores, shuffle partitions, and JDBC fetch sizes
based on job profile configurations ([resource] or auto-resolved workload heuristic).
"""

from typing import Dict, Any, Optional
from src.helpers.logger import setup_logger
from src.core.config import JobConfig, LoadType

logger = setup_logger("ResourceTuner")


class SparkResourceTuner:
    """Calculates auto-tuned PySpark cluster configs & JDBC parameters."""

    PROFILES = {
        "light": {
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": "10",
            "jdbc_fetch_size": 10000,
        },
        "medium": {
            "spark.executor.memory": "4g",
            "spark.executor.cores": "4",
            "spark.sql.shuffle.partitions": "100",
            "jdbc_fetch_size": 25000,
        },
        "heavy": {
            "spark.executor.memory": "8g",
            "spark.executor.cores": "4",
            "spark.memory.offHeap.enabled": "true",
            "spark.memory.offHeap.size": "2g",
            "spark.sql.shuffle.partitions": "200",
            "spark.driver.maxResultSize": "4g",
            "jdbc_fetch_size": 50000,
        },
    }

    @classmethod
    def resolve_profile(cls, config: JobConfig) -> str:
        """
        Resolves workload profile ('light', 'medium', 'heavy') from explicit [resource] profile
        or infers profile heuristically based on load type and JDBC partitions.
        """
        if config.resource and config.resource.profile:
            p = config.resource.profile.lower().strip()
            if p in cls.PROFILES:
                logger.info(f"Using explicitly configured resource profile: '{p}'")
                return p

        load_type = config.load.type
        partitions = config.jdbc.num_partitions if config.jdbc else 1

        if partitions >= 8 or (load_type in (LoadType.INCREMENTAL, LoadType.UPSERT) and partitions >= 4):
            resolved = "heavy"
        elif load_type == LoadType.FULL or partitions > 1:
            resolved = "medium"
        else:
            resolved = "light"

        logger.info(f"Auto-resolved workload profile: '{resolved}' (load_type='{load_type.value}', num_partitions={partitions})")
        return resolved

    @classmethod
    def get_spark_options(cls, config: JobConfig) -> Dict[str, str]:
        """Calculates PySpark spark.executor/shuffle config dictionary for the given job."""
        profile_name = cls.resolve_profile(config)
        base_opts = dict(cls.PROFILES.get(profile_name, cls.PROFILES["medium"]))
        base_opts.pop("jdbc_fetch_size", None)

        if config.resource and config.resource.custom_spark_options:
            for k, v in config.resource.custom_spark_options.items():
                base_opts[k] = str(v)

        logger.info(f"Generated Spark cluster options for profile '{profile_name}': {base_opts}")
        return base_opts

    @classmethod
    def get_tuned_fetch_size(cls, config: JobConfig) -> int:
        """Calculates optimal JDBC fetch size based on resource profile or explicitly defined fetch_size."""
        if config.jdbc and config.jdbc.fetch_size != 10000:
            return config.jdbc.fetch_size

        profile_name = cls.resolve_profile(config)
        fetch_size = cls.PROFILES.get(profile_name, {}).get("jdbc_fetch_size", 10000)
        logger.info(f"Tuned JDBC fetch size for profile '{profile_name}': {fetch_size}")
        return fetch_size
