import unittest
from unittest.mock import MagicMock

from src.core.config import (
    JobConfig, JobSection, SourceSection, TargetSection, LoadSection,
    LoadType, SourceType, TargetType, JDBCSection, ResourceSection
)
from src.helpers.tuner import SparkResourceTuner


class TestSparkResourceTuner(unittest.TestCase):

    def _create_config(self, load_type=LoadType.FULL, num_partitions=1, profile="auto"):
        jdbc_sec = JDBCSection(partition_column="id", num_partitions=num_partitions) if num_partitions > 1 else None
        return JobConfig(
            job=JobSection(job_id="test_job", job_name="Test Job", enabled=True),
            source=SourceSection(
                type=SourceType.ORACLE,
                connection="oracle_prod",
                schema="BANK",
                table="CUSTOMER",
                jdbc=jdbc_sec
            ),
            target=TargetSection(type=TargetType.ICEBERG, catalog="hive", database="edw", table="cust"),
            load=LoadSection(type=load_type),
            resources=ResourceSection(profile=profile),
            jdbc=jdbc_sec
        )

    def test_auto_profile_incremental_resolves_light(self):
        cfg = self._create_config(load_type=LoadType.INCREMENTAL, num_partitions=1)
        profile = SparkResourceTuner.resolve_profile(cfg)
        self.assertEqual(profile, "light")

        opts = SparkResourceTuner.get_spark_options(cfg)
        self.assertEqual(opts["spark.executor.memory"], "2g")
        self.assertEqual(opts["spark.executor.cores"], "2")
        self.assertEqual(opts["spark.sql.shuffle.partitions"], "10")

        fetch = SparkResourceTuner.get_tuned_fetch_size(cfg)
        self.assertEqual(fetch, 10000)

    def test_auto_profile_full_resolves_medium(self):
        cfg = self._create_config(load_type=LoadType.FULL, num_partitions=4)
        profile = SparkResourceTuner.resolve_profile(cfg)
        self.assertEqual(profile, "medium")

        opts = SparkResourceTuner.get_spark_options(cfg)
        self.assertEqual(opts["spark.executor.memory"], "4g")
        self.assertEqual(opts["spark.executor.cores"], "4")
        self.assertEqual(opts["spark.sql.shuffle.partitions"], "100")

        fetch = SparkResourceTuner.get_tuned_fetch_size(cfg)
        self.assertEqual(fetch, 25000)

    def test_auto_profile_upsert_resolves_heavy(self):
        cfg = self._create_config(load_type=LoadType.UPSERT, num_partitions=4)
        profile = SparkResourceTuner.resolve_profile(cfg)
        self.assertEqual(profile, "heavy")

        opts = SparkResourceTuner.get_spark_options(cfg)
        self.assertEqual(opts["spark.executor.memory"], "8g")
        self.assertEqual(opts["spark.memory.offHeap.size"], "2g")
        self.assertEqual(opts["spark.sql.shuffle.partitions"], "200")

        fetch = SparkResourceTuner.get_tuned_fetch_size(cfg)
        self.assertEqual(fetch, 50000)

    def test_auto_profile_high_partitions_escalates_to_heavy(self):
        cfg = self._create_config(load_type=LoadType.INCREMENTAL, num_partitions=12)
        profile = SparkResourceTuner.resolve_profile(cfg)
        self.assertEqual(profile, "heavy")

    def test_manual_profile_override(self):
        cfg = self._create_config(load_type=LoadType.INCREMENTAL, num_partitions=1, profile="heavy")
        profile = SparkResourceTuner.resolve_profile(cfg)
        self.assertEqual(profile, "heavy")


if __name__ == "__main__":
    unittest.main()
