import unittest
from src.core.config import (
    JobConfig,
    JobSection,
    SourceSection,
    TargetSection,
    LoadSection,
    SourceType,
    TargetType,
    LoadType,
)
from src.helpers.spark import SparkSessionFactory, get_cdp_spark_session


class TestSparkSessionFactory(unittest.TestCase):

    def setUp(self):
        self.config = JobConfig(
            job=JobSection(job_id="test_job", job_name="Test Job", enabled=True),
            source=SourceSection(
                type=SourceType.ORACLE,
                connection="oracle_conn",
                schema="BANK",
                table="CUSTOMER",
            ),
            target=TargetSection(
                type=TargetType.ICEBERG,
                catalog="hive",
                database="analytics",
                table="customer_iceberg",
            ),
            load=LoadSection(type=LoadType.FULL),
            spark={"spark.sql.shuffle.partitions": "40", "spark.ui.enabled": "false"},
        )

    def test_spark_session_creation(self):
        spark = SparkSessionFactory.get_session(config=self.config)
        # If PySpark is installed in the test environment, verify actual SparkSession props
        try:
            import pyspark
            self.assertIsNotNone(spark)
            self.assertEqual(spark.sparkContext.appName, "ETL_test_job")
            self.assertEqual(spark.conf.get("spark.sql.shuffle.partitions"), "40")
        except ImportError:
            # If PySpark is not installed, factory returns None stub with log warning
            self.assertIsNone(spark)

    def test_spark_session_extra_configs(self):
        spark = get_cdp_spark_session(
            config=self.config,
            extra_configs={"spark.custom.runtime.option": "test_val"}
        )
        try:
            import pyspark
            self.assertIsNotNone(spark)
            self.assertEqual(spark.conf.get("spark.custom.runtime.option"), "test_val")
        except ImportError:
            self.assertIsNone(spark)


if __name__ == "__main__":
    unittest.main()
