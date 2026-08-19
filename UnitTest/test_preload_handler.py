import os
import unittest
from unittest.mock import MagicMock

from src.core.config import (
    JobConfig,
    JobSection,
    SourceSection,
    SourceType,
    TargetSection,
    TargetType,
    LoadSection,
    LoadType,
    PreloadSection,
    ConfigError,
)
from src.core.hooks import PreloadHandler


class TestPreloadHandler(unittest.TestCase):

    def setUp(self):
        self.env_backup = dict(os.environ)
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        self.job_config = JobConfig(
            job=JobSection(job_id="cust_job", job_name="Cust Job", enabled=True),
            source=SourceSection(type=SourceType.ORACLE, connection="oracle_prod", schema="BANK", table="CUSTOMER"),
            target=TargetSection(type=TargetType.ICEBERG, catalog="hive", database="edw_bronze", table="customer"),
            load=LoadSection(type=LoadType.FULL),
            preload=PreloadSection(
                enabled=True,
                operations=["validate_source", "validate_target", "check_watermark"]
            )
        )

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env_backup)

    def test_execute_preload_hooks_success(self):
        mock_spark = MagicMock()
        handler = PreloadHandler(mock_spark, self.job_config)

        # Should execute all 3 hooks without throwing exception
        handler.execute_preload_hooks()

    def test_unknown_operation_raises_config_error(self):
        invalid_config = JobConfig(
            job=JobSection(job_id="cust_job", job_name="Cust Job", enabled=True),
            source=SourceSection(type=SourceType.ORACLE, connection="oracle_prod", schema="BANK", table="CUSTOMER"),
            target=TargetSection(type=TargetType.ICEBERG, catalog="hive", database="edw_bronze", table="customer"),
            load=LoadSection(type=LoadType.FULL),
            preload=PreloadSection(
                enabled=True,
                operations=["validate_source", "invalid_operation_name"]
            )
        )

        mock_spark = MagicMock()
        handler = PreloadHandler(mock_spark, invalid_config)

        with self.assertRaises(ConfigError) as ctx:
            handler.execute_preload_hooks()

        self.assertIn("Unknown preload operation 'invalid_operation_name'", str(ctx.exception))

    def test_preload_disabled_skips_operations(self):
        disabled_config = JobConfig(
            job=JobSection(job_id="cust_job", job_name="Cust Job", enabled=True),
            source=SourceSection(type=SourceType.ORACLE, connection="oracle_prod", schema="BANK", table="CUSTOMER"),
            target=TargetSection(type=TargetType.ICEBERG, catalog="hive", database="edw_bronze", table="customer"),
            load=LoadSection(type=LoadType.FULL),
            preload=PreloadSection(
                enabled=False,
                operations=["invalid_operation_name"]  # should be ignored because enabled=False
            )
        )

        mock_spark = MagicMock()
        handler = PreloadHandler(mock_spark, disabled_config)

        # Should skip execution completely without erroring
        handler.execute_preload_hooks()


if __name__ == "__main__":
    unittest.main()
