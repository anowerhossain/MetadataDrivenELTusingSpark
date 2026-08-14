import os
import unittest
from unittest.mock import MagicMock, patch

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
from main import run_pipeline, run_batch_pipeline, validate_config_file


class TestPipelineIntegration(unittest.TestCase):

    def setUp(self):
        self.env_backup = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env_backup)

    @patch("main.IcebergWriter")
    @patch("main.ReaderFactory.get_reader")
    @patch("main.get_cdp_spark_session")
    @patch("main.ConfigParser.load_toml")
    def test_pipeline_full_execution_flow(
        self,
        mock_load_toml,
        mock_get_spark,
        mock_get_reader,
        mock_iceberg_writer_cls,
    ):
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        mock_config = JobConfig(
            job=JobSection(job_id="customer_load", job_name="Customer Load", enabled=True),
            source=SourceSection(
                type=SourceType.ORACLE,
                connection="oracle_prod",
                schema="BANK",
                table="CUSTOMER",
            ),
            target=TargetSection(
                type=TargetType.ICEBERG,
                catalog="hive",
                database="edw_bronze",
                table="customer",
            ),
            load=LoadSection(type=LoadType.FULL),
        )
        mock_load_toml.return_value = mock_config

        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        mock_reader_inst = MagicMock()
        mock_df = MagicMock()
        mock_reader_inst.read.return_value = mock_df
        mock_get_reader.return_value = mock_reader_inst

        mock_writer_inst = MagicMock()
        mock_iceberg_writer_cls.return_value = mock_writer_inst

        exit_code = run_pipeline("config/jobs/customer.toml")

        self.assertEqual(exit_code, 0)
        mock_load_toml.assert_called_once_with("config/jobs/customer.toml")
        mock_get_spark.assert_called_once_with(config=mock_config)
        mock_get_reader.assert_called_once_with(mock_spark, mock_config.source, jdbc_config=mock_config.jdbc)
        mock_writer_inst.write.assert_called_once_with(mock_df, mode="overwrite")

    @patch("main.get_cdp_spark_session")
    @patch("main.ConfigParser.load_toml")
    def test_pipeline_validate_flag_does_not_start_spark(
        self,
        mock_load_toml,
        mock_get_spark,
    ):
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        mock_config = JobConfig(
            job=JobSection(job_id="customer_load", job_name="Customer Load", enabled=True),
            source=SourceSection(
                type=SourceType.ORACLE,
                connection="oracle_prod",
                schema="BANK",
                table="CUSTOMER",
            ),
            target=TargetSection(
                type=TargetType.ICEBERG,
                catalog="hive",
                database="edw_bronze",
                table="customer",
            ),
            load=LoadSection(type=LoadType.FULL),
        )
        mock_load_toml.return_value = mock_config

        exit_code = run_pipeline("config/jobs/customer.toml", validate_only=True)

        self.assertEqual(exit_code, 0)
        mock_get_spark.assert_not_called()

    @patch("main.get_cdp_spark_session")
    def test_pipeline_validation_failure_returns_exit_code_1(self, mock_get_spark):
        # Unset credentials so connection validation fails
        os.environ.clear()

        exit_code = run_pipeline("config/jobs/customer.toml", validate_only=True)

        self.assertEqual(exit_code, 1)
        mock_get_spark.assert_not_called()

    @patch("main.run_pipeline")
    def test_run_batch_pipeline_concurrent_execution(self, mock_run_pipeline):
        mock_run_pipeline.return_value = 0
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        exit_code = run_batch_pipeline("config/jobs", max_workers=2, validate_only=True)

        self.assertEqual(exit_code, 0)
        self.assertGreater(mock_run_pipeline.call_count, 1)

    def test_run_batch_pipeline_empty_directory_fails(self):
        exit_code = run_batch_pipeline("non_existent_dir_xyz", max_workers=2, validate_only=True)
        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
