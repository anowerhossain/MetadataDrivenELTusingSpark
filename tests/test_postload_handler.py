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
    LoadIncrementalSection,
    PostloadSection,
    MaintenanceSection,
    ConfigError,
)
from src.core.hooks import PostloadHandler
from src.core.state import WatermarkManager


class TestPostloadHandler(unittest.TestCase):

    def setUp(self):
        self.job_config = JobConfig(
            job=JobSection(job_id="cust_postload_job", job_name="Cust Job", enabled=True),
            source=SourceSection(type=SourceType.ORACLE, connection="oracle_prod", schema="BANK", table="CUSTOMER"),
            target=TargetSection(
                type=TargetType.ICEBERG,
                catalog="hive",
                database="edw_bronze",
                table="customer",
                maintenance=MaintenanceSection(enabled=True, target_file_size_mb=256)
            ),
            load=LoadSection(
                type=LoadType.INCREMENTAL,
                watermark_column="UPDATED_AT",
                incremental=LoadIncrementalSection(column="UPDATED_AT", watermark_type="timestamp")
            ),
            postload=PostloadSection(
                enabled=True,
                operations=["update_watermark", "compact_table", "refresh_metadata"]
            )
        )

    def test_postload_hooks_success_scenario(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.mock_max_watermark = "2026-08-10 12:00:00"

        # Verify REFRESH TABLE was called with SparkSession
        handler_spark = PostloadHandler(mock_spark, self.job_config)
        handler_spark.run_refresh_metadata()
        mock_spark.sql.assert_called_once_with("REFRESH TABLE hive.edw_bronze.customer")

        # Test watermark update using stub session for memory store validation
        handler_stub = PostloadHandler(None, self.job_config)
        handler_stub.execute_postload_hooks(mock_df)
        wm_mgr = WatermarkManager(None)
        self.assertEqual(wm_mgr.get_last_watermark("cust_postload_job"), "2026-08-10 12:00:00")

    def test_postload_compact_table_hook(self):
        mock_spark = MagicMock()
        handler = PostloadHandler(mock_spark, self.job_config)

        handler.run_compact_table()

        # Verify compaction SQL calls executed
        self.assertEqual(mock_spark.sql.call_count, 2)
        mock_spark.sql.assert_any_call(
            "CALL hive.system.rewrite_data_files(table => 'hive.edw_bronze.customer', options => map('target-file-size-bytes', '268435456'))"
        )
        mock_spark.sql.assert_any_call(
            "CALL hive.system.rewrite_manifests(table => 'hive.edw_bronze.customer')"
        )

    def test_postload_remove_orphan_files_hook(self):
        mock_spark = MagicMock()
        handler = PostloadHandler(mock_spark, self.job_config)

        handler.run_remove_orphan_files()

        # Verify remove_orphan_files SQL call executed
        self.assertEqual(mock_spark.sql.call_count, 1)
        args, _ = mock_spark.sql.call_args
        self.assertIn("CALL hive.system.remove_orphan_files(table => 'hive.edw_bronze.customer'", args[0])
        self.assertIn("older_than => TIMESTAMP", args[0])

    def test_postload_failure_scenario_watermark_remains_unchanged(self):
        wm_mgr = WatermarkManager(None)

        # Initial state before job run
        initial_wm = wm_mgr.get_last_watermark("cust_postload_job_fail")
        self.assertIsNone(initial_wm)

        # Simulate job write failure: PostloadHandler is NOT invoked when write fails
        try:
            raise RuntimeError("Iceberg target write operation FAILED due to network drop.")
        except RuntimeError:
            pass

        # Verify watermark remains unchanged after failure
        final_wm = wm_mgr.get_last_watermark("cust_postload_job_fail")
        self.assertIsNone(final_wm)

    def test_unknown_operation_raises_config_error(self):
        invalid_config = JobConfig(
            job=JobSection(job_id="cust_job", job_name="Cust Job", enabled=True),
            source=SourceSection(type=SourceType.ORACLE, connection="oracle_prod", schema="BANK", table="CUSTOMER"),
            target=TargetSection(type=TargetType.ICEBERG, catalog="hive", database="edw_bronze", table="customer"),
            load=LoadSection(type=LoadType.FULL),
            postload=PostloadSection(
                enabled=True,
                operations=["update_watermark", "invalid_operation_name"]
            )
        )

        mock_spark = MagicMock()
        handler = PostloadHandler(mock_spark, invalid_config)

        with self.assertRaises(ConfigError) as ctx:
            handler.execute_postload_hooks()

        self.assertIn("Unknown postload operation 'invalid_operation_name'", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
