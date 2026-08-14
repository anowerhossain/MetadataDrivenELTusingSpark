"""
Unit & Integration Tests for Task Abstraction Engine (BaseTask, TableLoadTask, QlikReplicateRefreshTask).
"""

import os
import unittest
from unittest.mock import MagicMock, patch
from src.core.task import BaseTask, TableLoadTask
from src.helpers.qlik_replicate import QlikReplicateRefreshTask
from src.core.config import ConfigParser, JobConfig


class DummyCustomTask(BaseTask):
    """Concrete subclass of BaseTask for testing base lifecycle."""
    def __init__(self, task_id="dummy_01", fail_validation=False, fail_execution=False):
        super().__init__(task_id=task_id, task_name="Dummy Task", task_type="dummy")
        self.fail_validation = fail_validation
        self.fail_execution = fail_execution

    def validate(self) -> bool:
        return not self.fail_validation

    def execute(self) -> bool:
        if self.fail_execution:
            raise RuntimeError("Execution error in dummy task")
        return True


class TestTaskAbstraction(unittest.TestCase):

    def test_basetask_success_lifecycle(self):
        """Test BaseTask successful validation and execution lifecycle."""
        task = DummyCustomTask(task_id="task_success")
        res = task.run()
        self.assertTrue(res)
        self.assertEqual(task.status, "SUCCESS")
        summary = task.get_summary()
        self.assertEqual(summary["task_id"], "task_success")
        self.assertEqual(summary["status"], "SUCCESS")
        self.assertEqual(summary["task_type"], "dummy")

    def test_basetask_validation_failure(self):
        """Test BaseTask handling validation failure."""
        task = DummyCustomTask(task_id="task_invalid", fail_validation=True)
        res = task.run()
        self.assertFalse(res)
        self.assertEqual(task.status, "FAILED")
        self.assertEqual(task.error_message, "Task validation check failed")

    def test_basetask_execution_exception(self):
        """Test BaseTask handling execution exception."""
        task = DummyCustomTask(task_id="task_error", fail_execution=True)
        with self.assertRaises(RuntimeError):
            task.run()
        self.assertEqual(task.status, "FAILED")

    @patch("src.helpers.spark.SparkSessionFactory.get_session")
    @patch("src.connectors.factory.ReaderFactory.get_reader")
    @patch("src.core.transformer.DataTransformer.transform")
    @patch("src.core.quality.DataQualityValidator.validate")
    @patch("src.core.writer.IcebergWriter.write")
    @patch("src.core.hooks.PreloadHandler.execute_preload_hooks")
    @patch("src.core.hooks.PostloadHandler.execute_postload_hooks")
    def test_table_load_task_execution(self, mock_postload, mock_preload, mock_write, mock_qual, mock_transform, mock_get_reader, mock_spark):
        """Test TableLoadTask execution using mock Spark pipeline components."""
        mock_reader = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_reader.read.return_value = mock_df
        mock_get_reader.return_value = mock_reader
        mock_transform.return_value = mock_df

        cfg_path = os.path.join("config", "jobs", "customer_load.toml")
        if not os.path.exists(cfg_path):
            self.skipTest(f"Config path {cfg_path} not found")

        task = TableLoadTask(cfg_path)
        self.assertEqual(task.task_type, "table_load")
        self.assertEqual(task.task_id, "customer_load")

        res = task.run()
        self.assertTrue(res)
        self.assertEqual(task.status, "SUCCESS")
        mock_get_reader.assert_called_once()
        mock_transform.assert_called_once()
        mock_write.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_qlik_replicate_refresh_task_success(self, mock_urlopen):
        """Test QlikReplicateRefreshTask REST API authentication, trigger action, and status polling."""
        mock_resp1 = MagicMock()
        mock_resp1.__enter__.return_value.read.return_value = b'{"token": "jwt_token"}'

        mock_resp2 = MagicMock()
        mock_resp2.__enter__.return_value.read.return_value = b'{"status": "SUCCESS"}'

        mock_resp3 = MagicMock()
        mock_resp3.__enter__.return_value.read.return_value = b'{"state": "COMPLETED"}'

        mock_urlopen.side_effect = [mock_resp1, mock_resp2, mock_resp3]

        task = QlikReplicateRefreshTask(
            task_id="qlik_orders_test",
            task_name="Qlik Orders Refresh",
            server_url="https://qlik-em.bank.local",
            qlik_task_name="OracleToIcebergOrders",
            action="RELOAD_TARGET",
            poll_interval_seconds=1,
            timeout_seconds=5,
            username="admin",
            password="password123"
        )

        res = task.run()
        self.assertTrue(res)
        self.assertEqual(task.status, "SUCCESS")

    @patch("urllib.request.urlopen")
    def test_qlik_replicate_refresh_task_failure(self, mock_urlopen):
        """Test QlikReplicateRefreshTask error handling when API returns error state."""
        mock_resp_trigger = MagicMock()
        mock_resp_trigger.__enter__.return_value.read.return_value = b'{"status": "SUCCESS"}'

        mock_resp_poll = MagicMock()
        mock_resp_poll.__enter__.return_value.read.return_value = b'{"state": "STOPPED_WITH_ERROR", "error_message": "Source DB error"}'

        mock_urlopen.side_effect = [mock_resp_trigger, mock_resp_poll, mock_resp_trigger, mock_resp_poll]

        task = QlikReplicateRefreshTask(
            task_id="qlik_orders_fail",
            task_name="Qlik Orders Refresh Fail",
            server_url="https://qlik-em.bank.local",
            qlik_task_name="OracleToIcebergOrders",
            poll_interval_seconds=1,
            timeout_seconds=5
        )

        with self.assertRaises(Exception):
            task.run()

        self.assertEqual(task.status, "FAILED")


if __name__ == "__main__":
    unittest.main()
