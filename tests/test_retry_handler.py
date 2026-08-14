import unittest
from unittest.mock import MagicMock

from src.core.config import RetrySection, ExecutionSection, ConfigError
from src.core.quality import DataQualityError
from src.helpers.retry import RetryHandler
from src.core.state import WatermarkManager


class TestRetryHandler(unittest.TestCase):

    def test_retry_with_execution_section_config(self):
        exec_config = ExecutionSection(retries=3, retry_delay_seconds=0.001)
        handler = RetryHandler(exec_config)

        mock_func = MagicMock(side_effect=[RuntimeError("Temporary Network Flake"), "SUCCESS"])
        res = handler.execute(mock_func, task_name="ExecTask")

        self.assertEqual(res, "SUCCESS")
        self.assertEqual(mock_func.call_count, 2)

    def test_retry_success_on_first_attempt(self):
        r_config = RetrySection(enabled=True, max_attempts=3, delay_seconds=0.001)
        handler = RetryHandler(r_config)

        mock_func = MagicMock(return_value="SUCCESS_DATA")
        res = handler.execute(mock_func, task_name="TestTask")

        self.assertEqual(res, "SUCCESS_DATA")
        self.assertEqual(mock_func.call_count, 1)

    def test_retry_success_on_second_attempt(self):
        r_config = RetrySection(enabled=True, max_attempts=3, delay_seconds=0.001)
        handler = RetryHandler(r_config)

        mock_func = MagicMock(side_effect=[RuntimeError("JDBC Timeout"), "SUCCESS_DATA"])
        res = handler.execute(mock_func, task_name="TestTask")

        self.assertEqual(res, "SUCCESS_DATA")
        self.assertEqual(mock_func.call_count, 2)

    def test_retry_exhausts_attempts_and_raises(self):
        r_config = RetrySection(enabled=True, max_attempts=3, delay_seconds=0.001)
        handler = RetryHandler(r_config)

        mock_func = MagicMock(side_effect=RuntimeError("Persistent Network Error"))

        with self.assertRaises(RuntimeError) as ctx:
            handler.execute(mock_func, task_name="TestTask")

        self.assertIn("Persistent Network Error", str(ctx.exception))
        self.assertEqual(mock_func.call_count, 3)

    def test_non_retryable_config_error_fails_fast(self):
        r_config = RetrySection(enabled=True, max_attempts=3, delay_seconds=0.001)
        handler = RetryHandler(r_config)

        mock_func = MagicMock(side_effect=ConfigError("Invalid DB Config"))

        with self.assertRaises(ConfigError):
            handler.execute(mock_func, task_name="TestTask")

        # Fails fast on 1st attempt without retrying
        self.assertEqual(mock_func.call_count, 1)

    def test_non_retryable_data_quality_error_fails_fast(self):
        r_config = RetrySection(enabled=True, max_attempts=3, delay_seconds=0.001)
        handler = RetryHandler(r_config)

        mock_func = MagicMock(side_effect=DataQualityError("Found NULL keys"))

        with self.assertRaises(DataQualityError):
            handler.execute(mock_func, task_name="TestTask")

        # Fails fast on 1st attempt without retrying
        self.assertEqual(mock_func.call_count, 1)

    def test_watermark_not_updated_on_retry_failure(self):
        wm_mgr = WatermarkManager(None)
        initial_wm = wm_mgr.get_last_watermark("failed_retry_job")
        self.assertIsNone(initial_wm)

        r_config = RetrySection(enabled=True, max_attempts=2, delay_seconds=0.001)
        handler = RetryHandler(r_config)

        def failing_etl_task():
            raise RuntimeError("Extraction Network Timeout")

        with self.assertRaises(RuntimeError):
            handler.execute(failing_etl_task, task_name="FailingJob")

        # Verify watermark remains unchanged (None) after failed attempts
        final_wm = wm_mgr.get_last_watermark("failed_retry_job")
        self.assertIsNone(final_wm)

    def test_disabled_retry_skips_retries(self):
        r_config = RetrySection(enabled=False, max_attempts=3, delay_seconds=0.001)
        handler = RetryHandler(r_config)

        mock_func = MagicMock(side_effect=RuntimeError("Transient Error"))

        with self.assertRaises(RuntimeError):
            handler.execute(mock_func, task_name="TestTask")

        self.assertEqual(mock_func.call_count, 1)


    def test_exponential_backoff_delay_calculation(self):
        r_config = RetrySection(enabled=True, max_attempts=3, delay_seconds=30.0, backoff_multiplier=2.0, exponential_backoff=True)
        handler = RetryHandler(r_config)

        self.assertEqual(handler.calculate_delay(1), 30.0)   # 1st retry (attempt 1) -> 30s
        self.assertEqual(handler.calculate_delay(2), 60.0)   # 2nd retry (attempt 2) -> 60s
        self.assertEqual(handler.calculate_delay(3), 120.0)  # 3rd retry (attempt 3) -> 120s


if __name__ == "__main__":
    unittest.main()
