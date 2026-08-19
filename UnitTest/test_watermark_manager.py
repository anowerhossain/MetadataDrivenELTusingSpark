import unittest
from unittest.mock import MagicMock

from src.core.state import WatermarkManager


class TestWatermarkManager(unittest.TestCase):

    def test_watermark_retrieval_returns_none_when_empty(self):
        wm_mgr = WatermarkManager(spark_session=None)
        val = wm_mgr.get_last_watermark("unknown_job")
        self.assertIsNone(val)

    def test_independent_job_watermark_tracking(self):
        wm_mgr = WatermarkManager(spark_session=None)

        # Update customer_load
        wm_mgr.update_watermark("customer_load", "2026-08-10 10:00:00")

        # Update account_load
        wm_mgr.update_watermark("account_load", "2026-08-10 10:05:00")

        # Retrieve and verify independent values
        self.assertEqual(wm_mgr.get_last_watermark("customer_load"), "2026-08-10 10:00:00")
        self.assertEqual(wm_mgr.get_last_watermark("account_load"), "2026-08-10 10:05:00")

    def test_max_watermark_computation_from_mock_df(self):
        wm_mgr = WatermarkManager(spark_session=None)
        self.assertIsNone(wm_mgr.get_max_watermark_from_df(None, "UPDATED_AT"))

        mock_df = MagicMock()
        mock_df.mock_max_watermark = "2026-08-10 12:00:00"

        val = wm_mgr.get_max_watermark_from_df(mock_df, "UPDATED_AT")
        self.assertEqual(val, "2026-08-10 12:00:00")

    def test_empty_watermark_update_rejected(self):
        wm_mgr = WatermarkManager(spark_session=None)
        success = wm_mgr.update_watermark("customer_load", "")
        self.assertFalse(success)
        self.assertIsNone(wm_mgr.get_last_watermark("customer_load"))


if __name__ == "__main__":
    unittest.main()
