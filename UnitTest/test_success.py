"""
Unit tests for SuccessHandler audit module.
"""

import os
import shutil
import tempfile
import unittest
from src.helpers.success import SuccessHandler


class TestSuccessHandler(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_record_success_creates_marker_file(self):
        config_path = os.path.join(self.temp_dir, "customer_test.toml")
        with open(config_path, "w") as f:
            f.write("[job]\njob_id='customer_test'\n")

        marker_path = SuccessHandler.record_success(
            config_path=config_path,
            job_id="customer_test",
            run_id="run_999",
            rows_read=500,
            rows_written=500,
            duration_seconds=1.25,
            base_dir=self.temp_dir
        )

        self.assertTrue(os.path.exists(marker_path))
        self.assertIn("customer_test.toml_", os.path.basename(marker_path))

        with open(marker_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("JOB ID                 : customer_test", content)
        self.assertIn("STATUS                 : SUCCESS", content)
        self.assertIn("ROWS READ              : 500", content)

    def test_get_success_jobs_lists_markers(self):
        config_path = os.path.join(self.temp_dir, "orders.toml")
        with open(config_path, "w") as f:
            f.write("[job]\njob_id='orders'\n")

        marker_path = SuccessHandler.record_success(
            config_path=config_path,
            job_id="orders",
            run_id="run_100",
            base_dir=self.temp_dir
        )

        date_str = os.path.basename(os.path.dirname(marker_path))
        jobs = SuccessHandler.get_success_jobs(date_str, base_dir=self.temp_dir)

        abs_path = os.path.abspath(config_path)
        self.assertIn(abs_path, jobs)
        self.assertEqual(len(jobs[abs_path]), 1)


if __name__ == "__main__":
    unittest.main()
