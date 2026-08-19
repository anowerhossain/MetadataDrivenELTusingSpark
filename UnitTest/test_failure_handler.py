import os
import shutil
import unittest
from datetime import datetime, timezone

from src.helpers.failure import FailureHandler


class TestFailureHandler(unittest.TestCase):

    def setUp(self):
        self.test_dir = os.path.join("tests", "tmp_failed_jobs")
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_record_failure_creates_marker_file(self):
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        config_path = os.path.join("config", "jobs", "mysql_orders.toml")
        err = RuntimeError("Database connection timed out")

        marker_file = FailureHandler.record_failure(config_path, "mysql_orders", err, base_dir=self.test_dir)

        self.assertTrue(os.path.exists(marker_file))
        self.assertIn("mysql_orders.toml_", marker_file)

        with open(marker_file, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn("JOB ID                 : mysql_orders", content)
            self.assertIn("Database connection timed out", content)

    def test_get_failed_jobs_deduplicates(self):
        date_str = "20260811"
        target_dir = os.path.join(self.test_dir, date_str)
        os.makedirs(target_dir, exist_ok=True)

        cfg1 = os.path.abspath(os.path.join("config", "jobs", "mysql_orders.toml"))
        cfg2 = os.path.abspath(os.path.join("config", "jobs", "postgres_payments.toml"))

        # Create two failure files for cfg1 and one for cfg2
        with open(os.path.join(target_dir, "mysql_orders.toml_100000.txt"), "w") as f:
            f.write(f"CONFIG PATH : {cfg1}\n")
        with open(os.path.join(target_dir, "mysql_orders.toml_120000.txt"), "w") as f:
            f.write(f"CONFIG PATH : {cfg1}\n")
        with open(os.path.join(target_dir, "postgres_payments.toml_110000.txt"), "w") as f:
            f.write(f"CONFIG PATH : {cfg2}\n")

        failed_dict = FailureHandler.get_failed_jobs(date_str, base_dir=self.test_dir)

        self.assertEqual(len(failed_dict), 2)
        self.assertIn(cfg1, failed_dict)
        self.assertIn(cfg2, failed_dict)
        self.assertEqual(len(failed_dict[cfg1]), 2)
        self.assertEqual(len(failed_dict[cfg2]), 1)

    def test_clear_job_failure_markers(self):
        date_str = "20260811"
        target_dir = os.path.join(self.test_dir, date_str)
        os.makedirs(target_dir, exist_ok=True)

        cfg1 = os.path.abspath(os.path.join("config", "jobs", "mysql_orders.toml"))

        marker1 = os.path.join(target_dir, "mysql_orders.toml_100000.txt")
        marker2 = os.path.join(target_dir, "mysql_orders.toml_120000.txt")

        with open(marker1, "w") as f:
            f.write(f"CONFIG PATH : {cfg1}\n")
        with open(marker2, "w") as f:
            f.write(f"CONFIG PATH : {cfg1}\n")

        deleted_count = FailureHandler.clear_job_failure_markers(date_str, cfg1, base_dir=self.test_dir)

        self.assertEqual(deleted_count, 2)
        self.assertFalse(os.path.exists(marker1))
        self.assertFalse(os.path.exists(marker2))
        self.assertFalse(os.path.exists(target_dir))


if __name__ == "__main__":
    unittest.main()
