"""
Unit tests for Job Pipeline Config Parser (src/core/job_pipeline.py).
Verifies higher-level Job workflow parsing from config/jobs/*.toml files.
"""

import os
import unittest
from src.core.job_pipeline import JobPipelineParser, JobPipelineConfig, JobTaskMapping


class TestJobPipeline(unittest.TestCase):
    """Test suite for JobPipelineParser and JobPipelineConfig."""

    def setUp(self):
        self.jobs_dir = os.path.join("config", "jobs")

    def test_job_pipeline_loading(self):
        """Test loading composite Job configuration from config/jobs/customer_sales_pipeline.toml."""
        job_file = os.path.join(self.jobs_dir, "customer_sales_pipeline.toml")
        if not os.path.exists(job_file):
            self.skipTest("Sample customer_sales_pipeline.toml not found")

        job_config = JobPipelineParser.load_job_toml(job_file)

        self.assertEqual(job_config.job_id, "customer_sales_pipeline")
        self.assertTrue(job_config.enabled)
        self.assertGreaterEqual(len(job_config.tasks), 3)

        # Check task mappings
        task_ids = [t.task_id for t in job_config.tasks]
        self.assertIn("bronze_orders_load", task_ids)
        self.assertIn("silver_orders_clean", task_ids)
        self.assertIn("gold_executive_report", task_ids)


if __name__ == "__main__":
    unittest.main()
