"""
Unit tests for Job Builder component (web_ui/components/job_builder.py).
Verifies TOML string generation and task dependency structure.
"""

import os
import unittest
from web_ui.components.job_builder import generate_job_toml_string
from src.core.job_pipeline import JobPipelineParser


class TestJobBuilder(unittest.TestCase):
    """Test suite for generate_job_toml_string and Job TOML output formatting."""

    def test_generate_job_toml_string(self):
        """Test creating TOML string for a composite Job pipeline."""
        job_id = "test_finance_job"
        job_name = "Test Finance Pipeline"
        description = "Test GL data flow"
        enabled = True
        task_mappings = [
            {"task_id": "bronze_orders_load", "task_file": "config/tasks/bronze_orders_load.toml", "depends_on": []},
            {"task_id": "silver_orders_clean", "task_file": "config/tasks/silver_orders_clean.toml", "depends_on": ["bronze_orders_load"]}
        ]

        toml_str = generate_job_toml_string(job_id, job_name, description, enabled, task_mappings)

        self.assertIn('[job]', toml_str)
        self.assertIn('job_id = "test_finance_job"', toml_str)
        self.assertIn('job_name = "Test Finance Pipeline"', toml_str)
        self.assertIn('task_id = "bronze_orders_load"', toml_str)
        self.assertIn('task_id = "silver_orders_clean"', toml_str)
        self.assertIn('depends_on = ["bronze_orders_load"]', toml_str)

        # Write to temporary file and parse back
        tmp_file = "scratch/temp_test_job.toml"
        os.makedirs("scratch", exist_ok=True)
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(toml_str)

        parsed_cfg = JobPipelineParser.load_job_toml(tmp_file)
        self.assertEqual(parsed_cfg.job_id, "test_finance_job")
        self.assertEqual(len(parsed_cfg.tasks), 2)


if __name__ == "__main__":
    unittest.main()
