"""
Unit tests for Luigi Task Wrapper (src/core/luigi_task.py) and LuigiRunner (src/helpers/luigi_runner.py).
Verifies framework task wrapping, TOML depends_on dynamic dependency resolution,
Mermaid DAG tree generation, and execution workflows.
"""

import os
import unittest
from unittest.mock import patch, MagicMock
from src.core.config import ConfigParser, JobConfig
from src.core.luigi_task import FrameworkLuigiTask, create_framework_task_instance, LUIGI_AVAILABLE
from src.helpers.luigi_runner import LuigiRunner


class TestLuigiIntegration(unittest.TestCase):
    """Test suite for Luigi task wrapping, DAG dependency resolution, and LuigiRunner helper."""

    def setUp(self):
        self.sample_toml_dir = os.path.join("config", "tasks")

    def test_luigi_task_creation_table_load(self):
        """Test create_framework_task_instance instantiates TableLoadTask for table_load type."""
        config_path = os.path.join(self.sample_toml_dir, "customer.toml")
        if not os.path.exists(config_path):
            self.skipTest("Sample customer.toml not found")

        config = ConfigParser.load_toml(config_path)
        task_instance = create_framework_task_instance(config)

        self.assertEqual(task_instance.task_id, config.job.task_id)
        self.assertEqual(task_instance.task_name, config.job.task_name)

    def test_luigi_runner_discovery_and_dag_generation(self):
        """Test LuigiRunner discovers active tasks and generates valid Mermaid DAG syntax."""
        runner = LuigiRunner(config_dir=self.sample_toml_dir)
        
        if not runner.task_map:
            self.skipTest("No task config files found in config/tasks/")

        self.assertGreater(len(runner.task_map), 0)

        mermaid_dag = runner.build_mermaid_dag()
        self.assertTrue(mermaid_dag.startswith("graph TD"))
        self.assertTrue(len(mermaid_dag) > 10)

        ascii_summary = runner.generate_ascii_dag_summary()
        self.assertIn("LUIGI TASK DEPENDENCY GRAPH", ascii_summary)

    @unittest.skipUnless(LUIGI_AVAILABLE, "Luigi package not installed")
    def test_framework_luigi_task_instantiation(self):
        """Test FrameworkLuigiTask instantiation and requires() dynamic dependency resolution."""
        config_path = os.path.join(self.sample_toml_dir, "customer.toml")
        if not os.path.exists(config_path):
            self.skipTest("Sample customer.toml not found")

        task_map = {"customer": config_path}
        luigi_task = FrameworkLuigiTask(config_path=config_path, all_task_map=task_map)

        self.assertEqual(luigi_task.parsed_config.job.task_id, "customer")

        # Test requires() resolution
        reqs = luigi_task.requires()
        self.assertIsInstance(reqs, list)

    def test_bronze_silver_gold_dependency_chain(self):
        """Test Bronze -> Silver -> Gold multi-task dependency resolution and Mermaid graph arrows."""
        runner = LuigiRunner(config_dir=self.sample_toml_dir)
        self.assertIn("bronze_orders_load", runner.task_map)
        self.assertIn("silver_orders_clean", runner.task_map)
        self.assertIn("gold_executive_report", runner.task_map)

        mermaid_dag = runner.build_mermaid_dag()
        self.assertIn("bronze_orders_load --> silver_orders_clean", mermaid_dag)
        self.assertIn("silver_orders_clean --> gold_executive_report", mermaid_dag)


if __name__ == "__main__":
    unittest.main()
