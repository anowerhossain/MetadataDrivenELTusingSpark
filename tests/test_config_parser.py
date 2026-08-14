import os
import tempfile
import unittest
from pathlib import Path

from src.core.config import (
    ConfigParser,
    JobConfig,
    ConfigError,
    SourceType,
    TargetType,
    LoadType,
)


class TestConfigParser(unittest.TestCase):

    def setUp(self):
        self.valid_toml_content = """
[job]
job_id = "customer"
job_name = "Customer ETL"
enabled = true
description = "Oracle Customer -> Bronze Iceberg"

[source]
type = "oracle"
connection = "oracle_prod"
schema = "BANK"
table = "CUSTOMER"

[source.extraction]
columns = ["CUSTOMER_ID", "CUSTOMER_NAME", "STATUS", "BALANCE", "UPDATED_AT"]

[source.jdbc]
fetch_size = 10000
partition_column = "CUSTOMER_ID"
num_partitions = 8

[load]
type = "INCREMENTAL"

[load.incremental]
column = "UPDATED_AT"
watermark_type = "timestamp"

[keys]
primary_key = ["CUSTOMER_ID"]
merge_keys = ["CUSTOMER_ID"]

[target]
catalog = "hive"
database = "edw_bronze"
table = "customer"

[target.partition]
type = "days"
column = "UPDATED_AT"

[transform.rename]
CUSTOMER_ID = "customer_id"
CUSTOMER_NAME = "customer_name"

[transform.cast]
CUSTOMER_ID = "BIGINT"
BALANCE = "DECIMAL(18,2)"

[transform.derived]
source_system = "'ORACLE'"

[schema]
evolution = true
add_columns = true

[preload]
enabled = true
operations = ["validate_source", "validate_target", "check_watermark"]

[postload]
enabled = true
operations = ["update_watermark", "refresh_metadata"]

[data_quality]
enabled = true
null_check = ["CUSTOMER_ID", "UPDATED_AT"]
unique_check = ["CUSTOMER_ID"]
minimum_rows = 1

[execution]
retries = 3
retry_delay_seconds = 30
"""

    def write_temp_toml(self, content: str) -> Path:
        temp_file = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".toml", delete=False)
        temp_file.write(content)
        temp_file.close()
        return Path(temp_file.name)

    def test_load_valid_configuration(self):
        toml_path = self.write_temp_toml(self.valid_toml_content)
        try:
            config = ConfigParser.load_toml(toml_path)
            self.assertIsInstance(config, JobConfig)
            self.assertEqual(config.job.job_id, "customer")
            self.assertEqual(config.job.job_name, "Customer ETL")
            self.assertEqual(config.job.description, "Oracle Customer -> Bronze Iceberg")
            self.assertEqual(config.load.type, LoadType.INCREMENTAL)
        finally:
            os.unlink(toml_path)

    def test_full_load_config_requires_no_incremental_section(self):
        content = """
[job]
job_id = "customer_full"
job_name = "Customer Full Load"
enabled = true

[source]
type = "oracle"
connection = "oracle_prod"
schema = "BANK"
table = "CUSTOMER"

[load]
type = "FULL"

[target]
catalog = "hive"
database = "edw_bronze"
table = "customer"
"""
        toml_path = self.write_temp_toml(content)
        try:
            config = ConfigParser.load_toml(toml_path)
            self.assertEqual(config.load.type, LoadType.FULL)
            self.assertIsNone(config.load.watermark_column)
            self.assertIsNone(config.load.incremental)
        finally:
            os.unlink(toml_path)

    def test_incremental_load_config_with_incremental_column(self):
        content = """
[job]
job_id = "customer_inc"
job_name = "Customer Incremental Load"
enabled = true

[source]
type = "oracle"
connection = "oracle_prod"
schema = "BANK"
table = "CUSTOMER"

[load]
type = "INCREMENTAL"

[load.incremental]
column = "UPDATED_AT"
watermark_type = "timestamp"

[target]
catalog = "hive"
database = "edw_bronze"
table = "customer"
"""
        toml_path = self.write_temp_toml(content)
        try:
            config = ConfigParser.load_toml(toml_path)
            self.assertEqual(config.load.type, LoadType.INCREMENTAL)
            self.assertEqual(config.load.watermark_column, "UPDATED_AT")
            self.assertIsNotNone(config.load.incremental)
            self.assertEqual(config.load.incremental.column, "UPDATED_AT")
            self.assertEqual(config.load.incremental.watermark_type, "timestamp")
        finally:
            os.unlink(toml_path)

    def test_incremental_load_missing_column_raises_config_error(self):
        content = """
[job]
job_id = "customer_inc_invalid"
job_name = "Customer Incremental Load Invalid"
enabled = true

[source]
type = "oracle"
connection = "oracle_prod"
schema = "BANK"
table = "CUSTOMER"

[load]
type = "INCREMENTAL"

[target]
catalog = "hive"
database = "edw_bronze"
table = "customer"
"""
        toml_path = self.write_temp_toml(content)
        try:
            with self.assertRaises(ConfigError) as ctx:
                ConfigParser.load_toml(toml_path)
            self.assertIn("requires 'load.incremental.column'", str(ctx.exception))
        finally:
            os.unlink(toml_path)

    def test_hierarchical_toml_structure_parsing(self):
        toml_path = self.write_temp_toml(self.valid_toml_content)
        try:
            config = ConfigParser.load_toml(toml_path)

            # Test [source.extraction] & [source.jdbc]
            self.assertIsNotNone(config.source.extraction)
            self.assertEqual(config.source.extraction.columns, ["CUSTOMER_ID", "CUSTOMER_NAME", "STATUS", "BALANCE", "UPDATED_AT"])

            self.assertIsNotNone(config.jdbc)
            self.assertEqual(config.jdbc.fetch_size, 10000)
            self.assertEqual(config.jdbc.partition_column, "CUSTOMER_ID")
            self.assertEqual(config.jdbc.num_partitions, 8)

            # Test [load.incremental] & [keys]
            self.assertIsNotNone(config.load.incremental)
            self.assertEqual(config.load.incremental.column, "UPDATED_AT")
            self.assertEqual(config.load.watermark_column, "UPDATED_AT")
            self.assertEqual(config.keys.primary_key, ["CUSTOMER_ID"])
            self.assertEqual(config.keys.merge_keys, ["CUSTOMER_ID"])
            self.assertEqual(config.load.merge_keys, ["CUSTOMER_ID"])

            # Test [target.partition]
            self.assertIsNotNone(config.target.partition)
            self.assertEqual(config.target.partition.type, "days")
            self.assertEqual(config.target.partition.column, "UPDATED_AT")

            # Test [transform]
            self.assertIsNotNone(config.transform)
            self.assertEqual(config.transform.rename["CUSTOMER_ID"], "customer_id")
            self.assertEqual(config.transform.cast["CUSTOMER_ID"], "BIGINT")
            self.assertEqual(config.transform.derived["source_system"], "'ORACLE'")

            # Test [schema], [preload], [postload]
            self.assertTrue(config.schema_config.evolution)
            self.assertTrue(config.schema_config.add_columns)
            self.assertEqual(config.preload.operations, ["validate_source", "validate_target", "check_watermark"])
            self.assertEqual(config.postload.operations, ["update_watermark", "refresh_metadata"])

            # Test [data_quality] & [execution]
            self.assertIsNotNone(config.data_quality)
            self.assertEqual(config.data_quality.null_check, ["CUSTOMER_ID", "UPDATED_AT"])
            self.assertEqual(config.data_quality.unique_check, ["CUSTOMER_ID"])
            self.assertEqual(config.data_quality.minimum_rows, 1)

            self.assertIsNotNone(config.execution)
            self.assertEqual(config.execution.retries, 3)
            self.assertEqual(config.execution.retry_delay_seconds, 30.0)
            self.assertEqual(config.retry.max_attempts, 3)
            self.assertEqual(config.retry.delay_seconds, 30.0)

        finally:
            os.unlink(toml_path)

    def test_missing_required_section_raises_config_error(self):
        content = """
[job]
job_id = "customer_upsert"
job_name = "Customer Upsert"
enabled = true

[source]
type = "oracle"
connection = "oracle_prod"
schema = "BANK"
table = "CUSTOMER"
"""
        toml_path = self.write_temp_toml(content)
        try:
            with self.assertRaises(ConfigError) as ctx:
                ConfigParser.load_toml(toml_path)
            self.assertIn("Missing required section", str(ctx.exception))
        finally:
            os.unlink(toml_path)


if __name__ == "__main__":
    unittest.main()
