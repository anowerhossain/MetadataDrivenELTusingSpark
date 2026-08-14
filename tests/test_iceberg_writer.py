import unittest
from unittest.mock import MagicMock, PropertyMock

from src.core.config import TargetSection, TargetType, TargetPartitionSection, SchemaSection, KeysSection, ConfigError
from src.core.writer import IcebergWriter


class TestIcebergWriter(unittest.TestCase):

    def setUp(self):
        self.target_config = TargetSection(
            type=TargetType.ICEBERG,
            catalog="hive",
            database="edw_bronze",
            table="customer",
        )

    def test_full_table_name_formatting(self):
        mock_spark = MagicMock()
        writer = IcebergWriter(mock_spark, self.target_config)
        self.assertEqual(writer.get_full_table_name(), "hive.edw_bronze.customer")

    def test_build_merge_condition_single_key(self):
        mock_spark = MagicMock()
        writer = IcebergWriter(mock_spark, self.target_config)
        cond = writer.build_merge_condition(["CUSTOMER_ID"])
        self.assertEqual(cond, "target.CUSTOMER_ID = source.CUSTOMER_ID")

    def test_build_merge_condition_composite_keys(self):
        mock_spark = MagicMock()
        writer = IcebergWriter(mock_spark, self.target_config)
        cond = writer.build_merge_condition(["CUSTOMER_ID", "ORG_ID"])
        self.assertEqual(cond, "target.CUSTOMER_ID = source.CUSTOMER_ID AND target.ORG_ID = source.ORG_ID")

    def test_schema_reconciliation_existing_schema_matches(self):
        mock_spark = MagicMock()
        mock_target_df = MagicMock()

        field_id = MagicMock(name="CUSTOMER_ID")
        type(field_id).name = PropertyMock(return_value="CUSTOMER_ID")
        type(field_id).dataType = PropertyMock(return_value="StringType")

        field_name = MagicMock(name="CUSTOMER_NAME")
        type(field_name).name = PropertyMock(return_value="CUSTOMER_NAME")
        type(field_name).dataType = PropertyMock(return_value="StringType")

        mock_target_df.schema = [field_id, field_name]
        mock_spark.table.return_value = mock_target_df

        source_df = MagicMock()
        source_df.schema = [field_id, field_name]

        schema_cfg = SchemaSection(evolution=True, add_columns=True)
        writer = IcebergWriter(mock_spark, self.target_config, schema_config=schema_cfg)

        # Existing schema -> should reconcile without error or DDL
        writer.reconcile_schema(source_df, "hive.edw_bronze.customer")
        mock_spark.sql.assert_not_called()

    def test_schema_reconciliation_new_column_evolution_enabled(self):
        mock_spark = MagicMock()
        mock_target_df = MagicMock()

        field_id = MagicMock(name="CUSTOMER_ID")
        type(field_id).name = PropertyMock(return_value="CUSTOMER_ID")
        type(field_id).dataType = PropertyMock(return_value="StringType")

        mock_data_type = MagicMock()
        mock_data_type.simpleString.return_value = "STRING"
        mock_data_type.__str__ = MagicMock(return_value="StringType")

        field_country = MagicMock(name="COUNTRY")
        type(field_country).name = PropertyMock(return_value="COUNTRY")
        type(field_country).dataType = PropertyMock(return_value=mock_data_type)

        mock_target_df.schema = [field_id]
        mock_spark.table.return_value = mock_target_df

        source_df = MagicMock()
        source_df.schema = [field_id, field_country]

        schema_cfg = SchemaSection(evolution=True, add_columns=True)
        writer = IcebergWriter(mock_spark, self.target_config, schema_config=schema_cfg)

        writer.reconcile_schema(source_df, "hive.edw_bronze.customer")
        mock_spark.sql.assert_called_once_with("ALTER TABLE hive.edw_bronze.customer ADD COLUMNS (COUNTRY STRING)")

    def test_schema_reconciliation_new_column_evolution_disabled(self):
        mock_spark = MagicMock()
        mock_target_df = MagicMock()

        field_id = MagicMock(name="CUSTOMER_ID")
        type(field_id).name = PropertyMock(return_value="CUSTOMER_ID")
        type(field_id).dataType = PropertyMock(return_value="StringType")

        field_country = MagicMock(name="COUNTRY")
        type(field_country).name = PropertyMock(return_value="COUNTRY")
        type(field_country).dataType = PropertyMock(return_value="StringType")

        mock_target_df.schema = [field_id]
        mock_spark.table.return_value = mock_target_df

        source_df = MagicMock()
        source_df.schema = [field_id, field_country]

        schema_cfg = SchemaSection(evolution=False, add_columns=False)
        writer = IcebergWriter(mock_spark, self.target_config, schema_config=schema_cfg)

        with self.assertRaises(ConfigError) as ctx:
            writer.reconcile_schema(source_df, "hive.edw_bronze.customer")
        self.assertIn("schema evolution is disabled", str(ctx.exception))

    def test_schema_reconciliation_incompatible_schema_type_mismatch(self):
        mock_spark = MagicMock()
        mock_target_df = MagicMock()

        field_src_age = MagicMock(name="AGE")
        type(field_src_age).name = PropertyMock(return_value="AGE")
        type(field_src_age).dataType = PropertyMock(return_value="StringType")

        field_tgt_age = MagicMock(name="AGE")
        type(field_tgt_age).name = PropertyMock(return_value="AGE")
        type(field_tgt_age).dataType = PropertyMock(return_value="IntegerType")

        mock_target_df.schema = [field_tgt_age]
        mock_spark.table.return_value = mock_target_df

        source_df = MagicMock()
        source_df.schema = [field_src_age]

        schema_cfg = SchemaSection(evolution=True, add_columns=True)
        writer = IcebergWriter(mock_spark, self.target_config, schema_config=schema_cfg)

        with self.assertRaises(ConfigError) as ctx:
            writer.reconcile_schema(source_df, "hive.edw_bronze.customer")
        self.assertIn("Incompatible schema change detected", str(ctx.exception))

    def test_create_table_with_target_partitioning(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False

        mock_df = MagicMock()
        mock_writer_v2 = MagicMock()
        mock_df.writeTo.return_value = mock_writer_v2
        mock_writer_v2.using.return_value = mock_writer_v2
        mock_writer_v2.partitionedBy.return_value = mock_writer_v2

        target_config_partitioned = TargetSection(
            type=TargetType.ICEBERG,
            catalog="hive",
            database="edw_bronze",
            table="customer",
            partition=TargetPartitionSection(type="days", column="UPDATED_AT")
        )

        writer = IcebergWriter(mock_spark, target_config_partitioned)
        success = writer.write(mock_df, mode="overwrite")

        self.assertTrue(success)
        mock_df.writeTo.assert_called_once_with("hive.edw_bronze.customer")
        mock_writer_v2.using.assert_called_once_with("iceberg")
        mock_writer_v2.partitionedBy.assert_called_once()
        mock_writer_v2.create.assert_called_once()


if __name__ == "__main__":
    unittest.main()
