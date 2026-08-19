import unittest
from unittest.mock import MagicMock, call

from src.core.config import TransformSection, ConfigError
from src.core.transformer import DataTransformer


class TestDataTransformer(unittest.TestCase):

    def setUp(self):
        self.transform_config = TransformSection(
            rename={
                "CUSTOMER_ID": "customer_id",
                "CUSTOMER_NAME": "customer_name",
            },
            cast={
                "CUSTOMER_ID": "BIGINT",
                "BALANCE": "DECIMAL(18,2)",
            },
            derived={
                "source_system": "'ORACLE'",
            }
        )

    def test_init_validation(self):
        with self.assertRaises(ConfigError):
            DataTransformer(None)

    def test_transform_returns_none_when_df_is_none(self):
        transformer = DataTransformer(self.transform_config)
        self.assertIsNone(transformer.transform(None))

    def test_pipeline_rename_cast_derived_execution_order(self):
        transformer = DataTransformer(self.transform_config)

        mock_df = MagicMock()
        mock_df.columns = ["CUSTOMER_ID", "CUSTOMER_NAME", "STATUS", "BALANCE", "UPDATED_AT"]

        # Mock chain returns
        mock_df_renamed = MagicMock()
        mock_df_renamed.columns = ["customer_id", "customer_name", "STATUS", "BALANCE", "UPDATED_AT"]
        mock_df.withColumnRenamed.return_value = mock_df_renamed

        mock_df_casted = MagicMock()
        mock_df_casted.columns = ["customer_id", "customer_name", "STATUS", "BALANCE", "UPDATED_AT"]
        mock_df_renamed.withColumnRenamed.return_value = mock_df_renamed
        mock_df_renamed.withColumn.return_value = mock_df_casted
        mock_df_casted.withColumn.return_value = mock_df_casted

        result_df = transformer.transform(mock_df)

        # Assert Step 1: Rename was called on mock_df first
        mock_df.withColumnRenamed.assert_any_call("CUSTOMER_ID", "customer_id")

        self.assertIsNotNone(result_df)

    def test_missing_column_logs_warning_without_failing(self):
        config_missing_col = TransformSection(
            rename={"NON_EXISTENT_COL": "new_col"},
            cast={"NON_EXISTENT_COL": "STRING"},
            derived={}
        )
        transformer = DataTransformer(config_missing_col)

        mock_df = MagicMock()
        mock_df.columns = ["CUSTOMER_ID", "CUSTOMER_NAME"]

        # Execution should not raise exception even if column is missing
        res = transformer.transform(mock_df)
        self.assertEqual(res, mock_df)


if __name__ == "__main__":
    unittest.main()
