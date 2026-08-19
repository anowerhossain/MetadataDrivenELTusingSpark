import unittest
from unittest.mock import MagicMock
from src.core.config import TransformSection, SourceExtractionSection, AuditColumnsSection
from src.core.transformer import DataTransformer


class TestAuditAndExclusions(unittest.TestCase):
    def setUp(self):
        self.mock_df = MagicMock()
        self.mock_df.columns = ["CUSTOMER_ID", "CUSTOMER_NAME", "SSN", "PASSWORD", "TEMP_FLAG"]
        self.mock_df.drop.return_value = self.mock_df

    def test_apply_exclusions(self):
        transform_cfg = TransformSection(exclude=["PASSWORD"])
        transformer = DataTransformer(transform_cfg)
        
        # Test dropping PASSWORD and SSN
        transformer.apply_exclusions(self.mock_df, v_lst_source_exclude=["SSN"])
        
        # Verify drop was called for matched columns (case-insensitive)
        self.assertTrue(self.mock_df.drop.called)
        drop_args = self.mock_df.drop.call_args[0]
        self.assertIn("PASSWORD", drop_args)
        self.assertIn("SSN", drop_args)

    def test_audit_columns_defaults(self):
        audit_sec = AuditColumnsSection()
        self.assertTrue(audit_sec.enabled)
        self.assertEqual(audit_sec.insert_ts_column, "dwh_insert_ts")
        self.assertEqual(audit_sec.updated_ts_column, "dwh_updated_ts")
        self.assertEqual(audit_sec.run_id_column, "dwh_etl_run_id")
        self.assertEqual(audit_sec.job_user_column, "dwh_job_user")
        self.assertEqual(audit_sec.timezone, "Asia/Dhaka")

    def test_audit_columns_section_parsing(self):
        toml_dict = {
            "enabled": True,
            "insert_ts_column": "dwh_insert_ts",
            "updated_ts_column": "dwh_updated_ts",
            "run_id_column": "dwh_etl_run_id",
            "job_user_column": "dwh_job_user"
        }
        audit_sec = AuditColumnsSection.from_dict(toml_dict)
        self.assertTrue(audit_sec.enabled)
        self.assertEqual(audit_sec.insert_ts_column, "dwh_insert_ts")
        self.assertEqual(audit_sec.updated_ts_column, "dwh_updated_ts")
        self.assertEqual(audit_sec.run_id_column, "dwh_etl_run_id")
        self.assertEqual(audit_sec.job_user_column, "dwh_job_user")

    def test_source_extraction_exclude_columns_parsing(self):
        toml_dict = {
            "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"],
            "exclude_columns": ["SSN", "PASSWORD"]
        }
        ext_sec = SourceExtractionSection.from_dict(toml_dict)
        self.assertEqual(ext_sec.columns, ["CUSTOMER_ID", "CUSTOMER_NAME"])
        self.assertEqual(ext_sec.exclude_columns, ["SSN", "PASSWORD"])

    def test_apply_audit_columns_disabled(self):
        audit_sec = AuditColumnsSection(enabled=False)
        transformer = DataTransformer(TransformSection())
        result_df = transformer.apply_audit_columns(self.mock_df, v_obj_audit_config=audit_sec)
        # Should return original dataframe without creating new columns
        self.assertEqual(result_df, self.mock_df)


if __name__ == "__main__":
    unittest.main()
