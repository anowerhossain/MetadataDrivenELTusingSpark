import unittest
from unittest.mock import MagicMock

from src.core.config import QualitySection, ConfigError
from src.core.quality import (
    DataQualityValidator,
    DataQualityError,
    QualityResult,
)


class TestDataQualityValidator(unittest.TestCase):

    def test_validator_invalid_config_raises_error(self):
        with self.assertRaises(ConfigError):
            DataQualityValidator({"check_null_keys": True})

    def test_validate_disabled_skips_checks(self):
        q_config = QualitySection(enabled=False, minimum_rows=100)
        validator = DataQualityValidator(q_config)

        mock_df = MagicMock()
        res = validator.validate(mock_df)
        self.assertTrue(res.passed)

    def test_validate_none_dataframe(self):
        q_config = QualitySection(check_null_keys=True, check_duplicate_keys=True)
        validator = DataQualityValidator(q_config)

        res = validator.validate(None, merge_keys=["CUSTOMER_ID"])
        self.assertTrue(res.passed)
        self.assertEqual(res.total_rows, 0)
        self.assertEqual(res.processed_rows, 0)

    def test_validate_passes_on_clean_data(self):
        q_config = QualitySection(check_null_keys=True, check_duplicate_keys=True)
        validator = DataQualityValidator(q_config)

        mock_df = MagicMock()
        mock_df.mock_quality_result = QualityResult(
            passed=True,
            total_rows=1000,
            processed_rows=1000,
            null_key_count=0,
            duplicate_key_count=0,
        )

        res = validator.validate(mock_df, merge_keys=["CUSTOMER_ID"])
        self.assertTrue(res.passed)
        self.assertEqual(res.total_rows, 1000)
        self.assertEqual(res.processed_rows, 1000)

    def test_validate_fails_on_null_check(self):
        q_config = QualitySection(enabled=True, null_check=["CUSTOMER_ID", "UPDATED_AT"])
        validator = DataQualityValidator(q_config)

        mock_df = MagicMock()
        mock_df.mock_quality_result = QualityResult(
            passed=False,
            total_rows=100,
            processed_rows=0,
            null_key_count=3,
            error_message="Rule [null_check] constraint violated: Found 3 records containing NULL values in columns ['CUSTOMER_ID', 'UPDATED_AT']",
        )

        with self.assertRaises(DataQualityError) as ctx:
            validator.validate(mock_df)

        self.assertIn("Rule [null_check]", str(ctx.exception))

    def test_validate_fails_on_unique_check(self):
        q_config = QualitySection(enabled=True, unique_check=["CUSTOMER_ID"])
        validator = DataQualityValidator(q_config)

        mock_df = MagicMock()
        mock_df.mock_quality_result = QualityResult(
            passed=False,
            total_rows=100,
            processed_rows=0,
            duplicate_key_count=2,
            error_message="Rule [unique_check] constraint violated: Found 2 duplicate record groups on columns ['CUSTOMER_ID']",
        )

        with self.assertRaises(DataQualityError) as ctx:
            validator.validate(mock_df)

        self.assertIn("Rule [unique_check]", str(ctx.exception))

    def test_validate_fails_on_minimum_rows(self):
        q_config = QualitySection(enabled=True, minimum_rows=10)
        validator = DataQualityValidator(q_config)

        mock_df = MagicMock()
        mock_df.count.return_value = 5  # 5 rows is less than minimum 10

        with self.assertRaises(DataQualityError) as ctx:
            validator.validate(mock_df)

        self.assertIn("below required minimum of 10", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
