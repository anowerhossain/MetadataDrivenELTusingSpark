"""
Unit Test Suite: PySpark DataFrame Record Extraction & Fetch Validation
Tests extracting database records into PySpark DataFrames from MySQL, Oracle, SFTP, and writing to Iceberg.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from src.core.config import SourceSection, SourceSFTPSection, SourceType, LoadSection, LoadType, JDBCSection
from src.connectors.mysql import MySQLReader
from src.connectors.oracle import OracleReader
from src.connectors.sftp import SFTPReader


class TestDataFrameRecordFetch(unittest.TestCase):

    def setUp(self):
        self.env_backup = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env_backup)

    @patch("src.connectors.mysql.MySQLConnectionResolver.resolve")
    def test_mysql_fetch_records_into_dataframe(self, mock_resolve):
        """Verifies extracting MySQL records into a PySpark DataFrame with column projections."""
        mock_conn = MagicMock()
        mock_conn.connection_name = "mysql_prod"
        mock_conn.to_jdbc_options.return_value = {
            "url": "jdbc:mysql://localhost:3306/crm_db",
            "user": "user",
            "password": "pass",
            "driver": "com.mysql.cj.jdbc.Driver",
            "fetchSize": "25000",
        }
        mock_resolve.return_value = mock_conn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 1500
        mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df

        source_cfg = SourceSection(type=SourceType.MYSQL, connection="mysql_prod", schema="crm_db", table="complaint")
        jdbc_cfg = JDBCSection(partition_column="ID", num_partitions=4, fetch_size=25000)
        reader = MySQLReader(mock_spark, source_cfg, jdbc_config=jdbc_cfg)

        load_cfg = LoadSection(type=LoadType.FULL)
        extracted_df = reader.read(load_config=load_cfg)

        self.assertIsNotNone(extracted_df)
        self.assertEqual(extracted_df.count(), 1500)
        mock_spark.read.format.assert_called_with("jdbc")

    @patch("src.connectors.oracle.OracleConnectionResolver.resolve")
    def test_oracle_fetch_records_into_dataframe(self, mock_resolve):
        """Verifies extracting Oracle records into a PySpark DataFrame."""
        mock_conn = MagicMock()
        mock_conn.connection_name = "oracle_prod"
        mock_conn.to_jdbc_options.return_value = {
            "url": "jdbc:oracle:thin:@//localhost:1521/ORCLPDB",
            "user": "hr",
            "password": "pass",
            "driver": "oracle.jdbc.OracleDriver",
            "fetchSize": "25000",
        }
        mock_resolve.return_value = mock_conn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 5000
        mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df

        source_cfg = SourceSection(type=SourceType.ORACLE, connection="oracle_prod", schema="HR", table="EMPLOYEES")
        reader = OracleReader(mock_spark, source_cfg)

        extracted_df = reader.read(load_config=LoadSection(type=LoadType.FULL))

        self.assertIsNotNone(extracted_df)
        self.assertEqual(extracted_df.count(), 5000)

    def test_sftp_csv_record_fetch(self):
        """Verifies parsing local CSV file feed into PySpark DataFrame."""
        import tempfile
        import pandas as pd

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "complaints_2026.csv")
            df_data = pd.DataFrame({
                "ID": [1, 2, 3],
                "REFERENCE_NUMBER": ["REF001", "REF002", "REF003"],
                "ACCOUNT_NUMBER": ["ACC1001", "ACC1002", "ACC1003"],
                "ACC_NAME": ["Alice", "Bob", "Charlie"],
                "MOBILE_NUMBER": ["01700000001", "01700000002", "01700000003"],
                "EMAIL_ADDRESS": ["a@bank.com", "b@bank.com", "c@bank.com"],
                "SEGMENT": ["RETAIL", "RETAIL", "CORPORATE"]
            })
            df_data.to_csv(csv_path, index=False)

            sftp_sec = SourceSFTPSection(path=tmpdir, file_pattern="*.csv", file_format="csv")
            source_cfg = SourceSection(type=SourceType.SFTP, connection="sftp_prod", schema="", table="complaint", sftp=sftp_sec)

            reader = SFTPReader(None, source_cfg)
            parsed_df = reader.read()

            self.assertIsNotNone(parsed_df)
            self.assertEqual(len(parsed_df), 3)
            self.assertIn("REFERENCE_NUMBER", parsed_df.columns)


if __name__ == "__main__":
    unittest.main()
