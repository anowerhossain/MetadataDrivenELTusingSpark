import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd

from src.core.config import (
    ConfigParser,
    JobConfig,
    SourceSection,
    SourceType,
    SourceSFTPSection,
    TargetSection,
    TargetType,
    LoadSection,
    LoadType,
)
from src.connectors.sftp import (
    SFTPConnectionResolver,
    SFTPConnectionConfig,
    SFTPFileMetadata,
    SFTPAuditLogger,
    SFTPReader,
)
from src.connectors.factory import ConnectionResolver, ReaderFactory
from main import validate_config_file


class TestSFTPReaderModule(unittest.TestCase):

    def setUp(self):
        self.env_backup = dict(os.environ)
        self.temp_dir = tempfile.mkdtemp()

        # Create sample test files
        self.csv_path = os.path.join(self.temp_dir, "test_invoices_2026.csv")
        with open(self.csv_path, "w", encoding="utf-8") as f:
            f.write("INVOICE_ID,CUSTOMER_ID,TOTAL_AMOUNT,STATUS\n")
            f.write("INV-1001,CUST-501,1500.50,PAID\n")
            f.write("INV-1002,CUST-502,299.99,PENDING\n")

        self.excel_path = os.path.join(self.temp_dir, "test_settlements_2026.xlsx")
        df_excel = pd.DataFrame([
            {"SETTLE_ID": "SETTL-901", "CARD_NO": "4532xxxx1122", "AMOUNT": 5000.00},
            {"SETTLE_ID": "SETTL-902", "CARD_NO": "4532xxxx3344", "AMOUNT": 1250.75},
        ])
        df_excel.to_excel(self.excel_path, index=False, sheet_name="Settlements")

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env_backup)
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_sftp_connection_resolver_from_env(self):
        os.environ["SFTP_PROD_HOST"] = "sftp.bank.local"
        os.environ["SFTP_PROD_PORT"] = "2222"
        os.environ["SFTP_PROD_USERNAME"] = "sftp_user"
        os.environ["SFTP_PROD_PASSWORD"] = "SecretSftpPassword123!"

        source_config = SourceSection(
            type=SourceType.SFTP,
            connection="sftp_prod",
            schema="sftp",
            table="invoices",
        )

        conn = ConnectionResolver.resolve(source_config)
        self.assertIsInstance(conn, SFTPConnectionConfig)
        self.assertEqual(conn.host, "sftp.bank.local")
        self.assertEqual(conn.port, 2222)
        self.assertEqual(conn.username, "sftp_user")
        self.assertEqual(conn.password, "SecretSftpPassword123!")

    def test_sftp_file_metadata_structure(self):
        meta = SFTPFileMetadata(
            file_name="test_invoices_2026.csv",
            file_path="/remote/test_invoices_2026.csv",
            file_size=1024,
            last_modified="2026-08-13 12:00:00",
            file_owner=None  # Must be None if not provided by SFTP, DO NOT guess
        )
        self.assertEqual(meta.file_name, "test_invoices_2026.csv")
        self.assertIsNone(meta.file_owner)

    def test_sftp_csv_reader_parsing_and_metadata(self):
        sftp_sec = SourceSFTPSection(
            path=self.temp_dir,
            file_pattern="*.csv",
            file_format="csv",
            delimiter=",",
            header=True,
            encoding="utf-8"
        )
        source_config = SourceSection(
            type=SourceType.SFTP,
            connection="sftp_prod",
            schema="sftp",
            table="invoices",
            sftp=sftp_sec
        )

        reader = ReaderFactory.get_reader(spark_session=None, source_config=source_config)
        self.assertIsInstance(reader, SFTPReader)

        df = reader.read()
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertIn("INVOICE_ID", df.columns)

    def test_sftp_excel_reader_parsing(self):
        sftp_sec = SourceSFTPSection(
            path=self.temp_dir,
            file_pattern="*.xlsx",
            file_format="excel",
            sheet_name="Settlements",
            header_row=0
        )
        source_config = SourceSection(
            type=SourceType.SFTP,
            connection="sftp_prod",
            schema="sftp",
            table="settlements",
            sftp=sftp_sec
        )

        reader = SFTPReader(spark_session=None, source_config=source_config)
        df = reader.read()

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertIn("SETTLE_ID", df.columns)

    def test_sftp_deduplication_tracking(self):
        sftp_sec = SourceSFTPSection(
            path=self.temp_dir,
            file_pattern="*.csv",
            file_format="csv",
        )
        source_config = SourceSection(
            type=SourceType.SFTP,
            connection="sftp_prod",
            schema="sftp",
            table="invoices",
            sftp=sftp_sec
        )

        reader = SFTPReader(spark_session=None, source_config=source_config)
        df1 = reader.read()
        self.assertEqual(len(df1), 2)

        # Second read without file changes should skip already processed file
        with patch.object(reader.watermark_mgr, "get_last_watermark") as mock_get_wm:
            st = os.stat(self.csv_path)
            mtime_str = pd.to_datetime(st.st_mtime, unit="s", utc=True).strftime("%Y-%m-%d %H:%M:%S")
            mock_get_wm.return_value = f"{st.st_size}_{mtime_str}"
            
            # Should log skip message and return existing frame cleanly
            df2 = reader.read()
            self.assertIsNotNone(df2)

    def test_sftp_audit_logger_stub(self):
        meta = SFTPFileMetadata(
            file_name="test_invoices_2026.csv",
            file_path="/remote/test_invoices_2026.csv",
            file_size=512,
            last_modified="2026-08-13 12:00:00",
            file_owner="sftp_user"
        )
        # Verify no error raised when logging audit record
        SFTPAuditLogger.log_audit(
            spark_session=None,
            metadata=meta,
            target_table="hive.edw_bronze.sftp_invoices",
            record_count=2,
            status="SUCCESS",
            error_message=None
        )


if __name__ == "__main__":
    unittest.main()
