import os
import unittest
from unittest.mock import MagicMock, patch

from src.core.config import SourceSection, SourceType, LoadSection, LoadType, JDBCSection, ConfigError
from src.connectors.sqlserver import SQLServerConnectionResolver, SQLServerConnectionConfig, SQLServerReader
from src.connectors.factory import ReaderFactory


class TestSQLServerReader(unittest.TestCase):

    def setUp(self):
        self.env_backup = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env_backup)

    def test_sqlserver_connection_resolver_success(self):
        os.environ["SQLSERVER_PROD_JDBC_URL"] = "jdbc:sqlserver://localhost:1433;databaseName=erp_db;encrypt=true;trustServerCertificate=true;"
        os.environ["SQLSERVER_PROD_USERNAME"] = "mssql_user"
        os.environ["SQLSERVER_PROD_PASSWORD"] = "mssql_secret"

        source_cfg = SourceSection(type=SourceType.SQLSERVER, connection="sqlserver_prod", schema="dbo", table="invoices")
        conn_cfg = SQLServerConnectionResolver.resolve(source_cfg)

        self.assertEqual(conn_cfg.connection_name, "sqlserver_prod")
        self.assertEqual(conn_cfg.username, "mssql_user")
        self.assertIn("jdbc:sqlserver://localhost:1433", conn_cfg.jdbc_url)
        self.assertEqual(conn_cfg.driver, "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    def test_sqlserver_connection_resolver_missing_env_raises_config_error(self):
        os.environ.clear()
        source_cfg = SourceSection(type=SourceType.SQLSERVER, connection="sqlserver_prod", schema="dbo", table="invoices")

        with self.assertRaises(ConfigError) as ctx:
            SQLServerConnectionResolver.resolve(source_cfg)

        self.assertIn("Missing environment variable 'SQLSERVER_PROD_JDBC_URL'", str(ctx.exception))

    def test_reader_factory_creates_sqlserver_reader(self):
        os.environ["SQLSERVER_PROD_JDBC_URL"] = "jdbc:sqlserver://localhost:1433;databaseName=erp_db"
        os.environ["SQLSERVER_PROD_USERNAME"] = "mssql_user"
        os.environ["SQLSERVER_PROD_PASSWORD"] = "mssql_secret"

        mock_spark = MagicMock()
        source_cfg = SourceSection(type=SourceType.SQLSERVER, connection="sqlserver_prod", schema="dbo", table="invoices")

        reader = ReaderFactory.get_reader(mock_spark, source_cfg)
        self.assertIsInstance(reader, SQLServerReader)

    @patch("src.connectors.sqlserver.SQLServerConnectionResolver.resolve")
    def test_sqlserver_reader_full_read(self, mock_resolve):
        mock_conn = SQLServerConnectionConfig(
            connection_name="sqlserver_prod",
            jdbc_url="jdbc:sqlserver://localhost:1433;databaseName=erp_db",
            username="mssql_user",
            password="mssql_secret",
            driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
        )
        mock_resolve.return_value = mock_conn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df

        source_cfg = SourceSection(type=SourceType.SQLSERVER, connection="sqlserver_prod", schema="dbo", table="invoices")
        reader = SQLServerReader(mock_spark, source_cfg)

        res_df = reader.read(load_config=LoadSection(type=LoadType.FULL))

        self.assertEqual(res_df, mock_df)
        mock_spark.read.format.assert_called_once_with("jdbc")

    @patch("src.connectors.sqlserver.SQLServerConnectionResolver.resolve")
    def test_sqlserver_reader_incremental_read(self, mock_resolve):
        mock_conn = SQLServerConnectionConfig(
            connection_name="sqlserver_prod",
            jdbc_url="jdbc:sqlserver://localhost:1433;databaseName=erp_db",
            username="mssql_user",
            password="mssql_secret",
            driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
        )
        mock_resolve.return_value = mock_conn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df

        source_cfg = SourceSection(type=SourceType.SQLSERVER, connection="sqlserver_prod", schema="dbo", table="invoices")
        reader = SQLServerReader(mock_spark, source_cfg)

        load_cfg = LoadSection(type=LoadType.INCREMENTAL, watermark_column="created_at")
        res_df = reader.read(load_config=load_cfg, last_watermark="2026-08-10 10:00:00")

        self.assertEqual(res_df, mock_df)


if __name__ == "__main__":
    unittest.main()
