import os
import unittest
from unittest.mock import MagicMock, patch

from src.core.config import SourceSection, SourceType, LoadSection, LoadType, JDBCSection, ConfigError
from src.connectors.mysql import MySQLConnectionResolver, MySQLConnectionConfig, MySQLReader
from src.connectors.factory import ReaderFactory


class TestMySQLReader(unittest.TestCase):

    def setUp(self):
        self.env_backup = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env_backup)

    def test_mysql_connection_resolver_success(self):
        os.environ["MYSQL_PROD_JDBC_URL"] = "jdbc:mysql://localhost:3306/sales_db?useSSL=false"
        os.environ["MYSQL_PROD_USERNAME"] = "db_user"
        os.environ["MYSQL_PROD_PASSWORD"] = "secret"

        source_cfg = SourceSection(type=SourceType.MYSQL, connection="mysql_prod", schema="sales_db", table="orders")
        conn_cfg = MySQLConnectionResolver.resolve(source_cfg)

        self.assertEqual(conn_cfg.connection_name, "mysql_prod")
        self.assertEqual(conn_cfg.username, "db_user")
        self.assertIn("jdbc:mysql://localhost:3306/sales_db", conn_cfg.jdbc_url)
        self.assertEqual(conn_cfg.driver, "com.mysql.cj.jdbc.Driver")

    def test_mysql_connection_resolver_missing_env_raises_config_error(self):
        os.environ.clear()
        source_cfg = SourceSection(type=SourceType.MYSQL, connection="mysql_prod", schema="sales_db", table="orders")

        with self.assertRaises(ConfigError) as ctx:
            MySQLConnectionResolver.resolve(source_cfg)

        self.assertIn("Missing environment variable 'MYSQL_PROD_JDBC_URL'", str(ctx.exception))

    def test_reader_factory_creates_mysql_reader(self):
        os.environ["MYSQL_PROD_JDBC_URL"] = "jdbc:mysql://localhost:3306/sales_db"
        os.environ["MYSQL_PROD_USERNAME"] = "db_user"
        os.environ["MYSQL_PROD_PASSWORD"] = "secret"

        mock_spark = MagicMock()
        source_cfg = SourceSection(type=SourceType.MYSQL, connection="mysql_prod", schema="sales_db", table="orders")

        reader = ReaderFactory.get_reader(mock_spark, source_cfg)
        self.assertIsInstance(reader, MySQLReader)

    @patch("src.connectors.mysql.MySQLConnectionResolver.resolve")
    def test_mysql_reader_full_read(self, mock_resolve):
        mock_conn = MySQLConnectionConfig(
            connection_name="mysql_prod",
            jdbc_url="jdbc:mysql://localhost:3306/sales_db",
            username="db_user",
            password="secret",
            driver="com.mysql.cj.jdbc.Driver"
        )
        mock_resolve.return_value = mock_conn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df

        source_cfg = SourceSection(type=SourceType.MYSQL, connection="mysql_prod", schema="sales_db", table="orders")
        reader = MySQLReader(mock_spark, source_cfg)

        res_df = reader.read(load_config=LoadSection(type=LoadType.FULL))

        self.assertEqual(res_df, mock_df)
        mock_spark.read.format.assert_called_once_with("jdbc")

    @patch("src.connectors.mysql.MySQLConnectionResolver.resolve")
    def test_mysql_reader_incremental_read(self, mock_resolve):
        mock_conn = MySQLConnectionConfig(
            connection_name="mysql_prod",
            jdbc_url="jdbc:mysql://localhost:3306/sales_db",
            username="db_user",
            password="secret",
            driver="com.mysql.cj.jdbc.Driver"
        )
        mock_resolve.return_value = mock_conn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df

        source_cfg = SourceSection(type=SourceType.MYSQL, connection="mysql_prod", schema="sales_db", table="orders")
        reader = MySQLReader(mock_spark, source_cfg)

        load_cfg = LoadSection(type=LoadType.INCREMENTAL, watermark_column="updated_at")
        res_df = reader.read(load_config=load_cfg, last_watermark="2026-08-10 10:00:00")

        self.assertEqual(res_df, mock_df)


if __name__ == "__main__":
    unittest.main()
