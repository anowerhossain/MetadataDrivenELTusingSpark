import os
import unittest
from unittest.mock import MagicMock, patch

from src.core.config import SourceSection, SourceType, LoadSection, LoadType, JDBCSection, ConfigError
from src.connectors.postgres import PostgresConnectionResolver, PostgresConnectionConfig, PostgresReader
from src.connectors.factory import ReaderFactory


class TestPostgresReader(unittest.TestCase):

    def setUp(self):
        self.env_backup = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env_backup)

    def test_postgres_connection_resolver_success(self):
        os.environ["POSTGRES_PROD_JDBC_URL"] = "jdbc:postgresql://localhost:5432/fin_db"
        os.environ["POSTGRES_PROD_USERNAME"] = "pg_user"
        os.environ["POSTGRES_PROD_PASSWORD"] = "pg_secret"

        source_cfg = SourceSection(type=SourceType.POSTGRESQL, connection="postgres_prod", schema="public", table="payments")
        conn_cfg = PostgresConnectionResolver.resolve(source_cfg)

        self.assertEqual(conn_cfg.connection_name, "postgres_prod")
        self.assertEqual(conn_cfg.username, "pg_user")
        self.assertIn("jdbc:postgresql://localhost:5432/fin_db", conn_cfg.jdbc_url)
        self.assertEqual(conn_cfg.driver, "org.postgresql.Driver")

    def test_postgres_connection_resolver_missing_env_raises_config_error(self):
        os.environ.clear()
        source_cfg = SourceSection(type=SourceType.POSTGRESQL, connection="postgres_prod", schema="public", table="payments")

        with self.assertRaises(ConfigError) as ctx:
            PostgresConnectionResolver.resolve(source_cfg)

        self.assertIn("Missing environment variable 'POSTGRES_PROD_JDBC_URL'", str(ctx.exception))

    def test_reader_factory_creates_postgres_reader(self):
        os.environ["POSTGRES_PROD_JDBC_URL"] = "jdbc:postgresql://localhost:5432/fin_db"
        os.environ["POSTGRES_PROD_USERNAME"] = "pg_user"
        os.environ["POSTGRES_PROD_PASSWORD"] = "pg_secret"

        mock_spark = MagicMock()
        source_cfg = SourceSection(type=SourceType.POSTGRESQL, connection="postgres_prod", schema="public", table="payments")

        reader = ReaderFactory.get_reader(mock_spark, source_cfg)
        self.assertIsInstance(reader, PostgresReader)

    @patch("src.connectors.postgres.PostgresConnectionResolver.resolve")
    def test_postgres_reader_full_read(self, mock_resolve):
        mock_conn = PostgresConnectionConfig(
            connection_name="postgres_prod",
            jdbc_url="jdbc:postgresql://localhost:5432/fin_db",
            username="pg_user",
            password="pg_secret",
            driver="org.postgresql.Driver"
        )
        mock_resolve.return_value = mock_conn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df

        source_cfg = SourceSection(type=SourceType.POSTGRESQL, connection="postgres_prod", schema="public", table="payments")
        reader = PostgresReader(mock_spark, source_cfg)

        res_df = reader.read(load_config=LoadSection(type=LoadType.FULL))

        self.assertEqual(res_df, mock_df)
        mock_spark.read.format.assert_called_once_with("jdbc")

    @patch("src.connectors.postgres.PostgresConnectionResolver.resolve")
    def test_postgres_reader_incremental_read(self, mock_resolve):
        mock_conn = PostgresConnectionConfig(
            connection_name="postgres_prod",
            jdbc_url="jdbc:postgresql://localhost:5432/fin_db",
            username="pg_user",
            password="pg_secret",
            driver="org.postgresql.Driver"
        )
        mock_resolve.return_value = mock_conn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df

        source_cfg = SourceSection(type=SourceType.POSTGRESQL, connection="postgres_prod", schema="public", table="payments")
        reader = PostgresReader(mock_spark, source_cfg)

        load_cfg = LoadSection(type=LoadType.INCREMENTAL, watermark_column="created_at")
        res_df = reader.read(load_config=load_cfg, last_watermark="2026-08-10 10:00:00")

        self.assertEqual(res_df, mock_df)


if __name__ == "__main__":
    unittest.main()
