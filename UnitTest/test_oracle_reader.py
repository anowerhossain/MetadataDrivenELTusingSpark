import os
import unittest
from unittest.mock import MagicMock

from src.core.config import (
    SourceSection,
    SourceType,
    LoadSection,
    LoadType,
    JDBCSection,
    SourceExtractionSection,
    ConfigError,
)
from src.connectors.oracle import OracleReader


class TestOracleReader(unittest.TestCase):

    def setUp(self):
        self.env_backup = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env_backup)

    def test_full_table_name_formatting(self):
        mock_spark = MagicMock()
        source_config_with_schema = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
        )
        reader = OracleReader(mock_spark, source_config_with_schema)
        self.assertEqual(reader.get_full_table_name(), "BANK.CUSTOMER")

    def test_build_incremental_query_default_columns(self):
        mock_spark = MagicMock()
        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
        )
        reader = OracleReader(mock_spark, source_config)

        query1 = reader.build_incremental_query("BANK.CUSTOMER", "UPDATED_AT", "2026-01-01 00:00:00")
        self.assertEqual(query1, "(SELECT * FROM BANK.CUSTOMER WHERE UPDATED_AT > '2026-01-01 00:00:00') AS incremental_src")

    def test_build_incremental_query_with_configured_columns(self):
        mock_spark = MagicMock()
        extraction_sec = SourceExtractionSection(columns=["CUSTOMER_ID", "CUSTOMER_NAME", "STATUS", "BALANCE", "UPDATED_AT"])
        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
            extraction=extraction_sec,
        )
        reader = OracleReader(mock_spark, source_config)

        query = reader.build_incremental_query("BANK.CUSTOMER", "UPDATED_AT", "2026-01-01 00:00:00")
        expected_sql = "(SELECT CUSTOMER_ID, CUSTOMER_NAME, STATUS, BALANCE, UPDATED_AT FROM BANK.CUSTOMER WHERE UPDATED_AT > '2026-01-01 00:00:00') AS incremental_src"
        self.assertEqual(query, expected_sql)

    def test_read_with_configured_columns_full_mode(self):
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        mock_spark = MagicMock()
        mock_reader_chain = MagicMock()
        mock_df = MagicMock()

        mock_spark.read.format.return_value = mock_reader_chain
        mock_reader_chain.options.return_value = mock_reader_chain
        mock_reader_chain.load.return_value = mock_df

        extraction_sec = SourceExtractionSection(columns=["CUSTOMER_ID", "CUSTOMER_NAME", "BALANCE"])
        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
            extraction=extraction_sec,
        )

        reader = OracleReader(mock_spark, source_config)
        df = reader.read()

        self.assertEqual(df, mock_df)
        kwargs = mock_reader_chain.options.call_args[1]
        self.assertEqual(kwargs["dbtable"], "(SELECT CUSTOMER_ID, CUSTOMER_NAME, BALANCE FROM BANK.CUSTOMER) AS full_src")

    def test_read_with_nested_source_jdbc_partitioning(self):
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        mock_spark = MagicMock()
        mock_reader_chain = MagicMock()
        mock_df = MagicMock()

        mock_spark.read.format.return_value = mock_reader_chain
        mock_reader_chain.options.return_value = mock_reader_chain
        mock_reader_chain.load.return_value = mock_df

        source_jdbc = JDBCSection(
            partition_column="CUSTOMER_ID",
            num_partitions=8,
            fetch_size=10000,
        )
        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
            jdbc=source_jdbc,
        )

        reader = OracleReader(mock_spark, source_config)
        df = reader.read()

        self.assertEqual(df, mock_df)
        kwargs = mock_reader_chain.options.call_args[1]
        self.assertEqual(kwargs["partitionColumn"], "CUSTOMER_ID")
        self.assertEqual(kwargs["numPartitions"], "8")
        self.assertEqual(kwargs["fetchSize"], "10000")

    def test_read_with_jdbc_partitioning(self):
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        mock_spark = MagicMock()
        mock_reader_chain = MagicMock()
        mock_df = MagicMock()

        mock_spark.read.format.return_value = mock_reader_chain
        mock_reader_chain.options.return_value = mock_reader_chain
        mock_reader_chain.load.return_value = mock_df

        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
        )
        jdbc_config = JDBCSection(
            partition_column="CUSTOMER_ID",
            lower_bound=1,
            upper_bound=10000000,
            num_partitions=8,
            fetch_size=10000,
        )

        reader = OracleReader(mock_spark, source_config, jdbc_config=jdbc_config)
        df = reader.read()

        self.assertEqual(df, mock_df)
        mock_spark.read.format.assert_called_once_with("jdbc")

        kwargs = mock_reader_chain.options.call_args[1]
        self.assertEqual(kwargs["partitionColumn"], "CUSTOMER_ID")
        self.assertEqual(kwargs["lowerBound"], "1")
        self.assertEqual(kwargs["upperBound"], "10000000")
        self.assertEqual(kwargs["numPartitions"], "8")
        self.assertEqual(kwargs["fetchSize"], "10000")

    def test_read_without_jdbc_partitioning(self):
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        mock_spark = MagicMock()
        mock_reader_chain = MagicMock()
        mock_df = MagicMock()

        mock_spark.read.format.return_value = mock_reader_chain
        mock_reader_chain.options.return_value = mock_reader_chain
        mock_reader_chain.load.return_value = mock_df

        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
        )

        reader = OracleReader(mock_spark, source_config, jdbc_config=None)
        df = reader.read()

        self.assertEqual(df, mock_df)
        kwargs = mock_reader_chain.options.call_args[1]
        self.assertNotIn("partitionColumn", kwargs)


if __name__ == "__main__":
    unittest.main()
