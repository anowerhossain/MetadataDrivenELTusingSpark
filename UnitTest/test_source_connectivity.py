"""
Unit Test Suite: Source System Connectivity & Credentials Resolver
Tests connection credential resolution and driver readiness for MySQL, Oracle, PostgreSQL, SQL Server, and SFTP.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from src.core.config import SourceSection, SourceType, ConfigError
from src.connectors.oracle import OracleConnectionResolver, OracleConnectionConfig
from src.connectors.mysql import MySQLConnectionResolver, MySQLConnectionConfig
from src.connectors.postgres import PostgresConnectionResolver, PostgresConnectionConfig
from src.connectors.sqlserver import SQLServerConnectionResolver, SQLServerConnectionConfig
from src.connectors.sftp import SFTPConnectionResolver, SFTPConnectionConfig


class TestSourceSystemConnectivity(unittest.TestCase):

    def setUp(self):
        self.env_backup = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env_backup)

    def test_mysql_connectivity_resolution(self):
        """Validates MySQL host, port, db, username, and password credential resolution."""
        os.environ["MYSQL_PROD_HOST"] = "mysql.company.local"
        os.environ["MYSQL_PROD_PORT"] = "3306"
        os.environ["MYSQL_PROD_DATABASE"] = "crm_db"
        os.environ["MYSQL_PROD_USERNAME"] = "db_user"
        os.environ["MYSQL_PROD_PASSWORD"] = "secret123"

        source_cfg = SourceSection(type=SourceType.MYSQL, connection="mysql_prod", schema="crm_db", table="complaint")
        conn_config: MySQLConnectionConfig = MySQLConnectionResolver.resolve(source_cfg)

        self.assertEqual(conn_config.connection_name, "mysql_prod")
        self.assertEqual(conn_config.username, "db_user")
        self.assertEqual(conn_config.driver, "com.mysql.cj.jdbc.Driver")
        self.assertIn("jdbc:mysql://mysql.company.local:3306/crm_db", conn_config.jdbc_url)
        self.assertEqual(conn_config.password, "secret123")

    def test_oracle_connectivity_resolution(self):
        """Validates Oracle Thin JDBC connection URL, host, port, service_name, and credentials resolution."""
        os.environ["ORACLE_PROD_HOST"] = "oracle.company.local"
        os.environ["ORACLE_PROD_PORT"] = "1521"
        os.environ["ORACLE_PROD_SERVICE_NAME"] = "ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "hr_user"
        os.environ["ORACLE_PROD_PASSWORD"] = "hr_pass"

        source_cfg = SourceSection(type=SourceType.ORACLE, connection="oracle_prod", schema="HR", table="EMPLOYEES")
        conn_config: OracleConnectionConfig = OracleConnectionResolver.resolve(source_cfg)

        self.assertEqual(conn_config.connection_name, "oracle_prod")
        self.assertEqual(conn_config.username, "hr_user")
        self.assertEqual(conn_config.driver, "oracle.jdbc.OracleDriver")
        self.assertIn("jdbc:oracle:thin:@//oracle.company.local:1521/ORCLPDB", conn_config.jdbc_url)

    def test_postgres_connectivity_resolution(self):
        """Validates PostgreSQL JDBC connection resolution."""
        os.environ["POSTGRES_PROD_HOST"] = "pg.company.local"
        os.environ["POSTGRES_PROD_PORT"] = "5432"
        os.environ["POSTGRES_PROD_DATABASE"] = "fin_db"
        os.environ["POSTGRES_PROD_USERNAME"] = "pg_user"
        os.environ["POSTGRES_PROD_PASSWORD"] = "pg_pass"

        source_cfg = SourceSection(type=SourceType.POSTGRESQL, connection="postgres_prod", schema="public", table="payments")
        conn_config: PostgresConnectionConfig = PostgresConnectionResolver.resolve(source_cfg)

        self.assertEqual(conn_config.connection_name, "postgres_prod")
        self.assertEqual(conn_config.username, "pg_user")
        self.assertEqual(conn_config.driver, "org.postgresql.Driver")
        self.assertIn("jdbc:postgresql://pg.company.local:5432/fin_db", conn_config.jdbc_url)

    def test_sqlserver_connectivity_resolution(self):
        """Validates MS SQL Server JDBC connection resolution."""
        os.environ["MSSQL_PROD_HOST"] = "sqlserver.company.local"
        os.environ["MSSQL_PROD_PORT"] = "1433"
        os.environ["MSSQL_PROD_DATABASE"] = "invoices_db"
        os.environ["MSSQL_PROD_USERNAME"] = "sql_user"
        os.environ["MSSQL_PROD_PASSWORD"] = "sql_pass"

        source_cfg = SourceSection(type=SourceType.SQLSERVER, connection="mssql_prod", schema="dbo", table="invoices")
        conn_config: SQLServerConnectionConfig = SQLServerConnectionResolver.resolve(source_cfg)

        self.assertEqual(conn_config.connection_name, "mssql_prod")
        self.assertEqual(conn_config.username, "sql_user")
        self.assertEqual(conn_config.driver, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        self.assertIn("jdbc:sqlserver://sqlserver.company.local:1433", conn_config.jdbc_url)

    def test_sftp_connectivity_resolution(self):
        """Validates SFTP SSH connection parameters resolution."""
        os.environ["SFTP_PROD_HOST"] = "sftp.bank.local"
        os.environ["SFTP_PROD_PORT"] = "22"
        os.environ["SFTP_PROD_USERNAME"] = "sftp_user"
        os.environ["SFTP_PROD_PASSWORD"] = "sftp_pass"

        source_cfg = SourceSection(type=SourceType.SFTP, connection="sftp_prod", schema="", table="invoices")
        conn_config: SFTPConnectionConfig = SFTPConnectionResolver.resolve(source_cfg)

        self.assertEqual(conn_config.connection_name, "sftp_prod")
        self.assertEqual(conn_config.host, "sftp.bank.local")
        self.assertEqual(conn_config.port, 22)
        self.assertEqual(conn_config.username, "sftp_user")


if __name__ == "__main__":
    unittest.main()
