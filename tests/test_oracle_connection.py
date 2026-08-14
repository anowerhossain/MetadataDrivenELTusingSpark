import os
import unittest

from src.core.config import SourceSection, SourceType, ConfigError
from src.connectors.oracle import (
    OracleConnectionConfig,
    OracleConnectionResolver,
)


class TestOracleConnectionResolver(unittest.TestCase):

    def setUp(self):
        # Store clean state of environment variables
        self.env_backup = dict(os.environ)

    def tearDown(self):
        # Restore environment variables
        os.environ.clear()
        os.environ.update(self.env_backup)

    def test_resolve_valid_connection_via_jdbc_url(self):
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
        )

        conn = OracleConnectionResolver.resolve(source_config)
        self.assertEqual(conn.connection_name, "oracle_prod")
        self.assertEqual(conn.jdbc_url, "jdbc:oracle:thin:@//localhost:1521/ORCLPDB")
        self.assertEqual(conn.username, "BANK_USER")
        self.assertEqual(conn.password, "SecretPassword123!")
        self.assertEqual(conn.driver, "oracle.jdbc.OracleDriver")

    def test_password_masking_in_repr_and_str(self):
        conn = OracleConnectionConfig(
            connection_name="oracle_prod",
            jdbc_url="jdbc:oracle:thin:@//localhost:1521/ORCLPDB",
            username="BANK_USER",
            password="SecretPassword123!",
        )

        repr_str = repr(conn)
        str_str = str(conn)

        self.assertNotIn("SecretPassword123!", repr_str)
        self.assertNotIn("SecretPassword123!", str_str)
        self.assertIn("password='***'", repr_str)
        self.assertIn("password='***'", str_str)

    def test_resolve_via_host_port_service(self):
        os.environ["ORACLE_PROD_HOST"] = "oracle-db-server.company.com"
        os.environ["ORACLE_PROD_PORT"] = "1521"
        os.environ["ORACLE_PROD_SERVICE_NAME"] = "ORCLPDB"
        os.environ["ORACLE_PROD_USER"] = "HR"
        os.environ["ORACLE_PROD_PASSWORD"] = "MySecurePassword"

        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="HR",
            table="EMPLOYEES",
        )

        conn = OracleConnectionResolver.resolve(source_config)
        self.assertEqual(
            conn.jdbc_url,
            "jdbc:oracle:thin:@//oracle-db-server.company.com:1521/ORCLPDB"
        )
        self.assertEqual(conn.username, "HR")
        self.assertEqual(conn.password, "MySecurePassword")

    def test_missing_jdbc_url_raises_config_error(self):
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
        )

        with self.assertRaises(ConfigError) as ctx:
            OracleConnectionResolver.resolve(source_config)
        self.assertIn("Missing environment variable 'ORACLE_PROD_JDBC_URL'", str(ctx.exception))

    def test_missing_username_raises_config_error(self):
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_PASSWORD"] = "SecretPassword123!"

        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
        )

        with self.assertRaises(ConfigError) as ctx:
            OracleConnectionResolver.resolve(source_config)
        self.assertIn("Missing environment variable 'ORACLE_PROD_USERNAME'", str(ctx.exception))

    def test_missing_password_raises_config_error(self):
        os.environ["ORACLE_PROD_JDBC_URL"] = "jdbc:oracle:thin:@//localhost:1521/ORCLPDB"
        os.environ["ORACLE_PROD_USERNAME"] = "BANK_USER"

        source_config = SourceSection(
            type=SourceType.ORACLE,
            connection="oracle_prod",
            schema="BANK",
            table="CUSTOMER",
        )

        with self.assertRaises(ConfigError) as ctx:
            OracleConnectionResolver.resolve(source_config)
        self.assertIn("Missing environment variable 'ORACLE_PROD_PASSWORD'", str(ctx.exception))

    def test_to_jdbc_options_dict(self):
        conn = OracleConnectionConfig(
            connection_name="oracle_prod",
            jdbc_url="jdbc:oracle:thin:@//localhost:1521/ORCLPDB",
            username="BANK_USER",
            password="SecretPassword123!",
            fetch_size=5000,
        )

        opts = conn.to_jdbc_options()
        self.assertEqual(opts["url"], "jdbc:oracle:thin:@//localhost:1521/ORCLPDB")
        self.assertEqual(opts["user"], "BANK_USER")
        self.assertEqual(opts["password"], "SecretPassword123!")
        self.assertEqual(opts["driver"], "oracle.jdbc.OracleDriver")
        self.assertEqual(opts["fetchSize"], "5000")


if __name__ == "__main__":
    unittest.main()
