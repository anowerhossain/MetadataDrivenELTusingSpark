"""
Connector Reader Factory & Connection Resolver Dispatcher Module.
Provides factory dispatchers for instantiating database readers and resolving connection credentials.
"""

from typing import Any, Optional
from src.core.config import SourceSection, SourceType, JDBCSection, ConfigError
from src.connectors.oracle import OracleReader, OracleConnectionResolver
from src.connectors.mysql import MySQLReader, MySQLConnectionResolver
from src.connectors.postgres import PostgresReader, PostgresConnectionResolver
from src.connectors.sqlserver import SQLServerReader, SQLServerConnectionResolver
from src.connectors.sftp import SFTPReader, SFTPConnectionResolver


class ReaderFactory:
    """Factory for instantiating source connector readers based on source.type."""

    @classmethod
    def get_reader(
        cls,
        spark_session: Any,
        source_config: SourceSection,
        jdbc_config: Optional[JDBCSection] = None
    ) -> Any:
        """Instantiates appropriate reader based on SourceType."""
        if not isinstance(source_config, SourceSection):
            raise ConfigError(f"Expected SourceSection configuration, got {type(source_config).__name__}.")

        if source_config.type == SourceType.ORACLE:
            return OracleReader(spark_session, source_config, jdbc_config=jdbc_config)
        elif source_config.type == SourceType.MYSQL:
            return MySQLReader(spark_session, source_config, jdbc_config=jdbc_config)
        elif source_config.type in (SourceType.POSTGRESQL, SourceType.POSTGRES):
            return PostgresReader(spark_session, source_config, jdbc_config=jdbc_config)
        elif source_config.type in (SourceType.SQLSERVER, SourceType.MSSQL):
            return SQLServerReader(spark_session, source_config, jdbc_config=jdbc_config)
        elif source_config.type == SourceType.SFTP:
            return SFTPReader(spark_session, source_config)
        else:
            raise ConfigError(f"Unsupported source reader type '{source_config.type.value}'.")


class ConnectionResolver:
    """Dispatches database credential resolution to specific connector resolvers."""

    @classmethod
    def resolve(cls, source_config: SourceSection) -> Any:
        """Resolves connection configuration based on source.type."""
        if not isinstance(source_config, SourceSection):
            raise ConfigError(f"Expected SourceSection configuration, got {type(source_config).__name__}.")

        if source_config.type == SourceType.ORACLE:
            return OracleConnectionResolver.resolve(source_config)
        elif source_config.type == SourceType.MYSQL:
            return MySQLConnectionResolver.resolve(source_config)
        elif source_config.type in (SourceType.POSTGRESQL, SourceType.POSTGRES):
            return PostgresConnectionResolver.resolve(source_config)
        elif source_config.type in (SourceType.SQLSERVER, SourceType.MSSQL):
            return SQLServerConnectionResolver.resolve(source_config)
        elif source_config.type == SourceType.SFTP:
            return SFTPConnectionResolver.resolve(source_config)
        else:
            raise ConfigError(f"Unsupported source connector type '{source_config.type.value}'.")
