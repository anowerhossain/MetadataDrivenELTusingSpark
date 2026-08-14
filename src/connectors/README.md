# `src/connectors` - Database JDBC Connectors & Factory Dispatchers

This sub-package contains database-specific JDBC connection resolvers, parallel PySpark readers, and dispatching factories for RDBMS source engines.

## Module Details

### 1. [`oracle.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/connectors/oracle.py)
- **Role**: Oracle Database Connector.
- **Key Responsibilities**:
  - `OracleConnectionConfig`: Validates Oracle JDBC connection parameters (`oracle_prod` connection profiles).
  - `OracleConnectionResolver`: Resolves credentials safely from environment variables (`ORACLE_PROD_JDBC_URL`, `ORACLE_PROD_HOST`, `ORACLE_PROD_PORT`, `ORACLE_PROD_SERVICE_NAME`, `ORACLE_PROD_USERNAME`, `ORACLE_PROD_PASSWORD`).
  - `OracleReader`: Reads data from Oracle into PySpark DataFrames via JDBC. Supports single-partition read and multi-partition parallel read (`partition_column`, `num_partitions`, `lower_bound`, `upper_bound`).

### 2. [`mysql.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/connectors/mysql.py)
- **Role**: MySQL Database Connector.
- **Key Responsibilities**:
  - `MySQLConnectionConfig`: Validates MySQL JDBC connection parameters.
  - `MySQLConnectionResolver`: Resolves MySQL connection credentials from environment variables (`MYSQL_PROD_JDBC_URL`, `MYSQL_PROD_HOST`, `MYSQL_PROD_PORT`, `MYSQL_PROD_DATABASE`, `MYSQL_PROD_USERNAME`, `MYSQL_PROD_PASSWORD`).
  - `MySQLReader`: Reads data from MySQL into PySpark DataFrames via JDBC. Supports full table reads and incremental watermark queries (`WHERE column > watermark`).

### 3. [`postgres.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/connectors/postgres.py)
- **Role**: PostgreSQL Database Connector.
- **Key Responsibilities**:
  - `PostgresConnectionConfig`: Validates PostgreSQL JDBC connection parameters (`org.postgresql.Driver`).
  - `PostgresConnectionResolver`: Resolves PostgreSQL connection credentials from environment variables (`POSTGRES_PROD_JDBC_URL`, `POSTGRES_PROD_HOST`, `POSTGRES_PROD_PORT`, `POSTGRES_PROD_DATABASE`, `POSTGRES_PROD_USERNAME`, `POSTGRES_PROD_PASSWORD`).
  - `PostgresReader`: Reads data from PostgreSQL schemas into PySpark DataFrames.

### 4. [`sqlserver.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/connectors/sqlserver.py)
- **Role**: Microsoft SQL Server Connector.
- **Key Responsibilities**:
  - `SQLServerConnectionConfig`: Validates SQL Server JDBC connection parameters (`com.microsoft.sqlserver.jdbc.SQLServerDriver`).
  - `SQLServerConnectionResolver`: Resolves SQL Server connection credentials from environment variables (`SQLSERVER_PROD_JDBC_URL`, `SQLSERVER_PROD_HOST`, `SQLSERVER_PROD_PORT`, `SQLSERVER_PROD_DATABASE`, `SQLSERVER_PROD_USERNAME`, `SQLSERVER_PROD_PASSWORD`).
  - `SQLServerReader`: Reads data from SQL Server tables/schemas into PySpark DataFrames.

### 5. [`factory.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/connectors/factory.py)
- **Role**: Connection & Reader Factory Dispatchers.
- **Key Responsibilities**:
  - `ConnectionResolver`: Factory dispatcher that routes connection validation to the appropriate database resolver (`oracle`, `mysql`, `postgres`, `sqlserver`).
  - `ReaderFactory`: Factory dispatcher that instantiates and returns the appropriate PySpark `Reader` implementation based on `source.type`.

---

## Import Example

```python
from src.connectors.factory import ConnectionResolver, ReaderFactory
from src.connectors.oracle import OracleReader, OracleConnectionResolver
from src.connectors.mysql import MySQLReader, MySQLConnectionResolver
from src.connectors.postgres import PostgresReader, PostgresConnectionResolver
from src.connectors.sqlserver import SQLServerReader, SQLServerConnectionResolver
```
