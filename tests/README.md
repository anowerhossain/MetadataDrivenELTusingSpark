# `tests` - Framework Unit Test Suite

This directory contains automated unit tests covering all components of the **Metadata-Driven PySpark ETL Framework**.

## Test Files Overview

| Test File | Target Component | Description |
| :--- | :--- | :--- |
| [`test_config_parser.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_config_parser.py) | `src.core.config` | Tests TOML parsing, environment variable expansion, and dataclass validation. |
| [`test_transformer.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_transformer.py) | `src.core.transformer` | Tests column renames, type casting, derived columns, and exclusion filtering. |
| [`test_iceberg_writer.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_iceberg_writer.py) | `src.core.writer` | Tests Iceberg write modes (append, overwrite, merge), schema evolution, compaction, and orphan file removal. |
| [`test_preload_handler.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_preload_handler.py) | `src.core.hooks` | Tests preload pre-flight checks and source DB connectivity verification. |
| [`test_postload_handler.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_postload_handler.py) | `src.core.hooks` | Tests postload watermark updates, snapshot expiration, compaction, and orphan file cleanup. |
| [`test_data_validator.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_data_validator.py) | `src.core.quality` | Tests data quality null checks, unique key checks, and minimum row count assertions. |
| [`test_watermark_manager.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_watermark_manager.py) | `src.core.state` | Tests high-watermark retrieval and persistence. |
| [`test_oracle_connection.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_oracle_connection.py) | `src.connectors.oracle` | Tests Oracle connection profile resolution and environment variable validation. |
| [`test_oracle_reader.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_oracle_reader.py) | `src.connectors.oracle` | Tests Oracle single-partition and multi-partition parallel JDBC extraction. |
| [`test_mysql_reader.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_mysql_reader.py) | `src.connectors.mysql` | Tests MySQL connection resolution and full/incremental JDBC reads. |
| [`test_postgres_reader.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_postgres_reader.py) | `src.connectors.postgres` | Tests PostgreSQL connection resolution and JDBC extraction. |
| [`test_sqlserver_reader.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_sqlserver_reader.py) | `src.connectors.sqlserver` | Tests Microsoft SQL Server connection resolution and JDBC extraction. |
| [`test_etl_logger.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_etl_logger.py) | `src.utils.logger` | Tests telemetry metrics tracking and JSON summary emission. |
| [`test_failure_handler.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_failure_handler.py) | `src.utils.failure` | Tests failure marker creation, deduplication, and cleanup. |
| [`test_spark_session.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_spark_session.py) | `src.utils.spark` | Tests SparkSession initialization and CDP catalog configuration. |
| [`test_retry_handler.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_retry_handler.py) | `src.utils.retry` | Tests retry logic, fast-fail handling, and exponential backoff calculations. |
| [`test_resource_tuner.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_tuner` | Tests workload profile resolution (`light`, `medium`, `heavy`, `auto`) and JDBC fetch size tuning. |
| [`test_audit_and_exclusions.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_audit_and_exclusions.py) | `src.core.transformer` | Tests DWH audit columns and exclusion column filtering. |
| [`test_pipeline_integration.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/tests/test_pipeline_integration.py) | `main.py` | End-to-end integration test of single-table and concurrent batch executions. |

---

## Running the Unit Tests

Execute the complete test suite from the project root:

```bash
python -m unittest discover -s tests
```

To run a specific test file:

```bash
python -m unittest tests/test_config_parser.py
```
