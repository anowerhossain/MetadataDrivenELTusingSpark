# `src` - Framework Package Root

This directory contains the primary Python source code for the **Metadata-Driven PySpark ETL Framework**.

## Directory Overview

The source tree is organized into three logical, high-cohesion sub-packages:

```text
src/
├── core/           # Core ETL framework engine & lifecycle handlers
├── connectors/     # Database JDBC connection resolvers and PySpark readers
└── utils/          # Shared logging, telemetry, retry, tuning, & failure utilities
```

## Sub-Package Descriptions

### 1. [`core/`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/core/README.md)
Contains the core business and engine logic of the ETL framework:
- **`config.py`**: TOML configuration parser, dataclass schemas, environment variable resolution.
- **`transformer.py`**: DataFrame column renames, type casting, derived columns, and exclusion filtering.
- **`writer.py`**: Apache Iceberg write engine (Append, Overwrite, Merge), schema evolution, compaction, and orphan file cleanup.
- **`hooks.py`**: Preload and Postload lifecycle execution hooks.
- **`quality.py`**: Data quality checks (null key check, duplicate key check, minimum row count).
- **`state.py`**: Iceberg watermark state management.

### 2. [`connectors/`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/connectors/README.md)
Contains database-specific connection resolvers and JDBC PySpark readers:
- **`oracle.py`**: Oracle connection resolver & parallel JDBC reader.
- **`mysql.py`**: MySQL connection resolver & parallel JDBC reader.
- **`postgres.py`**: PostgreSQL connection resolver & parallel JDBC reader.
- **`sqlserver.py`**: Microsoft SQL Server connection resolver & parallel JDBC reader.
- **`factory.py`**: Reader factory and connection resolver dispatchers.

### 3. [`utils/`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/utils/README.md)
Contains shared utility functions and helper modules:
- **`logger.py`**: Framework logging and `ETLLogger` telemetry tracking.
- **`failure.py`**: Timestamped failure marker logging and `--rerun-failed` recovery.
- **`spark.py`**: `SparkSessionFactory` for creating CDP / Iceberg Spark sessions.
- **`retry.py`**: `RetryHandler` supporting exponential backoff retries for transient errors.
- **`tuner.py`**: `SparkResourceTuner` for automated cluster profile and fetch size auto-tuning.

---

## Import Best Practices

All components should be imported directly from their respective sub-packages:

```python
from src.core.config import ConfigParser, JobConfig
from src.core.transformer import DataTransformer
from src.core.writer import IcebergWriter
from src.connectors.factory import ReaderFactory
from src.utils.logger import setup_logger
```
