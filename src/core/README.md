# `src/core` - Core ETL Framework Engine

This sub-package contains the foundational components that drive the metadata-driven PySpark ETL pipeline execution lifecycle.

## Module Details

### 1. [`config.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/core/config.py)
- **Role**: Configuration Schema & TOML Parser.
- **Key Responsibilities**:
  - Defines immutable dataclass schemas for pipeline sections (`JobSection`, `SourceSection`, `LoadSection`, `TargetSection`, `KeysSection`, `TransformSection`, `AuditColumnsSection`, `SchemaSection`, `PreloadSection`, `PostloadSection`, `QualitySection`, `ExecutionSection`, `RetrySection`, `ResourceSection`).
  - Implements `ConfigParser` to parse TOML configuration files with environment variable substitution (`${ENV_VAR:default}`).
  - Enforces strict validation rules (e.g. required sections, supported load types, connection credential validation).

### 2. [`transformer.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/core/transformer.py)
- **Role**: Data Transformation Engine.
- **Key Responsibilities**:
  - Implements `DataTransformer` to apply TOML-driven transformations to PySpark DataFrames.
  - Handles column renames (`[transform.rename]`).
  - Executes data type casting (`[transform.cast]`) with Spark SQL type resolution.
  - Computes derived column expressions (`[transform.derived]`).
  - Applies sensitive column exclusion filtering (`exclude`).
  - Appends DWH audit metadata columns (`dwh_insert_ts`, `dwh_updated_ts`, `dwh_etl_run_id`, `dwh_job_user`) localized to Bangladesh Standard Time (BST, `Asia/Dhaka`).

### 3. [`writer.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/core/writer.py)
- **Role**: Apache Iceberg Target Writer.
- **Key Responsibilities**:
  - Implements `IcebergWriter` for writing DataFrames into Apache Iceberg tables.
  - Supports `full` append/overwrite modes and `upsert` / `merge` MERGE INTO operations.
  - Handles safe additive schema evolution (`ALTER TABLE ... ADD COLUMNS`).
  - Applies table partitioning (e.g. `days(UPDATED_AT)`, `bucket(8, ID)`).
  - Executes Iceberg maintenance procedures:
    - **`rewrite_data_files`**: Compaction of small data files.
    - **`expire_snapshots`**: Removal of old table snapshots.
    - **`remove_orphan_files`**: Cleanup of unreferenced data files older than a specified retention period.

### 4. [`hooks.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/core/hooks.py)
- **Role**: Preload and Postload Execution Hooks.
- **Key Responsibilities**:
  - Implements `PreloadHandler`: Validates source DB connectivity, target table existence, and retrieves previous high-watermarks before extraction.
  - Implements `PostloadHandler`: Updates high-watermark state, triggers metadata refresh, and executes post-load Iceberg maintenance procedures (`compact_table`, `expire_snapshots`, `remove_orphan_files`).

### 5. [`quality.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/core/quality.py)
- **Role**: Data Quality Validation Engine.
- **Key Responsibilities**:
  - Implements `DataQualityValidator` and `QualityResult`.
  - Enforces `null_check` constraints on primary/business key columns.
  - Enforces `unique_check` constraints to detect duplicate records.
  - Validates minimum row count thresholds (`minimum_rows`).

### 6. [`state.py`](file:///C:/Users/SUNDARBAN%20IT/Documents/Extraction/src/core/state.py)
- **Role**: Watermark State Manager.
- **Key Responsibilities**:
  - Implements `WatermarkManager` for persisting incremental high-watermarks.
  - Supports Iceberg table property storage (`tblproperties`) and in-memory/file backends.

---

## Import Example

```python
from src.core.config import ConfigParser, JobConfig
from src.core.transformer import DataTransformer
from src.core.writer import IcebergWriter
from src.core.hooks import PreloadHandler, PostloadHandler
from src.core.quality import DataQualityValidator
from src.core.state import WatermarkManager
```
