# `config/jobs` - TOML Job Configurations Directory

This directory holds TOML configuration files that define end-to-end extraction and loading job pipelines.

## TOML Specification Sections

Every TOML job file consists of standard structured sections:

```toml
[job]
job_id = "customer_load"            # Unique identifier for telemetry & watermark tracking
job_name = "Customer Load Pipeline" # Human-readable name
enabled = true                     # Enable or disable job

[source]
type = "oracle"                     # RDBMS type: oracle, mysql, postgres, sqlserver
connection = "oracle_prod"          # Connection name matching environment variable prefix
schema = "BANK"                     # Source schema / database
table = "CUSTOMER"                  # Source table name

[load]
type = "full"                       # Load mode: full, incremental, upsert
watermark_column = "UPDATED_AT"     # Watermark column (required for incremental/upsert)

[target]
type = "iceberg"                    # Target engine
catalog = "hive"                    # Spark catalog name
database = "edw_bronze"            # Target Iceberg database
table = "customer"                  # Target Iceberg table

[transform.rename]
CUSTOMER_ID = "customer_id"         # Column renaming mapping

[transform.cast]
CUSTOMER_ID = "BIGINT"              # Data type casting mapping

[transform.derived]
source_system = "'ORACLE'"          # Calculated expression columns

[data_quality]
enabled = true                      # Enable quality checks
null_check = ["customer_id"]        # Primary key null checks
unique_check = ["customer_id"]      # Unique constraint check
minimum_rows = 1                    # Minimum required row threshold

[postload]
enabled = true                      # Enable post-load operations
operations = [
    "update_watermark",
    "compact_table",
    "expire_snapshots",
    "remove_orphan_files"           # Apache Iceberg orphan file removal procedure
]

[execution]
retries = 3                         # Number of retry attempts
retry_delay_seconds = 30            # Retry delay seconds
```

## Sample Job Files in this Directory

- `customer.toml`: Full load from Oracle to Iceberg.
- `customer_incremental.toml`: Incremental load from Oracle based on timestamp high-watermark.
- `customer_merge.toml`: Upsert / MERGE INTO Iceberg table.
- `mysql_orders.toml`: Incremental ingestion from MySQL.
- `postgres_payments.toml`: Incremental ingestion from PostgreSQL.
- `sqlserver_invoices.toml`: Incremental ingestion from SQL Server.

## CLI Usage

To validate or execute a job file:

```bash
# Single job dry-run validation
python main.py --config config/jobs/customer.toml --validate

# Single job execution
python main.py --config config/jobs/customer.toml

# Batch run all jobs in config/jobs/
python main.py --config-dir config/jobs/ --parallel 4
```
