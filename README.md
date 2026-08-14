# Production PySpark Metadata-Driven ETL Framework for Apache Iceberg on CDP

[![Engine](https://img.shields.io/badge/PySpark-3.x-orange.svg)](https://spark.apache.org/)
[![Table Format](https://img.shields.io/badge/Apache%20Iceberg-Catalog-blue.svg)](https://iceberg.apache.org/)
[![Platform](https://img.shields.io/badge/Cloudera-CDP%207.x-red.svg)](https://www.cloudera.com/)
[![Web UI](https://img.shields.io/badge/Streamlit-Control%20Panel-brightgreen.svg)](https://streamlit.io/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Tests](https://img.shields.io/badge/Unit%20Tests-103%20Passing-success.svg)](tests/)

A production-grade, enterprise metadata-driven ETL framework built with **Python**, **PySpark 3.x**, and **Apache Iceberg**, engineered for high-throughput execution on **Cloudera Data Platform (CDP)** using **TOML configuration files**.

---

## 📌 Project Overview

This framework automates and standardizes high-volume enterprise data ingestion from **Oracle Database**, **Microsoft SQL Server**, **MySQL Database**, **PostgreSQL Database**, and **SFTP File Shares (CSV / Excel)** into **Apache Iceberg** target tables cataloged directly in **CDP Hive Metastore**.

Instead of writing custom PySpark scripts for every table, this framework is **100% metadata-driven**. Onboarding a new database table or file feed requires **zero code changes**—simply define a single `.toml` job configuration file specifying the source, extraction boundaries, column transformations, data quality rules, target partitioning, schema evolution, pre/postload hooks, retry policies, and multi-event email routing.

---

## 🌐 Streamlit Web UI Control Panel (`app.py`)

The project includes an interactive, browser-based **Streamlit Web Application** for building, editing, validating, executing, and rerunning job pipelines visually without touching the command line.

### Launching the Web UI:
```bash
python -m streamlit run app.py
```
*Access in browser at: `http://localhost:8501`*

### Web UI Core Modules:
1. **📊 Dashboard & Job Executor**: View filterable job catalogs, validate credentials (`--validate`), and trigger single or parallel batch runs with real-time streaming terminal logs.
2. **➕ Visual Job Builder**: Form-driven wizard to create new TOML configuration files for **Oracle**, **SQL Server**, **MySQL**, **PostgreSQL**, and **SFTP**.
   - Includes **Section 9 (`[email_notification]`)** with multi-event tabs:
     - 🔴 **Failure Alerts (`on_failure`)**: Recipient routing and HTML failure alert templates.
     - 🟢 **Success Notices (`on_success`)**: Recipient routing and green execution summary templates.
     - ⚠️ **Quality Failure Alerts (`on_quality_failure`)**: Recipient routing and data quality violation templates.
3. **✏️ TOML Code Editor**: Open any `.toml` file in `config/jobs/`, edit syntax live in browser, validate schemas, and save.
4. **🚨 Failure Recovery Center**: Inspect timestamped failure marker files in `failed_jobs/YYYYMMDD/`, read error tracebacks, and launch single-click deduplicated reruns.

---

## 🏗️ Architecture & Component Workflow

```text
                     TOML Job Config (config/jobs/*.toml)
                                      │
                                      ▼
  Database / SFTP Connection Resolver (Oracle / SQL Server / MySQL / Postgres / SFTP)
                                      │
                                      ▼
                 Pre-flight Connection Check (--validate)
                                      │
                                      ▼
                       CDP SparkSession Factory
                                      │
                                      ▼
                Step 1: Preload Hooks ([preload] operations)
                ├── validate_source (Credential & table check)
                ├── validate_target (Iceberg catalog check)
                └── check_watermark (Retrieve previous state)
                                      │
                                      ▼
                Step 2: Source Extraction (ReaderFactory)
                ├── JDBC Parallel Splitting ([source.jdbc])
                ├── SFTP Ingestion (CSV / Excel .xlsx)
                └── Watermark Filtering ([load.incremental])
                                      │
                                      ▼
                Step 3: Data Transformation (DataTransformer)
                ├── Column Renaming ([transform.rename])
                ├── Data Type Casting ([transform.cast])
                ├── Derived Columns ([transform.derived])
                └── DWH Audit Injections (insert_ts, updated_ts, run_id, user)
                                      │
                                      ▼
                Step 4: Data Quality Validation (DataQualityValidator)
                ├── null_check (Fail fast on unexpected nulls)
                ├── unique_check (Verify primary key uniqueness)
                └── minimum_rows (Threshold verification)
                                      │
                                      ▼
                Step 5: Iceberg Target Write & Schema Evolution
                ├── Additive Schema Evolution ([schema] evolution/add_columns)
                ├── Target Partitioning ([target.partition] days/months/years)
                ├── Full Overwrite / Append
                └── Merge Upsert (MERGE INTO using [keys] merge_keys)
                                      │
                                      ▼
                Step 6: Postload Hooks ([postload] operations)
                ├── update_watermark (Committed ONLY after write success)
                ├── compact_table (Iceberg rewrite_data_files)
                └── refresh_metadata (REFRESH TABLE in Hive Catalog)
                                      │
                                      ▼
           Multi-Event Email Notification & Telemetry Logging
           ├── on_failure (Route error tracebacks to DevOps/On-Call)
           ├── on_success (Route completion reports to Business Owners)
           └── on_quality_failure (Route QA warnings to Data Governance)
```

---

## 📧 Reusable Email Template Engine & Multi-Event Routing

The framework features a dedicated **Email Template Engine** (`src/helpers/email_template.py`) that separates template formatting from email transport and supports dynamic HTML tables and event routing.

### Built-in HTML Alert Presets:
1. **`job_failed`**: Red header banner, job metadata, error traceback box, and optional data table.
2. **`job_success`**: Green header banner, execution metrics (rows read, rows written, duration).
3. **`data_quality_failed`**: Orange warning banner, quality rule details, and sample invalid records HTML table.
4. **`sla_breached`**: Yellow SLA alert banner, target completion time vs actual run duration.
5. **`missing_file`**: Red SFTP file alert banner, expected directory path and file pattern.
6. **`data_anomaly`**: Purple anomaly alert banner and sample anomalous data table.

### Multi-Event TOML Routing Syntax:
```toml
[email_notification]
enabled = true

[[email_notification.events]]
event = "on_failure"
enabled = true
to = ["devops@company.com", "oncall@company.com"]
template = "job_failed"
subject_prefix = "[CRITICAL FAILURE]"

[[email_notification.events]]
event = "on_success"
enabled = true
to = ["business-owner@company.com"]
template = "job_success"
subject_prefix = "[SUCCESS NOTICE]"

[[email_notification.events]]
event = "on_quality_failure"
enabled = true
to = ["data-qa@company.com"]
template = "data_quality_failed"
subject_prefix = "[DATA QUALITY ALERT]"
```

---

## ⚡ Dual-Level Parallelism Architecture

The framework is engineered to scale across enterprise data lakes using **two complementary levels of parallelism**:

```text
                       spark-submit main.py --config-dir config/jobs/ --parallel 4
                                                     │
       ┌───────────────────────┬─────────────────────┴─────────────────────┬───────────────────────┐
       ▼                       ▼                                           ▼                       ▼
Worker Thread 1         Worker Thread 2                             Worker Thread 3         Worker Thread 4
(Job: customer.toml)    (Job: sqlserver_inv.toml)                   (Job: sftp_invoices.toml)(Job: mysql_orders.toml)
 (Oracle Database)       (MS SQL Server)                             (SFTP Server)            (MySQL Database)
       │                       │                                           │                       │
Spark JDBC Splits       Spark JDBC Splits                           SFTP CSV/Excel Read     Spark JDBC Splits
([source.jdbc] n=8)     ([source.jdbc] n=4)                         (Pandas/Spark CSV)      ([source.jdbc] n=4)
 ┌───┬───┬───┬───┐       ┌───┬───┬───┬───┐                                                   ┌───┬───┬───┬───┐
 ▼   ▼   ▼   ▼       ▼   ▼   ▼   ▼                                                   ▼   ▼   ▼   ▼
Oracle Queries 1..8     SQL Server Queries 1..4                                             MySQL Queries 1..4
 └───┴───┴───┴───────┴───┴───┴───┴───────────────────────────────────────────────────────────┴───┴───┴───┘
                                                     │
                                                     ▼
                                    YARN Executor Cluster Resource Pool
                                                     │
                                                     ▼
                                    Target Iceberg Tables in CDP Catalog
```

1. **Multi-Table Concurrency (Job Parallelism)**:
   - Command: `spark-submit main.py --config-dir config/jobs/ --parallel N`
   - Executes up to $N$ table ETL pipelines in parallel using `ThreadPoolExecutor`.
2. **Distributed JDBC Partitioning (Intra-Table Parallelism)**:
   - Configured in `[source.jdbc]` per TOML file (`partition_column`, `num_partitions`, `lower_bound`, `upper_bound`).
   - PySpark splits single massive tables across YARN executors for concurrent reading.

---

## 🎛️ Dynamic Spark Resource Auto-Tuner (`[resources]`)

The framework includes an automated resource calculation engine (`SparkResourceTuner`) that dynamically assigns Spark cluster submit options based on workload profiles (`light`, `medium`, `heavy`):

| Profile | Workload Trigger Condition | Executor Memory | Executor Cores | Shuffle Partitions | Off-Heap Memory | JDBC Fetch Size |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| 🟢 **`light`** | Incremental load or `num_partitions < 4` | **2 GB** | **2** | **10** | Disabled | **10,000** |
| 🟡 **`medium`** | Standard Full load or `4 <= num_partitions <= 8` | **4 GB** | **4** | **100** | Disabled | **25,000** |
| 🔴 **`heavy`** | Large table, `num_partitions > 8`, or `UPSERT` MERGE INTO | **8 GB** | **4** | **200** | **2 GB** | **50,000** |

*You can explicitly override any option in TOML:*
```toml
[resources]
profile = "auto"              # Options: "auto", "light", "medium", "heavy"
executor_memory = "8g"        # Optional explicit override
shuffle_partitions = 200      # Optional explicit override
fetch_size = 50000            # Optional explicit override
```

---

## 🔑 Environment Credentials Setup

Credentials are securely resolved from environment variables based on the connection section specified in the TOML file (`connection = "oracle_prod"`, `connection = "sftp_prod"`, etc.).

### Linux / CDP Cluster Terminal (Bash):
```bash
# Oracle Credentials
export ORACLE_PROD_JDBC_URL="jdbc:oracle:thin:@//oracle-scan.bank.local:1521/ORCLPDB"
export ORACLE_PROD_USERNAME="BANK_ETL_USER"
export ORACLE_PROD_PASSWORD="SuperSecretPassword123!"

# MS SQL Server Credentials
export SQLSERVER_PROD_JDBC_URL="jdbc:sqlserver://mssql-server.bank.local:1433;databaseName=erp_db;encrypt=true;trustServerCertificate=true;"
export SQLSERVER_PROD_USERNAME="mssql_etl_user"
export SQLSERVER_PROD_PASSWORD="MssqlSecretPassword123!"

# MySQL Credentials
export MYSQL_PROD_JDBC_URL="jdbc:mysql://mysql-server.bank.local:3306/sales_db"
export MYSQL_PROD_USERNAME="mysql_etl_user"
export MYSQL_PROD_PASSWORD="MySqlSecretPassword123!"

# PostgreSQL Credentials
export POSTGRES_PROD_JDBC_URL="jdbc:postgresql://postgres-server.bank.local:5432/fin_db"
export POSTGRES_PROD_USERNAME="pg_etl_user"
export POSTGRES_PROD_PASSWORD="PgSecretPassword123!"

# SFTP Server Credentials
export SFTP_PROD_HOST="sftp.company.com"
export SFTP_PROD_PORT="22"
export SFTP_PROD_USERNAME="sftp_etl_user"
export SFTP_PROD_PASSWORD="SftpSecretPassword123!"

# SMTP Email Credentials
export SMTP_SERVER="mail.company.com"
export SMTP_PORT="587"
export SMTP_USERNAME="noreply@company.com"
export SMTP_PASSWORD="SmtpSecretPassword123!"
export SMTP_USE_TLS="true"
```

---

## 🚀 Execution Guide on CDP (`spark-submit`)

### 1. Pre-flight Validation (Dry-Run Mode)
Validate configuration syntax, database connectivity, and schemas without launching Spark jobs:
```bash
# Single Job Validation
python main.py --config config/jobs/sqlserver_invoices.toml --validate

# All Jobs Parallel Validation
python main.py --config-dir config/jobs/ --parallel 4 --validate
```

### 2. Single Job Submission
Submit a single table pipeline to the CDP YARN cluster:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "ETL_Single_SQLServer_Invoices" \
  --num-executors 5 \
  --executor-cores 4 \
  --executor-memory 8g \
  --driver-memory 4g \
  --jars /opt/cloudera/parcels/CDH/lib/oracle/ojdbc8.jar,/opt/cloudera/parcels/CDH/lib/mysql/mysql-connector-java.jar,/opt/cloudera/parcels/CDH/lib/postgresql/postgresql-jdbc.jar,/opt/cloudera/parcels/CDH/lib/sqlserver/mssql-jdbc.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive.type=hive \
  main.py --config config/jobs/sqlserver_invoices.toml
```

### 3. Multi-Table Batch Submission
Execute all TOML pipelines in `config/jobs/` in parallel:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "ETL_Multi_Source_Batch" \
  --num-executors 20 \
  --executor-cores 4 \
  --executor-memory 8g \
  --driver-memory 4g \
  --jars /opt/cloudera/parcels/CDH/lib/oracle/ojdbc8.jar,/opt/cloudera/parcels/CDH/lib/mysql/mysql-connector-java.jar,/opt/cloudera/parcels/CDH/lib/postgresql/postgresql-jdbc.jar,/opt/cloudera/parcels/CDH/lib/sqlserver/mssql-jdbc.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive.type=hive \
  main.py --config-dir config/jobs/ --parallel 4
```

### 4. Failure Recovery Rerun (`--rerun-failed`)
When any job fails, the framework logs timestamped error markers under `failed_jobs/YYYYMMDD/`. Rerun all failed jobs for a given date with deduplicated execution:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "ETL_Rerun_Failed_Recovery" \
  main.py --rerun-failed 20260814 --parallel 4
```

---

## 🧪 Running Unit Tests

Run the full automated unit test suite locally (**103 tests** across 20 test modules):

```bash
python -m unittest discover tests
```

---

## 📁 Directory Structure & File Map

```text
Extraction_v2.1/
├── app.py                      # Streamlit Web Control Panel
├── main.py                     # CLI Entry Point Driver
├── requirements.txt            # Local Development Dependencies
├── README.md                   # Enterprise Technical Documentation
├── config/
│   └── jobs/                   # TOML Pipeline Configuration Directory
│       ├── customer.toml
│       ├── sqlserver_invoices.toml
│       ├── mysql_orders.toml
│       ├── postgres_payments.toml
│       └── sftp_invoices_csv.toml
├── failed_jobs/                # Audit Failure Marker Storage (YYYYMMDD/)
├── success_jobs/               # Audit Success Marker Storage (YYYYMMDD/)
├── src/                        # Core Framework Package
│   ├── connectors/             # Oracle, SQL Server, MySQL, Postgres, SFTP Readers & Resolvers
│   │   ├── factory.py
│   │   ├── oracle.py
│   │   ├── mysql.py
│   │   ├── postgres.py
│   │   ├── sqlserver.py
│   │   └── sftp.py
│   ├── core/                   # Config Parser, Transformer, Writer, Quality Validator, Hooks, State
│   │   ├── config.py
│   │   ├── transformer.py
│   │   ├── writer.py
│   │   ├── quality.py
│   │   ├── state.py
│   │   └── hooks.py
│   └── helpers/                # Dedicated Infrastructure Helpers
│       ├── __init__.py
│       ├── spark.py            # SparkSession Factory (CDP Hive Metastore & Iceberg)
│       ├── logger.py           # ETLLogger & JSON Telemetry
│       ├── failure.py          # Failure Marker Recorder
│       ├── success.py          # Success Marker Recorder
│       ├── retry.py            # Retry Handler with Backoff
│       ├── tuner.py            # Spark Resource & Fetch Size Tuner
│       ├── email_notification.py # Multi-Event Email Delivery Helper
│       └── email_template.py   # Reusable HTML Template Engine & HTML Tables
└── tests/                      # Automated Unit Test Suite (103 unit tests)
```
