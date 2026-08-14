# Production PySpark Metadata-Driven ETL Framework for Apache Iceberg on CDP

[![Engine](https://img.shields.io/badge/PySpark-3.x-orange.svg)](https://spark.apache.org/)
[![Table Format](https://img.shields.io/badge/Apache%20Iceberg-Catalog-blue.svg)](https://iceberg.apache.org/)
[![Platform](https://img.shields.io/badge/Cloudera-CDP%207.x-red.svg)](https://www.cloudera.com/)
[![Web UI](https://img.shields.io/badge/Streamlit-Control%20Panel-brightgreen.svg)](https://streamlit.io/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Tests](https://img.shields.io/badge/Unit%20Tests-104%20Passing-success.svg)](tests/)

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
3. **✏️ TOML Code Editor**: Open any `.toml` file in `config/tasks/`, edit syntax live in browser, validate schemas, and save.
4. **🚨 Failure Recovery Center**: Inspect timestamped failure marker files in `failed_jobs/YYYYMMDD/`, read error tracebacks, and launch single-click deduplicated reruns.

---

## 🏗️ Architecture & Component Workflow

```text
                     TOML Job Config (config/tasks/*.toml)
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
           Multi-Event Email Notification & Audit Telemetry Logging
           ├── on_failure (Route error tracebacks to DevOps/On-Call)
           ├── on_success (Route completion reports to Business Owners)
           ├── on_quality_failure (Route QA warnings to Data Governance)
           └── etl_audit.etl_email_audit (Iceberg Audit Persistence)
```

---

## 📧 Reusable Email Template Engine & Iceberg Audit Table

The framework features a dedicated **Email Template Engine** (`src/helpers/email_template.py`) that separates template formatting from email transport and logs structured delivery audit telemetry into `etl_audit.etl_email_audit`.

### Iceberg Email Audit Table DDL (`sql/03_create_audit_tables_ddl.sql`):
```sql
CREATE TABLE IF NOT EXISTS etl_audit.etl_email_audit (
    notification_id     STRING          COMMENT 'Unique Email Notification Run UUID',
    job_id              STRING          COMMENT 'ETL Job ID (e.g. customer_load, sqlserver_invoices)',
    job_name            STRING          COMMENT 'Descriptive Job Name',
    run_id              STRING          COMMENT 'Execution Run ID',
    event_type          STRING          COMMENT 'Event Trigger (on_failure, on_success, on_quality_failure, etc.)',
    pipeline_status     STRING          COMMENT 'Pipeline Execution Status (FAILED, SUCCESS, etc.)',
    sender              STRING          COMMENT 'Sender Email Address (From)',
    recipients_to       STRING          COMMENT 'Comma-separated Primary Recipients (To)',
    recipients_cc       STRING          COMMENT 'Comma-separated CC Recipients',
    subject             STRING          COMMENT 'Evaluated Email Subject Line',
    template_used       STRING          COMMENT 'Template Used (job_failed, job_success, etc.)',
    email_status        STRING          COMMENT 'Email Delivery Status (SENT, FAILED, DISABLED, NO_RECIPIENTS)',
    error_message       STRING          COMMENT 'SMTP Delivery Error or Pipeline Error Summary',
    sent_timestamp      TIMESTAMP       COMMENT 'Timestamp when notification attempt occurred (UTC)'
)
USING iceberg
PARTITIONED BY (days(sent_timestamp), email_status)
TBLPROPERTIES ('format-version' = '2');
```

---

## ⚡ Dual-Level Parallelism Architecture

The framework is engineered to scale across enterprise data lakes using **two complementary levels of parallelism**:

```text
                       spark-submit main.py --config-dir config/tasks/ --parallel 4
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

---

## 🧪 Running Unit Tests

Run the full automated unit test suite locally (**104 tests** across 20 test modules):

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
├── failed_jobs/                # Audit Failure Marker Storage (YYYYMMDD/)
├── success_jobs/               # Audit Success Marker Storage (YYYYMMDD/)
├── sql/
│   ├── 01_create_databases.sql
│   ├── 02_create_target_tables_ddl.sql
│   ├── 03_create_audit_tables_ddl.sql # Includes etl_audit.etl_email_audit DDL
│   └── 04_sample_data_dml.sql
├── src/                        # Core Framework Package
│   ├── connectors/             # Oracle, SQL Server, MySQL, Postgres, SFTP Readers & Resolvers
│   ├── core/                   # Config Parser, Transformer, Writer, Quality Validator, Hooks, State
│   └── helpers/                # Dedicated Infrastructure Helpers
│       ├── __init__.py
│       ├── spark.py            # SparkSession Factory (CDP Hive Metastore & Iceberg)
│       ├── logger.py           # ETLLogger & JSON Telemetry
│       ├── failure.py          # Failure Marker Recorder
│       ├── success.py          # Success Marker Recorder
│       ├── retry.py            # Retry Handler with Backoff
│       ├── tuner.py            # Spark Resource & Fetch Size Tuner
│       ├── email_notification.py # Email Delivery & etl_email_audit Persistence
│       └── email_template.py   # Reusable HTML Template Engine & HTML Tables
└── tests/                      # Automated Unit Test Suite (104 unit tests)
```
