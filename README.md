# Production PySpark Metadata-Driven ETL Framework for Apache Iceberg on CDP

[![Engine](https://img.shields.io/badge/PySpark-3.x-orange.svg)](https://spark.apache.org/)
[![Table Format](https://img.shields.io/badge/Apache%20Iceberg-Catalog-blue.svg)](https://iceberg.apache.org/)
[![Platform](https://img.shields.io/badge/Cloudera-CDP%207.x-red.svg)](https://www.cloudera.com/)
[![Web UI](https://img.shields.io/badge/Streamlit-Control%20Panel-brightgreen.svg)](https://streamlit.io/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Tests](https://img.shields.io/badge/Unit%20Tests-112%20Passing-success.svg)](tests/)

A production-grade, enterprise metadata-driven ETL and task orchestration framework built with **Python**, **PySpark 3.x**, and **Apache Iceberg**, engineered for high-throughput execution on **Cloudera Data Platform (CDP)** using **TOML configuration files**.

---

## 📌 Project Overview

This framework automates and standardizes high-volume enterprise data ingestion from **Oracle Database**, **Microsoft SQL Server**, **MySQL Database**, **PostgreSQL Database**, and **SFTP File Shares (CSV / Excel)** into **Apache Iceberg** target tables cataloged directly in **CDP Hive Metastore**, as well as triggering external **Qlik Replicate**, **Qlik Sense**, and **Qlik NPrinting** data replication and reporting tasks.

Instead of writing custom PySpark scripts for every pipeline, this framework is **100% metadata-driven and task-centric**. Onboarding a new database table, file feed, or Qlik task requires **zero code changes**—simply define a single `.toml` task configuration file in `config/tasks/`.

---

## 🕸️ How to Create a Multi-Task Pipeline DAG (Luigi Orchestration)

In this framework, a **Job Pipeline (DAG)** is a combination of interconnected tasks (e.g. **Bronze Ingestion ➔ Silver Transformation ➔ Gold Analytics / Qlik Refresh**) linked dynamically using the `depends_on` metadata field. **Spotify Luigi** handles dependency resolution, concurrency, retries, and execution order.

### 3-Step Guide to Create a New Pipeline DAG:

#### 1️⃣ Step 1: Define the Root Task (Bronze Tier)
Create a `.toml` file in `config/tasks/` for raw ingestion. Leave `depends_on = []` empty so it runs immediately:

*File: `config/tasks/bronze_customers_load.toml`*
```toml
[task]
task_id = "bronze_customers_load"
task_name = "Bronze Raw Customers Ingestion"
type = "table_load"
enabled = true
depends_on = []  # <-- Root task (no dependencies)

[source]
type = "oracle"
connection = "oracle_prod"
schema = "CRM"
table = "CUSTOMERS"

[load]
type = "full"

[target]
catalog = "hive"
database = "bronze_db"
table = "raw_customers"
```

#### 2️⃣ Step 2: Define the Transformation Task (Silver Tier)
Create a second `.toml` task file specifying `depends_on = ["bronze_customers_load"]`:

*File: `config/tasks/silver_customers_clean.toml`*
```toml
[task]
task_id = "silver_customers_clean"
task_name = "Silver Clean Customers Transformation"
type = "table_load"
enabled = true
depends_on = ["bronze_customers_load"]  # <-- Luigi waits for bronze_customers_load!

[source]
type = "postgres"
connection = "postgres_dwh"
schema = "bronze_db"
table = "raw_customers"

[load]
type = "full"

[target]
catalog = "hive"
database = "silver_db"
table = "clean_customers"
```

#### 3️⃣ Step 3: Define the Analytics / Report Refresh Task (Gold Tier)
Create a third `.toml` task file specifying `depends_on = ["silver_customers_clean"]`:

*File: `config/tasks/gold_customer_dashboard.toml`*
```toml
[task]
task_id = "gold_customer_dashboard"
task_name = "Gold Customer Dashboard Reload"
type = "qlik_sense"
enabled = true
depends_on = ["silver_customers_clean"]  # <-- Luigi waits for silver_customers_clean!

[qlik_sense]
server_url = "https://qliksense.company.com"
app_id = "app-guid-customer-analytics"
timeout_seconds = 600
poll_interval_seconds = 10
```

#### 🔀 Multi-Parent Dependencies (Branching Pipelines)
If a task requires **multiple** upstream tasks to finish first, list all task IDs in `depends_on`:
```toml
[task]
task_id = "gold_executive_dashboard"
depends_on = ["silver_customers_clean", "silver_orders_clean"]  # <-- Luigi waits for BOTH!
```

---

## 🚀 CDP Deployment Guide & Step-by-Step Instructions

This section outlines how to package, deploy, and execute the framework on **Cloudera Data Platform (CDP)** (CDP Edge Node / Gateway Host / CDE Cluster).

### 📋 1. File Deployment Manifest (CDP Edge Node)

When copying the project to a CDP Gateway Host / Edge Node (e.g. `/opt/cloudera/etl_framework/`), include only production files:

| File / Folder Path | Required in CDP? | Description |
| :--- | :---: | :--- |
| `main.py` | ✅ **REQUIRED** | Core CLI Batch Driver & Entry Point. |
| `src/` | ✅ **REQUIRED** | Core framework engine (Connectors, Tasks, Writers, Transformers, Helpers). |
| `config/tasks/` | ✅ **REQUIRED** | Task TOML pipeline configuration directory. |
| `config/connections.toml` | ✅ **REQUIRED** | Environment connection parameters (Host, Port, DB Name, SFTP Path). |
| `credentials.toml` | ✅ **REQUIRED** | Protected secrets (DB Passwords, API Tokens, SMTP Passwords). Protected by `.gitignore`. |
| `sql/` | ✅ **REQUIRED** | DDL scripts for Apache Iceberg audit, watermark, and RBAC tables. |
| `templates/` | ✅ **REQUIRED** | Responsive HTML email alert templates. |
| `web_ui/` | 💡 *OPTIONAL* | Streamlit Control Center UI (Required if running Web UI on Edge Node). |
| `requirements.txt` | ✅ **REQUIRED** | Python dependencies (`tomli`, `pytz`, `requests`, `streamlit`, `pandas`). |
| `tests/` | ❌ **EXCLUDE** | Local unit test suite (Exclude from prod deployment). |
| `scratch/` | ❌ **EXCLUDE** | Scratch development scripts (Exclude from prod deployment). |
| `.git/` | ❌ **EXCLUDE** | Git repository metadata (Exclude from prod deployment). |

---

### 🔑 2. CDP Deployment & Execution Commands

#### Step A: Authenticate Kerberos (CDP Secured Cluster)
```bash
kinit -kt /etc/security/keytabs/etluser.keytab etluser@REALM.COMPANY.COM
```

#### Step B: Install Python Dependencies on Edge Node
```bash
pip install -r requirements.txt
```

#### Step C: Initialize Apache Iceberg Audit & Watermark Tables
Run the SQL DDL scripts using Spark SQL or Beeline to create metastore tables (`etl_audit`, `etl_watermark`):
```bash
spark-sql -f sql/01_create_audit_tables_ddl.sql
spark-sql -f sql/02_create_watermark_tables_ddl.sql
spark-sql -f sql/03_create_audit_tables_ddl.sql
spark-sql -f sql/04_create_rbac_tables_ddl.sql
```

#### Step D: Pre-Flight Connection Check (`--validate`)
Validate source database/SFTP connectivity and credentials without starting Spark:
```bash
python main.py --validate --config config/tasks/customer.toml
```

#### Step E: Execute Single Task on CDP YARN Cluster (`spark-submit`)
```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.2.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive.type=hive \
  --conf spark.sql.catalog.hive.uri=thrift://cdp-hms-host:9083 \
  main.py --config config/tasks/customer.toml
```

#### Step F: Parallel Batch Execution (All Active Tasks in `config/tasks/`)
Submit multiple task pipelines in parallel using YARN resource pooling:
```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.2.0 \
  main.py --config-dir config/tasks --parallel 4
```

#### Step G: Dry-Run Mode (Validate All Configurations)
```bash
python main.py --config-dir config/tasks --dry-run
```

#### Step H: Automated Failure Recovery / Deduplicated Rerun
Rerun only failed tasks recorded in `failed_jobs/YYYYMMDD/`:
```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  main.py --rerun-failed 20260815 --parallel 4
```

---

## 🌐 Streamlit Web UI Control Panel (`web_ui/app.py`)

Launch the interactive, browser-based Web UI on a CDP Edge Node or Gateway Host:

```bash
streamlit run web_ui/app.py --server.port 8501
```
*Access in browser at: `http://localhost:8501`*

### Web UI Core Modules & RBAC:
1. **🔐 User Authentication & Role-Based Access Control (RBAC)**: Secure login system powered by Apache Iceberg user database (`etl_audit.etl_users`).
   - 🛡️ **`admin` / `admin123`**: **ADMIN Role** (Full access: Create, Edit, Run, Delete, User & Access Control).
   - 💻 **`developer` / `dev123`**: **DEVELOPER Role** (Create, Edit, Run task pipelines).
   - 👁️ **`viewer` / `view123`**: **VIEWER Role** (Read-only access to task catalog and execution logs).
2. **📊 Dashboard & Task Executor**: View filterable task catalogs, validate credentials (`--validate`), and trigger task execution with real-time streaming terminal logs.
3. **➕ Visual Task Builder**: Form-driven wizard supporting **Table Load** (Oracle, MySQL, Postgres, SQL Server, SFTP -> Iceberg), **Qlik Replicate Refresh**, **Qlik Sense Report Refresh**, and **Qlik NPrinting Report**.
4. **✏️ TOML Task Editor**: Open any `.toml` file in `config/tasks/`, edit syntax live in browser, validate schemas, and save.
5. **🚨 Failure Recovery Center**: Inspect timestamped failure marker files in `failed_jobs/YYYYMMDD/`, read error tracebacks, and launch single-click deduplicated reruns.
6. **👥 User & Access Control**: Manage system user accounts, roles, and inspect Apache Iceberg RBAC DDL schemas (`sql/04_create_rbac_tables_ddl.sql`).

---

## 🧩 Complete Task Taxonomy

All execution units implement the **`BaseTask`** abstract interface (`src/core/task.py`):

```mermaid
graph TD
    BaseTask["BaseTask (Abstract Base)"] --> TableLoad["TableLoadTask (type = 'table_load')"]
    BaseTask --> QlikReplicate["QlikReplicateRefreshTask (type = 'qlik_replicate')"]
    BaseTask --> QlikSense["QlikSenseRefreshTask (type = 'qlik_sense')"]
    BaseTask --> QlikNPrinting["QlikNPrintingTask (type = 'qlik_nprinting')"]
```

| Task Type | Implementation Class | Description | Config Section |
| :--- | :--- | :--- | :--- |
| `table_load` | `TableLoadTask` | Database & SFTP source extraction, PySpark transformations, Quality checks, Iceberg loading. | `[task]`, `[source]`, `[target]` |
| `qlik_replicate` | `QlikReplicateRefreshTask` | Triggers & monitors Qlik Replicate tasks via REST API (`RELOAD_TARGET`, `RESUME`, `RUN`). | `[task]`, `[qlik_replicate]` |
| `qlik_sense` | `QlikSenseRefreshTask` | Triggers & monitors Qlik Sense App / Report reloads via QRS REST API. | `[task]`, `[qlik_sense]` |
| `qlik_nprinting` | `QlikNPrintingTask` | Triggers & monitors Qlik NPrinting report generation via NPrinting REST API. | `[task]`, `[qlik_nprinting]` |

---

## 📧 Reusable Email Template Engine & Iceberg Audit Table

The framework features a dedicated **Email Template Engine** (`src/helpers/email_template.py`) that separates template formatting from email transport and logs structured delivery audit telemetry into `etl_audit.etl_email_audit`.

### Iceberg Email Audit Table DDL (`sql/03_create_audit_tables_ddl.sql`):
```sql
CREATE TABLE IF NOT EXISTS etl_audit.etl_email_audit (
    notification_id     STRING          COMMENT 'Unique Email Notification Run UUID',
    job_id              STRING          COMMENT 'Task ID',
    job_name            STRING          COMMENT 'Descriptive Task Name',
    run_id              STRING          COMMENT 'Execution Run ID',
    event_type          STRING          COMMENT 'Event Trigger (on_failure, on_success, on_quality_failure)',
    pipeline_status     STRING          COMMENT 'Pipeline Execution Status (FAILED, SUCCESS)',
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

```text
                       spark-submit main.py --config-dir config/tasks/ --parallel 4
                                                     │
       ┌───────────────────────┬─────────────────────┴─────────────────────┬───────────────────────┐
       ▼                       ▼                                           ▼                       ▼
Worker Thread 1         Worker Thread 2                             Worker Thread 3         Worker Thread 4
(Task: customer.toml)   (Task: sqlserver_inv.toml)                  (Task: sftp_invoices.toml) (Task: qlik_orders.toml)
 (Oracle Database)       (MS SQL Server)                             (SFTP Server)            (Qlik Replicate REST API)
       │                       │                                           │                       │
Spark JDBC Splits       Spark JDBC Splits                           SFTP CSV/Excel Read     Qlik REST API Poll
([source.jdbc] n=8)     ([source.jdbc] n=4)                         (Pandas/Spark CSV)      (Action RELOAD_TARGET)
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

Run the full automated unit test suite locally (**112 tests** across 21 test modules):

```bash
python -m unittest discover tests
```

---

## 📁 Directory Structure & File Map

```text
Extraction_v2.1/
├── app.py                      # Root Streamlit Wrapper (Backward Compatible)
├── main.py                     # CLI Entry Point Driver
├── requirements.txt            # Python Dependencies
├── README.md                   # Enterprise Technical Documentation
├── web_ui/
│   └── app.py                  # Streamlit Web Control Center & RBAC Login
├── config/
│   ├── connections.toml        # Environment Database Connections
│   └── tasks/                  # TOML Task Pipeline Configuration Directory
├── failed_jobs/                # Audit Failure Marker Storage (YYYYMMDD/)
├── success_jobs/               # Audit Success Marker Storage (YYYYMMDD/)
├── sql/
│   ├── 01_create_audit_tables_ddl.sql    # etl_audit persistence DDL
│   ├── 02_create_watermark_tables_ddl.sql# etl_watermark DDL
│   ├── 03_create_audit_tables_ddl.sql    # etl_email_audit Persistence DDL
│   └── 04_create_rbac_tables_ddl.sql     # etl_users & etl_user_permissions DDL
├── templates/                  # Responsive HTML Email Notification Templates
├── src/                        # Core Framework Package
│   ├── connectors/             # Oracle, SQL Server, MySQL, Postgres, SFTP Readers
│   ├── core/                   # Task Engine (BaseTask, TableLoadTask), Config Parser, Transformer, Writer, Quality, Hooks
│   └── helpers/                # Framework Helpers
│       ├── spark.py            # SparkSession Factory (CDP Hive Metastore & Iceberg)
│       ├── qlik_replicate.py   # QlikReplicateRefreshTask Module
│       ├── qlik_sense.py       # QlikSenseRefreshTask Module
│       ├── qlik_nprinting.py    # QlikNPrintingTask Module
│       ├── logger.py           # ETLLogger & JSON Telemetry
│       ├── failure.py          # Failure Marker Recorder
│       ├── success.py          # Success Marker Recorder
│       ├── retry.py            # Retry Handler with Exponential Backoff
│       ├── tuner.py            # Spark Resource & Fetch Size Tuner
│       ├── email_notification.py # Email Delivery & etl_email_audit Persistence
│       └── email_template.py   # Reusable HTML Template Engine & HTML Tables
└── tests/                      # Automated Unit Test Suite (112 unit tests passing)
```
