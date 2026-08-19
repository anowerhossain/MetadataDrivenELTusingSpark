# 🕸️ Spotify Luigi Job Pipeline Configuration Guide (`config/jobs/`)

This directory contains higher-level **Job Workflow Pipeline TOML configurations** (`config/jobs/<job_id>.toml`). 

While files in `config/tasks/` define individual execution units (such as extracting a single Oracle table or refreshing a Qlik report), files in `config/jobs/` **compose multiple task files into interconnected business pipelines** orchestrated by **Spotify Luigi**.

---

## 📌 Tasks vs. Jobs: What is the Difference?

| Concept | Directory Path | Purpose | Example |
| :--- | :--- | :--- | :--- |
| **Task** | `config/tasks/*.toml` | Single extraction, transformation, or reporting action. | `oracle_employees_to_iceberg.toml` |
| **Job Pipeline** | `config/jobs/*.toml` | Complete multi-step business workflow combining tasks. | `complex_job_pipeline.toml` |

---

## 📐 TOML Schema Reference for Job Pipelines

Every `.toml` file in `config/jobs/` must follow this schema:

```toml
[job]
job_id = "unique_job_identifier"        # Unique alphanumeric job ID
job_name = "Descriptive Job Title"      # Human-readable pipeline name
description = "Pipeline summary..."      # Business description
enabled = true                          # Enable/disable entire pipeline (true/false)

# --- Task 1: Root Task (No Upstream Dependencies) ---
[[job.tasks]]
task_id = "task_id_1"                   # Matches task_id in config/tasks/
task_file = "config/tasks/task1.toml"   # Path to task TOML configuration file
depends_on = []                         # Empty array = Runs immediately (Root)

# --- Task 2: Downstream Task (Waits for Task 1) ---
[[job.tasks]]
task_id = "task_id_2"
task_file = "config/tasks/task2.toml"
depends_on = ["task_id_1"]              # Waits for task_id_1 to finish with SUCCESS
```

---

## 💡 3 Standardized Pipeline Templates

### 1️⃣ Template 1: Simple Linear Pipeline (`config/jobs/simple_job_pipeline.toml`)
*Use case: Sequential 2-tier pipeline where Silver transformation waits for Bronze ingestion.*

```toml
[job]
job_id = "simple_job_pipeline"
job_name = "Simple Linear Medallion Pipeline"
description = "Simple 2-step linear pipeline: Bronze Ingestion -> Silver Transformation"
enabled = true

[[job.tasks]]
task_id = "bronze_orders_load"
task_file = "config/tasks/bronze_orders_load.toml"
depends_on = []

[[job.tasks]]
task_id = "silver_orders_clean"
task_file = "config/tasks/silver_orders_clean.toml"
depends_on = ["bronze_orders_load"]
```

---

### 2️⃣ Template 2: Parallel Ingestion Pipeline (`config/jobs/parallel_job_pipeline.toml`)
*Use case: Concurrent ingestion of multiple independent databases across parallel worker threads.*

```toml
[job]
job_id = "parallel_job_pipeline"
job_name = "Parallel Multi-Source Ingestion Pipeline"
description = "Executes multiple independent Bronze database ingestion tasks concurrently in parallel Luigi worker threads."
enabled = true

[[job.tasks]]
task_id = "oracle_employees_to_iceberg"
task_file = "config/tasks/oracle_employees_to_iceberg.toml"
depends_on = []

[[job.tasks]]
task_id = "mysql_orders"
task_file = "config/tasks/mysql_orders.toml"
depends_on = []

[[job.tasks]]
task_id = "postgres_payments"
task_file = "config/tasks/postgres_payments.toml"
depends_on = []

[[job.tasks]]
task_id = "sqlserver_invoices"
task_file = "config/tasks/sqlserver_invoices.toml"
depends_on = []
```

---

### 3️⃣ Template 3: Complex Medallion Enterprise DAG (`config/jobs/complex_job_pipeline.toml`)
*Use case: Full enterprise Medallion architecture (Bronze Ingestion ➔ Silver Cleansing ➔ Gold Analytics ➔ Qlik Reload ➔ NPrinting).*

```toml
[job]
job_id = "complex_job_pipeline"
job_name = "Complex Medallion Enterprise & Qlik Pipeline"
description = "Full Medallion architecture: Multi-source Bronze Ingestion -> Silver Cleansing -> Gold Aggregation -> Qlik Reload & NPrinting Reports."
enabled = true

# --- Tier 1: Bronze Ingestion (Parallel Roots) ---
[[job.tasks]]
task_id = "oracle_employees_to_iceberg"
task_file = "config/tasks/oracle_employees_to_iceberg.toml"
depends_on = []

[[job.tasks]]
task_id = "mysql_orders"
task_file = "config/tasks/mysql_orders.toml"
depends_on = []

[[job.tasks]]
task_id = "sftp_invoices_csv"
task_file = "config/tasks/sftp_invoices_csv.toml"
depends_on = []

# --- Tier 2: Silver Transformations ---
[[job.tasks]]
task_id = "silver_orders_clean"
task_file = "config/tasks/silver_orders_clean.toml"
depends_on = ["mysql_orders", "sftp_invoices_csv"] # Waits for BOTH mysql_orders AND sftp_invoices_csv

# --- Tier 3: Gold Analytics & Reporting ---
[[job.tasks]]
task_id = "gold_executive_report"
task_file = "config/tasks/gold_executive_report.toml"
depends_on = ["silver_orders_clean", "oracle_employees_to_iceberg"]

# --- Tier 4: Qlik Integration Tasks ---
[[job.tasks]]
task_id = "qlik_sense_reload"
task_file = "config/tasks/qlik_sense_reload.toml"
depends_on = ["gold_executive_report"]

[[job.tasks]]
task_id = "qlik_nprinting_report"
task_file = "config/tasks/qlik_nprinting_report.toml"
depends_on = ["qlik_sense_reload"]
```

---

## 🚀 How to Execute a Job Pipeline

### Method A: Streamlit Web UI Control Panel (Easiest)
1. Launch the UI: `streamlit run web_ui/app.py`
2. Navigate to **4. Job Builder (Luigi workflow orchestrator (DAG))**.
3. Select your job pipeline from the dropdown (e.g. `complex_job_pipeline.toml`).
4. Click **Run Job Pipeline**.
5. Watch the DAG canvas animate in real time:
   - **`⏳ RUNNING...`**: Glowing Amber Border (`#f59e0b`).
   - **`✅ SUCCESS`**: Emerald Green Border (`#22c55e`).
   - **`🚨 FAILED`**: Crimson Red Border (`#ef4444`).

### Method B: CDP Gateway Node CLI (`spark-submit`)
Submit the pipeline to YARN using the `--use-luigi` flag:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4g \
  --executor-memory 8g \
  --num-executors 10 \
  --jars jars/ojdbc8-21.9.0.0.jar,jars/mysql-connector-j-8.1.0.jar,jars/iceberg-spark-runtime-3.2_2.12-1.2.0.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive.type=hive \
  --conf spark.sql.catalog.hive.uri=thrift://cdp-hms-host:9083 \
  main.py --use-luigi --job config/jobs/complex_job_pipeline.toml --parallel 4
```

---

## ⚠️ Best Practices & Validation Rules

1. **Unique Task IDs**: Ensure every `task_id` inside `[[job.tasks]]` is unique within the pipeline.
2. **Valid Task File Paths**: Ensure `task_file` points to an existing `.toml` file in `config/tasks/`.
3. **No Circular Dependencies**: Ensure task dependencies form a Directed Acyclic Graph (DAG) without circular loops (e.g. Task A ➔ Task B ➔ Task A).
