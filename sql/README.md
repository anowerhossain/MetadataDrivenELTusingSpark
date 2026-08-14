# CDP Apache Iceberg DDL & DML Management Guide

This directory contains the production SQL DDL (Data Definition Language) and DML (Data Manipulation Language) scripts to initialize all **Apache Iceberg databases, target data lake tables, governance audit stores, and seed sample data** on the Cloudera Data Platform (CDP).

---

## 📁 Directory File Index

| File Name | Target Engine | Purpose & Contents |
| :--- | :--- | :--- |
| **`01_create_databases.sql`** | Spark SQL / Hive / Impala | Creates `edw_bronze` (Data Lake Bronze Layer) and `etl_audit` (Governance & Audit Layer) databases. |
| **`02_create_target_tables_ddl.sql`** | Spark SQL / Hive / Impala | Creates all 6 Bronze target Apache Iceberg tables (`customer`, `sqlserver_invoices`, `mysql_orders`, `postgres_payments`, `sftp_invoices_csv`, `sftp_settlements_excel`). |
| **`03_create_audit_tables_ddl.sql`** | Spark SQL / Hive / Impala | Creates Iceberg governance audit tables (`sftp_file_audit`, `watermark_store`, `etl_pipeline_telemetry`). |
| **`04_sample_data_dml.sql`** | Spark SQL / Hive / Impala | Ingests sample seed records into target Iceberg tables and runs verification count queries. |

---

## 🛠️ Execution Guide on CDP

### Option 1: Execute via Spark SQL CLI
```bash
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive.type=hive \
  -f sql/01_create_databases.sql

spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive.type=hive \
  -f sql/02_create_target_tables_ddl.sql

spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive.type=hive \
  -f sql/03_create_audit_tables_ddl.sql

spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive.type=hive \
  -f sql/04_sample_data_dml.sql
```

---

### Option 2: Execute via Impala Shell
```bash
impala-shell -i impala-daemon.bank.local:21000 -f sql/01_create_databases.sql
impala-shell -i impala-daemon.bank.local:21000 -f sql/02_create_target_tables_ddl.sql
impala-shell -i impala-daemon.bank.local:21000 -f sql/03_create_audit_tables_ddl.sql
impala-shell -i impala-daemon.bank.local:21000 -f sql/04_sample_data_dml.sql
```

---

### Option 3: Automatic Execution via PySpark Framework
When running `spark-submit main.py --config config/tasks/customer.toml`, the framework (`IcebergWriter`) will **automatically execute `CREATE TABLE IF NOT EXISTS`** for target tables if they do not exist.
