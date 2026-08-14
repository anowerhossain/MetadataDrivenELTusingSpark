-- ==============================================================================
-- 01. CREATE HIVE / ICEBERG DATABASES ON CDP
-- ==============================================================================

-- 1. Create ETL Governance & Audit Database
CREATE DATABASE IF NOT EXISTS etl_audit
COMMENT 'ETL Pipeline Governance, Watermarks, File Processing & Run Telemetry Audit Database'
LOCATION 'hdfs:///warehouse/tablespace/external/hive/etl_audit.db';
