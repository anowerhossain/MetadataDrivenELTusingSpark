-- ==============================================================================
-- 03. ETL GOVERNANCE, WATERMARK & AUDIT TABLES DDL STATEMENTS
-- ==============================================================================

USE etl_audit;

-- ------------------------------------------------------------------------------
-- 1. SFTP File Ingestion Processing Audit Telemetry Table
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_audit.sftp_file_audit (
    file_name           STRING          COMMENT 'Basename of processed SFTP file (e.g. invoices_20260813.csv)',
    file_path           STRING          COMMENT 'Full remote SFTP path or local staging directory path',
    file_size_bytes     BIGINT          COMMENT 'Exact file size in bytes',
    last_modified_ts    STRING          COMMENT 'File modification timestamp (Last Updated Time)',
    file_owner          STRING          COMMENT 'SFTP file owner / Uploader ID / Excel author (NULL if unavailable)',
    target_table        STRING          COMMENT 'Target Iceberg table identifier (e.g. hive.edw_bronze.sftp_invoices_csv)',
    load_timestamp      STRING          COMMENT 'Timestamp when ingestion ran (UTC+6 BST)',
    record_count        BIGINT          COMMENT 'Total records read and written from file',
    status              STRING          COMMENT 'Ingestion Status (SUCCESS or FAILED)',
    error_message       STRING          COMMENT 'Failure error traceback or NULL if success'
)
USING iceberg
PARTITIONED BY (status)
TBLPROPERTIES (
    'format-version' = '2'
);


-- ------------------------------------------------------------------------------
-- 2. State Watermark Management Table (For Incremental & SFTP State Tracking)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_audit.watermark_store (
    job_id              STRING          COMMENT 'Unique ETL Pipeline Job ID or SFTP File Fingerprint Key',
    watermark_value     STRING          COMMENT 'Last processed high watermark timestamp or file (size_mtime) state',
    updated_at          TIMESTAMP       COMMENT 'Timestamp when state watermark was committed'
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2'
);


-- ------------------------------------------------------------------------------
-- 3. ETL Framework Execution Lifecycle Run Telemetry Table
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_audit.etl_pipeline_telemetry (
    run_id              STRING          COMMENT 'Unique Pipeline Execution Run UUID',
    job_id              STRING          COMMENT 'ETL Job ID (e.g. customer_load, sqlserver_invoices)',
    source_type         STRING          COMMENT 'Source Connector Type (oracle, sqlserver, mysql, postgres, sftp)',
    target_table        STRING          COMMENT 'Target Iceberg Table Identifier',
    start_time          TIMESTAMP       COMMENT 'Job Start Timestamp',
    end_time            TIMESTAMP       COMMENT 'Job Completion Timestamp',
    duration_seconds    DOUBLE          COMMENT 'Job Execution Duration in seconds',
    rows_read           BIGINT          COMMENT 'Total rows read from source database or SFTP file',
    rows_written        BIGINT          COMMENT 'Total rows committed to target Iceberg table',
    status              STRING          COMMENT 'Run Status (SUCCESS or FAILED)',
    error_message       STRING          COMMENT 'Error message summary if failed'
)
USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES (
    'format-version' = '2'
);


-- ------------------------------------------------------------------------------
-- 4. ETL Email Notification Audit Telemetry Table
-- ------------------------------------------------------------------------------
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
TBLPROPERTIES (
    'format-version' = '2'
);
