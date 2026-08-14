-- ==============================================================================
-- 02. APACHE ICEBERG BRONZE TARGET TABLES DDL STATEMENTS
-- ==============================================================================

USE edw_bronze;

-- ------------------------------------------------------------------------------
-- 1. Oracle Customer Target Iceberg Table
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.edw_bronze.customer (
    customer_id         BIGINT          COMMENT 'Unique Customer ID from Oracle BANK.CUSTOMER',
    customer_name       STRING          COMMENT 'Customer Full Name',
    status              STRING          COMMENT 'Account Status (ACTIVE, PENDING, INACTIVE)',
    balance             DECIMAL(18,2)   COMMENT 'Current Account Balance',
    updated_at          TIMESTAMP       COMMENT 'Source Record Modification Timestamp',
    dwh_insert_ts       TIMESTAMP       COMMENT 'DWH Ingestion Timestamp (BST, UTC+6)',
    dwh_updated_ts      TIMESTAMP       COMMENT 'DWH Last Update Execution Timestamp',
    dwh_etl_run_id      STRING          COMMENT 'Pipeline Execution Run ID',
    dwh_job_user        STRING          COMMENT 'Execution Account / User'
)
USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES (
    'write.object-storage.enabled' = 'true',
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);


-- ------------------------------------------------------------------------------
-- 2. Microsoft SQL Server Invoices Target Iceberg Table
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.edw_bronze.sqlserver_invoices (
    invoice_id          BIGINT          COMMENT 'Unique Invoice ID from SQL Server dbo.invoices',
    customer_id         BIGINT          COMMENT 'Customer Reference ID',
    total_amount        DECIMAL(18,2)   COMMENT 'Total Invoice Amount',
    status              STRING          COMMENT 'Invoice Payment Status (PAID, PENDING, OVERDUE)',
    created_at          TIMESTAMP       COMMENT 'Invoice Creation Timestamp',
    dwh_insert_ts       TIMESTAMP       COMMENT 'DWH Ingestion Timestamp (BST, UTC+6)',
    dwh_updated_ts      TIMESTAMP       COMMENT 'DWH Last Update Execution Timestamp',
    dwh_etl_run_id      STRING          COMMENT 'Pipeline Execution Run ID',
    dwh_job_user        STRING          COMMENT 'Execution Account / User'
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'write.object-storage.enabled' = 'true',
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);


-- ------------------------------------------------------------------------------
-- 3. MySQL Orders Target Iceberg Table
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.edw_bronze.mysql_orders (
    order_id            BIGINT          COMMENT 'Unique Order ID from MySQL sales_db.orders',
    customer_id         BIGINT          COMMENT 'Customer Reference ID',
    order_amount        DECIMAL(18,2)   COMMENT 'Order Subtotal Amount',
    status              STRING          COMMENT 'Order Status (COMPLETED, PROCESSING, CANCELLED)',
    created_at          TIMESTAMP       COMMENT 'Order Creation Timestamp',
    dwh_insert_ts       TIMESTAMP       COMMENT 'DWH Ingestion Timestamp (BST, UTC+6)',
    dwh_updated_ts      TIMESTAMP       COMMENT 'DWH Last Update Execution Timestamp',
    dwh_etl_run_id      STRING          COMMENT 'Pipeline Execution Run ID',
    dwh_job_user        STRING          COMMENT 'Execution Account / User'
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'write.object-storage.enabled' = 'true',
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);


-- ------------------------------------------------------------------------------
-- 4. PostgreSQL Payments Target Iceberg Table
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.edw_bronze.postgres_payments (
    payment_id          BIGINT          COMMENT 'Unique Payment Transaction ID from Postgres public.payments',
    account_id          BIGINT          COMMENT 'Account Reference ID',
    amount              DECIMAL(18,2)   COMMENT 'Payment Transaction Amount',
    payment_method      STRING          COMMENT 'Method (CREDIT_CARD, BANK_TRANSFER, BKASH, NAGAD)',
    created_at          TIMESTAMP       COMMENT 'Payment Transaction Timestamp',
    dwh_insert_ts       TIMESTAMP       COMMENT 'DWH Ingestion Timestamp (BST, UTC+6)',
    dwh_updated_ts      TIMESTAMP       COMMENT 'DWH Last Update Execution Timestamp',
    dwh_etl_run_id      STRING          COMMENT 'Pipeline Execution Run ID',
    dwh_job_user        STRING          COMMENT 'Execution Account / User'
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'write.object-storage.enabled' = 'true',
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);


-- ------------------------------------------------------------------------------
-- 5. SFTP Invoices CSV Target Iceberg Table
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.edw_bronze.sftp_invoices_csv (
    invoice_id          STRING          COMMENT 'Invoice ID from SFTP CSV dump',
    customer_id         STRING          COMMENT 'Customer Reference ID',
    total_amount        DECIMAL(18,2)   COMMENT 'Invoice Total Amount',
    status              STRING          COMMENT 'Invoice Processing Status',
    dwh_insert_ts       TIMESTAMP       COMMENT 'DWH Ingestion Timestamp (BST, UTC+6)',
    dwh_updated_ts      TIMESTAMP       COMMENT 'DWH Last Update Execution Timestamp',
    dwh_etl_run_id      STRING          COMMENT 'Pipeline Execution Run ID',
    dwh_job_user        STRING          COMMENT 'Execution Account / User'
)
USING iceberg
PARTITIONED BY (days(dwh_insert_ts))
TBLPROPERTIES (
    'write.object-storage.enabled' = 'true',
    'format-version' = '2'
);


-- ------------------------------------------------------------------------------
-- 6. SFTP Card Settlements Excel Target Iceberg Table
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hive.edw_bronze.sftp_settlements_excel (
    settle_id           STRING          COMMENT 'Settlement Reference ID from SFTP Excel file',
    card_no             STRING          COMMENT 'Masked Card Number',
    amount              DECIMAL(18,2)   COMMENT 'Settlement Clearing Amount',
    dwh_insert_ts       TIMESTAMP       COMMENT 'DWH Ingestion Timestamp (BST, UTC+6)',
    dwh_updated_ts      TIMESTAMP       COMMENT 'DWH Last Update Execution Timestamp',
    dwh_etl_run_id      STRING          COMMENT 'Pipeline Execution Run ID',
    dwh_job_user        STRING          COMMENT 'Execution Account / User'
)
USING iceberg
PARTITIONED BY (days(dwh_insert_ts))
TBLPROPERTIES (
    'write.object-storage.enabled' = 'true',
    'format-version' = '2'
);
