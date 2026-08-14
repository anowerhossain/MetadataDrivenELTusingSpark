-- ==============================================================================
-- 04. SAMPLE DATA DML INSERTION STATEMENTS (SEED & VERIFICATION DATA)
-- ==============================================================================

USE edw_bronze;

-- ------------------------------------------------------------------------------
-- 1. Seed Sample Records into Customer Iceberg Table
-- ------------------------------------------------------------------------------
INSERT INTO hive.edw_bronze.customer VALUES
(1001, 'Anower Hossain', 'ACTIVE', 150000.50, TIMESTAMP '2026-08-13 10:00:00', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'run_seed_001', 'anowerhossain'),
(1002, 'Brac Bank Treasury', 'ACTIVE', 5250000.00, TIMESTAMP '2026-08-13 10:30:00', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'run_seed_001', 'anowerhossain'),
(1003, 'Global Enterprise Corp', 'PENDING', 85000.75, TIMESTAMP '2026-08-13 11:15:00', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'run_seed_001', 'anowerhossain');


-- ------------------------------------------------------------------------------
-- 2. Seed Sample Records into SQL Server Invoices Iceberg Table
-- ------------------------------------------------------------------------------
INSERT INTO hive.edw_bronze.sqlserver_invoices VALUES
(9001, 1001, 12500.00, 'PAID', TIMESTAMP '2026-08-13 09:00:00', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'run_seed_002', 'anowerhossain'),
(9002, 1002, 45000.50, 'PENDING', TIMESTAMP '2026-08-13 09:30:00', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'run_seed_002', 'anowerhossain');


-- ------------------------------------------------------------------------------
-- 3. Seed Sample Records into MySQL Orders Iceberg Table
-- ------------------------------------------------------------------------------
INSERT INTO hive.edw_bronze.mysql_orders VALUES
(8001, 1001, 3500.00, 'COMPLETED', TIMESTAMP '2026-08-13 08:15:00', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'run_seed_003', 'anowerhossain'),
(8002, 1003, 1200.75, 'PROCESSING', TIMESTAMP '2026-08-13 08:45:00', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'run_seed_003', 'anowerhossain');


-- ------------------------------------------------------------------------------
-- 4. Seed Sample Records into PostgreSQL Payments Iceberg Table
-- ------------------------------------------------------------------------------
INSERT INTO hive.edw_bronze.postgres_payments VALUES
(7001, 1001, 3500.00, 'BKASH', TIMESTAMP '2026-08-13 08:20:00', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'run_seed_004', 'anowerhossain'),
(7002, 1002, 45000.50, 'BANK_TRANSFER', TIMESTAMP '2026-08-13 09:40:00', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'run_seed_004', 'anowerhossain');


-- ------------------------------------------------------------------------------
-- 5. Seed Sample Audit Records into SFTP File Ingestion Audit Table
-- ------------------------------------------------------------------------------
INSERT INTO hive.etl_audit.sftp_file_audit VALUES
('invoices_20260813.csv', '/remote/incoming/invoices_20260813.csv', 1048576, '2026-08-13 07:00:00', 'sftp_bank_user', 'hive.edw_bronze.sftp_invoices_csv', '2026-08-13 07:05:00', 1500, 'SUCCESS', NULL),
('card_settlements_20260813.xlsx', '/remote/incoming/card_settlements_20260813.xlsx', 2097152, '2026-08-13 07:30:00', 'VisaSettlementAdmin', 'hive.edw_bronze.sftp_settlements_excel', '2026-08-13 07:35:00', 850, 'SUCCESS', NULL);


-- ------------------------------------------------------------------------------
-- 6. Verification Queries Across Spark / Impala / Hive
-- ------------------------------------------------------------------------------
SELECT 'customer' AS tbl, COUNT(*) FROM hive.edw_bronze.customer
UNION ALL
SELECT 'sqlserver_invoices' AS tbl, COUNT(*) FROM hive.edw_bronze.sqlserver_invoices
UNION ALL
SELECT 'mysql_orders' AS tbl, COUNT(*) FROM hive.edw_bronze.mysql_orders
UNION ALL
SELECT 'postgres_payments' AS tbl, COUNT(*) FROM hive.edw_bronze.postgres_payments;

SELECT * FROM hive.etl_audit.sftp_file_audit ORDER BY load_timestamp DESC;
