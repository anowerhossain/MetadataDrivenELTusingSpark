-- ============================================================================
-- Apache Iceberg DDL: User Authentication & Role-Based Access Control (RBAC)
-- Database: etl_audit
-- Catalog:  hive / iceberg
-- ============================================================================

CREATE DATABASE IF NOT EXISTS etl_audit;

-- ----------------------------------------------------------------------------
-- Table 1: etl_audit.etl_users
-- Stores user accounts, encrypted password hashes, roles, and status.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_audit.etl_users (
    user_id          STRING        COMMENT 'Unique user account ID',
    username         STRING        COMMENT 'Unique login username',
    password_hash    STRING        COMMENT 'PBKDF2/SHA256 password hash',
    email            STRING        COMMENT 'User email address',
    role             STRING        COMMENT 'Assigned RBAC role: ADMIN, DEVELOPER, VIEWER',
    is_active        BOOLEAN       COMMENT 'User account active flag',
    created_at       TIMESTAMP     COMMENT 'Timestamp when account was created',
    updated_at       TIMESTAMP     COMMENT 'Timestamp of last update'
)
USING iceberg
LOCATION 'hdfs:///user/hive/warehouse/etl_audit.db/etl_users'
PARTITIONED BY (role)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'history.expire.max-snapshot-age-ms' = '604800000',
    'write.object-storage.enabled' = 'true'
);

-- ----------------------------------------------------------------------------
-- Table 2: etl_audit.etl_user_permissions
-- Maps RBAC roles to granular framework capabilities.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_audit.etl_user_permissions (
    role             STRING        COMMENT 'RBAC Role name (ADMIN, DEVELOPER, VIEWER)',
    permission_name  STRING        COMMENT 'Permission key (CREATE_TASK, EDIT_TASK, EXECUTE_TASK, DELETE_TASK, VIEW_CATALOG, MANAGE_USERS)',
    description      STRING        COMMENT 'Business description of permission',
    granted_at       TIMESTAMP     COMMENT 'Timestamp when permission was mapped'
)
USING iceberg
LOCATION 'hdfs:///user/hive/warehouse/etl_audit.db/etl_user_permissions'
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);

-- ----------------------------------------------------------------------------
-- Initial Metadata Seed Data
-- ----------------------------------------------------------------------------

-- Seed System Users
INSERT INTO etl_audit.etl_users VALUES
    ('usr_admin_01', 'admin', '$pbkdf2-sha256$29000$saltsalt$hashhash...', 'admin@company.com', 'ADMIN', true, current_timestamp(), current_timestamp()),
    ('usr_dev_01',   'developer', '$pbkdf2-sha256$29000$saltsalt$hashhash...', 'dev@company.com', 'DEVELOPER', true, current_timestamp(), current_timestamp()),
    ('usr_view_01',  'viewer', '$pbkdf2-sha256$29000$saltsalt$hashhash...', 'viewer@company.com', 'VIEWER', true, current_timestamp(), current_timestamp());

-- Seed Role Permissions
INSERT INTO etl_audit.etl_user_permissions VALUES
    ('ADMIN', 'VIEW_CATALOG', 'View task catalog, pipeline status, and audit logs', current_timestamp()),
    ('ADMIN', 'CREATE_TASK', 'Create new ETL and Qlik tasks', current_timestamp()),
    ('ADMIN', 'EDIT_TASK', 'Edit existing TOML task configurations', current_timestamp()),
    ('ADMIN', 'EXECUTE_TASK', 'Run task pipelines and trigger recovery', current_timestamp()),
    ('ADMIN', 'DELETE_TASK', 'Delete task configuration files', current_timestamp()),
    ('ADMIN', 'MANAGE_USERS', 'Create, update, and deactivate user accounts', current_timestamp()),
    
    ('DEVELOPER', 'VIEW_CATALOG', 'View task catalog, pipeline status, and audit logs', current_timestamp()),
    ('DEVELOPER', 'CREATE_TASK', 'Create new ETL and Qlik tasks', current_timestamp()),
    ('DEVELOPER', 'EDIT_TASK', 'Edit existing TOML task configurations', current_timestamp()),
    ('DEVELOPER', 'EXECUTE_TASK', 'Run task pipelines and trigger recovery', current_timestamp()),
    
    ('VIEWER', 'VIEW_CATALOG', 'View task catalog, pipeline status, and audit logs', current_timestamp());
