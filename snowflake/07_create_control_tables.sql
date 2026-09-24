-- ============================================================
-- Healthcare_Master_Data_Management - Pipeline Control / Utility Tables
-- Every table name here matches exactly what src/core/runtime_config.py
-- resolves (see the "Utility / Control Tables" section of that module) -
-- do not rename these without also updating runtime_config.py.
--
-- SCHEMA NOTE: These DDLs match the ACTUAL Databricks Unity Catalog table
-- schemas (verified via DESCRIBE TABLE). The previous version had stale
-- column definitions that did not match the Python code expectations.
-- ============================================================
USE DATABASE HMDM_DEV;
USE SCHEMA UTIL;

-- Ingestion configuration: which source files/APIs map to which RAW table.
-- Matches HMDM_DEV.UTIL.ctl_entity_mstr (19 columns verified in Databricks UC).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_entity_mstr (
    "source_identifier"    VARCHAR(200),
    "source_system_name"   VARCHAR(100),
    "source_location"      VARCHAR(1000),
    "source_name"          VARCHAR(200),
    "source_type"          VARCHAR(50),
    "source_delimiter"     VARCHAR(10),
    "raw_table_schema"     VARCHAR(200),
    "raw_table_name"       VARCHAR(200),
    "source_load_type"     VARCHAR(50),
    "archive_flag"         BOOLEAN,
    "full_load_flag"       BOOLEAN,
    "date_filter_col"      VARCHAR(200),
    "filter_cond"          VARCHAR(2000),
    "source_active_flag"   BOOLEAN DEFAULT TRUE,
    "source_primary_key"  VARCHAR(200),
    "std_table_schema"     VARCHAR(200),
    "std_table_name"       VARCHAR(200),
    "active_record_sql"    VARCHAR(4000),
    "delete_condition"     VARCHAR(4000)
);

-- Standardization rule configuration (Raw_to_Land sheet, control-table form).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_std_entity_mstr (
    "source_raw_table"      VARCHAR(200),
    "source_column"         VARCHAR(200),
    "target_landing_table"  VARCHAR(200),
    "target_column"         VARCHAR(200),
    "transformation_expr"   VARCHAR(4000),
    "active_flag"           BOOLEAN DEFAULT TRUE
);

-- Canonical code-list / lookup mapping configuration.
-- Matches the schema expected by src/canonical/canonical.py.
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_can_mapg (
    "source_system_name"    VARCHAR(100),
    "source_identifier"     VARCHAR(200),
    "src_tbl_nm"            VARCHAR(200),
    "tgt_tbl_nm"            VARCHAR(200),
    "src_attribute"         VARCHAR(200),
    "tgt_attribute"         VARCHAR(200),
    "join_condition"        VARCHAR(1000),
    "source_primary_key"   VARCHAR(200),
    "src_schema"            VARCHAR(200),
    "tgt_schema"            VARCHAR(200),
    "active_flag"           BOOLEAN DEFAULT TRUE
);

-- Shared pipeline execution log (every stage writes here).
-- Matches HMDM_DEV.UTIL.ctl_log_tbl (14 columns verified in Databricks UC).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_log_tbl (
    "run_id"              VARCHAR(100),
    "source_identifier"   VARCHAR(200),
    "source_system_name"  VARCHAR(100),
    "job_id"              VARCHAR(100),
    "module"              VARCHAR(200),
    "sub_module"          VARCHAR(200),
    "run_status"          VARCHAR(20),
    "error_description"   VARCHAR(4000),
    "run_url"             VARCHAR(1000),
    "start_time"          TIMESTAMP_NTZ,
    "end_time"            TIMESTAMP_NTZ,
    "time_elapsed"        DOUBLE,
    "user_id"             VARCHAR(100),
    "cluster_id"          VARCHAR(100)
);

-- DQ-specific execution log (one row per rule execution).
-- Matches HMDM_DEV.UTIL.ctl_dqm_log_tbl (11 columns verified in Databricks UC).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_dqm_log_tbl (
    "run_id"              VARCHAR(100),
    "batch_id"            NUMBER(19,0),
    "source_identifier"   VARCHAR(200),
    "source_system_name"  VARCHAR(100),
    "table_name"          VARCHAR(200),
    "rule_name"           VARCHAR(100),
    "rule_status"         VARCHAR(20),
    "error_description"   VARCHAR(4000),
    "record_count"        NUMBER(19,0),
    "start_time"          TIMESTAMP_NTZ,
    "end_time"            TIMESTAMP_NTZ
);

-- DQ reject records (rejected rows + which rule rejected them, for audit/reprocessing).
-- Matches HMDM_DEV.UTIL.dqm_reject_tbl (8 columns verified in Databricks UC).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.dqm_reject_tbl (
    "run_id"              VARCHAR(100),
    "batch_id"            NUMBER(19,0),
    "source_system_name"  VARCHAR(100),
    "table_name"          VARCHAR(200),
    "record_key"          VARCHAR(200),
    "rule_name"           VARCHAR(100),
    "error_description"   VARCHAR(4000),
    "rejected_at"         TIMESTAMP_NTZ
);

-- DQ rule configuration (control-table form of DQ_RULES in data_quality.py /
-- the Land_to_Stag sheet).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_dq_entity_mstr (
    "source_identifier"   VARCHAR(200),
    "table_name"          VARCHAR(200),
    "rule_name"           VARCHAR(100),
    "rule_type"           VARCHAR(100),
    "rule_description"    VARCHAR(4000),
    "rule_expression"     VARCHAR(4000),
    "column_name"         VARCHAR(200),
    "target_table"        VARCHAR(200),
    "execution_order"     NUMBER(5,0),
    "rule_status"         VARCHAR(20) DEFAULT 'ACTIVE',
    "active_flag"         BOOLEAN DEFAULT TRUE
);

-- Batch control table: drives get_batch_status_filter() dependency chain
-- (raw_ingestion -> stdz -> canonical -> dq -> ingress -> egress -> snowflake_sync).
-- Matches HMDM_DEV.UTIL.ctl_batch_log_tbl (11 columns verified in Databricks UC).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_batch_log_tbl (
    "batch_id"                NUMBER(19,0),
    "source_system_name"      VARCHAR(100),
    "batch_start_time"        TIMESTAMP_NTZ,
    "batch_end_time"          TIMESTAMP_NTZ,
    "raw_ingestion_status"    VARCHAR(1) DEFAULT 'N',
    "stdz_status"             VARCHAR(1) DEFAULT 'N',
    "canonical_status"        VARCHAR(1) DEFAULT 'N',
    "dq_status"               VARCHAR(1) DEFAULT 'N',
    "ingress_status"          VARCHAR(1) DEFAULT 'N',
    "egress_status"           VARCHAR(1) DEFAULT 'N',
    "snowflake_sync_status"   VARCHAR(1) DEFAULT 'N'
);

-- Alert-email distribution list + monitored-table list, per source system
-- and per environment recipient group (dev / test / ops).
-- Matches the schema expected by runtime_config.get_maillist().
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_mailing_list_mstr (
    "source_system_name"  VARCHAR(100),
    "email_id"            VARCHAR(2000),
    "table_list"          VARCHAR(4000),
    "recepient_type"      VARCHAR(50),
    "active_flag"         BOOLEAN DEFAULT TRUE
);
