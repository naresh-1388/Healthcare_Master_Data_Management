-- ============================================================
-- Healthcare_Master_Data_Management - Pipeline Control / Utility Tables
-- Every table name here matches exactly what src/core/runtime_config.py
-- resolves (see the "Utility / Control Tables" section of that module) -
-- do not rename these without also updating runtime_config.py.
-- ============================================================
USE DATABASE HMDM_DEV;
USE SCHEMA UTIL;

-- Ingestion configuration: which source files/APIs map to which RAW table.
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_entity_mstr (
    "SOURCE_IDENTIFIER"   VARCHAR(200),
    "SOURCE_SYSTEM_NAME"  VARCHAR(100),
    "SOURCE_PATH"         VARCHAR(1000),
    "TARGET_RAW_TABLE"    VARCHAR(200),
    "FILE_FORMAT"         VARCHAR(50),
    "ACTIVE_FLAG"         BOOLEAN DEFAULT TRUE,
    "CREATED_AT"          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Standardization rule configuration (Raw_to_Land sheet, control-table form).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_std_entity_mstr (
    "SOURCE_RAW_TABLE"     VARCHAR(200),
    "SOURCE_COLUMN"        VARCHAR(200),
    "TARGET_LANDING_TABLE" VARCHAR(200),
    "TARGET_COLUMN"        VARCHAR(200),
    "TRANSFORMATION_EXPR"  VARCHAR(4000),
    "ACTIVE_FLAG"          BOOLEAN DEFAULT TRUE
);

-- Canonical code-list / lookup mapping configuration.
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_can_mapg (
    "ENTITY_NAME"      VARCHAR(200),
    "COLUMN_NAME"      VARCHAR(200),
    "SOURCE_VALUE"     VARCHAR(500),
    "CANONICAL_VALUE"  VARCHAR(500),
    "ACTIVE_FLAG"      BOOLEAN DEFAULT TRUE
);

-- Shared pipeline execution log (every stage writes here).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_log_tbl (
    "LOG_ID"       NUMBER AUTOINCREMENT,
    "BATCH_ID"     NUMBER(19,0),
    "STAGE_NAME"   VARCHAR(100),
    "TABLE_NAME"   VARCHAR(200),
    "STATUS"       VARCHAR(20),
    "MESSAGE"      VARCHAR(4000),
    "JOB_RUN_URL"  VARCHAR(1000),
    "LOGGED_AT"    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- DQ-specific execution log (one row per rule execution).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_dqm_log_tbl (
    "LOG_ID"          NUMBER AUTOINCREMENT,
    "BATCH_ID"        NUMBER(19,0),
    "SOURCE_TABLE"    VARCHAR(200),
    "RULE_NAME"       VARCHAR(100),
    "PASSED_COUNT"    NUMBER(19,0),
    "REJECTED_COUNT"  NUMBER(19,0),
    "LOGGED_AT"       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- DQ reject records (rejected rows + which rule rejected them, for audit/reprocessing).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.dqm_reject_tbl (
    "BATCH_ID"         NUMBER(19,0),
    "SOURCE_TABLE"     VARCHAR(200),
    "DQ_RULE"          VARCHAR(100),
    "DQ_STATUS"        VARCHAR(20),
    "DQ_DESCRIPTION"   VARCHAR(4000),
    "DQ_APPL_COLUMN"   VARCHAR(200),
    "DQ_PROCESSED_AT"  TIMESTAMP_NTZ,
    "RECORD_PAYLOAD"   VARIANT
);

-- DQ rule configuration (control-table form of DQ_RULES in data_quality.py /
-- the Land_to_Stag sheet).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_dq_entity_mstr (
    "SOURCE_IDENTIFIER"  VARCHAR(200),
    "TABLE_NAME"         VARCHAR(200),
    "RULE_NAME"          VARCHAR(100),
    "RULE_TYPE"          VARCHAR(100),
    "RULE_DESCRIPTION"   VARCHAR(4000),
    "RULE_EXPRESSION"    VARCHAR(4000),
    "COLUMN_NAME"        VARCHAR(200),
    "TARGET_TABLE"       VARCHAR(200),
    "EXECUTION_ORDER"    NUMBER(5,0),
    "RULE_STATUS"        VARCHAR(20) DEFAULT 'ACTIVE',
    "ACTIVE_FLAG"        BOOLEAN DEFAULT TRUE
);

-- Batch control table: drives get_batch_status_filter() dependency chain
-- (raw_ingestion -> stdz -> canonical -> dq -> ingress -> egress).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_batch_log_tbl (
    "BATCH_ID"                NUMBER(19,0),
    "SOURCE_SYSTEM_NAME"      VARCHAR(100),
    "TABLE_NAME"              VARCHAR(200),
    "RAW_INGESTION_STATUS"    VARCHAR(1) DEFAULT 'N',
    "STDZ_STATUS"             VARCHAR(1) DEFAULT 'N',
    "CANONICAL_STATUS"        VARCHAR(1) DEFAULT 'N',
    "DQ_STATUS"               VARCHAR(1) DEFAULT 'N',
    "INGRESS_STATUS"          VARCHAR(1) DEFAULT 'N',
    "EGRESS_STATUS"           VARCHAR(1) DEFAULT 'N',
    "CREATED_AT"              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "UPDATED_AT"              TIMESTAMP_NTZ
);

-- Alert-email distribution list + monitored-table list, per source system
-- and per environment recipient group (dev / test / ops).
CREATE TABLE IF NOT EXISTS HMDM_DEV.UTIL.ctl_mailing_list_mstr (
    "SOURCE_SYSTEM_NAME"  VARCHAR(100),
    "RECIPIENT_GROUP"     VARCHAR(50),
    "EMAIL_ADDRESSES"     VARCHAR(2000),
    "MONITORED_TABLES"    VARCHAR(4000),
    "ACTIVE_FLAG"         BOOLEAN DEFAULT TRUE
);
