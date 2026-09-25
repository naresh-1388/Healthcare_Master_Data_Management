-- ============================================================
-- Healthcare_Master_Data_Management - Snowflake DDL
-- Layer: MASTER
-- Synced from Databricks HMDM_DEV.master.* by push_to_snowflake.py
-- Column names match Databricks Delta tables exactly.
-- Tables HCP, HCP_LICENSE, HCP_THERAPEUTIC_AREA kept from original DDL
-- (not synced from Databricks, but may be used by dbt/Snowpark).
-- ============================================================
USE DATABASE HMDM_DEV;
USE SCHEMA MASTER;

-- HCP master tables synced from Databricks (5 tables)
-- Each table has a DIFFERENT column set -- explicit DDL for each (no LIKE shortcuts).
-- gender column is MAP in Databricks -> VARCHAR(4000) in Snowflake (sync converts to string).

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP_SPECIALTY (
    "individualEid" VARCHAR(4000),
    "firstName" VARCHAR(4000),
    "middleName" VARCHAR(4000),
    "lastName" VARCHAR(4000),
    "countryCode" VARCHAR(4000),
    "batch_id" VARCHAR(4000),
    "LOAD_DATE" TIMESTAMP_NTZ,
    "source_name" VARCHAR(4000),
    "MDM_INGRESS_PROCESSED_AT" TIMESTAMP_NTZ,
    "SOURCE_SYSTEM_NAME" VARCHAR(4000),
    "EGRESS_LOAD_DATE" TIMESTAMP_NTZ,
    "fullName" VARCHAR(4000),
    "gender" VARCHAR(4000),
    "X_informatica_Specialty" VARCHAR(4000),
    "X_specialty_type" VARCHAR(4000),
    "X_informatica_rank" VARCHAR(4000),
    "X_specialty_status" VARCHAR(4000),
    "Qualification" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP_ALTERNATE_NAME (
    "individualEid" VARCHAR(4000),
    "firstName" VARCHAR(4000),
    "middleName" VARCHAR(4000),
    "lastName" VARCHAR(4000),
    "countryCode" VARCHAR(4000),
    "batch_id" VARCHAR(4000),
    "LOAD_DATE" TIMESTAMP_NTZ,
    "source_name" VARCHAR(4000),
    "MDM_INGRESS_PROCESSED_AT" TIMESTAMP_NTZ,
    "SOURCE_SYSTEM_NAME" VARCHAR(4000),
    "EGRESS_LOAD_DATE" TIMESTAMP_NTZ,
    "fullName" VARCHAR(4000),
    "gender" VARCHAR(4000),
    "AlternateName" VARCHAR(4000),
    "AlternateNameType" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP_EDUCATION (
    "individualEid" VARCHAR(4000),
    "firstName" VARCHAR(4000),
    "middleName" VARCHAR(4000),
    "lastName" VARCHAR(4000),
    "fullName" VARCHAR(4000),
    "gender" VARCHAR(4000),
    "batch_id" VARCHAR(4000),
    "LOAD_DATE" TIMESTAMP_NTZ,
    "source_name" VARCHAR(4000),
    "MDM_INGRESS_PROCESSED_AT" TIMESTAMP_NTZ,
    "countryCode" VARCHAR(4000),
    "SOURCE_SYSTEM_NAME" VARCHAR(4000),
    "EGRESS_LOAD_DATE" TIMESTAMP_NTZ,
    "Qualification" VARCHAR(4000),
    "X_institution_name" VARCHAR(4000),
    "X_graduation_year" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP_IDENTIFICATION (
    "individualEid" VARCHAR(4000),
    "firstName" VARCHAR(4000),
    "middleName" VARCHAR(4000),
    "lastName" VARCHAR(4000),
    "fullName" VARCHAR(4000),
    "gender" VARCHAR(4000),
    "batch_id" VARCHAR(4000),
    "LOAD_DATE" TIMESTAMP_NTZ,
    "source_name" VARCHAR(4000),
    "MDM_INGRESS_PROCESSED_AT" TIMESTAMP_NTZ,
    "countryCode" VARCHAR(4000),
    "SOURCE_SYSTEM_NAME" VARCHAR(4000),
    "EGRESS_LOAD_DATE" TIMESTAMP_NTZ,
    "X_informatica_License" VARCHAR(4000),
    "X_informatica_dea" VARCHAR(4000),
    "AlternateIdentifier" VARCHAR(4000)
);

-- HCO master tables synced from Databricks (5 tables)
-- HCO base + HCO_NAME have 10 cols (identical). Child tables add entity-specific cols.

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO (
    "organizationEid" VARCHAR(4000),
    "organizationName" VARCHAR(4000),
    "organizationType" VARCHAR(4000),
    "countryCode" VARCHAR(4000),
    "batch_id" VARCHAR(4000),
    "LOAD_DATE" TIMESTAMP_NTZ,
    "source_name" VARCHAR(4000),
    "MDM_INGRESS_PROCESSED_AT" TIMESTAMP_NTZ,
    "SOURCE_SYSTEM_NAME" VARCHAR(4000),
    "EGRESS_LOAD_DATE" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO_NAME LIKE HMDM_DEV.MASTER.HCO;

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO_ALTERNATE_IDENTIFIER (
    "organizationEid" VARCHAR(4000),
    "organizationName" VARCHAR(4000),
    "organizationType" VARCHAR(4000),
    "countryCode" VARCHAR(4000),
    "batch_id" VARCHAR(4000),
    "LOAD_DATE" TIMESTAMP_NTZ,
    "source_name" VARCHAR(4000),
    "MDM_INGRESS_PROCESSED_AT" TIMESTAMP_NTZ,
    "SOURCE_SYSTEM_NAME" VARCHAR(4000),
    "EGRESS_LOAD_DATE" TIMESTAMP_NTZ,
    "Alternate_Identifier" VARCHAR(4000),
    "X_identifier_type" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO_PHONE (
    "organizationEid" VARCHAR(4000),
    "organizationName" VARCHAR(4000),
    "organizationType" VARCHAR(4000),
    "countryCode" VARCHAR(4000),
    "batch_id" VARCHAR(4000),
    "LOAD_DATE" TIMESTAMP_NTZ,
    "source_name" VARCHAR(4000),
    "MDM_INGRESS_PROCESSED_AT" TIMESTAMP_NTZ,
    "SOURCE_SYSTEM_NAME" VARCHAR(4000),
    "EGRESS_LOAD_DATE" TIMESTAMP_NTZ,
    "Phone" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO_SPECIALTY (
    "organizationEid" VARCHAR(4000),
    "organizationName" VARCHAR(4000),
    "organizationType" VARCHAR(4000),
    "countryCode" VARCHAR(4000),
    "batch_id" VARCHAR(4000),
    "LOAD_DATE" TIMESTAMP_NTZ,
    "source_name" VARCHAR(4000),
    "MDM_INGRESS_PROCESSED_AT" TIMESTAMP_NTZ,
    "SOURCE_SYSTEM_NAME" VARCHAR(4000),
    "EGRESS_LOAD_DATE" TIMESTAMP_NTZ,
    "Specialty" VARCHAR(4000),
    "Specialty_Type" VARCHAR(4000),
    "Specialty_Rank" VARCHAR(4000),
    "Status" VARCHAR(4000)
);

-- FIX (Sep 25 2026): MASTER.HCP DDL updated to match actual Databricks schema.
-- Was old schema (SOURCE_ID, First_Name, etc.) but Databricks egress writes
-- individualEid, firstName, etc. Old DDL caused schema-mismatch recreate on every sync.
CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP (
    "individualEid" VARCHAR(4000),
    "firstName" VARCHAR(4000),
    "middleName" VARCHAR(4000),
    "lastName" VARCHAR(4000),
    "countryCode" VARCHAR(4000),
    "fullName" VARCHAR(4000),
    "gender" VARCHAR(4000),
    "prefixName" VARCHAR(4000),
    "X_transparency_reporting_name" VARCHAR(4000),
    "X_hcp_status" VARCHAR(4000),
    "X_informatica_type" VARCHAR(4000),
    "X_iqvia_title" VARCHAR(4000),
    "source_name" VARCHAR(4000),
    "Batch_ID" VARCHAR(4000),
    "Load_Date" TIMESTAMP_NTZ,
    "MDM_INGRESS_PROCESSED_AT" TIMESTAMP_NTZ,
    "SOURCE_SYSTEM_NAME" VARCHAR(4000),
    "EGRESS_LOAD_DATE" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP_LICENSE (
    "SOURCE_ID" VARCHAR(200),
    "Global_HCP_ID" VARCHAR(4000),
    "Source_PK" VARCHAR(4000),
    "License_Type" VARCHAR(4000),
    "License_Number" VARCHAR(4000),
    "Country" VARCHAR(4000),
    "State" VARCHAR(4000),
    "License_Sample_Eligibility" VARCHAR(4000),
    "Sampleability_Overall" VARCHAR(4000),
    "Sampleability_Last_Received_Date" TIMESTAMP_NTZ,
    "Sampleability_Fed_Sanctions_Date" TIMESTAMP_NTZ,
    "Sampleability_Designation_Status" VARCHAR(4000),
    "Issue_Date" TIMESTAMP_NTZ,
    "Expiry_Date" TIMESTAMP_NTZ,
    "Degree" VARCHAR(4000),
    "Adj_License_Exp_Date" TIMESTAMP_NTZ,
    "Adj_Code" VARCHAR(4000),
    "Adj_Codes_Descriptions" VARCHAR(4000),
    "License_Status" VARCHAR(4000),
    "_PUBLISHED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP_THERAPEUTIC_AREA (
    "SOURCE_ID" VARCHAR(200),
    "Global_HCP_ID" VARCHAR(4000),
    "Active_Indicator" VARCHAR(4000),
    "Therapeutic_Area" VARCHAR(4000),
    "Load_Date" TIMESTAMP_NTZ,
    "_PUBLISHED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

