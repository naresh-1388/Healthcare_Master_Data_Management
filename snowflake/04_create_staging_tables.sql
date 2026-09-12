-- ============================================================
-- Healthcare_Master_Data_Management - Snowflake DDL
-- Auto-generated from the audited HMDM_DEV mapping workbook.
-- Layer: STAGING
-- Every table/column here is taken directly from the workbook's
-- Land_to_Stag + Stg_MDM_Ingress-HCP/HCO sheet(s) - nothing here was invented independently of
-- that already-reviewed mapping.
-- ============================================================
USE DATABASE HMDM_DEV;
USE SCHEMA STAGING;

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCO_ADDRESS (
    "Address_Line_1" VARCHAR(4000),
    "City" VARCHAR(4000),
    "Postal_Code" VARCHAR(4000),
    "Country" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCO_ALTERNATE_NAME (
    "Alternate_Name" VARCHAR(4000),
    "Alternate_Name_Type" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCO_EMAIL (
    "Email" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCO_HIERARCHY (
    "Parent_Organization_EID" VARCHAR(4000),
    "Relationship_Type" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCO_IDENTIFICATION (
    "Identifier_Value" VARCHAR(4000),
    "Status" VARCHAR(4000),
    "Identifier_Issuer" VARCHAR(4000),
    "Issuing_Country" VARCHAR(4000),
    "Issuing_State" VARCHAR(4000),
    "Activation_Date" TIMESTAMP_NTZ,
    "Expiration_Date" TIMESTAMP_NTZ,
    "Source_Name" VARCHAR(4000),
    "Source_FK" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCO_NAME (
    "Source_Name" VARCHAR(4000),
    "Population_Name" VARCHAR(4000),
    "HCO_Name" VARCHAR(4000),
    "HCO_Subtype" VARCHAR(4000),
    "Country" VARCHAR(4000),
    "Bed_Count" NUMBER(10,0),
    "Resident_Count" NUMBER(10,0),
    "Website" VARCHAR(4000),
    "HCO_Type" VARCHAR(4000),
    "Status" VARCHAR(4000),
    "Transparency_Reporting_Name" VARCHAR(4000),
    "Official_Name" VARCHAR(4000),
    "Parent_Organization_Name" VARCHAR(4000),
    "Teaching_Hospital_Flag" BOOLEAN,
    "Profit_Flag" BOOLEAN,
    "Accept_Medicare" BOOLEAN,
    "E_Medical_Record" VARCHAR(4000),
    "Pay_Perform" VARCHAR(4000),
    "E_Prescribe" VARCHAR(4000),
    "Formulary" VARCHAR(4000),
    "Activation_Date" TIMESTAMP_NTZ,
    "Accept_Medicaid" BOOLEAN,
    "Ownership_Status" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ,
    "Third_Party_ID" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCO_PHONE (
    "Phone_PK" VARCHAR(4000),
    "Primary_Phone" VARCHAR(4000),
    "Phone_Usage_Type" VARCHAR(4000),
    "Phone_Type" VARCHAR(4000),
    "Phone_Number" VARCHAR(4000),
    "Phone_Number_Extension" VARCHAR(4000),
    "ISO" VARCHAR(4000),
    "Status" VARCHAR(4000),
    "Effective_Start_Date" TIMESTAMP_NTZ,
    "Effective_End_Date" TIMESTAMP_NTZ,
    "Source_Name" VARCHAR(4000),
    "Source_FK" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCO_SPECIALTY (
    "Specialty_PK" VARCHAR(4000),
    "Specialty_Rank" VARCHAR(4000),
    "Specialty" VARCHAR(4000),
    "Specialty_Type" VARCHAR(4000),
    "Status" VARCHAR(4000),
    "Specialty_Source" VARCHAR(4000),
    "Global_Specialty" VARCHAR(4000),
    "Group_Specialty" VARCHAR(4000),
    "Source_Name" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCO_TAX (
    "Tax_Number" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_ADDRESS (
    "X_hcp_address" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_ALTERNATE_NAME (
    "AlternateName" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_EDUCATION (
    "Qualification" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_EMAIL (
    "ElectronicAddress" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_HCO_AFFILIATION (
    "HCO_EID" VARCHAR(4000),
    "Relationship_Type" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_IDENTIFICATION (
    "AlternateIdentifier" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_LANGUAGE (
    "Language" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_LICENSE (
    "X_infac360ls_License" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_NAME (
    "X_transparency_reporting_name" VARCHAR(4000),
    "firstName" VARCHAR(4000),
    "middleName" VARCHAR(4000),
    "lastName" VARCHAR(4000),
    "fullName" VARCHAR(4000),
    "gender" VARCHAR(4000),
    "X_infac360ls_type" VARCHAR(4000),
    "X_hcp_status" VARCHAR(4000),
    "X_jisb_title" VARCHAR(4000),
    "prefixName" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_ORIGIN_UNIVERSITY (
    "University_Name" VARCHAR(4000),
    "University_Code" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_PHONE (
    "Phone" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_SPECIALTY (
    "X_infac360ls_Specialty" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_TAX (
    "X_infac360ls_dea" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.STAGING.HCP_TENDENCIES (
    "Code" VARCHAR(4000),
    "Rank" VARCHAR(4000)
);

