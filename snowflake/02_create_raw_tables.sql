-- ============================================================
-- Healthcare_Master_Data_Management - Snowflake DDL
-- Auto-generated from the audited HMDM_DEV mapping workbook.
-- Layer: RAW
-- Every table/column here is taken directly from the workbook's
-- Source_Raw sheet(s) - nothing here was invented independently of
-- that already-reviewed mapping.
-- ============================================================
USE DATABASE HMDM_DEV;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCO_ADDRESS (
    "organizationEid" VARCHAR(4000),
    "Address_Eid" VARCHAR(4000),
    "Address_Line_1" VARCHAR(4000),
    "City" VARCHAR(4000),
    "Postal_Code" VARCHAR(4000),
    "Country" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCO_ALTERNATE_NAME (
    "organizationEid" VARCHAR(4000),
    "Alternate_Name" VARCHAR(4000),
    "Alternate_Name_Type" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCO_EMAIL (
    "organizationEid" VARCHAR(4000),
    "Email" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCO_HIERARCHY (
    "organizationEid" VARCHAR(4000),
    "Parent_Organization_EID" VARCHAR(4000),
    "Relationship_Type" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCO_IDENTIFICATION (
    "organizationEid" VARCHAR(4000),
    "Identifier_Value" VARCHAR(4000),
    "Identifier_Type" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCO_NAME (
    "organizationEid" VARCHAR(4000),
    "HCO_Name" VARCHAR(4000),
    "HCO_Type" VARCHAR(4000),
    "Status" VARCHAR(4000),
    "Country" VARCHAR(4000),
    "Website" VARCHAR(4000),
    "Population_Name" VARCHAR(4000),
    "HCO_Subtype" VARCHAR(4000),
    "Bed_Count" NUMBER(10,0),
    "Resident_Count" NUMBER(10,0),
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
    "Third_Party_ID" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCO_PHONE (
    "organizationEid" VARCHAR(4000),
    "Phone_Number" VARCHAR(4000),
    "Phone_Type" VARCHAR(4000),
    "Phone_Status" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCO_SPECIALTY (
    "organizationEid" VARCHAR(4000),
    "Specialty" VARCHAR(4000),
    "Specialty_Rank" VARCHAR(4000),
    "Specialty_Type" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCO_TAX (
    "organizationEid" VARCHAR(4000),
    "Tax_Number" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_ADDRESS (
    "individualEid" VARCHAR(4000),
    "addressEid" VARCHAR(4000),
    "typeCode" VARCHAR(4000),
    "typeCodeLabel" VARCHAR(4000),
    "typeCodeCorporateLabel" VARCHAR(4000),
    "activityEid" VARCHAR(4000),
    "isMainActivity" VARCHAR(4000),
    "activityStartDate" TIMESTAMP_NTZ,
    "activityEndDate" TIMESTAMP_NTZ,
    "activityStateCode" VARCHAR(4000),
    "activityStateCorporateLabel" VARCHAR(4000),
    "activityStatusCode" VARCHAR(4000),
    "villageLabel" VARCHAR(4000),
    "villageLabel2" VARCHAR(4000),
    "dispatchLabel" VARCHAR(4000),
    "country" VARCHAR(4000),
    "longPostalCode" VARCHAR(4000),
    "addressShortLabel" VARCHAR(4000),
    "extensionLabel" VARCHAR(4000),
    "postalDistrict" VARCHAR(4000),
    "shortLabel2" VARCHAR(4000),
    "longLocalizedLabelRegion" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_ALTERNATE_NAME (
    "individualEid" VARCHAR(4000),
    "firstName2" VARCHAR(4000),
    "lastName2" VARCHAR(4000),
    "usualFirstName" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_EDUCATION (
    "individualEid" VARCHAR(4000),
    "Qualification" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_EMAIL (
    "individualEid" VARCHAR(4000),
    "Email" VARCHAR(4000),
    "Email_Usage_Type" VARCHAR(4000),
    "Email_Status" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_HCO_AFFILIATION (
    "individualEid" VARCHAR(4000),
    "HCO_EID" VARCHAR(4000),
    "Relationship_Type" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_IDENTIFICATION (
    "individualEid" VARCHAR(4000),
    "value" VARCHAR(4000),
    "typeLabel" VARCHAR(4000),
    "typeCorporateLabel" VARCHAR(4000),
    "number" VARCHAR(4000),
    "typeCode" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_LANGUAGE (
    "individualEid" VARCHAR(4000),
    "Language" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_NAME (
    "individualEid" VARCHAR(4000),
    "firstName" VARCHAR(4000),
    "firstName2" VARCHAR(4000),
    "firstNameInitials" VARCHAR(4000),
    "usualFirstName" VARCHAR(4000),
    "middleName" VARCHAR(4000),
    "lastName" VARCHAR(4000),
    "lastName2" VARCHAR(4000),
    "typeCode" VARCHAR(4000),
    "typeLabel" VARCHAR(4000),
    "typeCorporateLabel" VARCHAR(4000),
    "titleCode" VARCHAR(4000),
    "titleLabel" VARCHAR(4000),
    "titleCorporateLabel" VARCHAR(4000),
    "prefixNameCode" VARCHAR(4000),
    "prefixNameLabel" VARCHAR(4000),
    "prefixNameCorporateLabel" VARCHAR(4000),
    "genderCode" VARCHAR(4000),
    "genderLabel" VARCHAR(4000),
    "genderCorporateLabel" VARCHAR(4000),
    "statusCode" VARCHAR(4000),
    "statusLabel" VARCHAR(4000),
    "statusCorporateLabel" VARCHAR(4000),
    "statusDate" TIMESTAMP_NTZ,
    "stateCode" VARCHAR(4000),
    "stateLabel" VARCHAR(4000),
    "stateCorporateLabel" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_ORIGIN_UNIVERSITY (
    "individualEid" VARCHAR(4000),
    "University_Name" VARCHAR(4000),
    "University_Code" VARCHAR(4000),
    "Graduation_Year" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_PHONE (
    "individualEid" VARCHAR(4000),
    "Phone_Number" VARCHAR(4000),
    "Phone_Number_Extension" VARCHAR(4000),
    "ISO" VARCHAR(4000),
    "Primary_Phone" VARCHAR(4000),
    "Phone_Type" VARCHAR(4000),
    "Phone_Usage_Type" VARCHAR(4000),
    "Phone_Status" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_SPECIALTY (
    "individualEid" VARCHAR(4000),
    "Specialty_Type" VARCHAR(4000),
    "Specialty_Class" VARCHAR(4000),
    "Specialty_Rank" VARCHAR(4000),
    "Specialty_Status" VARCHAR(4000),
    "Global_Specialty" VARCHAR(4000),
    "Group_Specialty" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_TAX (
    "individualEid" VARCHAR(4000),
    "Tax_Number" VARCHAR(4000),
    "Tax_Number_Type" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.RAW.HCP_TENDENCIES (
    "individualEid" VARCHAR(4000),
    "code" VARCHAR(4000),
    "codeLabel" VARCHAR(4000),
    "codeCorporateLabel" VARCHAR(4000),
    "rank" VARCHAR(4000),
    "type" VARCHAR(4000),
    "listCode" VARCHAR(4000),
    "listCodeLabel" VARCHAR(4000),
    "listCodeCorporateLabel" VARCHAR(4000),
    "_INGESTED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "_BATCH_ID" NUMBER(19,0)
);

