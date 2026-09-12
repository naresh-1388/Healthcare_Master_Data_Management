-- ============================================================
-- Healthcare_Master_Data_Management - Snowflake DDL
-- Auto-generated from the audited HMDM_DEV mapping workbook.
-- Layer: MASTER
-- Every table/column here is taken directly from the workbook's
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet(s) - nothing here was invented independently of
-- that already-reviewed mapping.
-- ============================================================
USE DATABASE HMDM_DEV;
USE SCHEMA MASTER;

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO (
    "SOURCE_ID" VARCHAR(200),
    "X_hco_address" VARCHAR(4000),
    "X_hco_city" VARCHAR(4000),
    "X_hco_postal_code" VARCHAR(4000),
    "X_hco_country" VARCHAR(4000),
    "AlternateName" VARCHAR(4000),
    "AlternateNameType" VARCHAR(4000),
    "ElectronicAddress" VARCHAR(4000),
    "X_parent_organization_eid" VARCHAR(4000),
    "X_hierarchy_relationship_type" VARCHAR(4000),
    "X_infac360ls_tax_number" VARCHAR(4000),
    "_PUBLISHED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO_ALTERNATE_IDENTIFIER (
    "SOURCE_ID" VARCHAR(200),
    "altValue" VARCHAR(4000),
    "IdentifierStatus" VARCHAR(4000),
    "X_infa360_identifierIssuer" VARCHAR(4000),
    "X_infa360_issuingCountry" VARCHAR(4000),
    "X_infa360_issuingState" VARCHAR(4000),
    "X_activation_date" TIMESTAMP_NTZ,
    "X_expiration_date" TIMESTAMP_NTZ,
    "AlternateIdentifier_parentId" VARCHAR(4000),
    "_PUBLISHED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO_NAME (
    "SOURCE_ID" VARCHAR(200),
    "sourceSystem" VARCHAR(4000),
    "populationName" VARCHAR(4000),
    "HCO_name" VARCHAR(4000),
    "HCO_companyType" VARCHAR(4000),
    "HCO_countryOfIncorporation" VARCHAR(4000),
    "HCO_X_infa360_bedCount" NUMBER(10,0),
    "HCO_X_infa360_residentCount" NUMBER(10,0),
    "HCO_X_infa360_website" VARCHAR(4000),
    "HCO_X_infa360_type" VARCHAR(4000),
    "HCO_X_hco_status" VARCHAR(4000),
    "HCO_X_transparency_reporting_name" VARCHAR(4000),
    "HCO_X_official_name" VARCHAR(4000),
    "HCO_X_parent_organization_name" VARCHAR(4000),
    "HCO_X_TeachingHospitalFlag" BOOLEAN,
    "HCO_X_ProfitFlag" BOOLEAN,
    "HCO_X_AcceptMedicare" VARCHAR(4000),
    "HCO_X_EMedicalRecord" VARCHAR(4000),
    "HCO_X_PayPerform" VARCHAR(4000),
    "HCO_X_EPrescribe" VARCHAR(4000),
    "HCO_X_Formulary" VARCHAR(4000),
    "HCO_X_ActivationDate" TIMESTAMP_NTZ,
    "HCO_X_AcceptMedicaid" VARCHAR(4000),
    "HCO_X_ownership_status" VARCHAR(4000),
    "HCO_X_source_createdate" TIMESTAMP_NTZ,
    "HCO_X_source_updatedate" TIMESTAMP_NTZ,
    "HCO_X_third_party_id" VARCHAR(4000),
    "_PUBLISHED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO_PHONE (
    "SOURCE_ID" VARCHAR(200),
    "sourcePKey" VARCHAR(4000),
    "X_primary_phone" VARCHAR(4000),
    "X_phone_usage_type" VARCHAR(4000),
    "X_phone_type" VARCHAR(4000),
    "X_phone_number" VARCHAR(4000),
    "X_phone_number_extension" VARCHAR(4000),
    "X_iso" VARCHAR(4000),
    "X_phone_status" VARCHAR(4000),
    "X_effective_start_date" TIMESTAMP_NTZ,
    "X_effective_end_date" TIMESTAMP_NTZ,
    "X_phone_parentId" VARCHAR(4000),
    "_PUBLISHED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCO_SPECIALTY (
    "SOURCE_ID" VARCHAR(200),
    "sourcePKey" VARCHAR(4000),
    "X_infa360_rank" VARCHAR(4000),
    "X_infa360_Specialty" VARCHAR(4000),
    "X_specialty_type" VARCHAR(4000),
    "X_specialty_status" VARCHAR(4000),
    "X_specialty_source" VARCHAR(4000),
    "X_global_specialty" VARCHAR(4000),
    "X_group_specialty" VARCHAR(4000),
    "_PUBLISHED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP (
    "SOURCE_ID" VARCHAR(200),
    "Transparency_Reporting_Name" VARCHAR(4000),
    "First_Name" VARCHAR(4000),
    "Middle_Name" VARCHAR(4000),
    "Last_Name" VARCHAR(4000),
    "Full_Name" VARCHAR(4000),
    "Gender" VARCHAR(4000),
    "HCP_Type" VARCHAR(4000),
    "Status" VARCHAR(4000),
    "Title" VARCHAR(4000),
    "Prefix_Name" VARCHAR(4000),
    "Address" VARCHAR(4000),
    "Phone" VARCHAR(4000),
    "Qualification" VARCHAR(4000),
    "DEA_Number" VARCHAR(4000),
    "Alternate_Identifier" VARCHAR(4000),
    "Email" VARCHAR(4000),
    "HCO_Affiliation_EID" VARCHAR(4000),
    "HCO_Affiliation_Type" VARCHAR(4000),
    "Language" VARCHAR(4000),
    "Origin_University_Name" VARCHAR(4000),
    "Origin_University_Code" VARCHAR(4000),
    "Tendency_Code" VARCHAR(4000),
    "Tendency_Rank" VARCHAR(4000),
    "_PUBLISHED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP_ALTERNATE_NAME (
    "SOURCE_ID" VARCHAR(200),
    "Name_Type" VARCHAR(4000),
    "Alternate_Name" VARCHAR(4000),
    "Alternate_Name_Status" VARCHAR(4000),
    "Effective_Start_Date" TIMESTAMP_NTZ,
    "Effective_End_Date" TIMESTAMP_NTZ,
    "Load_Date" TIMESTAMP_NTZ,
    "_PUBLISHED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
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

CREATE TABLE IF NOT EXISTS HMDM_DEV.MASTER.HCP_SPECIALTY (
    "SOURCE_ID" VARCHAR(200),
    "Specialty" VARCHAR(4000),
    "Specialty_Type" VARCHAR(4000),
    "Specialty_Rank" VARCHAR(4000),
    "Taxonomy_Name" VARCHAR(4000),
    "Group" VARCHAR(4000),
    "Taxonomy_Code" VARCHAR(4000),
    "Sub_Classification" VARCHAR(4000),
    "Specialty_Status" VARCHAR(4000),
    "Load_Date" TIMESTAMP_NTZ,
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

