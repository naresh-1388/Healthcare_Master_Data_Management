-- ============================================================
-- Healthcare_Master_Data_Management - Snowflake DDL
-- Auto-generated from the audited HMDM_DEV mapping workbook.
-- Layer: MDM
-- Every table/column here is taken directly from the workbook's
-- Stg_MDM_Ingress-HCP / Stg_MDM_Ingress-HCO sheet(s) - nothing here was invented independently of
-- that already-reviewed mapping.
-- ============================================================
USE DATABASE HMDM_DEV;
USE SCHEMA MDM;

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.HCO (
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
    "X_infac360ls_tax_number" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.HCO_ALTERNATE_IDENTIFIER (
    "SOURCE_ID" VARCHAR(200),
    "altValue" VARCHAR(4000),
    "IdentifierStatus" VARCHAR(4000),
    "X_infa360_identifierIssuer" VARCHAR(4000),
    "X_infa360_issuingCountry" VARCHAR(4000),
    "X_infa360_issuingState" VARCHAR(4000),
    "X_activation_date" TIMESTAMP_NTZ,
    "X_expiration_date" TIMESTAMP_NTZ,
    "sourceSystem" VARCHAR(4000),
    "AlternateIdentifier_parentId" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.HCO_NAME (
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
    "HCO_X_third_party_id" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.HCO_PHONE (
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
    "sourceSystem" VARCHAR(4000),
    "X_phone_parentId" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.HCO_SPECIALTY (
    "SOURCE_ID" VARCHAR(200),
    "sourcePKey" VARCHAR(4000),
    "X_infa360_rank" VARCHAR(4000),
    "X_infa360_Specialty" VARCHAR(4000),
    "X_specialty_type" VARCHAR(4000),
    "X_specialty_status" VARCHAR(4000),
    "X_specialty_source" VARCHAR(4000),
    "X_global_specialty" VARCHAR(4000),
    "X_group_specialty" VARCHAR(4000),
    "sourceSystem" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.HCP (
    "SOURCE_ID" VARCHAR(200),
    "X_transparency_reporting_name" VARCHAR(4000),
    "firstName" VARCHAR(4000),
    "middleName" VARCHAR(4000),
    "lastName" VARCHAR(4000),
    "fullName" VARCHAR(4000),
    "gender" VARCHAR(4000),
    "X_infac360ls_type" VARCHAR(4000),
    "X_hcp_status" VARCHAR(4000),
    "X_jisb_title" VARCHAR(4000),
    "prefixName" VARCHAR(4000),
    "AlternateName" VARCHAR(4000),
    "X_hcp_address" VARCHAR(4000),
    "Phone" VARCHAR(4000),
    "X_infac360ls_Specialty" VARCHAR(4000),
    "Qualification" VARCHAR(4000),
    "X_infac360ls_License" VARCHAR(4000),
    "X_infac360ls_dea" VARCHAR(4000),
    "AlternateIdentifier" VARCHAR(4000),
    "ElectronicAddress" VARCHAR(4000),
    "X_hco_affiliation_eid" VARCHAR(4000),
    "X_hco_affiliation_type" VARCHAR(4000),
    "X_infac360ls_language" VARCHAR(4000),
    "X_origin_university_name" VARCHAR(4000),
    "X_origin_university_code" VARCHAR(4000),
    "X_infac360ls_tendency_code" VARCHAR(4000),
    "X_infac360ls_tendency_rank" VARCHAR(4000)
);

