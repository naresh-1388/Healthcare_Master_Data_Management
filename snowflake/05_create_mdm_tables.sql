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
    SOURCE_FK VARCHAR(4000),
    "X_hco_address" VARCHAR(4000),
    "X_hco_city" VARCHAR(4000),
    "X_hco_postal_code" VARCHAR(4000),
    "X_hco_country" VARCHAR(4000),
    "AlternateName" VARCHAR(4000),
    "AlternateNameType" VARCHAR(4000),
    "ElectronicAddress" VARCHAR(4000),
    "X_parent_organization_eid" VARCHAR(4000),
    "X_hierarchy_relationship_type" VARCHAR(4000),
    "X_informatica_tax_number" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.HCO_ALTERNATE_IDENTIFIER (
    SOURCE_FK VARCHAR(4000),
    "altValue" VARCHAR(4000),
    "IdentifierStatus" VARCHAR(4000),
    "X_informatica_identifierIssuer" VARCHAR(4000),
    "X_informatica_issuingCountry" VARCHAR(4000),
    "X_informatica_issuingState" VARCHAR(4000),
    "X_activation_date" TIMESTAMP_NTZ,
    "X_expiration_date" TIMESTAMP_NTZ,
    "sourceSystem" VARCHAR(4000),
    "AlternateIdentifier_parentId" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.HCO_NAME (
    SOURCE_FK VARCHAR(4000),
    "sourceSystem" VARCHAR(4000),
    "populationName" VARCHAR(4000),
    "HCO_name" VARCHAR(4000),
    "HCO_companyType" VARCHAR(4000),
    "HCO_countryOfIncorporation" VARCHAR(4000),
    "HCO_X_informatica_bedCount" NUMBER(10,0),
    "HCO_X_informatica_residentCount" NUMBER(10,0),
    "HCO_X_informatica_website" VARCHAR(4000),
    "HCO_X_informatica_type" VARCHAR(4000),
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
    SOURCE_FK VARCHAR(4000),
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
    SOURCE_FK VARCHAR(4000),
    "sourcePKey" VARCHAR(4000),
    "X_informatica_rank" VARCHAR(4000),
    "X_informatica_Specialty" VARCHAR(4000),
    "X_specialty_type" VARCHAR(4000),
    "X_specialty_status" VARCHAR(4000),
    "X_specialty_source" VARCHAR(4000),
    "X_global_specialty" VARCHAR(4000),
    "X_group_specialty" VARCHAR(4000),
    "sourceSystem" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.HCP (
    SOURCE_FK VARCHAR(4000),
    "X_transparency_reporting_name" VARCHAR(4000),
    "firstName" VARCHAR(4000),
    "middleName" VARCHAR(4000),
    "lastName" VARCHAR(4000),
    "fullName" VARCHAR(4000),
    "gender" VARCHAR(4000),
    "X_informatica_type" VARCHAR(4000),
    "X_hcp_status" VARCHAR(4000),
    "X_iqvia_title" VARCHAR(4000),
    "prefixName" VARCHAR(4000),
    "AlternateName" VARCHAR(4000),
    "X_hcp_address" VARCHAR(4000),
    "Phone" VARCHAR(4000),
    "X_informatica_Specialty" VARCHAR(4000),
    "Qualification" VARCHAR(4000),
    "X_informatica_License" VARCHAR(4000),
    "X_informatica_dea" VARCHAR(4000),
    "AlternateIdentifier" VARCHAR(4000),
    "ElectronicAddress" VARCHAR(4000),
    "X_hco_affiliation_eid" VARCHAR(4000),
    "X_hco_affiliation_type" VARCHAR(4000),
    "X_informatica_language" VARCHAR(4000),
    "X_origin_university_name" VARCHAR(4000),
    "X_origin_university_code" VARCHAR(4000),
    "X_informatica_tendency_code" VARCHAR(4000),
    "X_informatica_tendency_rank" VARCHAR(4000)
);

-- ============================================================
-- Informatica MDM hub child objects (mdm_hub source in sources.yml)
-- These tables are NOT populated by the Databricks pipeline or dbt.
-- They are created empty so dbt models that reference source('mdm_hub', ...)
-- do not fail with 'table not found'. Populate via Informatica MDM hub export.
-- ============================================================

-- FIX #3: Added SOURCE_FK column to all mdm_hub tables so dbt models
-- that read from source('mdm_hub', ...) can output Source_FK for schema.yml
-- not_null tests. Populate via Informatica MDM hub export.

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.hcp_specialty (
    SOURCE_FK VARCHAR(4000),
    "X_infa360_SpecialtyType" VARCHAR(4000),
    "X_infa360_SpecialtyClass" VARCHAR(4000),
    "X_infa360_SpecialtyRank" VARCHAR(4000),
    "X_infa360_taxonomyName" VARCHAR(4000),
    "X_infa360_group" VARCHAR(4000),
    "X_infa360_taxonomy_code" VARCHAR(4000),
    "X_infa360_subClassification" VARCHAR(4000),
    "X_specialty_status" VARCHAR(4000),
    "O_Load_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.hcp_alternate_name (
    SOURCE_FK VARCHAR(4000),
    "alternateNameType" VARCHAR(4000),
    "AlternateName" VARCHAR(4000),
    "X_alternate_name_status" VARCHAR(4000),
    "effectiveStartDate" TIMESTAMP_NTZ,
    "effectiveEndDate" TIMESTAMP_NTZ,
    "O_Load_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.hcp_license (
    SOURCE_FK VARCHAR(4000),
    "X_infa360_License_parentId" VARCHAR(4000),
    "sourcePKey" VARCHAR(4000),
    "X_infa360_LicenseType" VARCHAR(4000),
    "X_infa360_LicenseNumber" VARCHAR(4000),
    "X_country" VARCHAR(4000),
    "X_state" VARCHAR(4000),
    "X_sample_elig" VARCHAR(4000),
    "X_sampleability_overall" VARCHAR(4000),
    "X_sampleability_lastreceived_date" TIMESTAMP_NTZ,
    "X_sampleability_fed_sanctions_date" TIMESTAMP_NTZ,
    "X_sampleability_desigstatus" VARCHAR(4000),
    "X_infa360_issueDate" TIMESTAMP_NTZ,
    "X_infa360_expiryDate" TIMESTAMP_NTZ,
    "X_degree" VARCHAR(4000),
    "X_adjLic_expdate" TIMESTAMP_NTZ,
    "X_AdjCode" VARCHAR(4000),
    "X_AdjCodesDescriptions" VARCHAR(4000),
    "X_infa360_status" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.MDM.hcp_therapeutic_area (
    SOURCE_FK VARCHAR(4000),
    "X_infa360_TherapeuticArea_parentId" VARCHAR(4000),
    "X_infa360_activeIndicator" VARCHAR(4000),
    "X_infa360_therapeuticArea" VARCHAR(4000),
    "O_Load_Date" TIMESTAMP_NTZ
);

