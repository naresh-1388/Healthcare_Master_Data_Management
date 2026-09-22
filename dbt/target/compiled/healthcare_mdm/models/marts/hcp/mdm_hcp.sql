-- MDM ingress model for HMDM_DEV.MDM.HCP
-- Consolidates 14 STAGING tables onto the single flat
-- MDM.HCP base object, matching the Stg_MDM_Ingress sheet exactly.
-- Anchored on HCP_NAME (the primary name/identity record) and left-joined
-- to every other contributing table on Source_FK - this mirrors how
-- Informatica MDM lands multiple attribute groups directly onto one
-- base object.

with base as (
    select * from HMDM_DEV.STAGING_staging.stg_HCP_NAME
)

select
    base."X_transparency_reporting_name" as "X_transparency_reporting_name",
    base."firstName" as "firstName",
    base."middleName" as "middleName",
    base."lastName" as "lastName",
    base."fullName" as "fullName",
    base."gender" as "gender",
    base."X_infac360ls_type" as "X_infac360ls_type",
    base."X_hcp_status" as "X_hcp_status",
    base."X_jisb_title" as "X_jisb_title",
    base."prefixName" as "prefixName",
    hcp_alternate_name."AlternateName" as "AlternateName",
    hcp_address."X_hcp_address" as "X_hcp_address",
    hcp_phone."Phone" as "Phone",
    hcp_specialty."X_infac360ls_Specialty" as "X_infac360ls_Specialty",
    hcp_education."Qualification" as "Qualification",
    hcp_license."X_infac360ls_License" as "X_infac360ls_License",
    hcp_tax."X_infac360ls_dea" as "X_infac360ls_dea",
    hcp_identification."AlternateIdentifier" as "AlternateIdentifier",
    hcp_email."ElectronicAddress" as "ElectronicAddress",
    hcp_hco_affiliation."HCO_EID" as "X_hco_affiliation_eid",
    hcp_hco_affiliation."Relationship_Type" as "X_hco_affiliation_type",
    hcp_language."Language" as "X_infac360ls_language",
    hcp_origin_university."University_Name" as "X_origin_university_name",
    hcp_origin_university."University_Code" as "X_origin_university_code",
    hcp_tendencies."Code" as "X_infac360ls_tendency_code",
    hcp_tendencies."Rank" as "X_infac360ls_tendency_rank"
from base
left join HMDM_DEV.STAGING_staging.stg_HCP_ADDRESS as hcp_address
    on base."Source_FK" = hcp_address."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_ALTERNATE_NAME as hcp_alternate_name
    on base."Source_FK" = hcp_alternate_name."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_EDUCATION as hcp_education
    on base."Source_FK" = hcp_education."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_EMAIL as hcp_email
    on base."Source_FK" = hcp_email."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_HCO_AFFILIATION as hcp_hco_affiliation
    on base."Source_FK" = hcp_hco_affiliation."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_IDENTIFICATION as hcp_identification
    on base."Source_FK" = hcp_identification."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_LANGUAGE as hcp_language
    on base."Source_FK" = hcp_language."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_LICENSE as hcp_license
    on base."Source_FK" = hcp_license."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_ORIGIN_UNIVERSITY as hcp_origin_university
    on base."Source_FK" = hcp_origin_university."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_PHONE as hcp_phone
    on base."Source_FK" = hcp_phone."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_SPECIALTY as hcp_specialty
    on base."Source_FK" = hcp_specialty."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_TAX as hcp_tax
    on base."Source_FK" = hcp_tax."Source_FK"
left join HMDM_DEV.STAGING_staging.stg_HCP_TENDENCIES as hcp_tendencies
    on base."Source_FK" = hcp_tendencies."Source_FK"