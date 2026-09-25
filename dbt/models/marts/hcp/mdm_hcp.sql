-- MDM ingress model for HMDM_DEV.MDM.HCP
-- Consolidates 14 STAGING tables onto the single flat
-- MDM.HCP base object, matching the Stg_MDM_Ingress sheet exactly.
-- Anchored on HCP_NAME (the primary name/identity record) and left-joined
-- to every other contributing table on Source_FK - this mirrors how
-- Informatica MDM lands multiple attribute groups directly onto one
-- base object.
-- FIX #3: Source_FK included in output for schema.yml not_null tests.

with base as (
    select * from {{ ref('stg_HCP_NAME') }}
)

select
    base.SOURCE_FK as SOURCE_FK,
    base."X_transparency_reporting_name" as "X_transparency_reporting_name",
    base."firstName" as "firstName",
    base."middleName" as "middleName",
    base."lastName" as "lastName",
    base."fullName" as "fullName",
    base."gender" as "gender",
    base."X_informatica_type" as "X_informatica_type",
    base."X_hcp_status" as "X_hcp_status",
    base."X_iqvia_title" as "X_iqvia_title",
    base."prefixName" as "prefixName",
    hcp_alternate_name."AlternateName" as "AlternateName",
    hcp_address."X_hcp_address" as "X_hcp_address",
    hcp_phone."Phone" as "Phone",
    hcp_specialty."X_informatica_Specialty" as "X_informatica_Specialty",
    hcp_education."Qualification" as "Qualification",
    hcp_license."X_informatica_License" as "X_informatica_License",
    hcp_tax."X_informatica_dea" as "X_informatica_dea",
    hcp_identification."AlternateIdentifier" as "AlternateIdentifier",
    hcp_email."ElectronicAddress" as "ElectronicAddress",
    hcp_hco_affiliation."HCO_EID" as "X_hco_affiliation_eid",
    hcp_hco_affiliation."Relationship_Type" as "X_hco_affiliation_type",
    hcp_language."Language" as "X_informatica_language",
    hcp_origin_university."University_Name" as "X_origin_university_name",
    hcp_origin_university."University_Code" as "X_origin_university_code",
    hcp_tendencies."Code" as "X_informatica_tendency_code",
    hcp_tendencies."Rank" as "X_informatica_tendency_rank"
from base
left join {{ ref('stg_HCP_ADDRESS') }} as hcp_address
    on base.SOURCE_FK = hcp_address.SOURCE_FK
left join {{ ref('stg_HCP_ALTERNATE_NAME') }} as hcp_alternate_name
    on base.SOURCE_FK = hcp_alternate_name.SOURCE_FK
left join {{ ref('stg_HCP_EDUCATION') }} as hcp_education
    on base.SOURCE_FK = hcp_education.SOURCE_FK
left join {{ ref('stg_HCP_EMAIL') }} as hcp_email
    on base.SOURCE_FK = hcp_email.SOURCE_FK
left join {{ ref('stg_HCP_HCO_AFFILIATION') }} as hcp_hco_affiliation
    on base.SOURCE_FK = hcp_hco_affiliation.SOURCE_FK
left join {{ ref('stg_HCP_IDENTIFICATION') }} as hcp_identification
    on base.SOURCE_FK = hcp_identification.SOURCE_FK
left join {{ ref('stg_HCP_LANGUAGE') }} as hcp_language
    on base.SOURCE_FK = hcp_language.SOURCE_FK
left join {{ ref('stg_HCP_LICENSE') }} as hcp_license
    on base.SOURCE_FK = hcp_license.SOURCE_FK
left join {{ ref('stg_HCP_ORIGIN_UNIVERSITY') }} as hcp_origin_university
    on base.SOURCE_FK = hcp_origin_university.SOURCE_FK
left join {{ ref('stg_HCP_PHONE') }} as hcp_phone
    on base.SOURCE_FK = hcp_phone.SOURCE_FK
left join {{ ref('stg_HCP_SPECIALTY') }} as hcp_specialty
    on base.SOURCE_FK = hcp_specialty.SOURCE_FK
left join {{ ref('stg_HCP_TAX') }} as hcp_tax
    on base.SOURCE_FK = hcp_tax.SOURCE_FK
left join {{ ref('stg_HCP_TENDENCIES') }} as hcp_tendencies
    on base.SOURCE_FK = hcp_tendencies.SOURCE_FK
