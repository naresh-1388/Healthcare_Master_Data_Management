-- MDM ingress model for HMDM_DEV.MDM.HCP
-- Consolidates 14 STAGING tables onto the single flat
-- MDM.HCP base object, matching the Stg_MDM_Ingress sheet exactly.
-- Anchored on HCP_NAME (the primary name/identity record) and left-joined
-- to every other contributing table on Source_FK - this mirrors how
-- Informatica MDM lands multiple attribute groups directly onto one
-- base object.
-- FIX #3: Source_FK included in output for schema.yml not_null tests.

-- FIX #10: Pre-deduplicate each child table to one row per SOURCE_FK
-- using ROW_NUMBER before joining. This prevents row multiplication
-- (cartesian product) when child tables have multiple rows per entity.
-- Each child keeps the most recently loaded record (LOAD_DATE desc).

with base as (
    select * from {{ ref('stg_HCP_NAME') }}
),

hcp_address as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_ADDRESS') }}
    ) where _rn = 1
),

hcp_alternate_name as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_ALTERNATE_NAME') }}
    ) where _rn = 1
),

hcp_education as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_EDUCATION') }}
    ) where _rn = 1
),

hcp_email as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_EMAIL') }}
    ) where _rn = 1
),

hcp_hco_affiliation as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_HCO_AFFILIATION') }}
    ) where _rn = 1
),

hcp_identification as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_IDENTIFICATION') }}
    ) where _rn = 1
),

hcp_language as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_LANGUAGE') }}
    ) where _rn = 1
),

hcp_license as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_LICENSE') }}
    ) where _rn = 1
),

hcp_origin_university as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_ORIGIN_UNIVERSITY') }}
    ) where _rn = 1
),

hcp_phone as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_PHONE') }}
    ) where _rn = 1
),

hcp_specialty as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_SPECIALTY') }}
    ) where _rn = 1
),

hcp_tax as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_TAX') }}
    ) where _rn = 1
),

hcp_tendencies as (
    select * from (
        select *, row_number() over (partition by SOURCE_FK order by LOAD_DATE desc nulls last) as _rn
        from {{ ref('stg_HCP_TENDENCIES') }}
    ) where _rn = 1
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
left join hcp_address
    on base.SOURCE_FK = hcp_address.SOURCE_FK
left join hcp_alternate_name
    on base.SOURCE_FK = hcp_alternate_name.SOURCE_FK
left join hcp_education
    on base.SOURCE_FK = hcp_education.SOURCE_FK
left join hcp_email
    on base.SOURCE_FK = hcp_email.SOURCE_FK
left join hcp_hco_affiliation
    on base.SOURCE_FK = hcp_hco_affiliation.SOURCE_FK
left join hcp_identification
    on base.SOURCE_FK = hcp_identification.SOURCE_FK
left join hcp_language
    on base.SOURCE_FK = hcp_language.SOURCE_FK
left join hcp_license
    on base.SOURCE_FK = hcp_license.SOURCE_FK
left join hcp_origin_university
    on base.SOURCE_FK = hcp_origin_university.SOURCE_FK
left join hcp_phone
    on base.SOURCE_FK = hcp_phone.SOURCE_FK
left join hcp_specialty
    on base.SOURCE_FK = hcp_specialty.SOURCE_FK
left join hcp_tax
    on base.SOURCE_FK = hcp_tax.SOURCE_FK
left join hcp_tendencies
    on base.SOURCE_FK = hcp_tendencies.SOURCE_FK
