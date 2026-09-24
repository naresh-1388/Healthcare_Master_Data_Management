-- MDM ingress model for HMDM_DEV.MDM.HCO
-- Consolidates 5 STAGING tables onto the single flat
-- MDM.HCO base object, matching the Stg_MDM_Ingress sheet exactly.
-- Anchored on HCO_ADDRESS (the primary name/identity record) and left-joined
-- to every other contributing table on Source_FK - this mirrors how
-- Informatica MDM lands multiple attribute groups directly onto one
-- base object.

with base as (
    select * from {{ ref('stg_HCO_ADDRESS') }}
)

select
    base."Address_Line_1" as "X_hco_address",
    base."City" as "X_hco_city",
    base."Postal_Code" as "X_hco_postal_code",
    base."Country" as "X_hco_country",
    hco_alternate_name."Alternate_Name" as "AlternateName",
    hco_alternate_name."Alternate_Name_Type" as "AlternateNameType",
    hco_email."Email" as "ElectronicAddress",
    hco_hierarchy."Parent_Organization_EID" as "X_parent_organization_eid",
    hco_hierarchy."Relationship_Type" as "X_hierarchy_relationship_type",
    hco_tax."Tax_Number" as "X_informatica_tax_number"
from base
left join {{ ref('stg_HCO_ALTERNATE_NAME') }} as hco_alternate_name
    on base."Source_FK" = hco_alternate_name."Source_FK"
left join {{ ref('stg_HCO_EMAIL') }} as hco_email
    on base."Source_FK" = hco_email."Source_FK"
left join {{ ref('stg_HCO_HIERARCHY') }} as hco_hierarchy
    on base."Source_FK" = hco_hierarchy."Source_FK"
left join {{ ref('stg_HCO_TAX') }} as hco_tax
    on base."Source_FK" = hco_tax."Source_FK"
