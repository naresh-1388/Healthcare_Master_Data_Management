-- MDM ingress model for HMDM_DEV.MDM.HCO
-- Consolidates 6 STAGING tables onto the single flat
-- MDM.HCO base object, matching the Stg_MDM_Ingress sheet exactly.
-- Anchored on HCO_ADDRESS (address record) and left-joined to HCO_NAME
-- (org identity), HCO_ALTERNATE_NAME, HCO_EMAIL, HCO_HIERARCHY, and
-- HCO_TAX on Source_FK - this mirrors how Informatica MDM lands multiple
-- attribute groups directly onto one base object.
-- FIX #3: Source_FK included in output for schema.yml not_null tests.
-- FIX #6: Added join to stg_HCO_NAME for org identity (name, type, status,
-- flags, dates). Previously missing this join left HCO without org identity.

with base as (
    select * from {{ ref('stg_HCO_ADDRESS') }}
)

select
    base.SOURCE_FK as SOURCE_FK,
    hco_name."HCO_Name" as "HCO_name",
    hco_name."HCO_Subtype" as "HCO_companyType",
    hco_name."HCO_Type" as "HCO_X_informatica_type",
    hco_name."Status" as "HCO_X_hco_status",
    hco_name."Transparency_Reporting_Name" as "HCO_X_transparency_reporting_name",
    hco_name."Official_Name" as "HCO_X_official_name",
    hco_name."Parent_Organization_Name" as "HCO_X_parent_organization_name",
    hco_name."Teaching_Hospital_Flag" as "HCO_X_TeachingHospitalFlag",
    hco_name."Profit_Flag" as "HCO_X_ProfitFlag",
    hco_name."Accept_Medicare" as "HCO_X_AcceptMedicare",
    hco_name."E_Medical_Record" as "HCO_X_EMedicalRecord",
    hco_name."Pay_Perform" as "HCO_X_PayPerform",
    hco_name."E_Prescribe" as "HCO_X_EPrescribe",
    hco_name."Formulary" as "HCO_X_Formulary",
    hco_name."Activation_Date" as "HCO_X_ActivationDate",
    hco_name."Accept_Medicaid" as "HCO_X_AcceptMedicaid",
    hco_name."Ownership_Status" as "HCO_X_ownership_status",
    hco_name."Source_Created_Date" as "HCO_X_source_createdate",
    hco_name."Source_Updated_Date" as "HCO_X_source_updatedate",
    hco_name."Third_Party_ID" as "HCO_X_third_party_id",
    hco_name."Source_Name" as "sourceSystem",
    hco_name."Population_Name" as "populationName",
    hco_name."Country" as "HCO_countryOfIncorporation",
    hco_name."Bed_Count" as "HCO_X_informatica_bedCount",
    hco_name."Resident_Count" as "HCO_X_informatica_residentCount",
    hco_name."Website" as "HCO_X_informatica_website",
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
left join {{ ref('stg_HCO_NAME') }} as hco_name
    on base.SOURCE_FK = hco_name.SOURCE_FK
left join {{ ref('stg_HCO_ALTERNATE_NAME') }} as hco_alternate_name
    on base.SOURCE_FK = hco_alternate_name.SOURCE_FK
left join {{ ref('stg_HCO_EMAIL') }} as hco_email
    on base.SOURCE_FK = hco_email.SOURCE_FK
left join {{ ref('stg_HCO_HIERARCHY') }} as hco_hierarchy
    on base.SOURCE_FK = hco_hierarchy.SOURCE_FK
left join {{ ref('stg_HCO_TAX') }} as hco_tax
    on base.SOURCE_FK = hco_tax.SOURCE_FK
