-- MDM ingress model for HMDM_DEV.MDM.HCO_NAME
-- Straight column-level mapping from stg_hco_name, matching the
-- Stg_MDM_Ingress sheet's src_attribute -> tgt_attribute pairs exactly
-- (one dbt source table -> one MDM target table, no join needed).

select
    "Source_Name" as "sourceSystem",
    "Population_Name" as "populationName",
    "HCO_Name" as "HCO_name",
    "HCO_Subtype" as "HCO_companyType",
    "Country" as "HCO_countryOfIncorporation",
    "Bed_Count" as "HCO_X_informatica_bedCount",
    "Resident_Count" as "HCO_X_informatica_residentCount",
    "Website" as "HCO_X_informatica_website",
    "HCO_Type" as "HCO_X_informatica_type",
    "Status" as "HCO_X_hco_status",
    "Transparency_Reporting_Name" as "HCO_X_transparency_reporting_name",
    "Official_Name" as "HCO_X_official_name",
    "Parent_Organization_Name" as "HCO_X_parent_organization_name",
    "Teaching_Hospital_Flag" as "HCO_X_TeachingHospitalFlag",
    "Profit_Flag" as "HCO_X_ProfitFlag",
    "Accept_Medicare" as "HCO_X_AcceptMedicare",
    "E_Medical_Record" as "HCO_X_EMedicalRecord",
    "Pay_Perform" as "HCO_X_PayPerform",
    "E_Prescribe" as "HCO_X_EPrescribe",
    "Formulary" as "HCO_X_Formulary",
    "Activation_Date" as "HCO_X_ActivationDate",
    "Accept_Medicaid" as "HCO_X_AcceptMedicaid",
    "Ownership_Status" as "HCO_X_ownership_status",
    "Source_Created_Date" as "HCO_X_source_createdate",
    "Source_Updated_Date" as "HCO_X_source_updatedate",
    "Third_Party_ID" as "HCO_X_third_party_id"
from {{ ref('stg_HCO_NAME') }}
