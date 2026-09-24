-- Egress/Master model for HMDM_DEV.MASTER.HCO_NAME
-- Reads the mastered {{ ref('mdm_hco_name') }} record and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).

select
    "sourceSystem" as "sourceSystem",
    "populationName" as "populationName",
    "HCO_name" as "HCO_name",
    "HCO_companyType" as "HCO_companyType",
    "HCO_countryOfIncorporation" as "HCO_countryOfIncorporation",
    "HCO_X_informatica_bedCount" as "HCO_X_informatica_bedCount",
    "HCO_X_informatica_residentCount" as "HCO_X_informatica_residentCount",
    "HCO_X_informatica_website" as "HCO_X_informatica_website",
    "HCO_X_informatica_type" as "HCO_X_informatica_type",
    "HCO_X_hco_status" as "HCO_X_hco_status",
    "HCO_X_transparency_reporting_name" as "HCO_X_transparency_reporting_name",
    "HCO_X_official_name" as "HCO_X_official_name",
    "HCO_X_parent_organization_name" as "HCO_X_parent_organization_name",
    "HCO_X_TeachingHospitalFlag" as "HCO_X_TeachingHospitalFlag",
    "HCO_X_ProfitFlag" as "HCO_X_ProfitFlag",
    "HCO_X_AcceptMedicare" as "HCO_X_AcceptMedicare",
    "HCO_X_EMedicalRecord" as "HCO_X_EMedicalRecord",
    "HCO_X_PayPerform" as "HCO_X_PayPerform",
    "HCO_X_EPrescribe" as "HCO_X_EPrescribe",
    "HCO_X_Formulary" as "HCO_X_Formulary",
    "HCO_X_ActivationDate" as "HCO_X_ActivationDate",
    "HCO_X_AcceptMedicaid" as "HCO_X_AcceptMedicaid",
    "HCO_X_ownership_status" as "HCO_X_ownership_status",
    "HCO_X_source_createdate" as "HCO_X_source_createdate",
    "HCO_X_source_updatedate" as "HCO_X_source_updatedate",
    "HCO_X_third_party_id" as "HCO_X_third_party_id"
from {{ ref('mdm_hco_name') }}
