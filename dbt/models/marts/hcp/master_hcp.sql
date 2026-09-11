-- Egress/Master model for HMDM_DEV.MASTER.HCP
-- Reads the mastered {{ ref('mdm_hcp') }} record and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).

select
    "X_transparency_reporting_name" as "Transparency_Reporting_Name",
    "firstName" as "First_Name",
    "middleName" as "Middle_Name",
    "lastName" as "Last_Name",
    "fullName" as "Full_Name",
    "gender" as "Gender",
    "X_infac360ls_type" as "HCP_Type",
    "X_hcp_status" as "Status",
    "X_jisb_title" as "Title",
    "prefixName" as "Prefix_Name",
    "X_hcp_address" as "Address",
    "Phone" as "Phone",
    "Qualification" as "Qualification",
    "X_infac360ls_dea" as "DEA_Number",
    "AlternateIdentifier" as "Alternate_Identifier",
    "ElectronicAddress" as "Email",
    "X_hco_affiliation_eid" as "HCO_Affiliation_EID",
    "X_hco_affiliation_type" as "HCO_Affiliation_Type",
    "X_infac360ls_language" as "Language",
    "X_origin_university_name" as "Origin_University_Name",
    "X_origin_university_code" as "Origin_University_Code",
    "X_infac360ls_tendency_code" as "Tendency_Code",
    "X_infac360ls_tendency_rank" as "Tendency_Rank"
from {{ ref('mdm_hcp') }}
