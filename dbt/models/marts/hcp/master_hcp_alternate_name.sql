-- Egress/Master model for HMDM_DEV.MASTER.HCP_ALTERNATE_NAME
-- Reads the Informatica-MDM-hub-exposed child object directly
-- (not a model built by this dbt project - see the 'mdm_hub' source in
-- models/staging/sources.yml) and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).

select
    "alternateNameType" as "Name_Type",
    "AlternateName" as "Alternate_Name",
    "X_alternate_name_status" as "Alternate_Name_Status",
    "effectiveStartDate" as "Effective_Start_Date",
    "effectiveEndDate" as "Effective_End_Date",
    "O_Load_Date" as "Load_Date"
from {{ source('mdm_hub', 'hcp_alternate_name') }}
