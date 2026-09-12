-- Egress/Master model for HMDM_DEV.MASTER.HCP_THERAPEUTIC_AREA
-- Reads the Informatica-MDM-hub-exposed child object directly
-- (not a model built by this dbt project - see the 'mdm_hub' source in
-- models/staging/sources.yml) and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).

select
    "X_infa360_TherapeuticArea_parentId" as "Global_HCP_ID",
    "X_infa360_activeIndicator" as "Active_Indicator",
    "X_infa360_therapeuticArea" as "Therapeutic_Area",
    "O_Load_Date" as "Load_Date"
from {{ source('mdm_hub', 'hcp_therapeutic_area') }}
