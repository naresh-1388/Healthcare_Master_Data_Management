-- Egress/Master model for HMDM_DEV.MASTER.HCO_SPECIALTY
-- Reads the mastered HMDM_DEV.STAGING_mdm.mdm_hco_specialty record and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).

select
    "sourcePKey" as "sourcePKey",
    "X_infa360_rank" as "X_infa360_rank",
    "X_infa360_Specialty" as "X_infa360_Specialty",
    "X_specialty_type" as "X_specialty_type",
    "X_specialty_status" as "X_specialty_status",
    "X_specialty_source" as "X_specialty_source",
    "X_global_specialty" as "X_global_specialty",
    "X_group_specialty" as "X_group_specialty"
from HMDM_DEV.STAGING_mdm.mdm_hco_specialty