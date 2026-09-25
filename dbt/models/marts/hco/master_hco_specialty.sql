-- Egress/Master model for HMDM_DEV.MASTER.HCO_SPECIALTY
-- Reads the mastered {{ ref('mdm_hco_specialty') }} record and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).
-- FIX #3: Source_FK included in output for schema.yml not_null tests.

select
    "Source_FK" as "Source_FK",
    "sourcePKey" as "sourcePKey",
    "X_informatica_rank" as "X_informatica_rank",
    "X_informatica_Specialty" as "X_informatica_Specialty",
    "X_specialty_type" as "X_specialty_type",
    "X_specialty_status" as "X_specialty_status",
    "X_specialty_source" as "X_specialty_source",
    "X_global_specialty" as "X_global_specialty",
    "X_group_specialty" as "X_group_specialty"
from {{ ref('mdm_hco_specialty') }}
