-- Egress/Master model for HMDM_DEV.MASTER.HCO_PHONE
-- Reads the mastered {{ ref('mdm_hco_phone') }} record and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).
-- FIX #3: Source_FK included in output for schema.yml not_null tests.

select
    "Source_FK" as "Source_FK",
    "sourcePKey" as "sourcePKey",
    "X_primary_phone" as "X_primary_phone",
    "X_phone_usage_type" as "X_phone_usage_type",
    "X_phone_type" as "X_phone_type",
    "X_phone_number" as "X_phone_number",
    "X_phone_number_extension" as "X_phone_number_extension",
    "X_iso" as "X_iso",
    "X_phone_status" as "X_phone_status",
    "X_effective_start_date" as "X_effective_start_date",
    "X_effective_end_date" as "X_effective_end_date",
    "X_phone_parentId" as "X_phone_parentId"
from {{ ref('mdm_hco_phone') }}
