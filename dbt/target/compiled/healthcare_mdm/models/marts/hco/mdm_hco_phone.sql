-- MDM ingress model for HMDM_DEV.MDM.HCO_PHONE
-- Straight column-level mapping from stg_hco_phone, matching the
-- Stg_MDM_Ingress sheet's src_attribute -> tgt_attribute pairs exactly
-- (one dbt source table -> one MDM target table, no join needed).

select
    "Phone_PK" as "sourcePKey",
    "Primary_Phone" as "X_primary_phone",
    "Phone_Usage_Type" as "X_phone_usage_type",
    "Phone_Type" as "X_phone_type",
    "Phone_Number" as "X_phone_number",
    "Phone_Number_Extension" as "X_phone_number_extension",
    "ISO" as "X_iso",
    "Status" as "X_phone_status",
    "Effective_Start_Date" as "X_effective_start_date",
    "Effective_End_Date" as "X_effective_end_date",
    "Source_Name" as "sourceSystem",
    "Source_FK" as "X_phone_parentId"
from HMDM_DEV.STAGING_staging.stg_HCO_PHONE