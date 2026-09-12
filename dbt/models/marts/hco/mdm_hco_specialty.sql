-- MDM ingress model for HMDM_DEV.MDM.HCO_SPECIALTY
-- Straight column-level mapping from stg_hco_specialty, matching the
-- Stg_MDM_Ingress sheet's src_attribute -> tgt_attribute pairs exactly
-- (one dbt source table -> one MDM target table, no join needed).

select
    "Specialty_PK" as "sourcePKey",
    "Specialty_Rank" as "X_infa360_rank",
    "Specialty" as "X_infa360_Specialty",
    "Specialty_Type" as "X_specialty_type",
    "Status" as "X_specialty_status",
    "Specialty_Source" as "X_specialty_source",
    "Global_Specialty" as "X_global_specialty",
    "Group_Specialty" as "X_group_specialty",
    "Source_Name" as "sourceSystem"
from {{ ref('stg_hco_specialty') }}
