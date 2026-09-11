-- Thin passthrough view over {{ source('staging', 'HCO_PHONE') }}.
-- One column per Stg_MDM_Ingress-HCP / Stg_MDM_Ingress-HCO src_attribute
-- entry for this table in the HMDM_DEV mapping workbook. Kept as a plain
-- passthrough (no renaming/casting) so the mart layer below is the single
-- place that applies the Ingress sheet's actual target-attribute mapping.

select
    "Phone_PK",
    "Primary_Phone",
    "Phone_Usage_Type",
    "Phone_Type",
    "Phone_Number",
    "Phone_Number_Extension",
    "ISO",
    "Status",
    "Effective_Start_Date",
    "Effective_End_Date",
    "Source_Name",
    "Source_FK"
from {{ source('staging', 'HCO_PHONE') }}
