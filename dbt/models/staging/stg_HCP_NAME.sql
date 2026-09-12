-- Thin passthrough view over {{ source('staging', 'HCP_NAME') }}.
-- One column per Stg_MDM_Ingress-HCP / Stg_MDM_Ingress-HCO src_attribute
-- entry for this table in the HMDM_DEV mapping workbook. Kept as a plain
-- passthrough (no renaming/casting) so the mart layer below is the single
-- place that applies the Ingress sheet's actual target-attribute mapping.

select
    "X_transparency_reporting_name",
    "firstName",
    "middleName",
    "lastName",
    "fullName",
    "gender",
    "X_infac360ls_type",
    "X_hcp_status",
    "X_jisb_title",
    "prefixName"
from {{ source('staging', 'HCP_NAME') }}
