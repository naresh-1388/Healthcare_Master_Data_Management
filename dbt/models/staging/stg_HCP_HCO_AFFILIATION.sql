-- Thin passthrough view over {{ source('staging', 'HCP_HCO_AFFILIATION') }}.
-- One column per Stg_MDM_Ingress-HCP / Stg_MDM_Ingress-HCO src_attribute
-- entry for this table in the HMDM_DEV mapping workbook. Kept as a plain
-- passthrough (no renaming/casting) so the mart layer below is the single
-- place that applies the Ingress sheet's actual target-attribute mapping.

select
    "HCO_EID",
    "Relationship_Type"
from {{ source('staging', 'HCP_HCO_AFFILIATION') }}
