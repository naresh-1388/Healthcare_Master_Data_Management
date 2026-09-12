-- Thin passthrough view over {{ source('staging', 'HCO_ADDRESS') }}.
-- One column per Stg_MDM_Ingress-HCP / Stg_MDM_Ingress-HCO src_attribute
-- entry for this table in the HMDM_DEV mapping workbook. Kept as a plain
-- passthrough (no renaming/casting) so the mart layer below is the single
-- place that applies the Ingress sheet's actual target-attribute mapping.

select
    "Address_Line_1",
    "City",
    "Postal_Code",
    "Country"
from {{ source('staging', 'HCO_ADDRESS') }}
