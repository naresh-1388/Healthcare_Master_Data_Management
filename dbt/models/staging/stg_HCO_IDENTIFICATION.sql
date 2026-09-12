-- Thin passthrough view over {{ source('staging', 'HCO_IDENTIFICATION') }}.
-- One column per Stg_MDM_Ingress-HCP / Stg_MDM_Ingress-HCO src_attribute
-- entry for this table in the HMDM_DEV mapping workbook. Kept as a plain
-- passthrough (no renaming/casting) so the mart layer below is the single
-- place that applies the Ingress sheet's actual target-attribute mapping.

select
    "Identifier_Value",
    "Status",
    "Identifier_Issuer",
    "Issuing_Country",
    "Issuing_State",
    "Activation_Date",
    "Expiration_Date",
    "Source_Name",
    "Source_FK"
from {{ source('staging', 'HCO_IDENTIFICATION') }}
