-- Thin passthrough view over {{ source('staging', 'HCO_NAME') }}.
-- One column per Stg_MDM_Ingress-HCP / Stg_MDM_Ingress-HCO src_attribute
-- entry for this table in the HMDM_DEV mapping workbook. Kept as a plain
-- passthrough (no renaming/casting) so the mart layer below is the single
-- place that applies the Ingress sheet's actual target-attribute mapping.

select
    "Source_Name",
    "Population_Name",
    "HCO_Name",
    "HCO_Subtype",
    "Country",
    "Bed_Count",
    "Resident_Count",
    "Website",
    "HCO_Type",
    "Status",
    "Transparency_Reporting_Name",
    "Official_Name",
    "Parent_Organization_Name",
    "Teaching_Hospital_Flag",
    "Profit_Flag",
    "Accept_Medicare",
    "E_Medical_Record",
    "Pay_Perform",
    "E_Prescribe",
    "Formulary",
    "Activation_Date",
    "Accept_Medicaid",
    "Ownership_Status",
    "Source_Created_Date",
    "Source_Updated_Date",
    "Third_Party_ID"
from {{ source('staging', 'HCO_NAME') }}
