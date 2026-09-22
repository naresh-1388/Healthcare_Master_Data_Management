-- Staging view for HCO_NAME.
-- Maps Databricks staging columns to dbt-expected names.
-- Name fields from existing table columns + response_json extraction.

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        "source_name" as "Source_Name",
        "organization_name" as "HCO_Name",
        "organization_type" as "HCO_Subtype",
        "country_code" as "Country",
        PARSE_JSON("response_json") as j
    from {{ source('staging', 'HCO_NAME') }}
)
select
    "Source_FK",
    "Source_Name",
    CAST(NULL AS VARCHAR) as "Population_Name",
    "HCO_Name",
    "HCO_Subtype",
    "Country",
    CAST(NULL AS VARCHAR) as "Bed_Count",
    CAST(NULL AS VARCHAR) as "Resident_Count",
    CAST(NULL AS VARCHAR) as "Website",
    CAST(NULL AS VARCHAR) as "HCO_Type",
    CAST(NULL AS VARCHAR) as "Status",
    CAST(NULL AS VARCHAR) as "Transparency_Reporting_Name",
    CAST(NULL AS VARCHAR) as "Official_Name",
    CAST(NULL AS VARCHAR) as "Parent_Organization_Name",
    CAST(NULL AS VARCHAR) as "Teaching_Hospital_Flag",
    CAST(NULL AS VARCHAR) as "Profit_Flag",
    CAST(NULL AS VARCHAR) as "Accept_Medicare",
    CAST(NULL AS VARCHAR) as "E_Medical_Record",
    CAST(NULL AS VARCHAR) as "Pay_Perform",
    CAST(NULL AS VARCHAR) as "E_Prescribe",
    CAST(NULL AS VARCHAR) as "Formulary",
    CAST(NULL AS VARCHAR) as "Activation_Date",
    CAST(NULL AS VARCHAR) as "Accept_Medicaid",
    CAST(NULL AS VARCHAR) as "Ownership_Status",
    CAST(NULL AS VARCHAR) as "Source_Created_Date",
    CAST(NULL AS VARCHAR) as "Source_Updated_Date",
    CAST(NULL AS VARCHAR) as "Third_Party_ID"
from parsed
