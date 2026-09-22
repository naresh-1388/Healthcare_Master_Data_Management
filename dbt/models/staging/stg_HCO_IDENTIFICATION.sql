-- Staging view for HCO_IDENTIFICATION.
-- Maps Databricks staging columns to dbt-expected names.
-- Identifier fields extracted from response_json.

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        "source_name" as "Source_Name",
        PARSE_JSON("response_json") as j
    from {{ source('staging', 'HCO_IDENTIFICATION') }}
)
select
    "Source_FK",
    j['Identifiers'][0]['identifierValue']::VARCHAR as "Identifier_Value",
    CAST(NULL AS VARCHAR) as "Status",
    CAST(NULL AS VARCHAR) as "Identifier_Issuer",
    CAST(NULL AS VARCHAR) as "Issuing_Country",
    CAST(NULL AS VARCHAR) as "Issuing_State",
    CAST(NULL AS VARCHAR) as "Activation_Date",
    CAST(NULL AS VARCHAR) as "Expiration_Date",
    "Source_Name"
from parsed
