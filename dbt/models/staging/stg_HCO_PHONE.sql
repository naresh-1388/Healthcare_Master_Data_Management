-- Staging view for HCO_PHONE.
-- Maps Databricks staging columns to dbt-expected names.
-- Phone fields extracted from response_json.

with parsed as (
    select
        "iqvia_id" as SOURCE_FK,
        "source_name" as "Source_Name",
        PARSE_JSON("response_json") as j
    from {{ source('staging', 'HCO_PHONE') }}
)
select
    SOURCE_FK,
    CAST(NULL AS VARCHAR) as "Phone_PK",
    j['Phones'][0]['phoneNumber']::VARCHAR as "Primary_Phone",
    CAST(NULL AS VARCHAR) as "Phone_Usage_Type",
    j['Phones'][0]['phoneType']::VARCHAR as "Phone_Type",
    j['Phones'][0]['phoneNumber']::VARCHAR as "Phone_Number",
    CAST(NULL AS VARCHAR) as "Phone_Number_Extension",
    CAST(NULL AS VARCHAR) as "ISO",
    CAST(NULL AS VARCHAR) as "Status",
    CAST(NULL AS VARCHAR) as "Effective_Start_Date",
    CAST(NULL AS VARCHAR) as "Effective_End_Date",
    "Source_Name"
from parsed
