-- Staging view for HCP_SPECIALTY.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- X_informatica_Specialty extracted from response_json.

with parsed as (
    select
        "iqvia_id" as SOURCE_FK,
        PARSE_JSON("response_json") as j
    from {{ source('staging', 'HCP_SPECIALTY') }}
)
select
    SOURCE_FK,
    j['Specialty'][0]['Specialty Class']::VARCHAR as "X_informatica_Specialty"
from parsed
