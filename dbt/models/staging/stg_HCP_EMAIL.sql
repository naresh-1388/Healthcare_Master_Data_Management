-- Staging view for HCP_EMAIL.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- ElectronicAddress extracted from response_json (may be NULL if list is empty).

with parsed as (
    select
        "iqvia_id" as SOURCE_FK,
        PARSE_JSON("response_json") as j
    from {{ source('staging', 'HCP_EMAIL') }}
)
select
    SOURCE_FK,
    j['Email'][0]::VARCHAR as "ElectronicAddress"
from parsed
