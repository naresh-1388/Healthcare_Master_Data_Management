-- Staging view for HCP_IDENTIFICATION.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- AlternateIdentifier extracted from response_json.

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from HMDM_DEV.STAGING.HCP_IDENTIFICATION
)
select
    "Source_FK",
    j['Alternate Identifier'][0]['Identifier Value']::VARCHAR as "AlternateIdentifier"
from parsed