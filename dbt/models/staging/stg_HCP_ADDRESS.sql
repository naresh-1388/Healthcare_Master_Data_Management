-- Staging view for HCP_ADDRESS.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- X_hcp_address extracted from response_json.

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from {{ source('staging', 'HCP_ADDRESS') }}
)
select
    "Source_FK",
    j['HCP Address'][0]['Address Line 1']::VARCHAR as "X_hcp_address"
from parsed
