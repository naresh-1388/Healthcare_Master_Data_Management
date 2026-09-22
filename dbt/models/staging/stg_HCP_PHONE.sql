-- Staging view for HCP_PHONE.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- Phone extracted from response_json.

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from { source('staging', 'HCP_PHONE') }
)
select
    "Source_FK",
    j['Phone'][0]['Phone Number']::VARCHAR as "Phone"
from parsed
