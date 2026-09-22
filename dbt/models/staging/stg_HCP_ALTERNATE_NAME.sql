-- Staging view for HCP_ALTERNATE_NAME.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- AlternateName extracted from response_json (may be NULL if list is empty).

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from { source('staging', 'HCP_ALTERNATE_NAME') }
)
select
    "Source_FK",
    j['Alternate Name'][0]['Alternate Name']::VARCHAR as "AlternateName"
from parsed
