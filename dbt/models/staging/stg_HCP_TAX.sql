-- Staging view for HCP_TAX.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- X_informatica_dea extracted from response_json (DEA section, may be NULL if empty).

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from {{ source('staging', 'HCP_TAX') }}
)
select
    "Source_FK",
    j['DEA'][0]['DEA Number']::VARCHAR as "X_informatica_dea"
from parsed
