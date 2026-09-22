-- Staging view for HCO_TAX.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hco.sql).
-- Tax_Number extracted from response_json ('Tax Id' field).

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from {{ source('staging', 'HCO_TAX') }}
)
select
    "Source_FK",
    j['Tax Id']::VARCHAR as "Tax_Number"
from parsed
