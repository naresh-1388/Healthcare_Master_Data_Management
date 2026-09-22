-- Staging view for HCP_LANGUAGE.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- Language not present in mock API data -- return NULL.

select
    "iqvia_id" as "Source_FK",
    CAST(NULL AS VARCHAR) as "Language"
from {{ source('staging', 'HCP_LANGUAGE') }}
