-- Staging view for HCP_EDUCATION.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- Qualification not present in mock API data -- return NULL.

select
    "iqvia_id" as SOURCE_FK,
    CAST(NULL AS VARCHAR) as "Qualification"
from {{ source('staging', 'HCP_EDUCATION') }}
