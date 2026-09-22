-- Staging view for HCP_TENDENCIES.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- Tendency fields not present in mock API data -- return NULL.

select
    "iqvia_id" as "Source_FK",
    CAST(NULL AS VARCHAR) as "Code",
    CAST(NULL AS VARCHAR) as "Rank"
from HMDM_DEV.STAGING.HCP_TENDENCIES