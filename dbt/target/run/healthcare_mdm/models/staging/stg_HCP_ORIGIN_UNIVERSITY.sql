
  create or replace   view HMDM_DEV.STAGING_staging.stg_HCP_ORIGIN_UNIVERSITY
  
   as (
    -- Staging view for HCP_ORIGIN_UNIVERSITY.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- University fields not present in mock API data -- return NULL.

select
    "iqvia_id" as "Source_FK",
    CAST(NULL AS VARCHAR) as "University_Name",
    CAST(NULL AS VARCHAR) as "University_Code"
from HMDM_DEV.STAGING.HCP_ORIGIN_UNIVERSITY
  );

