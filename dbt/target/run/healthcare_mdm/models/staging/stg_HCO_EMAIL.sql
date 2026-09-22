
  create or replace   view HMDM_DEV.STAGING_staging.stg_HCO_EMAIL
  
   as (
    -- Staging view for HCO_EMAIL.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hco.sql).
-- Email field not present in mock API data -- return NULL.

select
    "iqvia_id" as "Source_FK",
    CAST(NULL AS VARCHAR) as "Email"
from HMDM_DEV.STAGING.HCO_EMAIL
  );

