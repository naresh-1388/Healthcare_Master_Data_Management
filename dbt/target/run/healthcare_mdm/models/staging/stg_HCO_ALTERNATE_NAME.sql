
  create or replace   view HMDM_DEV.STAGING_staging.stg_HCO_ALTERNATE_NAME
  
   as (
    -- Staging view for HCO_ALTERNATE_NAME.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hco.sql).
-- Alternate Name fields not present in mock API data -- return NULL.

select
    "iqvia_id" as "Source_FK",
    CAST(NULL AS VARCHAR) as "Alternate_Name",
    CAST(NULL AS VARCHAR) as "Alternate_Name_Type"
from HMDM_DEV.STAGING.HCO_ALTERNATE_NAME
  );

