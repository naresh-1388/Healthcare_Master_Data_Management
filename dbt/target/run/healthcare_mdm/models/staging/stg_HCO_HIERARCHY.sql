
  create or replace   view HMDM_DEV.STAGING_staging.stg_HCO_HIERARCHY
  
   as (
    -- Staging view for HCO_HIERARCHY.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hco.sql).
-- Hierarchy fields not present in mock API data -- return NULL.

select
    "iqvia_id" as "Source_FK",
    CAST(NULL AS VARCHAR) as "Parent_Organization_EID",
    CAST(NULL AS VARCHAR) as "Relationship_Type"
from HMDM_DEV.STAGING.HCO_HIERARCHY
  );

