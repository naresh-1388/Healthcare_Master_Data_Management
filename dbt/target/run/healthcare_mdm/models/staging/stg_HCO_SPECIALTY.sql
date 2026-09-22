
  create or replace   view HMDM_DEV.STAGING_staging.stg_HCO_SPECIALTY
  
   as (
    -- Staging view for HCO_SPECIALTY.
-- Maps Databricks staging columns to dbt-expected names.
-- Specialty fields not present in mock HCO API data -- return NULL.

select
    "iqvia_id" as "Source_FK",
    "source_name" as "Source_Name",
    CAST(NULL AS VARCHAR) as "Specialty_PK",
    CAST(NULL AS VARCHAR) as "Specialty_Rank",
    CAST(NULL AS VARCHAR) as "Specialty",
    CAST(NULL AS VARCHAR) as "Specialty_Type",
    CAST(NULL AS VARCHAR) as "Status",
    CAST(NULL AS VARCHAR) as "Specialty_Source",
    CAST(NULL AS VARCHAR) as "Global_Specialty",
    CAST(NULL AS VARCHAR) as "Group_Specialty"
from HMDM_DEV.STAGING.HCO_SPECIALTY
  );

