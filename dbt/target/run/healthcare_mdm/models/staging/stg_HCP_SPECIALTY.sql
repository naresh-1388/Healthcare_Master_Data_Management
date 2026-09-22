
  create or replace   view HMDM_DEV.STAGING_staging.stg_HCP_SPECIALTY
  
   as (
    -- Staging view for HCP_SPECIALTY.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- X_infac360ls_Specialty extracted from response_json.

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from HMDM_DEV.STAGING.HCP_SPECIALTY
)
select
    "Source_FK",
    j['Specialty'][0]['Specialty Class']::VARCHAR as "X_infac360ls_Specialty"
from parsed
  );

