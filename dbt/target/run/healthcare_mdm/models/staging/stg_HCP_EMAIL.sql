
  create or replace   view HMDM_DEV.STAGING_staging.stg_HCP_EMAIL
  
   as (
    -- Staging view for HCP_EMAIL.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- ElectronicAddress extracted from response_json (may be NULL if list is empty).

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from HMDM_DEV.STAGING.HCP_EMAIL
)
select
    "Source_FK",
    j['Email'][0]::VARCHAR as "ElectronicAddress"
from parsed
  );

