
  create or replace   view HMDM_DEV.STAGING_staging.stg_HCP_LICENSE
  
   as (
    -- Thin passthrough view over HMDM_DEV.STAGING.HCP_LICENSE.
-- Synced from Databricks by push_to_snowflake.py — all columns pass through.

-- HCP_LICENSE is not synced from Databricks (no staging table with data).
-- The Snowflake DDL creates a placeholder with only X_infac360ls_License.
-- Add a NULL Source_FK so the LEFT JOIN in mdm_hcp.sql does not fail.
select
    CAST(NULL AS VARCHAR) as "Source_FK",
    "X_infac360ls_License"
from HMDM_DEV.STAGING.HCP_LICENSE
  );

