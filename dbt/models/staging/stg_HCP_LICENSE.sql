-- Thin passthrough view over { source('staging', 'HCP_LICENSE') }.
-- Synced from Databricks by push_to_snowflake.py — all columns pass through.

-- HCP_LICENSE is not synced from Databricks (no staging table with data).
-- The Snowflake DDL creates a placeholder with only X_infac360ls_License.
-- Add a NULL Source_FK so the LEFT JOIN in mdm_hcp.sql does not fail.
select
    CAST(NULL AS VARCHAR) as "Source_FK",
    "X_infac360ls_License"
from { source('staging', 'HCP_LICENSE') }
