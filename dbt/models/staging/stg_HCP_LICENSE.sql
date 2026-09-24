-- Thin passthrough view over {{ source('staging', 'HCP_LICENSE') }}.
-- Synced from Databricks by push_to_snowflake.py — all columns pass through.

-- HCP_LICENSE is not synced from Databricks (no staging table with data).
-- The Snowflake DDL creates a placeholder table, but the column name
-- may differ between DDL versions (X_infa360_License vs X_informatica_License).
-- Use NULL casts to avoid dependency on the placeholder schema.
-- Add a NULL Source_FK so the LEFT JOIN in mdm_hcp.sql does not fail.
select
    CAST(NULL AS VARCHAR) as "Source_FK",
    CAST(NULL AS VARCHAR) as "X_informatica_License"
from {{ source('staging', 'HCP_LICENSE') }}
where 1 = 0
