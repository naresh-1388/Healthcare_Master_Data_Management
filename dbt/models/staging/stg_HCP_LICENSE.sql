-- Staging view for HCP_LICENSE.
-- HCP_LICENSE is NOT synced from Databricks (no staging table with data).
-- The Snowflake DDL creates a placeholder table, but the column name
-- may differ between DDL versions (X_infa360_License vs X_informatica_License).
-- Use NULL casts with WHERE 1=0 to avoid dependency on the placeholder schema.
-- Output column X_informatica_License matches what mdm_hcp.sql expects.
-- Add a NULL Source_FK so the LEFT JOIN in mdm_hcp.sql does not fail.
select
    CAST(NULL AS VARCHAR) as SOURCE_FK,
    CAST(NULL AS VARCHAR) as "X_informatica_License"
from {{ source('staging', 'HCP_LICENSE') }}
where 1 = 0
