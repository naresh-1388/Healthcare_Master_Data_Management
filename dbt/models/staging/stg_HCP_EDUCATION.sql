-- Thin passthrough view over { source('staging', 'HCP_EDUCATION') }.
-- Synced from Databricks by push_to_snowflake.py — all columns pass through.

select *
from { source('staging', 'HCP_EDUCATION') }
