-- Thin passthrough view over { source('staging', 'HCO_HIERARCHY') }.
-- Synced from Databricks by push_to_snowflake.py — all columns pass through.

select *
from { source('staging', 'HCO_HIERARCHY') }
