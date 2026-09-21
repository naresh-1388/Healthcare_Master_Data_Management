-- Thin passthrough view over { source('staging', 'HCO_ADDRESS') }.
-- Synced from Databricks by push_to_snowflake.py — all columns pass through.

select *
from { source('staging', 'HCO_ADDRESS') }
