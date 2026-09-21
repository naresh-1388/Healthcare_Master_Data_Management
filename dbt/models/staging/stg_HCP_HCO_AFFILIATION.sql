-- Thin passthrough view over { source('staging', 'HCP_HCO_AFFILIATION') }.
-- Synced from Databricks by push_to_snowflake.py — all columns pass through.

select *
from { source('staging', 'HCP_HCO_AFFILIATION') }
