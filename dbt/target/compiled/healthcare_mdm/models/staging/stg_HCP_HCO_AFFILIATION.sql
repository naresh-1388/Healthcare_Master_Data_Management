-- Staging view for HCP_HCO_AFFILIATION.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- Affiliation fields not present in mock API data -- return NULL.

select
    "iqvia_id" as "Source_FK",
    CAST(NULL AS VARCHAR) as "HCO_EID",
    CAST(NULL AS VARCHAR) as "Relationship_Type"
from HMDM_DEV.STAGING.HCP_HCO_AFFILIATION