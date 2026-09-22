-- Staging view for HCO_ADDRESS.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hco.sql).
-- Address fields extracted from response_json.

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from HMDM_DEV.STAGING.HCO_ADDRESS
)
select
    "Source_FK",
    j['Addresses'][0]['addressLine1']::VARCHAR as "Address_Line_1",
    j['Addresses'][0]['city']::VARCHAR as "City",
    j['Addresses'][0]['postalCode']::VARCHAR as "Postal_Code",
    j['Addresses'][0]['country']::VARCHAR as "Country"
from parsed