-- Staging view for HCP_NAME.
-- Maps Databricks staging columns to dbt-expected names.
-- Source_FK = iqvia_id (join key for mdm_hcp.sql).
-- Name and identity fields extracted from response_json.

with parsed as (
    select
        "iqvia_id" as "Source_FK",
        PARSE_JSON("response_json") as j
    from {{ source('staging', 'HCP_NAME') }}
)
select
    "Source_FK",
    j['Transparency Reporting Name']::VARCHAR as "X_transparency_reporting_name",
    j['First Name']::VARCHAR as "firstName",
    j['Middle Name']::VARCHAR as "middleName",
    j['Last Name']::VARCHAR as "lastName",
    j['Full Name']::VARCHAR as "fullName",
    j['Gender']['Name']::VARCHAR as "gender",
    j['Type']['Name']::VARCHAR as "X_informatica_type",
    j['HCP Status']['Name']::VARCHAR as "X_hcp_status",
    j['Title']::VARCHAR as "X_iqvia_title",
    j['Prefix Name']['Name']::VARCHAR as "prefixName"
from parsed
