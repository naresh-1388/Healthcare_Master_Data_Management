===============================================================================
== 00_CREATE_ROLE_AND_GRANTS
==
== Purpose: Create the HMDM_DEV_ROLE application role in Snowflake and grant
==   it all the permissions needed by the HMDM pipeline:
==
==     - Databricks push_to_snowflake.py (TRUNCATE-AND-LOAD sync)
==     - dbt run (creates staging views + marts tables)
==     - Snowpark publish_master_snapshot.py (creates date-stamped snapshot tables)
==
== Run this script ONCE in a Snowflake worksheet using ACCOUNTADMIN or
== SECURITYADMIN. After running, update the AWS Secrets Manager secret
== 'healthcare-mdm/dev/api-snowflake' to change snowflake_role from
== ACCOUNTADMIN to HMDM_DEV_ROLE.
==
== SECURITY NOTE: Do NOT use ACCOUNTADMIN for application access.
==   ACCOUNTADMIN can drop databases, change account settings, and see all
==   data in all databases. HMDM_DEV_ROLE is scoped to only what the pipeline
==   needs. This is the principle of least privilege.
==
== Instructions: Run in a Snowflake worksheet (Snowsight) as ACCOUNTADMIN.
==   Replace <YOUR_SNOWFLAKE_USER> with your actual Snowflake login username.
===============================================================================


-------------------------------------------------------------------------------
-- SECTION 1: CREATE THE APPLICATION ROLE
-------------------------------------------------------------------------------

CREATE ROLE IF NOT EXISTS HMDM_DEV_ROLE
    COMMENT = 'Application role for Healthcare MDM pipeline (dbt, sync, Snowpark)';


-------------------------------------------------------------------------------
-- SECTION 2: GRANT ROLE TO YOUR USER
-------------------------------------------------------------------------------
-- Replace <YOUR_SNOWFLAKE_USER> with your actual Snowflake login username.

GRANT ROLE HMDM_DEV_ROLE TO USER <YOUR_SNOWFLAKE_USER>;


-------------------------------------------------------------------------------
-- SECTION 3: GRANT WAREHOUSE ACCESS
-------------------------------------------------------------------------------
-- Replace HMDM_WH with your actual warehouse name if different.

GRANT USAGE ON WAREHOUSE HMDM_WH TO ROLE HMDM_DEV_ROLE;
GRANT OPERATE ON WAREHOUSE HMDM_WH TO ROLE HMDM_DEV_ROLE;


-------------------------------------------------------------------------------
-- SECTION 4: GRANT DATABASE ACCESS
-------------------------------------------------------------------------------

GRANT USAGE ON DATABASE HMDM_DEV TO ROLE HMDM_DEV_ROLE;
GRANT MONITOR ON DATABASE HMDM_DEV TO ROLE HMDM_DEV_ROLE;


-------------------------------------------------------------------------------
-- SECTION 5: GRANT SCHEMA ACCESS
-------------------------------------------------------------------------------

GRANT USAGE ON SCHEMA HMDM_DEV.RAW TO ROLE HMDM_DEV_ROLE;
GRANT USAGE ON SCHEMA HMDM_DEV.LANDING TO ROLE HMDM_DEV_ROLE;
GRANT USAGE ON SCHEMA HMDM_DEV.CANONICAL TO ROLE HMDM_DEV_ROLE;
GRANT USAGE ON SCHEMA HMDM_DEV.STAGING TO ROLE HMDM_DEV_ROLE;
GRANT USAGE ON SCHEMA HMDM_DEV.MDM TO ROLE HMDM_DEV_ROLE;
GRANT USAGE ON SCHEMA HMDM_DEV.MASTER TO ROLE HMDM_DEV_ROLE;
GRANT USAGE ON SCHEMA HMDM_DEV.UTIL TO ROLE HMDM_DEV_ROLE;


-------------------------------------------------------------------------------
-- SECTION 6: GRANT TABLE DML PERMISSIONS
-------------------------------------------------------------------------------
-- push_to_snowflake.py: SELECT, INSERT, UPDATE, DELETE on STAGING + MASTER
-- dbt: SELECT on STAGING, SELECT/INSERT/UPDATE/DELETE on MDM + MASTER
-- Snowpark: SELECT on MDM, INSERT/CREATE on MASTER

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA HMDM_DEV.STAGING TO ROLE HMDM_DEV_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA HMDM_DEV.MDM TO ROLE HMDM_DEV_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA HMDM_DEV.MASTER TO ROLE HMDM_DEV_ROLE;

-- RAW, LANDING, CANONICAL, UTIL: read-only for validation queries
GRANT SELECT ON ALL TABLES IN SCHEMA HMDM_DEV.RAW TO ROLE HMDM_DEV_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA HMDM_DEV.LANDING TO ROLE HMDM_DEV_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA HMDM_DEV.CANONICAL TO ROLE HMDM_DEV_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA HMDM_DEV.UTIL TO ROLE HMDM_DEV_ROLE;


-------------------------------------------------------------------------------
-- SECTION 7: GRANT DDL PERMISSIONS (CREATE TABLE / VIEW)
-------------------------------------------------------------------------------
-- dbt creates views in STAGING and tables in MDM.
-- Snowpark creates tables in MASTER.
-- push_to_snowflake.py may auto-create tables if they don't exist yet.

GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HMDM_DEV.STAGING TO ROLE HMDM_DEV_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HMDM_DEV.MDM TO ROLE HMDM_DEV_ROLE;
GRANT CREATE TABLE ON SCHEMA HMDM_DEV.MASTER TO ROLE HMDM_DEV_ROLE;


-------------------------------------------------------------------------------
-- SECTION 8: FUTURE GRANTS (auto-grant on new tables/views)
-------------------------------------------------------------------------------
-- Without future grants, every new table or view created by dbt or Snowpark
-- would need a manual GRANT statement. Future grants handle this automatically.

GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA HMDM_DEV.STAGING TO ROLE HMDM_DEV_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA HMDM_DEV.STAGING TO ROLE HMDM_DEV_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA HMDM_DEV.MDM TO ROLE HMDM_DEV_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA HMDM_DEV.MDM TO ROLE HMDM_DEV_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA HMDM_DEV.MASTER TO ROLE HMDM_DEV_ROLE;


-------------------------------------------------------------------------------
-- SECTION 9: VERIFY THE ROLE AND GRANTS
-------------------------------------------------------------------------------

-- 9A. Verify the role exists
SHOW ROLES LIKE 'HMDM_DEV_ROLE';

-- 9B. Verify the role was granted to your user
SHOW GRANTS OF ROLE HMDM_DEV_ROLE;

-- 9C. Verify grants TO the role
SHOW GRANTS TO ROLE HMDM_DEV_ROLE;

-- 9D. Switch to the new role and test access
USE ROLE HMDM_DEV_ROLE;
USE WAREHOUSE HMDM_WH;
USE DATABASE HMDM_DEV;
USE SCHEMA STAGING;

-- 9E. Test: can we SELECT from a staging table?
SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_NAME;

-- 9F. Test: can we CREATE a view (dbt needs this)?
CREATE OR REPLACE VIEW HMDM_DEV.STAGING._test_dbt_view AS
    SELECT 1 AS test_column;
DROP VIEW HMDM_DEV.STAGING._test_dbt_view;

-- 9G. Test: can we CREATE a table (Snowpark needs this)?
CREATE TABLE HMDM_DEV.MASTER._test_snowpark_table (id INT);
DROP TABLE HMDM_DEV.MASTER._test_snowpark_table;

-- 9H. Switch back to your original role
USE ROLE ACCOUNTADMIN;


-------------------------------------------------------------------------------
-- SECTION 10: UPDATE THE AWS SECRET (MANUAL STEP -- DO THIS AFTER RUNNING SQL)
-------------------------------------------------------------------------------
-- After this SQL runs successfully, update the AWS Secrets Manager secret
-- 'healthcare-mdm/dev/api-snowflake' to change the snowflake_role value.
--
-- FROM: "snowflake_role": "ACCOUNTADMIN"
-- TO:   "snowflake_role": "HMDM_DEV_ROLE"
--
-- Via AWS CLI:
--   aws secretsmanager get-secret-value --secret-id healthcare-mdm/dev/api-snowflake --query SecretString --output text > /tmp/secret.json
--   # Edit /tmp/secret.json: change snowflake_role from ACCOUNTADMIN to HMDM_DEV_ROLE
--   aws secretsmanager put-secret-value --secret-id healthcare-mdm/dev/api-snowflake --secret-string file:///tmp/secret.json
--
-- Or via AWS Secrets Manager console: edit the secret JSON and change
-- the snowflake_role value from ACCOUNTADMIN to HMDM_DEV_ROLE.
--
-- IMPORTANT: Read the existing secret first to preserve all other keys.
-- Do NOT overwrite the entire secret with just the role field.
-------------------------------------------------------------------------------