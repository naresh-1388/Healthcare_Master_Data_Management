%sql
===============================================================================
== 09_DBT_VALIDATIONS
==
== Purpose: Comprehensive validation of the dbt run results.
==   The dbt project (PASS=39, 0 errors) created:
==     - 23 staging views in STAGING_STAGING schema (stg_HCP_*, stg_HCO_*)
==     - 16 marts tables in STAGING_MDM schema (mdm_hcp, mdm_hco, master_*, child models)
==   These are built on top of the Snowflake STAGING source tables
==   (validated in 08_Snow_Validations).
==
== Schemas validated:
==   - HMDM_DEV.STAGING_STAGING = 23 dbt staging views (extract attributes from JSON)
==   - HMDM_DEV.STAGING_MDM    = 16 dbt marts tables (final MDM-ready output)
==
== NOTE: If your dbt profile uses a different database than HMDM_DEV,
==   replace HMDM_DEV with your dbt target database name in all queries below.
==   The dbt run logs show the exact schema names used (e.g., STAGING_staging,
==   STAGING_mdm). Check Section 1 first to confirm the correct schema names.
==
== Instructions: Run section by section in a Snowflake worksheet.
===============================================================================


-------------------------------------------------------------------------------
-- SECTION 1: dbt OBJECT INVENTORY AND SCHEMA DISCOVERY
--
-- Purpose: Confirm dbt created all expected objects (23 views + 16 tables = 39).
--   The dbt run reported PASS=39, so all objects should exist.
--   This section verifies that claim directly in Snowflake.
-------------------------------------------------------------------------------

-- 1A. List ALL schemas in HMDM_DEV (find the dbt-created schemas)
--     dbt creates schemas like STAGING_STAGING and STAGING_MDM
--     (target schema 'STAGING' + custom schema '_staging' or '_mdm')
SHOW SCHEMAS IN DATABASE HMDM_DEV;

-- 1B. List ALL views in the dbt staging schema (expected: 23 views)
--     These are: stg_HCO_ADDRESS, stg_HCO_ALTERNATE_NAME, ..., stg_HCP_TENDENCIES
--     If any view is missing, the dbt run had a partial failure
SHOW VIEWS IN SCHEMA HMDM_DEV.STAGING_STAGING;

-- 1C. List ALL tables in the dbt marts schema (expected: 16 tables)
--     These are: mdm_hcp, mdm_hco, master_hcp, master_hco, + 12 child models
SHOW TABLES IN SCHEMA HMDM_DEV.STAGING_MDM;

-- 1D. Count objects per dbt schema (quick validation against expected counts)
SELECT 
    'STAGING_STAGING (dbt views)' AS schema_name,
    COUNT(*) AS object_count,
    23 AS expected_count,
    CASE WHEN COUNT(*) = 23 THEN 'PASS' ELSE 'MISMATCH' END AS status
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'STAGING_STAGING' AND TABLE_CATALOG = 'HMDM_DEV'
UNION ALL
SELECT 
    'STAGING_MDM (dbt tables)',
    COUNT(*),
    16,
    CASE WHEN COUNT(*) = 16 THEN 'PASS' ELSE 'MISMATCH' END
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'STAGING_MDM' AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY schema_name;

-- 1E. Verify each expected dbt staging view exists (23 checks)
SELECT 'stg_HCO_ADDRESS' AS view_name, CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCO_ADDRESS' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END AS status
UNION ALL SELECT 'stg_HCO_ALTERNATE_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCO_ALTERNATE_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCO_EMAIL', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCO_EMAIL' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCO_HIERARCHY', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCO_HIERARCHY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCO_IDENTIFICATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCO_IDENTIFICATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCO_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCO_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCO_PHONE', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCO_PHONE' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCO_SPECIALTY', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCO_SPECIALTY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCO_TAX', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCO_TAX' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_ADDRESS', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_ADDRESS' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_ALTERNATE_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_ALTERNATE_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_EDUCATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_EDUCATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_EMAIL', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_EMAIL' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_HCO_AFFILIATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_HCO_AFFILIATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_IDENTIFICATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_IDENTIFICATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_LANGUAGE', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_LANGUAGE' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_LICENSE', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_LICENSE' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_ORIGIN_UNIVERSITY', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_ORIGIN_UNIVERSITY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_PHONE', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_PHONE' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_SPECIALTY', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_SPECIALTY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_TAX', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_TAX' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'stg_HCP_TENDENCIES', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='STAGING_STAGING' AND TABLE_NAME='stg_HCP_TENDENCIES' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
ORDER BY view_name;

-- 1F. Verify each expected dbt marts table exists (16 checks)
SELECT 'mdm_hcp' AS table_name, CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='mdm_hcp' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END AS status
UNION ALL SELECT 'master_hcp', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hcp' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'master_hcp_alternate_name', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hcp_alternate_name' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'master_hcp_license', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hcp_license' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'master_hcp_specialty', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hcp_specialty' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'master_hcp_therapeutic_area', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hcp_therapeutic_area' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'mdm_hco', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='mdm_hco' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'mdm_hco_name', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='mdm_hco_name' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'mdm_hco_phone', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='mdm_hco_phone' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'mdm_hco_specialty', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='mdm_hco_specialty' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'mdm_hco_alternate_identifier', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='mdm_hco_alternate_identifier' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'master_hco', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hco' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'master_hco_name', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hco_name' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'master_hco_phone', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hco_phone' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'master_hco_specialty', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hco_specialty' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'master_hco_alternate_identifier', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING_MDM' AND TABLE_NAME='master_hco_alternate_identifier' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
ORDER BY table_name;


-------------------------------------------------------------------------------
-- SECTION 2: dbt STAGING VIEW ROW COUNTS
--
-- Purpose: Check row counts in all 23 dbt staging views. These should match
--   the source STAGING table row counts (views are passthrough with column
--   extraction only — no filtering, no joins).
--
-- Expected pattern:
--   stg_HCP_NAME = 0 rows (source HCP_NAME has 0 rows due to DQ)
--   stg_HCP_ADDRESS, stg_HCP_SPECIALTY etc. = same count as source tables
--   stg_HCO_* = same count as source tables (likely 0 for mock data)
-------------------------------------------------------------------------------

-- 2A. Row counts for ALL 14 dbt HCP staging views
SELECT 'stg_HCP_ADDRESS'            AS view_name, COUNT(*) AS row_count FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS
UNION ALL SELECT 'stg_HCP_ALTERNATE_NAME',   COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ALTERNATE_NAME
UNION ALL SELECT 'stg_HCP_EDUCATION',        COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_EDUCATION
UNION ALL SELECT 'stg_HCP_EMAIL',            COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_EMAIL
UNION ALL SELECT 'stg_HCP_HCO_AFFILIATION',  COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_HCO_AFFILIATION
UNION ALL SELECT 'stg_HCP_IDENTIFICATION',   COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_IDENTIFICATION
UNION ALL SELECT 'stg_HCP_LANGUAGE',         COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_LANGUAGE
UNION ALL SELECT 'stg_HCP_LICENSE',          COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_LICENSE
UNION ALL SELECT 'stg_HCP_NAME',             COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME
UNION ALL SELECT 'stg_HCP_ORIGIN_UNIVERSITY', COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ORIGIN_UNIVERSITY
UNION ALL SELECT 'stg_HCP_PHONE',            COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_PHONE
UNION ALL SELECT 'stg_HCP_SPECIALTY',        COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_SPECIALTY
UNION ALL SELECT 'stg_HCP_TAX',              COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_TAX
UNION ALL SELECT 'stg_HCP_TENDENCIES',       COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_TENDENCIES
ORDER BY view_name;

-- 2B. Row counts for ALL 9 dbt HCO staging views
SELECT 'stg_HCO_ADDRESS'           AS view_name, COUNT(*) AS row_count FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS
UNION ALL SELECT 'stg_HCO_ALTERNATE_NAME',  COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ALTERNATE_NAME
UNION ALL SELECT 'stg_HCO_EMAIL',           COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_EMAIL
UNION ALL SELECT 'stg_HCO_HIERARCHY',       COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_HIERARCHY
UNION ALL SELECT 'stg_HCO_IDENTIFICATION',  COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_IDENTIFICATION
UNION ALL SELECT 'stg_HCO_NAME',            COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_NAME
UNION ALL SELECT 'stg_HCO_PHONE',           COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_PHONE
UNION ALL SELECT 'stg_HCO_SPECIALTY',       COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_SPECIALTY
UNION ALL SELECT 'stg_HCO_TAX',             COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_TAX
ORDER BY view_name;

-- 2C. Combined: ALL 23 views in one result set (HCP + HCO together)
SELECT 'stg_HCP_ADDRESS'            AS view_name, COUNT(*) AS rows, 'HCP' AS entity FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS
UNION ALL SELECT 'stg_HCP_ALTERNATE_NAME',   COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ALTERNATE_NAME
UNION ALL SELECT 'stg_HCP_EDUCATION',        COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_EDUCATION
UNION ALL SELECT 'stg_HCP_EMAIL',            COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_EMAIL
UNION ALL SELECT 'stg_HCP_HCO_AFFILIATION',  COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_HCO_AFFILIATION
UNION ALL SELECT 'stg_HCP_IDENTIFICATION',   COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_IDENTIFICATION
UNION ALL SELECT 'stg_HCP_LANGUAGE',         COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_LANGUAGE
UNION ALL SELECT 'stg_HCP_LICENSE',          COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_LICENSE
UNION ALL SELECT 'stg_HCP_NAME',             COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME
UNION ALL SELECT 'stg_HCP_ORIGIN_UNIVERSITY', COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ORIGIN_UNIVERSITY
UNION ALL SELECT 'stg_HCP_PHONE',            COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_PHONE
UNION ALL SELECT 'stg_HCP_SPECIALTY',        COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_SPECIALTY
UNION ALL SELECT 'stg_HCP_TAX',              COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_TAX
UNION ALL SELECT 'stg_HCP_TENDENCIES',       COUNT(*), 'HCP' FROM HMDM_DEV.STAGING_STAGING.stg_HCP_TENDENCIES
UNION ALL SELECT 'stg_HCO_ADDRESS',          COUNT(*), 'HCO' FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS
UNION ALL SELECT 'stg_HCO_ALTERNATE_NAME',   COUNT(*), 'HCO' FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ALTERNATE_NAME
UNION ALL SELECT 'stg_HCO_EMAIL',           COUNT(*), 'HCO' FROM HMDM_DEV.STAGING_STAGING.stg_HCO_EMAIL
UNION ALL SELECT 'stg_HCO_HIERARCHY',       COUNT(*), 'HCO' FROM HMDM_DEV.STAGING_STAGING.stg_HCO_HIERARCHY
UNION ALL SELECT 'stg_HCO_IDENTIFICATION',  COUNT(*), 'HCO' FROM HMDM_DEV.STAGING_STAGING.stg_HCO_IDENTIFICATION
UNION ALL SELECT 'stg_HCO_NAME',            COUNT(*), 'HCO' FROM HMDM_DEV.STAGING_STAGING.stg_HCO_NAME
UNION ALL SELECT 'stg_HCO_PHONE',           COUNT(*), 'HCO' FROM HMDM_DEV.STAGING_STAGING.stg_HCO_PHONE
UNION ALL SELECT 'stg_HCO_SPECIALTY',       COUNT(*), 'HCO' FROM HMDM_DEV.STAGING_STAGING.stg_HCO_SPECIALTY
UNION ALL SELECT 'stg_HCO_TAX',             COUNT(*), 'HCO' FROM HMDM_DEV.STAGING_STAGING.stg_HCO_TAX
ORDER BY entity, view_name;


-------------------------------------------------------------------------------
-- SECTION 3: dbt STAGING VIEW COLUMN STRUCTURE
--
-- Purpose: Verify each dbt staging view has the correct columns expected
--   by the downstream marts models. The critical columns are:
--     - Source_FK (mapped from iqvia_id, the join key for all marts)
--     - Entity-specific attributes (extracted from response_json via PARSE_JSON)
--
--   If Source_FK is missing, ALL marts will fail to join.
--   If attribute columns are missing, marts will have NULL values.
-------------------------------------------------------------------------------

-- 3A. Check Source_FK column exists in ALL 23 staging views
--     This is THE most critical column — it is the join key for mdm_hcp and mdm_hco
--     If any view is missing Source_FK, the marts LEFT JOIN will produce NULLs
SELECT 
    TABLE_NAME AS view_name,
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING_STAGING'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND COLUMN_NAME = 'Source_FK'
ORDER BY TABLE_NAME;

-- 3B. Describe key HCP staging views (verify extracted attribute columns)
--     stg_HCP_NAME should have: Source_FK, firstName, lastName, fullName, gender, etc.
--     stg_HCP_ADDRESS should have: Source_FK, Address_Line_1, City, Postal_Code, Country, etc.
--     stg_HCP_SPECIALTY should have: Source_FK, Specialty, Specialty_Type, etc.
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCP_NAME;
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS;
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCP_PHONE;
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCP_SPECIALTY;
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCP_LICENSE;
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCP_TAX;

-- 3C. Describe key HCO staging views
--     stg_HCO_ADDRESS should have: Source_FK, Address_Line_1, City, Postal_Code, Country
--     stg_HCO_NAME should have: Source_FK, HCO_Name, HCO_Type, etc.
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS;
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCO_NAME;
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCO_PHONE;
DESCRIBE VIEW HMDM_DEV.STAGING_STAGING.stg_HCO_SPECIALTY;

-- 3D. Count columns per staging view (verify views have more than just Source_FK)
--     Each view should have Source_FK + multiple extracted attribute columns
--     If a view has only 1-2 columns, the PARSE_JSON extraction may have failed
SELECT 
    TABLE_NAME AS view_name,
    COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING_STAGING'
  AND TABLE_CATALOG = 'HMDM_DEV'
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

-- 3E. List ALL columns for a few key staging views (full column inventory)
--     This shows exactly what dbt extracted from response_json
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING_STAGING'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_NAME = 'stg_HCP_NAME'
ORDER BY ORDINAL_POSITION;

SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING_STAGING'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_NAME = 'stg_HCO_ADDRESS'
ORDER BY ORDINAL_POSITION;


-------------------------------------------------------------------------------
-- SECTION 4: Source_FK DATA QUALITY CHECKS
--
-- Purpose: Verify Source_FK values are populated, unique, and consistent
--   across staging views. Source_FK = iqvia_id and is the join key for ALL
--   marts (mdm_hcp joins 14 HCP views on Source_FK, mdm_hco joins 5 HCO views).
--
--   stg_HCP_LICENSE has NULL Source_FK by design (placeholder table with no data).
--   All other views should have non-NULL Source_FK if source data exists.
-------------------------------------------------------------------------------

-- 4A. Source_FK presence check: total rows vs non-NULL Source_FK per view
--     If a view has rows but 0 non-NULL Source_FK values, the iqvia_id mapping failed
SELECT 'stg_HCP_NAME'       AS view_name, COUNT(*) AS total_rows, COUNT(Source_FK) AS non_null_fk FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME
UNION ALL SELECT 'stg_HCP_ADDRESS',   COUNT(*), COUNT(Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS
UNION ALL SELECT 'stg_HCP_PHONE',    COUNT(*), COUNT(Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_PHONE
UNION ALL SELECT 'stg_HCP_SPECIALTY',COUNT(*), COUNT(Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_SPECIALTY
UNION ALL SELECT 'stg_HCP_LICENSE',  COUNT(*), COUNT(Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_LICENSE
UNION ALL SELECT 'stg_HCO_ADDRESS',  COUNT(*), COUNT(Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS
UNION ALL SELECT 'stg_HCO_NAME',    COUNT(*), COUNT(Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_NAME
UNION ALL SELECT 'stg_HCO_PHONE',   COUNT(*), COUNT(Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_PHONE
ORDER BY view_name;

-- 4B. Check for duplicate Source_FK values (should be unique per view)
--     Duplicates would cause row multiplication in marts LEFT JOINs
SELECT 'stg_HCP_ADDRESS' AS view_name, 
       COUNT(*) AS total_rows,
       COUNT(DISTINCT Source_FK) AS distinct_fk,
       COUNT(*) - COUNT(DISTINCT Source_FK) AS duplicate_count
FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS
UNION ALL
SELECT 'stg_HCP_SPECIALTY', COUNT(*), COUNT(DISTINCT Source_FK), COUNT(*) - COUNT(DISTINCT Source_FK)
FROM HMDM_DEV.STAGING_STAGING.stg_HCP_SPECIALTY
UNION ALL
SELECT 'stg_HCP_PHONE', COUNT(*), COUNT(DISTINCT Source_FK), COUNT(*) - COUNT(DISTINCT Source_FK)
FROM HMDM_DEV.STAGING_STAGING.stg_HCP_PHONE
UNION ALL
SELECT 'stg_HCO_ADDRESS', COUNT(*), COUNT(DISTINCT Source_FK), COUNT(*) - COUNT(DISTINCT Source_FK)
FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS
ORDER BY view_name;

-- 4C. Show actual duplicate Source_FK values (if any)
--     This query returns rows only if duplicates exist
SELECT Source_FK, COUNT(*) AS dup_count
FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS
GROUP BY Source_FK
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 10;

-- 4D. Verify Source_FK values are consistent across HCP staging views
--     Same iqvia_id should appear in multiple HCP tables (same HCP has name, address, phone, etc.)
--     This checks how many Source_FK values are shared between key HCP views
SELECT 
    'HCP_NAME vs HCP_ADDRESS' AS comparison,
    (SELECT COUNT(DISTINCT Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME) AS name_fks,
    (SELECT COUNT(DISTINCT Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS) AS address_fks,
    (SELECT COUNT(DISTINCT n.Source_FK) 
     FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME n 
     INNER JOIN HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS a ON n.Source_FK = a.Source_FK) AS shared_fks
UNION ALL
SELECT 
    'HCP_ADDRESS vs HCP_PHONE',
    (SELECT COUNT(DISTINCT Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS),
    (SELECT COUNT(DISTINCT Source_FK) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_PHONE),
    (SELECT COUNT(DISTINCT a.Source_FK) 
     FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS a 
     INNER JOIN HMDM_DEV.STAGING_STAGING.stg_HCP_PHONE p ON a.Source_FK = p.Source_FK);


-------------------------------------------------------------------------------
-- SECTION 5: PARSE_JSON EXTRACTION VERIFICATION
--
-- Purpose: Verify that dbt staging views correctly extracted attributes from
--   response_json using PARSE_JSON. Compare what dbt extracted vs what is
--   actually in the raw response_json in the source table.
--   This confirms the JSON parsing logic is working correctly.
-------------------------------------------------------------------------------

-- 5A. Compare source table response_json with dbt view extracted columns
--     Source table (HCP_ADDRESS) has response_json, dbt view has extracted Address_Line_1
--     This join shows: raw JSON -> extracted column side by side
SELECT 
    s.iqvia_id,
    LEFT(s.response_json, 500) AS raw_json_preview,
    v.Source_FK,
    v."Address_Line_1" AS extracted_address_line_1,
    v."City" AS extracted_city
FROM HMDM_DEV.STAGING.HCP_ADDRESS s
LEFT JOIN HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS v
    ON s.iqvia_id = v.Source_FK
LIMIT 5;

-- 5B. Same comparison for HCP_SPECIALTY
SELECT 
    s.iqvia_id,
    LEFT(s.response_json, 500) AS raw_json_preview,
    v.Source_FK,
    v."X_infa360_Specialty" AS extracted_specialty
FROM HMDM_DEV.STAGING.HCP_SPECIALTY s
LEFT JOIN HMDM_DEV.STAGING_STAGING.stg_HCP_SPECIALTY v
    ON s.iqvia_id = v.Source_FK
LIMIT 5;

-- 5C. Check how many extracted columns are NULL vs non-NULL per view
--     High NULL percentages indicate missing fields in the mock API response_json
SELECT 
    'stg_HCP_ADDRESS' AS view_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN "Address_Line_1" IS NOT NULL THEN 1 ELSE 0 END) AS non_null_address,
    SUM(CASE WHEN "City" IS NOT NULL THEN 1 ELSE 0 END) AS non_null_city,
    SUM(CASE WHEN "Postal_Code" IS NOT NULL THEN 1 ELSE 0 END) AS non_null_postal,
    SUM(CASE WHEN "Country" IS NOT NULL THEN 1 ELSE 0 END) AS non_null_country
FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS
UNION ALL
SELECT 
    'stg_HCP_NAME',
    COUNT(*),
    SUM(CASE WHEN "firstName" IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN "lastName" IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN "fullName" IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN "gender" IS NOT NULL THEN 1 ELSE 0 END)
FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME;

-- 5D. Check the compiled view DDL (shows the PARSE_JSON logic dbt generated)
--     This is the actual SQL Snowflake executes when you query the view
SELECT GET_DDL('VIEW', 'HMDM_DEV.STAGING_STAGING.stg_HCP_NAME');
SELECT GET_DDL('VIEW', 'HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS');
SELECT GET_DDL('VIEW', 'HMDM_DEV.STAGING_STAGING.stg_HCP_SPECIALTY');
SELECT GET_DDL('VIEW', 'HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS');


-------------------------------------------------------------------------------
-- SECTION 6: dbt MARTS TABLE ROW COUNTS
--
-- Purpose: Check row counts in all 16 dbt marts tables. These are the final
--   MDM-ready output tables.
--
-- Expected pattern:
--   mdm_hcp = 0 rows (anchor stg_HCP_NAME has 0 rows due to DQ, so LEFT JOIN returns nothing)
--   mdm_hco = may have rows if HCO_ADDRESS source table has data
--   master_hcp = same as mdm_hcp (column-rename passthrough)
--   master_hco = same as mdm_hco
--   HCP child models = 0 rows (mdm_hub source tables are empty)
--   HCO child models = may have rows if HCO source data exists
-------------------------------------------------------------------------------

-- 6A. Row counts for ALL 16 dbt marts tables
SELECT 'mdm_hcp'                        AS table_name, COUNT(*) AS row_count FROM HMDM_DEV.STAGING_MDM.mdm_hcp
UNION ALL SELECT 'master_hcp',                    COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp
UNION ALL SELECT 'master_hcp_alternate_name',    COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp_alternate_name
UNION ALL SELECT 'master_hcp_license',           COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp_license
UNION ALL SELECT 'master_hcp_specialty',         COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp_specialty
UNION ALL SELECT 'master_hcp_therapeutic_area',  COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp_therapeutic_area
UNION ALL SELECT 'mdm_hco',                      COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco
UNION ALL SELECT 'mdm_hco_name',                 COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco_name
UNION ALL SELECT 'mdm_hco_phone',                 COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco_phone
UNION ALL SELECT 'mdm_hco_specialty',            COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco_specialty
UNION ALL SELECT 'mdm_hco_alternate_identifier', COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco_alternate_identifier
UNION ALL SELECT 'master_hco',                   COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco
UNION ALL SELECT 'master_hco_name',              COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco_name
UNION ALL SELECT 'master_hco_phone',             COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco_phone
UNION ALL SELECT 'master_hco_specialty',          COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco_specialty
UNION ALL SELECT 'master_hco_alternate_identifier', COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco_alternate_identifier
ORDER BY row_count DESC, table_name;

-- 6B. Row count summary by entity type
SELECT 
    'HCP marts (6 tables)' AS entity_group,
    SUM(cnt) AS total_rows
FROM (
    SELECT COUNT(*) AS cnt FROM HMDM_DEV.STAGING_MDM.mdm_hcp
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp_alternate_name
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp_license
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp_specialty
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp_therapeutic_area
) hcp
UNION ALL
SELECT 
    'HCO marts (10 tables)',
    SUM(cnt)
FROM (
    SELECT COUNT(*) AS cnt FROM HMDM_DEV.STAGING_MDM.mdm_hco
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco_name
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco_phone
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco_specialty
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco_alternate_identifier
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco_name
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco_phone
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco_specialty
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco_alternate_identifier
) hco
ORDER BY entity_group;


-------------------------------------------------------------------------------
-- SECTION 7: dbt MARTS TABLE COLUMN STRUCTURE
--
-- Purpose: Verify marts tables have the correct MDM-expected columns.
--   mdm_hcp should have columns from ALL 14 HCP staging views merged.
--   mdm_hco should have columns from ALL 5 HCO staging views merged.
--   master_hcp/master_hco should have renamed columns (sourcePKey, etc.).
-------------------------------------------------------------------------------

-- 7A. Describe main HCP marts table (mdm_hcp)
--     Should have: Source_FK, firstName, lastName, fullName, gender,
--     Phone, Specialty, License, Tax, Education, Email, etc. (all 14 views merged)
DESCRIBE TABLE HMDM_DEV.STAGING_MDM.mdm_hcp;

-- 7B. Describe main HCO marts table (mdm_hco)
--     Should have: Source_FK, X_hco_address, X_hco_city, AlternateName,
--     Email, Phone, Specialty, etc. (all 5 views merged)
DESCRIBE TABLE HMDM_DEV.STAGING_MDM.mdm_hco;

-- 7C. Describe master tables (column-renamed versions)
--     master_hcp should have: sourcePKey, firstName, lastName, etc.
--     master_hco should have: sourcePKey, X_hco_address, etc.
DESCRIBE TABLE HMDM_DEV.STAGING_MDM.master_hcp;
DESCRIBE TABLE HMDM_DEV.STAGING_MDM.master_hco;

-- 7D. Count columns per marts table
--     mdm_hcp should have many columns (14 views merged)
--     mdm_hco should have fewer (5 views merged)
SELECT 
    TABLE_NAME,
    COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING_MDM'
  AND TABLE_CATALOG = 'HMDM_DEV'
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

-- 7E. List ALL columns in mdm_hcp (full inventory)
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING_MDM'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_NAME = 'mdm_hcp'
ORDER BY ORDINAL_POSITION;

-- 7F. List ALL columns in mdm_hco (full inventory)
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING_MDM'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_NAME = 'mdm_hco'
ORDER BY ORDINAL_POSITION;

-- 7G. Verify Source_FK exists in marts tables (the join key)
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING_MDM'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND COLUMN_NAME = 'Source_FK'
ORDER BY TABLE_NAME;


-------------------------------------------------------------------------------
-- SECTION 8: SAMPLE DATA FROM dbt MARTS
--
-- Purpose: Inspect actual data in the marts tables. With mock data:
--   mdm_hcp = 0 rows (HCP_NAME anchor is empty), mdm_hco may have rows.
--   Even with 0 rows, the table structure must be correct for when
--   real data flows through.
-------------------------------------------------------------------------------

-- 8A. Sample data from main marts (may be empty)
SELECT * FROM HMDM_DEV.STAGING_MDM.mdm_hcp LIMIT 10;
SELECT * FROM HMDM_DEV.STAGING_MDM.mdm_hco LIMIT 10;

-- 8B. Sample data from master tables (column-renamed versions)
SELECT * FROM HMDM_DEV.STAGING_MDM.master_hcp LIMIT 10;
SELECT * FROM HMDM_DEV.STAGING_MDM.master_hco LIMIT 10;

-- 8C. Sample data from HCO child models
SELECT * FROM HMDM_DEV.STAGING_MDM.mdm_hco_name LIMIT 5;
SELECT * FROM HMDM_DEV.STAGING_MDM.mdm_hco_phone LIMIT 5;
SELECT * FROM HMDM_DEV.STAGING_MDM.mdm_hco_specialty LIMIT 5;
SELECT * FROM HMDM_DEV.STAGING_MDM.mdm_hco_alternate_identifier LIMIT 5;

-- 8D. Sample data from HCP child models (read from mdm_hub, expected empty)
SELECT * FROM HMDM_DEV.STAGING_MDM.master_hcp_specialty LIMIT 5;
SELECT * FROM HMDM_DEV.STAGING_MDM.master_hcp_alternate_name LIMIT 5;
SELECT * FROM HMDM_DEV.STAGING_MDM.master_hcp_license LIMIT 5;
SELECT * FROM HMDM_DEV.STAGING_MDM.master_hcp_therapeutic_area LIMIT 5;

-- 8E. Check table DDL (compiled SQL dbt generated)
--     This shows the JOIN logic with ref() to staging views
SELECT GET_DDL('TABLE', 'HMDM_DEV.STAGING_MDM.mdm_hcp');
SELECT GET_DDL('TABLE', 'HMDM_DEV.STAGING_MDM.mdm_hco');
SELECT GET_DDL('TABLE', 'HMDM_DEV.STAGING_MDM.master_hcp');
SELECT GET_DDL('TABLE', 'HMDM_DEV.STAGING_MDM.master_hco');


-------------------------------------------------------------------------------
-- SECTION 9: CROSS-LAYER ROW COUNT COMPARISON
--
-- Purpose: Verify data flows correctly from source -> dbt staging views ->
--   dbt marts. Row counts should be consistent across layers.
--   Source tables and dbt staging views should have EQUAL row counts
--   (views are passthrough, no filtering).
--   Marts may have fewer rows (LEFT JOINs on Source_FK).
-------------------------------------------------------------------------------

-- 9A. Compare source table vs dbt staging view row counts (should be EQUAL)
SELECT 
    'HCP_ADDRESS' AS entity,
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_ADDRESS) AS source_rows,
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS) AS dbt_view_rows,
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_ADDRESS) - 
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_ADDRESS) AS difference
UNION ALL
SELECT 'HCP_SPECIALTY',
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_SPECIALTY),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_SPECIALTY),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_SPECIALTY) - 
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_SPECIALTY)
UNION ALL
SELECT 'HCP_PHONE',
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_PHONE),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_PHONE),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_PHONE) - 
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_PHONE)
UNION ALL
SELECT 'HCP_NAME',
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_NAME),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_NAME) - 
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME)
UNION ALL
SELECT 'HCO_ADDRESS',
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_ADDRESS),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_ADDRESS) - 
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS)
UNION ALL
SELECT 'HCO_NAME',
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_NAME),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_NAME),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_NAME) - 
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_NAME)
ORDER BY entity;

-- 9B. Compare dbt staging view vs dbt marts row counts
--     mdm_hcp rows should be <= stg_HCP_NAME rows (LEFT JOIN anchor)
--     mdm_hco rows should be <= stg_HCO_ADDRESS rows (LEFT JOIN anchor)
SELECT 
    'stg_HCP_NAME -> mdm_hcp' AS flow,
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME) AS staging_rows,
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hcp) AS marts_rows,
    CASE 
        WHEN (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hcp) > 
             (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME)
        THEN 'ERROR: marts has MORE rows than staging'
        ELSE 'OK'
    END AS status
UNION ALL
SELECT 'stg_HCO_ADDRESS -> mdm_hco',
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco),
    CASE 
        WHEN (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco) > 
             (SELECT COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS)
        THEN 'ERROR: marts has MORE rows than staging'
        ELSE 'OK'
    END;

-- 9C. Compare mdm_* vs master_* row counts (should be EQUAL — master is passthrough rename)
SELECT 
    'mdm_hcp vs master_hcp' AS comparison,
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hcp) AS mdm_rows,
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp) AS master_rows,
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hcp) - 
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp) AS difference
UNION ALL
SELECT 'mdm_hco vs master_hco',
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco),
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco) - 
    (SELECT COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco)
ORDER BY comparison;


-------------------------------------------------------------------------------
-- SECTION 10: NULL ANALYSIS IN MARTS
--
-- Purpose: Check how many columns in the marts tables are entirely NULL.
--   With mock data, many extracted fields will be NULL (fields not present
--   in the mock API response_json). This is expected, not a bug.
--   When real IQVIA data flows through, these NULLs should decrease.
-------------------------------------------------------------------------------

-- 10A. NULL percentage per column in mdm_hco (if it has rows)
--     This shows which fields are populated vs which are missing from mock data
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    'Check sample data manually' AS null_check_note
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING_MDM'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_NAME = 'mdm_hco'
ORDER BY ORDINAL_POSITION
LIMIT 20;

-- 10B. If mdm_hco has rows, check NULL counts per key column
--     Uncomment and run if mdm_hco has data:
-- SELECT 
--     COUNT(*) AS total_rows,
--     SUM(CASE WHEN "X_hco_address" IS NOT NULL THEN 1 ELSE 0 END) AS non_null_address,
--     SUM(CASE WHEN "AlternateName" IS NOT NULL THEN 1 ELSE 0 END) AS non_null_alt_name,
--     SUM(CASE WHEN "ElectronicAddress" IS NOT NULL THEN 1 ELSE 0 END) AS non_null_email,
--     SUM(CASE WHEN "Phone" IS NOT NULL THEN 1 ELSE 0 END) AS non_null_phone
-- FROM HMDM_DEV.STAGING_MDM.mdm_hco;


-------------------------------------------------------------------------------
-- SECTION 11: dbt TESTS READINESS CHECK
--
-- Purpose: Verify the dbt test schema (schema.yml) is properly configured.
--   This is a metadata check — it validates that tests are defined in the
--   dbt project, not that they pass. Run 'dbt test --target dev' from CLI
--   to actually execute the tests.
--
--   NOTE: These are dbt CLI commands, NOT Snowflake SQL.
--   Run them from your dbt terminal, not from this Snowflake worksheet.
-------------------------------------------------------------------------------

-- 11A. Run ALL dbt tests (run from dbt CLI, NOT Snowflake):
--   dbt test --target dev

-- 11B. Run tests for specific staging model:
--   dbt test --target dev --select stg_HCP_NAME
--   dbt test --target dev --select stg_HCP_ADDRESS

-- 11C. Run tests for specific marts model:
--   dbt test --target dev --select mdm_hcp
--   dbt test --target dev --select mdm_hco

-- 11D. Run dbt with full refresh (rebuild all models from scratch):
--   dbt run --target dev --full-refresh

-- 11E. Run dbt source freshness (check if source data is stale):
--   dbt source freshness --target dev

-- 11F. Run dbt compile only (generate SQL without executing — for debugging):
--   dbt compile --target dev

-- 11G. Run dbt run with specific models only (for debugging):
--   dbt run --target dev --select stg_HCP_NAME
--   dbt run --target dev --select mdm_hcp
--   dbt run --target dev --select mdm_hco


-------------------------------------------------------------------------------
-- SECTION 12: OVERALL dbt PIPELINE HEALTH SUMMARY
--
-- Purpose: Single query that gives a complete overview of the dbt pipeline.
--   Run this to get a one-glance picture of whether dbt run was successful.
-------------------------------------------------------------------------------

-- 12A. Complete pipeline health summary
--     Shows: staging views (23), marts tables (16), with row counts and status
SELECT 
    'dbt STAGING VIEWS (23)' AS layer,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'STAGING_STAGING' AND TABLE_CATALOG = 'HMDM_DEV') AS object_count,
    23 AS expected_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'STAGING_STAGING' AND TABLE_CATALOG = 'HMDM_DEV') = 23 
        THEN 'PASS' ELSE 'MISMATCH' 
    END AS status
UNION ALL
SELECT 
    'dbt MARTS TABLES (16)',
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'STAGING_MDM' AND TABLE_CATALOG = 'HMDM_DEV' AND TABLE_TYPE = 'BASE TABLE'),
    16,
    CASE 
        WHEN (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'STAGING_MDM' AND TABLE_CATALOG = 'HMDM_DEV' AND TABLE_TYPE = 'BASE TABLE') = 16 
        THEN 'PASS' ELSE 'MISMATCH' 
    END
UNION ALL
SELECT 
    'dbt TOTAL OBJECTS (39)',
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'STAGING_STAGING' AND TABLE_CATALOG = 'HMDM_DEV') +
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'STAGING_MDM' AND TABLE_CATALOG = 'HMDM_DEV' AND TABLE_TYPE = 'BASE TABLE'),
    39,
    CASE 
        WHEN (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'STAGING_STAGING' AND TABLE_CATALOG = 'HMDM_DEV') +
             (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'STAGING_MDM' AND TABLE_CATALOG = 'HMDM_DEV' AND TABLE_TYPE = 'BASE TABLE') = 39 
        THEN 'PASS' ELSE 'MISMATCH' 
    END
ORDER BY layer;

-- 12B. Quick row count summary across all dbt layers
--     This shows data volume at each stage of the pipeline
SELECT 
    'stg_HCP_NAME (anchor for mdm_hcp)' AS object_name,
    COUNT(*) AS row_count FROM HMDM_DEV.STAGING_STAGING.stg_HCP_NAME
UNION ALL
SELECT 'mdm_hcp (14 HCP views joined)',
    COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hcp
UNION ALL
SELECT 'master_hcp (renamed mdm_hcp)',
    COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hcp
UNION ALL
SELECT 'stg_HCO_ADDRESS (anchor for mdm_hco)',
    COUNT(*) FROM HMDM_DEV.STAGING_STAGING.stg_HCO_ADDRESS
UNION ALL
SELECT 'mdm_hco (5 HCO views joined)',
    COUNT(*) FROM HMDM_DEV.STAGING_MDM.mdm_hco
UNION ALL
SELECT 'master_hco (renamed mdm_hco)',
    COUNT(*) FROM HMDM_DEV.STAGING_MDM.master_hco
ORDER BY row_count DESC;


-------------------------------------------------------------------------------
-- END OF 09_DBT_VALIDATIONS
-- 
-- Summary of what to look for:
--   Section 1:  23 views + 16 tables = 39 objects exist (PASS=39 confirmed)
--   Section 2:  Staging view row counts match source table row counts
--   Section 3:  Source_FK column exists in all views + attribute columns present
--   Section 4:  Source_FK is non-NULL, unique, and consistent across views
--   Section 5:  PARSE_JSON extraction matches raw response_json content
--   Section 6:  Marts row counts (mdm_hcp=0 expected, mdm_hco may have rows)
--   Section 7:  Marts have correct MDM-expected columns (14 views merged for HCP, 5 for HCO)
--   Section 8:  Sample data inspection (may be empty with mock data)
--   Section 9:  Cross-layer row count consistency (source = staging view, staging >= marts)
--   Section 10: NULL analysis (high NULLs expected with mock data)
--   Section 11: dbt test commands (run from CLI, not Snowflake)
--   Section 12: Overall pipeline health summary
--   
-- If any schema name does not match (e.g., STAGING_STAGING vs staging_staging),
--   check Section 1A output and adjust the schema names accordingly.
--   Snowflake schema names are case-sensitive — use exact names from SHOW SCHEMAS.
-------------------------------------------------------------------------------