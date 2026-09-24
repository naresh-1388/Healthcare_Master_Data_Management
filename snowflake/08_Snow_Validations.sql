===============================================================================
== 08_SNOWFLAKE_VALIDATIONS
== 
== Purpose: Comprehensive validation of the Databricks-to-Snowflake sync layer.
==   The push_to_snowflake.py script syncs 22 staging tables (13 HCP + 9 HCO)
==   and 9 master tables (4 HCP + 5 HCO) from Databricks Delta tables to
==   Snowflake using TRUNCATE-AND-LOAD (DELETE + INSERT) with transaction safety.
==
== Schemas validated:
==   - HMDM_DEV.STAGING  = 22 source staging tables synced from Databricks
==   - HMDM_DEV.MASTER   = 9 master tables synced from Databricks
==   - HMDM_DEV.MDM      = mdm_hub placeholder tables (DDL only, no data)
==
== Expected results:
==   - STAGING tables: HCP tables have mock data (HCP_NAME = 0 rows due to DQ),
==     HCO tables may be empty (mock Lambda does not return HCO data)
==   - MASTER tables: 0 rows (DDL placeholders, populated by Informatica MDM or
==     Snowpark snapshot script, NOT by Databricks sync)
==   - MDM hub tables: 0 rows (DDL placeholders for Informatica MDM engine)
==
== Instructions: Run section by section in a Snowflake worksheet.
===============================================================================


-------------------------------------------------------------------------------
-- SECTION 1: SCHEMA EXISTENCE AND OBJECT INVENTORY
--
-- Purpose: Confirm all expected schemas exist and contain the right number
--   of tables. This is the first check — if schemas or tables are missing,
--   the sync (push_to_snowflake.py) may not have run or may have failed.
-------------------------------------------------------------------------------

-- 1A. List all schemas in HMDM_DEV database
--     Expected: RAW, LANDING, CANONICAL, STAGING, MDM, MASTER, UTIL
--     If STAGING or MASTER is missing, the Snowflake DDL scripts did not run
SHOW SCHEMAS IN DATABASE HMDM_DEV;

-- 1B. Count objects in each schema — quick health check
--     This gives a single-row overview of how many tables exist per schema
SELECT 
    SCHEMA_NAME,
    COUNT(*) AS object_count
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'HMDM_DEV'
GROUP BY SCHEMA_NAME
ORDER BY SCHEMA_NAME;

-- 1C. List ALL tables in STAGING schema (should be 22: 13 HCP + 9 HCO)
--     These are the source tables synced from Databricks by push_to_snowflake.py
--     Each table has generic columns: batch_id, first_name, iqvia_id, response_json, etc.
SHOW TABLES IN SCHEMA HMDM_DEV.STAGING;

-- 1D. List ALL tables in MASTER schema (should be 9: 4 HCP + 5 HCO)
--     These are DDL placeholder tables for the Informatica MDM hub
--     They should be empty (0 rows) — populated by MDM, not by sync
SHOW TABLES IN SCHEMA HMDM_DEV.MASTER;

-- 1E. List ALL tables in MDM schema (mdm_hub tables for Informatica)
--     These include: hcp_specialty, hcp_alternate_name, hcp_license, 
--     hcp_therapeutic_area, and any master_* placeholder tables
SHOW TABLES IN SCHEMA HMDM_DEV.MDM;

-- 1F. Cross-check: verify expected table count per schema
--     STAGING should have 22, MASTER should have 9, MDM should have mdm_hub tables
SELECT 
    'STAGING' AS schema_name,
    COUNT(*) AS table_count,
    22 AS expected_count,
    CASE WHEN COUNT(*) = 22 THEN 'PASS' ELSE 'MISMATCH' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'STAGING' AND TABLE_CATALOG = 'HMDM_DEV'
UNION ALL
SELECT 
    'MASTER',
    COUNT(*),
    9,
    CASE WHEN COUNT(*) = 9 THEN 'PASS' ELSE 'MISMATCH' END
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'MASTER' AND TABLE_CATALOG = 'HMDM_DEV'
ORDER BY schema_name;


-------------------------------------------------------------------------------
-- SECTION 2: STAGING TABLE ROW COUNTS (DATABRICKS SYNC VERIFICATION)
--
-- Purpose: Check row counts in all 22 STAGING tables. These should match
--   the row counts in the corresponding Databricks Delta tables.
--   If a table has 0 rows in Snowflake but has data in Databricks, the sync
--   failed for that table.
--
-- Expected pattern:
--   HCP tables: Most have 3-10 mock rows, HCP_NAME = 0 rows (DQ rejects)
--   HCO tables: Likely 0 rows (mock Lambda does not return HCO data)
-------------------------------------------------------------------------------

-- 2A. Row counts for ALL 13 HCP STAGING tables (one query, alphabetical)
SELECT 'HCP_ADDRESS'             AS table_name, COUNT(*) AS row_count FROM HMDM_DEV.STAGING.HCP_ADDRESS
UNION ALL SELECT 'HCP_ALTERNATE_NAME',   COUNT(*) FROM HMDM_DEV.STAGING.HCP_ALTERNATE_NAME
UNION ALL SELECT 'HCP_EDUCATION',        COUNT(*) FROM HMDM_DEV.STAGING.HCP_EDUCATION
UNION ALL SELECT 'HCP_EMAIL',            COUNT(*) FROM HMDM_DEV.STAGING.HCP_EMAIL
UNION ALL SELECT 'HCP_HCO_AFFILIATION',  COUNT(*) FROM HMDM_DEV.STAGING.HCP_HCO_AFFILIATION
UNION ALL SELECT 'HCP_IDENTIFICATION',   COUNT(*) FROM HMDM_DEV.STAGING.HCP_IDENTIFICATION
UNION ALL SELECT 'HCP_LANGUAGE',         COUNT(*) FROM HMDM_DEV.STAGING.HCP_LANGUAGE
UNION ALL SELECT 'HCP_LICENSE',          COUNT(*) FROM HMDM_DEV.STAGING.HCP_LICENSE
UNION ALL SELECT 'HCP_NAME',             COUNT(*) FROM HMDM_DEV.STAGING.HCP_NAME
UNION ALL SELECT 'HCP_ORIGIN_UNIVERSITY', COUNT(*) FROM HMDM_DEV.STAGING.HCP_ORIGIN_UNIVERSITY
UNION ALL SELECT 'HCP_PHONE',            COUNT(*) FROM HMDM_DEV.STAGING.HCP_PHONE
UNION ALL SELECT 'HCP_SPECIALTY',        COUNT(*) FROM HMDM_DEV.STAGING.HCP_SPECIALTY
UNION ALL SELECT 'HCP_TAX',              COUNT(*) FROM HMDM_DEV.STAGING.HCP_TAX
UNION ALL SELECT 'HCP_TENDENCIES',       COUNT(*) FROM HMDM_DEV.STAGING.HCP_TENDENCIES
ORDER BY table_name;

-- 2B. Row counts for ALL 9 HCO STAGING tables
SELECT 'HCO_ADDRESS'          AS table_name, COUNT(*) AS row_count FROM HMDM_DEV.STAGING.HCO_ADDRESS
UNION ALL SELECT 'HCO_ALTERNATE_NAME',  COUNT(*) FROM HMDM_DEV.STAGING.HCO_ALTERNATE_NAME
UNION ALL SELECT 'HCO_EMAIL',            COUNT(*) FROM HMDM_DEV.STAGING.HCO_EMAIL
UNION ALL SELECT 'HCO_HIERARCHY',       COUNT(*) FROM HMDM_DEV.STAGING.HCO_HIERARCHY
UNION ALL SELECT 'HCO_IDENTIFICATION',  COUNT(*) FROM HMDM_DEV.STAGING.HCO_IDENTIFICATION
UNION ALL SELECT 'HCO_NAME',            COUNT(*) FROM HMDM_DEV.STAGING.HCO_NAME
UNION ALL SELECT 'HCO_PHONE',           COUNT(*) FROM HMDM_DEV.STAGING.HCO_PHONE
UNION ALL SELECT 'HCO_SPECIALTY',       COUNT(*) FROM HMDM_DEV.STAGING.HCO_SPECIALTY
UNION ALL SELECT 'HCO_TAX',             COUNT(*) FROM HMDM_DEV.STAGING.HCO_TAX
ORDER BY table_name;

-- 2C. Combined summary: ALL 22 STAGING tables in one result set
--     Useful for a quick single-glance overview of the entire sync state
SELECT 'HCP_ADDRESS' AS table_name, COUNT(*) AS Total_Rows, 'HCP' AS entity FROM HMDM_DEV.STAGING.HCP_ADDRESS
UNION ALL SELECT 'HCP_ALTERNATE_NAME',   COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_ALTERNATE_NAME
UNION ALL SELECT 'HCP_EDUCATION',        COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_EDUCATION
UNION ALL SELECT 'HCP_EMAIL',            COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_EMAIL
UNION ALL SELECT 'HCP_HCO_AFFILIATION',  COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_HCO_AFFILIATION
UNION ALL SELECT 'HCP_IDENTIFICATION',   COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_IDENTIFICATION
UNION ALL SELECT 'HCP_LANGUAGE',         COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_LANGUAGE
UNION ALL SELECT 'HCP_LICENSE',          COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_LICENSE
UNION ALL SELECT 'HCP_NAME',             COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_NAME
UNION ALL SELECT 'HCP_ORIGIN_UNIVERSITY', COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_ORIGIN_UNIVERSITY
UNION ALL SELECT 'HCP_PHONE',            COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_PHONE
UNION ALL SELECT 'HCP_SPECIALTY',        COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_SPECIALTY
UNION ALL SELECT 'HCP_TAX',              COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_TAX
UNION ALL SELECT 'HCP_TENDENCIES',       COUNT(*), 'HCP' FROM HMDM_DEV.STAGING.HCP_TENDENCIES
UNION ALL SELECT 'HCO_ADDRESS',          COUNT(*), 'HCO' FROM HMDM_DEV.STAGING.HCO_ADDRESS
UNION ALL SELECT 'HCO_ALTERNATE_NAME',   COUNT(*), 'HCO' FROM HMDM_DEV.STAGING.HCO_ALTERNATE_NAME
UNION ALL SELECT 'HCO_EMAIL',           COUNT(*), 'HCO' FROM HMDM_DEV.STAGING.HCO_EMAIL
UNION ALL SELECT 'HCO_HIERARCHY',        COUNT(*), 'HCO' FROM HMDM_DEV.STAGING.HCO_HIERARCHY
UNION ALL SELECT 'HCO_IDENTIFICATION',  COUNT(*), 'HCO' FROM HMDM_DEV.STAGING.HCO_IDENTIFICATION
UNION ALL SELECT 'HCO_NAME',            COUNT(*), 'HCO' FROM HMDM_DEV.STAGING.HCO_NAME
UNION ALL SELECT 'HCO_PHONE',           COUNT(*), 'HCO' FROM HMDM_DEV.STAGING.HCO_PHONE
UNION ALL SELECT 'HCO_SPECIALTY',        COUNT(*), 'HCO' FROM HMDM_DEV.STAGING.HCO_SPECIALTY
UNION ALL SELECT 'HCO_TAX',             COUNT(*), 'HCO' FROM HMDM_DEV.STAGING.HCO_TAX
ORDER BY entity, table_name;

-- 2D. Aggregate summary by entity type (HCP vs HCO)
--     Shows total rows across all HCP tables and all HCO tables
SELECT 
    'HCP (13 tables)' AS entity_group,
    SUM(row_count) AS total_rows,
    SUM(IFF(row_count > 0, 1, 0)) AS tables_with_data,
    13 AS total_tables
FROM (
    SELECT COUNT(*) AS row_count FROM HMDM_DEV.STAGING.HCP_ADDRESS
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_ALTERNATE_NAME
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_EDUCATION
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_EMAIL
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_HCO_AFFILIATION
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_IDENTIFICATION
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_LANGUAGE
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_LICENSE
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_NAME
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_ORIGIN_UNIVERSITY
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_PHONE
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_SPECIALTY
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_TAX
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCP_TENDENCIES
) hcp
UNION ALL
SELECT 
    'HCO (9 tables)',
    SUM(row_count),
    SUM(IFF(row_count > 0, 1, 0)),
    9
FROM (
    SELECT COUNT(*) AS row_count FROM HMDM_DEV.STAGING.HCO_ADDRESS
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_ALTERNATE_NAME
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_EMAIL
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_HIERARCHY
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_IDENTIFICATION
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_NAME
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_PHONE
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_SPECIALTY
    UNION ALL SELECT COUNT(*) FROM HMDM_DEV.STAGING.HCO_TAX
) hco
ORDER BY entity_group;


-------------------------------------------------------------------------------
-- SECTION 3: COLUMN STRUCTURE VALIDATION
--
-- Purpose: Verify that all STAGING tables have the expected generic columns
--   synced from Databricks. All tables should have the same generic schema
--   with the critical column being response_json (stringified JSON containing
--   all the actual API attribute data).
--
-- Expected generic columns (varies slightly by table but core set is):
--   batch_id, first_name, iqvia_id, source_system_name, response_json,
--   mdm_batch_id, Source_Name, LOAD_DATE, etc.
-------------------------------------------------------------------------------

-- 3A. Describe a few key STAGING tables to verify column structure
--     All tables should have generic columns + response_json
--     response_json is the key column — it contains the stringified JSON
--     that dbt staging views will PARSE_JSON to extract attributes
DESCRIBE TABLE HMDM_DEV.STAGING.HCP_ADDRESS;
DESCRIBE TABLE HMDM_DEV.STAGING.HCP_NAME;
DESCRIBE TABLE HMDM_DEV.STAGING.HCP_SPECIALTY;
DESCRIBE TABLE HMDM_DEV.STAGING.HCO_ADDRESS;
DESCRIBE TABLE HMDM_DEV.STAGING.HCO_NAME;

-- 3B. Check if response_json column exists in all STAGING tables
--     This is the most critical column — without it, dbt staging views
--     cannot extract any attributes from the API data
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND COLUMN_NAME ILIKE '%response_json%'
ORDER BY TABLE_NAME;

-- 3C. Check if iqvia_id column exists in all STAGING tables
--     iqvia_id is the source column that dbt maps to Source_FK
--     Without it, the join key for marts will be NULL
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND COLUMN_NAME ILIKE '%iqvia_id%'
ORDER BY TABLE_NAME;

-- 3D. Check if batch_id column exists in all STAGING tables
--     batch_id links records to the ctl_batch_log_tbl for lineage tracking
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND COLUMN_NAME ILIKE '%batch_id%'
ORDER BY TABLE_NAME;

-- 3E. Compare column counts across all STAGING tables
--     All tables should have similar column counts (generic schema)
--     If one table has a very different count, the sync may have failed
--     or the Databricks table schema may have diverged
SELECT 
    TABLE_NAME,
    COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STAGING'
  AND TABLE_CATALOG = 'HMDM_DEV'
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

-- 3F. Describe MASTER tables (should have MDM-specific columns)
--     These are NOT generic — they have columns like sourcePKey, firstName,
--     X_informatica_Specialty, etc. matching the Informatica MDM schema
DESCRIBE TABLE HMDM_DEV.MASTER.HCO;
DESCRIBE TABLE HMDM_DEV.MASTER.HCP_SPECIALTY;

-- 3G. Describe MDM hub tables (mdm_hub placeholders)
DESCRIBE TABLE HMDM_DEV.MDM.hcp_specialty;
DESCRIBE TABLE HMDM_DEV.MDM.hcp_license;


-------------------------------------------------------------------------------
-- SECTION 4: DATA QUALITY AND CONTENT CHECKS
--
-- Purpose: Inspect actual data content in the synced STAGING tables.
--   Verify response_json contains valid JSON, check for NULLs in key columns,
--   and look for any data integrity issues.
-------------------------------------------------------------------------------

-- 4A. Sample data from key HCP STAGING tables (verify sync worked)
--     Check that response_json column has actual JSON content
--     The JSON should contain API attributes like firstName, lastName, etc.
SELECT * FROM HMDM_DEV.STAGING.HCP_ADDRESS LIMIT 3;
SELECT * FROM HMDM_DEV.STAGING.HCP_SPECIALTY LIMIT 3;
SELECT * FROM HMDM_DEV.STAGING.HCP_PHONE LIMIT 3;
SELECT * FROM HMDM_DEV.STAGING.HCP_NAME LIMIT 3;

-- 4B. Sample data from key HCO STAGING tables
SELECT * FROM HMDM_DEV.STAGING.HCO_ADDRESS LIMIT 3;
SELECT * FROM HMDM_DEV.STAGING.HCO_NAME LIMIT 3;
SELECT * FROM HMDM_DEV.STAGING.HCO_PHONE LIMIT 3;

-- 4C. Inspect response_json content (first 1000 chars for readability)
--     This shows the actual JSON payload that dbt staging views parse
--     Verify it starts with '{' and contains expected API fields
SELECT 
    iqvia_id,
    LEFT(response_json, 1000) AS response_json_preview
FROM HMDM_DEV.STAGING.HCP_ADDRESS
LIMIT 5;

SELECT 
    iqvia_id,
    LEFT(response_json, 1000) AS response_json_preview
FROM HMDM_DEV.STAGING.HCP_SPECIALTY
LIMIT 5;

-- 4D. Check for NULL response_json (should never be NULL in synced data)
--     If response_json is NULL, the sync lost the API payload
SELECT 
    'HCP_ADDRESS' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(response_json) AS non_null_json,
    COUNT(*) - COUNT(response_json) AS null_json_count
FROM HMDM_DEV.STAGING.HCP_ADDRESS
UNION ALL
SELECT 'HCP_SPECIALTY', COUNT(*), COUNT(response_json), COUNT(*) - COUNT(response_json)
FROM HMDM_DEV.STAGING.HCP_SPECIALTY
UNION ALL
SELECT 'HCP_PHONE', COUNT(*), COUNT(response_json), COUNT(*) - COUNT(response_json)
FROM HMDM_DEV.STAGING.HCP_PHONE
UNION ALL
SELECT 'HCP_NAME', COUNT(*), COUNT(response_json), COUNT(*) - COUNT(response_json)
FROM HMDM_DEV.STAGING.HCP_NAME
UNION ALL
SELECT 'HCO_ADDRESS', COUNT(*), COUNT(response_json), COUNT(*) - COUNT(response_json)
FROM HMDM_DEV.STAGING.HCO_ADDRESS
UNION ALL
SELECT 'HCO_NAME', COUNT(*), COUNT(response_json), COUNT(*) - COUNT(response_json)
FROM HMDM_DEV.STAGING.HCO_NAME
ORDER BY table_name;

-- 4E. Check for NULL iqvia_id (the Source_FK source column)
--     If iqvia_id is NULL, the join key for dbt marts will be NULL
--     This would cause LEFT JOINs in mdm_hcp/mdm_hco to fail matching
SELECT 
    'HCP_ADDRESS' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(iqvia_id) AS non_null_iqvia,
    COUNT(*) - COUNT(iqvia_id) AS null_iqvia_count
FROM HMDM_DEV.STAGING.HCP_ADDRESS
UNION ALL
SELECT 'HCP_SPECIALTY', COUNT(*), COUNT(iqvia_id), COUNT(*) - COUNT(iqvia_id)
FROM HMDM_DEV.STAGING.HCP_SPECIALTY
UNION ALL
SELECT 'HCP_PHONE', COUNT(*), COUNT(iqvia_id), COUNT(*) - COUNT(iqvia_id)
FROM HMDM_DEV.STAGING.HCP_PHONE
UNION ALL
SELECT 'HCO_ADDRESS', COUNT(*), COUNT(iqvia_id), COUNT(*) - COUNT(iqvia_id)
FROM HMDM_DEV.STAGING.HCO_ADDRESS
ORDER BY table_name;

-- 4F. Validate response_json is valid JSON using TRY_PARSE_JSON
--     If PARSE_JSON fails, dbt staging views will return NULL for all extracted fields
SELECT 
    'HCP_ADDRESS' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN TRY_PARSE_JSON(response_json) IS NOT NULL THEN 1 ELSE 0 END) AS valid_json_count,
    SUM(CASE WHEN TRY_PARSE_JSON(response_json) IS NULL THEN 1 ELSE 0 END) AS invalid_json_count
FROM HMDM_DEV.STAGING.HCP_ADDRESS
UNION ALL
SELECT 'HCP_SPECIALTY', COUNT(*),
    SUM(CASE WHEN TRY_PARSE_JSON(response_json) IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN TRY_PARSE_JSON(response_json) IS NULL THEN 1 ELSE 0 END)
FROM HMDM_DEV.STAGING.HCP_SPECIALTY
UNION ALL
SELECT 'HCP_PHONE', COUNT(*),
    SUM(CASE WHEN TRY_PARSE_JSON(response_json) IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN TRY_PARSE_JSON(response_json) IS NULL THEN 1 ELSE 0 END)
FROM HMDM_DEV.STAGING.HCP_PHONE
ORDER BY table_name;

-- 4G. Extract specific fields from response_json to verify content
--     This mimics what dbt staging views do — extract attributes from JSON
--     If these return values, the JSON structure is correct for dbt to parse
SELECT 
    iqvia_id,
    PARSE_JSON(response_json):firstName::VARCHAR AS first_name,
    PARSE_JSON(response_json):lastName::VARCHAR AS last_name,
    PARSE_JSON(response_json):addresses[0]:addressLine1::VARCHAR AS address_line_1
FROM HMDM_DEV.STAGING.HCP_ADDRESS
LIMIT 5;

SELECT 
    iqvia_id,
    PARSE_JSON(response_json):specialty::VARCHAR AS specialty,
    PARSE_JSON(response_json):specialtyType::VARCHAR AS specialty_type
FROM HMDM_DEV.STAGING.HCP_SPECIALTY
LIMIT 5;


-------------------------------------------------------------------------------
-- SECTION 5: BATCH AND LINEAGE METADATA CHECKS
--
-- Purpose: Verify batch_id and source_system_name are populated correctly
--   in all synced tables. These columns link Snowflake records back to the
--   Databricks batch log (ctl_batch_log_tbl) for audit trail.
-------------------------------------------------------------------------------

-- 5A. Check batch_id distribution in HCP_ADDRESS
--     Each unique batch_id represents one API ingestion batch
SELECT 
    batch_id,
    COUNT(*) AS record_count
FROM HMDM_DEV.STAGING.HCP_ADDRESS
GROUP BY batch_id
ORDER BY batch_id;

-- 5B. Check source_system_name values (should be IQVIA_API)
SELECT 
    Source_Name,
    COUNT(*) AS record_count
FROM HMDM_DEV.STAGING.HCP_ADDRESS
GROUP BY Source_Name
ORDER BY record_count DESC;

-- 5C. Check mdm_batch_id values (links to Databricks batch log)
SELECT 
    mdm_batch_id,
    COUNT(*) AS record_count
FROM HMDM_DEV.STAGING.HCP_ADDRESS
GROUP BY mdm_batch_id
ORDER BY mdm_batch_id;

-- 5D. Check LOAD_DATE distribution (when sync ran)
--     All records should have a recent LOAD_DATE from the last sync run
SELECT 
    DATE(LOAD_DATE) AS load_date,
    COUNT(*) AS records_loaded
FROM HMDM_DEV.STAGING.HCP_ADDRESS
GROUP BY DATE(LOAD_DATE)
ORDER BY load_date DESC
LIMIT 10;

-- 5E. Cross-table batch consistency check
--     Verify the same batch_ids appear across multiple HCP tables
--     (they should — all HCP tables are loaded from the same API batches)
SELECT 
    'HCP_ADDRESS' AS tbl, 
    COLLECT_SET(batch_id) AS batch_ids
FROM HMDM_DEV.STAGING.HCP_ADDRESS
UNION ALL
SELECT 'HCP_SPECIALTY', COLLECT_SET(batch_id)
FROM HMDM_DEV.STAGING.HCP_SPECIALTY
UNION ALL
SELECT 'HCP_PHONE', COLLECT_SET(batch_id)
FROM HMDM_DEV.STAGING.HCP_PHONE;


-------------------------------------------------------------------------------
-- SECTION 6: DUPLICATE DETECTION
--
-- Purpose: Check for duplicate records in synced STAGING tables.
--   Duplicates could indicate sync re-runs without proper TRUNCATE-AND-LOAD.
--   The push_to_snowflake.py script uses DELETE + INSERT (idempotent),
--   so duplicates should NOT exist.
-------------------------------------------------------------------------------

-- 6A. Check for duplicate iqvia_id values in HCP tables
--     Each iqvia_id should appear only once per table (after dedup in Databricks)
SELECT 
    'HCP_ADDRESS' AS table_name,
    iqvia_id,
    COUNT(*) AS duplicate_count
FROM HMDM_DEV.STAGING.HCP_ADDRESS
GROUP BY iqvia_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10;

-- 6B. Check for full-row duplicates (all columns identical)
--     This should return 0 rows — TRUNCATE-AND-LOAD prevents duplicates
SELECT 
    'HCP_SPECIALTY' AS table_name,
    COUNT(*) - COUNT(DISTINCT iqvia_id) AS possible_duplicates
FROM HMDM_DEV.STAGING.HCP_SPECIALTY;

-- 6C. Check for duplicate (iqvia_id, batch_id) combinations
--     Same iqvia_id in the same batch indicates a sync issue
SELECT 
    iqvia_id,
    batch_id,
    COUNT(*) AS dup_count
FROM HMDM_DEV.STAGING.HCP_ADDRESS
GROUP BY iqvia_id, batch_id
HAVING COUNT(*) > 1
LIMIT 10;


-------------------------------------------------------------------------------
-- SECTION 7: MASTER TABLE VALIDATIONS (DDL PLACEHOLDER TABLES)
--
-- Purpose: Verify the 9 MASTER tables exist with correct schemas but are empty.
--   These tables are created by Snowflake DDL file 06_create_master_tables.sql.
--   They are NOT populated by the Databricks sync — they are filled by
--   Informatica MDM export or by the Snowpark snapshot script.
-------------------------------------------------------------------------------

-- 7A. Row counts for ALL 9 MASTER tables (expected: ALL ZERO)
SELECT 'HCP_SPECIALTY'              AS table_name, COUNT(*) AS row_count FROM HMDM_DEV.MASTER.HCP_SPECIALTY
UNION ALL SELECT 'HCP_ALTERNATE_NAME',      COUNT(*) FROM HMDM_DEV.MASTER.HCP_ALTERNATE_NAME
UNION ALL SELECT 'HCP_EDUCATION',           COUNT(*) FROM HMDM_DEV.MASTER.HCP_EDUCATION
UNION ALL SELECT 'HCP_IDENTIFICATION',      COUNT(*) FROM HMDM_DEV.MASTER.HCP_IDENTIFICATION
UNION ALL SELECT 'HCO',                     COUNT(*) FROM HMDM_DEV.MASTER.HCO
UNION ALL SELECT 'HCO_NAME',                COUNT(*) FROM HMDM_DEV.MASTER.HCO_NAME
UNION ALL SELECT 'HCO_ALTERNATE_IDENTIFIER',COUNT(*) FROM HMDM_DEV.MASTER.HCO_ALTERNATE_IDENTIFIER
UNION ALL SELECT 'HCO_PHONE',              COUNT(*) FROM HMDM_DEV.MASTER.HCO_PHONE
UNION ALL SELECT 'HCO_SPECIALTY',          COUNT(*) FROM HMDM_DEV.MASTER.HCO_SPECIALTY
ORDER BY table_name;

-- 7B. Check column names in MASTER tables
--     These should have MDM-specific columns (NOT generic like STAGING)
--     Example: sourcePKey, firstName, lastName, X_informatica_Specialty, etc.
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MASTER'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_NAME = 'HCO'
ORDER BY ORDINAL_POSITION;

-- 7C. Sample data (expected: empty — 0 rows)
SELECT * FROM HMDM_DEV.MASTER.HCO LIMIT 5;
SELECT * FROM HMDM_DEV.MASTER.HCP_SPECIALTY LIMIT 5;

-- 7D. Verify MASTER table DDL (to see full CREATE TABLE statement)
SELECT GET_DDL('TABLE', 'HMDM_DEV.MASTER.HCO');
SELECT GET_DDL('TABLE', 'HMDM_DEV.MASTER.HCP_SPECIALTY');


-------------------------------------------------------------------------------
-- SECTION 8: MDM HUB TABLE VALIDATIONS
--
-- Purpose: Verify the mdm_hub placeholder tables exist in HMDM_DEV.MDM schema.
--   These are created by Snowflake DDL file 05_create_mdm_tables.sql.
--   dbt HCP child master models (master_hcp_specialty, master_hcp_alternate_name,
--   master_hcp_license, master_hcp_therapeutic_area) read from these as a source.
--   Expected: ALL ZERO rows — populated by Informatica MDM engine, not by sync.
-------------------------------------------------------------------------------

-- 8A. Row counts for ALL MDM hub tables
SELECT 'hcp_specialty'         AS table_name, COUNT(*) AS row_count FROM HMDM_DEV.MDM.hcp_specialty
UNION ALL SELECT 'hcp_alternate_name',  COUNT(*) FROM HMDM_DEV.MDM.hcp_alternate_name
UNION ALL SELECT 'hcp_license',         COUNT(*) FROM HMDM_DEV.MDM.hcp_license
UNION ALL SELECT 'hcp_therapeutic_area',COUNT(*) FROM HMDM_DEV.MDM.hcp_therapeutic_area
ORDER BY table_name;

-- 8B. Check if dbt created master_hcp and master_hco tables in MDM schema
--     (dbt run logs showed these were created in MDM, not HMDM_DEV.MDM)
SELECT 'master_hcp' AS table_name, COUNT(*) AS row_count FROM HMDM_DEV.MDM.master_hcp
UNION ALL SELECT 'master_hco', COUNT(*) FROM HMDM_DEV.MDM.master_hco;

-- 8C. Column structure of MDM hub tables
DESCRIBE TABLE HMDM_DEV.MDM.hcp_specialty;
DESCRIBE TABLE HMDM_DEV.MDM.hcp_license;
DESCRIBE TABLE HMDM_DEV.MDM.hcp_alternate_name;
DESCRIBE TABLE HMDM_DEV.MDM.hcp_therapeutic_area;


-------------------------------------------------------------------------------
-- SECTION 9: SYNC COMPLETENESS CROSS-CHECK
--
-- Purpose: Verify that ALL expected tables were synced and no table was
--   missed by push_to_snowflake.py. Compare expected table list against
--   actual tables in Snowflake.
-------------------------------------------------------------------------------

-- 9A. Expected STAGING tables (22 total) — check each one exists
SELECT 
    'HCP_ADDRESS' AS expected_table,
    CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_ADDRESS' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END AS status
UNION ALL SELECT 'HCP_ALTERNATE_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_ALTERNATE_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_EDUCATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_EDUCATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_EMAIL', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_EMAIL' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_HCO_AFFILIATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_HCO_AFFILIATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_IDENTIFICATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_IDENTIFICATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_LANGUAGE', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_LANGUAGE' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_LICENSE', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_LICENSE' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_ORIGIN_UNIVERSITY', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_ORIGIN_UNIVERSITY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_PHONE', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_PHONE' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_SPECIALTY', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_SPECIALTY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_TAX', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_TAX' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_TENDENCIES', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCP_TENDENCIES' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_ADDRESS', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCO_ADDRESS' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_ALTERNATE_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCO_ALTERNATE_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_EMAIL', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCO_EMAIL' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_HIERARCHY', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCO_HIERARCHY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_IDENTIFICATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCO_IDENTIFICATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCO_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_PHONE', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCO_PHONE' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_SPECIALTY', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCO_SPECIALTY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_TAX', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STAGING' AND TABLE_NAME='HCO_TAX' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
ORDER BY expected_table;

-- 9B. Expected MASTER tables (9 total) — check each one exists
SELECT 
    'HCP_SPECIALTY' AS expected_table,
    CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MASTER' AND TABLE_NAME='HCP_SPECIALTY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END AS status
UNION ALL SELECT 'HCP_ALTERNATE_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MASTER' AND TABLE_NAME='HCP_ALTERNATE_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_EDUCATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MASTER' AND TABLE_NAME='HCP_EDUCATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCP_IDENTIFICATION', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MASTER' AND TABLE_NAME='HCP_IDENTIFICATION' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MASTER' AND TABLE_NAME='HCO' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_NAME', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MASTER' AND TABLE_NAME='HCO_NAME' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_ALTERNATE_IDENTIFIER', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MASTER' AND TABLE_NAME='HCO_ALTERNATE_IDENTIFIER' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_PHONE', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MASTER' AND TABLE_NAME='HCO_PHONE' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
UNION ALL SELECT 'HCO_SPECIALTY', CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MASTER' AND TABLE_NAME='HCO_SPECIALTY' AND TABLE_CATALOG='HMDM_DEV') THEN 'EXISTS' ELSE 'MISSING' END
ORDER BY expected_table;


-------------------------------------------------------------------------------
-- SECTION 10: OVERALL SYNC HEALTH SUMMARY
--
-- Purpose: One final query that gives a complete overview of the sync state.
--   Run this to get a single-glance picture of whether the Databricks-to-
--   Snowflake sync was successful.
-------------------------------------------------------------------------------

-- 10A. Complete summary: tables, rows, schemas, status
SELECT 
    'STAGING (Databricks sync)' AS layer,
    COUNT(*) AS table_count,
    SUM(ROW_COUNT) AS total_rows,
    '22 tables synced from Databricks' AS description
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'STAGING' 
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 
    'MASTER (DDL placeholders)',
    COUNT(*),
    SUM(ROW_COUNT),
    '9 tables, 0 rows (for Informatica MDM)'
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'MASTER'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 
    'MDM hub (DDL placeholders)',
    COUNT(*),
    SUM(ROW_COUNT),
    'mdm_hub tables, 0 rows (for Informatica MDM)'
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'MDM'
  AND TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY layer;

-- 10B. Quick check: any table with unexpected row count?
--     STAGING tables with 0 rows (except HCP_NAME) may indicate sync failure
--     MASTER/MDM tables with > 0 rows may indicate unexpected data
SELECT 
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    ROW_COUNT AS row_count,
    CASE 
        WHEN TABLE_SCHEMA = 'STAGING' AND ROW_COUNT = 0 AND TABLE_NAME != 'HCP_NAME' 
            THEN 'WARNING: 0 rows in STAGING table'
        WHEN TABLE_SCHEMA IN ('MASTER', 'MDM') AND ROW_COUNT > 0 
            THEN 'WARNING: unexpected data in placeholder table'
        ELSE 'OK'
    END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'HMDM_DEV'
  AND TABLE_TYPE = 'BASE TABLE'
  AND TABLE_SCHEMA IN ('STAGING', 'MASTER', 'MDM')
ORDER BY TABLE_SCHEMA, TABLE_NAME;


-------------------------------------------------------------------------------
-- END OF 08_SNOW_VALIDATIONS
-- 
-- Summary of what to look for:
--   Section 1:  All schemas and tables exist (22 STAGING + 9 MASTER + MDM hub)
--   Section 2:  Row counts match Databricks (HCP tables have mock data, HCP_NAME=0)
--   Section 3:  All STAGING tables have generic columns + response_json
--   Section 4:  response_json is valid JSON, no NULLs in key columns
--   Section 5:  batch_id and Source_Name populated correctly
--   Section 6:  No duplicate records (TRUNCATE-AND-LOAD prevents this)
--   Section 7:  MASTER tables exist but are empty (DDL placeholders)
--   Section 8:  MDM hub tables exist but are empty (DDL placeholders)
--   Section 9:  All expected tables present (no sync gaps)
--   Section 10: Overall health summary
-------------------------------------------------------------------------------