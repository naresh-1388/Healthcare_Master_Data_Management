-- ============================================================
-- Healthcare_Master_Data_Management - Snowflake DROP ALL
--
-- Purpose: Drop ALL tables, views, and schemas created by files
-- 01-06 in the HMDM_DEV database. Complete teardown script.
-- Run this before re-running 01-06 for a clean rebuild.
--
-- What it drops:
--   - All views in STAGING, MDM (dbt-created)
--   - All tables in RAW, LANDING, CANONICAL, STAGING, MDM, MASTER, UTIL
--   - All 7 schemas themselves (CASCADE drops everything inside)
--
-- What it keeps:
--   - The HMDM_DEV database (so you can re-run 01_create_schemas.sql)
--   - The HMDM_DEV_ROLE role and grants (managed by 00_create_role_and_grants.sql)
--
-- Usage: Select ALL lines below and run in Snowflake worksheet (Snowsight).
--   Then re-run 01_create_schemas.sql through 06_create_master_tables.sql.
-- ============================================================

-- CASCADE drops all tables, views, and objects inside each schema.
-- One DROP SCHEMA per schema -- pure SQL, no stored procedure needed.

DROP SCHEMA IF EXISTS HMDM_DEV.RAW CASCADE;
DROP SCHEMA IF EXISTS HMDM_DEV.LANDING CASCADE;
DROP SCHEMA IF EXISTS HMDM_DEV.CANONICAL CASCADE;
DROP SCHEMA IF EXISTS HMDM_DEV.STAGING CASCADE;
DROP SCHEMA IF EXISTS HMDM_DEV.MDM CASCADE;
DROP SCHEMA IF EXISTS HMDM_DEV.MASTER CASCADE;
DROP SCHEMA IF EXISTS HMDM_DEV.UTIL CASCADE;

-- Verify all schemas are gone (should return 0 rows for our schemas)
SHOW SCHEMAS IN DATABASE HMDM_DEV;

-- ============================================================
-- Optional: Drop the entire HMDM_DEV database completely.
-- Only use if starting from absolute scratch (re-run 00 + 01-06).
-- This also drops the PUBLIC schema and any stored procedures.
-- ============================================================
-- DROP DATABASE IF EXISTS HMDM_DEV;

-- Step 1: Run 10_drop_all.sql (lines 24-30)        → Drops 7 schemas
-- Step 2: Run 01_create_schemas.sql                 → Recreates 7 schemas (but NO permissions)
-- Step 3: Re-run 00_create_role_and_grants.sql     → Role already exists (no-op), but GRANTs reapply
--         OR just run the GRANT lines from 00      → Skip CREATE ROLE, only run GRANT statements
-- Step 4: Run 02-06                                 → Create tables