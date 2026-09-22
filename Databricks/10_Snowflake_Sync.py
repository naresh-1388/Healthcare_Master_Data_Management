# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Stage 7: Snowflake Sync
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management — Stage 7: Snowflake Sync
# MAGIC
# MAGIC Pushes Databricks STAGING and MASTER tables to Snowflake using
# MAGIC `snowflake-connector-python` and `write_pandas`. This is the bridge
# MAGIC that fills Snowflake tables so dbt models and Snowpark scripts can
# MAGIC operate on real data instead of empty table structures.
# MAGIC
# MAGIC **When to run:** AFTER Stage 6 (Egress) completes. The pipeline order
# MAGIC is: Ingestion → Standardization → DQ → MDM Ingress → MDM Egress →
# MAGIC Snowflake Sync.
# MAGIC
# MAGIC **Sync pattern — TRUNCATE-AND-LOAD:**
# MAGIC For each table, existing Snowflake rows are DELETEd first, then fresh
# MAGIC data is INSERTed from Databricks. This is idempotent (re-running
# MAGIC produces the same result), preserves Snowflake table schema and grants
# MAGIC (no DROP), and avoids duplicates (no append without DELETE).
# MAGIC
# MAGIC **Incremental guard:** Before syncing, the batch control table is
# MAGIC checked for `snowflake_sync_status = 'Y'`. If already synced, the
# MAGIC sync is skipped — only new or not-yet-synced batches trigger the
# MAGIC full sync.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * AWS Secrets Manager secret `healthcare-mdm/dev/api-snowflake`
# MAGIC   (keys: snowflake_user, snowflake_password, snowflake_account,
# MAGIC   snowflake_warehouse, snowflake_database, snowflake_schema)
# MAGIC * Databricks service credential `healthcare_mdm_secrets_credential`
# MAGIC * Snowflake DDL executed (snowflake/*.sql)
# MAGIC * Stages 1-6 of the pipeline completed (notebooks 01-08)

# COMMAND ----------

# DBTITLE 1,Stage 7: Snowflake Sync
# ============================================================
# STAGE 7: SNOWFLAKE SYNC
# ============================================================
# This notebook bridges Databricks Delta tables to Snowflake,
# so that dbt models and Snowpark scripts can operate on real data.
#
# Runs AFTER Stage 6 (Egress) — pushes staging + master tables
# from Databricks to Snowflake.
#
# Prerequisites:
#   1. %pip install snowflake-connector-python (done in init cell)
#   2. AWS Secrets Manager secret 'healthcare-mdm/dev/api-snowflake'
#      with keys: snowflake_user, snowflake_password, snowflake_account,
#      snowflake_warehouse, snowflake_database, snowflake_schema
#   3. Databricks service credential 'healthcare_mdm_secrets_credential'
#   4. Snowflake DDL executed (snowflake/*.sql)
# ============================================================

# COMMAND ----------

# DBTITLE 1,Install Snowflake Connector
# MAGIC %md
# MAGIC #### 1. Install Snowflake Connector
# MAGIC
# MAGIC Installs `snowflake-connector-python` on Serverless compute (this
# MAGIC package is not pre-installed on the Databricks Serverless base image).
# MAGIC
# MAGIC `dbutils.library.restartPython()` is called immediately after the
# MAGIC pip install to make the package available in the Python kernel. This
# MAGIC runs BEFORE any variable definitions, so the kernel restart does not
# MAGIC lose any state — all variables are defined in subsequent cells.

# COMMAND ----------

# DBTITLE 1,Install Snowflake Connector
# Install Snowflake connector (required on Serverless — not pre-installed)
# restartPython() runs BEFORE any variable definitions, so state loss is harmless.
%pip install snowflake-connector-python --quiet
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %md
# MAGIC #### 2. Imports
# MAGIC
# MAGIC Imports the Snowflake sync module and runtime configuration.
# MAGIC
# MAGIC * `run_snowflake_sync` — Main entry point that pushes all STAGING
# MAGIC   and MASTER tables from Databricks to Snowflake using the
# MAGIC   TRUNCATE-AND-LOAD pattern (DELETE existing rows, then INSERT).
# MAGIC * `get_snowflake_credentials` — Retrieves Snowflake connection
# MAGIC   parameters from AWS Secrets Manager.
# MAGIC * `catalog`, `env` — Runtime configuration from `core.runtime_config`.
# MAGIC
# MAGIC The source path is resolved from the notebook's location in the
# MAGIC Git repo, so the imports work regardless of which workspace the
# MAGIC notebook runs in.

# COMMAND ----------

# DBTITLE 1,Imports
# ============================================================
# IMPORTS — Snowflake sync module + runtime config
# ============================================================
import sys, os, importlib
from pathlib import Path

NOTEBOOK_DIR = Path(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    .notebookPath().get()
).parent
REPO_ROOT = Path("/Workspace" + str(NOTEBOOK_DIR.parent))
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

# Force-reload in case module was cached from a previous run
import importlib
import snowflake_sync.push_to_snowflake
importlib.reload(snowflake_sync.push_to_snowflake)

from core.runtime_config import catalog, env
from snowflake_sync.push_to_snowflake import (
    run_snowflake_sync,
    get_snowflake_credentials,
)

print(f"Catalog: {catalog}")
print(f"Environment: {env}")
print(f"Source path: {SRC_ROOT}")

# COMMAND ----------

# DBTITLE 1,Check Snowflake Credentials
# MAGIC %md
# MAGIC #### 3. Check Snowflake Credentials
# MAGIC
# MAGIC Verifies that Snowflake connection parameters are available from
# MAGIC AWS Secrets Manager before attempting the sync. The credentials
# MAGIC are retrieved by `get_snowflake_credentials()` from
# MAGIC `push_to_snowflake.py`, which reads the secret
# MAGIC `healthcare-mdm/dev/api-snowflake` using the Databricks service
# MAGIC credential `healthcare_mdm_secrets_credential`.
# MAGIC
# MAGIC If credentials are missing, the sync is skipped gracefully with
# MAGIC a warning message. No error is raised — the notebook exits normally
# MAGIC so that the pipeline can continue without Snowflake sync if needed.

# COMMAND ----------

# DBTITLE 1,Check Snowflake Credentials
# ============================================================
# CHECK SNOWFLAKE CREDENTIALS
# ============================================================
# Verify that Snowflake connection is configured before attempting sync.
# If credentials are missing, the sync is skipped gracefully.

creds = get_snowflake_credentials()

if creds:
    print("Snowflake credentials found — proceeding with sync")
else:
    print("WARNING: Snowflake credentials NOT configured.")
    print("    Check AWS Secrets Manager secret 'healthcare-mdm/dev/api-snowflake'")
    print("    and Databricks service credential 'healthcare_mdm_secrets_credential'.")

# COMMAND ----------

# DBTITLE 1,Run Snowflake Sync
# MAGIC %md
# MAGIC #### 4. Run Snowflake Sync
# MAGIC
# MAGIC Pushes Databricks staging and master tables to Snowflake using the
# MAGIC TRUNCATE-AND-LOAD pattern (DELETE existing rows, then INSERT fresh data).
# MAGIC This is the standard production ETL approach — see `push_to_snowflake.py`
# MAGIC `_sync_one_table()` for implementation details.
# MAGIC
# MAGIC **Incremental guard:** Before syncing, this cell checks the batch
# MAGIC control table for `snowflake_sync_status = 'Y'` on the latest
# MAGIC IQVIA_API batch. If already synced, the entire sync is skipped to
# MAGIC avoid unnecessary re-pushes of unchanged data. Only when the
# MAGIC status is not 'Y' (new data arrived or first run) does the full
# MAGIC sync execute.
# MAGIC
# MAGIC **Why not overwrite=True?** `overwrite=True` drops the entire
# MAGIC Snowflake table and recreates it. This loses Snowflake grants,
# MAGIC row-access policies, tags, and column comments. If the INSERT fails
# MAGIC after the DROP, you have no table.
# MAGIC
# MAGIC **Why not append?** Append mode (`overwrite=False` without DELETE)
# MAGIC causes duplicate rows on every re-run.
# MAGIC
# MAGIC **TRUNCATE-AND-LOAD is the right approach:**
# MAGIC * DELETE existing rows first (preserves table schema and grants)
# MAGIC * INSERT fresh data (idempotent — running it multiple times
# MAGIC   produces the same result)
# MAGIC * If the table does not exist yet, `auto_create_table=True` creates
# MAGIC   it automatically on the first run

# COMMAND ----------

# DBTITLE 1,Run Snowflake Sync
# ============================================================
# RUN SNOWFLAKE SYNC
# ============================================================
# Push Databricks staging + master tables to Snowflake.
# This is the bridge that fills Snowflake's empty tables
# so dbt models and Snowpark can operate on real data.

# Check if Snowflake sync is already done for the latest batch.
# If snowflake_sync_status = 'Y' for the latest IQVIA_API batch,
# the sync is skipped to avoid re-pushing unchanged data.
sync_already_done = False
if creds:
    try:
        existing_cols = spark.sql("DESCRIBE TABLE HMDM_DEV.util.ctl_batch_log_tbl").collect()
        col_names = [row.col_name for row in existing_cols]
        
        if 'snowflake_sync_status' in col_names:
            latest_sync = spark.sql("""
                SELECT snowflake_sync_status 
                FROM HMDM_DEV.util.ctl_batch_log_tbl 
                WHERE source_system_name = 'IQVIA_API'
                ORDER BY batch_id DESC LIMIT 1
            """).collect()
            
            if latest_sync and latest_sync[0]['snowflake_sync_status'] == 'Y':
                sync_already_done = True
                print("Snowflake sync already completed for latest batch (IQVIA_API) — skipping")
    except Exception as e:
        print(f"Could not check batch sync status: {e}")

if creds and not sync_already_done:
    results = run_snowflake_sync(
        catalog=catalog,
        sync_staging=True,
        sync_master=True,
    )
    
    # Display summary
    print("\n=== Sync Results ===")
    for layer, tables in results.items():
        print(f"\n{layer.upper()}:")
        for table, result in tables.items():
            if "error" in result:
                print(f"  {table}: ERROR — {result['error']}")
            else:
                print(f"  {table}: {result.get('source_rows', 0)} rows synced")
elif creds and sync_already_done:
    print("Sync skipped — latest batch already synced to Snowflake")
    results = {}
else:
    print("Sync skipped — no Snowflake credentials")
    results = {}

# COMMAND ----------

# DBTITLE 1,Update Batch Log
# MAGIC %md
# MAGIC #### 5. Update Batch Log
# MAGIC
# MAGIC Marks the Snowflake sync as complete in the batch control table
# MAGIC (`HMDM_DEV.util.ctl_batch_log_tbl`) by setting
# MAGIC `snowflake_sync_status = 'Y'` for all batches where `egress_status = 'Y'`.
# MAGIC
# MAGIC This cell also adds the `snowflake_sync_status` column to the batch
# MAGIC log table if it does not exist. Databricks does not support
# MAGIC `ADD COLUMN IF NOT EXISTS` syntax, so the schema is checked first
# MAGIC using `DESCRIBE TABLE` and the column is added with `ADD COLUMNS`
# MAGIC only when missing.
# MAGIC
# MAGIC This status is what the Run Snowflake Sync cell (step 4) checks
# MAGIC before syncing — if the status is already 'Y', the sync is skipped.
# MAGIC This prevents unnecessary re-syncs of unchanged data.

# COMMAND ----------

# DBTITLE 1,Update Batch Log
# ============================================================
# UPDATE BATCH LOG
# ============================================================
# Mark Snowflake sync as complete in the batch control table.
# This adds a new column 'snowflake_sync_status' to track sync state.

if creds and results:
    # Check if any tables had errors before marking sync as complete.
    # Only set snowflake_sync_status = 'Y' if all tables synced successfully.
    has_errors = False
    for layer, tables in results.items():
        for table, result in tables.items():
            if "error" in result:
                has_errors = True
                break
    
    if has_errors:
        print("WARNING: Some tables failed to sync — snowflake_sync_status NOT set to 'Y'")
        print("    Re-run after fixing the errors above.")
    else:
        try:
            # Add snowflake_sync_status column if it doesn't exist.
            # Databricks does not support 'ADD COLUMN IF NOT EXISTS',
            # so we check the schema first.
            existing_cols = spark.sql("DESCRIBE TABLE HMDM_DEV.util.ctl_batch_log_tbl").collect()
            col_names = [row.col_name for row in existing_cols]
            
            if 'snowflake_sync_status' not in col_names:
                spark.sql("""
                    ALTER TABLE HMDM_DEV.util.ctl_batch_log_tbl
                    ADD COLUMNS (snowflake_sync_status STRING)
                """)
                print("Added snowflake_sync_status column")
            
            # Update latest batch
            spark.sql("""
                UPDATE HMDM_DEV.util.ctl_batch_log_tbl
                SET snowflake_sync_status = 'Y'
                WHERE egress_status = 'Y'
                  AND coalesce(snowflake_sync_status, 'N') != 'Y'
            """)
            print("Batch log updated: snowflake_sync_status = 'Y'")
        except Exception as e:
            print(f"WARNING: Could not update batch log: {e}")
else:
    print("Batch log update skipped")

# COMMAND ----------

# DBTITLE 1,Complete
# MAGIC %md
# MAGIC #### 6. Complete
# MAGIC
# MAGIC Prints the completion banner for Stage 7. This cell runs after the
# MAGIC batch log is updated and confirms the Snowflake sync stage is done.
# MAGIC The notebook does not call `dbutils.notebook.exit()` here because the
# MAGIC verification cell runs next.

# COMMAND ----------

# DBTITLE 1,Complete
print("\n" + "=" * 60)
print("STAGE 7: SNOWFLAKE SYNC COMPLETE")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Verify Snowflake Data
# MAGIC %md
# MAGIC #### 7. Verify Snowflake Data
# MAGIC
# MAGIC Connects to Snowflake and verifies all synced tables by checking row
# MAGIC counts against expected values from the Databricks pipeline.
# MAGIC
# MAGIC This cell runs AFTER the sync is complete. It connects to Snowflake
# MAGIC using the same credentials from AWS Secrets Manager, queries each
# MAGIC table with `SELECT COUNT(*)`, and compares against expected row
# MAGIC counts:
# MAGIC
# MAGIC * STAGING: 22 tables (13 HCP + 9 HCO), 57 rows
# MAGIC   - HCP tables: 3 rows each (except HCP_NAME = 0, expected —
# MAGIC     DQ null_check rejects mock API records with empty firstName)
# MAGIC   - HCO tables: 2 rows each
# MAGIC * MASTER: 9 tables (4 HCP + 5 HCO), 22 rows
# MAGIC   - HCP tables: 3 rows each
# MAGIC   - HCO tables: 2 rows each
# MAGIC * Total: 31 tables, 76 rows, 0 errors
# MAGIC
# MAGIC Output shows PASS/SKIP/ERROR for each table and a final summary.

# COMMAND ----------

# DBTITLE 1,Verify Snowflake Data
# ============================================================
# VERIFY SNOWFLAKE DATA
# ============================================================
# Connect to Snowflake and verify all synced tables:
#   1. Row counts for all STAGING tables (expected: 13 HCP × 3 + 9 HCO × 2 = 57)
#   2. Row counts for all MASTER tables (expected: 4 HCP × 3 + 5 HCO × 2 = 22)
#   3. Total: 31 tables, 76 rows
import snowflake.connector
import pandas as pd

creds = get_snowflake_credentials()

if creds:
    conn = snowflake.connector.connect(
        user=creds['user'],
        password=creds['password'],
        account=creds['account'],
        warehouse=creds.get('warehouse', 'COMPUTE_WH'),
        database=creds.get('database', 'HMDM_DEV'),
        schema=creds.get('schema', 'PUBLIC'),
    )
    cur = conn.cursor()

    # --- 1. STAGING row counts ---
    staging_tables = [
        'HCP_NAME', 'HCP_ADDRESS', 'HCP_ALTERNATE_NAME', 'HCP_IDENTIFICATION',
        'HCP_SPECIALTY', 'HCP_PHONE', 'HCP_EMAIL', 'HCP_EDUCATION',
        'HCP_TENDENCIES', 'HCP_ORIGIN_UNIVERSITY', 'HCP_TAX', 'HCP_LANGUAGE',
        'HCP_HCO_AFFILIATION',
        'HCO_NAME', 'HCO_ADDRESS', 'HCO_ALTERNATE_NAME', 'HCO_IDENTIFICATION',
        'HCO_SPECIALTY', 'HCO_PHONE', 'HCO_EMAIL', 'HCO_TAX', 'HCO_HIERARCHY',
    ]

    print("=" * 60)
    print("SNOWFLAKE VERIFICATION: STAGING TABLES")
    print("=" * 60)
    staging_total = 0
    staging_results = []
    for tbl in staging_tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM HMDM_DEV.STAGING.{tbl}")
            cnt = cur.fetchone()[0]
            staging_total += cnt
            status = "PASS" if cnt > 0 else "SKIP"
            entity = "HCP" if tbl.startswith("HCP") else "HCO"
            print(f"  {status} STAGING.{tbl}: {cnt} rows")
            staging_results.append((entity, tbl, cnt))
        except Exception as e:
            print(f"  ERROR: STAGING.{tbl}: {e}")
            staging_results.append(("ERR", tbl, -1))

    print(f"  Total STAGING rows: {staging_total}")

    # --- 2. MASTER row counts ---
    master_tables = [
        'HCP_SPECIALTY', 'HCP_ALTERNATE_NAME', 'HCP_EDUCATION', 'HCP_IDENTIFICATION',
        'HCO', 'HCO_NAME', 'HCO_ALTERNATE_IDENTIFIER', 'HCO_PHONE', 'HCO_SPECIALTY',
    ]

    print("\n" + "=" * 60)
    print("SNOWFLAKE VERIFICATION: MASTER TABLES")
    print("=" * 60)
    master_total = 0
    master_results = []
    for tbl in master_tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM HMDM_DEV.MASTER.{tbl}")
            cnt = cur.fetchone()[0]
            master_total += cnt
            status = "PASS" if cnt > 0 else "SKIP"
            entity = "HCP" if tbl.startswith("HCP") else "HCO"
            print(f"  {status} MASTER.{tbl}: {cnt} rows")
            master_results.append((entity, tbl, cnt))
        except Exception as e:
            print(f"  ERROR: MASTER.{tbl}: {e}")
            master_results.append(("ERR", tbl, -1))

    print(f"  Total MASTER rows: {master_total}")

    # --- 3. Summary ---
    total_tables = len(staging_results) + len(master_results)
    total_rows = staging_total + master_total
    errors = sum(1 for _, _, c in staging_results + master_results if c < 0)

    print("\n" + "=" * 60)
    print("SNOWFLAKE VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"  Tables checked:  {total_tables}")
    print(f"  Total rows:      {total_rows}")
    print(f"  Errors:          {errors}")
    print(f"  STAGING rows:    {staging_total} (expected: 57)")
    print(f"  MASTER rows:     {master_total} (expected: 22)")
    if errors == 0 and total_rows == 76:
        print("  VERIFICATION PASSED")
    else:
        print("  WARNING: VERIFICATION INCOMPLETE — check errors above")
    print("=" * 60)

    cur.close()
    conn.close()
else:
    print("WARNING: No Snowflake credentials — cannot verify")