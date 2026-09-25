# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Stage 7: Snowflake Sync
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management -- Stage 7: Snowflake Sync
# MAGIC
# MAGIC Pushes Databricks STAGING and MASTER tables to Snowflake using
# MAGIC `snowflake-connector-python` and `write_pandas`. This is the bridge
# MAGIC that fills Snowflake tables so dbt models and Snowpark scripts can
# MAGIC operate on real data instead of empty table structures.
# MAGIC
# MAGIC **When to run:** AFTER Stage 6 (Egress) completes. The pipeline order
# MAGIC is: Ingestion -> Standardization -> DQ -> MDM Ingress -> MDM Egress ->
# MAGIC Snowflake Sync.
# MAGIC
# MAGIC **Sync pattern -- TRUNCATE-AND-LOAD:**
# MAGIC For each table, existing Snowflake rows are DELETEd first, then fresh
# MAGIC data is INSERTed from Databricks. This is idempotent (re-running
# MAGIC produces the same result), preserves Snowflake table schema and grants
# MAGIC (no DROP), and avoids duplicates (no append without DELETE).
# MAGIC
# MAGIC **Incremental guard:** Before syncing, the batch control table is
# MAGIC checked for `snowflake_sync_status = 'Y'`. If already synced, the
# MAGIC sync is skipped to save Snowflake credits. The `force_resync` widget
# MAGIC (default: false) can be set to 'true' to force a full re-sync after
# MAGIC pipeline replay. By default, no automatic reset is performed.
# MAGIC
# MAGIC **Missing table handling:** If a Databricks source table does not
# MAGIC exist, the sync records an error (not a silent SKIP) so the batch is
# MAGIC not falsely marked as successfully synced.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * AWS Secrets Manager secret `healthcare-mdm/dev/api-snowflake`
# MAGIC   (keys: snowflake_user, snowflake_password, snowflake_account,
# MAGIC   snowflake_warehouse, snowflake_database, snowflake_schema)
# MAGIC * Databricks service credential `healthcare_mdm_secrets_credential`
# MAGIC * Snowflake DDL executed (snowflake/*.sql)
# MAGIC * Stages 1-6 of the pipeline completed (notebooks 03-08)

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
# MAGIC lose any state -- all variables are defined in subsequent cells.

# COMMAND ----------

# DBTITLE 1,Install Snowflake Connector
# Install Snowflake connector (required on Serverless -- not pre-installed)
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
# MAGIC * `run_snowflake_sync` -- Main entry point that pushes all STAGING
# MAGIC   and MASTER tables from Databricks to Snowflake using the
# MAGIC   TRUNCATE-AND-LOAD pattern (DELETE existing rows, then INSERT).
# MAGIC * `get_snowflake_credentials` -- Retrieves Snowflake connection
# MAGIC   parameters from AWS Secrets Manager.
# MAGIC * `catalog`, `env` -- Runtime configuration from `core.runtime_config`.
# MAGIC
# MAGIC The source path is resolved from the notebook's location in the
# MAGIC Git repo, so the imports work regardless of which workspace the
# MAGIC notebook runs in.

# COMMAND ----------

# DBTITLE 1,Imports
# ============================================================
# IMPORTS -- Snowflake sync module + runtime config
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
# MAGIC If credentials are missing, the pipeline FAILS with an error.

# COMMAND ----------

# DBTITLE 1,Check Snowflake Credentials
# ============================================================
# CHECK SNOWFLAKE CREDENTIALS
# ============================================================
# Verify that Snowflake connection is configured before attempting sync.
# If credentials are missing, the pipeline FAILS with an error.

creds = get_snowflake_credentials()

if creds:
    print("Snowflake credentials found -- proceeding with sync")
else:
    print("ERROR: Snowflake credentials NOT configured.")
    print("    Check AWS Secrets Manager secret 'healthcare-mdm/dev/api-snowflake'")
    print("    and Databricks service credential 'healthcare_mdm_secrets_credential'.")
    raise RuntimeError("Snowflake credentials not found. Pipeline cannot continue without Snowflake sync.")

# COMMAND ----------

# DBTITLE 1,Run Snowflake Sync
# MAGIC %md
# MAGIC #### 4. Run Snowflake Sync
# MAGIC
# MAGIC Pushes Databricks staging and master tables to Snowflake using the
# MAGIC TRUNCATE-AND-LOAD pattern (DELETE existing rows, then INSERT fresh data).
# MAGIC This is the standard production ETL approach -- see `push_to_snowflake.py`
# MAGIC `_sync_one_table()` for implementation details.
# MAGIC
# MAGIC **Incremental guard:** Before syncing, this cell checks if
# MAGIC `snowflake_sync_status = 'Y'` for the latest IQVIA_API batch. If already
# MAGIC synced, the entire sync is skipped to save Snowflake credits. The
# MAGIC `force_resync` widget (default: false) can be set to 'true' to force a
# MAGIC full re-sync after pipeline replay.

# COMMAND ----------

# DBTITLE 1,Run Snowflake Sync
# ============================================================
# RUN SNOWFLAKE SYNC
# ============================================================
# Push Databricks staging + master tables to Snowflake.
# This is the bridge that fills Snowflake's empty tables
# so dbt models and Snowpark can operate on real data.

# Incremental guard: Check if Snowflake sync is already done for the
# latest batch. If snowflake_sync_status = 'Y', the sync is skipped
# to avoid re-pushing unchanged data and wasting Snowflake credits.
#
# NOTE: Do NOT reset snowflake_sync_status to 'N' before checking.
# The previous version reset to 'N' then checked for 'Y', which
# meant the guard never triggered -- every run did a full re-sync.
# Now we only reset when the user explicitly sets the
# 'force_resync' widget to 'true' (useful for pipeline replay).

# Add force_resync widget (default: false)
# Set to 'true' to force a full re-sync even if status is already 'Y'.
dbutils.widgets.text("force_resync", "false", "Force re-sync (true/false)")
force_resync = dbutils.widgets.get("force_resync").strip().lower() == "true"

if force_resync:
    print("force_resync=true -- resetting snowflake_sync_status to 'N'")
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET snowflake_sync_status = 'N'
        WHERE source_system_name = 'IQVIA_API'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = 'IQVIA_API'
          )
    """)
    print("Reset snowflake_sync_status to 'N' for latest IQVIA_API batch")

# Check if Snowflake sync is already done for the latest batch.
sync_already_done = False
if creds:
    try:
        existing_cols = spark.sql(f"DESCRIBE TABLE {catalog}.util.ctl_batch_log_tbl").collect()
        col_names = [row.col_name for row in existing_cols]
        
        if 'snowflake_sync_status' in col_names:
            latest_sync = spark.sql(f"""
                SELECT snowflake_sync_status 
                FROM {catalog}.util.ctl_batch_log_tbl 
                WHERE source_system_name = 'IQVIA_API'
                ORDER BY batch_id DESC LIMIT 1
            """).collect()
            
            if latest_sync and latest_sync[0]['snowflake_sync_status'] == 'Y':
                sync_already_done = True
                print("Snowflake sync already completed for latest batch (IQVIA_API) -- skipping")
    except Exception as e:
        print(f"Could not check batch sync status: {e}")

if creds and not sync_already_done:
    results = run_snowflake_sync(
        catalog=catalog,
        sync_staging=True,
        sync_master=True,
    )
    
    # Display summary and collect failed tables
    failed_tables = []
    print("\n=== Sync Results ===")
    for layer, tables in results.items():
        print(f"\n{layer.upper()}:")
        for table, result in tables.items():
            if "error" in result:
                print(f"  {table}: ERROR -- {result['error']}")
                failed_tables.append(f"{layer}.{table}")
            else:
                print(f"  {table}: {result.get('source_rows', 0)} rows synced")
    
    # If ANY sync errors occurred, FAIL the pipeline immediately.
    if failed_tables:
        error_msg = f"Snowflake sync FAILED for {len(failed_tables)} tables: {failed_tables}"
        print(f"\nERROR: {error_msg}")
        raise RuntimeError(error_msg)
elif creds and sync_already_done:
    print("Sync skipped -- latest batch already synced to Snowflake")
    results = {}
else:
    print("ERROR: Snowflake sync skipped -- no credentials. Pipeline FAILING.")
    raise RuntimeError("Snowflake sync failed: no credentials. Pipeline cannot succeed without Snowflake sync.")

# COMMAND ----------

# DBTITLE 1,Update Batch Log
# MAGIC %md
# MAGIC #### 5. Update Batch Log
# MAGIC
# MAGIC Marks the Snowflake sync as complete in the batch control table
# MAGIC (`HMDM_DEV.util.ctl_batch_log_tbl`) by setting
# MAGIC `snowflake_sync_status = 'Y'` for all batches where `egress_status = 'Y'`.

# COMMAND ----------

# DBTITLE 1,Update Batch Log
# ============================================================
# UPDATE BATCH LOG
# ============================================================
# Mark Snowflake sync as complete in the batch control table.

if creds and results:
    # Check if any tables had errors before marking sync as complete.
    has_errors = False
    for layer, tables in results.items():
        for table, result in tables.items():
            if "error" in result:
                has_errors = True
                break
    
    if has_errors:
        print("WARNING: Some tables failed to sync -- snowflake_sync_status NOT set to 'Y'")
        print("    Re-run after fixing the errors above.")
    else:
        try:
            existing_cols = spark.sql(f"DESCRIBE TABLE {catalog}.util.ctl_batch_log_tbl").collect()
            col_names = [row.col_name for row in existing_cols]
            
            if 'snowflake_sync_status' not in col_names:
                spark.sql(f"""
                    ALTER TABLE {catalog}.util.ctl_batch_log_tbl
                    ADD COLUMNS (snowflake_sync_status STRING)
                """)
                print("Added snowflake_sync_status column")
            
            spark.sql(f"""
                UPDATE {catalog}.util.ctl_batch_log_tbl
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
# MAGIC Connects to Snowflake and verifies all synced tables by comparing
# MAGIC Snowflake row counts against Databricks source row counts.
# MAGIC
# MAGIC This cell runs AFTER the sync is complete. It connects to Snowflake
# MAGIC using the same credentials from AWS Secrets Manager, then for each
# MAGIC table:
# MAGIC * Queries the Databricks source count (`SELECT COUNT(*)`)
# MAGIC * Queries the Snowflake target count (`SELECT COUNT(*)`)
# MAGIC * Compares the two: PASS if they match, MISMATCH if they differ
# MAGIC
# MAGIC Tables verified:
# MAGIC * STAGING: 22 tables (13 HCP + 9 HCO)
# MAGIC * MASTER: 10 tables (5 HCP + 5 HCO, including HCP main table)
# MAGIC * Total: 32 tables
# MAGIC
# MAGIC No hard-coded expected row counts are used -- the comparison is
# MAGIC always Databricks source count vs Snowflake target count.
# MAGIC
# MAGIC If any verification errors or mismatches occur, the pipeline FAILS
# MAGIC immediately -- dbt must not build models on inconsistent data.

# COMMAND ----------

# DBTITLE 1,Verify Snowflake Data
# ============================================================
# VERIFY SNOWFLAKE DATA
# ============================================================
# Connect to Snowflake and verify all synced tables by comparing
# Snowflake row counts against Databricks source row counts.
import snowflake.connector
import pandas as pd

creds = get_snowflake_credentials()

if creds:
    sf_db = creds.get('database', catalog)
    conn = snowflake.connector.connect(
        user=creds['user'],
        password=creds['password'],
        account=creds['account'],
        role=creds.get('role', ''),
        warehouse=creds.get('warehouse', 'COMPUTE_WH'),
        database=sf_db,
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
        dbx_table = tbl.lower()
        try:
            dbx_cnt = spark.sql(f"SELECT COUNT(*) FROM {catalog}.staging.{dbx_table}").collect()[0][0]
        except Exception:
            dbx_cnt = -1
        try:
            cur.execute(f"SELECT COUNT(*) FROM {sf_db}.STAGING.{tbl}")
            sf_cnt = cur.fetchone()[0]
            staging_total += sf_cnt
            if dbx_cnt < 0:
                status = "SKIP"
                print(f"  {status} STAGING.{tbl}: {sf_cnt} rows (source table missing in Databricks)")
            elif sf_cnt == dbx_cnt:
                status = "PASS"
                print(f"  {status} STAGING.{tbl}: {sf_cnt} rows (source={dbx_cnt})")
            else:
                status = "MISMATCH"
                print(f"  {status} STAGING.{tbl}: Snowflake={sf_cnt} vs Databricks={dbx_cnt}")
            entity = "HCP" if tbl.startswith("HCP") else "HCO"
            staging_results.append((entity, tbl, sf_cnt, dbx_cnt, status))
        except Exception as e:
            print(f"  ERROR: STAGING.{tbl}: {e}")
            staging_results.append(("ERR", tbl, -1, dbx_cnt, "ERROR"))

    print(f"  Total STAGING rows (Snowflake): {staging_total}")

    # --- 2. MASTER row counts ---
    master_tables = [
        'HCP', 'HCP_SPECIALTY', 'HCP_ALTERNATE_NAME', 'HCP_EDUCATION', 'HCP_IDENTIFICATION',
        'HCO', 'HCO_NAME', 'HCO_ALTERNATE_IDENTIFIER', 'HCO_PHONE', 'HCO_SPECIALTY',
    ]

    print("\n" + "=" * 60)
    print("SNOWFLAKE VERIFICATION: MASTER TABLES")
    print("=" * 60)
    master_total = 0
    master_results = []
    for tbl in master_tables:
        dbx_table = tbl.lower()
        try:
            dbx_cnt = spark.sql(f"SELECT COUNT(*) FROM {catalog}.master.{dbx_table}").collect()[0][0]
        except Exception:
            dbx_cnt = -1
        try:
            cur.execute(f"SELECT COUNT(*) FROM {sf_db}.MASTER.{tbl}")
            sf_cnt = cur.fetchone()[0]
            master_total += sf_cnt
            if dbx_cnt < 0:
                status = "SKIP"
                print(f"  {status} MASTER.{tbl}: {sf_cnt} rows (source table missing in Databricks)")
            elif sf_cnt == dbx_cnt:
                status = "PASS"
                print(f"  {status} MASTER.{tbl}: {sf_cnt} rows (source={dbx_cnt})")
            else:
                status = "MISMATCH"
                print(f"  {status} MASTER.{tbl}: Snowflake={sf_cnt} vs Databricks={dbx_cnt}")
            entity = "HCP" if tbl.startswith("HCP") else "HCO"
            master_results.append((entity, tbl, sf_cnt, dbx_cnt, status))
        except Exception as e:
            print(f"  ERROR: MASTER.{tbl}: {e}")
            master_results.append(("ERR", tbl, -1, dbx_cnt, "ERROR"))

    print(f"  Total MASTER rows (Snowflake): {master_total}")

    # --- 3. Summary ---
    total_tables = len(staging_results) + len(master_results)
    total_rows = staging_total + master_total
    errors = sum(1 for r in staging_results + master_results if r[4] in ("ERROR",))
    mismatches = sum(1 for r in staging_results + master_results if r[4] == "MISMATCH")

    print("\n" + "=" * 60)
    print("SNOWFLAKE VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"  Tables checked:  {total_tables}")
    print(f"  Total rows:      {total_rows}")
    print(f"  Errors:          {errors}")
    print(f"  Mismatches:      {mismatches}")
    print(f"  STAGING rows:    {staging_total}")
    print(f"  MASTER rows:     {master_total}")
    if errors == 0 and mismatches == 0:
        print("  VERIFICATION PASSED")
    else:
        print("  ERROR: VERIFICATION FAILED -- check errors/mismatches above")
    print("=" * 60)
    
    if errors > 0 or mismatches > 0:
        error_msg = f"Snowflake verification FAILED: {errors} errors, {mismatches} mismatches"
        raise RuntimeError(error_msg)

    cur.close()
    conn.close()
else:
    print("WARNING: No Snowflake credentials -- cannot verify")