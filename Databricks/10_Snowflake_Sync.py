# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
# Install Snowflake connector (required on Serverless — not pre-installed)
# restartPython() runs BEFORE any variable definitions, so state loss is harmless.
%pip install snowflake-connector-python --quiet
dbutils.library.restartPython()

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
# ============================================================
# CHECK SNOWFLAKE CREDENTIALS
# ============================================================
# Verify that Snowflake connection is configured before attempting sync.
# If credentials are missing, the sync is skipped gracefully.

creds = get_snowflake_credentials()

if creds:
    print("✅ Snowflake credentials found — proceeding with sync")
else:
    print("⚠️  Snowflake credentials NOT configured.")
    print("    Check AWS Secrets Manager secret 'healthcare-mdm/dev/api-snowflake'")
    print("    and Databricks service credential 'healthcare_mdm_secrets_credential'.")

# COMMAND ----------

# DBTITLE 1,Run Snowflake Sync
# ============================================================
# RUN SNOWFLAKE SYNC
# ============================================================
# Push Databricks staging + master tables to Snowflake.
# This is the bridge that fills Snowflake's empty tables
# so dbt models and Snowpark can operate on real data.

if creds:
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
else:
    print("⏭️  Sync skipped — no Snowflake credentials")
    results = {}

# COMMAND ----------

# DBTITLE 1,Update Batch Log
# ============================================================
# UPDATE BATCH LOG
# ============================================================
# Mark Snowflake sync as complete in the batch control table.
# This adds a new column 'snowflake_sync_status' to track sync state.

if creds and results:
    try:
        # Add snowflake_sync_status column if it doesn't exist
        spark.sql("""
            ALTER TABLE HMDM_DEV.util.ctl_batch_log_tbl
            ADD COLUMN IF NOT EXISTS snowflake_sync_status STRING
        """)
        
        # Update latest batch
        spark.sql("""
            UPDATE HMDM_DEV.util.ctl_batch_log_tbl
            SET snowflake_sync_status = 'Y'
            WHERE egress_status = 'Y'
              AND coalesce(snowflake_sync_status, 'N') != 'Y'
        """)
        print("✅ Batch log updated: snowflake_sync_status = 'Y'")
    except Exception as e:
        print(f"⚠️  Could not update batch log: {e}")
else:
    print("⏭️  Batch log update skipped")

# COMMAND ----------

# DBTITLE 1,Complete
print("\n" + "=" * 60)
print("STAGE 7: SNOWFLAKE SYNC COMPLETE")
print("=" * 60)