# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 2 : Raw -> Landing Standardization
# MAGIC
# MAGIC Applies the field-level standardization rules documented in the
# MAGIC `Raw_to_Land` sheet: Source_FK / surrogate-key generation, trimming,
# MAGIC CASE/lookup standardization, and the common Source_Name /
# MAGIC Source_Created_Date / Source_Updated_Date audit columns, for every RAW
# MAGIC table, writing the result to the LANDING layer.

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %md #### 1. Widgets
# MAGIC
# MAGIC Select the source system and entity type from the widget panel at the top of the notebook before running standardization. The next cell creates these widgets and filters the source identifiers based on the entity type selection.
# MAGIC
# MAGIC * **Source System**: IQVIA_API (production pipeline)
# MAGIC * **Entity Type**: HCP, HCO, or BOTH -- controls which source identifiers are standardized

# COMMAND ----------

# DBTITLE 1,Widget Setup
# ============================================================
# WIDGET SETUP -- SOURCE SYSTEM AND ENTITY TYPE
# ============================================================
# These widgets appear at the top of the notebook.
# Select Source System and Entity Type before running standardization.
# Entity Type controls which source identifiers are processed:
#   HCP  -> only hcp_* identifiers
#   HCO  -> only hco_* identifiers
#   BOTH -> all identifiers
# ============================================================

# Remove old widgets from previous notebook versions
try:
    dbutils.widgets.remove("source_identifiers")
except Exception:
    pass

# Create dropdown widgets
dbutils.widgets.dropdown("source_system_name", "IQVIA_API", ["IQVIA_API"], "Source System")
dbutils.widgets.dropdown("entity_type", "BOTH", ["HCP", "HCO", "BOTH"], "Entity Type")

# Read widget values
source_system_name = dbutils.widgets.get("source_system_name")
SELECTED_ENTITY = dbutils.widgets.get("entity_type")

# All configured source identifiers (full set)
ALL_IDENTIFIERS = [
    'hcp_name', 'hcp_address', 'hcp_alternate_name', 'hcp_identification',
    'hcp_specialty', 'hcp_phone', 'hcp_email', 'hcp_education',
    'hcp_tendencies', 'hcp_origin_university', 'hcp_tax', 'hcp_language',
    'hcp_hco_affiliation',
    'hco_name', 'hco_address', 'hco_alternate_name', 'hco_identification',
    'hco_specialty', 'hco_phone', 'hco_email', 'hco_tax', 'hco_hco_hierarchy',
]

# Filter identifiers by entity type
if SELECTED_ENTITY == "HCP":
    source_identifiers = [s for s in ALL_IDENTIFIERS if s.startswith("hcp_")]
elif SELECTED_ENTITY == "HCO":
    source_identifiers = [s for s in ALL_IDENTIFIERS if s.startswith("hco_")]
else:
    source_identifiers = ALL_IDENTIFIERS

print(f"Source System : {source_system_name}")
print(f"Entity Type   : {SELECTED_ENTITY}")
print(f"Identifiers   : {len(source_identifiers)} entities")
for sid in source_identifiers:
    print(f"  - {sid}")

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %md #### 2. Imports
# MAGIC
# MAGIC Imports the standardization pipeline function and runtime configuration. The `main_standardization_pipeline` function processes one source identifier at a time -- the notebook loops over all selected identifiers based on the Entity Type widget.

# COMMAND ----------

import sys
import os
import importlib

# Add src directory to Python path
src_path = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Force reload to pick up latest changes
import standardization.standardization
importlib.reload(standardization.standardization)

from standardization.standardization import main_standardization_pipeline
from core.runtime_config import catalog, env, get_notebook_run_url

# COMMAND ----------

# DBTITLE 1,Infrastructure Verification
# MAGIC %md
# MAGIC #### 2.5 Infrastructure Verification
# MAGIC
# MAGIC **This section verifies required infrastructure exists:**
# MAGIC - Source schema: `raw` (input from previous stage)
# MAGIC - Target schema: `landing` (output of this stage)
# MAGIC - Control tables for batch tracking
# MAGIC
# MAGIC **Safe to re-run:** All operations are idempotent.

# COMMAND ----------

# DBTITLE 1,Display Current Infrastructure
# MAGIC %sql
# MAGIC -- Verify schemas exist
# MAGIC SHOW SCHEMAS IN HMDM_DEV;
# MAGIC
# MAGIC -- Check if raw schema has tables
# MAGIC SHOW TABLES IN HMDM_DEV.raw;

# COMMAND ----------

# DBTITLE 1,Verify Landing Schema
# MAGIC %sql
# MAGIC -- Create landing schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.landing
# MAGIC COMMENT 'Landing layer - standardized format data';
# MAGIC
# MAGIC -- Show landing tables (after standardization runs)
# MAGIC SHOW TABLES IN HMDM_DEV.landing;

# COMMAND ----------

# DBTITLE 1,Run Standardization
# MAGIC %md #### 3. Run standardization
# MAGIC
# MAGIC This cell executes the standardization pipeline for every entity selected by the Entity Type widget.
# MAGIC
# MAGIC **Batch reset logic (important):** Only the **latest batch** for the selected source system is reset to `stdz_status = 'N'` before processing. This uses `batch_id = (SELECT MAX(batch_id) ...)` to target just the newest batch -- old batches that were already standardized keep their 'Y' status and are **not** reprocessed. When a new batch arrives tomorrow, yesterday's batch stays untouched.
# MAGIC
# MAGIC **Processing flow:**
# MAGIC 1. Reset latest batch `stdz_status` to 'N' (latest batch only, not all)
# MAGIC 2. Loop over each source identifier filtered by Entity Type widget (HCP=13, HCO=9, BOTH=22)
# MAGIC 3. For each entity, call `main_standardization_pipeline(source_identifier, source_system_name, tbl_nm)` which:
# MAGIC    - Reads raw data from `raw.<entity>`
# MAGIC    - Applies standardization rules (deduplication, type casting, null handling)
# MAGIC    - Writes results to `landing.<entity>`
# MAGIC 4. After all entities processed, update latest batch `stdz_status` to 'Y' (only if no failures)
# MAGIC
# MAGIC **Failure handling:** If an entity fails, the loop continues with the remaining entities. Only actual failures prevent the batch status from being set to 'Y'.

# COMMAND ----------

# DBTITLE 1,Run Standardization Pipeline
# Re-read widget values (SQL cells between may clear Python namespace)
source_system_name = dbutils.widgets.get("source_system_name")
SELECTED_ENTITY = dbutils.widgets.get("entity_type")

# Rebuild source_identifiers if not in scope
if 'source_identifiers' not in dir():
    ALL_IDENTIFIERS = [
        'hcp_name', 'hcp_address', 'hcp_alternate_name', 'hcp_identification',
        'hcp_specialty', 'hcp_phone', 'hcp_email', 'hcp_education',
        'hcp_tendencies', 'hcp_origin_university', 'hcp_tax', 'hcp_language',
        'hcp_hco_affiliation',
        'hco_name', 'hco_address', 'hco_alternate_name', 'hco_identification',
        'hco_specialty', 'hco_phone', 'hco_email', 'hco_tax', 'hco_hco_hierarchy',
    ]
    if SELECTED_ENTITY == "HCP":
        source_identifiers = [s for s in ALL_IDENTIFIERS if s.startswith("hcp_")]
    elif SELECTED_ENTITY == "HCO":
        source_identifiers = [s for s in ALL_IDENTIFIERS if s.startswith("hco_")]
    else:
        source_identifiers = ALL_IDENTIFIERS

print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Source      : {source_system_name}")
print(f"Entity Type : {SELECTED_ENTITY}")
print(f"Entities    : {len(source_identifiers)} identifiers")
print("=" * 60)

# Reset stdz_status for the LATEST batch only (avoids reprocessing old batches)
spark.sql(f"""
    UPDATE {catalog}.util.ctl_batch_log_tbl
    SET stdz_status = 'N'
    WHERE source_system_name = '{source_system_name}'
      AND batch_id = (
          SELECT MAX(batch_id)
          FROM {catalog}.util.ctl_batch_log_tbl
          WHERE source_system_name = '{source_system_name}'
      )
""")
print(f"Reset stdz_status to N for latest batch ({source_system_name})")

failures = []
successes = []
skips = []
for source_identifier in source_identifiers:
    try:
        print(f"\n--- Standardizing {source_identifier} ---")
        result = main_standardization_pipeline(
            source_identifier=source_identifier,
            source_system_name=source_system_name,
            tbl_nm=source_identifier,
            skip_batch_update=True,
        )
        print(result)
        successes.append(source_identifier)
    except Exception as exc:  # noqa: BLE001
        error_msg = str(exc)
        if 'DELTA_TABLE_NOT_FOUND' in error_msg or "doesn't exist" in error_msg:
            print(f"SKIP: {source_identifier} -> source RAW table not found (expected for mock data gaps)")
            skips.append(source_identifier)
        else:
            print(f"FAILED: {source_identifier} -> {exc}")
            failures.append((source_identifier, error_msg))

# Update batch status ONCE after all entities have been processed
# Only mark as 'Y' if there are no failures
if not failures and successes:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET stdz_status = 'Y'
        WHERE source_system_name = '{source_system_name}'
          AND COALESCE(stdz_status, 'N') = 'N'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nBatch status updated to Y for {source_system_name} ({len(successes)} entities processed)")
elif failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET stdz_status = 'N'
        WHERE source_system_name = '{source_system_name}'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nBatch status set to N (failures occurred)")

print(f"\nSummary: {len(successes)} succeeded, {len(failures)} failed, {len(skips)} skipped")
if failures:
    for src, err in failures:
        print(f"  FAILED: {src}: {err}")
if skips:
    for src in skips:
        print(f"  SKIPPED: {src} (RAW table missing -- mock data gap)")

# COMMAND ----------

# DBTITLE 1,Result
# MAGIC %md #### 4. Result
# MAGIC
# MAGIC Checks if any entities failed during standardization. If all succeeded, exits with SUCCESS. If any failed, raises a RuntimeError listing the failed entities.

# COMMAND ----------

if failures:
    raise RuntimeError(f"Standardization failed for {len(failures)} tables: {failures}")

dbutils.notebook.exit("SUCCESS")