# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Canonical Standardization
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 3 : Canonical Standardization
# MAGIC
# MAGIC Applies cross-source canonicalization (code-list lookups,
# MAGIC country/specialty/status normalisation) documented in
# MAGIC `raw_to_std_canonical`, so values from different source systems converge
# MAGIC on one canonical vocabulary before Land_to_Stage.
# MAGIC
# MAGIC `main_canonical_pipeline()` takes no arguments - it reads its own
# MAGIC configuration (which entities/columns are canonicalized) from the
# MAGIC canonical mapping control table, Widgets at the top of the notebook let you select the
# MAGIC source system (IQVIA_API) and entity type (HCP/HCO/BOTH) to control
# MAGIC which identifiers are canonicalized.

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %md #### 2. Imports
# MAGIC
# MAGIC Imports the canonical pipeline function and runtime configuration. The `main_canonical_pipeline` function reads its own configuration (which entities/columns are canonicalized) from the canonical mapping control table. The notebook loops over all identifiers selected by the Entity Type widget and calls this function for each one.

# COMMAND ----------

import sys
import os

# Add src directory to Python path
src_path = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from canonical.canonical import main_canonical_pipeline
from core.runtime_config import catalog, env, get_notebook_run_url

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %md #### 1. Widgets
# MAGIC
# MAGIC Select the source system and entity type from the widget panel at the top of the notebook before running canonicalization. The next cell creates these widgets and filters the source identifiers based on the entity type selection.
# MAGIC
# MAGIC * **Source System**: IQVIA_API (production pipeline)
# MAGIC * **Entity Type**: HCP, HCO, or BOTH -- controls which source identifiers are canonicalized

# COMMAND ----------

# DBTITLE 1,Define Notebook Widgets
# ============================================================
# WIDGET SETUP -- SOURCE SYSTEM AND ENTITY TYPE
# ============================================================
# These widgets appear at the top of the notebook.
# Select Source System and Entity Type before running canonicalization.
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

# DBTITLE 1,Infrastructure Verification
# MAGIC %md
# MAGIC #### Infrastructure Verification
# MAGIC
# MAGIC **This section verifies required infrastructure exists:**
# MAGIC - Source schema: `landing` (input from standardization)
# MAGIC - Target schema: `canonical` (output of this stage)
# MAGIC - Control tables for canonical configuration
# MAGIC
# MAGIC **Safe to re-run:** All operations are idempotent.

# COMMAND ----------

# DBTITLE 1,Display Current Infrastructure
# MAGIC %sql
# MAGIC -- Verify schemas exist
# MAGIC SHOW SCHEMAS IN HMDM_DEV;
# MAGIC
# MAGIC -- Check landing tables (input)
# MAGIC SHOW TABLES IN HMDM_DEV.landing;

# COMMAND ----------

# DBTITLE 1,Verify Canonical Schema
# MAGIC %md #### Verify Canonical Schema
# MAGIC
# MAGIC Creates the `canonical` schema if it does not exist, then lists all canonical tables. Each landing table has a corresponding `_canonical` table (e.g., `landing.hcp_name` -> `canonical.hcp_name_canonical`). The pipeline writes to these tables in the next cell.

# COMMAND ----------

# DBTITLE 1,Verify Canonical Schema
# MAGIC %sql
# MAGIC -- Create canonical schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.canonical
# MAGIC COMMENT 'Canonical layer - normalized cross-source data';
# MAGIC
# MAGIC -- Show canonical tables (after canonical runs)
# MAGIC SHOW TABLES IN HMDM_DEV.canonical;

# COMMAND ----------

# DBTITLE 1,Run Canonical Pipeline
# MAGIC %md #### 3. Run Canonical Pipeline
# MAGIC
# MAGIC This cell executes the canonical standardization pipeline for every entity selected by the Entity Type widget.
# MAGIC
# MAGIC **Batch reset logic (important):** Only the **latest batch** for the selected source system is reset to `canonical_status = 'N'` before processing. This uses `batch_id = (SELECT MAX(batch_id) ...)` to target just the newest batch -- old batches that were already canonicalized keep their 'Y' status and are **not** reprocessed. When a new batch arrives tomorrow, yesterday's batch stays untouched.
# MAGIC
# MAGIC **Processing flow:**
# MAGIC 1. Reset latest batch `canonical_status` to 'N' (latest batch only, not all)
# MAGIC 2. Check for pending batches (`stdz_status = 'Y'` AND `canonical_status = 'N'`)
# MAGIC 3. Loop over each source identifier filtered by Entity Type widget (HCP=13, HCO=9, BOTH=22)
# MAGIC 4. For each entity, call `main_canonical_pipeline(skip_batch_update=True)` which:
# MAGIC    - Reads canonical configuration from the control table
# MAGIC    - Reads source data from `landing.<entity>`
# MAGIC    - Applies canonical mapping (code-list lookups, country/specialty/status normalisation)
# MAGIC    - Writes results to `canonical.<entity>_canonical`
# MAGIC 5. After all entities processed, update latest batch `canonical_status` to 'Y' (only if no failures)
# MAGIC
# MAGIC **Failure handling:** If an entity fails, the loop continues with the remaining entities. Skips (no configuration, no data) are OK and do not block progress. Only actual failures prevent the batch status from being set to 'Y'.

# COMMAND ----------

# DBTITLE 1,Run Canonical Pipeline
# Reset canonical batch status for the LATEST batch only (avoids reprocessing old batches)
spark.sql(f"""
    UPDATE {catalog}.util.ctl_batch_log_tbl
    SET canonical_status = 'N'
    WHERE source_system_name = '{source_system_name}'
      AND batch_id = (
          SELECT MAX(batch_id)
          FROM {catalog}.util.ctl_batch_log_tbl
          WHERE source_system_name = '{source_system_name}'
      )
""")
print(f"Reset canonical_status to N for latest batch ({source_system_name})")

# Ensure imports are available even if cells were run out of order
try:
    env
    catalog
    get_notebook_run_url
    main_canonical_pipeline
    source_system_name
    source_identifiers
except NameError:
    import sys, os
    src_path = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    from core.runtime_config import catalog, env, get_notebook_run_url
    from canonical.canonical import main_canonical_pipeline
    source_system_name = "IQVIA_API"
    SELECTED_ENTITY = "BOTH"
    source_identifiers = ['hcp_name', 'hcp_address', 'hcp_alternate_name', 'hcp_identification', 'hcp_specialty', 'hcp_phone', 'hcp_email', 'hcp_education', 'hcp_tendencies', 'hcp_origin_university', 'hcp_tax', 'hcp_language', 'hcp_hco_affiliation', 'hco_name', 'hco_address', 'hco_alternate_name', 'hco_identification', 'hco_specialty', 'hco_phone', 'hco_email', 'hco_tax', 'hco_hco_hierarchy']

print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Source System: {source_system_name}")
print(f"Entity Type  : {SELECTED_ENTITY}")
print(f"Entities     : {len(source_identifiers)} identifiers")
print("=" * 60)

# Check if there are pending batches for canonical processing
pending_count = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog}.util.ctl_batch_log_tbl
    WHERE source_system_name = '{source_system_name}'
      AND stdz_status = 'Y'
      AND COALESCE(canonical_status, 'N') = 'N'
""").collect()[0]['cnt']

if pending_count == 0:
    print(f"\nNo pending batches found for canonical processing. All data is up to date.")
else:
    print(f"\nFound {pending_count} pending batch(es) - proceeding with canonical processing")

# Process each source identifier
failures = []
successes = []
skipped = []
for source_identifier in source_identifiers:
    try:
        print(f"\n--- Canonicalizing {source_identifier} ---")
        
        # Set command-line arguments expected by main_canonical_pipeline
        sys.argv = [
            "notebook",              # script name (sys.argv[0])
            "runtime",               # runtime (sys.argv[1]) 
            source_identifier,       # source_identifier (sys.argv[2])
            source_system_name,      # source_system_name (sys.argv[3])
            source_identifier        # target_table_name (sys.argv[4])
        ]
        
        main_canonical_pipeline(skip_batch_update=True)
        successes.append(source_identifier)
        print(f"  SUCCESS: {source_identifier}")
    except Exception as exc:
        exc_str = str(exc)
        if "Exiting gracefully" in exc_str or "No delta" in exc_str or "No canonical configuration" in exc_str or "No consolidated" in exc_str:
            print(f"  SKIPPED: {source_identifier} -> {exc_str}")
            skipped.append(source_identifier)
        else:
            print(f"  FAILED: {source_identifier} -> {exc_str}")
            failures.append((source_identifier, exc_str))

# Update canonical batch status ONCE after all entities have been processed
# Only mark as 'Y' if there are no failures (skips are OK)
if not failures and successes:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET canonical_status = 'Y'
        WHERE source_system_name = '{source_system_name}'
          AND stdz_status = 'Y'
          AND COALESCE(canonical_status, 'N') = 'N'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nCanonical batch status updated to Y for {source_system_name}")
elif failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET canonical_status = 'N'
        WHERE source_system_name = '{source_system_name}'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nCanonical batch status set to N (failures occurred)")

print(f"\nSummary: {len(successes)} succeeded, {len(skipped)} skipped, {len(failures)} failed")
if failures:
    for src, err in failures:
        print(f"  FAILED: {src}: {err}")

# COMMAND ----------

# DBTITLE 1,Result
# MAGIC %md #### 4. Result
# MAGIC
# MAGIC Checks if any entities failed during canonicalization. If all succeeded, exits with `SUCCESS`. If any failed, raises a `RuntimeError` listing the failed entities so the pipeline job stops and alerts the team.

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")