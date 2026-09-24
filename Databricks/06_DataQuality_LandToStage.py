# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 4 : Land -> Stage Data Quality
# MAGIC
# MAGIC Applies every configured DQ rule from the `Land_to_Stag` sheet
# MAGIC (null_check, name_address_completeness_check, address_mdr_check,
# MAGIC mdr_check, affiliation_mdr_check, hierarchy_mdr_check) to each LANDING
# MAGIC table, writing passing rows to STAGING and rejected rows to the DQ
# MAGIC reject table. See `src/dq/data_quality.py::DQ_RULES` for the full,
# MAGIC one-row-per-Land_to_Stag-entry rule list.

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %md #### 1. Widgets
# MAGIC
# MAGIC Select the source system and entity type from the widget panel at the top of the notebook before running DQ validation. The next cell creates these widgets and filters the source tables based on the entity type selection.
# MAGIC
# MAGIC * **Source System**: IQVIA_API (production pipeline)
# MAGIC * **Entity Type**: HCP, HCO, or BOTH — controls which landing tables are validated
# MAGIC * **Source Identifier**: DQ config identifier for control-table rule lookup (default: IQVIA_HMDM)
# MAGIC * **Batch ID**: Optional — blank means latest pending batch

# COMMAND ----------

# DBTITLE 1,Widget Setup
# ============================================================
# WIDGET SETUP — SOURCE SYSTEM AND ENTITY TYPE
# ============================================================
# These widgets appear at the top of the notebook.
# Select Source System and Entity Type before running DQ validation.
# Entity Type controls which landing tables are validated:
#   HCP  → only hcp_* tables
#   HCO  → only hco_* tables
#   BOTH → all tables with configured DQ rules
# source_identifier and batch_id are DQ-specific config widgets.
# ============================================================

# Remove old widgets from previous notebook versions
try:
    dbutils.widgets.remove("source_identifiers")
except Exception:
    pass

# Create dropdown widgets
dbutils.widgets.dropdown("source_system_name", "IQVIA_API", ["IQVIA_API"], "Source System")
dbutils.widgets.dropdown("entity_type", "BOTH", ["HCP", "HCO", "BOTH"], "Entity Type")

# DQ-specific config widgets (kept as text — not entity type or source system)
dbutils.widgets.text("source_identifier", "IQVIA_HMDM", "Source config identifier (for DQ rule lookup)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = latest pending batch)")

# Read widget values
source_system_name = dbutils.widgets.get("source_system_name")
SELECTED_ENTITY = dbutils.widgets.get("entity_type")
source_identifier = dbutils.widgets.get("source_identifier")
batch_id_param = dbutils.widgets.get("batch_id")
batch_id = int(batch_id_param) if batch_id_param.strip() else None

# All configured source identifiers (for display)
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
print(f"Batch ID      : {batch_id or 'latest'}")
for sid in source_identifiers:
    print(f"  - {sid}")

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %md #### 2. Imports
# MAGIC
# MAGIC Imports the DQ pipeline function and runtime configuration. The `main_data_quality_pipeline` function runs every configured DQ rule for one LANDING table and writes passing rows to STAGING and rejected rows to the DQ reject table. The notebook loops over all tables selected by the Entity Type widget.

# COMMAND ----------

import sys
import os
import importlib

# Add src directory to Python path
src_path = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Force reload to pick up latest code changes
import dq.data_quality
import core.runtime_config
importlib.reload(dq.data_quality)
importlib.reload(core.runtime_config)

from dq.data_quality import main_data_quality_pipeline, get_rules_for_source, get_rules
from core.runtime_config import catalog, env, get_notebook_run_url

# COMMAND ----------

# DBTITLE 1,Infrastructure Verification
# MAGIC %md
# MAGIC     #### 2.5 Infrastructure Verification
# MAGIC
# MAGIC Verifies the `landing` schema (input) and `staging` schema (output) exist. All operations are idempotent.

# COMMAND ----------

# DBTITLE 1,Display Current Infrastructure
# MAGIC %sql
# MAGIC -- Verify schemas exist
# MAGIC SHOW SCHEMAS IN HMDM_DEV;
# MAGIC
# MAGIC -- Check landing tables (input)
# MAGIC SHOW TABLES IN HMDM_DEV.landing;

# COMMAND ----------

# DBTITLE 1,Verify Staging Schema
# MAGIC %md #### Verify Staging Schema
# MAGIC
# MAGIC Creates the `staging` schema if it does not exist, then lists all staging tables.

# COMMAND ----------

# DBTITLE 1,Verify Staging Schema
# MAGIC %sql
# MAGIC -- Create staging schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.staging
# MAGIC COMMENT 'Staging layer - data quality validated records';
# MAGIC
# MAGIC -- Show staging tables (after DQ runs)
# MAGIC SHOW TABLES IN HMDM_DEV.staging;

# COMMAND ----------

# DBTITLE 1,Run DQ Validation
# MAGIC %md #### 3. Run DQ validation
# MAGIC
# MAGIC This cell executes the data quality pipeline for every table selected by the Entity Type widget.
# MAGIC
# MAGIC **Batch reset logic (important):** Only the **latest batch** for the selected source system is reset to `dq_status = 'N'` before processing. This uses `batch_id = (SELECT MAX(batch_id) ...)` to target just the newest batch — old batches that were already DQ-validated keep their 'Y' status and are **not** reprocessed. When a new batch arrives tomorrow, yesterday's batch stays untouched.
# MAGIC
# MAGIC **Processing flow:**
# MAGIC 1. Reset latest batch `dq_status` to 'N' (latest batch only, not all)
# MAGIC 2. Get all DQ rules from `get_rules()` and filter by Entity Type widget (HCP=13, HCO=9, BOTH=22)
# MAGIC 3. For each table, call `main_data_quality_pipeline(source_identifier, source_table, ...)` which:
# MAGIC    - Reads data from `landing.<entity>`
# MAGIC    - Applies DQ rules (null_check, name_address_completeness, MDR checks)
# MAGIC    - Writes passing rows to `staging.<entity>` (overwrite mode)
# MAGIC    - Returns rejected rows for DQ reject table
# MAGIC 4. After all tables processed, update latest batch `dq_status` to 'Y' (only if no failures)
# MAGIC
# MAGIC **Failure handling:** If a table fails, the loop continues with the remaining tables. Missing tables (upstream not run yet) are separated from actual errors in the result cell.

# COMMAND ----------

# DBTITLE 1,Run DQ Pipeline
print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Source      : {source_system_name}")
print(f"Entity Type : {SELECTED_ENTITY}")
print(f"Entities    : {len(source_identifiers)} identifiers")
print("=" * 60)

# Get bare table names from DQ rules
bare_source_tables = sorted({rule.source_table for rule in get_rules()})
# Filter by entity type
if SELECTED_ENTITY == "HCP":
    bare_source_tables = [t for t in bare_source_tables if t.startswith("hcp_")]
elif SELECTED_ENTITY == "HCO":
    bare_source_tables = [t for t in bare_source_tables if t.startswith("hco_")]
print(f"Source tables with configured DQ rules ({SELECTED_ENTITY}): {bare_source_tables}")

# Construct fully qualified table names.
# DQ reads from CANONICAL layer (output of 05_Canonical_Standardization)
# and writes passing rows to STAGING layer.
source_schema = f"{catalog}.canonical"
staging_schema = f"{catalog}.staging"

# Reference table mapping: each entity -> its parent/reference table for DQ checks
# (name tables reference their address table for completeness check;
#  address tables reference name tables for MDR check;
#  child tables reference their name table for MDR check)
REFERENCE_TABLE_MAP = {
    "hcp_name":              "hcp_address",
    "hco_name":              "hco_address",
    "hcp_address":           "hcp_name",
    "hco_address":           "hco_name",
    "hcp_email":              "hcp_name",
    "hcp_alternate_name":     "hcp_name",
    "hcp_identification":     "hcp_name",
    "hcp_specialty":           "hcp_name",
    "hcp_phone":              "hcp_name",
    "hcp_education":           "hcp_name",
    "hcp_origin_university":   "hcp_name",
    "hcp_tax":                "hcp_name",
    "hcp_tendencies":          "hcp_name",
    "hcp_language":            "hcp_name",
    "hco_email":              "hco_name",
    "hco_alternate_name":     "hco_name",
    "hco_identification":     "hco_name",
    "hco_specialty":           "hco_name",
    "hco_phone":              "hco_name",
    "hco_tax":                "hco_name",
    "hcp_hco_affiliation":    "hcp_name",
    "hco_hco_hierarchy":       "hco_name",
}

# DQ metadata columns added by the pipeline (must be dropped before staging write)
DQ_METADATA_COLS = ["DQ_RULE", "DQ_STATUS", "DQ_DESCRIPTION", "DQ_APPL_COLUMN", "DQ_PROCESSED_AT"]

# Reset DQ batch status to 'N' for this source system so we can reprocess
spark.sql(f"""
    UPDATE {catalog}.util.ctl_batch_log_tbl
    SET dq_status = 'N'
    WHERE source_system_name = '{source_system_name}'
      AND batch_id = (
          SELECT MAX(batch_id)
          FROM {catalog}.util.ctl_batch_log_tbl
          WHERE source_system_name = '{source_system_name}'
      )
""")
print(f"DQ batch status reset to 'N' for latest batch ({source_system_name})")

failures = []
succeeded = 0
for bare_table in bare_source_tables:
    # Construct fully qualified source and target table names (from CANONICAL)
    # Canonical tables have _canonical suffix (e.g., canonical.hcp_name_canonical)
    source_table_fqn = f"{source_schema}.{bare_table}_canonical"
    target_table_fqn = f"{staging_schema}.{bare_table}"
    
    # Resolve reference table for this entity
    ref_bare = REFERENCE_TABLE_MAP.get(bare_table)
    ref_table_fqn = f"{source_schema}.{ref_bare}_canonical" if ref_bare else None
    
    try:
        print(f"\n--- DQ: {source_table_fqn} -> {target_table_fqn} (ref={ref_table_fqn}) ---")
        passed_df, rejected_df = main_data_quality_pipeline(
            source_identifier=source_identifier,
            source_table=source_table_fqn,
            source_system_name=source_system_name,
            reference_table=ref_table_fqn,
            batch_id=batch_id,
            skip_batch_update=True,
        )
        passed_count = passed_df.count()
        rejected_count = rejected_df.count()
        print(f"  passed={passed_count} rejected={rejected_count}")
        
        # Write passed rows to staging table (drop DQ metadata columns)
        staging_cols = [c for c in passed_df.columns if c not in DQ_METADATA_COLS]
        staging_df = passed_df.select(*staging_cols)
        
        (spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table_fqn} USING DELTA AS SELECT * FROM {source_schema}.{bare_table}_canonical WHERE 1=0")
         if not spark.catalog.tableExists(target_table_fqn) else None)
        
        staging_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_fqn)
        print(f"  SUCCESS: {bare_table} -> {target_table_fqn} ({passed_count} rows)")
        succeeded += 1
    except Exception as exc:  # noqa: BLE001
        exc_str = str(exc)
        if "does not exist" in exc_str or "not found" in exc_str:
            print(f"  SKIPPED: {bare_table} -> {exc_str}")
        else:
            print(f"  FAILED: {bare_table} -> {exc_str}")
            failures.append((bare_table, exc_str))

# Update DQ batch status to 'Y' once after all entities
if not failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET dq_status = 'Y'
        WHERE source_system_name = '{source_system_name}'
          AND COALESCE(dq_status, 'N') = 'N'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nDQ batch status updated to 'Y' for {source_system_name}")
elif failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET dq_status = 'N'
        WHERE source_system_name = '{source_system_name}'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nDQ batch status set to N (failures occurred)")

print(f"\nSummary: {succeeded} succeeded, {len(failures)} failed")

# COMMAND ----------

# DBTITLE 1,Result
# MAGIC %md #### 4. Result
# MAGIC
# MAGIC Checks if any tables failed during DQ validation. Missing tables (upstream pipeline not run yet) are separated from actual errors — missing tables produce a warning, actual errors raise a `RuntimeError`. If all succeeded, exits with `SUCCESS`.

# COMMAND ----------

if failures:
    # Separate missing tables/columns from actual errors
    missing_tables = [(tbl, err) for tbl, err in failures if "does not exist" in err or "not found" in err]
    actual_errors = [(tbl, err) for tbl, err in failures if "does not exist" not in err and "not found" not in err]
    
    if actual_errors:
        raise RuntimeError(f"DQ failed for {len(actual_errors)} tables: {actual_errors}")
    else:
        print(f"\nWarning: {len(missing_tables)} source tables do not exist yet:")
        for tbl, err in missing_tables:
            print(f"  - {tbl}")
        print("\nThis is expected if upstream pipeline stages have not run yet.")
        print("Run the ingestion and standardization notebooks first to create these tables.")
else:
    print("\nAll entities processed successfully through DQ validation.")
    
    # Show staging tables created
    staging_tables = spark.sql(f"SHOW TABLES IN {catalog}.staging").collect()
    print(f"\nStaging tables: {len(staging_tables)}")
    for row in staging_tables:
        print(f"  - {row.tableName}")

dbutils.notebook.exit("SUCCESS")