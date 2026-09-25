# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 6 : MDM Egress (HCP Master + HCO Master)
# MAGIC
# MAGIC Exposes the mastered MDM.HCP / MDM.HCO records (and their child objects -
# MAGIC Specialty, Identification, Alternate Name, Education for HCP; Name,
# MAGIC Alternate Identifier, Phone, Specialty for HCO) to downstream consumers,
# MAGIC following the `MDM_HUB_Egress-HCP_Master` and `MDM_HUB_Egress-HCO_Master`
# MAGIC sheets.

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %md #### 1. Widgets
# MAGIC
# MAGIC Select the source system and entity type from the widget panel at the top of the notebook before running MDM egress. The next cell creates these widgets and builds the egress group list based on the entity type selection.
# MAGIC
# MAGIC * **Source System**: IQVIA_API (production pipeline)
# MAGIC * **Entity Type**: HCP, HCO, or BOTH -- controls which MDM tables are egressed to MASTER
# MAGIC * **Batch ID**: Optional -- blank means latest eligible batch
# MAGIC * **Write Mode**: append or overwrite for the Master output table

# COMMAND ----------

# DBTITLE 1,Widget Setup
# ============================================================
# WIDGET SETUP -- SOURCE SYSTEM AND ENTITY TYPE
# ============================================================
# These widgets appear at the top of the notebook.
# Select Source System and Entity Type before running MDM egress.
# Entity Type controls which MDM tables are egressed to MASTER:
#   HCP  -> 5 HCP egress groups -> MASTER.HCP
#   HCO  -> 5 HCO egress groups -> MASTER.HCO
#   BOTH -> 10 egress groups -> MASTER.HCP and MASTER.HCO
# FIX #9: Egress group paths now use {catalog} variable instead of
# hardcoded HMDM_DEV for multi-environment support.
# ============================================================

# Import catalog from runtime_config (needed for egress group paths).
import sys, os
_src_path = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)
from core.runtime_config import catalog

# Create dropdown widgets
dbutils.widgets.dropdown("source_system_name", "IQVIA_API", ["IQVIA_API"], "Source System")
dbutils.widgets.dropdown("entity_type", "BOTH", ["HCP", "HCO", "BOTH"], "Entity Type")

# Egress-specific config widgets
dbutils.widgets.text("batch_id", "", "Batch ID (blank = latest eligible batch)")
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite"], "Write mode for Master output table")

# Read widget values
source_system_name = dbutils.widgets.get("source_system_name")
SELECTED_ENTITY = dbutils.widgets.get("entity_type")
batch_id_param = dbutils.widgets.get("batch_id")
batch_id = int(batch_id_param) if batch_id_param.strip() else None
write_mode = dbutils.widgets.get("write_mode")

# Mirrors the MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheets:
# each (source_table, target_table) pair below is one egress group.
HCP_EGRESS_GROUPS = [
    (f"{catalog}.MDM.HCP",                 f"{catalog}.MASTER.HCP"),
    (f"{catalog}.MDM.HCP_SPECIALTY",       f"{catalog}.MASTER.HCP_SPECIALTY"),
    (f"{catalog}.MDM.HCP_ALTERNATE_NAME",  f"{catalog}.MASTER.HCP_ALTERNATE_NAME"),
    (f"{catalog}.MDM.HCP_EDUCATION",       f"{catalog}.MASTER.HCP_EDUCATION"),
    (f"{catalog}.MDM.HCP_IDENTIFICATION", f"{catalog}.MASTER.HCP_IDENTIFICATION"),
]
HCO_EGRESS_GROUPS = [
    (f"{catalog}.MDM.HCO",                       f"{catalog}.MASTER.HCO"),
    (f"{catalog}.MDM.HCO_NAME",                  f"{catalog}.MASTER.HCO_NAME"),
    (f"{catalog}.MDM.HCO_ALTERNATE_IDENTIFIER",  f"{catalog}.MASTER.HCO_ALTERNATE_IDENTIFIER"),
    (f"{catalog}.MDM.HCO_PHONE",                 f"{catalog}.MASTER.HCO_PHONE"),
    (f"{catalog}.MDM.HCO_SPECIALTY",             f"{catalog}.MASTER.HCO_SPECIALTY"),
]

# Build egress groups based on widget selection
if SELECTED_ENTITY == "HCP":
    egress_groups = HCP_EGRESS_GROUPS
elif SELECTED_ENTITY == "HCO":
    egress_groups = HCO_EGRESS_GROUPS
else:  # BOTH
    egress_groups = HCP_EGRESS_GROUPS + HCO_EGRESS_GROUPS

print(f"Source System : {source_system_name}")
print(f"Entity Type   : {SELECTED_ENTITY}")
print(f"Write Mode    : {write_mode}")
print(f"Batch ID      : {batch_id or 'latest'}")
hcp_g = len(HCP_EGRESS_GROUPS) if SELECTED_ENTITY in ("HCP", "BOTH") else 0
hco_g = len(HCO_EGRESS_GROUPS) if SELECTED_ENTITY in ("HCO", "BOTH") else 0
print(f"Egress Groups : {len(egress_groups)} ({hcp_g} HCP + {hco_g} HCO)")
for source_table, target_table in egress_groups:
    print(f"  - {source_table} -> {target_table}")

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %md #### 2. Imports
# MAGIC
# MAGIC Imports the MDM egress pipeline function and runtime configuration. The `run_mdm_egress` function reads from one MDM table and writes to its corresponding MASTER table.

# COMMAND ----------

import sys
import os
import importlib

# Add src directory to Python path
src_path = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Reload modules to pick up changes
if 'mdm.mdm_egress' in sys.modules:
    importlib.reload(sys.modules['mdm.mdm_egress'])
if 'core.runtime_config' in sys.modules:
    importlib.reload(sys.modules['core.runtime_config'])

from mdm.mdm_egress import main_pipeline as run_mdm_egress
from core.runtime_config import catalog, env, get_notebook_run_url

# COMMAND ----------

# DBTITLE 1,Infrastructure Verification
# MAGIC %md
# MAGIC #### 2.5 Infrastructure Verification
# MAGIC
# MAGIC Verifies the `mdm` schema (input) and `master` schema (output) exist. All operations are idempotent.

# COMMAND ----------

# DBTITLE 1,Display Current Infrastructure
# Verify schemas exist
# FIX #9: Uses catalog variable instead of hardcoded HMDM_DEV.
spark.sql(f"SHOW SCHEMAS IN {catalog}").show()

# Check MDM tables (input)
spark.sql(f"SHOW TABLES IN {catalog}.mdm").show()

# COMMAND ----------

# DBTITLE 1,Verify Master Schema
# MAGIC %md
# MAGIC #### Verify Master Schema
# MAGIC
# MAGIC Creates the `master` schema if it does not exist, then lists all master tables.

# COMMAND ----------

# DBTITLE 1,Verify Master Schema
# Create MASTER schema if not exists
# FIX #9: Uses catalog variable instead of hardcoded HMDM_DEV.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.master COMMENT 'Master layer - golden records for consumption'")

# Show MASTER tables (after egress runs)
spark.sql(f"SHOW TABLES IN {catalog}.master").show()

# COMMAND ----------

# DBTITLE 1,Run MDM Egress
# MAGIC %md #### 3. Run MDM Egress
# MAGIC
# MAGIC This cell executes the MDM egress pipeline for every egress group selected by the Entity Type widget.
# MAGIC
# MAGIC **Batch reset logic (important):** Only the **latest batch** for the selected source system is reset to `egress_status = 'N'` before processing. This uses `batch_id = (SELECT MAX(batch_id) ...)` to target just the newest batch -- old batches keep their 'Y' status and are **not** reprocessed.
# MAGIC
# MAGIC **Processing flow:**
# MAGIC 1. Reset latest batch `egress_status` to 'N' (latest batch only, not all)
# MAGIC 2. Resolve `batch_id` via SQL query (bypasses module's `get_latest_eligible_batch()`
# MAGIC    which uses `spark.table()` and may not reflect the UPDATE above on Spark Connect)
# MAGIC 3. Loop over each (source_table, target_table) egress group:
# MAGIC    - HCP -> 5 groups (HCP, HCP_SPECIALTY, HCP_ALTERNATE_NAME, HCP_EDUCATION, HCP_IDENTIFICATION)
# MAGIC    - HCO -> 5 groups (HCO, HCO_NAME, HCO_ALTERNATE_IDENTIFIER, HCO_PHONE, HCO_SPECIALTY)
# MAGIC    - BOTH -> 10 groups (all HCP + all HCO)
# MAGIC 4. For each group, call `run_mdm_egress(spark, source_system_name, source_table, target_table, batch_id, write_mode, skip_batch_update=True)` which:
# MAGIC    - Reads mastered data from `mdm.<entity>`
# MAGIC    - Writes to `master.<entity>` using the selected write mode (append/overwrite)
# MAGIC    - Returns count of rows written
# MAGIC    - `skip_batch_update=True` defers batch status update to this cell (after all groups)
# MAGIC 5. After all groups processed, update latest batch `egress_status` to 'Y' (only if no failures)
# MAGIC
# MAGIC **Failure handling:** If a source table doesn't exist yet (upstream not run), it is skipped. Other failures are logged and the loop continues. Only actual failures prevent the batch status from being set to 'Y'.

# COMMAND ----------

# DBTITLE 1,Run MDM Egress
print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Source      : {source_system_name}")
print(f"Entity Type : {SELECTED_ENTITY}")
print(f"Write Mode  : {write_mode}")
hcp_g = len(HCP_EGRESS_GROUPS) if SELECTED_ENTITY in ("HCP", "BOTH") else 0
hco_g = len(HCO_EGRESS_GROUPS) if SELECTED_ENTITY in ("HCO", "BOTH") else 0
print(f"Egress Groups: {len(egress_groups)} ({hcp_g} HCP + {hco_g} HCO)")
print("=" * 60)

# Reset egress AND Snowflake sync batch status to 'N' so both stages reprocess.
# This prevents stale Snowflake data when Databricks master is updated (FIX #7).
# When egress replays with new data, Snowflake sync must also replay (not skip
# based on old snowflake_sync_status='Y').
spark.sql(f"""
    UPDATE {catalog}.util.ctl_batch_log_tbl
    SET egress_status = 'N',
        snowflake_sync_status = 'N'
    WHERE source_system_name = '{source_system_name}'
      AND batch_id = (
          SELECT MAX(batch_id)
          FROM {catalog}.util.ctl_batch_log_tbl
          WHERE source_system_name = '{source_system_name}'
      )
""")
print(f"Egress and Snowflake sync status reset to 'N' for latest batch ({source_system_name})")

# Resolve batch_id if not provided via widget.
# This bypasses the module's get_latest_eligible_batch() which uses
# spark.table() and may not reflect the UPDATE above on Spark Connect.
if batch_id is None:
    _bid_row = spark.sql(f"""
        SELECT MAX(CAST(batch_id AS INT)) AS max_bid
        FROM {catalog}.util.ctl_batch_log_tbl
        WHERE source_system_name = '{source_system_name}'
          AND ingress_status = 'Y'
    """).collect()
    if _bid_row and _bid_row[0]['max_bid'] is not None:
        batch_id = int(_bid_row[0]['max_bid'])
        print(f"Resolved batch_id = {batch_id} (latest with ingress_status=Y)")
    else:
        print("WARNING: No batch with ingress_status=Y found")

failures = []
rows_written = 0
hcp_rows = 0
hco_rows = 0
for source_table, target_table in egress_groups:
    # FIX #4: Expected MDM source table missing is a FAILURE (not silent SKIP).
    # HCP/HCO egress groups are defined explicitly -- if a table is in the list,
    # it's expected to exist. If it doesn't, that's an upstream pipeline failure.
    if not spark.catalog.tableExists(source_table):
        print(f"\n--- Egress: {source_table} -> {target_table} ---")
        error_msg = f"Expected MDM source table does not exist (upstream pipeline failure)"
        print(f"ERROR: {error_msg}")
        failures.append((source_table, error_msg))
        continue
    try:
        print(f"\n--- Egress: {source_table} -> {target_table} ---")
        result = run_mdm_egress(
            spark=spark,
            source_system_name=source_system_name,
            source_table=source_table,
            target_table=target_table,
            batch_id=batch_id,
            write_mode=write_mode,
            skip_batch_update=True,
        )
        print(result)
        written = result.get("written_records", 0)
        rows_written += written
        if "HCP" in source_table:
            hcp_rows += written
        else:
            hco_rows += written
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {source_table} -> {target_table}: {exc}")
        failures.append((source_table, str(exc)))

# Update egress batch status to 'Y' once after all groups
if not failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET egress_status = 'Y'
        WHERE source_system_name = '{source_system_name}'
          AND COALESCE(egress_status, 'N') = 'N'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nEgress batch status updated to 'Y' for {source_system_name}")
elif failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET egress_status = 'N'
        WHERE source_system_name = '{source_system_name}'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nEgress batch status set to N (failures occurred)")

print(f"\nSummary: {rows_written} rows written, {len(failures)} failed")
print("\n" + "=" * 60)
print("MDM EGRESS SUMMARY")
print("=" * 60)
print(f"Source System : {source_system_name}")
print(f"Entity Type   : {SELECTED_ENTITY}")
hcp_groups = len(HCP_EGRESS_GROUPS) if SELECTED_ENTITY in ("HCP", "BOTH") else 0
hco_groups = len(HCO_EGRESS_GROUPS) if SELECTED_ENTITY in ("HCO", "BOTH") else 0
print(f"  HCP: {hcp_groups} groups, {hcp_rows} rows")
print(f"  HCO: {hco_groups} groups, {hco_rows} rows")
print(f"  Total: {len(egress_groups)} groups, {rows_written} rows")

# COMMAND ----------

# DBTITLE 1,Master Table Row Counts
# MAGIC %md #### Master Table Row Counts
# MAGIC
# MAGIC Shows row counts for all MASTER tables grouped by entity type (HCP and HCO). This verifies the egress output before the result check.

# COMMAND ----------

# DBTITLE 1,Show Master Row Counts
# Show row counts for all MASTER tables, grouped by entity type
# FIX #9: Use {catalog} variable instead of hardcoded hmdm_dev.
master_tables = spark.sql(f"SHOW TABLES IN {catalog}.master").collect()

results = []
for row in master_tables:
    tbl = row.tableName
    try:
        cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.master.{tbl}").collect()[0]['cnt']
    except Exception:
        cnt = 0
    entity = "HCP" if tbl.startswith("hcp") else "HCO" if tbl.startswith("hco") else "UNKNOWN"
    results.append((entity, tbl, cnt))

# Sort by entity type then table name
results.sort(key=lambda x: (x[0], x[1]))

# Create DataFrame and display
from pyspark.sql.types import StructType, StructField, StringType, LongType
schema = StructType([
    StructField("entity_type", StringType(), True),
    StructField("master_table", StringType(), True),
    StructField("row_count", LongType(), True),
])
display(spark.createDataFrame(results, schema))

# COMMAND ----------

# DBTITLE 1,Result
# MAGIC %md #### 4. Result
# MAGIC
# MAGIC Checks if any groups failed during MDM egress. If all succeeded, exits with `SUCCESS`. If any failed, raises a `RuntimeError` listing the failed groups.

# COMMAND ----------

if failures:
    raise RuntimeError(f"MDM Egress failed for {len(failures)} groups: {failures}")

dbutils.notebook.exit("SUCCESS")