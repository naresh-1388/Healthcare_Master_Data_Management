# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 6 : MDM Egress (HCP Master + HCO Master)
# MAGIC
# MAGIC Exposes the mastered MDM.HCP / MDM.HCO records (and their child objects -
# MAGIC Specialty, License, Alternate Name, Therapeutic Area for HCP; Name,
# MAGIC Alternate Identifier, Phone, Specialty for HCO) to downstream consumers,
# MAGIC following the `MDM_HUB_Egress-HCP_Master` and `MDM_HUB_Egress-HCO_Master`
# MAGIC sheets.

# COMMAND ----------

# MAGIC %md #### 1. Widgets

# COMMAND ----------

dbutils.widgets.text("source_system_name", "IQVIA_API", "Source system")
dbutils.widgets.dropdown("entity_type", "HCP", ["HCP", "HCO"], "Entity type to egress")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = latest eligible batch)")
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite"], "Write mode for the Master output table")

source_system_name = dbutils.widgets.get("source_system_name")
entity_type = dbutils.widgets.get("entity_type")
batch_id_param = dbutils.widgets.get("batch_id")
batch_id = int(batch_id_param) if batch_id_param.strip() else None
write_mode = dbutils.widgets.get("write_mode")

# Mirrors the MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheets:
# each (source_table, target_table) pair below is one egress group.
HCP_EGRESS_GROUPS = [
    ("HMDM_DEV.MDM.HCP",                 "HMDM_DEV.MASTER.HCP"),
    ("HMDM_DEV.MDM.HCP_SPECIALTY",       "HMDM_DEV.MASTER.HCP_SPECIALTY"),
    ("HMDM_DEV.MDM.HCP_ALTERNATE_NAME",  "HMDM_DEV.MASTER.HCP_ALTERNATE_NAME"),
    ("HMDM_DEV.MDM.HCP_THERAPEUTIC_AREA","HMDM_DEV.MASTER.HCP_THERAPEUTIC_AREA"),
    ("HMDM_DEV.MDM.HCP_LICENSE",         "HMDM_DEV.MASTER.HCP_LICENSE"),
]
HCO_EGRESS_GROUPS = [
    ("HMDM_DEV.MDM.HCO",                       "HMDM_DEV.MASTER.HCO"),
    ("HMDM_DEV.MDM.HCO_NAME",                  "HMDM_DEV.MASTER.HCO_NAME"),
    ("HMDM_DEV.MDM.HCO_ALTERNATE_IDENTIFIER",  "HMDM_DEV.MASTER.HCO_ALTERNATE_IDENTIFIER"),
    ("HMDM_DEV.MDM.HCO_PHONE",                 "HMDM_DEV.MASTER.HCO_PHONE"),
    ("HMDM_DEV.MDM.HCO_SPECIALTY",             "HMDM_DEV.MASTER.HCO_SPECIALTY"),
]

egress_groups = HCP_EGRESS_GROUPS if entity_type == "HCP" else HCO_EGRESS_GROUPS

# COMMAND ----------

# MAGIC %md #### 2. Imports

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
# MAGIC **This section verifies required infrastructure exists:**
# MAGIC - Source schema: `mdm` (input from MDM ingress)
# MAGIC - Target schema: `master` (egress/consumption layer)
# MAGIC - Master tables for downstream consumption
# MAGIC
# MAGIC **Safe to re-run:** All operations are idempotent.

# COMMAND ----------

# DBTITLE 1,Display Current Infrastructure
# MAGIC %sql
# MAGIC -- Verify schemas exist
# MAGIC SHOW SCHEMAS IN HMDM_DEV;
# MAGIC
# MAGIC -- Check MDM tables (input)
# MAGIC SHOW TABLES IN HMDM_DEV.mdm;

# COMMAND ----------

# DBTITLE 1,Verify Master Schema
# MAGIC %sql
# MAGIC -- Create MASTER schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.master
# MAGIC COMMENT 'Master layer - golden records for consumption';
# MAGIC
# MAGIC -- Show MASTER tables (after egress runs)
# MAGIC SHOW TABLES IN HMDM_DEV.master;

# COMMAND ----------

# MAGIC %md #### 3. Run egress for every configured group

# COMMAND ----------

print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Entity type : {entity_type}")

# Reset egress batch status to 'N' for this source system so we can reprocess
spark.sql(f"""
    UPDATE {catalog}.util.ctl_batch_log_tbl
    SET egress_status = 'N'
    WHERE source_system_name = '{source_system_name}'
""")
print(f"Egress batch status reset to 'N' for {source_system_name}")

failures = []
rows_written = 0
for source_table, target_table in egress_groups:
    # Skip MDM source tables that don't exist yet (e.g. child tables not created)
    if not spark.catalog.tableExists(source_table):
        print(f"\n--- Egress: {source_table} -> {target_table} ---")
        print(f"SKIPPED: source table does not exist")
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
        rows_written += result.get("written_records", 0)
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {source_table} -> {target_table}: {exc}")
        failures.append((source_table, str(exc)))

# Update egress batch status to 'Y' once after all groups
if not failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET egress_status = 'Y'
        WHERE source_system_name = '{source_system_name}'
    """)
    print(f"\nEgress batch status updated to 'Y' for {source_system_name}")

print(f"\nSummary: {rows_written} rows written, {len(failures)} failed")

# COMMAND ----------

# MAGIC %md #### 4. Result

# COMMAND ----------

if failures:
    raise RuntimeError(f"MDM Egress failed for {len(failures)} groups: {failures}")

dbutils.notebook.exit("SUCCESS")