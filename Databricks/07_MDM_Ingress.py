# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 5 : MDM Ingress (HCP + HCO)
# MAGIC
# MAGIC Loads the STAGING tables into the Informatica MDM hub objects
# MAGIC (`HMDM_DEV.MDM.HCP` and `HMDM_DEV.MDM.HCO`), following the
# MAGIC `Stg_MDM_Ingress-HCP` and `Stg_MDM_Ingress-HCO` sheets exactly.

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %md #### 1. Widgets
# MAGIC
# MAGIC Select the source system and entity type from the widget panel at the top of the notebook before running MDM ingress. The next cell creates these widgets and builds the table list based on the entity type selection.
# MAGIC
# MAGIC * **Source System**: IQVIA_API (production pipeline)
# MAGIC * **Entity Type**: HCP, HCO, or BOTH — controls which STAGING tables are ingressed to MDM.HCP / MDM.HCO
# MAGIC * **Source Identifier**: MDM config identifier (default: IQVIA_HMDM)

# COMMAND ----------

# DBTITLE 1,Widget Setup
# ============================================================
# WIDGET SETUP — SOURCE SYSTEM AND ENTITY TYPE
# ============================================================
# These widgets appear at the top of the notebook.
# Select Source System and Entity Type before running MDM ingress.
# Entity Type controls which STAGING tables are ingressed:
#   HCP  → only hcp_* tables → MDM.HCP
#   HCO  → only hco_* tables → MDM.HCO
#   BOTH → all tables → MDM.HCP and MDM.HCO
# ============================================================

# Remove old widgets from previous notebook versions
try:
    dbutils.widgets.remove("staging_tables")
except Exception:
    pass

# Create dropdown widgets
dbutils.widgets.dropdown("source_system_name", "IQVIA_API", ["IQVIA_API"], "Source System")
dbutils.widgets.dropdown("entity_type", "BOTH", ["HCP", "HCO", "BOTH"], "Entity Type")

# MDM-specific config widget (kept as text)
dbutils.widgets.text("source_identifier", "IQVIA_HMDM", "Source config identifier")

# Read widget values
source_system_name = dbutils.widgets.get("source_system_name")
SELECTED_ENTITY = dbutils.widgets.get("entity_type")
source_identifier = dbutils.widgets.get("source_identifier")

# HCP and HCO staging tables
HCP_TABLES = [
    'hcp_name', 'hcp_address', 'hcp_alternate_name', 'hcp_identification',
    'hcp_specialty', 'hcp_phone', 'hcp_email', 'hcp_education',
    'hcp_tendencies', 'hcp_origin_university', 'hcp_tax', 'hcp_language',
    'hcp_hco_affiliation',
]
HCO_TABLES = [
    'hco_name', 'hco_address', 'hco_alternate_name', 'hco_identification',
    'hco_specialty', 'hco_phone', 'hco_email', 'hco_tax', 'hco_hco_hierarchy',
]

# Build (table, entity_type) pairs based on widget selection
if SELECTED_ENTITY == "HCP":
    table_entity_pairs = [(t, "HCP") for t in HCP_TABLES]
elif SELECTED_ENTITY == "HCO":
    table_entity_pairs = [(t, "HCO") for t in HCO_TABLES]
else:  # BOTH
    table_entity_pairs = [(t, "HCP") for t in HCP_TABLES] + [(t, "HCO") for t in HCO_TABLES]

print(f"Source System : {source_system_name}")
print(f"Entity Type   : {SELECTED_ENTITY}")
hcp_count = len([t for t,e in table_entity_pairs if e == 'HCP'])
hco_count = len([t for t,e in table_entity_pairs if e == 'HCO'])
print(f"Tables        : {len(table_entity_pairs)} ({hcp_count} HCP + {hco_count} HCO)")
for table, ent in table_entity_pairs:
    print(f"  - {table} ({ent})")

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %md #### 2. Imports
# MAGIC
# MAGIC Imports the MDM ingress function and runtime configuration. The `run_mdm_ingress` function writes one STAGING table's attributes onto the core MDM.HCP or MDM.HCO object per call.

# COMMAND ----------

import sys
import os
import importlib

# Add src directory to Python path
src_path = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Reload modules to pick up changes
if 'mdm.mdm_ingress' in sys.modules:
    importlib.reload(sys.modules['mdm.mdm_ingress'])
if 'core.runtime_config' in sys.modules:
    importlib.reload(sys.modules['core.runtime_config'])

from mdm.mdm_ingress import main as run_mdm_ingress
from core.runtime_config import catalog, env, get_notebook_run_url

# COMMAND ----------

# DBTITLE 1,Infrastructure Verification
# MAGIC %md
# MAGIC #### 2.5 Infrastructure Verification
# MAGIC
# MAGIC Verifies the `staging` schema (input) and `mdm` schema (output) exist. All operations are idempotent.

# COMMAND ----------

# DBTITLE 1,Display Current Infrastructure
# MAGIC %sql
# MAGIC -- Verify schemas exist
# MAGIC SHOW SCHEMAS IN HMDM_DEV;
# MAGIC
# MAGIC -- Check staging tables (input)
# MAGIC SHOW TABLES IN HMDM_DEV.staging;

# COMMAND ----------

# DBTITLE 1,Verify MDM Schema
# MAGIC %md #### Verify MDM Schema
# MAGIC
# MAGIC Creates the `mdm` schema if it does not exist, then lists all MDM tables.

# COMMAND ----------

# DBTITLE 1,Verify MDM Schema
# MAGIC %sql
# MAGIC -- Create MDM schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.mdm
# MAGIC COMMENT 'MDM publish layer - master data records';
# MAGIC
# MAGIC -- Show MDM tables (after ingress runs)
# MAGIC SHOW TABLES IN HMDM_DEV.mdm;

# COMMAND ----------

# DBTITLE 1,Run MDM Ingress
# MAGIC %md #### 3. Run MDM Ingress
# MAGIC
# MAGIC This cell executes the MDM ingress pipeline for every table selected by the Entity Type widget.
# MAGIC
# MAGIC **Batch reset logic (important):** Only the **latest batch** for the selected source system is reset to `ingress_status = 'N'` before processing. This uses `batch_id = (SELECT MAX(batch_id) ...)` to target just the newest batch — old batches keep their 'Y' status and are **not** reprocessed.
# MAGIC
# MAGIC **Processing flow:**
# MAGIC 1. Reset latest batch `ingress_status` to 'N' (latest batch only, not all)
# MAGIC 2. Loop over each (table, entity_type) pair from the widget selection:
# MAGIC    - HCP → 13 hcp_* tables → MDM.HCP
# MAGIC    - HCO → 9 hco_* tables → MDM.HCO
# MAGIC    - BOTH → 22 tables → MDM.HCP and MDM.HCO (each table ingressed with its correct entity type)
# MAGIC 3. For each table, call `run_mdm_ingress(spark, source_identifier, source_system_name, source_table, entity_type)` which:
# MAGIC    - Reads validated data from `staging.<entity>`
# MAGIC    - Writes attributes onto the core MDM.HCP or MDM.HCO object
# MAGIC    - Returns count of rows written
# MAGIC 4. After all tables processed, update latest batch `ingress_status` to 'Y' (only if no failures)
# MAGIC
# MAGIC **Failure handling:** If a table fails, the loop continues with the remaining tables. Only actual failures prevent the batch status from being set to 'Y'.

# COMMAND ----------

# DBTITLE 1,Run MDM Ingress
print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Source      : {source_system_name}")
print(f"Entity Type : {SELECTED_ENTITY}")
hcp_count = len([t for t,e in table_entity_pairs if e == 'HCP'])
hco_count = len([t for t,e in table_entity_pairs if e == 'HCO'])
print(f"Tables      : {len(table_entity_pairs)} ({hcp_count} HCP + {hco_count} HCO)")
print("=" * 60)

# Reset ingress batch status to 'N' for this source system so we can reprocess
spark.sql(f"""
    UPDATE {catalog}.util.ctl_batch_log_tbl
    SET ingress_status = 'N'
    WHERE source_system_name = '{source_system_name}'
      AND batch_id = (
          SELECT MAX(batch_id)
          FROM {catalog}.util.ctl_batch_log_tbl
          WHERE source_system_name = '{source_system_name}'
      )
""")
print(f"Ingress batch status reset to 'N' for latest batch ({source_system_name})")

failures = []
rows_written = 0
hcp_rows = 0
hco_rows = 0
for source_table, entity_type in table_entity_pairs:
    try:
        print(f"\n--- Ingress: {source_table} -> MDM.{entity_type} ---")
        count = run_mdm_ingress(
            spark=spark,
            source_identifier=source_identifier,
            source_system_name=source_system_name,
            source_table=source_table,
            entity_type=entity_type,
            skip_batch_update=True,
        )
        print(f"rows written: {count}")
        rows_written += count or 0
        if entity_type == "HCP":
            hcp_rows += count or 0
        else:
            hco_rows += count or 0
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {source_table} -> {exc}")
        failures.append((source_table, str(exc)))

# Update ingress batch status to 'Y' once after all entities
if not failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET ingress_status = 'Y'
        WHERE source_system_name = '{source_system_name}'
          AND COALESCE(ingress_status, 'N') = 'N'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nIngress batch status updated to 'Y' for {source_system_name}")
elif failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET ingress_status = 'N'
        WHERE source_system_name = '{source_system_name}'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM {catalog}.util.ctl_batch_log_tbl
              WHERE source_system_name = '{source_system_name}'
          )
    """)
    print(f"\nIngress batch status set to N (failures occurred)")

print(f"\nSummary: {rows_written} rows written, {len(failures)} failed")
print("\n" + "=" * 60)
print("MDM INGRESS SUMMARY")
print("=" * 60)
print(f"Source System : {source_system_name}")
print(f"Entity Type   : {SELECTED_ENTITY}")
hcp_tables = len([t for t,e in table_entity_pairs if e == 'HCP'])
hco_tables = len([t for t,e in table_entity_pairs if e == 'HCO'])
print(f"  HCP: {hcp_tables} tables, {hcp_rows} rows")
print(f"  HCO: {hco_tables} tables, {hco_rows} rows")
print(f"  Total: {len(table_entity_pairs)} tables, {rows_written} rows")

# COMMAND ----------

# DBTITLE 1,MDM Table Row Counts
# MAGIC %md #### MDM Table Row Counts
# MAGIC
# MAGIC Shows row counts for all MDM tables grouped by entity type (HCP and HCO). This verifies the ingress output before the result check.

# COMMAND ----------

# DBTITLE 1,Show MDM Row Counts
# Show row counts for all MDM tables, grouped by entity type
mdm_tables = spark.sql("SHOW TABLES IN hmdm_dev.mdm").collect()

results = []
for row in mdm_tables:
    tbl = row.tableName
    try:
        cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM hmdm_dev.mdm.{tbl}").collect()[0]['cnt']
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
    StructField("mdm_table", StringType(), True),
    StructField("row_count", LongType(), True),
])
display(spark.createDataFrame(results, schema))

# COMMAND ----------

# DBTITLE 1,Result
# MAGIC %md #### 4. Result
# MAGIC
# MAGIC Checks if any tables failed during MDM ingress. If all succeeded, exits with `SUCCESS`. If any failed, raises a `RuntimeError` listing the failed tables.

# COMMAND ----------

# DBTITLE 1,Cell 13
if failures:
    raise RuntimeError(f"MDM Ingress failed for {len(failures)} tables: {failures}")

dbutils.notebook.exit("SUCCESS")