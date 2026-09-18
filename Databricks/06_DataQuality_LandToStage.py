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

# MAGIC %md #### 1. Widgets

# COMMAND ----------

dbutils.widgets.text("source_system_name", "IQVIA", "Source system")
dbutils.widgets.text("source_identifier", "IQVIA_HMDM", "Source configuration identifier (for control-table rule lookup)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = latest pending batch per table)")

source_system_name = dbutils.widgets.get("source_system_name")
source_identifier = dbutils.widgets.get("source_identifier")
batch_id_param = dbutils.widgets.get("batch_id")
batch_id = int(batch_id_param) if batch_id_param.strip() else None

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
# MAGIC #### 2.5 Infrastructure Verification
# MAGIC
# MAGIC **This section verifies required infrastructure exists:**
# MAGIC - Source schema: `landing` (input from standardization)
# MAGIC - Target schema: `staging` (output of DQ validation)
# MAGIC - DQ control tables for rule configuration
# MAGIC - DQ reject table for failed records
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

# DBTITLE 1,Verify Staging Schema
# MAGIC %sql
# MAGIC -- Create staging schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.staging
# MAGIC COMMENT 'Staging layer - data quality validated records';
# MAGIC
# MAGIC -- Show staging tables (after DQ runs)
# MAGIC SHOW TABLES IN HMDM_DEV.staging;

# COMMAND ----------

# MAGIC %md #### 3. Run every configured DQ rule, grouped by (source_table, target_table)
# MAGIC
# MAGIC `main_data_quality_pipeline(source_identifier, source_table, ...)` runs
# MAGIC every rule configured for one LANDING table and writes the result to its
# MAGIC STAGING table, so this cell drives it once per distinct source_table in
# MAGIC DQ_RULES - covering the entire Land_to_Stag sheet in one notebook run.

# COMMAND ----------

print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")

# Get bare table names from DQ rules
bare_source_tables = sorted({rule.source_table for rule in get_rules()})
print(f"Source tables with configured DQ rules: {bare_source_tables}")

# Construct fully qualified table names for landing layer
landing_schema = f"{catalog}.landing"
staging_schema = f"{catalog}.staging"

failures = []
for bare_table in bare_source_tables:
    # Construct fully qualified source and target table names
    source_table_fqn = f"{landing_schema}.{bare_table}"
    target_table_fqn = f"{staging_schema}.{bare_table}"
    
    try:
        print(f"\n--- DQ: {source_table_fqn} -> {target_table_fqn} ---")
        passed_df, rejected_df = main_data_quality_pipeline(
            source_identifier=source_identifier,
            source_table=source_table_fqn,
            source_system_name=source_system_name,
            reference_table=None,  # resolved internally per-rule where required
            batch_id=batch_id,
        )
        print(f"passed={passed_df.count()} rejected={rejected_df.count()}")
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {bare_table} -> {exc}")
        failures.append((bare_table, str(exc)))

# COMMAND ----------

# MAGIC %md #### 4. Result

# COMMAND ----------

if failures:
    # Separate missing tables from actual errors
    missing_tables = [(tbl, err) for tbl, err in failures if "does not exist" in err]
    actual_errors = [(tbl, err) for tbl, err in failures if "does not exist" not in err]
    
    if actual_errors:
        raise RuntimeError(f"DQ failed for {len(actual_errors)} tables: {actual_errors}")
    else:
        print(f"\nWarning: {len(missing_tables)} source tables do not exist yet:")
        for tbl, err in missing_tables:
            print(f"  - {tbl}")
        print("\nThis is expected if upstream pipeline stages have not run yet.")
        print("Run the ingestion and standardization notebooks first to create these tables.")

dbutils.notebook.exit("SUCCESS")