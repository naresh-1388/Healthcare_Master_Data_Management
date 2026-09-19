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

dbutils.widgets.text("source_system_name", "IQVIA_API", "Source system")
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
""")
print(f"DQ batch status reset to 'N' for {source_system_name}")

failures = []
succeeded = 0
for bare_table in bare_source_tables:
    # Construct fully qualified source and target table names
    source_table_fqn = f"{landing_schema}.{bare_table}"
    target_table_fqn = f"{staging_schema}.{bare_table}"
    
    # Resolve reference table for this entity
    ref_bare = REFERENCE_TABLE_MAP.get(bare_table)
    ref_table_fqn = f"{landing_schema}.{ref_bare}" if ref_bare else None
    
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
        
        (spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table_fqn} USING DELTA AS SELECT * FROM {landing_schema}.{bare_table} WHERE 1=0")
         if not spark.catalog.tableExists(target_table_fqn) else None)
        
        staging_df.write.format("delta").mode("overwrite").saveAsTable(target_table_fqn)
        print(f"  SUCCESS: {bare_table} -> {target_table_fqn} ({passed_count} rows)")
        succeeded += 1
    except Exception as exc:  # noqa: BLE001
        print(f"  FAILED: {bare_table} -> {exc}")
        failures.append((bare_table, str(exc)))

# Update DQ batch status to 'Y' once after all entities
if not failures:
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET dq_status = 'Y'
        WHERE source_system_name = '{source_system_name}'
    """)
    print(f"\nDQ batch status updated to 'Y' for {source_system_name}")

print(f"\nSummary: {succeeded} succeeded, {len(failures)} failed")

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
else:
    print("\nAll entities processed successfully through DQ validation.")
    
    # Show staging tables created
    staging_tables = spark.sql(f"SHOW TABLES IN {catalog}.staging").collect()
    print(f"\nStaging tables: {len(staging_tables)}")
    for row in staging_tables:
        print(f"  - {row.tableName}")

dbutils.notebook.exit("SUCCESS")