# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
# MAGIC canonical mapping control table, so this notebook has no widgets beyond
# MAGIC informational logging.

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

# COMMAND ----------

# DBTITLE 1,Define Notebook Widgets
dbutils.widgets.text("source_system_name", "IQVIA", "Source system")
dbutils.widgets.text(
    "source_identifiers",
    ",".join(['hcp_name', 'hcp_address', 'hcp_alternate_name', 'hcp_identification', 'hcp_specialty', 'hcp_phone', 'hcp_email', 'hcp_education', 'hcp_tendencies', 'hcp_origin_university', 'hcp_tax', 'hcp_language', 'hcp_hco_affiliation', 'hco_name', 'hco_address', 'hco_alternate_name', 'hco_identification', 'hco_specialty', 'hco_phone', 'hco_email', 'hco_tax', 'hco_hco_hierarchy']),
    "Comma-separated source identifiers to canonicalize (blank = all configured)",
)

source_system_name = dbutils.widgets.get("source_system_name")
source_identifiers = [s.strip() for s in dbutils.widgets.get("source_identifiers").split(",") if s.strip()]

print(f"Source System: {source_system_name}")
print(f"Source Identifiers: {len(source_identifiers)} items")

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
# MAGIC %sql
# MAGIC -- Create canonical schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.canonical
# MAGIC COMMENT 'Canonical layer - normalized cross-source data';
# MAGIC
# MAGIC -- Show canonical tables (after canonical runs)
# MAGIC SHOW TABLES IN HMDM_DEV.canonical;

# COMMAND ----------

# DBTITLE 1,Run Canonical Pipeline
print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Source System: {source_system_name}")
print(f"Source Identifiers: {source_identifiers}")

# Check if there are pending batches for canonical processing
pending_count = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog}.util.ctl_batch_log_tbl
    WHERE source_system_name = '{source_system_name}'
      AND stdz_status = 'Y'
      AND canonical_status = 'N'
""").collect()[0]['cnt']

if pending_count == 0:
    # No pending batches - reset status to allow re-running (for testing/development)
    spark.sql(f"""
        UPDATE {catalog}.util.ctl_batch_log_tbl
        SET canonical_status = 'N'
        WHERE source_system_name = '{source_system_name}'
          AND stdz_status = 'Y'
          AND canonical_status = 'Y'
    """)
    print(f"\nNo pending batches found - reset canonical_status for {source_system_name} source (for testing)")
else:
    print(f"\nFound {pending_count} pending batch(es) - proceeding with normal pipeline execution")

# Process each source identifier
failures = []
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
        
        result = main_canonical_pipeline()
        print(result)
    except Exception as exc:
        print(f"FAILED: {source_identifier} -> {exc}")
        failures.append((source_identifier, str(exc)))

if failures:
    print(f"\n{len(failures)} source(s) failed:")
    for src, err in failures:
        print(f"  - {src}: {err}")
else:
    print(f"\nAll {len(source_identifiers)} source(s) canonicalized successfully")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")