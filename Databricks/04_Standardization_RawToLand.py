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

# MAGIC %md #### 1. Widgets

# COMMAND ----------

dbutils.widgets.text("source_system_name", "IQVIA", "Source system")
dbutils.widgets.text(
    "source_identifiers",
    ",".join(['hcp_name', 'hcp_address', 'hcp_alternate_name', 'hcp_identification', 'hcp_specialty', 'hcp_phone', 'hcp_email', 'hcp_education', 'hcp_tendencies', 'hcp_origin_university', 'hcp_tax', 'hcp_language', 'hcp_hco_affiliation', 'hco_name', 'hco_address', 'hco_alternate_name', 'hco_identification', 'hco_specialty', 'hco_phone', 'hco_email', 'hco_tax', 'hco_hco_hierarchy']),
    "Comma-separated RAW tables to standardize (blank = every table configured for this source)",
)

source_system_name = dbutils.widgets.get("source_system_name")
source_identifiers = [s.strip() for s in dbutils.widgets.get("source_identifiers").split(",") if s.strip()]

# COMMAND ----------

# MAGIC %md #### 2. Imports

# COMMAND ----------

import sys
import os

# Add src directory to Python path
src_path = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

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

# MAGIC %md #### 3. Run standardization
# MAGIC
# MAGIC `main_standardization_pipeline(source_identifier, source_system_name, tbl_nm)`
# MAGIC accepts an explicit table name; when source_identifiers is left at its
# MAGIC default (every configured entity) this loops table-by-table, mirroring
# MAGIC the Raw_to_Land sheet exactly.

# COMMAND ----------

print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")

failures = []
for source_identifier in source_identifiers:
    try:
        print(f"\n--- Standardizing {source_identifier} ---")
        result = main_standardization_pipeline(
            source_identifier=source_identifier,
            source_system_name=source_system_name,
            tbl_nm=source_identifier,
        )
        print(result)
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {source_identifier} -> {exc}")
        failures.append((source_identifier, str(exc)))

# COMMAND ----------

# MAGIC %md #### 4. Result

# COMMAND ----------

if failures:
    raise RuntimeError(f"Standardization failed for {len(failures)} tables: {failures}")

dbutils.notebook.exit("SUCCESS")