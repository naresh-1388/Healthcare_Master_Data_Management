# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 1 : Source -> Raw Ingestion
# MAGIC
# MAGIC Reads the newly-arrived IQVIA source files (per the `Source_Raw` sheet of
# MAGIC the HMDM_DEV mapping workbook), applies the configured JSON-path-to-RAW-column
# MAGIC extraction, and lands the result as RAW Delta tables partitioned by batch.
# MAGIC
# MAGIC Business logic lives in `src/ingestion/src_to_raw_ingestion.py::run_ingestion_pipeline`
# MAGIC - this notebook is a thin, per-source-identifier orchestration loop so it
# MAGIC can be run interactively (cell-by-cell) for testing, or as a scheduled
# MAGIC Databricks Workflow task.

# COMMAND ----------

# MAGIC %md #### 1. Widgets

# COMMAND ----------

dbutils.widgets.text("source_system_name", "IQVIA", "Source system")
dbutils.widgets.text(
    "source_identifiers",
    ",".join(['hcp_name', 'hcp_address', 'hcp_alternate_name', 'hcp_identification', 'hcp_specialty', 'hcp_phone', 'hcp_email', 'hcp_education', 'hcp_tendencies', 'hcp_origin_university', 'hcp_tax', 'hcp_language', 'hcp_hco_affiliation', 'hco_name', 'hco_address', 'hco_alternate_name', 'hco_identification', 'hco_specialty', 'hco_phone', 'hco_email', 'hco_tax', 'hco_hco_hierarchy']),
    "Comma-separated source identifiers to ingest (blank = configured default set)",
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

from ingestion.src_to_raw_ingestion import run_ingestion_pipeline
from core.runtime_config import catalog, env, get_notebook_run_url

# COMMAND ----------

# DBTITLE 1,Infrastructure Verification
# MAGIC %md
# MAGIC #### 2.5 Infrastructure Verification
# MAGIC
# MAGIC **This section verifies and creates required Databricks infrastructure:**
# MAGIC - Catalog: `HMDM_DEV` (or environment-specific)
# MAGIC - Schemas: `raw`, `landing`, `staging`, `mdm`, `util`
# MAGIC - Control tables in `util` schema
# MAGIC
# MAGIC **Safe to re-run:** All CREATE statements use `IF NOT EXISTS` to ensure idempotency.

# COMMAND ----------

# DBTITLE 1,Display Current Infrastructure
# MAGIC %sql
# MAGIC -- Check if HMDM_DEV catalog exists
# MAGIC SHOW CATALOGS LIKE 'HMDM_DEV';
# MAGIC
# MAGIC -- Show schemas in HMDM_DEV (if it exists)
# MAGIC SHOW SCHEMAS IN HMDM_DEV;

# COMMAND ----------

# DBTITLE 1,Create Infrastructure If Not Exists
# MAGIC %sql
# MAGIC -- Create catalog if not exists
# MAGIC CREATE CATALOG IF NOT EXISTS HMDM_DEV
# MAGIC COMMENT 'Healthcare Master Data Management - Main Catalog';
# MAGIC
# MAGIC -- Create schemas if not exist
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.raw
# MAGIC COMMENT 'Raw data layer - unprocessed source data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.landing
# MAGIC COMMENT 'Landing layer - standardized format';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.staging
# MAGIC COMMENT 'Staging layer - data quality validated';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.mdm
# MAGIC COMMENT 'MDM publish layer - master data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS HMDM_DEV.util
# MAGIC COMMENT 'Utility schema - control tables and configuration';
# MAGIC
# MAGIC -- Verify schemas were created
# MAGIC SHOW SCHEMAS IN HMDM_DEV;

# COMMAND ----------

# MAGIC %md #### 3. Run ingestion for every configured source identifier
# MAGIC
# MAGIC `run_ingestion_pipeline(spark, source_system_name, source_identifier)` is a
# MAGIC **per-entity** entry point (one HCP/HCO table at a time), so this cell
# MAGIC loops over every entity configured above. A failure on one entity is
# MAGIC logged and re-raised after the loop finishes the remaining entities, so
# MAGIC one bad source file does not silently block every other table.

# COMMAND ----------

print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Source      : {source_system_name}")
print(f"Entities    : {source_identifiers}")

failures = []
for source_identifier in source_identifiers:
    try:
        print(f"\n--- Ingesting {source_identifier} ---")
        result = run_ingestion_pipeline(
            spark_session=spark,
            source_system_name=source_system_name,
            source_identifier=source_identifier,
        )
        print(result)
    except Exception as exc:  # noqa: BLE001 - intentionally broad: one bad
                              # entity must not stop the rest of the batch
        print(f"FAILED: {source_identifier} -> {exc}")
        failures.append((source_identifier, str(exc)))

# COMMAND ----------

# MAGIC %md #### 4. Result

# COMMAND ----------

if failures:
    raise RuntimeError(f"Ingestion failed for {len(failures)} entities: {failures}")

dbutils.notebook.exit("SUCCESS")