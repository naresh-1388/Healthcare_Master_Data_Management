# Databricks notebook source
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 5 : MDM Ingress (HCP + HCO)
# MAGIC
# MAGIC Loads the STAGING tables into the Informatica MDM hub objects
# MAGIC (`HMDM_DEV.MDM.HCP` and `HMDM_DEV.MDM.HCO`), following the
# MAGIC `Stg_MDM_Ingress-HCP` and `Stg_MDM_Ingress-HCO` sheets exactly.

# COMMAND ----------
# MAGIC %md #### 1. Widgets

# COMMAND ----------
dbutils.widgets.text("source_system_name", "IQVIA", "Source system")
dbutils.widgets.text("source_identifier", "IQVIA_HMDM", "Source configuration identifier")
dbutils.widgets.dropdown("entity_type", "HCP", ["HCP", "HCO"], "Entity type to ingress")
dbutils.widgets.text(
    "staging_tables",
    "",
    "Comma-separated STAGING tables to ingress (blank = every table configured for the chosen entity_type)",
)

source_system_name = dbutils.widgets.get("source_system_name")
source_identifier = dbutils.widgets.get("source_identifier")
entity_type = dbutils.widgets.get("entity_type")
staging_tables_param = [s.strip() for s in dbutils.widgets.get("staging_tables").split(",") if s.strip()]

DEFAULT_HCP_STAGING_TABLES = ['hcp_name', 'hcp_address', 'hcp_alternate_name', 'hcp_identification', 'hcp_specialty', 'hcp_phone', 'hcp_email', 'hcp_education', 'hcp_tendencies', 'hcp_origin_university', 'hcp_tax', 'hcp_language', 'hcp_hco_affiliation']
DEFAULT_HCO_STAGING_TABLES = ['hco_name', 'hco_address', 'hco_alternate_name', 'hco_identification', 'hco_specialty', 'hco_phone', 'hco_email', 'hco_tax', 'hco_hco_hierarchy']

staging_tables = staging_tables_param or (
    DEFAULT_HCP_STAGING_TABLES if entity_type == "HCP" else DEFAULT_HCO_STAGING_TABLES
)

# COMMAND ----------
# MAGIC %md #### 2. Imports

# COMMAND ----------
from mdm.mdm_ingress import main as run_mdm_ingress
from core.runtime_config import catalog, env, get_notebook_run_url

# COMMAND ----------
# MAGIC %md #### 3. Run ingress for every configured STAGING table
# MAGIC
# MAGIC `mdm_ingress.main(spark, source_identifier, source_system_name,
# MAGIC source_table, entity_type, ...)` writes one STAGING table's attributes
# MAGIC onto the core MDM.HCP / MDM.HCO object per call, so this cell loops over
# MAGIC every STAGING table for the chosen entity type - covering the whole
# MAGIC Stg_MDM_Ingress-HCP / Stg_MDM_Ingress-HCO sheet in one run.

# COMMAND ----------
print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Entity type : {entity_type}")
print(f"Tables      : {staging_tables}")

failures = []
rows_written = 0
for source_table in staging_tables:
    try:
        print(f"\n--- Ingress: {source_table} -> MDM.{entity_type} ---")
        count = run_mdm_ingress(
            spark=spark,
            source_identifier=source_identifier,
            source_system_name=source_system_name,
            source_table=source_table,
            entity_type=entity_type,
        )
        print(f"rows written: {count}")
        rows_written += count or 0
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {source_table} -> {exc}")
        failures.append((source_table, str(exc)))

# COMMAND ----------
# MAGIC %md #### 4. Result

# COMMAND ----------
print(f"Total rows written across all tables: {rows_written}")
if failures:
    raise RuntimeError(f"MDM Ingress failed for {len(failures)} tables: {failures}")

dbutils.notebook.exit("SUCCESS")
