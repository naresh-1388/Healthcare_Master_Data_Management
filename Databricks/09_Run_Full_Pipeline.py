# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Full Pipeline Orchestrator
# MAGIC
# MAGIC Runs every pipeline stage end-to-end, in the correct dependency order,
# MAGIC by calling each stage's notebook with `dbutils.notebook.run()`. This is
# MAGIC the notebook to attach as the single task in a Databricks Workflow (or
# MAGIC to run interactively for a full end-to-end test), instead of manually
# MAGIC opening and running notebooks 03-08 one at a time.
# MAGIC
# MAGIC Order matches the medallion architecture documented in the HMDM_DEV
# MAGIC mapping workbook:
# MAGIC
# MAGIC 1. `03_Ingestion_SrcToRaw`        (Source -> Raw)
# MAGIC 2. `04_Standardization_RawToLand` (Raw -> Landing)
# MAGIC 3. `05_Canonical_Standardization` (canonical code-list normalisation)
# MAGIC 4. `06_DataQuality_LandToStage`   (Landing -> Staging, with DQ rules)
# MAGIC 5. `07_MDM_Ingress` (BOTH)        (Staging -> MDM.HCP and MDM.HCO)
# MAGIC 6. `08_MDM_Egress` (BOTH)         (MDM -> Master/downstream)

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %md #### 1. Widgets
# MAGIC
# MAGIC Select the source system from the widget panel at the top of the notebook before running the full pipeline.
# MAGIC
# MAGIC * **Source System**: IQVIA_API (production pipeline)
# MAGIC * **Batch ID**: Optional — blank means auto-detect at each stage
# MAGIC * **Stage Timeout**: Per-stage timeout in seconds (default: 3600)

# COMMAND ----------

# DBTITLE 1,Widget Setup
dbutils.widgets.dropdown("source_system_name", "IQVIA_API", ["IQVIA_API"], "Source System")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto-detect pending batch at each stage)")
dbutils.widgets.text("stage_timeout_seconds", "3600", "Per-stage timeout (seconds)")

source_system_name = dbutils.widgets.get("source_system_name")
batch_id = dbutils.widgets.get("batch_id")
timeout = int(dbutils.widgets.get("stage_timeout_seconds"))

print(f"Source System : {source_system_name}")
print(f"Batch ID      : {batch_id or 'auto-detect'}")
print(f"Stage Timeout : {timeout} seconds")

# COMMAND ----------

# MAGIC %md #### 2. Run every stage in order
# MAGIC
# MAGIC Each `dbutils.notebook.run()` call executes that notebook as an isolated
# MAGIC job run with its own cluster context, and raises if the child notebook
# MAGIC does not call `dbutils.notebook.exit("SUCCESS")` - so a failure at any
# MAGIC stage stops the whole pipeline here rather than silently continuing
# MAGIC into a stage whose input was never produced.

# COMMAND ----------

# DBTITLE 1,Run pipeline stages
# NOTE: notebook names below are relative paths - dbutils.notebook.run()
# resolves them relative to THIS notebook's own folder, so this works
# regardless of which user/workspace path the repo is checked out under.
# (An earlier version of this cell hard-coded an absolute
# /Users/<personal-email>/... path, which only worked in that one
# person's workspace - avoid re-introducing that.)

print("STAGE 1/6: Source -> Raw ingestion")
dbutils.notebook.run("03_Ingestion_SrcToRaw", timeout, {"source_system_name": source_system_name, "source_identifiers": ""})

print("STAGE 2/6: Raw -> Landing standardization")
dbutils.notebook.run("04_Standardization_RawToLand", timeout, {"source_system_name": source_system_name, "source_identifiers": ""})

print("STAGE 3/6: Canonical standardization")
dbutils.notebook.run("05_Canonical_Standardization", timeout, {"source_system_name": source_system_name, "source_identifiers": ""})

print("STAGE 4/6: Landing -> Staging data quality")
dbutils.notebook.run(
    "06_DataQuality_LandToStage",
    timeout,
    # NOTE: 06 reads "source_identifier" (singular) and "batch_id" - not
    # "source_identifiers" (plural, that name belongs to 03/04/05 only).
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "batch_id": batch_id},
)

print("STAGE 5/6: MDM Ingress (HCP + HCO)")
dbutils.notebook.run(
    "07_MDM_Ingress", timeout,
    # NOTE: 07 reads "source_identifier" (singular), not "source_identifiers".
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "entity_type": "BOTH"},
)

print("STAGE 6/6: MDM Egress (HCP Master + HCO Master)")
dbutils.notebook.run(
    "08_MDM_Egress", timeout,
    # NOTE: 08 has no "source_identifier(s)" widget at all - it reads
    # "batch_id" and "write_mode" instead.
    {"source_system_name": source_system_name, "batch_id": batch_id, "entity_type": "BOTH"},
)

# COMMAND ----------

# DBTITLE 1,Result
# MAGIC %md #### 3. Result
# MAGIC
# MAGIC If all 6 stages completed without error, exits with `SUCCESS`. Any stage failure raises an exception and stops the pipeline.

# COMMAND ----------

print("Full pipeline completed successfully.")
dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

