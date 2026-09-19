# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
# MAGIC 5. `07_MDM_Ingress` (HCP)         (Staging -> MDM.HCP)
# MAGIC 6. `07_MDM_Ingress` (HCO)         (Staging -> MDM.HCO)
# MAGIC 7. `08_MDM_Egress` (HCP)          (MDM.HCP -> HCP Master/downstream)
# MAGIC 8. `08_MDM_Egress` (HCO)          (MDM.HCO -> HCO Master/downstream)

# COMMAND ----------

# MAGIC %md #### 1. Widgets

# COMMAND ----------

dbutils.widgets.text("source_system_name", "IQVIA", "Source system")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto-detect pending batch at each stage)")
dbutils.widgets.text("stage_timeout_seconds", "3600", "Per-stage timeout (seconds)")

source_system_name = dbutils.widgets.get("source_system_name")
batch_id = dbutils.widgets.get("batch_id")
timeout = int(dbutils.widgets.get("stage_timeout_seconds"))

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

print("STAGE 5/6: MDM Ingress (HCP, then HCO)")
dbutils.notebook.run(
    "07_MDM_Ingress", timeout,
    # NOTE: 07 reads "source_identifier" (singular), not "source_identifiers".
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "entity_type": "HCP"},
)
dbutils.notebook.run(
    "07_MDM_Ingress", timeout,
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "entity_type": "HCO"},
)

print("STAGE 6/6: MDM Egress (HCP Master, then HCO Master)")
dbutils.notebook.run(
    "08_MDM_Egress", timeout,
    # NOTE: 08 has no "source_identifier(s)" widget at all - it reads
    # "batch_id" and "write_mode" instead.
    {"source_system_name": source_system_name, "batch_id": batch_id, "entity_type": "HCP"},
)
dbutils.notebook.run(
    "08_MDM_Egress", timeout,
    {"source_system_name": source_system_name, "batch_id": batch_id, "entity_type": "HCO"},
)

# COMMAND ----------

# MAGIC %md #### 3. Result

# COMMAND ----------

print("Full pipeline completed successfully.")
dbutils.notebook.exit("SUCCESS")