# Databricks notebook source
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
common_params = {"source_system_name": source_system_name, "batch_id": batch_id}

print("STAGE 1/6: Source -> Raw ingestion")
dbutils.notebook.run("03_Ingestion_SrcToRaw", timeout, common_params)

print("STAGE 2/6: Raw -> Landing standardization")
dbutils.notebook.run("04_Standardization_RawToLand", timeout, common_params)

print("STAGE 3/6: Canonical standardization")
dbutils.notebook.run("05_Canonical_Standardization", timeout, {})

print("STAGE 4/6: Landing -> Staging data quality")
dbutils.notebook.run(
    "06_DataQuality_LandToStage",
    timeout,
    {"source_system_name": source_system_name, "batch_id": batch_id, "source_identifier": "IQVIA_HMDM"},
)

print("STAGE 5/6: MDM Ingress (HCP, then HCO)")
dbutils.notebook.run(
    "07_MDM_Ingress", timeout,
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "entity_type": "HCP"},
)
dbutils.notebook.run(
    "07_MDM_Ingress", timeout,
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "entity_type": "HCO"},
)

print("STAGE 6/6: MDM Egress (HCP Master, then HCO Master)")
dbutils.notebook.run(
    "08_MDM_Egress", timeout,
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
