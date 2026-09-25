# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Full Pipeline Orchestrator
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management -- Full Pipeline Orchestrator
# MAGIC
# MAGIC Runs every pipeline stage end-to-end in the correct dependency order by calling each stage's notebook with `dbutils.notebook.run()`. This is the single notebook to attach as a Databricks Workflow task or to run interactively for a full end-to-end test.
# MAGIC
# MAGIC **Pipeline stages (in order):**
# MAGIC
# MAGIC | Stage | Notebook | Description |
# MAGIC |-------|---------|-------------|
# MAGIC | 1 | 03_Ingestion_SrcToRaw | Source -> Raw |
# MAGIC | 2 | 04_Standardization_RawToLand | Raw -> Landing |
# MAGIC | 3 | 05_Canonical_Standardization | Canonical code-list normalisation |
# MAGIC | 4 | 06_DataQuality_LandToStage | Landing -> Staging with DQ rules |
# MAGIC | 5 | 07_MDM_Ingress | Staging -> MDM.HCP and MDM.HCO |
# MAGIC | 6 | 08_MDM_Egress | MDM -> Master / downstream |
# MAGIC | 7 | 09_Snowflake_Sync | Databricks -> Snowflake bridge |
# MAGIC | 8 | 10_Dbt_Snowpark | dbt run + dbt tests + Snowpark snapshot |
# MAGIC
# MAGIC Each `dbutils.notebook.run()` call executes that notebook as an isolated job run. If a stage fails (does not call `dbutils.notebook.exit("SUCCESS")`), the pipeline stops immediately -- no silent continuation into a stage whose input was never produced.

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %md
# MAGIC #### 1. Widgets
# MAGIC
# MAGIC Select the source system and optional batch ID before running the full pipeline.
# MAGIC
# MAGIC * **Source System**: IQVIA_API (production pipeline)
# MAGIC * **Batch ID**: Optional -- blank means auto-detect at each stage
# MAGIC * **Stage Timeout**: Per-stage timeout in seconds (default: 3600)

# COMMAND ----------

# DBTITLE 1,Widget Setup
# ============================================================
# WIDGET SETUP
# ============================================================
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

# DBTITLE 1,Run all stages
# MAGIC %md
# MAGIC #### 2. Run all 8 stages in order
# MAGIC
# MAGIC This single cell calls every pipeline notebook sequentially. Each `dbutils.notebook.run()` call blocks until that stage completes (or times out). If any stage raises an exception or does not exit with `"SUCCESS"`, the pipeline stops here.
# MAGIC
# MAGIC **Pass-through parameters:**
# MAGIC * Stages 1-3 (03/04/05) receive `source_system_name` and an empty `source_identifiers` (meaning process ALL entities).
# MAGIC * Stage 4 (06 DQ) receives `source_identifier` (singular) = `IQVIA_HMDM` and optional `batch_id`.
# MAGIC * Stage 5 (07 MDM Ingress) receives `source_identifier` = `IQVIA_HMDM` and `entity_type` = `BOTH`.
# MAGIC * Stage 6 (08 MDM Egress) receives `batch_id` and `entity_type` = `BOTH` (no source_identifier widget).
# MAGIC * Stage 7 (09 Snowflake Sync) has no extra parameters -- it reads the batch control table internally.
# MAGIC * Stage 8 (10 dbt + Snowpark) has no extra parameters -- dbt reads Snowflake state directly.

# COMMAND ----------

# DBTITLE 1,Run all 8 pipeline stages
# ============================================================
# FULL PIPELINE -- ALL 8 STAGES IN ONE CELL
# ============================================================
# Each dbutils.notebook.run() call executes that notebook as an
# isolated job run with its own cluster context. If the child notebook
# does not call dbutils.notebook.exit("SUCCESS"), it raises and the
# pipeline stops here -- no silent continuation.
#
# Notebook names are relative paths resolved by dbutils.notebook.run()
# relative to THIS notebook's own folder (Databricks/), so this works
# regardless of which user/workspace the repo is checked out under.
# ============================================================

print("=" * 60)
print("HEALTHCARE MDM FULL PIPELINE STARTED")
print(f"Source: {source_system_name} | Batch: {batch_id or 'auto'} | Timeout: {timeout}s")
print("=" * 60)

# ------------------------------------------------------------
# STAGE 1/8: Source -> Raw Ingestion
# Reads IQVIA source files and lands them as RAW Delta tables.
# Processes all configured source identifiers for the selected entity type.
# ------------------------------------------------------------
print("\nSTAGE 1/8: Source -> Raw ingestion")
dbutils.notebook.run(
    "03_Ingestion_SrcToRaw",
    timeout,
    {"source_system_name": source_system_name, "source_identifiers": ""},
)

# ------------------------------------------------------------
# STAGE 2/8: Raw -> Landing Standardization
# Applies field-level standardization rules (trimming, CASE/lookup,
# Source_FK generation) and writes to the LANDING layer.
# ------------------------------------------------------------
print("\nSTAGE 2/8: Raw -> Landing standardization")
dbutils.notebook.run(
    "04_Standardization_RawToLand",
    timeout,
    {"source_system_name": source_system_name, "source_identifiers": ""},
)

# ------------------------------------------------------------
# STAGE 3/8: Canonical Standardization
# Applies cross-source canonicalization (code-list lookups,
# country/specialty/status normalisation) so values from different
# source systems converge on one canonical vocabulary.
# ------------------------------------------------------------
print("\nSTAGE 3/8: Canonical standardization")
dbutils.notebook.run(
    "05_Canonical_Standardization",
    timeout,
    {"source_system_name": source_system_name, "source_identifiers": ""},
)

# ------------------------------------------------------------
# STAGE 4/8: Landing -> Staging Data Quality
# Runs 24 DQ rules against canonical tables. Records that pass are
# written to STAGING; rejected records go to dqm_reject_tbl.
# NOTE: 06 reads "source_identifier" (singular) and "batch_id".
# ------------------------------------------------------------
print("\nSTAGE 4/8: Landing -> Staging data quality")
dbutils.notebook.run(
    "06_DataQuality_LandToStage",
    timeout,
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "batch_id": batch_id},
)

# ------------------------------------------------------------
# STAGE 5/8: MDM Ingress (HCP + HCO)
# Loads STAGING tables into the MDM hub objects (MDM.HCP and MDM.HCO).
# Each staging table's attributes are written onto the core MDM object.
# NOTE: 07 reads "source_identifier" (singular), not plural.
# ------------------------------------------------------------
print("\nSTAGE 5/8: MDM Ingress (HCP + HCO)")
dbutils.notebook.run(
    "07_MDM_Ingress",
    timeout,
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "entity_type": "BOTH"},
)

# ------------------------------------------------------------
# STAGE 6/8: MDM Egress (HCP Master + HCO Master)
# Exposes mastered MDM records to downstream consumers via MASTER tables.
# 5 HCP egress groups + 5 HCO egress groups = 10 total.
# NOTE: 08 has no source_identifier widget -- reads batch_id and write_mode.
# ------------------------------------------------------------
print("\nSTAGE 6/8: MDM Egress (HCP Master + HCO Master)")
dbutils.notebook.run(
    "08_MDM_Egress",
    timeout,
    {"source_system_name": source_system_name, "batch_id": batch_id, "entity_type": "BOTH"},
)

# ------------------------------------------------------------
# STAGE 7/8: Snowflake Sync (Databricks -> Snowflake)
# Pushes all STAGING and MASTER tables from Databricks to Snowflake
# using TRUNCATE-AND-LOAD pattern (DELETE existing rows, then INSERT).
# Verifies row counts match between Databricks and Snowflake.
# ------------------------------------------------------------
print("\nSTAGE 7/8: Snowflake Sync (Databricks -> Snowflake)")
dbutils.notebook.run("09_Snowflake_Sync", timeout, {})

# ------------------------------------------------------------
# STAGE 8/8: dbt Run + dbt Tests + Snowpark Snapshot
# Runs dbt models (23 staging views + 16 marts tables) on Snowflake.
# Runs dbt tests (not_null Source_FK checks on all models).
# Publishes date-stamped master snapshot tables via Snowpark.
# This is the final Snowflake-side post-processing stage.
# ------------------------------------------------------------
print("\nSTAGE 8/8: dbt run + dbt tests + Snowpark snapshot")
dbutils.notebook.run("10_Dbt_Snowpark", timeout, {})

# ------------------------------------------------------------
# PIPELINE COMPLETE
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("FULL PIPELINE COMPLETED SUCCESSFULLY -- ALL 8 STAGES PASSED")
print("=" * 60)
dbutils.notebook.exit("SUCCESS")