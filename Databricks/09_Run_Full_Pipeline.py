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
# MAGIC 7. `10_Snowflake_Sync`            (Databricks -> Snowflake bridge)

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

print("STAGE 1/9: Source -> Raw ingestion")
dbutils.notebook.run("03_Ingestion_SrcToRaw", timeout, {"source_system_name": source_system_name, "source_identifiers": ""})

print("STAGE 2/9: Raw -> Landing standardization")
dbutils.notebook.run("04_Standardization_RawToLand", timeout, {"source_system_name": source_system_name, "source_identifiers": ""})

print("STAGE 3/9: Canonical standardization")
dbutils.notebook.run("05_Canonical_Standardization", timeout, {"source_system_name": source_system_name, "source_identifiers": ""})

print("STAGE 4/9: Landing -> Staging data quality")
dbutils.notebook.run(
    "06_DataQuality_LandToStage",
    timeout,
    # NOTE: 06 reads "source_identifier" (singular) and "batch_id" - not
    # "source_identifiers" (plural, that name belongs to 03/04/05 only).
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "batch_id": batch_id},
)

print("STAGE 5/9: MDM Ingress (HCP + HCO)")
dbutils.notebook.run(
    "07_MDM_Ingress", timeout,
    # NOTE: 07 reads "source_identifier" (singular), not "source_identifiers".
    {"source_system_name": source_system_name, "source_identifier": "IQVIA_HMDM", "entity_type": "BOTH"},
)

print("STAGE 6/9: MDM Egress (HCP Master + HCO Master)")
dbutils.notebook.run(
    "08_MDM_Egress", timeout,
    # NOTE: 08 has no "source_identifier(s)" widget at all - it reads
    # "batch_id" and "write_mode" instead.
    {"source_system_name": source_system_name, "batch_id": batch_id, "entity_type": "BOTH"},
)

print("STAGE 7/9: Snowflake Sync (Databricks -> Snowflake)")
dbutils.notebook.run("10_Snowflake_Sync", timeout, {})

# COMMAND ----------

# DBTITLE 1,Stage 8 - dbt Run
# MAGIC %md #### Stage 8/9: dbt Run
# MAGIC
# MAGIC Runs dbt models (staging views + marts tables) on Snowflake using run_dbt.sh.
# MAGIC This creates 23 staging views and 16 marts tables in Snowflake.

# COMMAND ----------

print("STAGE 8/9: dbt run (Snowflake staging views + marts tables)")
import subprocess
dbt_dir = "/Workspace/Repos/naresh.mayari@gmail.com/Healthcare_Master_Data_Management/dbt"
dbt_script = os.path.join(dbt_dir, "run_dbt.sh")
result = subprocess.run(
    [dbt_script, "dbt", "run", "--target", "dev", "--profiles-dir", "."],
    capture_output=True, text=True, timeout=600
)
if result.returncode != 0:
    print(f"dbt run FAILED:\n{result.stderr}")
    dbutils.notebook.exit("FAILED at STAGE 8/9: dbt run failed")
print(f"dbt run SUCCESS: {result.stdout[-500:]}")

# COMMAND ----------

# DBTITLE 1,Stage 9 - Snowpark Snapshot
# MAGIC %md #### Stage 9/9: Snowpark Snapshot
# MAGIC
# MAGIC Publishes date-stamped master snapshot tables (HCP + HCO) using publish_master_snapshot.py.

# COMMAND ----------

print("STAGE 9/9: Snowpark snapshot (publish master snapshots)")
snowpark_script = "/Workspace/Repos/naresh.mayari@gmail.com/Healthcare_Master_Data_Management/snowflake/snowpark/publish_master_snapshot.py"
for entity in ["HCP", "HCO"]:
    result = subprocess.run(
        ["python", snowpark_script, "--entity", entity],
        capture_output=True, text=True, timeout=300
    )
    if result.returncode != 0:
        print(f"Snowpark snapshot {entity} FAILED:\n{result.stderr}")
        dbutils.notebook.exit(f"FAILED at STAGE 9/9: Snowpark {entity} snapshot failed")
    print(f"Snowpark {entity} snapshot SUCCESS")

# COMMAND ----------

# DBTITLE 1,Result
# MAGIC %md #### 3. Result
# MAGIC
# MAGIC If all 9 stages completed without error, exits with `SUCCESS`. Any stage failure raises an exception and stops the pipeline.

# COMMAND ----------

print("Full pipeline completed successfully - all 9 stages.")
dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

