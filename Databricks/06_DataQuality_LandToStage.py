# Databricks notebook source
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
dbutils.widgets.text("source_system_name", "IQVIA", "Source system")
dbutils.widgets.text("source_identifier", "IQVIA_HMDM", "Source configuration identifier (for control-table rule lookup)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = latest pending batch per table)")

source_system_name = dbutils.widgets.get("source_system_name")
source_identifier = dbutils.widgets.get("source_identifier")
batch_id_param = dbutils.widgets.get("batch_id")
batch_id = int(batch_id_param) if batch_id_param.strip() else None

# COMMAND ----------
# MAGIC %md #### 2. Imports

# COMMAND ----------
from dq.data_quality import main_data_quality_pipeline, get_rules_for_source, get_rules
from core.runtime_config import catalog, env, get_notebook_run_url

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

source_tables = sorted({rule.source_table for rule in get_rules()})
print(f"Source tables with configured DQ rules: {source_tables}")

failures = []
for source_table in source_tables:
    target_table = next(
        (r.target_table for r in get_rules_for_source(source_table)),
        source_table,
    )
    try:
        print(f"\n--- DQ: {source_table} -> {target_table} ---")
        passed_df, rejected_df = main_data_quality_pipeline(
            source_identifier=source_identifier,
            source_table=source_table,
            source_system_name=source_system_name,
            reference_table=None,  # resolved internally per-rule where required
            batch_id=batch_id,
        )
        print(f"passed={passed_df.count()} rejected={rejected_df.count()}")
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {source_table} -> {exc}")
        failures.append((source_table, str(exc)))

# COMMAND ----------
# MAGIC %md #### 4. Result

# COMMAND ----------
if failures:
    raise RuntimeError(f"DQ failed for {len(failures)} tables: {failures}")

dbutils.notebook.exit("SUCCESS")
