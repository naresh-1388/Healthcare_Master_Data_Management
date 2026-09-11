# Databricks notebook source
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 6 : MDM Egress (HCP Master + HCO Master)
# MAGIC
# MAGIC Exposes the mastered MDM.HCP / MDM.HCO records (and their child objects -
# MAGIC Specialty, License, Alternate Name, Therapeutic Area for HCP; Name,
# MAGIC Alternate Identifier, Phone, Specialty for HCO) to downstream consumers,
# MAGIC following the `MDM_HUB_Egress-HCP_Master` and `MDM_HUB_Egress-HCO_Master`
# MAGIC sheets.

# COMMAND ----------
# MAGIC %md #### 1. Widgets

# COMMAND ----------
dbutils.widgets.text("source_system_name", "IQVIA", "Source system")
dbutils.widgets.dropdown("entity_type", "HCP", ["HCP", "HCO"], "Entity type to egress")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = latest eligible batch)")
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite"], "Write mode for the Master output table")

source_system_name = dbutils.widgets.get("source_system_name")
entity_type = dbutils.widgets.get("entity_type")
batch_id_param = dbutils.widgets.get("batch_id")
batch_id = int(batch_id_param) if batch_id_param.strip() else None
write_mode = dbutils.widgets.get("write_mode")

# Mirrors the MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheets:
# each (source_table, target_table) pair below is one egress group.
HCP_EGRESS_GROUPS = [
    ("HMDM_DEV.MDM.HCP",                 "HMDM_DEV.MASTER.HCP"),
    ("HMDM_DEV.MDM.HCP_SPECIALTY",       "HMDM_DEV.MASTER.HCP_SPECIALTY"),
    ("HMDM_DEV.MDM.HCP_ALTERNATE_NAME",  "HMDM_DEV.MASTER.HCP_ALTERNATE_NAME"),
    ("HMDM_DEV.MDM.HCP_THERAPEUTIC_AREA","HMDM_DEV.MASTER.HCP_THERAPEUTIC_AREA"),
    ("HMDM_DEV.MDM.HCP_LICENSE",         "HMDM_DEV.MASTER.HCP_LICENSE"),
]
HCO_EGRESS_GROUPS = [
    ("HMDM_DEV.MDM.HCO",                       "HMDM_DEV.MASTER.HCO"),
    ("HMDM_DEV.MDM.HCO_NAME",                  "HMDM_DEV.MASTER.HCO_NAME"),
    ("HMDM_DEV.MDM.HCO_ALTERNATE_IDENTIFIER",  "HMDM_DEV.MASTER.HCO_ALTERNATE_IDENTIFIER"),
    ("HMDM_DEV.MDM.HCO_PHONE",                 "HMDM_DEV.MASTER.HCO_PHONE"),
    ("HMDM_DEV.MDM.HCO_SPECIALTY",             "HMDM_DEV.MASTER.HCO_SPECIALTY"),
]

egress_groups = HCP_EGRESS_GROUPS if entity_type == "HCP" else HCO_EGRESS_GROUPS

# COMMAND ----------
# MAGIC %md #### 2. Imports

# COMMAND ----------
from mdm.mdm_egress import main_pipeline as run_mdm_egress
from core.runtime_config import catalog, env, get_notebook_run_url

# COMMAND ----------
# MAGIC %md #### 3. Run egress for every configured group

# COMMAND ----------
print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")
print(f"Entity type : {entity_type}")

failures = []
for source_table, target_table in egress_groups:
    try:
        print(f"\n--- Egress: {source_table} -> {target_table} ---")
        result = run_mdm_egress(
            spark=spark,
            source_system_name=source_system_name,
            source_table=source_table,
            target_table=target_table,
            batch_id=batch_id,
            write_mode=write_mode,
        )
        print(result)
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {source_table} -> {target_table}: {exc}")
        failures.append((source_table, str(exc)))

# COMMAND ----------
# MAGIC %md #### 4. Result

# COMMAND ----------
if failures:
    raise RuntimeError(f"MDM Egress failed for {len(failures)} groups: {failures}")

dbutils.notebook.exit("SUCCESS")
