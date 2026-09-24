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
# MAGIC opening and running notebooks 03-10 one at a time.
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
# MAGIC 8. dbt run                        (Snowflake staging views + marts tables)
# MAGIC 9. Snowpark snapshot              (Date-stamped master snapshot tables)
# MAGIC
# MAGIC All paths (dbt, Snowpark) are resolved dynamically from the notebook
# MAGIC context -- no hard-coded workspace paths, so this works for any team
# MAGIC member who checks out the repo.
# MAGIC
# MAGIC **API-to-RAW is a separate flow:** The main pipeline starts at Stage 1
# MAGIC (03_Ingestion_SrcToRaw) which reads from the RAW schema. The API-to-RAW
# MAGIC caller (src/api/api_to_raw_caller.py) is a separate real-time integration
# MAGIC exercised from 02_ValidationS_for_APIs and writes to raw.hcp_api_data /
# MAGIC raw.hco_api_data. There is no production transformation from those API
# MAGIC tables to the per-entity raw tables (raw.hcp_name, raw.hcp_address, etc.)
# MAGIC that the main pipeline consumes. This is by design -- the AWS Lambda
# MAGIC bridge feeds the Informatica MDM Hub, and the Databricks pipeline picks up
# MAGIC data from a different source. If you want the Lambda to feed the Databricks
# MAGIC pipeline directly, a transformation step must be added separately.
# MAGIC
# MAGIC **External dependency -- Informatica MDM Hub:** The HCP child master dbt
# MAGIC models (master_hcp_specialty, master_hcp_alternate_name, master_hcp_license,
# MAGIC master_hcp_therapeutic_area) read from source('mdm_hub', ...) which are
# MAGIC HMDM_DEV.MDM.* tables created by the Informatica MDM Hub, NOT by this
# MAGIC repository. These tables must exist and be populated before the dbt models
# MAGIC that depend on them can run successfully.

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %md #### 1. Widgets
# MAGIC
# MAGIC Select the source system from the widget panel at the top of the notebook before running the full pipeline.
# MAGIC
# MAGIC * **Source System**: IQVIA_API (production pipeline)
# MAGIC * **Batch ID**: Optional -- blank means auto-detect at each stage
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
# MAGIC
# MAGIC Repo root is resolved dynamically from the notebook context (no
# MAGIC hard-coded paths). dbt is installed if not already present. The
# MAGIC `--profiles-dir` argument is set to the dbt directory absolute path,
# MAGIC and `cwd=dbt_dir` ensures dbt finds profiles.yml in the right place.

# COMMAND ----------

# DBTITLE 1,Stage 8 - dbt Run
print("STAGE 8/9: dbt run (Snowflake staging views + marts tables)")

# Resolve repo root dynamically from notebook context (no hard-coded paths).
# This works regardless of which user/workspace the repo is checked out under.
import os
import subprocess
from pathlib import Path

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# e.g. /Repos/<user>/Healthcare_Master_Data_Management/Databricks/09_Run_Full_Pipeline
# parent.parent goes up from Databricks/09_Run_Full_Pipeline to the repo root.
_repo_root = Path("/Workspace" + _notebook_path).parent.parent
dbt_dir = str(_repo_root / "dbt")
dbt_script = os.path.join(dbt_dir, "run_dbt.sh")

# Ensure dbt is installed (not guaranteed on fresh Databricks environments).
try:
    subprocess.run(["dbt", "--version"], capture_output=True, check=True)
    print("dbt already installed")
except (FileNotFoundError, subprocess.CalledProcessError):
    print("dbt not found -- installing dbt-snowflake...")
    subprocess.run(["pip", "install", "dbt-snowflake", "--quiet"], check=True)
    print("dbt-snowflake installed")

# Run dbt with cwd=dbt_dir so --profiles-dir . resolves to the dbt folder
# (not the notebook's own working directory).
result = subprocess.run(
    [dbt_script, "dbt", "run", "--target", "dev", "--profiles-dir", dbt_dir],
    capture_output=True, text=True, timeout=600, cwd=dbt_dir,
)
if result.returncode != 0:
    print(f"dbt run FAILED:\n{result.stderr}")
    dbutils.notebook.exit("FAILED at STAGE 8/9: dbt run failed")
print(f"dbt run SUCCESS: {result.stdout[-500:]}")

# COMMAND ----------

# DBTITLE 1,Stage 9 - Snowpark Snapshot
# MAGIC %md #### Stage 9/9: Snowpark Snapshot
# MAGIC
# MAGIC Publishes date-stamped master snapshot tables (HCP + HCO) using
# MAGIC publish_master_snapshot.py.
# MAGIC
# MAGIC Snowflake credentials are fetched from AWS Secrets Manager in this
# MAGIC Python process and passed explicitly as environment variables to the
# MAGIC Snowpark subprocess. This is necessary because credentials exported by
# MAGIC run_dbt.sh (Stage 8) inside its own shell process do not propagate back
# MAGIC to this parent Python process.
# MAGIC
# MAGIC Repo root is resolved dynamically from the notebook context (no
# MAGIC hard-coded paths).

# COMMAND ----------

# DBTITLE 1,Stage 9 - Snowpark Snapshot
print("STAGE 9/9: Snowpark snapshot (publish master snapshots)")

# Resolve repo root dynamically (same pattern as Stage 8 -- no hard-coded paths).
import os
import subprocess
import json
from pathlib import Path

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = Path("/Workspace" + _notebook_path).parent.parent
snowpark_script = str(_repo_root / "snowflake" / "snowpark" / "publish_master_snapshot.py")

# Fetch Snowflake credentials from AWS Secrets Manager and pass them
# explicitly to the Snowpark subprocess as environment variables.
#
# run_dbt.sh (Stage 8) exports SNOWFLAKE_* env vars inside its own shell
# process, but those do NOT propagate back to this Python parent process.
# So we fetch credentials here using the same boto3 + Databricks service
# credential pattern, then pass them via env= to the subprocess.
def _get_snowflake_env():
    """Fetch Snowflake credentials from AWS Secrets Manager for subprocess env."""
    import boto3
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    _spark = SparkSession.builder.getOrCreate()
    _dbutils = DBUtils(_spark)
    session = boto3.Session(
        botocore_session=_dbutils.credentials.getServiceCredentialsProvider(
            "healthcare_mdm_secrets_credential"
        ),
        region_name="us-east-1",
    )
    sm = session.client("secretsmanager")
    resp = sm.get_secret_value(SecretId="healthcare-mdm/dev/api-snowflake")
    secret = json.loads(resp["SecretString"])
    return {
        "SNOWFLAKE_ACCOUNT": secret["snowflake_account"].replace(".snowflakecomputing.com", ""),
        "SNOWFLAKE_USER": secret["snowflake_user"],
        "SNOWFLAKE_PASSWORD": secret["snowflake_password"],
        "SNOWFLAKE_WAREHOUSE": secret["snowflake_warehouse"],
        "SNOWFLAKE_DATABASE": secret["snowflake_database"],
        "SNOWFLAKE_SCHEMA": secret["snowflake_schema"],
        "SNOWFLAKE_ROLE": secret.get("snowflake_role", "HMDM_DEV_ROLE"),
    }

# Build env for Snowpark subprocess: inherit parent env + add Snowflake creds.
env = os.environ.copy()
env.update(_get_snowflake_env())
print("Snowflake credentials loaded for Snowpark subprocess")

for entity in ["HCP", "HCO"]:
    result = subprocess.run(
        ["python", snowpark_script, "--entity", entity],
        capture_output=True, text=True, timeout=300, env=env,
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

