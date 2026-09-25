# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Stage 8: dbt + Snowpark
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management -- Stage 8: dbt Run + Tests + Snowpark Snapshot
# MAGIC
# MAGIC This notebook is the final Snowflake-side post-processing stage. It runs
# MAGIC after Snowflake Sync (09) has pushed all Databricks data to Snowflake.
# MAGIC
# MAGIC **Three steps in order:**
# MAGIC
# MAGIC 1. **dbt run** -- Creates 23 staging views and 16 marts tables in Snowflake.
# MAGIC    dbt reads from Snowflake STAGING and MASTER tables (populated by Stage 7)
# MAGIC    and builds transformation models (joins, column renames, JSON extraction).
# MAGIC 2. **dbt tests** -- Runs not_null Source_FK tests on all 23 staging models
# MAGIC    and 16 marts models. Ensures data integrity before downstream use.
# MAGIC 3. **Snowpark snapshot** -- Publishes date-stamped master snapshot tables
# MAGIC    (HCP + HCO) so consumers can query point-in-time master data.
# MAGIC
# MAGIC **When to run:** AFTER Stage 7 (Snowflake Sync) completes.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC * Snowflake Sync (09) completed -- Snowflake STAGING and MASTER tables populated
# MAGIC * dbt project at `dbt/` folder with `dbt_project.yml` and `profiles.yml`
# MAGIC * Snowflake credentials in AWS Secrets Manager (`healthcare-mdm/dev/api-snowflake`)
# MAGIC * Snowpark script at `snowflake/snowpark/publish_master_snapshot.py`
# MAGIC * External dependency: Informatica MDM Hub tables (HMDM_DEV.MDM.*) must exist
# MAGIC   for HCP child master models to succeed

# COMMAND ----------

# DBTITLE 1,Install dbt
# MAGIC %md
# MAGIC #### 1. Install dbt
# MAGIC
# MAGIC Installs `dbt-snowflake` on Serverless compute if not already present.
# MAGIC `dbutils.library.restartPython()` is called to make the package available.
# MAGIC This runs BEFORE any variable definitions, so the kernel restart does not
# MAGIC lose any state.

# COMMAND ----------

# DBTITLE 1,Install dbt
# Install dbt-snowflake if not already present
import subprocess

try:
    subprocess.run(["dbt", "--version"], capture_output=True, check=True)
    print("dbt already installed")
except (FileNotFoundError, subprocess.CalledProcessError):
    print("dbt not found -- installing dbt-snowflake...")
    subprocess.run(["pip", "install", "dbt-snowflake", "--quiet"], check=True)
    print("dbt-snowflake installed")
    dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,dbt Run
# MAGIC %md
# MAGIC #### 2. dbt Run
# MAGIC
# MAGIC Runs dbt models (staging views + marts tables) on Snowflake using `run_dbt.sh`.
# MAGIC This creates 23 staging views and 16 marts tables in Snowflake.
# MAGIC
# MAGIC Repo root is resolved dynamically from the notebook context (no hard-coded
# MAGIC paths). dbt is installed if not already present. The `--profiles-dir` argument
# MAGIC is set to the dbt directory absolute path, and `cwd=dbt_dir` ensures dbt finds
# MAGIC `profiles.yml` in the right place.
# MAGIC
# MAGIC **What dbt creates:**
# MAGIC * **STAGING schema (23 views):** Thin views over Snowflake STAGING tables.
# MAGIC   Maps `iqvia_id` as `Source_FK`, extracts attributes from `response_json`
# MAGIC   using `PARSE_JSON`.
# MAGIC * **MDM schema (16 tables):** `mdm_hcp` (joins 14 staging views on Source_FK),
# MAGIC   `master_hcp` (renames columns), 4 HCP child models (read from mdm_hub source),
# MAGIC   `mdm_hco` (joins 5 staging views), 5 HCO child models.
# MAGIC
# MAGIC **External dependency:** HCP child master models (master_hcp_specialty,
# MAGIC master_hcp_alternate_name, master_hcp_license, master_hcp_therapeutic_area)
# MAGIC read from `source('mdm_hub', ...)` which are HMDM_DEV.MDM.* tables created
# MAGIC by the Informatica MDM Hub, NOT by this repository.

# COMMAND ----------

# DBTITLE 1,dbt Run
print("STAGE 8: dbt run (Snowflake staging views + marts tables)")

# Resolve repo root dynamically from notebook context (no hard-coded paths).
import os
import subprocess
import json
from pathlib import Path

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
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

# Fetch Snowflake credentials from AWS Secrets Manager and pass them
# as environment variables to the dbt subprocess.
# run_dbt.sh tries to use AWS CLI to fetch credentials, but AWS CLI is
# not available on Databricks Serverless compute. So we fetch credentials
# here using boto3 + Databricks service credential, then pass them via
# env= to the subprocess (same pattern as the Snowpark snapshot cell).
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
        "SNOWFLAKE_ROLE": secret["snowflake_role"],
    }

# Build env for dbt subprocess: inherit parent env + add Snowflake creds.
env = os.environ.copy()
env.update(_get_snowflake_env())
print("Snowflake credentials loaded for dbt subprocess")

# Run dbt with cwd=dbt_dir so --profiles-dir . resolves to the dbt folder.
result = subprocess.run(
    [dbt_script, "dbt", "run", "--target", "dev", "--profiles-dir", dbt_dir],
    capture_output=True, text=True, timeout=600, cwd=dbt_dir, env=env,
)
if result.returncode != 0:
    print(f"dbt run FAILED (returncode={result.returncode}):")
    print(f"STDERR:\n{result.stderr[-3000:]}")
    print(f"STDOUT:\n{result.stdout[-3000:]}")
    raise RuntimeError(f"dbt run failed with returncode {result.returncode}")
print(f"dbt run SUCCESS: {result.stdout[-500:]}")

# COMMAND ----------

# DBTITLE 1,dbt Tests
# MAGIC %md
# MAGIC #### 3. dbt Tests
# MAGIC
# MAGIC Runs dbt tests (not_null Source_FK checks) on all 23 staging models and
# MAGIC 16 marts models defined in `schema.yml` files.
# MAGIC
# MAGIC This ensures that every model has a non-null `Source_FK` column -- the
# MAGIC join key used by all marts models. If any test fails, the pipeline stops
# MAGIC here so downstream consumers do not receive models with missing keys.
# MAGIC
# MAGIC **Test results:**
# MAGIC * PASS = all not_null tests passed
# MAGIC * FAIL = one or more models have NULL Source_FK values (data quality issue)

# COMMAND ----------

# DBTITLE 1,dbt Tests
print("STAGE 8: dbt tests (not_null Source_FK checks)")

# Resolve paths (same pattern as dbt run cell above).
import os
import subprocess
import json
from pathlib import Path

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = Path("/Workspace" + _notebook_path).parent.parent
dbt_dir = str(_repo_root / "dbt")
dbt_script = os.path.join(dbt_dir, "run_dbt.sh")

# Fetch Snowflake credentials and pass via env (same as dbt run cell).
def _get_snowflake_env():
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
        "SNOWFLAKE_ROLE": secret["snowflake_role"],
    }

env = os.environ.copy()
env.update(_get_snowflake_env())

# Run dbt test with the same profile/target as dbt run.
result = subprocess.run(
    [dbt_script, "dbt", "test", "--target", "dev", "--profiles-dir", dbt_dir],
    capture_output=True, text=True, timeout=600, cwd=dbt_dir, env=env,
)

# dbt test exit code: 0 = all pass, 1 = some fail, 2 = error
if result.returncode != 0:
    print(f"dbt tests FAILED (exit code {result.returncode}):")
    print(f"STDERR:\n{result.stderr[-2000:]}")
    print(f"STDOUT:\n{result.stdout[-2000:]}")
    raise RuntimeError(f"dbt tests failed with exit code {result.returncode}")

print(f"dbt tests SUCCESS: all not_null Source_FK tests passed")
print(result.stdout[-500:])

# COMMAND ----------

# DBTITLE 1,Snowpark Snapshot
# MAGIC %md
# MAGIC #### 4. Snowpark Snapshot
# MAGIC
# MAGIC Publishes date-stamped master snapshot tables (HCP + HCO) using
# MAGIC `publish_master_snapshot.py`.
# MAGIC
# MAGIC Snowflake credentials are fetched from AWS Secrets Manager in this Python
# MAGIC process and passed explicitly as environment variables to the Snowpark
# MAGIC subprocess. This is necessary because credentials exported by `run_dbt.sh`
# MAGIC inside its own shell process do not propagate back to this parent Python
# MAGIC process.
# MAGIC
# MAGIC Repo root is resolved dynamically from the notebook context (no hard-coded
# MAGIC paths).
# MAGIC
# MAGIC **What it creates:**
# MAGIC * `MASTER.master_hcp_snapshot_YYYYMMDD` -- snapshot of master_hcp
# MAGIC * `MASTER.master_hco_snapshot_YYYYMMDD` -- snapshot of master_hco
# MAGIC
# MAGIC These snapshots allow consumers to query point-in-time master data without
# MAGIC affecting the live master tables.

# COMMAND ----------

# DBTITLE 1,Snowpark Snapshot
print("STAGE 8: Snowpark snapshot (publish master snapshots)")

# Resolve repo root dynamically (same pattern as dbt cells -- no hard-coded paths).
import os
import subprocess
import json
import sys
from pathlib import Path

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = Path("/Workspace" + _notebook_path).parent.parent
snowpark_script = str(_repo_root / "snowflake" / "snowpark" / "publish_master_snapshot.py")

# Fetch Snowflake credentials from AWS Secrets Manager and pass them
# explicitly to the Snowpark subprocess as environment variables.
#
# run_dbt.sh exports SNOWFLAKE_* env vars inside its own shell process,
# but those do NOT propagate back to this Python parent process.
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
        "SNOWFLAKE_ROLE": secret["snowflake_role"],
    }

# Build env for Snowpark subprocess: inherit parent env + add Snowflake creds.
env = os.environ.copy()
env.update(_get_snowflake_env())
print("Snowflake credentials loaded for Snowpark subprocess")

# Install snowflake-snowpark-python if not already present (required by the script).
# Use subprocess pip install so the package is available to sys.executable
# (the subprocess Python), not just the notebook kernel.
install_result = subprocess.run(
    [sys.executable, "-m", "pip", "install", "snowflake-snowpark-python", "--quiet"],
    capture_output=True, text=True, timeout=120,
)
if install_result.returncode == 0:
    print("snowflake-snowpark-python installed/confirmed")
else:
    print(f"pip install warning: {install_result.stderr[-200:]}")
    # Try anyway -- might already be installed
    pass

for entity in ["HCP", "HCO"]:
    result = subprocess.run(
        [sys.executable, snowpark_script, "--entity", entity],
        capture_output=True, text=True, timeout=300, env=env,
    )
    if result.returncode != 0:
        print(f"Snowpark snapshot {entity} FAILED:")
        print(f"STDERR:\n{result.stderr[-3000:]}")
        print(f"STDOUT:\n{result.stdout[-3000:]}")
        raise RuntimeError(f"Snowpark {entity} snapshot failed")
    print(f"Snowpark {entity} snapshot SUCCESS")
    print(result.stdout[-500:])

# COMMAND ----------

# DBTITLE 1,Result
# MAGIC %md
# MAGIC #### 5. Result
# MAGIC
# MAGIC If all three steps (dbt run, dbt tests, Snowpark snapshot) completed
# MAGIC without error, exits with `SUCCESS`. Any step failure raises an exception
# MAGIC and stops the pipeline.

# COMMAND ----------

# DBTITLE 1,Result
print("\n" + "=" * 60)
print("STAGE 8: dbt + Snowpark COMPLETE")
print("=" * 60)
print("\nFull pipeline completed successfully -- all stages passed.")
dbutils.notebook.exit("SUCCESS")