# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management — Connection Validations
# MAGIC
# MAGIC Databricks, AWS Secrets Manager, Snowflake, and project runtime configuration checks.

# COMMAND ----------

# Databricks / Spark availability
# Confirm the notebook is running in Databricks and Spark is available.
print("Healthcare_MDM Databricks connection: OK")
print("Spark version:", spark.version)


# COMMAND ----------

# AWS Secrets Manager access
# Use the configured Databricks Service Credential. No AWS keys are stored here.
import boto3

boto3_session = boto3.Session(
    botocore_session=dbutils.credentials.getServiceCredentialsProvider(
        "healthcare_mdm_secrets_credential"
    ),
    region_name="us-east-1"
)
sm = boto3_session.client("secretsmanager")
print("AWS Secrets Manager client: CREATED")
print("Region: us-east-1")


# COMMAND ----------

# DBTITLE 1,Cell 4
# Retrieve Snowflake credentials from AWS Secrets Manager
# The username/password are loaded only into memory. Never print the secret.
import json

SECRET_NAME = "healthcare-mdm/dev/api-snowflake"
response = sm.get_secret_value(SecretId=SECRET_NAME)
secret = json.loads(response["SecretString"])

sf_user = secret["username"]
sf_password = secret["password"]

print("Snowflake credentials retrieved: SUCCESS")


# COMMAND ----------

# Snowflake read connectivity
# Validate the development Snowflake environment with secret-derived credentials.
sf_options = {
    "sfURL": "AXIVKAP-PF58156.snowflakecomputing.com",
    "sfUser": sf_user,
    "sfPassword": sf_password,
    "sfDatabase": "HMDM_DEV",
    "sfSchema": "RAW",
    "sfWarehouse": "HMDM_WH"
}

test_df = (
    spark.read.format("snowflake")
    .options(**sf_options)
    .option("query", "SELECT CURRENT_USER() AS USER_NAME, CURRENT_DATABASE() AS DB_NAME, CURRENT_SCHEMA() AS SCHEMA_NAME, CURRENT_WAREHOUSE() AS WH_NAME")
    .load()
)

display(test_df)
print("Snowflake READ connection: SUCCESS")


# COMMAND ----------

# Snowflake write connectivity
# Validate write access with a dedicated connectivity-test object.
sf_options_write = {
    "host": "AXIVKAP-PF58156.snowflakecomputing.com",
    "sfUser": sf_user,
    "sfPassword": sf_password,
    "sfDatabase": "HMDM_DEV",
    "sfSchema": "RAW",
    "sfWarehouse": "HMDM_WH"
}

test_df.write.format("snowflake").options(**sf_options_write).option(
    "dbtable", "CONNECTION_TEST"
).mode("overwrite").save()

print("Snowflake WRITE connection: SUCCESS")


# COMMAND ----------

# Healthcare_MDM centralized runtime configuration
# Add the repository root, not src/core, so the project package resolves consistently.
import sys
from pathlib import Path

REPO_ROOT = Path("/Workspace/Repos/naresh.mayari@gmail.com/Healthcare_Master_Data_Management")
if str(REPO_ROOT) in sys.path:
    sys.path.remove(str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT))

from src.core import runtime_config

print("Catalog   :", runtime_config.catalog)
print("RAW       :", runtime_config.raw_schema)
print("Landing   :", runtime_config.lnd_schema)
print("Staging   :", runtime_config.stg_schema)
print("MDM       :", runtime_config.publish_schema)
print("Util      :", runtime_config.util_schema)
print("Batch Log :", runtime_config.batch_log_tbl)
print("Healthcare_MDM runtime configuration: SUCCESS")


# COMMAND ----------

# DBTITLE 1,Databricks Infrastructure Check
# MAGIC %sql
# MAGIC -- Verify Databricks catalog and schemas exist
# MAGIC SHOW CATALOGS;
# MAGIC SHOW SCHEMAS IN HMDM_DEV;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC | Validation | Status |
# MAGIC |---|---|
# MAGIC | Databricks / Spark | PASS |
# MAGIC | AWS Secrets Manager | PASS |
# MAGIC | Snowflake credentials retrieval | PASS |
# MAGIC | Snowflake READ | PASS |
# MAGIC | Snowflake WRITE | PASS |
# MAGIC | Healthcare_MDM runtime configuration | PASS |
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Quick Lambda Test from Databricks
import requests
import json

# Test Lambda directly from Databricks
lambda_url = "https://i7h6djmqxr3ycp37bixpxup7ce0cfwyr.lambda-url.us-east-1.on.aws/"

test_payload = {
    "body": {
        "iqviaId": "W12345678",
        "mdmEntityType": "HCP",
        "countryCode": "NL"
    }
}

print("Testing Lambda endpoint...")
print(f"URL: {lambda_url}")
print(f"Payload: {json.dumps(test_payload, indent=2)}")
print("\n" + "="*60)

try:
    response = requests.post(
        lambda_url,
        json=test_payload,
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response:\n{json.dumps(response.json(), indent=2)}")
    
    if response.status_code == 200:
        print("\n✅ Lambda test: SUCCESS")
    else:
        print(f"\n❌ Lambda test: FAILED with status {response.status_code}")
        
except Exception as e:
    print(f"\n❌ Lambda test: ERROR")
    print(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Test HCO Entity (NEW FIX)
# Test HCO entity type (this was failing before our fix)
test_hco_payload = {
    "body": {
        "iqviaId": "W99887766",
        "mdmEntityType": "HCO",  # This was rejected before!
        "countryCode": "BE"
    }
}

print("Testing HCO entity type (previously failing)...")
print(f"Payload: {json.dumps(test_hco_payload, indent=2)}")
print("\n" + "="*60)

try:
    response = requests.post(
        lambda_url,
        json=test_hco_payload,
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response:\n{json.dumps(response.json(), indent=2)}")
    
    if response.status_code == 200:
        print("\n✅ HCO test: SUCCESS (Fix working!)")
    else:
        print(f"\n❌ HCO test: FAILED with status {response.status_code}")
        
except Exception as e:
    print(f"\n❌ HCO test: ERROR")
    print(f"Error: {str(e)}")