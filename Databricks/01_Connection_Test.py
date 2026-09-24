# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management -- Connection Validations
# MAGIC

# COMMAND ----------

# DBTITLE 1,Why: Install Snowflake
# MAGIC %md
# MAGIC #### Install Snowflake Connector
# MAGIC
# MAGIC The `snowflake-connector-python` package is required to connect to Snowflake. It is not pre-installed on Serverless compute, so we install it here. `restartPython()` is called to make the package available. This cell is placed first so the restart does not clear any variables defined later.

# COMMAND ----------

# DBTITLE 1,Install Snowflake Connector
# Install Snowflake connector (required on Serverless -- not pre-installed)
# restartPython() runs BEFORE any variable definitions, so state loss is harmless.
%pip install snowflake-connector-python --quiet
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Why: Spark Check
# MAGIC %md
# MAGIC #### Databricks / Spark Check
# MAGIC
# MAGIC Confirms the notebook is running in a Databricks environment with Spark available. Prints the Spark version as a simple validation. This is the base check -- if Spark is not available, remaining cells will not work.

# COMMAND ----------

# Databricks / Spark availability
# Confirm the notebook is running in Databricks and Spark is available.
print("Healthcare_MDM Databricks connection: OK")
print("Spark version:", spark.version)


# COMMAND ----------

# DBTITLE 1,Why: AWS Secrets
# MAGIC %md
# MAGIC #### AWS Secrets Manager Connection
# MAGIC
# MAGIC Connects to AWS Secrets Manager to retrieve Snowflake credentials. Uses Databricks service credential (`dbutils.credentials.getServiceCredentialsProvider`) -- no AWS keys are stored in the code.

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

# DBTITLE 1,Why: Get Credentials
# MAGIC %md
# MAGIC #### Retrieve Snowflake Credentials
# MAGIC
# MAGIC Reads Snowflake account details from AWS Secrets Manager -- user, password, warehouse, database, and schema. These stay in memory only and are never printed. The secret contains: `snowflake_user`, `snowflake_password`, `snowflake_account`, `snowflake_warehouse`, `snowflake_database`, `snowflake_schema`.

# COMMAND ----------

# DBTITLE 1,Cell 4
# Retrieve Snowflake credentials from AWS Secrets Manager
# The username/password are loaded only into memory. Never print the secret.
import json

SECRET_NAME = "healthcare-mdm/dev/api-snowflake"
response = sm.get_secret_value(SecretId=SECRET_NAME)
secret = json.loads(response["SecretString"])

sf_user = secret["snowflake_user"]
sf_password = secret["snowflake_password"]

print("Snowflake credentials retrieved: SUCCESS")


# COMMAND ----------

# DBTITLE 1,Why: Snowflake READ
# MAGIC %md
# MAGIC #### Snowflake READ Connectivity Test
# MAGIC
# MAGIC Connects to Snowflake and runs a simple `SELECT` query to verify the connection works and credentials are correct. Results are displayed in a table. Confirms read permission is available.

# COMMAND ----------

# DBTITLE 1,Snowflake read connectivity
# Snowflake read connectivity
# Validate the development Snowflake environment with secret-derived credentials.
# Use snowflake-connector-python for Serverless compatibility

import snowflake.connector

# Create connection (eager -- raises immediately on failure)
conn = snowflake.connector.connect(
    account=secret["snowflake_account"].replace('.snowflakecomputing.com', ''),
    user=sf_user,
    password=sf_password,
    warehouse=secret["snowflake_warehouse"],
    database=secret["snowflake_database"],
    schema=secret["snowflake_schema"]
)

try:
    # Execute test query (eager -- raises on SQL error)
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER() AS USER_NAME, CURRENT_DATABASE() AS DB_NAME, CURRENT_SCHEMA() AS SCHEMA_NAME, CURRENT_WAREHOUSE() AS WH_NAME")
    result = cursor.fetchall()
    
    # Convert to DataFrame for display
    import pandas as pd
    test_df = pd.DataFrame(result, columns=["USER_NAME", "DB_NAME", "SCHEMA_NAME", "WH_NAME"])
    display(test_df)
    
    cursor.close()
    conn.close()
    
    print("Snowflake READ connection: SUCCESS")
    
except Exception as e:
    if 'conn' in dir() and conn:
        conn.close()
    print(f"Snowflake connection failed: {str(e)}")
    print("\nNote: Serverless compute requires snowflake-connector-python.")
    print("Install with: %pip install snowflake-connector-python")

# COMMAND ----------

# DBTITLE 1,Why: Snowflake WRITE
# MAGIC %md
# MAGIC #### Snowflake WRITE Connectivity Test
# MAGIC
# MAGIC Connects to Snowflake and runs `CREATE TABLE` and `INSERT` statements to verify write access. Creates a temporary test table, inserts a row, then closes the connection. Confirms write permission is available.

# COMMAND ----------

# DBTITLE 1,Snowflake write connectivity
# Snowflake write connectivity
# Validate write access with a dedicated connectivity-test object.

import snowflake.connector

# Create connection (eager -- raises immediately on failure)
conn = snowflake.connector.connect(
    account=secret["snowflake_account"].replace('.snowflakecomputing.com', ''),
    user=sf_user,
    password=sf_password,
    warehouse=secret["snowflake_warehouse"],
    database=secret["snowflake_database"],
    schema=secret["snowflake_schema"]
)

try:
    # Create test table (eager -- raises on SQL error)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE OR REPLACE TABLE CONNECTION_TEST (
            USER_NAME VARCHAR,
            DB_NAME VARCHAR,
            SCHEMA_NAME VARCHAR,
            WH_NAME VARCHAR,
            TEST_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    
    # Insert test data (eager -- raises on SQL error)
    cursor.execute("""
        INSERT INTO CONNECTION_TEST (USER_NAME, DB_NAME, SCHEMA_NAME, WH_NAME)
        SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Snowflake WRITE connection: SUCCESS")
    
except Exception as e:
    if 'conn' in dir() and conn:
        conn.close()
    print(f"Snowflake write failed: {str(e)}")


# COMMAND ----------

# DBTITLE 1,Why: SQL Check
# MAGIC %md
# MAGIC #### Databricks Infrastructure Check
# MAGIC
# MAGIC Lists all catalogs and schemas in the HMDM_DEV catalog to confirm the Databricks infrastructure is set up correctly. Verifies that required schemas (raw, landing, canonical, staging, mdm, master, util) exist.

# COMMAND ----------

# DBTITLE 1,Databricks Infrastructure Check
# MAGIC %sql
# MAGIC -- Verify Databricks catalog and schemas exist
# MAGIC SHOW CATALOGS;
# MAGIC SHOW SCHEMAS IN HMDM_DEV;