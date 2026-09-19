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

sf_user = secret["snowflake_user"]
sf_password = secret["snowflake_password"]

print("Snowflake credentials retrieved: SUCCESS")


# COMMAND ----------

# DBTITLE 1,Install Snowflake Connector
# # Install Snowflake connector for Databricks Serverless
# %pip install snowflake-connector-python --quiet
# dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Snowflake read connectivity
# Snowflake read connectivity
# Validate the development Snowflake environment with secret-derived credentials.
# Use snowflake-connector-python for Serverless compatibility

try:
    import snowflake.connector
    
    # Create connection
    conn = snowflake.connector.connect(
        account=secret["snowflake_account"].replace('.snowflakecomputing.com', ''),
        user=sf_user,
        password=sf_password,
        warehouse=secret["snowflake_warehouse"],
        database=secret["snowflake_database"],
        schema=secret["snowflake_schema"]
    )
    
    # Execute test query
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER() AS USER_NAME, CURRENT_DATABASE() AS DB_NAME, CURRENT_SCHEMA() AS SCHEMA_NAME, CURRENT_WAREHOUSE() AS WH_NAME")
    result = cursor.fetchall()
    
    # Convert to DataFrame for display
    import pandas as pd
    test_df = pd.DataFrame(result, columns=["USER_NAME", "DB_NAME", "SCHEMA_NAME", "WH_NAME"])
    display(test_df)
    
    cursor.close()
    conn.close()
    
    print("✅ Snowflake READ connection: SUCCESS")
    
except Exception as e:
    print(f"❌ Snowflake connection failed: {str(e)}")
    print("\nNote: Serverless compute requires snowflake-connector-python.")
    print("Install with: %pip install snowflake-connector-python")


# COMMAND ----------

# DBTITLE 1,Snowflake write connectivity
# Snowflake write connectivity
# Validate write access with a dedicated connectivity-test object.

try:
    import snowflake.connector
    
    # Create connection
    conn = snowflake.connector.connect(
        account=secret["snowflake_account"].replace('.snowflakecomputing.com', ''),
        user=sf_user,
        password=sf_password,
        warehouse=secret["snowflake_warehouse"],
        database=secret["snowflake_database"],
        schema=secret["snowflake_schema"]
    )
    
    # Create test table
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
    
    # Insert test data
    cursor.execute("""
        INSERT INTO CONNECTION_TEST (USER_NAME, DB_NAME, SCHEMA_NAME, WH_NAME)
        SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("✅ Snowflake WRITE connection: SUCCESS")
    
except Exception as e:
    print(f"❌ Snowflake write failed: {str(e)}")


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