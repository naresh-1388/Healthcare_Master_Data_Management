#!/usr/bin/env bash
# ============================================================
# dbt runner -- fetches Snowflake credentials from AWS Secrets
# Manager, exports as env vars, then runs dbt.
#
# No credentials are hardcoded. The same AWS Secrets Manager
# secret used by 01_Connection_Test and push_to_snowflake.py
# is used here: 'healthcare-mdm/dev/api-snowflake'.
#
# Usage:
#   ./run_dbt.sh dbt run --profiles-dir .
#   ./run_dbt.sh dbt run --select stg_HCP_NAME --profiles-dir .
#   ./run_dbt.sh dbt test --profiles-dir .
#   ./run_dbt.sh dbt docs generate --profiles-dir .
#
# Prerequisites:
#   - dbt installed (pip install dbt-snowflake)
#     NOTE: The full pipeline notebook (09_Run_Full_Pipeline.py Stage 8)
#     installs dbt-snowflake automatically if it is not already present.
#   - AWS CLI configured with access to Secrets Manager
#     OR running inside Databricks (uses boto3 + service credential)
#   - jq installed (for JSON parsing)
#     OR Python 3 (fallback when jq is not available)
#   - Databricks service credential 'healthcare_mdm_secrets_credential'
#     OR AWS credentials with secretsmanager:GetSecretValue
# ============================================================
set -euo pipefail

SECRET_NAME="healthcare-mdm/dev/api-snowflake"
REGION="us-east-1"

# ---- Fetch credentials from AWS Secrets Manager ----
# Skip if Snowflake credentials are already set in environment
# (e.g., when called from a Databricks notebook that fetched them via boto3).
if [ -n "${SNOWFLAKE_ACCOUNT:-}" ] && [ -n "${SNOWFLAKE_PASSWORD:-}" ]; then
    echo "Snowflake credentials already set in environment -- skipping AWS fetch"
else
    echo "Fetching Snowflake credentials from AWS Secrets Manager..."

    if command -v aws &>/dev/null; then
        SECRET_JSON=$(aws secretsmanager get-secret-value \
            --secret-id "$SECRET_NAME" \
            --region "$REGION" \
            --query SecretString \
            --output text)
    elif [ -n "${DATABRICKS_TOKEN:-}" ]; then
        # Running inside Databricks -- use boto3 via Python
        SECRET_JSON=$(python3 -c "
import boto3, json
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
session = boto3.Session(
    botocore_session=dbutils.credentials.getServiceCredentialsProvider(
        'healthcare_mdm_secrets_credential'
    ),
    region_name='$REGION'
)
sm = session.client('secretsmanager')
resp = sm.get_secret_value(SecretId='$SECRET_NAME')
print(resp['SecretString'])
")
    else
        echo "ERROR: AWS CLI not found and not running in Databricks."
        echo "Install AWS CLI or run from Databricks notebook."
        echo "Or set SNOWFLAKE_* environment variables before calling this script."
        exit 1
    fi
fi

# ---- Parse and export as environment variables ----
# Only parse if credentials were fetched from AWS (not pre-set in env).
if [ -z "${SNOWFLAKE_ACCOUNT:-}" ] || [ -z "${SNOWFLAKE_PASSWORD:-}" ]; then
    # Use jq if available; fall back to Python (jq is not guaranteed on
    # Databricks Serverless compute).
    if command -v jq &>/dev/null; then
    export SNOWFLAKE_ACCOUNT=$(echo "$SECRET_JSON" | jq -r '.snowflake_account' | sed 's/\.snowflakecomputing\.com//')
    export SNOWFLAKE_USER=$(echo "$SECRET_JSON" | jq -r '.snowflake_user')
    export SNOWFLAKE_PASSWORD=$(echo "$SECRET_JSON" | jq -r '.snowflake_password')
    export SNOWFLAKE_WAREHOUSE=$(echo "$SECRET_JSON" | jq -r '.snowflake_warehouse')
    export SNOWFLAKE_DATABASE=$(echo "$SECRET_JSON" | jq -r '.snowflake_database')
    export SNOWFLAKE_SCHEMA=$(echo "$SECRET_JSON" | jq -r '.snowflake_schema')
else
    echo "jq not found -- using Python for JSON parsing"
    eval "$(echo "$SECRET_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
account = d.get('snowflake_account', '').replace('.snowflakecomputing.com', '')
for key in ['account', 'user', 'password', 'warehouse', 'database', 'schema']:
    val = d.get(f'snowflake_{key}', '')
    if key == 'account':
        val = account
    print(f\"export SNOWFLAKE_{key.upper()}='{val}'\")
")"
    fi
fi
export SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-HMDM_DEV_ROLE}"

echo "Credentials loaded: account=$SNOWFLAKE_ACCOUNT warehouse=$SNOWFLAKE_WAREHOUSE"
echo "Running: $*"

# ---- Run dbt ----
exec "$@"