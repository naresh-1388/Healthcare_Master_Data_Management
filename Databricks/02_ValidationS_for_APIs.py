# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ============================================================
# INITIALIZATION — SOURCE PATH AND COMMON IMPORTS
# ============================================================
# This is the single initialization/import cell for the notebook.
# Do not repeat project imports inside individual test cells.
#
# The repository root is added first so that the project can be
# imported as the `src` package. This avoids collisions with any
# unrelated Databricks/Python package named `api`.
# ============================================================

import sys
import importlib
from pathlib import Path

# Resolve the repo root relative to this notebook's own location instead
# of hard-coding one person's /Workspace/Repos/<email>/... path, so this
# notebook works unmodified in any teammate's workspace or a Databricks Job.
NOTEBOOK_DIR = Path(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    .notebookPath().get()
).parent
REPO_ROOT = Path("/Workspace" + str(NOTEBOOK_DIR.parent))
SRC_ROOT = REPO_ROOT / "src"

assert SRC_ROOT.exists(), (
    f"Healthcare_MDM src directory was not found: {SRC_ROOT}. "
    "This notebook expects to live in <repo_root>/Databricks/ - if you "
    "moved it elsewhere, set REPO_ROOT explicitly instead."
)

# Put the repository root first so `src.*` resolves to this project.
repo_path = str(REPO_ROOT)
if repo_path in sys.path:
    sys.path.remove(repo_path)
sys.path.insert(0, repo_path)

# Import only modules required by the validation tests.
# This intentionally avoids importing API modules that can require
# runtime environment variables or AWS Secrets Manager credentials.
from src.api import transform_to_iqvia
from src.api import transform_to_mdm_hub
from src.dq import data_quality
from src.core import runtime_config

# Reload the modules so the notebook uses the current Git repository code.
importlib.reload(transform_to_iqvia)
importlib.reload(transform_to_mdm_hub)
importlib.reload(data_quality)
importlib.reload(runtime_config)

print("Healthcare_MDM source path:", SRC_ROOT)
print("Common project imports: PASS")


# COMMAND ----------

# DBTITLE 1,Infrastructure Verification for Tests
# MAGIC %md
# MAGIC # Infrastructure Verification
# MAGIC
# MAGIC **This section verifies that required test infrastructure exists:**
# MAGIC - Test schemas: `staging`, `util`
# MAGIC - Test tables: DQ test table, control tables
# MAGIC - Test data: TEST_HCP records
# MAGIC
# MAGIC **Safe to re-run:** All checks and creation statements are idempotent.

# COMMAND ----------

# DBTITLE 1,Display Test Infrastructure
# MAGIC %sql
# MAGIC -- Display all schemas in catalog
# MAGIC SHOW SCHEMAS IN HMDM_DEV;
# MAGIC
# MAGIC -- Display control tables in util schema
# MAGIC SHOW TABLES IN HMDM_DEV.util;

# COMMAND ----------

# DBTITLE 1,Verify Test Data Tables Exist
# Verify required test infrastructure
required_test_tables = [
    "HMDM_DEV.staging.hcp_name_dq_test",  # DQ test table
    "HMDM_DEV.util.ctl_batch_log_tbl",    # Batch control
    "HMDM_DEV.util.ctl_log_tbl",           # Audit log
    "HMDM_DEV.util.ctl_dqm_log_tbl",       # DQ log
    "HMDM_DEV.util.dqm_reject_tbl",        # DQ reject
]

print("Checking test infrastructure...\n")

missing_tables = []
for table in required_test_tables:
    exists = spark.catalog.tableExists(table)
    status = "EXISTS" if exists else "MISSING"
    print(f"{status}: {table}")
    if not exists:
        missing_tables.append(table)

if missing_tables:
    print(f"\nWarning: {len(missing_tables)} table(s) missing.")
    print("Some tests may fail. Run setup notebooks first to create test data.")
else:
    print("\nAll required test infrastructure exists!")

# COMMAND ----------

# DBTITLE 1,Runtime Configuration Check
# ============================================================
# ENVIRONMENT / RUNTIME CONFIGURATION CHECK
# ============================================================
# Confirm that the centralized runtime configuration resolves the
# Databricks development environment used for these tests.
# ============================================================

print("Catalog       :", runtime_config.catalog)
print("RAW schema    :", runtime_config.raw_schema)
print("Landing schema:", runtime_config.lnd_schema)
print("Staging schema:", runtime_config.stg_schema)
print("MDM schema    :", runtime_config.publish_schema)
print("Util schema   :", runtime_config.util_schema)
print("DQ config     :", runtime_config.dqm_config_tbl)

assert runtime_config.catalog.upper() == "HMDM_DEV"
assert runtime_config.raw_schema.upper() == "HMDM_DEV.RAW"
assert runtime_config.lnd_schema.upper() == "HMDM_DEV.LANDING"
assert runtime_config.stg_schema.upper() == "HMDM_DEV.STAGING"
assert runtime_config.publish_schema.upper() == "HMDM_DEV.MDM"
assert runtime_config.util_schema.upper() == "HMDM_DEV.UTIL"

print("\nRuntime configuration check: PASS")

# COMMAND ----------

# ============================================================
# PROJECT DQ RULESET CHECK
# ============================================================
# The project workbook contains 24 DQ rules. The implementation
# should retain that rule set.
# ============================================================

print("Configured project DQ rules:", len(data_quality.DQ_RULES))

assert len(data_quality.DQ_RULES) == 24, (
    f"Expected 24 project DQ rules, found {len(data_quality.DQ_RULES)}"
)

print("DQ rule-set validation: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1 — IQVIA transformation: Netherlands positive case

# COMMAND ----------

# ============================================================
# TEST 1 — IQVIA POSITIVE CASE: NETHERLANDS
# ============================================================
# Expected result:
#   NL -> WNL
#
# The input uses the nested structure expected by the project
# transformation function.
# ============================================================

mock_hcp_nl = {
    "hcp": {
        "firstName": "John",
        "middleName": "A",
        "lastName": "Smith",
    },
    "address": {
        "primary": "10 Main Street",
        "countryCode": "NL",
        "city": "Amsterdam",
        "longPostalCode": "1011AB",
        "type": "Primary",
    },
}

iqvia_nl = transform_to_iqvia.transform_to_iqvia(mock_hcp_nl)

assert isinstance(iqvia_nl, dict)
assert iqvia_nl["codBases"] == ["WNL"]

print("IQVIA codBases:", iqvia_nl["codBases"])
print("IQVIA fields:", len(iqvia_nl["fields"]))
print("TEST 1 — IQVIA NL positive case: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2 — IQVIA transformation: Belgium positive case

# COMMAND ----------

# ============================================================
# TEST 2 — IQVIA POSITIVE CASE: BELGIUM
# ============================================================
# Expected result:
#   BE -> WBE
# ============================================================

mock_hcp_be = {
    "hcp": {
        "firstName": "Mary",
        "middleName": "B",
        "lastName": "Jones",
    },
    "address": {
        "primary": "20 Main Street",
        "countryCode": "BE",
        "city": "Brussels",
        "longPostalCode": "1000",
        "type": "Primary",
    },
}

iqvia_be = transform_to_iqvia.transform_to_iqvia(mock_hcp_be)

assert isinstance(iqvia_be, dict)
assert iqvia_be["codBases"] == ["WBE"]

print("IQVIA codBases:", iqvia_be["codBases"])
print("IQVIA fields:", len(iqvia_be["fields"]))
print("TEST 2 — IQVIA BE positive case: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3 — IQVIA transformation: unsupported country negative case

# COMMAND ----------

# ============================================================
# TEST 3 — IQVIA NEGATIVE CASE: UNSUPPORTED COUNTRY
# ============================================================
# US is intentionally outside the project country-to-codBase
# mapping used by the IQVIA transformation.
#
# Expected behavior:
#   The transformation raises IQVIATransformationError.
# ============================================================

mock_hcp_us = {
    "hcp": {
        "firstName": "John",
        "middleName": "A",
        "lastName": "Smith",
    },
    "address": {
        "primary": "10 Main Street",
        "countryCode": "US",
        "city": "New York",
        "longPostalCode": "10001",
        "type": "Primary",
    },
}

try:
    transform_to_iqvia.transform_to_iqvia(mock_hcp_us)
    raise AssertionError(
        "Expected IQVIATransformationError for unsupported country US."
    )
except transform_to_iqvia.IQVIATransformationError as exc:
    print("Expected error:", exc)
    print("TEST 3 — Unsupported country negative case: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4 — IQVIA transformation: missing countryCode negative case

# COMMAND ----------

# ============================================================
# TEST 4 — IQVIA NEGATIVE CASE: MISSING COUNTRY CODE
# ============================================================
# Expected behavior:
#   The transformation raises IQVIATransformationError because
#   address.countryCode is mandatory for codBase selection.
# ============================================================

mock_hcp_missing_country = {
    "hcp": {
        "firstName": "John",
        "middleName": "A",
        "lastName": "Smith",
    },
    "address": {
        "primary": "10 Main Street",
        "city": "Amsterdam",
        "longPostalCode": "1011AB",
        "type": "Primary",
    },
}

try:
    transform_to_iqvia.transform_to_iqvia(mock_hcp_missing_country)
    raise AssertionError(
        "Expected IQVIATransformationError for missing countryCode."
    )
except transform_to_iqvia.IQVIATransformationError as exc:
    print("Expected error:", exc)
    print("TEST 4 — Missing countryCode negative case: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5 — Data Quality: firstName NULL rejection

# COMMAND ----------

# ============================================================
# TEST 5 — DATA QUALITY NEGATIVE CASE
# ============================================================
# Purpose:
#   Execute the TEST_HCP DQ configuration against the dedicated
#   DQ test table where HCP001.firstName is NULL.
#
# Expected result:
#   PASS count   = 2
#   REJECT count = 1
#
# The rejection should be associated with the configured
# first_name_null_check / null_check rule.
# ============================================================

DQ_TEST_TABLE = "HMDM_DEV.staging.hcp_name_dq_test"

assert spark.catalog.tableExists(DQ_TEST_TABLE), (
    f"Expected DQ test table does not exist: {DQ_TEST_TABLE}. "
    "Run the existing TEST_HCP setup before executing Test 5."
)

passed_df, rejected_df = data_quality.execute_source_dq(
    source_identifier="TEST_HCP",
    source_table=DQ_TEST_TABLE,
    source_system_name="TEST",
)

pass_count = passed_df.count()
reject_count = rejected_df.count()

print("PASS count   :", pass_count)
print("REJECT count :", reject_count)

assert pass_count == 2, f"Expected 2 passing records, found {pass_count}"
assert reject_count == 1, f"Expected 1 rejected record, found {reject_count}"

print("\nRejected record:")
display(rejected_df)

print("TEST 5 — Data Quality rejection: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6 — Batch / Control / Audit verification

# COMMAND ----------

# ============================================================
# TEST 6 — BATCH / CONTROL / AUDIT VERIFICATION
# ============================================================
# Purpose:
#   Verify that the existing Databricks control/audit framework
#   is present and that TEST_HCP has progressed through RAW and
#   Standardization.
#
# This test does not create new production control tables.
# ============================================================

from pyspark.sql import functions as F

UTIL = "HMDM_DEV.util"

BATCH_TBL = f"{UTIL}.ctl_batch_log_tbl"
LOG_TBL = f"{UTIL}.ctl_log_tbl"
DQM_LOG_TBL = f"{UTIL}.ctl_dqm_log_tbl"
REJECT_TBL = f"{UTIL}.dqm_reject_tbl"

required_tables = [
    BATCH_TBL,
    LOG_TBL,
    DQM_LOG_TBL,
    REJECT_TBL,
]

print("Checking required control/audit tables...")

for table_name in required_tables:
    exists = spark.catalog.tableExists(table_name)
    print(f"{table_name} -> {'EXISTS' if exists else 'MISSING'}")
    assert exists, f"Required control/audit table is missing: {table_name}"

print("\nControl/audit table existence check: PASS")

# ------------------------------------------------------------
# Verify TEST batch history.
# ------------------------------------------------------------

batch_df = (
    spark.table(BATCH_TBL)
    .filter(F.col("source_system_name") == "TEST")
    .orderBy(F.col("batch_id").desc())
)

assert batch_df.count() > 0, "No TEST batch found in ctl_batch_log_tbl"

print("\nLatest TEST batch:")
display(batch_df.limit(5))

latest_batch = batch_df.limit(1).collect()[0]

print("Batch ID        :", latest_batch["batch_id"])
print("RAW ingestion   :", latest_batch["raw_ingestion_status"])
print("Standardization :", latest_batch["stdz_status"])
print("Canonical       :", latest_batch["canonical_status"])
print("DQ              :", latest_batch["dq_status"])
print("Ingress         :", latest_batch["ingress_status"])
print("Egress          :", latest_batch["egress_status"])

assert latest_batch["raw_ingestion_status"] == "Y"
assert latest_batch["stdz_status"] == "Y"

print("\nBatch progression check (RAW -> Standardization): PASS")

# ------------------------------------------------------------
# Verify pipeline audit logs.
# ------------------------------------------------------------

log_df = (
    spark.table(LOG_TBL)
    .filter(F.col("source_system_name") == "TEST")
    .orderBy(F.col("start_time").desc())
)

assert log_df.count() > 0, "No pipeline audit log found for TEST"

print("\nRecent pipeline audit logs:")
display(log_df.limit(10))

print("Pipeline audit log check: PASS")

# ------------------------------------------------------------
# Verify DQM log and rejection table structures.
# ------------------------------------------------------------

dqm_df = spark.table(DQM_LOG_TBL)
reject_df = spark.table(REJECT_TBL)

print("\nDQM log columns:")
print(dqm_df.columns)
print("DQM log table availability check: PASS")

print("\nDQ rejection table columns:")
print(reject_df.columns)
print("DQ rejection table availability check: PASS")

print("\n" + "=" * 60)
print("TEST 6 — BATCH / CONTROL / AUDIT VERIFICATION: PASS")
print("=" * 60)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7 — MDM_HUB transformation verification

# COMMAND ----------

# ============================================================
# TEST 7 — MDM_HUB TRANSFORMATION VERIFICATION
# ============================================================
# IMPORTANT:
#   The project's MDM_HUB transformation does NOT consume the IQVIA
#   request payload produced by transform_to_iqvia().
#
#   Its source mapping uses the original SBC-style dotted source
#   attributes such as:
#       hcp.firstName
#       hcp.middleName
#       hcp.lastName
#       address.city
#       address.primary
#       address.countryCode
#       address.longPostalCode
#       address.type
#
# Therefore this test uses the actual MDM_HUB input contract from
# the project source instead of incorrectly passing the IQVIA
# response into the MDM_HUB function.
# ============================================================

mock_mdm_hub_input = {
    "hcp.firstName": "John",
    "hcp.middleName": "A",
    "hcp.lastName": "Smith",
    "address.primary": "10 Main Street",
    "address.countryCode": "NL",
    "address.city": "Amsterdam",
    "address.longPostalCode": "1011AB",
    "address.type": "Primary",
}

mdm_hub_response = transform_to_mdm_hub.transform_to_mdm_hub(mock_mdm_hub_input)

assert isinstance(mdm_hub_response, dict)

search_controls = mdm_hub_response["searchControls"]
search_record = mdm_hub_response["data"]["searchRecord"]

# Validate country-code -> population mapping.
assert search_controls["population"] == "netherlands"

# Validate the mapped search-record fields.
assert search_record["firstName"] == "John"
assert search_record["middleName"] == "A"
assert search_record["lastName"] == "Smith"
assert search_record["fullName"] == "John A Smith"

assert search_record["X_hcp_address"][0]["X_city"] == "Amsterdam"
assert search_record["X_hcp_address"][0]["X_address_line_1"] == "10 Main Street"
assert search_record["X_hcp_address"][0]["X_country"]["Code"] == "NL"
assert search_record["X_hcp_address"][0]["X_postal_code"] == "1011AB"
assert search_record["X_hcp_address"][0]["X_address_type"]["Code"] == "Primary"

print("MDM_HUB population:", search_controls["population"])
print("MDM_HUB fullName:", search_record["fullName"])
print("MDM_HUB address:", search_record["X_hcp_address"][0])

print("\nTEST 7 — MDM_HUB transformation verification: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC # Validation Summary
# MAGIC
# MAGIC If all seven test cells complete successfully, the current functional validation set is:
# MAGIC
# MAGIC | Test | Area | Expected |
# MAGIC |---|---|---|
# MAGIC | 1 | IQVIA — NL | PASS |
# MAGIC | 2 | IQVIA — BE | PASS |
# MAGIC | 3 | IQVIA — unsupported country | PASS |
# MAGIC | 4 | IQVIA — missing countryCode | PASS |
# MAGIC | 5 | Data Quality rejection | PASS |
# MAGIC | 6 | Batch / Control / Audit | PASS |
# MAGIC | 7 | MDM_HUB transformation | PASS |
# MAGIC
# MAGIC **Next step after Test 7:** review the complete notebook execution from the first cell through Test 7. Only after that should we finalize the notebook and move to the remaining project implementation work.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configure AWS Credentials (from Secrets Manager)
# ============================================================
# AWS SECRETS MANAGER ACCESS
# ============================================================
# Use Databricks Service Credential to access AWS Secrets Manager.
# NO HARDCODED CREDENTIALS!
# ============================================================

import boto3
import json
from botocore.exceptions import ClientError

print("Accessing AWS Secrets Manager via Databricks Service Credential...\n")
print("="*60)

try:
    # Use Databricks Service Credential for AWS access
    boto3_session = boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(
            "healthcare_mdm_secrets_credential"
        ),
        region_name="us-east-1"
    )
    sm = boto3_session.client("secretsmanager")
    
    print("AWS Secrets Manager client created: SUCCESS")
    print("Region: us-east-1\n")
    
    # Test by retrieving secret
    response = sm.get_secret_value(SecretId='healthcare-mdm/dev/api-snowflake')
    secret = json.loads(response['SecretString'])
    
    print("Secrets Manager access: SUCCESS!")
    print("="*60)
    print(f"Secret Keys: {list(secret.keys())}")
    print(f"Lambda URL: {secret.get('lambda_url', 'N/A')[:50]}...")
    print(f"API Key: {secret.get('lambda_api_key', 'N/A')[:15]}...")
    print("="*60)
    print("\nREADY FOR TEST 8!")
    
except ClientError as e:
    error_code = e.response['Error']['Code']
    print(f"\n❌ FAILED: {error_code}")
    print(f"Message: {e.response['Error']['Message']}")
    
    if error_code == 'AccessDeniedException':
        print("\nVerify SecretsManagerReadWrite is attached to service credential")
        
except Exception as e:
    print(f"\nError: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Test 8 - API to RAW Pipeline
import sys
import importlib
from pyspark.sql import functions as F

if 'src.api.api_to_raw_caller' in sys.modules:
    importlib.reload(sys.modules['src.api.api_to_raw_caller'])

from src.api import api_to_raw_caller

print("Executing API to RAW Pipeline...\n")
print("="*60)

try:
    # Get AWS credentials using Databricks Service Credential
    import boto3
    import json
    from botocore.exceptions import ClientError
    
    boto3_session = boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(
            "healthcare_mdm_secrets_credential"
        ),
        region_name="us-east-1"
    )
    sm = boto3_session.client("secretsmanager")
    
    # Retrieve secrets
    response = sm.get_secret_value(SecretId='healthcare-mdm/dev/api-snowflake')
    secrets = json.loads(response['SecretString'])
    
    lambda_url = secrets.get('lambda_url')
    lambda_api_key = secrets.get('lambda_api_key')
    
    print(f"Lambda URL: {lambda_url[:50]}...")
    print("Calling API to RAW pipeline...\n")
    
    # Call the pipeline with explicit lambda credentials
    result = api_to_raw_caller.run_api_to_raw(
        max_records=5,
        lambda_url=lambda_url,
        lambda_api_key=lambda_api_key
    )
    
    print("\n" + "="*60)
    print("API TO RAW PIPELINE COMPLETED")
    print("="*60)
    print(f"Total Processed: {result['total_processed']}")
    print(f"Total Success  : {result['total_success']}")
    print(f"Total Failed   : {result['total_failed']}")
    print(f"HCP Count      : {result['hcp_count']}")
    print(f"HCO Count      : {result['hco_count']}")
    print("="*60)
    
    if result['total_success'] > 0:
        print("\nHCP RAW Data:")
        hcp_df = spark.table("HMDM_DEV.raw.hcp_api_data").orderBy(F.col("create_date").desc()).limit(5)
        display(hcp_df)
        
        print("\nHCO RAW Data:")
        hco_df = spark.table("HMDM_DEV.raw.hco_api_data").orderBy(F.col("create_date").desc()).limit(5)
        display(hco_df)
        
        print("\nTEST 8 - API to RAW pipeline: PASS")
    else:
        print("\nTEST 8 - API to RAW pipeline: FAIL")
        print("No records processed successfully")
        
except Exception as e:
    print(f"\nTEST 8 - API to RAW pipeline: ERROR")
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()