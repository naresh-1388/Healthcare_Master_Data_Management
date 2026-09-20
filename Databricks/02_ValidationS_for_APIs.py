# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Notebook Title
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management — API and Transformation Validations
# MAGIC
# MAGIC This notebook validates the project's API transformation functions, data quality rules, batch control framework, end-to-end API-to-RAW pipeline, and HCO data pipeline. Use the widgets at the top to select the source system (IQVIA_API or TEST) and entity type (HCP, HCO, or BOTH). Each test is self-contained and can be run independently.

# COMMAND ----------

# DBTITLE 1,Why: Widgets
# MAGIC %md
# MAGIC #### Widget Setup
# MAGIC
# MAGIC Select the source system and entity type before running tests. These widgets control which source system and entity type the pipeline tests use.
# MAGIC
# MAGIC * **Source System**: IQVIA_API (production pipeline) or TEST (test pipeline)
# MAGIC * **Entity Type**: HCP, HCO, or BOTH — controls which pipeline verification tests run

# COMMAND ----------

# DBTITLE 1,Widget Setup
# ============================================================
# WIDGET SETUP — SOURCE SYSTEM AND ENTITY TYPE
# ============================================================
# These widgets appear at the top of the notebook.
# Select Source System and Entity Type before running tests.
# ============================================================

dbutils.widgets.dropdown("source_system", "IQVIA_API", ["IQVIA_API", "TEST"], "Source System")
dbutils.widgets.dropdown("entity_type", "BOTH", ["HCP", "HCO", "BOTH"], "Entity Type")

# Read widget values into Python variables for use in test cells
SELECTED_SOURCE = dbutils.widgets.get("source_system")
SELECTED_ENTITY = dbutils.widgets.get("entity_type")

print(f"Source System : {SELECTED_SOURCE}")
print(f"Entity Type   : {SELECTED_ENTITY}")

# COMMAND ----------

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
# MAGIC - Test data: TEST_HCP records (HCP only — no HCO DQ test table exists)
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

# DBTITLE 1,Why: Verify Test Tables
# MAGIC %md
# MAGIC #### Verify Test Data Tables
# MAGIC
# MAGIC Checks that all required test infrastructure tables exist before running validation tests. This includes the DQ test table, batch control log, audit log, DQ log, and DQ reject table. If any table is missing, a warning is printed — some tests may fail.

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

# DBTITLE 1,Why: Runtime Config
# MAGIC %md
# MAGIC #### Runtime Configuration Check
# MAGIC
# MAGIC Confirms that the centralized runtime configuration resolves the correct Databricks development environment. Validates catalog name, schema names (raw, landing, staging, mdm, util), and DQ config table. All assertions must pass before proceeding to tests.

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

# DBTITLE 1,Why: DQ Ruleset
# MAGIC %md
# MAGIC #### Project DQ Ruleset Check
# MAGIC
# MAGIC Confirms that the project's data quality configuration contains exactly 24 DQ rules as defined in the project workbook. This ensures no rules were accidentally added or removed during development.

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

# DBTITLE 1,Tests 1-4 IQVIA Transformation
# MAGIC %md
# MAGIC ## Tests 1-4 — IQVIA Transformation (HCP)
# MAGIC
# MAGIC The `transform_to_iqvia` function maps HCP records to IQVIA API format using country-to-codBase mapping (NL to WNL, BE to WBE). This function is HCP-specific — it uses `firstName`, `lastName`, and `middleName` fields that do not apply to HCO records.
# MAGIC
# MAGIC Four tests are combined in the next cell:
# MAGIC * Test 1: Netherlands positive case — NL maps to WNL
# MAGIC * Test 2: Belgium positive case — BE maps to WBE
# MAGIC * Test 3: Unsupported country (US) — raises IQVIATransformationError
# MAGIC * Test 4: Missing countryCode — raises IQVIATransformationError
# MAGIC
# MAGIC Widget selection: These tests run when Entity Type is HCP or BOTH. They are skipped when HCO is selected because the transformation function does not support HCO fields.

# COMMAND ----------

# DBTITLE 1,Tests 1-4 - IQVIA Transformation
# ============================================================
# TESTS 1-4 — IQVIA TRANSFORMATION (HCP ONLY)
# ============================================================
# The transform_to_iqvia function maps HCP records to IQVIA
# API format using country-to-codBase mapping:
#   NL -> WNL, BE -> WBE
#
# This function is HCP-specific (uses firstName, lastName,
# middleName fields). HCO records use different fields.
#
# Test 1: Netherlands positive case (NL -> WNL)
# Test 2: Belgium positive case (BE -> WBE)
# Test 3: Unsupported country negative case (US -> error)
# Test 4: Missing countryCode negative case (-> error)
#
# Widget: Runs when entity_type is HCP or BOTH.
#         Skipped when HCO is selected.
# ============================================================

print(f"Entity Type selected : {SELECTED_ENTITY}")
print(f"Source System selected: {SELECTED_SOURCE}")
print("=" * 60)

if SELECTED_ENTITY == "HCO":
    print("Tests 1-4 — IQVIA Transformation: SKIPPED")
    print("Reason: transform_to_iqvia is HCP-specific (uses firstName, lastName).")
    print("HCO records use different fields (organizationName, etc.) and are")
    print("not supported by this function.")
    print("=" * 60)
else:
    test_results = []

    # --- Test 1: Netherlands positive case (NL -> WNL) ---
    mock_hcp_nl = {
        "hcp": {"firstName": "John", "middleName": "A", "lastName": "Smith"},
        "address": {"primary": "10 Main Street", "countryCode": "NL",
                     "city": "Amsterdam", "longPostalCode": "1011AB", "type": "Primary"},
    }
    iqvia_nl = transform_to_iqvia.transform_to_iqvia(mock_hcp_nl)
    assert isinstance(iqvia_nl, dict)
    assert iqvia_nl["codBases"] == ["WNL"]
    print(f"Test 1 — IQVIA NL: codBases={iqvia_nl['codBases']}, fields={len(iqvia_nl['fields'])} — PASS")
    test_results.append(("Test 1 — IQVIA NL positive", "PASS"))

    # --- Test 2: Belgium positive case (BE -> WBE) ---
    mock_hcp_be = {
        "hcp": {"firstName": "Mary", "middleName": "B", "lastName": "Jones"},
        "address": {"primary": "20 Main Street", "countryCode": "BE",
                     "city": "Brussels", "longPostalCode": "1000", "type": "Primary"},
    }
    iqvia_be = transform_to_iqvia.transform_to_iqvia(mock_hcp_be)
    assert isinstance(iqvia_be, dict)
    assert iqvia_be["codBases"] == ["WBE"]
    print(f"Test 2 — IQVIA BE: codBases={iqvia_be['codBases']}, fields={len(iqvia_be['fields'])} — PASS")
    test_results.append(("Test 2 — IQVIA BE positive", "PASS"))

    # --- Test 3: Unsupported country negative case (US -> error) ---
    mock_hcp_us = {
        "hcp": {"firstName": "John", "middleName": "A", "lastName": "Smith"},
        "address": {"primary": "10 Main Street", "countryCode": "US",
                     "city": "New York", "longPostalCode": "10001", "type": "Primary"},
    }
    try:
        transform_to_iqvia.transform_to_iqvia(mock_hcp_us)
        raise AssertionError("Expected IQVIATransformationError for unsupported country US.")
    except transform_to_iqvia.IQVIATransformationError as exc:
        print(f"Test 3 — Unsupported country: {exc} — PASS")
        test_results.append(("Test 3 — Unsupported country negative", "PASS"))

    # --- Test 4: Missing countryCode negative case (-> error) ---
    mock_hcp_missing_country = {
        "hcp": {"firstName": "John", "middleName": "A", "lastName": "Smith"},
        "address": {"primary": "10 Main Street",
                     "city": "Amsterdam", "longPostalCode": "1011AB", "type": "Primary"},
    }
    try:
        transform_to_iqvia.transform_to_iqvia(mock_hcp_missing_country)
        raise AssertionError("Expected IQVIATransformationError for missing countryCode.")
    except transform_to_iqvia.IQVIATransformationError as exc:
        print(f"Test 4 — Missing countryCode: {exc} — PASS")
        test_results.append(("Test 4 — Missing countryCode negative", "PASS"))

    print("\n" + "=" * 60)
    print(f"IQVIA Transformation Tests (Entity: {SELECTED_ENTITY}, Source: {SELECTED_SOURCE})")
    print("=" * 60)
    for name, status in test_results:
        print(f"  {name}: {status}")
    print("=" * 60)


# COMMAND ----------

# DBTITLE 1,Test 5 header
# MAGIC %md
# MAGIC ## Test 5 — Data Quality: firstName NULL Rejection (HCP)
# MAGIC
# MAGIC Runs the project DQ rules against the HCP test table where one record has a NULL `firstName`. Expects 2 records to pass and 1 to be rejected. The DQ framework identifies and rejects records with NULL mandatory fields.
# MAGIC
# MAGIC Widget selection: This test runs when Entity Type is HCP or BOTH. It is skipped when HCO is selected because the DQ test table (`hcp_name_dq_test`) and the `TEST_HCP` source identifier are HCP-specific.

# COMMAND ----------

# DBTITLE 1,Test 5 - DQ rejection
# ============================================================
# TEST 5 — DATA QUALITY: firstName NULL REJECTION (HCP)
# ============================================================
# Purpose:
#   Execute the TEST_HCP DQ configuration against the dedicated
#   DQ test table where HCP001.firstName is NULL.
#
# Expected result:
#   PASS count   = 2
#   REJECT count = 1
#
# Widget: Runs when entity_type is HCP or BOTH.
#         Skipped when HCO is selected because the DQ test
#         table (hcp_name_dq_test) and TEST_HCP source
#         identifier are HCP-specific.
# ============================================================

print(f"Entity Type selected: {SELECTED_ENTITY}")
print("=" * 60)

if SELECTED_ENTITY == "HCO":
    print("Test 5 — DQ firstName NULL Rejection: SKIPPED")
    print("Reason: The DQ test table (hcp_name_dq_test) and TEST_HCP")
    print("source identifier are HCP-specific. No HCO DQ test table exists.")
    print("=" * 60)
else:
    DQ_TEST_TABLE = "HMDM_DEV.staging.hcp_name_dq_test"

    assert spark.catalog.tableExists(DQ_TEST_TABLE), (
        f"Expected DQ test table does not exist: {DQ_TEST_TABLE}. "
        "Run the existing TEST_HCP setup before executing Test 5."
    )

    passed_df, rejected_df = data_quality.execute_source_dq(
        source_identifier="TEST_HCP",
        source_table=DQ_TEST_TABLE,
        source_system_name="TEST",  # TEST_HCP DQ rules configured under TEST only
    )

    pass_count = passed_df.count()
    reject_count = rejected_df.count()

    print("PASS count   :", pass_count)
    print("REJECT count :", reject_count)

    assert pass_count == 2, f"Expected 2 passing records, found {pass_count}"
    assert reject_count == 1, f"Expected 1 rejected record, found {reject_count}"

    print("\nRejected record:")
    display(rejected_df)

    print(f"\nTest 5 — DQ rejection (Entity: {SELECTED_ENTITY}): PASS")
    print("=" * 60)


# COMMAND ----------

# DBTITLE 1,Test 6 header
# MAGIC %md
# MAGIC ## Test 6 — Batch / Control / Audit verification
# MAGIC
# MAGIC Verifies that the Databricks control and audit framework is properly set up. Checks that control tables (batch log, audit log, DQ log, reject table) exist, that the selected source system batch history shows RAW and Standardization completed, and that pipeline audit logs are present. Uses the source system selected in the widget (IQVIA_API or TEST).

# COMMAND ----------

# DBTITLE 1,Test 6 - Batch/Control/Audit
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
    .filter(F.col("source_system_name") == SELECTED_SOURCE)
    .orderBy(F.col("batch_id").desc())
)

assert batch_df.count() > 0, f"No {SELECTED_SOURCE} batch found in ctl_batch_log_tbl"

print(f"\nLatest {SELECTED_SOURCE} batch:")
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
    .filter(F.col("source_system_name") == SELECTED_SOURCE)
    .orderBy(F.col("start_time").desc())
)

assert log_df.count() > 0, f"No pipeline audit log found for {SELECTED_SOURCE}"

print(f"\nRecent pipeline audit logs for {SELECTED_SOURCE}:")
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

# DBTITLE 1,Test 7 header
# MAGIC %md
# MAGIC ## Test 7 — MDM_HUB Transformation (HCP)
# MAGIC
# MAGIC Validates the MDM_HUB transformation function using a mock HCP input with SBC-style dotted attributes (`hcp.firstName`, `address.countryCode`, etc.). Verifies country-to-population mapping (NL to netherlands), field mapping (firstName, lastName, fullName), and nested address structure.
# MAGIC
# MAGIC Widget selection: This test runs when Entity Type is HCP or BOTH. It is skipped when HCO is selected because the MDM_HUB transformation uses HCP-specific field mappings.

# COMMAND ----------

# DBTITLE 1,Test 7 - MDM_HUB
# ============================================================
# TEST 7 — MDM_HUB TRANSFORMATION (HCP)
# ============================================================
# The MDM_HUB transformation maps SBC-style dotted attributes
# (hcp.firstName, address.countryCode, etc.) to the MDM Hub
# search record format. This function is HCP-specific.
#
# Widget: Runs when entity_type is HCP or BOTH.
#         Skipped when HCO is selected.
# ============================================================

print(f"Entity Type selected: {SELECTED_ENTITY}")
print("=" * 60)

if SELECTED_ENTITY == "HCO":
    print("Test 7 — MDM_HUB Transformation: SKIPPED")
    print("Reason: The MDM_HUB transformation uses HCP-specific field")
    print("mappings (hcp.firstName, hcp.lastName, etc.). HCO uses")
    print("different fields (organizationName, etc.).")
    print("=" * 60)
else:
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

    assert search_controls["population"] == "netherlands"
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

    print(f"\nTest 7 — MDM_HUB transformation (Entity: {SELECTED_ENTITY}): PASS")
    print("=" * 60)


# COMMAND ----------

# DBTITLE 1,Validation Summary
# MAGIC %md
# MAGIC # Validation Summary
# MAGIC
# MAGIC The notebook has 6 test cells covering 9 functional validations. Tests 1-4 are combined in one cell, Tests 8-9 are combined in one cell. Widget selection (Entity Type, Source System) controls which tests run and what output is displayed.
# MAGIC
# MAGIC | Test | Area | Entity | Cell |
# MAGIC |---|---|---|---|
# MAGIC | 1-4 | IQVIA transformation (NL, BE, unsupported, missing country) | HCP only | Combined |
# MAGIC | 5 | Data Quality — firstName NULL rejection | HCP only | Single |
# MAGIC | 6 | Batch / Control / Audit verification | Both (widget-driven) | Single |
# MAGIC | 7 | MDM_HUB transformation | HCP only | Single |
# MAGIC | 8-9 | API to RAW pipeline + HCO pipeline verification | HCP + HCO (widget-driven) | Combined |
# MAGIC
# MAGIC Widget behavior:
# MAGIC * **HCP** — Tests 1-7 run (HCP transformations, DQ, batch, MDM_HUB). Tests 8-9 show HCP RAW data only.
# MAGIC * **HCO** — Tests 1-4, 5, 7 are skipped (HCP-specific). Tests 8-9 show HCO RAW data and HCO pipeline layers.
# MAGIC * **BOTH** — All tests run. Tests 8-9 show both HCP and HCO data.
# MAGIC * **Source System** — Test 6 filters batch and audit logs by the selected source system.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Why: AWS Secrets for Test 8
# MAGIC %md
# MAGIC #### AWS Secrets Manager Access for Test 8
# MAGIC
# MAGIC Connects to AWS Secrets Manager using Databricks service credential to retrieve the Lambda URL and API key needed for the API-to-RAW pipeline test. No AWS keys are stored in the code. This is a pre-check before Test 8.

# COMMAND ----------

# DBTITLE 1,Configure AWS Credentials
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
    print(f"\nFAILED: {error_code}")
    print(f"Message: {e.response['Error']['Message']}")
    
    if error_code == 'AccessDeniedException':
        print("\nVerify SecretsManagerReadWrite is attached to service credential")
        
except Exception as e:
    print(f"\nError: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Tests 8-9 API Pipeline
# MAGIC %md
# MAGIC ## Tests 8-9 — API Pipeline Verification (HCP + HCO)
# MAGIC
# MAGIC Test 8 calls the Lambda API to fetch HCP and HCO records and writes them to the RAW layer. Test 9 verifies HCO data exists across all pipeline layers (RAW, Staging, MDM, Master).
# MAGIC
# MAGIC Widget selection controls the output:
# MAGIC * **HCP** — shows HCP RAW data only; Test 9 (HCO pipeline) is skipped
# MAGIC * **HCO** — shows HCO RAW data and HCO pipeline layer verification
# MAGIC * **BOTH** — shows both HCP and HCO RAW data, plus HCO pipeline layer verification

# COMMAND ----------

# DBTITLE 1,Tests 8-9 - API Pipeline
# ============================================================
# TESTS 8-9 — API PIPELINE VERIFICATION (HCP + HCO)
# ============================================================
# Test 8: Calls Lambda API to fetch HCP and HCO records,
#         writes them to the RAW layer.
# Test 9: Verifies HCO data exists across all pipeline layers
#         (RAW, Staging, MDM, Master).
#
# Widget controls output based on Entity Type:
#   HCP  -> show HCP RAW data only
#   HCO  -> show HCO RAW data + HCO pipeline layers
#   BOTH -> show both HCP and HCO RAW data + HCO pipeline layers
# ============================================================

import sys, importlib
from pyspark.sql import functions as F

if 'src.api.api_to_raw_caller' in sys.modules:
    importlib.reload(sys.modules['src.api.api_to_raw_caller'])

from src.api import api_to_raw_caller

print(f"Entity Type selected : {SELECTED_ENTITY}")
print(f"Source System selected: {SELECTED_SOURCE}")
print("=" * 60)

test_results = []

# ------------------------------------------------------------
# TEST 8 — API to RAW Pipeline
# ------------------------------------------------------------
print("\nTEST 8 — API to RAW Pipeline")
print("-" * 60)

try:
    import boto3, json
    from botocore.exceptions import ClientError

    boto3_session = boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(
            "healthcare_mdm_secrets_credential"
        ),
        region_name="us-east-1"
    )
    sm = boto3_session.client("secretsmanager")
    response = sm.get_secret_value(SecretId='healthcare-mdm/dev/api-snowflake')
    secrets = json.loads(response['SecretString'])

    lambda_url = secrets.get('lambda_url')
    lambda_api_key = secrets.get('lambda_api_key')

    print(f"Lambda URL: {lambda_url[:50]}...")

    result = api_to_raw_caller.run_api_to_raw(
        max_records=5,
        lambda_url=lambda_url,
        lambda_api_key=lambda_api_key
    )

    print(f"Total Processed: {result['total_processed']}")
    print(f"Total Success  : {result['total_success']}")
    print(f"Total Failed   : {result['total_failed']}")
    print(f"HCP Count      : {result['hcp_count']}")
    print(f"HCO Count      : {result['hco_count']}")

    if result['total_success'] > 0:
        if SELECTED_ENTITY in ("HCP", "BOTH"):
            print("\nHCP RAW Data:")
            hcp_df = spark.table("HMDM_DEV.raw.hcp_api_data").orderBy(F.col("create_date").desc()).limit(5)
            display(hcp_df)

        if SELECTED_ENTITY in ("HCO", "BOTH"):
            print("\nHCO RAW Data:")
            hco_df = spark.table("HMDM_DEV.raw.hco_api_data").orderBy(F.col("create_date").desc()).limit(5)
            display(hco_df)

        print("TEST 8 — API to RAW pipeline: PASS")
        test_results.append(("Test 8 — API to RAW", "PASS"))
    else:
        print("TEST 8 — API to RAW pipeline: FAIL")
        print("No records processed successfully")
        test_results.append(("Test 8 — API to RAW", "FAIL"))

except Exception as e:
    print(f"TEST 8 — API to RAW pipeline: ERROR")
    print(f"Error: {str(e)}")
    test_results.append(("Test 8 — API to RAW", "ERROR"))

# ------------------------------------------------------------
# TEST 9 — HCO Data Pipeline Verification
# ------------------------------------------------------------
if SELECTED_ENTITY in ("HCO", "BOTH"):
    print("\n" + "=" * 60)
    print("TEST 9 — HCO Data Pipeline Verification")
    print("-" * 60)

    failures = []

    # Layer 1: RAW
    print("\nHCO RAW Layer:")
    raw_table = "HMDM_DEV.raw.hco_api_data"
    if spark.catalog.tableExists(raw_table):
        raw_count = spark.table(raw_table).count()
        print(f"  {raw_table}: {raw_count} rows")
        if raw_count == 0:
            failures.append(f"{raw_table} has 0 rows")
    else:
        print(f"  {raw_table}: MISSING")
        failures.append(f"{raw_table} does not exist")

    # Layer 2: Staging
    print("\nHCO Staging Layer:")
    hco_staging_tables = [
        "hco_name", "hco_alternate_name", "hco_phone", "hco_specialty",
        "hco_address", "hco_email", "hco_identification",
        "hco_hco_hierarchy", "hco_tax",
    ]
    staging_total = 0
    for tbl in hco_staging_tables:
        full_name = f"HMDM_DEV.staging.{tbl}"
        if spark.catalog.tableExists(full_name):
            cnt = spark.table(full_name).count()
            staging_total += cnt
            print(f"  staging.{tbl}: {cnt} rows")
            if cnt == 0:
                failures.append(f"{full_name} has 0 rows")
        else:
            print(f"  staging.{tbl}: MISSING")
            failures.append(f"{full_name} does not exist")
    print(f"  Staging total: {staging_total} rows across {len(hco_staging_tables)} tables")

    # Layer 3: MDM
    print("\nHCO MDM Layer:")
    hco_mdm_tables = [
        "hco", "hco_name", "hco_alternate_identifier", "hco_phone",
        "hco_specialty", "hco_alternate_name", "hco_address",
        "hco_email", "hco_hco_hierarchy", "hco_tax",
    ]
    mdm_total = 0
    for tbl in hco_mdm_tables:
        full_name = f"HMDM_DEV.mdm.{tbl}"
        if spark.catalog.tableExists(full_name):
            cnt = spark.table(full_name).count()
            mdm_total += cnt
            print(f"  mdm.{tbl}: {cnt} rows")
            if cnt == 0:
                failures.append(f"{full_name} has 0 rows")
        else:
            print(f"  mdm.{tbl}: MISSING")
            failures.append(f"{full_name} does not exist")
    print(f"  MDM total: {mdm_total} rows across {len(hco_mdm_tables)} tables")

    # Layer 4: Master
    print("\nHCO Master Layer:")
    hco_master_tables = [
        "hco", "hco_name", "hco_alternate_identifier",
        "hco_phone", "hco_specialty",
    ]
    master_total = 0
    for tbl in hco_master_tables:
        full_name = f"HMDM_DEV.master.{tbl}"
        if spark.catalog.tableExists(full_name):
            cnt = spark.table(full_name).count()
            master_total += cnt
            print(f"  master.{tbl}: {cnt} rows")
            if cnt == 0:
                failures.append(f"{full_name} has 0 rows")
        else:
            print(f"  master.{tbl}: MISSING")
            failures.append(f"{full_name} does not exist")
    print(f"  Master total: {master_total} rows across {len(hco_master_tables)} tables")

    if not failures:
        print(f"\nTEST 9 — HCO Pipeline: PASS")
        print(f"  RAW: {raw_count}, Staging: {staging_total}, MDM: {mdm_total}, Master: {master_total}")
        test_results.append(("Test 9 — HCO Pipeline", "PASS"))
    else:
        print(f"\nTEST 9 — HCO Pipeline: FAIL ({len(failures)} issues)")
        for f in failures:
            print(f"  - {f}")
        test_results.append(("Test 9 — HCO Pipeline", "FAIL"))
else:
    print("\nTEST 9 — HCO Pipeline Verification: SKIPPED")
    print(f"Reason: Entity Type is {SELECTED_ENTITY} (requires HCO or BOTH)")
    test_results.append(("Test 9 — HCO Pipeline", "SKIPPED"))

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print("\n" + "=" * 60)
print(f"API Pipeline Tests (Entity: {SELECTED_ENTITY}, Source: {SELECTED_SOURCE})")
print("=" * 60)
for name, status in test_results:
    print(f"  {name}: {status}")
print("=" * 60)