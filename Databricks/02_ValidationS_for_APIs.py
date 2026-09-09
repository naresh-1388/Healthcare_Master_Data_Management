# Databricks notebook source
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

REPO_ROOT = Path(
    "/Workspace/Users/naresh.mayari@gmail.com/Healthcare_Master_Data_Management"
)
SRC_ROOT = REPO_ROOT / "src"

assert SRC_ROOT.exists(), (
    f"Healthcare_MDM src directory was not found: {SRC_ROOT}. "
    "Update REPO_ROOT if the Git repository is located elsewhere."
)

# Put the repository root first so `src.*` resolves to this project.
repo_path = str(REPO_ROOT)
if repo_path in sys.path:
    sys.path.remove(repo_path)
sys.path.insert(0, repo_path)

# Import only modules required by the validation tests.
# This intentionally avoids importing API modules that can require
# runtime environment variables or AWS Secrets Manager credentials.
from src.api import transform_to_jisb
from src.api import transform_to_orieo
from src.dq import data_quality
from src.core import runtime_config

# Reload the modules so the notebook uses the current Git repository code.
importlib.reload(transform_to_jisb)
importlib.reload(transform_to_orieo)
importlib.reload(data_quality)
importlib.reload(runtime_config)

print("Healthcare_MDM source path:", SRC_ROOT)
print("Common project imports: PASS")


# COMMAND ----------

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

assert runtime_config.catalog == "HMDM_DEV"
assert runtime_config.raw_schema == "HMDM_DEV.raw"
assert runtime_config.lnd_schema == "HMDM_DEV.landing"
assert runtime_config.stg_schema == "HMDM_DEV.staging"
assert runtime_config.publish_schema == "HMDM_DEV.mdm"
assert runtime_config.util_schema == "HMDM_DEV.util"

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
# MAGIC ## Test 1 — JISB transformation: Netherlands positive case

# COMMAND ----------

# ============================================================
# TEST 1 — JISB POSITIVE CASE: NETHERLANDS
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

jisb_nl = transform_to_jisb.transform_to_jisb(mock_hcp_nl)

assert isinstance(jisb_nl, dict)
assert jisb_nl["codBases"] == ["WNL"]

print("JISB codBases:", jisb_nl["codBases"])
print("JISB fields:", len(jisb_nl["fields"]))
print("TEST 1 — JISB NL positive case: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2 — JISB transformation: Belgium positive case

# COMMAND ----------

# ============================================================
# TEST 2 — JISB POSITIVE CASE: BELGIUM
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

jisb_be = transform_to_jisb.transform_to_jisb(mock_hcp_be)

assert isinstance(jisb_be, dict)
assert jisb_be["codBases"] == ["WBE"]

print("JISB codBases:", jisb_be["codBases"])
print("JISB fields:", len(jisb_be["fields"]))
print("TEST 2 — JISB BE positive case: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3 — JISB transformation: unsupported country negative case

# COMMAND ----------

# ============================================================
# TEST 3 — JISB NEGATIVE CASE: UNSUPPORTED COUNTRY
# ============================================================
# US is intentionally outside the project country-to-codBase
# mapping used by the JISB transformation.
#
# Expected behavior:
#   The transformation raises JISBTransformationError.
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
    transform_to_jisb.transform_to_jisb(mock_hcp_us)
    raise AssertionError(
        "Expected JISBTransformationError for unsupported country US."
    )
except transform_to_jisb.JISBTransformationError as exc:
    print("Expected error:", exc)
    print("TEST 3 — Unsupported country negative case: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4 — JISB transformation: missing countryCode negative case

# COMMAND ----------

# ============================================================
# TEST 4 — JISB NEGATIVE CASE: MISSING COUNTRY CODE
# ============================================================
# Expected behavior:
#   The transformation raises JISBTransformationError because
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
    transform_to_jisb.transform_to_jisb(mock_hcp_missing_country)
    raise AssertionError(
        "Expected JISBTransformationError for missing countryCode."
    )
except transform_to_jisb.JISBTransformationError as exc:
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
# MAGIC ## Test 7 — ORIEO transformation verification

# COMMAND ----------

# ============================================================
# TEST 7 — ORIEO TRANSFORMATION VERIFICATION
# ============================================================
# IMPORTANT:
#   The project's ORIEO transformation does NOT consume the JISB
#   request payload produced by transform_to_jisb().
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
# Therefore this test uses the actual ORIEO input contract from
# the project source instead of incorrectly passing the JISB
# response into the ORIEO function.
# ============================================================

mock_orieo_input = {
    "hcp.firstName": "John",
    "hcp.middleName": "A",
    "hcp.lastName": "Smith",
    "address.primary": "10 Main Street",
    "address.countryCode": "NL",
    "address.city": "Amsterdam",
    "address.longPostalCode": "1011AB",
    "address.type": "Primary",
}

orieo_response = transform_to_orieo.transform_to_orieo(mock_orieo_input)

assert isinstance(orieo_response, dict)

search_controls = orieo_response["searchControls"]
search_record = orieo_response["data"]["searchRecord"]

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

print("ORIEO population:", search_controls["population"])
print("ORIEO fullName:", search_record["fullName"])
print("ORIEO address:", search_record["X_hcp_address"][0])

print("\nTEST 7 — ORIEO transformation verification: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC # Validation Summary
# MAGIC
# MAGIC If all seven test cells complete successfully, the current functional validation set is:
# MAGIC
# MAGIC | Test | Area | Expected |
# MAGIC |---|---|---|
# MAGIC | 1 | JISB — NL | PASS |
# MAGIC | 2 | JISB — BE | PASS |
# MAGIC | 3 | JISB — unsupported country | PASS |
# MAGIC | 4 | JISB — missing countryCode | PASS |
# MAGIC | 5 | Data Quality rejection | PASS |
# MAGIC | 6 | Batch / Control / Audit | PASS |
# MAGIC | 7 | ORIEO transformation | PASS |
# MAGIC
# MAGIC **Next step after Test 7:** review the complete notebook execution from the first cell through Test 7. Only after that should we finalize the notebook and move to the remaining project implementation work.
# MAGIC

# COMMAND ----------



# COMMAND ----------

