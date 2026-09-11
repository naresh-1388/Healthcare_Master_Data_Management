"""
MDM Ingress

Purpose
-------
Publishes DQ-approved Landing/Staging data to the MDM publish layer.

Purpose:
    Publish DQ-approved HCP/HCO records to the environment-specific MDM layer.

HCP field mappings are supplied at execution time; HCO field mappings are defined in this module.

Pipeline gating:
RAW -> Standardization -> Canonical -> DQ -> MDM Ingress
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from ..core.runtime_config import (
        get_batch_status_filter,
        update_batch_log_tbl,
    )
except ImportError:
    from core.runtime_config import (
        get_batch_status_filter,
        update_batch_log_tbl,
    )

try:
    from ..core.logging_utils import logger as project_logger
except Exception:
    project_logger = None


logger = project_logger or logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_PUBLISH_SCHEMA = "mdm"

INGRESS_MODULE = "ingress"


# Physical HCO MDM targets follow the project environment naming convention.
HCO_TARGET_TABLES = [
    "hco_name",
    "hco_alternate_identifier",
    "hco_phone",
    "hco_specialty",
]


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _get_spark() -> SparkSession:
    """
    Resolve the active Spark session.
    """
    try:
        return SparkSession.getActiveSession()
    except Exception:
        return None


def _qualify_table(table_name: str, catalog: Optional[str] = None) -> str:
    """
    Qualify a Databricks/Spark table name when a catalog is supplied.
    """
    if not table_name:
        raise ValueError("Table name cannot be empty.")

    table_name = table_name.strip()

    if table_name.count(".") >= 2:
        return table_name

    if catalog:
        return f"{catalog}.{table_name}"

    return table_name


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Check whether a Spark table exists.
    """
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        try:
            spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False


def _require_columns(
    df: DataFrame,
    required_columns: List[str],
    table_name: str,
) -> None:
    """
    Fail fast when a required source column is missing.
    """
    actual = {c.lower() for c in df.columns}

    missing = [
        column
        for column in required_columns
        if column.lower() not in actual
    ]

    if missing:
        raise ValueError(
            f"Required columns missing from {table_name}: {missing}. "
            f"Available columns: {df.columns}"
        )


def _get_column_case_insensitive(
    df: DataFrame,
    column_name: str,
):
    """
    Return a Spark column using case-insensitive column resolution.
    """
    for actual_column in df.columns:
        if actual_column.lower() == column_name.lower():
            return F.col(actual_column)

    raise ValueError(
        f"Column '{column_name}' not found. "
        f"Available columns: {df.columns}"
    )


# ---------------------------------------------------------------------------
# Batch handling
# ---------------------------------------------------------------------------

def get_pending_ingress_batches(
    spark: SparkSession,
    source_system_name: str,
) -> List[int]:
    """
    Return batches eligible for MDM ingress.

    The project runtime configuration defines ingress as:

        raw_ingestion_status = Y
        stdz_status = Y
        canonical_status = Y
        dq_status = Y
        ingress_status != Y
    """

    try:
        from ..core.runtime_config import batch_log_tbl
    except ImportError:
        from core.runtime_config import batch_log_tbl

    condition = get_batch_status_filter(
        INGRESS_MODULE,
        source_system_name,
    )

    query = f"""
        SELECT batch_id
        FROM {batch_log_tbl}
        WHERE {condition}
        ORDER BY batch_id
    """

    rows = spark.sql(query).collect()

    return [
        int(row["batch_id"])
        for row in rows
        if row["batch_id"] is not None
    ]


# ---------------------------------------------------------------------------
# Source reading
# ---------------------------------------------------------------------------

def read_source_table(
    spark: SparkSession,
    source_table: str,
    batch_id: Optional[int] = None,
) -> DataFrame:
    """
    Read the validated staging source.

    If batch_id is supplied, only that batch is processed.
    """

    if not _table_exists(spark, source_table):
        raise ValueError(
            f"Source table does not exist: {source_table}"
        )

    df = spark.table(source_table)

    if batch_id is not None:
        _require_columns(
            df,
            ["BATCH_ID"],
            source_table,
        )

        df = df.filter(
            _get_column_case_insensitive(df, "BATCH_ID") == F.lit(batch_id)
        )

    return df


# ---------------------------------------------------------------------------
# DQ / ingress safety
# ---------------------------------------------------------------------------

def validate_ingress_input(
    df: DataFrame,
    source_table: str,
) -> DataFrame:
    """
    Validate the minimum metadata required before publishing.

    This function does not replace data_quality.py.

    DQ is expected to have completed before ingress because the project
    explicitly gates ingress on dq_status = Y.
    """

    _require_columns(
        df,
        ["BATCH_ID"],
        source_table,
    )

    if df.limit(1).count() == 0:
        logger.info(
            "No records available for ingress from %s.",
            source_table,
        )
        return df

    return df


def deduplicate_for_ingress(
    df: DataFrame,
    business_key: str,
) -> DataFrame:
    """
    Remove duplicate business records inside the current ingress batch.

    The source project emphasizes business-key-based deduplication before
    publishing to MDM.
    """

    if business_key not in df.columns:
        logger.warning(
            "Business key '%s' not present. "
            "Skipping ingress deduplication.",
            business_key,
        )
        return df

    if "LOAD_DATE" in df.columns:
        from pyspark.sql.window import Window

        window = Window.partitionBy(
            F.col(business_key)
        ).orderBy(
            F.col("LOAD_DATE").desc_nulls_last()
        )

        return (
            df.withColumn("_ingress_rn", F.row_number().over(window))
              .filter(F.col("_ingress_rn") == 1)
              .drop("_ingress_rn")
        )

    return df.dropDuplicates([business_key])


# ---------------------------------------------------------------------------
# Mapping interface
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Project-supported detailed MDM ingress mappings
# ---------------------------------------------------------------------------
# HCO: 56 field-level mappings are already present in the existing project
# implementation. Keep them here as the detailed source-to-target contract.
HCO_INGRESS_MAPPING = [
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Source_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "sourceSystem",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Population_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "populationName",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "HCO_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_name",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "HCO_Subtype",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_companyType",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Country",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_countryOfIncorporation",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Bed_Count",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_infa360_bedCount",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Resident_Count",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_infa360_residentCount",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Website",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_infa360_website",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "HCO_Type",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_infa360_type",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_hco_status",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Transparency_Reporting_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_transparency_reporting_name",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Official_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_official_name",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Parent_Organization_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_parent_organization_name",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Teaching_Hospital_Flag",
        "data_type": "boolean",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_TeachingHospitalFlag",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Profit_Flag",
        "data_type": "boolean",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_ProfitFlag",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Accept_Medicare",
        "data_type": "boolean",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_AcceptMedicare",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "E_Medical_Record",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_EMedicalRecord",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Pay_Perform",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_PayPerform",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "E_Prescribe",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_EPrescribe",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Formulary",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_Formulary",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Activation_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_ActivationDate",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Accept_Medicaid",
        "data_type": "boolean",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_AcceptMedicaid",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Ownership_Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_ownership_status",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Source_Created_Date",
        "data_type": "timestamp",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_source_createdate",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Source_Updated_Date",
        "data_type": "timestamp",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_source_updatedate",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": "staging.hco_name",
        "src_attribute": "Third_Party_ID",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_name"
        ),
        "tgt_attribute": "HCO_X_third_party_id",
        "tgt_data_type": "string",
    },

    # -------------------------------------------------------------
    # HCO Identification
    # -------------------------------------------------------------

    {
        "src_tbl_nm": (
            "staging.hco_identification"
        ),
        "src_attribute": "Identifier_Value",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_alternate_identifier"
        ),
        "tgt_attribute": "altValue",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_identification"
        ),
        "src_attribute": "Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_alternate_identifier"
        ),
        "tgt_attribute": "IdentifierStatus",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_identification"
        ),
        "src_attribute": "Identifier_Issuer",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_alternate_identifier"
        ),
        "tgt_attribute": "X_infa360_identifierIssuer",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_identification"
        ),
        "src_attribute": "Issuing_Country",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_alternate_identifier"
        ),
        "tgt_attribute": "X_infa360_issuingCountry",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_identification"
        ),
        "src_attribute": "Issuing_State",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_alternate_identifier"
        ),
        "tgt_attribute": "X_infa360_issuingState",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_identification"
        ),
        "src_attribute": "Activation_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_alternate_identifier"
        ),
        "tgt_attribute": "X_activation_date",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": (
            "staging.hco_identification"
        ),
        "src_attribute": "Expiration_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_alternate_identifier"
        ),
        "tgt_attribute": "X_expiration_date",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": (
            "staging.hco_identification"
        ),
        "src_attribute": "Source_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_alternate_identifier"
        ),
        "tgt_attribute": "sourceSystem",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_identification"
        ),
        "src_attribute": "Source_FK",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_alternate_identifier"
        ),
        "tgt_attribute": "AlternateIdentifier_parentId",
        "tgt_data_type": "string",
    },

    # -------------------------------------------------------------
    # HCO Phone
    # -------------------------------------------------------------

    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Phone_PK",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "sourcePKey",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Primary_Phone",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_primary_phone",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Phone_Usage_Type",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_phone_usage_type",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Phone_Type",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_phone_type",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Phone_Number",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_phone_number",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Phone_Number_Extension",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_phone_number_extension",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "ISO",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_iso",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_phone_status",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Effective_Start_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_effective_start_date",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Effective_End_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_effective_end_date",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Source_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "sourceSystem",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_phone"
        ),
        "src_attribute": "Source_FK",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_phone"
        ),
        "tgt_attribute": "X_phone_parentId",
        "tgt_data_type": "string",
    },

    # -------------------------------------------------------------
    # HCO Specialty
    # -------------------------------------------------------------

    {
        "src_tbl_nm": (
            "staging.hco_specialty"
        ),
        "src_attribute": "Specialty_PK",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_specialty"
        ),
        "tgt_attribute": "sourcePKey",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_specialty"
        ),
        "src_attribute": "Specialty_Rank",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_specialty"
        ),
        "tgt_attribute": "X_infa360_rank",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_specialty"
        ),
        "src_attribute": "Specialty",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_specialty"
        ),
        "tgt_attribute": "X_infa360_Specialty",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_specialty"
        ),
        "src_attribute": "Specialty_Type",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_specialty"
        ),
        "tgt_attribute": "X_specialty_type",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_specialty"
        ),
        "src_attribute": "Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_specialty"
        ),
        "tgt_attribute": "X_specialty_status",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_specialty"
        ),
        "src_attribute": "Specialty_Source",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_specialty"
        ),
        "tgt_attribute": "X_specialty_source",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_specialty"
        ),
        "src_attribute": "Global_Specialty",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_specialty"
        ),
        "tgt_attribute": "X_global_specialty",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_specialty"
        ),
        "src_attribute": "Group_Specialty",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_specialty"
        ),
        "tgt_attribute": "X_group_specialty",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "staging.hco_specialty"
        ),
        "src_attribute": "Source_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "hco_specialty"
        ),
        "tgt_attribute": "sourceSystem",
        "tgt_data_type": "string",
    },
]


# HCP: the supplied project contains HCP MDM payload attributes in the
# existing API implementation, but no dedicated column-level HCP ingress
# mapping. Keep this as a logical payload contract only; do not fabricate
# physical Informatica/Snowflake tables.
HCP_TARGET_TABLE = "hcp"
# Development smoke-test mapping for the validated HCP canonical payload.
DEV_HCP_FIELD_MAPPING = {
    "individualEid": "individualEid",
    "firstName": "firstName",
    "middleName": "middleName",
    "lastName": "lastName",
    "genderCode": "genderCode",
    "countryCode": "countryCode",
}

HCP_PAYLOAD_ATTRIBUTES = [
    "X_transparency_reporting_name",
    "firstName", "middleName", "lastName", "fullName", "gender",
    "X_infac360ls_type", "X_hcp_status", "X_jisb_title", "prefixName",
    "AlternateName", "X_hcp_address", "Phone",
    "X_infac360ls_Specialty", "Qualification",
    "X_infac360ls_License", "X_infac360ls_dea",
    "AlternateIdentifier", "ElectronicAddress",
]

def get_hcp_ingress_mapping() -> List[Tuple[str, str]]:
    """Return HCP payload mappings supported by the project."""
    return [(
        attribute,
        f"{HCP_TARGET_TABLE}/{attribute}",
    ) for attribute in HCP_PAYLOAD_ATTRIBUTES]


def get_hco_ingress_mapping() -> List[Tuple[str, str]]:
    """Return all 56 HCO field-level source -> physical-target mappings."""
    return [
        (m["src_attribute"], m["tgt_tbl_nm"])
        for m in HCO_INGRESS_MAPPING
    ]


def get_hco_ingress_field_mapping() -> List[dict]:
    """Return all 56 existing HCO field-level mappings."""
    return list(HCO_INGRESS_MAPPING)


# ---------------------------------------------------------------------------
# HCP ingress
# ---------------------------------------------------------------------------

def prepare_hcp_ingress(
    df: DataFrame,
    source_table: str,
    target_table: str,
    field_mapping: Optional[Dict[str, str]] = None,
) -> Dict[str, DataFrame]:
    """
    Prepare HCP data for an explicitly approved physical target.

    Prepare HCP records for the configured MDM target using the supplied field mapping.
    """
    if not target_table:
        raise ValueError(
            "Physical HCP ingress target_table is required. "
            "Do not derive it from an Informatica logical object name."
        )

    # The development smoke-test mapping matches the validated canonical HCP payload.
    if not field_mapping and target_table and target_table.lower() == "hcp":
        field_mapping = dict(DEV_HCP_FIELD_MAPPING)
        logger.info(
            "Using TEST_HCP physical field mapping for target %s.",
            target_table,
        )

    if not field_mapping:
        raise RuntimeError(
            "HCP ingress field mapping is not supplied. "
            "The project source does not contain an approved production "
            "column-level HCP ingress mapping."
        )

    source_columns = {c.lower(): c for c in df.columns}
    select_exprs = []

    for source_column, target_column in field_mapping.items():
        actual_column = source_columns.get(source_column.lower())
        if actual_column is None:
            continue
        select_exprs.append(
            F.col(actual_column).alias(target_column)
        )

    if not select_exprs:
        raise RuntimeError(
            f"No configured HCP ingress columns were found in source table: "
            f"{source_table}"
        )

    selected_names = {
        str(target_column).lower()
        for source_column, target_column in field_mapping.items()
        if str(source_column).lower() in source_columns
    }

    for column_name in df.columns:
        if column_name.upper() in {"BATCH_ID", "LOAD_DATE", "SOURCE_NAME"}:
            if str(column_name).lower() not in selected_names:
                select_exprs.append(F.col(column_name))

    return {target_table: df.select(*select_exprs)}


# ---------------------------------------------------------------------------
# HCO ingress
# ---------------------------------------------------------------------------

def prepare_hco_ingress(
    df: DataFrame,
    source_table: str,
    target_table_map: Optional[Dict[str, str]] = None,
) -> Dict[str, DataFrame]:
    """
    Prepare HCO data using the configured HCO field-level mappings.

    Multiple source fields can belong to the same physical MDM target, so
    mappings are consolidated per logical target instead of overwriting
    earlier fields in a dictionary.
    """
    if not target_table_map:
        raise RuntimeError(
            "HCO physical target_table_map is required. "
            "The project contains logical Informatica target objects, "
            "not approved physical Snowflake target names."
        )

    prepared: Dict[str, DataFrame] = {}
    actual_table = source_table.lower()
    source_columns = {c.lower(): c for c in df.columns}

    grouped: Dict[str, list] = {}

    for mapping in HCO_INGRESS_MAPPING:
        mapped_source = mapping["src_tbl_nm"].lower()
        if mapped_source.split(".")[-1] != actual_table.split(".")[-1]:
            continue

        source_column = mapping["src_attribute"]
        if source_column.lower() not in source_columns:
            continue

        logical_target = mapping["tgt_tbl_nm"]
        physical_target = target_table_map.get(logical_target)
        if not physical_target:
            raise RuntimeError(
                f"No physical target configured for HCO target table: "
                f"{logical_target}"
            )

        grouped.setdefault(physical_target, []).append(mapping)

    for physical_target, mappings in grouped.items():
        cols = []

        for mapping in mappings:
            source_column = mapping["src_attribute"]
            expr = F.col(source_columns[source_column.lower()])
            target_type = mapping.get("tgt_data_type")

            if target_type == "integer":
                expr = expr.cast("integer")
            elif target_type == "date/time":
                expr = expr.cast("timestamp")
            elif target_type == "date":
                expr = expr.cast("date")
            elif target_type == "string":
                expr = expr.cast("string")

            cols.append(expr.alias(mapping["tgt_attribute"]))

        metadata_names = {c.upper() for c in df.columns}
        for c in df.columns:
            if c.upper() in {"BATCH_ID", "LOAD_DATE", "SOURCE_NAME"}:
                if c.upper() not in metadata_names:
                    continue
                if c not in {x.name for x in cols if hasattr(x, "name")}:
                    cols.append(F.col(c))

        prepared[physical_target] = df.select(*cols)

    return prepared


# ---------------------------------------------------------------------------
# Physical publish layer
# ---------------------------------------------------------------------------

def write_prepared_ingress(
    spark: SparkSession,
    prepared: Dict[str, DataFrame],
    publish_schema: str,
    batch_id: int,
) -> int:
    """
    Write prepared ingress datasets to an already approved physical
    publish table.

    Physical target names MUST be supplied explicitly.

    This function intentionally does not derive physical Snowflake/
    Databricks table names from Informatica logical object paths.
    """

    if not prepared:
        logger.warning(
            "No prepared ingress datasets were generated for batch %s.",
            batch_id,
        )
        return 0

    processed = 0

    for physical_target, df in prepared.items():

        if not physical_target:
            raise ValueError(
                "Physical target name is required for ingress write."
            )

        qualified_target = _qualify_table(
            physical_target,
            publish_schema,
        )

        if not _table_exists(spark, qualified_target):
            raise RuntimeError(
                f"Approved physical MDM target table does not exist: "
                f"{qualified_target}. "
                "The ingress module will not create an invented "
                "target table."
            )

        output_df = df

        if "MDM_INGRESS_PROCESSED_AT" not in output_df.columns:
            output_df = output_df.withColumn(
                "MDM_INGRESS_PROCESSED_AT",
                F.current_timestamp(),
            )

        (
            output_df
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(qualified_target)
        )

        count = output_df.count()

        logger.info(
            "Published %s records to %s for batch %s.",
            count,
            qualified_target,
            batch_id,
        )

        processed += count

    return processed


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_hcp_ingress(
    spark: SparkSession,
    source_table: str,
    source_system_name: str,
    batch_id: int,
    publish_schema: str = DEFAULT_PUBLISH_SCHEMA,
    target_table: Optional[str] = None,
    field_mapping: Optional[Dict[str, str]] = None,
) -> int:
    """
    Process one HCP ingress batch.

    Verify, filter, deduplicate, map, and publish one HCP batch.
    """

    logger.info(
        "Starting HCP MDM ingress. source=%s batch_id=%s",
        source_table,
        batch_id,
    )

    df = read_source_table(
        spark=spark,
        source_table=source_table,
        batch_id=batch_id,
    )

    df = validate_ingress_input(
        df=df,
        source_table=source_table,
    )

    if "individualEid" in df.columns:
        df = deduplicate_for_ingress(
            df=df,
            business_key="individualEid",
        )

    prepared = prepare_hcp_ingress(
        df=df,
        source_table=source_table,
        target_table=target_table,
        field_mapping=field_mapping,
    )

    return write_prepared_ingress(
        spark=spark,
        prepared=prepared,
        publish_schema=publish_schema,
        batch_id=batch_id,
    )


def process_hco_ingress(
    spark: SparkSession,
    source_table: str,
    source_system_name: str,
    batch_id: int,
    publish_schema: str = DEFAULT_PUBLISH_SCHEMA,
    target_table_map: Optional[Dict[str, str]] = None,
) -> int:
    """
    Process one HCO ingress batch.
    """

    logger.info(
        "Starting HCO MDM ingress. source=%s batch_id=%s",
        source_table,
        batch_id,
    )

    df = read_source_table(
        spark=spark,
        source_table=source_table,
        batch_id=batch_id,
    )

    df = validate_ingress_input(
        df=df,
        source_table=source_table,
    )

    if "source_id" in df.columns:
        df = deduplicate_for_ingress(
            df=df,
            business_key="source_id",
        )

    prepared = prepare_hco_ingress(
        df=df,
        source_table=source_table,
        target_table_map=target_table_map,
    )

    return write_prepared_ingress(
        spark=spark,
        prepared=prepared,
        publish_schema=publish_schema,
        batch_id=batch_id,
    )


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def _mark_ingress_status(spark, source_system_name: str, batch_id: int) -> None:
    """Mark the completed ingress batch as successful."""
    try:
        from src.core.runtime_config import batch_log_tbl
    except Exception:
        try:
            from core.runtime_config import batch_log_tbl
        except Exception:
            batch_log_tbl = "HMDM_DEV.util.ctl_batch_log_tbl"

    source_sql = str(source_system_name).replace("'", "''")
    spark.sql(
        f"""
        UPDATE {batch_log_tbl}
        SET ingress_status = 'Y'
        WHERE source_system_name = '{source_sql}'
          AND batch_id = {int(batch_id)}
        """
    )


def process_ingress(
    spark: SparkSession,
    source_identifier: str,
    source_system_name: str,
    source_table: str,
    entity_type: str,
    publish_schema: str = DEFAULT_PUBLISH_SCHEMA,
    target_table: Optional[str] = None,
    field_mapping: Optional[Dict[str, str]] = None,
    target_table_map: Optional[Dict[str, str]] = None,
) -> int:
    """
    Main ingress entry point.

    Only batches satisfying the project's ingress gating condition
    are processed.
    """

    pending_batches = get_pending_ingress_batches(
        spark=spark,
        source_system_name=source_system_name,
    )

    if not pending_batches:
        logger.info(
            "No batches are eligible for MDM ingress for source system %s.",
            source_system_name,
        )
        return 0

    total_processed = 0

    for batch_id in pending_batches:

        logger.info(
            "Processing ingress batch %s for %s.",
            batch_id,
            source_identifier,
        )

        try:

            if entity_type.upper() == "HCP":

                processed = process_hcp_ingress(
                    spark=spark,
                    source_table=source_table,
                    source_system_name=source_system_name,
                    batch_id=batch_id,
                    publish_schema=publish_schema,
                    target_table=target_table,
                    field_mapping=field_mapping,
                )

            elif entity_type.upper() == "HCO":

                processed = process_hco_ingress(
                    spark=spark,
                    source_table=source_table,
                    source_system_name=source_system_name,
                    batch_id=batch_id,
                    publish_schema=publish_schema,
                    target_table_map=target_table_map,
                )

            else:
                raise ValueError(
                    f"Unsupported entity_type '{entity_type}'. "
                    "Expected HCP or HCO."
                )

            # Update only the actual control-table column that exists.
            update_batch_log_tbl(
                "ingress",
                "Y",
                batch_id,
                source_system_name,
            )

            total_processed += processed

            logger.info(
                "Ingress batch %s completed successfully. "
                "Records processed=%s",
                batch_id,
                processed,
            )

        except Exception:
            logger.exception(
                "Ingress failed for batch %s.",
                batch_id,
            )
            raise

    return total_processed


def main(
    spark: SparkSession,
    source_identifier: str,
    source_system_name: str,
    source_table: str,
    entity_type: str,
    publish_schema: str = DEFAULT_PUBLISH_SCHEMA,
    target_table: Optional[str] = None,
    field_mapping: Optional[Dict[str, str]] = None,
    target_table_map: Optional[Dict[str, str]] = None,
) -> int:
    """
    Public entry point for Databricks/Airflow.
    """

    start_time = datetime.utcnow()

    logger.info(
        "MDM ingress started. source_identifier=%s "
        "source_system=%s entity_type=%s source_table=%s",
        source_identifier,
        source_system_name,
        entity_type,
        source_table,
    )

    try:

        processed = process_ingress(
            spark=spark,
            source_identifier=source_identifier,
            source_system_name=source_system_name,
            source_table=source_table,
            entity_type=entity_type,
            publish_schema=publish_schema,
            target_table=target_table,
            field_mapping=field_mapping,
            target_table_map=target_table_map,
        )

        elapsed = (
            datetime.utcnow() - start_time
        ).total_seconds()

        logger.info(
            "MDM ingress completed successfully. "
            "records=%s elapsed_seconds=%s",
            processed,
            elapsed,
        )

        return processed

    except Exception:
        logger.exception(
            "MDM ingress failed. source_identifier=%s "
            "source_system=%s entity_type=%s",
            source_identifier,
            source_system_name,
            entity_type,
        )
        raise
