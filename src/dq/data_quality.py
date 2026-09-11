"""
Healthcare MDM - Data Quality (Land_to_Stage) engine.

This module implements the "Land_to_Stage" pipeline stage: it takes the
already-standardized LANDING-layer Delta tables and applies the project's
configured Data Quality (DQ) rules before writing the surviving records to
the STAGING layer. This is the exact stage documented as "Land_to_Stag" in
the HMDM_DEV mapping workbook - every DQ_RULES entry below corresponds to
one row of that sheet (source table, rule name/description, the column the
rule is evaluated on, and the STAGING target table).

Rule types implemented:
    - null_check:                      reject rows where a given column is NULL.
    - name_address_completeness_check: reject a name-table row unless a
                                        matching, complete address row
                                        exists for the same source_id.
    - address_mdr_check:               reject an address row unless the
                                        parent name record exists AND the
                                        address itself is non-blank
                                        ("MDR" = Mandatory Data Requirement).
    - mdr_check / affiliation_mdr_check / hierarchy_mdr_check:
                                        reject a child-table row (email,
                                        phone, specialty, tax, affiliation,
                                        hierarchy, etc.) unless its foreign
                                        key exists in the corresponding
                                        parent/name table.

High-level flow (see main_data_quality_pipeline / execute_source_dq):
    1. Look up which DQ rules apply to a source system (either from the
       DQ_RULES constant below, or from the ctl_dq_entity_mstr control
       table when one is configured for the environment).
    2. For each rule, read the relevant LANDING table(s), apply the rule's
       filtering logic, and write the passing rows to the STAGING table.
    3. Rejected rows are written to the DQ reject table (dqm_reject_tbl)
       together with the rule that rejected them, for audit and reprocessing.
    4. Every rule execution and the overall batch outcome are logged to the
       DQ log table (dqm_log_tbl) and the shared pipeline log table
       (log_tbl_nm), and the batch control table is updated so the next
       pipeline stage (MDM ingress) knows this batch's DQ step is complete.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

MODULE_NAME = "DATA_QUALITY"


# ---------------------------------------------------------------------
# Control tables
# ---------------------------------------------------------------------

try:
    from ..core.runtime_config import (
        dqm_config_tbl,
        dqm_log_tbl,
        dqm_reject_tbl,
        batch_log_tbl,
        log_tbl_nm,
    )
except Exception:
    try:
        from core.runtime_config import (
            dqm_config_tbl,
            dqm_log_tbl,
            dqm_reject_tbl,
            batch_log_tbl,
            log_tbl_nm,
        )
    except Exception:
        # Local/test fallback only.
        dqm_config_tbl = (
            "HMDM_DEV.util.ctl_dq_entity_mstr"
        )
        dqm_log_tbl = (
            "HMDM_DEV.util.ctl_dqm_log_tbl"
        )
        dqm_reject_tbl = (
            "HMDM_DEV.util.dqm_reject_tbl"
        )
        batch_log_tbl = (
            "HMDM_DEV.util.ctl_batch_log_tbl"
        )
        log_tbl_nm = (
            "HMDM_DEV.util.ctl_log_tbl"
        )


DQ_CONFIG_TABLE = dqm_config_tbl


class DQProcessingError(Exception):
    """Raised when DQ processing itself fails."""


# ---------------------------------------------------------------------
# DQ Rule
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class DQRule:
    """
    One configured Data Quality rule, corresponding to a single row of the
    Land_to_Stag sheet in the HMDM_DEV mapping workbook.

    Attributes:
        source_table: LANDING-layer table the rule reads from (e.g. "hcp_name").
        rule_name: Name of the rule implementation to apply - must match one
            of the rule functions dispatched in apply_rule() (e.g.
            "null_check", "mdr_check", "address_mdr_check").
        rule_description: Human-readable explanation of what the rule
            rejects, used for logging and for the reject-table audit trail.
        dq_application_column: The specific column the rule is evaluated
            against (e.g. the column that must not be NULL, or the foreign
            key column that must exist in the parent table).
        target_table: STAGING-layer table that passing rows are written to.
    """
    source_table: str
    rule_name: str
    rule_description: str
    dq_application_column: str
    target_table: str


# ---------------------------------------------------------------------
# Configured Landing-to-Staging DQ rules
# ---------------------------------------------------------------------

DQ_RULES: List[DQRule] = [
    DQRule(
        # BUGFIX: source_table was incorrectly "hcp_name" even though the
        # rule's column ("hco_name") and target_table are both HCO-side.
        # This mirrored the same copy-paste error found and corrected in
        # the Land_to_Stag sheet of the HMDM_DEV mapping workbook.
        "hco_name",
        "null_check",
        "Reject records if the value of the column is Null",
        "hco_name",
        "hco_name",
    ),
    DQRule(
        "hcp_name",
        "null_check",
        "Reject records if the value of the column is Null",
        "first_name",
        "hco_name",
    ),
    DQRule(
        "hcp_name",
        "name_address_completeness_check",
        "Reject records in name table if no complete address is present",
        "source_id",
        "hcp_name",
    ),
    DQRule(
        "hco_name",
        "name_address_completeness_check",
        "Reject records in name table if no complete address is present",
        "source_id",
        "hco_name",
    ),
    DQRule(
        "hcp_address",
        "address_mdr_check",
        "Reject records if no complete address is present or source_fk is not present in name table",
        "source_fk",
        "hcp_address",
    ),
    DQRule(
        "hco_address",
        "address_mdr_check",
        "Reject records if no complete address is present or source_fk is not present in name table",
        "source_fk",
        "hco_address",
    ),
    DQRule(
        "hcp_email",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in email table",
        "source_fk",
        "hcp_email",
    ),
    DQRule(
        "hcp_alternate_name",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in alternate name table",
        "source_fk",
        "hcp_alternate_name",
    ),
    DQRule(
        "hcp_identification",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in identification table",
        "source_fk",
        "hcp_identification",
    ),
    DQRule(
        "hcp_specialty",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in specialty table",
        "source_fk",
        "hcp_specialty",
    ),
    DQRule(
        "hcp_phone",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in phone table",
        "source_fk",
        "hcp_phone",
    ),
    DQRule(
        "hcp_education",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in education table",
        "source_fk",
        "hcp_education",
    ),
    DQRule(
        "hcp_origin_university",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in origin university table",
        "source_fk",
        "hcp_origin_university",
    ),
    DQRule(
        "hcp_tax",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in table",
        "source_fk",
        "hcp_tax",
    ),
    DQRule(
        "hco_tax",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in table",
        "source_fk",
        "hco_tax",
    ),
    DQRule(
        "hco_email",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in email table",
        "source_fk",
        "hco_email",
    ),
    DQRule(
        "hco_alternate_name",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in alternate name table",
        "source_fk",
        "hco_alternate_name",
    ),
    DQRule(
        "hco_identification",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in identification table",
        "source_fk",
        "hco_identification",
    ),
    DQRule(
        "hco_specialty",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in specialty table",
        "source_fk",
        "hco_specialty",
    ),
    DQRule(
        "hco_phone",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in phone table",
        "source_fk",
        "hco_phone",
    ),
    DQRule(
        "hcp_hco_affiliation",
        "affiliation_mdr_check",
        "Reject records in name table if source id is not present",
        "source_id",
        "hcp_hco_affiliation",
    ),
    DQRule(
        "hco_hco_hierarchy",
        "hierarchy_mdr_check",
        "Reject records if source id is not present in name table",
        "source_id",
        "hco_hco_hierarchy",
    ),
    DQRule(
        "hcp_tendencies",
        "mdr_check",
        # BUGFIX: description previously said "phone table" (copy-paste
        # from the HCP_PHONE rule above) instead of "tendencies table".
        "Reject records if the value of the source_fk is not present in tendencies table",
        "source_fk",
        "hcp_tendencies",
    ),
    DQRule(
        "hcp_language",
        "mdr_check",
        # BUGFIX: description previously said "phone table" (copy-paste
        # from the HCP_PHONE rule above) instead of "language table".
        "Reject records if the value of the source_fk is not present in language table",
        "source_fk",
        "hcp_language",
    ),
]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def qualified(
    table_name: str,
    catalog_name: Optional[str] = None,
) -> str:
    """
    Ensure a table name is fully qualified with the current Unity Catalog
    name, without double-prefixing a name that is already qualified.

    Args:
        table_name: The table name to qualify, e.g. "staging.hcp_name" or
            an already-qualified "HMDM_DEV.staging.hcp_name".
        catalog_name: The catalog to prefix with (typically the
            environment's `catalog` value from runtime_config). If None,
            the table name is returned unchanged.

    Returns:
        str: The fully-qualified table name.

    Raises:
        DQProcessingError: if table_name is empty, since every DQ operation
            needs a concrete table to read from or write to.
    """

    if not table_name:
        raise DQProcessingError(
            "table_name cannot be empty."
        )

    if catalog_name and not table_name.startswith(
        f"{catalog_name}."
    ):
        return f"{catalog_name}.{table_name}"

    return table_name


def resolve_column(
    df: DataFrame,
    column_name: str,
):
    """Resolve dataframe column case-insensitively."""

    if df is None:
        raise DQProcessingError(
            "Source dataframe cannot be None."
        )

    if not column_name:
        raise DQProcessingError(
            "DQ application column cannot be empty."
        )

    lookup = {
        column.lower(): column
        for column in df.columns
    }

    actual = lookup.get(column_name.lower())

    if actual is None:
        raise DQProcessingError(
            f"Column '{column_name}' not found "
            f"in source dataframe. "
            f"Available columns: {df.columns}"
        )

    return F.col(actual)


def non_blank(column):
    """Treat NULL and blank/whitespace strings as invalid."""

    return (
        column.isNotNull()
        & (
            F.trim(column.cast("string")) != ""
        )
    )


def table_exists(table_name: str) -> bool:
    """Safe table-existence check."""

    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


# ---------------------------------------------------------------------
# Batch handling
# ---------------------------------------------------------------------

def resolve_batch_id(
    source_df: DataFrame,
    batch_id: Optional[int] = None,
) -> Optional[int]:
    """
    Resolve the DQ batch.

    If batch_id is explicitly supplied, use it.

    Otherwise, when BATCH_ID exists, process only the latest
    batch in the source dataframe.

    This prevents historical batches from being processed again
    when the staging table contains multiple batches.
    """

    if batch_id is not None:
        return int(batch_id)

    batch_column = None

    for column in source_df.columns:
        if column.lower() == "batch_id":
            batch_column = column
            break

    if batch_column is None:
        return None

    latest_row = (
        source_df
        .select(F.max(F.col(batch_column)).alias("_latest_batch_id"))
        .collect()[0]
    )

    latest_batch_id = latest_row["_latest_batch_id"]

    if latest_batch_id is None:
        return None

    return int(latest_batch_id)


def filter_to_batch(
    source_df: DataFrame,
    batch_id: Optional[int] = None,
) -> Tuple[DataFrame, Optional[int]]:
    """
    Restrict source dataframe to one batch.

    If BATCH_ID is present and batch_id is not supplied,
    the latest BATCH_ID is selected.
    """

    resolved_batch_id = resolve_batch_id(
        source_df,
        batch_id,
    )

    if resolved_batch_id is None:
        return source_df, None

    batch_column = resolve_column(
        source_df,
        "BATCH_ID",
    )

    filtered_df = source_df.filter(
        batch_column == F.lit(resolved_batch_id)
    )

    return filtered_df, resolved_batch_id


# ---------------------------------------------------------------------
# Rule: null_check
# ---------------------------------------------------------------------

def apply_null_check(
    df: DataFrame,
    column_name: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Implements the "null_check" DQ rule: split a dataframe into rows that
    pass (the target column is non-null and non-blank) and rows that fail.

    Args:
        df: The LANDING-layer dataframe to check.
        column_name: Name of the column that must not be null/blank
            (resolved case-insensitively).

    Returns:
        tuple[DataFrame, DataFrame]: (passed_rows, rejected_rows).
    """

    column = resolve_column(
        df,
        column_name,
    )

    valid_condition = non_blank(column)

    passed = df.filter(valid_condition)

    rejected = df.filter(
        ~valid_condition
    )

    return passed, rejected


# ---------------------------------------------------------------------
# Rule: name_address_completeness_check
# ---------------------------------------------------------------------

def apply_name_address_completeness_check(
    name_df: DataFrame,
    address_df: DataFrame,
    key_column: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Implements the "name_address_completeness_check" DQ rule: a name-table
    row (HCP_NAME or HCO_NAME) only passes if there is at least one
    non-blank address row for the same key in the corresponding address
    table. This prevents HCPs/HCOs with no usable address from being
    mastered downstream.

    Args:
        name_df: The LANDING-layer name dataframe (e.g. hcp_name/hco_name).
        address_df: The LANDING-layer address dataframe for the same
            entity (e.g. hcp_address/hco_address).
        key_column: The join key present in both dataframes (typically
            "source_id"/"source_fk") used to match a name row to its
            address row(s).

    Returns:
        tuple[DataFrame, DataFrame]: (passed_rows, rejected_rows), both
        with the original name_df schema (the temporary join helper
        columns are dropped before returning).
    """

    name_key = resolve_column(
        name_df,
        key_column,
    )

    address_key = resolve_column(
        address_df,
        key_column,
    )

    complete_addresses = (
        address_df
        .filter(non_blank(address_key))
        .select(
            address_key.alias("_dq_key")
        )
        .distinct()
    )

    evaluated = (
        name_df
        .withColumn(
            "_dq_name_key",
            name_key,
        )
        .join(
            complete_addresses,
            F.col("_dq_name_key")
            == F.col("_dq_key"),
            "left",
        )
    )

    passed = (
        evaluated
        .filter(
            F.col("_dq_key").isNotNull()
        )
        .drop(
            "_dq_name_key",
            "_dq_key",
        )
    )

    rejected = (
        evaluated
        .filter(
            F.col("_dq_key").isNull()
        )
        .drop(
            "_dq_name_key",
            "_dq_key",
        )
    )

    return passed, rejected


# ---------------------------------------------------------------------
# Rule: address_mdr_check
# ---------------------------------------------------------------------

def apply_address_mdr_check(
    address_df: DataFrame,
    name_df: DataFrame,
    source_fk_column: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Implements the "address_mdr_check" DQ rule: an address row only passes
    if (a) its own address value is non-blank AND (b) its foreign key
    exists among the parent name table's keys. Rejects orphaned addresses
    and addresses that point to a name record that does not exist.

    Args:
        address_df: The LANDING-layer address dataframe (e.g. hcp_address).
        name_df: The LANDING-layer name dataframe used to validate the
            foreign key (e.g. hcp_name). The parent key column is detected
            automatically by checking, in order: Source_ID, Source_FK,
            Third_Party_ID, individualEid, individualId.
        source_fk_column: The foreign-key column on address_df that should
            match a key in name_df.

    Returns:
        tuple[DataFrame, DataFrame]: (passed_rows, rejected_rows), with the
        original address_df schema.

    Raises:
        DQProcessingError: if none of the expected parent-key candidate
            columns are present on name_df.
    """

    address_fk = resolve_column(
        address_df,
        source_fk_column,
    )

    name_key_candidates = [
        "Source_ID",
        "Source_FK",
        "Third_Party_ID",
        "individualEid",
        "individualId",
    ]

    name_key = None

    name_columns = {
        column.lower()
        for column in name_df.columns
    }

    for candidate in name_key_candidates:
        if candidate.lower() in name_columns:
            name_key = resolve_column(
                name_df,
                candidate,
            )
            break

    if name_key is None:
        raise DQProcessingError(
            "Unable to identify the name-table key column "
            "required by address_mdr_check. "
            f"Available columns: {name_df.columns}"
        )

    name_keys = (
        name_df
        .filter(non_blank(name_key))
        .select(
            name_key.alias("_dq_name_key")
        )
        .distinct()
    )

    evaluated = (
        address_df
        .withColumn(
            "_dq_address_key",
            address_fk,
        )
        .join(
            name_keys,
            F.col("_dq_address_key")
            == F.col("_dq_name_key"),
            "left",
        )
    )

    valid_condition = (
        non_blank(
            F.col("_dq_address_key")
        )
        & F.col("_dq_name_key").isNotNull()
    )

    passed = (
        evaluated
        .filter(valid_condition)
        .drop(
            "_dq_address_key",
            "_dq_name_key",
        )
    )

    rejected = (
        evaluated
        .filter(~valid_condition)
        .drop(
            "_dq_address_key",
            "_dq_name_key",
        )
    )

    return passed, rejected


# ---------------------------------------------------------------------
# Rule: mdr_check
# ---------------------------------------------------------------------

def apply_mdr_check(
    child_df: DataFrame,
    parent_df: DataFrame,
    source_fk_column: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Implements the generic "mdr_check" DQ rule used by every HCP/HCO child
    table (email, phone, specialty, tax, education, alternate name,
    identification, origin university, tendencies, language, etc.): a
    child-table row only passes if its foreign key exists in the parent
    name table. This is the workhorse rule referenced by most entries in
    DQ_RULES.

    Args:
        child_df: The LANDING-layer child dataframe (e.g. hcp_specialty).
        parent_df: The LANDING-layer parent/name dataframe used to
            validate the foreign key. The parent key column is detected
            automatically by checking, in order: Source_ID, Source_FK,
            Third_Party_ID, individualEid, individualId.
        source_fk_column: The foreign-key column on child_df that should
            match a key in parent_df.

    Returns:
        tuple[DataFrame, DataFrame]: (passed_rows, rejected_rows), with the
        original child_df schema.

    Raises:
        DQProcessingError: if none of the expected parent-key candidate
            columns are present on parent_df.
    """

    child_fk = resolve_column(
        child_df,
        source_fk_column,
    )

    parent_key_candidates = [
        "Source_ID",
        "Source_FK",
        "Third_Party_ID",
        "individualEid",
        "individualId",
    ]

    parent_key = None

    parent_columns = {
        column.lower()
        for column in parent_df.columns
    }

    for candidate in parent_key_candidates:
        if candidate.lower() in parent_columns:
            parent_key = resolve_column(
                parent_df,
                candidate,
            )
            break

    if parent_key is None:
        raise DQProcessingError(
            "Unable to identify parent key for mdr_check. "
            f"Available columns: {parent_df.columns}"
        )

    parent_keys = (
        parent_df
        .filter(non_blank(parent_key))
        .select(
            parent_key.alias("_dq_parent_key")
        )
        .distinct()
    )

    evaluated = (
        child_df
        .withColumn(
            "_dq_child_fk",
            child_fk,
        )
        .join(
            parent_keys,
            F.col("_dq_child_fk")
            == F.col("_dq_parent_key"),
            "left",
        )
    )

    valid_condition = (
        non_blank(
            F.col("_dq_child_fk")
        )
        & F.col("_dq_parent_key").isNotNull()
    )

    passed = (
        evaluated
        .filter(valid_condition)
        .drop(
            "_dq_child_fk",
            "_dq_parent_key",
        )
    )

    rejected = (
        evaluated
        .filter(~valid_condition)
        .drop(
            "_dq_child_fk",
            "_dq_parent_key",
        )
    )

    return passed, rejected


# ---------------------------------------------------------------------
# Rule: affiliation_mdr_check
# ---------------------------------------------------------------------

def apply_affiliation_mdr_check(
    affiliation_df: DataFrame,
    name_df: DataFrame,
    source_id_column: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Implements the "affiliation_mdr_check" DQ rule for HCP_HCO_AFFILIATION:
    an affiliation row only passes if its source_id exists in the HCP name
    table. Thin wrapper around apply_mdr_check() kept as its own named rule
    so it can be referenced directly from DQ_RULES / the control table.
    """
    return apply_mdr_check(
        affiliation_df,
        name_df,
        source_id_column,
    )


# ---------------------------------------------------------------------
# Rule: hierarchy_mdr_check
# ---------------------------------------------------------------------

def apply_hierarchy_mdr_check(
    hierarchy_df: DataFrame,
    name_df: DataFrame,
    source_id_column: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Implements the "hierarchy_mdr_check" DQ rule for HCO_HCO_HIERARCHY: a
    parent/child hierarchy row only passes if its source_id exists in the
    HCO name table. Thin wrapper around apply_mdr_check() kept as its own
    named rule so it can be referenced directly from DQ_RULES / the control
    table.
    """
    return apply_mdr_check(
        hierarchy_df,
        name_df,
        source_id_column,
    )


# ---------------------------------------------------------------------
# Generic rule dispatcher
# ---------------------------------------------------------------------

def apply_rule(
    rule: DQRule,
    source_df: DataFrame,
    reference_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    Dispatch a DQRule to the correct rule-implementation function based on
    its rule_name, and run it. This is the single entry point every caller
    (run_single_rule / execute_source_dq) uses instead of calling the
    apply_*_check functions directly.

    Args:
        rule: The DQRule to execute.
        source_df: The LANDING-layer dataframe the rule is evaluated against.
        reference_df: The parent/reference dataframe required by every rule
            type except "null_check" (e.g. the name table for mdr_check,
            the address table for name_address_completeness_check).

    Returns:
        tuple[DataFrame, DataFrame]: (passed_rows, rejected_rows).

    Raises:
        DQProcessingError: if rule is None, if a required reference_df is
            missing, or if rule.rule_name does not match any implemented
            rule type.
    """

    if rule is None:
        raise DQProcessingError(
            "DQ rule cannot be None."
        )

    rule_type = rule.rule_name.strip().lower()

    if rule_type == "null_check":

        return apply_null_check(
            source_df,
            rule.dq_application_column,
        )

    if rule_type == "name_address_completeness_check":

        if reference_df is None:
            raise DQProcessingError(
                "Address reference dataframe is required "
                "for name_address_completeness_check."
            )

        return apply_name_address_completeness_check(
            source_df,
            reference_df,
            rule.dq_application_column,
        )

    if rule_type == "address_mdr_check":

        if reference_df is None:
            raise DQProcessingError(
                "Name reference dataframe is required "
                "for address_mdr_check."
            )

        return apply_address_mdr_check(
            source_df,
            reference_df,
            rule.dq_application_column,
        )

    if rule_type == "mdr_check":

        if reference_df is None:
            raise DQProcessingError(
                "Parent/reference dataframe is required "
                "for mdr_check."
            )

        return apply_mdr_check(
            source_df,
            reference_df,
            rule.dq_application_column,
        )

    if rule_type == "affiliation_mdr_check":

        if reference_df is None:
            raise DQProcessingError(
                "Name dataframe is required "
                "for affiliation_mdr_check."
            )

        return apply_affiliation_mdr_check(
            source_df,
            reference_df,
            rule.dq_application_column,
        )

    if rule_type == "hierarchy_mdr_check":

        if reference_df is None:
            raise DQProcessingError(
                "Name dataframe is required "
                "for hierarchy_mdr_check."
            )

        return apply_hierarchy_mdr_check(
            source_df,
            reference_df,
            rule.dq_application_column,
        )

    raise DQProcessingError(
        f"Unsupported DQ rule: {rule.rule_name}"
    )


# ---------------------------------------------------------------------
# DQ metadata
# ---------------------------------------------------------------------

def add_dq_metadata(
    df: DataFrame,
    rule: DQRule,
    status: str,
) -> DataFrame:
    """
    Stamp a dataframe with audit columns describing which DQ rule produced
    it and whether the rows passed or were rejected. Used to enrich both
    the passed-rows dataframe (written to STAGING) and the rejected-rows
    dataframe (written to the DQ reject table).

    Args:
        df: The dataframe to stamp (either the passed or rejected half of
            a rule's output).
        rule: The DQRule that was evaluated.
        status: "PASS" or "REJECT".

    Returns:
        DataFrame: the input dataframe with DQ_RULE, DQ_STATUS,
        DQ_DESCRIPTION, DQ_APPL_COLUMN and DQ_PROCESSED_AT columns added.
    """

    return (
        df
        .withColumn(
            "DQ_RULE",
            F.lit(rule.rule_name),
        )
        .withColumn(
            "DQ_STATUS",
            F.lit(status),
        )
        .withColumn(
            "DQ_DESCRIPTION",
            F.lit(rule.rule_description),
        )
        .withColumn(
            "DQ_APPL_COLUMN",
            F.lit(
                rule.dq_application_column
            ),
        )
        .withColumn(
            "DQ_PROCESSED_AT",
            F.current_timestamp(),
        )
    )


# ---------------------------------------------------------------------
# Run one rule
# ---------------------------------------------------------------------

def run_single_rule(
    rule: DQRule,
    source_df: DataFrame,
    reference_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    Run one DQRule end-to-end: apply the rule logic via apply_rule(), then
    stamp both the passed and rejected outputs with DQ audit metadata
    (add_dq_metadata) so downstream writers know which rule produced each
    row and what the outcome was.

    Args:
        rule: The DQRule to execute.
        source_df: The LANDING-layer dataframe to check.
        reference_df: The parent/reference dataframe required by most rule
            types (see apply_rule for details).

    Returns:
        tuple[DataFrame, DataFrame]: (passed_rows, rejected_rows), each
        with DQ_RULE/DQ_STATUS/DQ_DESCRIPTION/DQ_APPL_COLUMN/
        DQ_PROCESSED_AT columns added.
    """

    passed, rejected = apply_rule(
        rule=rule,
        source_df=source_df,
        reference_df=reference_df,
    )

    passed = add_dq_metadata(
        passed,
        rule,
        "PASS",
    )

    rejected = add_dq_metadata(
        rejected,
        rule,
        "REJECT",
    )

    return passed, rejected


# ---------------------------------------------------------------------
# Convert control-table row -> DQRule
# ---------------------------------------------------------------------

def _control_row_to_rule(row) -> DQRule:
    """
    Convert one row of the ctl_dq_entity_mstr control table into a DQRule
    instance, so environments that manage DQ rules in the control table
    (rather than the hard-coded DQ_RULES list) can be dispatched through
    exactly the same apply_rule() logic.

    Args:
        row: A Spark Row from ctl_dq_entity_mstr with columns table_name,
            rule_name, rule_type, rule_description, rule_expression,
            column_name.

    Returns:
        DQRule: constructed using rule_type as the dispatch key (falling
        back to rule_name only when rule_type is blank), and falling back
        to rule_expression or a generic message when rule_description is
        blank.
    """

    source_table = row["table_name"]

    configured_rule_name = row["rule_name"]

    rule_type = row["rule_type"]

    rule_name = (
        configured_rule_name
        if configured_rule_name
        else rule_type
    )

    description = (
        row["rule_description"]
        or row["rule_expression"]
        or f"DQ rule: {rule_name}"
    )

    dq_column = row["column_name"]

    target_table = row["table_name"]

    return DQRule(
        source_table=source_table,
        # Dispatcher requires rule type here.
        rule_name=rule_type,
        rule_description=description,
        dq_application_column=dq_column,
        target_table=target_table,
    )


# ---------------------------------------------------------------------
# Load DQ rules from centralized control configuration
# ---------------------------------------------------------------------

def get_configured_rules_for_source(
    source_identifier: str,
    source_system_name: Optional[str] = None,
) -> List[DQRule]:
    """
    Load the active, configured DQ rules for a source from the
    ctl_dq_entity_mstr control table (used in environments where DQ rules
    are managed centrally rather than hard-coded in DQ_RULES).

    Args:
        source_identifier: The source configuration identifier to filter
            on. If falsy, an empty list is returned immediately (no
            control-table lookup performed).
        source_system_name: Optional additional filter on source system
            name, for sources with multiple identifiers.

    Returns:
        list[DQRule]: the matching rules, restricted to rows where
        rule_status = 'ACTIVE' and active_flag = true, ordered by
        execution_order. Returns an empty list if the control table does
        not exist in this environment (falling back to DQ_RULES is the
        caller's responsibility - see get_rules_for_source).

    Raises:
        DQProcessingError: if the control table exists but is missing one
            of the columns this function depends on.
    """

    if not source_identifier:
        return []

    if not table_exists(DQ_CONFIG_TABLE):
        return []

    df = spark.table(
        DQ_CONFIG_TABLE
    )

    required_columns = {
        "source_identifier",
        "source_system_name",
        "table_name",
        "column_name",
        "rule_name",
        "rule_type",
        "rule_expression",
        "rule_description",
        "rule_status",
        "active_flag",
        "execution_order",
    }

    available_columns = {
        column.lower()
        for column in df.columns
    }

    missing_columns = {
        column
        for column in required_columns
        if column.lower()
        not in available_columns
    }

    if missing_columns:
        raise DQProcessingError(
            "DQ configuration table is missing "
            f"required columns: "
            f"{sorted(missing_columns)}"
        )

    condition = (
        F.upper(
            F.trim(
                F.col("source_identifier")
            )
        )
        == source_identifier.strip().upper()
    )

    if source_system_name is not None:

        condition = condition & (
            F.upper(
                F.trim(
                    F.col("source_system_name")
                )
            )
            == source_system_name.strip().upper()
        )

    condition = condition & (
        F.upper(
            F.trim(
                F.col("rule_status")
            )
        )
        == "ACTIVE"
    )

    condition = condition & (
        F.coalesce(
            F.col("active_flag"),
            F.lit(False),
        )
        == F.lit(True)
    )

    rows = (
        df
        .filter(condition)
        .orderBy(
            F.col("execution_order")
        )
        .collect()
    )

    return [
        _control_row_to_rule(row)
        for row in rows
    ]


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

def get_rules() -> List[DQRule]:
    """Return a copy of the full hard-coded DQ_RULES list (all entities)."""
    return list(DQ_RULES)


def get_rules_for_source(
    source_table: str,
    source_system_name: Optional[str] = None,
    source_identifier: Optional[str] = None,
) -> List[DQRule]:
    """
    Resolve the list of DQ rules that apply to one source_table, preferring
    control-table configuration (get_configured_rules_for_source) when a
    source_identifier is supplied and the control table has matching
    active rules, and otherwise falling back to the hard-coded DQ_RULES
    list filtered by source_table name.

    Args:
        source_table: The LANDING-layer table name to get rules for (e.g.
            "hcp_name"), matched case-insensitively against DQ_RULES.
        source_system_name: Optional filter passed through to the
            control-table lookup.
        source_identifier: Optional source configuration identifier; when
            provided, the control table is checked first.

    Returns:
        list[DQRule]: the rules to apply to this table, in the order they
        should be executed.
    """

    configured_rules: List[DQRule] = []

    if source_identifier:

        configured_rules = (
            get_configured_rules_for_source(
                source_identifier=source_identifier,
                source_system_name=source_system_name,
            )
        )

    if configured_rules:
        return configured_rules

    return [
        rule
        for rule in DQ_RULES
        if rule.source_table.lower()
        == source_table.lower()
    ]


def get_rule_count() -> int:
    """Return the total number of hard-coded rules in DQ_RULES."""
    return len(DQ_RULES)


# ---------------------------------------------------------------------
# Execute rules
# ---------------------------------------------------------------------

def execute_rules(
    source_df: DataFrame,
    rules: List[DQRule],
    reference_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    Run a whole list of DQ rules against one source dataframe, chaining
    them so that a row must pass every rule to reach the final STAGING
    output (a row rejected by an earlier rule is not re-evaluated by later
    rules).

    Args:
        source_df: The LANDING-layer dataframe to check.
        rules: The ordered list of DQRule objects to apply (see
            get_rules_for_source).
        reference_df: The parent/reference dataframe required by most rule
            types; passed through unchanged to every rule in the list.

    Returns:
        tuple[DataFrame, DataFrame]:
          - passed_df: rows that survived every rule, ready for STAGING.
          - rejected_df: the union of every rule's rejected rows (each
            still tagged with which rule rejected it via add_dq_metadata),
            or an empty-but-schema-matching dataframe if nothing was
            rejected.

    Raises:
        DQProcessingError: if source_df is None or rules is empty, since
            running with no rules would silently pass every row through.
    """

    if source_df is None:
        raise DQProcessingError(
            "source_df cannot be None."
        )

    if not rules:
        raise DQProcessingError(
            "No active DQ rules found for the supplied source."
        )

    passed_df = source_df

    rejected_dfs: List[DataFrame] = []

    for rule in rules:

        if passed_df.limit(1).count() == 0:
            break

        current_passed, current_rejected = (
            run_single_rule(
                rule=rule,
                source_df=passed_df,
                reference_df=reference_df,
            )
        )

        rejected_dfs.append(
            current_rejected
        )

        passed_df = current_passed

    if rejected_dfs:

        rejected_df = rejected_dfs[0]

        for next_rejected in rejected_dfs[1:]:

            rejected_df = (
                rejected_df.unionByName(
                    next_rejected,
                    allowMissingColumns=True,
                )
            )

    else:

        rejected_df = source_df.limit(0)

    return passed_df, rejected_df


# ---------------------------------------------------------------------
# DQ result summary
# ---------------------------------------------------------------------

def get_result_counts(
    passed_df: DataFrame,
    rejected_df: DataFrame,
) -> Dict[str, int]:
    """
    Compute simple pass/reject row counts for a DQ run, used in log
    messages and email alert bodies.

    Args:
        passed_df: The dataframe of rows that passed all DQ rules.
        rejected_df: The dataframe of rows rejected by any DQ rule.

    Returns:
        dict: {"passed_count": int, "rejected_count": int}.
    """

    return {
        "passed_count": passed_df.count(),
        "rejected_count": rejected_df.count(),
    }


# ---------------------------------------------------------------------
# Direct source DQ execution helper
# ---------------------------------------------------------------------

def execute_source_dq(
    source_identifier: str,
    source_table: str,
    source_system_name: Optional[str] = None,
    reference_table: Optional[str] = None,
    batch_id: Optional[int] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    Convenience, single-table entry point that resolves the applicable DQ
    rules and runs them, without writing anything to STAGING itself -
    intended for ad-hoc/interactive use (e.g. testing a table's rules in a
    notebook) rather than the full batch pipeline (see
    main_data_quality_pipeline for the production entry point).

    Args:
        source_identifier: Source configuration identifier used to look up
            control-table rules, if configured.
        source_table: Fully-qualified LANDING-layer table to check.
        source_system_name: Optional source-system filter.
        reference_table: Fully-qualified parent/reference table required
            by most rule types (e.g. the matching name table).
        batch_id: Optional explicit batch to restrict processing to; if
            omitted, the latest batch present in source_table is used
            (see filter_to_batch).

    Returns:
        tuple[DataFrame, DataFrame]: (passed_df, rejected_df).

    Raises:
        DQProcessingError: if source_table is blank or does not exist.
    """

    try:

        if not source_table:
            raise DQProcessingError(
                "source_table cannot be empty."
            )

        if not table_exists(source_table):
            raise DQProcessingError(
                f"Source table does not exist: "
                f"{source_table}"
            )

        # -------------------------------------------------------------
        # Read source table
        # -------------------------------------------------------------

        source_df = spark.table(
            source_table
        )

        # -------------------------------------------------------------
        # Resolve and apply batch filter
        # -------------------------------------------------------------

        source_df, resolved_batch_id = (
            filter_to_batch(
                source_df=source_df,
                batch_id=batch_id,
            )
        )

        # -------------------------------------------------------------
        # Load DQ rules
        # -------------------------------------------------------------

        rules = get_rules_for_source(
            source_table=source_table,
            source_system_name=source_system_name,
            source_identifier=source_identifier,
        )

        if not rules:
            raise DQProcessingError(
                f"No DQ rules found for "
                f"source_identifier={source_identifier}, "
                f"source_table={source_table}"
            )

        # -------------------------------------------------------------
        # Reference dataframe
        # -------------------------------------------------------------

        reference_df = None

        if reference_table:

            if not table_exists(
                reference_table
            ):
                raise DQProcessingError(
                    f"Reference table does not exist: "
                    f"{reference_table}"
                )

            reference_df = spark.table(
                reference_table
            )

        # -------------------------------------------------------------
        # Logging
        # -------------------------------------------------------------

        source_count = source_df.count()

        print(
            f"{MODULE_NAME}: "
            f"source_identifier={source_identifier}, "
            f"source_system_name={source_system_name}, "
            f"source_table={source_table}, "
            f"batch_id={resolved_batch_id}, "
            f"source_records={source_count}, "
            f"rules={len(rules)}"
        )

        # -------------------------------------------------------------
        # Execute DQ
        # -------------------------------------------------------------

        return execute_rules(
            source_df=source_df,
            rules=rules,
            reference_df=reference_df,
        )

    except DQProcessingError:
        raise

    except Exception as exc:

        raise DQProcessingError(
            f"DQ execution failed for "
            f"source_identifier={source_identifier}, "
            f"source_table={source_table}: {exc}"
        ) from exc


# ---------------------------------------------------------------------
# Complete DQ pipeline
# ---------------------------------------------------------------------

def main_data_quality_pipeline(
    source_identifier: str,
    source_table: str,
    source_system_name: Optional[str] = None,
    reference_table: Optional[str] = None,
    batch_id: Optional[int] = None,
) -> Tuple[DataFrame, DataFrame]:

    """
    Execute DQ and persist audit/rejection results.

    Steps:
      1. Execute configured DQ rules.
      2. Write rule-level results to DQ log table.
      3. Write rejected records to rejection table.
      4. Mark the batch DQ status as Y.
    """

    pipeline_start = datetime.now()

    try:
        passed_df, rejected_df = execute_source_dq(
            source_identifier=source_identifier,
            source_table=source_table,
            source_system_name=source_system_name,
            reference_table=reference_table,
            batch_id=batch_id,
        )

        passed_count = passed_df.count()
        rejected_count = rejected_df.count()

        if batch_id is None:
            _, resolved_batch_id = filter_to_batch(
                source_df=spark.table(source_table),
                batch_id=None,
            )
        else:
            resolved_batch_id = int(batch_id)

        rules = get_configured_rules_for_source(
            source_identifier=source_identifier,
            source_system_name=source_system_name,
        )

        if not rules:
            raise DQProcessingError(
                f"No active configured DQ rules found for "
                f"source_identifier={source_identifier}"
            )

        log_rows = []

        for rule in rules:
            rule_rejected_count = (
                rejected_df
                .filter(F.col("DQ_RULE") == rule.rule_name)
                .count()
            )

            rule_status = (
                "PASS"
                if rule_rejected_count == 0
                else "FAIL"
            )

            error_description = (
                None
                if rule_rejected_count == 0
                else rule.rule_description
            )

            log_rows.append(
                (
                    None,
                    int(resolved_batch_id),
                    source_identifier,
                    source_system_name,
                    source_table,
                    rule.rule_name,
                    rule_status,
                    error_description,
                    int(rule_rejected_count),
                    pipeline_start,
                    datetime.now(),
                )
            )

        dq_log_schema = StructType([
            StructField("run_id", StringType(), True),
            StructField("batch_id", LongType(), True),
            StructField("source_identifier", StringType(), True),
            StructField("source_system_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("rule_name", StringType(), True),
            StructField("rule_status", StringType(), True),
            StructField("error_description", StringType(), True),
            StructField("record_count", LongType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
        ])

        dq_log_df = spark.createDataFrame(
            log_rows,
            schema=dq_log_schema,
        )

        dq_log_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(dqm_log_tbl)

        if rejected_count > 0:

            if "individualEid" in rejected_df.columns:
                record_key = F.col(
                    "individualEid"
                ).cast("string")
            else:
                record_key = F.lit(None).cast("string")

            reject_df = (
                rejected_df
                .withColumn(
                    "run_id",
                    F.lit(None).cast("string"),
                )
                .withColumn(
                    "batch_id",
                    F.lit(int(resolved_batch_id)).cast("long"),
                )
                .withColumn(
                    "source_system_name",
                    F.lit(source_system_name),
                )
                .withColumn(
                    "table_name",
                    F.lit(source_table),
                )
                .withColumn(
                    "record_key",
                    record_key,
                )
                .withColumn(
                    "rule_name",
                    F.col("DQ_RULE").cast("string"),
                )
                .withColumn(
                    "error_description",
                    F.col("DQ_DESCRIPTION").cast("string"),
                )
                .withColumn(
                    "rejected_at",
                    F.current_timestamp(),
                )
                .select(
                    "run_id",
                    "batch_id",
                    "source_system_name",
                    "table_name",
                    "record_key",
                    "rule_name",
                    "error_description",
                    "rejected_at",
                )
            )

            reject_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(dqm_reject_tbl)

        if source_system_name is None:
            raise DQProcessingError(
                "source_system_name is required to update "
                "batch log."
            )

        safe_source_system = source_system_name.replace(
            "'",
            "''",
        )

        spark.sql(
            f"""
            UPDATE {batch_log_tbl}
            SET dq_status = 'Y'
            WHERE batch_id = {int(resolved_batch_id)}
              AND source_system_name = '{safe_source_system}'
            """
        )

        print(
            f"{MODULE_NAME}: DQ pipeline completed successfully. "
            f"source_identifier={source_identifier}, "
            f"batch_id={resolved_batch_id}, "
            f"passed={passed_count}, "
            f"rejected={rejected_count}"
        )

        return passed_df, rejected_df

    except DQProcessingError:
        raise

    except Exception as exc:
        raise DQProcessingError(
            f"Complete DQ pipeline failed for "
            f"source_identifier={source_identifier}, "
            f"source_table={source_table}: {exc}"
        ) from exc



# ---------------------------------------------------------------------
# Configuration inspection helper
# ---------------------------------------------------------------------

def get_configured_rule_count(
    source_identifier: str,
    source_system_name: Optional[str] = None,
) -> int:
    """
    Return how many active DQ rules are configured for a source in the
    control table (0 if the control table is not used in this environment
    or has no active rules for this source).
    """

    return len(
        get_configured_rules_for_source(
            source_identifier=source_identifier,
            source_system_name=source_system_name,
        )
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

if __name__ == "__main__":

    print(
        f"{MODULE_NAME}: "
        f"{get_rule_count()} configured "
        f"Excel DQ rules"
    )

    for rule in DQ_RULES:

        print(
            f"{rule.source_table} | "
            f"{rule.rule_name} | "
            f"{rule.dq_application_column} | "
            f"{rule.target_table}"
        )