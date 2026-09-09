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
        "hcp_name",
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
        "Reject records if the value of the source_fk is not present in phone table",
        "source_fk",
        "hcp_tendencies",
    ),
    DQRule(
        "hcp_language",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in phone table",
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
    return list(DQ_RULES)


def get_rules_for_source(
    source_table: str,
    source_system_name: Optional[str] = None,
    source_identifier: Optional[str] = None,
) -> List[DQRule]:

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
    return len(DQ_RULES)


# ---------------------------------------------------------------------
# Execute rules
# ---------------------------------------------------------------------

def execute_rules(
    source_df: DataFrame,
    rules: List[DQRule],
    reference_df: Optional[DataFrame] = None,
) -> Tuple[DataFrame, DataFrame]:

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