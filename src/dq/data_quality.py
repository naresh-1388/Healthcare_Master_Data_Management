"""
Healthcare MDM Data Quality

Source of truth:
    Data Info.xlsx -> Land_to_Stag

Supported DQ rule types:
    null_check
    name_address_completeness_check
    address_mdr_check
    mdr_check
    affiliation_mdr_check
    hierarchy_mdr_check

The 24 default DQ rules below are taken from the supplied
Land_to_Stag mapping.

Source-specific DQ configuration can additionally be loaded
from the centralized DQ control table defined in common_variables.py.

No credentials are hardcoded here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

MODULE_NAME = "DATA_QUALITY"


# ---------------------------------------------------------------------
# Existing project control-table references
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
        from ..core.runtime_config import (
            dqm_config_tbl,
            dqm_log_tbl,
            dqm_reject_tbl,
            batch_log_tbl,
            log_tbl_nm,
        )

    except Exception:

        # Local/test fallback only.
        # Production Databricks execution should resolve these
        # from common_variables.py.
        dqm_config_tbl = (
            "healthcare_mdm_dev.util.ctl_dq_entity_mstr"
        )

        dqm_log_tbl = (
            "healthcare_mdm_dev.util.ctl_dqm_log_tbl"
        )

        dqm_reject_tbl = (
            "healthcare_mdm_dev.util.dqm_reject_tbl"
        )

        batch_log_tbl = (
            "healthcare_mdm_dev.util.ctl_batch_log_tbl"
        )

        log_tbl_nm = (
            "healthcare_mdm_dev.util.ctl_log_tbl"
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
# Exact Land_to_Stag rules from Data Info.xlsx
# ---------------------------------------------------------------------

DQ_RULES: List[DQRule] = [

    DQRule(
        "Par_lake_lnd_hcp_name",
        "null_check",
        "Reject records if the value of the column is Null",
        "hco_name",
        "Par_lake_stg_hco_name",
    ),

    DQRule(
        "Par_lake_lnd_hcp_name",
        "null_check",
        "Reject records if the value of the column is Null",
        "first_name",
        "Par_lake_stg_hco_name",
    ),

    DQRule(
        "Par_lake_lnd_hcp_name",
        "name_address_completeness_check",
        "Reject records in name table if no complete address is present",
        "source_id",
        "Par_lake_stg_hcp_name",
    ),

    DQRule(
        "Par_lake_lnd_hco_name",
        "name_address_completeness_check",
        "Reject records in name table if no complete address is present",
        "source_id",
        "Par_lake_stg_hco_name",
    ),

    DQRule(
        "Par_lake_lnd_hcp_address",
        "address_mdr_check",
        "Reject records if no complete address is present or source_fk is not present in name table",
        "source_fk",
        "Par_lake_stg_hcp_address",
    ),

    DQRule(
        "Par_lake_lnd_hco_address",
        "address_mdr_check",
        "Reject records if no complete address is present or source_fk is not present in name table",
        "source_fk",
        "Par_lake_stg_hco_address",
    ),

    DQRule(
        "Par_lake_lnd_hcp_email",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in email table",
        "source_fk",
        "Par_lake_stg_hcp_email",
    ),

    DQRule(
        "Par_lake_lnd_hcp_alternatename",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in alternate name table",
        "source_fk",
        "Par_lake_stg_hcp_alternatename",
    ),

    DQRule(
        "Par_lake_lnd_hcp_identification",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in identification table",
        "source_fk",
        "Par_lake_stg_hcp_identification",
    ),

    DQRule(
        "Par_lake_lnd_hcp_specialty",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in specialty table",
        "source_fk",
        "Par_lake_stg_hcp_specialty",
    ),

    DQRule(
        "Par_lake_lnd_hcp_phone",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in phone table",
        "source_fk",
        "Par_lake_stg_hcp_phone",
    ),

    DQRule(
        "Par_lake_lnd_hcp_education",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in education table",
        "source_fk",
        "Par_lake_stg_hcp_education",
    ),

    DQRule(
        "Par_lake_lnd_hcp_origin_university",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in origin university table",
        "source_fk",
        "Par_lake_stg_hcp_origin_university",
    ),

    DQRule(
        "Par_lake_lnd_hcp_tax",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in table",
        "source_fk",
        "Par_lake_stg_hcp_tax",
    ),

    DQRule(
        "Par_lake_lnd_hco_tax",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in table",
        "source_fk",
        "Par_lake_stg_hco_tax",
    ),

    DQRule(
        "Par_lake_lnd_hco_email",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in email table",
        "source_fk",
        "Par_lake_stg_hco_email",
    ),

    DQRule(
        "Par_lake_lnd_hco_alternatename",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in alternate name table",
        "source_fk",
        "Par_lake_stg_hco_alternatename",
    ),

    DQRule(
        "Par_lake_lnd_hco_identification",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in identification table",
        "source_fk",
        "Par_lake_stg_hco_identification",
    ),

    DQRule(
        "Par_lake_lnd_hco_specialty",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in specialty table",
        "source_fk",
        "Par_lake_stg_hco_specialty",
    ),

    DQRule(
        "Par_lake_lnd_hco_phone",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in phone table",
        "source_fk",
        "Par_lake_stg_hco_phone",
    ),

    DQRule(
        "Par_lake_lnd_hcp_hco_affiliation",
        "affiliation_mdr_check",
        "Reject records in name table if source id is not present",
        "source_id",
        "Par_lake_stg_hcp_hco_affiliation",
    ),

    DQRule(
        "Par_lake_lnd_hco_hco_hierarchy",
        "hierarchy_mdr_check",
        "Reject records if source id is not present in name table",
        "source_id",
        "Par_lake_stg_hco_hierarchy",
    ),

    DQRule(
        "Par_lake_lnd_hcp_tendencies",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in phone table",
        "source_fk",
        "Par_lake_stg_hcp_tendencies",
    ),

    DQRule(
        "Par_lake_lnd_hcp_language",
        "mdr_check",
        "Reject records if the value of the source_fk is not present in phone table",
        "source_fk",
        "Par_lake_stg_hcp_language",
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
    """
    Resolve dataframe column case-insensitively.
    """

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

    actual = lookup.get(
        column_name.lower()
    )

    if actual is None:
        raise DQProcessingError(
            f"Column '{column_name}' not found "
            f"in source dataframe. "
            f"Available columns: {df.columns}"
        )

    return F.col(actual)


def non_blank(column):
    """
    Treat NULL and blank/whitespace strings as invalid.
    """

    return (
        column.isNotNull()
        & (
            F.trim(
                column.cast("string")
            ) != ""
        )
    )


def table_exists(table_name: str) -> bool:
    """
    Safe table-existence check.
    """

    try:
        return spark.catalog.tableExists(
            table_name
        )
    except Exception:
        return False


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

    valid_condition = non_blank(
        column
    )

    passed = df.filter(
        valid_condition
    )

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
        .filter(
            non_blank(address_key)
        )
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
            "Unable to identify the name-table "
            "key column required by address_mdr_check. "
            f"Available columns: {name_df.columns}"
        )

    name_keys = (
        name_df
        .filter(
            non_blank(name_key)
        )
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
            "Unable to identify parent key "
            "for mdr_check. "
            f"Available columns: {parent_df.columns}"
        )

    parent_keys = (
        parent_df
        .filter(
            non_blank(parent_key)
        )
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

    rule_type = (
        rule.rule_name
        .strip()
        .lower()
    )

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
        rule,
        source_df,
        reference_df,
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

    if not table_exists(
        DQ_CONFIG_TABLE
    ):
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
            f"required columns: {sorted(missing_columns)}"
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

        rejected_df = (
            source_df.limit(0)
        )

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
) -> Tuple[DataFrame, DataFrame]:

    try:

        if not source_table:
            raise DQProcessingError(
                "source_table cannot be empty."
            )

        if not table_exists(
            source_table
        ):

            raise DQProcessingError(
                f"Source table does not exist: "
                f"{source_table}"
            )

        source_df = spark.table(
            source_table
        )

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

        print(
            f"{MODULE_NAME}: "
            f"source_identifier={source_identifier}, "
            f"source_system_name={source_system_name}, "
            f"source_table={source_table}, "
            f"rules={len(rules)}"
        )

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