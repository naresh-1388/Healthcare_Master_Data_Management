"""
Healthcare MDM Data Quality

Source of truth:
    Data Info.xlsx -> Land_to_Stag

This module implements only the DQ rule types explicitly present
in the mapping workbook:

    null_check
    name_address_completeness_check
    address_mdr_check
    mdr_check
    affiliation_mdr_check
    hierarchy_mdr_check

No additional DQ rules are invented here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()


MODULE_NAME = "DATA_QUALITY"


class DQProcessingError(Exception):
    """Raised when DQ processing itself fails."""


@dataclass(frozen=True)
class DQRule:
    source_table: str
    rule_name: str
    rule_description: str
    dq_application_column: str
    target_table: str


# ---------------------------------------------------------------------
# Exact Land_to_Stag rules from Excel
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

    if catalog_name and not table_name.startswith(
        f"{catalog_name}."
    ):
        return f"{catalog_name}.{table_name}"

    return table_name


def resolve_column(
    df: DataFrame,
    column_name: str,
):
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
            f"in source dataframe."
        )

    return F.col(actual)


def non_blank(column):
    return (
        column.isNotNull()
        & (F.trim(column.cast("string")) != "")
    )


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

    passed = df.filter(
        non_blank(column)
    )

    rejected = df.filter(
        ~non_blank(column)
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
    ]

    name_key = None

    for candidate in name_key_candidates:
        if candidate.lower() in {
            column.lower()
            for column in name_df.columns
        }:
            name_key = resolve_column(
                name_df,
                candidate,
            )
            break

    if name_key is None:
        raise DQProcessingError(
            "Unable to identify the name-table "
            "key column required by address_mdr_check."
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

    passed = (
        evaluated
        .filter(
            non_blank(
                F.col("_dq_address_key")
            )
            & F.col("_dq_name_key").isNotNull()
        )
        .drop(
            "_dq_address_key",
            "_dq_name_key",
        )
    )

    rejected = (
        evaluated
        .filter(
            ~(
                non_blank(
                    F.col("_dq_address_key")
                )
                & F.col("_dq_name_key").isNotNull()
            )
        )
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
            "for mdr_check."
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

    if rule.rule_name == "null_check":

        return apply_null_check(
            source_df,
            rule.dq_application_column,
        )

    if rule.rule_name == "name_address_completeness_check":

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

    if rule.rule_name == "address_mdr_check":

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

    if rule.rule_name == "mdr_check":

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

    if rule.rule_name == "affiliation_mdr_check":

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

    if rule.rule_name == "hierarchy_mdr_check":

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
# DQ result
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
            F.lit(rule.dq_application_column),
        )
        .withColumn(
            "DQ_PROCESSED_AT",
            F.current_timestamp(),
        )
    )


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
# Public API
# ---------------------------------------------------------------------

def get_rules() -> List[DQRule]:
    return list(DQ_RULES)


def get_rules_for_source(
    source_table: str,
) -> List[DQRule]:

    return [
        rule
        for rule in DQ_RULES
        if rule.source_table.lower()
        == source_table.lower()
    ]


def get_rule_count() -> int:
    return len(DQ_RULES)


if __name__ == "__main__":

    print(
        f"{MODULE_NAME}: "
        f"{get_rule_count()} configured DQ rules"
    )

    for rule in DQ_RULES:
        print(
            f"{rule.source_table} | "
            f"{rule.rule_name} | "
            f"{rule.dq_application_column} | "
            f"{rule.target_table}"
        )

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# 1) DQ rule definitions must come from the supplied project mapping/configuration.
# 2) Source, target, rejection and DQ-log schemas/tables must be configured in
#    the centralized control configuration.
# 3) Do not hardcode credentials here. Databricks/Snowflake access belongs in
#    the runtime connection/secret configuration.
# ============================================================================

