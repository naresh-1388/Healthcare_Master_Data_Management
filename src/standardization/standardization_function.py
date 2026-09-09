"""
Healthcare MDM - Standardization Function Library.

Purpose
-------
This module contains reusable Spark DataFrame standardization functions
used by:

    src.standardization.standardization

The standardization framework is configuration-driven. The configured
function name from ctl_std_entity_mstr is resolved through function_mapping.

The project documentation describes Landing-layer standardization as
structural standardization, including:

    - Enterprise column naming
    - Datatype conversion
    - Hash/business key generation where required
    - Gender/title/type code standardization
    - Duplicate removal
    - Whitespace trimming
    - Null handling

Business-specific validation belongs to the Staging / DQ layer and is
therefore not implemented here.

Each function follows the contract expected by standardization.py:

    function(dataframe, column_name) -> dataframe
"""

from __future__ import annotations

from typing import Callable, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Generic text standardization
# ---------------------------------------------------------------------------

def trim(df: DataFrame, column_name: str) -> DataFrame:
    """
    Remove leading and trailing whitespace from a column.
    """
    return df.withColumn(
        column_name,
        F.trim(F.col(column_name))
    )


def upper(df: DataFrame, column_name: str) -> DataFrame:
    """
    Trim whitespace and convert text to uppercase.
    """
    return df.withColumn(
        column_name,
        F.upper(F.trim(F.col(column_name)))
    )


def lower(df: DataFrame, column_name: str) -> DataFrame:
    """
    Trim whitespace and convert text to lowercase.
    """
    return df.withColumn(
        column_name,
        F.lower(F.trim(F.col(column_name)))
    )


def proper(df: DataFrame, column_name: str) -> DataFrame:
    """
    Trim whitespace and convert text to title/proper case.
    """
    return df.withColumn(
        column_name,
        F.initcap(F.trim(F.col(column_name)))
    )


# ---------------------------------------------------------------------------
# Null / blank handling
# ---------------------------------------------------------------------------

def null_if_blank(df: DataFrame, column_name: str) -> DataFrame:
    """
    Convert blank or whitespace-only strings to NULL.

    Existing non-blank values are retained.
    """
    value = F.col(column_name)

    return df.withColumn(
        column_name,
        F.when(
            value.isNull(),
            F.lit(None)
        )
        .when(
            F.trim(value.cast("string")) == "",
            F.lit(None)
        )
        .otherwise(value)
    )


# ---------------------------------------------------------------------------
# Character standardization
# ---------------------------------------------------------------------------

def remove_special_chars(df: DataFrame, column_name: str) -> DataFrame:
    """
    Remove non-alphanumeric characters while retaining spaces.

    This is a technical cleansing function only.
    It does not apply a business-specific healthcare rule.
    """
    return df.withColumn(
        column_name,
        F.regexp_replace(
            F.col(column_name).cast("string"),
            r"[^A-Za-z0-9 ]",
            ""
        )
    )


# ---------------------------------------------------------------------------
# Phone standardization
# ---------------------------------------------------------------------------

def normalize_phone(df: DataFrame, column_name: str) -> DataFrame:
    """
    Retain numeric characters from a phone-number column.

    Country-specific phone-number formatting is intentionally not applied
    because the supplied project files do not define a country-specific
    phone normalization rule.
    """
    return df.withColumn(
        column_name,
        F.regexp_replace(
            F.col(column_name).cast("string"),
            r"[^0-9]",
            ""
        )
    )


# ---------------------------------------------------------------------------
# Postal-code standardization
# ---------------------------------------------------------------------------

def normalize_postal_code(df: DataFrame, column_name: str) -> DataFrame:
    """
    Trim whitespace and convert postal-code values to uppercase.
    """
    return df.withColumn(
        column_name,
        F.upper(F.trim(F.col(column_name).cast("string")))
    )


# ---------------------------------------------------------------------------
# Code standardization
# ---------------------------------------------------------------------------

def normalize_code(df: DataFrame, column_name: str) -> DataFrame:
    """
    Generic code normalization.

    Codes are trimmed and converted to uppercase.
    """
    return df.withColumn(
        column_name,
        F.upper(F.trim(F.col(column_name).cast("string")))
    )


# ---------------------------------------------------------------------------
# Function registry
# ---------------------------------------------------------------------------
#
# standardization.py resolves configured rule_function values through
# this dictionary.
#
# Example:
#
#     rule_function = "trim"
#
#     function_mapping["trim"](df, "firstName")
#
# ---------------------------------------------------------------------------

function_mapping: Dict[str, Callable[[DataFrame, str], DataFrame]] = {
    "trim": trim,
    "upper": upper,
    "lower": lower,
    "proper": proper,
    "null_if_blank": null_if_blank,
    "remove_special_chars": remove_special_chars,
    "normalize_phone": normalize_phone,
    "normalize_postal_code": normalize_postal_code,
    "normalize_code": normalize_code,
}


# ---------------------------------------------------------------------------
# Public module exports
# ---------------------------------------------------------------------------

__all__ = [
    "trim",
    "upper",
    "lower",
    "proper",
    "null_if_blank",
    "remove_special_chars",
    "normalize_phone",
    "normalize_postal_code",
    "normalize_code",
    "function_mapping",
]