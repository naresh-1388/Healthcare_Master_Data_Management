"""
Landing -> Staging transformation for Healthcare_MDM.

Purpose:
    Transform configured Landing records into the Staging layer.

The implemented HCP name transformation uses the documented source columns and mapping expressions.
"""

from __future__ import annotations

import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from ..core.logging_utils import logger, log_event_detail
    from ..core.runtime_config import (
        batch_log_tbl,
        catalog,
        cluster_id,
        get_batch_status_filter,
        get_notebook_run_url,
        job_id,
        run_id,
        update_batch_log_tbl,
    )
except ImportError:
    from core.logging_utils import logger, log_event_detail
    from core.runtime_config import (
        batch_log_tbl,
        catalog,
        cluster_id,
        get_batch_status_filter,
        get_notebook_run_url,
        job_id,
        run_id,
        update_batch_log_tbl,
    )

spark = SparkSession.builder.getOrCreate()

MODULE_NAME = "Landing_to_Staging"


class GracefulExit(Exception):
    """Expected pipeline termination."""


class StagingProcessingError(Exception):
    """Landing -> Staging processing failure."""


def _required_columns(df: DataFrame, columns: list[str]) -> None:
    """
    Assert that every column in `columns` is present on `df`.

    Raises:
        StagingProcessingError: listing every missing column, so a
        Landing-schema drift is caught immediately instead of failing
        later with a confusing "column not found" Spark error.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise StagingProcessingError(
            f"Landing table is missing required columns: {missing}"
        )


def transform_hcp_name(landing_df: DataFrame) -> DataFrame:
    """
    Transform the current HCP Landing dataset into the existing TEST_HCP
    staging contract.

    The output column names/types match the configured HCP name staging table.
    """
    required = [
        "countryCode",
        "firstName",
        "genderCode",
        "individualEid",
        "lastName",
        "middleName",
        "LOAD_DATE",
        "BATCH_ID",
        "Source_Name",
    ]
    _required_columns(landing_df, required)

    # Full_Name follows the supplied Raw_to_Land mapping pattern:
    # trim(regexp_replace(concat_ws(...), ...)).
    full_name = F.trim(
        F.regexp_replace(
            F.concat_ws(
                " ",
                F.coalesce(F.col("firstName"), F.lit("")),
                F.coalesce(F.col("middleName"), F.lit("")),
                F.coalesce(F.col("lastName"), F.lit("")),
            ),
            r"\s+",
            " ",
        )
    )

    return landing_df.select(
        F.upper(
            F.md5(F.coalesce(F.col("individualEid"), F.lit("")))
        ).alias("Name_PK"),
        F.col("firstName").cast("string").alias("firstName"),
        F.col("middleName").cast("string").alias("middleName"),
        F.col("lastName").cast("string").alias("lastName"),
        full_name.alias("Full_Name"),
        F.col("genderCode").cast("string").alias("genderCode"),
        F.col("countryCode").cast("string").alias("Country"),
        F.col("individualEid").cast("string").alias("individualEid"),
        F.col("Source_Name").cast("string").alias("Source_Name"),
        F.col("LOAD_DATE").cast("timestamp").alias("LOAD_DATE"),
        F.col("BATCH_ID").cast("long").alias("BATCH_ID"),
    )


def get_pending_batches(source_system_name: str) -> list[int]:
    """
    Look up every batch_id that has completed the upstream stage (DQ) but
    not yet the staging stage itself, using the shared batch-status filter
    from runtime_config, so this module only ever processes new work.

    Args:
        source_system_name: The source system to filter the batch control
            table on.

    Returns:
        list[int]: pending batch IDs, in ascending order (oldest first).
    """
    condition = get_batch_status_filter("staging", source_system_name)

    rows = (
        spark.sql(
            f"""
            SELECT DISTINCT batch_id
            FROM {batch_log_tbl}
            WHERE {condition}
            ORDER BY batch_id
            """
        )
        .collect()
    )

    return [int(r["batch_id"]) for r in rows if r["batch_id"] is not None]


def process_hcp_name(
    source_identifier: str,
    source_system_name: str,
    landing_table: str,
    staging_table: str,
    batch_id: Optional[int] = None,
) -> int:
    """
    Process Landing HCP name records into Staging.

    batch_id can be supplied explicitly for a controlled test run.
    """
    start_time = datetime.now()
    run_url = get_notebook_run_url()

    if batch_id is None:
        pending = get_pending_batches(source_system_name)
        if not pending:
            raise GracefulExit("No batches ready for Landing -> Staging.")
        batch_ids = pending
    else:
        batch_ids = [int(batch_id)]

    batch_sql = ", ".join(str(x) for x in batch_ids)

    landing_df = spark.table(landing_table).filter(
        F.col("BATCH_ID").isin(batch_ids)
    )

    if landing_df.limit(1).count() == 0:
        raise GracefulExit(
            f"No Landing records found for batch_id IN ({batch_sql})."
        )

    staging_df = transform_hcp_name(landing_df)

    count = staging_df.count()
    if count == 0:
        raise GracefulExit("No transformed records available for Staging.")

    (
        staging_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(staging_table)
    )

    log_event_detail(
        "Landing -> Staging - HCP Name",
        "Passed",
        f"Successfully processed {count} records.",
        run_url,
        source_identifier,
        source_system_name,
        job_id,
        MODULE_NAME,
        start_time,
        cluster_id,
        run_id,
    )

    logger.info(
        f"Landing -> Staging completed: {count} records -> {staging_table}"
    )
    return count


def main(
    source_identifier: Optional[str] = None,
    source_system_name: Optional[str] = None,
    landing_table: Optional[str] = None,
    staging_table: Optional[str] = None,
    batch_id: Optional[int] = None,
) -> None:
    """
    Programmatic entry point.

    Example:
        process_hcp_name(
            "<source_identifier>",
            "<source_system_name>",
            "HMDM_DEV.landing.hcp",
            "HMDM_DEV.staging.hcp_name",
            <batch_id>,
        )
    """
    if source_identifier is None:
        args = sys.argv[1:]
        if len(args) < 4:
            raise ValueError(
                "Expected: <source_identifier> <source_system_name> "
                "<landing_table> <staging_table> [batch_id]"
            )
        source_identifier, source_system_name, landing_table, staging_table = args[:4]
        batch_id = int(args[4]) if len(args) > 4 else None

    process_hcp_name(
        source_identifier=source_identifier,
        source_system_name=source_system_name,
        landing_table=landing_table,
        staging_table=staging_table,
        batch_id=batch_id,
    )


if __name__ == "__main__":
    main()
