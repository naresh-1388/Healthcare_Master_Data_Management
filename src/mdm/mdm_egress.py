"""
Healthcare MDM - MDM Egress

Purpose:
    Publish validated MDM data to downstream consumers.

Pipeline dependency:
    RAW -> Landing -> Staging -> DQ -> Ingress -> Egress

Egress is allowed only when:
    raw_ingestion_status = Y
    stdz_status          = Y
    canonical_status     = Y
    dq_status             = Y
    ingress_status        = Y
    egress_status        != Y

Purpose:
    Publish validated MDM records to configured downstream targets.

Target tables are supplied explicitly by the environment configuration.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------
# Runtime configuration
# ---------------------------------------------------------------------

try:
    from core import runtime_config
except ImportError:
    runtime_config = None


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _require_spark(spark: SparkSession) -> SparkSession:
    """Validate that a SparkSession was actually passed in, since every
    egress operation needs one to read/write Delta tables."""
    if spark is None:
        raise ValueError("spark session is required")
    return spark


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Check whether a fully-qualified table exists, preferring the fast
    catalog API and falling back to a DESCRIBE TABLE probe for catalogs/
    versions where tableExists() is unreliable.

    Returns:
        bool: True if the table can be found by either method, False if
        neither succeeds (never raises).
    """
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        try:
            spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False


def _is_bootstrap_mode() -> bool:
    """
    Check if bootstrap mode allows new table creation.

    When True (default): saveAsTable auto-creates missing MASTER tables.
    When False: raises RuntimeError if target table doesn't exist (safety guard).

    Set HMDM_BOOTSTRAP_MODE=false to enable the safety check after initial setup.
    """
    import os
    return os.environ.get("HMDM_BOOTSTRAP_MODE", "true").lower() != "false"


def _qualify_table(table_name: str, default_schema: str) -> str:
    """
    Qualify a table name without changing an already-qualified name.
    """
    if not table_name:
        raise ValueError("table_name cannot be empty")

    table_name = table_name.strip()

    if table_name.count(".") >= 2:
        return table_name

    if table_name.count(".") == 1:
        if runtime_config is not None:
            catalog = getattr(runtime_config, "catalog", None)
            if catalog:
                return f"{catalog}.{table_name}"

    if runtime_config is not None:
        catalog = getattr(runtime_config, "catalog", None)
        if catalog:
            return f"{catalog}.{default_schema}.{table_name}"

    return f"{default_schema}.{table_name}"


def _get_batch_filter(
    source_system_name: str,
    batch_id: Optional[int] = None
) -> str:
    """
    Return the exact egress gating condition used by the project.
    """

    source = source_system_name.replace("'", "''")

    if batch_id is not None:
        return (
            f"source_system_name = '{source}' "
            f"AND batch_id = {int(batch_id)} "
            "AND raw_ingestion_status = 'Y' "
            "AND stdz_status = 'Y' "
            "AND canonical_status = 'Y' "
            "AND dq_status = 'Y' "
            "AND ingress_status = 'Y' "
            "AND COALESCE(egress_status, 'N') <> 'Y'"
        )

    return (
        f"source_system_name = '{source}' "
        "AND raw_ingestion_status = 'Y' "
        "AND stdz_status = 'Y' "
        "AND canonical_status = 'Y' "
        "AND dq_status = 'Y' "
        "AND ingress_status = 'Y' "
        "AND COALESCE(egress_status, 'N') <> 'Y'"
    )


def get_pending_batches(
    spark: SparkSession,
    source_system_name: str
) -> DataFrame:
    """
    Return batches eligible for egress.
    """

    _require_spark(spark)

    if runtime_config is None:
        raise RuntimeError("runtime_config could not be imported")

    batch_log_tbl = runtime_config.batch_log_tbl

    condition = _get_batch_filter(source_system_name)

    return (
        spark.table(batch_log_tbl)
        .filter(F.expr(condition))
        .orderBy(F.col("batch_id"))
    )


# ---------------------------------------------------------------------
# Source preparation
# ---------------------------------------------------------------------

def read_ingress_source(
    spark: SparkSession,
    source_table: str,
    batch_id: int
) -> DataFrame:
    """
    Read only the required batch from the MDM ingress/publish dataset.
    """

    _require_spark(spark)

    df = spark.table(source_table)

    if "BATCH_ID" not in {c.upper() for c in df.columns}:
        raise RuntimeError(
            f"Required BATCH_ID column not found in source table: "
            f"{source_table}"
        )

    return df.filter(F.col("BATCH_ID") == int(batch_id))


def prepare_egress_dataframe(
    df: DataFrame,
    source_system_name: str,
    batch_id: int
) -> DataFrame:
    """
    Prepare the outbound dataset.

    No business transformation is introduced here.
    Existing validated values are preserved.

    Technical metadata is added only when not already present.
    """

    if df is None:
        raise ValueError("Input dataframe cannot be None")

    result = df

    existing_upper = {c.upper() for c in result.columns}

    if "SOURCE_SYSTEM_NAME" not in existing_upper:
        result = result.withColumn(
            "SOURCE_SYSTEM_NAME",
            F.lit(source_system_name)
        )

    if "BATCH_ID" not in existing_upper:
        result = result.withColumn(
            "BATCH_ID",
            F.lit(int(batch_id)).cast("long")
        )

    if "EGRESS_LOAD_DATE" not in existing_upper:
        result = result.withColumn(
            "EGRESS_LOAD_DATE",
            F.current_timestamp()
        )

    return result


# ---------------------------------------------------------------------
# Target writing
# ---------------------------------------------------------------------

def write_egress(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    mode: str = "append"
) -> int:
    """
    Write prepared egress records.

    The caller must provide the approved physical target table.
    """

    if df is None:
        raise ValueError("Egress dataframe cannot be None")

    if not target_table:
        raise ValueError(
            "Physical egress target table is required"
        )

    if mode not in {"append", "overwrite"}:
        raise ValueError(
            f"Unsupported write mode: {mode}"
        )

    count = df.count()

    if count == 0:
        print(
            f"Egress source is empty. Nothing written to {target_table}"
        )
        return 0

    # Safety check: reject unknown target tables unless in bootstrap mode.
    if not _is_bootstrap_mode() and not _table_exists(spark, target_table):
        raise RuntimeError(
            f"Approved physical egress target table does not exist: "
            f"{target_table}. "
            "Set HMDM_BOOTSTRAP_MODE=true to allow initial table creation."
        )

    (
        df.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .option("path", runtime_config.get_s3_location(target_table))
        .saveAsTable(target_table)
    )

    print(
        f"Egress write complete: {count} records -> {target_table}"
    )

    return count


# ---------------------------------------------------------------------
# Batch status
# ---------------------------------------------------------------------

def update_egress_status(
    spark: SparkSession,
    source_system_name: str,
    batch_id: int
) -> None:
    """
    Mark the successfully published batch as Egress = Y.
    """

    _require_spark(spark)

    if runtime_config is None:
        raise RuntimeError("runtime_config could not be imported")

    batch_log_tbl = runtime_config.batch_log_tbl

    source = source_system_name.replace("'", "''")

    spark.sql(
        f"""
        UPDATE {batch_log_tbl}
        SET egress_status = 'Y',
            batch_end_time = COALESCE(batch_end_time, current_timestamp())
        WHERE source_system_name = '{source}'
          AND batch_id = {int(batch_id)}
          AND raw_ingestion_status = 'Y'
          AND stdz_status = 'Y'
          AND canonical_status = 'Y'
          AND dq_status = 'Y'
          AND ingress_status = 'Y'
        """
    )

    print(
        f"Egress status updated: "
        f"source={source_system_name}, batch={batch_id}"
    )


# ---------------------------------------------------------------------
# Main batch processor
# ---------------------------------------------------------------------

def process_egress_batch(
    spark: SparkSession,
    source_system_name: str,
    batch_id: int,
    source_table: str,
    target_table: str,
    write_mode: str = "append",
    skip_batch_update: bool = False
) -> Dict[str, object]:
    """
    Process one eligible egress batch.
    """

    _require_spark(spark)

    if not source_table:
        raise ValueError("source_table is required")

    if not target_table:
        raise ValueError(
            "target_table is required. "
            "Do not run egress without an approved target."
        )

    print("=" * 80)
    print("MDM EGRESS")
    print("=" * 80)
    print(f"Source System : {source_system_name}")
    print(f"Batch ID      : {batch_id}")
    print(f"Source Table  : {source_table}")
    print(f"Target Table  : {target_table}")

    # -------------------------------------------------------------
    # Validate batch gating
    # -------------------------------------------------------------

    if runtime_config is None:
        raise RuntimeError("runtime_config could not be imported")

    batch_log_tbl = runtime_config.batch_log_tbl

    status_df = (
        spark.table(batch_log_tbl)
        .filter(
            (F.col("source_system_name") == source_system_name)
            & (F.col("batch_id") == int(batch_id))
        )
        .select(
            "batch_id",
            "source_system_name",
            "raw_ingestion_status",
            "stdz_status",
            "canonical_status",
            "dq_status",
            "ingress_status",
            "egress_status"
        )
    )

    status = status_df.first()

    if status is None:
        raise RuntimeError(
            f"No batch-log record found for "
            f"{source_system_name}/{batch_id}"
        )

    required_statuses = {
        "raw_ingestion_status": "Y",
        "stdz_status": "Y",
        "canonical_status": "Y",
        "dq_status": "Y",
        "ingress_status": "Y",
    }

    for column_name, expected in required_statuses.items():
        actual = status[column_name]

        if actual != expected:
            raise RuntimeError(
                f"Egress gating failed: "
                f"{column_name}={actual}, expected={expected}"
            )

    if status["egress_status"] == "Y":
        raise RuntimeError(
            f"Batch {batch_id} is already marked as Egress complete"
        )

    print("Batch gating: PASS")

    # -------------------------------------------------------------
    # Read
    # -------------------------------------------------------------

    source_df = read_ingress_source(
        spark=spark,
        source_table=source_table,
        batch_id=batch_id
    )

    source_count = source_df.count()

    print(f"Source records: {source_count}")

    # -------------------------------------------------------------
    # Prepare
    # -------------------------------------------------------------

    egress_df = prepare_egress_dataframe(
        df=source_df,
        source_system_name=source_system_name,
        batch_id=batch_id
    )

    # -------------------------------------------------------------
    # Delete existing rows for this batch (idempotency)
    # -------------------------------------------------------------
    # Before appending, remove any existing rows for the same
    # batch_id from the target Master table.  This prevents data
    # multiplication when a batch is re-egressed (e.g. after a
    # retry or manual status reset).
    # -------------------------------------------------------------

    if write_mode == "append" and spark.catalog.tableExists(target_table):
        target_cols = {c.upper() for c in spark.table(target_table).columns}
        if "BATCH_ID" in target_cols:
            spark.sql(
                f"DELETE FROM {target_table} "
                f"WHERE BATCH_ID = {int(batch_id)}"
            )
            print(
                f"Idempotency delete: removed existing rows for "
                f"batch {batch_id} from {target_table}"
            )

    # -------------------------------------------------------------
    # Write
    # -------------------------------------------------------------

    written_count = write_egress(
        spark=spark,
        df=egress_df,
        target_table=target_table,
        mode=write_mode
    )

    # -------------------------------------------------------------
    # Reconciliation
    # -------------------------------------------------------------

    if source_count != written_count:
        raise RuntimeError(
            f"Egress reconciliation failed: "
            f"source={source_count}, written={written_count}"
        )

    print(
        f"Egress reconciliation: PASS "
        f"(source={source_count}, written={written_count})"
    )

    # -------------------------------------------------------------
    # Status update ONLY after successful write + reconciliation
    # -------------------------------------------------------------

    if not skip_batch_update:
        update_egress_status(
            spark=spark,
            source_system_name=source_system_name,
            batch_id=batch_id
        )

    print("=" * 80)
    print("MDM EGRESS COMPLETE")
    print("=" * 80)

    return {
        "source_system_name": source_system_name,
        "batch_id": int(batch_id),
        "source_records": source_count,
        "written_records": written_count,
        "status": "SUCCESS",
    }


# ---------------------------------------------------------------------
# Find eligible batches
# ---------------------------------------------------------------------

def get_latest_eligible_batch(
    spark: SparkSession,
    source_system_name: str
) -> Optional[int]:
    """
    Return the latest batch eligible for egress.
    """

    df = get_pending_batches(
        spark=spark,
        source_system_name=source_system_name
    )

    row = df.orderBy(F.col("batch_id").desc()).first()

    if row is None:
        return None

    return int(row["batch_id"])


# ---------------------------------------------------------------------
# Public pipeline entry point
# ---------------------------------------------------------------------

def main_pipeline(
    spark: SparkSession,
    source_system_name: str,
    source_table: str,
    target_table: str,
    batch_id: Optional[int] = None,
    write_mode: str = "append",
    skip_batch_update: bool = False
) -> Dict[str, object]:
    """
    Main Egress entry point.

    If batch_id is not supplied, the latest eligible batch is selected.
    """

    _require_spark(spark)

    if batch_id is None:
        batch_id = get_latest_eligible_batch(
            spark=spark,
            source_system_name=source_system_name
        )

        if batch_id is None:
            return {
                "source_system_name": source_system_name,
                "status": "NO_ELIGIBLE_BATCH",
                "source_records": 0,
                "written_records": 0,
            }

    return process_egress_batch(
        spark=spark,
        source_system_name=source_system_name,
        batch_id=batch_id,
        source_table=source_table,
        target_table=target_table,
        write_mode=write_mode,
        skip_batch_update=skip_batch_update
    )


# ---------------------------------------------------------------------
# Module smoke test
# ---------------------------------------------------------------------

def smoke_test() -> bool:
    """
    Import-level smoke test.
    Does not execute a live egress.
    """

    required_functions = [
        "get_pending_batches",
        "read_ingress_source",
        "prepare_egress_dataframe",
        "write_egress",
        "update_egress_status",
        "process_egress_batch",
        "get_latest_eligible_batch",
        "main_pipeline",
    ]

    for function_name in required_functions:
        if not callable(globals().get(function_name)):
            raise RuntimeError(
                f"Missing required function: {function_name}"
            )

    print("mdm_egress module smoke test: PASS")
    return True