"""
Canonical transformation pipeline.

Flow:
    Batch Status
        -> Canonical Configuration
        -> RAW/Standardized Source Data
        -> Config-driven Column Mapping
        -> Consolidation
        -> Canonical Target Delta Table
"""

import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

try:
    from ..core.runtime_config import (
        batch_log_tbl,
        canonical_config_tbl,
        catalog,
        cluster_id,
        get_batch_status_filter,
        get_notebook_run_url,
        ingestion_config_tbl,
        job_id,
        run_id,
        update_batch_log_tbl,
    )

    from ..core.logging_utils import (
        log_event_detail,
        logger,
    )

except ImportError:
    from core.runtime_config import (
        batch_log_tbl,
        canonical_config_tbl,
        catalog,
        cluster_id,
        get_batch_status_filter,
        get_notebook_run_url,
        get_s3_location,
        ingestion_config_tbl,
        job_id,
        run_id,
        update_batch_log_tbl,
    )

    from core.logging_utils import (
        log_event_detail,
        logger,
    )


spark = SparkSession.builder.getOrCreate()

MODULE_NAME = "Canonical"


class GracefulExit(Exception):
    """Expected condition where canonical processing can stop safely."""


# ============================================================================
# PARAMETERS
# ============================================================================

def read_parameters():
    """
    Expected execution arguments:

        sys.argv[2] -> source_identifier
        sys.argv[3] -> source_system_name
        sys.argv[4] -> target_table_name
    """

    try:
        source_identifier = sys.argv[2]
        source_system_name = sys.argv[3]
        target_table_name = sys.argv[4]

    except IndexError as exc:
        raise ValueError(
            "Missing required arguments. Expected: "
            "script.py <runtime> <source_identifier> "
            "<source_system_name> <target_table_name>"
        ) from exc

    return (
        source_identifier,
        source_system_name,
        target_table_name,
    )


# ============================================================================
# BATCH CONDITION
# ============================================================================

def get_batch_condition(
    source_system_name,
    source_identifier,
    run_url,
    staging_start_time,
):
    """Return pending canonical batch condition."""

    where_condition = get_batch_status_filter(
        "canonical",
        source_system_name,
    )

    batch_rows = (
        spark.sql(
            f"""
            SELECT collect_list(batch_id) AS batch_ids
            FROM {batch_log_tbl}
            WHERE {where_condition}
            """
        )
        .head()
    )

    batch_ids = batch_rows["batch_ids"]

    logger.info(
        f"Canonical pending batch IDs: {batch_ids}"
    )

    if not batch_ids:

        log_event_detail(
            f"DOM - {source_identifier}",
            "Passed",
            "No records to be processed",
            run_url,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            staging_start_time,
            cluster_id,
            run_id,
        )

        raise GracefulExit(
            "No delta condition to process. Exiting gracefully"
        )

    return (
        "batch_id IN ("
        + ", ".join(
            str(batch_id)
            for batch_id in batch_ids
        )
        + ")"
    )


# ============================================================================
# CANONICAL CONFIGURATION
# ============================================================================

def get_canonical_config_details(
    src_table_name,
    target_table_name,
    canonical_config_df,
    batch_condition,
    source_identifier,
    source_system_name,
    run_url,
    staging_start_time,
):
    """Retrieve source and target schema details."""

    try:

        config_filter = (
            (
                F.lower(
                    F.trim(
                        F.col("tgt_tbl_nm")
                    )
                )
                == target_table_name.strip().lower()
            )
            &
            (
                F.lower(
                    F.trim(
                        F.col("src_tbl_nm")
                    )
                )
                == src_table_name.strip().lower()
            )
        )

        config_row = (
            canonical_config_df
            .filter(config_filter)
            .select(
                "src_schema",
                "src_tbl_nm",
                "tgt_schema",
                "tgt_tbl_nm",
            )
            .first()
        )

        if config_row is None:
            raise ValueError(
                f"No canonical configuration found for "
                f"source={src_table_name}, "
                f"target={target_table_name}"
            )

        src_schema = (
            f"{catalog}.{config_row['src_schema']}"
        )

        src_tbl_nm = config_row["src_tbl_nm"]

        tgt_schema = (
            f"{catalog}.{config_row['tgt_schema']}"
        )

        tgt_tbl_nm = config_row["tgt_tbl_nm"]

        logger.info(
            f"Canonical config resolved: "
            f"src_schema={src_schema}, "
            f"src_tbl_nm={src_tbl_nm}, "
            f"tgt_schema={tgt_schema}, "
            f"tgt_tbl_nm={tgt_tbl_nm}"
        )

        return (
            src_schema,
            src_tbl_nm,
            tgt_schema,
            tgt_tbl_nm,
        )

    except Exception as exc:

        error_message = (
            str(exc).split("stacktrace")[0]
        )

        log_event_detail(
            f"Fetching Value from Config File - "
            f"{target_table_name}",
            "Failed",
            error_message,
            run_url,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            staging_start_time,
            cluster_id,
            run_id,
        )

        update_batch_log_tbl(
            "canonical",
            "N",
            batch_condition,
            source_system_name,
        )

        raise RuntimeError(
            f"Unable to retrieve canonical configuration "
            f"for {target_table_name}"
        ) from exc


# ============================================================================
# SOURCE DATA
# ============================================================================

def get_delta_data_from_src(
    src_schema,
    src_tbl_nm,
    batch_condition,
    source_identifier,
    source_system_name,
    run_url,
    staging_start_time,
):
    """
    Read pending source data and deduplicate using configured PK.

    Deduplication ordering:
        1. last_update_date, when available
        2. batch_id, when last_update_date is unavailable
    """

    try:

        source_table = (
            f"{src_schema}.{src_tbl_nm}"
        )

        logger.info(
            f"Reading canonical source table: "
            f"{source_table}"
        )

        # ------------------------------------------------------------
        # Get ingestion configuration
        # ------------------------------------------------------------

        config_row = (
            spark.sql(
                f"""
                SELECT source_primary_key
                FROM {ingestion_config_tbl}
                WHERE (
                    LOWER(TRIM(std_table_name))
                        = LOWER(TRIM('{src_tbl_nm}'))
                    OR
                    LOWER(TRIM(raw_table_name))
                        = LOWER(TRIM('{src_tbl_nm}'))
                )
                AND LOWER(TRIM(source_system_name))
                    = LOWER(TRIM('{source_system_name}'))
                AND source_active_flag = true
                """
            )
            .first()
        )

        primary_key = (
            config_row["source_primary_key"]
            if config_row is not None
            else None
        )

        logger.info(
            f"Canonical source primary key: "
            f"{primary_key}"
        )

        # ------------------------------------------------------------
        # Read source schema
        # ------------------------------------------------------------

        source_df = spark.table(
            source_table
        )

        source_columns = {
            column.lower()
            for column in source_df.columns
        }

        # ------------------------------------------------------------
        # Read pending batch data
        # ------------------------------------------------------------

        pending_df = source_df.filter(
            F.expr(batch_condition)
        )

        pending_count = pending_df.count()

        logger.info(
            f"Canonical pending source count: "
            f"{pending_count}"
        )

        if pending_count == 0:
            raise GracefulExit(
                f"No delta data for {src_tbl_nm}"
            )

        # ------------------------------------------------------------
        # Deduplication
        # ------------------------------------------------------------

        if primary_key:

            primary_key = (
                primary_key
                .replace(",", " ")
                .strip()
            )

            # Determine ordering column dynamically.
            if "last_update_date" in source_columns:

                order_column = "last_update_date"

                logger.info(
                    "Canonical dedup ordering column: "
                    "last_update_date"
                )

            elif "batch_id" in source_columns:

                order_column = "batch_id"

                logger.info(
                    "Canonical dedup ordering column: "
                    "batch_id"
                )

            else:

                order_column = None

                logger.warning(
                    "Neither last_update_date nor batch_id "
                    "is available. Reading configured PK rows "
                    "without ordering-based deduplication."
                )

            if order_column:

                query = f"""
                    SELECT *
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY {primary_key}
                                   ORDER BY {order_column} DESC
                               ) AS rw_num
                        FROM {source_table}
                        WHERE {batch_condition}
                    )
                    WHERE rw_num = 1
                """

                source_df = (
                    spark.sql(query)
                    .drop("rw_num")
                )

            else:

                source_df = (
                    pending_df
                    .dropDuplicates(
                        primary_key.split()
                    )
                )

        else:

            logger.warning(
                "No primary key found in ingestion config. "
                "Reading source rows without PK deduplication."
            )

            source_df = pending_df

        # ------------------------------------------------------------
        # Final count
        # ------------------------------------------------------------

        record_count = source_df.count()

        logger.info(
            f"Canonical source data count after "
            f"deduplication: {record_count}"
        )

        if record_count == 0:
            raise GracefulExit(
                f"No canonical records after deduplication "
                f"for {src_tbl_nm}"
            )

        return source_df

    except GracefulExit:
        raise

    except Exception as exc:

        error_message = (
            str(exc).split("stacktrace")[0]
        )

        log_event_detail(
            f"Retrieving delta data - {src_tbl_nm}",
            "Failed",
            error_message,
            run_url,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            staging_start_time,
            cluster_id,
            run_id,
        )

        update_batch_log_tbl(
            "canonical",
            "N",
            batch_condition,
            source_system_name,
        )

        raise RuntimeError(
            f"Unable to retrieve delta data from "
            f"{src_tbl_nm}: {error_message}"
        ) from exc

# ============================================================================
# COLUMN MAPPING
# ============================================================================

def retrieve_column_mapping(
    src_tbl_nm,
    tgt_tbl_nm,
    batch_condition,
    source_identifier,
    source_system_name,
    run_url,
    staging_start_time,
):
    """Retrieve configured source-to-target canonical mappings."""

    try:

        mapping_df = spark.sql(
            f"""
            SELECT
                src_attribute,
                tgt_attribute,
                join_condition
            FROM {canonical_config_tbl}
            WHERE LOWER(TRIM(source_system_name))
                    = LOWER(TRIM('{source_system_name}'))
              AND LOWER(TRIM(source_identifier))
                    = LOWER(TRIM('{source_identifier}'))
              AND LOWER(TRIM(tgt_tbl_nm))
                    = LOWER(TRIM('{tgt_tbl_nm}'))
              AND LOWER(TRIM(src_tbl_nm))
                    = LOWER(TRIM('{src_tbl_nm}'))
              AND LOWER(TRIM(module))
                    = LOWER(TRIM('{MODULE_NAME}'))
            """
        )

        mapping_count = mapping_df.count()

        logger.info(
            f"Canonical mapping rows found: "
            f"{mapping_count}"
        )

        if mapping_count == 0:
            raise ValueError(
                f"No canonical column mappings found for "
                f"source={src_tbl_nm}, "
                f"target={tgt_tbl_nm}, "
                f"source_identifier={source_identifier}, "
                f"source_system_name={source_system_name}"
            )

        join_condition_row = (
            mapping_df
            .agg(
                F.concat_ws(
                    " ",
                    F.collect_set(
                        F.when(
                            F.col("join_condition").isNotNull(),
                            F.col("join_condition"),
                        )
                    ),
                ).alias("join_condition")
            )
            .first()
        )

        join_condition = ""

        if (
            join_condition_row
            and join_condition_row["join_condition"]
        ):
            join_condition = (
                join_condition_row["join_condition"]
            )

        logger.info(
            f"Canonical join condition: "
            f"{join_condition if join_condition else 'NONE'}"
        )

        return (
            mapping_df,
            join_condition,
        )

    except Exception as exc:

        error_message = (
            str(exc).split("stacktrace")[0]
        )

        log_event_detail(
            f"Creating canonical mapping table - "
            f"{tgt_tbl_nm}",
            "Failed",
            error_message,
            run_url,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            staging_start_time,
            cluster_id,
            run_id,
        )

        update_batch_log_tbl(
            "canonical",
            "N",
            batch_condition,
            source_system_name,
        )

        raise RuntimeError(
            f"Unable to retrieve canonical mappings "
            f"for {tgt_tbl_nm}: {error_message}"
        ) from exc


# ============================================================================
# MAPPING STRING
# ============================================================================

def build_mapping_string(mapping_df):
    """Build SELECT projection from configuration."""

    try:

        mapping_expressions = []

        rows = mapping_df.collect()

        if not rows:
            raise ValueError(
                "Canonical mapping configuration is empty."
            )

        for row in rows:

            src_attribute = row["src_attribute"]
            tgt_attribute = row["tgt_attribute"]

            if not tgt_attribute:
                continue

            # Skip Load_Date -- create_mapped_data() adds it automatically
            if tgt_attribute.strip().lower() == "load_date":
                logger.info(
                    f"Skipping canonical mapping "
                    f"{src_attribute}->{tgt_attribute} "
                    f"(collides with auto-added Load_Date)"
                )
                continue

            if src_attribute is None:

                expression = (
                    f"CAST(NULL AS STRING) "
                    f"AS {tgt_attribute}"
                )

            else:

                source_lower = (
                    src_attribute.lower()
                )

                if any(
                    keyword in source_lower
                    for keyword in (
                        "timestamp",
                        "date",
                        "datetime",
                        "time",
                        "dob",
                        "cast",
                    )
                ):

                    expression = (
                        f"{src_attribute} "
                        f"AS {tgt_attribute}"
                    )

                else:

                    expression = (
                        f"CAST({src_attribute} AS STRING) "
                        f"AS {tgt_attribute}"
                    )

            mapping_expressions.append(
                expression
            )

        if not mapping_expressions:
            raise ValueError(
                "No valid canonical mapping expressions generated."
            )

        mapping_string = ", ".join(
            mapping_expressions
        )

        logger.info(
            f"Canonical mapping expression: "
            f"{mapping_string}"
        )

        return mapping_string

    except Exception as exc:

        raise RuntimeError(
            "Error while generating canonical "
            "mapping string"
        ) from exc


# ============================================================================
# MAPPED DATA
# ============================================================================

def create_mapped_data(
    source_df,
    mapping_string,
    join_condition,
    src_tbl_nm,
    batch_condition,
    source_identifier,
    source_system_name,
    run_url,
    staging_start_time,
):
    """Create mapped canonical DataFrame."""

    try:

        source_df.createOrReplaceTempView(
            "delta_vw"
        )

        mapped_sql = f"""
            SELECT DISTINCT
                {mapping_string},
                n.Batch_ID,
                current_timestamp() AS Load_Date
            FROM delta_vw n
            {join_condition}
        """

        logger.info(
            "Executing canonical mapping SQL."
        )

        mapped_df = spark.sql(
            mapped_sql
        )

        record_count = (
            mapped_df.count()
        )

        logger.info(
            f"Canonical mapped data created for "
            f"{src_tbl_nm}; count={record_count}"
        )

        if record_count == 0:
            raise GracefulExit(
                f"No mapped canonical records for "
                f"{src_tbl_nm}"
            )

        return mapped_df

    except GracefulExit:
        raise

    except Exception as exc:

        error_message = (
            str(exc).split("stacktrace")[0]
        )

        log_event_detail(
            f"Creating mapped data - {src_tbl_nm}",
            "Failed",
            error_message,
            run_url,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            staging_start_time,
            cluster_id,
            run_id,
        )

        update_batch_log_tbl(
            "canonical",
            "N",
            batch_condition,
            source_system_name,
        )

        raise RuntimeError(
            f"Error creating mapped data for "
            f"{src_tbl_nm}: {error_message}"
        ) from exc


# ============================================================================
# CREATE CANONICAL DATA
# ============================================================================

def create_canonical_view(
    src_tbl_nm,
    tgt_tbl_nm,
    canonical_config_df,
    batch_condition,
    source_identifier,
    source_system_name,
    run_url,
    staging_start_time,
):
    """Build canonical data for one configured source table."""

    (
        src_schema,
        resolved_src_tbl_nm,
        tgt_schema,
        resolved_tgt_tbl_nm,
    ) = get_canonical_config_details(
        src_tbl_nm,
        tgt_tbl_nm,
        canonical_config_df,
        batch_condition,
        source_identifier,
        source_system_name,
        run_url,
        staging_start_time,
    )

    delta_df = get_delta_data_from_src(
        src_schema,
        resolved_src_tbl_nm,
        batch_condition,
        source_identifier,
        source_system_name,
        run_url,
        staging_start_time,
    )

    (
        mapping_df,
        join_condition,
    ) = retrieve_column_mapping(
        resolved_src_tbl_nm,
        resolved_tgt_tbl_nm,
        batch_condition,
        source_identifier,
        source_system_name,
        run_url,
        staging_start_time,
    )

    mapping_string = build_mapping_string(
        mapping_df
    )

    result_df = create_mapped_data(
        delta_df,
        mapping_string,
        join_condition,
        resolved_src_tbl_nm,
        batch_condition,
        source_identifier,
        source_system_name,
        run_url,
        staging_start_time,
    )

    return (
        result_df,
        tgt_schema,
        resolved_tgt_tbl_nm,
    )


# ============================================================================
# WRITE TARGET
# ============================================================================

def write_to_target_table(
    tgt_schema,
    tgt_tbl_nm,
    consolidated_df,
    batch_condition,
    source_system_name,
    source_identifier,
    run_url,
    staging_start_time,
):
    """Write consolidated canonical data to Delta target."""

    target_table = (
        f"{tgt_schema}.{tgt_tbl_nm}"
    )

    try:

        logger.info(
            f"Canonical target write started: "
            f"{target_table}"
        )

        # Ensure target schema exists.
        spark.sql(
            f"""
            CREATE SCHEMA IF NOT EXISTS {tgt_schema}
            """
        )

        table_exists = (
            spark.catalog.tableExists(
                target_table
            )
        )

        record_count = (
            consolidated_df.count()
        )

        logger.info(
            f"Canonical target exists={table_exists}; "
            f"records_to_write={record_count}"
        )

        if not table_exists:

            (
                consolidated_df.write
                .format("delta")
                .mode("overwrite")
                .option(
                    "overwriteSchema",
                    "true",
                )
                .option("path", get_s3_location(target_table))
                .saveAsTable(
                    target_table
                )
            )

            logger.info(
                f"{target_table} created with "
                f"{record_count} records"
            )

        else:

            # Delete existing rows for the pending batch.
            #
            # Source_Name may not exist in every canonical
            # test mapping, so delete by batch_id only.
            spark.sql(
                f"""
                DELETE FROM {target_table}
                WHERE {batch_condition}
                """
            )

            (
                consolidated_df.write
                .format("delta")
                .mode("append")
                .option(
                    "mergeSchema",
                    "true",
                )
                .option("path", get_s3_location(target_table))
                .saveAsTable(
                    target_table
                )
            )

            logger.info(
                f"{target_table} appended with "
                f"{record_count} records"
            )

        # OPTIMIZE is best effort.
        try:

            spark.sql(
                f"OPTIMIZE {target_table}"
            )

            logger.info(
                f"OPTIMIZE completed for "
                f"{target_table}"
            )

        except Exception as optimize_exc:

            logger.warning(
                f"OPTIMIZE skipped for "
                f"{target_table}: "
                f"{str(optimize_exc)}"
            )

        logger.info(
            f"Canonical target write completed: "
            f"{target_table}"
        )

    except Exception as exc:

        error_message = (
            str(exc).split("stacktrace")[0]
        )

        log_event_detail(
            f"Writing Canonical table - "
            f"{tgt_tbl_nm}",
            "Failed",
            error_message,
            run_url,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            staging_start_time,
            cluster_id,
            run_id,
        )

        update_batch_log_tbl(
            "canonical",
            "N",
            batch_condition,
            source_system_name,
        )

        raise RuntimeError(
            f"Error writing Canonical table - "
            f"{tgt_tbl_nm}: {error_message}"
        ) from exc


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main_canonical_pipeline(skip_batch_update: bool = False):
    """Execute complete canonical pipeline."""

    (
        source_identifier,
        source_system_name,
        target_table_name,
    ) = read_parameters()

    run_url = get_notebook_run_url()

    staging_start_time = datetime.now()

    logger.info(
        f"Starting Canonical processing: "
        f"source_identifier={source_identifier}, "
        f"source_system_name={source_system_name}, "
        f"target={target_table_name}"
    )

    # ------------------------------------------------------------------------
    # BATCH
    # ------------------------------------------------------------------------

    batch_condition = get_batch_condition(
        source_system_name,
        source_identifier,
        run_url,
        staging_start_time,
    )

    # ------------------------------------------------------------------------
    # CANONICAL CONFIG
    # ------------------------------------------------------------------------

    canonical_config_df = (
        spark.table(
            canonical_config_tbl
        )
    )

    canonical_config_df = (
        canonical_config_df
        .filter(
            (
                F.lower(
                    F.trim(
                        F.col(
                            "source_system_name"
                        )
                    )
                )
                == source_system_name.strip().lower()
            )
            &
            (
                F.lower(
                    F.trim(
                        F.col(
                            "source_identifier"
                        )
                    )
                )
                == source_identifier.strip().lower()
            )
        )
    )

    config_count = (
        canonical_config_df.count()
    )

    logger.info(
        f"Canonical configuration rows found: "
        f"{config_count}"
    )

    if config_count == 0:

        raise GracefulExit(
            f"No canonical configuration found for "
            f"source_identifier={source_identifier}, "
            f"source_system_name={source_system_name}"
        )

    # ------------------------------------------------------------------------
    # SOURCE TABLES
    # ------------------------------------------------------------------------

    # Resolve configured source tables. In the existing control framework,
    # the runtime target can correspond to the configured std table. First use
    # an explicit canonical target match; if that is absent, resolve the
    # source table through ctl_entity_mstr for the requested source.
    # Resolve configured source/target pairs.
    #
    # The canonical target is resolved from ctl_can_mapg so source and target names remain configuration-driven.
    source_tables = (
        canonical_config_df
        .filter(
            F.lower(F.trim(F.col("src_tbl_nm")))
            == target_table_name.strip().lower()
        )
        .select("src_tbl_nm", "tgt_tbl_nm")
        .distinct()
        .collect()
    )

    if not source_tables:
        entity_rows = (
            spark.table(ingestion_config_tbl)
            .filter(
                (F.lower(F.trim(F.col("source_identifier"))) == source_identifier.strip().lower())
                & (F.lower(F.trim(F.col("source_system_name"))) == source_system_name.strip().lower())
                & (F.lower(F.trim(F.col("std_table_name"))) == target_table_name.strip().lower())
                & (F.col("source_active_flag") == True)
            )
            .select("std_table_name")
            .distinct()
            .collect()
        )

        if entity_rows:
            configured_source = entity_rows[0]["std_table_name"]
            source_tables = (
                canonical_config_df
                .filter(
                    F.lower(F.trim(F.col("src_tbl_nm")))
                    == configured_source.strip().lower()
                )
                .select("src_tbl_nm", "tgt_tbl_nm")
                .distinct()
                .collect()
            )

    if not source_tables:
        raise GracefulExit(
            f"No canonical source tables found for "
            f"target {target_table_name}"
        )

    # ------------------------------------------------------------------------
    # CONSOLIDATE
    # ------------------------------------------------------------------------

    consolidated_df = None

    target_schema = None

    resolved_target_name = None

    for row in source_tables:

        src_tbl_nm = row["src_tbl_nm"]
        configured_target_tbl_nm = row["tgt_tbl_nm"]

        logger.info(
            f"Processing canonical source table: "
            f"{src_tbl_nm} -> target table: "
            f"{configured_target_tbl_nm}"
        )

        try:

            (
                result_df,
                target_schema,
                resolved_target_name,
            ) = create_canonical_view(
                src_tbl_nm,
                configured_target_tbl_nm,
                canonical_config_df,
                batch_condition,
                source_identifier,
                source_system_name,
                run_url,
                staging_start_time,
            )

            if consolidated_df is None:

                consolidated_df = (
                    result_df
                )

            else:

                consolidated_df = (
                    consolidated_df.unionByName(
                        result_df,
                        allowMissingColumns=True,
                    )
                )

        except GracefulExit as exc:

            logger.info(
                f"Skipping source table "
                f"{src_tbl_nm}: {exc}"
            )

            continue

    # ------------------------------------------------------------------------
    # VALIDATE CONSOLIDATED DATA
    # ------------------------------------------------------------------------

    if (
        consolidated_df is None
        or consolidated_df.count() == 0
    ):

        log_event_detail(
            f"Canonical - {target_table_name}",
            "Passed",
            "No consolidated data to process.",
            run_url,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            staging_start_time,
            cluster_id,
            run_id,
        )

        raise GracefulExit(
            f"No consolidated data to write for "
            f"target table: {target_table_name}"
        )

    final_record_count = (
        consolidated_df.count()
    )

    logger.info(
        f"Canonical consolidated data ready: "
        f"{final_record_count} records"
    )

    logger.info(
        f"Canonical target: "
        f"{target_schema}.{resolved_target_name}"
    )

    # ------------------------------------------------------------------------
    # WRITE
    # ------------------------------------------------------------------------

    write_to_target_table(
        target_schema,
        resolved_target_name,
        consolidated_df,
        batch_condition,
        source_system_name,
        source_identifier,
        run_url,
        staging_start_time,
    )

    # ------------------------------------------------------------------------
    # UPDATE BATCH STATUS
    # ------------------------------------------------------------------------

    if not skip_batch_update:
        update_batch_log_tbl(
            "canonical",
            "Y",
            batch_condition,
            source_system_name,
        )

        logger.info(
            f"Canonical batch status updated to Y "
            f"for source system {source_system_name}"
        )
    else:
        logger.info(
            "Skipping canonical batch status update "
            "(skip_batch_update=True)"
        )

    # ------------------------------------------------------------------------
    # FINAL LOG
    # ------------------------------------------------------------------------

    logger.info(
        f"Canonical table "
        f"{target_schema}.{resolved_target_name} "
        f"updated with "
        f"{final_record_count} records"
    )

    log_event_detail(
        f"Canonical - {resolved_target_name}",
        "Passed",
        (
            f"Successfully processed "
            f"{final_record_count} records."
        ),
        run_url,
        source_identifier,
        source_system_name,
        job_id,
        MODULE_NAME,
        staging_start_time,
        cluster_id,
        run_id,
    )


# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================

if __name__ == "__main__":

    try:

        main_canonical_pipeline()

    except GracefulExit as exc:

        logger.info(
            str(exc)
        )


# ============================================================================
# USER CONFIGURATION
# ============================================================================
#
# 1) Source/target schemas and table names are resolved from the
#    canonical configuration table.
#
# 2) Databricks/Spark runtime must have access to the configured
#    catalog/schema.
#
# 3) If the target is migrated to Snowflake, configure the
#    Databricks-to-Snowflake connection separately.
#
# 4) Credentials must not be placed in this source file.
#
# ============================================================================