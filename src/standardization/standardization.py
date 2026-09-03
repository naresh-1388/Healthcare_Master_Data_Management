"""
Standardization pipeline.

Reads ingestion and standardization metadata, identifies pending batches,
deduplicates RAW records, applies configured standardization functions,
and writes the standardized Delta table.
"""

import sys
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

try:
    from ..core.runtime_config import (
        DEFAULT_ALERT_EMAILS,
        batch_log_tbl,
        catalog,
        cluster_id,
        email_config,
        get_batch_status_filter,
        get_maillist,
        get_notebook_run_url,
        ingestion_config_tbl,
        job_id,
        run_id,
        standardization_config_tbl,
    )
    from ..core.data_io import send_email
    from ..core.logging_utils import log_event_detail, logger

    # The supplied project does not contain this implementation.
    # Keep the dependency/interface unchanged.
    from .standardization_function import function_mapping

except ImportError:
    from core.runtime_config import (
        DEFAULT_ALERT_EMAILS,
        batch_log_tbl,
        catalog,
        cluster_id,
        email_config,
        get_batch_status_filter,
        get_maillist,
        get_notebook_run_url,
        ingestion_config_tbl,
        job_id,
        run_id,
        standardization_config_tbl,
    )
    from core.data_io import send_email
    from core.logging_utils import log_event_detail, logger
    from standardization_function import function_mapping


spark = SparkSession.builder.getOrCreate()

MODULE_NAME = "Standardization"


class GracefulExit(Exception):
    """Expected condition where processing can end without failure."""


def read_parameters():
    """
    Expected arguments:
        1. source_identifier
        2. source_system_name
        3. table_name
    """
    try:
        source_identifier = sys.argv[1]
        source_system_name = sys.argv[2]
        table_name = sys.argv[3]
    except IndexError as exc:
        logger.error(
            "Arguments missing. Expected: "
            "script.py <id> <system> <table_name>"
        )
        raise ValueError(
            "Missing required command line arguments."
        ) from exc

    logger.info(
        f"Parameters: ID={source_identifier}, "
        f"System={source_system_name}, "
        f"Table={table_name}"
    )

    return (
        source_identifier,
        source_system_name,
        table_name,
    )


def setup_pipeline_environment(source_system_name):
    """Build runtime values required by the standardization pipeline."""

    run_url = get_notebook_run_url()

    stdz_where_condition = get_batch_status_filter(
        "stdz",
        source_system_name,
    )

    canonical_where_condition = get_batch_status_filter(
        "canonical",
        source_system_name,
    )

    staging_start_time = datetime.now()

    mail_result = (
        get_maillist(source_system_name)
        if catalog
        else (DEFAULT_ALERT_EMAILS, [])
    )

    # get_maillist() returns the configured tuple in the supplied code.
    to_list = mail_result

    logger.info(
        f"{stdz_where_condition}, "
        f"{canonical_where_condition}"
    )

    return (
        run_url,
        stdz_where_condition,
        canonical_where_condition,
        staging_start_time,
        to_list,
    )


def get_delta_condition(
    stdz_where_condition,
    canonical_where_condition,
    run_url,
    source_identifier,
    source_system_name,
    staging_start_time,
):
    """Identify pending batch IDs for standardization."""

    batch_ids = (
        spark.sql(
            f"""
            SELECT collect_list(batch_id) AS batch_id
            FROM {batch_log_tbl}
            WHERE {stdz_where_condition}
               OR {canonical_where_condition}
            """
        )
        .head()[0]
    )

    logger.info(batch_ids)

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
            "No delta condition to process.. "
            "Exiting gracefully"
        )

    delta_condition = (
        "batch_id IN ("
        + ", ".join(str(batch_id) for batch_id in batch_ids)
        + ")"
    )

    logger.info(delta_condition)

    return delta_condition


def load_ingestion_config(
    source_identifier,
    table_name,
    run_url,
    source_system_name,
    staging_start_time,
    to_list,
):
    """Load the active ingestion configuration."""

    try:
        config_df = spark.table(
            ingestion_config_tbl
        )

        ingestion_details = config_df.filter(
            (F.col("source_identifier") == source_identifier)
            & (F.col("source_active_flag") == "true")
        )

        if ingestion_details.count() == 0:
            send_email(
                subject=f"No records found for {source_identifier}",
                body=(
                    f"Failed to retrieve details from "
                    f"{ingestion_config_tbl} for "
                    f"source_identifier {source_identifier} "
                    "due to inactive source or missing identifier."
                ),
                to_email=to_list,
                smtp_server=email_config["smtp_server"],
                smtp_user=email_config["smtp_user"],
            )

            log_event_detail(
                f"stdz - {table_name}",
                "Passed",
                "No record found",
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
                f"No record in {ingestion_config_tbl} "
                f"table for source_identifier "
                f"{source_identifier}"
            )

        return ingestion_details.collect()[0]

    except GracefulExit:
        raise

    except Exception as exc:
        logger.error(
            f"Error Loading Ingestion Configs for "
            f"{source_identifier}: {exc}"
        )
        raise RuntimeError(
            f"Error Loading Ingestion Configs for "
            f"{source_identifier}: {exc}"
        ) from exc


def load_standardization_config(source_identifier):
    """Load active standardization rules."""

    try:
        config_df = spark.table(
            standardization_config_tbl
        )

        return (
            config_df.filter(
                (F.col("source_identifier") == source_identifier)
                & (F.col("rule_status") == "TRUE")
            )
            .collect()
        )

    except Exception as exc:
        logger.error(
            f"Error Loading Standardization Configs "
            f"for {source_identifier}: {exc}"
        )
        raise RuntimeError(
            f"Error Loading Standardization Configs "
            f"for {source_identifier}: {exc}"
        ) from exc


def prepare_tables(
    ingestion_details,
    delta_condition,
    source_system_name,
):
    """Read RAW data, apply batch filtering and primary-key deduplication."""

    try:
        raw_table = (
            f"{catalog}."
            f"{ingestion_details['raw_table_schema']}."
            f"{ingestion_details['raw_table_name']}"
        )

        standardization_table = (
            f"{catalog}."
            f"{ingestion_details['std_table_schema']}."
            f"{ingestion_details['std_table_name']}"
        )

        primary_key = ingestion_details[
            "source_primary_key"
        ]

        active_record_sql = ingestion_details[
            "initial_load_sql"
        ]

        raw_df = spark.table(raw_table)
        raw_df.createOrReplaceTempView(
            "raw_table_vw"
        )

        logger.info(
            f"raw table {raw_table} count: "
            f"{raw_df.count()}"
        )

        record_count = (
            spark.table(batch_log_tbl)
            .filter(
                f"""
                {delta_condition}
                AND source_system_name = '{source_system_name}'
                AND batch_start_time >
                    '1900-01-01T00:00:00.000+00:00'
                """
            )
            .count()
        )

        if record_count == 1 and active_record_sql:
            active_record_df = spark.sql(
                active_record_sql
            )

            active_record_df.createOrReplaceTempView(
                "raw_table_vw"
            )

            logger.info(
                f"raw table {raw_table} count after "
                f"initial load condition: "
                f"{active_record_df.count()}"
            )

        if primary_key:
            primary_key = primary_key.replace(
                " ",
                ",",
            )

            query = f"""
                SELECT *
                FROM (
                    SELECT *,
                           row_number() OVER (
                               PARTITION BY {primary_key}
                               ORDER BY last_update_date DESC
                           ) AS rw_num
                    FROM raw_table_vw
                    WHERE {delta_condition}
                      AND Source_Name = '{source_system_name}'
                )
                WHERE rw_num = 1
            """

            df = (
                spark.sql(query)
                .drop("rw_num")
            )

            logger.info(query)

        else:
            query = f"""
                SELECT *
                FROM raw_table_vw
                WHERE {delta_condition}
                  AND Source_Name = '{source_system_name}'
            """

            df = spark.sql(query)
            logger.info(query)

        df = df.toDF(
            *[
                column.replace(" ", "_")
                for column in df.columns
            ]
        )

        logger.info(
            f"Final data to process for {raw_table}: "
            f"{df.count()}"
        )

        return (
            df,
            df.columns,
            raw_table,
            standardization_table,
            ingestion_details["std_table_name"],
        )

    except Exception as exc:
        logger.error(
            f"Error during preparing tables: {exc}"
        )
        raise RuntimeError(
            f"Error during preparing tables: {exc}"
        ) from exc


def execute_standardization(
    standardization_df,
    original_column_order,
    standardization_details,
    source_identifier,
):
    """
    Apply configured standardization functions.

    rename_columns and custom_transformation remain skipped,
    matching the supplied implementation.
    """

    for rule in standardization_details:
        try:
            rule_function = rule["rule_function"]

            if rule_function in (
                "rename_columns",
                "custom_transformation",
            ):
                continue

            standardization_df = function_mapping[
                rule_function
            ](
                standardization_df,
                rule["column_name"],
            )

            standardization_df = standardization_df.select(
                *[
                    column
                    for column in original_column_order
                    if column in standardization_df.columns
                ]
            )

        except Exception as exc:
            logger.error(
                "Error during data standardization for "
                f"{source_identifier}: {exc}"
            )

            raise RuntimeError(
                "Error during data standardization for "
                f"{source_identifier}: {exc}"
            ) from exc

    return standardization_df


def write_standardization_table(
    standardization_df,
    standardization_table,
):
    """Write standardized data to the Delta table."""

    try:
        if standardization_df.count() > 0:

            standardization_df = standardization_df.withColumn(
                "LOAD_DATE",
                F.current_timestamp(),
            )

            (
                standardization_df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(standardization_table)
            )

            logger.info(
                f"Data loaded into {standardization_table}"
            )

            logger.info(
                f"Optimizing Delta table: "
                f"{standardization_table}"
            )

            spark.sql(
                f"OPTIMIZE {standardization_table}"
            )

        else:
            raise GracefulExit(
                f"No new data to load for "
                f"{standardization_table}"
            )

    except GracefulExit:
        raise

    except Exception as exc:
        logger.error(
            f"Error writing {standardization_table}: {exc}"
        )
        raise RuntimeError(
            f"Error writing {standardization_table}: {exc}"
        ) from exc


def main_pipeline():
    """Main standardization pipeline."""

    logger.info("Reading parameters")

    (
        source_identifier,
        source_system_name,
        table_name,
    ) = read_parameters()

    logger.info(
        "Starting to read environment for "
        "running the tables"
    )

    (
        run_url,
        stdz_where,
        canonical_where,
        start_time,
        to_list,
    ) = setup_pipeline_environment(
        source_system_name
    )

    delta_condition = get_delta_condition(
        stdz_where,
        canonical_where,
        run_url,
        source_identifier,
        source_system_name,
        start_time,
    )

    ingestion_details = load_ingestion_config(
        source_identifier,
        table_name,
        run_url,
        source_system_name,
        start_time,
        to_list,
    )

    standardization_details = (
        load_standardization_config(
            source_identifier
        )
    )

    (
        standardization_df,
        original_columns,
        _,
        standardization_table,
        standardization_table_name,
    ) = prepare_tables(
        ingestion_details,
        delta_condition,
        source_system_name,
    )

    standardization_df = execute_standardization(
        standardization_df,
        original_columns,
        standardization_details,
        source_identifier,
    )

    write_standardization_table(
        standardization_df,
        standardization_table,
    )

    log_event_detail(
        f"stdz - {standardization_table_name}",
        "Passed",
        "",
        run_url,
        source_identifier,
        source_system_name,
        job_id,
        MODULE_NAME,
        start_time,
        cluster_id,
        run_id,
    )


if __name__ == "__main__":
    try:
        main_pipeline()
    except GracefulExit as exc:
        logger.info(str(exc))

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# 1) Standardization rules are metadata-driven from the supplied control table.
# 2) Source/target schema and table names must be supplied through project config.
# 3) Do not add business rules here unless they exist in the supplied mapping/rules.
# 4) The function mapping implementation is supplied separately; do not invent
#    new standardization functions to fill missing project rules.
# ============================================================================

