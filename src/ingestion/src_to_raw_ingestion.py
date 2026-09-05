"""
RAW ingestion pipeline.

Reads source metadata from the ingestion control table, processes pending
batches, applies incremental/full-load rules, writes RAW Delta tables,
archives source files, and records execution details.
"""

import sys
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType

try:
    from ..core.runtime_config import (
        DEFAULT_ALERT_EMAILS,
        archive_path,
        batch_log_tbl,
        catalog,
        util_schema,
        cluster_id,
        email_config,
        env,
        get_maillist,
        get_notebook_run_url,
        ingestion_config_tbl,
        job_id,
        run_id,
    )
    from ..core.data_io import (
        archive_file,
        file_exists_dynamic,
        read_data_dynamic,
        send_email,
    )
    from ..core.logging_utils import log_event_detail, logger

except ImportError:
    from core.runtime_config import (
        DEFAULT_ALERT_EMAILS,
        archive_path,
        util_schema,
        batch_log_tbl,
        catalog,
        cluster_id,
        email_config,
        env,
        get_maillist,
        get_notebook_run_url,
        ingestion_config_tbl,
        job_id,
        run_id,
    )
    from core.data_io import (
        archive_file,
        file_exists_dynamic,
        read_data_dynamic,
        send_email,
    )
    from core.logging_utils import log_event_detail, logger


spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

MODULE_NAME = "RAW_SOURCE_READ_DATA"
RUN_URL = get_notebook_run_url()
SOURCE_IDENTIFIER = sys.argv[1]
SOURCE_SYSTEM_NAME = sys.argv[2]

TO_LIST, INCLUDE_TABLE_LIST = (
    get_maillist(SOURCE_SYSTEM_NAME)
    if catalog
    else (DEFAULT_ALERT_EMAILS, [])
)

STAGING_START_TIME = datetime.now()


def apply_delta_filter(
    df,
    full_load_flag,
    source_load_type,
    date_filter_col,
    batch_start_date,
    batch_end_date,
    max_loaddate,
    filter_cond,
):
    """Apply configured load and date filtering."""

    if full_load_flag is True:
        logger.info(
            "Full load required - skipping batch date filtering."
        )
        return df

    if filter_cond is not None:
        logger.info(
            f"Applying filter condition: {filter_cond}"
        )
        df = df.filter(filter_cond)

    if (
        source_load_type == "delta"
        and date_filter_col.upper()
        in [column.upper() for column in df.columns]
    ):
        df = df.withColumn(
            "parsed_ts",
            F.coalesce(
                F.to_timestamp(
                    date_filter_col,
                    "yyyy-MM-dd HH:mm:ss",
                ),
                F.to_timestamp(
                    date_filter_col,
                    "yyyy-MM-dd'T'HH:mm:ss.SSSxxx",
                ),
                F.to_timestamp(
                    date_filter_col,
                    "MM/dd/yyyy HH:mm:ss.SSSSSS",
                ),
                F.to_timestamp(
                    date_filter_col,
                    "MM/dd/yyyy HH:mm:ss",
                ),
            ),
        )

        if max_loaddate:
            logger.info(
                "Filtering records using batch window "
                f"({batch_start_date} to {batch_end_date}) OR "
                f"records newer than max loaddate ({max_loaddate})"
            )

            df = df.filter(
                (
                    (F.col("parsed_ts") > batch_start_date)
                    & (F.col("parsed_ts") <= batch_end_date)
                )
                | (F.col("parsed_ts") > max_loaddate)
            ).drop("parsed_ts")

        else:
            logger.info(
                "Filtering records using batch window only "
                f"({batch_start_date} to {batch_end_date})"
            )

            df = df.filter(
                (F.col("parsed_ts") > batch_start_date)
                & (F.col("parsed_ts") <= batch_end_date)
            ).drop("parsed_ts")

    return df


def process_daily_batches(
    batch_df,
    config_df,
    archive_path,
    email_config,
    source_system_name,
    source_identifier,
):
    """Process all pending batches for a source system."""

    try:
        unprocessed_batches_df = (
            spark.read.table(batch_df)
            .filter(
                F.col("source_system_name")
                == source_system_name
            )
            .filter(
                (F.col("raw_ingestion_status") != "Y")
                | F.col("raw_ingestion_status").isNull()
            )
        )

        unprocessed_batches_rows = (
            unprocessed_batches_df.collect()
        )

        if not unprocessed_batches_rows:
            logger.info(
                f"No unprocessed batches found for "
                f"{source_system_name}. All data is up to date."
            )

            log_event_detail(
                "Batch Check",
                "Passed",
                f"No unprocessed batches found for "
                f"{source_system_name}.",
                RUN_URL,
                source_identifier,
                source_system_name,
                job_id,
                MODULE_NAME,
                STAGING_START_TIME,
                cluster_id,
                run_id,
            )
            return

    except Exception as exc:
        logger.info(
            f"Error checking batch log table: {exc}"
        )

        log_event_detail(
            "Batch Check",
            "Failed",
            f"Error checking batch table: {exc}",
            RUN_URL,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            STAGING_START_TIME,
            cluster_id,
            run_id,
        )

        send_email(
            subject=f"Batch log table Error: {source_identifier}",
            body=(
                f"Failed to read batch log table for source "
                f"{source_identifier}. Error: {exc}"
            ),
            to_email=TO_LIST,
            smtp_server=email_config["smtp_server"],
            smtp_user=email_config["smtp_user"],
        )
        raise

    try:
        batch_start_date = (
            unprocessed_batches_df
            .agg(
                F.min("batch_start_time")
                .alias("batch_start_date")
            )
            .collect()[0]["batch_start_date"]
        )

        batch_end_date = (
            unprocessed_batches_df
            .agg(
                F.max("batch_end_time")
                .alias("batch_end_date")
            )
            .collect()[0]["batch_end_date"]
        )

        batch_id = [
            row["batch_id"]
            for row in (
                unprocessed_batches_df
                .select("batch_id")
                .collect()
            )
        ]

        condition = (
            "batch_id IN ("
            + ", ".join(str(batch) for batch in batch_id)
            + ")"
        )

        delete_condition = (
            f"where {condition} "
            f"and Source_Name = '{source_system_name}'"
        )

        logger.info(
            f"Processing batch {batch_id} for date range "
            f"{batch_start_date} to {batch_end_date}"
        )

        process_files_from_metadata(
            config_df,
            archive_path,
            email_config,
            source_identifier,
            batch_start_date,
            batch_end_date,
            delete_condition,
            batch_id,
        )

    except Exception as exc:
        logger.info(
            f"Processing failed for batch {batch_id}. "
            f"Error: {exc}"
        )

        send_email(
            subject=f"Batch Processing Error: {source_identifier}",
            body=(
                f"Failed to process batch {batch_id} "
                f"for source {source_identifier}. "
                f"Error: {exc}"
            ),
            to_email=TO_LIST,
            smtp_server=email_config["smtp_server"],
            smtp_user=email_config["smtp_user"],
        )

        log_event_detail(
            "Batch Processing",
            "Failed",
            (
                f"Processing failed for batch {batch_id}. "
                f"Error: {exc}"
            ),
            RUN_URL,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            STAGING_START_TIME,
            cluster_id,
            run_id,
        )
        raise

    logger.info("All pending batches processed.")


def process_files_from_metadata(
    config_df,
    archive_path,
    email_config,
    source_identifier,
    batch_start_date,
    batch_end_date,
    delete_condition,
    batch_id,
):
    """Process one configured source into its RAW Delta table."""

    source_system_name = "Not defined"

    try:
        sources_df = config_df.filter(
            (F.col("source_identifier") == source_identifier)
            & (F.col("source_active_flag") == True)
        )

        if sources_df.count() == 0:
            error_message = (
                f"No active sources found for system "
                f"'{source_identifier}'"
            )

            logger.info(error_message)

            log_event_detail(
                "Metadata Check",
                "Failed",
                error_message,
                RUN_URL,
                source_identifier,
                source_system_name,
                job_id,
                MODULE_NAME,
                STAGING_START_TIME,
                cluster_id,
                run_id,
            )
            return

        row = sources_df.collect()[0]

        source_system_name = row["source_system_name"]
        folder_path = row["source_location"].replace(
            "{env}",
            env,
        )
        filename_template = row["source_name"]
        source_type = row["source_type"]
        delimiter = row["source_delimiter"] or ","
        schema_name = row["raw_table_schema"]
        table_name = row["raw_table_name"]

        target_table_full_name = (
            f"{catalog}.{schema_name}.{table_name}"
        )

        source_load_type = row["source_load_type"]
        archive_flag = row["archive_flag"]
        full_load_flag = row["full_load_flag"]
        date_filter_col = row["date_filter_col"]
        filter_cond = row["filter_cond"]

        logger.info(
            f"Processing {source_system_name}: "
            f"{folder_path}/{filename_template}"
        )

    except Exception as exc:
        error_message = (
            f"Error during metadata lookup for "
            f"'{source_identifier}': {exc}"
        )

        logger.info(error_message)

        send_email(
            subject=f"Metadata Lookup Error: {source_identifier}",
            body=error_message,
            to_email=TO_LIST,
            smtp_server=email_config["smtp_server"],
            smtp_user=email_config["smtp_user"],
        )

        log_event_detail(
            f"Metadata Lookup {source_identifier}",
            "Failed",
            error_message,
            RUN_URL,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            STAGING_START_TIME,
            cluster_id,
            run_id,
        )
        raise

    try:
        logger.info(
            f"Checking file existence for {source_identifier}"
        )

        resolved_path = file_exists_dynamic(
            source_type,
            folder_path,
            filename_template,
        )

        if not resolved_path:
            raise FileNotFoundError(
                f"File/Table not found: "
                f"{folder_path}/{filename_template}"
            )

        logger.info(
            f"File path exists for {source_identifier}"
        )

    except FileNotFoundError as exc:
        error_message = str(exc)

        logger.info(
            f"Known error during file existence check: "
            f"{error_message}"
        )

        send_email(
            subject=(
                f"File Not Found: "
                f"{folder_path}/{filename_template}"
            ),
            body=(
                f"Failed to find the source file for "
                f"{folder_path}/{filename_template}. "
                f"Error: {error_message}"
            ),
            to_email=TO_LIST,
            smtp_server=email_config["smtp_server"],
            smtp_user=email_config["smtp_user"],
        )

        log_event_detail(
            f"File Existence Check "
            f"{folder_path}/{filename_template}",
            "Failed",
            error_message,
            RUN_URL,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            STAGING_START_TIME,
            cluster_id,
            run_id,
        )

        raise FileNotFoundError(
            f"File Existence Check Error - "
            f"{folder_path}/{filename_template}"
        ) from exc

    except Exception as exc:
        error_message = (
            f"Unexpected error during file existence check: "
            f"{exc}"
        )

        logger.info(error_message)

        send_email(
            subject=(
                f"File Check Error: "
                f"{folder_path}/{filename_template}"
            ),
            body=error_message,
            to_email=TO_LIST,
            smtp_server=email_config["smtp_server"],
            smtp_user=email_config["smtp_user"],
        )

        log_event_detail(
            f"File Existence Check "
            f"{folder_path}/{filename_template}",
            "Failed",
            error_message,
            RUN_URL,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            STAGING_START_TIME,
            cluster_id,
            run_id,
        )

        raise RuntimeError(
            f"File Existence Check Error - "
            f"{folder_path}/{filename_template}"
        ) from exc

    try:
        logger.info(
            "Starting to read source and determine load mode "
            f"for {source_identifier}"
        )

        is_table_exist = spark.catalog.tableExists(
            target_table_full_name
        )

        max_loaddate = None

        if (
            is_table_exist
            and source_load_type == "delta"
        ):
            date_column = (
                date_filter_col.strip()
                if date_filter_col
                and date_filter_col.strip()
                else "NULL"
            )

            max_loaddate = (
                spark.sql(
                    f"SELECT MAX({date_column}) "
                    f"FROM {target_table_full_name}"
                )
                .first()[0]
            )

            logger.info(
                f"Initial max loaddate: {max_loaddate}"
            )

            spark.sql(
                f"DELETE FROM {target_table_full_name} "
                f"{delete_condition}"
            )

            max_loaddate = (
                spark.sql(
                    f"SELECT MAX({date_column}) "
                    f"FROM {target_table_full_name}"
                )
                .first()[0]
            )

            logger.info(
                f"Post-delete max loaddate: {max_loaddate}"
            )

        df = read_data_dynamic(
            source_type,
            folder_path,
            filename_template,
            options={
                "delimiter": delimiter,
                "header": "true",
                "inferSchema": "true",
            },
        )

        df = apply_delta_filter(
            df,
            full_load_flag,
            source_load_type,
            date_filter_col,
            batch_start_date,
            batch_end_date,
            max_loaddate,
            filter_cond,
        )

        df = df.toDF(
            *[column.replace(" ", "_") for column in df.columns]
        ).distinct()

        df = df.withColumn(
            "LOAD_DATE",
            F.current_timestamp(),
        )

        max_batch_id = max(batch_id)

        df = (
            df.withColumn(
                "BATCH_ID",
                F.lit(max_batch_id),
            )
            .withColumn(
                "Source_Name",
                F.lit(source_system_name),
            )
        )

        if is_table_exist:
            logger.info("Performing incremental load.")

            target_schema = (
                spark.read
                .table(target_table_full_name)
                .schema
            )

            source_col_map = {
                column.upper(): column
                for column in df.columns
            }

            source_schema = {
                field.name.upper(): field.dataType
                for field in df.schema
            }

            select_exprs = []

            for target_field in target_schema:
                target_col = target_field.name
                target_col_upper = target_col.upper()

                if target_col_upper in source_col_map:
                    source_col = source_col_map[
                        target_col_upper
                    ]

                    if target_col_upper == "BATCH_ID":
                        expr = (
                            F.col(source_col)
                            .cast("int")
                            .alias(target_col)
                        )

                    elif (
                        source_schema[target_col_upper]
                        == target_field.dataType
                    ):
                        expr = F.col(source_col).alias(
                            target_col
                        )

                    else:
                        expr = (
                            F.col(source_col)
                            .cast(target_field.dataType)
                            .alias(target_col)
                        )

                else:
                    expr = (
                        F.lit(None)
                        .cast(target_field.dataType)
                        .alias(target_col)
                    )

                select_exprs.append(expr)

            df = df.select(*select_exprs)

            load_mode = (
                "overwrite"
                if source_load_type == "overwrite"
                else "append"
            )

        else:
            logger.info("Performing initial load.")

            for field in df.schema.fields:
                if field.name == "BATCH_ID":
                    df = df.withColumn(
                        field.name,
                        F.col(field.name).cast("long"),
                    )

                elif not isinstance(
                    field.dataType,
                    TimestampType,
                ):
                    df = df.withColumn(
                        field.name,
                        F.col(field.name).cast("string"),
                    )

            load_mode = "overwrite"

    except Exception as exc:
        error_message = (
            f"Error during data read or load mode "
            f"determination: {exc}"
        )

        logger.info(error_message)

        send_email(
            subject=(
                f"Data Read Error: "
                f"{folder_path}/{filename_template}"
            ),
            body=error_message,
            to_email=TO_LIST,
            smtp_server=email_config["smtp_server"],
            smtp_user=email_config["smtp_user"],
        )

        log_event_detail(
            f"Data Read/Load Mode - "
            f"{folder_path}/{filename_template}",
            "Failed",
            error_message,
            RUN_URL,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            STAGING_START_TIME,
            cluster_id,
            run_id,
        )

        raise RuntimeError(
            f"Data Read/Load Mode Error - "
            f"{folder_path}/{filename_template}"
        ) from exc

    try:
        if df.count() > 0:
            record_count = df.count()

            logger.info(
                f"Dataframe count: {record_count}. "
                "Writing data."
            )

            df.write.format("delta").mode(
                load_mode
            ).saveAsTable(
                target_table_full_name
            )

            logger.info("Write complete.")

            if full_load_flag is True:
                logger.info(
                    "Updating full_load_flag to false"
                )

                # Preserved exactly from the source implementation.
                spark.sql(
                    f"""
                    UPDATE {ingestion_config_tbl.rsplit(".", 1)[0]}.ctl_entity_mstr
                    SET full_load_flag = false
                    WHERE source_identifier = '{source_identifier}'
                    """
                )

            logger.info(
                f"Optimizing Delta table: "
                f"{target_table_full_name}"
            )

            spark.sql(
                f"OPTIMIZE {target_table_full_name}"
            )

        else:
            raise ValueError(
                "Source file is empty or no new data found "
                f"{target_table_full_name}"
            )

    except ValueError as exc:
        error_message = str(exc)

        logger.info(
            f"Known error during write: {error_message}"
        )

        log_event_detail(
            f"Data Write - {target_table_full_name}",
            "Failed",
            error_message,
            RUN_URL,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            STAGING_START_TIME,
            cluster_id,
            run_id,
        )

        if table_name.lower() in INCLUDE_TABLE_LIST:
            send_email(
                subject=(
                    f"No new data found: "
                    f"{target_table_full_name}"
                ),
                body=error_message,
                to_email=TO_LIST,
                smtp_server=email_config["smtp_server"],
                smtp_user=email_config["smtp_user"],
            )

    except Exception as exc:
        error_message = str(exc).split("\n")[0]

        logger.info(error_message)

        send_email(
            subject=(
                f"Data Write Error: "
                f"{target_table_full_name}"
            ),
            body=error_message,
            to_email=TO_LIST,
            smtp_server=email_config["smtp_server"],
            smtp_user=email_config["smtp_user"],
        )

        log_event_detail(
            f"Data Write - {target_table_full_name}",
            "Failed",
            error_message,
            RUN_URL,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            STAGING_START_TIME,
            cluster_id,
            run_id,
        )

        raise RuntimeError(
            f"Data Write Error - {target_table_full_name}"
        ) from exc

    try:
        logger.info(
            f"Checking if we need to archive file "
            f"{source_system_name}"
        )

        archive_file(
            source_type,
            archive_flag,
            resolved_path,
            archive_path,
            source_system_name,
        )

    except Exception as exc:
        error_message = (
            f"Error during file archiving: {exc}"
        )

        logger.info(error_message)

        send_email(
            subject=f"Archiving Error: {source_system_name}",
            body=error_message,
            to_email=TO_LIST,
            smtp_server=email_config["smtp_server"],
            smtp_user=email_config["smtp_user"],
        )

        log_event_detail(
            f"File Archiving - {source_system_name}",
            "Failed",
            error_message,
            RUN_URL,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            STAGING_START_TIME,
            cluster_id,
            run_id,
        )

        raise RuntimeError(
            f"File Archiving error - {source_system_name}"
        ) from exc

    logger.info("Processing complete.")

    log_event_detail(
        f"Overall Process completed for - "
        f"{folder_path}/{filename_template}",
        "Passed",
        "All steps completed successfully.",
        RUN_URL,
        source_identifier,
        source_system_name,
        job_id,
        MODULE_NAME,
        STAGING_START_TIME,
        cluster_id,
        run_id,
    )


def get_config_df(spark_session, ingestion_config_table):
    """Load ingestion configuration."""
    return spark_session.read.table(
        ingestion_config_table
    )


def run_ingestion_pipeline(
    spark_session,
    source_system_name,
    source_identifier,
):
    """Entry point for the RAW ingestion pipeline."""

    try:
        config_df = get_config_df(
            spark_session,
            ingestion_config_tbl,
        )
        batch_df_name = batch_log_tbl

    except Exception as exc:
        logger.error(
            f"FATAL: Failed to load config or batch "
            f"table name: {exc}"
        )
        raise

    process_daily_batches(
        batch_df_name,
        config_df,
        archive_path,
        email_config,
        source_system_name,
        source_identifier,
    )


if __name__ == "__main__":
    run_ingestion_pipeline(
        spark,
        SOURCE_SYSTEM_NAME,
        SOURCE_IDENTIFIER,
    )

# ============================================================================
# USER CONFIGURATION - DATBRICKS / AWS / SNOWFLAKE RAW
# ============================================================================
# 1) Databricks runtime: attach/install the required Spark, Delta, boto3 and
#    project dependencies before running this job.
# 2) AWS S3: configure the AWS runtime role/credentials and the project bucket
#    supplied in common_variables.py; do NOT paste AWS keys into this file.
# 3) Source paths, raw schema, raw table, load type and filters are metadata-
#    driven. Enter/change them in the project's ingestion control/config table,
#    not by hardcoding them here.
# 4) Snowflake RAW: the final account/database/schema/warehouse credentials must
#    be supplied in the Databricks-to-Snowflake connection configuration.
#    Do NOT hardcode Snowflake username/password/private key in this file.
# 5) If using a Snowflake connector/secret, place the secret name/connection
#    identifier in the Databricks secret scope or AWS Secrets Manager as agreed
#    for the environment, then reference it from runtime configuration.
# ============================================================================

