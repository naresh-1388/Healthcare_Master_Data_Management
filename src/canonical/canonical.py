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
from pyspark.sql.utils import AnalysisException

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
    from ..core.logging_utils import log_event_detail, logger
except ImportError:
    from core.runtime_config import (
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
    from core.logging_utils import log_event_detail, logger


spark = SparkSession.builder.getOrCreate()

MODULE_NAME = "Canonical"


class GracefulExit(Exception):
    """Expected condition where canonical processing can stop safely."""


def read_parameters():
    """
    Expected execution arguments:

        sys.argv[2] -> source_identifier
        sys.argv[3] -> source_system_name
        sys.argv[4] -> target_table_name

    sys.argv[1] is intentionally not used because the supplied
    canonical implementation starts reading from position 2.
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


def get_batch_condition(
    source_system_name,
    source_identifier,
    run_url,
    staging_start_time,
):
    """Return the pending canonical batch condition."""

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
            "No delta condition to process.. "
            "Exiting gracefully"
        )

    return (
        "batch_id IN ("
        + ", ".join(str(batch_id) for batch_id in batch_ids)
        + ")"
    )


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
    """Retrieve source and target schema details from canonical config."""

    try:
        config_filter = (
            (F.lower(F.col("tgt_tbl_nm")) == target_table_name.lower())
            & (F.lower(F.col("src_tbl_nm")) == src_table_name.lower())
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

        src_schema = f"{catalog}.{config_row['src_schema']}"
        src_tbl_nm = config_row["src_tbl_nm"]

        tgt_schema = f"{catalog}.{config_row['tgt_schema']}"
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
        error_message = str(exc).split("stacktrace")[0]

        log_event_detail(
            f"Fetching Value from Config File - {target_table_name}",
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


def get_delta_data_from_src(
    src_schema,
    src_tbl_nm,
    batch_condition,
    source_identifier,
    source_system_name,
    run_url,
    staging_start_time,
):
    """Read pending source data and deduplicate using configured PK."""

    try:
        config_row = (
            spark.sql(
                f"""
                SELECT source_primary_key
                FROM {ingestion_config_tbl}
                WHERE (
                    std_table_name = '{src_tbl_nm}'
                    OR raw_table_name = '{src_tbl_nm}'
                )
                AND source_system_name = '{source_identifier}'
                AND is_active = true
                """
            )
            .first()
        )

        primary_key = (
            config_row["source_primary_key"]
            if config_row is not None
            else None
        )

        source_table = f"{src_schema}.{src_tbl_nm}"

        if primary_key:
            primary_key = primary_key.replace(",", " ")

            query = f"""
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY {primary_key}
                               ORDER BY last_update_date DESC
                           ) AS rw_num
                    FROM {source_table}
                    WHERE {batch_condition}
                )
                WHERE rw_num = 1
            """

            source_df = spark.sql(query).drop("rw_num")

        else:
            source_df = spark.sql(
                f"""
                SELECT *
                FROM {source_table}
                WHERE {batch_condition}
                """
            )

        return source_df

    except Exception as exc:
        error_message = str(exc).split("stacktrace")[0]

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
            f"Unable to retrieve delta data from {src_tbl_nm}"
        ) from exc


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
            WHERE source_system_name = '{source_identifier}'
              AND tgt_tbl_nm = '{tgt_tbl_nm}'
              AND src_tbl_nm = '{src_tbl_nm}'
              AND module = '{MODULE_NAME}'
            """
        )

        join_condition_row = (
            mapping_df
            .agg(
                F.concat_ws(
                    " ",
                    F.collect_set("join_condition"),
                ).alias("join_condition")
            )
            .first()
        )

        join_condition = (
            join_condition_row["join_condition"]
            if join_condition_row
            and join_condition_row["join_condition"]
            else ""
        )

        return mapping_df, join_condition

    except Exception as exc:
        error_message = str(exc).split("stacktrace")[0]

        log_event_detail(
            f"Creating canonical mapping table - {tgt_tbl_nm}",
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
            f"for {tgt_tbl_nm}"
        ) from exc


def build_mapping_string(mapping_df):
    """
    Build the SELECT projection from configuration.

    Date/time/cast expressions are preserved as expressions.
    Other source attributes are cast to STRING, matching the
    supplied canonical implementation.
    """

    try:
        mapping_expressions = []

        for row in mapping_df.collect():

            src_attribute = row["src_attribute"]
            tgt_attribute = row["tgt_attribute"]

            if src_attribute is None:
                expression = (
                    f"CAST(NULL AS STRING) AS {tgt_attribute}"
                )

            else:
                source_lower = src_attribute.lower()

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
                        f"{src_attribute} AS {tgt_attribute}"
                    )

                else:
                    expression = (
                        f"CAST({src_attribute} AS STRING) "
                        f"AS {tgt_attribute}"
                    )

            mapping_expressions.append(expression)

        return ", ".join(mapping_expressions)

    except Exception as exc:
        raise RuntimeError(
            "Error while generating canonical mapping string"
        ) from exc


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

        mapped_df = spark.sql(mapped_sql)

        logger.info(
            f"Canonical mapped data created for "
            f"{src_tbl_nm}; count={mapped_df.count()}"
        )

        return mapped_df

    except Exception as exc:
        error_message = str(exc).split("stacktrace")[0]

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
            f"Error creating mapped data for {src_tbl_nm}"
        ) from exc


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

    if delta_df.count() == 0:
        raise GracefulExit(
            f"No delta data for {resolved_src_tbl_nm}"
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

    target_table = f"{tgt_schema}.{tgt_tbl_nm}"

    for attempt in range(5):

        try:
            table_exists = spark.catalog.tableExists(
                target_table
            )

            if not table_exists:

                (
                    consolidated_df.write
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(target_table)
                )

                logger.info(
                    f"{target_table} created with "
                    f"{consolidated_df.count()} records"
                )

            else:

                spark.sql(
                    f"""
                    DELETE FROM {target_table}
                    WHERE {batch_condition}
                      AND Source_Name = '{source_system_name}'
                    """
                )

                (
                    consolidated_df.write
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(target_table)
                )

                logger.info(
                    f"{target_table} appended with "
                    f"{consolidated_df.count()} records"
                )

            spark.sql(
                f"OPTIMIZE {target_table}"
            )

            return

        except Exception as exc:

            if attempt == 4:
                error_message = (
                    str(exc).split("stacktrace")[0]
                )

                log_event_detail(
                    f"Appending values in Canonical table - "
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
                    f"{tgt_tbl_nm}"
                ) from exc

            logger.warning(
                f"Canonical write attempt {attempt + 1} "
                f"failed. Retrying..."
            )

            time.sleep(10)


def main_canonical_pipeline():
    """Execute the complete canonical pipeline."""

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

    batch_condition = get_batch_condition(
        source_system_name,
        source_identifier,
        run_url,
        staging_start_time,
    )

    canonical_config_df = (
        spark.table(canonical_config_tbl)
        .filter(
            F.lower(F.col("source_system_name"))
            == source_identifier.lower()
        )
    )

    source_tables = (
        canonical_config_df
        .filter(
            F.lower(F.col("tgt_tbl_nm"))
            == target_table_name.lower()
        )
        .select("src_tbl_nm")
        .distinct()
        .collect()
    )

    if not source_tables:
        raise GracefulExit(
            f"No canonical source tables found for "
            f"target {target_table_name}"
        )

    consolidated_df = None
    target_schema = None
    resolved_target_name = target_table_name

    for row in source_tables:

        src_tbl_nm = row["src_tbl_nm"]

        logger.info(
            f"Processing canonical source table: "
            f"{src_tbl_nm}"
        )

        try:
            (
                result_df,
                target_schema,
                resolved_target_name,
            ) = create_canonical_view(
                src_tbl_nm,
                target_table_name,
                canonical_config_df,
                batch_condition,
                source_identifier,
                source_system_name,
                run_url,
                staging_start_time,
            )

            if consolidated_df is None:
                consolidated_df = result_df

            else:
                consolidated_df = (
                    consolidated_df.unionByName(
                        result_df,
                        allowMissingColumns=True,
                    )
                )

        except GracefulExit as exc:
            logger.info(
                f"Skipping source table {src_tbl_nm}: {exc}"
            )
            continue

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

    record_count = consolidated_df.count()

    logger.info(
        f"Canonical table "
        f"{target_schema}.{resolved_target_name} "
        f"updated with {record_count} records"
    )

    log_event_detail(
        f"Canonical - {resolved_target_name}",
        "Passed",
        f"Successfully processed {record_count} records.",
        run_url,
        source_identifier,
        source_system_name,
        job_id,
        MODULE_NAME,
        staging_start_time,
        cluster_id,
        run_id,
    )


if __name__ == "__main__":
    try:
        main_canonical_pipeline()

    except GracefulExit as exc:
        logger.info(str(exc))

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# 1) Source/target schemas and table names are resolved from the supplied
#    canonical configuration table. Do not hardcode business table names here.
# 2) Databricks/Spark runtime must have access to the configured catalog/schema.
# 3) If the target is migrated to Snowflake, configure the Databricks-to-Snowflake
#    connection separately; do not place credentials in this source file.
# ============================================================================

