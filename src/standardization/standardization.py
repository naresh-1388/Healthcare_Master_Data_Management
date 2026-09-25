"""
Healthcare_MDM - RAW to Landing Standardization

Purpose:
    Apply configured RAW-to-Landing standardization rules and write the Landing data.

Rules are read from ctl_std_entity_mstr and executed through the shared function mapping.
"""

from __future__ import annotations

import sys
import logging
import uuid
from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------
# PROJECT IMPORTS
# ---------------------------------------------------------------------

try:
    from src.core.runtime_config import (
        catalog as CATALOG,
        util_schema as UTIL_SCHEMA,
        raw_schema as RAW_SCHEMA,
        lnd_schema as LANDING_SCHEMA,
    )
except Exception:
    try:
        from core.runtime_config import (
            catalog as CATALOG,
            get_s3_location,
            util_schema as UTIL_SCHEMA,
            raw_schema as RAW_SCHEMA,
            lnd_schema as LANDING_SCHEMA,
        )
    except Exception:
        CATALOG = "HMDM_DEV"
        UTIL_SCHEMA = f"{CATALOG}.util"
        RAW_SCHEMA = f"{CATALOG}.raw"
        LANDING_SCHEMA = f"{CATALOG}.landing"


try:
    from src.core.logging_utils import logger
except Exception:
    logger = logging.getLogger("Healthcare_MDM.Standardization")

    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s "
                   "%(name)s - %(message)s"
        )


try:
    from src.standardization.standardization_function import (
        function_mapping
    )
except Exception:
    function_mapping = {}


# ---------------------------------------------------------------------
# SPARK
# ---------------------------------------------------------------------

try:
    spark
except NameError:
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()

        if spark is None:
            spark = SparkSession.builder.getOrCreate()

    except Exception as exc:
        raise RuntimeError(
            f"Unable to obtain Spark session: {exc}"
        )


# ---------------------------------------------------------------------
# CONFIGURATION TABLES
# ---------------------------------------------------------------------

INGESTION_CONFIG_TBL = (
    f"{UTIL_SCHEMA}.ctl_entity_mstr"
)

STANDARDIZATION_CONFIG_TBL = (
    f"{UTIL_SCHEMA}.ctl_std_entity_mstr"
)

BATCH_LOG_TBL = (
    f"{UTIL_SCHEMA}.ctl_batch_log_tbl"
)

LOG_TBL = (
    f"{UTIL_SCHEMA}.ctl_log_tbl"
)


# ---------------------------------------------------------------------
# PIPELINE METADATA
# ---------------------------------------------------------------------

MODULE_NAME = "Standardization"

PIPELINE_START_TIME = None

RUN_ID = str(uuid.uuid4())

JOB_ID = "Healthcare_MDM_Standardization"

CLUSTER_ID = ""

RUN_URL = ""


# ---------------------------------------------------------------------
# EXCEPTION
# ---------------------------------------------------------------------

class GracefulExit(Exception):
    """
    Used when the pipeline has nothing to process.
    """
    pass


# ---------------------------------------------------------------------
# READ PARAMETERS
# ---------------------------------------------------------------------

def read_parameters(
    source_identifier: Optional[str] = None,
    source_system_name: Optional[str] = None,
    tbl_nm: Optional[str] = None
) -> Tuple[str, str, str]:
    """
    Read standardization parameters.

    Accept direct function arguments or command-line parameters.
    """

    # -------------------------------------------------------------
    # DIRECT FUNCTION ARGUMENTS
    # -------------------------------------------------------------

    if (
        source_identifier is not None
        and source_system_name is not None
        and tbl_nm is not None
    ):
        logger.info(
            "Parameters received directly: "
            "ID=%s, System=%s, Table=%s",
            source_identifier,
            source_system_name,
            tbl_nm
        )

        return (
            str(source_identifier),
            str(source_system_name),
            str(tbl_nm)
        )

    # -------------------------------------------------------------
    # COMMAND LINE ARGUMENTS
    # -------------------------------------------------------------

    args = sys.argv[1:]

    if len(args) >= 3:

        source_identifier = args[0]
        source_system_name = args[1]
        tbl_nm = args[2]

        logger.info(
            "Parameters received from sys.argv: "
            "ID=%s, System=%s, Table=%s",
            source_identifier,
            source_system_name,
            tbl_nm
        )

        return (
            source_identifier,
            source_system_name,
            tbl_nm
        )

    # -------------------------------------------------------------
    # FAIL CLEARLY
    # -------------------------------------------------------------

    raise ValueError(
        "Missing required arguments. "
        "Expected: "
        "source_identifier, source_system_name, table_name"
    )


# ---------------------------------------------------------------------
# BATCH STATUS CONDITION
# ---------------------------------------------------------------------

def get_batch_status_filter(
    module_name: str,
    source_system_name: str
) -> str:
    """
    Return pending batch condition.

    Standardization processes records where stdz_status is not Y.
    """

    module_name = module_name.lower()

    if module_name == "stdz":
        status_column = "stdz_status"

    elif module_name == "canonical":
        status_column = "canonical_status"

    elif module_name == "dq":
        status_column = "dq_status"

    elif module_name == "ingress":
        status_column = "ingress_status"

    elif module_name == "egress":
        status_column = "egress_status"

    else:
        raise ValueError(
            f"Unsupported module for batch status: {module_name}"
        )

    return (
        f"source_system_name = "
        f"'{source_system_name}' "
        f"AND COALESCE({status_column}, 'N') <> 'Y'"
    )


# ---------------------------------------------------------------------
# GET DELTA CONDITION
# ---------------------------------------------------------------------

def get_delta_condition(
    source_system_name: str
) -> str:
    """
    Get pending batch IDs for the source system.
    """

    condition = get_batch_status_filter(
        "stdz",
        source_system_name
    )

    query = f"""
        SELECT collect_list(batch_id) AS batch_ids
        FROM {BATCH_LOG_TBL}
        WHERE {condition}
    """

    logger.info(
        "Fetching pending batch IDs using condition: %s",
        condition
    )

    row = spark.sql(query).first()

    if row is None:
        raise GracefulExit(
            f"No pending batch found for "
            f"source_system_name={source_system_name}"
        )

    batch_ids = row["batch_ids"]

    if not batch_ids:
        raise GracefulExit(
            f"No pending batch found for "
            f"source_system_name={source_system_name}"
        )

    batch_ids = [
        int(batch_id)
        for batch_id in batch_ids
        if batch_id is not None
    ]

    if not batch_ids:
        raise GracefulExit(
            f"No valid batch IDs found for "
            f"source_system_name={source_system_name}"
        )

    condition = (
        "mdm_batch_id IN ("
        + ", ".join(str(x) for x in batch_ids)
        + ")"
    )

    logger.info(
        "Delta condition: %s",
        condition
    )

    return condition


# ---------------------------------------------------------------------
# LOAD INGESTION CONFIG
# ---------------------------------------------------------------------

def load_ingestion_configs(
    source_identifier: str
):
    """
    Load active ingestion configuration.
    """

    logger.info(
        "Loading ingestion configuration for %s",
        source_identifier
    )

    df = spark.table(INGESTION_CONFIG_TBL)

    config_df = df.filter(
        (
            F.col("source_identifier")
            == source_identifier
        )
        &
        (
            F.upper(
                F.col("source_active_flag").cast("string")
            ) == "TRUE"
        )
    )

    records = config_df.collect()

    if not records:
        raise GracefulExit(
            f"No active ingestion configuration found "
            f"for source_identifier={source_identifier}"
        )

    if len(records) > 1:
        logger.warning(
            "Multiple ingestion configuration records found "
            "for %s. Using first active record.",
            source_identifier
        )

    config = records[0]

    logger.info(
        "Ingestion configuration loaded: "
        "raw=%s.%s, landing=%s.%s",
        config["raw_table_schema"],
        config["raw_table_name"],
        config["std_table_schema"],
        config["std_table_name"]
    )

    return config


# ---------------------------------------------------------------------
# LOAD STANDARDIZATION CONFIG
# ---------------------------------------------------------------------

def load_standardization_configs(
    source_identifier: str
):
    """
    Load active standardization rules.

    Business rules are NOT created here.
    """

    logger.info(
        "Loading standardization rules for %s",
        source_identifier
    )

    df = spark.table(
        STANDARDIZATION_CONFIG_TBL
    )

    config_df = df.filter(
        (
            F.col("source_identifier")
            == source_identifier
        )
        &
        (
            F.upper(
                F.col("rule_status").cast("string")
            ) == "TRUE"
        )
    )

    rules = config_df.collect()

    logger.info(
        "Standardization rule count for %s: %s",
        source_identifier,
        len(rules)
    )

    return rules


# ---------------------------------------------------------------------
# PRIMARY KEY PARSER
# ---------------------------------------------------------------------

def _parse_primary_key(
    primary_key: Optional[str]
):
    """
    Parse configured primary-key string.

    Supports:
        individualEid

    and:
        col1 col2

    and:
        col1,col2
    """

    if primary_key is None:
        return []

    value = str(primary_key).strip()

    if not value:
        return []

    value = value.replace(",", " ")

    columns = [
        column.strip()
        for column in value.split()
        if column.strip()
    ]

    return columns


# ---------------------------------------------------------------------
# TABLE NAME HELPERS
# ---------------------------------------------------------------------

def _qualify_table(schema_name: str, table_name: str) -> str:
    """Return a catalog-qualified table name without duplicating the catalog."""
    schema_value = str(schema_name).strip()
    table_value = str(table_name).strip()

    if table_value.count(".") >= 2:
        return table_value
    if schema_value.count(".") >= 1:
        return f"{schema_value}.{table_value}"
    return f"{CATALOG}.{schema_value}.{table_value}"


def _sql_literal(value: object) -> str:
    """Escape a value for use in a Spark SQL string literal."""
    return str(value).replace("'", "''")


# ---------------------------------------------------------------------
# PREPARE TABLES
# ---------------------------------------------------------------------

def prepare_tables(
    ingestion_details,
    delta_condition: str,
    source_system_name: str
):
    """
    Read RAW records for pending batches and prepare
    the dataframe for standardization.
    """

    raw_schema = ingestion_details[
        "raw_table_schema"
    ]

    raw_table_name = ingestion_details[
        "raw_table_name"
    ]

    std_schema = ingestion_details[
        "std_table_schema"
    ]

    std_table_name = ingestion_details[
        "std_table_name"
    ]

    raw_table = _qualify_table(raw_schema, raw_table_name)
    std_table = _qualify_table(std_schema, std_table_name)

    logger.info(
        "RAW table: %s",
        raw_table
    )

    logger.info(
        "Landing table: %s",
        std_table
    )

    # -------------------------------------------------------------
    # READ RAW
    # -------------------------------------------------------------

    raw_df = spark.table(raw_table)

    logger.info(
        "RAW table count: %s",
        raw_df.count()
    )

    raw_df.createOrReplaceTempView(
        "raw_table_vw"
    )

    # -------------------------------------------------------------
    # ACTIVE RECORD SQL
    #
    # Original source calls this initial_load_sql.
    # Use the active-record expression configured for the source entity.
    # Prefer active_record_sql because that is the actual
    # project configuration column.
    # -------------------------------------------------------------

    active_record_sql = None

    if "active_record_sql" in ingestion_details:
        active_record_sql = (
            ingestion_details["active_record_sql"]
        )

    elif "initial_load_sql" in ingestion_details:
        active_record_sql = (
            ingestion_details["initial_load_sql"]
        )

    if (
        active_record_sql is not None
        and str(active_record_sql).strip()
        and str(active_record_sql).lower() != "none"
    ):
        logger.info(
            "Applying active record SQL."
        )

        active_record_df = spark.sql(
            active_record_sql
        )

        active_record_df.createOrReplaceTempView(
            "raw_table_vw"
        )

        logger.info(
            "RAW count after active record condition: %s",
            active_record_df.count()
        )

    # -------------------------------------------------------------
    # RAW COLUMNS
    # -------------------------------------------------------------

    raw_columns = set(raw_df.columns)

    primary_key = _parse_primary_key(
        ingestion_details["source_primary_key"]
    )

    # Keep only PK columns which actually exist.
    existing_primary_key = [
        column
        for column in primary_key
        if column in raw_columns
    ]

    if primary_key and not existing_primary_key:
        raise RuntimeError(
            "Configured source_primary_key columns "
            f"{primary_key} are not present in RAW table "
            f"{raw_table}. Available columns: "
            f"{raw_df.columns}"
        )

    # -------------------------------------------------------------
    # ORDER COLUMN
    #
    # Project source uses last_update_date.
    #
    # TEST RAW data contains LOAD_DATE instead.
    # LOAD_DATE is used only as a technical fallback when
    # last_update_date is absent.
    # -------------------------------------------------------------

    if "last_update_date" in raw_columns:
        order_column = "last_update_date"

    elif "LAST_UPDATE_DATE" in raw_columns:
        order_column = "LAST_UPDATE_DATE"

    elif "LOAD_DATE" in raw_columns:
        order_column = "LOAD_DATE"

    else:
        order_column = None

    # -------------------------------------------------------------
    # SOURCE FILTER
    # -------------------------------------------------------------

    source_filter = (
        f"Source_Name = '{_sql_literal(source_system_name)}'"
    )

    # -------------------------------------------------------------
    # DEDUPLICATION
    # -------------------------------------------------------------

    if existing_primary_key:

        partition_columns = ", ".join(
            existing_primary_key
        )

        if order_column:

            logger.info(
                "Deduplicating RAW records using "
                "primary key=%s and order column=%s",
                existing_primary_key,
                order_column
            )

            query = f"""
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY {partition_columns}
                            ORDER BY {order_column} DESC
                        ) AS rw_num
                    FROM raw_table_vw
                    WHERE {delta_condition}
                    AND {source_filter}
                )
                WHERE rw_num = 1
            """

            df = spark.sql(query).drop(
                "rw_num"
            )

        else:

            logger.warning(
                "No last_update_date or LOAD_DATE found. "
                "Using dropDuplicates on configured "
                "primary key."
            )

            df = spark.sql(
                f"""
                SELECT *
                FROM raw_table_vw
                WHERE {delta_condition}
                AND {source_filter}
                """
            )

            df = df.dropDuplicates(
                existing_primary_key
            )

    else:

        logger.info(
            "No source primary key configured. "
            "Reading records without deduplication."
        )

        df = spark.sql(
            f"""
            SELECT *
            FROM raw_table_vw
            WHERE {delta_condition}
            AND {source_filter}
            """
        )

    # -------------------------------------------------------------
    # NORMALIZE COLUMN NAMES
    # -------------------------------------------------------------

    df = df.toDF(
        *[
            column.replace(" ", "_")
            for column in df.columns
        ]
    )

    logger.info(
        "Final records to standardize: %s",
        df.count()
    )

    return (
        df,
        df.columns,
        raw_table,
        std_table,
        std_table_name
    )


# ---------------------------------------------------------------------
# EXECUTE STANDARDIZATION
# ---------------------------------------------------------------------

def execute_standardization(
    stdn_df: DataFrame,
    original_column_order,
    standardization_details,
    source_identifier: str
) -> DataFrame:
    """
    Apply configured standardization functions.

    No function is invented if it does not exist.
    """

    if not standardization_details:

        logger.info(
            "No active standardization rules found "
            "for %s. Passing data through.",
            source_identifier
        )

        return stdn_df

    for rule in standardization_details:

        rule_function = rule[
            "rule_function"
        ]

        column_name = rule[
            "column_name"
        ]

        logger.info(
            "Applying standardization rule: "
            "function=%s, column=%s",
            rule_function,
            column_name
        )

        # ---------------------------------------------------------
        # SOURCE-CODE BEHAVIOR
        # ---------------------------------------------------------

        if rule_function in (
            "rename_columns",
            "custom_transformation"
        ):
            logger.info(
                "Skipping framework-level rule: %s",
                rule_function
            )
            continue

        # ---------------------------------------------------------
        # FUNCTION MUST EXIST
        # ---------------------------------------------------------

        if rule_function not in function_mapping:

            available = sorted(
                function_mapping.keys()
            )

            raise RuntimeError(
                "Configured standardization function "
                f"'{rule_function}' is not available "
                f"for source_identifier={source_identifier}. "
                f"Available functions: {available}"
            )

        # ---------------------------------------------------------
        # APPLY FUNCTION
        # ---------------------------------------------------------

        try:

            stdn_df = function_mapping[
                rule_function
            ](
                stdn_df,
                column_name
            )

        except Exception as exc:

            raise RuntimeError(
                "Error applying standardization "
                f"function '{rule_function}' "
                f"to column '{column_name}' "
                f"for source_identifier="
                f"{source_identifier}: {exc}"
            ) from exc

        # ---------------------------------------------------------
        # PRESERVE ORIGINAL COLUMN ORDER
        # ---------------------------------------------------------

        stdn_df = stdn_df.select(
            *[
                column
                for column in original_column_order
                if column in stdn_df.columns
            ]
        )

    return stdn_df


# ---------------------------------------------------------------------
# WRITE LANDING
# ---------------------------------------------------------------------

def write_standardization_table(
    stdn_df: DataFrame,
    std_table: str,
    full_load_flag: bool = False,
):
    """
    Write standardized dataframe to Landing.

    Full loads replace the Landing target. Incremental loads append the
    Append the selected batch while retaining existing standardized data.
    """
    record_count = stdn_df.count()

    if record_count == 0:
        raise GracefulExit(f"No new data to load for {std_table}")

    logger.info(
        "Writing %s records into %s using %s mode",
        record_count,
        std_table,
        "overwrite" if full_load_flag else "append",
    )

    output_df = stdn_df.withColumn("LOAD_DATE", F.current_timestamp())
    load_mode = "overwrite" if full_load_flag else "append"

    # -------------------------------------------------------------
    # Delete existing rows for pending batches (idempotency)
    # -------------------------------------------------------------
    # Before appending, remove any existing rows for the same
    # batch_id values from the Landing table.  This prevents data
    # multiplication when standardization is re-run for the same
    # batch (e.g. after a failure and retry).
    # -------------------------------------------------------------

    if not full_load_flag and spark.catalog.tableExists(std_table):
        df_columns = {c.lower() for c in stdn_df.columns}
        if "batch_id" in df_columns:
            batch_ids = [
                row.batch_id
                for row in stdn_df.select("batch_id").distinct().collect()
            ]
            batch_id_list = ", ".join(
                str(int(b)) for b in batch_ids if b is not None
            )
            if batch_id_list:
                spark.sql(
                    f"DELETE FROM {std_table} "
                    f"WHERE batch_id IN ({batch_id_list})"
                )
                logger.info(
                    "Idempotency delete: removed existing rows "
                    "for batches %s from %s",
                    batch_id_list,
                    std_table,
                )

    if spark.catalog.tableExists(std_table) and not full_load_flag:
        target_schema = spark.table(std_table).schema
        source_columns = {c.upper(): c for c in output_df.columns}
        select_exprs = []

        for field in target_schema:
            target_col = field.name
            source_col = source_columns.get(target_col.upper())
            if source_col:
                select_exprs.append(
                    F.col(source_col).cast(field.dataType).alias(target_col)
                )
            else:
                select_exprs.append(
                    F.lit(None).cast(field.dataType).alias(target_col)
                )

        output_df = output_df.select(*select_exprs)

    (
        output_df.write
        .format("delta")
        .mode(load_mode)
        .option("path", get_s3_location(std_table))
        .saveAsTable(std_table)
    )

    logger.info("Data successfully loaded into %s", std_table)

    try:
        spark.sql(f"OPTIMIZE {std_table}")
        logger.info("OPTIMIZE completed for %s", std_table)
    except Exception as exc:
        logger.warning("OPTIMIZE failed for %s: %s", std_table, exc)


# ---------------------------------------------------------------------
# UPDATE BATCH STATUS
# ---------------------------------------------------------------------

def update_batch_standardization_status(
    source_system_name: str,
    status: str = "Y"
):
    """
    Update stdz_status for pending batches.
    """

    condition = get_batch_status_filter(
        "stdz",
        source_system_name
    )

    query = f"""
        UPDATE {BATCH_LOG_TBL}
        SET stdz_status = '{_sql_literal(status)}'
        WHERE {condition}
    """

    logger.info(
        "Updating standardization batch status: %s",
        status
    )

    spark.sql(query)

    logger.info(
        "Standardization batch status updated."
    )


# ---------------------------------------------------------------------
# WRITE PIPELINE LOG
# ---------------------------------------------------------------------

def write_log(
    source_identifier=None,
    source_system_name=None,
    run_status=None,
    error_description=None,
    module=None,
    sub_module=None,
    run_url=None,
    start_time=None,
    end_time=None,
    run_id=None,
    job_id=None,
    user_id=None,
    cluster_id=None,
):
    """
    Writes standardization pipeline execution details into ctl_log_tbl.

    Argument order is kept compatible with the existing standardization
    pipeline call while using an explicit Spark schema to avoid
    CANNOT_DETERMINE_TYPE errors.
    """

    try:
        from datetime import datetime
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            TimestampType,
            DoubleType,
        )

        print(f"INFO: Writing pipeline log: status={run_status}")

        # ---------------------------------------------------------
        # Default runtime values
        # ---------------------------------------------------------
        if run_id is None:
            run_id = "0000"

        if job_id is None:
            job_id = "0000"

        if user_id is None:
            user_id = ""

        if cluster_id is None:
            cluster_id = ""

        if run_url is None:
            run_url = ""

        if error_description is None:
            error_description = ""

        if start_time is None:
            start_time = datetime.now()

        if end_time is None:
            end_time = datetime.now()

        # ---------------------------------------------------------
        # Calculate execution duration
        # ---------------------------------------------------------
        try:
            time_elapsed = (
                end_time - start_time
            ).total_seconds()
        except Exception:
            time_elapsed = 0.0

        # ---------------------------------------------------------
        # Normalize values
        # ---------------------------------------------------------
        run_id = str(run_id)
        job_id = str(job_id)

        if source_identifier is not None:
            source_identifier = str(source_identifier)

        if source_system_name is not None:
            source_system_name = str(source_system_name)

        if run_status is not None:
            run_status = str(run_status)

        if error_description is not None:
            error_description = str(error_description)

        if module is not None:
            module = str(module)

        if sub_module is not None:
            sub_module = str(sub_module)

        run_url = str(run_url)
        user_id = str(user_id)
        cluster_id = str(cluster_id)

        # ---------------------------------------------------------
        # Explicit schema
        # ---------------------------------------------------------
        log_schema = StructType([
            StructField("run_id", StringType(), True),
            StructField("source_identifier", StringType(), True),
            StructField("source_system_name", StringType(), True),
            StructField("job_id", StringType(), True),
            StructField("module", StringType(), True),
            StructField("sub_module", StringType(), True),
            StructField("run_status", StringType(), True),
            StructField("error_description", StringType(), True),
            StructField("run_url", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("time_elapsed", DoubleType(), True),
            StructField("user_id", StringType(), True),
            StructField("cluster_id", StringType(), True),
        ])

        # ---------------------------------------------------------
        # Build log record
        # ---------------------------------------------------------
        log_data = [[
            run_id,
            source_identifier,
            source_system_name,
            job_id,
            module,
            sub_module,
            run_status,
            error_description,
            run_url,
            start_time,
            end_time,
            float(time_elapsed),
            user_id,
            cluster_id,
        ]]

        # ---------------------------------------------------------
        # Create DataFrame with explicit schema
        # ---------------------------------------------------------
        log_df = spark.createDataFrame(
            log_data,
            schema=log_schema
        )

        # ---------------------------------------------------------
        # Write to project logging table
        # ---------------------------------------------------------
        log_df.write \
            .format("delta") \
            .mode("append") \
            .option("path", get_s3_location(LOG_TBL)) \
            .saveAsTable(LOG_TBL)

        print(
            f"INFO: Pipeline log successfully written to {LOG_TBL}"
        )

    except Exception as e:
        print(
            f"WARNING: Unable to write pipeline log: {e}"
        )
# ---------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------

def main_pipeline(
    source_identifier: Optional[str] = None,
    source_system_name: Optional[str] = None,
    tbl_nm: Optional[str] = None,
    skip_batch_update: bool = False
):
    """
    Main RAW -> Landing standardization pipeline.

    IMPORTANT:
        Do NOT reset source_identifier/source_system_name here.

        They are passed directly into read_parameters().
    """

    global PIPELINE_START_TIME

    PIPELINE_START_TIME = datetime.now()

    logger.info(
        "Starting Healthcare_MDM Standardization"
    )

    try:

        # ---------------------------------------------------------
        # PARAMETERS
        # ---------------------------------------------------------

        (
            source_identifier,
            source_system_name,
            tbl_nm
        ) = read_parameters(
            source_identifier,
            source_system_name,
            tbl_nm
        )

        logger.info(
            "Parameters resolved successfully: "
            "ID=%s, System=%s, Table=%s",
            source_identifier,
            source_system_name,
            tbl_nm
        )

        # ---------------------------------------------------------
        # DELTA CONDITION
        # ---------------------------------------------------------

        delta_condition = get_delta_condition(
            source_system_name
        )

        # ---------------------------------------------------------
        # INGESTION CONFIG
        # ---------------------------------------------------------

        ingestion_details = (
            load_ingestion_configs(
                source_identifier
            )
        )

        # ---------------------------------------------------------
        # STANDARDIZATION CONFIG
        # ---------------------------------------------------------

        standardization_details = (
            load_standardization_configs(
                source_identifier
            )
        )

        # ---------------------------------------------------------
        # PREPARE RAW DATA
        # ---------------------------------------------------------

        (
            stdn_df,
            original_columns,
            raw_table,
            std_table,
            std_table_name
        ) = prepare_tables(
            ingestion_details,
            delta_condition,
            source_system_name
        )

        # ---------------------------------------------------------
        # APPLY STANDARDIZATION
        # ---------------------------------------------------------

        stdn_df = execute_standardization(
            stdn_df,
            original_columns,
            standardization_details,
            source_identifier
        )

        # ---------------------------------------------------------
        # WRITE LANDING
        # ---------------------------------------------------------

        raw_full_load_flag = ingestion_details["full_load_flag"]
        if isinstance(raw_full_load_flag, str):
            full_load_flag = raw_full_load_flag.strip().upper() in (
                "TRUE", "Y", "YES", "1"
            )
        else:
            full_load_flag = bool(raw_full_load_flag)

        write_standardization_table(
            stdn_df,
            std_table,
            full_load_flag=full_load_flag
        )

        # ---------------------------------------------------------
        # UPDATE BATCH STATUS
        # ---------------------------------------------------------

        if not skip_batch_update:
            update_batch_standardization_status(
                source_system_name,
                "Y"
            )
        else:
            logger.info(
                "Skipping batch status update (skip_batch_update=True)"
            )

        # ---------------------------------------------------------
        # LOG SUCCESS
        # ---------------------------------------------------------

        write_log(
            source_identifier,
            source_system_name,
            "Passed",
            ""
        )

        logger.info(
            "Healthcare_MDM Standardization completed successfully."
        )

        logger.info(
            "RAW table: %s",
            raw_table
        )

        logger.info(
            "Landing table: %s",
            std_table
        )

        logger.info(
            "Records processed: %s",
            stdn_df.count()
        )

        return stdn_df

    except GracefulExit as exc:

        logger.info(
            "Standardization exited gracefully: %s",
            exc
        )

        if (
            source_identifier is not None
            and source_system_name is not None
        ):

            write_log(
                source_identifier,
                source_system_name,
                "Passed",
                str(exc)
            )

        return None

    except Exception as exc:

        logger.exception(
            "Standardization failed"
        )

        if (
            source_identifier is not None
            and source_system_name is not None
        ):

            write_log(
                source_identifier,
                source_system_name,
                "Failed",
                str(exc)
            )

        raise


# ---------------------------------------------------------------------
# COMPATIBILITY WRAPPER
# ---------------------------------------------------------------------

def main_standardization_pipeline(
    source_identifier: Optional[str] = None,
    source_system_name: Optional[str] = None,
    tbl_nm: Optional[str] = None,
    skip_batch_update: bool = False
):
    """
    Compatibility function used by Databricks notebook calls.
    """

    return main_pipeline(
        source_identifier=source_identifier,
        source_system_name=source_system_name,
        tbl_nm=tbl_nm,
        skip_batch_update=skip_batch_update
    )


# ---------------------------------------------------------------------
# SCRIPT ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":

    try:

        main_pipeline()

    except GracefulExit as exc:

        logger.info(
            "Standardization completed with no work: %s",
            exc
        )

    except Exception:

        logger.exception(
            "Healthcare_MDM Standardization failed."
        )

        raise