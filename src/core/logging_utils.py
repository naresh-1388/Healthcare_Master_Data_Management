"""
Healthcare MDM - Centralized Logging Utilities

Purpose:
    Provides centralized application logging and Databricks
    execution logging for the Healthcare MDM pipeline.

Responsibilities:
    - Initialize application logger
    - Retrieve Databricks dbutils
    - Retrieve active SparkSession
    - Create centralized log table
    - Insert pipeline execution log records
"""

from datetime import datetime
import logging

from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Project configuration
# ---------------------------------------------------------------------------

try:
    from .runtime_config import spark, log_tbl_nm

except ImportError:
    try:
        from runtime_config import spark, log_tbl_nm

    except ImportError:
        # Script-mode fallback when this file is executed outside the package.
        from core.runtime_config import spark, log_tbl_nm


# ---------------------------------------------------------------------------
# Logger initialization
# ---------------------------------------------------------------------------

LOGGER_NAME = "databricks_logger"

logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.INFO)

if not logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(levelname)s: %(message)s"
    )

    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


# ---------------------------------------------------------------------------
# Databricks utilities
# ---------------------------------------------------------------------------

def get_dbutils():
    """
    Retrieve the active Databricks dbutils object.

    Returns:
        dbutils object when available, otherwise None.
    """

    if "spark" in globals() and spark is not None:
        if hasattr(spark, "dbutils"):
            return spark.dbutils

    try:
        import IPython

        ipython = IPython.get_ipython()

        if ipython is not None:
            return ipython.user_ns.get("dbutils")

    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark_session():
    """
    Safely retrieve the active SparkSession.

    Returns:
        SparkSession when available, otherwise None.
    """

    try:
        return SparkSession.builder.getOrCreate()

    except Exception as exc:
        logger.error(
            f"Unable to retrieve SparkSession: {exc}"
        )
        return None


# ---------------------------------------------------------------------------
# Log table creation
# ---------------------------------------------------------------------------

def create_log_tables(log_table_name):
    """
    Create the centralized pipeline log table if it does not exist.

    The schema follows the existing project logging implementation.
    """

    try:
        spark_session = get_spark_session()

        if spark_session is None:
            raise RuntimeError(
                "SparkSession is not available."
            )

        spark_session.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {log_table_name} (
                run_id STRING,
                source_identifier STRING,
                source_system_name STRING,
                job_id BIGINT,
                module STRING,
                sub_module STRING,
                run_status STRING,
                error_description STRING,
                run_url STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                time_elapsed FLOAT,
                user_id STRING,
                cluster_id STRING
            )
            """
        )

        logger.info(
            f"Log table is available: {log_table_name}"
        )

    except Exception as exc:
        logger.error(
            f"Failed to create log table: {exc}"
        )


# ---------------------------------------------------------------------------
# Event logging
# ---------------------------------------------------------------------------

def log_event_detail(
    sub_module,
    run_status,
    err_msg,
    run_url,
    source_identifier,
    source_system_name,
    job_id,
    module_name,
    staging_start_time,
    cluster_id,
    run_id,
):
    """
    Insert one pipeline execution event into the centralized log table.

    Parameters:
        sub_module:
            Pipeline sub-module or processing step.

        run_status:
            Status of the processing step.

        err_msg:
            Error or informational message.

        run_url:
            Databricks notebook/job run URL.

        source_identifier:
            Source configuration identifier.

        source_system_name:
            Source system name.

        job_id:
            Databricks job ID.

        module_name:
            Pipeline module name.

        staging_start_time:
            Start timestamp used for elapsed-time calculation.

        cluster_id:
            Databricks cluster ID.

        run_id:
            Databricks run ID.
    """

    try:
        dbutils_local = get_dbutils()
        spark_session = get_spark_session()

        if spark_session is None:
            raise RuntimeError(
                "SparkSession is not available."
            )

        # ---------------------------------------------------------------
        # Retrieve executing user
        # ---------------------------------------------------------------

        user_id = ""

        if dbutils_local is not None:
            try:
                user_id = (
                    dbutils_local
                    .notebook
                    .entry_point
                    .getDbutils()
                    .notebook()
                    .getContext()
                    .userName()
                    .get()
                )

            except Exception:
                user_id = ""

        # ---------------------------------------------------------------
        # Execution timing
        # ---------------------------------------------------------------

        error_message = repr(err_msg)

        end_time = datetime.now()

        elapsed_time = (
            end_time - staging_start_time
        ).total_seconds()

        # ---------------------------------------------------------------
        # Insert execution log
        # ---------------------------------------------------------------

        spark_session.sql(
            f"""
            INSERT INTO {log_tbl_nm}
            VALUES (
                {run_id},
                '{source_identifier}',
                '{source_system_name}',
                {job_id},
                '{module_name}',
                '{sub_module}',
                '{run_status}',
                {error_message},
                '{run_url}',
                '{staging_start_time}',
                '{end_time}',
                {elapsed_time},
                '{user_id}',
                '{cluster_id}'
            )
            """
        )

        logger.info(
            "Log record inserted successfully."
        )

    except Exception as exc:
        logger.error(
            f"Failed to insert log record: {exc}"
        )

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# Logging destination schema/table names come from the centralized project
# configuration. If environment-specific names change, update the central
# configuration rather than scattering table names through individual jobs.
# ============================================================================

