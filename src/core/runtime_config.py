"""
Runtime configuration module for the Healthcare MDM pipeline.

This module is imported by every pipeline stage (ingestion, standardization,
canonical, data quality, MDM ingress/egress) and is responsible for:

  1. Detecting which environment the code is running in (dev / test / prod)
     by inspecting the Databricks workspace URL, and resolving the correct
     Unity Catalog name, S3 bucket, and alert-mail recipient group for that
     environment.
  2. Building the fully-qualified schema names for every layer of the
     medallion architecture (raw, landing, staging, mdm/publish, util) so
     that no other module has to hard-code a catalog or schema name.
  3. Building the fully-qualified names of the utility/control tables that
     drive the pipeline (batch control table, standardization/canonical/DQ
     rule master tables, execution log tables, mailing-list master table).
  4. Capturing the current Databricks job/run/cluster/task IDs so that log
     entries and email alerts can link back to the exact job run that
     produced them.
  5. Providing small helper functions used across the pipeline:
       - get_notebook_run_url(): builds a clickable link to the current job run.
       - get_batch_status_filter(): builds the SQL WHERE clause used to pick
         up only the batches that are eligible for a given pipeline stage,
         based on the upstream stage's completion status.
       - update_batch_log_tbl(): marks a batch as complete/failed for a
         given pipeline stage in the control table.
       - get_maillist(): looks up the alert email addresses and the list of
         monitored tables for a given source system.

Every value here is resolved once, at import time, so downstream modules
simply do ``from core.runtime_config import catalog, raw_schema, ...``
instead of re-deriving the environment on every call.
"""

from pyspark.sql import SparkSession
import os
import re

# ============================================================
# Global Runtime Variables
# ============================================================

catalog = None
omni_catalog = None
env = None
mail_recipient = None
s3_bucket = None
spark = None
dbutils = None

# Safe runtime defaults. Environment-specific values are resolved below.
DEFAULT_ALERT_EMAILS = []

email_config = {
    "smtp_server": os.getenv("HEALTHCARE_MDM_SMTP_SERVER", "smtp.gmail.com"),
    "smtp_user": os.getenv("HEALTHCARE_MDM_SMTP_USER", "naresh.mayari@gmail.com"),
    "smtp_port": int(os.getenv("HEALTHCARE_MDM_SMTP_PORT", "587")),
}

# SMTP password is stored in Databricks secrets -- never in env vars or code.
# Secret scope "healthcare_mdm"  key "smtp_password"
# To set it up:
#   databricks secrets create-scope healthcare_mdm
#   databricks secrets put-secret healthcare_mdm smtp_password --string-value <GMAIL_APP_PASSWORD>
# In code:  dbutils.secrets.get("healthcare_mdm", "smtp_password")

def _get_smtp_password():
    """Retrieve SMTP password from Databricks secrets."""
    try:
        from pyspark.dbutils import DBUtils
        _spark = SparkSession.builder.getOrCreate()
        _dbutils = DBUtils(_spark)
        return _dbutils.secrets.get("healthcare_mdm", "smtp_password")
    except Exception:
        return ""

archive_path = os.getenv("HEALTHCARE_MDM_ARCHIVE_PATH")


# ============================================================
# Environment Detection
# ============================================================

def detect_environment(spark_session: SparkSession):
    """
    Work out which environment (test / prod / dev) the current Databricks
    workspace belongs to, purely from the workspace URL, and return the
    environment-specific settings that the rest of the pipeline needs.

    Args:
        spark_session: The active SparkSession. When it is falsy (e.g. when
            running outside Databricks, such as in a local unit test), the
            function falls back to a synthetic "local_dev_workspace" URL so
            that it always resolves to the DEV environment instead of
            raising an error.

    Returns:
        A 4-tuple of:
          - local_catalog (str): the Unity Catalog name for this environment
            (e.g. "HMDM_DEV", "HMDM_TST", "HMDM_PROD").
          - local_env (str): short environment code ("dev", "tst", "prd").
          - local_mail_recipient (str): which recipient group in the mailing
            list master table should receive alerts for this environment.
          - local_s3_bucket (str): the S3 bucket used for archiving raw
            source files in this environment.

    Raises:
        ValueError: if the workspace URL does not match any known
            test/prod/dev naming pattern, since running with an unknown
            catalog/bucket would risk reading or writing the wrong
            environment's data.
    """
    local_catalog = None
    local_env = None
    local_mail_recipient = None
    local_s3_bucket = None
    env_matched = False

    try:
        if spark_session:
            base_url = spark_session.conf.get(
                "spark.databricks.workspaceUrl"
            ).lower()
        else:
            base_url = "local_dev_workspace"

        print(f"Workspace URL: {base_url}")

        # ----------------------------------------------------
        # 1. TEST
        # ----------------------------------------------------
        if base_url.startswith("https://tst-") or "tst" in base_url:
            local_catalog = "HMDM_TST"
            local_env = "tst"
            local_mail_recipient = "test"
            local_s3_bucket = (
                "tpc-aws-ted-tst-eda-kokoroih-eu-central-1"
            )
            env_matched = True

        # ----------------------------------------------------
        # 2. PROD
        # ----------------------------------------------------
        elif base_url.startswith("https://prd-") or "prd" in base_url:
            local_catalog = "HMDM_PROD"
            local_env = "prd"
            local_mail_recipient = "ops"
            local_s3_bucket = (
                "tpc-aws-ted-prd-eda-kokoroih-eu-central-1"
            )
            env_matched = True

        # ----------------------------------------------------
        # 3. DEV / LOCAL
        # ----------------------------------------------------
        elif (
            "dev" in base_url
            or base_url == "local_dev_workspace"
            or base_url.startswith("dbc-")
        ):
            local_catalog = "HMDM_DEV"
            local_env = "dev"
            local_mail_recipient = "dev"
            local_s3_bucket = "healthcare-master-data-management"
            env_matched = True

        if not env_matched:
            raise ValueError(
                f"Unknown Environment for workspace: {base_url}"
            )

        return (
            local_catalog,
            local_env,
            local_mail_recipient,
            local_s3_bucket,
        )

    except Exception as e:
        if "Unknown Environment" in str(e):
            raise e

        print(f"Error during environment detection: {e}")
        raise e


# ============================================================
# Initialize Spark + Environment
# ============================================================
# Detection strategy:
#   1. Try to use the existing active SparkSession (does NOT create a
#      new one) and call detect_environment() to resolve the correct
#      catalog/env/s3_bucket from the workspace URL.
#   2. If that fails (e.g. serverless edge cases, missing workspace URL,
#      import-time issues), fall back to DEV defaults so the pipeline
#      can still run in development without crashing.
# This ensures production/job clusters get the CORRECT catalog (HMDM_PROD,
# HMDM_TST) while serverless/dev falls back safely to HMDM_DEV.
# ============================================================

try:
    spark = SparkSession.builder.getOrCreate()

    (
        catalog,
        env,
        mail_recipient,
        s3_bucket,
    ) = detect_environment(spark)

    print(f"Catalog Name : {catalog}")
    print(f"Environment  : {env}")
    print(f"S3 Bucket    : {s3_bucket}")

except Exception as e:
    print(f"Environment detection failed, using DEV fallback: {e}")
    catalog = "hmdm_dev"
    env = "dev"
    mail_recipient = "dev"
    s3_bucket = "healthcare-master-data-management"
    spark = None

# Archive path for raw source file archiving (used by archive_file()
# in data_io.py, called from src_to_raw_ingestion.py).  Resolved here
# because it depends on s3_bucket which is only known after
# environment detection.
archive_path = os.getenv(
    "HEALTHCARE_MDM_ARCHIVE_PATH",
    f"s3://{s3_bucket}/healthcare-mdm/archieve/",
)
print(f"Archive Path : {archive_path}")


# ------------------------------------------------------------
# S3 Table Location Helper
# ------------------------------------------------------------
# All Databricks UC managed tables are stored in the project S3
# bucket with the path pattern: s3://{bucket}/{schema}/{table}
# Use get_s3_location() in every saveAsTable call to ensure
# tables go to the project bucket, not Databricks default storage.
# Example: df.write.option("path", get_s3_location(target_table)).saveAsTable(target_table)

def get_s3_location(full_table_name):
    """
    Return the S3 LOCATION for a Databricks UC table.

    Args:
        full_table_name: Qualified table name like 'hmdm_dev.landing.hcp_name'

    Returns:
        S3 path like 's3://healthcare-master-data-management/landing/hcp_name'
    """
    parts = str(full_table_name).split(".")
    if len(parts) >= 3:
        schema_name = parts[1]
        table_name = parts[2]
        return f"s3://{s3_bucket}/{schema_name}/{table_name}"
    return None


# ============================================================
# Schema Configuration
# ============================================================

try:
    # --------------------------------------------------------
    # Raw
    # --------------------------------------------------------
    raw_schema = f"{catalog}.raw"

    # --------------------------------------------------------
    # Landing
    # --------------------------------------------------------
    lnd_schema = f"{catalog}.landing"

    # --------------------------------------------------------
    # Canonical (cross-source code-list/vocabulary normalisation,
    # between Landing and Staging - see src/canonical/canonical.py)
    # --------------------------------------------------------
    can_schema = f"{catalog}.canonical"

    # --------------------------------------------------------
    # Staging
    # --------------------------------------------------------
    stg_schema = f"{catalog}.staging"

    # --------------------------------------------------------
    # Publish / MDM
    # --------------------------------------------------------
    publish_schema = f"{catalog}.mdm"

    # --------------------------------------------------------
    # Master (Egress/downstream-ready mastered data - see
    # src/mdm/mdm_egress.py)
    # --------------------------------------------------------
    master_schema = f"{catalog}.master"

    # --------------------------------------------------------
    # Utility / Control
    # --------------------------------------------------------
    util_schema = f"{catalog}.util"

    # Compatibility aliases used by project modules.
    CATALOG = catalog
    UTIL_SCHEMA = util_schema
    RAW_SCHEMA = raw_schema
    LANDING_SCHEMA = lnd_schema
    CANONICAL_SCHEMA = can_schema
    STAGING_SCHEMA = stg_schema
    PUBLISH_SCHEMA = publish_schema
    MASTER_SCHEMA = master_schema

    print(f"Raw Schema       : {raw_schema}")
    print(f"Landing Schema   : {lnd_schema}")
    print(f"Canonical Schema : {can_schema}")
    print(f"Staging Schema   : {stg_schema}")
    print(f"Publish Schema   : {publish_schema}")
    print(f"Master Schema    : {master_schema}")
    print(f"Util Schema      : {util_schema}")

except Exception as e:
    print(e)


# ============================================================
# Utility / Control Tables
# ============================================================

try:
    ingestion_config_tbl = f"{util_schema}.ctl_entity_mstr"

    standardization_config_tbl = (
        f"{util_schema}.ctl_std_entity_mstr"
    )

    canonical_config_tbl = (
        f"{util_schema}.ctl_can_mapg"
    )

    log_tbl_nm = f"{util_schema}.ctl_log_tbl"

    dqm_log_tbl = f"{util_schema}.ctl_dqm_log_tbl"

    dqm_reject_tbl = f"{util_schema}.dqm_reject_tbl"

    dqm_config_tbl = f"{util_schema}.ctl_dq_entity_mstr"

    batch_log_tbl = f"{util_schema}.ctl_batch_log_tbl"

    mail_master_tbl = (
        f"{util_schema}.ctl_mailing_list_mstr"
    )

    print(f"Batch Log Table : {batch_log_tbl}")

except Exception as e:
    print(e)


# ============================================================
# Runtime / Job Context IDs
# ============================================================

cluster_id = "0000"
job_id = "0000"
run_id = "0000"
task_id = "0000"

try:
    from pyspark.dbutils import DBUtils

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    try:
        ctx = (
            dbutils.notebook
            .entry_point
            .getDbutils()
            .notebook()
            .getContext()
        )

        cluster_id = ctx.clusterId().get()
        run_id = ctx.jobRunId().get()
        job_id = ctx.jobId().get()
        task_id = ctx.idInJob().get()

    except Exception:
        # Serverless / Spark Connect does not expose JVM-level notebook
        # context APIs.  Silently fall back to default IDs (no warning).
        cluster_id = "0000"
        job_id = "0000"
        run_id = "0000"
        task_id = "0000"

except Exception:
    print(
        "DBUtils not available. "
        "Using default runtime IDs."
    )


# ============================================================
# Databricks Notebook Run URL
# ============================================================

def get_notebook_run_url():
    """
    Construct the Databricks notebook run URL using
    the current workspace URL and runtime job context.
    """
    try:
        spark_session = SparkSession.builder.getOrCreate()

        base_url = spark_session.conf.get(
            "spark.databricks.workspaceUrl"
        )
        base_url = base_url.strip()
        if base_url.startswith("https://"):
            base_url = base_url[len("https://"): ]
        elif base_url.startswith("http://"):
            base_url = base_url[len("http://"): ]
        base_url = base_url.rstrip("/")

    except Exception:
        base_url = "unknown.databricks.com"

    run_url = (
        f"https://{base_url}/jobs/"
        f"{job_id}/runs/{run_id}?o={task_id}"
    )

    print(run_url)

    return run_url


# ============================================================
# Batch Status Filter
# ============================================================

def get_batch_status_filter(module, source_system_name):
    """
    Build the SQL WHERE-clause fragment that selects the batches which are
    ready to be picked up by a given pipeline stage.

    The pipeline is a strict sequence of stages
    (raw_ingestion -> stdz -> canonical -> dq -> ingress -> egress), and each
    stage should only process a batch once every stage before it has already
    completed successfully (status = 'Y') AND this stage itself has not
    already completed for that batch. This function encodes that
    dependency chain so every module asks the control table the same way.

    Args:
        module: Name of the pipeline stage requesting work, e.g.
            "rawingestion", "stdz", "canonical", "dq", "ingress", "egress"
            (case-insensitive).
        source_system_name: The source system to filter on (e.g. "IQVIA").

    Returns:
        str: A SQL boolean expression suitable for use in a WHERE clause
        against the batch control table.

    Raises:
        ValueError: if an unrecognised module name is passed in, since
            silently returning no filter could cause a stage to
            accidentally reprocess every batch.
    """
    if (
        module.lower() == "rawingestion"
        or module.lower() == "raw_ingestion"
    ):
        return (
            f"source_system_name = '{source_system_name}' "
            f"and coalesce(raw_ingestion_status,'N')!='Y'"
        )

    elif module.lower() == "stdz":
        return (
            f"source_system_name = '{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and coalesce(stdz_status,'N')!='Y'"
        )

    elif module.lower() == "canonical":
        return (
            f"source_system_name = '{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and stdz_status='Y' "
            f"and coalesce(canonical_status,'N')!='Y'"
        )

    elif module.lower() == "dq":
        return (
            f"source_system_name = '{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and stdz_status='Y' "
            f"and canonical_status='Y' "
            f"and coalesce(dq_status,'N')!='Y'"
        )

    elif module.lower() == "ingress":
        return (
            f"source_system_name = '{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and stdz_status='Y' "
            f"and canonical_status='Y' "
            f"and dq_status='Y' "
            f"and coalesce(ingress_status,'N')!='Y'"
        )

    elif module.lower() == "egress":
        return (
            f"source_system_name = '{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and stdz_status='Y' "
            f"and canonical_status='Y' "
            f"and dq_status='Y' "
            f"and ingress_status='Y' "
            f"and coalesce(egress_status,'N')!='Y'"
        )

    else:
        raise ValueError(
            f"Unsupported batch-status module: {module}"
        )


# ============================================================
# Update Batch Log
# ============================================================

def update_batch_log_tbl(column, value, batch_id=None, source=None):
    """
    Update a batch-status column in the control table.

    Project callers use module names such as:
      raw_ingestion, stdz, canonical, dq, ingress, egress

    The physical control-table columns are:
      raw_ingestion_status, stdz_status, canonical_status,
      dq_status, ingress_status, egress_status

    batch_id may be either a numeric ID or an existing SQL condition such as
    "batch_id IN (20260906060913)".
    """
    try:
        source = "" if source is None else str(source).replace("'", "''")
        value = "" if value is None else str(value).replace("'", "''")

        column_map = {
            "rawingestion": "raw_ingestion_status",
            "raw_ingestion": "raw_ingestion_status",
            "stdz": "stdz_status",
            "canonical": "canonical_status",
            "dq": "dq_status",
            "ingress": "ingress_status",
            "egress": "egress_status",
        }

        column_key = str(column).strip().lower()
        column_status = column_map.get(column_key, column)

        if batch_id is None or batch_id == "":
            where_clause = f"source_system_name = '{source}'"

        elif isinstance(batch_id, str):
            batch_id_text = batch_id.strip()

            if batch_id_text.lower().startswith("batch_id "):
                where_clause = (
                    f"({batch_id_text}) "
                    f"AND source_system_name = '{source}'"
                )
            else:
                normalized_batch_id = int(batch_id_text)
                where_clause = (
                    f"batch_id = {normalized_batch_id} "
                    f"AND source_system_name = '{source}'"
                )

        else:
            normalized_batch_id = int(batch_id)
            where_clause = (
                f"batch_id = {normalized_batch_id} "
                f"AND source_system_name = '{source}'"
            )

        spark.sql(
            f"""
            UPDATE {batch_log_tbl}
            SET {column_status} = '{value}'
            WHERE {where_clause}
            """
        )

    except Exception as e:
        print(e)

        raise RuntimeError(
            f"Error while updating {batch_log_tbl} "
            f"for column {column}"
        ) from e

# ============================================================
# Mailing List
# ============================================================

def get_maillist(source_name):
    """
    Look up the alert-email distribution list and the list of tables that
    should be actively monitored for a given source system, for the current
    environment's recipient group (dev / test / ops).

    Args:
        source_name: The source system name to look up (e.g. "IQVIA").

    Returns:
        tuple[list[str], list[str]]:
          - The list of email addresses to notify (falls back to
            DEFAULT_ALERT_EMAILS if no active row is found or the source
            system is not configured).
          - The list of table names configured for monitoring for this
            source system (empty list if none configured).
    """
    email_ids_df = spark.sql(
        f"""
        SELECT
            email_id,
            table_list
        FROM {mail_master_tbl}
        WHERE source_system_name = '{source_name}'
          AND active_flag = true
          AND recepient_type = '{mail_recipient}'
        """
    )

    row = email_ids_df.first()

    if not row:
        return DEFAULT_ALERT_EMAILS, []

    emails = (
        [e.strip() for e in row.email_id.split(",")]
        if row.email_id
        else []
    )

    tables = (
        [t.strip() for t in row.table_list.split(",")]
        if row.table_list
        else []
    )

    return emails, tables


# ============================================================
# S3 Timestamp Checkpoint Functions
# ============================================================
# Implements the batch control checkpoint mechanism documented in
# Start_file.docx: execution timestamps are written to S3 as
# checkpoint files, so the next pipeline run can determine the
# incremental window by reading the previous execution's end
# timestamp.
# ============================================================

from datetime import datetime as _datetime


def _get_dbutils_for_s3():
    """Get dbutils for S3 file operations."""
    try:
        from pyspark.dbutils import DBUtils
        _spark = SparkSession.builder.getOrCreate()
        return DBUtils(_spark)
    except Exception:
        return None


def _s3_timestamp_path(source_system_name):
    """Build the S3 path for a source system's timestamp file."""
    return (
        f"s3://{s3_bucket}/healthcare-mdm/batch_control/"
        f"timestamps/{source_system_name.lower()}_timestamp.txt"
    )


def write_s3_timestamp(source_system_name, start_timestamp=None, end_timestamp=None):
    """
    Write the pipeline execution timestamps to S3 as a checkpoint
    file.

    Called after a pipeline run completes so the next run can read
    the previous execution window and process only changed records.

    Args:
        source_system_name: The source system (e.g. "IQVIA_API").
        start_timestamp: Batch start timestamp.  If None, uses
            "1900-01-01 00:00:00" (full-load sentinel -- every record
            satisfies > 1900).
        end_timestamp: Batch end timestamp.  If None, uses current
            timestamp.

    Returns:
        str: The S3 path where the timestamp file was written, or
        None on failure.
    """
    if start_timestamp is None:
        start_timestamp = "1900-01-01 00:00:00"
    if end_timestamp is None:
        end_timestamp = _datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    _dbutils = _get_dbutils_for_s3()
    if _dbutils is None:
        print("WARNING: dbutils not available. S3 timestamp not written.")
        return None

    file_path = _s3_timestamp_path(source_system_name)
    source_upper = source_system_name.upper().replace("-", "_").replace(" ", "_")
    content = (
        f"{source_upper}_START_TIMESTAMP\n{start_timestamp}\n"
        f"{source_upper}_END_TIMESTAMP\n{end_timestamp}\n"
    )

    try:
        _dbutils.fs.put(file_path, content, overwrite=True)
        print(f"S3 timestamp checkpoint written: {file_path}")
        print(f"  Start: {start_timestamp}")
        print(f"  End:   {end_timestamp}")
        return file_path
    except Exception as e:
        print(f"WARNING: Failed to write S3 timestamp: {e}")
        return None


def get_s3_timestamp(source_system_name):
    """
    Read the previous pipeline execution timestamps from S3.

    Called at the start of a pipeline run to determine the incremental
    window: only records modified after the previous end_timestamp
    should be processed.

    Args:
        source_system_name: The source system (e.g. "IQVIA_API").

    Returns:
        tuple: (start_timestamp, end_timestamp) as strings.
        If no checkpoint file exists, returns
        ("1900-01-01 00:00:00", None) which forces a full load.
    """
    _dbutils = _get_dbutils_for_s3()
    if _dbutils is None:
        print("WARNING: dbutils not available. Returning default timestamp.")
        return "1900-01-01 00:00:00", None

    file_path = _s3_timestamp_path(source_system_name)

    try:
        content = _dbutils.fs.head(file_path)
        lines = content.strip().split("\n")

        start_ts = "1900-01-01 00:00:00"
        end_ts = None

        for i, line in enumerate(lines):
            if "START_TIMESTAMP" in line and i + 1 < len(lines):
                start_ts = lines[i + 1].strip()
            elif "END_TIMESTAMP" in line and i + 1 < len(lines):
                end_ts = lines[i + 1].strip()

        print(f"S3 timestamp checkpoint read: {file_path}")
        print(f"  Start: {start_ts}")
        print(f"  End:   {end_ts}")
        return start_ts, end_ts
    except Exception:
        print(
            f"No S3 timestamp checkpoint found for "
            f"{source_system_name}. Using full load (1900)."
        )
        return "1900-01-01 00:00:00", None


def determine_source_delta_ids(
    spark_session,
    source_dict,
    start_timestamp,
    end_timestamp,
):
    """
    Identify records that have changed between two execution
    timestamps.

    Instead of processing the entire source table, only records whose
    timestamp column falls between the previous and current execution
    windows are selected for processing.

    Args:
        spark_session: The active SparkSession.
        source_dict: A dictionary mapping table names to a
            (primary_key_column, timestamp_column) tuple.
            Example:
                {
                    "hmdm_dev.raw.hcp_api_data": ("iqvia_id", "load_date"),
                    "hmdm_dev.raw.hco_api_data": ("iqvia_id", "load_date"),
                }
        start_timestamp: The previous execution's end timestamp.
        end_timestamp: The current execution's end timestamp.

    Returns:
        list: Unique primary key values that changed in the window.
        Returns an empty list if no changes are found.
    """
    if not source_dict:
        print("WARNING: source_dict is empty. No delta IDs to determine.")
        return []

    all_delta_ids = []

    for table_name, (pk_column, ts_column) in source_dict.items():
        try:
            query = f"""
                SELECT DISTINCT {pk_column} AS delta_id
                FROM {table_name}
                WHERE {ts_column} > '{start_timestamp}'
                  AND {ts_column} <= '{end_timestamp}'
            """

            rows = spark_session.sql(query).collect()
            table_ids = [
                row["delta_id"] for row in rows
                if row["delta_id"] is not None
            ]

            print(f"  {table_name}: {len(table_ids)} changed records")
            all_delta_ids.extend(table_ids)

        except Exception as e:
            print(f"  WARNING: Error querying {table_name}: {e}")

    unique_delta_ids = list(set(all_delta_ids))

    print(
        f"Total delta IDs: {len(unique_delta_ids)} "
        f"(from {len(source_dict)} tables)"
    )
    return unique_delta_ids
