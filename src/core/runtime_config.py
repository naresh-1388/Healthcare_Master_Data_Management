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
    "smtp_server": os.getenv("HEALTHCARE_MDM_SMTP_SERVER", ""),
    "smtp_user": os.getenv("HEALTHCARE_MDM_SMTP_USER", ""),
}

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
# SERVERLESS WORKAROUND:
#   Creating a new SparkSession here causes catalog context loss.
#   For now, hardcode catalog values for DEV environment.
#   In production (Job clusters), this would use detect_environment().
# ============================================================

try:
    # Check if we're on Serverless (via environment variable or compute ID)
    import os
    compute_id = os.getenv("DATABRICKS_RUNTIME_VERSION", "")
    
    # For Serverless, hardcode catalog to avoid SparkSession creation
    # This preserves the catalog set in the calling notebook
    is_serverless = True  # TODO: Detect properly in production
    
    if is_serverless:
        # SERVERLESS: Hardcoded values (no new SparkSession!)
        catalog = "hmdm_dev"  # Lowercase to match Unity Catalog
        env = "dev"
        mail_recipient = "dev"
        s3_bucket = "healthcare-master-data-management"
        spark = None  # Will be passed by caller
        
        print("[SERVERLESS MODE] Using hardcoded catalog configuration")
        print(f"Catalog Name : {catalog}")
        print(f"Environment  : {env}")
        print(f"S3 Bucket    : {s3_bucket}")
    else:
        # PRODUCTION: Dynamic detection with new SparkSession
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
    print(f"Environment initialization skipped: {e}")
    # Fallback to DEV defaults
    catalog = "hmdm_dev"  # Lowercase to match Unity Catalog
    env = "dev"
    mail_recipient = "dev"
    s3_bucket = "healthcare-master-data-management"
    spark = None


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
