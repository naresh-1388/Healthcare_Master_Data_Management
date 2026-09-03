"""
Healthcare MDM - Runtime Configuration

Central configuration for:
    - Databricks environment detection
    - Unity Catalog/schema names
    - Control table names
    - Databricks runtime identifiers
    - Batch status conditions
    - Notification configuration
    - Databricks run URL

Source of truth:
    Existing project common_variables.py
"""

from __future__ import annotations

import os
from typing import Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


# ---------------------------------------------------------------------------
# Runtime defaults
# ---------------------------------------------------------------------------

catalog: Optional[str] = None
omni_catalog: Optional[str] = None
env: Optional[str] = None
mail_recipient: Optional[str] = None
s3_bucket: Optional[str] = None

spark: Optional[SparkSession] = None
dbutils = None


# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------

def detect_environment(
    spark_session: Optional[SparkSession],
) -> Tuple[str, str, str, str]:
    """
    Detect project environment from Databricks workspace URL.

    Returns:
        Tuple containing:
            catalog
            environment
            mail recipient type
            S3 bucket
    """

    if spark_session is not None:
        base_url = (
            spark_session.conf
            .get("spark.databricks.workspaceUrl")
            .lower()
        )
    else:
        base_url = "local_dev_workspace"

    if (
        base_url.startswith("https://tst-")
        or "tst" in base_url
    ):
        return (
            "detst_eda",
            "tst",
            "test",
            "tpc-aws-ted-tst-eda-kokoroih-eu-central-1",
        )

    if (
        base_url.startswith("https://prd-")
        or "prd" in base_url
    ):
        return (
            "deprd_eda",
            "prd",
            "ops",
            "tpc-aws-ted-prd-eda-kokoroih-eu-central-1",
        )

    if (
        "dev" in base_url
        or base_url == "local_dev_workspace"
    ):
        return (
            "dedev_eda",
            "dev",
            "dev",
            "tpc-aws-ted-dev-eda-kokoroih-eu-central-1",
        )

    raise ValueError(
        f"Unknown Databricks environment: {base_url}"
    )


# ---------------------------------------------------------------------------
# Spark initialization
# ---------------------------------------------------------------------------

def get_spark_session() -> SparkSession:
    """
    Retrieve the existing SparkSession or create one.
    """

    return SparkSession.builder.getOrCreate()


try:
    spark = get_spark_session()

    (
        catalog,
        env,
        mail_recipient,
        s3_bucket,
    ) = detect_environment(spark)

except Exception:
    # Preserve the original project's ability to import this module
    # when Spark/Databricks context is unavailable.
    spark = None


# ---------------------------------------------------------------------------
# Schema configuration
# ---------------------------------------------------------------------------

if catalog:
    raw_schema = f"{catalog}.eda_de_kokoro_mdm_raw"
    stg_schema = f"{catalog}.eda_de_kokoro_mdm_lake"
    publish_schema = f"{catalog}.eda_de_kokoro_mdm_hub"
    util_schema = f"{catalog}.eda_de_kokoro_mdm_util"

else:
    raw_schema = None
    stg_schema = None
    publish_schema = None
    util_schema = None


# ---------------------------------------------------------------------------
# Control tables
# ---------------------------------------------------------------------------

if util_schema:

    ingestion_config_tbl = (
        f"{util_schema}.ctl_entity_mstr"
    )

    standardization_config_tbl = (
        f"{util_schema}.ctl_std_entity_mstr"
    )

    canonical_config_tbl = (
        f"{util_schema}.ctl_can_mapg"
    )

    log_tbl_nm = (
        f"{util_schema}.ctl_log_tbl"
    )

    dqm_log_tbl = (
        f"{util_schema}.ctl_dqm_log_tbl"
    )

    dqm_reject_tbl = (
        f"{util_schema}.dqm_reject_tbl"
    )

    dqm_config_tbl = (
        f"{util_schema}.ctl_dq_entity_mstr"
    )

    batch_log_tbl = (
        f"{util_schema}.ctl_batch_log_tbl"
    )

    mail_master_tbl = (
        f"{util_schema}.ctl_mailing_list_mstr"
    )

else:

    ingestion_config_tbl = None
    standardization_config_tbl = None
    canonical_config_tbl = None
    log_tbl_nm = None
    dqm_log_tbl = None
    dqm_reject_tbl = None
    dqm_config_tbl = None
    batch_log_tbl = None
    mail_master_tbl = None


# ---------------------------------------------------------------------------
# Runtime identifiers
# ---------------------------------------------------------------------------

cluster_id = "0000"
job_id = "0000"
run_id = "0000"
task_id = "0000"


def _initialize_runtime_context() -> None:
    """
    Populate Databricks runtime identifiers when DBUtils is available.
    """

    global dbutils
    global cluster_id
    global job_id
    global run_id
    global task_id

    try:
        from pyspark.dbutils import DBUtils

        spark_session = get_spark_session()
        dbutils = DBUtils(spark_session)

        context = (
            dbutils.notebook
            .entry_point
            .getDbutils()
            .notebook()
            .getContext()
        )

        cluster_id = context.clusterId().get()
        run_id = context.jobRunId().get()
        job_id = context.jobId().get()
        task_id = context.idInJob().get()

    except Exception:
        cluster_id = "0000"
        job_id = "0000"
        run_id = "0000"
        task_id = "0000"


_initialize_runtime_context()


# ---------------------------------------------------------------------------
# Archive / email configuration
# ---------------------------------------------------------------------------

if s3_bucket:
    archive_path = (
        f"s3://{s3_bucket}"
    )
else:
    archive_path = None


email_config = {
    "smtp_server": os.getenv(
        "SMTP_SERVER",
        "",
    ),
    "smtp_user": os.getenv(
        "SMTP_USER",
        "",
    ),
}


# The original common_variables.py references DEFAULT_ALERT_EMAILS
# but does not define its concrete value.
#
# Therefore no project-specific email address is invented here.
#
# Optional environment variable:
#     DEFAULT_ALERT_EMAILS="a@company.com,b@company.com"
#
default_alert_emails_raw = os.getenv(
    "DEFAULT_ALERT_EMAILS",
    "",
)

DEFAULT_ALERT_EMAILS = [
    email.strip()
    for email in default_alert_emails_raw.split(",")
    if email.strip()
]


# ---------------------------------------------------------------------------
# Databricks run URL
# ---------------------------------------------------------------------------

def get_notebook_run_url() -> str:
    """
    Construct the Databricks job/run URL.
    """

    try:
        spark_session = get_spark_session()

        base_url = spark_session.conf.get(
            "spark.databricks.workspaceUrl"
        )

    except Exception:
        base_url = "unknown.databricks.com"

    return (
        f"https://{base_url}"
        f"/jobs/{job_id}"
        f"/runs/{run_id}"
        f"?o={task_id}"
    )


# ---------------------------------------------------------------------------
# Batch status conditions
# ---------------------------------------------------------------------------

def get_batch_status_filter(
    module: str,
    source_system_name: str,
) -> Optional[str]:
    """
    Return the SQL condition used to select pending batches
    for each pipeline module.
    """

    module_name = module.lower()

    if module_name in {
        "rawingestion",
        "raw_ingestion",
    }:
        return (
            "source_system_name = "
            f"'{source_system_name}' "
            "and coalesce(raw_ingestion_status,'N')!='Y'"
        )

    if module_name == "stdz":
        return (
            "source_system_name = "
            f"'{source_system_name}' "
            "and raw_ingestion_status='Y' "
            "and coalesce(stdz_status,'N')!='Y'"
        )

    if module_name == "canonical":
        return (
            "source_system_name = "
            f"'{source_system_name}' "
            "and raw_ingestion_status='Y' "
            "and stdz_status='Y' "
            "and coalesce(canonical_status,'N')!='Y'"
        )

    if module_name == "dq":
        return (
            "source_system_name = "
            f"'{source_system_name}' "
            "and raw_ingestion_status='Y' "
            "and stdz_status='Y' "
            "and canonical_status='Y' "
            "and coalesce(dq_status,'N')!='Y'"
        )

    if module_name == "ingress":
        return (
            "source_system_name = "
            f"'{source_system_name}' "
            "and raw_ingestion_status='Y' "
            "and stdz_status='Y' "
            "and canonical_status='Y' "
            "and dq_status='Y' "
            "and coalesce(ingress_status,'N')!='Y'"
        )

    if module_name == "egress":
        return (
            "source_system_name = "
            f"'{source_system_name}' "
            "and raw_ingestion_status='Y' "
            "and stdz_status='Y' "
            "and canonical_status='Y' "
            "and dq_status='Y' "
            "and ingress_status='Y' "
            "and coalesce(egress_status,'N')!='Y'"
        )

    return None


# ---------------------------------------------------------------------------
# Batch status update
# ---------------------------------------------------------------------------

def update_batch_log_tbl(
    column: str,
    value: str,
    batch_id,
    source: str,
) -> None:
    """
    Update the status column in the batch log table.

    Existing project behavior is retained:
        - No batch_id -> update using module status condition.
        - batch_id supplied -> use it as the SQL WHERE expression.

    Note:
        Existing callers pass SQL expressions such as:
            batch_id IN (...)
        rather than always passing a single numeric ID.
    """

    if spark is None:
        raise RuntimeError(
            "SparkSession is not available."
        )

    if not batch_log_tbl:
        raise RuntimeError(
            "Batch log table is not configured."
        )

    column_status = (
        f"{column}_status"
    )

    if batch_id is None or batch_id == "":

        batch_id_filter = get_batch_status_filter(
            column,
            source,
        )

        if not batch_id_filter:
            raise ValueError(
                f"Unsupported batch module: {column}"
            )

        where_clause = batch_id_filter

    else:

        where_clause = (
            f"{batch_id} "
            f"AND source_system_name = '{source}'"
        )

    try:

        spark.sql(
            f"""
            UPDATE {batch_log_tbl}
            SET {column_status} = '{value}'
            WHERE {where_clause}
            """
        )

    except Exception as exc:

        raise RuntimeError(
            f"Error while updating {batch_log_tbl} "
            f"for column {column_status}"
        ) from exc


# ---------------------------------------------------------------------------
# Mailing list
# ---------------------------------------------------------------------------

def get_maillist(source_name: str):
    """
    Retrieve active notification recipients and table list
    for a source system.
    """

    if spark is None:
        return DEFAULT_ALERT_EMAILS, []

    if not mail_master_tbl:
        return DEFAULT_ALERT_EMAILS, []

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
        [
            email.strip()
            for email in row.email_id.split(",")
        ]
        if row.email_id
        else []
    )

    tables = (
        [
            table.strip()
            for table in row.table_list.split(",")
        ]
        if row.table_list
        else []
    )

    return emails, tables

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# Update environment-specific catalog/schema values here ONLY if they differ
# from the supplied project configuration. Do not hardcode credentials here.
# AWS bucket/region values are also environment configuration and should match
# the supplied project environment variables/configuration.
# ============================================================================

