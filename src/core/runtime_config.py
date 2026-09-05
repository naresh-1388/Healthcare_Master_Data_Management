from pyspark.sql import SparkSession
import os


# ============================================================
# Global Runtime Variables
# ============================================================

catalog = None
omni_catalog = None
env = None
mail_recipient = None
s3_bucket = None

# Safe runtime defaults. Environment-specific values are resolved below.
DEFAULT_ALERT_EMAILS = []
email_config = {
    "smtp_server": os.getenv("HEALTHCARE_MDM_SMTP_SERVER", ""),
    "smtp_user": os.getenv("HEALTHCARE_MDM_SMTP_USER", ""),
}
archive_path = None

spark = None
dbutils = None


# ============================================================
# Environment Detection
# ============================================================

def detect_environment(spark_session: SparkSession):

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

            local_catalog = "detst_eda"
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

            local_catalog = "deprd_eda"
            local_env = "prd"
            local_mail_recipient = "ops"
            local_s3_bucket = (
                "tpc-aws-ted-prd-eda-kokoroih-eu-central-1"
            )

            env_matched = True

        # ----------------------------------------------------
        # 3. DEV / LOCAL
        #
        # Actual project environment:
        # Catalog : healthcare_mdm_dev
        # S3      : healthcare-master-data-management
        #
        # dbc- detection is required for the current
        # Databricks workspace.
        # ----------------------------------------------------
        elif (
            "dev" in base_url
            or base_url == "local_dev_workspace"
            or base_url.startswith("dbc-")
        ):

            local_catalog = "healthcare_mdm_dev"
            local_env = "dev"
            local_mail_recipient = "dev"
            local_s3_bucket = "healthcare-master-data-management"

            env_matched = True

        # ----------------------------------------------------
        # Unknown Environment
        # ----------------------------------------------------
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

except Exception:
    pass


# ============================================================
# Schema Configuration
#
# IMPORTANT:
# All utility/control/config table names are derived from
# util_schema.
#
# Individual Python files should NOT hardcode these table names.
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
    # Staging
    # --------------------------------------------------------
    stg_schema = f"{catalog}.staging"

    # --------------------------------------------------------
    # Publish / MDM
    # --------------------------------------------------------
    publish_schema = f"{catalog}.mdm"

    # --------------------------------------------------------
    # Utility / Control
    #
    # NEW PROJECT STANDARD:
    # healthcare_mdm_dev.util
    # --------------------------------------------------------
    util_schema = f"{catalog}.util"

    # Compatibility aliases used by the standardization module.
    CATALOG = catalog
    UTIL_SCHEMA = util_schema
    RAW_SCHEMA = raw_schema
    LANDING_SCHEMA = lnd_schema
    STAGING_SCHEMA = stg_schema
    PUBLISH_SCHEMA = publish_schema

    # Default archive root is configuration-driven; no source path is invented.
    archive_path = os.getenv("HEALTHCARE_MDM_ARCHIVE_PATH")

    print(f"Raw Schema     : {raw_schema}")
    print(f"Landing Schema : {lnd_schema}")
    print(f"Staging Schema : {stg_schema}")
    print(f"Publish Schema : {publish_schema}")
    print(f"Util Schema    : {util_schema}")

except Exception as e:
    print(e)


# ============================================================
# Utility / Control Tables
#
# These are centrally derived from util_schema.
# ============================================================

try:

    # Ingestion configuration
    ingestion_config_tbl = (
        f"{util_schema}.ctl_entity_mstr"
    )

    # Standardization configuration
    standardization_config_tbl = (
        f"{util_schema}.ctl_std_entity_mstr"
    )

    # Canonical mapping configuration
    canonical_config_tbl = (
        f"{util_schema}.ctl_can_mapg"
    )

    # General execution log
    log_tbl_nm = (
        f"{util_schema}.ctl_log_tbl"
    )

    # DQ execution log
    dqm_log_tbl = (
        f"{util_schema}.ctl_dqm_log_tbl"
    )

    # DQ rejected records
    dqm_reject_tbl = (
        f"{util_schema}.dqm_reject_tbl"
    )

    # DQ rule configuration
    dqm_config_tbl = (
        f"{util_schema}.ctl_dq_entity_mstr"
    )

    # Batch control log
    batch_log_tbl = (
        f"{util_schema}.ctl_batch_log_tbl"
    )

    # Mailing list configuration
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

    except Exception as inner_err:

        print(
            "Warning: Failed to extract job context. "
            "Using default runtime IDs."
        )

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
    Constructs the Databricks notebook run URL using
    the current workspace URL and runtime job context.
    """

    try:

        spark_session = (
            SparkSession.builder.getOrCreate()
        )

        base_url = spark_session.conf.get(
            "spark.databricks.workspaceUrl"
        )

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

def get_batch_status_filter(
    module,
    source_system_name
):

    if (
        module.lower() == "rawingestion"
        or module.lower() == "raw_ingestion"
    ):

        return (
            f"source_system_name = "
            f"'{source_system_name}' "
            f"and coalesce(raw_ingestion_status,'N')!='Y'"
        )

    elif module.lower() == "stdz":

        return (
            f"source_system_name = "
            f"'{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and coalesce(stdz_status,'N')!='Y'"
        )

    elif module.lower() == "canonical":

        return (
            f"source_system_name = "
            f"'{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and stdz_status='Y' "
            f"and coalesce(canonical_status,'N')!='Y'"
        )

    elif module.lower() == "dq":

        return (
            f"source_system_name = "
            f"'{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and stdz_status='Y' "
            f"and canonical_status='Y' "
            f"and coalesce(dq_status,'N')!='Y'"
        )

    elif module.lower() == "ingress":

        return (
            f"source_system_name = "
            f"'{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and stdz_status='Y' "
            f"and canonical_status='Y' "
            f"and dq_status='Y' "
            f"and coalesce(ingress_status,'N')!='Y'"
        )

    elif module.lower() == "egress":

        return (
            f"source_system_name = "
            f"'{source_system_name}' "
            f"and raw_ingestion_status='Y' "
            f"and stdz_status='Y' "
            f"and canonical_status='Y' "
            f"and dq_status='Y' "
            f"and ingress_status='Y' "
            f"and coalesce(egress_status,'N')!='Y'"
        )

    else:

        return None


# ============================================================
# Update Batch Log
# ============================================================

def update_batch_log_tbl(
    column,
    value,
    batch_id,
    source
):

    try:

        if batch_id is None or batch_id == "":

            batch_id_filter = get_batch_status_filter(
                column,
                source
            )

            column_status = column + "_status"

            spark.sql(
                f"""
                UPDATE {batch_log_tbl}
                SET {column_status} = '{value}'
                WHERE {batch_id_filter}
                """
            )

        else:

            column_status = column + "_status"

            spark.sql(
                f"""
                UPDATE {batch_log_tbl}
                SET {column_status} = '{value}'
                WHERE batch_id = {batch_id}
                  AND source_system_name = '{source}'
                """
            )

    except Exception as e:

        print(e)

        final_column = (
            column + "_status"
            if batch_id is None or batch_id == ""
            else column
        )

        raise RuntimeError(
            f"Error while updating "
            f"{batch_log_tbl} "
            f"for column {final_column}"
        ) from e


# ============================================================
# Mailing List
# ============================================================

def get_maillist(source_name):

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