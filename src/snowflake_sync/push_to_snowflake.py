"""
Snowflake Sync Module — bridges Databricks Delta tables to Snowflake.

This module reads Databricks Unity Catalog tables (staging, master)
and writes them to Snowflake, so that dbt models and Snowpark scripts
can operate on real data instead of empty table structures.

Architecture:
    Databricks (HMDM_DEV.staging.*, HMDM_DEV.master.*)
        ↓ snowflake-connector-python
    Snowflake  (HMDM_DEV.STAGING.*, HMDM_DEV.MASTER.*)
        ↓ dbt models build marts
    Snowflake  (HMDM_DEV.MDM.*, HMDM_DEV.MASTER.* marts)
        ↓ Snowpark snapshot
    Snowflake  (snapshot tables for downstream consumers)

Credentials:
    Snowflake connection parameters are read from AWS Secrets Manager
    using the same pattern as 01_Connection_Test notebook:
      - Databricks service credential: 'healthcare_mdm_secrets_credential'
      - AWS Secrets Manager secret: 'healthcare-mdm/dev/api-snowflake'
      - Keys: snowflake_user, snowflake_password, snowflake_account,
              snowflake_warehouse, snowflake_database, snowflake_schema

    No credentials are hardcoded in this file.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

try:
    import snowflake.connector
except ImportError:
    snowflake = None

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None


# ============================================================
# Table Mapping: Databricks → Snowflake
# ============================================================
# Each entry maps a Databricks table to its Snowflake equivalent.
# Databricks uses lowercase snake_case; Snowflake uses UPPERCASE.
# ============================================================

STAGING_TABLE_MAP: Dict[str, str] = {
    "hcp_name": "HCP_NAME",
    "hcp_address": "HCP_ADDRESS",
    "hcp_specialty": "HCP_SPECIALTY",
    "hcp_identification": "HCP_IDENTIFICATION",
    "hcp_phone": "HCP_PHONE",
    "hcp_email": "HCP_EMAIL",
    "hcp_alternate_name": "HCP_ALTERNATE_NAME",
    "hcp_education": "HCP_EDUCATION",
    "hcp_tendencies": "HCP_TENDENCIES",
    "hcp_origin_university": "HCP_ORIGIN_UNIVERSITY",
    "hcp_tax": "HCP_TAX",
    "hcp_language": "HCP_LANGUAGE",
    "hcp_hco_affiliation": "HCP_HCO_AFFILIATION",
    "hco_name": "HCO_NAME",
    "hco_address": "HCO_ADDRESS",
    "hco_alternate_name": "HCO_ALTERNATE_NAME",
    "hco_identification": "HCO_IDENTIFICATION",
    "hco_specialty": "HCO_SPECIALTY",
    "hco_phone": "HCO_PHONE",
    "hco_email": "HCO_EMAIL",
    "hco_tax": "HCO_TAX",
    "hco_hco_hierarchy": "HCO_HIERARCHY",
}

MASTER_TABLE_MAP: Dict[str, str] = {
    "hcp_specialty": "HCP_SPECIALTY",
    "hcp_alternate_name": "HCP_ALTERNATE_NAME",
    "hcp_education": "HCP_EDUCATION",
    "hcp_identification": "HCP_IDENTIFICATION",
    "hco": "HCO",
    "hco_name": "HCO_NAME",
    "hco_alternate_identifier": "HCO_ALTERNATE_IDENTIFIER",
    "hco_phone": "HCO_PHONE",
    "hco_specialty": "HCO_SPECIALTY",
}


# ============================================================
# Snowflake Connection
# ============================================================

def get_snowflake_credentials() -> Optional[Dict[str, str]]:
    """
    Retrieve Snowflake connection parameters from AWS Secrets Manager.

    Uses the same pattern as 01_Connection_Test notebook:
      - Databricks service credential 'healthcare_mdm_secrets_credential'
      - AWS Secrets Manager secret 'healthcare-mdm/dev/api-snowflake'
      - Keys: snowflake_user, snowflake_password, snowflake_account,
              snowflake_warehouse, snowflake_database, snowflake_schema

    Returns:
        Dict with keys: account, user, password, warehouse, database,
        schema.  Or None if credentials cannot be retrieved.
    """
    try:
        import boto3

        from pyspark.dbutils import DBUtils

        _spark = SparkSession.builder.getOrCreate()
        _dbutils = DBUtils(_spark)

        # Connect to AWS Secrets Manager using Databricks service credential
        boto3_session = boto3.Session(
            botocore_session=_dbutils.credentials.getServiceCredentialsProvider(
                "healthcare_mdm_secrets_credential"
            ),
            region_name="us-east-1",
        )
        sm = boto3_session.client("secretsmanager")

        # Retrieve the secret
        SECRET_NAME = "healthcare-mdm/dev/api-snowflake"
        response = sm.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(response["SecretString"])

        account = secret["snowflake_account"].replace(".snowflakecomputing.com", "")
        user = secret["snowflake_user"]
        password = secret["snowflake_password"]
        warehouse = secret["snowflake_warehouse"]
        database = secret["snowflake_database"]
        schema = secret["snowflake_schema"]

        creds = {
            "account": account,
            "user": user,
            "password": password,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
        }

        print("Snowflake credentials: loaded from AWS Secrets Manager")
        return creds

    except Exception as e:
        print(f"Snowflake credentials: FAILED — {e}")
        print("  Check AWS Secrets Manager secret 'healthcare-mdm/dev/api-snowflake'")
        print("  and Databricks service credential 'healthcare_mdm_secrets_credential'.")
        return None


def create_snowflake_connection(creds: Dict[str, str]):
    """
    Create a Snowflake connection using snowflake-connector-python.
    """
    if snowflake is None:
        raise ImportError(
            "snowflake-connector-python is not installed. "
            "Run: %pip install snowflake-connector-python"
        )

    conn = snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        role=creds.get("role", ""),
        warehouse=creds.get("warehouse", ""),
        database=creds.get("database", "HMDM_DEV"),
        schema=creds.get("schema", "PUBLIC"),
    )
    print(f"Connected to Snowflake: {creds['account']}")
    return conn


# ============================================================
# Table Sync Logic
# ============================================================

def _get_spark():
    """Get the active SparkSession."""
    return SparkSession.builder.getOrCreate()


def _table_exists_in_databricks(full_table_name: str) -> bool:
    """Check if a table exists in Databricks Unity Catalog."""
    try:
        _spark = _get_spark()
        return _spark.catalog.tableExists(full_table_name)
    except Exception:
        return False


def _get_databricks_table_count(full_table_name: str) -> int:
    """Get row count of a Databricks table."""
    try:
        _spark = _get_spark()
        return _spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]
    except Exception:
        return 0


def _convert_dict_columns_to_json(pdf):
    """
    Convert dict/map columns to JSON strings for Snowflake compatibility.
    Snowflake does not support Python dict type in write_pandas,
    so we convert any column whose values are dicts to JSON strings.
    """
    # pdf is a pandas DataFrame (from df.toPandas()), not a Spark DataFrame.
    # Compute column list once; iterate over a static list, not an iterator.
    col_list: list = list(pdf.columns)  # noqa: SCPAP001
    for col in col_list:
        sample = pdf[col].dropna()
        if len(sample) > 0 and isinstance(sample.iloc[0], dict):
            pdf[col] = pdf[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            print(f"    [type-fix] {col}: dict \u2192 JSON string")
    return pdf


def _sync_one_table(
    conn,
    dbx_table: str,
    sf_schema: str,
    sf_table: str,
    sf_database: str = "HMDM_DEV",
) -> Dict[str, int]:
    """
    Sync a single Databricks table to Snowflake using TRUNCATE-AND-LOAD.

    This is the standard production ETL pattern:
      1. Read Databricks table as pandas DataFrame
      2. Convert dict/map/struct/array columns to strings (Snowflake
         cannot bind Python dict types via write_pandas)
      3. DELETE all existing rows from the Snowflake table
         (TRUNCATE is faster but cannot be rolled back; DELETE is safer)
      4. INSERT the fresh data using write_pandas in append mode
         (overwrite=False — we already cleared existing rows)

    Why TRUNCATE-AND-LOAD instead of overwrite=True or append:
      - overwrite=True drops the entire table and recreates it. This
        loses Snowflake grants, row-access policies, tags, and column
        comments. If the INSERT fails after the DROP, you have no table.
      - append (overwrite=False without DELETE) causes duplicate rows
        on every re-run.
      - DELETE + INSERT is idempotent, preserves schema/grants, and
        is safe if the INSERT fails (table still exists, just empty).

    The table is auto-created on first run if it does not exist.

    Returns:
        Dict with keys: source_rows, deleted_rows, inserted_rows
    """
    from snowflake.connector.pandas_tools import write_pandas

    _spark = _get_spark()

    # Read Databricks table
    df = _spark.sql(f"SELECT * FROM {dbx_table}")
    row_count = df.count()

    if row_count == 0:
        print(f"  {dbx_table} → {sf_schema}.{sf_table}: 0 rows (skip)")
        return {"source_rows": 0, "deleted_rows": 0, "inserted_rows": 0}

    # Cast complex types (map, struct, array) to string for safe pandas
    # conversion on Serverless (Spark Connect). Simple types stay as-is.
    from pyspark.sql.types import MapType, StructType, ArrayType

    for field in df.schema.fields:
        if isinstance(field.dataType, (MapType, StructType, ArrayType)):
            df = df.withColumn(field.name, df[field.name].cast("string"))
            print(f"    [type-cast] {field.name}: {field.dataType.simpleString()} → string")

    # Collect data as pandas DataFrame for write_pandas bulk insert.
    #
    # Two strategies based on row count:
    #   - Small data (<10k rows): df.collect() + row.asDict() — avoids
    #     Spark Connect PlanMetrics serialization bug in toPandas().
    #   - Large data (>=10k rows): df.toPandas() — faster, but may hit
    #     the PlanMetrics bug on Serverless. If it fails, falls back
    #     to collect() in chunks.
    import pandas as _pd

    SMALL_BATCH_THRESHOLD = 10_000

    if row_count < SMALL_BATCH_THRESHOLD:
        # Small batch: collect() is safe and avoids PlanMetrics bug
        spark_rows = df.collect()
        data_list = [row.asDict() for row in spark_rows]
        pdf = _pd.DataFrame(data_list)
    else:
        # Large batch: try toPandas() first (faster), fall back to collect()
        try:
            pdf = df.toPandas()
            print(f"    [large-batch] Used toPandas() for {row_count} rows")
        except Exception as toPandas_err:
            print(f"    [large-batch] toPandas() failed ({toPandas_err}), falling back to collect()")
            spark_rows = df.collect()
            data_list = [row.asDict() for row in spark_rows]
            pdf = _pd.DataFrame(data_list)

    # Build fully qualified Snowflake table name
    sf_full = f"{sf_database}.{sf_schema}.{sf_table}"

    # Transaction safety: disable autocommit so DELETE + INSERT run
    # inside a single transaction. If INSERT fails after DELETE,
    # ROLLBACK restores the old rows — the table is not left empty.
    # (Snowflake defaults to autocommit=True, which would commit
    # the DELETE immediately, before INSERT is attempted.)
    conn.autocommit(False)
    cur = conn.cursor()
    deleted_rows = 0

    try:
        # Step 1: DELETE existing rows from Snowflake table.
        # This is the TRUNCATE-AND-LOAD pattern — clear old data first,
        # then insert fresh data. If the table does not exist yet,
        # auto_create_table=True in write_pandas will create it.
        try:
            cur.execute(f"DELETE FROM {sf_full}")
            deleted_rows = cur.rowcount if cur.rowcount is not None else 0
        except Exception as del_err:
            # Table does not exist yet — write_pandas will auto-create it
            if "does not exist" in str(del_err).lower() or "object does not exist" in str(del_err).lower():
                pass  # Expected on first run
            else:
                raise

        # Step 2: INSERT fresh data using write_pandas in append mode.
        # overwrite=False because we already deleted existing rows.
        # auto_create_table=True creates the table on first run.
        success, nchunks, nrows, _ = write_pandas(
            conn,
            pdf,
            sf_table,
            schema=sf_schema,
            database=sf_database,
            overwrite=False,
            auto_create_table=True,
        )

        # Both DELETE and INSERT succeeded — commit the transaction.
        conn.commit()

        print(
            f"  {dbx_table} → {sf_full}: "
            f"{row_count} rows (deleted={deleted_rows}, inserted={nrows})"
        )
        return {
            "source_rows": row_count,
            "deleted_rows": deleted_rows,
            "inserted_rows": nrows,
        }

    except Exception:
        # Either DELETE or INSERT failed — rollback restores old data.
        # The table keeps its previous contents instead of being left empty.
        conn.rollback()
        raise

    finally:
        cur.close()
        # Restore autocommit to True (Snowflake default) for other operations.
        conn.autocommit(True)


def sync_staging_to_snowflake(
    catalog: str = "HMDM_DEV",
    sf_database: str = "HMDM_DEV",
    sf_schema: str = "STAGING",
) -> Dict[str, Dict]:
    """
    Sync all Databricks staging tables to Snowflake STAGING schema.

    This is the bridge that fills Snowflake's empty STAGING tables
    so dbt models can read real data.

    Args:
        catalog: Databricks catalog name.
        sf_database: Snowflake database name.
        sf_schema: Snowflake schema name (STAGING).

    Returns:
        Dict mapping table name → sync result.
    """
    creds = get_snowflake_credentials()
    if creds is None:
        print("ERROR: Cannot sync — Snowflake credentials not configured.")
        return {}

    conn = create_snowflake_connection(creds)
    results = {}

    print(f"\n{'=' * 60}")
    print(f"SNOWFLAKE SYNC: {catalog}.staging → {sf_database}.{sf_schema}")
    print(f"{'=' * 60}")

    for dbx_table_name, sf_table_name in STAGING_TABLE_MAP.items():
        dbx_full = f"{catalog}.staging.{dbx_table_name}"

        if not _table_exists_in_databricks(dbx_full):
            print(f"  SKIP: {dbx_full} does not exist in Databricks")
            continue

        try:
            result = _sync_one_table(
                conn, dbx_full, sf_schema, sf_table_name, sf_database=sf_database
            )
            results[dbx_table_name] = result
        except Exception as e:
            print(f"  ERROR: {dbx_full} → {sf_schema}.{sf_table_name}: {e}")
            results[dbx_table_name] = {"error": str(e)}

    conn.close()
    return results


def sync_master_to_snowflake(
    catalog: str = "HMDM_DEV",
    sf_database: str = "HMDM_DEV",
    sf_schema: str = "MASTER",
) -> Dict[str, Dict]:
    """
    Sync all Databricks master tables to Snowflake MASTER schema.

    This fills Snowflake's MASTER tables so the Snowpark snapshot
    script can create date-stamped snapshots.

    Args:
        catalog: Databricks catalog name.
        sf_database: Snowflake database name.
        sf_schema: Snowflake schema name (MASTER).

    Returns:
        Dict mapping table name → sync result.
    """
    creds = get_snowflake_credentials()
    if creds is None:
        print("ERROR: Cannot sync — Snowflake credentials not configured.")
        return {}

    conn = create_snowflake_connection(creds)
    results = {}

    print(f"\n{'=' * 60}")
    print(f"SNOWFLAKE SYNC: {catalog}.master → {sf_database}.{sf_schema}")
    print(f"{'=' * 60}")

    for dbx_table_name, sf_table_name in MASTER_TABLE_MAP.items():
        dbx_full = f"{catalog}.master.{dbx_table_name}"

        if not _table_exists_in_databricks(dbx_full):
            print(f"  SKIP: {dbx_full} does not exist in Databricks")
            continue

        try:
            result = _sync_one_table(
                conn, dbx_full, sf_schema, sf_table_name, sf_database=sf_database
            )
            results[dbx_table_name] = result
        except Exception as e:
            print(f"  ERROR: {dbx_full} → {sf_schema}.{sf_table_name}: {e}")
            results[dbx_table_name] = {"error": str(e)}

    conn.close()
    return results


def run_snowflake_sync(
    catalog: str = "HMDM_DEV",
    sync_staging: bool = True,
    sync_master: bool = True,
) -> Dict[str, Dict]:
    """
    Main entry point — sync Databricks tables to Snowflake.

    Called by notebook 10_Snowflake_Sync after Egress (Stage 6)
    completes, so Snowflake has fresh data for dbt and Snowpark.

    Args:
        catalog: Databricks catalog name.
        sync_staging: If True, sync staging tables.
        sync_master: If True, sync master tables.

    Returns:
        Combined results dict for all synced tables.
    """
    all_results = {}
    start_time = datetime.now()

    print(f"\n{'=' * 60}")
    print(f"SNOWFLAKE SYNC STARTED at {start_time}")
    print(f"{'=' * 60}")

    if sync_staging:
        staging_results = sync_staging_to_snowflake(catalog=catalog)
        all_results["staging"] = staging_results

    if sync_master:
        master_results = sync_master_to_snowflake(catalog=catalog)
        all_results["master"] = master_results

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Summary
    total_synced = 0
    total_errors = 0
    total_rows = 0

    for layer, tables in all_results.items():
        for table_name, result in tables.items():
            if "error" in result:
                total_errors += 1
            else:
                total_synced += 1
                total_rows += result.get("source_rows", 0)

    print(f"\n{'=' * 60}")
    print(f"SNOWFLAKE SYNC COMPLETE")
    print(f"  Tables synced: {total_synced}")
    print(f"  Total rows:    {total_rows}")
    print(f"  Errors:        {total_errors}")
    print(f"  Duration:      {duration:.1f}s")
    print(f"{'=' * 60}")

    return all_results