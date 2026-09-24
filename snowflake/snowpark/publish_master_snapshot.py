"""
Healthcare MDM - Snowpark publish job.

Runs entirely inside Snowflake (via Snowpark) to snapshot the current
HMDM_DEV.MDM.master_hcp / HMDM_DEV.MDM.master_hco tables into date-stamped,
immutable snapshot tables for downstream consumers who need a stable
"as of" view rather than reading the continuously-updated master tables
directly.

This complements (does not replace) the dbt models in dbt/models/marts/,
which build the master tables themselves - this job runs after dbt and
only makes read-only snapshot copies.

Credential passing:
    This script reads Snowflake connection parameters from environment
    variables (see CONNECTION_PARAMETERS_ENV_VARS). When called from
    09_Run_Full_Pipeline.py (Stage 9), the parent Python notebook fetches
    credentials from AWS Secrets Manager and passes them explicitly via
    the env= parameter of subprocess.run(). This is necessary because
    credentials exported by run_dbt.sh (Stage 8) inside its own shell
    process do NOT propagate back to the parent Python process.

Usage:
    python snowpark/publish_master_snapshot.py --entity HCP
    python snowpark/publish_master_snapshot.py --entity HCO
"""

import argparse
import datetime as dt

from snowflake.snowpark import Session

CONNECTION_PARAMETERS_ENV_VARS = {
    "account": "SNOWFLAKE_ACCOUNT",
    "user": "SNOWFLAKE_USER",
    "password": "SNOWFLAKE_PASSWORD",
    "role": "SNOWFLAKE_ROLE",
    "warehouse": "SNOWFLAKE_WAREHOUSE",
    "database": "SNOWFLAKE_DATABASE",
    "schema": "SNOWFLAKE_SCHEMA",
}


def build_session() -> Session:
    """
    Build a Snowpark Session from environment variables (see
    CONNECTION_PARAMETERS_ENV_VARS), so no credentials are hard-coded in
    this file.

    Returns:
        Session: an active Snowpark session.

    Raises:
        KeyError: if a required environment variable is not set.
    """
    import os

    params = {
        key: os.environ[env_var]
        for key, env_var in CONNECTION_PARAMETERS_ENV_VARS.items()
        if env_var in os.environ
    }
    return Session.builder.configs(params).create()


def publish_snapshot(session: Session, entity: str) -> str:
    """
    Copy HMDM_DEV.MDM.master_<entity> into a new, immutable
    HMDM_DEV.MASTER.<entity>_SNAPSHOT_<YYYYMMDD> table.

    The source table is created by dbt (master_hcp / master_hco) in the
    MDM schema. The snapshot is stored in the MASTER schema.

    Args:
        session: Active Snowpark session.
        entity: "HCP" or "HCO".

    Returns:
        str: the fully-qualified name of the snapshot table created.

    Raises:
        ValueError: if entity is not "HCP" or "HCO".
    """
    if entity not in ("HCP", "HCO"):
        raise ValueError(f"entity must be 'HCP' or 'HCO', got {entity!r}")

    # dbt marts write to HMDM_DEV.MDM schema (see dbt_project.yml +schema: mdm).
    # The model names are master_hcp / master_hco (lowercase).
    source_table = f"HMDM_DEV.MDM.master_{entity.lower()}"
    snapshot_date = dt.date.today().strftime("%Y%m%d")
    snapshot_table = f"HMDM_DEV.MASTER.{entity}_SNAPSHOT_{snapshot_date}"

    df = session.table(source_table)
    df.write.mode("overwrite").save_as_table(snapshot_table)

    return snapshot_table


def main() -> None:
    """CLI entry point: parse --entity and publish that entity's snapshot."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entity", choices=["HCP", "HCO"], required=True)
    args = parser.parse_args()

    session = build_session()
    try:
        snapshot_table = publish_snapshot(session, args.entity)
        print(f"Published snapshot: {snapshot_table}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
