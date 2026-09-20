"""
Data I/O utilities for Healthcare MDM.

Source:
- Existing common_functions.py
- Purpose: provide shared data-access helpers for the project pipelines.
"""

from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, Optional

import json
import os
import smtplib
import requests

from pyspark.sql import Row

try:
    from delta.tables import DeltaTable
except ImportError:
    class DeltaTable:
        """
        Minimal stand-in for delta.tables.DeltaTable used only when the
        delta-spark package is not installed (e.g. local unit tests
        outside a Databricks/Delta runtime). isDeltaTable() always
        reports False in that case, which safely routes read_data_dynamic
        into the plain file-format detection path instead of assuming
        Delta.
        """

        @staticmethod
        def isDeltaTable(spark, path):
            """Always False outside a real Delta Lake runtime."""
            return False

try:
    from .runtime_config import spark
except ImportError:
    from runtime_config import spark


class DBFSFile:
    """Lightweight representation of a DBFS file."""

    def __init__(self, name, path):
        """
        Args:
            name: The file's base name (as returned by dbutils.fs.ls).
            path: The file's full DBFS path.
        """
        self.name = name
        self.path = path


def get_dbutils():
    """
    Locate and return the Databricks ``dbutils`` object, regardless of
    whether the code is running as a notebook (where ``dbutils`` is
    injected into the IPython user namespace) or as a plain Python module
    that only has access to the SparkSession.

    Returns:
        The dbutils object if it can be found, otherwise None (callers
        must handle the None case, e.g. when running in a local/unit-test
        environment that has no Databricks runtime).
    """
    # First choice: dbutils attached directly to the SparkSession.
    if "spark" in globals() and hasattr(spark, "dbutils"):
        return spark.dbutils

    # Fallback: pull dbutils out of the notebook's IPython namespace.
    # This is needed because dbutils is a notebook-injected global and is
    # not always reachable via the SparkSession object itself.
    try:
        import IPython

        ipython = IPython.get_ipython()
        if ipython is not None:
            return ipython.user_ns.get("dbutils")
    except Exception:
        pass

    return None


def file_exists(path: str) -> bool:
    """
    Check whether a given DBFS/cloud storage path exists and contains at
    least one file.

    Args:
        path: The DBFS (or mounted cloud storage) path to check.

    Returns:
        bool: True if the path can be listed and contains one or more
        entries; False if dbutils is unavailable, the path does not exist,
        or the listing raises any error (deliberately fails "closed" so a
        missing path never crashes the calling pipeline stage).
    """
    dbutils_local = get_dbutils()

    if dbutils_local is None:
        return False

    try:
        return len(dbutils_local.fs.ls(path)) > 0
    except Exception:
        return False


def send_email(
    subject,
    body,
    to_email,
    smtp_server,
    smtp_user,
    smtp_password="",
    smtp_port=587,
):
    """
    Send a plain-text alert email via SMTP (used for pipeline
    success/failure notifications).

    Supports both unauthenticated relays (smtp_password="") and
    authenticated SMTP (e.g. Gmail App Password).  Defaults to
    port 587 with STARTTLS, which is the standard for Gmail and
    most modern SMTP providers.

    Args:
        subject: Email subject. May be passed as a list of strings, in
            which case the parts are joined with a single space.
        body: Email body. May be passed as a list of strings (e.g. one
            line per error), in which case the parts are joined with
            newlines.
        to_email: A single recipient address, or a list of addresses.
        smtp_server: Hostname of the SMTP relay to send through.
        smtp_user: The "From" address / SMTP auth user. May be passed as a
            list, in which case only the first entry is used.
        smtp_password: SMTP auth password (e.g. Gmail App Password).
            Leave empty for unauthenticated internal relays.
        smtp_port: SMTP port (default 587 for STARTTLS).

    Returns:
        None. Success or failure is only reported via print statements -
        this function intentionally never raises, so a failed notification
        can never bring down the pipeline run that triggered it.
    """

    # Normalise list-shaped inputs (some callers build these dynamically
    # from control-table rows) down to the plain strings the email
    # library expects.
    if isinstance(subject, list):
        subject = " ".join(map(str, subject))

    if isinstance(body, list):
        body = "\n".join(map(str, body))

    if isinstance(smtp_user, list):
        smtp_user = smtp_user[0]

    if isinstance(to_email, str):
        to_emails = [to_email]
    else:
        to_emails = to_email

    msg = MIMEText(body)
    msg["Subject"] = str(subject)
    msg["From"] = str(smtp_user)
    msg["To"] = ", ".join(map(str, to_emails))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            if smtp_password:
                server.login(smtp_user, smtp_password)
            server.sendmail(
                smtp_user,
                to_emails,
                msg.as_string(),
            )

        print(f"Email sent to {to_emails}")

    except Exception as exc:
        print(f"Failed to send email: {exc}")


def archive_file(
    source_type: str,
    archive_flag,
    source_file_path: str,
    archive_base_path: str,
    source_name: str,
) -> None:
    """
    Archive a successfully processed source file.

    Archiving is performed only for file sources when archive_flag is True.
    """

    if source_type != "file" or archive_flag is not True:
        print(
            f"Not required for other source types like {source_type} "
            f"and archive_flag {archive_flag}"
        )
        return

    dbutils_local = get_dbutils()

    try:
        today_str = datetime.now().strftime("%Y-%m-%d")

        archive_path = (
            f"{archive_base_path.rstrip('/')}/{source_name}/{today_str}"
        )

        dbutils_local.fs.mkdirs(archive_path)

        file_name = source_file_path.split("/")[-1]
        destination_path = f"{archive_path}/{file_name}"

        dbutils_local.fs.cp(
            source_file_path,
            destination_path,
        )

        print(
            f"File archived successfully: {destination_path}"
        )

    except Exception as exc:
        print(
            f"Error archiving file {source_file_path}: {str(exc)}"
        )


def read_file(
    path: str,
    file_format: str,
    options: Optional[Dict[str, str]] = None
):
    """
    Reads data from a file path given format and options.
    """

    options = options or {}

    if file_format in ["csv", "txt"]:
        return spark.read.options(**options).csv(path)

    elif file_format == "json":
        return spark.read.options(**options).json(path)

    elif file_format == "parquet":
        return spark.read.parquet(path)

    elif file_format == "delta":
        return spark.read.format("delta").load(path)

    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def read_table(
    table_name: str,
    filename_template: str,
):
    """
    Read a Spark SQL table.
    """
    return spark.read.table(
        f"{table_name}.{filename_template}"
    )


def read_api(
    api_url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
):
    """
    Read records from an HTTP GET API.

    This generic helper intentionally remains GET-based.
    IQVIA-specific POST behavior is handled separately by the API code.
    """

    response = requests.get(
        api_url,
        headers=headers,
        params=params,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"API call failed with status "
            f"{response.status_code}: {response.text}"
        )

    json_data = response.json()

    if isinstance(json_data, dict):
        if (
            "data" in json_data
            and isinstance(json_data["data"], list)
        ):
            records = json_data["data"]
        else:
            records = [json_data]

    elif isinstance(json_data, list):
        records = json_data

    else:
        raise ValueError(
            "Unsupported JSON structure"
        )

    rows = [
        Row(
            **{
                key: str(value) if value is not None else None
                for key, value in item.items()
            }
        )
        for item in records
    ]

    return spark.createDataFrame(rows)


def read_data_dynamic(
    source_type: str,
    source_path: str,
    filename_template: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    api_headers: Optional[Dict[str, str]] = None,
    api_params: Optional[Dict[str, str]] = None,
):
    """
    Dynamically read data from file, table, or API.

    Supported source types:
        - file
        - table-like source types ending with "table"
        - api
    """

    if source_type == "file":

        full_data_path = (
            f"{source_path}/{filename_template}"
        )

        if DeltaTable.isDeltaTable(
            spark,
            full_data_path,
        ):
            file_format = "delta"

        else:
            file_format = (
                Path(filename_template)
                .suffix
                .lstrip(".")
                .lower()
            )

            dbutils_local = get_dbutils()

            try:
                files = [
                    file_info.name
                    for file_info in dbutils_local.fs.ls(
                        full_data_path
                    )
                ]

                for name in files:

                    if name.endswith(".csv"):
                        file_format = "csv"
                        break

                    if name.endswith(".txt"):
                        file_format = "txt"
                        break

                    if name.endswith(".parquet"):
                        file_format = "parquet"
                        break

                    if name.endswith(".orc"):
                        file_format = "orc"
                        break

                    if name.endswith(".json"):
                        file_format = "json"
                        break

            except Exception:
                pass

        if not file_format:
            raise ValueError(
                "file_format must be provided when "
                "source_type is 'file'"
            )

        file_path = (
            file_exists_dynamic(
                source_type,
                source_path,
                filename_template,
            )
            if filename_template
            else source_path
        )

        if not file_path:
            print(
                f"No file found for template: "
                f"{filename_template}"
            )
            return None

        return read_file(
            file_path,
            file_format,
            options=options,
        )

    if source_type.endswith("table"):
        return read_table(
            source_path,
            filename_template,
        )

    if source_type == "api":
        return read_api(
            source_path,
            api_headers,
            api_params,
        )

    raise ValueError(
        f"Unsupported source_type: {source_type}"
    )


def file_exists_dynamic(
    source_type: str,
    folder_path: str,
    filename_template: str,
):
    """
    Find a file/table/API resource dynamically.

    For files, YYYYMMDDHHMMSS is treated as a dynamic
    timestamp token and the latest matching filename is returned.
    """

    if source_type == "file":

        dynamic_token = "YYYYMMDDHHMMSS"
        dbutils_local = get_dbutils()

        try:
            files = dbutils_local.fs.ls(folder_path)

        except Exception as exc:
            print(
                f"Error accessing folder "
                f"{folder_path}: {exc}"
            )
            return None

        if dynamic_token in filename_template:

            prefix, suffix = filename_template.split(
                dynamic_token,
                1,
            )

            matching_files = [
                file_info
                for file_info in files
                if file_info.name.startswith(prefix)
                and file_info.name.endswith(suffix)
            ]

            if not matching_files:
                return None

            latest_file = sorted(
                matching_files,
                key=lambda file_info: file_info.name,
                reverse=True,
            )[0]

            return latest_file.path

        matching_files = [
            file_info
            for file_info in files
            if file_info.name == filename_template
        ]

        if not matching_files:
            return None

        return f"{folder_path}/{filename_template}"

    if source_type.endswith("table"):

        try:
            if spark.catalog.tableExists(
                f"{folder_path}.{filename_template}"
            ):
                return "table_exists"

        except Exception as exc:
            print(
                f"Error checking Spark table: {exc}"
            )

        return None

    if source_type == "api":

        try:
            response = requests.get(
                folder_path,
                timeout=5,
            )

            if response.status_code == 200:
                return "api_available"

        except Exception as exc:
            print(
                f"API check failed: {exc}"
            )

        return None

    raise ValueError(
        f"Unsupported resource type: {source_type}"
    )

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# File/API connection settings are passed by the calling pipeline.
# Put AWS credentials in the AWS/Databricks runtime, not in this file.
# If a source or archive path is metadata-driven, update the control/config table
# rather than hardcoding the path in this utility module.
# ============================================================================

