"""
SBC Lambda orchestration layer.

Source of truth:
    sbc (1).py

Responsibilities:
    - Authenticate incoming requests.
    - Validate search payload.
    - Build MDM_HUB and IQVIA request payloads.
    - Execute MDM_HUB-only / MDM_HUB+IQVIA flows.
    - Apply IQVIA deduplication.
    - Process API responses.
    - Combine responses.
    - Write error/audit information to Databricks SQL.

Important:
    Values that were placeholders or undefined in the source implementation
    are intentionally not invented here.
"""

from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3
import requests

from .duplicate_iqvia_records import deduplicate_iqvia_records
from .process_iqvia_response import process_iqvia_response
from .process_mdm_hub_response import process_mdm_hub_response
from .transform_to_iqvia import transform_to_iqvia
from .transform_to_mdm_hub import transform_to_mdm_hub


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("sbc_MDM_HUB")
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------

# Runtime configuration. Values are supplied by the deployment environment;
# credentials and tokens are never hard-coded in source control.
TABLE_NAME = os.getenv("table_name")
AUDIT_TABLE_NAME = os.getenv("audit_table_name")
SERVER_HOSTNAME = os.getenv("server_hostname")
ACCESS_TOKEN = os.getenv("access_token")
WAREHOUSE_ID = os.getenv("warehouse_id")
API_KEY = os.getenv("api_key")

SECRET_NAME = os.getenv("secret_name")
REGION_NAME = os.getenv("region", "us-east-1")

MDM_HUB_URL = os.getenv("MDM_HUB_url")
IQVIA_URL = os.getenv("iqvia_url")


# ---------------------------------------------------------------------------
# Runtime constants
# ---------------------------------------------------------------------------

MAX_RETRIES = 3
BACKOFF_SECONDS = 2
CONTENT_TYPE = "application/json"


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------

def get_secret(
    secret_name: str,
    region_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Retrieve API credentials from AWS Secrets Manager.

    Source behavior:
        - SecretString is JSON-decoded.
        - SecretBinary is returned directly.

    No secret names, fields, or credentials are invented.
    """
    client = boto3.client(
        "secretsmanager",
        region_name=region_name or REGION_NAME,
    )

    try:
        response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as exc:
        raise RuntimeError(
            f"Error retrieving secret: {exc}"
        ) from exc

    if "SecretString" in response:
        return json.loads(response["SecretString"])

    return response["SecretBinary"]


# Load API credentials lazily. Importing this module must not call AWS.
MDM_HUB_USERNAME: Optional[str] = None
MDM_HUB_PASSWORD: Optional[str] = None
IQVIA_USERNAME: Optional[str] = None
IQVIA_PASSWORD: Optional[str] = None


def load_runtime_credentials() -> None:
    """Load MDM_HUB/IQVIA credentials from AWS Secrets Manager at runtime."""
    global MDM_HUB_USERNAME, MDM_HUB_PASSWORD
    global IQVIA_USERNAME, IQVIA_PASSWORD

    if not SECRET_NAME:
        raise RuntimeError("Missing required runtime configuration: secret_name")

    secret = get_secret(SECRET_NAME, REGION_NAME)
    if not isinstance(secret, dict):
        raise RuntimeError("AWS Secrets Manager secret must contain a JSON object")

    MDM_HUB_USERNAME = secret.get("username")
    MDM_HUB_PASSWORD = secret.get("password")
    IQVIA_USERNAME = secret.get("iqvia_username")
    IQVIA_PASSWORD = secret.get("iqvia_password")


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def current_time() -> str:
    """Return current local timestamp in source-compatible format."""
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def sql_value(value: Any) -> str:
    """
    Convert a Python value into a SQL literal.

    This preserves the source implementation's SQL construction behavior.
    """
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        return str(value)

    return "'" + str(value).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Request validation
# ---------------------------------------------------------------------------

def validate_request_payload(
    payload: Dict[str, Any],
    mandatory_fields: list[str],
) -> list[Dict[str, Any]]:
    """
    Validate incoming request payload.

    Source-supported validations:
        - mandatory fields
        - nested fields
        - null/blank values
        - mdmEntityType must be HCP
        - hcp.specialty must be a list when supplied
        - hcp.email basic format validation
    """
    errors: list[Dict[str, Any]] = []

    payload = payload or {}

    # --------------------------------------------------
    # Mandatory fields
    # --------------------------------------------------

    for field in mandatory_fields:
        value = None
        exists = False

        if field in payload:
            value = payload[field]
            exists = True

        else:
            keys = field.split(".")
            current: Any = payload

            for index, key in enumerate(keys):

                if isinstance(current, dict) and key in current:
                    current = current[key]

                elif isinstance(current, list):

                    if not current:
                        exists = False
                        break

                    values = []
                    any_valid = False

                    for item_index, item in enumerate(current):
                        temp = item
                        valid = True

                        for nested_key in keys[index:]:
                            if (
                                isinstance(temp, dict)
                                and nested_key in temp
                            ):
                                temp = temp[nested_key]
                            else:
                                valid = False
                                break

                        if valid:
                            any_valid = True
                            values.append(
                                (item_index, temp)
                            )

                    if not any_valid:
                        exists = False
                        break

                    value = values
                    exists = True
                    break

                else:
                    exists = False
                    break

            else:
                value = current
                exists = True

        if not exists:
            errors.append(
                {
                    "field_path": field,
                    "error_type": "MISSING",
                    "message": (
                        f"Missing mandatory field: {field}"
                    ),
                }
            )
            continue

        # --------------------------------------------------
        # List of nested values
        # --------------------------------------------------

        if (
            isinstance(value, list)
            and value
            and isinstance(value[0], tuple)
        ):
            for index, item_value in value:

                if item_value is None:
                    errors.append(
                        {
                            "field_path": f"{field}[{index}]",
                            "error_type": "NULL",
                            "message": (
                                f"Null value for mandatory field: "
                                f"{field}[{index}]"
                            ),
                        }
                    )

                elif (
                    isinstance(item_value, str)
                    and not item_value.strip()
                ):
                    errors.append(
                        {
                            "field_path": f"{field}[{index}]",
                            "error_type": "BLANK",
                            "message": (
                                f"Blank value for mandatory field: "
                                f"{field}[{index}]"
                            ),
                        }
                    )

        else:

            if value is None:
                errors.append(
                    {
                        "field_path": field,
                        "error_type": "NULL",
                        "message": (
                            f"Null value for mandatory field: {field}"
                        ),
                    }
                )

            elif (
                isinstance(value, str)
                and not value.strip()
            ):
                errors.append(
                    {
                        "field_path": field,
                        "error_type": "BLANK",
                        "message": (
                            f"Blank value for mandatory field: {field}"
                        ),
                    }
                )

            elif isinstance(value, list):

                if not value:
                    errors.append(
                        {
                            "field_path": field,
                            "error_type": "NULL",
                            "message": (
                                f"Null value for mandatory field: {field}"
                            ),
                        }
                    )

                for index, item_value in enumerate(value):

                    if item_value is None:
                        errors.append(
                            {
                                "field_path": f"{field}[{index}]",
                                "error_type": "NULL",
                                "message": (
                                    f"Null value for mandatory field: "
                                    f"{field}[{index}]"
                                ),
                            }
                        )

                    elif (
                        isinstance(item_value, str)
                        and not item_value.strip()
                    ):
                        errors.append(
                            {
                                "field_path": f"{field}[{index}]",
                                "error_type": "BLANK",
                                "message": (
                                    f"Blank value for mandatory field: "
                                    f"{field}[{index}]"
                                ),
                            }
                        )

    # --------------------------------------------------
    # MDM entity
    # --------------------------------------------------

    mdm_entity = (
        payload.get("mdmEntityType") or ""
    ).strip()

    if not mdm_entity:
        errors.append(
            {
                "field_path": "mdmEntityType",
                "error_type": "MISSING",
                "message": (
                    "mdmEntityType must be provided "
                    "and should be HCP"
                ),
            }
        )

    elif mdm_entity.upper() != "HCP":
        errors.append(
            {
                "field_path": "mdmEntityType",
                "error_type": "INVALID",
                "message": "mdmEntityType must be 'HCP'",
            }
        )

    # --------------------------------------------------
    # Specialty
    # --------------------------------------------------

    specialty = payload.get("hcp.specialty")

    if specialty is not None and not isinstance(
        specialty,
        list,
    ):
        errors.append(
            {
                "field_path": "hcp.specialty",
                "error_type": "INVALID_TYPE",
                "message": (
                    "hcp.specialty must be an array/list"
                ),
            }
        )

    # --------------------------------------------------
    # Email
    # --------------------------------------------------

    email = (
        payload.get("hcp.email") or ""
    ).strip()

    if email:
        if (
            "@" not in email
            or email.startswith("@")
            or email.endswith("@")
        ):
            errors.append(
                {
                    "field_path": "hcp.email",
                    "error_type": "INVALID_FORMAT",
                    "message": (
                        "hcp.email must be a valid email address"
                    ),
                }
            )

    if not errors:
        logger.info(
            "[VALIDATION] Payload validation passed successfully"
        )

    return errors


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def extract_error_details(
    last_error: Any,
    api_name: str = "",
) -> Dict[str, Any]:
    """Extract structured error information from an API/error response."""
    try:
        if hasattr(last_error, "text"):
            error_json = json.loads(
                last_error.text
            )

        elif isinstance(last_error, dict):
            error_json = last_error

        else:
            try:
                error_json = json.loads(
                    str(last_error)
                )
            except Exception:
                error_json = {
                    "errorSummary": str(last_error)
                }

        details = (
            error_json
            .get("errorDetail", {})
            .get("details", [])
        )

        first_detail = (
            details[0]
            if details
            else {}
        )

        errors_list = (
            error_json
            .get("response", {})
            .get("errors", [])
        )

        first_error = (
            errors_list[0]
            if errors_list
            else {}
        )

        status_code = (
            error_json.get("status")
            or first_error.get("httpStatusCode")
            or 500
        )

        message = (
            error_json.get("errorSummary")
            or first_error.get("message")
            or str(error_json)
        )

        return {
            "status_code": status_code,
            "error_code": error_json.get(
                "errorCode"
            ),
            "message": (
                f"[{api_name}] {message}"
                if api_name
                else message
            ),
            "details": (
                [
                    {
                        "code": first_detail.get("code"),
                        "message": (
                            first_detail.get("message")
                            or first_error.get("details")
                        ),
                        "localizedMessage": (
                            first_detail.get(
                                "localizedMessage"
                            )
                        ),
                    }
                ]
                if first_detail or first_error
                else []
            ),
            "retryable": True,
        }

    except Exception:
        logger.exception(
            "Error extracting error details"
        )

        return {
            "status_code": 500,
            "error_code": "UNKNOWN_ERROR",
            "message": str(last_error),
            "details": [],
            "retryable": False,
        }


def build_structured_error_response(
    error_info: Dict[str, Any],
    request_id: str = "",
) -> Dict[str, Any]:
    """Build Lambda-compatible structured error response."""
    try:
        return {
            "statusCode": (
                error_info.get("status_code")
                or 500
            ),
            "headers": {
                "Content-Type": CONTENT_TYPE
            },
            "body": json.dumps(
                {
                    "status": "error",
                    "error": {
                        "code": (
                            error_info.get("error_code")
                            or ""
                        ),
                        "message": (
                            error_info.get("message")
                            or ""
                        ),
                        "details": (
                            error_info.get("details")
                            or []
                        ),
                        "retryable": error_info.get(
                            "retryable",
                            False,
                        ),
                        "requestId": request_id,
                    },
                }
            ),
        }

    except Exception as exc:
        logger.exception(
            "Error building structured error response"
        )

        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": CONTENT_TYPE
            },
            "body": json.dumps(
                {
                    "status": "error",
                    "error": {
                        "code": "UNKNOWN_ERROR",
                        "message": str(exc),
                        "details": [],
                        "retryable": False,
                        "requestId": request_id,
                    },
                }
            ),
        }


# ---------------------------------------------------------------------------
# Databricks SQL
# ---------------------------------------------------------------------------

def _databricks_headers() -> Dict[str, str]:
    """Build Databricks SQL API headers."""
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": CONTENT_TYPE,
    }


def _execute_databricks_statement(
    query: str,
) -> Dict[str, Any]:
    """
    Execute a Databricks SQL statement and wait for completion.

    Source behavior:
        - POST statement.
        - Poll statement status up to 10 times.
        - One-second interval.
    """
    url = (
        f"https://{SERVER_HOSTNAME}"
        "/api/2.0/sql/statements/"
    )

    headers = _databricks_headers()

    response = requests.post(
        url,
        headers=headers,
        json={
            "statement": query,
            "warehouse_id": WAREHOUSE_ID,
        },
    )

    if response.status_code != 200:
        raise RuntimeError(
            response.text
        )

    statement_id = response.json()["statement_id"]

    status_url = (
        f"https://{SERVER_HOSTNAME}"
        f"/api/2.0/sql/statements/{statement_id}"
    )

    for _ in range(10):
        status_response = requests.get(
            status_url,
            headers=headers,
        )

        status_json = status_response.json()
        state = status_json["status"]["state"]

        if state == "SUCCEEDED":
            return {
                "status": "success"
            }

        if state in {
            "FAILED",
            "CANCELED",
        }:
            raise RuntimeError(
                status_json
            )

        time.sleep(1)

    raise RuntimeError(
        "Query did not finish in time"
    )


def write_to_databricks(
    record: Dict[str, Any],
) -> Dict[str, Any]:
    """Write API error information to the configured error table."""
    query = f"""
        INSERT INTO {TABLE_NAME} (
            STATUS,
            ERROR_CODE,
            ERROR_URL,
            ERROR_SUMMARY,
            ERROR_ID,
            DETAIL_CODE,
            DETAIL_MESSAGE,
            LOCALIZED_MESSAGE,
            PATH,
            HOST,
            SERVER_ERROR,
            LOCALIZED_ERROR_SUMMARY,
            CREATED_TS,
            ERROR_TYPE,
            PAYLOAD
        )
        VALUES (
            {sql_value(record.get("STATUS"))},
            {sql_value(record.get("ERROR_CODE"))},
            {sql_value(record.get("ERROR_URL"))},
            {sql_value(record.get("ERROR_SUMMARY"))},
            {sql_value(record.get("ERROR_ID"))},
            {sql_value(record.get("DETAIL_CODE"))},
            {sql_value(record.get("DETAIL_MESSAGE"))},
            {sql_value(record.get("LOCALIZED_MESSAGE"))},
            {sql_value(record.get("PATH"))},
            {sql_value(record.get("HOST"))},
            {sql_value(record.get("SERVER_ERROR"))},
            {sql_value(record.get("LOCALIZED_ERROR_SUMMARY"))},
            TIMESTAMP {sql_value(record.get("created_ts"))},
            {sql_value(record.get("ERROR_TYPE"))},
            {sql_value(record.get("PAYLOAD"))}
        )
    """

    try:
        return _execute_databricks_statement(
            query
        )

    except Exception:
        logger.critical(
            "Databricks write failed",
            exc_info=True,
        )
        raise


def write_to_audit_table(
    record: Dict[str, Any],
) -> Dict[str, Any]:
    """Write request/response audit record to configured audit table."""
    query = f"""
        INSERT INTO {AUDIT_TABLE_NAME} (
            request_id,
            request_payload,
            initiated_by,
            request_received_time,
            response_sent_time,
            response_time,
            searched_in_iqvia,
            MDM_HUB_original_response,
            MDM_HUB_structured_response,
            iqvia_original_response,
            iqvia_structured_response
        )
        VALUES (
            {sql_value(record.get("request_id"))},
            {sql_value(record.get("request_payload"))},
            {sql_value(record.get("initiated_by"))},
            TIMESTAMP {sql_value(record.get("request_received_time"))},
            TIMESTAMP {sql_value(record.get("response_sent_time"))},
            {sql_value(record.get("response_time"))},
            {str(record.get("searched_in_iqvia", False)).lower()},
            {sql_value(record.get("MDM_HUB_original_response"))},
            {sql_value(record.get("MDM_HUB_structured_response"))},
            {sql_value(record.get("iqvia_original_response"))},
            {sql_value(record.get("iqvia_structured_response"))}
        )
    """

    try:
        return _execute_databricks_statement(
            query
        )

    except Exception:
        logger.critical(
            "Databricks audit write failed",
            exc_info=True,
        )
        raise


# ---------------------------------------------------------------------------
# API execution
# ---------------------------------------------------------------------------

def call_api_with_retry(
    username: str,
    password: str,
    url: str,
    payload: str,
    max_retries: int,
    backoff_seconds: int,
    api_name: str,
):
    """
    Call MDM_HUB/IQVIA API with retry behavior from the source.

    NOTE:
        The MDM_HUB login endpoint in the supplied source was a literal
        "https://<MDM_HUB_HOST>/..." placeholder - never a real URL.
        Fixed to derive the real host from `url` (the configured
        MDM_HUB_URL, which is itself a full search/lookup endpoint with
        its own path) via urlparse, so the login call goes to the same
        host without inheriting `url`'s own path suffix. If your real
        Informatica MDM Hub uses a different login path than the SIF
        default below, override it with the MDM_HUB_LOGIN_PATH env var.
    """
    try:
        logger.info(
            f"Creating session for {api_name}"
        )

        if api_name == "MDM_HUB":

            parsed_url = urlparse(url)
            mdm_hub_host = f"{parsed_url.scheme}://{parsed_url.netloc}"
            mdm_hub_login_url = (
                f"{mdm_hub_host}"
                f"{os.getenv('MDM_HUB_LOGIN_PATH', '/sas/public/core/v3/login')}"
            )

            session_response = requests.request(
                "POST",
                mdm_hub_login_url,
                headers={
                    "Accept": CONTENT_TYPE,
                    "Content-Type": CONTENT_TYPE,
                },
                data=json.dumps(
                    {
                        "username": username,
                        "password": password,
                    }
                ),
                timeout=30,
            )

            session_id = session_response.json()[
                "userInfo"
            ]["sessionId"]

            headers = {
                "Content-Type": CONTENT_TYPE,
                "IDS-SESSION-ID": session_id,
            }

        else:
            # The supplied source references BASIC_AUTH_TOKEN,
            # but does not define it.
            #
            # Do not invent or hard-code the credential here.
            basic_auth_token = os.environ.get(
                "BASIC_AUTH_TOKEN"
            )

            if not basic_auth_token:
                raise RuntimeError(
                    "BASIC_AUTH_TOKEN is not defined "
                    "in the supplied source or environment."
                )

            headers = {
                "Content-Type": CONTENT_TYPE,
                "Authorization": (
                    f"Basic {basic_auth_token}"
                ),
            }

    except Exception as exc:
        logger.error(
            f"Session creation failed for {api_name}"
        )

        raise RuntimeError(
            f"Session creation failed for {api_name}: {exc}"
        ) from exc

    logger.info(
        f"[{api_name}] Started at {current_time()}"
    )

    last_error: Any = None
    last_status_code: Optional[int] = None

    for attempt in range(
        1,
        max_retries + 1,
    ):
        try:
            logger.info(
                f"Calling {api_name} API "
                f"(attempt {attempt}/{max_retries})"
            )

            response = requests.request(
                "POST",
                url=url,
                headers=headers,
                data=payload,
                timeout=30,
            )

            last_status_code = (
                response.status_code
            )

            if response.status_code == 200:
                logger.info(
                    f"[{api_name}] Completed at "
                    f"{current_time()}"
                )
                return response

            response_content_type = (
                response.headers.get(
                    "Content-Type",
                    "",
                )
            )

            if (
                "application/json"
                in response_content_type.lower()
            ):
                try:
                    last_error = response.json()
                except Exception:
                    last_error = {
                        "status": response.status_code,
                        "errorSummary": response.text,
                    }
            else:
                last_error = {
                    "status": response.status_code,
                    "errorSummary": response.text,
                }

            logger.warning(
                f"[{api_name}] API returned failure "
                f"Status={response.status_code}"
            )

            logger.warning(
                f"[{api_name}] Response Body: "
                f"{response.text[:2000]}"
            )

        except requests.exceptions.RequestException as exc:
            last_error = str(exc)

            logger.warning(
                f"[{api_name}] RequestException: "
                f"{last_error}"
            )

        except Exception as exc:
            last_error = str(exc)

            logger.warning(
                f"[{api_name}] Unexpected exception: "
                f"{last_error}"
            )

        if attempt < max_retries:
            sleep_time = (
                backoff_seconds * attempt
            )

            logger.info(
                f"Retrying after {sleep_time} seconds"
            )

            time.sleep(sleep_time)

    logger.critical(
        f"{api_name} API failed after "
        f"{max_retries} attempts"
    )

    # --------------------------------------------------
    # Build source-compatible error record
    # --------------------------------------------------

    try:
        if hasattr(last_error, "text"):
            error_json = json.loads(
                last_error.text
            )

        elif isinstance(last_error, dict):
            error_json = last_error

        else:
            error_json = {
                "errorSummary": str(last_error)
            }

        details = (
            error_json
            .get("errorDetail", {})
            .get("details", [])
        )

        first_detail = (
            details[0]
            if details
            else {}
        )

        errors_list = (
            error_json
            .get("response", {})
            .get("errors", [])
        )

        first_error = (
            errors_list[0]
            if errors_list
            else {}
        )

        record = {
            "STATUS": (
                last_status_code
                or error_json.get("status")
                or first_error.get(
                    "httpStatusCode"
                )
            ),
            "ERROR_CODE": error_json.get(
                "errorCode"
            ),
            "ERROR_URL": error_json.get(
                "errorUrl"
            ),
            "ERROR_SUMMARY": (
                f"[{api_name}] "
                f"{error_json.get('errorSummary')}"
                if error_json.get(
                    "errorSummary"
                )
                else (
                    f"[{api_name}] "
                    f"{first_error.get('message', 'Unknown error')}"
                )
            ),
            "ERROR_ID": (
                error_json.get("errorId")
                or first_error.get("id")
            ),
            "DETAIL_CODE": first_detail.get(
                "code"
            ),
            "DETAIL_MESSAGE": (
                first_detail.get("message")
                or first_error.get("details")
            ),
            "LOCALIZED_MESSAGE": (
                first_detail.get(
                    "localizedMessage"
                )
            ),
            "PATH": error_json.get("path"),
            "HOST": error_json.get("host"),
            "SERVER_ERROR": error_json.get(
                "serverError"
            ),
            "LOCALIZED_ERROR_SUMMARY": (
                error_json.get(
                    "localizedErrorSummary"
                )
            ),
            "ERROR_TYPE": api_name,
            "PAYLOAD": json.dumps(
                error_json
            ),
            "created_ts": datetime.now(
                timezone.utc
            ),
        }

        result = write_to_databricks(
            record
        )

        logger.info(
            f"Databricks result: {result}"
        )

    except Exception:
        logger.exception(
            "Error while building error record"
        )

    return None


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def has_high_score(
    MDM_HUB_raw: Dict[str, Any],
    match_score: int,
) -> bool:
    """Return True when any MDM_HUB record meets match score."""
    records = (
        MDM_HUB_raw
        .get("searchResult", {})
        .get("records", [])
    )

    return any(
        (
            record.get("_meta") or {}
        ).get("score", 0) >= match_score
        for record in records
    )


def combined_response(
    MDM_HUB_response: Optional[Dict[str, Any]] = None,
    iqvia_response: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Combine structured MDM_HUB and IQVIA responses."""
    combined_records: list[Dict[str, Any]] = []

    for response in (
        MDM_HUB_response,
        iqvia_response,
    ):
        if not response:
            continue

        body = response.get("body")

        if isinstance(body, str):
            body = json.loads(body)

        records = (
            (body.get("searchResult") or {})
            .get("records", [])
        )

        combined_records.extend(
            records
        )

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": CONTENT_TYPE
        },
        "body": json.dumps(
            {
                "searchResult": {
                    "totalRecords": len(
                        combined_records
                    ),
                    "records": combined_records,
                }
            }
        ),
    }


def build_sbc_audit_record(
    payload: Dict[str, Any],
    headers: Dict[str, Any],
    request_received_time: datetime,
    response_sent_time: datetime,
    MDM_HUB_original: Optional[Dict[str, Any]] = None,
    MDM_HUB_structured: Optional[Dict[str, Any]] = None,
    iqvia_original: Optional[Dict[str, Any]] = None,
    iqvia_structured: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build the SBC audit record.

    The supplied source creates this record but does not return it.
    Returning it here is required for the subsequent audit-table call.
    """
    payload = payload or {}
    headers = headers or {}

    searched_in_iqvia = (
        str(
            headers.get(
                "iqvia",
                "",
            )
        ).lower()
        == "true"
    )

    response_time = int(
        (
            response_sent_time
            - request_received_time
        ).total_seconds()
        * 1000
    )

    return {
        "request_id": payload.get(
            "requestID",
            "",
        ),
        "request_payload": json.dumps(
            payload
        ),
        "initiated_by": payload.get(
            "clientName",
            "",
        ),
        "request_received_time": (
            request_received_time.isoformat()
        ),
        "response_sent_time": (
            response_sent_time.isoformat()
        ),
        "response_time": response_time,
        "searched_in_iqvia": searched_in_iqvia,
        "MDM_HUB_original_response": json.dumps(
            MDM_HUB_original or {}
        ),
        "MDM_HUB_structured_response": json.dumps(
            MDM_HUB_structured or {}
        ),
        "iqvia_original_response": json.dumps(
            iqvia_original or {}
        ),
        "iqvia_structured_response": json.dumps(
            iqvia_structured or {}
        ),
    }


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def authenticate_request(
    headers: Dict[str, Any],
) -> Tuple[bool, str]:
    """Authenticate incoming Bearer token."""
    headers = headers or {}

    headers_lower = {
        str(key).lower(): value
        for key, value in headers.items()
    }

    auth_header = headers_lower.get(
        "authorization",
        "",
    )

    if not auth_header:
        return (
            False,
            "Unauthorized: Missing Authorization header",
        )

    try:
        token_type, token = (
            auth_header.split()
        )
    except ValueError:
        return (
            False,
            "Bad Request: Invalid Authorization header format",
        )

    if token_type.lower() != "bearer":
        return (
            False,
            "Unauthorized: Authorization type must be Bearer",
        )

    if token != API_KEY:
        return (
            False,
            "Forbidden: Invalid API key",
        )

    return True, "Authenticated"


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(
    event: Dict[str, Any],
    context: Any,
) -> Dict[str, Any]:
    """AWS Lambda entry point for SBC search."""
    load_runtime_credentials()

    logger.info(
        "Lambda execution STARTED"
    )

    logger.info(
        f"[LAMBDA] Started at {current_time()}"
    )

    request_received_time = (
        datetime.now(timezone.utc)
    )

    event = event or {}

    body = event.get("body")
    headers = event.get("headers") or {}

    authenticated, message = (
        authenticate_request(headers)
    )

    if not authenticated:
        return {
            "statusCode": 401,
            "headers": {
                "Content-Type": CONTENT_TYPE
            },
            "body": json.dumps(
                {
                    "message": message
                }
            ),
        }

    try:
        if isinstance(body, str):
            payload_dict = json.loads(body)
        else:
            payload_dict = body or {}

    except json.JSONDecodeError as exc:
        error_info = extract_error_details(
            {
                "errorSummary": str(exc)
            },
            api_name="VALIDATION",
        )

        return build_structured_error_response(
            error_info
        )

    request_type = (
        payload_dict.get("requestType")
        or ""
    ).strip().lower()

    logger.info(
        f"Request Type received: {request_type}"
    )

    if request_type != "search":
        logger.warning(
            "Lambda execution skipped for "
            f"requestType: {request_type}"
        )

        return {
            "status": "skipped",
            "message": (
                "Lambda executes only for "
                "requestType = Search. "
                f"Received: {request_type}"
            ),
        }

    request_id = ""

    mandatory_fields = [
        "hcp.firstName"
    ]

    errors = validate_request_payload(
        payload_dict,
        mandatory_fields,
    )

    if errors:
        logger.error(
            "Payload validation failed"
        )

        error_info = extract_error_details(
            {
                "errorSummary": str(errors)
            },
            api_name="VALIDATION",
        )

        return build_structured_error_response(
            error_info,
            request_id=request_id,
        )

    # --------------------------------------------------
    # Build MDM_HUB payload
    # --------------------------------------------------

    MDM_HUB_payload_dict = transform_to_mdm_hub(
        payload_dict
    )

    MDM_HUB_payload = json.dumps(
        MDM_HUB_payload_dict
    )

    logger.info(
        f"MDM_HUB payload: {MDM_HUB_payload}"
    )

    # --------------------------------------------------
    # Build IQVIA payload
    # --------------------------------------------------

    iqvia_payload_dict = transform_to_iqvia(
        payload_dict
    )

    iqvia_payload = json.dumps(
        iqvia_payload_dict
    )

    logger.info(
        f"iqvia payload: {iqvia_payload}"
    )

    # --------------------------------------------------
    # Headers
    # --------------------------------------------------

    iqvia_header = headers.get(
        "iqvia",
        False,
    )

    if isinstance(iqvia_header, str):
        iqvia_flag = (
            iqvia_header.strip().lower()
            == "true"
        )
    else:
        iqvia_flag = bool(
            iqvia_header
        )

    match_score_header = headers.get(
        "matchScore",
        0,
    )

    match_score = int(
        match_score_header
    )

    logger.info(
        f"iqvia flag = {iqvia_flag}"
    )

    logger.info(
        f"matchScore = {match_score}"
    )

    MDM_HUB_raw = None
    MDM_HUB_structured = None
    iqvia_raw = None
    iqvia_structured = None

    # ==================================================
    # MDM_HUB ONLY
    # ==================================================

    if not iqvia_flag:

        logger.info(
            "Executing MDM_HUB ONLY"
        )

        try:
            MDM_HUB_response = call_api_with_retry(
                username=MDM_HUB_USERNAME,
                password=MDM_HUB_PASSWORD,
                url=MDM_HUB_URL,
                payload=MDM_HUB_payload,
                max_retries=MAX_RETRIES,
                backoff_seconds=BACKOFF_SECONDS,
                api_name="MDM_HUB",
            )

            if MDM_HUB_response is None:
                raise RuntimeError(
                    "MDM_HUB API returned no response"
                )

            MDM_HUB_raw = (
                MDM_HUB_response.json()
            )

            if (
                MDM_HUB_response.status_code
                != 200
            ):
                error_info = (
                    extract_error_details(
                        MDM_HUB_response,
                        api_name="MDM_HUB",
                    )
                )

                return build_structured_error_response(
                    error_info,
                    request_id=request_id,
                )

            MDM_HUB_structured = (
                process_mdm_hub_response(
                    MDM_HUB_raw,
                    match_score,
                )
            )

            MDM_HUB_records = (
                MDM_HUB_structured
                .get("searchResult", {})
                .get("records", [])
            )

            final_response = (
                combined_response(
                    MDM_HUB_response=MDM_HUB_structured
                )
            )

            # --------------------------------------------------
            # IQVIA fallback
            # --------------------------------------------------

            if (
                not MDM_HUB_records
                or not has_high_score(
                    MDM_HUB_raw,
                    match_score,
                )
            ):
                logger.info(
                    "No MDM_HUB results greater than "
                    "match score. Calling iqvia."
                )

                iqvia_response = (
                    call_api_with_retry(
                        username=IQVIA_USERNAME,
                        password=IQVIA_PASSWORD,
                        url=IQVIA_URL,
                        payload=iqvia_payload,
                        max_retries=MAX_RETRIES,
                        backoff_seconds=BACKOFF_SECONDS,
                        api_name="iqvia",
                    )
                )

                if iqvia_response is None:
                    raise RuntimeError(
                        "iqvia API returned no response"
                    )

                if (
                    iqvia_response.status_code
                    != 200
                ):
                    error_info = (
                        extract_error_details(
                            iqvia_response,
                            api_name="iqvia",
                        )
                    )

                    return build_structured_error_response(
                        error_info,
                        request_id=request_id,
                    )

                iqvia_raw = (
                    iqvia_response.json()
                )

                iqvia_structured = (
                    process_iqvia_response(
                        iqvia_raw
                    )
                )

                final_response = (
                    combined_response(
                        MDM_HUB_response=MDM_HUB_structured,
                        iqvia_response=iqvia_structured,
                    )
                )

            response_sent_time = (
                datetime.now(timezone.utc)
            )

            audit_record = (
                build_sbc_audit_record(
                    payload=payload_dict,
                    headers=headers,
                    request_received_time=(
                        request_received_time
                    ),
                    response_sent_time=(
                        response_sent_time
                    ),
                    MDM_HUB_original=MDM_HUB_raw,
                    MDM_HUB_structured=(
                        MDM_HUB_structured
                    ),
                    iqvia_original=iqvia_raw,
                    iqvia_structured=(
                        iqvia_structured
                    ),
                )
            )

            audit_result = (
                write_to_audit_table(
                    audit_record
                )
            )

            logger.info(
                f"Databricks audit write result: "
                f"{audit_result}"
            )

            logger.info(
                f"[LAMBDA] Ended at {current_time()}"
            )

            return final_response

        except Exception as exc:
            logger.exception(
                "MDM_HUB failed"
            )

            error_info = extract_error_details(
                exc,
                api_name="MDM_HUB",
            )

            return build_structured_error_response(
                error_info,
                request_id=request_id,
            )

    # ==================================================
    # MDM_HUB + IQVIA IN PARALLEL
    # ==================================================

    logger.info(
        "Executing MDM_HUB + iqvia in parallel"
    )

    raw_results: Dict[str, Any] = {}
    results: Dict[str, Any] = {}

    with ThreadPoolExecutor(
        max_workers=2
    ) as executor:

        futures = {
            executor.submit(
                call_api_with_retry,
                MDM_HUB_USERNAME,
                MDM_HUB_PASSWORD,
                MDM_HUB_URL,
                MDM_HUB_payload,
                MAX_RETRIES,
                BACKOFF_SECONDS,
                "MDM_HUB",
            ): "MDM_HUB",

            executor.submit(
                call_api_with_retry,
                IQVIA_USERNAME,
                IQVIA_PASSWORD,
                IQVIA_URL,
                iqvia_payload,
                MAX_RETRIES,
                BACKOFF_SECONDS,
                "iqvia",
            ): "iqvia",
        }

        for future in as_completed(
            futures
        ):
            api_name = futures[future]

            try:
                response = future.result()

                if response is None:
                    raise RuntimeError(
                        f"{api_name} API returned no response"
                    )

                raw = response.json()

                if response.status_code != 200:
                    error_info = (
                        extract_error_details(
                            response,
                            api_name=api_name.upper(),
                        )
                    )

                    results[api_name] = (
                        build_structured_error_response(
                            error_info,
                            request_id=request_id,
                        )
                    )

                else:
                    raw_results[api_name] = raw

                    if api_name == "MDM_HUB":
                        MDM_HUB_raw = raw
                    else:
                        iqvia_raw = raw

            except Exception as exc:
                logger.exception(
                    f"{api_name} failed"
                )

                error_info = (
                    extract_error_details(
                        exc,
                        api_name=api_name.upper(),
                    )
                )

                results[api_name] = (
                    build_structured_error_response(
                        error_info,
                        request_id=request_id,
                    )
                )

    # ==================================================
    # DEDUPLICATION
    # ==================================================

    try:
        if (
            "MDM_HUB" in raw_results
            and "iqvia" in raw_results
        ):
            logger.info(
                "Running deduplication between "
                "MDM_HUB and iqvia"
            )

            raw_results["iqvia"] = (
                deduplicate_iqvia_records(
                    raw_results["MDM_HUB"],
                    raw_results["iqvia"],
                )
            )

        else:
            logger.info(
                "Skipping deduplication"
            )

    except Exception as exc:
        logger.exception(
            "Deduplication failed"
        )

        results["dedupError"] = (
            build_structured_error_response(
                extract_error_details(
                    exc,
                    api_name="DEDUP",
                ),
                request_id=request_id,
            )
        )

    # ==================================================
    # PROCESS MDM_HUB
    # ==================================================

    if (
        "MDM_HUB" in raw_results
        and "MDM_HUB" not in results
    ):
        results["MDM_HUB"] = (
            process_mdm_hub_response(
                raw_results["MDM_HUB"],
                match_score,
            )
        )

        MDM_HUB_structured = (
            results["MDM_HUB"]
        )

    # ==================================================
    # DETERMINE IQVIA INCLUSION
    # ==================================================

    include_iqvia = False
    MDM_HUB_has_high = False

    if "MDM_HUB" in raw_results:
        MDM_HUB_has_high = has_high_score(
            raw_results["MDM_HUB"],
            match_score,
        )

    if iqvia_flag:
        include_iqvia = True

    elif not MDM_HUB_has_high:
        include_iqvia = True

    # ==================================================
    # PROCESS IQVIA
    # ==================================================

    if (
        include_iqvia
        and "iqvia" in raw_results
        and "iqvia" not in results
    ):
        results["iqvia"] = (
            process_iqvia_response(
                raw_results["iqvia"]
            )
        )

        iqvia_structured = (
            results["iqvia"]
        )

    # ==================================================
    # AUDIT
    # ==================================================

    response_sent_time = (
        datetime.now(timezone.utc)
    )

    audit_record = (
        build_sbc_audit_record(
            payload=payload_dict,
            headers=headers,
            request_received_time=(
                request_received_time
            ),
            response_sent_time=(
                response_sent_time
            ),
            MDM_HUB_original=MDM_HUB_raw,
            MDM_HUB_structured=(
                MDM_HUB_structured
            ),
            iqvia_original=iqvia_raw,
            iqvia_structured=(
                iqvia_structured
            ),
        )
    )

    audit_result = write_to_audit_table(
        audit_record
    )

    logger.info(
        f"[AUDIT] Databricks audit result: "
        f"{audit_result}"
    )

    logger.info(
        f"[LAMBDA] Ended at {current_time()}"
    )

    return combined_response(
        MDM_HUB_response=results.get(
            "MDM_HUB"
        ),
        iqvia_response=results.get(
            "iqvia"
        ),
    )
