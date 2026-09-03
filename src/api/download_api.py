"""
HCP MDM download API.

Supported lookup inputs:
    - ORIEOId
    - jisbId
    - externalId

externalId routing:
    - starts with "20" -> ORIEO ID
    - starts with "W"  -> JISB ID
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import boto3
import requests


BACKOFF = 2
RETRIES = 3
CONTENT_TYPE = "application/json"

API_KEY = os.environ["api_key"]
BASE_ORIEO_URL = os.environ["base_ORIEO_url"]  # USER: set your ORIEO base URL here via runtime env/Databricks secret.
JISB_URL = os.environ["jisb_url"]  # USER: set your JISB API URL here via runtime env/Databricks secret.

REGION_NAME = os.environ["region"]
SECRET_NAME = os.environ["secret_name"]  # USER: set your AWS Secrets Manager secret name here.

logger = logging.getLogger("Download_API")
logger.setLevel(logging.INFO)


def get_secret(
    secret_name: str,
    region_name: str = REGION_NAME,
) -> dict[str, Any]:
    """Retrieve API credentials from AWS Secrets Manager."""
    client = boto3.client(
        "secretsmanager",
        region_name=region_name,
    )

    try:
        response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as exc:
        raise Exception(
            f"Error retrieving secret: {str(exc)}"
        ) from exc

    if "SecretString" in response:
        return json.loads(response["SecretString"])

    return response["SecretBinary"]


_SECRET = get_secret(SECRET_NAME)

ORIEO_USERNAME = _SECRET.get("username")
ORIEO_PASSWORD = _SECRET.get("password")
JISB_USERNAME = _SECRET.get("jisb_username")
JISB_PASSWORD = _SECRET.get("jisb_password")


def validate_request_payload(
    payload: dict[str, Any] | None,
) -> list[dict[str, str]]:
    """Validate the lookup request."""
    errors: list[dict[str, str]] = []
    payload = payload or {}

    logger.info(
        "[VALIDATION] Starting lookup payload validation"
    )

    mdm_entity = (
        payload.get("mdmEntityType") or ""
    ).strip()

    if not mdm_entity:
        errors.append(
            {
                "field_path": "mdmEntityType",
                "error_type": "MISSING",
                "message": (
                    "mdmEntityType is required and must be 'HCP'"
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

    orieo_id = (
        payload.get("ORIEOId") or ""
    ).strip()

    jisb_id = (
        payload.get("jisbId") or ""
    ).strip()

    external_id = (
        payload.get("externalId") or ""
    ).strip()

    provided_ids = [
        value
        for value in (
            orieo_id,
            jisb_id,
            external_id,
        )
        if value
    ]

    if len(provided_ids) == 0:
        errors.append(
            {
                "field_path": (
                    "ORIEOId/jisbId/externalId"
                ),
                "error_type": "MISSING",
                "message": (
                    "One of ORIEOId, jisbId or externalId "
                    "must be provided"
                ),
            }
        )
    elif len(provided_ids) > 1:
        errors.append(
            {
                "field_path": (
                    "ORIEOId/jisbId/externalId"
                ),
                "error_type": "INVALID",
                "message": (
                    "Only one of ORIEOId, jisbId or externalId "
                    "should be provided"
                ),
            }
        )

    if errors:
        logger.error(
            "[VALIDATION] Payload validation failed: %s",
            errors,
        )
    else:
        logger.info(
            "[VALIDATION] Payload validation passed successfully"
        )

    return errors


def call_api_with_retry(
    payload: dict[str, Any],
    username: str,
    password: str,
    url: str,
    retries: int,
    backoff: int,
) -> dict[str, Any]:
    """
    Fetch either an ORIEO record or a JISB record with retry logic.
    """
    orieo_id = (
        payload.get("ORIEOId") or ""
    ).strip()

    jisb_id = (
        payload.get("jisbId") or ""
    ).strip()

    lookup_type = (
        "ORIEO"
        if orieo_id
        else "jisb"
    )

    for attempt in range(1, retries + 1):
        try:
            logger.info(
                "Attempt %s for %s",
                attempt,
                lookup_type,
            )

            if orieo_id:
                logger.info(
                    "Fetching ORIEO data for ID: %s",
                    orieo_id,
                )

                # Endpoint intentionally preserved from supplied
                # source as a deployment-supplied value.
                session_response = requests.request(
                    "POST",
                    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
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

                session_id = json.loads(
                    session_response.text
                )["userinfo"]["sessionId"]

                headers = {
                    "Content-Type": CONTENT_TYPE,
                    "IDS-SESSION-ID": session_id,
                }

                orieo_url = (
                    f"{url}/{orieo_id}"
                    "?_showContentMeta=true"
                )

                logger.info(
                    "ORIEO url: %s",
                    orieo_url,
                )

                response = requests.get(
                    url=orieo_url,
                    headers=headers,
                    timeout=30,
                )

                logger.info(
                    "[LOOKUP] ORIEO response status: %s",
                    response.status_code,
                )

                if response.status_code != 200:
                    raise RuntimeError(
                        f"ORIEO API failed: {response.text}"
                    )

                return response.json()

            logger.info(
                "[LOOKUP] Fetching jisb data for ID: %s",
                jisb_id,
            )

            jisb_headers = {
                "Content-Type": CONTENT_TYPE,
                "Authorization": (
                    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
                ),
            }

            if not jisb_id or len(jisb_id) < 3:
                raise ValueError(
                    "Invalid jisbId: cannot derive codBase"
                )

            cod_base = jisb_id[:3]

            jisb_payload = {
                "resultSize": "10",
                "entityType": "Activity",
                "codBases": [cod_base],
                "fields": [
                    {
                        "method": "EXACT",
                        "name": "individual.individualId",
                        "values": [jisb_id],
                    }
                ],
            }

            response = requests.request(
                "POST",
                JISB_URL,
                headers=jisb_headers,
                data=json.dumps(jisb_payload),
                timeout=30,
            )

            logger.info(
                "jisb response status: %s",
                response.status_code,
            )

            if response.status_code != 200:
                raise RuntimeError(
                    f"jisb API failed: {response.text}"
                )

            return response.json()

        except Exception:
            logger.exception(
                "Attempt %s failed",
                attempt,
            )

            if attempt == retries:
                logger.error(
                    "All retry attempts failed"
                )
                raise

            time.sleep(backoff ** attempt)

    raise RuntimeError(
        "API call failed without returning a response"
    )


def transform_orieo_download_response(
    raw: dict[str, Any] | None,
) -> dict[str, Any]:
    """Transform an ORIEO download response."""
    try:
        raw = raw or {}

        phones = []

        for phone in raw.get("X_phone") or []:
            phones.append(
                {
                    "Phone Number": phone.get(
                        "X_phone_number",
                        "",
                    ),
                    "Phone Number Extension": phone.get(
                        "X_phone_number_extension",
                        "",
                    ),
                    "National Format": phone.get(
                        "nationalFormat",
                        "",
                    ),
                    "International Format": phone.get(
                        "internationalFormat",
                        "",
                    ),
                    "ISO": phone.get(
                        "X_iso",
                        "",
                    ),
                    "Phone Status": phone.get(
                        "X_phone_status",
                        "",
                    ),
                    "Primary Phone": phone.get(
                        "X_primary_phone",
                        "",
                    ),
                    "Phone Type": {
                        "Code": (
                            phone.get("X_phone_type") or {}
                        ).get("Code", ""),
                        "Name": (
                            phone.get("X_phone_type") or {}
                        ).get("Name", ""),
                    },
                    "Phone Usage Type": {
                        "Code": (
                            phone.get(
                                "X_phone_usage_type"
                            )
                            or {}
                        ).get("Code", ""),
                        "Name": (
                            phone.get(
                                "X_phone_usage_type"
                            )
                            or {}
                        ).get("Name", ""),
                    },
                    "Effective Start Date": phone.get(
                        "X_effective_start_date",
                        "",
                    ),
                    "Effective End Date": phone.get(
                        "X_effective_end_date",
                        "",
                    ),
                }
            )

        alternate_identifiers = []

        for alternate_identifier in (
            raw.get("AlternateIdentifier") or []
        ):
            alternate_identifiers.append(
                {
                    "Identifier Value": (
                        alternate_identifier.get(
                            "AlternateIdentifierValue",
                            "",
                        )
                    ),
                    "Identifier Type": {
                        "Code": (
                            alternate_identifier.get(
                                "AlternateIdentifierType"
                            )
                            or {}
                        ).get("Code", ""),
                        "Name": (
                            alternate_identifier.get(
                                "AlternateIdentifierType"
                            )
                            or {}
                        ).get("Name", ""),
                    },
                }
            )

        addresses = []

        for address in raw.get("X_hcp_address") or []:
            addresses.append(
                {
                    "Address Line 1": address.get(
                        "X_address_line_1",
                        "",
                    ),
                    "Address Line 2": address.get(
                        "X_address_line_2",
                        "",
                    ),
                    "Address Line 3": address.get(
                        "X_address_line_3",
                        "",
                    ),
                    "City": address.get(
                        "X_city",
                        "",
                    ),
                    "Postal Code": address.get(
                        "X_postal_code",
                        "",
                    ),
                    "Primary Flag": address.get(
                        "X_primary_flag",
                        False,
                    ),
                    "Address Rank": address.get(
                        "X_AddressRank",
                        "",
                    ),
                    "Country": {
                        "Code": (
                            address.get("X_country") or {}
                        ).get("Code", ""),
                        "Name": (
                            address.get("X_country") or {}
                        ).get("Name", ""),
                    },
                    "State/Province": {
                        "Code": (
                            address.get("X_state") or {}
                        ).get("Code", ""),
                        "Name": (
                            address.get("X_state") or {}
                        ).get("Name", ""),
                    },
                    "Address Type": {
                        "Code": (
                            address.get(
                                "X_address_type"
                            )
                            or {}
                        ).get("Code", ""),
                        "Name": (
                            address.get(
                                "X_address_type"
                            )
                            or {}
                        ).get("Name", ""),
                    },
                    "Address Status": {
                        "Code": (
                            address.get(
                                "X_address_status"
                            )
                            or {}
                        ).get("Code", ""),
                        "Name": (
                            address.get(
                                "X_address_status"
                            )
                            or {}
                        ).get("Name", ""),
                    },
                    "Longitude": address.get(
                        "X_longitude",
                        "",
                    ),
                    "Latitude": address.get(
                        "X_latitude",
                        "",
                    ),
                    "Effective Date": address.get(
                        "X_EffectiveDate",
                        "",
                    ),
                    "End Date": address.get(
                        "X_EndDate",
                        "",
                    ),
                }
            )

        specialties = []

        for specialty in (
            raw.get("X_infac360ls_Specialty") or []
        ):
            specialties.append(
                {
                    "Specialty Type": specialty.get(
                        "X_infac360ls_specialtyType",
                        "",
                    ),
                    "Specialty Class": (
                        specialty.get(
                            "X_infac360ls_specialtyClass"
                        )
                        or {}
                    ).get("Name", ""),
                    "Specialty Rank": {
                        "Code": (
                            specialty.get(
                                "X_infac360ls_specialtyRank"
                            )
                            or {}
                        ).get("Code", ""),
                        "Name": (
                            specialty.get(
                                "X_infac360ls_specialtyRank"
                            )
                            or {}
                        ).get("Name", ""),
                    },
                    "Specialty Status": {
                        "Code": (
                            specialty.get(
                                "X_specialty_status"
                            )
                            or {}
                        ).get("Code", ""),
                        "Name": (
                            specialty.get(
                                "X_specialty_status"
                            )
                            or {}
                        ).get("Name", ""),
                    },
                    "Global Specialty": {
                        "Code": (
                            specialty.get(
                                "X_global_specialty"
                            )
                            or {}
                        ).get("Code", ""),
                        "Name": (
                            specialty.get(
                                "X_global_specialty"
                            )
                            or {}
                        ).get("Name", ""),
                    },
                    "Group Specialty": {
                        "Code": (
                            specialty.get(
                                "X_group_specialty"
                            )
                            or {}
                        ).get("Code", ""),
                        "Name": (
                            specialty.get(
                                "X_group_specialty"
                            )
                            or {}
                        ).get("Name", ""),
                    },
                }
            )

        alternate_names = []

        for alternate_name in (
            raw.get("AlternateName") or []
        ):
            alternate_names.append(
                {
                    "Alternate Name": alternate_name.get(
                        "AlternateName",
                        "",
                    ),
                    "Alternate Name Type": (
                        alternate_name.get(
                            "alternateNameType"
                        )
                        or {}
                    ).get("Name", ""),
                    "Alternate Name Status": (
                        alternate_name.get(
                            "X_alternate_name_status"
                        )
                        or {}
                    ).get("Name", ""),
                }
            )

        licenses = []

        for license_data in (
            raw.get("X_infac360ls_License") or []
        ):
            licenses.append(
                {
                    "License Number": license_data.get(
                        "X_infac360ls_licenseNumber",
                        "",
                    ),
                    "License Type": license_data.get(
                        "X_infac360ls_licenseType",
                        "",
                    ),
                }
            )

        dea_list = []

        for dea in (
            raw.get("X_infac360ls_dea") or []
        ):
            dea_list.append(
                {
                    "DEA Number": dea.get(
                        "X_infac360ls_deaNumber",
                        "",
                    )
                }
            )

        tax_details = []

        for tax in raw.get("TaxDetail") or []:
            tax_details.append(
                {
                    "Tax Number": tax.get(
                        "taxNumber",
                        "",
                    ),
                    "Tax Number Type": {
                        "Code": (
                            tax.get("taxNumberType") or {}
                        ).get("Code", ""),
                        "Name": (
                            tax.get("taxNumberType") or {}
                        ).get("Name", ""),
                    },
                }
            )

        emails = []

        for email in raw.get("X_email") or []:
            emails.append(
                {
                    "Email": email.get(
                        "X_email",
                        "",
                    ),
                    "Email Usage Type": email.get(
                        "X_email_usage_type",
                        "",
                    ),
                    "Email Status": email.get(
                        "X_status",
                        "",
                    ),
                    "Effective Start Date": email.get(
                        "X_effective_start_date",
                        "",
                    ),
                    "Effective End Date": email.get(
                        "X_effective_end_date",
                        "",
                    ),
                }
            )

        third_party_id = raw.get(
            "X_third_party_id",
            "",
        )

        meta = raw.get("_meta") or {}

        transformed_meta = {
            "id": meta.get("id", ""),
            "businessId": meta.get("businessId", ""),
            "businessEntity": meta.get(
                "businessEntity",
                "",
            ),
            "createdBy": meta.get(
                "createdBy",
                "",
            ),
            "creationDate": meta.get(
                "creationDate",
                "",
            ),
            "updatedBy": meta.get(
                "updatedBy",
                "",
            ),
            "lastUpdatedDate": meta.get(
                "lastUpdatedDate",
                "",
            ),
            "state": meta.get(
                "state",
                "",
            ),
            "consolidation": meta.get(
                "consolidation",
                "",
            ),
            "searchIndex": meta.get(
                "searchIndex",
                "",
            ),
            "validation": meta.get(
                "validation",
                "",
            ),
            "sourcePrimaryKey": third_party_id,
        }

        data = {
            "Title": raw.get(
                "X_jisb_title",
                "",
            ),
            "First Name": raw.get(
                "firstName",
                "",
            ),
            "Middle Name": raw.get(
                "middleName",
                "",
            ),
            "Full Name": raw.get(
                "fullName",
                "",
            ),
            "Last Name": raw.get(
                "lastName",
                "",
            ),
            "HCP Institution Name": raw.get(
                "X_infac360ls_department",
                "",
            ),
            "VLKP URL": raw.get(
                "X_vlkp_url",
                "",
            ),
            "Transparency Reporting Name": raw.get(
                "X_transparency_reporting_name",
                "",
            ),
            "Third Party Id": third_party_id,
            "Website": raw.get(
                "X_infac360ls_website",
                "",
            ),
            "Gender": {
                "Code": (
                    raw.get("gender") or {}
                ).get("Code", ""),
                "Name": (
                    raw.get("gender") or {}
                ).get("Name", ""),
            },
            "HCP Status": {
                "Code": (
                    raw.get("X_hcp_status") or {}
                ).get("Code", ""),
                "Name": (
                    raw.get("X_hcp_status") or {}
                ).get("Name", ""),
            },
            "Type": {
                "Code": (
                    raw.get(
                        "X_infac360ls_type"
                    )
                    or {}
                ).get("Code", ""),
                "Name": (
                    raw.get(
                        "X_infac360ls_type"
                    )
                    or {}
                ).get("Name", ""),
            },
            "Prefix Name": {
                "Code": (
                    raw.get("prefixName") or {}
                ).get("Code", ""),
                "Name": (
                    raw.get("prefixName") or {}
                ).get("Name", ""),
            },
            "Suffix Name": {
                "Code": (
                    raw.get("suffixName") or {}
                ).get("Code", ""),
                "Name": (
                    raw.get("suffixName") or {}
                ).get("Name", ""),
            },
            "Birth Date": raw.get(
                "birthDate",
                "",
            ),
            "HCP Address": addresses,
            "Alternate Identifier": alternate_identifiers,
            "Specialty": specialties,
            "Alternate Name": alternate_names,
            "Phone": phones,
            "License": licenses,
            "DEA": dea_list,
            "Tax Details": tax_details,
            "Email": emails,
            "_meta": transformed_meta,
        }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": CONTENT_TYPE,
            },
            "body": json.dumps(data),
        }

    except Exception:
        logger.exception(
            "[TRANSFORM_ORIEO] Failed"
        )
        raise


def extract_error_details(
    last_error: Any,
    api_name: str = "",
) -> dict[str, Any]:
    """Extract structured error information."""
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

        status_code = (
            error_json.get("status")
            or first_error.get(
                "httpStatusCode"
            )
        )

        message = (
            error_json.get("errorSummary")
            or first_error.get("message")
            or str(error_json)
        )

        extracted = {
            "status_code": status_code,
            "error_code": error_json.get(
                "errorCode"
            ),
            "message": (
                f"[{api_name}] {message}"
                if api_name
                else message
            ),
            "details": [],
        }

        if first_detail or first_error:
            extracted["details"].append(
                {
                    "code": first_detail.get(
                        "code"
                    ),
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
            )

        extracted["retryable"] = True

        return extracted

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
    error_info: dict[str, Any],
    request_id: str = "",
) -> dict[str, Any]:
    """Build the standard structured API error response."""
    try:
        return {
            "status": "error",
            "statusCode": (
                error_info.get("status_code")
                or 500
            ),
            "error": {
                "code": (
                    error_info.get("error_code")
                    or ""
                ),
                "message": (
                    error_info.get("message")
                    or ""
                ),
                "details": error_info.get(
                    "details",
                    [],
                ),
                "retryable": error_info.get(
                    "retryable",
                    False,
                ),
                "requestId": request_id,
            },
        }

    except Exception as exc:
        logger.exception(
            "Error building structured error response"
        )

        return {
            "status": "error",
            "statusCode": 500,
            "error": {
                "code": "UNKNOWN_ERROR",
                "message": str(exc),
                "details": [],
                "retryable": False,
                "requestId": request_id,
            },
        }


def transform_jisb_to_orieo_post(
    jisb_response: dict[str, Any],
) -> dict[str, Any]:
    """Transform the JISB response into an ORIEO POST payload."""
    try:
        results = (
            jisb_response.get("response") or {}
        ).get("results", [])

        first_result = (
            results[0]
            if results
            else {}
        )

        individual = (
            first_result.get("individual")
            or {}
        )

        current_timestamp = int(
            time.time() * 1000
        )

        field_data: list[dict[str, Any]] = []

        first_name = individual.get(
            "firstName"
        ) or ""

        middle_name = individual.get(
            "middleName"
        ) or ""

        last_name = individual.get(
            "lastName"
        ) or ""

        full_name = (
            f"{first_name} "
            f"{middle_name or ''} "
            f"{last_name}"
        ).strip()

        transparency_name = (
            individual.get("usualFirstName")
            or full_name
        )

        gender = {
            "Code": (
                individual.get("genderCode")
                or ""
            )
        }

        hcp_type = {
            "Code": (
                individual.get("typeCode")
                or ""
            )
        }

        hcp_status = {
            "Code": (
                individual.get("stateCode")
                or ""
            )
        }

        prefix = {
            "Code": (
                individual.get("prefixNameCode")
                or ""
            )
        }

        jisb_title = {
            "Code": (
                individual.get("titleCode")
                or ""
            )
        }

        specialties = []

        ada = individual.get("ada") or {}

        for ada_key, ada_value in ada.items():
            if ada_value.get("listCode") != "SP":
                continue

            rank_suffix = (
                ada_key.split(",")[1]
                if "," in ada_key
                else "1"
            )

            specialty_object = {
                "X_infac360ls_specialtyClass": {
                    "Code": ada_value.get(
                        "code",
                        "",
                    ),
                    "Name": ada_value.get(
                        "codeCorporateLabel",
                        "",
                    ),
                },
                "X_infac360ls_specialtyRank": None,
            }

            group_specialty = ada.get(
                f"1GS,{rank_suffix}"
            )

            if (
                group_specialty
                and group_specialty.get("code")
            ):
                specialty_object[
                    "X_group_specialty"
                ] = {
                    "Code": group_specialty.get(
                        "code"
                    )
                }

            global_specialty = ada.get(
                f"1SP,{rank_suffix}"
            )

            if (
                global_specialty
                and global_specialty.get("code")
            ):
                specialty_object[
                    "X_global_specialty"
                ] = {
                    "Code": global_specialty.get(
                        "code"
                    )
                }

            specialties.append(
                specialty_object
            )

        alternate_identifiers = [
            {
                "alternateIdentifierValue": (
                    individual.get(
                        "individualId"
                    )
                ),
                "alternateIdentifierType": {
                    "Code": "jisb ID",
                    "Name": "jisb ID",
                },
            }
        ]

        for key_type, key_object in (
            individual.get(
                "externalKeys",
                {},
            )
            or {}
        ).items():
            if (
                key_type in ("111", "121")
                and isinstance(
                    key_object,
                    dict,
                )
            ):
                alternate_identifiers.append(
                    {
                        "alternateIdentifierValue": (
                            key_object.get(
                                "value",
                                "",
                            )
                        ),
                        "alternateIdentifierType": {
                            "Code": key_type,
                            "Name": key_object.get(
                                "typeLabel",
                                "",
                            ),
                        },
                    }
                )

        addresses = []
        seen_addresses: set[str] = set()

        for result in results:
            workplace = (
                result.get("workplace")
                or {}
            )

            activity = (
                result.get("activity")
                or {}
            )

            workplace_addresses = (
                workplace.get(
                    "workplaceAddresses"
                )
                or {}
            )

            is_main_activity = bool(
                activity.get(
                    "isMainActivity",
                    False,
                )
            )

            for key, address_object in (
                workplace_addresses.items()
            ):
                address = (
                    address_object.get(
                        "address"
                    )
                    or {}
                )

                address_id = (
                    f"{workplace.get('workplaceId', '')}"
                    f"_{key}"
                )

                if address_id in seen_addresses:
                    continue

                seen_addresses.add(
                    address_id
                )

                country_reference = (
                    address.get(
                        "postalTownReference",
                        {},
                    )
                )

                subdivisions = (
                    country_reference.get(
                        "subdivisions",
                        {},
                    )
                )

                country_object = (
                    subdivisions.get(
                        "COUNTRY",
                        {},
                    )
                )

                state_object = (
                    subdivisions.get(
                        "SUB.3",
                        {},
                    )
                )

                segmentations = (
                    address.get(
                        "segmentations",
                        {},
                    )
                )

                mapped_address = {
                    "id": address_id,
                    "x_address_line_1": address.get(
                        "addressLongLabel",
                        "",
                    ),
                    "x_address_line_2": address.get(
                        "extensionLabel",
                        "",
                    ),
                    "x_city": (
                        country_reference.get(
                            "villageLabel"
                        )
                        or country_reference.get(
                            "dispatchLabel",
                            "",
                        )
                    ),
                    "x_postal_code": address.get(
                        "longPostalcode",
                        "",
                    ),
                    "x_primary_flag": (
                        is_main_activity
                    ),
                    "x_country": {
                        "Code": country_reference.get(
                            "country",
                            "",
                        ),
                        "Name": country_object.get(
                            "longLocalizedLabel",
                            "",
                        ),
                    },
                    "x_state": {
                        "Code": state_object.get(
                            "longLocalizedLabel",
                            "",
                        ),
                        "Name": state_object.get(
                            "longLocalizedLabel",
                            "",
                        ),
                    },
                    "x_address_type": {
                        "Code": address_object.get(
                            "typeCode",
                            "",
                        ),
                        "Name": address_object.get(
                            "typeCodeCorporateLabel",
                            "",
                        ),
                    },
                    "x_brick_code": (
                        segmentations
                        .get("UG1", {})
                        .get("brickEid", "")
                    ),
                }

                addresses.append(
                    mapped_address
                )

                field_data.append(
                    {
                        "fieldRef": (
                            f"x_hcp_address"
                            f"[{address_id}]"
                        ),
                        "sourceLastUpdateDate": (
                            current_timestamp
                        ),
                    }
                )

        qualification = []

        if individual.get("thesisYear"):
            qualification.append(
                {
                    "X_infac360ls_degreeYear": (
                        individual.get(
                            "thesisYear"
                        )
                    )
                }
            )

        phones = []

        telephones = (
            first_result
            .get("workplace", {})
            .get("telephones", {})
        )

        for key, phone_object in (
            telephones.items()
        ):
            phone_id = (
                f"ph_{key.replace(',', '_')}"
            )

            label = (
                phone_object.get(
                    "typeCorporateLabel"
                )
                or ""
            ).lower()

            if "fax" in label:
                phone_type = {
                    "Code": "Fax",
                    "Name": "Fax",
                }
            else:
                phone_type = {
                    "Code": "Mobile",
                    "Name": "Mobile",
                }

            phones.append(
                {
                    "id": phone_id,
                    "phoneNumber": phone_object.get(
                        "callNumberForDisplay",
                        "",
                    ),
                    "iso": first_result.get(
                        "country",
                        "",
                    ),
                    "phoneType": phone_type,
                }
            )

            field_data.append(
                {
                    "fieldRef": (
                        f"x_phone[{phone_id}]"
                    ),
                    "sourceLastUpdateDate": (
                        current_timestamp
                    ),
                }
            )

        field_data.append(
            {
                "fieldRef": "/",
                "sourceLastUpdateDate": (
                    current_timestamp
                ),
            }
        )

        return {
            "X_transparency_reporting_name": (
                transparency_name
            ),
            "firstName": first_name,
            "middleName": middle_name,
            "lastName": last_name,
            "fullName": full_name,
            "gender": gender,
            "X_infac360ls_type": hcp_type,
            "X_hcp_status": hcp_status,
            "X_jisb_title": jisb_title,
            "prefixName": prefix,
            "AlternateName": [],
            "X_hcp_address": addresses,
            "Phone": phones,
            "X_infac360ls_Specialty": specialties,
            "Qualification": qualification,
            "X_infac360ls_License": [],
            "X_infac360ls_dea": [],
            "AlternateIdentifier": alternate_identifiers,
            "ElectronicAddress": [],
            "_contentMeta": {
                "trust": {
                    "fieldData": field_data
                }
            },
        }

    except Exception:
        logger.exception(
            "[TRANSFORM] Failed"
        )
        raise


def post_orieo_entity(
    payload: dict[str, Any],
    jisb_id: str,
    username: str,
    password: str,
    base_url: str,
) -> dict[str, Any]:
    """Create an ORIEO entity using a JISB source key."""
    try:
        session_response = requests.request(
            "POST",
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
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

        session_id = json.loads(
            session_response.text
        )["userinfo"]["sessionId"]

        headers = {
            "Content-Type": CONTENT_TYPE,
            "IDS-SESSION-ID": session_id,
        }

        orieo_post_url = (
            f"{base_url}"
            "?sourceSystem=jisb"
            f"&sourcePKey={jisb_id}"
            "&resolveCrosswalk=false"
        )

        response = requests.request(
            "POST",
            url=orieo_post_url,
            headers=headers,
            data=json.dumps(payload),
            timeout=30,
        )

        logger.info(
            "Status of creating record in MDM: %s",
            response.status_code,
        )

        if response.status_code not in (
            200,
            201,
            202,
        ):
            raise RuntimeError(
                "Record creation in MDM failed: "
                f"{response.text}"
            )

        return response.json()

    except Exception:
        logger.exception(
            "Record creation Failed"
        )
        raise


def authenticate_request(
    headers: dict[str, Any] | None,
) -> tuple[bool, str]:
    """Validate the Bearer API key."""
    headers_lower = {
        key.lower(): value
        for key, value in (headers or {}).items()
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
        token_type, token = auth_header.split()
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


def lambda_handler(
    event: dict[str, Any],
    context: Any,
) -> dict[str, Any]:
    """AWS Lambda entry point for HCP lookup."""
    logger.info(
        "STARTED getting the details"
    )

    payload = event.get("body")
    headers = event.get("headers", {})

    is_authenticated, message = (
        authenticate_request(headers)
    )

    logger.info(message)

    if not is_authenticated:
        return {
            "statusCode": 401,
            "body": message,
        }

    if isinstance(payload, str):
        payload = json.loads(payload)
    else:
        payload = payload or {}

    logger.info(
        "Incoming payload: %s",
        payload,
    )

    errors = validate_request_payload(
        payload
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
            request_id="",
        )

    logger.info(
        "Payload validation successful"
    )

    orieo_id = (
        payload.get("ORIEOId") or ""
    ).strip()

    jisb_id = (
        payload.get("jisbId") or ""
    ).strip()

    external_id = (
        payload.get("externalId") or ""
    ).strip()

    if external_id:
        if external_id.startswith("20"):
            logger.info(
                "External ID %s identified as ORIEO ID",
                external_id,
            )

            orieo_id = external_id
            payload["ORIEOId"] = external_id
            payload["jisbId"] = ""

        elif external_id.upper().startswith("W"):
            logger.info(
                "External ID %s identified as jisb ID",
                external_id,
            )

            jisb_id = external_id
            payload["jisbId"] = external_id
            payload["ORIEOId"] = ""

        else:
            logger.error(
                "Invalid externalId received: %s",
                external_id,
            )

            error_info = extract_error_details(
                {
                    "errorSummary": (
                        "externalId must start with "
                        "'20' (ORIEO ID) or 'W' (jisb ID)"
                    )
                },
                api_name="VALIDATION",
            )

            return build_structured_error_response(
                error_info,
                request_id="",
            )

    if jisb_id:
        try:
            logger.info(
                "Fetching details from jisb for ID: %s",
                jisb_id,
            )

            jisb_response = call_api_with_retry(
                payload,
                JISB_USERNAME,
                JISB_PASSWORD,
                JISB_URL,
                RETRIES,
                BACKOFF,
            )

            logger.info(
                "jisb fetch successful"
            )

            orieo_payload = (
                transform_jisb_to_orieo_post(
                    jisb_response
                )
            )

            orieo_post_response = (
                post_orieo_entity(
                    payload=orieo_payload,
                    jisb_id=jisb_id,
                    username=ORIEO_USERNAME,
                    password=ORIEO_PASSWORD,
                    base_url=BASE_ORIEO_URL,
                )
            )

            logger.info(
                "ORIEO POST successful"
            )

            orieo_id_generated = (
                orieo_post_response.get(
                    "businessId"
                )
            )

            if not orieo_id_generated:
                raise RuntimeError(
                    "ORIEO ID not returned after POST"
                )

            logger.info(
                "Generated ORIEO ID: %s",
                orieo_id_generated,
            )

            orieo_get_payload = {
                "ORIEOId": orieo_id_generated
            }

            orieo_get_response = (
                call_api_with_retry(
                    orieo_get_payload,
                    ORIEO_USERNAME,
                    ORIEO_PASSWORD,
                    BASE_ORIEO_URL,
                    RETRIES,
                    BACKOFF,
                )
            )

            logger.info(
                "ORIEO GET successful"
            )

            return transform_orieo_download_response(
                orieo_get_response
            )

        except Exception as exc:
            logger.exception(
                "jisb->ORIEO post Failed"
            )

            error_info = extract_error_details(
                exc,
                api_name="jisb_ORIEO_FLOW",
            )

            return build_structured_error_response(
                error_info,
                request_id="",
            )

    if orieo_id:
        try:
            logger.info(
                "Fetching details from ORIEO for ID: %s",
                orieo_id,
            )

            response = call_api_with_retry(
                payload,
                ORIEO_USERNAME,
                ORIEO_PASSWORD,
                BASE_ORIEO_URL,
                RETRIES,
                BACKOFF,
            )

            logger.info(
                "Details fetched successfully"
            )

            return transform_orieo_download_response(
                response
            )

        except Exception as exc:
            logger.exception(
                "[LOOKUP] API call failed"
            )

            error_info = extract_error_details(
                exc,
                api_name="ORIEO_FETCH",
            )

            return build_structured_error_response(
                error_info,
                request_id="",
            )

    # Validation normally prevents this path.
    return build_structured_error_response(
        {
            "status_code": 400,
            "error_code": "INVALID_REQUEST",
            "message": "No valid lookup identifier provided",
            "details": [],
            "retryable": False,
        },
        request_id="",
    )

# ============================================================================
# USER CONFIGURATION - SET THESE IN THE RUNTIME ENVIRONMENT / AWS
# ============================================================================
# 1) base_ORIEO_url : ORIEO base API URL. Set in Databricks secret/env config.
# 2) jisb_url       : JISB API URL. Set in Databricks secret/env config.
# 3) secret_name    : AWS Secrets Manager secret name containing API credentials.
# 4) AWS region     : configure the Databricks/AWS runtime region used by boto3.
# 5) API_KEY        : configure the inbound API bearer key in the runtime; do NOT hardcode it.
# 6) Do not put usernames/passwords directly in this file. Put them in the AWS
#    Secrets Manager secret using the source-defined keys consumed above.
# 7) If ORIEO/JISB endpoint paths differ by environment, change only the
#    environment variables, not the transformation logic below.
# ============================================================================

