"""
ORIEO response transformation.

Source of truth:
    process_orieo_response.py

The function filters ORIEO records using the supplied match score
and transforms accepted records into the response structure used
by the SBC flow.
"""

from __future__ import annotations

from typing import Any, Dict, List


class ORIEOResponseProcessingError(Exception):
    """Raised when an ORIEO response cannot be processed."""


def _get_records(response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(response_json, dict):
        raise ORIEOResponseProcessingError(
            "ORIEO response must be a dictionary."
        )

    search_result = response_json.get("searchResult") or {}

    if not isinstance(search_result, dict):
        raise ORIEOResponseProcessingError(
            "ORIEO response contains an invalid searchResult."
        )

    records = search_result.get("records") or []

    if not isinstance(records, list):
        raise ORIEOResponseProcessingError(
            "ORIEO searchResult.records must be a list."
        )

    return records


def process_orieo_response(
    response_json: Dict[str, Any],
    match_score: int,
) -> Dict[str, Any]:
    """
    Filter and transform ORIEO records.

    Records with score below match_score are excluded.
    """

    try:
        records = _get_records(response_json)

        filtered_records: List[Dict[str, Any]] = []

        for record in records:

            if not isinstance(record, dict):
                continue

            meta = record.get("_meta") or {}
            data = record.get("data") or {}

            score = meta.get("score", 0)

            if score < match_score:
                continue

            addresses = data.get("X_hcp_address") or []

            if not isinstance(addresses, list):
                addresses = []

            alternate_identifiers = (
                data.get("AlternateIdentifier") or []
            )

            if not isinstance(alternate_identifiers, list):
                alternate_identifiers = []

            transformed = {
                "orieoid": meta.get(
                    "businessId",
                    "",
                ),
                "score": score,
                "First Name": data.get(
                    "firstName",
                    "",
                ),
                "Full Name": data.get(
                    "fullName",
                    "",
                ),
                "Last Name": data.get(
                    "lastName",
                    "",
                ),
                "Gender": (
                    data.get("gender") or {}
                ).get(
                    "Name",
                    "",
                ),
                "HCP Address": [
                    {
                        "Address Line 1": (
                            address.get(
                                "x_address_line_1",
                                "",
                            )
                        ),
                        "City": (
                            address.get(
                                "x_city",
                                "",
                            )
                        ),
                        "State": "",
                        "Primary Flag": (
                            address.get(
                                "x_primary_flag",
                                "",
                            )
                        ),
                        "Postal Code": (
                            address.get(
                                "x_postal_code",
                                "",
                            )
                        ),
                        "Country": (
                            address.get(
                                "x_country",
                                {},
                            )
                        ),
                    }
                    for address in addresses
                    if isinstance(address, dict)
                ],
                "AlternateIdentifier": [
                    {
                        "Identifier Type": (
                            identifier.get(
                                "alternateIdentifierType",
                                {},
                            ) or {}
                        ).get(
                            "Name",
                            "",
                        ),
                        "Identifier Value": (
                            identifier.get(
                                "alternateIdentifierValue",
                                "",
                            )
                        ),
                    }
                    for identifier in alternate_identifiers
                    if isinstance(identifier, dict)
                ],
            }

            filtered_records.append(transformed)

        return {
            "searchResult": {
                "totalRecords": len(
                    filtered_records
                ),
                "records": filtered_records,
            }
        }

    except ORIEOResponseProcessingError:
        raise

    except Exception as exc:
        raise ORIEOResponseProcessingError(
            f"Error processing ORIEO response: {exc}"
        ) from exc


__all__ = [
    "ORIEOResponseProcessingError",
    "process_orieo_response",
]

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# No credentials, database names, schemas, or paths belong in this parser.
# It only transforms the ORIEO response structure supplied by the caller.
# ============================================================================

