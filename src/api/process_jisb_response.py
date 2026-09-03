"""
Transform JISB API response into the project response structure.
"""

from __future__ import annotations

import logging
from typing import Any


logger = logging.getLogger(__name__)


def process_jisb_response(response_json: dict[str, Any]) -> dict[str, Any]:
    """
    Process the JISB API response.

    Source response structure:
        response.results[]

    Output structure:
        {
            "searchResult": {
                "totalRecords": <count>,
                "records": [...]
            }
        }

    Notes:
        - Source field name `individualeId` is intentionally preserved.
        - Gender is intentionally returned as an empty string.
        - AlternateIdentifier structure is preserved from the source.
    """
    try:
        records = (
            response_json.get("response", {}).get("results", [])
            or []
        )

        transformed_records: list[dict[str, Any]] = []

        for record in records:
            individual = record.get("individual", {}) or {}
            workplace = record.get("workplace", {}) or {}

            first_name = individual.get("firstName") or ""
            middle_name = individual.get("middleName") or ""
            last_name = individual.get("lastName") or ""

            full_name = " ".join(
                filter(
                    None,
                    [
                        first_name,
                        middle_name,
                        last_name,
                    ],
                )
            )

            # Preserved from source implementation.
            gender = ""

            hcp_addresses: list[dict[str, Any]] = []

            workplace_addresses = (
                workplace.get("workplaceAddresses") or {}
            )

            for _, address_wrapper in workplace_addresses.items():
                address = (
                    address_wrapper.get("address") or {}
                )

                postal_reference = (
                    address.get("postalTownReference") or {}
                )

                subdivisions = (
                    postal_reference.get("subdivisions") or {}
                )

                country = (
                    subdivisions.get("COUNTRY", {}) or {}
                )

                address_entry = {
                    "Address Line 1": (
                        address.get("addressLongLabel") or ""
                    ),
                    "City": (
                        postal_reference.get("villageLabel") or ""
                    ),
                    "State": "",
                    "Primary Flag": (
                        address_wrapper.get(
                            "typeCodeCorporateLabel"
                        )
                        or ""
                    ),
                    "Postal Code": (
                        address.get("longPostalCode") or ""
                    ),
                    "Country": {
                        "Code": (
                            country.get("externalId") or ""
                        ),
                        "Name": (
                            country.get("longLocalizedLabel")
                            or ""
                        ),
                    },
                }

                hcp_addresses.append(address_entry)

            # Preserved exactly from source behavior.
            alternate_identifiers = [
                {
                    "Identifier Type": "",
                    "Identifier Value": "",
                },
                {
                    "Identifier Type": "",
                    "alternateIdentifierValue": "",
                },
            ]

            transformed_record = {
                "individualeId": (
                    individual.get("individualeId") or ""
                ),
                "First Name": first_name,
                "Full Name": full_name,
                "Last Name": last_name,
                "Gender": gender,
                "HCP Address": hcp_addresses,
                "AlternateIdentifier": alternate_identifiers,
            }

            transformed_records.append(transformed_record)

        return {
            "searchResult": {
                "totalRecords": len(transformed_records),
                "records": transformed_records,
            }
        }

    except Exception:
        logger.exception("Error processing JISB response")
        raise

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# No credentials, database names, schemas, or paths belong in this parser.
# It only transforms the JISB response structure supplied by the caller.
# ============================================================================

