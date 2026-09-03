"""Build the ORIEO search request used by the SBC flow.

This is the single production ORIEO transformation module.
The template and mappings below follow the connected SBC source exactly.
"""

from __future__ import annotations

import copy
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

FIRST_NAME = "hcp.firstName"
MIDDLE_NAME = "hcp.middleName"
LAST_NAME = "hcp.lastName"
ADDRESS_COUNTRY = "address.country"
ADDRESS_COUNTRY_CODE = "address.countryCode"


ORIEO_TEMPLATE: Dict[str, Any] = {
    "searchControls": {
        "maxRecordsToReturn": "20",
        "searchLevel": "Typical",
        "fileRecordLimit": "1000",
        "population": "brazil",
    },
    "data": {
        "searchRecord": {
            "firstName": "",
            "middleName": "",
            "fullName": "",
            "lastName": "",
            "X_vlkp_url": "",
            "X_infac360ls_department": "",
            "gender": {"Code": "", "Name": ""},
            "X_infac360ls_type": {"Code": "", "Name": ""},
            "X_hcp_status": {"Code": "", "Name": ""},
            "AlternateName": [{"AlternateName": ""}],
            "X_hcp_address": [
                {
                    "X_address_line_1": "",
                    "X_city": "",
                    "X_state": {"Code": "", "Name": ""},
                    "X_postal_code": "",
                    "X_country": {"Code": "BR", "Name": ""},
                    "X_primary_flag": "",
                    "X_address_type": {"Code": "", "Name": ""},
                    "X_address_status": {"Code": "", "Name": ""},
                }
            ],
            "X_infac360ls_Specialty": [
                {
                    "X_infac360ls_specialtyType": "",
                    "X_infac360ls_specialtyRank": {"Code": "", "Name": ""},
                    "X_specialty_status": {"Code": "", "Name": ""},
                }
            ],
            "X_infac360ls_License": [
                {
                    "X_infac360ls_licenseNumber": "",
                    "X_infac360ls_licenseType": "",
                }
            ],
            "X_infac360ls_dea": [{"X_infac360ls_deaNumber": ""}],
            "AlternateIdentifier": [
                {
                    "alternateIdentifierType": {"Code": "", "Name": ""},
                    "alternateIdentifierValue": "",
                }
            ],
            "Phone": [{"phoneNumber": ""}],
            "TaxDetail": [
                {
                    "taxNumber": "",
                    "taxNumberType": {"Code": "", "Name": ""},
                }
            ],
            "ElectronicAddress": [{"electronicAddress": ""}],
        }
    },
}


ORIEO_FIELD_MAPPING = (
    (FIRST_NAME, ("data", "searchRecord", "firstName")),
    (MIDDLE_NAME, ("data", "searchRecord", "middleName")),
    (LAST_NAME, ("data", "searchRecord", "lastName")),
    (
        "address.city",
        ("data", "searchRecord", "X_hcp_address", 0, "X_city"),
    ),
    (
        "address.primary",
        ("data", "searchRecord", "X_hcp_address", 0, "X_address_line_1"),
    ),
    (
        ADDRESS_COUNTRY_CODE,
        ("data", "searchRecord", "X_hcp_address", 0, "X_country", "Code"),
    ),
    (
        "address.longPostalCode",
        ("data", "searchRecord", "X_hcp_address", 0, "X_postal_code"),
    ),
    (
        "address.type",
        ("data", "searchRecord", "X_hcp_address", 0, "X_address_type", "Code"),
    ),
)

COUNTRY_CODE_TO_POPULATION = {
    "NL": "netherlands",
    "BE": "belgium",
}


def _set_nested(obj: Dict[str, Any], path: tuple[Any, ...], value: Any) -> None:
    current: Any = obj
    for index, key in enumerate(path):
        if index == len(path) - 1:
            current[key] = value
        else:
            current = current[key]


def transform_to_orieo(incoming: Dict[str, Any] | None) -> Dict[str, Any]:
    """Transform the incoming SBC payload into the ORIEO request."""

    incoming = incoming or {}
    output = copy.deepcopy(ORIEO_TEMPLATE)

    for source, path in ORIEO_FIELD_MAPPING:
        value = incoming.get(source, "")
        if isinstance(value, list):
            value = value[0] if value else ""
        _set_nested(output, path, value)
        logger.info("Mapped '%s' -> %s", source, path)

    population = incoming.get(ADDRESS_COUNTRY, "")
    if not population:
        country_code = str(
            incoming.get(ADDRESS_COUNTRY_CODE, "") or ""
        ).strip().upper()
        population = COUNTRY_CODE_TO_POPULATION.get(country_code, "")

    full_name = " ".join(
        filter(
            None,
            [
                incoming.get(FIRST_NAME, ""),
                incoming.get(MIDDLE_NAME, ""),
                incoming.get(LAST_NAME, ""),
            ],
        )
    )

    output["data"]["searchRecord"]["fullName"] = full_name
    output["searchControls"]["population"] = str(population).lower()

    return output


__all__ = [
    "COUNTRY_CODE_TO_POPULATION",
    "ORIEO_FIELD_MAPPING",
    "ORIEO_TEMPLATE",
    "transform_to_orieo",
]

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# No credentials are required in this transformation module.
# ORIEO endpoint/authentication values belong in the calling API/runtime config.
# ============================================================================

