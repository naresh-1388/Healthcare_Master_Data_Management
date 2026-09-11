"""Build the MDM_HUB search request used by the SBC flow.

This is the single production MDM_HUB transformation module.
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


MDM_HUB_TEMPLATE: Dict[str, Any] = {
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


MDM_HUB_FIELD_MAPPING = (
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
    """
    Set a value inside a nested dict/list structure given a path of
    keys/indices, creating no intermediate structure (callers are expected
    to have already built the parent containers along `path`).

    Args:
        obj: The root dict to write into (mutated in place).
        path: Sequence of keys/indices describing where to write, e.g.
            ("address", 0, "line1").
        value: The value to assign at the final path element.

    Returns:
        None. Mutates obj in place.
    """
    current: Any = obj
    for index, key in enumerate(path):
        if index == len(path) - 1:
            current[key] = value
        else:
            current = current[key]


def transform_to_mdm_hub(incoming: Dict[str, Any] | None) -> Dict[str, Any]:
    """Transform the incoming SBC payload into the MDM_HUB request."""

    incoming = incoming or {}
    output = copy.deepcopy(MDM_HUB_TEMPLATE)

    for source, path in MDM_HUB_FIELD_MAPPING:
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
    "MDM_HUB_FIELD_MAPPING",
    "MDM_HUB_TEMPLATE",
    "transform_to_mdm_hub",
]

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# No credentials are required in this transformation module.
# MDM_HUB endpoint/authentication values belong in the calling API/runtime config.
# ============================================================================

