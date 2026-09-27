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

# HCO field constants
HCO_NAME = "hco.organizationName"
HCO_TYPE = "hco.organizationType"
HCO_COUNTRY = "hco.country"


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
            "X_informatica_department": "",
            "gender": {"Code": "", "Name": ""},
            "X_informatica_type": {"Code": "", "Name": ""},
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
            "X_informatica_Specialty": [
                {
                    "X_informatica_specialtyType": "",
                    "X_informatica_specialtyRank": {"Code": "", "Name": ""},
                    "X_specialty_status": {"Code": "", "Name": ""},
                }
            ],
            "X_informatica_License": [
                {
                    "X_informatica_licenseNumber": "",
                    "X_informatica_licenseType": "",
                }
            ],
            "X_informatica_dea": [{"X_informatica_deaNumber": ""}],
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


# HCO MDM Hub template -- organization-shaped search record.
# HCO entities use organization fields (name, type, address, phone, etc.)
# instead of individual fields (firstName, lastName, etc.) used by HCP.
HCO_MDM_HUB_TEMPLATE: Dict[str, Any] = {
    "searchControls": {
        "maxRecordsToReturn": "20",
        "searchLevel": "Typical",
        "fileRecordLimit": "1000",
        "population": "brazil",
    },
    "data": {
        "searchRecord": {
            "HCO_Name": "",
            "HCO_Type": {"Code": "", "Name": ""},
            "HCO_Subtype": "",
            "Status": {"Code": "", "Name": ""},
            "Official_Name": "",
            "Transparency_Reporting_Name": "",
            "Parent_Organization_Name": "",
            "Teaching_Hospital_Flag": "",
            "Profit_Flag": "",
            "Accept_Medicare": "",
            "Accept_Medicaid": "",
            "Ownership_Status": "",
            "Bed_Count": "",
            "Resident_Count": "",
            "Website": "",
            "X_hco_address": [
                {
                    "X_address_line_1": "",
                    "X_city": "",
                    "X_state": {"Code": "", "Name": ""},
                    "X_postal_code": "",
                    "X_country": {"Code": "", "Name": ""},
                    "X_address_type": {"Code": "", "Name": ""},
                }
            ],
            "Alternate_Name": [
                {
                    "Alternate_Name": "",
                    "Alternate_Name_Type": {"Code": "", "Name": ""},
                }
            ],
            "ElectronicAddress": [{"electronicAddress": ""}],
            "Phone": [{"phoneNumber": ""}],
            "TaxDetail": [
                {
                    "taxNumber": "",
                    "taxNumberType": {"Code": "", "Name": ""},
                }
            ],
            "X_hco_hierarchy": [
                {
                    "Parent_Organization_EID": "",
                    "Relationship_Type": {"Code": "", "Name": ""},
                }
            ],
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

# HCO field mapping -- maps incoming SBC HCO payload fields to the
# HCO MDM Hub template paths.
HCO_MDM_HUB_FIELD_MAPPING = (
    (HCO_NAME, ("data", "searchRecord", "HCO_Name")),
    (HCO_TYPE, ("data", "searchRecord", "HCO_Type", "Code")),
    ("address.city", ("data", "searchRecord", "X_hco_address", 0, "X_city")),
    ("address.primary", ("data", "searchRecord", "X_hco_address", 0, "X_address_line_1")),
    (ADDRESS_COUNTRY_CODE, ("data", "searchRecord", "X_hco_address", 0, "X_country", "Code")),
    ("address.longPostalCode", ("data", "searchRecord", "X_hco_address", 0, "X_postal_code")),
    ("address.type", ("data", "searchRecord", "X_hco_address", 0, "X_address_type", "Code")),
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


def transform_to_mdm_hub(
    incoming: Dict[str, Any] | None,
    entity_type: str = "HCP",
) -> Dict[str, Any]:
    """Transform the incoming SBC payload into the MDM_HUB request.

    Args:
        incoming: The SBC search payload from the client.
        entity_type: "HCP" or "HCO" -- controls which MDM Hub template
            and field mapping is used. HCP uses individual fields
            (firstName, lastName, etc.); HCO uses organization fields
            (organizationName, organizationType, etc.).
    """
    incoming = incoming or {}

    if entity_type.upper() == "HCO":
        template = HCO_MDM_HUB_TEMPLATE
        field_mapping = HCO_MDM_HUB_FIELD_MAPPING
    else:
        template = MDM_HUB_TEMPLATE
        field_mapping = MDM_HUB_FIELD_MAPPING

    output = copy.deepcopy(template)

    for source, path in field_mapping:
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

    if entity_type.upper() == "HCO":
        org_name = incoming.get(HCO_NAME, "")
        output["data"]["searchRecord"]["Official_Name"] = org_name
        full_name = org_name
    else:
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
    "HCO_MDM_HUB_FIELD_MAPPING",
    "HCO_MDM_HUB_TEMPLATE",
    "transform_to_mdm_hub",
]

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# No credentials are required in this transformation module.
# MDM_HUB endpoint/authentication values belong in the calling API/runtime config.
# ============================================================================

