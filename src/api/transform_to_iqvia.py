"""Transform the incoming SBC search payload into a IQVIA request payload.

Source of truth for the connected SBC flow: `sbc (1).py`.
The standalone transformation sample is not used to alter this contract.
"""

from __future__ import annotations

from typing import Any, Dict


FIRST_NAME = "hcp.firstName"
MIDDLE_NAME = "hcp.middleName"
LAST_NAME = "hcp.lastName"
ADDRESS_COUNTRY_CODE = "address.countryCode"


IQVIA_FIELD_MAPPING = (
    (FIRST_NAME, "individual.firstName", "EXACT", 1),
    (MIDDLE_NAME, "individual.middleName", "Fuzzy", 1),
    (LAST_NAME, "individual.lastName", "Fuzzy", 1),
    ("address.primary", "address.shortlabel", "Fuzzy", 1),
    (ADDRESS_COUNTRY_CODE, "address.country", "EXACT", 1),
    ("address.city", "address.villagelabel", "Fuzzy", 1),
    ("address.longPostalCode", "address.longPostalCode", "EXACT", 1),
    ("address.type", "address.type", "Fuzzy", 1),
)


COUNTRY_TO_CODBASE = {
    "NL": "WNL",
    "BE": "WBE",
}


class IQVIATransformationError(ValueError):
    """Raised when the incoming payload cannot be transformed for IQVIA."""


def _values(value: Any) -> list[Any]:
    """
    Normalise a raw field value into a clean list of non-blank values,
    so downstream IQVIA transformation code can always iterate a list
    regardless of whether the source field was scalar, list, or blank.

    Args:
        value: The raw value read from the incoming record (may be a
            scalar, a list, or None).

    Returns:
        list: If value is a list, returns only its non-null/non-blank
        entries. Otherwise wraps a single non-blank scalar in a
        one-element list, or returns an empty list for blank/None input.
    """
    if isinstance(value, list):
        return [
            item
            for item in value
            if item is not None and str(item).strip()
        ]

    if value is None or not str(value).strip():
        return []

    return [value.strip() if isinstance(value, str) else value]


def _get_nested_value(incoming: Dict[str, Any], path: str) -> Any:
    """Read a dotted path such as 'address.countryCode' from nested input."""
    current: Any = incoming

    for part in path.split("."):
        if not isinstance(current, dict):
            return None

        current = current.get(part)

        if current is None:
            return None

    return current


def transform_to_iqvia(
    incoming: Dict[str, Any] | None,
) -> Dict[str, Any]:
    """Build the IQVIA request payload used by the SBC flow."""

    incoming = incoming or {}

    country = str(
        _get_nested_value(incoming, ADDRESS_COUNTRY_CODE) or ""
    ).strip().upper()

    if not country:
        raise IQVIATransformationError(
            "Missing mandatory field: address.countryCode for iqvia codBase mapping"
        )

    cod_base = COUNTRY_TO_CODBASE.get(country)

    if not cod_base:
        raise IQVIATransformationError(
            f"Unsupported country for iqvia codBase mapping: {country}"
        )

    output: Dict[str, Any] = {
        "resultSize": "20",
        "entityType": "Activity",
        "codBases": [cod_base],
        "fields": [],
    }

    for source, target, method, precision in IQVIA_FIELD_MAPPING:
        values = _values(
            _get_nested_value(incoming, source)
        )

        if not values:
            continue

        output["fields"].append(
            {
                "method": method,
                "name": target,
                "values": values,
                "fuzzyPrecision": precision,
            }
        )

    return output


__all__ = [
    "ADDRESS_COUNTRY_CODE",
    "COUNTRY_TO_CODBASE",
    "IQVIA_FIELD_MAPPING",
    "IQVIATransformationError",
    "transform_to_iqvia",
]