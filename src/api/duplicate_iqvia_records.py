"""
Remove IQVIA records that are already represented in the MDM_HUB response.

The duplicate check is based on:
    MDM_HUB AlternateIdentifier:
        alternateIdentifierType.Code == "IQVIA ID"

against:

    IQVIA individual.individualEid
"""

from __future__ import annotations

import logging
from typing import Any


logger = logging.getLogger(__name__)


def deduplicate_iqvia_records(
    mdm_hub_response: dict[str, Any],
    iqvia_response: dict[str, Any],
) -> dict[str, Any]:
    """
    Remove IQVIA records whose individualEid already exists as a
    IQVIA ID in the MDM_HUB response.

    The IQVIA response is returned with:
        response.results
        response.resultSize
        response.totalNumberOfResults

    updated after duplicate removal.
    """
    try:
        mdm_hub_records = (
            mdm_hub_response
            .get("searchResult", {})
            .get("records", [])
            or []
        )

        mdm_hub_iqvia_ids: set[Any] = set()

        for record in mdm_hub_records:
            data = record.get("data", {}) or {}

            alternate_identifiers = (
                data.get("AlternateIdentifier") or []
            )

            for alternate_identifier in alternate_identifiers:
                identifier_type = (
                    alternate_identifier
                    .get("alternateIdentifierType", {})
                    .get("Code")
                )

                identifier_value = (
                    alternate_identifier
                    .get("alternateIdentifierValue")
                )

                if (
                    identifier_type
                    and identifier_type.strip().upper() == "IQVIA ID"
                    and identifier_value
                ):
                    mdm_hub_iqvia_ids.add(identifier_value)

        logger.info(
            "Extracted %d IQVIA IDs from MDM_HUB response",
            len(mdm_hub_iqvia_ids),
        )

        response_block = (
            iqvia_response.get("response", {}) or {}
        )

        results = response_block.get("results", []) or []

        logger.info(
            "IQVIA records before filtering: %d",
            len(results),
        )

        filtered_results: list[dict[str, Any]] = []

        for result in results:
            individual = result.get("individual", {}) or {}

            # Preserve source field name exactly.
            individual_eid = individual.get("individualEid")

            if (
                individual_eid
                and individual_eid in mdm_hub_iqvia_ids
            ):
                logger.info(
                    "Removing duplicate IQVIA record: %s",
                    individual_eid,
                )
                continue

            filtered_results.append(result)

        logger.info(
            "IQVIA records after filtering: %d",
            len(filtered_results),
        )

        response_block["results"] = filtered_results
        response_block["resultSize"] = len(filtered_results)
        response_block["totalNumberOfResults"] = len(
            filtered_results
        )

        iqvia_response["response"] = response_block

        return iqvia_response

    except Exception:
        logger.exception(
            "Error during IQVIA deduplication"
        )
        return iqvia_response

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# No credentials or database configuration belongs in this pure transformation.
# The caller supplies the IQVIA and MDM_HUB response objects.
# ============================================================================

