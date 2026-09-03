"""
Remove JISB records that are already represented in the ORIEO response.

The duplicate check is based on:
    ORIEO AlternateIdentifier:
        alternateIdentifierType.Code == "JISB ID"

against:

    JISB individual.individualEid
"""

from __future__ import annotations

import logging
from typing import Any


logger = logging.getLogger(__name__)


def deduplicate_jisb_records(
    orieo_response: dict[str, Any],
    jisb_response: dict[str, Any],
) -> dict[str, Any]:
    """
    Remove JISB records whose individualEid already exists as a
    JISB ID in the ORIEO response.

    The JISB response is returned with:
        response.results
        response.resultSize
        response.totalNumberOfResults

    updated after duplicate removal.
    """
    try:
        orieo_records = (
            orieo_response
            .get("searchResult", {})
            .get("records", [])
            or []
        )

        orieo_jisb_ids: set[Any] = set()

        for record in orieo_records:
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
                    and identifier_type.strip().upper() == "JISB ID"
                    and identifier_value
                ):
                    orieo_jisb_ids.add(identifier_value)

        logger.info(
            "Extracted %d JISB IDs from ORIEO response",
            len(orieo_jisb_ids),
        )

        response_block = (
            jisb_response.get("response", {}) or {}
        )

        results = response_block.get("results", []) or []

        logger.info(
            "JISB records before filtering: %d",
            len(results),
        )

        filtered_results: list[dict[str, Any]] = []

        for result in results:
            individual = result.get("individual", {}) or {}

            # Preserve source field name exactly.
            individual_eid = individual.get("individualEid")

            if (
                individual_eid
                and individual_eid in orieo_jisb_ids
            ):
                logger.info(
                    "Removing duplicate JISB record: %s",
                    individual_eid,
                )
                continue

            filtered_results.append(result)

        logger.info(
            "JISB records after filtering: %d",
            len(filtered_results),
        )

        response_block["results"] = filtered_results
        response_block["resultSize"] = len(filtered_results)
        response_block["totalNumberOfResults"] = len(
            filtered_results
        )

        jisb_response["response"] = response_block

        return jisb_response

    except Exception:
        logger.exception(
            "Error during JISB deduplication"
        )
        return jisb_response

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# No credentials or database configuration belongs in this pure transformation.
# The caller supplies the JISB and ORIEO response objects.
# ============================================================================

