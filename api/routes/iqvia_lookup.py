"""
Mock IQVIA "search/lookup" endpoint - POST /v1/lookup.

This is a SEPARATE, realistic-shaped endpoint from the simple CRUD routes
in routes/hcp.py (/api/v1/hcp/). The CRUD routes are a convenient place
for a human to type in simple, flat fake HCP data
(individual.firstName, etc.) - this endpoint reads that same stored
record and reshapes it into the deeply-nested wire format the real
IQVIA OneKey "Activity search" API returns, and that
src/api/download_api.py::transform_iqvia_to_mdm_hub_post() actually
parses (response.results[].individual/workplace/activity, with
individual.ada / individual.externalKeys / workplace.workplaceAddresses
/ workplace.telephones).

Point download_api.py's `iqvia_url` env var at:
    https://<this-deployment>/v1/lookup
"""

from fastapi import APIRouter

import store

router = APIRouter(tags=["IQVIA Lookup (mock)"])


@router.post("/v1/lookup")
def lookup(payload: dict):
    """
    Accepts the real IQVIA "Activity search" request shape:
        {
          "resultSize": "10",
          "entityType": "Activity",
          "codBases": ["W12"],
          "fields": [{"method": "EXACT", "name": "individual.individualId",
                       "values": ["W12345678"]}]
        }
    Looks up "individual.individualId" among the values, finds the
    matching stored HCP (see store.py), and returns it reshaped into the
    real IQVIA response envelope: {"response": {"results": [...]}}.
    """

    iqvia_id = None
    for field in payload.get("fields", []):
        if field.get("name") == "individual.individualId":
            values = field.get("values") or []
            iqvia_id = values[0] if values else None
            break

    stored = store.get_hcp(iqvia_id) if iqvia_id else None

    if stored is None:
        # Real IQVIA returns an empty results list for a no-match search,
        # not a 404 - download_api.py handles first_result = {} gracefully.
        return {"response": {"results": []}}

    individual_in = stored.get("individual", {})
    result = _build_iqvia_result(iqvia_id, individual_in)
    return {"response": {"results": [result]}}


def _build_iqvia_result(iqvia_id: str, individual_in: dict) -> dict:
    """
    Reshape a simple stored HCP record into one IQVIA "results[]" entry.

    Args:
        iqvia_id: The individual.individualId being looked up.
        individual_in: The flat stored record's "individual" sub-dict
            (from the CRUD layer's HCPResponse model - firstName,
            lastName, etc.).

    Returns:
        dict: one entry of response.results[], matching every field
        transform_iqvia_to_mdm_hub_post() reads (individual.ada,
        individual.externalKeys, workplace.workplaceAddresses,
        workplace.telephones, activity.isMainActivity, country).
    """

    first_name = individual_in.get("firstName", "")
    last_name = individual_in.get("lastName", "")
    middle_name = individual_in.get("middleName", "")

    return {
        "country": "US",
        "activity": {"isMainActivity": True},
        "individual": {
            "individualId": iqvia_id,
            "firstName": first_name,
            "middleName": middle_name,
            "lastName": last_name,
            "usualFirstName": first_name,
            "genderCode": "M",
            "typeCode": "MD",
            "stateCode": "ACTIVE",
            "prefixNameCode": "DR",
            "titleCode": "PHYSICIAN",
            "thesisYear": "2005",
            # "ada" holds specialty/taxonomy codes as a flat dict keyed
            # "<listCode-prefix>,<rank>" - "1SP,1" = primary specialty,
            # "1GS,1" = its group specialty. Only listCode == "SP" rows
            # are read as specialties by the real transform function.
            "ada": {
                "1SP,1": {
                    "listCode": "SP",
                    "code": "CARD",
                    "codeCorporateLabel": "Cardiology",
                },
                "1GS,1": {"code": "MED"},
            },
            # externalKeys: only key types "111"/"121" are read (alternate
            # identifiers such as NPI/state license number).
            "externalKeys": {
                "111": {"value": "1234567890", "typeLabel": "NPI"},
            },
        },
        "workplace": {
            "workplaceId": f"WP-{iqvia_id}",
            "workplaceAddresses": {
                "1": {
                    "typeCode": "PROFESSIONAL",
                    "typeCodeCorporateLabel": "Professional",
                    "address": {
                        "addressLongLabel": "123 Medical Plaza",
                        "extensionLabel": "Suite 100",
                        "longPostalcode": "02101",
                        "segmentations": {"UG1": {"brickEid": "BRK001"}},
                        "postalTownReference": {
                            "villageLabel": "Boston",
                            "country": "US",
                            "subdivisions": {
                                "COUNTRY": {"longLocalizedLabel": "United States"},
                                "SUB.3": {"longLocalizedLabel": "Massachusetts"},
                            },
                        },
                    },
                }
            },
            "telephones": {
                "1": {
                    "typeCorporateLabel": "Office",
                    "callNumberForDisplay": "617-555-1234",
                }
            },
        },
    }
