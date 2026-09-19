"""
Mock Informatica MDM Hub endpoints - matches the exact URL/response shape
src/api/download_api.py's call_api_with_retry() and post_mdm_hub_entity()
build and parse:

    POST {base_MDM_HUB_url}/security/login
        -> {"userinfo": {"sessionId": "..."}}
    POST {base_MDM_HUB_url}?sourceSystem=iqvia&sourcePKey=<id>&resolveCrosswalk=false
        -> {"businessId": "<generated id>"}   (create)
    GET  {base_MDM_HUB_url}/{businessId}?_showContentMeta=true
        -> the stored record                  (read back)

Point download_api.py's `base_MDM_HUB_url` env var at this deployment's
root URL (no path suffix - the paths above are appended by download_api.py
itself).
"""

from fastapi import APIRouter, HTTPException, Request

import mdm_store

router = APIRouter(tags=["MDM Hub (mock)"])


@router.post("/security/login")
def login(credentials: dict):
    """
    Mock session login. Accepts any {"username": ..., "password": ...}
    and returns a fixed fake session ID - this mock does not validate
    credentials (the real Secrets-Manager-sourced username/password are
    still required upstream to reach this far, so an extra check here
    would be redundant for a local/dev mock).
    """
    return {"userinfo": {"sessionId": "mock-session-id-000"}}


@router.post("/")
def create_entity(payload: dict, request: Request):
    """
    Mock entity creation. Stores `payload` (the X_-prefixed MDM.HCP-shaped
    body download_api.py::transform_iqvia_to_mdm_hub_post() builds) as-is
    under a newly generated businessId, and returns that ID - matching
    the real Hub's create-then-return-ID contract.

    NOTE: this stores the payload exactly as received. If some of its
    keys (e.g. "Phone", "ElectronicAddress", "_contentMeta") don't match
    the keys transform_mdm_hub_download_response() later reads on GET
    (e.g. "X_phone", "X_email", "_meta"), those fields will legitimately
    come back empty on GET - see the note flagged separately about this
    naming mismatch in download_api.py's own POST vs GET transform
    functions. This mock does not paper over that; it reproduces
    whatever the real Hub would actually do with these exact payload keys.
    """
    business_id = mdm_store.create_record(payload)
    return {"businessId": business_id}


@router.get("/{business_id}")
def get_entity(business_id: str):
    """
    Mock entity read-back. Returns the previously stored record for
    `business_id`, or 404 if it was never created (mirrors the real
    Hub's lookup-by-ID behaviour).
    """
    record = mdm_store.get_record(business_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"MDM record '{business_id}' not found")
    return record
