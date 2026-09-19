"""
Mock HCP (Healthcare Professional) REST endpoints, matching the shape of
the real IQVIA individual API closely enough to develop and test the
Source_Raw / SBC integration against, without live IQVIA credentials.

Implements full CRUD (GET/POST/PUT/PATCH/DELETE) against an in-memory
store (see store.py) so the Search-Before-Create flow (src/api/sbc.py)
and the ingestion pipeline can both be exercised end-to-end: create a
record, confirm it comes back on GET, update it, then delete it - the
same lifecycle a real IQVIA-integrated system would go through.
"""

from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder

from models import HCPResponse
import store

router = APIRouter(
    prefix="/api/v1/hcp",
    tags=["Healthcare Professional (HCP)"]
)


@router.get("/")
def get_all_hcp():
    """Return every HCP record currently in the store."""

    return store.list_hcp()


@router.get("/{hcp_id}")
def get_hcp_by_id(hcp_id: str):
    """
    Return the HCP record matching individualEid, or a 404 if none exists
    (mirrors the real API's lookup-by-ID shape for the SBC/MDM-search
    integration to test against).
    """

    record = store.get_hcp(hcp_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"HCP '{hcp_id}' not found")
    return record


@router.post("/", status_code=201)
def create_hcp(payload: HCPResponse):
    """
    Create a new HCP record. Returns 201 with the created record, or 409
    if an HCP with the same individualEid already exists (matching a real
    API's typical create-conflict behaviour).
    """

    hcp_id = payload.individual.individualEid
    try:
        record = store.create_hcp(hcp_id, jsonable_encoder(payload))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return record


@router.put("/{hcp_id}")
def replace_hcp(hcp_id: str, payload: HCPResponse):
    """
    Fully replace an HCP record with `payload` (PUT semantics - every
    field is overwritten, unlike PATCH). Creates the record if it does
    not already exist, matching common REST API "upsert" PUT behaviour.
    """

    return store.replace_hcp(hcp_id, jsonable_encoder(payload))


@router.patch("/{hcp_id}")
def update_hcp(hcp_id: str, partial: dict):
    """
    Merge only the fields present in `partial` into an existing HCP
    record (PATCH semantics - fields not mentioned are left unchanged).
    Returns 404 if hcp_id does not exist (PATCH never creates).
    """

    record = store.update_hcp(hcp_id, partial)
    if record is None:
        raise HTTPException(status_code=404, detail=f"HCP '{hcp_id}' not found")
    return record


@router.delete("/{hcp_id}", status_code=204)
def delete_hcp(hcp_id: str):
    """Delete an HCP record. Returns 404 if it did not exist."""

    if not store.delete_hcp(hcp_id):
        raise HTTPException(status_code=404, detail=f"HCP '{hcp_id}' not found")
    return None
