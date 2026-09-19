"""
Mock HCO (Healthcare Organization) REST endpoints, matching the shape of
the real IQVIA organization API closely enough to develop and test the
Source_Raw / SBC integration against, without live IQVIA credentials.

Implements full CRUD (GET/POST/PUT/PATCH/DELETE) against an in-memory
store (see store.py) - same rationale as routes/hcp.py.
"""

from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder

from models import HCOResponse
import store

router = APIRouter(
    prefix="/api/v1/hco",
    tags=["Healthcare Organization (HCO)"]
)


@router.get("/")
def get_all_hco():
    """Return every HCO record currently in the store."""

    return store.list_hco()


@router.get("/{hco_id}")
def get_hco_by_id(hco_id: str):
    """
    Return the HCO record matching organizationEid, or a 404 if none
    exists (mirrors the real API's lookup-by-ID shape for the
    SBC/MDM-search integration to test against).
    """

    record = store.get_hco(hco_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"HCO '{hco_id}' not found")
    return record


@router.post("/", status_code=201)
def create_hco(payload: HCOResponse):
    """
    Create a new HCO record. Returns 201 with the created record, or 409
    if an HCO with the same organizationEid already exists.
    """

    hco_id = payload.organization.organizationEid
    try:
        record = store.create_hco(hco_id, jsonable_encoder(payload))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return record


@router.put("/{hco_id}")
def replace_hco(hco_id: str, payload: HCOResponse):
    """
    Fully replace an HCO record with `payload` (PUT semantics). Creates
    the record if it does not already exist (upsert PUT behaviour).
    """

    return store.replace_hco(hco_id, jsonable_encoder(payload))


@router.patch("/{hco_id}")
def update_hco(hco_id: str, partial: dict):
    """
    Merge only the fields present in `partial` into an existing HCO
    record (PATCH semantics). Returns 404 if hco_id does not exist.
    """

    record = store.update_hco(hco_id, partial)
    if record is None:
        raise HTTPException(status_code=404, detail=f"HCO '{hco_id}' not found")
    return record


@router.delete("/{hco_id}", status_code=204)
def delete_hco(hco_id: str):
    """Delete an HCO record. Returns 404 if it did not exist."""

    if not store.delete_hco(hco_id):
        raise HTTPException(status_code=404, detail=f"HCO '{hco_id}' not found")
    return None
