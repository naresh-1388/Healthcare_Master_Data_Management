"""
Mock HCP (Healthcare Professional) REST endpoints, matching the shape of
the real IQVIA individual API closely enough to develop and test the
Source_Raw / SBC integration against, without live IQVIA credentials.
"""

from fastapi import APIRouter
from sample_data import hcp_sample

router = APIRouter(
    prefix="/api/v1/hcp",
    tags=["Healthcare Professional (HCP)"]
)


@router.get("/")
def get_all_hcp():
    """Return the single fixed sample HCP record (mock 'list all')."""

    return hcp_sample


@router.get("/{hcp_id}")
def get_hcp_by_id(hcp_id: str):
    """
    Return the sample HCP record if hcp_id matches its individualEid,
    otherwise a not-found message (mirrors the real API's lookup-by-ID
    shape for the SBC/MDM-search integration to test against).
    """

    if hcp_sample.individual.individualEid == hcp_id:
        return hcp_sample

    return {
        "message": f"HCP '{hcp_id}' not found"
    }