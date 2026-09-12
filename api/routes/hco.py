"""
Mock HCO (Healthcare Organization) REST endpoints, matching the shape of
the real IQVIA organization API closely enough to develop and test the
Source_Raw / SBC integration against, without live IQVIA credentials.
"""

from fastapi import APIRouter
from sample_data import hco_sample

router = APIRouter(
    prefix="/api/v1/hco",
    tags=["Healthcare Organization (HCO)"]
)


@router.get("/")
def get_all_hco():
    """Return the single fixed sample HCO record (mock 'list all')."""

    return hco_sample


@router.get("/{hco_id}")
def get_hco_by_id(hco_id: str):
    """
    Return the sample HCO record if hco_id matches its organizationEid,
    otherwise a not-found message (mirrors the real API's lookup-by-ID
    shape for the SBC/MDM-search integration to test against).
    """

    if hco_sample.organization.organizationEid == hco_id:
        return hco_sample

    return {
        "message": f"HCO '{hco_id}' not found"
    }