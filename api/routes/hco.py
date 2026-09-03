from fastapi import APIRouter
from sample_data import hco_sample

router = APIRouter(
    prefix="/api/v1/hco",
    tags=["Healthcare Organization (HCO)"]
)


@router.get("/")
def get_all_hco():

    return hco_sample


@router.get("/{hco_id}")
def get_hco_by_id(hco_id: str):

    if hco_sample.organization.organizationEid == hco_id:
        return hco_sample

    return {
        "message": f"HCO '{hco_id}' not found"
    }