from fastapi import APIRouter
from sample_data import hcp_sample

router = APIRouter(
    prefix="/api/v1/hcp",
    tags=["Healthcare Professional (HCP)"]
)


@router.get("/")
def get_all_hcp():

    return hcp_sample


@router.get("/{hcp_id}")
def get_hcp_by_id(hcp_id: str):

    if hcp_sample.individual.individualEid == hcp_id:
        return hcp_sample

    return {
        "message": f"HCP '{hcp_id}' not found"
    }