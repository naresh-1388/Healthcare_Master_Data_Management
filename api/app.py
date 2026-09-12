"""
Healthcare Master Data Management - Mock Clarivate/IQVIA Healthcare REST API.

This is a lightweight FastAPI app that stands in for the real
Clarivate/IQVIA HCP and HCO REST APIs during local development and
testing, so the ingestion pipeline (src/ingestion/src_to_raw_ingestion.py)
and the real-time Search-Before-Create flow (src/api/sbc.py) can be
exercised end-to-end without needing real IQVIA credentials. It serves
the fixed sample payloads defined in sample_data.py.
"""

from fastapi import FastAPI

from routes.hcp import router as hcp_router
from routes.hco import router as hco_router


app = FastAPI(

    title="Healthcare Master Data Management API",

    description="Mock Clarivate Healthcare REST API",

    version="1.0.0"

)


@app.get("/")
def home():
    """Health-check / root endpoint - confirms the mock API is running."""

    return {

        "Project": "Healthcare Master Data Management",

        "Version": "1.0.0",

        "Status": "Running"

    }


app.include_router(hcp_router)

app.include_router(hco_router)