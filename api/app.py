"""
Healthcare Master Data Management - Mock Clarivate/IQVIA Healthcare REST API.

This is a lightweight FastAPI app that stands in for the real
Clarivate/IQVIA HCP and HCO REST APIs during local development and
testing, so the ingestion pipeline (src/ingestion/src_to_raw_ingestion.py)
and the real-time Search-Before-Create flow (src/api/sbc.py) can be
exercised end-to-end without needing real IQVIA credentials.

Two things live in this one app, at different paths, so one deployment
covers both external systems src/api/download_api.py talks to:
  - /api/v1/hcp, /api/v1/hco   - simple CRUD "data entry" routes (enter
    fake HCP/HCO data by hand - see routes/hcp.py, routes/hco.py)
  - /v1/lookup                  - the realistic, deeply-nested IQVIA
    search response shape download_api.py actually parses (see
    routes/iqvia_lookup.py) - reads the same data entered via the CRUD
    routes above and reshapes it
  - /security/login, /, /{id}   - a mock Informatica MDM Hub (see
    routes/mdm_hub.py)

When deploying, set download_api.py's env vars to:
    iqvia_url        = https://<this deployment>/v1/lookup
    base_MDM_HUB_url = https://<this deployment>
"""

from fastapi import FastAPI

from routes.hcp import router as hcp_router
from routes.hco import router as hco_router
from routes.iqvia_lookup import router as iqvia_lookup_router
from routes.mdm_hub import router as mdm_hub_router


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

app.include_router(iqvia_lookup_router)

app.include_router(mdm_hub_router)