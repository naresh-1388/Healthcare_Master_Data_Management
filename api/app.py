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

    return {

        "Project": "Healthcare Master Data Management",

        "Version": "1.0.0",

        "Status": "Running"

    }


app.include_router(hcp_router)

app.include_router(hco_router)