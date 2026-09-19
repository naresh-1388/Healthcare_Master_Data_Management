"""
AWS Lambda entry point for the mock Healthcare MDM API (app.py).

FastAPI apps are ASGI applications - they don't natively speak the
API-Gateway/Lambda-Function-URL "event" format the way `download_api.py`'s
hand-written `lambda_handler` does. Mangum is the standard adapter
library that converts between the two, so `app.py`/`routes/`/`store.py`
run completely unchanged, whether started locally with
`uvicorn app:app --reload` or invoked here inside Lambda.

Deployment handler string: `lambda_function.handler`
"""

from mangum import Mangum

from app import app

handler = Mangum(app)
