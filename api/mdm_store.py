"""
Persistent store for mock MDM Hub (Informatica) records, created via
routes/mdm_hub.py's POST endpoint and read back via its GET endpoint.

Same DynamoDB-with-local-fallback pattern as store.py - see that file's
docstring for the rationale. Kept as a separate table/store from
store.py's HCP/HCO records because it holds a different shape (the
X_-prefixed MDM.HCP schema, not the flat IQVIA-CRUD shape) and is keyed
by a generated MDM businessId rather than an IQVIA individualEid.
"""

import os
import uuid
import boto3
from botocore.exceptions import BotoCoreError, ClientError

TABLE_NAME = os.getenv("HMDM_MDM_HUB_TABLE", "healthcare-mdm-mock-mdm-hub")
REGION = os.getenv("AWS_REGION", "us-east-1")

_table = None
_local_fallback: dict[str, dict] = {}


def _get_table():
    global _table
    if _table is not None:
        return _table
    try:
        table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
        table.load()
        _table = table
        return _table
    except (BotoCoreError, ClientError, Exception):
        _table = False
        return None


def create_record(payload: dict) -> str:
    """
    Store a new MDM record and return its generated businessId.

    Args:
        payload: The X_-prefixed MDM.HCP-shaped record (as built by
            download_api.py::transform_iqvia_to_mdm_hub_post()).

    Returns:
        str: a generated businessId (mirrors the real MDM Hub assigning
        an ID on create).
    """
    business_id = f"MDM{uuid.uuid4().hex[:12].upper()}"
    record = {**payload, "businessId": business_id}
    table = _get_table()
    if table:
        table.put_item(Item={"pk": business_id, **record})
    else:
        _local_fallback[business_id] = record
    return business_id


def get_record(business_id: str) -> dict | None:
    """Return a stored MDM record by businessId, or None if not found."""
    table = _get_table()
    if table:
        resp = table.get_item(Key={"pk": business_id})
        return resp.get("Item")
    return _local_fallback.get(business_id)
