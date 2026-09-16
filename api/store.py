"""
Persistent data store for the mock Healthcare MDM API - DynamoDB-backed.

Replaces the earlier in-memory dict (which lost all data on every
restart/cold-start - unusable once this API runs inside AWS Lambda,
since Lambda is stateless and can restart between any two requests).

Falls back to a local in-memory dict automatically when DynamoDB is not
reachable (e.g. running `uvicorn app:app --reload` on your laptop with
no AWS credentials configured) - so local development still works with
zero AWS setup, and only production/Lambda needs the real table.
"""

import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError

TABLE_NAME = os.getenv("HMDM_MOCK_API_TABLE", "healthcare-mdm-mock-api")
REGION = os.getenv("AWS_REGION", "us-east-1")

_dynamodb = None
_table = None
_local_fallback: dict[str, dict] = {}  # used only if DynamoDB is unreachable


def _get_table():
    """Lazily connect to DynamoDB on first use; None if unreachable."""
    global _dynamodb, _table
    if _table is not None:
        return _table
    try:
        _dynamodb = boto3.resource("dynamodb", region_name=REGION)
        _table = _dynamodb.Table(TABLE_NAME)
        _table.load()  # forces a real call, so a missing table/creds fails fast, here
        return _table
    except (BotoCoreError, ClientError, Exception):
        _table = False  # sentinel: "checked, unavailable" (not None = "not checked yet")
        return None


def _put(pk: str, record: dict) -> dict:
    table = _get_table()
    if table:
        table.put_item(Item={"pk": pk, **record})
    else:
        _local_fallback[pk] = record
    return record


def _get(pk: str) -> dict | None:
    table = _get_table()
    if table:
        resp = table.get_item(Key={"pk": pk})
        return resp.get("Item")
    return _local_fallback.get(pk)


def _delete(pk: str) -> bool:
    table = _get_table()
    if table:
        existing = _get(pk)
        if existing is None:
            return False
        table.delete_item(Key={"pk": pk})
        return True
    return _local_fallback.pop(pk, None) is not None


def _list(prefix: str) -> list[dict]:
    table = _get_table()
    if table:
        # Scan + filter by prefix - fine at mock-API scale; a real
        # production table would use a Global Secondary Index instead.
        resp = table.scan()
        return [item for pk, item in ((i["pk"], i) for i in resp.get("Items", [])) if pk.startswith(prefix)]
    return [v for k, v in _local_fallback.items() if k.startswith(prefix)]


# --- Seed the store with the original fixed sample records, once ---
def _seed_if_empty():
    from sample_data import hcp_sample, hco_sample
    if _get(f"HCP#{hcp_sample.individual.individualEid}") is None:
        _put(f"HCP#{hcp_sample.individual.individualEid}", hcp_sample.model_dump())
    if _get(f"HCO#{hco_sample.organization.organizationEid}") is None:
        _put(f"HCO#{hco_sample.organization.organizationEid}", hco_sample.model_dump())


_seed_if_empty()


# --- Public API used by routes/hcp.py ---

def list_hcp() -> list[dict]:
    """Return every HCP record currently in the store."""
    return _list("HCP#")


def get_hcp(hcp_id: str) -> dict | None:
    """Return one HCP record by individualEid, or None if not found."""
    return _get(f"HCP#{hcp_id}")


def create_hcp(hcp_id: str, record: dict) -> dict:
    """Add a new HCP record. Raises ValueError if hcp_id already exists."""
    if get_hcp(hcp_id) is not None:
        raise ValueError(f"HCP '{hcp_id}' already exists")
    return _put(f"HCP#{hcp_id}", record)


def replace_hcp(hcp_id: str, record: dict) -> dict:
    """Fully replace an existing HCP record (PUT semantics)."""
    return _put(f"HCP#{hcp_id}", record)


def update_hcp(hcp_id: str, partial: dict) -> dict | None:
    """Merge `partial` fields into an existing HCP record (PATCH semantics)."""
    existing = get_hcp(hcp_id)
    if existing is None:
        return None
    existing.update(partial)
    return _put(f"HCP#{hcp_id}", existing)


def delete_hcp(hcp_id: str) -> bool:
    """Remove an HCP record. Returns True if it existed, False otherwise."""
    return _delete(f"HCP#{hcp_id}")


# --- Same five operations, for HCO ---

def list_hco() -> list[dict]:
    """Return every HCO record currently in the store."""
    return _list("HCO#")


def get_hco(hco_id: str) -> dict | None:
    """Return one HCO record by organizationEid, or None if not found."""
    return _get(f"HCO#{hco_id}")


def create_hco(hco_id: str, record: dict) -> dict:
    """Add a new HCO record. Raises ValueError if hco_id already exists."""
    if get_hco(hco_id) is not None:
        raise ValueError(f"HCO '{hco_id}' already exists")
    return _put(f"HCO#{hco_id}", record)


def replace_hco(hco_id: str, record: dict) -> dict:
    """Fully replace an existing HCO record (PUT semantics)."""
    return _put(f"HCO#{hco_id}", record)


def update_hco(hco_id: str, partial: dict) -> dict | None:
    """Merge `partial` fields into an existing HCO record (PATCH semantics)."""
    existing = get_hco(hco_id)
    if existing is None:
        return None
    existing.update(partial)
    return _put(f"HCO#{hco_id}", existing)


def delete_hco(hco_id: str) -> bool:
    """Remove an HCO record. Returns True if it existed, False otherwise."""
    return _delete(f"HCO#{hco_id}")
