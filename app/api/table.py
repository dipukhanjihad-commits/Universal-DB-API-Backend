"""
Table API Router — Generic CRUD endpoints for any table in the active database.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from app.adapters.registry import registry
from app.core.models import ApiResponse, UpsertRequest
from app.core.security import assert_sql_safe, validate_table, validate_column
from app.core.config import config

router = APIRouter(prefix="/table", tags=["Table CRUD"])


def _get_adapter(db_key: Optional[str] = None):
    return registry.get(db_key)


def _apply_defaults(table: str, data: dict) -> dict:
    """Fill missing columns with default values from config schema, if defined."""
    try:
        schemas = config.get("default_schemas")
        if schemas and table in schemas:
            schema = schemas[table]
            for col, definition in schema.items():
                if col not in data:
                    default = definition.get("default")
                    if default == "now":
                        from datetime import datetime, timezone
                        data[col] = datetime.now(timezone.utc).isoformat()
                    elif default is not None:
                        data[col] = default
    except Exception:
        pass
    return data


# ── GET all ──────────────────────────────────────────────────────────────────
@router.get("/{table}", response_model=ApiResponse)
async def get_all(
    table: str,
    db: Optional[str] = Query(default=None, description="Database config key"),
    request: Request = None,
):
    validate_table(table)

    # Check for column=value query params (filter mode)
    reserved = {"db"}
    filter_params = {
        k: v for k, v in request.query_params.items() if k not in reserved
    }

    adapter = _get_adapter(db)

    if filter_params:
        if len(filter_params) > 1:
            raise HTTPException(
                status_code=400,
                detail="Only one column filter is supported per request."
            )
        col, val = next(iter(filter_params.items()))
        validate_column(col)
        rows = await adapter.filter(table, col, val)
    else:
        rows = await adapter.get_all(table)

    return ApiResponse.ok(rows)


# ── GET by ID ─────────────────────────────────────────────────────────────────
@router.get("/{table}/{id}", response_model=ApiResponse)
async def get_by_id(
    table: str,
    id: Any,
    db: Optional[str] = Query(default=None),
):
    validate_table(table)
    adapter = _get_adapter(db)
    row = await adapter.get_by_id(table, id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Row with id={id} not found in '{table}'.")
    return ApiResponse.ok(row)


# ── POST upsert ───────────────────────────────────────────────────────────────
@router.post("/{table}", response_model=ApiResponse)
async def upsert(
    table: str,
    body: UpsertRequest,
    db: Optional[str] = Query(default=None),
):
    validate_table(table)
    adapter = _get_adapter(db)

    data = dict(body.data)
    data = _apply_defaults(table, data)

    row = await adapter.insert_or_update(table, data, pk=body.pk)
    return ApiResponse.ok(row, message="Upsert successful.")


# ── DELETE ────────────────────────────────────────────────────────────────────
@router.delete("/{table}/{id}", response_model=ApiResponse)
async def delete(
    table: str,
    id: Any,
    db: Optional[str] = Query(default=None),
):
    validate_table(table)
    adapter = _get_adapter(db)
    deleted = await adapter.delete(table, id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Row with id={id} not found in '{table}'.")
    return ApiResponse.ok(None, message=f"Row {id} deleted from '{table}'.")
