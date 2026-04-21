"""
Custom API Engine — CRUD for custom API definitions stored in SQLite metadata DB.
Executes stored SQL using the appropriate database adapter.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from app.adapters.registry import registry
from app.core.models import CustomApiCreate, CustomApiUpdate
from app.core.security import assert_sql_safe, sanitize_sql_value

_TABLE = "custom_apis"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS custom_apis (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    method      TEXT    NOT NULL DEFAULT 'GET',
    sql         TEXT    NOT NULL,
    db_type     TEXT    NOT NULL DEFAULT 'sqlite',
    db_key      TEXT,
    created_at  TEXT    NOT NULL
)
"""


async def ensure_schema() -> None:
    """Create the custom_apis table in metadata SQLite if not present."""
    meta = registry.get_meta()
    await meta.query(_CREATE_TABLE_SQL)


# ── CRUD ─────────────────────────────────────────────────────────────────────
async def create_api(payload: CustomApiCreate) -> Dict[str, Any]:
    assert_sql_safe(payload.sql)
    meta = registry.get_meta()

    existing = await meta.filter(_TABLE, "name", payload.name)
    if existing:
        raise HTTPException(status_code=409, detail=f"Custom API '{payload.name}' already exists.")

    data = {
        "name": payload.name,
        "method": payload.method.value,
        "sql": payload.sql,
        "db_type": payload.db_type.value,
        "db_key": payload.db_key,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    return await meta.insert_or_update(_TABLE, data)


async def get_api(name: str) -> Dict[str, Any]:
    meta = registry.get_meta()
    rows = await meta.filter(_TABLE, "name", name)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Custom API '{name}' not found.")
    return rows[0]


async def list_apis() -> List[Dict[str, Any]]:
    meta = registry.get_meta()
    return await meta.get_all(_TABLE)


async def update_api(name: str, payload: CustomApiUpdate) -> Dict[str, Any]:
    record = await get_api(name)

    if payload.sql is not None:
        assert_sql_safe(payload.sql)

    updates: Dict[str, Any] = {"id": record["id"]}
    if payload.method is not None:
        updates["method"] = payload.method.value
    if payload.sql is not None:
        updates["sql"] = payload.sql
    if payload.db_type is not None:
        updates["db_type"] = payload.db_type.value
    if payload.db_key is not None:
        updates["db_key"] = payload.db_key

    meta = registry.get_meta()
    return await meta.insert_or_update(_TABLE, updates)


async def delete_api(name: str) -> bool:
    record = await get_api(name)
    meta = registry.get_meta()
    return await meta.delete(_TABLE, record["id"])


# ── Execution ─────────────────────────────────────────────────────────────────
async def execute_api(name: str, params: list) -> List[Dict[str, Any]]:
    """
    Load a stored custom API by name and execute its SQL with injected params
    using the correct database adapter.
    """
    record = await get_api(name)
    sql: str = record["sql"]
    db_key: Optional[str] = record.get("db_key")

    assert_sql_safe(sql)

    # Build param dict with type coercion
    param_dict: Dict[str, Any] = {}
    for p in params:
        param_dict[p.name] = sanitize_sql_value(p.value, p.type.value)

    adapter = registry.get(db_key)
    return await adapter.query(sql, param_dict)
