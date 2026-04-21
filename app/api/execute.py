"""
SQL Execution Router — Secure parameterized SQL execution endpoint.
All queries must use named placeholders. Direct string interpolation is rejected.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.adapters.registry import registry
from app.core.config import config
from app.core.models import ApiResponse, ExecuteSqlRequest
from app.core.security import assert_sql_safe, sanitize_sql_value

router = APIRouter(tags=["SQL Execution"])


@router.post("/execute", response_model=ApiResponse)
async def execute_sql(body: ExecuteSqlRequest):
    """
    Execute a parameterized SQL statement.

    - All values must be passed as typed parameters.
    - DDL keywords (DROP, ALTER, TRUNCATE, etc.) are blocked.
    - Results are capped at the configured max_result_rows limit.
    """
    # Check raw SQL execution is allowed
    try:
        allow_raw = bool(config["security"]["allow_raw_sql"])
    except (KeyError, AttributeError):
        allow_raw = True

    if not allow_raw:
        raise HTTPException(
            status_code=403,
            detail="Raw SQL execution is disabled by configuration."
        )

    sql = body.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL cannot be empty.")

    assert_sql_safe(sql)

    # Build typed param dict
    param_dict = {}
    for p in body.params:
        param_dict[p.name] = sanitize_sql_value(p.value, p.type.value)

    adapter = registry.get(body.db_key)

    try:
        results = await adapter.query(sql, param_dict)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Query execution failed: {exc}"
        )

    # Enforce row limit
    try:
        max_rows = int(config["security"]["max_result_rows"])
    except (KeyError, AttributeError, TypeError):
        max_rows = 10000

    if len(results) > max_rows:
        results = results[:max_rows]

    return ApiResponse.ok(results, message=f"Query returned {len(results)} row(s).")
