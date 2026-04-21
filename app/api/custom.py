"""
Custom API Router — Register, manage, and execute stored SQL-backed API endpoints.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from app.core.models import (
    ApiResponse,
    CustomApiCreate,
    CustomApiExecuteRequest,
    CustomApiUpdate,
)
from app.engine import custom_api as engine

router = APIRouter(prefix="/custom", tags=["Custom APIs"])


# ── Management endpoints ──────────────────────────────────────────────────────
@router.get("", response_model=ApiResponse)
async def list_custom_apis():
    """List all registered custom APIs."""
    apis = await engine.list_apis()
    return ApiResponse.ok(apis)


@router.post("", response_model=ApiResponse)
async def create_custom_api(payload: CustomApiCreate):
    """Register a new custom API backed by a parameterized SQL query."""
    record = await engine.create_api(payload)
    return ApiResponse.ok(record, message=f"Custom API '{payload.name}' created.")


@router.put("/{name}", response_model=ApiResponse)
async def update_custom_api(name: str, payload: CustomApiUpdate):
    """Update an existing custom API definition."""
    record = await engine.update_api(name, payload)
    return ApiResponse.ok(record, message=f"Custom API '{name}' updated.")


@router.delete("/{name}", response_model=ApiResponse)
async def delete_custom_api(name: str):
    """Remove a custom API definition."""
    await engine.delete_api(name)
    return ApiResponse.ok(None, message=f"Custom API '{name}' deleted.")


# ── Execution endpoints ───────────────────────────────────────────────────────
@router.get("/{name}", response_model=ApiResponse)
async def execute_custom_api_get(name: str, request_params: Optional[str] = Query(default=None)):
    """
    Execute a GET custom API.
    For parameterized GET requests, use POST /custom/{name}/run instead.
    """
    results = await engine.execute_api(name, [])
    return ApiResponse.ok(results)


@router.post("/{name}/run", response_model=ApiResponse)
async def execute_custom_api_post(name: str, body: CustomApiExecuteRequest):
    """Execute a custom API with typed parameters."""
    results = await engine.execute_api(name, body.params)
    return ApiResponse.ok(results)
