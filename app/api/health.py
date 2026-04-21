"""
Health & Info Router — Liveness probe and runtime status endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter

from app.adapters.registry import registry
from app.core.config import config
from app.core.models import ApiResponse

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=ApiResponse)
async def health():
    """Liveness probe."""
    return ApiResponse.ok({"status": "ok"}, message="Service is healthy.")


@router.get("/info", response_model=ApiResponse)
async def info():
    """Return connected databases and active config summary."""
    active_db = str(config.get("active_db", "unknown"))
    connected = registry.list_keys()

    return ApiResponse.ok({
        "active_db": active_db,
        "connected_databases": connected,
        "server": config.get("server").to_dict() if hasattr(config.get("server"), "to_dict") else {},
    })
