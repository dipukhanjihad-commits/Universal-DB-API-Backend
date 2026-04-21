"""
Application Factory — Creates and configures the FastAPI app.
Handles startup (DB connections, schema init) and shutdown (graceful disconnect).
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.adapters.registry import registry
from app.api import custom, execute, health, table
from app.engine.custom_api import ensure_schema


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup: connect all databases, ensure metadata schema.
    Shutdown: gracefully disconnect all adapters.
    """
    print("[Startup] Initializing database connections...")
    await registry.init_all()
    print("[Startup] Ensuring metadata schema...")
    await ensure_schema()
    print("[Startup] Ready.")

    yield  # ← Application runs here

    print("[Shutdown] Disconnecting databases...")
    await registry.shutdown_all()
    print("[Shutdown] Done.")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Universal DB API",
        description=(
            "A production-grade universal database API layer with pluggable adapters, "
            "generic CRUD, secure SQL execution, and a custom API engine."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ── CORS ─────────────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Global exception handler ──────────────────────────────────────────────
    @app.exception_handler(Exception)
    async def unhandled_exception(request: Request, exc: Exception):
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "data": None,
                "message": f"Internal server error: {type(exc).__name__}: {exc}",
            },
        )

    # ── Routers ───────────────────────────────────────────────────────────────
    app.include_router(health.router)
    app.include_router(table.router)
    app.include_router(execute.router)
    app.include_router(custom.router)

    return app


app = create_app()
