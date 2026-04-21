"""
Adapter Registry — Resolves config database keys to live adapter instances.
Manages connection lifecycle for all registered databases.
"""

from __future__ import annotations

from typing import Dict

from fastapi import HTTPException

from app.adapters.base import DatabaseAdapter
from app.core.config import config


class AdapterRegistry:
    """
    Singleton that holds connected adapter instances keyed by config db key.
    Adapters are created and connected at startup, disconnected at shutdown.
    """

    def __init__(self) -> None:
        self._adapters: Dict[str, DatabaseAdapter] = {}

    def _build_adapter(self, db_key: str) -> DatabaseAdapter:
        """Instantiate the correct adapter from config."""
        # Handle special sqlite_meta key
        if db_key == "sqlite_meta":
            db_cfg = config.get("sqlite_meta")
            db_type = "sqlite"
        else:
            dbs = config.get("databases")
            if dbs is None or db_key not in dbs:
                raise HTTPException(
                    status_code=404,
                    detail=f"Database key '{db_key}' not found in config."
                )
            db_cfg_frozen = dbs[db_key]
            db_cfg = db_cfg_frozen.to_dict() if hasattr(db_cfg_frozen, "to_dict") else dict(db_cfg_frozen)
            db_type = db_cfg.get("type", "sqlite")

        if hasattr(db_cfg, "to_dict"):
            db_cfg = db_cfg.to_dict()

        match db_type:
            case "sqlite":
                try:
                    import aiosqlite  # noqa: F401
                    from app.adapters.sqlite_adapter import SQLiteAdapter
                    return SQLiteAdapter(db_cfg)
                except ImportError:
                    from app.adapters.sqlite_sync_adapter import SQLiteSyncAdapter
                    return SQLiteSyncAdapter(db_cfg)
            case "postgresql":
                from app.adapters.postgresql_adapter import PostgreSQLAdapter
                return PostgreSQLAdapter(db_cfg)
            case "mysql":
                from app.adapters.mysql_adapter import MySQLAdapter
                return MySQLAdapter(db_cfg)
            case "mongodb":
                from app.adapters.mongodb_adapter import MongoDBAdapter
                return MongoDBAdapter(db_cfg)
            case _:
                raise HTTPException(
                    status_code=500,
                    detail=f"Unknown database type '{db_type}' for key '{db_key}'."
                )

    async def init_all(self) -> None:
        """Connect all databases defined in config."""
        keys_to_connect = []

        # Always connect metadata db
        keys_to_connect.append("sqlite_meta")

        # Connect active_db
        active = str(config.get("active_db", ""))
        if active and active not in keys_to_connect:
            keys_to_connect.append(active)

        # Optionally pre-connect all defined databases
        dbs = config.get("databases")
        if dbs:
            for key in dbs.keys():
                if key not in keys_to_connect:
                    keys_to_connect.append(key)

        for key in keys_to_connect:
            try:
                adapter = self._build_adapter(key)
                await adapter.connect()
                self._adapters[key] = adapter
                print(f"[DB] Connected: {key} ({adapter.adapter_name()})")
            except Exception as exc:
                print(f"[DB] WARNING — Could not connect '{key}': {exc}")

    async def shutdown_all(self) -> None:
        for key, adapter in self._adapters.items():
            try:
                await adapter.disconnect()
                print(f"[DB] Disconnected: {key}")
            except Exception as exc:
                print(f"[DB] WARNING — Error disconnecting '{key}': {exc}")
        self._adapters.clear()

    def get(self, db_key: str | None = None) -> DatabaseAdapter:
        """
        Resolve an adapter by key.
        Falls back to active_db if key is None.
        """
        key = db_key or str(config.get("active_db", "sqlite_main"))
        if key not in self._adapters:
            raise HTTPException(
                status_code=503,
                detail=f"Database '{key}' is not connected. Check config and startup logs."
            )
        return self._adapters[key]

    def get_meta(self) -> DatabaseAdapter:
        return self.get("sqlite_meta")

    def list_keys(self) -> list[str]:
        return list(self._adapters.keys())


# ── Singleton instance ────────────────────────────────────────────────────────
registry = AdapterRegistry()
