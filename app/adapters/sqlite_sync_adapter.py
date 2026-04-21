"""
SQLite Sync-in-Thread Adapter — stdlib sqlite3 wrapped in asyncio.to_thread.
Zero external dependencies. Drop-in replacement for SQLiteAdapter when aiosqlite
is not available, or as a fallback in constrained environments.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.adapters.base import DatabaseAdapter
from app.core.security import validate_identifier


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return dict(row)


def _named_to_positional(sql: str, params: Dict[str, Any]):
    """Convert :name style params to ? style for sqlite3."""
    positional = []
    def replacer(m):
        key = m.group(1)
        positional.append(params[key])
        return "?"
    converted = re.sub(r":([a-zA-Z_][a-zA-Z0-9_]*)", replacer, sql)
    return converted, positional


class SQLiteSyncAdapter(DatabaseAdapter):
    """
    Wraps synchronous sqlite3 calls in asyncio.to_thread for non-blocking I/O.
    Suitable for development, metadata storage, and environments without aiosqlite.
    """

    def __init__(self, db_config: dict) -> None:
        self._path = db_config.get("path", "./data/app.db")
        self._conn: Optional[sqlite3.Connection] = None

    # ── Lifecycle ────────────────────────────────────────────────────────────
    async def connect(self) -> None:
        def _open():
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self._path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.commit()
            return conn
        self._conn = await asyncio.to_thread(_open)

    async def disconnect(self) -> None:
        if self._conn:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    # ── Internal helpers ─────────────────────────────────────────────────────
    def _sync_execute(self, sql: str, params=()):
        assert self._conn, "Not connected"
        cur = self._conn.execute(sql, params)
        self._conn.commit()
        return cur

    def _sync_fetchall(self, sql: str, params=()) -> List[Dict[str, Any]]:
        assert self._conn, "Not connected"
        cur = self._conn.execute(sql, params)
        rows = cur.fetchall()
        return [_row_to_dict(r) for r in rows]

    async def _execute(self, sql: str, params=()):
        return await asyncio.to_thread(self._sync_execute, sql, params)

    async def _fetchall(self, sql: str, params=()) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._sync_fetchall, sql, params)

    # ── Interface ────────────────────────────────────────────────────────────
    async def get_all(self, table: str) -> List[Dict[str, Any]]:
        validate_identifier(table)
        return await self._fetchall(f"SELECT * FROM {table}")

    async def get_by_id(self, table: str, id: Any) -> Optional[Dict[str, Any]]:
        validate_identifier(table)
        rows = await self._fetchall(f"SELECT * FROM {table} WHERE id = ?", (id,))
        return rows[0] if rows else None

    async def filter(self, table: str, column: str, value: Any) -> List[Dict[str, Any]]:
        validate_identifier(table)
        validate_identifier(column)
        return await self._fetchall(f"SELECT * FROM {table} WHERE {column} = ?", (value,))

    async def insert_or_update(
        self, table: str, data: Dict[str, Any], pk: str = "id"
    ) -> Dict[str, Any]:
        validate_identifier(table)
        validate_identifier(pk)

        pk_value = data.get(pk)
        existing = None
        if pk_value is not None:
            existing = await self.get_by_id(table, pk_value)

        if existing:
            update_data = {k: v for k, v in data.items() if k != pk}
            if update_data:
                set_clause = ", ".join(f"{validate_identifier(k)} = ?" for k in update_data)
                values = list(update_data.values()) + [pk_value]
                await self._execute(
                    f"UPDATE {table} SET {set_clause} WHERE {pk} = ?", values
                )
        else:
            cols = ", ".join(validate_identifier(k) for k in data)
            placeholders = ", ".join("?" * len(data))
            values = list(data.values())
            cur = await self._execute(
                f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", values
            )
            if pk not in data:
                pk_value = cur.lastrowid

        result = await self.get_by_id(table, pk_value)
        return result or data

    async def delete(self, table: str, id: Any) -> bool:
        validate_identifier(table)
        cur = await self._execute(f"DELETE FROM {table} WHERE id = ?", (id,))
        return (cur.rowcount or 0) > 0

    async def query(
        self, sql: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        positional_sql, positional_params = _named_to_positional(sql, params or {})
        return await self._fetchall(positional_sql, positional_params)
