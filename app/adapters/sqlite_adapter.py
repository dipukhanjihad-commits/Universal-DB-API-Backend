"""
SQLite Adapter — aiosqlite-based async implementation.
Used for both data storage and metadata/config storage.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

from app.adapters.base import DatabaseAdapter
from app.core.security import validate_identifier


def _row_to_dict(row: aiosqlite.Row, description) -> Dict[str, Any]:
    return {description[i][0]: row[i] for i in range(len(description))}


class SQLiteAdapter(DatabaseAdapter):
    def __init__(self, db_config: dict) -> None:
        self._path = db_config.get("path", "./data/app.db")
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")

    async def disconnect(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    # ── Internal helpers ─────────────────────────────────────────────────────
    async def _execute(self, sql: str, params=()) -> aiosqlite.Cursor:
        assert self._conn, "Not connected"
        cursor = await self._conn.execute(sql, params)
        await self._conn.commit()
        return cursor

    async def _fetchall(self, sql: str, params=()) -> List[Dict[str, Any]]:
        assert self._conn, "Not connected"
        async with self._conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            desc = cursor.description or []
            return [_row_to_dict(r, desc) for r in rows]

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
            # UPDATE — only set provided columns, never touch pk
            update_data = {k: v for k, v in data.items() if k != pk}
            if update_data:
                set_clause = ", ".join(f"{validate_identifier(k)} = ?" for k in update_data)
                values = list(update_data.values()) + [pk_value]
                await self._execute(f"UPDATE {table} SET {set_clause} WHERE {pk} = ?", values)
        else:
            # INSERT
            cols = ", ".join(validate_identifier(k) for k in data)
            placeholders = ", ".join("?" * len(data))
            values = list(data.values())
            cursor = await self._execute(
                f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", values
            )
            if pk not in data:
                pk_value = cursor.lastrowid

        result = await self.get_by_id(table, pk_value)
        return result or data

    async def delete(self, table: str, id: Any) -> bool:
        validate_identifier(table)
        cursor = await self._execute(f"DELETE FROM {table} WHERE id = ?", (id,))
        return (cursor.rowcount or 0) > 0

    async def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Executes named-placeholder SQL (:name) by converting to positional (?) for SQLite.
        """
        assert self._conn, "Not connected"
        positional_sql, positional_params = _named_to_positional(sql, params or {})
        return await self._fetchall(positional_sql, positional_params)


def _named_to_positional(sql: str, params: Dict[str, Any]):
    """Convert :name style params to ? style for SQLite."""
    import re
    positional = []
    def replacer(m):
        key = m.group(1)
        positional.append(params[key])
        return "?"
    converted = re.sub(r":([a-zA-Z_][a-zA-Z0-9_]*)", replacer, sql)
    return converted, positional
