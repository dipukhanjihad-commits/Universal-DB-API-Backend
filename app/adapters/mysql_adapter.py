"""
MySQL Adapter — aiomysql-based async implementation.
Uses %s positional binding (PyMySQL/aiomysql convention).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

try:
    import aiomysql
    HAS_AIOMYSQL = True
except ImportError:
    HAS_AIOMYSQL = False

from app.adapters.base import DatabaseAdapter
from app.core.security import validate_identifier


def _named_to_pyformat(sql: str, params: Dict[str, Any]):
    """Convert :name style to %(name)s style for aiomysql."""
    converted = re.sub(r":([a-zA-Z_][a-zA-Z0-9_]*)", r"%(\1)s", sql)
    return converted, params


class MySQLAdapter(DatabaseAdapter):
    def __init__(self, db_config: dict) -> None:
        if not HAS_AIOMYSQL:
            raise RuntimeError("aiomysql is not installed. Run: pip install aiomysql")
        self._config = db_config
        self._pool = None

    async def connect(self) -> None:
        cfg = self._config
        self._pool = await aiomysql.create_pool(
            host=cfg.get("host", "localhost"),
            port=int(cfg.get("port", 3306)),
            db=cfg.get("database"),
            user=cfg.get("username"),
            password=cfg.get("password"),
            autocommit=True,
            minsize=2,
            maxsize=10,
            cursorclass=aiomysql.DictCursor,
        )

    async def disconnect(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    # ── Internal helpers ─────────────────────────────────────────────────────
    async def _fetchall(self, sql: str, params=None) -> List[Dict[str, Any]]:
        assert self._pool
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                return await cur.fetchall()

    async def _execute(self, sql: str, params=None) -> int:
        assert self._pool
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                return cur.rowcount

    # ── Interface ────────────────────────────────────────────────────────────
    async def get_all(self, table: str) -> List[Dict[str, Any]]:
        validate_identifier(table)
        return await self._fetchall(f"SELECT * FROM `{table}`")

    async def get_by_id(self, table: str, id: Any) -> Optional[Dict[str, Any]]:
        validate_identifier(table)
        rows = await self._fetchall(f"SELECT * FROM `{table}` WHERE id = %s", (id,))
        return rows[0] if rows else None

    async def filter(self, table: str, column: str, value: Any) -> List[Dict[str, Any]]:
        validate_identifier(table)
        validate_identifier(column)
        return await self._fetchall(f"SELECT * FROM `{table}` WHERE `{column}` = %s", (value,))

    async def insert_or_update(
        self, table: str, data: Dict[str, Any], pk: str = "id"
    ) -> Dict[str, Any]:
        validate_identifier(table)
        validate_identifier(pk)

        pk_value = data.get(pk)
        existing = await self.get_by_id(table, pk_value) if pk_value is not None else None

        if existing:
            update_data = {k: v for k, v in data.items() if k != pk}
            if update_data:
                set_clause = ", ".join(f"`{validate_identifier(k)}` = %s" for k in update_data)
                values = list(update_data.values()) + [pk_value]
                await self._execute(f"UPDATE `{table}` SET {set_clause} WHERE `{pk}` = %s", values)
        else:
            cols = ", ".join(f"`{validate_identifier(k)}`" for k in data)
            placeholders = ", ".join("%s" * len(data))
            values = list(data.values())
            await self._execute(f"INSERT INTO `{table}` ({cols}) VALUES ({placeholders})", values)

        return await self.get_by_id(table, pk_value) or data

    async def delete(self, table: str, id: Any) -> bool:
        validate_identifier(table)
        rows = await self._execute(f"DELETE FROM `{table}` WHERE id = %s", (id,))
        return rows > 0

    async def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        converted, p = _named_to_pyformat(sql, params or {})
        return await self._fetchall(converted, p)
