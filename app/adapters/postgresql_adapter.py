"""
PostgreSQL Adapter — asyncpg-based async implementation.
Uses named parameters via $N positional binding (asyncpg convention).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    HAS_ASYNCPG = False

from app.adapters.base import DatabaseAdapter
from app.core.security import validate_identifier


def _named_to_dollar(sql: str, params: Dict[str, Any]):
    """Convert :name style to $1, $2... style for asyncpg, returning ordered values."""
    ordered = []
    counter = [0]

    def replacer(m):
        key = m.group(1)
        if key not in [k for k, _ in ordered]:
            counter[0] += 1
            ordered.append((key, params[key]))
        idx = next(i + 1 for i, (k, _) in enumerate(ordered) if k == key)
        return f"${idx}"

    converted = re.sub(r":([a-zA-Z_][a-zA-Z0-9_]*)", replacer, sql)
    return converted, [v for _, v in ordered]


class PostgreSQLAdapter(DatabaseAdapter):
    def __init__(self, db_config: dict) -> None:
        if not HAS_ASYNCPG:
            raise RuntimeError("asyncpg is not installed. Run: pip install asyncpg")
        self._config = db_config
        self._pool: Optional["asyncpg.Pool"] = None

    async def connect(self) -> None:
        cfg = self._config
        self._pool = await asyncpg.create_pool(
            host=cfg.get("host", "localhost"),
            port=int(cfg.get("port", 5432)),
            database=cfg.get("database"),
            user=cfg.get("username"),
            password=cfg.get("password"),
            min_size=2,
            max_size=10,
        )

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    # ── Internal helpers ─────────────────────────────────────────────────────
    async def _fetchall(self, sql: str, *args) -> List[Dict[str, Any]]:
        assert self._pool
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return [dict(r) for r in rows]

    async def _execute(self, sql: str, *args) -> str:
        assert self._pool
        async with self._pool.acquire() as conn:
            return await conn.execute(sql, *args)

    # ── Interface ────────────────────────────────────────────────────────────
    async def get_all(self, table: str) -> List[Dict[str, Any]]:
        validate_identifier(table)
        return await self._fetchall(f'SELECT * FROM "{table}"')

    async def get_by_id(self, table: str, id: Any) -> Optional[Dict[str, Any]]:
        validate_identifier(table)
        rows = await self._fetchall(f'SELECT * FROM "{table}" WHERE id = $1', id)
        return rows[0] if rows else None

    async def filter(self, table: str, column: str, value: Any) -> List[Dict[str, Any]]:
        validate_identifier(table)
        validate_identifier(column)
        return await self._fetchall(f'SELECT * FROM "{table}" WHERE "{column}" = $1', value)

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
                set_parts = [f'"{validate_identifier(k)}" = ${i+1}' for i, k in enumerate(update_data)]
                values = list(update_data.values()) + [pk_value]
                set_clause = ", ".join(set_parts)
                await self._execute(
                    f'UPDATE "{table}" SET {set_clause} WHERE "{pk}" = ${len(values)}',
                    *values
                )
        else:
            cols = ", ".join(f'"{validate_identifier(k)}"' for k in data)
            placeholders = ", ".join(f"${i+1}" for i in range(len(data)))
            values = list(data.values())
            if pk not in data:
                row = await self._fetchall(
                    f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders}) RETURNING *',
                    *values
                )
                return row[0] if row else data
            else:
                await self._execute(
                    f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})',
                    *values
                )

        return await self.get_by_id(table, pk_value) or data

    async def delete(self, table: str, id: Any) -> bool:
        validate_identifier(table)
        status = await self._execute(f'DELETE FROM "{table}" WHERE id = $1', id)
        return status.endswith("1")

    async def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        converted, values = _named_to_dollar(sql, params or {})
        return await self._fetchall(converted, *values)
