"""
MongoDB Adapter — motor-based async implementation.
Maps the relational interface onto MongoDB collections.
'table' → collection name, 'id' → _id field.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

from fastapi import HTTPException

from app.adapters.base import DatabaseAdapter
from app.core.security import validate_identifier


def _normalize(doc: dict) -> dict:
    """Convert ObjectId _id to string 'id' for consistent API responses."""
    if doc is None:
        return doc
    out = dict(doc)
    if "_id" in out:
        out["id"] = str(out.pop("_id"))
    return out


class MongoDBAdapter(DatabaseAdapter):
    def __init__(self, db_config: dict) -> None:
        if not HAS_MOTOR:
            raise RuntimeError("motor is not installed. Run: pip install motor")
        self._uri = db_config.get("uri", "mongodb://localhost:27017")
        self._db_name = db_config.get("database", "mydb")
        self._client = None
        self._db = None

    async def connect(self) -> None:
        self._client = AsyncIOMotorClient(self._uri)
        self._db = self._client[self._db_name]

    async def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def _col(self, table: str):
        validate_identifier(table)
        return self._db[table]

    # ── Interface ────────────────────────────────────────────────────────────
    async def get_all(self, table: str) -> List[Dict[str, Any]]:
        cursor = self._col(table).find()
        return [_normalize(doc) async for doc in cursor]

    async def get_by_id(self, table: str, id: Any) -> Optional[Dict[str, Any]]:
        from bson import ObjectId
        try:
            oid = ObjectId(str(id))
        except Exception:
            oid = id
        doc = await self._col(table).find_one({"_id": oid})
        return _normalize(doc) if doc else None

    async def filter(self, table: str, column: str, value: Any) -> List[Dict[str, Any]]:
        validate_identifier(column)
        cursor = self._col(table).find({column: value})
        return [_normalize(doc) async for doc in cursor]

    async def insert_or_update(
        self, table: str, data: Dict[str, Any], pk: str = "id"
    ) -> Dict[str, Any]:
        from bson import ObjectId
        col = self._col(table)
        pk_value = data.get(pk)

        if pk_value is not None:
            try:
                oid = ObjectId(str(pk_value))
            except Exception:
                oid = pk_value

            update_data = {k: v for k, v in data.items() if k != pk}
            result = await col.update_one(
                {"_id": oid},
                {"$set": update_data},
                upsert=True,
            )
            doc = await col.find_one({"_id": oid})
        else:
            result = await col.insert_one(data)
            doc = await col.find_one({"_id": result.inserted_id})

        return _normalize(doc) if doc else data

    async def delete(self, table: str, id: Any) -> bool:
        from bson import ObjectId
        try:
            oid = ObjectId(str(id))
        except Exception:
            oid = id
        result = await self._col(table).delete_one({"_id": oid})
        return result.deleted_count > 0

    async def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        MongoDB does not support SQL. Interprets 'sql' as a JSON aggregation pipeline string
        or raises a descriptive error.
        """
        import json
        try:
            pipeline = json.loads(sql)
            if not isinstance(pipeline, list):
                pipeline = [pipeline]
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400,
                detail="MongoDB adapter: 'sql' must be a valid JSON aggregation pipeline array."
            )
        # Extract collection name from params if provided
        collection_name = (params or {}).get("collection", "default")
        cursor = self._col(collection_name).aggregate(pipeline)
        return [_normalize(doc) async for doc in cursor]
