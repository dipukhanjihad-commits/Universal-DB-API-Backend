"""
DB Adapter Interface — Abstract base class defining the universal database contract.
All concrete adapters must implement every method defined here.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class DatabaseAdapter(ABC):
    """
    Universal interface for all database backends.
    Concrete implementations must not expose backend-specific APIs
    beyond what is defined here.
    """

    # ── Lifecycle ────────────────────────────────────────────────────────────
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection / connection pool."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully close all connections."""
        ...

    # ── CRUD ─────────────────────────────────────────────────────────────────
    @abstractmethod
    async def get_all(self, table: str) -> List[Dict[str, Any]]:
        """Return all rows from a table."""
        ...

    @abstractmethod
    async def get_by_id(self, table: str, id: Any) -> Optional[Dict[str, Any]]:
        """Return a single row by primary key value, or None."""
        ...

    @abstractmethod
    async def filter(
        self, table: str, column: str, value: Any
    ) -> List[Dict[str, Any]]:
        """Return rows where column equals value."""
        ...

    @abstractmethod
    async def insert_or_update(
        self, table: str, data: Dict[str, Any], pk: str = "id"
    ) -> Dict[str, Any]:
        """
        Upsert: if pk exists and matches a row → UPDATE.
        Otherwise → INSERT.
        Returns the final row state.
        """
        ...

    @abstractmethod
    async def delete(self, table: str, id: Any) -> bool:
        """Delete row by primary key. Returns True if a row was deleted."""
        ...

    @abstractmethod
    async def query(
        self, sql: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a parameterized SQL query and return results.
        Params must use named placeholders (:name style).
        """
        ...

    # ── Helpers ──────────────────────────────────────────────────────────────
    def adapter_name(self) -> str:
        return self.__class__.__name__
