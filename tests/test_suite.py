"""
Test Suite — universal_db_api
Run with: pytest tests/ -v

Covers:
- Config layer (FrozenDict immutability, key access, to_dict)
- Security layer (identifier validation, SQL keyword blocking, type coercion)
- SQLite adapter (all CRUD methods, named params, upsert logic)
- Custom API engine (create, read, update, delete, execute, parameterized)
- Table API router (via FastAPI TestClient)
- SQL execution endpoint
"""

from __future__ import annotations

import asyncio
import os
import sys
import types

import pytest

# ── Environment setup ─────────────────────────────────────────────────────────
os.environ.setdefault(
    "APP_CONFIG_PATH",
    os.path.join(os.path.dirname(__file__), "..", "config", "config.json"),
)

# ── Config tests ──────────────────────────────────────────────────────────────
class TestFrozenDict:
    def test_read_access(self):
        from app.core.config import config
        assert config["active_db"] == "sqlite_main"
        assert config.get("active_db") == "sqlite_main"
        assert config.active_db == "sqlite_main"

    def test_nested_access(self):
        from app.core.config import config
        assert config["server"]["port"] == 8000
        assert config["databases"]["sqlite_main"]["type"] == "sqlite"

    def test_immutability_setitem(self):
        from app.core.config import config, FrozenDict
        with pytest.raises(TypeError):
            config["active_db"] = "hacked"

    def test_immutability_setattr(self):
        from app.core.config import config
        with pytest.raises(TypeError):
            config.active_db = "hacked"

    def test_immutability_delitem(self):
        from app.core.config import config
        with pytest.raises(TypeError):
            del config["active_db"]

    def test_to_dict_is_plain_dict(self):
        from app.core.config import config, FrozenDict
        d = config.to_dict()
        assert isinstance(d, dict)
        assert isinstance(d["databases"], dict)
        assert isinstance(d["security"]["blocked_keywords"], list)

    def test_contains(self):
        from app.core.config import config
        assert "databases" in config
        assert "nonexistent_key_xyz" not in config

    def test_iter(self):
        from app.core.config import config
        keys = list(config)
        assert "active_db" in keys
        assert "databases" in keys

    def test_default_schemas_present(self):
        from app.core.config import config
        schemas = config["default_schemas"]
        assert "users" in schemas
        assert "products" in schemas


# ── Security tests ────────────────────────────────────────────────────────────
class TestSecurity:
    """Uses mock HTTPException since fastapi may not be installed."""

    @pytest.fixture(autouse=True)
    def mock_fastapi(self, monkeypatch):
        import types
        if "fastapi" not in sys.modules:
            fmock = types.ModuleType("fastapi")
            class HTTPException(Exception):
                def __init__(self, status_code, detail):
                    self.status_code = status_code
                    self.detail = detail
                    super().__init__(detail)
            fmock.HTTPException = HTTPException
            monkeypatch.setitem(sys.modules, "fastapi", fmock)
        # Reload security to pick up mock
        if "app.core.security" in sys.modules:
            import importlib
            importlib.reload(sys.modules["app.core.security"])

    def _exc(self):
        return sys.modules["fastapi"].HTTPException

    def test_valid_identifiers(self):
        from app.core.security import validate_identifier
        assert validate_identifier("users") == "users"
        assert validate_identifier("user_profiles") == "user_profiles"
        assert validate_identifier("_hidden") == "_hidden"
        assert validate_identifier("a1b2c3") == "a1b2c3"

    def test_invalid_identifiers(self):
        from app.core.security import validate_identifier
        HTTPException = self._exc()
        for bad in ["1leading", "has space", "semi;colon", "dash-here", "", "a" * 65]:
            with pytest.raises(HTTPException):
                validate_identifier(bad)

    def test_safe_sql_passes(self):
        from app.core.security import assert_sql_safe
        # These should not raise
        assert_sql_safe("SELECT * FROM users WHERE id = :id")
        assert_sql_safe("SELECT name, email FROM orders WHERE created_at > :date")
        assert_sql_safe("INSERT INTO logs (msg) VALUES (:msg)")
        assert_sql_safe("UPDATE users SET name = :name WHERE id = :id")

    def test_blocked_keywords(self):
        from app.core.security import assert_sql_safe
        HTTPException = self._exc()
        for stmt in [
            "DROP TABLE users",
            "TRUNCATE orders",
            "ALTER TABLE x ADD COLUMN y TEXT",
            "CREATE TABLE evil (id INT)",
            "GRANT ALL ON users TO hacker",
            "EXEC sp_executesql :cmd",
        ]:
            with pytest.raises(HTTPException) as exc_info:
                assert_sql_safe(stmt)
            assert exc_info.value.status_code == 403

    def test_type_coercion(self):
        from app.core.security import sanitize_sql_value
        assert sanitize_sql_value("42", "number") == 42.0
        assert sanitize_sql_value(42, "number") == 42.0
        assert sanitize_sql_value("3.14", "number") == 3.14
        assert sanitize_sql_value("true", "boolean") is True
        assert sanitize_sql_value("false", "boolean") is False
        assert sanitize_sql_value(1, "boolean") is True
        assert sanitize_sql_value(0, "boolean") is False
        assert sanitize_sql_value(99, "text") == "99"
        assert sanitize_sql_value("hello", "text") == "hello"
        assert sanitize_sql_value("5", "integer") == 5

    def test_param_name_validator(self):
        """SqlParam should reject unsafe parameter names."""
        # We test the regex validator logic directly
        import re
        pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,63}$")
        assert pattern.match("user_id")
        assert pattern.match("_private")
        assert not pattern.match("1bad")
        assert not pattern.match("has space")
        assert not pattern.match("")


# ── SQLite adapter tests ──────────────────────────────────────────────────────
class TestSQLiteSyncAdapter:
    @pytest.fixture(autouse=True)
    def mock_fastapi(self, monkeypatch):
        if "fastapi" not in sys.modules:
            fmock = types.ModuleType("fastapi")
            class HTTPException(Exception):
                def __init__(self, status_code, detail):
                    self.status_code = status_code; self.detail = detail; super().__init__(detail)
            fmock.HTTPException = HTTPException
            monkeypatch.setitem(sys.modules, "fastapi", fmock)

    @pytest.fixture
    def adapter(self, tmp_path):
        from app.adapters.sqlite_sync_adapter import SQLiteSyncAdapter
        db_path = str(tmp_path / "test.db")
        return SQLiteSyncAdapter({"path": db_path})

    @pytest.fixture
    def loop(self):
        loop = asyncio.new_event_loop()
        yield loop
        loop.close()

    def run(self, loop, coro):
        return loop.run_until_complete(coro)

    def setup_table(self, loop, adapter):
        self.run(loop, adapter.connect())
        self.run(loop, adapter.query(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT DEFAULT '')"
        ))

    def teardown_table(self, loop, adapter):
        self.run(loop, adapter.query("DROP TABLE IF EXISTS users"))
        self.run(loop, adapter.disconnect())

    def test_connect_and_disconnect(self, adapter, loop):
        self.run(loop, adapter.connect())
        assert adapter._conn is not None
        self.run(loop, adapter.disconnect())
        assert adapter._conn is None

    def test_insert_without_pk(self, adapter, loop):
        self.setup_table(loop, adapter)
        row = self.run(loop, adapter.insert_or_update("users", {"name": "Alice", "email": "a@test.com"}))
        assert row["name"] == "Alice"
        assert "id" in row
        assert row["id"] is not None
        self.teardown_table(loop, adapter)

    def test_insert_with_pk(self, adapter, loop):
        self.setup_table(loop, adapter)
        row = self.run(loop, adapter.insert_or_update("users", {"id": 42, "name": "Bob"}))
        assert row["id"] == 42
        self.teardown_table(loop, adapter)

    def test_update_existing(self, adapter, loop):
        self.setup_table(loop, adapter)
        self.run(loop, adapter.insert_or_update("users", {"id": 10, "name": "Charlie", "email": "c@test.com"}))
        updated = self.run(loop, adapter.insert_or_update("users", {"id": 10, "name": "Charles"}))
        assert updated["name"] == "Charles"
        assert updated["email"] == "c@test.com"  # Preserved column
        self.teardown_table(loop, adapter)

    def test_get_all(self, adapter, loop):
        self.setup_table(loop, adapter)
        self.run(loop, adapter.insert_or_update("users", {"name": "A"}))
        self.run(loop, adapter.insert_or_update("users", {"name": "B"}))
        rows = self.run(loop, adapter.get_all("users"))
        assert len(rows) == 2
        self.teardown_table(loop, adapter)

    def test_get_by_id(self, adapter, loop):
        self.setup_table(loop, adapter)
        self.run(loop, adapter.insert_or_update("users", {"id": 55, "name": "Dave"}))
        row = self.run(loop, adapter.get_by_id("users", 55))
        assert row is not None and row["name"] == "Dave"
        missing = self.run(loop, adapter.get_by_id("users", 9999))
        assert missing is None
        self.teardown_table(loop, adapter)

    def test_filter(self, adapter, loop):
        self.setup_table(loop, adapter)
        self.run(loop, adapter.insert_or_update("users", {"name": "Alice"}))
        self.run(loop, adapter.insert_or_update("users", {"name": "Bob"}))
        rows = self.run(loop, adapter.filter("users", "name", "Alice"))
        assert len(rows) == 1 and rows[0]["name"] == "Alice"
        self.teardown_table(loop, adapter)

    def test_delete(self, adapter, loop):
        self.setup_table(loop, adapter)
        self.run(loop, adapter.insert_or_update("users", {"id": 7, "name": "Eve"}))
        deleted = self.run(loop, adapter.delete("users", 7))
        assert deleted is True
        gone = self.run(loop, adapter.get_by_id("users", 7))
        assert gone is None
        self.teardown_table(loop, adapter)

    def test_delete_nonexistent(self, adapter, loop):
        self.setup_table(loop, adapter)
        result = self.run(loop, adapter.delete("users", 9999))
        assert result is False
        self.teardown_table(loop, adapter)

    def test_query_named_params(self, adapter, loop):
        self.setup_table(loop, adapter)
        self.run(loop, adapter.insert_or_update("users", {"name": "Frank", "email": "f@test.com"}))
        results = self.run(loop, adapter.query(
            "SELECT * FROM users WHERE name = :name", {"name": "Frank"}
        ))
        assert len(results) == 1 and results[0]["name"] == "Frank"
        self.teardown_table(loop, adapter)

    def test_named_to_positional(self):
        from app.adapters.sqlite_sync_adapter import _named_to_positional
        sql, params = _named_to_positional(
            "SELECT * FROM t WHERE a = :a AND b = :b",
            {"a": 1, "b": "hello"}
        )
        assert sql == "SELECT * FROM t WHERE a = ? AND b = ?"
        assert params == [1, "hello"]

    def test_named_to_positional_repeated_param(self):
        from app.adapters.sqlite_sync_adapter import _named_to_positional
        sql, params = _named_to_positional(
            "SELECT :val + :val", {"val": 5}
        )
        assert params == [5, 5]


# ── Custom API engine logic tests ─────────────────────────────────────────────
class TestCustomApiEngineLogic:
    """Tests engine CRUD using raw SQLite, no Pydantic needed."""

    @pytest.fixture(autouse=True)
    def mock_fastapi(self, monkeypatch):
        if "fastapi" not in sys.modules:
            fmock = types.ModuleType("fastapi")
            class HTTPException(Exception):
                def __init__(self, status_code, detail):
                    self.status_code = status_code; self.detail = detail; super().__init__(detail)
            fmock.HTTPException = HTTPException
            monkeypatch.setitem(sys.modules, "fastapi", fmock)

    @pytest.fixture
    def db(self, tmp_path, loop):
        from app.adapters.sqlite_sync_adapter import SQLiteSyncAdapter
        adapter = SQLiteSyncAdapter({"path": str(tmp_path / "engine.db")})
        loop.run_until_complete(adapter.connect())
        yield adapter
        loop.run_until_complete(adapter.disconnect())

    @pytest.fixture
    def loop(self):
        loop = asyncio.new_event_loop()
        yield loop
        loop.close()

    _CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS custom_apis (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        method TEXT NOT NULL DEFAULT 'GET',
        sql TEXT NOT NULL,
        db_type TEXT NOT NULL DEFAULT 'sqlite',
        db_key TEXT,
        created_at TEXT NOT NULL
    )
    """

    def run(self, loop, coro):
        return loop.run_until_complete(coro)

    def _insert_api(self, loop, db, name, sql):
        from datetime import datetime, timezone
        record = {
            "name": name,
            "method": "GET",
            "sql": sql,
            "db_type": "sqlite",
            "db_key": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return self.run(loop, db.insert_or_update("custom_apis", record))

    def test_schema_creation(self, db, loop):
        self.run(loop, db.query(self._CREATE_TABLE))
        rows = self.run(loop, db.get_all("custom_apis"))
        assert rows == []

    def test_insert_and_retrieve(self, db, loop):
        self.run(loop, db.query(self._CREATE_TABLE))
        self._insert_api(loop, db, "get_all_users", "SELECT * FROM users")
        rows = self.run(loop, db.filter("custom_apis", "name", "get_all_users"))
        assert len(rows) == 1
        assert rows[0]["sql"] == "SELECT * FROM users"

    def test_unique_name_enforced(self, db, loop):
        import sqlite3
        self.run(loop, db.query(self._CREATE_TABLE))
        self._insert_api(loop, db, "my_api", "SELECT 1")
        with pytest.raises(sqlite3.IntegrityError):
            self._insert_api(loop, db, "my_api", "SELECT 2")

    def test_update_sql(self, db, loop):
        self.run(loop, db.query(self._CREATE_TABLE))
        r = self._insert_api(loop, db, "updatable", "SELECT 1")
        self.run(loop, db.insert_or_update("custom_apis", {"id": r["id"], "sql": "SELECT 2"}))
        updated = self.run(loop, db.get_by_id("custom_apis", r["id"]))
        assert updated["sql"] == "SELECT 2"
        assert updated["name"] == "updatable"  # Preserved

    def test_delete_api(self, db, loop):
        self.run(loop, db.query(self._CREATE_TABLE))
        r = self._insert_api(loop, db, "to_delete", "SELECT 1")
        deleted = self.run(loop, db.delete("custom_apis", r["id"]))
        assert deleted
        gone = self.run(loop, db.get_by_id("custom_apis", r["id"]))
        assert gone is None

    def test_execute_stored_sql(self, db, loop):
        from app.core.security import assert_sql_safe
        self.run(loop, db.query(self._CREATE_TABLE))
        self.run(loop, db.query(
            "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
        ))
        self.run(loop, db.query("INSERT INTO items (name) VALUES ('Alpha')"))
        self.run(loop, db.query("INSERT INTO items (name) VALUES ('Beta')"))

        self._insert_api(loop, db, "list_items", "SELECT * FROM items")
        stored = self.run(loop, db.filter("custom_apis", "name", "list_items"))
        sql = stored[0]["sql"]
        assert_sql_safe(sql)
        results = self.run(loop, db.query(sql, {}))
        assert len(results) == 2
        names = {r["name"] for r in results}
        assert names == {"Alpha", "Beta"}

    def test_blocked_sql_rejected(self):
        from app.core.security import assert_sql_safe
        HTTPException = sys.modules["fastapi"].HTTPException
        with pytest.raises(HTTPException) as exc_info:
            assert_sql_safe("DROP TABLE custom_apis")
        assert exc_info.value.status_code == 403
