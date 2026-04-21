# Universal DB API

A production-grade Python backend framework built with FastAPI that acts as a **universal database API layer** and **custom API engine**. Plug in any database backend, expose full CRUD over HTTP instantly, execute parameterized SQL securely, and register custom SQL-backed API endpoints — all from a single immutable config file.

---

## Architecture

```
universal_db_api/
├── app/
│   ├── main.py                     # FastAPI app factory + lifespan handler
│   ├── api/
│   │   ├── table.py                # Generic CRUD endpoints (/table/*)
│   │   ├── execute.py              # SQL execution endpoint (/execute)
│   │   ├── custom.py               # Custom API management (/custom/*)
│   │   └── health.py               # Health + info endpoints
│   ├── adapters/
│   │   ├── base.py                 # Abstract DatabaseAdapter interface
│   │   ├── registry.py             # Adapter factory + connection lifecycle
│   │   ├── sqlite_adapter.py       # Async SQLite (aiosqlite)
│   │   ├── sqlite_sync_adapter.py  # Sync SQLite fallback (stdlib only)
│   │   ├── postgresql_adapter.py   # PostgreSQL (asyncpg)
│   │   ├── mysql_adapter.py        # MySQL (aiomysql)
│   │   └── mongodb_adapter.py      # MongoDB (motor)
│   ├── core/
│   │   ├── config.py               # Immutable FrozenDict config loader
│   │   ├── models.py               # Pydantic request/response schemas
│   │   └── security.py             # SQL injection prevention, validation
│   └── engine/
│       └── custom_api.py           # Custom API CRUD + execution engine
├── config/
│   └── config.json                 # Single source of truth (immutable at runtime)
├── data/                           # Auto-created SQLite files land here
├── tests/
│   └── test_suite.py               # Full test suite (pytest-compatible)
├── run.py                          # Entry point — reads port from config
└── requirements.txt
```

### Design Principles

| Concern | Approach |
|---|---|
| **DB abstraction** | Adapter pattern — swap backends without touching API code |
| **Config** | FrozenDict singleton — loaded once, immutable at runtime |
| **Security** | Identifier allowlist + keyword blocking + parameter binding only |
| **Metadata** | Dedicated SQLite DB for custom API definitions (never mixed with app data) |
| **Async** | Full async/await throughout; sync-in-thread fallback for stdlib SQLite |

---

## Installation

### Minimum (SQLite only)
```bash
pip install fastapi uvicorn aiosqlite pydantic
```

### Full (all database backends)
```bash
pip install fastapi uvicorn aiosqlite pydantic asyncpg aiomysql "motor[asyncio]"
```

---

## Quick Start

### 1. Configure `config/config.json`

```json
{
  "server": { "port": 8000, "host": "0.0.0.0" },
  "active_db": "sqlite_main",
  "databases": {
    "sqlite_main": { "type": "sqlite", "path": "./data/app.db" }
  },
  "metadata_db": "sqlite_meta",
  "sqlite_meta": { "type": "sqlite", "path": "./data/meta.db" },
  "security": {
    "blocked_keywords": ["DROP", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE", "EXEC"],
    "max_result_rows": 10000,
    "allow_raw_sql": true
  }
}
```

### 2. Start the server

```bash
python run.py
```

Or directly with uvicorn:
```bash
uvicorn app.main:app --port 8000 --reload
```

### 3. Open interactive docs

```
http://localhost:8000/docs
```

---

## API Reference

### Health

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness probe |
| `GET` | `/info` | Connected DBs, active config |

---

### Generic Table CRUD — `/table/{table}`

All endpoints accept an optional `?db=<config_key>` query parameter to target a specific database connection. Defaults to `active_db` from config.

#### GET all rows
```http
GET /table/users
GET /table/users?db=postgres_main
```

#### GET with column filter
```http
GET /table/users?name=alice
GET /table/orders?status=pending
```

#### GET by primary key
```http
GET /table/users/42
```

#### Upsert (insert or update)
```http
POST /table/users
Content-Type: application/json

{
  "data": { "id": 42, "name": "Alice", "email": "alice@example.com" },
  "pk": "id"
}
```

**Upsert rules:**
- If `pk` field is present in `data` and a matching row exists → **UPDATE** (only provided columns; untouched columns are preserved)
- If `pk` is absent or no matching row exists → **INSERT**
- Columns present in `default_schemas` config but missing from `data` → filled with schema defaults
- Extra columns not in the schema → ignored during default-filling (passed through as-is)

#### DELETE by primary key
```http
DELETE /table/users/42
```

---

### SQL Execution — `/execute`

Executes a parameterized SQL statement using the active (or specified) database adapter.

```http
POST /execute
Content-Type: application/json

{
  "sql": "SELECT * FROM users WHERE name = :name AND active = :active",
  "params": [
    { "name": "name",   "value": "alice",  "type": "text"    },
    { "name": "active", "value": "true",   "type": "boolean" }
  ],
  "db_key": "postgres_main"
}
```

**Supported parameter types:**

| Type | Python coercion |
|---|---|
| `text` | `str(value)` |
| `number` | `float(value)` |
| `integer` | `int(value)` |
| `boolean` | `bool` (accepts `"true"`, `"1"`, `"yes"`) |
| `date` / `datetime` | `str(value)` (driver handles conversion) |
| `json` | `json.dumps(value)` if not already string |
| `blob` | raw passthrough |

**Security guarantees:**
- All values are bound via the driver's parameterized query API — never string-interpolated
- DDL keywords (`DROP`, `TRUNCATE`, `ALTER`, `CREATE`, `GRANT`, `REVOKE`, `EXEC`) are blocked before the query reaches the driver
- Results are capped at `security.max_result_rows` from config
- Raw SQL can be disabled entirely via `"allow_raw_sql": false` in config

---

### Custom API Engine — `/custom`

Define reusable, named SQL-backed endpoints stored in the metadata SQLite database.

#### List all custom APIs
```http
GET /custom
```

#### Register a new custom API
```http
POST /custom
Content-Type: application/json

{
  "name": "active_users",
  "method": "GET",
  "sql": "SELECT * FROM users WHERE status = 'active' ORDER BY created_at DESC",
  "db_type": "postgresql",
  "db_key": "postgres_main"
}
```

#### Update an existing custom API
```http
PUT /custom/active_users
Content-Type: application/json

{
  "sql": "SELECT id, name, email FROM users WHERE status = 'active'"
}
```

#### Execute a GET custom API (no params)
```http
GET /custom/active_users
```

#### Execute a custom API with parameters
```http
POST /custom/find_user/run
Content-Type: application/json

{
  "params": [
    { "name": "email", "value": "alice@example.com", "type": "text" }
  ]
}
```

#### Delete a custom API
```http
DELETE /custom/active_users
```

---

## Database Configuration

### PostgreSQL
```json
"databases": {
  "pg_prod": {
    "type": "postgresql",
    "host": "db.example.com",
    "port": 5432,
    "database": "mydb",
    "username": "app_user",
    "password": "secret"
  }
}
```
Install: `pip install asyncpg`

### MySQL
```json
"databases": {
  "mysql_prod": {
    "type": "mysql",
    "host": "db.example.com",
    "port": 3306,
    "database": "mydb",
    "username": "root",
    "password": "secret"
  }
}
```
Install: `pip install aiomysql`

### MongoDB
```json
"databases": {
  "mongo_prod": {
    "type": "mongodb",
    "uri": "mongodb://localhost:27017",
    "database": "mydb"
  }
}
```
Install: `pip install motor`

**MongoDB note:** The `/execute` endpoint for MongoDB interprets the `sql` field as a JSON aggregation pipeline array. Specify the collection name via `params`: `{ "name": "collection", "value": "users", "type": "text" }`.

---

## Default Schema Definitions

Define column defaults in config to auto-fill missing fields on upsert:

```json
"default_schemas": {
  "users": {
    "id":         { "type": "integer", "primary_key": true, "auto_increment": true },
    "name":       { "type": "text",    "default": "" },
    "email":      { "type": "text",    "default": "" },
    "created_at": { "type": "datetime","default": "now" }
  }
}
```

The special default `"now"` is replaced with the current UTC ISO timestamp at upsert time.

---

## Adding a New Database Adapter

1. Create `app/adapters/your_db_adapter.py` implementing `DatabaseAdapter`:

```python
from app.adapters.base import DatabaseAdapter

class YourDBAdapter(DatabaseAdapter):
    async def connect(self): ...
    async def disconnect(self): ...
    async def get_all(self, table): ...
    async def get_by_id(self, table, id): ...
    async def filter(self, table, column, value): ...
    async def insert_or_update(self, table, data, pk="id"): ...
    async def delete(self, table, id): ...
    async def query(self, sql, params=None): ...
```

2. Register it in `app/adapters/registry.py` inside `_build_adapter()`:

```python
case "yourdb":
    from app.adapters.your_db_adapter import YourDBAdapter
    return YourDBAdapter(db_cfg)
```

3. Add a connection entry under `databases` in `config.json`.

---

## Security Model

| Threat | Mitigation |
|---|---|
| SQL injection via values | Driver-level parameter binding (never string interpolation) |
| SQL injection via identifiers | Strict `^[a-zA-Z_][a-zA-Z0-9_]{0,63}$` allowlist for all table/column names |
| DDL execution | Blocked keyword list checked before any query reaches the driver |
| Result flooding | Configurable `max_result_rows` cap |
| Config tampering | FrozenDict raises `TypeError` on any runtime mutation attempt |
| Metadata pollution | Custom API definitions stored in isolated `sqlite_meta` database |

---

## Running Tests

```bash
# With pytest installed
pytest tests/ -v

# Without pytest (stdlib only)
python tests/test_suite.py
```

Test coverage includes:
- FrozenDict immutability (setitem, setattr, delitem)
- Config nested access and to_dict round-trip  
- Identifier validation (allowlist + rejection cases)
- SQL keyword blocking (DROP, TRUNCATE, ALTER, GRANT, EXEC)
- Type coercion for all supported param types
- SQLite adapter: insert/update/delete/filter/get_all/get_by_id/named params
- Upsert semantics (insert path, update path, column preservation)
- Custom API engine: create, list, get, update, execute, parameterized execute, delete
- UNIQUE constraint enforcement on API names
- Blocked SQL rejection at API creation time

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `APP_CONFIG_PATH` | `./config/config.json` | Override config file location |
