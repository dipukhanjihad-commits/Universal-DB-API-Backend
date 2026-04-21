"""
Pydantic Models — All request bodies, response envelopes, and domain types.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ── Enums ────────────────────────────────────────────────────────────────────
class DbType(str, Enum):
    sqlite = "sqlite"
    postgresql = "postgresql"
    mysql = "mysql"
    mongodb = "mongodb"


class HttpMethod(str, Enum):
    GET = "GET"
    POST = "POST"


class ParamType(str, Enum):
    number = "number"
    integer = "integer"
    text = "text"
    boolean = "boolean"
    date = "date"
    datetime_ = "datetime"
    json = "json"
    blob = "blob"


# ── Generic response envelope ─────────────────────────────────────────────────
class ApiResponse(BaseModel):
    success: bool = True
    data: Any = None
    message: Optional[str] = None
    count: Optional[int] = None

    @classmethod
    def ok(cls, data: Any, message: str = "OK", count: int | None = None) -> "ApiResponse":
        c = count if count is not None else (len(data) if isinstance(data, list) else None)
        return cls(success=True, data=data, message=message, count=c)

    @classmethod
    def error(cls, message: str) -> "ApiResponse":
        return cls(success=False, data=None, message=message)


# ── Table / upsert ────────────────────────────────────────────────────────────
class UpsertRequest(BaseModel):
    data: Dict[str, Any] = Field(..., description="Row data as key-value pairs")
    pk: str = Field(default="id", description="Primary key field name")


# ── SQL execution ─────────────────────────────────────────────────────────────
class SqlParam(BaseModel):
    name: str = Field(..., description="Named placeholder without the colon, e.g. 'user_id'")
    value: Any = Field(..., description="Parameter value")
    type: ParamType = Field(default=ParamType.text, description="Value type for coercion")

    @field_validator("name")
    @classmethod
    def name_must_be_safe(cls, v: str) -> str:
        import re
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,63}$", v):
            raise ValueError(f"Parameter name '{v}' is not a valid identifier.")
        return v


class ExecuteSqlRequest(BaseModel):
    sql: str = Field(..., description="Parameterized SQL using :name placeholders")
    params: List[SqlParam] = Field(default_factory=list)
    db_key: Optional[str] = Field(
        default=None,
        description="Optional config database key to target. Defaults to active_db."
    )


# ── Custom API ────────────────────────────────────────────────────────────────
class CustomApiCreate(BaseModel):
    name: str = Field(..., pattern=r"^[a-zA-Z_][a-zA-Z0-9_-]{0,63}$")
    method: HttpMethod = HttpMethod.GET
    sql: str
    db_type: DbType = DbType.sqlite
    db_key: Optional[str] = Field(
        default=None,
        description="Config database key to use. Falls back to active_db if omitted."
    )

    @field_validator("sql")
    @classmethod
    def sql_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("SQL cannot be empty.")
        return v.strip()


class CustomApiUpdate(BaseModel):
    method: Optional[HttpMethod] = None
    sql: Optional[str] = None
    db_type: Optional[DbType] = None
    db_key: Optional[str] = None

    @field_validator("sql")
    @classmethod
    def sql_not_empty_if_provided(cls, v: str | None) -> str | None:
        if v is not None and not v.strip():
            raise ValueError("SQL cannot be empty.")
        return v.strip() if v else v


class CustomApiRecord(BaseModel):
    id: int
    name: str
    method: str
    sql: str
    db_type: str
    db_key: Optional[str]
    created_at: str

    class Config:
        from_attributes = True


class CustomApiExecuteRequest(BaseModel):
    params: List[SqlParam] = Field(default_factory=list)
