"""
Security Layer — SQL injection prevention, identifier validation, query sanitization.
All user-supplied table/column names are validated before use.
All query execution uses parameter binding only.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import HTTPException

from app.core.config import config

# ── Identifier validation ────────────────────────────────────────────────────
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,63}$")


def validate_identifier(name: str, label: str = "identifier") -> str:
    """
    Validates a table or column name against a strict allowlist pattern.
    Raises HTTP 400 if the name is unsafe.
    """
    if not _SAFE_IDENTIFIER.match(name):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {label} '{name}'. Only alphanumeric characters and underscores allowed."
        )
    return name


def validate_table(table: str) -> str:
    return validate_identifier(table, "table name")


def validate_column(column: str) -> str:
    return validate_identifier(column, "column name")


# ── SQL keyword blocking ─────────────────────────────────────────────────────
def _get_blocked_keywords() -> list[str]:
    try:
        return list(config["security"]["blocked_keywords"])
    except (KeyError, AttributeError):
        return ["DROP", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE", "EXEC", "EXECUTE"]


def assert_sql_safe(sql: str) -> None:
    """
    Checks for blocked DDL/admin keywords in a SQL string.
    Raises HTTP 403 if any dangerous keyword is found.
    This is a defense-in-depth measure; parameter binding is the primary protection.
    """
    blocked = _get_blocked_keywords()
    upper_sql = sql.upper()

    # Tokenize to avoid false positives (e.g., 'execution' containing 'exec')
    tokens = re.findall(r"\b\w+\b", upper_sql)
    for keyword in blocked:
        if keyword.upper() in tokens:
            raise HTTPException(
                status_code=403,
                detail=f"SQL contains a restricted keyword: '{keyword}'. "
                       f"DDL and administrative statements are not permitted."
            )


def sanitize_sql_value(value: Any, declared_type: str) -> Any:
    """
    Coerces a query parameter value to its declared Python type.
    Raises HTTP 422 on type mismatch.
    """
    try:
        match declared_type.lower():
            case "number":
                return float(value)
            case "integer" | "int":
                return int(value)
            case "boolean" | "bool":
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ("true", "1", "yes")
            case "text" | "string" | "str":
                return str(value)
            case "date" | "datetime":
                return str(value)  # Passed as string; DB driver handles conversion
            case "json":
                import json
                return json.dumps(value) if not isinstance(value, str) else value
            case "blob":
                return value  # Raw bytes passthrough
            case _:
                return value
    except (ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Cannot coerce value '{value}' to type '{declared_type}': {exc}"
        )
