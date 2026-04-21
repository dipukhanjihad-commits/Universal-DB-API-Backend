"""
Config Layer — Immutable configuration loaded once at startup.
Uses a FrozenDict-like structure to prevent runtime mutation.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterator


class FrozenDict:
    """
    Immutable dictionary wrapper. Values are recursively frozen.
    Raises TypeError on any attempted mutation.
    """

    def __init__(self, data: dict) -> None:
        object.__setattr__(self, "_data", {
            k: FrozenDict(v) if isinstance(v, dict) else
               tuple(FrozenDict(i) if isinstance(i, dict) else i for i in v) if isinstance(v, list) else v
            for k, v in data.items()
        })

    # ── Read access ─────────────────────────────────────────────────────────
    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __getattr__(self, key: str) -> Any:
        try:
            return self._data[key]
        except KeyError:
            raise AttributeError(f"Config has no key '{key}'")

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"FrozenDict({self._data!r})"

    # ── Block all mutation ───────────────────────────────────────────────────
    def __setattr__(self, key: str, value: Any) -> None:
        raise TypeError("Config is immutable and cannot be modified at runtime.")

    def __setitem__(self, key: str, value: Any) -> None:
        raise TypeError("Config is immutable and cannot be modified at runtime.")

    def __delitem__(self, key: str) -> None:
        raise TypeError("Config is immutable and cannot be modified at runtime.")

    # ── Serialization helper ─────────────────────────────────────────────────
    def to_dict(self) -> dict:
        result = {}
        for k, v in self._data.items():
            if isinstance(v, FrozenDict):
                result[k] = v.to_dict()
            elif isinstance(v, tuple):
                result[k] = [i.to_dict() if isinstance(i, FrozenDict) else i for i in v]
            else:
                result[k] = v
        return result


def _load_config(path: str | Path) -> FrozenDict:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return FrozenDict(raw)


# ── Singleton ────────────────────────────────────────────────────────────────
_CONFIG_PATH = os.environ.get(
    "APP_CONFIG_PATH",
    Path(__file__).parent.parent / "config" / "config.json"
)
config: FrozenDict = _load_config(_CONFIG_PATH)
