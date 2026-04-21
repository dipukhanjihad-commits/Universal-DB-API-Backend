"""
Entry Point — Reads server config and starts Uvicorn.
Run: python run.py
"""

import json
import os
import sys
from pathlib import Path


def get_server_config() -> dict:
    config_path = Path(__file__).parent / "config" / "config.json"
    with open(config_path) as f:
        cfg = json.load(f)
    return cfg.get("server", {})


if __name__ == "__main__":
    import uvicorn

    server = get_server_config()
    host = server.get("host", "0.0.0.0")
    port = int(server.get("port", 8000))
    reload = bool(server.get("reload", False))

    print(f"Starting Universal DB API on http://{host}:{port}")
    print(f"Docs available at http://{host}:{port}/docs")

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )
