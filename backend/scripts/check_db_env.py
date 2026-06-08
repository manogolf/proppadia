#!/usr/bin/env python3
"""Safe DB environment diagnostics for Make/Codex/local runs."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import sys
from typing import Any, Optional
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False


REQUIRED_DB_VARS = ("DATABASE_URL", "SUPABASE_DB_URL")
OPTIONAL_SUPABASE_VARS = (
    "SUPABASE_URL",
    "VITE_SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_PUBLISHABLE_KEY",
    "VITE_SUPABASE_PUBLISHABLE_KEY",
    "SUPABASE_ANON_KEY",
    "VITE_SUPABASE_ANON_KEY",
)


def _clean(value: Optional[str]) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] in {"'", '"'} and text[-1] == text[0]:
        text = text[1:-1].strip()
    return text


def _mask_host(host: str) -> str:
    if not host:
        return ""
    if host in {"localhost", "127.0.0.1", "::1"}:
        return host
    parts = host.split(".")
    if len(parts) <= 2:
        return host[:2] + "***" if len(host) > 2 else "***"
    return f"{parts[0][:2]}***.{'.'.join(parts[-2:])}"


def _parse_db_url(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    return {
        "present": bool(url),
        "scheme": parsed.scheme or "",
        "hostname_present": bool(parsed.hostname),
        "hostname_masked": _mask_host(parsed.hostname or ""),
        "port_present": bool(parsed.port),
        "path_present": bool(parsed.path and parsed.path != "/"),
        "username_present": bool(parsed.username),
        "password_present": bool(parsed.password),
    }


def _dns_check(host: str) -> dict[str, Any]:
    if not host:
        return {"attempted": False, "ok": False, "error": "missing_hostname"}
    try:
        infos = socket.getaddrinfo(host, None)
        return {"attempted": True, "ok": True, "result_count": len(infos), "error": ""}
    except Exception as exc:
        return {"attempted": True, "ok": False, "result_count": 0, "error": f"{type(exc).__name__}: {exc}"}


def _connection_check(enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {"attempted": False, "ok": None, "error": ""}
    try:
        from backend.shared.db.pg import pg_fetchone

        row = pg_fetchone("SELECT 1 AS ok")
        return {"attempted": True, "ok": bool(row and row.get("ok") == 1), "error": ""}
    except Exception as exc:
        return {"attempted": True, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Safely diagnose DB env loading.")
    parser.add_argument("--check-connection", action="store_true")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    env_paths = [repo_root / ".env", repo_root / "backend/.env"]
    loaded = []
    for path in env_paths:
        loaded.append({"path": str(path), "exists": path.exists(), "loaded": bool(load_dotenv(path, override=False))})

    db_url = _clean(os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL"))
    parsed = _parse_db_url(db_url)
    host = urlparse(db_url).hostname or ""
    dns = _dns_check(host)
    conn = _connection_check(bool(args.check_connection))

    payload = {
        "cwd": str(Path.cwd()),
        "python_executable": sys.executable,
        "running_under_make": bool(os.getenv("MAKEFLAGS") or os.getenv("MAKELEVEL")),
        "running_under_codex_hint": bool(os.getenv("CODEX_SANDBOX") or os.getenv("CODEX_HOME")),
        "env_files": loaded,
        "required_env": {name: bool(_clean(os.getenv(name))) for name in REQUIRED_DB_VARS},
        "optional_supabase_env": {name: bool(_clean(os.getenv(name))) for name in OPTIONAL_SUPABASE_VARS},
        "db_url": parsed,
        "dns": dns,
        "connection": conn,
        "required_by": {
            "today_workspace": [
                "DATABASE_URL or SUPABASE_DB_URL",
                "psycopg",
                "DNS/network access to DB hostname",
            ],
            "hits_environment": [
                "DATABASE_URL or SUPABASE_DB_URL",
                "psycopg",
                "DNS/network access to DB hostname",
            ],
            "db_helper": "backend.shared.db.pg -> backend.supabase.supabase_utils.get_database_url",
        },
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    if not db_url or not parsed["hostname_present"] or not dns["ok"] or (conn["attempted"] and not conn["ok"]):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
