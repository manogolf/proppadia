"""Shared DB-health checks for API routers."""

from __future__ import annotations

from typing import Any, Dict

from backend.app.deps import pg_fetchone


def ping_db() -> Dict[str, Any]:
    _ok, row, err = pg_fetchone("SELECT 1 AS ok")
    return {"ok": bool(row), "err": err}
