"""NHL database connection routing with an opt-in, fail-closed restricted mode.

Legacy mode deliberately preserves today's SUPABASE_DB_URL/DATABASE_URL
behavior. Restricted mode requires separate research, ingestion, and
operations-result DSNs and never falls back to a legacy variable.
"""

from __future__ import annotations

import os
from typing import Mapping
from urllib.parse import quote, unquote, urlsplit, urlunsplit


_MODE_KEY = "NHL_DB_ACCESS_MODE"
_ACCESS = {
    "research": ("NHL_RESEARCH_DATABASE_URL", "proppadia_nhl_research_login"),
    "ingestion": ("NHL_INGEST_DATABASE_URL", "proppadia_nhl_daily_login"),
    # Operational results are writable, but this login has no raw-source DML.
    "operations": ("NHL_OPERATIONS_DATABASE_URL", "proppadia_nhl_operations_login"),
}


def psql_dsn_without_password(url: str) -> tuple[str, str | None]:
    """Return a libpq URI safe for argv plus its password for child env use."""
    parsed = urlsplit(url)
    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "") or None
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise RuntimeError("NHL_PSQL_DSN_INVALID")
    hostname = parsed.hostname
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    if parsed.port:
        hostname += f":{parsed.port}"
    netloc = f"{quote(username, safe='')}@{hostname}" if username else hostname
    safe_url = urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
    return safe_url, password


def restricted_mode(environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    mode = env.get(_MODE_KEY, "legacy").strip().lower() or "legacy"
    if mode not in {"legacy", "restricted"}:
        raise RuntimeError("NHL_DB_ACCESS_MODE_INVALID")
    return mode == "restricted"


def database_url(
    access: str,
    *,
    environ: Mapping[str, str] | None = None,
    legacy_url: str | None = None,
) -> str:
    """Resolve an NHL DSN; reject privileged/legacy fallback in restricted mode."""
    if access not in _ACCESS:
        raise ValueError("NHL_DATABASE_ACCESS_CLASS_INVALID")
    env = os.environ if environ is None else environ
    if restricted_mode(env):
        key, required_role = _ACCESS[access]
        value = (env.get(key) or "").strip()
        if not value:
            raise RuntimeError(f"NHL_RESTRICTED_DATABASE_URL_REQUIRED:{key}")
        parsed = urlsplit(value)
        username = unquote(parsed.username or "")
        # Supabase pooler usernames may append a project reference after a dot.
        role_part = username.split(".", 1)[0]
        if parsed.scheme not in {"postgres", "postgresql"} or role_part != required_role:
            raise RuntimeError(f"NHL_RESTRICTED_DATABASE_ROLE_MISMATCH:{access}")
        return value

    value = (legacy_url or env.get("SUPABASE_DB_URL") or env.get("DATABASE_URL") or "").strip()
    if not value:
        raise RuntimeError("NHL_DATABASE_URL_REQUIRED")
    return value


def routed_database_environment(
    access: str, *, environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return the selected role DSN and its legacy aliases for a child.

    Existing ingestion scripts resolve their access class again inside the
    child. Keep only that selected restricted variable available so the child
    can validate its role; the CLI strips all other role DSNs before launch.
    """
    env = os.environ if environ is None else environ
    url = database_url(access, environ=env)
    result = {"SUPABASE_DB_URL": url, "DATABASE_URL": url}
    if restricted_mode(env):
        key, _ = _ACCESS[access]
        result[key] = url
    return result

