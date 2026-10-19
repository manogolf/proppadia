"""NHL database connection routing with an opt-in, fail-closed restricted mode.

Legacy mode deliberately preserves today's SUPABASE_DB_URL/DATABASE_URL
behavior. Restricted mode requires separate research, ingestion, and
operations-result DSNs and never falls back to a legacy variable.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Mapping, Sequence
from urllib.parse import quote, unquote, urlsplit, urlunsplit


_MODE_KEY = "NHL_DB_ACCESS_MODE"
_ACCESS = {
    "research": ("NHL_RESEARCH_DATABASE_URL", "proppadia_nhl_research_login"),
    "ingestion": ("NHL_INGEST_DATABASE_URL", "proppadia_nhl_daily_login"),
    # Operational results are writable, but this login has no raw-source DML.
    "operations": ("NHL_OPERATIONS_DATABASE_URL", "proppadia_nhl_operations_login"),
}


# Login roles with owner-level reach on the Supabase project.
OWNER_ROLES = frozenset({"postgres", "supabase_admin"})
# Legacy aliases and libpq routes a child could fall back to.
_FALLBACK_KEYS = frozenset({"SUPABASE_DB_URL", "DATABASE_URL", "DB"})
# Variables that application processes connect with; an owner login in any of
# them is what the step 7 guard refuses.
APPLICATION_ALIASES = _FALLBACK_KEYS | frozenset(key for key, _ in _ACCESS.values())
_GUARD_KEY = "NHL_OWNER_LOGIN_GUARD"
_REFERENCE = re.compile(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?")


def _login_role(value: object) -> str | None:
    text = str(value or "").strip()
    if not text.startswith(("postgres://", "postgresql://")):
        return None
    try:
        username = unquote(urlsplit(text).username or "")
    except ValueError:
        return None
    # Supabase pooler usernames may append a project reference after a dot.
    return username.split(".", 1)[0] or None


def owner_login_keys(environ: Mapping[str, str]) -> list[str]:
    """Names (never values) of variables that hold an owner-role login URL."""
    return sorted(key for key, value in environ.items() if _login_role(value) in OWNER_ROLES)


def owner_login_keys_in_file(path: Path) -> list[str]:
    """Names of dotenv assignments in ``path`` that hold an owner-role login URL."""
    return owner_login_keys(_dotenv_values(path))


def owner_login_guard(path: Path) -> dict[str, object]:
    """Names-only state of the owner-login guard for an env file (plan step 7).

    The guard is off until the file sets ``NHL_OWNER_LOGIN_GUARD=enforce``,
    which the operator adds when removing the owner login (plan step 4).
    """
    values = _dotenv_values(path)
    owners = set(owner_login_keys(values))
    return {
        "enforced": values.get(_GUARD_KEY, "").strip().lower() == "enforce",
        "owner_application_aliases": sorted(owners & APPLICATION_ALIASES),
    }


def require_no_owner_application_alias(path: Path) -> None:
    """Refuse to start when the enforced guard finds an owner login in an alias."""
    guard = owner_login_guard(path)
    if guard["enforced"] and guard["owner_application_aliases"]:
        raise RuntimeError(
            "NHL_OWNER_LOGIN_IN_APPLICATION_ALIAS:" + ",".join(guard["owner_application_aliases"]))


def _dotenv_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    path = Path(path)
    if path.is_file():
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if line.startswith("export "):
                line = line[len("export "):].lstrip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip().strip("'\"")
            # Follow a whole-value $NAME / ${NAME} reference to an earlier line.
            reference = _REFERENCE.fullmatch(value)
            values[key.strip()] = values.get(reference.group(1), "") if reference else value
    return values


def owner_free_child_environment(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Copy of an environment that a restricted NHL child may inherit.

    In restricted mode, drop the legacy aliases, libpq routes (``PG*`` except
    ``PGAPPNAME``), and any other variable holding an owner-role login. The
    restricted ``NHL_*`` routes stay. Legacy mode returns an unchanged copy.
    """
    env = dict(os.environ if environ is None else environ)
    if not restricted_mode(env):
        return env
    owners = set(owner_login_keys(env))
    return {
        key: value for key, value in env.items()
        if key not in _FALLBACK_KEYS and key not in owners
        and not (key.startswith("PG") and key != "PGAPPNAME")
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


def psql_command_without_password(
    cmd: Sequence[object], environ: Mapping[str, str] | None = None,
) -> tuple[list[str], dict[str, str]]:
    """Move any password in a psql argv DSN into the child's PGPASSWORD.

    Keeps the password out of process listings and out of command text that
    failure handlers copy into logs and receipts.
    """
    args = [str(part) for part in cmd]
    env = dict(os.environ if environ is None else environ)
    if not args or Path(args[0]).name != "psql":
        return args, env
    for index, token in enumerate(args[1:], start=1):
        if token.startswith(("postgres://", "postgresql://")):
            safe_dsn, password = psql_dsn_without_password(token)
            args[index] = safe_dsn
            if password:
                env["PGPASSWORD"] = password
            break
    return args, env


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
        if _login_role(value) != required_role:
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

