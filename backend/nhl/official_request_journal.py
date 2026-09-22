"""Governed official-NHL HTTP accounting and response reuse.

The postgame reconciler enables this module through environment variables.  In
that mode every HTTP attempt is an append-only journal record and schedule /
boxscore authority responses may be reused by child collectors.  Standalone
collector behavior is unchanged when no governed context is present.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlsplit

import requests


ENV_REQUIRED = "NHL_OFFICIAL_REQUEST_JOURNAL_REQUIRED"
ENV_RUN_ID = "NHL_RECONCILIATION_RUN_ID"
ENV_JOURNAL = "NHL_OFFICIAL_REQUEST_JOURNAL"
ENV_CACHE = "NHL_OFFICIAL_RESPONSE_CACHE"
ENV_SLATE = "NHL_REQUESTED_SLATE_DATE"
ENV_GAME_HASH = "NHL_CANONICAL_GAME_SET_HASH"
ENV_GAME_IDS = "NHL_CANONICAL_GAME_IDS"
ENV_SOURCE_CACHE = "NHL_OFFICIAL_RESPONSE_SOURCE_CACHE"
ENV_SOURCE_RUN_ID = "NHL_OFFICIAL_RESPONSE_SOURCE_RUN_ID"
ENV_SOURCE_JOURNAL_SHA256 = "NHL_OFFICIAL_RESPONSE_SOURCE_JOURNAL_SHA256"
CONTRACT = "NHL_OFFICIAL_REQUEST_JOURNAL_V1"
SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]+$")
ROSTER_REDIRECT_POLICY = "NHL_ROSTER_CURRENT_TO_OFFICIAL_SEASON_V1"
ROSTER_TEAM = re.compile(r"^[A-Z]{3}$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def request_run_tree_fingerprint(request_root: Path, *, repository_root: Path) -> str:
    base = repository_root.resolve()
    lines = []
    for path in sorted(item for item in request_root.rglob("*") if item.is_file()):
        try:
            relative = path.resolve().relative_to(base).as_posix()
        except ValueError as error:
            raise RuntimeError("REQUEST_RUN_OUTSIDE_REPOSITORY") from error
        lines.append(f"{sha256_file(path)}  {relative}\n")
    return sha256_bytes("".join(lines).encode())


def canonical_game_set_hash(game_ids: Iterable[int]) -> str:
    payload = json.dumps(sorted({int(value) for value in game_ids}), separators=(",", ":"))
    return sha256_bytes(payload.encode())


def official_season_id(repository_season: int) -> str:
    season = int(repository_season)
    if season < 2000 or season > 2999:
        raise RuntimeError("OFFICIAL_ROSTER_REPOSITORY_SEASON_INVALID")
    return f"{season:04d}{season + 1:04d}"


def _sanitized_target(url: str) -> dict[str, Any]:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except (TypeError, ValueError) as error:
        raise RuntimeError("OFFICIAL_REDIRECT_LOCATION_MALFORMED") from error
    return {"scheme": parsed.scheme.lower(), "host": (parsed.hostname or "").lower(),
            "port": port, "path": parsed.path}


def _validate_roster_redirect_source(url: str, policy: dict[str, Any]) -> tuple[str, int]:
    if policy.get("policy") != ROSTER_REDIRECT_POLICY:
        raise RuntimeError("OFFICIAL_REDIRECT_POLICY_UNKNOWN")
    team = str(policy.get("team") or "").upper()
    if not ROSTER_TEAM.fullmatch(team):
        raise RuntimeError("OFFICIAL_ROSTER_TEAM_INVALID")
    try:
        season = int(policy.get("repository_season"))
        parsed = urlsplit(url)
        port = parsed.port
    except (TypeError, ValueError) as error:
        raise RuntimeError("OFFICIAL_ROSTER_REDIRECT_SOURCE_MALFORMED") from error
    if (parsed.scheme.lower() != "https" or (parsed.hostname or "").lower() != "api-web.nhle.com"
            or port not in (None, 443) or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment or parsed.path != f"/v1/roster/{team}/current"):
        raise RuntimeError("OFFICIAL_ROSTER_REDIRECT_SOURCE_NOT_ALLOWLISTED")
    return team, season


def _resolve_roster_redirect(location: str | None, *, source_url: str,
                             team: str, repository_season: int,
                             visited: set[str]) -> tuple[str, dict[str, Any]]:
    if not location:
        raise RuntimeError("OFFICIAL_REDIRECT_LOCATION_MISSING")
    try:
        normalized = urljoin(source_url, location)
        parsed = urlsplit(normalized)
        port = parsed.port
    except (TypeError, ValueError) as error:
        raise RuntimeError("OFFICIAL_REDIRECT_LOCATION_MALFORMED") from error
    expected_path = f"/v1/roster/{team}/{official_season_id(repository_season)}"
    if normalized in visited:
        raise RuntimeError("OFFICIAL_REDIRECT_LOOP")
    if parsed.scheme.lower() != "https":
        raise RuntimeError("OFFICIAL_REDIRECT_HTTPS_REQUIRED")
    if (parsed.hostname or "").lower() != "api-web.nhle.com":
        raise RuntimeError("OFFICIAL_REDIRECT_HOST_NOT_ALLOWLISTED")
    if port not in (None, 443):
        raise RuntimeError("OFFICIAL_REDIRECT_NONSTANDARD_PORT")
    if parsed.username is not None or parsed.password is not None:
        raise RuntimeError("OFFICIAL_REDIRECT_CREDENTIALS_FORBIDDEN")
    if parsed.query:
        raise RuntimeError("OFFICIAL_REDIRECT_QUERY_FORBIDDEN")
    if parsed.fragment:
        raise RuntimeError("OFFICIAL_REDIRECT_FRAGMENT_FORBIDDEN")
    if parsed.path != expected_path:
        raise RuntimeError("OFFICIAL_REDIRECT_ROSTER_IDENTITY_MISMATCH")
    return normalized, _sanitized_target(normalized)


def verify_payload_identity(family: str, identity: dict[str, Any], body: bytes) -> None:
    try:
        payload = json.loads(body)
    except Exception as error:
        raise RuntimeError("OFFICIAL_RESPONSE_NOT_JSON") from error
    if family == "SCHEDULE":
        slate = str(identity.get("slate_date") or "")
        parent_dates = {str(day.get("date") or "") for day in payload.get("gameWeek", []) or []}
        top_dates = {str(game.get("gameDate") or "") for game in payload.get("games", []) or []}
        if slate not in parent_dates and slate not in top_dates:
            raise RuntimeError("OFFICIAL_RESPONSE_SCHEDULE_DATE_MISMATCH")
    elif family == "BOXSCORE":
        expected = int(identity["game_id"])
        raw = payload.get("id") or payload.get("gameId") or payload.get("gamePk")
        if raw is None or int(raw) != expected:
            raise RuntimeError("OFFICIAL_RESPONSE_BOXSCORE_GAME_MISMATCH")


@dataclass(frozen=True)
class RequestContext:
    run_id: str
    journal_path: Path
    cache_dir: Path
    slate_date: str
    game_set_hash: str
    game_ids: frozenset[int]
    source_cache_dir: Path | None = None
    source_run_id: str | None = None
    source_journal_sha256: str | None = None

    @classmethod
    def from_env(cls, *, required: bool | None = None) -> "RequestContext | None":
        must_exist = os.environ.get(ENV_REQUIRED) == "1" if required is None else required
        values = {
            "run_id": os.environ.get(ENV_RUN_ID, "").strip(),
            "journal": os.environ.get(ENV_JOURNAL, "").strip(),
            "cache": os.environ.get(ENV_CACHE, "").strip(),
            "slate": os.environ.get(ENV_SLATE, "").strip(),
            "hash": os.environ.get(ENV_GAME_HASH, "").strip(),
            "ids": os.environ.get(ENV_GAME_IDS, "").strip(),
        }
        if not any(values.values()) and not must_exist:
            return None
        missing = sorted(key for key, value in values.items() if not value)
        if missing:
            raise RuntimeError(f"OFFICIAL_REQUEST_JOURNAL_CONFIG_INCOMPLETE:{','.join(missing)}")
        if not SAFE_TOKEN.fullmatch(values["run_id"]):
            raise RuntimeError("OFFICIAL_REQUEST_RUN_ID_INVALID")
        try:
            ids = frozenset(int(value) for value in values["ids"].split(","))
        except ValueError as error:
            raise RuntimeError("OFFICIAL_REQUEST_GAME_IDS_INVALID") from error
        if not ids or canonical_game_set_hash(ids) != values["hash"]:
            raise RuntimeError("OFFICIAL_REQUEST_CANONICAL_GAME_SET_HASH_MISMATCH")
        source_values = {
            "cache": os.environ.get(ENV_SOURCE_CACHE, "").strip(),
            "run_id": os.environ.get(ENV_SOURCE_RUN_ID, "").strip(),
            "journal_sha256": os.environ.get(ENV_SOURCE_JOURNAL_SHA256, "").strip(),
        }
        if any(source_values.values()) and not all(source_values.values()):
            raise RuntimeError("OFFICIAL_RESPONSE_SOURCE_CONFIG_INCOMPLETE")
        if source_values["run_id"] and not SAFE_TOKEN.fullmatch(source_values["run_id"]):
            raise RuntimeError("OFFICIAL_RESPONSE_SOURCE_RUN_ID_INVALID")
        if source_values["journal_sha256"] and not re.fullmatch(r"[0-9a-f]{64}", source_values["journal_sha256"]):
            raise RuntimeError("OFFICIAL_RESPONSE_SOURCE_JOURNAL_HASH_INVALID")
        if source_values["cache"]:
            source_journal = Path(source_values["cache"]).parent / "official_request_journal.jsonl"
            if (not source_journal.is_file() or
                    sha256_bytes(source_journal.read_bytes()) != source_values["journal_sha256"]):
                raise RuntimeError("OFFICIAL_RESPONSE_SOURCE_JOURNAL_CHANGED")
        context = cls(
            run_id=values["run_id"], journal_path=Path(values["journal"]),
            cache_dir=Path(values["cache"]), slate_date=values["slate"],
            game_set_hash=values["hash"], game_ids=ids,
            source_cache_dir=Path(source_values["cache"]) if source_values["cache"] else None,
            source_run_id=source_values["run_id"] or None,
            source_journal_sha256=source_values["journal_sha256"] or None,
        )
        context._ensure_storage()
        return context

    def _ensure_storage(self) -> None:
        for directory in (self.journal_path.parent, self.cache_dir):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(directory, 0o700)
        if not self.journal_path.exists():
            try:
                descriptor = os.open(self.journal_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(descriptor)
            except FileExistsError:
                pass
        os.chmod(self.journal_path, 0o600)

    def validate_identity(self, identity: dict[str, Any]) -> None:
        if identity.get("slate_date") not in (None, self.slate_date):
            raise RuntimeError("OFFICIAL_REQUEST_UNRELATED_SLATE_DATE")
        if identity.get("game_id") is not None and int(identity["game_id"]) not in self.game_ids:
            raise RuntimeError("OFFICIAL_REQUEST_UNRELATED_GAME_ID")

    def append(self, record: dict[str, Any]) -> None:
        safe = {
            "contract_version": CONTRACT, "run_id": self.run_id,
            "canonical_game_set_hash": self.game_set_hash,
            **record,
        }
        data = (json.dumps(safe, sort_keys=True, separators=(",", ":")) + "\n").encode()
        descriptor = os.open(self.journal_path, os.O_APPEND | os.O_WRONLY, 0o600)
        try:
            os.write(descriptor, data)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _cache_index(self, family: str, identity: dict[str, Any], cache_dir: Path | None = None) -> Path:
        token = sha256_bytes(json.dumps(
            {"endpoint_family": family, "identity": identity},
            sort_keys=True, separators=(",", ":"),
        ).encode())
        return (cache_dir or self.cache_dir) / "index" / f"{token}.json"

    def preserve(self, family: str, identity: dict[str, Any], body: bytes) -> tuple[Path, str]:
        verify_payload_identity(family, identity, body)
        digest = sha256_bytes(body)
        object_dir = self.cache_dir / "objects"
        index_path = self._cache_index(family, identity)
        object_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        index_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(object_dir, 0o700)
        os.chmod(index_path.parent, 0o700)
        object_path = object_dir / f"{digest}.json"
        if not object_path.exists():
            descriptor = os.open(object_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                os.write(descriptor, body)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        metadata = {
            "contract_version": CONTRACT, "endpoint_family": family,
            "identity": identity, "response_sha256": digest,
            "response_bytes": len(body), "object_name": object_path.name,
        }
        encoded = (json.dumps(metadata, sort_keys=True, indent=2) + "\n").encode()
        if index_path.exists():
            if json.loads(index_path.read_text()) != metadata:
                raise RuntimeError("OFFICIAL_RESPONSE_CACHE_INDEX_CONFLICT")
        else:
            descriptor = os.open(index_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                os.write(descriptor, encoded)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return object_path, digest

    def reuse(self, family: str, identity: dict[str, Any]) -> tuple[bytes, str, dict[str, Any]]:
        cache_dir = self.cache_dir
        index_path = self._cache_index(family, identity, cache_dir)
        cross_run = False
        if not index_path.is_file() and self.source_cache_dir is not None:
            cache_dir = self.source_cache_dir
            index_path = self._cache_index(family, identity, cache_dir)
            cross_run = True
        if not index_path.is_file():
            raise RuntimeError("PRESERVED_RESPONSE_NOT_FOUND")
        metadata = json.loads(index_path.read_text())
        if metadata.get("endpoint_family") != family or metadata.get("identity") != identity:
            raise RuntimeError("PRESERVED_RESPONSE_IDENTITY_MISMATCH")
        object_path = cache_dir / "objects" / str(metadata.get("object_name", ""))
        body = object_path.read_bytes()
        digest = sha256_bytes(body)
        if digest != metadata.get("response_sha256") or len(body) != metadata.get("response_bytes"):
            raise RuntimeError("PRESERVED_RESPONSE_HASH_MISMATCH")
        verify_payload_identity(family, identity, body)
        provenance = {
            "source_run_id": self.source_run_id if cross_run else self.run_id,
            "source_journal_sha256": self.source_journal_sha256 if cross_run else None,
            "source_response_index_sha256": sha256_bytes(index_path.read_bytes()),
            "source_response_object_sha256": digest,
            "cross_run_reuse": cross_run,
        }
        return body, digest, provenance


class PreservedResponse:
    def __init__(self, body: bytes, status_code: int = 200):
        self.content = body
        self.status_code = status_code

    def json(self) -> Any:
        return json.loads(self.content)

    def raise_for_status(self) -> None:
        if not 200 <= self.status_code < 300:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def _unguarded_get(session: Any, url: str, **kwargs: Any) -> Any:
    return session.get(url, **kwargs) if session is not None else requests.get(url, **kwargs)


def official_get(
    url: str, *, stage: str, endpoint_family: str, identity: dict[str, Any],
    timeout: float, session: Any = None, params: dict[str, Any] | None = None,
    max_attempts: int = 1, retry_statuses: Iterable[int] = (), backoff_seconds: float = 0.0,
    request_class: str = "PRIMARY", authority_boundary: bool = False,
    preserve_response: bool = False, reuse_preserved: bool = False,
    redirect_policy: dict[str, Any] | None = None,
) -> Any:
    """GET an official JSON response with exact governed attempt accounting."""
    context = RequestContext.from_env()
    if context is None:
        return _unguarded_get(session, url, timeout=timeout, **({"params": params} if params else {}))
    redirect_team = None
    redirect_season = None
    try:
        context.validate_identity(identity)
        if redirect_policy is not None:
            if endpoint_family != "ROSTER" or params:
                raise RuntimeError("OFFICIAL_REDIRECT_POLICY_SCOPE_INVALID")
            redirect_team, redirect_season = _validate_roster_redirect_source(url, redirect_policy)
    except RuntimeError as error:
        context.append({
            "event_kind": "REQUEST_REJECTED", "timestamp_utc": utc_now(), "pid": os.getpid(),
            "caller_stage": stage, "endpoint_family": endpoint_family,
            "resource_identity": identity, "request_class": request_class,
            "authority_boundary": authority_boundary, "final_disposition": str(error),
        })
        raise
    logical_id = uuid.uuid4().hex
    if reuse_preserved:
        started = time.monotonic()
        body, digest, provenance = context.reuse(endpoint_family, identity)
        context.append({
            "event_kind": "PRESERVED_RESPONSE_REUSE", "timestamp_utc": utc_now(),
            "request_start_utc": utc_now(), "request_end_utc": utc_now(), "pid": os.getpid(),
            "logical_request_id": logical_id, "caller_stage": stage,
            "endpoint_family": endpoint_family, "resource_identity": identity,
            "attempt_number": 0, "request_class": request_class,
            "authority_boundary": authority_boundary, "http_status": 200,
            "response_bytes": len(body), "response_sha256": digest,
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "cache_hit": True, "response_preserved": True,
            "final_disposition": "PRESERVED_RESPONSE_REUSE",
            **provenance,
        })
        return PreservedResponse(body)

    retry_statuses = {int(value) for value in retry_statuses}
    current_url = url
    visited = {url}
    redirect_hop = 0
    attempt = 0
    attempt_in_target = 1
    retry_number = 0
    attempt_reason = "INITIAL"
    while True:
        attempt += 1
        start_utc, started = utc_now(), time.monotonic()
        status = None
        body = b""
        digest = None
        error_name = None
        response = None
        redirect_target = None
        redirect_location = None
        redirect_error = None
        follow_redirect = False
        try:
            # A fresh default Session has no urllib3 retry policy.  This ensures
            # each transport attempt is visible to this loop and the journal.
            transport = requests.Session()
            try:
                if session is not None and getattr(session, "headers", None):
                    transport.headers.update(dict(session.headers))
                response = transport.get(
                    current_url, timeout=timeout, allow_redirects=False,
                    **({"params": params} if params else {}),
                )
            finally:
                transport.close()
            status = int(response.status_code)
            body = bytes(response.content)
            digest = sha256_bytes(body)
            if 300 <= status < 400 and redirect_policy is not None:
                try:
                    if status not in {307, 308}:
                        raise RuntimeError("OFFICIAL_REDIRECT_STATUS_NOT_ALLOWLISTED")
                    location = response.headers.get("Location")
                    if redirect_hop >= 1:
                        candidate = urljoin(current_url, location) if location else ""
                        if candidate in visited:
                            raise RuntimeError("OFFICIAL_REDIRECT_LOOP")
                        raise RuntimeError("OFFICIAL_REDIRECT_MAX_HOPS_EXCEEDED")
                    normalized, redirect_target = _resolve_roster_redirect(
                        location, source_url=current_url, team=str(redirect_team),
                        repository_season=int(redirect_season), visited=visited,
                    )
                    redirect_location = normalized
                    follow_redirect = True
                    success = False
                    retryable = False
                    disposition = "ALLOWED_REDIRECT"
                except RuntimeError as error:
                    redirect_error = str(error)
                    success = False
                    retryable = False
                    disposition = "REDIRECT_REJECTED"
            else:
                success = 200 <= status < 300
                retryable = status in retry_statuses and attempt_in_target < max_attempts
                disposition = "SUCCESS" if success else ("RETRYABLE_HTTP_ERROR" if retryable else "HTTP_ERROR")
        except Exception as error:  # transport failure, recorded before retry/raise
            error_name = f"{type(error).__name__}:{error}"
            success = False
            retryable = attempt_in_target < max_attempts
            disposition = "TRANSPORT_ERROR_RETRY" if retryable else "TRANSPORT_ERROR_EXHAUSTED"
        record = {
            "event_kind": "NETWORK_ATTEMPT", "timestamp_utc": utc_now(),
            "request_start_utc": start_utc, "request_end_utc": utc_now(), "pid": os.getpid(),
            "logical_request_id": logical_id, "caller_stage": stage,
            "endpoint_family": endpoint_family, "resource_identity": identity,
            "attempt_number": attempt, "request_class": request_class,
            "attempt_reason": attempt_reason, "retry_number": retry_number,
            "redirect_hop": redirect_hop,
            "authority_boundary": authority_boundary, "http_status": status,
            "transport_error": error_name, "response_bytes": len(body) if body else 0,
            "response_sha256": digest, "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "cache_hit": False, "response_preserved": bool(preserve_response and success),
            "final_disposition": disposition,
        }
        if redirect_policy is not None:
            record["request_target"] = _sanitized_target(current_url)
        if redirect_target is not None and redirect_location is not None:
            record.update({
                "redirect_location": redirect_location,
                "redirect_target": redirect_target,
                "redirect_target_sha256": sha256_bytes(redirect_location.encode()),
            })
        if redirect_error is not None:
            record["redirect_rejection"] = redirect_error
        context.append(record)
        if redirect_error is not None:
            raise RuntimeError(redirect_error)
        if follow_redirect:
            current_url = str(redirect_location)
            visited.add(current_url)
            redirect_hop = 1
            attempt_in_target = 1
            attempt_reason = "REDIRECT_FOLLOW"
            continue
        if success:
            if preserve_response:
                context.preserve(endpoint_family, identity, body)
            return PreservedResponse(body, status)
        if not retryable:
            if response is not None:
                return PreservedResponse(body, int(status))
            raise RuntimeError(error_name or f"OFFICIAL_HTTP_STATUS_{status}")
        retry_number += 1
        attempt_in_target += 1
        attempt_reason = "RETRY"
        if backoff_seconds:
            time.sleep(backoff_seconds * (2 ** (retry_number - 1)))


def read_journal(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def verify_preserved_response_run(request_root: Path, *, expected_run_id: str,
                                  slate_date: str, game_ids: Iterable[int],
                                  repository_root: Path | None = None,
                                  expected_journal_sha256: str | None = None,
                                  expected_tree_fingerprint: str | None = None) -> dict[str, Any]:
    """Verify a failed authority run as an immutable, exact eight-response source."""
    if request_root.name != expected_run_id or not SAFE_TOKEN.fullmatch(expected_run_id):
        raise RuntimeError("PRESERVED_SOURCE_RUN_ID_MISMATCH")
    journal = request_root / "official_request_journal.jsonl"
    cache = request_root / "preserved_responses"
    if not journal.is_file() or not cache.is_dir():
        raise RuntimeError("PRESERVED_SOURCE_RUN_INCOMPLETE")
    journal_digest = sha256_bytes(journal.read_bytes())
    if expected_journal_sha256 is not None and journal_digest != expected_journal_sha256:
        raise RuntimeError("PRESERVED_SOURCE_JOURNAL_CHANGED")
    tree_fingerprint = (request_run_tree_fingerprint(request_root, repository_root=repository_root)
                        if repository_root is not None else None)
    if expected_tree_fingerprint is not None and tree_fingerprint != expected_tree_fingerprint:
        raise RuntimeError("PRESERVED_SOURCE_TREE_FINGERPRINT_MISMATCH")
    ids = sorted({int(value) for value in game_ids})
    expected_hash = canonical_game_set_hash(ids)
    records = read_journal(journal)
    if len(records) != 1 + len(ids):
        raise RuntimeError(f"PRESERVED_SOURCE_JOURNAL_CARDINALITY:{len(records)}")
    if any(row.get("run_id") != expected_run_id for row in records):
        raise RuntimeError("PRESERVED_SOURCE_JOURNAL_RUN_ID_MISMATCH")
    if any(row.get("canonical_game_set_hash") != expected_hash for row in records):
        raise RuntimeError("PRESERVED_SOURCE_GAME_SET_HASH_MISMATCH")
    expected = [("SCHEDULE", {"slate_date": slate_date})] + [
        ("BOXSCORE", {"slate_date": slate_date, "game_id": gid}) for gid in ids
    ]
    actual = [(row.get("endpoint_family"), row.get("resource_identity")) for row in records]
    if (sorted(actual, key=lambda value: json.dumps(value, sort_keys=True)) !=
            sorted(expected, key=lambda value: json.dumps(value, sort_keys=True))):
        raise RuntimeError("PRESERVED_SOURCE_RESPONSE_IDENTITY_SET_MISMATCH")
    if any(row.get("event_kind") != "NETWORK_ATTEMPT" or
           row.get("final_disposition") != "SUCCESS" or
           not row.get("authority_boundary") or not row.get("response_preserved")
           for row in records):
        raise RuntimeError("PRESERVED_SOURCE_AUTHORITY_RECORD_INVALID")
    response_bindings: list[dict[str, Any]] = []
    expected_indexes: set[str] = set()
    expected_objects: set[str] = set()
    for family, identity in expected:
        token = sha256_bytes(json.dumps(
            {"endpoint_family": family, "identity": identity},
            sort_keys=True, separators=(",", ":"),
        ).encode())
        index = cache / "index" / f"{token}.json"
        if not index.is_file():
            raise RuntimeError("PRESERVED_SOURCE_INDEX_MISSING")
        metadata = json.loads(index.read_text())
        if metadata.get("endpoint_family") != family or metadata.get("identity") != identity:
            raise RuntimeError("PRESERVED_SOURCE_INDEX_IDENTITY_MISMATCH")
        object_path = cache / "objects" / str(metadata.get("object_name", ""))
        if not object_path.is_file():
            raise RuntimeError("PRESERVED_SOURCE_OBJECT_MISSING")
        body = object_path.read_bytes()
        digest = sha256_bytes(body)
        if digest != metadata.get("response_sha256") or len(body) != metadata.get("response_bytes"):
            raise RuntimeError("PRESERVED_SOURCE_RESPONSE_HASH_MISMATCH")
        verify_payload_identity(family, identity, body)
        if family == "SCHEDULE":
            payload = json.loads(body)
            schedule_ids: set[int] = set()
            for day in payload.get("gameWeek", []) or []:
                if str(day.get("date") or "") == slate_date:
                    schedule_ids.update(int(game.get("id") or game.get("gamePk") or game.get("gameId"))
                                        for game in day.get("games", []) or [])
            schedule_ids.update(int(game.get("id") or game.get("gamePk") or game.get("gameId"))
                                for game in payload.get("games", []) or []
                                if str(game.get("gameDate") or "") == slate_date)
            if schedule_ids != set(ids):
                raise RuntimeError("PRESERVED_SOURCE_SCHEDULE_GAME_SET_MISMATCH")
        journal_row = next(row for row in records
                           if row.get("endpoint_family") == family and row.get("resource_identity") == identity)
        if journal_row.get("response_sha256") != digest or journal_row.get("response_bytes") != len(body):
            raise RuntimeError("PRESERVED_SOURCE_JOURNAL_RESPONSE_MISMATCH")
        expected_indexes.add(index.name); expected_objects.add(object_path.name)
        response_bindings.append({
            "endpoint_family": family, "resource_identity": identity,
            "index_sha256": sha256_bytes(index.read_bytes()),
            "object_sha256": digest, "response_bytes": len(body),
        })
    actual_indexes = {path.name for path in (cache / "index").glob("*.json")}
    actual_objects = {path.name for path in (cache / "objects").glob("*.json")}
    if actual_indexes != expected_indexes or actual_objects != expected_objects:
        raise RuntimeError("PRESERVED_SOURCE_CACHE_OBJECT_SET_MISMATCH")
    return {
        "contract_version": "NHL_CROSS_RUN_PRESERVED_RESPONSE_REUSE_V1",
        "role": "AUTHORITY_RESPONSE_SOURCE",
        "source_run_id": expected_run_id, "source_journal_sha256": journal_digest,
        "tree_fingerprint": tree_fingerprint,
        "canonical_game_set_hash": expected_hash, "authority_responses": len(response_bindings),
        "source_cache": str(cache), "responses": response_bindings,
        "response_set_sha256": sha256_bytes(json.dumps(
            response_bindings, sort_keys=True, separators=(",", ":")).encode()),
    }


def verify_failed_execution_ancestor(
    request_root: Path, *, expected_run_id: str, slate_date: str,
    game_ids: Iterable[int], response_source: dict[str, Any],
    repository_root: Path, expected_journal_sha256: str,
    expected_tree_fingerprint: str,
) -> dict[str, Any]:
    if request_root.name != expected_run_id or not SAFE_TOKEN.fullmatch(expected_run_id):
        raise RuntimeError("FAILED_ANCESTOR_RUN_ID_MISMATCH")
    journal = request_root / "official_request_journal.jsonl"
    if not journal.is_file():
        raise RuntimeError("FAILED_ANCESTOR_JOURNAL_MISSING")
    journal_digest = sha256_bytes(journal.read_bytes())
    if journal_digest != expected_journal_sha256:
        raise RuntimeError("FAILED_ANCESTOR_JOURNAL_CHANGED")
    tree_fingerprint = request_run_tree_fingerprint(request_root, repository_root=repository_root)
    if tree_fingerprint != expected_tree_fingerprint:
        raise RuntimeError("FAILED_ANCESTOR_TREE_FINGERPRINT_MISMATCH")
    ids = sorted({int(value) for value in game_ids})
    game_hash = canonical_game_set_hash(ids)
    records = read_journal(journal)
    if not records or any(row.get("run_id") != expected_run_id for row in records):
        raise RuntimeError("FAILED_ANCESTOR_JOURNAL_RUN_ID_MISMATCH")
    if any(row.get("canonical_game_set_hash") != game_hash for row in records):
        raise RuntimeError("FAILED_ANCESTOR_GAME_SET_HASH_MISMATCH")
    source_run_id = str(response_source["source_run_id"])
    if source_run_id == expected_run_id:
        raise RuntimeError("REQUEST_LINEAGE_CYCLE")
    source_journal_hash = str(response_source["source_journal_sha256"])
    source_responses = {
        (row["index_sha256"], row["object_sha256"], int(row["response_bytes"]))
        for row in response_source["responses"]
    }
    reuses = [row for row in records if row.get("event_kind") == "PRESERVED_RESPONSE_REUSE"]
    if not reuses:
        raise RuntimeError("FAILED_ANCESTOR_REUSE_CHAIN_MISSING")
    for row in reuses:
        if (row.get("source_run_id") != source_run_id
                or row.get("source_journal_sha256") != source_journal_hash
                or not row.get("cross_run_reuse")):
            raise RuntimeError("FAILED_ANCESTOR_REUSE_SOURCE_MISMATCH")
        key = (row.get("source_response_index_sha256"),
               row.get("source_response_object_sha256"), int(row.get("response_bytes") or 0))
        if key not in source_responses or row.get("response_sha256") != key[1]:
            raise RuntimeError("FAILED_ANCESTOR_REUSE_OBJECT_MISMATCH")
    failures = [row for row in records if row.get("event_kind") == "NETWORK_ATTEMPT"
                and row.get("final_disposition") != "SUCCESS"]
    if not failures:
        raise RuntimeError("FAILED_ANCESTOR_FAILURE_EVIDENCE_MISSING")
    if any(row.get("response_preserved") for row in failures):
        raise RuntimeError("FAILED_ANCESTOR_FAILURE_MARKED_REUSABLE")
    response_cache = request_root / "preserved_responses"
    cache_files = list(response_cache.rglob("*")) if response_cache.exists() else []
    if any(path.is_file() for path in cache_files):
        raise RuntimeError("FAILED_ANCESTOR_UNEXPECTED_RESPONSE_OBJECT")
    roster_redirects = [row for row in failures if row.get("endpoint_family") == "ROSTER"
                        and int(row.get("http_status") or 0) in {307, 308}]
    if not roster_redirects:
        raise RuntimeError("FAILED_ANCESTOR_ROSTER_REDIRECT_MISSING")
    return {
        "contract_version": "NHL_FAILED_EXECUTION_ANCESTOR_V1",
        "role": "FAILED_EXECUTION_ANCESTOR", "run_id": expected_run_id,
        "journal_sha256": journal_digest, "tree_fingerprint": tree_fingerprint,
        "canonical_game_set_hash": game_hash, "journal_records": len(records),
        "reuse_records": len(reuses), "failed_network_attempts": len(failures),
        "relationship": {
            "authority_response_source_run_id": source_run_id,
            "authority_response_source_journal_sha256": source_journal_hash,
            "authority_response_set_sha256": response_source["response_set_sha256"],
            "reuse_chain_verified": True,
        },
        "non_reusable_roster_redirects": [{
            "http_status": int(row["http_status"]),
            "resource_identity": row["resource_identity"],
            "response_sha256": row.get("response_sha256"),
            "response_bytes": int(row.get("response_bytes") or 0),
        } for row in roster_redirects],
    }


def summarize_journal(path: Path, *, run_id: str, expected_game_hash: str) -> dict[str, Any]:
    records = read_journal(path)
    if not records:
        raise RuntimeError("OFFICIAL_REQUEST_JOURNAL_EMPTY")
    if any(row.get("run_id") != run_id for row in records):
        raise RuntimeError("OFFICIAL_REQUEST_JOURNAL_RUN_ID_MISMATCH")
    if any(row.get("canonical_game_set_hash") != expected_game_hash for row in records):
        raise RuntimeError("OFFICIAL_REQUEST_JOURNAL_GAME_HASH_MISMATCH")
    attempts = [row for row in records if row.get("event_kind") == "NETWORK_ATTEMPT"]
    reuses = [row for row in records if row.get("event_kind") == "PRESERVED_RESPONSE_REUSE"]
    rejected = [row for row in records if row.get("event_kind") == "REQUEST_REJECTED"]
    if len(attempts) + len(reuses) + len(rejected) != len(records):
        raise RuntimeError("OFFICIAL_REQUEST_JOURNAL_UNKNOWN_EVENT")
    required = {"timestamp_utc", "pid", "caller_stage", "endpoint_family",
                "resource_identity", "final_disposition"}
    if any(required - set(row) for row in records):
        raise RuntimeError("OFFICIAL_REQUEST_JOURNAL_RECORD_INCOMPLETE")
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in attempts + reuses:
        logical_id = str(row.get("logical_request_id") or "")
        if not logical_id:
            raise RuntimeError("OFFICIAL_REQUEST_LOGICAL_ID_MISSING")
        groups.setdefault(logical_id, []).append(row)
    for rows in groups.values():
        kinds = {row["event_kind"] for row in rows}
        if len(kinds) != 1:
            raise RuntimeError("OFFICIAL_REQUEST_LOGICAL_ID_KIND_CONFLICT")
        if "PRESERVED_RESPONSE_REUSE" in kinds and len(rows) != 1:
            raise RuntimeError("OFFICIAL_REQUEST_DUPLICATE_REUSE_RECORD")
        if "NETWORK_ATTEMPT" in kinds:
            numbers = sorted(int(row.get("attempt_number") or 0) for row in rows)
            if numbers != list(range(1, len(numbers) + 1)):
                raise RuntimeError("OFFICIAL_REQUEST_ATTEMPT_SEQUENCE_INVALID")
            if sum(row.get("final_disposition") == "SUCCESS" for row in rows) > 1:
                raise RuntimeError("OFFICIAL_REQUEST_MULTIPLE_SUCCESS_RESPONSES")
    logical = {row.get("logical_request_id") for row in attempts + reuses if row.get("logical_request_id")}
    authority = {row.get("logical_request_id") for row in attempts + reuses
                 if row.get("authority_boundary") and row.get("logical_request_id")}
    successes = [row for row in attempts if row.get("final_disposition") == "SUCCESS"]
    redirects = [row for row in attempts if row.get("final_disposition") == "ALLOWED_REDIRECT"]
    failed = [row for row in attempts
              if row.get("final_disposition") not in {"SUCCESS", "ALLOWED_REDIRECT"}]
    families: dict[str, dict[str, int]] = {}
    identities: dict[str, dict[str, int]] = {}
    for row in attempts + reuses:
        family = str(row.get("endpoint_family"))
        bucket = families.setdefault(
            family, {"logical_requests": 0, "network_attempts": 0,
                     "reuses": 0, "redirects": 0})
        if row.get("event_kind") == "NETWORK_ATTEMPT":
            bucket["network_attempts"] += 1
            bucket["redirects"] += int(row.get("final_disposition") == "ALLOWED_REDIRECT")
        else:
            bucket["reuses"] += 1
        identity_key = json.dumps(row.get("resource_identity") or {}, sort_keys=True, separators=(",", ":"))
        identity_bucket = identities.setdefault(
            identity_key, {"logical_requests": 0, "network_attempts": 0,
                           "reuses": 0, "redirects": 0})
        if row.get("event_kind") == "NETWORK_ATTEMPT":
            identity_bucket["network_attempts"] += 1
            identity_bucket["redirects"] += int(
                row.get("final_disposition") == "ALLOWED_REDIRECT")
        else:
            identity_bucket["reuses"] += 1
    for family, bucket in families.items():
        bucket["logical_requests"] = len({row.get("logical_request_id") for row in attempts + reuses
                                           if row.get("endpoint_family") == family})
    for identity_key, bucket in identities.items():
        bucket["logical_requests"] = len({row.get("logical_request_id") for row in attempts + reuses
                                           if json.dumps(row.get("resource_identity") or {}, sort_keys=True,
                                                         separators=(",", ":")) == identity_key})
    return {
        "contract_version": CONTRACT, "run_id": run_id,
        "authority_boundary_logical_requests": len(authority),
        "total_logical_requests": len(logical), "total_network_attempts": len(attempts),
        "successful_responses": len(successes), "failed_attempts": len(failed),
        "allowed_redirects": len(redirects),
        "retries": sum(1 for row in attempts
                       if (row.get("attempt_reason") == "RETRY"
                           or ("attempt_reason" not in row
                               and int(row.get("attempt_number") or 0) > 1))),
        "fallback_attempts": sum(1 for row in attempts if row.get("request_class") == "FALLBACK"),
        "cache_reuse_events": len(reuses), "unexpected_requests": len(rejected),
        "counts_by_endpoint_family": dict(sorted(families.items())),
        "counts_by_resource_identity": dict(sorted(identities.items())),
        "bookmaker_requests": 0, "odds_api_requests": 0, "paid_credits": 0,
        "journal_records": len(records),
    }
