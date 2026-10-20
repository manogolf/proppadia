"""Signed, append-only phase-authority entries in the database (DB chain V1).

The committed snapshot stays the root of trust. The local daily run appends
one entry per new postseason gamePk that its fully verified runtime overlay
accepted. Each entry links to the previous one by SHA-256 hash, starting from
the committed anchor descriptor. Each entry is HMAC-signed with a key the
database never holds. The hosted API re-verifies every entry on each read and
adds an entry's game only when the whole chain verifies.

Contract: docs/contracts/mlb_phase_authority_snapshot_only_read_v1.md.
"""
from __future__ import annotations

import functools
import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from backend.mlb.season_transition.contract_v1 import GAME_TYPE_CONTRACT
from backend.mlb.season_transition.game_phase_authority_v1 import (
    SOURCE_VERIFICATION_RAW_BYTES,
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityRecord,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    REPO_ROOT,
    canonical_json_bytes,
    sha256_file,
)

CONFIG_PATH = Path(__file__).with_name("phase_authority_db_chain.json")
CONFIG_CONTRACT = "MLB_PHASE_AUTHORITY_DB_CHAIN_V1"
ENTRY_CONTRACT = "MLB_PHASE_AUTHORITY_DB_CHAIN_ENTRY_V1"
KEY_ENV = "MLB_PHASE_AUTHORITY_CHAIN_KEY"
KEY_ID_DOMAIN = b"MLB_PHASE_AUTHORITY_DB_CHAIN_KEY_ID_V1\n"
SOURCE_VERIFICATION_DB_CHAIN = "COMMITTED_SNAPSHOT_AND_SIGNED_DB_CHAIN"
CLASSIFICATION_FIELDS = (
    "source_season", "source_game_type", "season_phase", "postseason_round", "source_round",
)
_SHA256 = re.compile(r"[0-9a-f]{64}")

FETCH_SQL = """
    SELECT chain_id, sequence, game_pk, prev_entry_sha256, entry_sha256, entry_text,
           signature_hmac_sha256, signing_key_id
      FROM mlb.phase_authority_chain_entries
     WHERE chain_id = %s
     ORDER BY sequence
"""
APPEND_SQL = """
    SELECT mlb.append_phase_authority_chain_entry(
        %s, %s, %s, %s, %s, %s, %s, %s
    ) AS result_count
"""


class PhaseChainError(GamePhaseAuthorityError):
    """The DB chain or its configuration failed verification."""


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    config = json.loads(path.read_text(encoding="utf-8"))
    if (config.get("contract_name") != CONFIG_CONTRACT or config.get("schema_version") != 1
            or not _SHA256.fullmatch(str(config.get("anchor_descriptor_sha256") or ""))
            or not str(config.get("chain_id") or "")
            or not isinstance(config.get("supported_season"), int)
            or not config.get("admitted_phases")):
        raise PhaseChainError("DB_CHAIN_CONFIG_INVALID")
    key_id = config.get("signing_key_id")
    if key_id is not None and not _SHA256.fullmatch(str(key_id)):
        raise PhaseChainError("DB_CHAIN_CONFIG_INVALID")
    return config


def signing_key_id(key: bytes) -> str:
    return hashlib.sha256(KEY_ID_DOMAIN + key).hexdigest()


def load_key(config: Mapping[str, Any], environ: Mapping[str, str] = os.environ) -> bytes | None:
    """Return the signing key, or None when it is absent. Never logs the key."""
    text = str(environ.get(KEY_ENV) or "").strip()
    if not text:
        return None
    if not re.fullmatch(r"[0-9a-f]{64}", text):
        raise PhaseChainError("DB_CHAIN_KEY_MALFORMED")
    key = bytes.fromhex(text)
    if not config.get("signing_key_id") or signing_key_id(key) != config["signing_key_id"]:
        raise PhaseChainError("DB_CHAIN_KEY_ID_MISMATCH")
    return key


def _signature(key: bytes, raw: bytes) -> str:
    return hmac.new(key, raw, hashlib.sha256).hexdigest()


def _descriptor_lineage(path_text: str, sha256: str, *, root: Path) -> set[str]:
    """Hashes of a committed descriptor and all of its verified ancestors."""
    lineage: set[str] = set()
    path, expected = (root / path_text).resolve(), sha256
    while expected and path.is_file() and sha256_file(path) == expected:
        lineage.add(expected)
        data = json.loads(path.read_text(encoding="utf-8"))
        parent = data.get("parent_descriptor_path")
        if not parent:
            break
        expected = str(data.get("parent_descriptor_sha256") or "")
        path = (path.parent / str(parent)).resolve()
    return lineage


def record_payload(record: GamePhaseAuthorityRecord) -> dict[str, Any]:
    return {
        "game_pk": int(record.game_pk),
        "source_season": int(record.source_season),
        "source_game_type": record.source_game_type,
        "season_phase": record.season_phase,
        "postseason_round": record.postseason_round,
        "season_name": record.season_name,
        "source_round": record.source_round,
        "schedule_relationships": dict(record.schedule_relationships),
        "scheduled_start_utc": record.scheduled_start_utc,
        "phase_decision": record.phase_decision,
        "source_path": record.primary_source_path,
        "source_sha256": record.primary_source_sha256,
    }


def validate_record(record: Mapping[str, Any], config: Mapping[str, Any]) -> None:
    """Re-derive the classification from the phase contract; never trust stored labels."""
    try:
        pk = int(record["game_pk"])
    except (KeyError, TypeError, ValueError):
        raise PhaseChainError("DB_CHAIN_RECORD_GAME_PK_INVALID") from None
    season = config["supported_season"]
    spec = GAME_TYPE_CONTRACT.get(str(record.get("source_game_type") or ""))
    if (pk <= 0 or record.get("source_season") != season or spec is None
            or spec["phase"] not in config["admitted_phases"]
            or record.get("season_phase") != spec["phase"]
            or record.get("postseason_round") != spec["postseason_round"]
            or record.get("season_name") != f"MLB_{season}_{spec['phase']}"
            or not str(record.get("source_round") or "").strip()
            or not str(record.get("phase_decision") or "").strip()
            or not isinstance(record.get("schedule_relationships"), dict)
            or not _SHA256.fullmatch(str(record.get("source_sha256") or ""))
            or not str(record.get("source_path") or "")):
        raise PhaseChainError("DB_CHAIN_RECORD_CLASSIFICATION_INVALID", game_pk=pk)
    try:
        start = datetime.fromisoformat(str(record.get("scheduled_start_utc")).replace("Z", "+00:00"))
    except ValueError:
        raise PhaseChainError("DB_CHAIN_RECORD_START_INVALID", game_pk=pk) from None
    if start.tzinfo is None or start.year != season:
        raise PhaseChainError("DB_CHAIN_RECORD_START_INVALID", game_pk=pk)


def _authority_record(record: Mapping[str, Any]) -> GamePhaseAuthorityRecord:
    return GamePhaseAuthorityRecord(
        game_pk=int(record["game_pk"]),
        source_season=int(record["source_season"]),
        source_game_type=str(record["source_game_type"]),
        season_phase=str(record["season_phase"]),
        postseason_round=record["postseason_round"],
        season_name=str(record["season_name"]),
        source_round=str(record["source_round"]),
        schedule_relationships=dict(record["schedule_relationships"]),
        primary_source_path=str(record["source_path"]),
        primary_source_sha256=str(record["source_sha256"]),
        source_paths=(str(record["source_path"]),),
        source_hashes=(str(record["source_sha256"]),),
        phase_decision=str(record["phase_decision"]),
        authority_status="AUTHORITATIVE_UNAMBIGUOUS",
        scheduled_start_utc=str(record["scheduled_start_utc"]),
    )


def _same_classification(existing: GamePhaseAuthorityRecord, record: Mapping[str, Any]) -> bool:
    return all(getattr(existing, field) == record.get(field) for field in CLASSIFICATION_FIELDS)


@dataclass(frozen=True)
class VerifiedChain:
    entry_count: int
    head_sha256: str
    game_pks: frozenset[int]
    records: Mapping[int, GamePhaseAuthorityRecord]


def verify_chain(
    rows: Iterable[Mapping[str, Any]],
    *,
    config: Mapping[str, Any],
    key: bytes,
    base: CanonicalGamePhaseAuthority,
    root: Path = REPO_ROOT,
) -> VerifiedChain:
    """Verify every entry from the committed anchor; any defect fails the whole chain."""
    anchor = str(config["anchor_descriptor_sha256"])
    metadata = base.metadata
    if anchor not in _descriptor_lineage(
            metadata.snapshot_descriptor_path, metadata.snapshot_descriptor_sha256, root=root):
        raise PhaseChainError("DB_CHAIN_ANCHOR_NOT_IN_ACTIVE_LINEAGE")
    key_id = signing_key_id(key)
    previous = anchor
    seen: set[int] = set()
    added: dict[int, GamePhaseAuthorityRecord] = {}
    count = 0
    for count, row in enumerate(rows, 1):
        text = str(row.get("entry_text") or "")
        raw = text.encode("utf-8")
        try:
            entry = json.loads(text)
        except json.JSONDecodeError:
            raise PhaseChainError("DB_CHAIN_ENTRY_MALFORMED", detail=str(count)) from None
        if (not isinstance(entry, dict) or canonical_json_bytes(entry) != raw
                or hashlib.sha256(raw).hexdigest() != row.get("entry_sha256")):
            raise PhaseChainError("DB_CHAIN_ENTRY_HASH_MISMATCH", detail=str(count))
        record = entry.get("record") or {}
        if (row.get("chain_id") != config["chain_id"] or int(row.get("sequence") or 0) != count
                or entry.get("contract_name") != ENTRY_CONTRACT or entry.get("schema_version") != 1
                or entry.get("chain_id") != config["chain_id"] or entry.get("sequence") != count
                or entry.get("anchor_descriptor_sha256") != anchor
                or entry.get("prev_entry_sha256") != previous
                or row.get("prev_entry_sha256") != previous
                or int(row.get("game_pk") or 0) != record.get("game_pk")):
            raise PhaseChainError("DB_CHAIN_LINK_INVALID", detail=str(count))
        if (row.get("signing_key_id") != key_id or entry.get("signing_key_id") != key_id
                or not hmac.compare_digest(_signature(key, raw), str(row.get("signature_hmac_sha256") or ""))):
            raise PhaseChainError("DB_CHAIN_SIGNATURE_INVALID", detail=str(count))
        validate_record(record, config)
        pk = int(record["game_pk"])
        if pk in seen:
            raise PhaseChainError("DB_CHAIN_DUPLICATE_GAME_PK", game_pk=pk)
        seen.add(pk)
        try:
            existing = base.lookup_exact(pk)
        except GamePhaseAuthorityError as exc:
            if exc.code != "GAME_PHASE_ABSENT":
                raise PhaseChainError("DB_CHAIN_BASE_CONFLICT", game_pk=pk, detail=exc.code) from None
            existing = None
        if existing is not None:
            # A later committed child may already hold the game; it must agree.
            if not _same_classification(existing, record):
                raise PhaseChainError("DB_CHAIN_BASE_CONFLICT", game_pk=pk)
        else:
            added[pk] = _authority_record(record)
        previous = str(row["entry_sha256"])
    return VerifiedChain(count, previous, frozenset(seen), added)


class ChainExtendedAuthority(CanonicalGamePhaseAuthority):
    """Committed snapshot plus exact games from a fully verified DB chain."""

    def __init__(self, base: CanonicalGamePhaseAuthority, chain: VerifiedChain) -> None:
        self.base = base
        self.chain = chain
        self.source_verification = SOURCE_VERIFICATION_DB_CHAIN

    @property
    def metadata(self):
        return self.base.metadata

    def lookup_exact(self, game_pk: Any) -> GamePhaseAuthorityRecord:
        try:
            return self.base.lookup_exact(game_pk)
        except GamePhaseAuthorityError as exc:
            if exc.code != "GAME_PHASE_ABSENT":
                raise
            record = self.chain.records.get(int(game_pk))
            if record is None:
                raise
            return record

    def is_runtime_exact(self, game_pk: int) -> bool:
        # Exact sourced gamePks are fresh by their signed source hash, not by
        # the committed snapshot's date horizon.
        return int(game_pk) in self.chain.records


def _default_connect():
    from backend.shared.db.pg import pg_connect

    return pg_connect()


def ingest_connect(environ: Mapping[str, str] = os.environ):
    """Command-scoped writer connection on the restricted MLB ingest login.

    Independent of the process-wide ``MLB_USE_RESTRICTED_INGEST`` route, so the
    rest of the daily run keeps its own connections. Never the owner login.
    """
    from backend.shared.db.pg import (
        MLB_INGEST_DATABASE_URL_ENV,
        configure_mlb_ingest_connection,
        verify_mlb_ingest_production_target,
    )

    url = str(environ.get(MLB_INGEST_DATABASE_URL_ENV) or "").strip()
    if not url:
        raise PhaseChainError("DB_CHAIN_WRITER_REQUIRES_RESTRICTED_INGEST")
    verify_mlb_ingest_production_target(url)
    import psycopg
    import psycopg.rows

    try:
        conn = psycopg.connect(url, row_factory=psycopg.rows.dict_row, prepare_threshold=None,
                               options="-c search_path=mlb,public")
    except Exception:
        raise PhaseChainError("DB_CHAIN_WRITER_CONNECTION_FAILED") from None
    try:
        configure_mlb_ingest_connection(conn)
    except Exception:
        conn.close()
        raise
    return conn


def fetch_chain_rows(conn, chain_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(FETCH_SQL, (chain_id,))
        rows = cur.fetchall() or []
    names = ("chain_id", "sequence", "game_pk", "prev_entry_sha256", "entry_sha256",
             "entry_text", "signature_hmac_sha256", "signing_key_id")
    return [dict(row) if isinstance(row, Mapping) else dict(zip(names, row)) for row in rows]


def extend_public_authority(
    base: CanonicalGamePhaseAuthority,
    *,
    connect: Callable[[], Any] = _default_connect,
    environ: Mapping[str, str] = os.environ,
    config_path: Path = CONFIG_PATH,
    root: Path = REPO_ROOT,
) -> tuple[CanonicalGamePhaseAuthority, dict[str, Any]]:
    """Return the authority the hosted read uses and a public extension status.

    Configuration gaps and an unreachable table leave the committed snapshot
    alone, which only withholds rows. A chain that fails verification raises
    ``PhaseChainError`` so the request publishes nothing.
    """
    config = load_config(config_path)
    if config.get("signing_key_id") is None:
        return base, {"status": "DB_CHAIN_NOT_CONFIGURED"}
    try:
        key = load_key(config, environ)
    except PhaseChainError as exc:
        return base, {"status": exc.code}
    if key is None:
        return base, {"status": "DB_CHAIN_KEY_UNAVAILABLE"}
    try:
        with connect() as conn:
            rows = fetch_chain_rows(conn, str(config["chain_id"]))
    except Exception as exc:  # noqa: BLE001 - transport and missing-table failures withhold only
        state = getattr(exc, "sqlstate", None)
        return base, {"status": "DB_CHAIN_UNAVAILABLE",
                      "error": type(exc).__name__ + (f" (SQLSTATE {state})" if state else "")}
    chain = verify_chain(rows, config=config, key=key, base=base, root=root)
    status = {"status": "DB_CHAIN_VERIFIED", "entry_count": chain.entry_count,
              "head_entry_sha256": chain.head_sha256, "extension_game_count": len(chain.records)}
    if not chain.records:
        return base, status
    return ChainExtendedAuthority(base, chain), status


def publish_runtime_records(
    runtime_authority: Any,
    game_pks: Iterable[int],
    *,
    selection_receipt_sha256: str,
    connect: Callable[[], Any] | None = None,
    environ: Mapping[str, str] = os.environ,
    config_path: Path = CONFIG_PATH,
    root: Path = REPO_ROOT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Append signed entries for exact games the fully verified overlay accepted."""
    config = load_config(config_path)
    if config.get("signing_key_id") is None:
        raise PhaseChainError("DB_CHAIN_NOT_CONFIGURED")
    key = load_key(config, environ)
    if key is None:
        raise PhaseChainError("DB_CHAIN_KEY_UNAVAILABLE")
    base = runtime_authority.base
    if getattr(base, "source_verification", None) != SOURCE_VERIFICATION_RAW_BYTES:
        raise PhaseChainError("DB_CHAIN_WRITER_REQUIRES_FULL_VERIFICATION")
    if not _SHA256.fullmatch(str(selection_receipt_sha256 or "")):
        raise PhaseChainError("DB_CHAIN_SELECTION_RECEIPT_HASH_INVALID")
    candidates = sorted({int(pk) for pk in game_pks if runtime_authority.is_runtime_exact(int(pk))})
    if not candidates:
        return {"status": "NOTHING_TO_PUBLISH", "appended_game_pks": []}
    if connect is None:
        connect = functools.partial(ingest_connect, environ)
    stamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    published_at = stamp.isoformat().replace("+00:00", "Z")
    key_id = signing_key_id(key)
    appended: list[int] = []
    with connect() as conn:
        rows = fetch_chain_rows(conn, str(config["chain_id"]))
        chain = verify_chain(rows, config=config, key=key, base=base, root=root)
        by_pk = {int(row["game_pk"]): json.loads(row["entry_text"])["record"] for row in rows}
        previous, sequence = (chain.head_sha256, chain.entry_count)
        with conn.cursor() as cur:
            for pk in candidates:
                record = record_payload(runtime_authority.lookup_exact(pk))
                validate_record(record, config)
                try:
                    committed = base.lookup_exact(pk)
                except GamePhaseAuthorityError as exc:
                    if exc.code != "GAME_PHASE_ABSENT":
                        raise PhaseChainError("DB_CHAIN_BASE_CONFLICT", game_pk=pk, detail=exc.code) from None
                    committed = None
                if committed is not None:
                    if not _same_classification(committed, record):
                        raise PhaseChainError("DB_CHAIN_BASE_CONFLICT", game_pk=pk)
                    continue
                if pk in by_pk:
                    if any(by_pk[pk].get(field) != record.get(field) for field in CLASSIFICATION_FIELDS):
                        raise PhaseChainError("DB_CHAIN_EXISTING_GAME_CONFLICT", game_pk=pk)
                    continue
                sequence += 1
                entry = {
                    "anchor_descriptor_sha256": config["anchor_descriptor_sha256"],
                    "chain_id": config["chain_id"],
                    "contract_name": ENTRY_CONTRACT,
                    "evidence": {
                        "local_base_descriptor_sha256": base.metadata.snapshot_descriptor_sha256,
                        "published_at_utc": published_at,
                        "selection_receipt_sha256": selection_receipt_sha256,
                    },
                    "prev_entry_sha256": previous,
                    "record": record,
                    "schema_version": 1,
                    "sequence": sequence,
                    "signing_key_id": key_id,
                }
                raw = canonical_json_bytes(entry)
                entry_sha = hashlib.sha256(raw).hexdigest()
                cur.execute(APPEND_SQL, (
                    config["chain_id"], sequence, pk, previous, entry_sha,
                    raw.decode("utf-8"), _signature(key, raw), key_id,
                ))
                result = cur.fetchone()
                count = result["result_count"] if isinstance(result, Mapping) else (result or [None])[0]
                if int(count or 0) != 1:
                    raise PhaseChainError("DB_CHAIN_APPEND_NOT_ACCEPTED", game_pk=pk)
                previous = entry_sha
                appended.append(pk)
        conn.commit()
    return {"status": "PUBLISHED" if appended else "ALREADY_PUBLISHED",
            "appended_game_pks": appended, "entry_count": sequence, "head_entry_sha256": previous}
