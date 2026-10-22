"""Read-only validation for prior-day reconciliation certificates."""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping

from backend.nhl.game_phase import season_for_date


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RECONCILIATION_ROOT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
DEFAULT_SLATE_ROOT = ROOT / "artifacts/operational/nhl/slates"
DEFAULT_ROSTER_ROOT = ROOT / "artifacts/operational/nhl/roster_observations"
# Official schedule states (gameScheduleState) that void a game: postponed,
# suspended or cancelled. Its predictions are never graded, like a book
# cancelling wagers; a rescheduled game gets new predictions on its new date.
VOID_SCHEDULE_STATES = frozenset({"PPD", "SUSP", "CNCL"})
# A game the official schedule no longer lists on its original date, whose own
# official box score shows another date, is void the same way (P2.24 D).
MOVED_GAME_STATE = "MOVED"
VOID_GAME_STATES = VOID_SCHEDULE_STATES | {MOVED_GAME_STATE}
VOIDED_GAMES_FILE = "schedule_voided_games.csv"
SCHEDULE_STATE_PROBE_CONTRACT = "NHL_SCHEDULE_STATE_PROBE_V1"
REQUIRED_PACKAGE_FILES = {
    "canonical_admitted_slate.csv",
    "canonical_game_outcomes.csv",
    "graded_moneyline.csv",
    "graded_puck_line.csv",
}


# A prior day is certified by a FINAL package, as a verified off day, or as a
# verified day on which every scheduled game was void.
PRIOR_DAY_CERTIFICATE_STATUSES = frozenset(
    {"CERTIFIED_FINAL", "VERIFIED_EMPTY_SCHEDULE", "VERIFIED_ALL_GAMES_VOID"})


PARTIAL_ROSTER_STATUS = "VERIFIED_PARTIAL_CURRENT_SLATE_ROSTER"
PARTIAL_ROSTER_NOT_SUPPORTED = "CURRENT_SLATE_ROSTER_PARTIAL_COVERAGE_NOT_SUPPORTED"


class RosterObservationNotReady(RuntimeError):
    """The current slate has no roster package yet; a later poll may retry."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_files(package: Path) -> set[str]:
    manifest = package / "SHA256SUMS"
    if not manifest.is_file():
        raise RuntimeError("RECONCILIATION_PACKAGE_MANIFEST_MISSING")
    entries: dict[str, str] = {}
    for line in manifest.read_text().splitlines():
        try:
            digest, name = line.split("  ", 1)
        except ValueError:
            raise RuntimeError("RECONCILIATION_PACKAGE_MANIFEST_INVALID") from None
        if Path(name).name != name or name in entries or len(digest) != 64:
            raise RuntimeError("RECONCILIATION_PACKAGE_MANIFEST_INVALID")
        entries[name] = digest
    for name, digest in entries.items():
        path = package / name
        if not path.is_file() or _sha256(path) != digest:
            raise RuntimeError("RECONCILIATION_PACKAGE_HASH_MISMATCH")
    if not REQUIRED_PACKAGE_FILES.issubset(entries):
        raise RuntimeError("RECONCILIATION_PACKAGE_REQUIRED_OUTPUT_MISSING")
    return set(entries)


def _csv_rows(path: Path, *, slate_date: str) -> list[dict[str, str]]:
    with path.open(newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise RuntimeError("RECONCILIATION_PACKAGE_OUTCOME_SCHEMA_INVALID")
        rows = list(reader)
    if not rows:
        raise RuntimeError("RECONCILIATION_PACKAGE_OUTCOME_SET_EMPTY")
    if any(row.get("slate_date") != slate_date for row in rows):
        raise RuntimeError("RECONCILIATION_PACKAGE_SLATE_MISMATCH")
    ids = [row.get("game_id", "") for row in rows]
    if any(not value.isdigit() for value in ids) or len(ids) != len(set(ids)):
        raise RuntimeError("RECONCILIATION_PACKAGE_GAME_SET_INVALID")
    return rows


def verify_reconciliation_certificate(
    package: Path, slate_date: str,
) -> dict[str, object]:
    """Verify package completion, all hashes, and exact FINAL game coverage."""
    package = Path(package).resolve()
    files = _manifest_files(package)
    try:
        marker = json.loads((package / "RUN_COMPLETE.json").read_text())
        summary = json.loads((package / "summary.json").read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("RECONCILIATION_PACKAGE_COMPLETION_EVIDENCE_INVALID") from error
    if marker.get("status") != "COMPLETE" or summary.get("status") != "COMPLETE":
        raise RuntimeError("RECONCILIATION_PACKAGE_NOT_COMPLETE")
    if summary.get("slate_date") != slate_date:
        raise RuntimeError("RECONCILIATION_PACKAGE_SLATE_MISMATCH")

    admitted = _csv_rows(package / "canonical_admitted_slate.csv", slate_date=slate_date)
    outcomes = _csv_rows(package / "canonical_game_outcomes.csv", slate_date=slate_date)
    admitted_ids = {row["game_id"] for row in admitted}
    outcome_ids = {row["game_id"] for row in outcomes}
    if admitted_ids != outcome_ids:
        raise RuntimeError("RECONCILIATION_PACKAGE_GAME_COVERAGE_MISMATCH")
    required_outcome_fields = {
        "official_final", "official_final_home_goals", "official_final_away_goals",
        "outcome_source", "outcome_source_timestamp_utc",
    }
    if not required_outcome_fields.issubset(outcomes[0]):
        raise RuntimeError("RECONCILIATION_PACKAGE_OUTCOME_SCHEMA_INVALID")
    for row in outcomes:
        if row.get("official_final", "").strip().lower() not in {"true", "1"}:
            raise RuntimeError("RECONCILIATION_PACKAGE_OUTCOMES_NOT_FINAL")
        if row.get("outcome_source") != "NHL_OFFICIAL_GAMECENTER":
            raise RuntimeError("RECONCILIATION_PACKAGE_OUTCOME_SOURCE_INVALID")
        if not row.get("outcome_source_timestamp_utc"):
            raise RuntimeError("RECONCILIATION_PACKAGE_OUTCOME_TIMESTAMP_MISSING")
        for score in ("official_final_home_goals", "official_final_away_goals"):
            if not row.get(score, "").isdigit():
                raise RuntimeError("RECONCILIATION_PACKAGE_OFFICIAL_SCORE_INVALID")
    if summary.get("games") is not None and int(summary["games"]) != len(outcomes):
        raise RuntimeError("RECONCILIATION_PACKAGE_GAME_COUNT_MISMATCH")
    certificate = {
        "status": "CERTIFIED_FINAL",
        "slate_date": slate_date,
        "package_path": str(package),
        "package_manifest_sha256": _sha256(package / "SHA256SUMS"),
        "game_ids": sorted(map(int, outcome_ids)),
        "game_count": len(outcomes),
        "files": sorted(files),
    }
    if VOIDED_GAMES_FILE in files:
        voided = _csv_rows(package / VOIDED_GAMES_FILE, slate_date=slate_date)
        if (any(row.get("game_schedule_state") not in VOID_GAME_STATES for row in voided)
                or {row["game_id"] for row in voided} & admitted_ids):
            raise RuntimeError("RECONCILIATION_PACKAGE_VOIDED_GAMES_INVALID")
        certificate["voided_game_ids"] = sorted(int(row["game_id"]) for row in voided)
    return certificate


def schedule_hash_matches(recorded: object, raw_path: Path) -> bool:
    """True when a slate's recorded raw_source_hash matches its retained file.

    import_schedule_today records the hash of the compact, key-sorted JSON it
    fetched but writes the file indented, so accept that canonical form as
    well as the exact bytes (the same rule as daily_capture.load_canonical_slate).
    """
    if recorded == _sha256(raw_path):
        return True
    canonical = json.dumps(json.loads(Path(raw_path).read_text()),
                           sort_keys=True, separators=(",", ":")) + "\n"
    return recorded == hashlib.sha256(canonical.encode()).hexdigest()


def verified_empty_schedule(
    slate_date: str, *, slate_root: Path = DEFAULT_SLATE_ROOT,
) -> dict[str, object]:
    """Return verified empty-slate evidence from the retained official schedule."""
    day = Path(slate_root) / slate_date
    raw_path = day / "raw_schedule_response.json"
    health_path = day / "slate_health.json"
    try:
        health = json.loads(health_path.read_text())
        payload = json.loads(raw_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("PRIOR_DATE_SCHEDULE_EVIDENCE_MISSING_OR_INVALID") from error
    if (health.get("slate_date") != slate_date
            or health.get("completion_status") != "VALID_EMPTY_SLATE"
            or health.get("downstream_ready") is not True
            or health.get("source") != f"https://api-web.nhle.com/v1/schedule/{slate_date}"
            or not schedule_hash_matches(health.get("raw_source_hash"), raw_path)):
        raise RuntimeError("PRIOR_DATE_EMPTY_SCHEDULE_CERTIFICATE_INVALID")
    weeks = payload.get("gameWeek")
    if not isinstance(weeks, list):
        raise RuntimeError("PRIOR_DATE_SCHEDULE_SCHEMA_INVALID")
    games = [game for day_record in weeks
             if day_record.get("date") == slate_date
             for game in (day_record.get("games") or [])]
    if games or int(health.get("normalized_game_count", -1)) != 0:
        raise RuntimeError("PRIOR_DATE_SCHEDULE_NOT_EMPTY")
    return {
        "status": "VERIFIED_EMPTY_SCHEDULE",
        "slate_date": slate_date,
        "schedule_sha256": _sha256(raw_path),
    }


def _preserved_object(cache: Path, family: str, identity: dict[str, object]) -> dict:
    """Read one hash-verified preserved official response from a request run."""
    token = hashlib.sha256(json.dumps(
        {"endpoint_family": family, "identity": identity},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    metadata = json.loads((cache / "index" / f"{token}.json").read_text())
    body = (cache / "objects" / str(metadata["object_name"])).read_bytes()
    if (metadata.get("endpoint_family") != family or metadata.get("identity") != identity
            or hashlib.sha256(body).hexdigest() != metadata.get("response_sha256")):
        raise RuntimeError("SCHEDULE_STATE_PROBE_RESPONSE_HASH_MISMATCH")
    return json.loads(body)


def verify_schedule_state_probe(
    reconciliation_root: Path, slate_date: str, run_id: str,
) -> dict[str, object]:
    """Re-verify a schedule-state probe's void games from its preserved responses.

    PPD/SUSP/CNCL games must carry that state on the preserved official
    schedule. A MOVED game must be absent from it, and its preserved official
    box score must give another game date.
    """
    root = Path(reconciliation_root)
    receipt_path = root / "acquisition_receipts" / slate_date / f"{run_id}.json"
    request_root = root / "request_runs" / slate_date / run_id
    try:
        receipt = json.loads(receipt_path.read_text())
        journal_sha = _sha256(request_root / "official_request_journal.jsonl")
        cache = request_root / "preserved_responses"
        schedule = _preserved_object(cache, "SCHEDULE", {"slate_date": slate_date})
    except (OSError, KeyError, ValueError) as error:
        raise RuntimeError("SCHEDULE_STATE_PROBE_EVIDENCE_MISSING_OR_INVALID") from error
    if (receipt.get("contract_version") != SCHEDULE_STATE_PROBE_CONTRACT
            or receipt.get("status") != "COMPLETE" or receipt.get("run_id") != run_id
            or receipt.get("slate_date") != slate_date
            or receipt.get("journal_sha256") != journal_sha):
        raise RuntimeError("SCHEDULE_STATE_PROBE_RECEIPT_INVALID")
    listed = {int(game["id"]): str(game.get("gameScheduleState") or "OK").upper()
              for day in schedule.get("gameWeek") or [] if str(day.get("date")) == slate_date
              for game in day.get("games") or []}
    voided = {int(k): str(v) for k, v in (receipt.get("voided_games") or {}).items()}
    game_ids = {int(value) for value in receipt.get("game_ids") or []}
    if not voided or set(voided) - game_ids:
        raise RuntimeError("SCHEDULE_STATE_PROBE_VOIDED_GAMES_INVALID")
    moved = {game_id for game_id, state in voided.items() if state == MOVED_GAME_STATE}
    if set(listed) | moved != game_ids or moved & set(listed):
        raise RuntimeError("SCHEDULE_STATE_PROBE_GAME_SET_MISMATCH")
    for game_id, state in voided.items():
        if state in VOID_SCHEDULE_STATES:
            if listed.get(game_id) != state:
                raise RuntimeError(f"SCHEDULE_STATE_PROBE_STATE_MISMATCH:{game_id}")
        elif state == MOVED_GAME_STATE:
            try:
                box = _preserved_object(
                    cache, "BOXSCORE", {"slate_date": slate_date, "game_id": game_id})
            except (OSError, KeyError, ValueError) as error:
                raise RuntimeError(f"SCHEDULE_STATE_PROBE_MOVED_EVIDENCE_MISSING:{game_id}") from error
            if int(box.get("id") or 0) != game_id or str(box.get("gameDate") or "") in {"", slate_date}:
                raise RuntimeError(f"SCHEDULE_STATE_PROBE_MOVED_GAME_NOT_MOVED:{game_id}")
        else:
            raise RuntimeError(f"SCHEDULE_STATE_PROBE_STATE_INVALID:{game_id}")
    if any(state != "OK" for game_id, state in listed.items() if game_id not in voided):
        raise RuntimeError("SCHEDULE_STATE_PROBE_UNRECORDED_VOID_STATE")
    return {"run_id": run_id, "receipt_sha256": _sha256(receipt_path),
            "receipt": receipt, "voided_games": voided, "game_ids": sorted(game_ids)}


def _all_void_probe_run_ids(reconciliation_root: Path, slate_date: str) -> list[str]:
    day = Path(reconciliation_root) / "acquisition_receipts" / slate_date
    run_ids = []
    for path in sorted(day.glob("*.json") if day.is_dir() else []):
        try:
            receipt = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if (receipt.get("contract_version") == SCHEDULE_STATE_PROBE_CONTRACT
                and receipt.get("all_scheduled_games_void") is True):
            run_ids.append(path.stem)
    return run_ids


def verified_all_games_void(
    slate_date: str, *, reconciliation_root: Path = DEFAULT_RECONCILIATION_ROOT,
) -> dict[str, object]:
    """Certify a day on which every scheduled game was void (P2.24 D).

    Like an off day there is no reconciliation package; the certificate is the
    latest schedule-state probe that found every scheduled game void, re-verified
    from its preserved official responses.
    """
    run_ids = _all_void_probe_run_ids(reconciliation_root, slate_date)
    if not run_ids:
        raise RuntimeError("PRIOR_DATE_ALL_GAMES_VOID_PROBE_MISSING")
    probe = verify_schedule_state_probe(reconciliation_root, slate_date, run_ids[-1])
    if set(probe["voided_games"]) != set(probe["game_ids"]):
        raise RuntimeError("PRIOR_DATE_NOT_ALL_GAMES_VOID")
    return {
        "status": "VERIFIED_ALL_GAMES_VOID",
        "slate_date": slate_date,
        "probe_run_id": probe["run_id"],
        "probe_receipt_sha256": probe["receipt_sha256"],
        "game_ids": [],
        "game_count": 0,
        "voided_game_ids": sorted(probe["voided_games"]),
        "voided_games": {str(k): v for k, v in sorted(probe["voided_games"].items())},
    }


def certificate_identity(certificate: object) -> str | None:
    """Return the stable hash that identifies a prior-day certificate.

    A reconciled day is identified by its package manifest. A verified empty
    (off) day has no package, so it is identified by the hash of its retained
    official schedule response. Every certificate comparison uses this.
    """
    if not isinstance(certificate, dict):
        return None
    if certificate.get("package_manifest_sha256"):
        return str(certificate["package_manifest_sha256"])
    if (certificate.get("status") == "VERIFIED_EMPTY_SCHEDULE"
            and certificate.get("schedule_sha256")):
        return str(certificate["schedule_sha256"])
    if (certificate.get("status") == "VERIFIED_ALL_GAMES_VOID"
            and certificate.get("probe_receipt_sha256")):
        return str(certificate["probe_receipt_sha256"])
    return None


def verify_prior_day_certificate(
    certificate: dict[str, object], prior_date: str, *,
    slate_root: Path = DEFAULT_SLATE_ROOT, fallback_package: object = None,
    reconciliation_root: Path = DEFAULT_RECONCILIATION_ROOT,
) -> dict[str, object]:
    """Re-verify a recorded prior-day certificate from its retained evidence.

    A ``VERIFIED_EMPTY_SCHEDULE`` certificate is checked against the retained
    empty official schedule; any other against its reconciliation package.
    The caller compares ``certificate_identity`` of the two.
    """
    if certificate.get("status") == "VERIFIED_EMPTY_SCHEDULE":
        return verified_empty_schedule(prior_date, slate_root=slate_root)
    if certificate.get("status") == "VERIFIED_ALL_GAMES_VOID":
        return verified_all_games_void(prior_date, reconciliation_root=reconciliation_root)
    package = certificate.get("package_path") or fallback_package or ""
    return verify_reconciliation_certificate(Path(str(package)), prior_date)


def require_prior_day_certificate(
    slate_date: str, *, reconciliation_root: Path = DEFAULT_RECONCILIATION_ROOT,
    slate_root: Path = DEFAULT_SLATE_ROOT,
) -> dict[str, object]:
    """Require a verified certificate, or verified empty prior-date schedule."""
    day = Path(reconciliation_root) / slate_date
    packages = sorted(day.glob("reconciliation=*") if day.is_dir() else [])
    if len(packages) > 1:
        raise RuntimeError("MULTIPLE_RECONCILIATION_PACKAGES_FOR_SLATE")
    if packages:
        return verify_reconciliation_certificate(packages[0], slate_date)
    incomplete = list(day.glob(".reconciliation.incomplete.*")) if day.is_dir() else []
    if incomplete:
        raise RuntimeError("PARTIAL_RECONCILIATION_PACKAGE_REQUIRES_REVIEW")
    if _all_void_probe_run_ids(reconciliation_root, slate_date):
        return verified_all_games_void(slate_date, reconciliation_root=reconciliation_root)
    if _schedule_has_games(Path(slate_root) / slate_date / "slate_health.json"):
        # A day with games is certified only by its package; until the morning
        # reconciliation creates it, say so instead of failing the empty-day check.
        raise RuntimeError(
            "PRIOR_DAY_RECONCILIATION_CERTIFICATE_REQUIRED:PRIOR_DAY_RECONCILIATION_NOT_YET_CREATED")
    try:
        return verified_empty_schedule(slate_date, slate_root=slate_root)
    except RuntimeError as error:
        raise RuntimeError(f"PRIOR_DAY_RECONCILIATION_CERTIFICATE_REQUIRED:{error}") from error


def _schedule_has_games(health_path: Path) -> bool:
    try:
        health = json.loads(Path(health_path).read_text())
        return int(health.get("normalized_game_count") or 0) > 0
    except (OSError, ValueError, TypeError, AttributeError):
        return False


def require_slate_history_certificate(slate_date: str) -> dict[str, object]:
    """Require certification of the immediately prior operational date."""
    prior_date = (date.fromisoformat(slate_date) - timedelta(days=1)).isoformat()
    return require_prior_day_certificate(prior_date)


def require_slate_roster_observation(
    slate_date: str, *, slate_root: Path = DEFAULT_SLATE_ROOT,
    roster_root: Path = DEFAULT_ROSTER_ROOT,
) -> dict[str, object]:
    """Require the current slate's verified, complete prestart roster package."""
    from backend.nhl.daily_capture import canonical_game_set_hash, load_canonical_slate
    from backend.nhl.daily_orchestration import verify_slate_roster_coverage

    schedule_dir = Path(slate_root) / slate_date
    try:
        games = load_canonical_slate(
            slate_date=slate_date,
            raw_schedule_path=schedule_dir / "raw_schedule_response.json",
            slate_health_path=schedule_dir / "slate_health.json",
        )
    except FileNotFoundError as error:
        if (schedule_dir / "slate_health.json").exists():
            raise
        # Overnight polls run before the morning schedule import; retry later.
        raise RosterObservationNotReady("CURRENT_SLATE_SCHEDULE_NOT_IMPORTED") from error
    game_ids = [int(game.game_id) for game in games]
    game_hash = canonical_game_set_hash(game_ids)
    if not game_ids:
        return {"status": "VERIFIED_EMPTY_CURRENT_SLATE", "slate_date": slate_date,
                "canonical_game_set_hash": game_hash, "game_ids": []}

    season = season_for_date(slate_date)
    day = Path(roster_root) / f"season={season}" / f"slate_date={slate_date}"
    observations = sorted(day.glob("observation=*") if day.is_dir() else [])
    if not observations:
        raise RosterObservationNotReady("CURRENT_SLATE_ROSTER_OBSERVATION_MISSING")
    candidate = observations[-1]
    try:
        summary = json.loads((candidate / "observation_summary.json").read_text())
        phase = str(summary["phase"])
        verified = verify_slate_roster_coverage(
            candidate, season=season, slate_date=slate_date, phase=phase,
            canonical_game_ids=game_ids, canonical_game_set_hash=game_hash,
            # The newest snapshot may follow a puck drop; observers predict
            # only games still pregame, so it stays valid for those games.
            require_strictly_prestart=False,
        )
    except (OSError, KeyError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise RuntimeError("CURRENT_SLATE_ROSTER_OBSERVATION_INVALID") from error
    except RuntimeError as error:
        raise RuntimeError(f"CURRENT_SLATE_ROSTER_OBSERVATION_INVALID:{error}") from error
    result = {
        "status": "VERIFIED_CURRENT_SLATE_ROSTER",
        "slate_date": slate_date,
        "package_path": verified["path"],
        "package_manifest_sha256": verified["manifest_sha256"],
        "observation_timestamp_utc": verified["observation_timestamp_utc"],
        # False when the snapshot followed a puck drop; it is still valid for
        # the games not yet started.
        "strictly_prestart": verified.get("strictly_prestart"),
        "canonical_game_set_hash": game_hash,
        "game_ids": game_ids,
    }
    if verified.get("roster_coverage") == "PARTIAL":
        # Only games whose two teams both have clean evidence are usable (P2.13).
        result.update(
            status=PARTIAL_ROSTER_STATUS,
            covered_game_ids=verified["covered_game_ids"],
            covered_game_set_hash=verified["covered_game_set_hash"],
            excluded_game_ids=verified["excluded_game_ids"],
            roster_game_exclusions=verified["roster_game_exclusions"],
        )
    return result


def roster_covered_game_ids(roster: Mapping[str, Any]) -> set[int] | None:
    """Covered games of a verified current-slate roster; None means every game."""
    status = roster.get("status")
    if status == PARTIAL_ROSTER_STATUS:
        return {int(game_id) for game_id in roster["covered_game_ids"]}
    if status in {"VERIFIED_CURRENT_SLATE_ROSTER", "VERIFIED_EMPTY_CURRENT_SLATE"}:
        return None
    raise RuntimeError(f"CURRENT_SLATE_ROSTER_STATUS_UNKNOWN:{status}")


def require_full_roster_coverage(roster: Mapping[str, Any]) -> None:
    """Block a lane that has no per-game exclusion surface on a partial roster."""
    if roster.get("status") == PARTIAL_ROSTER_STATUS:
        raise RuntimeError(PARTIAL_ROSTER_NOT_SUPPORTED)


def daily_receipt_source_proofs(
    receipt: dict[str, object], *, slate_root: Path = DEFAULT_SLATE_ROOT,
    accept_partial: bool = False,
) -> dict[str, Any]:
    """Re-verify the prior-day certificate and roster a daily run was built on.

    Mirrors the cross-market resilient adapter. A run recorded before
    certificates were written (2026-10-18) is re-verified from the prior-day
    reconciliation package it recorded. Raises RuntimeError when either proof
    does not verify. A partial-coverage roster (P2.13) is rejected unless the
    caller passes ``accept_partial`` and then restricts itself to the returned
    ``covered_game_ids``.
    """
    from backend.nhl.daily_orchestration import verify_slate_roster_coverage

    slate_date = str(receipt.get("slate_date") or "")
    prior = receipt.get("prior_day_learning") or {}
    certificate = prior.get("certificate") or {} if isinstance(prior, dict) else {}
    prior_date = (date.fromisoformat(slate_date) - timedelta(days=1)).isoformat()
    verified = verify_prior_day_certificate(
        certificate, prior_date, slate_root=slate_root,
        fallback_package=prior.get("reconciliation_package"))
    identity = certificate_identity(verified)
    if (not identity or verified.get("status") not in PRIOR_DAY_CERTIFICATE_STATUSES
            or (certificate_identity(certificate) and certificate_identity(certificate) != identity)):
        raise RuntimeError("DAILY_RECEIPT_PRIOR_CERTIFICATE_UNVERIFIED")
    roster = receipt.get("roster_observation") or {}
    roster_path = Path(str(roster.get("path") or ""))
    roster_hash = hashlib.sha256((roster_path / "SHA256SUMS").read_bytes()).hexdigest()
    if roster.get("manifest_sha256") != roster_hash:
        raise RuntimeError("DAILY_RECEIPT_ROSTER_MANIFEST_MISMATCH")
    summary = json.loads((roster_path / "observation_summary.json").read_text())
    game_ids = [int(game_id) for game_id in receipt.get("canonical_game_ids") or []]
    verified_roster = verify_slate_roster_coverage(
        roster_path, season=int(summary["season"]), slate_date=slate_date,
        phase=str(summary["phase"]), canonical_game_ids=game_ids,
        canonical_game_set_hash=str(receipt.get("canonical_game_set_hash") or ""),
        require_strictly_prestart=False)
    if verified_roster.get("manifest_sha256") != roster_hash:
        raise RuntimeError("DAILY_RECEIPT_ROSTER_UNVERIFIED")
    proofs: dict[str, Any] = {
        "certificate_sha256": str(identity), "roster_source_sha256": roster_hash}
    if verified_roster.get("roster_coverage") == "PARTIAL":
        if not accept_partial:
            raise RuntimeError(f"DAILY_RECEIPT_{PARTIAL_ROSTER_NOT_SUPPORTED}")
        proofs.update(roster_coverage="PARTIAL",
                      covered_game_ids=verified_roster["covered_game_ids"])
    return proofs


def admitted_scheduled_starts(package: Path) -> dict[int, str]:
    """Return each admitted (played) game's scheduled UTC start from a package."""
    with (Path(package) / "canonical_admitted_slate.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if rows and "scheduled_start_time_utc" not in rows[0]:
        raise ValueError("RECONCILIATION_ADMITTED_SLATE_SCHEDULED_START_MISSING")
    return {int(row["game_id"]): str(row["scheduled_start_time_utc"]) for row in rows}
