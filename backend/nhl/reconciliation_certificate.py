"""Read-only validation for prior-day reconciliation certificates."""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RECONCILIATION_ROOT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
DEFAULT_SLATE_ROOT = ROOT / "artifacts/operational/nhl/slates"
DEFAULT_ROSTER_ROOT = ROOT / "artifacts/operational/nhl/roster_observations"
REQUIRED_PACKAGE_FILES = {
    "canonical_admitted_slate.csv",
    "canonical_game_outcomes.csv",
    "graded_moneyline.csv",
    "graded_puck_line.csv",
}


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
    return {
        "status": "CERTIFIED_FINAL",
        "slate_date": slate_date,
        "package_path": str(package),
        "package_manifest_sha256": _sha256(package / "SHA256SUMS"),
        "game_ids": sorted(map(int, outcome_ids)),
        "game_count": len(outcomes),
        "files": sorted(files),
    }


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
            or health.get("raw_source_hash") != _sha256(raw_path)):
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
    try:
        return verified_empty_schedule(slate_date, slate_root=slate_root)
    except RuntimeError as error:
        raise RuntimeError(f"PRIOR_DAY_RECONCILIATION_CERTIFICATE_REQUIRED:{error}") from error


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
    from backend.nhl.daily_orchestration import verify_roster_observation_reuse

    schedule_dir = Path(slate_root) / slate_date
    games = load_canonical_slate(
        slate_date=slate_date,
        raw_schedule_path=schedule_dir / "raw_schedule_response.json",
        slate_health_path=schedule_dir / "slate_health.json",
    )
    game_ids = [int(game.game_id) for game in games]
    game_hash = canonical_game_set_hash(game_ids)
    if not game_ids:
        return {"status": "VERIFIED_EMPTY_CURRENT_SLATE", "slate_date": slate_date,
                "canonical_game_set_hash": game_hash, "game_ids": []}

    year, month = map(int, slate_date[:7].split("-"))
    season = year if month >= 7 else year - 1
    day = Path(roster_root) / f"season={season}" / f"slate_date={slate_date}"
    observations = sorted(day.glob("observation=*") if day.is_dir() else [])
    if not observations:
        raise RuntimeError("CURRENT_SLATE_ROSTER_OBSERVATION_MISSING")
    candidate = observations[-1]
    try:
        summary = json.loads((candidate / "observation_summary.json").read_text())
        phase = str(summary["phase"])
        verified = verify_roster_observation_reuse(
            candidate, season=season, slate_date=slate_date, phase=phase,
            canonical_game_ids=game_ids, canonical_game_set_hash=game_hash,
        )
    except (OSError, KeyError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise RuntimeError("CURRENT_SLATE_ROSTER_OBSERVATION_INVALID") from error
    except RuntimeError as error:
        raise RuntimeError(f"CURRENT_SLATE_ROSTER_OBSERVATION_INVALID:{error}") from error
    return {
        "status": "VERIFIED_CURRENT_SLATE_ROSTER",
        "slate_date": slate_date,
        "package_path": verified["path"],
        "package_manifest_sha256": verified["manifest_sha256"],
        "observation_timestamp_utc": verified["observation_timestamp_utc"],
        "canonical_game_set_hash": game_hash,
        "game_ids": game_ids,
    }
