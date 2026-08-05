from __future__ import annotations

import contextlib
import csv
import hashlib
import json
import os
import shutil
import tempfile
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = ROOT / "backend/mlb/config/dh_forward_validation_v1.json"
PACIFIC = ZoneInfo("America/Los_Angeles")


def load_config(path: Path = CONFIG_PATH) -> dict:
    payload = json.loads(path.read_text())
    for key in ("scorer_path", "prediction_ledger", "outcome_ledger", "outcome_source_lineage_ledger", "immutable_grade_sources", "rolling_status", "capture_audit", "prior_feed_cache", "lock_dir", "backup_dir", "provenance_prediction_seed"):
        payload[key] = ROOT / payload[key]
    return payload


def sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_identity(game_date: str, game_pk: int, team_id: int, player_id: int, semantic_id: str) -> str:
    return f"{game_date}|{int(game_pk)}|{int(team_id)}|{int(player_id)}|{semantic_id}"


def fetch_json(url: str, timeout: int = 20) -> tuple[dict, bytes]:
    request = urllib.request.Request(url, headers={"User-Agent": "proppadia-dh-forward-validation/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
    return json.loads(raw), raw


def schedule_url(day: str) -> str:
    return f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={day}"


def feed_url(game_pk: int) -> str:
    return f"https://statsapi.mlb.com/api/v1.1/game/{int(game_pk)}/feed/live"


@contextlib.contextmanager
def exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        try:
            pid = int(dict(part.split("=", 1) for part in path.read_text().split() if "=" in part).get("pid", "0"))
            os.kill(pid, 0)
        except (ValueError, KeyError, ProcessLookupError, PermissionError):
            path.unlink(missing_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise RuntimeError(f"PROCESS_LOCKED:{path}") from exc
    try:
        os.write(fd, f"pid={os.getpid()} created={datetime.now(timezone.utc).isoformat()}\n".encode())
        os.close(fd)
        yield
    finally:
        path.unlink(missing_ok=True)


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists() or path.stat().st_size == 0:
        return [], []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def atomic_write_csv(path: Path, fields: list[str], rows: list[dict], backup_dir: Path | None = None, fail_before_replace: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and backup_dir is not None:
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        shutil.copy2(path, backup_dir / f"{path.name}.{stamp}.bak")
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader(); writer.writerows(rows)
            handle.flush(); os.fsync(handle.fileno())
        if fail_before_replace:
            raise RuntimeError("SIMULATED_ATOMIC_APPEND_FAILURE")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name): os.unlink(tmp_name)


def append_unique_atomic(path: Path, fields: list[str], new_rows: list[dict], identity_field: str, backup_dir: Path, fail_before_replace: bool = False) -> tuple[int, int]:
    old_fields, old_rows = read_csv(path)
    if old_fields and old_fields != fields:
        raise RuntimeError("LEDGER_SCHEMA_MISMATCH")
    seen = {row[identity_field] for row in old_rows}
    admitted = [row for row in new_rows if row[identity_field] not in seen]
    duplicate_count = len(new_rows) - len(admitted)
    if admitted:
        backup_dir.mkdir(parents=True, exist_ok=True)
        if path.exists():
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            shutil.copy2(path, backup_dir / f"{path.name}.{stamp}.bak")
        path.parent.mkdir(parents=True, exist_ok=True)
        old_bytes = path.read_bytes() if path.exists() else b""
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as raw:
                raw.write(old_bytes)
                if old_bytes and not old_bytes.endswith(b"\n"): raw.write(b"\n")
                text = __import__("io").TextIOWrapper(raw, encoding="utf-8", newline="", write_through=True)
                writer = csv.DictWriter(text, fieldnames=fields, extrasaction="ignore")
                if not old_bytes: writer.writeheader()
                writer.writerows(admitted); text.flush(); os.fsync(raw.fileno()); text.detach()
            if fail_before_replace: raise RuntimeError("SIMULATED_ATOMIC_APPEND_FAILURE")
            os.replace(tmp_name, path)
            if old_bytes and not path.read_bytes().startswith(old_bytes): raise RuntimeError("APPEND_PREFIX_INTEGRITY_FAILURE")
        finally:
            if os.path.exists(tmp_name): os.unlink(tmp_name)
    elif not path.exists():
        atomic_write_csv(path, fields, [], backup_dir)
    return len(admitted), duplicate_count


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def pacific_today() -> str:
    return datetime.now(PACIFIC).date().isoformat()


def date_range(end_exclusive: str, days: int):
    end = date.fromisoformat(end_exclusive)
    for offset in range(days, 0, -1):
        yield (end - timedelta(days=offset)).isoformat()


def capture_window_state(schedule: dict, now: datetime, hours_before: int) -> str:
    starts = [parse_utc(game["gameDate"]) for block in schedule.get("dates", []) for game in block.get("games", [])]
    if not starts: return "NO_GAMES"
    if now < min(starts) - timedelta(hours=hours_before): return "BEFORE_WINDOW"
    if now >= max(starts): return "AFTER_WINDOW"
    return "ACTIVE"


def validate_scorer(config: dict, artifact: dict) -> str:
    if sha256_path(config["scorer_path"]) != config["scorer_sha256"]:
        raise RuntimeError("BLOCKED_SCORER_HASH_OR_SCHEMA:SCORER_HASH")
    if artifact.get("feature_columns") != config["feature_columns"]:
        raise RuntimeError("BLOCKED_SCORER_HASH_OR_SCHEMA:FEATURE_SCHEMA")
    if float(artifact.get("regularization_C")) != float(config["regularization_C"]):
        raise RuntimeError("BLOCKED_SCORER_HASH_OR_SCHEMA:C")
    if int(artifact.get("shrinkage_alpha")) != int(config["shrinkage_alpha"]):
        raise RuntimeError("BLOCKED_SCORER_HASH_OR_SCHEMA:ALPHA")
    if float(artifact.get("reference_cutoff_80")) != float(config["frozen_top20_cutoff"]):
        raise RuntimeError("BLOCKED_SCORER_HASH_OR_SCHEMA:CUTOFF")
    return hashlib.sha256(json.dumps(config["feature_columns"], separators=(",", ":")).encode()).hexdigest()


def update_rolling_status(config: dict) -> dict:
    _, predictions = read_csv(config["prediction_ledger"]); _, outcomes = read_csv(config["outcome_ledger"])
    outcome_by_id = {r["canonical_identity"]: r for r in outcomes}
    graded = [outcome_by_id[r["canonical_identity"]] for r in predictions if r["canonical_identity"] in outcome_by_id and outcome_by_id[r["canonical_identity"]].get("grading_status", "").startswith("RESOLVED_")]
    top_predictions = [r for r in predictions if r.get("forward_top20") == "1"]
    top_ids = {r["canonical_identity"] for r in top_predictions}
    top_graded = [r for r in graded if r["canonical_identity"] in top_ids]
    rest_graded = [r for r in graded if r["canonical_identity"] not in top_ids]
    event = lambda rs: sum(r.get("pinch_hit_before_fourth_pa") == "1" for r in rs)
    prediction_by_id = {r["canonical_identity"]: r for r in predictions}
    paired = [(prediction_by_id[r["canonical_identity"]], r) for r in graded]
    def available_mean(values):
        nums = [float(v) for v in values if v not in (None, "")]
        return sum(nums)/len(nums) if nums else None
    baseline_mean = available_mean(p.get("baseline_expected_pa") for p,_ in paired)
    adjusted_mean = available_mean(p.get("adjusted_expected_pa") for p,_ in paired)
    actual_mean = available_mean(o.get("original_dh_plate_appearances") for _,o in paired)
    baseline_error = available_mean(abs(float(p["baseline_expected_pa"])-float(o["original_dh_plate_appearances"])) for p,o in paired if p.get("baseline_expected_pa") not in (None,"") and o.get("original_dh_plate_appearances") not in (None,""))
    adjusted_error = available_mean(abs(float(p["adjusted_expected_pa"])-float(o["original_dh_plate_appearances"])) for p,o in paired if p.get("adjusted_expected_pa") not in (None,"") and o.get("original_dh_plate_appearances") not in (None,""))
    from collections import Counter
    payload = {
        "status": config["evidence_status"], "production_status": config["production_status"],
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "captured_dates": sorted({r["game_date"] for r in predictions}),
        "captured_games": len({r["game_pk"] for r in predictions}), "captured_starting_dhs": len(predictions),
        "top20_rows": len(top_predictions), "graded_rows": len(graded), "unresolved_rows": len(predictions)-len(graded),
        "pinch_hit_before_fourth_events": event(graded),
        "top20_event_prevalence": event(top_graded)/len(top_graded) if top_graded else None,
        "remaining_population_event_prevalence": event(rest_graded)/len(rest_graded) if rest_graded else None,
        "event_capture": event(top_graded)/event(graded) if event(graded) else None,
        "risk_ratio": (event(top_graded)/len(top_graded))/(event(rest_graded)/len(rest_graded)) if top_graded and rest_graded and event(rest_graded) else None,
        "opportunity_evidence": {"baseline_expected_pa_mean":baseline_mean,"adjusted_expected_pa_mean":adjusted_mean,"actual_pa_mean":actual_mean,"baseline_pa_mae":baseline_error,"adjusted_pa_mae":adjusted_error},
        "hits_o15_evidence": {"available_rows":sum(r.get("hits_o15_outcome") in ("WIN","LOSS") for r in graded),"wins":sum(r.get("hits_o15_outcome")=="WIN" for r in graded),"losses":sum(r.get("hits_o15_outcome")=="LOSS" for r in graded)},
        "concentration": {"player_counts":dict(Counter(r["player_mlb_id"] for r in predictions)),"team_counts":dict(Counter(r["team_mlb_id"] for r in predictions)),"date_counts":dict(Counter(r["game_date"] for r in predictions)),"batting_slot_counts":dict(Counter(r["batting_order"] for r in predictions)),"month_counts":dict(Counter(r["game_date"][:7] for r in predictions))},
        "automatic_replication_decision": "NOT_AUTHORIZED"
    }
    config["rolling_status"].parent.mkdir(parents=True, exist_ok=True)
    tmp = config["rolling_status"].with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True)+"\n"); os.replace(tmp, config["rolling_status"])
    return payload
