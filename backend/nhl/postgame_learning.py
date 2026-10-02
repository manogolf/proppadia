"""Idempotent morning handoff from official outcomes to shadow grades."""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.cross_market_shadow.core import grade_capture, verify_manifest
from backend.nhl.game_phase import phase_for_game_type, regular_season_evaluation_eligible


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RECONCILIATION_ROOT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
DEFAULT_CROSS_MARKET_ROOT = ROOT / "artifacts/operational/nhl/cross_market_shadow"


def prior_et_slate(now: datetime | None = None) -> str:
    now = now or datetime.now(ZoneInfo("America/New_York"))
    if now.tzinfo is None:
        raise ValueError("ET_CLOCK_MUST_BE_TIMEZONE_AWARE")
    return (now.astimezone(ZoneInfo("America/New_York")).date() - timedelta(days=1)).isoformat()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_reconciliation_package(path: Path, slate_date: str) -> dict:
    path = Path(path).resolve()
    manifest = path / "SHA256SUMS"
    if not manifest.is_file():
        raise RuntimeError("RECONCILIATION_PACKAGE_MANIFEST_MISSING")
    entries = {}
    for line in manifest.read_text().splitlines():
        try:
            digest, name = line.split("  ", 1)
        except ValueError:
            raise RuntimeError("RECONCILIATION_PACKAGE_MANIFEST_INVALID") from None
        file_path = path / name
        if Path(name).name != name or not file_path.is_file() or _sha(file_path) != digest:
            raise RuntimeError("RECONCILIATION_PACKAGE_HASH_MISMATCH")
        entries[name] = digest
    marker = json.loads((path / "RUN_COMPLETE.json").read_text())
    summary = json.loads((path / "summary.json").read_text())
    if marker.get("status") != "COMPLETE" or summary.get("status") != "COMPLETE":
        raise RuntimeError("RECONCILIATION_PACKAGE_NOT_COMPLETE")
    if summary.get("slate_date") != slate_date:
        raise RuntimeError("RECONCILIATION_PACKAGE_SLATE_MISMATCH")
    required = {"canonical_game_outcomes.csv", "graded_moneyline.csv", "graded_puck_line.csv"}
    if not required.issubset(entries):
        raise RuntimeError("RECONCILIATION_PACKAGE_REQUIRED_OUTPUT_MISSING")
    return summary


def _existing_reconciliation(root: Path, slate_date: str) -> Path | None:
    day = root / slate_date
    packages = sorted(day.glob("reconciliation=*") if day.exists() else [])
    if len(packages) > 1:
        raise RuntimeError("MULTIPLE_RECONCILIATION_PACKAGES_FOR_SLATE")
    if not packages:
        incomplete = list(day.glob(".reconciliation.incomplete.*")) if day.exists() else []
        if incomplete:
            raise RuntimeError("PARTIAL_RECONCILIATION_PACKAGE_REQUIRES_REVIEW")
        return None
    return packages[0]


def _retained_schedule_game_count(slate_date: str) -> int | None:
    path = ROOT / "artifacts/operational/nhl/slates" / slate_date / "raw_schedule_response.json"
    if not path.is_file():
        return None
    payload = json.loads(path.read_text())
    weeks = payload.get("gameWeek") or []
    return sum(len(day.get("games") or []) for day in weeks
               if str(day.get("date")) == slate_date)


def _write_phase_restatement(package: Path, root: Path) -> Path:
    """Publish corrected evaluation labels beside, never over, the original package."""
    source_ml = package / "graded_moneyline.csv"
    source_pl = package / "graded_puck_line.csv"
    games = pd.read_csv(package / "canonical_game_outcomes.csv")
    grades = {}
    for lane, source in (("moneyline", source_ml), ("puck_line", source_pl)):
        frame = pd.read_csv(source)
        frame["canonical_phase"] = frame.game_type_code.map(phase_for_game_type)
        eligible = frame.game_type_code.map(regular_season_evaluation_eligible)
        frame["grading_status"] = "PRESEASON_NON_EVALUATION"
        frame.loc[eligible, "grading_status"] = "REGULAR_SEASON_GRADED"
        frame.loc[frame.canonical_phase.eq("POSTSEASON"), "grading_status"] = "POSTSEASON_NON_REGULAR_SEASON_EVALUATION"
        frame.loc[frame.canonical_phase.eq("UNKNOWN_GAME_TYPE"), "grading_status"] = "GAME_TYPE_UNRESOLVED"
        frame["regular_season_evaluation_target"] = pd.NA
        winner = frame.official_full_game_winner
        frame.loc[eligible, "regular_season_evaluation_target"] = winner.map({"HOME": 1, "AWAY": 0})[eligible]
        if lane == "moneyline" and "model_favored_team" in frame:
            home_won = winner.eq("HOME")
            frame["prediction_correct"] = frame.model_favored_team.eq(frame.home_team).eq(home_won).where(eligible)
        if lane == "puck_line":
            margin = pd.to_numeric(frame.official_final_home_goals, errors="coerce") - pd.to_numeric(frame.official_final_away_goals, errors="coerce")
            frame["actual_margin_class"] = pd.Series(pd.NA, index=frame.index, dtype="string")
            frame.loc[eligible & margin.le(-2), "actual_margin_class"] = "AWAY_BY_2_PLUS"
            frame.loc[eligible & margin.abs().lt(2), "actual_margin_class"] = "ONE_GOAL_GAME"
            frame.loc[eligible & margin.ge(2), "actual_margin_class"] = "HOME_BY_2_PLUS"
            probability_columns = ["away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability"]
            if set(probability_columns).issubset(frame.columns):
                frame["predicted_margin_class"] = frame[probability_columns].idxmax(axis=1).map({
                    "away_by_2_plus_probability": "AWAY_BY_2_PLUS",
                    "one_goal_game_probability": "ONE_GOAL_GAME",
                    "home_by_2_plus_probability": "HOME_BY_2_PLUS",
                })
                frame["prediction_correct"] = frame.predicted_margin_class.eq(frame.actual_margin_class).where(eligible)
        grades[lane] = frame
    for lane in ("points", "saves"):
        source = package / f"graded_{lane}.csv"
        if not source.is_file():
            continue
        frame = pd.read_csv(source)
        if lane == "points" and "participation_state" in frame:
            unknown = frame.participation_state.isna()
            frame.loc[unknown, "grading_status"] = "PARTICIPATION_STATUS_UNRESOLVED"
        elif lane == "saves" and "actual_start_flag" in frame:
            unknown = frame.actual_start_flag.isna()
            confirmed_relief = frame.actual_start_flag.eq(False)
            frame.loc[unknown, "grading_status"] = "STARTER_STATUS_UNRESOLVED"
            frame.loc[confirmed_relief, "grading_status"] = "DID_NOT_START_NOT_GRADEABLE_CONDITIONAL"
        grades[lane] = frame
    source_names = ["SHA256SUMS", "canonical_game_outcomes.csv", "graded_moneyline.csv", "graded_puck_line.csv"]
    source_names.extend(name for name in ("graded_points.csv", "graded_saves.csv") if (package / name).is_file())
    source_hashes = {name: _sha(package / name) for name in source_names}
    identity = hashlib.sha256(json.dumps({"contract": "NHL_PHASE_RESTATEMENT_V3",
        "source": str(package), "source_hashes": source_hashes}, sort_keys=True).encode()).hexdigest()
    destination = root / "learning_restatements" / package.parent.name / f"restatement={identity[:20]}"
    if destination.is_dir():
        return destination
    destination.mkdir(parents=True, exist_ok=False)
    for lane, frame in grades.items():
        frame.to_csv(destination / f"graded_{lane}.csv", index=False)
    (destination / "lineage.json").write_text(json.dumps({
        "contract_version": "NHL_PHASE_RESTATEMENT_V3", "source_package": str(package),
        "source_hashes": source_hashes, "game_count": len(games),
        "phase_authority": "official canonical game_type_code; 1 preseason, 2 regular, 3 postseason",
    }, indent=2, sort_keys=True) + "\n")
    files = sorted(path for path in destination.iterdir() if path.is_file())
    (destination / "SHA256SUMS").write_text("".join(f"{_sha(path)}  {path.name}\n" for path in files))
    return destination


def _grade_final_capture(package: Path, slate_date: str, cross_root: Path) -> tuple[str, str | None]:
    final_root = cross_root / "season=2026" / f"slate_date={slate_date}" / "run_type=FINAL_PREGAME"
    runs = sorted(final_root.glob("state=*")) if final_root.exists() else []
    if not runs:
        return "NOT_AVAILABLE", None
    if len(runs) != 1:
        raise RuntimeError("FINAL_PREGAME_CAPTURE_CARDINALITY_INVALID")
    run = runs[0]
    verify_manifest(run)
    outcomes = pd.read_csv(package / "canonical_game_outcomes.csv")
    outcomes = outcomes.rename(columns={
        "official_final_home_goals": "final_home_goals",
        "official_final_away_goals": "final_away_goals",
        "official": "game_status",
    })
    outcomes["game_status"] = outcomes.get("official_final", True).map(
        lambda value: "FINAL" if bool(value) else "SCHEDULED")
    outcomes["outcome_source"] = "NHL_OFFICIAL_GAMECENTER"
    required = ["canonical_season", "game_id", "final_home_goals", "final_away_goals",
                "game_status", "outcome_source", "outcome_source_timestamp_utc"]
    missing = set(required) - set(outcomes)
    if missing:
        raise RuntimeError("RECONCILIATION_OUTCOME_SCHEMA_INCOMPLETE")
    with tempfile.TemporaryDirectory(prefix="nhl_morning_grade_") as temp_dir:
        temporary = Path(temp_dir) / "official_outcomes.csv"
        outcomes[required].to_csv(temporary, index=False)
        grading_time = datetime.now(ZoneInfo("UTC")).isoformat()
        destination = grade_capture(run, temporary, cross_root / "grades", grading_time)
    challenger_present = any((run / name).is_file() for name in (
        "moneyline_shot_finishing_challenger_v3_predictions.csv",
        "puck_line_v2_shot_prior_challenger_predictions.csv",
    ))
    return ("COMPLETE" if challenger_present else "NO_CHALLENGER_ARTIFACT"), str(destination)


def ensure_prior_learning(
    slate_date: str, *, reconciliation_root: Path = DEFAULT_RECONCILIATION_ROOT,
    cross_market_root: Path = DEFAULT_CROSS_MARKET_ROOT,
    create_if_missing: bool = True,
) -> dict:
    """Reuse a valid package or ask the governed reconciler to create one."""
    root = Path(reconciliation_root).resolve()
    package = _existing_reconciliation(root, slate_date)
    created = False
    if package is None and create_if_missing:
        if _retained_schedule_game_count(slate_date) == 0:
            return {"prior_slate_date": slate_date, "official_outcomes_status": "NO_PRIOR_GAMES",
                    "reconciliation_status": "NO_PRIOR_GAMES", "reconciliation_package": None,
                    "moneyline_grade_status": "NOT_APPLICABLE", "puck_line_grade_status": "NOT_APPLICABLE",
                    "challenger_grade_status": "NOT_APPLICABLE", "provider_calls": 0,
                    "credits_consumed": 0}
        python = ROOT / ".venv/bin/python"
        command = [str(python if python.is_file() else sys.executable), str(ROOT / "backend/nhl/scripts/run_nhl_postgame_reconciliation.py"),
                   "--execute", "--date", slate_date, "--output-root", str(root)]
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
        if result.returncode == 2:
            return {"prior_slate_date": slate_date, "official_outcomes_status": "NOT_FINAL",
                    "reconciliation_status": "NOT_FINAL", "reconciliation_package": None,
                    "moneyline_grade_status": "NOT_RUN", "puck_line_grade_status": "NOT_RUN",
                    "challenger_grade_status": "NOT_RUN", "failure": None,
                    "provider_calls": 0, "credits_consumed": 0}
        if result.returncode:
            detail = (result.stdout or "") + (result.stderr or "")
            raise RuntimeError("GOVERNED_RECONCILIATION_FAILED:" + detail[-1500:])
        package = _existing_reconciliation(root, slate_date)
        created = True
    if package is None:
        return {"prior_slate_date": slate_date, "official_outcomes_status": "NO_PACKAGE",
                "reconciliation_status": "NOT_RUN", "reconciliation_package": None,
                "moneyline_grade_status": "NOT_RUN", "puck_line_grade_status": "NOT_RUN",
                "challenger_grade_status": "NOT_RUN", "provider_calls": 0,
                "credits_consumed": 0}
    summary = verify_reconciliation_package(package, slate_date)
    restatement = _write_phase_restatement(package, root)
    grade_status, grade_path = _grade_final_capture(
        package, slate_date, Path(cross_market_root).resolve())
    games = pd.read_csv(package / "canonical_game_outcomes.csv")
    ml = pd.read_csv(restatement / "graded_moneyline.csv")
    pl = pd.read_csv(restatement / "graded_puck_line.csv")
    prop_counts = {}
    for lane in ("sog", "points", "saves"):
        path = restatement / f"graded_{lane}.csv"
        if not path.is_file():
            path = package / f"graded_{lane}.csv"
        if path.is_file():
            prop_frame = pd.read_csv(path)
            status_column = "grading_status" if "grading_status" in prop_frame else "grading_state"
            counts = prop_frame[status_column].value_counts().to_dict() if status_column in prop_frame else {}
        else:
            counts = {}
        prop_counts[lane] = {str(key): int(value) for key, value in counts.items()}
    return {
        "prior_slate_date": slate_date, "canonical_phase": (
            "REGULAR_SEASON" if games.game_type_code.astype(int).eq(2).all() else "MIXED_OR_NON_REGULAR"),
        "official_outcomes_status": "FINAL" if games.official_final.astype(bool).all() else "INCOMPLETE",
        "reconciliation_status": "CREATED" if created else "REUSED_VALID_PACKAGE",
        "reconciliation_package": str(package),
        "phase_restatement": str(restatement),
        "moneyline_grade_status": ("NOT_AVAILABLE" if ml.empty else "COMPLETE"
                                    if ml.grading_status.eq("REGULAR_SEASON_GRADED").all()
                                    else "NON_EVALUATION_OR_INCOMPLETE"),
        "moneyline_grade_counts": {str(key): int(value) for key, value in ml.grading_status.value_counts().items()},
        "puck_line_grade_status": ("NOT_AVAILABLE" if pl.empty else "COMPLETE"
                                    if pl.grading_status.eq("REGULAR_SEASON_GRADED").all()
                                    else "NON_EVALUATION_OR_INCOMPLETE"),
        "puck_line_grade_counts": {str(key): int(value) for key, value in pl.grading_status.value_counts().items()},
        "challenger_grade_status": grade_status, "cross_market_grade_package": grade_path,
        "sog_grade_counts": prop_counts["sog"], "points_grade_counts": prop_counts["points"],
        "saves_grade_counts": prop_counts["saves"],
        "unresolved_counts": {lane: sum(value for key, value in counts.items()
                                          if "UNRESOLVED" in key or "STATUS_UNRESOLVED" in key)
                               for lane, counts in prop_counts.items()},
        "game_count": int(summary.get("games", len(games))),
        "provider_calls": int((summary.get("official_request_accounting") or {}).get("total_logical_requests", 0)) if created else 0,
        "credits_consumed": 0,
    }
