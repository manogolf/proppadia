"""Idempotent morning handoff from official outcomes to shadow grades."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.cross_market_shadow.core import grade_capture, verify_manifest
from backend.nhl.game_phase import authorized_season_for_date, phase_for_game_type, regular_season_evaluation_eligible
from backend.nhl.postgame_reconcile.core import (
    OFFICIAL_LINEUP_ABSENCE, _official_lineup_absences, _sog_outcomes,
)
from backend.nhl.sog_cold_start.core import grade_predictions as grade_sog_predictions
from backend.nhl.performance_summary import (
    build_production_sog_reference,
    discover_daily_market_coverage,
    generate_from_artifacts,
    select_authoritative_summary,
)
from backend.nhl.sog_fixed_blend import grade_slate as grade_sog_fixed_blend_slate
from backend.nhl.sog_divergence_shadow import grade_slate as grade_sog_divergence_shadow_slate
from backend.nhl.sog_structural_correction_shadow import grade_slate as grade_sog_structural_correction_shadow_slate
from backend.nhl.sog_shrunk_rate_shadow import grade_slate as grade_sog_shrunk_rate_shadow_slate
from backend.nhl.sog_hgb_count_model import grade_slate as grade_sog_count_hgb_shadow_slate
from backend.nhl.sog_formula_shadow import grade_slate as grade_sog_formula_shadow_slate
from backend.nhl.points_hgb_shadow import (
    discover_daily_points_authority,
    grade_prior_hgb_capture,
)
from backend.nhl.reconciliation_certificate import (
    verify_reconciliation_certificate,
    schedule_hash_matches,
    verified_all_games_void,
    verified_empty_schedule,
)


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RECONCILIATION_ROOT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
DEFAULT_CROSS_MARKET_ROOT = ROOT / "artifacts/operational/nhl/cross_market_shadow"


def prior_et_slate(now: datetime | None = None) -> str:
    now = now or datetime.now(ZoneInfo("America/New_York"))
    if now.tzinfo is None:
        raise ValueError("ET_CLOCK_MUST_BE_TIMEZONE_AWARE")
    return (datetime_et_date(now) - timedelta(days=1)).isoformat()


def datetime_et_date(now: datetime) -> date:
    if now.tzinfo is None:
        raise ValueError("ET_CLOCK_MUST_BE_TIMEZONE_AWARE")
    return now.astimezone(ZoneInfo("America/New_York")).date()


def current_et_slate(now: datetime | None = None) -> str:
    now = now or datetime.now(ZoneInfo("America/New_York"))
    return datetime_et_date(now).isoformat()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_reconciliation_package(path: Path, slate_date: str) -> dict:
    verify_reconciliation_certificate(path, slate_date)
    return json.loads((Path(path) / "summary.json").read_text())


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
    health_path = path.with_name("slate_health.json")
    if not path.is_file() or not health_path.is_file():
        return None
    payload = json.loads(path.read_text())
    health = json.loads(health_path.read_text())
    if (health.get("slate_date") != slate_date
            or health.get("downstream_ready") is not True
            or health.get("source") != f"https://api-web.nhle.com/v1/schedule/{slate_date}"
            or not schedule_hash_matches(health.get("raw_source_hash"), path)):
        raise RuntimeError("PRIOR_DATE_SCHEDULE_CERTIFICATE_INVALID")
    weeks = payload.get("gameWeek")
    if not isinstance(weeks, list):
        raise RuntimeError("PRIOR_DATE_SCHEDULE_SCHEMA_INVALID")
    count = sum(len(day.get("games") or []) for day in weeks
                if str(day.get("date")) == slate_date)
    expected = health.get("normalized_game_count")
    if expected is None or int(expected) != count:
        raise RuntimeError("PRIOR_DATE_SCHEDULE_GAME_COUNT_MISMATCH")
    if count == 0:
        verified_empty_schedule(slate_date, slate_root=path.parents[1])
    return count


PHASE_RESTATEMENT_CONTRACT = "NHL_PHASE_RESTATEMENT_V4"
_SOG_OUTCOME_COLUMNS = ("official_final", "official_sog", "participation_status", "outcome_source",
                        "outcome_source_timestamp_utc", "grading_state", "grading_timestamp_utc")


def _restate_sog_participation(frame: pd.DataFrame, skaters: pd.DataFrame) -> pd.DataFrame:
    """Re-grade sealed SOG prediction rows against the package's official skater outcomes.

    Only rows that were ungraded may change: predicted skaters who appeared now
    settle, and those absent from a complete official lineup become
    NO_APPEARANCE. Any other change means the inputs moved and is refused.
    """
    if frame.empty or "grading_state" not in frame:
        return frame
    timestamp = str(frame.grading_timestamp_utc.dropna().iloc[0])
    predictions = frame.drop(columns=[c for c in _SOG_OUTCOME_COLUMNS if c in frame])
    regraded = grade_sog_predictions(
        predictions, _sog_outcomes(skaters, predictions), grading_timestamp_utc=timestamp)
    regraded = regraded[frame.columns].reset_index(drop=True)
    original = frame.reset_index(drop=True)
    changed = regraded.grading_state.ne(original.grading_state)
    if (changed & original.grading_state.ne("UNRESOLVED_UNGRADED")).any():
        raise RuntimeError("SOG_RESTATEMENT_CHANGED_A_RESOLVED_GRADE")
    return regraded


def _restate_points_participation(frame: pd.DataFrame, skaters: pd.DataFrame) -> pd.DataFrame:
    """Mark predicted skaters absent from a complete official lineup as non-participants."""
    if frame.empty or "participation_state" not in frame:
        return frame
    frame = frame.copy()
    absent = _official_lineup_absences(frame, skaters)
    absent_keys = set(map(tuple, absent.to_numpy()))
    keys = frame[["game_id", "player_id"]].astype("int64").apply(tuple, axis=1)
    absent_rows = frame.participation_state.isna() & keys.isin(absent_keys)
    frame.loc[absent_rows, "participation_state"] = OFFICIAL_LINEUP_ABSENCE
    frame.loc[absent_rows, "grading_status"] = "NONPARTICIPANT_UNGRADED"
    frame.loc[frame.participation_state.isna(), "grading_status"] = "PARTICIPATION_STATUS_UNRESOLVED"
    return frame


def _write_phase_restatement(package: Path, root: Path) -> Path:
    """Publish corrected evaluation labels beside, never over, the original package."""
    source_ml = package / "graded_moneyline.csv"
    source_pl = package / "graded_puck_line.csv"
    games = pd.read_csv(package / "canonical_game_outcomes.csv")
    grades = {}
    for lane, source in (("moneyline", source_ml), ("puck_line", source_pl)):
        frame = pd.read_csv(source)
        if frame.empty:
            # No card for this lane (e.g. a preseason slate): nothing to relabel.
            grades[lane] = frame
            continue
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
    # Participation is judged only against the package's official skater outcomes.
    skater_path = package / "canonical_skater_outcomes.csv"
    skaters = pd.read_csv(skater_path) if skater_path.is_file() else None
    grade_changes: dict[str, int] = {}
    for lane in ("points", "saves", "sog", "resilient_sog", "resilient_points"):
        source = package / f"graded_{lane}.csv"
        if not source.is_file() or source.stat().st_size == 0:
            continue
        frame = pd.read_csv(source)
        original = frame.copy()
        if lane in {"points", "resilient_points"} and "participation_state" in frame:
            if skaters is not None:
                frame = _restate_points_participation(frame, skaters)
            else:
                frame.loc[frame.participation_state.isna(), "grading_status"] = "PARTICIPATION_STATUS_UNRESOLVED"
        elif lane in {"sog", "resilient_sog"}:
            if skaters is None:
                continue
            frame = _restate_sog_participation(frame, skaters)
        elif lane == "saves" and "actual_start_flag" in frame:
            unknown = frame.actual_start_flag.isna()
            confirmed_relief = frame.actual_start_flag.eq(False)
            frame.loc[unknown, "grading_status"] = "STARTER_STATUS_UNRESOLVED"
            frame.loc[confirmed_relief, "grading_status"] = "DID_NOT_START_NOT_GRADEABLE_CONDITIONAL"
        status = next((c for c in ("grading_status", "grading_state") if c in frame), None)
        if status:
            grade_changes[lane] = int(frame[status].reset_index(drop=True).ne(
                original[status].reset_index(drop=True)).sum())
        grades[lane] = frame
    source_names = ["SHA256SUMS", "canonical_game_outcomes.csv", "graded_moneyline.csv",
                    "graded_puck_line.csv"]
    if skaters is not None:
        source_names.append("canonical_skater_outcomes.csv")
    source_names.extend(f"graded_{lane}.csv" for lane in grades if lane not in {"moneyline", "puck_line"})
    source_hashes = {name: _sha(package / name) for name in source_names}
    identity = hashlib.sha256(json.dumps({"contract": PHASE_RESTATEMENT_CONTRACT,
        "source": str(package), "source_hashes": source_hashes}, sort_keys=True).encode()).hexdigest()
    destination = root / "learning_restatements" / package.parent.name / f"restatement={identity[:20]}"
    if destination.is_dir():
        return destination
    destination.mkdir(parents=True, exist_ok=False)
    for lane, frame in grades.items():
        frame.to_csv(destination / f"graded_{lane}.csv", index=False)
    (destination / "lineage.json").write_text(json.dumps({
        "contract_version": PHASE_RESTATEMENT_CONTRACT, "source_package": str(package),
        "source_hashes": source_hashes, "game_count": len(games),
        "phase_authority": "official canonical game_type_code; 1 preseason, 2 regular, 3 postseason",
        "participation_authority": (
            "official box score: a predicted skater absent from a final game whose box score "
            "lists dressed skaters for both teams did not play; SOG and resilient SOG are "
            "re-graded from the sealed prediction rows and canonical_skater_outcomes.csv"),
        "grade_changes": {lane: int(changes) for lane, changes in grade_changes.items()},
    }, indent=2, sort_keys=True) + "\n")
    files = sorted(path for path in destination.iterdir() if path.is_file())
    (destination / "SHA256SUMS").write_text("".join(f"{_sha(path)}  {path.name}\n" for path in files))
    return destination


def _grade_final_capture(package: Path, slate_date: str, cross_root: Path) -> tuple[str, str | None]:
    final_root = cross_root / f"season={authorized_season_for_date(slate_date)}" / f"slate_date={slate_date}" / "run_type=FINAL_PREGAME"
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


def _all_games_void_result(slate_date: str, root: Path, provider_calls: int = 0) -> dict:
    """Prior-learning result for a day on which every scheduled game was void."""
    certificate = verified_all_games_void(slate_date, reconciliation_root=root)
    return {"prior_slate_date": slate_date, "official_outcomes_status": "ALL_GAMES_VOID",
            "reconciliation_status": "ALL_GAMES_VOID", "reconciliation_package": None,
            "voided_game_ids": certificate["voided_game_ids"],
            "moneyline_grade_status": "NOT_APPLICABLE", "puck_line_grade_status": "NOT_APPLICABLE",
            "challenger_grade_status": "NOT_APPLICABLE", "provider_calls": provider_calls,
            "credits_consumed": 0}


def _unauthorized_new_player_lookups(result) -> list[int]:
    """Return new-player IDs when the only preflight failure is an unapproved lookup."""
    if result.returncode != 5:
        return []
    try:
        payload = json.loads(result.stdout)
    except (ValueError, TypeError):
        return []
    if not str(payload.get("failure") or "").startswith("AUTHORIZED_PLAYER_LOOKUP_SET_MISMATCH:"):
        return []
    preflight = payload.get("database_preflight") or {}
    classification = preflight.get("classification") or {}
    if classification.get("conflict") or preflight.get("authorized_new_official_lookup_ids"):
        return []
    return sorted(int(value) for value in classification.get("new_official_lookup") or [])


def ensure_prior_learning(
    slate_date: str, *, reconciliation_root: Path = DEFAULT_RECONCILIATION_ROOT,
    cross_market_root: Path = DEFAULT_CROSS_MARKET_ROOT,
    daily_run_root: Path | None = None,
    market_integrity_archive_root: Path | None = None,
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
        try:
            # A day already certified as all-void needs no new official request.
            return _all_games_void_result(slate_date, root)
        except RuntimeError as error:
            if "PRIOR_DATE_ALL_GAMES_VOID_PROBE_MISSING" not in str(error):
                raise
        python = ROOT / ".venv/bin/python"
        reconciler = str(ROOT / "backend/nhl/scripts/run_nhl_postgame_reconciliation.py")
        base = [str(python if python.is_file() else sys.executable), reconciler,
                "--date", slate_date, "--output-root", str(root)]
        # The governed executor's database identity partition is built from
        # typed, immutable authority and roster evidence. Acquire that evidence
        # first, then hand its run ID back as both typed sources. Players with
        # no database row are looked up automatically (see below).
        acquired = subprocess.run(
            [*base, "--authority-roster-acquisition", "--prior-learning"], cwd=ROOT,
            capture_output=True, text=True, check=False)
        if acquired.returncode == 2:
            return {"prior_slate_date": slate_date, "official_outcomes_status": "NOT_FINAL",
                    "reconciliation_status": "NOT_FINAL", "reconciliation_package": None,
                    "moneyline_grade_status": "NOT_RUN", "puck_line_grade_status": "NOT_RUN",
                    "challenger_grade_status": "NOT_RUN", "failure": None,
                    "provider_calls": 0, "credits_consumed": 0}
        if acquired.returncode:
            detail = (acquired.stdout or "") + (acquired.stderr or "")
            raise RuntimeError("GOVERNED_AUTHORITY_ROSTER_ACQUISITION_FAILED:" + detail[-1500:])
        try:
            acquisition = json.loads(acquired.stdout)
            run_id = str(acquisition["run_id"])
            if acquisition.get("status") == "ALL_SCHEDULED_GAMES_VOID" and run_id:
                return _all_games_void_result(slate_date, root, provider_calls=int(
                    (acquisition.get("request_accounting") or {}).get("total_logical_requests", 1)))
            if acquisition.get("status") != "COMPLETE" or not run_id:
                raise ValueError("acquisition receipt not complete")
        except (ValueError, KeyError, TypeError) as error:
            raise RuntimeError("GOVERNED_AUTHORITY_ROSTER_ACQUISITION_RECEIPT_INVALID") from error
        acquisition_requests = int(
            (acquisition.get("request_accounting") or {}).get("total_logical_requests", 0))
        execute = [*base, "--execute",
                   "--prior-learning",
                   "--response-source", f"AUTHORITY_RESPONSE_SOURCE={run_id}",
                   "--response-source", f"ROSTER_RESPONSE_SOURCE={run_id}"]
        result = subprocess.run(execute, cwd=ROOT, capture_output=True, text=True, check=False)
        new_players = _unauthorized_new_player_lookups(result)
        if new_players:
            # A player new to the database (for example a late call-up) is
            # looked up and added automatically. The reconciler still requires
            # the lookup set to match exactly, and identity conflicts still fail.
            result = subprocess.run(
                [*execute, *[arg for player_id in new_players
                             for arg in ("--authorized-player-lookup-id", str(player_id))]],
                cwd=ROOT, capture_output=True, text=True, check=False)
        if result.returncode == 2:
            return {"prior_slate_date": slate_date, "official_outcomes_status": "NOT_FINAL",
                    "reconciliation_status": "NOT_FINAL", "reconciliation_package": None,
                    "moneyline_grade_status": "NOT_RUN", "puck_line_grade_status": "NOT_RUN",
                    "challenger_grade_status": "NOT_RUN", "failure": None,
                    "provider_calls": acquisition_requests, "credits_consumed": 0}
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
    challengers: dict[str, pd.DataFrame] = {}
    challenger_sources: dict[str, Path] = {}
    if grade_path:
        grade_package = Path(grade_path)
        verify_manifest(grade_package)
        for lane, name in (
            ("moneyline", "graded_moneyline_shot_finishing_challenger_v3_results.csv"),
            ("puck_line", "graded_puck_line_v2_shot_prior_model_results.csv"),
            # The cross-market shadow model (V2 since the 2026-10-14 Moneyline cutover).
            ("moneyline_shadow", "graded_moneyline_shadow_results.csv"),
        ):
            candidate = grade_package / name
            if candidate.is_file():
                challengers[lane] = pd.read_csv(candidate)
                challenger_sources[lane] = candidate
    coverage = discover_daily_market_coverage(
        slate_date=slate_date,
        grades={"points": pd.read_csv(restatement / "graded_points.csv")
                if (restatement / "graded_points.csv").is_file() else pd.DataFrame(),
                "saves": pd.read_csv(restatement / "graded_saves.csv")
                if (restatement / "graded_saves.csv").is_file() else pd.DataFrame(),
                "sog": pd.read_csv(package / "graded_sog.csv")
                if (package / "graded_sog.csv").is_file() else pd.DataFrame()},
        daily_run_root=Path(daily_run_root or os.environ.get(
            "NHL_DAILY_RECEIPT_ROOT", ROOT / "artifacts/operational/nhl/daily_runs")),
        integrity_archive_root=Path(market_integrity_archive_root or
            ROOT / "backend/nhl/exports/odds_history"),
    )
    production_sog_reference = None
    production_sog_reference_sources: dict[str, str] = {}
    if "sog" in coverage:
        production_sog_reference, production_sog_reference_sources = build_production_sog_reference(
            slate_date=slate_date, package=package,
            daily_run_root=Path(daily_run_root or os.environ.get(
                "NHL_DAILY_RECEIPT_ROOT", ROOT / "artifacts/operational/nhl/daily_runs")),
            market_coverage=coverage.get("sog"),
        )
    try:
        fixed_blend_grade = grade_sog_fixed_blend_slate(
            slate_date=slate_date, reconciliation_package=package,
            daily_run_root=ROOT / "artifacts/operational/nhl/sog_fixed_blend_shadows",
            output_root=ROOT / "artifacts/operational/nhl/sog_fixed_blend_grades",
        )
    except Exception as error:
        fixed_blend_grade = {"status": "GRADE_FAILED_NONBLOCKING",
                             "reason": f"{type(error).__name__}:{error}"}
    try:
        divergence_shadow_grade = grade_sog_divergence_shadow_slate(
            slate_date=slate_date, reconciliation_package=package,
            capture_root=ROOT / "artifacts/operational/nhl/sog_divergence_shadows",
        )
    except Exception as error:
        divergence_shadow_grade = {"status": "GRADE_FAILED_NONBLOCKING",
                                   "role": "PROSPECTIVE_SHADOW",
                                   "reason": f"{type(error).__name__}:{error}"}
    try:
        structural_shadow_grade = grade_sog_structural_correction_shadow_slate(
            slate_date=slate_date, reconciliation_package=package,
            capture_root=ROOT / "artifacts/operational/nhl/sog_structural_correction_shadows",
        )
    except Exception as error:
        structural_shadow_grade = {"status": "GRADE_FAILED_NONBLOCKING", "role": "PROSPECTIVE_SHADOW",
                                   "reason": f"{type(error).__name__}:{error}", "grades": []}
    try:
        shrunk_rate_shadow_grade = grade_sog_shrunk_rate_shadow_slate(
            slate_date=slate_date, reconciliation_package=package,
            capture_root=ROOT / "artifacts/operational/nhl/sog_shrunk_rate_shadows",
        )
    except Exception as error:
        shrunk_rate_shadow_grade = {"status": "GRADE_FAILED_NONBLOCKING", "role": "PROSPECTIVE_SHADOW",
                                    "reason": f"{type(error).__name__}:{error}", "grades": []}
    try:
        count_hgb_shadow_grade = grade_sog_count_hgb_shadow_slate(
            slate_date=slate_date, reconciliation_package=package,
            capture_root=ROOT / "artifacts/operational/nhl/sog_count_hgb_shadows",
        )
    except Exception as error:
        count_hgb_shadow_grade = {"status": "GRADE_FAILED_NONBLOCKING", "role": "PROSPECTIVE_SHADOW",
                                  "reason": f"{type(error).__name__}:{error}", "grades": []}
    try:
        formula_shadow_grade = grade_sog_formula_shadow_slate(
            slate_date=slate_date, reconciliation_package=package,
            capture_root=ROOT / "artifacts/operational/nhl/sog_formula_shadows",
        )
    except Exception as error:
        formula_shadow_grade = {"status": "GRADE_FAILED_NONBLOCKING", "role": "PROSPECTIVE_SHADOW",
                                "reason": f"{type(error).__name__}:{error}", "grades": []}
    daily_run_root = Path(daily_run_root or os.environ.get(
        "NHL_DAILY_RECEIPT_ROOT", ROOT / "artifacts/operational/nhl/daily_runs"))
    try:
        hgb_grade = grade_prior_hgb_capture(
            slate_date=slate_date, reconciliation_package=package,
            daily_run_root=daily_run_root)
    except Exception as error:
        hgb_grade = {"status": "GRADE_FAILED_NONBLOCKING",
                     "reason": f"{type(error).__name__}:{error}"}
    points_authority_context = discover_daily_points_authority(
        slate_date=slate_date, daily_run_root=daily_run_root)
    if points_authority_context and hgb_grade.get("status") == "COMPLETE":
        points_authority_context["hgb_evaluation"] = {
            "role": hgb_grade.get("evaluation_role", "SHADOW"),
            "grade_path": hgb_grade.get("grade_path"),
            "grade_sha256": hgb_grade.get("grade_sha256"),
            "metrics": hgb_grade.get("hgb_metrics"),
            "phoenix_same_capture_threshold_metrics": hgb_grade.get(
                "phoenix_same_capture_threshold_metrics"),
            "phoenix_same_capture_selected_side_record": hgb_grade.get(
                "phoenix_same_capture_selected_side_record"),
            "selected_side_comparison": hgb_grade.get("selected_side_comparison"),
            "comparison_type": hgb_grade.get("comparison_type"),
        }
    performance_json, performance_md, performance = generate_from_artifacts(
        package=package, restatement=restatement,
        reconciliation_status="CREATED" if created else "REUSED_VALID_PACKAGE",
        challengers=challengers or None,
        challenger_source_artifacts=challenger_sources or None,
        market_coverage=coverage,
        production_sog_reference=production_sog_reference,
        production_sog_reference_sources=production_sog_reference_sources,
        points_authority_context=points_authority_context,
        sog_fixed_blend_context=fixed_blend_grade,
        sog_divergence_shadow_context=divergence_shadow_grade,
        sog_structural_correction_shadow_context=structural_shadow_grade,
        sog_shrunk_rate_shadow_context=shrunk_rate_shadow_grade,
        sog_count_hgb_shadow_context=count_hgb_shadow_grade,
        sog_formula_shadow_context=formula_shadow_grade,
    )
    performance_json, performance = select_authoritative_summary(
        root=performance_json.parent.parent, expected=performance)
    performance_md = performance_json.parent / "performance_summary.md"
    return {
        "prior_slate_date": slate_date, "canonical_phase": (
            "REGULAR_SEASON" if games.game_type_code.astype(int).eq(2).all() else "MIXED_OR_NON_REGULAR"),
        "official_outcomes_status": "FINAL" if games.official_final.astype(bool).all() else "INCOMPLETE",
        "reconciliation_status": "CREATED" if created else "REUSED_VALID_PACKAGE",
        "reconciliation_package": str(package),
        "phase_restatement": str(restatement),
        "performance_summary_json": str(performance_json),
        "performance_summary_md": str(performance_md),
        "performance_summary_identity": performance.get("summary_identity"),
        "performance_summary_revision": performance.get("summary_revision"),
        "points_hgb_shadow_grade": hgb_grade,
        "sog_fixed_blend_shadow_grade": fixed_blend_grade,
        "sog_d10_d20_divergence_shadow_grade": divergence_shadow_grade,
        "sog_d10_structural_correction_shadow_grade": structural_shadow_grade,
        "sog_shrunk_rate_k180_shadow_grade": shrunk_rate_shadow_grade,
        "sog_count_hgb_shadow_grade": count_hgb_shadow_grade,
        "sog_formula_shadow_grade": formula_shadow_grade,
        "performance_summary_lineage_key": performance.get("revision_lineage_key"),
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
        "points_status": summary.get("points_status"),
        "points_warning": summary.get("points_warning"),
        "saves_grade_counts": prop_counts["saves"],
        "unresolved_counts": {lane: sum(value for key, value in counts.items()
                                          if "UNRESOLVED" in key or "STATUS_UNRESOLVED" in key)
                               for lane, counts in prop_counts.items()},
        "game_count": int(summary.get("games", len(games))),
        "provider_calls": (
            acquisition_requests
            + int((summary.get("official_request_accounting") or {}).get(
                "total_logical_requests", 0)) if created else 0),
        "credits_consumed": 0,
    }
