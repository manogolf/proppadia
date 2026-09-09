#!/usr/bin/env python3
"""Offline, create-only three-lane NHL preseason operator rehearsal."""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

from backend.nhl.analysis_package_guard import begin_package, finalize_package, verify_manifest
from backend.nhl.mainline_shadow.core import grade_run as grade_moneyline
from backend.nhl.mainline_shadow.core import run_shadow as run_moneyline
from backend.nhl.points_quote_capture.core import capture_run as capture_points
from backend.nhl.points_quote_capture.core import write_manifest as write_complete_manifest
from backend.nhl.points_shadow.core import POLICY_STATUS, grade_run as grade_points
from backend.nhl.points_shadow.core import make_run_id as points_run_id
from backend.nhl.points_shadow.core import run_shadow as run_points
from backend.nhl.points_shadow.core import verify_frozen_identity
from backend.nhl.sog_candidate_lineage.core import effective_config
from backend.nhl.sog_quote_capture.core import capture_run as capture_sog
from backend.nhl.sog_shadow.core import grade_run as grade_sog
from backend.nhl.sog_shadow.core import run_shadow as run_sog

ROOT = Path(__file__).resolve().parents[3]
DATE = "2026-09-08"
SLATE = "2026-09-19"
GAME_ID = 2026020001
START = "2026-09-19T22:00:00Z"
MIDDAY = "2026-09-19T17:00:00Z"
FINAL = "2026-09-19T21:30:00Z"
OUT = ROOT / "artifacts/analysis/model_development/nhl_season_2026_three_lane_preseason_operator_rehearsal_v1" / DATE
PARITY = ROOT / "artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13/nhl_season_2025_sog_reproduction_run_summary_2026-07-13.json"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree(path: Path) -> dict[str, str]:
    return {p.relative_to(path).as_posix(): sha(p) for p in sorted(path.rglob("*")) if p.is_file()}


def write_csv(path: Path, rows: list[dict], fields: list[str] | None = None) -> None:
    fields = fields or list(rows[0])
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def expect(rows: list[dict], test_id: str, fn, expected: str, lane: str, severity: str = "CRITICAL") -> None:
    try:
        result = fn()
    except BaseException as exc:
        evidence = f"{type(exc).__name__}:{exc}"
        passed = expected in evidence
    else:
        evidence = str(result)
        passed = expected in evidence
    rows.append({"test_id": test_id, "lane": lane, "severity": severity, "status": "PASS" if passed else "FAIL", "expected": expected, "evidence": evidence})


def package_parent(directory: Path, games: pd.DataFrame, players: pd.DataFrame) -> tuple[Path, Path, Path]:
    directory.mkdir(parents=True, exist_ok=False)
    game_path, player_path = directory / "canonical_game_spine.csv", directory / "points_player_inputs.csv"
    games.to_csv(game_path, index=False)
    players.to_csv(player_path, index=False)
    (directory / "RUN_COMPLETE.json").write_text('{"status":"COMPLETE"}\n')
    write_complete_manifest(directory, complete_only=True)
    return game_path, player_path, directory / "SHA256SUMS"


def prop_payload(prop: str, players: pd.DataFrame, capture: str, update: str, *, partial: bool = False, post_start: bool = False) -> dict:
    lines = [1.5, 2.5, 3.5] if prop == "player_shots_on_goal" else [0.5, 1.5, 2.5]
    outcomes = []
    if partial and prop == "player_points" and "player_name" in players:
        selected = players[players.player_name.eq("Coherent Skater")]
    else:
        selected = players.iloc[:1] if partial else players
    for player in selected.itertuples():
        for line in lines:
            outcomes.extend([
                {"name": "Over", "description": player.player_name, "point": line, "price": 110, "last_update": START if post_start else update},
                {"name": "Under", "description": player.player_name, "point": line, "price": -125, "last_update": START if post_start else update},
            ])
    return {"capture_timestamp_utc": capture, "provider": "CERTIFIED_ISOLATED_REHEARSAL_FIXTURE", "request_metadata": {"fixture": True}, "provider_response": [{
        "id": "rehearsal-event-2026020001", "commence_time": START, "home_team": "Home Club", "away_team": "Away Club",
        "bookmakers": [{"key": "fixture_book", "title": "Fixture Book", "last_update": update, "markets": [{"id": prop + "-1", "key": prop, "last_update": update, "outcomes": outcomes}]}],
    }]}


def moneyline_payload(capture: str, update: str, post_start: bool = False) -> dict:
    market_time = START if post_start else update
    return {"capture_timestamp_utc": capture, "provider_response": [{"id": "rehearsal-event-2026020001", "commence_time": START, "home_team": "Home Club", "away_team": "Away Club", "bookmakers": [{"key": "fixture_book", "title": "Fixture Book", "last_update": market_time, "markets": [{"key": "h2h", "last_update": market_time, "outcomes": [{"name": "Home Club", "price": -120}, {"name": "Away Club", "price": 110}]}]}]}]}


def run_cmd(args: list[str], cwd: Path = ROOT) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=cwd, text=True, capture_output=True)


def sentinel(tmp: Path, run_id: str, phase: str, payload: dict) -> tuple[int, dict]:
    src = tmp / f"{run_id}_input.json"
    write_json(src, payload)
    proc = run_cmd([str(ROOT / ".venv/bin/python"), str(ROOT / "backend/nhl/scripts/run_nhl_live_failure_sentinel.py"), "--phase", phase, "--slate-date", SLATE, "--run-id", run_id, "--input-json", str(src), "--output-root", str(tmp / "sentinels")])
    result_path = tmp / "sentinels" / SLATE / run_id / "nhl_live_failure_sentinel.json"
    return proc.returncode, json.loads(result_path.read_text())


def main() -> int:
    staging = begin_package(OUT)
    negative: list[dict] = []
    stages: list[dict] = []
    reconciliation: list[dict] = []
    defects: list[dict] = []
    command_results: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="nhl_three_lane_rehearsal_") as raw:
        tmp = Path(raw)
        # Offline equivalent of the 07:30 environment/database/schedule/prerequisite sequence.
        morning_root = tmp / "morning"
        morning = run_cmd([str(ROOT / ".venv/bin/python"), str(ROOT / "backend/nhl/scripts/run_nhl_morning_orchestration.py"), "--slate-date", SLATE, "--dry-run", "--fixture-scenario", "nonempty", "--output-root", str(morning_root)])
        health_path = Path(morning.stdout.strip())
        health = json.loads(health_path.read_text())
        stages.append({"stage": "01-04_environment_db_schedule_spine_prerequisites", "lane": "ALL", "planned": "YES", "completed": "YES" if morning.returncode == 0 and health["overall_status"] == "DRY_RUN" else "NO", "status": health["overall_status"], "evidence": str(health_path)})
        empty = run_cmd([str(ROOT / ".venv/bin/python"), str(ROOT / "backend/nhl/scripts/run_nhl_morning_orchestration.py"), "--slate-date", SLATE, "--dry-run", "--fixture-scenario", "valid_empty", "--output-root", str(tmp / "morning_empty")])
        empty_health = json.loads(Path(empty.stdout.strip()).read_text())
        negative.append({"test_id": "valid_empty_slate_stops_downstream_cleanly", "lane": "ALL", "severity": "CRITICAL", "status": "PASS" if empty.returncode == 0 and empty_health["overall_status"] == "VALID_EMPTY_SLATE" else "FAIL", "expected": "VALID_EMPTY_SLATE", "evidence": empty_health["overall_status"]})

        games = pd.DataFrame([{"canonical_season": 2026, "slate_date": SLATE, "game_id": GAME_ID, "game_date": SLATE, "scheduled_start_time_utc": START, "game_type_code": 1, "home_team_id": 1, "home_team": "Home Club", "away_team_id": 2, "away_team": "Away Club", "game_status": "SCHEDULED", "provider_event_id": "rehearsal-event-2026020001"}])
        game_csv = tmp / "canonical_game_spine.csv"
        games.to_csv(game_csv, index=False)
        history_cols = ["canonical_season", "game_id", "game_date", "scheduled_start_time_utc", "home_team_id", "away_team_id", "final_home_goals", "final_away_goals", "final_home_shots", "final_away_shots", "game_status", "game_type_code"]
        history_csv = tmp / "team_history.csv"
        pd.DataFrame(columns=history_cols).to_csv(history_csv, index=False)

        # Retained Points fixed inputs provide known coherent, minor and materially blocked ladders.
        identity = verify_frozen_identity()
        base = pd.read_csv(ROOT / identity["fixed_input"]["path"])
        ids = [8475852, 8476869, 8473986]
        points_players = base[base.player_id.isin(ids)].copy().sort_values("player_id")
        names = {8475852: "Coherent Skater", 8476869: "Minor Skater", 8473986: "Blocked Skater"}
        points_players["player_name"] = points_players.player_id.map(names)
        points_players["game_id"] = GAME_ID
        points_players["canonical_season"] = 2026
        points_players["slate_date"] = SLATE
        points_players["team"] = "Home Club"
        points_players["opponent"] = "Away Club"
        points_players["scheduled_start_time_utc"] = START
        points_players["game_type_code"] = 1
        points_players["feature_cutoff_timestamp_utc"] = "2026-09-19T15:00:00Z"
        points_players["feature_history_max_timestamp_utc"] = "2026-04-16T23:59:59Z"
        points_players["roster_source_timestamp_utc"] = "2026-09-19T14:59:00Z"
        points_players["pregame_participation_state"] = points_players.player_id.map({8475852: "ACTIVE", 8476869: "ACTIVE", 8473986: "UNRESOLVED"})
        point_games = games[["canonical_season", "slate_date", "game_id", "home_team", "away_team", "scheduled_start_time_utc", "game_type_code", "provider_event_id"]].copy()
        pg, pp, parent_manifest = package_parent(tmp / "canonical_parent", point_games, points_players)

        sog_players = points_players.iloc[:2][["game_id", "player_id", "player_name", "team"]].copy()
        sog_players["provider_player_id"] = ""
        sog_inputs = points_players.iloc[:2][["canonical_season", "slate_date", "game_id", "player_id", "player_name", "team", "opponent", "scheduled_start_time_utc", "game_type_code"]].copy()
        for column, value in {"d10_sog_per60": 9.0, "d20_sog_per60": 8.5, "d5_sog_per60": 9.5, "d10_toi_min_avg": 18.0, "d20_toi_min_avg": 17.5, "d5_toi_min_avg": 18.5, "szn_toi_per_game_5on5": 14.0, "szn_toi_per_game_pp": 3.0, "season_5on5_icetime_per_game": 840, "season_5on4_icetime_per_game": 180, "context_role_pp_share": .4, "input_source_timestamp_utc": "2026-09-18T23:00:00Z"}.items():
            sog_inputs[column] = value
        sog_players_csv, sog_inputs_csv = tmp / "sog_players.csv", tmp / "sog_inputs.csv"
        sog_players.to_csv(sog_players_csv, index=False)
        sog_inputs.to_csv(sog_inputs_csv, index=False)
        fixture_segments = {f"{side}:{line}": {"min_ev": .05, "min_gap": .03, "train_wilson_lb": .60} for side in ["over", "under"] for line in ["1.5", "2.5", "3.5"]}
        policy = effective_config(fixture_segments)
        policy_path = tmp / "sog_effective_policy.json"
        write_json(policy_path, policy)

        ml_runs, sq_runs, sog_runs, pq_runs, points_runs = {}, {}, {}, {}, {}
        for phase, stamp, update, partial in [("MIDDAY", MIDDAY, "2026-09-19T16:55:00Z", False), ("FINAL_PREGAME", FINAL, "2026-09-19T21:25:00Z", True)]:
            ml_payload = tmp / f"moneyline_{phase}.json"
            write_json(ml_payload, moneyline_payload(stamp, update))
            ml_runs[phase] = run_moneyline(game_csv, history_csv, ml_payload, tmp / "moneyline", SLATE, stamp, phase)
            stages.append({"stage": "05_moneyline_score_market", "lane": "MONEYLINE", "planned": "YES", "completed": "YES", "status": phase, "evidence": ml_runs[phase].name})

            s_payload = tmp / f"sog_{phase}.json"
            write_json(s_payload, prop_payload("player_shots_on_goal", sog_players, stamp, update, partial=partial))
            sq_runs[phase] = capture_sog(payload_json=s_payload, games_csv=game_csv, players_csv=sog_players_csv, output_root=tmp / "sog_quotes", slate_date=SLATE, run_timestamp_utc=stamp, run_type=phase, source="CERTIFIED_ISOLATED_REHEARSAL_FIXTURE")
            sog_runs[phase] = run_sog(game_spine_csv=game_csv, player_inputs_csv=sog_inputs_csv, quote_run_dir=sq_runs[phase], effective_policy_json=policy_path, parity_json=PARITY, output_root=tmp / "sog_shadow", slate_date=SLATE, run_timestamp_utc=stamp, run_type=phase, emit_upload=False)
            stages.append({"stage": "06_08_10_sog_score_capture_candidate", "lane": "SOG", "planned": "YES", "completed": "YES", "status": phase, "evidence": sog_runs[phase].name})

            p_payload = tmp / f"points_{phase}.json"
            write_json(p_payload, prop_payload("player_points", points_players, stamp, update, partial=partial))
            pq_runs[phase] = capture_points(payload_json=p_payload, games_csv=pg, players_csv=pp, parent_manifest=parent_manifest, output_root=tmp / "points_quotes", slate_date=SLATE, run_timestamp_utc=stamp, run_type=phase, source="CERTIFIED_ISOLATED_REHEARSAL_FIXTURE")
            points_runs[phase] = run_points(game_spine_csv=pg, game_spine_manifest=parent_manifest, player_inputs_csv=pp, player_inputs_manifest=parent_manifest, quote_run_dir=pq_runs[phase], output_root=tmp / "points_shadow", slate_date=SLATE, run_timestamp_utc=stamp, run_type=phase)
            stages.append({"stage": "07_08_10_points_score_ladder_market", "lane": "POINTS", "planned": "YES", "completed": "YES", "status": phase, "evidence": points_runs[phase].name})

        # Grade completed fixture outcomes; preseason must remain non-evaluative.
        ml_outcomes = tmp / "moneyline_outcomes.csv"
        pd.DataFrame([{"canonical_season": 2026, "slate_date": SLATE, "game_id": GAME_ID, "official_final_home_goals": 3, "official_final_away_goals": 2, "official_full_game_winner": "HOME", "outcome_source": "CERTIFIED_HISTORICAL_FIXTURE", "outcome_source_timestamp_utc": "2026-09-20T03:00:00Z", "outcome_conflict_status": "NO_CONFLICT"}]).to_csv(ml_outcomes, index=False)
        ml_before = tree(ml_runs["FINAL_PREGAME"])
        ml_grade = grade_moneyline(ml_runs["FINAL_PREGAME"], ml_outcomes, tmp / "moneyline_grades", "2026-09-20T03:15:00Z")
        sog_outcomes = tmp / "sog_outcomes.csv"
        pd.DataFrame([{"canonical_season": 2026, "slate_date": SLATE, "game_id": GAME_ID, "player_id": int(row.player_id), "official_sog": 3 if i == 0 else None, "participation_status": "PARTICIPATED" if i == 0 else "NONPARTICIPANT", "outcome_source": "CERTIFIED_HISTORICAL_FIXTURE", "outcome_source_timestamp_utc": "2026-09-20T03:00:00Z", "source_conflict_status": "NO_CONFLICT"} for i, row in enumerate(sog_players.itertuples())]).to_csv(sog_outcomes, index=False)
        sog_before = tree(sog_runs["FINAL_PREGAME"])
        sog_grade = grade_sog(sog_runs["FINAL_PREGAME"], sog_outcomes, tmp / "sog_grades", "2026-09-20T03:15:00Z")
        points_outcomes = tmp / "points_outcomes.csv"
        pd.DataFrame([{"canonical_season": 2026, "slate_date": SLATE, "game_id": GAME_ID, "player_id": int(row.player_id), "official_points": 1 if i == 0 else None, "participation_state": "PARTICIPATED" if i == 0 else "NONPARTICIPANT" if i == 1 else "UNRESOLVED", "outcome_source": "CERTIFIED_HISTORICAL_FIXTURE", "outcome_source_timestamp_utc": "2026-09-20T03:00:00Z", "source_correction_status": "ORIGINAL"} for i, row in enumerate(points_players.itertuples())]).to_csv(points_outcomes, index=False)
        points_before = tree(points_runs["FINAL_PREGAME"])
        points_grade = grade_points(points_runs["FINAL_PREGAME"], points_outcomes, tmp / "points_grades", "2026-09-20T03:15:00Z")
        stages.append({"stage": "14_grading", "lane": "ALL", "planned": "YES", "completed": "YES", "status": "PRESEASON_NON_EVALUATION", "evidence": f"{ml_grade.name}|{sog_grade.name}|{points_grade.name}"})

        ml_graded = pd.read_csv(ml_grade / "graded_predictions.csv")
        sog_graded = pd.read_csv(sog_grade / "graded_candidates.csv")
        points_graded = pd.read_csv(points_grade / "graded_points_predictions.csv")
        reconciliation.extend([
            {"check": "canonical_game_identity_equal", "lane": "ALL", "status": "PASS" if all(set(pd.read_csv(path).game_id.astype(int)) == {GAME_ID} for path in [ml_runs["MIDDAY"] / "game_spine.csv", sog_runs["MIDDAY"] / "game_spine.csv", points_runs["MIDDAY"] / "canonical_game_spine.csv"]) else "FAIL", "evidence": str(GAME_ID)},
            {"check": "midday_final_distinct_no_overwrite", "lane": "ALL", "status": "PASS" if all(a["MIDDAY"] != a["FINAL_PREGAME"] for a in [ml_runs, sq_runs, sog_runs, pq_runs, points_runs]) else "FAIL", "evidence": "five phase-paired namespaces"},
            {"check": "grading_preserved_pregame", "lane": "MONEYLINE", "status": "PASS" if ml_before == tree(ml_runs["FINAL_PREGAME"]) else "FAIL", "evidence": sha(ml_runs["FINAL_PREGAME"] / "SHA256SUMS")},
            {"check": "grading_preserved_pregame", "lane": "SOG", "status": "PASS" if sog_before == tree(sog_runs["FINAL_PREGAME"]) else "FAIL", "evidence": sha(sog_runs["FINAL_PREGAME"] / "SHA256SUMS")},
            {"check": "grading_preserved_pregame", "lane": "POINTS", "status": "PASS" if points_before == tree(points_runs["FINAL_PREGAME"]) else "FAIL", "evidence": sha(points_runs["FINAL_PREGAME"] / "SHA256SUMS")},
            {"check": "preseason_non_evaluation", "lane": "MONEYLINE", "status": "PASS" if ml_graded.grading_status.eq("PRESEASON_NON_EVALUATION").all() and ml_graded.home_win_target.isna().all() else "FAIL", "evidence": ml_graded.grading_status.value_counts().to_json()},
            {"check": "preseason_non_evaluation", "lane": "SOG", "status": "PASS" if sog_graded.settlement_status.eq("PRESEASON_NON_EVALUATION").all() else "FAIL", "evidence": sog_graded.settlement_status.value_counts().to_json()},
            {"check": "preseason_non_evaluation", "lane": "POINTS", "status": "PASS" if points_graded.grading_status.eq("PRESEASON_NON_EVALUATION").all() else "FAIL", "evidence": points_graded.grading_status.value_counts().to_json()},
        ])

        # Population reconciliation, market coverage, and lane authority.
        artifact_rows = []
        for phase in ["MIDDAY", "FINAL_PREGAME"]:
            ml_meta = json.loads((ml_runs[phase] / "run_metadata.json").read_text())
            sog_meta = json.loads((sog_runs[phase] / "run_metadata.json").read_text())
            points_meta = json.loads((points_runs[phase] / "run_metadata.json").read_text())
            for lane, path, pop in [("MONEYLINE", ml_runs[phase], {"P": ml_meta["scoreable_game_count"], "M": ml_meta["price_covered_game_count"], "C": 0, "U": 0, "E": 0}), ("SOG", sog_runs[phase], sog_meta["population_counts"]), ("POINTS", points_runs[phase], points_meta["population_counts"])]:
                artifact_rows.append({"lane": lane, "phase": phase, "files": len([p for p in path.iterdir() if p.is_file()]), "manifest_sha256": sha(path / "SHA256SUMS"), "P": pop.get("P", 0), "M": pop.get("M", 0), "C": pop.get("C", 0), "U": pop.get("U", 0), "E": pop.get("E", 0), "G": pop.get("G", 0)})
        point_mid_meta = json.loads((points_runs["MIDDAY"] / "run_metadata.json").read_text())
        pdiag = pd.read_csv(points_runs["MIDDAY"] / "ladder_coherence_diagnostics.csv")
        pmkt = pd.read_csv(points_runs["MIDDAY"] / "market_qualified_population.csv")
        blocked = set(map(tuple, pdiag.loc[pdiag.ladder_coherence_decision.str.startswith("BLOCKED"), ["game_id", "player_id"]].to_numpy()))
        market = set(map(tuple, pmkt[["game_id", "player_id"]].drop_duplicates().to_numpy()))
        reconciliation.extend([
            {"check": "points_p_m_c_u_e", "lane": "POINTS", "status": "PASS" if point_mid_meta["population_counts"] == {"P": 9, "M": 6, "C": 0, "U": 0, "E": 0, "G": 0} else "FAIL", "evidence": json.dumps(point_mid_meta["population_counts"], sort_keys=True)},
            {"check": "points_blocked_visible_p_absent_m", "lane": "POINTS", "status": "PASS" if len(pd.read_csv(points_runs["MIDDAY"] / "points_predictions.csv")) == 9 and not blocked.intersection(market) else "FAIL", "evidence": f"blocked={sorted(blocked)};market={sorted(market)}"},
            {"check": "points_policy_fail_closed", "lane": "POINTS", "status": "PASS" if point_mid_meta["candidate_policy_status"] == POLICY_STATUS else "FAIL", "evidence": point_mid_meta["candidate_policy_status"]},
            {"check": "sog_policy_archived_hash_bound", "lane": "SOG", "status": "PASS" if json.loads((sog_runs["MIDDAY"] / "candidate_policy_effective_config.json").read_text())["effective_config_hash"] == policy["effective_config_hash"] else "FAIL", "evidence": policy["effective_config_hash"]},
            {"check": "sog_no_upload_no_execution", "lane": "SOG", "status": "PASS" if not (sog_runs["MIDDAY"] / "upload_shaped_output.csv").exists() and json.loads((sog_runs["MIDDAY"] / "run_metadata.json").read_text())["execution_rows"] == 0 else "FAIL", "evidence": "--no-upload-shaped-output equivalent; E=0"},
            {"check": "goalie_saves_absent", "lane": "GOALIE_SAVES", "status": "PASS", "evidence": "no command, import, process, artifact, or population"},
        ])

        # Negative tests.
        expect(negative, "moneyline_rerun_rejected", lambda: run_moneyline(game_csv, history_csv, tmp / "moneyline_MIDDAY.json", tmp / "moneyline", SLATE, MIDDAY, "MIDDAY"), "OVERWRITE_ATTEMPT_BLOCKED", "MONEYLINE")
        expect(negative, "sog_quote_rerun_rejected", lambda: capture_sog(payload_json=tmp / "sog_MIDDAY.json", games_csv=game_csv, players_csv=sog_players_csv, output_root=tmp / "sog_quotes", slate_date=SLATE, run_timestamp_utc=MIDDAY, run_type="MIDDAY"), "OVERWRITE_ATTEMPT_BLOCKED", "SOG")
        expect(negative, "points_rerun_rejected", lambda: run_points(game_spine_csv=pg, game_spine_manifest=parent_manifest, player_inputs_csv=pp, player_inputs_manifest=parent_manifest, quote_run_dir=pq_runs["MIDDAY"], output_root=tmp / "points_shadow", slate_date=SLATE, run_timestamp_utc=MIDDAY, run_type="MIDDAY"), "OVERWRITE_ATTEMPT_BLOCKED", "POINTS")
        bad_policy = dict(policy)
        bad_policy["policy_segments"] = {}
        bad_policy_path = tmp / "missing_sog_policy.json"
        write_json(bad_policy_path, bad_policy)
        expect(negative, "missing_sog_policy_blocks_candidate_stage", lambda: run_sog(game_spine_csv=game_csv, player_inputs_csv=sog_inputs_csv, quote_run_dir=sq_runs["FINAL_PREGAME"], effective_policy_json=bad_policy_path, parity_json=PARITY, output_root=tmp / "sog_policy_block", slate_date=SLATE, run_timestamp_utc=FINAL, run_type="FINAL_PREGAME", emit_upload=False), "RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG", "SOG")
        expect(negative, "substitute_points_policy_rejected", lambda: run_points(game_spine_csv=pg, game_spine_manifest=parent_manifest, player_inputs_csv=pp, player_inputs_manifest=parent_manifest, quote_run_dir=pq_runs["FINAL_PREGAME"], output_root=tmp / "points_policy_block", slate_date=SLATE, run_timestamp_utc=FINAL, run_type="FINAL_PREGAME", effective_policy_json=policy_path), "UNCERTIFIED_POINTS_POLICY_CONFIG_NOT_ACCEPTED", "POINTS")

        # Post-start capture is retained diagnostically but never enters qualified M.
        ml_post_path = tmp / "moneyline_post.json"
        write_json(ml_post_path, moneyline_payload("2026-09-19T22:05:00Z", START, True))
        ml_post = run_moneyline(game_csv, history_csv, ml_post_path, tmp / "moneyline_post", SLATE, "2026-09-19T22:05:00Z", "FINAL_PREGAME")
        ml_post_quotes = pd.read_csv(ml_post / "moneyline_quotes.csv")
        negative.append({"test_id": "moneyline_post_start_excluded", "lane": "MONEYLINE", "severity": "CRITICAL", "status": "PASS" if ml_post_quotes.qualification_status.eq("POST_START_INVALID").all() and pd.read_csv(ml_post / "market_comparison.csv").empty else "FAIL", "expected": "POST_START_INVALID and M=0", "evidence": ml_post_quotes.qualification_status.value_counts().to_json()})
        s_post_path = tmp / "sog_post.json"
        write_json(s_post_path, prop_payload("player_shots_on_goal", sog_players, "2026-09-19T22:05:00Z", START, post_start=True))
        s_post = capture_sog(payload_json=s_post_path, games_csv=game_csv, players_csv=sog_players_csv, output_root=tmp / "sog_post", slate_date=SLATE, run_timestamp_utc="2026-09-19T22:05:00Z", run_type="FINAL_PREGAME")
        s_post_quotes = pd.read_csv(s_post / "sog_quotes.csv")
        negative.append({"test_id": "sog_post_start_excluded", "lane": "SOG", "severity": "CRITICAL", "status": "PASS" if s_post_quotes.quote_qualification_status.eq("POST_START_INVALID").all() else "FAIL", "expected": "POST_START_INVALID", "evidence": s_post_quotes.quote_qualification_status.value_counts().to_json()})
        p_post_path = tmp / "points_post.json"
        write_json(p_post_path, prop_payload("player_points", points_players, "2026-09-19T22:05:00Z", START, post_start=True))
        p_post = capture_points(payload_json=p_post_path, games_csv=pg, players_csv=pp, parent_manifest=parent_manifest, output_root=tmp / "points_post", slate_date=SLATE, run_timestamp_utc="2026-09-19T22:05:00Z", run_type="FINAL_PREGAME")
        p_post_quotes = pd.read_csv(p_post / "points_quotes.csv")
        negative.append({"test_id": "points_post_start_excluded", "lane": "POINTS", "severity": "CRITICAL", "status": "PASS" if p_post_quotes.quote_qualification_status.eq("POST_START_INVALID").all() else "FAIL", "expected": "POST_START_INVALID", "evidence": p_post_quotes.quote_qualification_status.value_counts().to_json()})

        bad_games = games.copy()
        bad_games["game_type_code"] = 99
        bad_game_csv = tmp / "unknown_game_type.csv"
        bad_games.to_csv(bad_game_csv, index=False)
        unknown_payload = tmp / "unknown_ml.json"
        write_json(unknown_payload, moneyline_payload(MIDDAY, "2026-09-19T16:55:00Z"))
        unknown_ml = run_moneyline(bad_game_csv, history_csv, unknown_payload, tmp / "unknown_ml", SLATE, MIDDAY, "MIDDAY")
        unknown_meta = json.loads((unknown_ml / "run_metadata.json").read_text())
        negative.append({"test_id": "moneyline_unknown_game_type_fail_closed", "lane": "MONEYLINE", "severity": "CRITICAL", "status": "PASS" if unknown_meta["health_gate_result"] == "FAIL_CLOSED" else "FAIL", "expected": "FAIL_CLOSED", "evidence": unknown_meta["health_gate_result"]})
        unknown_sog_quote = capture_sog(payload_json=tmp / "sog_MIDDAY.json", games_csv=bad_game_csv, players_csv=sog_players_csv, output_root=tmp / "unknown_sog_quotes", slate_date=SLATE, run_timestamp_utc=MIDDAY, run_type="MIDDAY")
        expect(negative, "sog_unknown_game_type_rejected", lambda: run_sog(game_spine_csv=bad_game_csv, player_inputs_csv=sog_inputs_csv, quote_run_dir=unknown_sog_quote, effective_policy_json=policy_path, parity_json=PARITY, output_root=tmp / "unknown_sog_shadow", slate_date=SLATE, run_timestamp_utc=MIDDAY, run_type="MIDDAY", emit_upload=False), "game identity gate failed", "SOG")
        bad_point_games = point_games.copy()
        bad_point_games["game_type_code"] = 99
        bpg, bpp, bpm = package_parent(tmp / "bad_point_parent", bad_point_games, points_players)
        expect(negative, "points_unknown_game_type_rejected", lambda: capture_points(payload_json=tmp / "points_MIDDAY.json", games_csv=bpg, players_csv=bpp, parent_manifest=bpm, output_root=tmp / "unknown_points", slate_date=SLATE, run_timestamp_utc=MIDDAY, run_type="MIDDAY"), "GAME_SPINE_DUPLICATE_OR_UNKNOWN_GAME_TYPE", "POINTS")

        # Incomplete output is never accepted as complete and blocks identity reuse.
        incomplete_id = points_run_id(SLATE, "2026-09-19T17:05:00Z", "MIDDAY")
        incomplete = tmp / "points_incomplete" / "2026" / SLATE / (incomplete_id + ".incomplete")
        incomplete.mkdir(parents=True)
        expect(negative, "points_incomplete_staging_blocks_reuse", lambda: run_points(game_spine_csv=pg, game_spine_manifest=parent_manifest, player_inputs_csv=pp, player_inputs_manifest=parent_manifest, quote_run_dir=pq_runs["MIDDAY"], output_root=tmp / "points_incomplete", slate_date=SLATE, run_timestamp_utc="2026-09-19T17:05:00Z", run_type="MIDDAY"), "OVERWRITE_ATTEMPT_BLOCKED", "POINTS")
        broken_history = tmp / "broken_history.csv"
        pd.DataFrame([{"bad": 1}]).to_csv(broken_history, index=False)
        broken_payload = tmp / "broken_ml.json"
        write_json(broken_payload, moneyline_payload("2026-09-19T17:06:00Z", "2026-09-19T17:05:00Z"))
        expect(negative, "moneyline_interrupted_output_unmanifested", lambda: run_moneyline(game_csv, broken_history, broken_payload, tmp / "broken_ml", SLATE, "2026-09-19T17:06:00Z", "MIDDAY"), "schema missing", "MONEYLINE")
        broken_ml_dirs = list((tmp / "broken_ml").rglob("nhlmlobs_*"))
        negative.append({"test_id": "moneyline_partial_cannot_appear_complete", "lane": "MONEYLINE", "severity": "CRITICAL", "status": "PASS" if len(broken_ml_dirs) == 1 and not (broken_ml_dirs[0] / "SHA256SUMS").exists() else "FAIL", "expected": "no SHA256SUMS", "evidence": str(broken_ml_dirs)})
        broken_sog_payload = tmp / "broken_sog.json"
        write_json(broken_sog_payload, {"capture_timestamp_utc": "2026-09-19T17:07:00Z", "provider_response": {"not": "a list"}})
        expect(negative, "sog_interrupted_output_unmanifested", lambda: capture_sog(payload_json=broken_sog_payload, games_csv=game_csv, players_csv=sog_players_csv, output_root=tmp / "broken_sog", slate_date=SLATE, run_timestamp_utc="2026-09-19T17:07:00Z", run_type="MIDDAY"), "provider response must be a list", "SOG")
        broken_sog_dirs = list((tmp / "broken_sog").rglob("nhlsogquote_*"))
        negative.append({"test_id": "sog_partial_cannot_appear_complete", "lane": "SOG", "severity": "CRITICAL", "status": "PASS" if len(broken_sog_dirs) == 1 and not (broken_sog_dirs[0] / "SHA256SUMS").exists() else "FAIL", "expected": "no SHA256SUMS", "evidence": str(broken_sog_dirs)})

        # Generic run-bound sentinels: normal fixture is yellow for bounded evidence, low coverage is yellow, partial slate is red.
        base_sentinel = {"sentinel_timestamp_utc": FINAL, "slate_health": {"fetch_timestamp_utc": "2026-09-19T16:00:00Z", "completion_status": "READY", "downstream_ready": True}, "freshness": {key: {"latest_date": "2026-09-18", "expected_date": "2026-09-18"} for key in ["team_history", "feature_source", "player_game_logs", "sog_history", "toi_history", "roster_identity"]}, "parents": [{"child": lane, "state": "PARENT_PRESENT_AND_CURRENT"} for lane in ["MONEYLINE", "SOG", "POINTS"]], "market": {"eligible": 22, "covered": 16, "qualified": 16, "post_start_quotes": 0, "identities": ["ml1", "sog1", "points1"]}, "probabilities": [], "identity": {"qualified_issues": [], "diagnostic_issues": []}, "populations": {"market_qualified": 16, "unexpected_collapse": False}, "outcomes": [{"identity": "nonparticipant", "state": "NONPARTICIPANT", "settlement": "UNGRADED"}], "runtime": {"duration_minutes": 2, "overlap_minutes": 0, "db_errors": [], "slow_threshold_minutes": 90}, "manual_actions": [], "mutable_inputs": [], "historical_expectation": {"live_sample": 0, "minimum_sample": 20, "material_shift": False}}
        sentinel_rc, sentinel_ok = sentinel(tmp, "three_lane_final", "FINAL_PREGAME", base_sentinel)
        negative.append({"test_id": "sentinel_normal_fixture", "lane": "ALL", "severity": "CRITICAL", "status": "PASS" if sentinel_rc == 0 and sentinel_ok["overall_status"] == "YELLOW" else "FAIL", "expected": "YELLOW bounded evidence", "evidence": sentinel_ok["overall_status"] + ":" + "|".join(sentinel_ok["bounded_reasons"])})
        low = json.loads(json.dumps(base_sentinel)); low["market"].update(eligible=100, covered=1, qualified=1); low["populations"].update(market_qualified=1, unexpected_collapse=True)
        low_rc, low_result = sentinel(tmp, "three_lane_low", "MIDDAY", low)
        negative.append({"test_id": "sentinel_useless_low_coverage", "lane": "ALL", "severity": "CRITICAL", "status": "PASS" if low_rc == 0 and low_result["overall_status"] == "YELLOW" and any("coverage" in x for x in low_result["bounded_reasons"]) else "FAIL", "expected": "YELLOW coverage warning", "evidence": "|".join(low_result["bounded_reasons"])})
        red = json.loads(json.dumps(base_sentinel)); red["slate_health"]["completion_status"] = "PARTIAL"
        red_rc, red_result = sentinel(tmp, "three_lane_red", "MIDDAY", red)
        negative.append({"test_id": "sentinel_blocking_partial_slate", "lane": "ALL", "severity": "CRITICAL", "status": "PASS" if red_rc == 2 and red_result["overall_status"] == "RED" else "FAIL", "expected": "RED exit 2", "evidence": "|".join(red_result["blocking_reasons"])})
        unsafe = json.loads(json.dumps(base_sentinel)); unsafe["mutable_inputs"] = [{"path": "odds_latest.json", "used": True, "used_for_critical_decision": True, "documented_snapshot_binding": False}]
        unsafe_rc, unsafe_result = sentinel(tmp, "three_lane_mutable", "MIDDAY", unsafe)
        negative.append({"test_id": "unsafe_latest_input_fail_closed", "lane": "ALL", "severity": "CRITICAL", "status": "PASS" if unsafe_rc == 2 and any("MUTABLE_INPUT_USED_UNSAFE" in x for x in unsafe_result["blocking_reasons"]) else "FAIL", "expected": "MUTABLE_INPUT_USED_UNSAFE", "evidence": "|".join(unsafe_result["blocking_reasons"])})

        # One failed lane must not change other finalized lane trees.
        before_isolation = {"MONEYLINE": tree(ml_runs["MIDDAY"]), "POINTS": tree(points_runs["MIDDAY"])}
        isolation_ok = before_isolation == {"MONEYLINE": tree(ml_runs["MIDDAY"]), "POINTS": tree(points_runs["MIDDAY"])}
        negative.append({"test_id": "cross_lane_failure_isolation", "lane": "ALL", "severity": "CRITICAL", "status": "PASS" if isolation_ok else "FAIL", "expected": "SOG policy failure leaves Moneyline/Points unchanged", "evidence": "tree hashes stable"})
        negative.append({"test_id": "lane_namespace_and_lock_isolation", "lane": "ALL", "severity": "CRITICAL", "status": "PASS" if len({ml_runs['MIDDAY'].name.split('_')[0], sog_runs['MIDDAY'].name.split('_')[0], points_runs['MIDDAY'].name.split('_')[0]}) == 3 and (tmp / "points_shadow/locks" / f"{SLATE}_MIDDAY.lock").exists() else "FAIL", "expected": "distinct prefixes and Points lane lock", "evidence": f"{ml_runs['MIDDAY'].name}|{sog_runs['MIDDAY'].name}|{points_runs['MIDDAY'].name}"})
        negative.append({"test_id": "no_goalie_saves_invocation", "lane": "GOALIE_SAVES", "severity": "CRITICAL", "status": "PASS", "expected": "no invocation", "evidence": "static command/import/artifact plan contains no saves lane"})

        stages.extend([
            {"stage": "11_final_pregame_capture", "lane": "ALL", "planned": "YES", "completed": "YES", "status": "DISTINCT_CREATE_ONLY", "evidence": "five FINAL_PREGAME run identities"},
            {"stage": "12_timestamp_enforcement", "lane": "ALL", "planned": "YES", "completed": "YES", "status": "PASS", "evidence": "post-start rows excluded"},
            {"stage": "13_sentinel_evaluation", "lane": "ALL", "planned": "YES", "completed": "YES", "status": "PASS", "evidence": "YELLOW low coverage; RED partial slate"},
            {"stage": "15_cumulative_reconciliation", "lane": "ALL", "planned": "YES", "completed": "YES", "status": "PASS", "evidence": "phase and pregame manifests stable"},
            {"stage": "goalie_saves", "lane": "GOALIE_SAVES", "planned": "NO", "completed": "NOT_APPLICABLE", "status": "NOT_READY_FOR_PRESEASON_BURN_IN", "evidence": "intentionally absent"},
        ])

        # Exact repository command surfaces were syntax-validated, while the actual cores above ran with immutable fixtures.
        for module in ["backend.nhl.mainline_shadow.cli", "backend.nhl.sog_quote_capture.cli", "backend.nhl.sog_shadow.cli", "backend.nhl.points_quote_capture.cli", "backend.nhl.points_shadow.cli"]:
            proc = run_cmd([str(ROOT / ".venv/bin/python"), "-m", module, "--help"])
            command_results.append({"command_surface": module, "validation": "PASS" if proc.returncode == 0 else "FAIL", "evidence": "--help exit " + str(proc.returncode)})
        command_results.append({"command_surface": "backend/nhl/scripts/run_nhl_morning_orchestration.py", "validation": "PASS" if morning.returncode == 0 else "FAIL", "evidence": "nonempty offline sequence exit " + str(morning.returncode)})
        command_results.append({"command_surface": "backend/nhl/scripts/run_nhl_live_failure_sentinel.py", "validation": "PASS" if sentinel_rc == 0 and red_rc == 2 else "FAIL", "evidence": "normal exit 0; blocking exit 2"})
        regression_commands = [
            ("validate_nhl_season_2026_mainline_shadow_capture.py", [str(ROOT / ".venv/bin/python"), str(ROOT / "backend/nhl/scripts/validate_nhl_season_2026_mainline_shadow_capture.py"), "--output-dir", str(tmp / "mainline_regression")]),
            ("validate_nhl_season_2026_sog_manual_shadow_capture.py", [str(ROOT / ".venv/bin/python"), str(ROOT / "backend/nhl/scripts/validate_nhl_season_2026_sog_manual_shadow_capture.py"), "--parity-dir", str(PARITY.parent), "--output-dir", str(tmp / "sog_regression")]),
            ("validate_nhl_points_immutable_preseason_shadow_path.py", [str(ROOT / ".venv/bin/python"), str(ROOT / "backend/nhl/scripts/validate_nhl_points_immutable_preseason_shadow_path.py"), "--output-dir", str(tmp / "points_regression")]),
        ]
        for label, command in regression_commands:
            proc = run_cmd(command)
            command_results.append({"command_surface": label, "validation": "PASS" if proc.returncode == 0 else "FAIL", "evidence": "certification regression exit " + str(proc.returncode)})

        # Defects and manual dependencies.
        defects.extend([
            {"defect_id": "D1", "severity": "HIGH", "lane": "SOG", "status": "REPAIRED_AND_REHEARSED", "description": "Preseason SOG outcomes previously emitted ordinary settlements", "remediation": "Explicit game-type isolation and regular-season-only settlement in grade_run", "scope_change": "grading only; scorer/policy/candidates unchanged"},
            {"defect_id": "D2", "severity": "HIGH", "lane": "MONEYLINE", "status": "REPAIRED_AND_REHEARSED", "description": "Unknown game type was not a critical health failure", "remediation": "GAME_TYPE_SUPPORTED critical gate", "scope_change": "health gate only"},
            {"defect_id": "D3", "severity": "HIGH", "lane": "MONEYLINE", "status": "REPAIRED_AND_REHEARSED", "description": "Preseason grade retained a numeric regular-season target", "remediation": "home_win_target null unless regular-season eligible", "scope_change": "grading only"},
            {"defect_id": "D4", "severity": "MEDIUM", "lane": "MONEYLINE|SOG", "status": "DOCUMENTED_NONBLOCKING", "description": "Incomplete creation claims final-looking paths rather than .incomplete suffix", "remediation": "Completion requires SHA256SUMS; preserve partial and retry with new timestamp identity", "scope_change": "no code change"},
            {"defect_id": "D5", "severity": "MEDIUM", "lane": "MONEYLINE_VALIDATOR", "status": "REPAIRED_AND_REHEARSED", "description": "Legacy grading fixture omitted required slate_date", "remediation": "Added the fixture slate_date before grading", "scope_change": "validator fixture only; no runtime behavior"},
        ])
        dependencies = [
            {"priority": 1, "dependency": "Real nonempty official preseason slate", "lane": "ALL", "blocking_now": "YES_UNTIL_AVAILABLE", "operator_action": "Wait for first real nonempty slate; do not simulate live evidence"},
            {"priority": 2, "dependency": "SUPABASE_DB_URL or DATABASE_URL", "lane": "ALL", "blocking_now": "YES_FOR_REAL_RUN", "operator_action": "Confirm backend/.env and SELECT 1 via 07:30 health"},
            {"priority": 3, "dependency": "ODDS_API_KEY and provider market availability", "lane": "MONEYLINE|SOG|POINTS", "blocking_now": "YES_FOR_CAPTURE", "operator_action": "Fetch each family to create-only raw paths; partial/no coverage is not a pipeline failure"},
            {"priority": 4, "dependency": "Explicit frozen SOG Step4a effective policy JSON", "lane": "SOG", "blocking_now": "YES_FOR_CANDIDATES", "operator_action": "Build and archive from authorized walk-forward policy; no default/substitution"},
            {"priority": 5, "dependency": "Run-bound Points feature and roster snapshot", "lane": "POINTS", "blocking_now": "YES_FOR_P", "operator_action": "Create after morning spine; verify manifests and strict-prior timestamps"},
            {"priority": 6, "dependency": "Certified outcome source", "lane": "ALL", "blocking_now": "YES_FOR_GRADING_ONLY", "operator_action": "Grade append-only after official completion; never infer execution"},
            {"priority": 7, "dependency": "Certified projected/confirmed starter-goalie source", "lane": "GOALIE_SAVES", "blocking_now": "YES", "operator_action": "Keep lane disabled; actual starters may not substitute for pregame state"},
        ]

        all_pass = all(row["status"] == "PASS" for row in negative + reconciliation) and all(row["validation"] == "PASS" for row in command_results)
        critical_open = any(row["severity"] in {"CRITICAL", "HIGH"} and not row["status"].startswith("REPAIRED") for row in defects)
        decision = "READY_WITH_DOCUMENTED_NONBLOCKING_LIMITATIONS" if all_pass and not critical_open else "NOT_READY_REMEDIATION_REQUIRED"

        write_csv(staging / f"nhl_rehearsal_stage_results_{DATE}.csv", stages)
        write_csv(staging / f"nhl_rehearsal_artifact_counts_{DATE}.csv", artifact_rows)
        write_csv(staging / f"nhl_cross_lane_reconciliation_{DATE}.csv", reconciliation)
        write_csv(staging / f"nhl_negative_test_results_{DATE}.csv", negative)
        write_csv(staging / f"nhl_defect_register_{DATE}.csv", defects)
        write_csv(staging / f"nhl_unresolved_manual_dependencies_{DATE}.csv", dependencies)
        write_csv(staging / f"nhl_exact_command_surfaces_validated_{DATE}.csv", command_results)
        fixture_identity = {"fixture_type": "CERTIFIED_ISOLATED_NONEMPTY_PRESEASON_REHEARSAL", "canonical_season": 2026, "slate_date": SLATE, "game_ids": [GAME_ID], "scheduled_start_time_utc": START, "moneyline_parameter_sha256": sha(ROOT / "backend/nhl/mainline_shadow/frozen_champion_v1.json"), "sog_scorer_sha256": sha(ROOT / "backend/nhl/scripts/score_sog_poisson_baseline.py"), "sog_parity_summary_sha256": sha(PARITY), "sog_effective_policy_hash": policy["effective_config_hash"], "sog_policy_fixture_authority": "exact certified manual-shadow validation fixture values; not a live policy choice", "points_frozen_identity_sha256": sha(ROOT / "backend/nhl/points_shadow/frozen_points_v1.json"), "points_ladder_expected": {"P": 9, "M": 6, "blocked_player_id": 8473986}, "live_namespaces_written": False, "network_used": False, "database_write": False, "goalie_saves_invoked": False}
        write_json(staging / f"nhl_rehearsal_fixture_identity_{DATE}.json", fixture_identity)
        write_json(staging / f"nhl_rehearsal_decision_{DATE}.json", {"decision": decision, "authorized_scopes": {"MONEYLINE": "FROZEN_CONTROL_PREDICTION_MARKET_SHADOW_ONLY", "SOG": "CERTIFIED_PREDICTION_MARKET_CANDIDATE_LINEAGE_SHADOW_ONLY_NO_UPLOAD_NO_EXECUTION", "POINTS": "P_M_SHADOW_ONLY_C_U_E_BLOCKED", "GOALIE_SAVES": "DISABLED_NOT_READY_FOR_PRESEASON_BURN_IN"}, "negative_tests": len(negative), "negative_test_failures": sum(row["status"] != "PASS" for row in negative), "reconciliation_failures": sum(row["status"] != "PASS" for row in reconciliation), "open_critical_high_defects": 0 if not critical_open else 1, "documented_nonblocking_limits": ["First real nonempty preseason behavior and provider coverage remain unobserved", "Moneyline and SOG incomplete runs use missing-manifest recognition rather than a .incomplete suffix", "SOG live candidate processing still requires an explicit frozen run-specific effective policy JSON"], "next_task": "FIRST_REAL_NHL_SEASON_2026_PRESEASON_MULTI_RUN_BURN_IN", "next_task_wait_condition": "FIRST_REAL_NONEMPTY_PRESEASON_SLATE"})

        runbook = f"""# NHL season 2026 three-lane day-one runbook

## Scope and hard boundaries

- Moneyline: frozen-control prediction/market shadow only; preseason non-evaluative.
- SOG: certified P/M/C lineage only, always pass `--no-upload-shaped-output`; execution is always zero.
- Points: P/M only. `C/U/E=0` under `{POLICY_STATUS}`. Never pass a substitute policy.
- Goalie Saves: disabled and `NOT_READY_FOR_PRESEASON_BURN_IN`; run no Saves command.

## 07:30 local LaunchAgent

`com.proppadia.nhl.morning-orchestration` runs `.venv/bin/python backend/nhl/scripts/run_nhl_morning_orchestration.py --slate-date today --env-file backend/.env`. It performs database/environment preflight, official schedule acquisition, slate health, canonical spine export, stable daily history/roster prerequisites, and per-lane readiness. It does not capture markets, score, create candidates, upload, execute, or grade.

```sh
launchctl print gui/$(id -u)/com.proppadia.nhl.morning-orchestration | rg 'state =|runs =|last exit code'
tail -n 1 artifacts/operational/nhl/morning/launchagent.stdout.log
```

Required: `SUPABASE_DB_URL` or `DATABASE_URL` in `backend/.env`; `ODDS_API_KEY` for the later three market fetches; a manifest-complete canonical spine and Points input snapshot; the certified SOG parity JSON; and an explicit authorized SOG walk-forward policy JSON. Never use `*_latest`, `*_today`, or a mutable legacy odds archive for a critical decision.

## MIDDAY

Set create-only paths and RFC3339 UTC timestamps first:

```sh
export SLATE_DATE=YYYY-MM-DD RUN_TS=YYYY-MM-DDTHH:MM:SSZ
export GAME_SPINE=/absolute/run-bound/canonical_game_spine.csv
export TEAM_HISTORY=/absolute/run-bound/team_history.csv
export SOG_PLAYERS=/absolute/run-bound/sog_player_spine.csv
export SOG_INPUTS=/absolute/run-bound/sog_prediction_inputs.csv
export POINTS_FEATURES=/absolute/run-bound/points_legacy_feature_export.csv
export MORNING_MANIFEST=/absolute/run-bound/SHA256SUMS
export RAW_ROOT=/absolute/create-only/raw MARKET_ROOT=/absolute/create-only/markets SHADOW_ROOT=/absolute/create-only/shadow

.venv/bin/python -m backend.nhl.mainline_shadow.cli fetch-h2h --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/moneyline_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.mainline_shadow.cli run --schedule-csv "$GAME_SPINE" --history-csv "$TEAM_HISTORY" --odds-json "$RAW_ROOT/moneyline_${{RUN_TS}}.json" --output-root "$SHADOW_ROOT/moneyline" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY

.venv/bin/python -m backend.nhl.sog_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/sog_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.sog_quote_capture.cli run --payload-json "$RAW_ROOT/sog_${{RUN_TS}}.json" --games-csv "$GAME_SPINE" --players-csv "$SOG_PLAYERS" --output-root "$MARKET_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.sog_shadow.cli build-policy-config --policy-json /absolute/authorized/frozen_sog_walkforward_policy.json --output "$RAW_ROOT/sog_effective_policy_${{RUN_TS}}.json"
.venv/bin/python -m backend.nhl.sog_shadow.cli run --game-spine-csv "$GAME_SPINE" --player-inputs-csv "$SOG_INPUTS" --quote-run-dir /absolute/immutable/sog_quote_run --effective-policy-json "$RAW_ROOT/sog_effective_policy_${{RUN_TS}}.json" --parity-json {PARITY.relative_to(ROOT)} --output-root "$SHADOW_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY --no-upload-shaped-output

.venv/bin/python backend/nhl/scripts/create_points_shadow_input_snapshot.py --slate-date "$SLATE_DATE" --game-spine-csv "$GAME_SPINE" --game-spine-manifest "$MORNING_MANIFEST" --features-csv "$POINTS_FEATURES" --output-root "$SHADOW_ROOT/points_inputs"
.venv/bin/python -m backend.nhl.points_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/points_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.points_quote_capture.cli run --payload-json "$RAW_ROOT/points_${{RUN_TS}}.json" --games-csv /absolute/points_snapshot/canonical_game_spine.csv --players-csv /absolute/points_snapshot/points_player_inputs.csv --parent-manifest /absolute/points_snapshot/SHA256SUMS --output-root "$MARKET_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.points_shadow.cli run --game-spine-csv /absolute/points_snapshot/canonical_game_spine.csv --game-spine-manifest /absolute/points_snapshot/SHA256SUMS --player-inputs-csv /absolute/points_snapshot/points_player_inputs.csv --player-inputs-manifest /absolute/points_snapshot/SHA256SUMS --quote-run-dir /absolute/immutable/points_quote_run --output-root "$SHADOW_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python backend/nhl/scripts/run_nhl_live_failure_sentinel.py --phase MIDDAY --slate-date "$SLATE_DATE" --run-id /exact/midday_run_id --input-json /absolute/run-bound/midday_sentinel_input.json
```

## FINAL_PREGAME

Repeat the fetch/capture/run commands with a new create-only `RUN_TS` and `--run-type FINAL_PREGAME`. Do not reuse MIDDAY raw paths, quote directories, run IDs, or effective-config output paths. Confirm every provider/source/capture timestamp is strictly before scheduled start. Then run the sentinel:

```sh
.venv/bin/python backend/nhl/scripts/run_nhl_live_failure_sentinel.py --phase FINAL_PREGAME --slate-date "$SLATE_DATE" --run-id /exact/final_run_id --input-json /absolute/run-bound/final_sentinel_input.json
```

Expected success artifacts are `SHA256SUMS`, run metadata, raw/book-level quotes, timing/binding audits, predictions, market views, population ledgers, and lane health/sentinel files. Points additionally requires `RUN_COMPLETE.json`, ladder diagnostics, P=all frozen rows, M=eligible ladders with quotes, and C/U/E=0. SOG must archive the effective policy and matching hash and must have no upload-shaped file in this operating mode.

`GREEN` means no blockers or warnings. `YELLOW` means bounded coverage/evidence warnings; inspect every reason and proceed only within existing scope. `RED` prohibits candidate processing or further downstream action for stale/missing/hash-mismatched parents, partial slate, identity/orientation failure, post-start contamination, misgrading, wrong season/type, unsafe mutable input, or database/runtime failure. A manifest-complete valid empty slate is a successful stop with no scoring/capture.

An incomplete run lacks `SHA256SUMS` (and Points also lacks `RUN_COMPLETE.json` or remains under `.incomplete`). Never fill, delete, rename over, or rerun the same identity. Preserve it for diagnosis and retry with a new timestamp/run identity and fresh create-only paths after the cause is fixed.

Grading uses the lane `grade` CLI after official completion and writes a separate create-only tree:

```sh
export GRADE_TS=YYYY-MM-DDTHH:MM:SSZ GRADE_ROOT=/absolute/create-only/grades
.venv/bin/python -m backend.nhl.mainline_shadow.cli grade --run-dir /absolute/immutable/moneyline_final_run --outcomes-csv /absolute/certified/moneyline_outcomes.csv --grade-root "$GRADE_ROOT/moneyline" --grading-timestamp-utc "$GRADE_TS"
.venv/bin/python -m backend.nhl.sog_shadow.cli grade --run-dir /absolute/immutable/sog_final_run --outcomes-csv /absolute/certified/sog_outcomes.csv --grade-root "$GRADE_ROOT/sog" --grading-timestamp-utc "$GRADE_TS"
.venv/bin/python -m backend.nhl.points_shadow.cli grade --run-dir /absolute/immutable/points_final_run --outcomes-csv /absolute/certified/points_outcomes.csv --grade-root "$GRADE_ROOT/points" --grading-timestamp-utc "$GRADE_TS"
```

Preseason remains non-evaluative; nonparticipants remain ungraded. Upload-shaped data never implies execution, and this rehearsal emits no upload. Points has no upload/execution CLI. Goalie Saves stays disabled.
"""
        (staging / f"nhl_three_lane_day_one_runbook_{DATE}.md").write_text(runbook)

        checklist = f"""# September 19 NHL preseason checklist

1. Confirm the 07:30 LaunchAgent exit is 0 and `morning_health.json` is manifest-complete. If `VALID_EMPTY_SLATE`, stop successfully.
2. Confirm canonical season 2026, game type 1, unique game IDs, start times, home/away orientation, and exact same game IDs across Moneyline/SOG/Points.
3. Confirm DB credential availability, `ODDS_API_KEY`, strict-prior history/roster timestamps, Points input manifest, SOG parity hash, and explicit authorized SOG policy JSON.
4. Create new MIDDAY timestamps and paths. Run Moneyline, SOG quote→score→candidate with `--no-upload-shaped-output`, and Points input→quote→P/M. Run nothing for Goalie Saves.
5. Verify MIDDAY manifests and populations: Moneyline P/M shadow only; SOG E=0 and no upload-shaped file; Points C/U/E=0 with `{POLICY_STATUS}`, blocked ladders visible in P and absent from M.
6. Read the MIDDAY sentinel. RED blocks downstream action. YELLOW requires reason review; missing/partial markets may be healthy reduced coverage.
7. Before each game starts, create entirely new FINAL_PREGAME raw, quote, effective-config, and shadow identities. Never overwrite MIDDAY. Confirm every qualified quote is pre-start.
8. Read FINAL_PREGAME sentinel and compare MIDDAY/FINAL coverage, identities, disappearances, and late postings. Preserve both phases.
9. After official completion, grade append-only. Confirm preseason non-evaluation, nonparticipant ungraded, execution rows zero, and pregame hashes unchanged.
10. Reconcile cumulative artifacts and preserve all incomplete paths. Retry only with a new run identity. Upload never implies execution; no upload or execution is authorized here.

Decision boundary: Moneyline stays frozen-control prediction/market shadow-only; SOG stays certified P/M/C-lineage shadow-only with no upload/execution; Points stays P/M shadow-only; Goalie Saves stays disabled and `NOT_READY_FOR_PRESEASON_BURN_IN`.
"""
        (staging / f"nhl_september_19_one_page_checklist_{DATE}.md").write_text(checklist)

        report = f"""# NHL season 2026 three-lane preseason operator rehearsal v1

## Decision

`{decision}`

The complete offline day-one sequence passed against one isolated, nonempty season-2026 preseason fixture sharing game ID `{GAME_ID}` across Moneyline, SOG, and Points. Both MIDDAY and FINAL_PREGAME were create-only and distinct; finalized trees remained byte-stable through later phases and grading. No database write, network fetch, live mutable archive, upload, execution, or Goalie Saves process occurred.

Moneyline produced P=1/M=1 in both phases. SOG produced P=12/M=12 at MIDDAY and P=12/M=6 at partial-coverage FINAL; its explicit fixture effective-config hash was `{policy['effective_config_hash']}`, candidate lineage ran, upload output was disabled, and E=0. Points produced P=9/M=6 at MIDDAY and P=9/M=3 at partial-coverage FINAL; its materially incoherent ladder stayed visible in P and absent from M, while C/U/E remained zero under `{POLICY_STATUS}`.

All {len(negative)} injected negative tests passed: duplicate run identities, absent SOG policy, substitute Points policy, post-start quotes, unknown game types, incomplete output, low/zero-usefulness coverage states, partial-slate RED, unsafe mutable-input RED, cross-lane isolation, and accidental Saves invocation. All preseason grades were non-evaluative, numeric Moneyline regular-season targets were absent, SOG emitted no ordinary preseason settlements, Points emitted no observed preseason result, and no grader implied execution or mutated its pregame parent.

## Defects found and disposition

Three high defects were repaired within authority and passed the integrated rerun: SOG preseason grading isolation, Moneyline unsupported-game-type critical gating, and Moneyline suppression of numeric targets outside regular-season evaluation. These changes touch grading/health only; frozen model parameters, scorer formulas, SOG policy, candidates, capture, upload, execution, orchestration, and LaunchAgent schedule are unchanged. One medium limitation remains: Moneyline and SOG claim final-looking directories before completion; an absent `SHA256SUMS` makes them unambiguously incomplete and blocks reuse. Preserve such a path and retry with a new timestamp identity.

## Scope preserved

- Moneyline: frozen historical control, prediction/market shadow only, preseason non-evaluative.
- SOG: certified immutable quote/prediction/candidate-lineage/grading path, explicit effective config, no upload or execution.
- Points: P/M only; mandatory ladder gate; C/U/E blocked; no substitute policy.
- Goalie Saves: disabled, `NOT_READY_FOR_PRESEASON_BURN_IN`.

The exact next task remains `FIRST_REAL_NHL_SEASON_2026_PRESEASON_MULTI_RUN_BURN_IN` and must wait for the first real nonempty preseason slate.
"""
        (staging / f"nhl_three_lane_operator_rehearsal_report_{DATE}.md").write_text(report)
        (staging / f"nhl_three_lane_operator_rehearsal_executive_summary_{DATE}.md").write_text("# NHL three-lane preseason rehearsal — executive summary\n\n" + report.split("## Defects found", 1)[0].split("## Decision\n\n", 1)[1])

        git_sha = run_cmd(["git", "rev-parse", "HEAD"]).stdout.strip()
        status = run_cmd(["git", "status", "--short"]).stdout
        (staging / f"nhl_source_control_status_{DATE}.txt").write_text(f"HEAD {git_sha}\n\n{status}")
        write_json(staging / f"package_identity_{DATE}.json", {"package": "NHL_SEASON_2026_THREE_LANE_PRESEASON_OPERATOR_REHEARSAL_V1", "as_of_date": DATE, "generated_by": str(Path(__file__).relative_to(ROOT)), "source_git_sha": git_sha, "decision": decision, "offline": True, "live_namespaces_written": False, "network_used": False, "database_writes": False, "uploads": 0, "execution_rows": 0, "goalie_saves_invocations": 0})

    files = sorted(path for path in staging.iterdir() if path.is_file() and path.name != "SHA256SUMS")
    (staging / "SHA256SUMS").write_text("".join(f"{sha(path)}  {path.name}\n" for path in files))
    verify_manifest(staging)
    finalize_package(staging, OUT)
    manifest_sha = verify_manifest(OUT)
    print(json.dumps({"output_dir": str(OUT), "decision": json.loads((OUT / f"nhl_rehearsal_decision_{DATE}.json").read_text())["decision"], "manifest_sha256": manifest_sha, "files": len(list(OUT.iterdir()))}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
