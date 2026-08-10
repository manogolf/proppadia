#!/usr/bin/env python3
"""Build the bounded NHL 2026 opening/preseason/context readiness package."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

from backend.nhl.mainline_shadow.core import FEATURES, historical_parity, load_parameters, score_features
from backend.nhl.analysis_package_guard import require_create_only

DATE = "2026-07-13"
CHAMPION = "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1"
PREDICTION_SHA = "83beb11588f7e7e31919f23be2dea51ff49863954fc9be750509b30a0eff2cda"
PARENTS = {
    "frozen_certification": ("nhl_moneyline_frozen_baseline_certification", "8bb36073fee4f055f399c651f942b8de6eb1bb3b75b96b6112dd9d4af4224cf5"),
    "prospective_readiness": ("nhl_season_2026_mainline_prospective_readiness", "bccba8eae9f13088d1b25f057349039a5ecccb688acdc09c0355dec1e83f3a95"),
    "shadow_implementation": ("nhl_season_2026_mainline_shadow_capture_implementation", "62de5b047b0121664ede00ce197b339968eec00ace64f6a782ca2850a366b09c"),
}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def js(value, path: Path) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, lineterminator="\n", float_format="%.15g")


def metrics(group: pd.DataFrame) -> dict:
    y = group.home_win_target.to_numpy(float)
    p = group.home_win_probability.to_numpy(float)
    eps = 1e-15
    return {
        "row_count": len(group),
        "mean_probability": float(p.mean()),
        "observed_home_win_rate": float(y.mean()),
        "accuracy": float(((p >= .5) == y).mean()),
        "brier": float(np.mean((p - y) ** 2)),
        "log_loss": float(-np.mean(y * np.log(np.clip(p, eps, 1-eps)) + (1-y) * np.log(np.clip(1-p, eps, 1-eps)))),
        "calibration_gap": float(p.mean() - y.mean()),
    }


def history_bin(n: int) -> str:
    if n <= 5:
        return str(n)
    if n <= 9:
        return "6-9"
    return "10+"


def quality(n: int) -> str:
    if n == 0:
        return "SEASON_OPEN_NO_HISTORY"
    if n <= 2:
        return "EARLY_SEASON_SPARSE_HISTORY"
    if n <= 9:
        return "PARTIAL_CURRENT_SEASON_HISTORY"
    return "MATURE_CURRENT_SEASON_HISTORY"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[3])
    ap.add_argument("--output-dir", type=Path)
    args = ap.parse_args()
    root = args.repo_root.resolve()
    base = root / "artifacts/analysis/model_development"
    out = (args.output_dir or base / f"nhl_season_2026_opening_preseason_context_readiness/{DATE}").resolve()
    parent_dirs = {k: base / name / DATE for k, (name, _) in PARENTS.items()}
    parent_before = {str(f): sha(f) for d in parent_dirs.values() for f in d.iterdir() if f.is_file()}
    for key, directory in parent_dirs.items():
        assert sha(directory / "SHA256SUMS") == PARENTS[key][1]
        subprocess.run(["shasum", "-a", "256", "-c", "SHA256SUMS"], cwd=directory, check=True, capture_output=True)
    require_create_only(out)
    out.mkdir(parents=True)

    baseline = base / f"nhl_moneyline_simple_baseline_process_validation/{DATE}"
    matrix_path = baseline / f"nhl_moneyline_simple_baseline_feature_matrix_audit_{DATE}.csv"
    predictions_path = baseline / f"nhl_moneyline_simple_baseline_control_predictions_{DATE}.csv"
    population_path = base / f"nhl_full_game_moneyline_population_certification/{DATE}/nhl_full_game_moneyline_outcome_qualification_ledger_{DATE}.csv"
    assert sha(predictions_path) == PREDICTION_SHA
    parity = historical_parity(matrix_path, predictions_path)
    assert parity.status.iloc[0] == "PASS"
    params = load_parameters()
    assert params["champion_identity"] == CHAMPION and params["historical_prediction_sha256"] == PREDICTION_SHA

    matrix = pd.read_csv(matrix_path)
    pred = pd.read_csv(predictions_path)
    pop = pd.read_csv(population_path)
    keys = ["canonical_season", "game_id"]
    d = pred.merge(matrix, on=keys, validate="one_to_one", suffixes=("", "_matrix")).merge(
        pop[keys + ["game_date", "scheduled_start_time_utc", "home_team_id", "away_team_id", "game_type"]],
        on=keys, validate="one_to_one", suffixes=("", "_population")
    )
    d["game_date"] = pd.to_datetime(d.game_date)
    d = d.sort_values(["canonical_season", "game_date", "scheduled_start_time_utc", "game_id"], kind="mergesort")
    counts: dict[tuple[int, int], int] = {}
    home_prior, away_prior = [], []
    for row in d.itertuples(index=False):
        hkey, akey = (int(row.canonical_season), int(row.home_team_id)), (int(row.canonical_season), int(row.away_team_id))
        home_prior.append(counts.get(hkey, 0)); away_prior.append(counts.get(akey, 0))
        counts[hkey] = counts.get(hkey, 0) + 1; counts[akey] = counts.get(akey, 0) + 1
    d["home_prior_games"] = home_prior; d["away_prior_games"] = away_prior
    d["minimum_team_prior_games"] = d[["home_prior_games", "away_prior_games"]].min(axis=1)
    d["maximum_team_prior_games"] = d[["home_prior_games", "away_prior_games"]].max(axis=1)
    d["prior_game_count_group"] = d.minimum_team_prior_games.map(history_bin)
    d["opening_state_classification"] = d.minimum_team_prior_games.map(quality)
    d["row_qualification"] = np.where(d[[f"was_imputed__{f}" for f in FEATURES]].any(axis=1), "MIN_HISTORY_IMPUTED", "FULLY_OBSERVED")

    feature_rows = []
    definitions = {
        "diff_std_goal_diff_pg": ("home minus away season-to-date goal differential per game", 1, params["features"]["diff_std_goal_diff_pg"]["median"], "10+ for stable depth label; mathematically observed after both teams have 1"),
        "diff_r10_goal_diff_pg": ("home minus away prior-up-to-10 goal differential per game", 1, params["features"]["diff_r10_goal_diff_pg"]["median"], "10+ for both teams"),
        "diff_std_shot_diff_pg": ("home minus away season-to-date shot differential per game", 1, params["features"]["diff_std_shot_diff_pg"]["median"], "10+ for stable depth label; mathematically observed after both teams have 1"),
        "diff_days_rest": ("home minus away whole-day rest from prior completed game", 1, params["features"]["diff_days_rest"]["median"], "after both teams have 1"),
        "home_back_to_back": ("home prior-game interval equals one calendar day", 1, params["features"]["home_back_to_back"]["median"], "after home has 1"),
        "away_back_to_back": ("away prior-game interval equals one calendar day", 1, params["features"]["away_back_to_back"]["median"], "after away has 1"),
    }
    for f in FEATURES:
        definition, minimum, median, fully = definitions[f]
        feature_rows.append({"feature": f, "definition": definition, "season_reset_rule": "filter completed history to canonical_season == target canonical_season", "minimum_prior_game_requirement": minimum, "missingness_behavior": "raw null then frozen fit-only median imputation", "frozen_imputation_value": median, "prior_season_information_excluded": True, "scoreable_on_opening_night": True, "opening_night_status": "IMPUTED", "fully_current_season_informed": fully, "champion_change": "NONE"})
    csv(pd.DataFrame(feature_rows), out / f"nhl_season_2026_champion_opening_state_feature_audit_{DATE}.csv")

    status_cols = []
    sim_cols = keys + ["game_date", "home_team", "away_team", "home_prior_games", "away_prior_games", "minimum_team_prior_games", "maximum_team_prior_games", "prior_game_count_group", "opening_state_classification", "row_qualification", "home_win_probability", "home_win_target", "missing_feature_count"]
    sim = d[sim_cols].copy()
    for f in FEATURES:
        col = f"{f}_status"; sim[col] = np.where(d[f"was_imputed__{f}"], "IMPUTED", "OBSERVED"); status_cols.append(col)
    sim["fully_observed_fields"] = sim[status_cols].eq("OBSERVED").sum(axis=1)
    sim["imputed_fields"] = sim[status_cols].eq("IMPUTED").sum(axis=1)
    csv(sim[sim.canonical_season.isin([2023, 2024])], out / f"nhl_season_2026_historical_opening_state_simulation_{DATE}.csv")

    depth_rows = []
    ordered_bins = ["0", "1", "2", "3", "4", "5", "6-9", "10+"]
    for season_scope, frame in [("2023", d[d.canonical_season.eq(2023)]), ("2024", d[d.canonical_season.eq(2024)]), ("COMBINED", d)]:
        for label in ordered_bins:
            g = frame[frame.prior_game_count_group.eq(label)]
            if len(g):
                depth_rows.append({"season_scope": season_scope, "prior_game_count_group": label, "natural_basis": "minimum(home_prior_games,away_prior_games)", "quality_classification": quality(0 if label == "0" else 1 if label in ["1", "2"] else 3 if label != "10+" else 10), "imputed_rows": int(g.row_qualification.eq("MIN_HISTORY_IMPUTED").sum()), **metrics(g)})
    depth = pd.DataFrame(depth_rows)
    csv(depth, out / f"nhl_season_2026_early_season_history_depth_characterization_{DATE}.csv")

    carry = pd.DataFrame([
        ["NHL strict-prior builder season filter", "backend/nhl/mainline_shadow/core.py:_team_history", "EXISTING_REPOSITORY_CONCEPT", "HISTORICALLY_REPLAYABLE", "explicitly excludes other seasons; no carryover"],
        ["NHL season-to-date feature spine", "backend/nhl/scripts/build_nhl_moneyline_team_goalie_feature_spine.py", "EXISTING_REPOSITORY_CONCEPT", "HISTORICALLY_REPLAYABLE", "season reset and prior_season_carryover=NO"],
        ["MLB prior-season/rolling concepts", "repository MLB pipelines", "EXISTING_REPOSITORY_CONCEPT", "REQUIRES_NEW_RESEARCH", "sport-specific framework ideas only; not portable champion inputs"],
        ["NHL prior-season shrinkage/bootstrap model", "none", "NOT_AVAILABLE", "REQUIRES_NEW_RESEARCH", "could only be a separately specified challenger"],
        ["season 2026 prospective prior-season carryover", "none", "PROSPECTIVE_ONLY", "REQUIRES_NEW_RESEARCH", "not implemented or authorized"],
    ], columns=["concept", "repository_evidence", "repository_classification", "replay_classification", "finding"])
    csv(carry, out / f"nhl_season_2026_prior_season_carryover_inventory_{DATE}.csv")

    opening_contract = {
        "canonical_season": 2026, "champion_identity": CHAMPION, "champion_prediction_sha256": PREDICTION_SHA,
        "season_history_rule": "completed games with canonical_season=2026 and scheduled start strictly before target start/run",
        "prediction_policy": "score every identity-valid pregame row the frozen scorer can produce; sparse history does not suppress prediction",
        "required_retained_fields": ["canonical_season", "game_id", "home_prior_games", "away_prior_games"] + FEATURES + [f"{f}_status" for f in FEATURES] + ["opening_state_classification", "champion_home_win_probability", "no_history_flag", "limited_history_flag"],
        "classifications": {"SEASON_OPEN_NO_HISTORY": "minimum prior count 0", "EARLY_SEASON_SPARSE_HISTORY": "minimum prior count 1-2", "PARTIAL_CURRENT_SEASON_HISTORY": "minimum prior count 3-9", "MATURE_CURRENT_SEASON_HISTORY": "minimum prior count 10+"},
        "no_history_flag": "home_prior_games == 0 or away_prior_games == 0", "limited_history_flag": "minimum prior games < 10",
        "imputation": {f: params["features"][f]["median"] for f in FEATURES}, "prior_season_carryover": False,
        "candidate_policy_separation": "prediction generation is independent from any later candidate/recommendation policy; recommendations remain unauthorized",
    }
    js(opening_contract, out / f"nhl_season_2026_opening_state_contract_{DATE}.json")

    preseason_identity = pd.DataFrame([
        ["official schedule exposure", "api-web.nhle.com/v1/schedule/{date}", "READY_WITH_BOUNDED_LIMITS", "date-specific NHL schedule exposes preseason when published; live season 2026 fixture required"],
        ["game type", "gameType numeric", "READY", "1=PRESEASON; 2=REGULAR_SEASON; 3=POSTSEASON"],
        ["game ID", "NHL game id", "READY", "starting-year season prefix plus type segment; preserve provider id"],
        ["scheduled start", "startTimeUTC", "READY_WITH_BOUNDED_LIMITS", "archive every schedule response because starts can change"],
        ["home/away", "homeTeam/awayTeam ids and abbreviations", "READY", "deterministic official identities"],
        ["canonical season", "derived from game season starting year", "READY", "always canonical_season=2026; never multi-year shorthand"],
        ["franchise mapping", "official numeric team id plus abbreviation", "READY_WITH_BOUNDED_LIMITS", "persist both; validate transitions and neutral-site preseason games"],
        ["reschedule/postponement", "game id, schedule state, revised start", "READY_WITH_BOUNDED_LIMITS", "append snapshots; do not overwrite original start/state"],
        ["existing mainline game-type propagation", "GAME_COLS omits game_type", "NOT_READY", "current run archive cannot safely prove PRESEASON/REGULAR_SEASON/POSTSEASON isolation"],
    ], columns=["dimension", "source_or_field", "readiness", "evidence_or_limit"])
    csv(preseason_identity, out / f"nhl_season_2026_preseason_identity_readiness_{DATE}.csv")

    preseason_contract = {
        "mode": "PLUMBING_VALIDATION_ONLY", "prediction_label": "PRESEASON_NON_EVALUATION", "canonical_season": 2026,
        "allowed": ["schedule ingestion", "game binding", "H2H odds capture", "sportsbook coverage", "quote timestamps", "raw archive preservation", "MIDDAY run", "FINAL_PREGAME run", "immutable run chaining", "market movement observation", "outcome grading", "reschedule handling", "overwrite protection", "health gates", "champion scoring-path execution"],
        "prohibited": ["champion performance certification", "model calibration claims", "ROI claims", "promotion", "candidate selection", "threshold tuning", "wager recommendations"],
        "evaluation_filter": "game_type == 2 only; game_type == 1 is structurally excluded before metric computation",
        "required_before_first_run": "propagate official game_type through game_spine, run metadata, predictions, grading, and population gates",
    }
    js(preseason_contract, out / f"nhl_season_2026_preseason_shadow_validation_contract_{DATE}.json")

    repeat = pd.DataFrame([
        [1, "MIDDAY", "create new run id", "earlier provider-bound quotes and raw response", "no prior path exists"],
        [2, "FINAL_PREGAME", "create distinct later run id for same games", "later provider-bound quotes plus raw response", "MIDDAY tree hash unchanged"],
        [3, "COMPARE", "join by canonical_season+game_id+sportsbook", "movement computable only where both states exist", "no requirement every book appears twice"],
        [4, "POSTGAME", "append separate grade artifact", "official outcome/context only", "both pregame manifests unchanged"],
    ], columns=["sequence", "state", "identity_requirement", "preservation_requirement", "assertion"])
    csv(repeat, out / f"nhl_season_2026_preseason_repeated_run_plan_{DATE}.csv")

    goalie = pd.DataFrame([
        ["NHL official roster", "public read-only API; already connected", "none evident", "NHL player/team IDs", "team roster only; no projected/confirmed starter semantics", "response/capture time only", "current polling", "possible only via own raw snapshots", "READY_WITH_MINOR_INTEGRATION", "not sufficient as goalie projection"],
        ["NHL official gamecenter/boxscore", "public read-only API; repository consumers exist", "none evident", "excellent NHL game/player IDs", "actual participating/starter context after lineup/game", "capture time; event state", "live/postgame", "raw archive possible prospectively", "READY_NOW", "actual starter only; never substitute for pregame projection"],
        ["SportsDataIO projected/confirmed goalies", "commercial JSON API; not connected", "API key/paid product or trial", "vendor IDs require NHL crosswalk", "projected night before; confirmed following announcements", "update fields/capture must be schema-proven", "updates as news arrives", "own append-only polling required", "SOURCE_IDENTIFIED_NOT_CONNECTED", "best current goalie path; proof capture required before timestamp certification"],
        ["Sportradar NHL feeds", "commercial JSON API; not connected", "API key trial/production", "strong game/team/player vendor identity", "roster/injury and lineups; projected goalie semantics not proven", "feed generated time likely but response audit required", "provider dependent", "own archive required", "DEFER", "inferior evidence for projected/confirmed goalie requirement"],
        ["Daily Faceoff / public web reports", "web pages; no governed repository integration", "terms and access not established", "name/team matching", "projected/confirmed labels", "page times not certified as source event time", "frequent", "scraping archive would be custom", "DEFER", "web scraping not authorized or policy-certified"],
    ], columns=["source_name", "access_mechanism", "cost_auth", "player_id_quality", "semantics", "timestamp_availability", "update_frequency", "historical_archive", "readiness", "integration_finding"])
    csv(goalie, out / f"nhl_season_2026_goalie_source_inventory_{DATE}.csv")

    goalie_contract = {
        "grain": ["canonical_season", "game_id", "team_id", "source", "source_timestamp_utc", "goalie_player_id"],
        "required_fields": ["canonical_season", "game_id", "team_id", "goalie_player_id", "goalie_name", "status", "source", "source_timestamp_utc", "capture_timestamp_utc", "scheduled_start_time_utc"],
        "statuses": ["NO_PROJECTION", "PROJECTED", "LIKELY", "CONFIRMED", "CHANGED_AFTER_CONFIRMATION", "UNKNOWN"],
        "pregame_qualified_rule": "source_timestamp_utc < scheduled_start_time_utc AND deterministic game/team binding",
        "capture_rule": "capture_timestamp_utc must also precede scheduled start for pregame-qualified use",
        "actual_starter_separation": "actual starter is appended as outcome/context and never rewrites projected/confirmed observations",
        "current_readiness": "NOT_READY", "blocker": "no connected source with schema-proven provider event timestamp and deterministic NHL identity crosswalk",
    }
    js(goalie_contract, out / f"nhl_season_2026_goalie_timestamp_certification_contract_{DATE}.json")

    lineup = pd.DataFrame([
        ["NHL official roster endpoint", "roster state", "excellent NHL IDs", "capture timestamp only", "poll-driven", "own archive possible", "team; not game-specific", "READY_WITH_MINOR_INTEGRATION", "best currently connected roster source; not injury/lineup/scratch"],
        ["NHL official gamecenter/boxscore", "actual game roster/scratch context", "excellent NHL IDs", "capture/event state", "pregame/live/postgame", "own archive possible", "deterministic game/team", "READY_WITH_MINOR_INTEGRATION", "actual submitted/participating state; semantics require live fixture audit"],
        ["SportsDataIO line combinations", "projected current lines/pairs/PP units", "vendor IDs need crosswalk", "schema timestamp proof required", "throughout season", "provider says current only; own polling required", "team; bind to next game locally", "SOURCE_IDENTIFIED_NOT_CONNECTED", "best single lineup/injury path when combined with same-provider injuries"],
        ["SportsDataIO injuries", "injury state/player availability", "vendor IDs need crosswalk", "start date exists; update timestamp proof required", "news-driven", "own polling required", "team/player; game binding derived", "SOURCE_IDENTIFIED_NOT_CONNECTED", "NHL disclosure is incomplete; preserve source uncertainty"],
        ["Sportradar NHL injuries", "injury state", "strong vendor identity", "response schema audit required", "provider dependent", "own polling required", "league/team/player", "SOURCE_IDENTIFIED_NOT_CONNECTED", "documented active injuries endpoint; not connected"],
        ["public team/media pages", "projected/confirmed lines and scratches", "name matching variable", "often page/update time inadequate", "event-driven", "scraping archive custom", "team/game ambiguous", "DEFER", "scraping not authorized or governed"],
    ], columns=["source_name", "context_type", "identity_quality", "timestamp_availability", "update_frequency", "historical_preservation", "binding", "readiness", "finding"])
    csv(lineup, out / f"nhl_season_2026_lineup_injury_source_inventory_{DATE}.csv")

    readiness = pd.DataFrame([
        ["goalie", "SportsDataIO projected/confirmed goalies", "SOURCE_IDENTIFIED_NOT_CONNECTED", "one schema/sample proof capture, NHL ID crosswalk, source timestamp verification, append-only poller"],
        ["actual_goalie", "NHL official gamecenter", "READY_NOW", "append only after game as separate context/outcome"],
        ["roster", "NHL official roster", "READY_WITH_MINOR_INTEGRATION", "immutable raw snapshots and capture timestamps"],
        ["lineup_injury", "SportsDataIO line combinations plus injuries", "SOURCE_IDENTIFIED_NOT_CONNECTED", "one provider account/path, schema proof, NHL crosswalk, append-only polling"],
        ["scratch", "NHL gamecenter plus SportsDataIO context", "READY_WITH_MINOR_INTEGRATION", "prove pregame availability and distinguish confirmed from inferred absence"],
    ], columns=["context", "recommended_source_path", "readiness", "next_evidence"])
    csv(readiness, out / f"nhl_season_2026_context_source_readiness_{DATE}.csv")

    archive_contract = {
        "grain": ["canonical_season", "game_id", "source", "source_timestamp_utc", "context_type"], "append_only": True,
        "context_types": ["ROSTER_STATE", "INJURY_STATE", "PROJECTED_LINEUP", "CONFIRMED_LINEUP", "SCRATCH_STATE", "PROJECTED_GOALIE", "CONFIRMED_GOALIE", "ACTUAL_GOALIE"],
        "required_envelope": ["canonical_season", "game_id", "team_id", "source", "source_record_id", "source_timestamp_utc", "capture_timestamp_utc", "scheduled_start_time_utc", "context_type", "status", "raw_payload_sha256", "supersedes_event_id"],
        "event_id": "sha256(canonical JSON of grain plus source_record_id plus raw_payload_sha256)",
        "collision_policy": "same event id plus same bytes is idempotent; same event id plus different bytes fails closed",
        "transition_policy": "later state references earlier event but never mutates it", "post_start_policy": "retain but mark POST_START_CONTEXT; exclude from pregame-qualified populations",
    }
    js(archive_contract, out / f"nhl_season_2026_context_archive_contract_{DATE}.json")

    checklist_pre = """# NHL Season 2026 Preseason Operational Checklist\n\nMode: `PLUMBING_VALIDATION_ONLY`; every probability: `PRESEASON_NON_EVALUATION`.\n\n## Before run\n\n- [ ] Official schedule import succeeds and raw response is immutable.\n- [ ] `canonical_season=2026`, `game_type=PRESEASON`, game ID, teams, and start are retained.\n- [ ] Champion parameter and 2,798-row parity hashes pass.\n- [ ] H2H endpoint responds; an empty-book response is handled explicitly.\n- [ ] Create-only run path does not exist.\n\n## MIDDAY\n\n- [ ] Strict-prior builder and frozen scoring execute; opening-state labels are retained.\n- [ ] Raw odds, provider source timestamps, capture timestamp, and Population A/B counts are preserved.\n- [ ] Health ledger passes or fails closed.\n\n## FINAL_PREGAME\n\n- [ ] Same games bind to a distinct run ID; new quotes create a new snapshot.\n- [ ] MIDDAY hashes remain unchanged; missing books are allowed.\n- [ ] Post-start quotes/context are retained diagnostically but rejected from pregame qualification.\n\n## Postgame\n\n- [ ] Official outcome is appended in a separate grade tree.\n- [ ] Both pregame manifests remain unchanged.\n- [ ] No preseason row enters champion performance/calibration/ROI evaluation.\n"""
    (out / f"nhl_season_2026_preseason_operational_checklist_{DATE}.md").write_text(checklist_pre)
    checklist_reg = """# NHL Season 2026 Regular-Season Opening Checklist\n\n- [ ] Official `REGULAR_SEASON` game type and `canonical_season=2026` mapping are verified.\n- [ ] Opening-state classification, prior-game counts, per-feature observed/imputed status, and sparse-history flags persist.\n- [ ] Frozen champion identity, prediction hash, parameter hash, and parity remain exact.\n- [ ] All six semantics and frozen imputation values are unchanged; no prior-season carryover exists.\n- [ ] Real H2H source/capture timestamps and two-run immutability passed during preseason.\n- [ ] Post-start rejection, reschedule handling, health gates, and append-only grading passed.\n- [ ] Evaluation filters prove zero preseason contamination.\n- [ ] Goalie/lineup state is captured if certified; absence is explicit and does not block champion MVP.\n- [ ] No wager, recommendation, candidate, execution, or automated schedule output is enabled.\n"""
    (out / f"nhl_season_2026_regular_season_opening_checklist_{DATE}.md").write_text(checklist_reg)

    timeline = pd.DataFrame([
        ["PRE_PRESEASON", "propagate game_type; add opening-state fields/contracts; validate parent/parity; establish source account/sample schemas; implement append-only context envelope", "no live game required"],
        ["PRESEASON_VALIDATION", "run human-initiated MIDDAY and FINAL_PREGAME captures; validate odds, run identity, reschedules, grading, overwrite rejection, context timestamps", "real preseason slate required; non-evaluation only"],
        ["REGULAR_SEASON_OPEN", "pass regular checklist; prove game_type=REGULAR_SEASON and zero preseason contamination", "first regular-season slate"],
        ["EARLY_SEASON_OBSERVATION", "monitor opening-state mix, feature missingness, source coverage, timestamp rejection, schedule revisions through first 10 games per team", "descriptive monitoring only"],
    ], columns=["stage", "tasks", "gate"])
    csv(timeline, out / f"nhl_season_2026_context_readiness_timeline_{DATE}.csv")

    no_history = d[d.minimum_team_prior_games.eq(0)]
    mature = d[d.minimum_team_prior_games.ge(10)]
    decisions = {
        "NHL_SEASON_2026_OPENING_STATE_BEHAVIOR_CERTIFIED": "READY",
        "NHL_SEASON_2026_EARLY_SEASON_HISTORY_DEPTH_CHARACTERIZED": "READY",
        "NHL_SEASON_2026_PRIOR_SEASON_CARRYOVER_STATUS": "NOT_AVAILABLE",
        "NHL_SEASON_2026_PRESEASON_GAME_IDENTITY_READINESS": "READY_WITH_BOUNDED_LIMITS",
        "NHL_SEASON_2026_PRESEASON_SHADOW_VALIDATION_READINESS": "BLOCKED_BY_GAME_TYPE_PROPAGATION_AND_LIVE_PRESEASON_FIXTURE",
        "NHL_SEASON_2026_REPEATED_RUN_VALIDATION_READINESS": "READY_WITH_BOUNDED_LIMITS",
        "NHL_SEASON_2026_GOALIE_SOURCE_READINESS": "SOURCE_IDENTIFIED_NOT_CONNECTED",
        "NHL_SEASON_2026_GOALIE_TIMESTAMP_CERTIFICATION_READINESS": "NOT_READY",
        "NHL_SEASON_2026_LINEUP_INJURY_SOURCE_READINESS": "SOURCE_IDENTIFIED_NOT_CONNECTED",
        "NHL_SEASON_2026_CONTEXT_ARCHIVE_READINESS": "READY_WITH_BOUNDED_LIMITS",
        "NHL_SEASON_2026_REGULAR_SEASON_OPENING_READINESS": "BLOCKED_BY_PRESEASON_LIVE_VALIDATION",
        "NHL_SEASON_2026_MAINLINE_WAGER_RECOMMENDATION_READINESS": "NOT_READY",
    }
    next_task = "NHL_SEASON_2026_GAME_TYPE_PROPAGATION_AND_PRESEASON_ISOLATION_IMPLEMENTATION"
    decision = {"decisions": decisions, "recommended_next_bounded_task": next_task,
        "unlocked": ["One bounded implementation that propagates official game_type through immutable mainline artifacts and mechanically excludes preseason from regular-season evaluation"],
        "still_blocked": ["first real preseason validation until schedule exists", "goalie timestamp certification until provider schema/sample capture", "lineup/injury timestamp certification until provider schema/sample capture", "regular-season operational trust until preseason gates pass", "wager recommendations", "ROI", "retraining", "challenger research", "automatic scheduling", "production promotion"],
        "champion_changed": False, "refit_performed": False, "preseason_evaluation_authorized": False}
    js(decision, out / f"nhl_season_2026_opening_preseason_context_decision_{DATE}.json")

    report = f"""# NHL Season 2026 Opening-State, Preseason, and Context Readiness\n\n## Outcome\n\nThe frozen `{CHAMPION}` is scoreable on opening night without prior-season carryover. With no current-season history, all six raw inputs are null and the unchanged scorer applies its frozen fit-only medians, producing the stored bootstrap probability path and an explicit `SEASON_OPEN_NO_HISTORY` / `MIN_HISTORY_IMPUTED` label. Each strength/rest field becomes observed after the relevant teams have one completed season-2026 game; the prior-10 field is not fully depth-populated until both teams have 10. Sparse history never silently suppresses a score.\n\nAcross seasons 2023 and 2024, {len(no_history):,} games had at least one team with zero prior games and {len(mature):,} games had both teams at 10+ prior games. The attached row-level simulation preserves the exact frozen probabilities and outcomes; the depth table reports descriptive accuracy, Brier, log loss, and calibration without tuning or refitting.\n\n## Preseason and identity\n\nThe official schedule represents preseason with game type 1, regular season with 2, and postseason with 3, and supplies game IDs, teams, and scheduled starts. The repository importer retains `game_type`, but the current mainline shadow `GAME_COLS` and run archive omit it. Therefore preseason plumbing is not safe for execution until game type is propagated through spine, prediction, metadata, grading, and evaluation filters. Preseason must be labeled `PLUMBING_VALIDATION_ONLY` and probabilities `PRESEASON_NON_EVALUATION`.\n\n## Goalie and lineup sources\n\nThe official NHL roster and gamecenter feeds provide strong NHL identity and actual-game context, but not a certified projected/confirmed starter feed. SportsDataIO is the best currently documented single goalie path: its workflow states projected goalies are published the night before and confirmed starters update with announcements. It is commercial and not connected; provider event timestamps and NHL-ID crosswalks require a schema proof capture.\n\nFor lineup/injury, SportsDataIO is also the best single path because it documents current line combinations (even-strength and power-play) and distinct injury status. Its line combinations are not historically available, so prospective append-only polling is essential. Official roster state remains useful separately. No public-web scraping integration is recommended.\n\nPrimary documentation inspected: https://sportsdata.io/developers/workflow-guide/nhl, https://sportsdata.io/nhl-api, https://developer.sportradar.com/ice-hockey/v5/reference/nhl-injuries, and the repository-connected NHL schedule/roster/gamecenter endpoints.\n\n## Contracts and readiness\n\nGoalie, roster, injury, projected lineup, confirmed lineup, scratch, and actual-goalie events remain separate context types. The archive grain is `canonical_season + game_id + source + source_timestamp_utc + context_type`; transitions append and never overwrite. Pregame qualification requires deterministic binding and source/capture timestamps before scheduled start.\n\nThe exactly one next bounded task is `{next_task}`. It unlocks safe preseason isolation and a later human-initiated real preseason validation; it does not unlock live execution, wagering, evaluation claims, or automation.\n\n## Decisions\n\n""" + "\n".join(f"- `{k}` = `{v}`" for k, v in decisions.items()) + "\n"
    (out / f"nhl_season_2026_opening_preseason_context_readiness_report_{DATE}.md").write_text(report)
    one_page = report.split("## Decisions", 1)[0] + "## Decision summary\n\n" + "\n".join(f"- `{k}` = `{v}`" for k, v in decisions.items()) + "\n"
    (out / f"nhl_season_2026_opening_preseason_context_one_page_summary_{DATE}.md").write_text(one_page)

    package_text = "\n".join(p.read_text(errors="ignore") for p in out.iterdir() if p.suffix in {".md", ".json", ".csv"})
    assert not re.search(r"\b20\d{2}[-–/]20\d{2}\b", package_text)
    assert parent_before == {str(f): sha(f) for d in parent_dirs.values() for f in d.iterdir() if f.is_file()}
    source_text = Path(__file__).read_text()
    forbidden_fit_tokens = ["Logistic" + "Regression(", ".fi" + "t(", "Grid" + "SearchCV"]
    assert not any(term in source_text for term in forbidden_fit_tokens)
    identity = {"package_name": "nhl_season_2026_opening_preseason_context_readiness", "version": "1.0.0", "as_of_date": DATE,
        "generated_by": str(Path(__file__).relative_to(root)), "canonical_season": 2026, "champion_identity": CHAMPION,
        "champion_prediction_sha256": PREDICTION_SHA, "parent_manifest_sha256": {k: digest for k, (_, digest) in PARENTS.items()},
        "historical_rows": len(d), "no_refit_assertion": True, "champion_changed": False, "parent_source_mutation_check": "PASS",
        "historical_parity_maximum_delta": float(parity.maximum_probability_delta.iloc[0]), "external_writes": False}
    js(identity, out / f"package_identity_{DATE}.json")
    files = sorted(p for p in out.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (out / "SHA256SUMS").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))
    subprocess.run(["shasum", "-a", "256", "-c", "SHA256SUMS"], cwd=out, check=True, capture_output=True)
    print(json.dumps({"output_dir": str(out), "rows": len(d), "no_history_games": len(no_history), "mature_games": len(mature), "manifest_sha256": sha(out / "SHA256SUMS")}, indent=2))


if __name__ == "__main__":
    main()
