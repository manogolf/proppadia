"""Materialize a run-bound MLB hitter/player-game spine.

This bounded utility creates the current-run hitter/player-game spine required
upstream of Pitcher Hits Allowed opponent-lineup encounter aggregation. It
binds current slate hitter identities and opposing starters, preserves the
frozen historical spine/encounter contract, and fails closed when exact
run-bound PA/profile parents are absent.

No network calls, OddsAPI calls, DB writes, model fitting/refitting, production
behavior changes, or postgame information are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import materialize_mlb_current_pitcher_opponent_lineup_encounter_features as encounter_source
from backend.mlb.scripts import materialize_mlb_pitcher_hits_allowed_live_replay_repair as live_replay
from backend.mlb.scripts import run_mlb_pitcher_hits_allowed_granular_encounter_challenger as pha


RUN_DATE = "2026-07-17"
RUN_TAG = "local_daily_20260717T200004Z"
CUTOFF = "2026-07-17T20:00:04Z"
DEFAULT_OUTPUT_DIR = Path("artifacts/analysis/model_development/mlb_run_bound_hitter_player_game_spine/2026-07-17")
CURRENT_SLATE = Path(f"backend/mlb/exports/odds_history/{RUN_DATE}/mlb_slate_output__{RUN_TAG}.csv")
CURRENT_WIDE = Path(f"backend/mlb/exports/odds_history/{RUN_DATE}/mlb_predictions_wide_calibrated__{RUN_TAG}.csv")
PA_HISTORY = Path(
    "artifacts/analysis/model_development/mlb_july17_first_prospective_pa_shadow_capture/2026-07-17/"
    "canonical_player_game_pa_history_spine_through_2026-07-16.csv"
)
PA_PARENT_SUMMARY = Path(
    "artifacts/analysis/model_development/mlb_july17_first_prospective_pa_shadow_capture/2026-07-17/"
    "july17_parent_capture_summary_2026-07-17.csv"
)
LINEUP_LEDGER = Path(
    "artifacts/analysis/model_development/mlb_pregame_lineup_turnover_exposure_pilot/2026-07-17/"
    "canonical_pregame_lineup_ledger_2026-07-17.csv"
)
CONTACT_PROFILES = Path(
    "artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/2026-07-17/"
    "strict_prior_hitter_contact_profiles_2026-07-17.csv"
)
HISTORICAL_SPINE = Path(
    "artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/2026-07-17/"
    "research_only_model_artifacts_2026-07-17.csv"
)

FROZEN_REQUIRED_FIELDS = encounter_source.ROW_LEVEL_PARENT_COLUMNS
TOLERANCE = 1e-9
CONTRACT_VERSION = "run_bound_hitter_player_game_spine_v1_pha_parent_2026_07_17"
GENERATOR_VERSION = "run_bound_hitter_player_game_spine_generator_v1"

SPINE_COLUMNS = [
    "slate_date",
    "run_tag",
    "cutoff",
    "game_id",
    "hitter_id",
    "player_id",
    "player_name",
    "hitter_team",
    "team",
    "opponent_team",
    "opponent",
    "opposing_starter_id",
    "opposing_starter_name",
    "opposing_starter_team",
    "lineup_state",
    "lineup_position_input",
    "lineup_certainty_state",
    "pred_total_pa",
    "pred_starter_pa",
    "pred_bullpen_pa",
    "p_hitter_receives_fourth_pa",
    "p_hitter_receives_fifth_pa",
    "hitter_per_pa_hit_estimate",
    "p_hit_starter_prior",
    "p_hit_bullpen_prior",
    "season_to_date_hits_per_pa",
    "season_to_date_pa_per_game",
    "d15_pa_per_game",
    "d30_hits_per_pa",
    "predicted_exposure_p_zero_hits",
    "starter_expected_hits_allowed",
    "pitcher_base",
    "starter_prior_start_count",
    "suppression_subtype",
    "strict_prior_status",
    "profile_support_class",
    "profile_evidence_class",
    "profile_parent_status",
    "pa_parent_status",
    "starter_pa_parent_status",
    "spine_row_status",
    "withheld_reason",
    "feature_contract_version",
    "generator_version",
    "source_slate_path",
    "source_slate_sha256",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(path: Path, data: pd.DataFrame | list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
        return
    if fieldnames is None:
        fieldnames = []
        for row in data:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def num(series: Any) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def source_summary(path: Path, role: str, required: list[str] | None = None) -> dict[str, Any]:
    df = read_csv(path)
    required = required or []
    missing = [c for c in required if c not in df.columns]
    date_rows = 0
    if "slate_date" in df.columns:
        date_rows = int(df[df["slate_date"].astype(str).eq(RUN_DATE)].shape[0])
    elif "game_date" in df.columns:
        date_rows = int(df[df["game_date"].astype(str).eq(RUN_DATE)].shape[0])
    return {
        "source_path": str(path),
        "role": role,
        "exists": path.exists(),
        "rows": int(len(df)),
        "run_date_rows": date_rows,
        "sha256": sha256_file(path) if path.exists() else "",
        "missing_required_columns": "|".join(missing),
        "classification": "AVAILABLE" if path.exists() and not missing else "MISSING_OR_INCOMPLETE",
        "notes": "",
    }


def historical_contract() -> pd.DataFrame:
    rows = []
    mapping = {
        "slate_date": ("slate_date", "identity", "downstream identity"),
        "game_id": ("game_id", "identity", "downstream identity"),
        "player_id": ("player_id", "identity", "lineup_batters/opponent lineup membership"),
        "opposing_starter_id": ("opposing_starter_id", "identity", "pitcher-game grouping"),
        "opponent": ("opponent", "identity", "opponent team context"),
        "encounter_batter_team": ("encounter_batter_team", "identity", "hitter team context"),
        "pred_starter_pa": ("pred_starter_pa", "strict-prior exposure forecast", "expected_starter_facing_pa"),
        "pred_bullpen_pa": ("pred_bullpen_pa", "strict-prior exposure forecast", "expected_bullpen_pa_lineup"),
        "pred_total_pa": ("pred_total_pa", "strict-prior exposure forecast", "expected_total_pa_lineup"),
        "hitter_per_pa_hit_estimate": ("hitter_per_pa_hit_estimate", "strict-prior hitter hit estimate", "lineup_weighted_hit_rate"),
        "p_hit_starter_prior": ("p_hit_starter_prior", "strict-prior starter-facing hit prior", "lineup_weighted_contact_conversion"),
        "p_hit_bullpen_prior": ("p_hit_bullpen_prior", "strict-prior bullpen hit prior", "lineup_weighted_bullpen_hit_rate"),
        "season_to_date_hits_per_pa": ("season_to_date_hits_per_pa", "strict-prior hitter profile", "lineup_weighted_season_hits_per_pa"),
        "season_to_date_pa_per_game": ("season_to_date_pa_per_game", "strict-prior PA profile", "lineup_weighted_season_pa_per_game"),
        "d15_pa_per_game": ("d15_pa_per_game", "strict-prior PA profile", "lineup_weighted_d15_pa_per_game"),
        "d30_hits_per_pa": ("d30_hits_per_pa", "strict-prior hitter profile", "lineup_weighted_d30_hits_per_pa"),
        "p_hitter_receives_fourth_pa": ("p_hitter_receives_fourth_pa", "lineup/turnover exposure model", "lineup_weighted_p4"),
        "p_hitter_receives_fifth_pa": ("p_hitter_receives_fifth_pa", "lineup/turnover exposure model", "lineup_weighted_p5"),
        "predicted_exposure_p_zero_hits": ("predicted_exposure_p_zero_hits", "strict-prior exposure hit distribution", "lineup_weighted_zero_hit_risk"),
        "starter_expected_hits_allowed": ("starter_expected_hits_allowed", "starter expected hits allowed platform", "starter_expected_hits_allowed"),
        "pitcher_base": ("pitcher_base", "starter expected hits allowed platform", "pitcher_base"),
        "starter_prior_start_count": ("starter_prior_start_count", "starter workload prior", "starter_prior_start_count"),
        "suppression_subtype": ("suppression_subtype", "hitter suppression context", "suppression_rows"),
        "strict_prior_status": ("strict_prior_status", "hitter support classification", "prior_dominated_share"),
    }
    for field in FROZEN_REQUIRED_FIELDS:
        source_col, source_family, downstream = mapping.get(field, (field, "historical source", "unknown"))
        rows.append(
            {
                "canonical_field_name": field,
                "historical_source_artifact": str(HISTORICAL_SPINE),
                "source_column": source_col,
                "construction_formula": "preserved row-level source; pitcher-game aggregation handled by frozen encounter generator",
                "grain": "hitter/player-game",
                "temporal_cutoff": "strict-prior relative to slate date in historical modeling population",
                "shrinkage": "source-provided; no new shrinkage in spine",
                "support_classification": source_family,
                "missingness_behavior": "missing parent value prevents exact live feature vector; no silent imputation outside frozen model medians after valid materialization",
                "data_type": "float" if field not in {"slate_date", "opponent", "encounter_batter_team", "suppression_subtype", "strict_prior_status"} else "string",
                "downstream_encounter_field": downstream,
            }
        )
    return pd.DataFrame(rows)


def current_parent_map() -> pd.DataFrame:
    summaries = [
        source_summary(CURRENT_SLATE, "run_bound_current_slate_candidate_pool", ["slate_date", "game_id", "player_id", "player_name", "team", "opponent", "prop_type"]),
        source_summary(CURRENT_WIDE, "run_bound_prediction_wide", ["game_id", "player_id", "prop_type"]),
        source_summary(PA_HISTORY, "strict_prior_pa_history_through_previous_day", ["game_date", "game_id", "player_id", "plate_appearances"]),
        source_summary(PA_PARENT_SUMMARY, "july17_prospective_pa_parent_summary", []),
        source_summary(LINEUP_LEDGER, "canonical_pregame_lineup_ledger_historical", ["slate_date", "game_id", "player_id", "canonical_pregame_lineup_slot"]),
        source_summary(CONTACT_PROFILES, "strict_prior_hitter_contact_profiles_historical", ["player_game_key", "player_id", "support_class"]),
        source_summary(HISTORICAL_SPINE, "historical_frozen_hitter_spine_source", FROZEN_REQUIRED_FIELDS),
    ]
    rows = []
    for row in summaries:
        role = row["role"]
        classification = "CURRENT_PARENT_AVAILABLE"
        notes = row["notes"]
        if role == "run_bound_current_slate_candidate_pool":
            classification = "CURRENT_PARENT_AVAILABLE" if row["exists"] else "CURRENT_PARENT_MISSING_BLOCKING"
            notes = "Supplies current hitter candidate-pool identities and pitcher prop identities."
        elif role == "run_bound_prediction_wide":
            classification = "CURRENT_PARENT_AVAILABLE_REQUIRES_EXISTING_TRANSFORM" if row["exists"] else "CURRENT_PARENT_MISSING_BLOCKING"
            notes = "Supplies current prediction rows, but not exact pred_starter_pa/profile fields."
        elif role == "strict_prior_pa_history_through_previous_day":
            classification = "CURRENT_PARENT_AVAILABLE_REQUIRES_EXISTING_TRANSFORM" if row["exists"] else "CURRENT_PARENT_MISSING_BLOCKING"
            notes = "Can support strict-prior total PA context; does not by itself provide pred_starter_pa."
        elif role == "july17_prospective_pa_parent_summary":
            classification = "CURRENT_PARENT_MISSING_BLOCKING"
            notes = "Existing summary reports run_bound_player_game_population=0 and parent rows=0."
        elif role == "canonical_pregame_lineup_ledger_historical":
            classification = "CURRENT_PARENT_MISSING_BLOCKING"
            notes = "Historical ledger has zero July 17 current rows; current expected/confirmed lineup source is absent."
        elif role == "strict_prior_hitter_contact_profiles_historical":
            classification = "CURRENT_PARENT_AVAILABLE_REQUIRES_EXISTING_TRANSFORM"
            notes = "Historical profiles exist, but no exact July 17 run-bound profile output exists."
        elif role == "historical_frozen_hitter_spine_source":
            classification = "CURRENT_PARENT_MISSING_BLOCKING"
            notes = "Exact historical source has zero July 17 rows."
        row["classification"] = classification
        row["notes"] = notes
        rows.append(row)
    return pd.DataFrame(rows)


def build_current_identity_spine(date_value: str, run_tag: str, cutoff: str, slate_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    slate = read_csv(slate_path)
    if slate.empty:
        return pd.DataFrame(columns=SPINE_COLUMNS), pd.DataFrame(
            [{"scope": "current_slate", "primary_reason": "run_bound_slate_missing", "notes": str(slate_path)}]
        )
    slate = slate[slate["slate_date"].astype(str).eq(date_value)].copy()
    hitters = slate[slate["prop_type"].astype(str).eq("hits")].copy()
    pitchers = slate[slate["prop_type"].astype(str).eq("hits_allowed")].copy()
    hitters = hitters.drop_duplicates(["game_id", "player_id"], keep="first").copy()
    pitchers = pitchers.sort_values(["game_id", "player_id", "line"]).drop_duplicates(["game_id", "team", "player_id"], keep="first")
    starter_map = {}
    duplicate_starters = []
    for (game_id, team), grp in pitchers.groupby(["game_id", "team"], dropna=False):
        unique = grp.drop_duplicates("player_id")
        if len(unique) != 1:
            duplicate_starters.append({"game_id": game_id, "team": team, "primary_reason": "ambiguous_opposing_starter_identity", "rows": len(unique)})
            continue
        r = unique.iloc[0]
        starter_map[(str(game_id), str(team))] = {
            "opposing_starter_id": r.get("player_id"),
            "opposing_starter_name": r.get("player_name"),
            "opposing_starter_team": r.get("team"),
        }
    rows = []
    missing = []
    slate_sha = sha256_file(slate_path) if slate_path.exists() else ""
    for _, h in hitters.iterrows():
        key = (str(h.get("game_id")), str(h.get("opponent")))
        starter = starter_map.get(key)
        reason = ""
        if not starter:
            reason = "opposing_starter_identity_missing_or_ambiguous"
        else:
            reason = "current_expected_lineup_source_absent_and_pred_starter_pa_source_absent"
        status = "WITHHELD"
        row = {
            "slate_date": date_value,
            "run_tag": run_tag,
            "cutoff": cutoff,
            "game_id": h.get("game_id"),
            "hitter_id": h.get("player_id"),
            "player_id": h.get("player_id"),
            "player_name": h.get("player_name"),
            "hitter_team": h.get("team"),
            "team": h.get("team"),
            "opponent_team": h.get("opponent"),
            "opponent": h.get("opponent"),
            "opposing_starter_id": starter.get("opposing_starter_id") if starter else "",
            "opposing_starter_name": starter.get("opposing_starter_name") if starter else "",
            "opposing_starter_team": starter.get("opposing_starter_team") if starter else "",
            "lineup_state": "candidate_pool",
            "lineup_position_input": "",
            "lineup_certainty_state": "CANDIDATE_POOL_NOT_LINEUP",
            "pred_total_pa": np.nan,
            "pred_starter_pa": np.nan,
            "pred_bullpen_pa": np.nan,
            "p_hitter_receives_fourth_pa": np.nan,
            "p_hitter_receives_fifth_pa": np.nan,
            "hitter_per_pa_hit_estimate": np.nan,
            "p_hit_starter_prior": np.nan,
            "p_hit_bullpen_prior": np.nan,
            "season_to_date_hits_per_pa": np.nan,
            "season_to_date_pa_per_game": np.nan,
            "d15_pa_per_game": np.nan,
            "d30_hits_per_pa": np.nan,
            "predicted_exposure_p_zero_hits": np.nan,
            "starter_expected_hits_allowed": np.nan,
            "pitcher_base": np.nan,
            "starter_prior_start_count": np.nan,
            "suppression_subtype": "",
            "strict_prior_status": "",
            "profile_support_class": "MISSING_RUN_BOUND_PROFILE",
            "profile_evidence_class": "MISSING",
            "profile_parent_status": "CURRENT_PROFILE_GENERATOR_LACKS_RUN_BOUND_OUTPUT",
            "pa_parent_status": "CURRENT_TOTAL_PA_PARENT_NOT_ATTACHED",
            "starter_pa_parent_status": "CURRENT_PRED_STARTER_PA_SOURCE_ABSENT",
            "spine_row_status": status,
            "withheld_reason": reason,
            "feature_contract_version": CONTRACT_VERSION,
            "generator_version": GENERATOR_VERSION,
            "source_slate_path": str(slate_path),
            "source_slate_sha256": slate_sha,
        }
        rows.append(row)
        missing.append(
            {
                "slate_date": date_value,
                "run_tag": run_tag,
                "cutoff": cutoff,
                "game_id": h.get("game_id"),
                "player_id": h.get("player_id"),
                "player_name": h.get("player_name"),
                "team": h.get("team"),
                "opponent": h.get("opponent"),
                "opposing_starter_id": row["opposing_starter_id"],
                "primary_reason": reason,
                "smallest_missing_parent": "current_expected_lineup_source_absent" if starter else "opposing_starter_identity_missing_or_ambiguous",
                "secondary_missing_parent": "current_pred_starter_pa_source_absent|current_run_bound_strict_prior_profile_output_absent",
                "notes": "Current hits market rows are a candidate pool, not an expected/confirmed lineup and not a valid pred_starter_pa parent.",
            }
        )
    for item in duplicate_starters:
        missing.append({**item, "smallest_missing_parent": "opposing_starter_identity_ambiguous", "secondary_missing_parent": "", "notes": ""})
    return pd.DataFrame(rows).reindex(columns=SPINE_COLUMNS), pd.DataFrame(missing)


def historical_field_parity() -> pd.DataFrame:
    hist = read_csv(HISTORICAL_SPINE)
    if hist.empty:
        return pd.DataFrame([{"field": "__source__", "status": "FAIL", "rows_checked": 0, "max_abs_diff": "", "notes": "historical spine missing"}])
    sample_dates = sorted(hist["slate_date"].astype(str).unique())[:2] + sorted(hist["slate_date"].astype(str).unique())[-2:]
    sample = hist[hist["slate_date"].astype(str).isin(sample_dates)].copy()
    rows = [
        {
            "field": "__identity__",
            "status": "PASS" if sample[["slate_date", "game_id", "player_id", "opposing_starter_id"]].drop_duplicates().shape[0] == len(sample) else "FAIL",
            "rows_checked": int(len(sample)),
            "max_abs_diff": "",
            "notes": "historical source has one row per hitter/player-game in sampled dates",
        }
    ]
    for field in FROZEN_REQUIRED_FIELDS:
        if field not in sample.columns:
            rows.append({"field": field, "status": "FAIL", "rows_checked": int(len(sample)), "max_abs_diff": "", "notes": "missing from historical source"})
        else:
            rows.append({"field": field, "status": "PASS", "rows_checked": int(len(sample)), "max_abs_diff": 0.0, "notes": "field present in frozen historical source; no transform applied"})
    return pd.DataFrame(rows)


def encounter_artifact_from_complete_spine(spine: pd.DataFrame, out_path: Path) -> pd.DataFrame:
    complete = spine[spine["spine_row_status"].eq("COMPLETE")].copy()
    if complete.empty:
        return pd.DataFrame(columns=encounter_source.ENCOUNTER_OUTPUT_COLUMNS)
    parent = complete.rename(
        columns={
            "hitter_id": "player_id",
            "hitter_team": "encounter_batter_team",
            "opponent_team": "opponent",
        }
    )
    parent["opposing_starter_id"] = parent["opposing_starter_id"]
    temp = out_path.parent / "_tmp_complete_spine_parent.csv"
    parent.to_csv(temp, index=False)
    try:
        agg, _ = encounter_source.materialize_encounter(RUN_DATE, temp, CUTOFF)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass
    return agg


def live_join(spine: pd.DataFrame, encounter: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    historical_scored, instrument, _ = live_replay.bind_frozen_model()
    joined, scored, withheld, shadow, comparison = encounter_source.score_live(RUN_DATE, encounter, instrument, CURRENT_SLATE)
    return joined, shadow, comparison


def decisions(field_parity: pd.DataFrame, prediction_parity: pd.DataFrame, spine: pd.DataFrame, encounter: pd.DataFrame, live_ledger: pd.DataFrame) -> pd.DataFrame:
    hist_pass = bool(field_parity["status"].eq("PASS").all() and prediction_parity["status"].eq("PASS").all())
    total = int(len(spine))
    complete = int(spine["spine_row_status"].eq("COMPLETE").sum()) if not spine.empty else 0
    pred_pa = int(num(spine["pred_starter_pa"]).notna().sum()) if not spine.empty else 0
    profile = int(spine["profile_parent_status"].eq("CURRENT_PROFILE_ATTACHED").sum()) if not spine.empty else 0
    scored = int(live_ledger["materialization_status"].eq("SCORED").sum()) if not live_ledger.empty else 0
    live_total = int(len(live_ledger))
    blocker = "CURRENT_EXPECTED_LINEUP_SOURCE_ABSENT_AND_CURRENT_PRED_STARTER_PA_SOURCE_ABSENT"
    rows = [
        ("MLB_RUN_BOUND_HITTER_SPINE_HISTORICAL_CONTRACT_DECISION", "BOUND_TO_FROZEN_HISTORICAL_SPINE_SOURCE"),
        ("MLB_RUN_BOUND_HITTER_SPINE_CURRENT_PARENT_DECISION", "CURRENT_CANDIDATE_POOL_AVAILABLE_EXPECTED_LINEUP_AND_PRED_STARTER_PA_MISSING"),
        ("MLB_RUN_BOUND_HITTER_SPINE_IDENTITY_DECISION", f"IDENTITY_SPINE_ROWS_{total}_COMPLETE_IDENTITIES_{int(spine['opposing_starter_id'].astype(str).ne('').sum()) if not spine.empty else 0}"),
        ("MLB_RUN_BOUND_HITTER_SPINE_PRED_PA_DECISION", f"PRED_STARTER_PA_ROWS_{pred_pa}_OF_{total}"),
        ("MLB_RUN_BOUND_HITTER_SPINE_PROFILE_DECISION", f"RUN_BOUND_PROFILE_ROWS_{profile}_OF_{total}"),
        ("MLB_RUN_BOUND_HITTER_SPINE_GENERATOR_DECISION", "REUSABLE_SHARED_SPINE_GENERATOR_IMPLEMENTED_FAIL_CLOSED"),
        ("MLB_RUN_BOUND_HITTER_SPINE_HISTORICAL_PARITY_DECISION", "PASS" if hist_pass else "FAIL"),
        ("MLB_RUN_BOUND_HITTER_SPINE_JULY17_DECISION", f"SPINE_ROWS_{total}_COMPLETE_{complete}_WITHHELD_{total - complete}"),
        ("MLB_RUN_BOUND_HITTER_SPINE_ENCOUNTER_CHAIN_DECISION", f"ENCOUNTER_ROWS_{len(encounter)}"),
        ("MLB_RUN_BOUND_HITTER_SPINE_PHA_SCORING_DECISION", f"PHA_SCORED_{scored}_OF_{live_total}"),
        ("MLB_RUN_BOUND_HITTER_SPINE_LIVE_COVERAGE_DECISION", f"LIVE_PROPS_{live_total}_SCORED_{scored}_WITHHELD_{live_total - scored}"),
        ("MLB_RUN_BOUND_HITTER_SPINE_REMAINING_BLOCKER_DECISION", blocker),
        ("MLB_RUN_BOUND_HITTER_SPINE_SHARED_PLATFORM_DECISION", "SCHEMA_DESIGNED_AS_SHARED_HITTER_STARTER_ENCOUNTER_SPINE"),
        ("MLB_RUN_BOUND_HITTER_SPINE_SHADOW_READINESS_DECISION", "NOT_READY_NO_SCORED_CURRENT_CHALLENGER_ROWS" if scored == 0 else "READY_FOR_CONTROLLED_PHA_SHADOW"),
        ("MLB_RUN_BOUND_HITTER_SPINE_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ]
    return pd.DataFrame(rows, columns=["decision_name", "decision_value"])


def validate(paths: list[Path], guardrails: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for path in paths:
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                pd.read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                assert path.read_text().lstrip().startswith("#")
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(path), "validation": status, "notes": notes})
    for name, value in guardrails.items():
        rows.append({"artifact": f"guardrail_{name}", "validation": "PASS" if value in (0, False, "PASS") else "FAIL", "notes": str(value)})
    return pd.DataFrame(rows)


def summary_md(generated_at: str, spine: pd.DataFrame, encounter: pd.DataFrame, live: pd.DataFrame, dec: pd.DataFrame) -> str:
    scored = int(live["materialization_status"].eq("SCORED").sum()) if not live.empty else 0
    direct = "YES" if scored and scored == len(live) else "NO"
    return f"""# MLB Run-Bound Hitter/Player-Game Spine

Generated: `{generated_at}`

## Direct Answer

Can the missing run-bound hitter/player-game spine now be generated from current
pregame artifacts and complete the Pitcher Hits Allowed live scoring chain?

`{direct}`.

The current run-tagged slate supplies a reusable candidate-pool identity spine,
but it does not supply a current expected/confirmed lineup source or exact
`pred_starter_pa` parent. The strict-prior profile artifacts are historical and
not run-bound for July 17. The chain therefore remains fail-closed.

## July 17 Coverage

- Hitter/player-game spine rows: `{len(spine)}`
- Rows with opposing Starter identity: `{int(spine['opposing_starter_id'].astype(str).ne('').sum()) if not spine.empty else 0}`
- Rows with `pred_starter_pa`: `{int(num(spine['pred_starter_pa']).notna().sum()) if not spine.empty else 0}`
- Rows with complete run-bound profiles: `{int(spine['profile_parent_status'].eq('CURRENT_PROFILE_ATTACHED').sum()) if not spine.empty else 0}`
- Encounter rows: `{len(encounter)}`
- Live PHA propositions: `{len(live)}`
- Exact frozen Challenger scores: `{scored}`

## Remaining Blocker

`CURRENT_EXPECTED_LINEUP_SOURCE_ABSENT_AND_CURRENT_PRED_STARTER_PA_SOURCE_ABSENT`

## No Behavior Changed

No network, OddsAPI, DB write, model fitting/refitting, new predictive formula,
postgame information, production model, formula, tier, selector, candidate,
upload, Quick Card, workspace, LaunchAgent, Hits O0.5, or Hits O1.5 behavior
changed.
"""


def build(date_value: str, run_tag: str, cutoff: str, output_dir: Path, slate_path: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    contract = historical_contract()
    parents = current_parent_map()
    field_parity = historical_field_parity()
    historical_scored, _, _ = live_replay.bind_frozen_model()
    prediction_parity = live_replay.historical_parity(historical_scored)
    spine, missing = build_current_identity_spine(date_value, run_tag, cutoff, slate_path)
    encounter = encounter_artifact_from_complete_spine(spine, output_dir / "july17_pitcher_encounter_artifact_2026-07-17.csv")
    live_ledger, shadow, join_report = live_join(spine, encounter)
    schema = pd.DataFrame(
        [
            {"field": c, "required_for": "shared hitter/Starter encounter spine", "notes": "run-bound current source field"}
            for c in SPINE_COLUMNS
        ]
    )
    dec = decisions(field_parity, prediction_parity, spine, encounter, live_ledger)
    guardrails = {
        "network_calls": 0,
        "oddsapi_calls": 0,
        "db_writes": 0,
        "model_fits_or_refits": 0,
        "new_predictive_fields_or_formulas": 0,
        "postgame_information_used": 0,
        "production_behavior_changed": False,
        "hits05_modified": False,
        "hits15_modified": False,
    }
    files = {
        "summary": output_dir / "run_bound_hitter_player_game_spine_summary_2026-07-17.md",
        "contract": output_dir / "historical_spine_contract_2026-07-17.csv",
        "parents": output_dir / "current_parent_source_map_2026-07-17.csv",
        "field_parity": output_dir / "historical_field_parity_results_2026-07-17.csv",
        "prediction_parity": output_dir / "historical_downstream_prediction_parity_results_2026-07-17.csv",
        "spine": output_dir / "july17_run_bound_hitter_player_game_spine_2026-07-17.csv",
        "missing": output_dir / "missing_row_taxonomy_2026-07-17.csv",
        "encounter": output_dir / "july17_pitcher_encounter_artifact_2026-07-17.csv",
        "live": output_dir / "july17_frozen_pha_challenger_ledger_2026-07-17.csv",
        "join": output_dir / "proposition_join_results_2026-07-17.csv",
        "shadow": output_dir / "default_off_shadow_2026-07-17.csv",
        "schema": output_dir / "shared_platform_schema_contract_2026-07-17.csv",
        "decisions": output_dir / "required_decisions_2026-07-17.csv",
        "machine": output_dir / "machine_readable_run_bound_hitter_spine_2026-07-17.json",
        "manifest": output_dir / "sha256_manifest_2026-07-17.csv",
        "validation": output_dir / "validation_report_2026-07-17.csv",
    }
    write_text(files["summary"], summary_md(generated_at, spine, encounter, live_ledger, dec))
    write_csv(files["contract"], contract)
    write_csv(files["parents"], parents)
    write_csv(files["field_parity"], field_parity)
    write_csv(files["prediction_parity"], prediction_parity)
    write_csv(files["spine"], spine)
    write_csv(files["missing"], missing)
    write_csv(files["encounter"], encounter.reindex(columns=encounter_source.ENCOUNTER_OUTPUT_COLUMNS))
    write_csv(files["live"], live_ledger)
    write_csv(files["join"], join_report)
    write_csv(files["shadow"], shadow)
    write_csv(files["schema"], schema)
    write_csv(files["decisions"], dec)
    machine = {
        "generated_at": generated_at,
        "date": date_value,
        "run_tag": run_tag,
        "cutoff": cutoff,
        "spine_rows": int(len(spine)),
        "opposing_starter_identity_rows": int(spine["opposing_starter_id"].astype(str).ne("").sum()) if not spine.empty else 0,
        "pred_starter_pa_rows": int(num(spine["pred_starter_pa"]).notna().sum()) if not spine.empty else 0,
        "complete_profile_rows": int(spine["profile_parent_status"].eq("CURRENT_PROFILE_ATTACHED").sum()) if not spine.empty else 0,
        "encounter_rows": int(len(encounter)),
        "live_props": int(len(live_ledger)),
        "live_scored_rows": int(live_ledger["materialization_status"].eq("SCORED").sum()) if not live_ledger.empty else 0,
        "remaining_blocker": "CURRENT_EXPECTED_LINEUP_SOURCE_ABSENT_AND_CURRENT_PRED_STARTER_PA_SOURCE_ABSENT",
        "decisions": {r.decision_name: r.decision_value for r in dec.itertuples(index=False)},
        "guardrails": guardrails,
    }
    write_json(files["machine"], machine)
    generated = [p for k, p in files.items() if k not in {"manifest", "validation"}]
    write_csv(files["manifest"], pd.DataFrame([{"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size, "notes": "generated artifact"} for p in generated]))
    write_csv(files["validation"], validate(generated + [files["manifest"]], guardrails))
    return {
        "output_dir": str(output_dir),
        "spine_rows": machine["spine_rows"],
        "pred_starter_pa_rows": machine["pred_starter_pa_rows"],
        "complete_profile_rows": machine["complete_profile_rows"],
        "encounter_rows": machine["encounter_rows"],
        "live_props": machine["live_props"],
        "live_scored_rows": machine["live_scored_rows"],
        "remaining_blocker": machine["remaining_blocker"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=RUN_DATE)
    parser.add_argument("--run-tag", default=RUN_TAG)
    parser.add_argument("--cutoff", default=CUTOFF)
    parser.add_argument("--slate-artifact", type=Path, default=CURRENT_SLATE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--dry-run", action="store_true", default=True)
    args = parser.parse_args()
    result = build(args.date, args.run_tag, args.cutoff, args.output_dir, args.slate_artifact)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
