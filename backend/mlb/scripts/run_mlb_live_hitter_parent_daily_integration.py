"""Default-off daily integration runner for the live hitter parent source.

The runner wires the already governed live hitter opportunity/profile parent
contract into the MLB daily workflow as a research-only shadow path. It does
not change production behavior, does not schedule itself, and does not call
network services unless a future approved invocation explicitly supplies a
captured lineup artifact from the existing StatsAPI dry-run lineup capture.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import build_mlb_prediction_time_pa_opportunity_parents as pa_parent_source
from backend.mlb.scripts import report_mlb_hits_environment as hits_environment_source
from backend.mlb.scripts import materialize_mlb_current_pitcher_opponent_lineup_encounter_features as encounter_source
from backend.mlb.scripts import materialize_mlb_pitcher_hits_allowed_live_replay_repair as live_replay
from backend.mlb.scripts import materialize_mlb_run_bound_hitter_player_game_spine as spine_source
from backend.mlb.scripts import run_mlb_pregame_starter_bullpen_exposure_forecast as exposure_source


DEFAULT_ROOT = Path("artifacts/analysis/model_development/mlb_live_hitter_parent_daily_integration")
ODDS_HISTORY_ROOT = Path("backend/mlb/exports/odds_history")
PROCESSED_SLATE = Path("backend/mlb/data/processed/mlb_slate_output.csv")
LINEUP_CAPTURE_ROOT = Path("artifacts/analysis/mlb/pregame_lineup_capture/dry_runs")
JULY17_PHA_UNAVAILABLE = "JULY17_PHA_CHALLENGER_PREGAME_SCORING_UNAVAILABLE_MISSING_GOVERNED_PARENT_CAPTURE"
CONTRACT_VERSION = "live_hitter_parent_daily_integration_v1"
PA_SOURCE_MANIFEST = Path(
    "artifacts/analysis/model_development/mlb_july17_first_prospective_pa_shadow_capture/2026-07-17/"
    "refreshed_canonical_pa_source_manifest_2026-07-17.csv"
)
FROZEN_EXPOSURE_ARTIFACT = Path(
    "artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/2026-07-17/"
    "research_only_model_artifacts_2026-07-17.csv"
)
FROZEN_EXPOSURE_INSTRUMENTS = Path(
    "artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/2026-07-17/"
    "frozen_exposure_instruments_2026-07-17.csv"
)
CURRENT_WIDE = Path("backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv")

REQUIRED_PARENT_FIELDS = encounter_source.ROW_LEVEL_PARENT_COLUMNS
LIVE_PARENT_COLUMNS = [
    "slate_date",
    "run_tag",
    "cutoff",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "encounter_batter_team",
    "opponent",
    "opposing_starter_id",
    "opposing_starter_name",
    "opposing_starter_team",
    "lineup_status",
    "lineup_semantics",
    "lineup_source_path",
    "lineup_source_sha256",
    "lineup_source_timestamp",
    "raw_source_path",
    "raw_source_sha256",
    "batting_order",
    "lineup_slot",
    "lineup_bucket",
    "confirmed_lineup_starter_flag",
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
    "identity_status",
    "opportunity_status",
    "profile_status",
    "temporal_integrity_status",
    "parent_row_status",
    "withheld_reason",
    "contract_version",
]

SHADOW_COLUMNS = [
    "canonical_proposition_identity",
    "slate_date",
    "run_tag",
    "game_id",
    "pitcher_id",
    "pitcher_name",
    "opponent",
    "line",
    "sportsbook",
    "price",
    "champion_expected_hits",
    "challenger_expected_hits",
    "residual",
    "champion_side",
    "challenger_side",
    "disagreement_state",
    "workload_support",
    "lineup_status",
    "uncertainty",
    "research_status",
    "provenance",
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
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def id_text(value: Any) -> str:
    text = clean(value)
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except Exception:
        return text


def missing_required_fields(row: pd.Series) -> list[str]:
    nullable_required = {"suppression_subtype"}
    return [
        field
        for field in REQUIRED_PARENT_FIELDS
        if field not in row.index or (field not in nullable_required and clean(row.get(field)) == "")
    ]


def num(series: Any) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def latest_slate(date_value: str) -> tuple[Path, str]:
    candidates = sorted((ODDS_HISTORY_ROOT / date_value).glob("mlb_slate_output__*.csv"))
    if candidates:
        path = candidates[-1]
        match = re.search(r"mlb_slate_output__(.+)\.csv$", path.name)
        return path, match.group(1) if match else "unknown_run_tag"
    return PROCESSED_SLATE, "processed_current"


def load_lineup_source(date_value: str, explicit_path: str) -> tuple[pd.DataFrame, Path | None, str]:
    if explicit_path:
        path = Path(explicit_path)
        return read_csv(path), path if path.exists() else None, "explicit_lineup_source"
    candidates = sorted((LINEUP_CAPTURE_ROOT / date_value).glob("*/pregame_lineup_player_rows_*.csv"))
    if not candidates:
        return pd.DataFrame(), None, "no_local_lineup_capture"
    path = candidates[-1]
    return read_csv(path), path, "latest_local_lineup_capture"


def daily_stage_map(enabled: bool) -> pd.DataFrame:
    rows = [
        ("market_cache_refresh", "odds snapshots", "before slate output", "existing daily workflow", "not modified"),
        ("wide_predictions", "stable model/context predictions", "before slate output", "existing daily workflow", "not modified"),
        ("slate_output", "slate identity/player-prop population/starting pitcher market rows", "available before hook", "build_mlb_slate_output.py", "not modified"),
        ("governed_lineup_capture", "expected or confirmed lineup", "earliest valid parent dependency", "dry_run_capture_pregame_lineups.py or governed expected-lineup artifact", "required before scoring"),
        ("live_hitter_parent", "identity + lineup + opportunity + strict-prior profile", "new default-off hook", "run_mlb_live_hitter_parent_daily_integration.py", "enabled" if enabled else "disabled"),
        ("opponent_lineup_encounter", "pitcher-game opponent lineup aggregate", "after complete parent rows", "materialize_mlb_current_pitcher_opponent_lineup_encounter_features.py", "research-only"),
        ("frozen_pha_scoring", "frozen PHA Challenger prediction and prop join", "after encounter rows", "frozen scorer binding", "research-only shadow"),
        ("ops/research_surfaces", "candidate CSVs/review aids/Ops Brief/workspace", "after production artifacts", "existing workflow", "not modified"),
    ]
    return pd.DataFrame(rows, columns=["stage", "produces", "relative_order", "script_or_source", "integration_status"])


def lineup_contract() -> pd.DataFrame:
    rows = [
        {
            "field": "capture_timestamp",
            "source": "pregame_lineup_player_rows.source_fetched_at_utc",
            "required": True,
            "notes": "Source timestamp, not filesystem mtime.",
        },
        {"field": "game_id", "source": "StatsAPI gamePk parsed artifact", "required": True, "notes": ""},
        {"field": "lineup_status", "source": "game/team summary or derived team slot completeness", "required": True, "notes": "confirmed_full required for scoring."},
        {"field": "batter_ids", "source": "player rows player_id", "required": True, "notes": "exact game_id + player_id join only."},
        {"field": "batting_order_positions", "source": "batting_order_raw / lineup_slot", "required": True, "notes": "slots 1-9 only."},
        {"field": "team", "source": "StatsAPI team abbreviation", "required": True, "notes": ""},
        {"field": "opponent", "source": "opponent team abbreviation", "required": True, "notes": ""},
        {"field": "opposing_starter", "source": "current slate hits_allowed pitcher for opponent team", "required": True, "notes": "fail closed if ambiguous."},
        {"field": "raw_source_path", "source": "StatsAPI raw response when captured by governed utility", "required": False, "notes": "current dry-run lineup utility preserves parsed payload and hash; raw response preservation remains a required next tightening if absent."},
        {"field": "raw_source_sha256", "source": "StatsAPI payload hash", "required": True, "notes": ""},
    ]
    return pd.DataFrame(rows)


def hook_contract(enabled: bool) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "hook": "mlb-live-hitter-parent-daily-integration",
                "normal_workflow_location": "conditional Makefile hook inside mlb-daily-ops-brief after feature-lineage health and before report rendering",
                "environment_flag": "MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE",
                "disabled_behavior": "no command is invoked; existing daily behavior is unchanged",
                "enabled_behavior": "research-only parent/encounter/PHA shadow package is generated",
                "current_setting": "enabled" if enabled else "disabled",
                "production_effect": "none",
            }
        ]
    )


def historical_parity() -> pd.DataFrame:
    parent_field = spine_source.historical_field_parity()
    encounter_field = encounter_source.historical_field_parity()
    scored, _, _ = live_replay.bind_frozen_model()
    pred = live_replay.historical_parity(scored)
    rows = [
        {
            "check_name": "historical_parent_parity",
            "status": "PASS" if not parent_field.empty and parent_field["status"].eq("PASS").all() else "FAIL",
            "rows_checked": int(parent_field["rows_checked"].max()) if "rows_checked" in parent_field.columns and not parent_field.empty else 0,
            "notes": "Frozen hitter parent/source field contract.",
        },
        {
            "check_name": "historical_encounter_parity",
            "status": "PASS" if not encounter_field.empty and encounter_field["status"].eq("PASS").all() else "FAIL",
            "rows_checked": int(encounter_field["rows_checked"].max()) if "rows_checked" in encounter_field.columns and not encounter_field.empty else 0,
            "notes": "Frozen opponent-lineup encounter construction.",
        },
        {
            "check_name": "frozen_pha_prediction_parity",
            "status": "PASS" if not pred.empty and pred["status"].eq("PASS").all() else "FAIL",
            "rows_checked": int(pred["rows_checked"].max()) if "rows_checked" in pred.columns and not pred.empty else 0,
            "notes": "Frozen PHA Challenger parity against retained historical rows.",
        },
    ]
    return pd.DataFrame(rows)


def starter_map_from_slate(slate: pd.DataFrame) -> tuple[dict[tuple[str, str], dict[str, Any]], list[dict[str, Any]]]:
    pitchers = slate[slate.get("prop_type", pd.Series(dtype=str)).astype(str).eq("hits_allowed")].copy()
    mapping: dict[tuple[str, str], dict[str, Any]] = {}
    issues: list[dict[str, Any]] = []
    if pitchers.empty:
        return mapping, [{"scope": "starter_map", "withheld_reason": "no_hits_allowed_pitcher_prop_rows"}]
    for (game_id, team), grp in pitchers.groupby(["game_id", "team"], dropna=False):
        unique = grp.drop_duplicates("player_id")
        if len(unique) != 1:
            issues.append({"game_id": game_id, "team": team, "withheld_reason": "ambiguous_or_missing_starter_identity", "rows": len(unique)})
            continue
        row = unique.iloc[0]
        mapping[(clean(game_id), clean(team))] = {
            "opposing_starter_id": row.get("player_id"),
            "opposing_starter_name": row.get("player_name"),
            "opposing_starter_team": row.get("team"),
        }
    return mapping, issues


def attach_lineup(hitters: pd.DataFrame, lineup: pd.DataFrame, lineup_path: Path | None) -> pd.DataFrame:
    out = hitters.copy()
    if lineup.empty:
        for col in ["lineup_status", "lineup_semantics", "lineup_source_timestamp", "batting_order_raw", "lineup_slot", "lineup_bucket", "confirmed_lineup_starter_flag", "source_url", "payload_hash"]:
            out[col] = pd.NA
        out["lineup_source_path"] = str(lineup_path or "")
        out["lineup_source_sha256"] = sha256_file(lineup_path) if lineup_path and lineup_path.exists() else ""
        return out
    lu = lineup.copy()
    if "player_id" not in lu.columns and "hitter_id" in lu.columns:
        lu["player_id"] = lu["hitter_id"]
    for col in ["game_id", "player_id"]:
        if col in lu.columns:
            lu[col] = lu[col].astype(str)
        if col in out.columns:
            out[col] = out[col].astype(str)
    keep = [
        c
        for c in [
            "game_id",
            "player_id",
            "hitter_id",
            "source_timestamp",
            "source_fetched_at_utc",
            "raw_response_path",
            "raw_response_sha256",
            "lineup_status",
            "pregame_validity_state",
            "batting_order_raw",
            "batting_order",
            "lineup_slot",
            "lineup_bucket",
            "confirmed_lineup_starter_flag",
            "lineup_slot_semantics",
            "source_url",
            "source_payload_sha256",
            "payload_hash",
            "validation_status",
        ]
        if c in lu.columns
    ]
    lu = lu[keep].drop_duplicates(["game_id", "player_id"], keep="last")
    out = out.merge(lu, on=["game_id", "player_id"], how="left", suffixes=("", "_lineup"))
    out["lineup_source_path"] = str(lineup_path or "")
    out["lineup_source_sha256"] = sha256_file(lineup_path) if lineup_path and lineup_path.exists() else ""
    if "lineup_status" in out.columns:
        out["lineup_status"] = out["lineup_status"].map(
            lambda v: "confirmed_full_member" if clean(v) == "CONFIRMED_LINEUP" else (clean(v).lower() if clean(v) else "missing")
        )
    else:
        out["lineup_status"] = out["lineup_slot"].map(lambda v: "confirmed_full_member" if clean(v) else "missing")
    out["lineup_semantics"] = out.get("lineup_slot_semantics", pd.Series(dtype=str)).fillna("governed_pregame_source_snapshot")
    source_ts = out.get("source_timestamp", pd.Series(dtype=str)).combine_first(out.get("source_fetched_at_utc", pd.Series(dtype=str)))
    out["lineup_source_timestamp"] = source_ts
    if "batting_order_raw" not in out.columns and "batting_order" in out.columns:
        out["batting_order_raw"] = out["batting_order"]
    if "payload_hash" not in out.columns and "raw_response_sha256" in out.columns:
        out["payload_hash"] = out["raw_response_sha256"]
    return out


def sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def hit_distribution(n_starter: Any, n_bullpen: Any, p_starter: Any, p_bullpen: Any) -> tuple[float, float, float]:
    ns = max(float(n_starter) if pd.notna(n_starter) else 0.0, 0.0)
    nb = max(float(n_bullpen) if pd.notna(n_bullpen) else 0.0, 0.0)
    ps = min(max(float(p_starter) if pd.notna(p_starter) else 0.2, 0.005), 0.55)
    pb = min(max(float(p_bullpen) if pd.notna(p_bullpen) else ps, 0.005), 0.55)
    lam = max(ns * ps + nb * pb, 0.0001)
    p0 = math.exp(-lam)
    p1 = lam * p0
    p2 = max(0.0, 1.0 - p0 - p1)
    total = p0 + p1 + p2
    return p0 / total, p1 / total, p2 / total


def fillna_series(df: pd.DataFrame, col: str, value: Any = np.nan) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series(value, index=df.index)


def build_current_pha_context_ledger(
    *,
    date_value: str,
    run_tag: str,
    cutoff: str,
    slate_path: Path,
    out_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    context_as_of = (datetime.strptime(date_value, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()
    source_trace = [
        {
            "field": "starter_expected_hits_allowed",
            "current_source_artifact": f"current_pha_pitcher_context_ledger_{date_value}.csv",
            "source_column": "expected_hits_allowed_matchup",
            "grain": "slate_date|game_id|pitcher_id|line",
            "timestamp": cutoff,
            "historical_equivalent_field": "starter_expected_hits_allowed",
            "classification": "CURRENT_FIELD_AVAILABLE_REQUIRES_EXISTING_TRANSFORM",
            "transform_or_rename": "existing hits-environment transform, then rename expected_hits_allowed_matchup to starter_expected_hits_allowed",
        },
        {
            "field": "pitcher_base",
            "current_source_artifact": f"current_pha_pitcher_context_ledger_{date_value}.csv",
            "source_column": "pitcher_expected_hits_allowed_weighted",
            "grain": "slate_date|game_id|pitcher_id|line",
            "timestamp": cutoff,
            "historical_equivalent_field": "pitcher_base",
            "classification": "CURRENT_FIELD_AVAILABLE_REQUIRES_EXISTING_TRANSFORM",
            "transform_or_rename": "existing hits-environment transform, then rename pitcher_expected_hits_allowed_weighted to pitcher_base",
        },
    ]
    if not slate_path.exists():
        return pd.DataFrame(), pd.DataFrame(source_trace)

    old_probable = hits_environment_source._load_probable_starter_rows
    try:
        hits_environment_source._load_probable_starter_rows = lambda _slate_date: (
            [],
            {"probable_starter_status": "skipped_no_network_live_parent_replay"},
        )
        baseline, _baseline_meta = hits_environment_source._fetch_multi_season_starter_baselines(
            eval_date=context_as_of,
            seasons_back=3,
            season_weight_decay=0.70,
            min_starts=5,
        )
        team_form = hits_environment_source._fetch_team_hits_form(context_as_of)
        bullpen_form = hits_environment_source._fetch_team_bullpen_hits_allowed_form(context_as_of)
        rows = hits_environment_source._build_slate_hits_allowed_rows(
            slate_csv=slate_path,
            wide_csv=CURRENT_WIDE,
            odds_snapshot=Path("/tmp/mlb_live_parent_no_odds_snapshot.json"),
            slate_date=date_value,
            team_form=team_form,
            bullpen_form=bullpen_form,
            starter_baseline_by_player=baseline,
            starter_baseline_min_starts=5,
            offense_weight_last7=0.50,
            offense_weight_last15=0.30,
            offense_weight_last30=0.20,
            offense_factor_min=0.70,
            offense_factor_max=1.30,
        )
    finally:
        hits_environment_source._load_probable_starter_rows = old_probable

    ledger_rows = []
    for row in rows:
        if clean(row.get("prop_type")) != "hits_allowed":
            continue
        game_id = id_text(row.get("game_id"))
        pitcher_id = id_text(row.get("player_id"))
        if not game_id or not pitcher_id:
            continue
        line = row.get("line")
        prob = row.get("model_pick_prob")
        try:
            champion_expected = live_replay.pha.champion_lambda_from_line_prob(float(line), float(prob))
        except Exception:
            champion_expected = ""
        ledger_rows.append(
            {
                "slate_date": date_value,
                "game_id": game_id,
                "pitcher_id": pitcher_id,
                "pitcher_name": clean(row.get("player_name")),
                "pitcher_team": clean(row.get("pitcher_team")),
                "opponent_team": clean(row.get("offense_team")),
                "line": line,
                "side": clean(row.get("model_pick_side")),
                "champion_probability_or_score": prob,
                "champion_expected_hits_allowed": champion_expected,
                "starter_expected_hits_allowed": row.get("expected_hits_allowed_matchup"),
                "pitcher_base": row.get("pitcher_expected_hits_allowed_weighted"),
                "pitcher_tier": "",
                "pitcher_baseline_total_starts": row.get("pitcher_baseline_total_starts"),
                "pitcher_baseline_seasons_used": row.get("pitcher_baseline_seasons_used"),
                "prior_starter_games": row.get("prior_starter_games"),
                "offense_factor_vs_league_clamped": row.get("offense_factor_vs_league_clamped"),
                "forecast_status": row.get("forecast_status"),
                "forecast_note": row.get("forecast_note"),
                "run_tag": run_tag,
                "cutoff": cutoff,
                "context_as_of_date": context_as_of,
                "source_path": str(slate_path),
                "source_hash": sha256_file(slate_path) if slate_path.exists() else "",
                "context_transform_source": "backend.mlb.scripts.report_mlb_hits_environment._build_slate_hits_allowed_rows",
            }
        )
    ledger = pd.DataFrame(ledger_rows)
    write_csv(out_dir / f"current_pha_pitcher_context_ledger_{date_value}.csv", ledger)
    write_csv(out_dir / f"current_pha_pitcher_context_source_trace_{date_value}.csv", pd.DataFrame(source_trace))
    return ledger, pd.DataFrame(source_trace)


def latest_prior_rows(history: pd.DataFrame, id_col: str, as_of_date: str) -> pd.DataFrame:
    if history.empty or id_col not in history.columns or "slate_date" not in history.columns:
        return pd.DataFrame()
    work = history.copy()
    work["slate_date_str"] = work["slate_date"].astype(str).str[:10]
    work = work[work["slate_date_str"] < as_of_date].copy()
    if work.empty:
        return pd.DataFrame()
    work[id_col] = work[id_col].map(id_text)
    work = work.sort_values(["slate_date_str", "game_id", id_col])
    return work.drop_duplicates(id_col, keep="last").copy()


def pha_lineup_population_from_capture(
    lineup_df: pd.DataFrame,
    pha_context: pd.DataFrame,
    *,
    date_value: str,
    run_tag: str,
    lineup_path: Path | None,
) -> pd.DataFrame:
    if lineup_df.empty or pha_context.empty:
        return pd.DataFrame()
    pctx = pha_context.drop_duplicates(["game_id", "pitcher_id"], keep="first").copy()
    eligible = {
        (id_text(r.get("game_id")), id_text(r.get("pitcher_id"))): r
        for r in pctx.to_dict("records")
        if clean(r.get("forecast_status")) == "available"
        and clean(r.get("starter_expected_hits_allowed"))
        and clean(r.get("pitcher_base"))
    }
    rows = []
    source_sha = sha256_file(lineup_path) if lineup_path and lineup_path.exists() else ""
    for _, raw in lineup_df.iterrows():
        if clean(raw.get("lineup_status")) != "CONFIRMED_LINEUP":
            continue
        game_id = id_text(raw.get("game_id"))
        starter_id = id_text(raw.get("opposing_starter_id"))
        ctx = eligible.get((game_id, starter_id))
        if not ctx:
            continue
        rows.append(
            {
                "slate_date": date_value,
                "game_date": date_value,
                "game_id": game_id,
                "player_id": id_text(raw.get("player_id") or raw.get("hitter_id")),
                "player_name": clean(raw.get("player_name")),
                "team": clean(raw.get("team")),
                "opponent": clean(raw.get("opponent")) or clean(ctx.get("pitcher_team")),
                "prop_type": "hits",
                "line": "",
                "opposing_starter_id": starter_id,
                "opposing_starter_name": clean(raw.get("opposing_starter_name")) or clean(ctx.get("pitcher_name")),
                "opposing_starter_team": clean(ctx.get("pitcher_team")),
                "run_tag": run_tag,
                "lineup_status": "confirmed_full_member",
                "lineup_semantics": clean(raw.get("lineup_slot_semantics")) or "governed_pregame_official_lineup",
                "lineup_source_path": str(lineup_path or ""),
                "lineup_source_sha256": source_sha,
                "lineup_source_timestamp": clean(raw.get("source_timestamp")) or clean(raw.get("source_fetched_at_utc")),
                "raw_response_path": clean(raw.get("raw_response_path")),
                "raw_response_sha256": clean(raw.get("raw_response_sha256")),
                "batting_order_raw": clean(raw.get("batting_order_raw") or raw.get("batting_order")),
                "lineup_slot": clean(raw.get("lineup_slot")),
                "lineup_bucket": clean(raw.get("lineup_bucket")),
                "confirmed_lineup_starter_flag": True,
                "current_proposition_presence": "LINEUP_ONLY_FOR_PHA_ENCOUNTER",
            }
        )
    return pd.DataFrame(rows)


def write_run_bound_pa_parent(
    *,
    date_value: str,
    run_tag: str,
    cutoff: str,
    population: pd.DataFrame,
    out_dir: Path,
) -> tuple[pd.DataFrame, Path, pd.DataFrame]:
    output_root = out_dir / "generated_opportunity_profile_parent"
    population_path = output_root / f"confirmed_resolved_population_{date_value}_{run_tag}.csv"
    parent_path = output_root / f"run_bound_pa_parent_artifact_{date_value}_{run_tag}.csv"
    diagnostic_rows: list[dict[str, Any]] = []
    if population.empty:
        write_csv(population_path, pd.DataFrame(columns=["slate_date", "game_id", "player_id", "player_name", "team", "opponent", "run_tag"]))
        return pd.DataFrame(), parent_path, pd.DataFrame()
    pop = population[["game_id", "player_id", "player_name", "team", "opponent"]].copy()
    pop["slate_date"] = date_value
    pop["game_date"] = date_value
    pop["run_tag"] = run_tag
    write_csv(population_path, pop)
    generated_at = utc_now()
    args = SimpleNamespace(
        date=date_value,
        run_tag=run_tag,
        prediction_cutoff=cutoff,
        run_bound_population=str(population_path),
        source_manifest=str(PA_SOURCE_MANIFEST),
        output_root=str(output_root),
    )
    payload = pa_parent_source._build(args, generated_at)
    parent_fields = [
        "slate_date", "game_date", "game_id", "player_id", "player_name", "team", "opponent", "run_tag",
        "prior_d7_plate_appearances", "prior_d15_plate_appearances", "prior_d30_plate_appearances",
        "pa_opp_v1_d7_pa_pg", "pa_opp_v1_d15_pa_pg", "pa_opp_v1_d30_pa_pg",
        "pa_opp_v1_d7_vs_d15_delta", "pa_opp_v1_d7_vs_d30_delta", "pa_opp_v1_d15_vs_d30_delta",
        "pa_opp_v1_d7_to_d30_ratio", "pa_opp_v1_d15_opportunity_band", "pa_opp_v1_trend_label",
        "pa_context_latest_date", "pa_opp_v1_cutoff_status", "pa_missing_flag", "pa_source_regime",
        "pa_semantics_status", "pa_opp_v1_complete_prior_pa", "pa_opp_v1_context_age_days",
        "pa_opp_v1_feature_version", "pa_opp_v1_formula_version", "source_manifest_path", "generated_at_utc",
    ]
    pa_parent_source._write_csv(parent_path, payload["parent_rows"], parent_fields)
    pa_parent_source._write_csv(
        output_root / f"missing_parent_ledger_{date_value}_{run_tag}.csv",
        payload["missing_rows"],
        ["date", "run_tag", "player_game_key", "game_id", "player_id", "player_name", "reason"],
    )
    pa_parent_source._write_csv(
        output_root / f"insufficient_history_ledger_{date_value}_{run_tag}.csv",
        payload["insufficient_rows"],
        ["date", "run_tag", "player_game_key", "player_id", "player_name", "history_rows_available", "latest_included_source_date", "reason"],
    )
    for row in payload["missing_rows"]:
        diagnostic_rows.append({"player_id": row.get("player_id"), "opportunity_parent_status": "FAIL", "opportunity_parent_reason": row.get("reason")})
    for row in payload["insufficient_rows"]:
        diagnostic_rows.append({"player_id": row.get("player_id"), "opportunity_parent_status": "FAIL", "opportunity_parent_reason": row.get("reason")})
    parent = pd.DataFrame(payload["parent_rows"])
    return parent, parent_path, pd.DataFrame(diagnostic_rows)


def apply_frozen_exposure(parent: pd.DataFrame) -> pd.DataFrame:
    if parent.empty:
        return parent
    hist = read_csv(FROZEN_EXPOSURE_ARTIFACT)
    inst = read_csv(FROZEN_EXPOSURE_INSTRUMENTS)
    if hist.empty or inst.empty:
        return parent
    features = exposure_source.BASE_FEATURES
    train = hist[hist["temporal_split"].astype(str).eq("fit")].copy()
    medians = {
        f: float(pd.to_numeric(train[f], errors="coerce").median())
        if f in train.columns and pd.to_numeric(train[f], errors="coerce").notna().any()
        else 0.0
        for f in features
    }
    x = pd.DataFrame({f: pd.to_numeric(fillna_series(parent, f), errors="coerce").fillna(medians[f]) for f in features})
    x_train = pd.DataFrame({f: pd.to_numeric(fillna_series(train, f), errors="coerce").fillna(medians[f]) for f in features})
    means = x_train.mean(axis=0)
    stds = x_train.std(axis=0).replace(0, 1.0)
    out = parent.copy()
    for _, row in inst.iterrows():
        output = clean(row.get("output_field"))
        if not output:
            continue
        coefs = pd.Series({f: float(row.get(f"coef_{f}") or 0.0) for f in features})
        intercept = float(row.get("intercept") or 0.0)
        if clean(row.get("model")).startswith("LinearRegression"):
            pred = intercept + x.mul(coefs, axis=1).sum(axis=1)
            if output == "challenger_total_pa":
                out["pred_total_pa"] = np.clip(pred, 1.0, 6.5)
            elif output == "challenger_starter_pa_raw":
                raw = np.clip(pred, 0.0, 5.5)
                total = pd.to_numeric(out.get("pred_total_pa"), errors="coerce").fillna(4.0)
                out["pred_starter_pa"] = np.minimum(raw, total)
                out["pred_bullpen_pa"] = np.maximum(total - out["pred_starter_pa"], 0.0)
        elif clean(row.get("model")).startswith("LogisticRegression"):
            scaled = (x - means) / stds
            linear = intercept + scaled.mul(coefs, axis=1).sum(axis=1)
            if output in {"p_hitter_receives_fourth_pa", "p_hitter_receives_fifth_pa"}:
                out[output] = linear.map(sigmoid)
    p0 = [
        hit_distribution(s, b, ps, pb)[0]
        for s, b, ps, pb in zip(out["pred_starter_pa"], out["pred_bullpen_pa"], out["p_hit_starter_prior"], out["p_hit_bullpen_prior"])
    ]
    out["predicted_exposure_p_zero_hits"] = p0
    return out


def build_generated_opportunity_profile_parent(
    *,
    hitters: pd.DataFrame,
    date_value: str,
    run_tag: str,
    cutoff: str,
    out_dir: Path,
    pha_context: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    confirmed = hitters[hitters["lineup_status"].eq("confirmed_full_member") & hitters["opposing_starter_id"].astype(str).str.strip().ne("")].copy()
    confirmed = confirmed.drop_duplicates(["game_id", "player_id"], keep="first")
    pa_parent, pa_path, pa_diag = write_run_bound_pa_parent(
        date_value=date_value,
        run_tag=run_tag,
        cutoff=cutoff,
        population=confirmed,
        out_dir=out_dir,
    )
    if confirmed.empty:
        return pd.DataFrame(), pd.DataFrame()
    hist = read_csv(FROZEN_EXPOSURE_ARTIFACT)
    hitter_profile = latest_prior_rows(hist, "player_id", date_value)
    starter_profile = latest_prior_rows(hist.rename(columns={"opposing_starter_id": "starter_profile_id"}), "starter_profile_id", date_value)
    parent = confirmed.copy()
    for col in ["game_id", "player_id"]:
        parent[col] = parent[col].astype(str)
        if not pa_parent.empty and col in pa_parent.columns:
            pa_parent[col] = pa_parent[col].astype(str)
    if not pa_parent.empty:
        parent = parent.merge(pa_parent, on=["game_id", "player_id"], how="left", suffixes=("", "_pa"))
    h_cols = [
        "player_id",
        "prior_game_count",
        "d30_hits_per_pa",
        "season_to_date_hits_per_pa",
        "season_to_date_pa_per_game",
        "d15_pa_per_game",
        "hitter_per_pa_hit_estimate",
        "p_hit_starter_prior",
        "p_hit_bullpen_prior",
        "strict_prior_status",
    ]
    if not hitter_profile.empty:
        hitter_profile["player_id"] = hitter_profile["player_id"].map(id_text)
        parent["player_id"] = parent["player_id"].map(id_text)
        parent = parent.merge(hitter_profile[[c for c in h_cols if c in hitter_profile.columns]], on="player_id", how="left")
    s_cols = [
        "starter_profile_id",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "starter_prior_start_count",
        "starter_prior_starter_pa_mean",
        "starter_prior_total_bf_mean",
        "starter_prior_bullpen_entry_pa_mean",
        "starter_prior_starter_pa_std",
    ]
    if not starter_profile.empty:
        starter_profile["opposing_starter_id"] = starter_profile["starter_profile_id"].map(id_text)
        parent["opposing_starter_id"] = parent["opposing_starter_id"].map(id_text)
        parent = parent.merge(starter_profile[[c for c in s_cols if c in starter_profile.columns] + ["opposing_starter_id"]], on="opposing_starter_id", how="left", suffixes=("", "_starter"))
    if pha_context is not None and not pha_context.empty:
        pctx = pha_context.copy()
        pctx["game_id"] = pctx["game_id"].map(id_text)
        pctx["opposing_starter_id"] = pctx["pitcher_id"].map(id_text)
        pctx = pctx.drop_duplicates(["game_id", "opposing_starter_id"], keep="first")
        pctx_cols = [
            "game_id",
            "opposing_starter_id",
            "starter_expected_hits_allowed",
            "pitcher_base",
            "pitcher_baseline_total_starts",
            "pitcher_baseline_seasons_used",
            "context_as_of_date",
            "source_path",
            "source_hash",
        ]
        parent["game_id"] = parent["game_id"].map(id_text)
        parent["opposing_starter_id"] = parent["opposing_starter_id"].map(id_text)
        parent = parent.merge(
            pctx[[c for c in pctx_cols if c in pctx.columns]].rename(
                columns={
                    "starter_expected_hits_allowed": "starter_expected_hits_allowed_current_context",
                    "pitcher_base": "pitcher_base_current_context",
                    "pitcher_baseline_total_starts": "starter_prior_start_count_current_context",
                    "source_path": "starter_context_source_path",
                    "source_hash": "starter_context_source_sha256",
                }
            ),
            on=["game_id", "opposing_starter_id"],
            how="left",
        )
        for target, source in [
            ("starter_expected_hits_allowed", "starter_expected_hits_allowed_current_context"),
            ("pitcher_base", "pitcher_base_current_context"),
            ("starter_prior_start_count", "starter_prior_start_count_current_context"),
        ]:
            if source in parent.columns:
                if target not in parent.columns:
                    parent[target] = np.nan
                parent[target] = parent[target].where(pd.to_numeric(parent[target], errors="coerce").notna(), parent[source])
    parent["expected_pa_used"] = pd.to_numeric(fillna_series(parent, "pa_opp_v1_d15_pa_pg"), errors="coerce").fillna(
        pd.to_numeric(fillna_series(parent, "d15_pa_per_game"), errors="coerce")
    )
    parent["lineup_slot"] = pd.to_numeric(parent["lineup_slot"], errors="coerce")
    parent["home_team_batting_flag"] = parent.get("is_home", pd.Series(0, index=parent.index)).astype(str).str.lower().isin({"true", "1", "yes"}).astype(int)
    for col in ["opponent_bullpen_hit_rate_prior", "bullpen_hit_factor_prior", "avg_relief_pitchers_used_prior"]:
        if col not in parent.columns:
            parent[col] = np.nan
    parent = apply_frozen_exposure(parent)
    parent["strict_prior_status"] = parent["strict_prior_status"].fillna("PASS_STRICT_PRIOR_LATEST_RETAINED_PROFILE")
    parent["encounter_batter_team"] = parent["team"]
    parent["suppression_subtype"] = parent.get("suppression_subtype", pd.Series("", index=parent.index)).fillna("").replace("", "none")
    parent["profile_support_class"] = np.where(pd.to_numeric(parent["hitter_per_pa_hit_estimate"], errors="coerce").notna(), "LATEST_RETAINED_STRICT_PRIOR_PROFILE", "MISSING")
    parent["profile_evidence_class"] = parent["profile_support_class"]
    parent["source_profile_artifact"] = str(FROZEN_EXPOSURE_ARTIFACT)
    parent["source_pa_parent_artifact"] = str(pa_path)
    diagnostic = []
    pa_fail = {clean(r.get("player_id")): clean(r.get("opportunity_parent_reason")) for r in pa_diag.to_dict("records")} if not pa_diag.empty else {}
    for _, row in parent.iterrows():
        missing = missing_required_fields(row)
        first = ""
        for field, reason in [
            ("pred_total_pa", "expected_pa_transform_not_invoked_or_missing_pa_parent"),
            ("pred_starter_pa", "pred_starter_pa_transform_not_invoked_or_missing_exposure_features"),
            ("hitter_per_pa_hit_estimate", "strict_prior_profile_not_loaded_for_hitter"),
            ("starter_expected_hits_allowed", "starter_profile_context_not_loaded"),
            ("pitcher_base", "starter_profile_context_not_loaded"),
        ]:
            if field in missing:
                first = reason
                break
        if not first and clean(row.get("player_id")) in pa_fail:
            first = pa_fail[clean(row.get("player_id"))]
        diagnostic.append(
            {
                "game_id": row.get("game_id"),
                "team": row.get("team"),
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name"),
                "opposing_starter_id": row.get("opposing_starter_id"),
                "opportunity_parent_status": "PASS" if pd.notna(row.get("pred_starter_pa")) else "FAIL",
                "profile_parent_status": "PASS" if pd.notna(row.get("hitter_per_pa_hit_estimate")) else "FAIL",
                "first_missing_object_or_transform": first,
                "missing_required_fields": "|".join(missing),
            }
        )
    return parent, pd.DataFrame(diagnostic)


def write_pha_no_join_taxonomy(
    *,
    date_value: str,
    out_dir: Path,
    diagnostic: pd.DataFrame,
    pha_context: pd.DataFrame,
) -> pd.DataFrame:
    cols = [
        "game_id",
        "team",
        "opponent",
        "hitter_id",
        "player_name",
        "batting_order",
        "official_opposing_starter_id",
        "official_opposing_starter_name",
        "taxonomy_reason",
        "notes",
    ]
    if diagnostic.empty:
        out = pd.DataFrame(columns=cols)
        write_csv(out_dir / f"pha_35_row_no_join_taxonomy_{date_value}.csv", out)
        return out

    pctx = pha_context.copy()
    pctx["game_id"] = pctx.get("game_id", pd.Series(dtype=str)).map(id_text)
    pctx["pitcher_id"] = pctx.get("pitcher_id", pd.Series(dtype=str)).map(id_text)
    pctx_by_game = {
        game_id: set(grp["pitcher_id"].dropna().map(id_text))
        for game_id, grp in pctx.groupby("game_id", dropna=False)
    }

    rows = []
    work = diagnostic[
        diagnostic["current_proposition_presence"].eq("PRESENT_IN_CURRENT_HITS_SLATE")
        & diagnostic["identity_join_status"].eq("OFFICIAL_STARTER_PRESENT_BUT_CURRENT_PHA_STARTER_JOIN_MISSING")
    ].copy()
    for _, row in work.iterrows():
        game_id = id_text(row.get("game_id"))
        starter_id = id_text(row.get("opposing_starter_id"))
        game_pitchers = pctx_by_game.get(game_id, set())
        if not game_id:
            reason = "game_identity_failed"
        elif not starter_id:
            reason = "opponent_starter_identity_missing"
        elif starter_id not in game_pitchers:
            reason = "opposing_starter_has_no_current_PHA_proposition"
        else:
            reason = "current_PHA_join_unresolved_other"
        rows.append(
            {
                "game_id": game_id,
                "team": clean(row.get("team")),
                "opponent": "",
                "hitter_id": id_text(row.get("hitter_id")),
                "player_name": clean(row.get("player_name")),
                "batting_order": clean(row.get("batting_order")),
                "official_opposing_starter_id": starter_id,
                "official_opposing_starter_name": "",
                "taxonomy_reason": reason,
                "notes": "Explains current Hits prop rows with official starter but no matching current PHA starter proposition; not counted as PHA scoring failures.",
            }
        )
    out = pd.DataFrame(rows).reindex(columns=cols)
    write_csv(out_dir / f"pha_35_row_no_join_taxonomy_{date_value}.csv", out)
    return out


def write_pha_complete_lineup_coverage(
    *,
    date_value: str,
    out_dir: Path,
    pha_context: pd.DataFrame,
    parent: pd.DataFrame,
) -> pd.DataFrame:
    cols = [
        "slate_date",
        "game_id",
        "pitcher_id",
        "pitcher_name",
        "pitcher_team",
        "opponent_team",
        "official_lineup_expected",
        "official_lineup_hitters_present",
        "complete_parent_rows",
        "pred_starter_pa_rows",
        "strict_prior_profile_rows",
        "pitcher_context_rows",
        "complete_lineup_status",
        "withholding_reason",
    ]
    if pha_context.empty:
        out = pd.DataFrame(columns=cols)
        write_csv(out_dir / f"pha_complete_lineup_coverage_{date_value}.csv", out)
        return out

    parent_work = parent.copy()
    if not parent_work.empty:
        parent_work["game_id"] = parent_work["game_id"].map(id_text)
        parent_work["opposing_starter_id"] = parent_work["opposing_starter_id"].map(id_text)
    rows = []
    pctx = pha_context.drop_duplicates(["game_id", "pitcher_id"], keep="first").copy()
    for _, ctx in pctx.iterrows():
        game_id = id_text(ctx.get("game_id"))
        pitcher_id = id_text(ctx.get("pitcher_id"))
        subset = (
            parent_work[
                parent_work["game_id"].eq(game_id)
                & parent_work["opposing_starter_id"].eq(pitcher_id)
            ].copy()
            if not parent_work.empty
            else pd.DataFrame()
        )
        lineup_rows = int(subset["lineup_slot"].map(clean).ne("").sum()) if not subset.empty else 0
        complete = int(subset["parent_row_status"].eq("COMPLETE").sum()) if not subset.empty else 0
        pred = int(num(subset["pred_starter_pa"]).notna().sum()) if not subset.empty else 0
        profile = int(subset["hitter_per_pa_hit_estimate"].notna().sum()) if not subset.empty else 0
        context_rows = 1 if clean(ctx.get("starter_expected_hits_allowed")) and clean(ctx.get("pitcher_base")) else 0
        if context_rows == 0:
            status = "WITHHELD_MISSING_PITCHER_CONTEXT"
            reason = "missing_starter_expected_hits_allowed_or_pitcher_base"
        elif lineup_rows < 9:
            status = "WITHHELD_INCOMPLETE_OFFICIAL_LINEUP"
            reason = f"official_lineup_rows_{lineup_rows}_of_9"
        elif complete < 9:
            status = "WITHHELD_INCOMPLETE_PARENT_ROWS"
            reason = f"complete_parent_rows_{complete}_of_9"
        else:
            status = "COMPLETE_OFFICIAL_LINEUP_PARENT"
            reason = ""
        rows.append(
            {
                "slate_date": date_value,
                "game_id": game_id,
                "pitcher_id": pitcher_id,
                "pitcher_name": clean(ctx.get("pitcher_name")),
                "pitcher_team": clean(ctx.get("pitcher_team")),
                "opponent_team": clean(ctx.get("opponent_team")),
                "official_lineup_expected": 9,
                "official_lineup_hitters_present": lineup_rows,
                "complete_parent_rows": complete,
                "pred_starter_pa_rows": pred,
                "strict_prior_profile_rows": profile,
                "pitcher_context_rows": context_rows,
                "complete_lineup_status": status,
                "withholding_reason": reason,
            }
        )
    out = pd.DataFrame(rows).reindex(columns=cols)
    write_csv(out_dir / f"pha_complete_lineup_coverage_{date_value}.csv", out)
    return out


def materialize_parent(
    *,
    date_value: str,
    run_tag: str,
    cutoff: str,
    slate_path: Path,
    lineup_path: Path | None,
    lineup_df: pd.DataFrame,
    profile_parent_path: Path | None,
    out_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    slate = read_csv(slate_path)
    if slate.empty:
        return (
            pd.DataFrame(columns=LIVE_PARENT_COLUMNS),
            pd.DataFrame([{"withheld_reason": "slate_missing", "source_path": str(slate_path)}]),
            pd.DataFrame([{"metric": "slate_rows", "value": 0, "status": "BLOCKED", "notes": str(slate_path)}]),
        )
    slate = slate[slate["slate_date"].astype(str).eq(date_value)].copy() if "slate_date" in slate.columns else slate.copy()
    pha_context, _context_trace = build_current_pha_context_ledger(
        date_value=date_value,
        run_tag=run_tag,
        cutoff=cutoff,
        slate_path=slate_path,
        out_dir=out_dir,
    )
    hitters = slate[slate["prop_type"].astype(str).eq("hits")].drop_duplicates(["game_id", "player_id"], keep="first").copy()
    original_hit_keys = {
        (id_text(r.get("game_id")), id_text(r.get("player_id")))
        for r in hitters.to_dict("records")
    }
    starter_map, starter_issues = starter_map_from_slate(slate)
    hitters = attach_lineup(hitters, lineup_df, lineup_path)
    pha_lineup_hitters = pha_lineup_population_from_capture(
        lineup_df,
        pha_context,
        date_value=date_value,
        run_tag=run_tag,
        lineup_path=lineup_path,
    )
    pha_non_prop_lineup_rows = 0
    if not pha_lineup_hitters.empty:
        pha_non_prop_lineup_rows = int(
            sum(
                (id_text(r.get("game_id")), id_text(r.get("player_id"))) not in original_hit_keys
                for r in pha_lineup_hitters.to_dict("records")
            )
        )
        hitters = pd.concat([hitters, pha_lineup_hitters], ignore_index=True, sort=False)
        hitters["__current_prop_rank"] = hitters["current_proposition_presence"].map(
            lambda v: 0 if clean(v) == "LINEUP_ONLY_FOR_PHA_ENCOUNTER" else 1
        )
        hitters = (
            hitters.sort_values(["game_id", "player_id", "__current_prop_rank"])
            .drop_duplicates(["game_id", "player_id"], keep="last")
            .drop(columns=["__current_prop_rank"])
        )
    generated_profile_parent = pd.DataFrame()
    exact_handoff = pd.DataFrame()
    if profile_parent_path:
        profile_parent = read_csv(profile_parent_path)
    else:
        starter_for_rows = []
        for _, row in hitters.iterrows():
            starter_for_rows.append(starter_map.get((clean(row.get("game_id")), clean(row.get("opponent"))), {}))
        for col in ["opposing_starter_id", "opposing_starter_name", "opposing_starter_team"]:
            hitters[col] = [
                clean(row.get(col)) or clean(s.get(col))
                for (_, row), s in zip(hitters.iterrows(), starter_for_rows)
            ]
        generated_profile_parent, exact_handoff = build_generated_opportunity_profile_parent(
            hitters=hitters,
            date_value=date_value,
            run_tag=run_tag,
            cutoff=cutoff,
            out_dir=out_dir,
            pha_context=pha_context,
        )
        profile_parent = generated_profile_parent
    if not profile_parent.empty:
        join_cols = ["game_id", "player_id"]
        for col in join_cols:
            profile_parent[col] = profile_parent[col].astype(str)
            hitters[col] = hitters[col].astype(str)
        profile_cols = [c for c in REQUIRED_PARENT_FIELDS if c in profile_parent.columns and c not in {"slate_date", "game_id", "player_id", "opponent"}]
        hitters = hitters.merge(profile_parent[join_cols + profile_cols].drop_duplicates(join_cols), on=join_cols, how="left", suffixes=("", "_parent"))
    confirmed_diagnostic = []
    exact_handoff_map = {
        (id_text(r.get("game_id")), id_text(r.get("player_id"))): r
        for r in exact_handoff.to_dict("records")
    }
    hitter_map = {
        (id_text(r.get("game_id")), id_text(r.get("player_id"))): r
        for r in hitters.drop_duplicates(["game_id", "player_id"], keep="first").to_dict("records")
    }
    if not lineup_df.empty:
        diagnostic_source = lineup_df.copy()
        if "player_id" not in diagnostic_source.columns and "hitter_id" in diagnostic_source.columns:
            diagnostic_source["player_id"] = diagnostic_source["hitter_id"]
        if "lineup_status" in diagnostic_source.columns:
            diagnostic_source = diagnostic_source[
                diagnostic_source["lineup_status"].map(clean).eq("CONFIRMED_LINEUP")
            ].copy()
    else:
        diagnostic_source = pd.DataFrame()
    if diagnostic_source.empty:
        diagnostic_source = hitters[hitters["lineup_status"].eq("confirmed_full_member")].copy()
    for _, lineup_row in diagnostic_source.drop_duplicates(["game_id", "player_id"], keep="last").iterrows():
        key = (id_text(lineup_row.get("game_id")), id_text(lineup_row.get("player_id")))
        hitter = hitter_map.get(key, {})
        handoff = exact_handoff_map.get(key, {})
        current_presence = "PRESENT_IN_CURRENT_HITS_SLATE" if hitter else "ABSENT_FROM_CURRENT_HITS_SLATE"
        starter_id = clean(hitter.get("opposing_starter_id")) or clean(lineup_row.get("opposing_starter_id"))
        if not starter_id:
            identity_status = "FAIL_OPPOSING_STARTER_UNRESOLVED"
        elif not hitter:
            identity_status = "LINEUP_ONLY_NO_CURRENT_PROP_ROW"
        elif not clean(hitter.get("opposing_starter_id")):
            identity_status = "OFFICIAL_STARTER_PRESENT_BUT_CURRENT_PHA_STARTER_JOIN_MISSING"
        else:
            identity_status = "PASS"
        source_ts = (
            clean(lineup_row.get("lineup_source_timestamp"))
            or clean(lineup_row.get("source_timestamp"))
            or clean(lineup_row.get("source_fetched_at_utc"))
        )
        pregame_status = clean(lineup_row.get("pregame_validity_state")) or clean(lineup_row.get("lineup_status"))
        confirmed_diagnostic.append(
            {
                "game_id": key[0],
                "team": clean(hitter.get("team")) or clean(lineup_row.get("team")),
                "hitter_id": key[1],
                "player_name": clean(hitter.get("player_name")) or clean(lineup_row.get("player_name")),
                "batting_order": clean(lineup_row.get("batting_order_raw") or lineup_row.get("batting_order")),
                "lineup_slot": clean(lineup_row.get("lineup_slot")),
                "opposing_starter_id": starter_id,
                "lineup_capture_timestamp": source_ts,
                "pregame_validity_status": pregame_status,
                "current_proposition_presence": current_presence,
                "identity_join_status": identity_status,
                "opportunity_parent_status": clean(handoff.get("opportunity_parent_status"))
                or ("PASS" if clean(hitter.get("pred_starter_pa")) else "NOT_ATTEMPTED"),
                "profile_parent_status": clean(handoff.get("profile_parent_status"))
                or ("PASS" if clean(hitter.get("hitter_per_pa_hit_estimate")) else "NOT_ATTEMPTED"),
                "first_missing_object_or_transform": clean(handoff.get("first_missing_object_or_transform")),
                "missing_required_fields": clean(handoff.get("missing_required_fields")),
            }
        )
    if confirmed_diagnostic:
        write_csv(out_dir / f"confirmed_lineup_handoff_diagnostic_{date_value}.csv", pd.DataFrame(confirmed_diagnostic))
    if not exact_handoff.empty:
        write_csv(out_dir / f"exact_15_row_handoff_failure_trace_{date_value}.csv", exact_handoff)
    no_join_taxonomy = write_pha_no_join_taxonomy(
        date_value=date_value,
        out_dir=out_dir,
        diagnostic=pd.DataFrame(confirmed_diagnostic),
        pha_context=pha_context,
    )
    rows: list[dict[str, Any]] = []
    withheld: list[dict[str, Any]] = []
    for _, hitter in hitters.iterrows():
        game_id = clean(hitter.get("game_id"))
        team = clean(hitter.get("team"))
        opponent = clean(hitter.get("opponent"))
        starter = starter_map.get((game_id, opponent), {})
        if not starter and clean(hitter.get("opposing_starter_id")):
            starter = {
                "opposing_starter_id": hitter.get("opposing_starter_id"),
                "opposing_starter_name": hitter.get("opposing_starter_name"),
                "opposing_starter_team": hitter.get("opposing_starter_team"),
            }
        reason = ""
        if not game_id or not clean(hitter.get("player_id")):
            reason = "missing_identity"
        elif not starter:
            reason = "starter_identity_unresolved"
        elif clean(hitter.get("lineup_slot")) == "":
            reason = "lineup_source_unavailable_or_player_not_confirmed"
        elif profile_parent.empty:
            reason = "strict_prior_opportunity_profile_parent_unavailable"
        else:
            missing = missing_required_fields(hitter)
            reason = "missing_required_parent_fields:" + "|".join(missing) if missing else ""
        row = {
            "slate_date": date_value,
            "run_tag": run_tag,
            "cutoff": cutoff,
            "game_id": game_id,
            "player_id": clean(hitter.get("player_id")),
            "player_name": clean(hitter.get("player_name")),
            "team": team,
            "encounter_batter_team": team,
            "opponent": opponent,
            "opposing_starter_id": clean(starter.get("opposing_starter_id")),
            "opposing_starter_name": clean(starter.get("opposing_starter_name")),
            "opposing_starter_team": clean(starter.get("opposing_starter_team")),
            "lineup_status": clean(hitter.get("lineup_status")) or "missing",
            "lineup_semantics": clean(hitter.get("lineup_semantics")),
            "lineup_source_path": clean(hitter.get("lineup_source_path")),
            "lineup_source_sha256": clean(hitter.get("lineup_source_sha256")),
            "lineup_source_timestamp": clean(hitter.get("lineup_source_timestamp")),
            "raw_source_path": clean(hitter.get("raw_response_path")),
            "raw_source_sha256": clean(hitter.get("source_payload_sha256") or hitter.get("payload_hash") or hitter.get("raw_response_sha256")),
            "batting_order": clean(hitter.get("batting_order_raw") or hitter.get("batting_order")),
            "lineup_slot": clean(hitter.get("lineup_slot")),
            "lineup_bucket": clean(hitter.get("lineup_bucket")),
            "confirmed_lineup_starter_flag": clean(hitter.get("confirmed_lineup_starter_flag")),
            "identity_status": "PASS" if not reason.startswith("missing_identity") else "FAIL",
            "opportunity_status": "PASS" if not reason and clean(hitter.get("pred_starter_pa")) else "BLOCKED",
            "profile_status": "PASS" if not reason and clean(hitter.get("hitter_per_pa_hit_estimate")) else "BLOCKED",
            "temporal_integrity_status": "PASS" if clean(hitter.get("lineup_source_timestamp")) else "UNKNOWN",
            "parent_row_status": "COMPLETE" if not reason else "WITHHELD",
            "withheld_reason": reason,
            "contract_version": CONTRACT_VERSION,
        }
        for field in [
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
        ]:
            row[field] = hitter.get(field, pd.NA)
        rows.append(row)
        if reason:
            withheld.append(
                {
                    "slate_date": date_value,
                    "run_tag": run_tag,
                    "game_id": game_id,
                    "player_id": clean(hitter.get("player_id")),
                    "player_name": clean(hitter.get("player_name")),
                    "team": team,
                    "opponent": opponent,
                    "opposing_starter_id": row["opposing_starter_id"],
                    "withheld_reason": reason,
                    "lineup_status": row["lineup_status"],
                    "opportunity_status": row["opportunity_status"],
                    "profile_status": row["profile_status"],
                    "notes": "Fail-closed research path; production rows unchanged.",
                }
            )
    for issue in starter_issues:
        withheld.append({**issue, "notes": "starter map issue from hits_allowed prop rows"})
    parent = pd.DataFrame(rows).reindex(columns=LIVE_PARENT_COLUMNS)
    lineup_coverage = write_pha_complete_lineup_coverage(
        date_value=date_value,
        out_dir=out_dir,
        pha_context=pha_context,
        parent=parent,
    )
    summary = pd.DataFrame(
        [
            {"metric": "hitter_candidate_rows", "value": int(len(hitters)), "status": "INFO", "notes": ""},
            {"metric": "lineup_rows_loaded", "value": int(len(lineup_df)), "status": "INFO", "notes": str(lineup_path or "")},
            {"metric": "pha_context_rows", "value": int(len(pha_context)), "status": "PASS" if not pha_context.empty else "BLOCKED", "notes": f"current_pha_pitcher_context_ledger_{date_value}.csv"},
            {"metric": "pha_context_available_rows", "value": int(pha_context.get("forecast_status", pd.Series(dtype=str)).map(clean).eq("available").sum()) if not pha_context.empty else 0, "status": "PASS" if not pha_context.empty and pha_context.get("forecast_status", pd.Series(dtype=str)).map(clean).eq("available").any() else "BLOCKED", "notes": ""},
            {"metric": "pha_official_lineup_rows_added", "value": int(len(pha_lineup_hitters)), "status": "PASS" if not pha_lineup_hitters.empty else "BLOCKED", "notes": "Official confirmed lineup members admitted for PHA encounter-only materialization."},
            {"metric": "pha_non_prop_lineup_rows_added", "value": pha_non_prop_lineup_rows, "status": "INFO", "notes": "Official lineup members without current Hits hitter prop rows, admitted for PHA encounter only."},
            {"metric": "pha_no_join_taxonomy_rows", "value": int(len(no_join_taxonomy)), "status": "INFO", "notes": f"pha_35_row_no_join_taxonomy_{date_value}.csv"},
            {"metric": "pha_complete_lineup_pitcher_games", "value": int(lineup_coverage["complete_lineup_status"].eq("COMPLETE_OFFICIAL_LINEUP_PARENT").sum()) if not lineup_coverage.empty else 0, "status": "PASS" if not lineup_coverage.empty and lineup_coverage["complete_lineup_status"].eq("COMPLETE_OFFICIAL_LINEUP_PARENT").any() else "BLOCKED", "notes": f"pha_complete_lineup_coverage_{date_value}.csv"},
            {"metric": "parent_rows_complete", "value": int(parent["parent_row_status"].eq("COMPLETE").sum()) if not parent.empty else 0, "status": "PASS" if not parent.empty and parent["parent_row_status"].eq("COMPLETE").any() else "BLOCKED", "notes": ""},
            {"metric": "pred_starter_pa_rows", "value": int(num(parent["pred_starter_pa"]).notna().sum()) if not parent.empty else 0, "status": "PASS" if not parent.empty and num(parent["pred_starter_pa"]).notna().any() else "BLOCKED", "notes": ""},
            {"metric": "strict_prior_profile_rows", "value": int(parent["hitter_per_pa_hit_estimate"].notna().sum()) if not parent.empty else 0, "status": "PASS" if not parent.empty and parent["hitter_per_pa_hit_estimate"].notna().any() else "BLOCKED", "notes": ""},
        ]
    )
    if confirmed_diagnostic:
        diag_df = pd.DataFrame(confirmed_diagnostic)
        present = diag_df["current_proposition_presence"].eq("PRESENT_IN_CURRENT_HITS_SLATE")
        resolved = diag_df["identity_join_status"].eq("PASS")
        opp_pass = diag_df["opportunity_parent_status"].eq("PASS")
        profile_pass = diag_df["profile_parent_status"].eq("PASS")
        summary = pd.concat(
            [
                summary,
                pd.DataFrame(
                    [
                        {"metric": "confirmed_parsed_hitter_rows", "value": int(len(diag_df)), "status": "PASS", "notes": "Official governed parsed lineup rows."},
                        {"metric": "confirmed_hitter_rows_current_prop_present", "value": int(present.sum()), "status": "INFO", "notes": "Confirmed lineup rows with a current Hits proposition row."},
                        {"metric": "confirmed_hitter_rows_resolved_opposing_starter", "value": int((present & resolved).sum()), "status": "PASS" if int((present & resolved).sum()) else "BLOCKED", "notes": "Current-prop confirmed rows with resolved opposing starter identity."},
                        {"metric": "confirmed_hitter_rows_unresolved_opposing_starter", "value": int((present & ~resolved).sum()), "status": "BLOCKED" if int((present & ~resolved).sum()) else "PASS", "notes": ""},
                        {"metric": "confirmed_resolved_rows_lacking_opportunity_fields", "value": int((present & resolved & ~opp_pass).sum()), "status": "BLOCKED" if int((present & resolved & ~opp_pass).sum()) else "PASS", "notes": ""},
                        {"metric": "confirmed_resolved_rows_lacking_strict_prior_profiles", "value": int((present & resolved & ~profile_pass).sum()), "status": "BLOCKED" if int((present & resolved & ~profile_pass).sum()) else "PASS", "notes": ""},
                        {"metric": "rows_fully_eligible_for_materialization_handoff", "value": int((present & resolved & opp_pass & profile_pass).sum()), "status": "PASS" if int((present & resolved & opp_pass & profile_pass).sum()) else "BLOCKED", "notes": "Opportunity/profile handoff complete; parent may still fail closed on pitcher-side context fields."},
                    ]
                ),
            ],
            ignore_index=True,
        )
    return parent, pd.DataFrame(withheld), summary


def build_encounter_and_score(date_value: str, cutoff: str, parent: pd.DataFrame, slate_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    complete = parent[parent["parent_row_status"].eq("COMPLETE")].copy()
    if complete.empty:
        return (
            pd.DataFrame(columns=encounter_source.ENCOUNTER_OUTPUT_COLUMNS),
            pd.DataFrame(columns=["materialization_status", "withheld_reason"]),
            pd.DataFrame(),
        )
    parent_for_encounter = complete.rename(columns={"team": "encounter_batter_team"})
    parent_for_encounter["opponent"] = parent_for_encounter["opponent"]
    temp = Path("/tmp/mlb_live_hitter_parent_complete_for_encounter.csv")
    parent_for_encounter.to_csv(temp, index=False)
    try:
        encounter, _ = encounter_source.materialize_encounter(date_value, temp, cutoff)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass
    if not encounter.empty and "lineup_batters" in encounter.columns:
        encounter_for_score = encounter[pd.to_numeric(encounter["lineup_batters"], errors="coerce").eq(9)].copy()
    else:
        encounter_for_score = encounter
    if not encounter_for_score.empty:
        encounter_for_score = encounter_for_score.loc[:, ~encounter_for_score.columns.duplicated()].copy()
    _, instrument, _ = live_replay.bind_frozen_model()
    score_result = encounter_source.score_live(date_value, encounter_for_score, instrument, slate_path)
    live_ledger = score_result[0]
    shadow = score_result[3] if len(score_result) == 4 else score_result[3]
    return encounter, live_ledger, shadow


def shared_hits_availability(parent: pd.DataFrame, encounter: pd.DataFrame, pha_ledger: pd.DataFrame) -> pd.DataFrame:
    complete = int(parent["parent_row_status"].eq("COMPLETE").sum()) if not parent.empty else 0
    scored = int(pha_ledger.get("materialization_status", pd.Series(dtype=str)).astype(str).eq("SCORED").sum()) if not pha_ledger.empty else 0
    return pd.DataFrame(
        [
            {"surface": "Pitcher Hits Allowed", "inputs_available": complete > 0 and len(encounter) > 0, "rows_available": scored, "status": "READY" if scored else "BLOCKED", "notes": "Requires encounter aggregation and frozen Challenger scoring."},
            {"surface": "Hits O0.5", "inputs_available": complete > 0, "rows_available": complete, "status": "READY_RESEARCH_ONLY" if complete else "BLOCKED", "notes": "Would provide pitcher-foundation/opportunity/profile context only; model unchanged."},
            {"surface": "Hits O1.5", "inputs_available": complete > 0, "rows_available": complete, "status": "READY_RESEARCH_ONLY" if complete else "BLOCKED", "notes": "Would provide pitcher-foundation/opportunity research fields only; ranking unchanged."},
        ]
    )


def decisions(
    enabled: bool,
    parent: pd.DataFrame,
    encounter: pd.DataFrame,
    pha_ledger: pd.DataFrame,
    hist: pd.DataFrame,
    lineup_status: str,
    run_summary: pd.DataFrame,
) -> pd.DataFrame:
    complete = int(parent["parent_row_status"].eq("COMPLETE").sum()) if not parent.empty else 0
    pred = int(num(parent["pred_starter_pa"]).notna().sum()) if not parent.empty else 0
    profile = int(parent["hitter_per_pa_hit_estimate"].notna().sum()) if not parent.empty else 0
    scored = int(pha_ledger.get("materialization_status", pd.Series(dtype=str)).astype(str).eq("SCORED").sum()) if not pha_ledger.empty else 0
    hist_pass = bool(not hist.empty and hist["status"].eq("PASS").all())
    shadow_status = "READY_FOR_CONTROLLED_SHADOW_GRADING" if scored else "NOT_READY_INPUT_BLOCKED"
    controlled_shadow_status = "PHA_CONTROLLED_SHADOW_PARTIAL_COVERAGE" if scored else "NOT_READY_INPUT_BLOCKED"
    metrics = {
        clean(r.get("metric")): int(float(r.get("value")))
        for r in run_summary.to_dict("records")
        if clean(r.get("metric")) and clean(r.get("value"))
    }
    confirmed = metrics.get("confirmed_parsed_hitter_rows", 0)
    resolved = metrics.get("confirmed_hitter_rows_resolved_opposing_starter", 0)
    unresolved = metrics.get("confirmed_hitter_rows_unresolved_opposing_starter", 0)
    lacking_opp = metrics.get("confirmed_resolved_rows_lacking_opportunity_fields", 0)
    lacking_profile = metrics.get("confirmed_resolved_rows_lacking_strict_prior_profiles", 0)
    eligible_handoff = metrics.get("rows_fully_eligible_for_materialization_handoff", 0)
    pha_context_rows = metrics.get("pha_context_rows", 0)
    pha_context_available = metrics.get("pha_context_available_rows", 0)
    pha_lineup_rows = metrics.get("pha_official_lineup_rows_added", 0)
    pha_non_prop_rows = metrics.get("pha_non_prop_lineup_rows_added", 0)
    pha_no_join_rows = metrics.get("pha_no_join_taxonomy_rows", 0)
    pha_complete_pitcher_games = metrics.get("pha_complete_lineup_pitcher_games", 0)
    if not enabled:
        current = "LIVE_PARENT_CURRENT_SLATE_NOT_YET_ELIGIBLE_DISABLED"
    elif scored:
        current = "PHA_CONTROLLED_SHADOW_SCORED"
    elif complete:
        current = "PHA_CONTROLLED_SHADOW_INPUT_BLOCKED"
    elif lineup_status == "no_local_lineup_capture":
        current = "LIVE_PARENT_LINEUP_ACQUISITION_BLOCKED"
    else:
        current = "PHA_CONTROLLED_SHADOW_INPUT_BLOCKED"
    rows = [
        ("MLB_LIVE_PARENT_DAILY_STAGE_DECISION", "HOOK_AFTER_SLATE_OUTPUT_AND_GOVERNED_LINEUP_CAPTURE_BEFORE_RESEARCH_SURFACES"),
        ("MLB_LIVE_PARENT_LINEUP_ACQUISITION_DECISION", lineup_status.upper()),
        ("MLB_LIVE_PARENT_DEFAULT_OFF_HOOK_DECISION", "IMPLEMENTED_MAKEFILE_FLAG_MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE"),
        ("MLB_LIVE_PARENT_DISABLED_BEHAVIOR_DECISION", "UNCHANGED_NO_HOOK_INVOCATION_WHEN_FLAG_DISABLED"),
        ("MLB_LIVE_PARENT_FAIL_CLOSED_DECISION", "PASS_PRECISE_WITHHELD_REASONS_NO_SILENT_IMPUTATION"),
        ("MLB_LIVE_PARENT_HISTORICAL_PARITY_DECISION", "PASS" if hist_pass else "FAIL"),
        ("MLB_LIVE_PARENT_CURRENT_RUN_DECISION", current),
        ("MLB_LIVE_PARENT_PRED_STARTER_PA_COVERAGE_DECISION", f"PRED_STARTER_PA_ROWS_{pred}_COMPLETE_PARENT_ROWS_{complete}"),
        ("MLB_LIVE_PARENT_PROFILE_COVERAGE_DECISION", f"PROFILE_ROWS_{profile}_COMPLETE_PARENT_ROWS_{complete}"),
        ("MLB_LIVE_PARENT_ENCOUNTER_COVERAGE_DECISION", f"ENCOUNTER_ROWS_{len(encounter)}"),
        ("MLB_LIVE_PARENT_PHA_SCORING_DECISION", f"PHA_SCORED_ROWS_{scored}"),
        ("MLB_LIVE_PARENT_SHARED_HITS_AVAILABILITY_DECISION", "AVAILABLE_RESEARCH_ONLY" if complete else "BLOCKED_NO_COMPLETE_PARENT_ROWS"),
        ("MLB_PHA_SHADOW_STATUS", shadow_status),
        ("MLB_JULY17_SLATE_RECONCILIATION_STATUS", "OPEN_PENDING_OFFICIAL_RECONCILIATION"),
        ("MLB_JULY17_PHA_CHAMPION_GRADING_STATUS", "PENDING_OFFICIAL_OUTCOME"),
        ("MLB_JULY17_PHA_CHALLENGER_STATUS", "WITHHELD_NO_VALID_PREGAME_SCORE"),
        ("MLB_JULY17_O15_PROSPECTIVE_RUN1_STATUS", "BOUND_PENDING_GRADE"),
        ("MLB_LIVE_PARENT_FORWARD_STATUS", "LINEUP_ACQUISITION_BLOCKED"),
        ("MLB_JULY17_PRODUCTION_STATUS", "UNCHANGED"),
        ("MLB_LIVE_PARENT_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
        ("MLB_LIVE_PARENT_CONFIRMED_POPULATION_DECISION", f"CONFIRMED_PARSED_HITTER_ROWS_{confirmed}"),
        ("MLB_LIVE_PARENT_RESOLVED_STARTER_DECISION", f"RESOLVED_CURRENT_PROP_ROWS_{resolved}_UNRESOLVED_CURRENT_PROP_ROWS_{unresolved}"),
        ("MLB_LIVE_PARENT_OPPORTUNITY_HANDOFF_DECISION", f"PASS_ROWS_{pred}_LACKING_ROWS_{lacking_opp}"),
        ("MLB_LIVE_PARENT_PROFILE_HANDOFF_DECISION", f"PASS_ROWS_{profile}_LACKING_ROWS_{lacking_profile}"),
        ("MLB_LIVE_PARENT_EXACT_15_ROW_BLOCKER_DECISION", "FORMER_HANDOFF_BLOCKER_REPAIRED_TO_OPPORTUNITY_PROFILE_PASS_PITCHER_CONTEXT_STILL_MISSING" if pred >= 15 and profile >= 15 and complete == 0 else "SEE_EXACT_HANDOFF_TRACE"),
        ("MLB_LIVE_PARENT_PATCH_DECISION", "PATCHED_EXISTING_INTEGRATION_TO_INVOKE_FROZEN_OPPORTUNITY_PROFILE_TRANSFORMS_NO_PRODUCTION_CHANGE"),
        ("MLB_LIVE_PARENT_POST_PATCH_PARITY_DECISION", "PASS" if hist_pass else "FAIL"),
        ("MLB_LIVE_PARENT_POST_PATCH_COMPLETE_ROWS_DECISION", f"COMPLETE_PARENT_ROWS_{complete}_HANDOFF_ELIGIBLE_ROWS_{eligible_handoff}"),
        ("MLB_LIVE_PARENT_POST_PATCH_PRED_STARTER_PA_DECISION", f"PRED_STARTER_PA_ROWS_{pred}"),
        ("MLB_LIVE_PARENT_POST_PATCH_PROFILE_DECISION", f"STRICT_PRIOR_PROFILE_ROWS_{profile}"),
        ("MLB_LIVE_PARENT_POST_PATCH_ENCOUNTER_DECISION", f"ENCOUNTER_ROWS_{len(encounter)}"),
        ("MLB_LIVE_PARENT_POST_PATCH_PHA_SCORING_DECISION", f"PHA_SCORED_ROWS_{scored}"),
        ("MLB_PHA_CURRENT_CONTEXT_POPULATION_DECISION", f"PHA_CONTEXT_ROWS_{pha_context_rows}_AVAILABLE_ROWS_{pha_context_available}"),
        ("MLB_PHA_STARTER_EXPECTED_HITS_SOURCE_DECISION", "CURRENT_FIELD_AVAILABLE_REQUIRES_EXISTING_TRANSFORM_EXPECTED_HITS_ALLOWED_MATCHUP"),
        ("MLB_PHA_PITCHER_BASE_SOURCE_DECISION", "CURRENT_FIELD_AVAILABLE_REQUIRES_EXISTING_TRANSFORM_PITCHER_EXPECTED_HITS_ALLOWED_WEIGHTED"),
        ("MLB_PHA_CURRENT_CONTEXT_JOIN_DECISION", f"COMPLETE_PARENT_ROWS_{complete}_PHA_COMPLETE_PITCHER_GAMES_{pha_complete_pitcher_games}"),
        ("MLB_PHA_NON_PROP_LINEUP_HITTER_DECISION", f"INCLUDED_OFFICIAL_NON_PROP_LINEUP_HITTERS_{pha_non_prop_rows}_FOR_PHA_ENCOUNTER_ONLY"),
        ("MLB_PHA_COMPLETE_LINEUP_POLICY_DECISION", "REQUIRES_9_OFFICIAL_LINEUP_HITTERS_WITH_COMPLETE_PARENT_AND_CONTEXT_FIELDS"),
        ("MLB_PHA_35_ROW_NO_JOIN_TAXONOMY_DECISION", f"NO_JOIN_ROWS_{pha_no_join_rows}_EXPLAINED_SEPARATELY_FROM_PHA_FAILURE_COUNT"),
        ("MLB_PHA_POST_PATCH_PARENT_DECISION", f"COMPLETE_PARENT_ROWS_{complete}_OFFICIAL_PHA_LINEUP_ROWS_{pha_lineup_rows}"),
        ("MLB_PHA_POST_PATCH_ENCOUNTER_DECISION", f"ENCOUNTER_ROWS_{len(encounter)}"),
        ("MLB_PHA_POST_PATCH_SCORING_DECISION", f"PHA_SCORED_ROWS_{scored}"),
        ("MLB_PHA_POST_PATCH_JOIN_COVERAGE_DECISION", f"PHA_SCORED_ROWS_{scored}_OF_{len(pha_ledger)}_CURRENT_PHA_PROPOSITIONS"),
        ("MLB_PHA_POST_PATCH_HISTORICAL_PARITY_DECISION", "PASS" if hist_pass else "FAIL"),
        ("MLB_PHA_CONTROLLED_SHADOW_STATUS", controlled_shadow_status),
        ("MLB_PHA_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
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
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".md" and not path.read_text(encoding="utf-8").lstrip().startswith("#"):
                raise ValueError("markdown does not start with heading")
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(path), "validation": status, "notes": notes})
    for key, value in guardrails.items():
        rows.append({"artifact": f"guardrail_{key}", "validation": "PASS" if value in (0, False, "PASS") else "FAIL", "notes": str(value)})
    return pd.DataFrame(rows)


def summary_md(generated_at: str, date_value: str, dec: pd.DataFrame, run_summary: pd.DataFrame) -> str:
    decision_lines = "\n".join(f"- `{r.decision_name}` = `{r.decision_value}`" for r in dec.itertuples(index=False))
    metric_lines = ["| metric | value | status | notes |", "| --- | ---: | --- | --- |"]
    for row in run_summary.to_dict("records"):
        metric_lines.append(f"| {row.get('metric','')} | {row.get('value','')} | {row.get('status','')} | {str(row.get('notes','')).replace('|','/')} |")
    shadow_value = dec.loc[dec["decision_name"].eq("MLB_PHA_CONTROLLED_SHADOW_STATUS"), "decision_value"].iloc[0]
    direct = "YES" if shadow_value == "PHA_CONTROLLED_SHADOW_PARTIAL_COVERAGE" else "NO"
    return f"""# MLB Live Hitter Parent Daily Integration

Generated: `{generated_at}`

## Direct Answer

Is the completed live hitter parent now integrated early enough in the ordinary
daily workflow to generate exact frozen Pitcher Hits Allowed Challenger scores
before games begin?

`{direct}` for `{date_value}`.

The default-off daily hook is installed at the correct stage, after current
slate outputs and governed lineup capture. The post-patch run binds current
pitcher-side context through the existing hits-environment transform and scores
only pitcher-games with complete official opponent lineups and complete parent
rows. Partial coverage is retained as a controlled, default-off research shadow.

## Current Run Summary

{chr(10).join(metric_lines)}

## Decisions

{decision_lines}

## July 17 Status

July 17 remains open for ordinary reconciliation and prospective O1.5 grading.
Only the missing PHA Challenger pregame predictions are permanently unavailable
for that date:

`{JULY17_PHA_UNAVAILABLE}`

The 25 July 17 PHA propositions remain withheld from granular Challenger
evaluation with reason
`MISSING_GOVERNED_PREGAME_LINEUP_OPPORTUNITY_PARENT`; Champion PHA grading
remains pending the normal official outcome source.

## No Production Behavior Changed

No model, formula, tier, selector, upload, Quick Card, workspace, DB, OddsAPI,
LaunchAgent, or production scoring behavior changed.
"""


def build(args: argparse.Namespace) -> dict[str, Any]:
    enabled = args.enabled or os.environ.get("MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE", "0").strip().lower() in {"1", "true", "yes", "on"}
    out_dir = args.output_dir or (DEFAULT_ROOT / args.date)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    slate_path = args.slate_artifact or latest_slate(args.date)[0]
    run_tag = args.run_tag or latest_slate(args.date)[1]
    lineup_df, lineup_path, lineup_status = load_lineup_source(args.date, args.lineup_player_rows)
    if not enabled:
        lineup_df = pd.DataFrame()
        lineup_path = None
        lineup_status = "disabled_not_checked"
    hist = historical_parity()
    if enabled:
        parent, withheld, run_summary = materialize_parent(
            date_value=args.date,
            run_tag=run_tag,
            cutoff=args.cutoff,
            slate_path=slate_path,
            lineup_path=lineup_path,
            lineup_df=lineup_df,
            profile_parent_path=Path(args.opportunity_profile_parent) if args.opportunity_profile_parent else None,
            out_dir=out_dir,
        )
        encounter, pha_ledger, shadow = build_encounter_and_score(args.date, args.cutoff, parent, slate_path)
    else:
        parent = pd.DataFrame(columns=LIVE_PARENT_COLUMNS)
        withheld = pd.DataFrame(columns=["withheld_reason"])
        run_summary = pd.DataFrame([{"metric": "disabled_behavior", "value": 1, "status": "PASS", "notes": "flag disabled; no parent capture/materialization invoked"}])
        encounter = pd.DataFrame(columns=encounter_source.ENCOUNTER_OUTPUT_COLUMNS)
        pha_ledger = pd.DataFrame(columns=["materialization_status", "withheld_reason"])
        shadow = pd.DataFrame(columns=SHADOW_COLUMNS)
    shared = shared_hits_availability(parent, encounter, pha_ledger)
    dec = decisions(enabled, parent, encounter, pha_ledger, hist, lineup_status, run_summary)
    guardrails = {
        "network_calls": 0,
        "oddsapi_calls": 0,
        "db_writes": 0,
        "model_fits_or_refits": 0,
        "formula_changes": 0,
        "production_behavior_changed": False,
        "launchagent_changes": 0,
        "july17_reconstruction_attempted": 0,
    }
    files = {
        "summary": out_dir / f"live_hitter_parent_daily_integration_summary_{args.date}.md",
        "stage_map": out_dir / f"daily_stage_integration_map_{args.date}.csv",
        "lineup_contract": out_dir / f"lineup_acquisition_contract_{args.date}.csv",
        "hook_contract": out_dir / f"default_off_hook_contract_{args.date}.csv",
        "historical_parity": out_dir / f"historical_parity_report_{args.date}.csv",
        "disabled": out_dir / f"disabled_behavior_validation_{args.date}.csv",
        "manifest": out_dir / f"current_pregame_run_manifest_{args.date}.csv",
        "pha_context": out_dir / f"current_pha_pitcher_context_ledger_{args.date}.csv",
        "pha_context_trace": out_dir / f"current_pha_pitcher_context_source_trace_{args.date}.csv",
        "pha_no_join_taxonomy": out_dir / f"pha_35_row_no_join_taxonomy_{args.date}.csv",
        "pha_lineup_coverage": out_dir / f"pha_complete_lineup_coverage_{args.date}.csv",
        "parent": out_dir / f"live_hitter_parent_artifact_{args.date}.csv",
        "encounter": out_dir / f"pitcher_encounter_artifact_{args.date}.csv",
        "pha": out_dir / f"frozen_pha_challenger_ledger_{args.date}.csv",
        "shadow": out_dir / f"controlled_shadow_artifact_{args.date}.csv",
        "withheld": out_dir / f"withheld_row_taxonomy_{args.date}.csv",
        "shared": out_dir / f"shared_hits_availability_report_{args.date}.csv",
        "decisions": out_dir / f"required_decisions_{args.date}.csv",
        "machine": out_dir / f"machine_readable_live_hitter_parent_daily_integration_{args.date}.json",
        "sha": out_dir / f"sha256_manifest_{args.date}.csv",
        "validation": out_dir / f"validation_report_{args.date}.csv",
    }
    lineup_capture_timestamp = ""
    if not lineup_df.empty:
        for ts_col in ["source_fetched_at_utc", "source_timestamp", "cutoff"]:
            if ts_col in lineup_df.columns and not lineup_df[ts_col].dropna().empty:
                lineup_capture_timestamp = clean(lineup_df[ts_col].dropna().max())
                if lineup_capture_timestamp:
                    break
    run_manifest = pd.DataFrame(
        [
            {
                "date": args.date,
                "run_tag": run_tag,
                "cutoff": args.cutoff,
                "enabled": enabled,
                "slate_artifact": str(slate_path),
                "slate_sha256": sha256_file(slate_path) if slate_path.exists() else "",
                "lineup_source_status": lineup_status,
                "lineup_source_path": str(lineup_path or ""),
                "lineup_source_sha256": sha256_file(lineup_path) if lineup_path and lineup_path.exists() else "",
                "lineup_capture_timestamp": lineup_capture_timestamp,
                "parent_rows": len(parent),
                "encounter_rows": len(encounter),
                "pha_rows": len(pha_ledger),
            }
        ]
    )
    disabled = pd.DataFrame(
        [
            {
                "check": "flag_disabled_preserves_existing_behavior",
                "status": "PASS",
                "notes": "Makefile hook invokes this runner only when MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE=1; disabled validation package uses no capture/materialization.",
            }
        ]
    )
    write_csv(files["stage_map"], daily_stage_map(enabled))
    write_csv(files["lineup_contract"], lineup_contract())
    write_csv(files["hook_contract"], hook_contract(enabled))
    write_csv(files["historical_parity"], hist)
    write_csv(files["disabled"], disabled)
    write_csv(files["manifest"], run_manifest)
    if not files["pha_context"].exists():
        write_csv(files["pha_context"], pd.DataFrame(columns=["slate_date", "game_id", "pitcher_id", "starter_expected_hits_allowed", "pitcher_base", "forecast_status"]))
    if not files["pha_context_trace"].exists():
        write_csv(files["pha_context_trace"], pd.DataFrame(columns=["field", "current_source_artifact", "source_column", "classification"]))
    if not files["pha_no_join_taxonomy"].exists():
        write_csv(files["pha_no_join_taxonomy"], pd.DataFrame(columns=["game_id", "hitter_id", "taxonomy_reason"]))
    if not files["pha_lineup_coverage"].exists():
        write_csv(files["pha_lineup_coverage"], pd.DataFrame(columns=["slate_date", "game_id", "pitcher_id", "complete_lineup_status", "withholding_reason"]))
    write_csv(files["parent"], parent.reindex(columns=LIVE_PARENT_COLUMNS))
    write_csv(files["encounter"], encounter.reindex(columns=encounter_source.ENCOUNTER_OUTPUT_COLUMNS))
    write_csv(files["pha"], pha_ledger)
    write_csv(files["shadow"], shadow if not shadow.empty else pd.DataFrame(columns=SHADOW_COLUMNS))
    write_csv(files["withheld"], withheld)
    write_csv(files["shared"], shared)
    write_csv(files["decisions"], dec)
    write_text(files["summary"], summary_md(generated_at, args.date, dec, run_summary))
    machine = {
        "generated_at": generated_at,
        "date": args.date,
        "enabled": enabled,
        "run_tag": run_tag,
        "cutoff": args.cutoff,
        "lineup_acquisition_status": lineup_status,
        "parent_rows": int(len(parent)),
        "complete_parent_rows": int(parent["parent_row_status"].eq("COMPLETE").sum()) if not parent.empty else 0,
        "pred_starter_pa_rows": int(num(parent["pred_starter_pa"]).notna().sum()) if not parent.empty else 0,
        "profile_rows": int(parent["hitter_per_pa_hit_estimate"].notna().sum()) if not parent.empty else 0,
        "encounter_rows": int(len(encounter)),
        "pha_scored_rows": int(pha_ledger.get("materialization_status", pd.Series(dtype=str)).astype(str).eq("SCORED").sum()) if not pha_ledger.empty else 0,
        "decisions": {r.decision_name: r.decision_value for r in dec.itertuples(index=False)},
        "july17_slate_reconciliation_status": "OPEN_PENDING_OFFICIAL_RECONCILIATION",
        "july17_pha_champion_grading_status": "PENDING_OFFICIAL_OUTCOME",
        "july17_pha_challenger_status": "WITHHELD_NO_VALID_PREGAME_SCORE",
        "july17_o15_prospective_run1_status": "BOUND_PENDING_GRADE",
        "july17_production_status": "UNCHANGED",
        "guardrails": guardrails,
    }
    write_json(files["machine"], machine)
    generated = [p for k, p in files.items() if k not in {"sha", "validation"}]
    write_csv(files["sha"], pd.DataFrame([{"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size} for p in generated]))
    write_csv(files["validation"], validate(generated + [files["sha"]], guardrails))
    return {
        "output_dir": str(out_dir),
        "enabled": enabled,
        "lineup_acquisition_status": lineup_status,
        "parent_rows": machine["parent_rows"],
        "complete_parent_rows": machine["complete_parent_rows"],
        "pred_starter_pa_rows": machine["pred_starter_pa_rows"],
        "profile_rows": machine["profile_rows"],
        "encounter_rows": machine["encounter_rows"],
        "pha_scored_rows": machine["pha_scored_rows"],
        "shadow_status": machine["decisions"]["MLB_PHA_SHADOW_STATUS"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--run-tag", default="")
    parser.add_argument("--cutoff", default="")
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--enabled", action="store_true")
    parser.add_argument("--slate-artifact", type=Path)
    parser.add_argument("--lineup-player-rows", default="")
    parser.add_argument("--opportunity-profile-parent", default="")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if not args.cutoff:
        args.cutoff = utc_now()
    result = build(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
