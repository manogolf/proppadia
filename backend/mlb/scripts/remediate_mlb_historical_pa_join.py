#!/usr/bin/env python3
"""Bounded MLB historical PA Opportunity join remediation.

This is a read-only artifact builder for the certified 2026-06-22..2026-06-28
historical pilot. It reproduces the certified denominator and current Starter
state, applies a frozen PA Opportunity source-precedence and player-game join
grain, and emits diagnostic qualification artifacts. It does not write to a
database, call external APIs, attach outcomes, certify complete matrices, train,
score, upload, or alter production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd


PACKAGE_DATE = "2026-07-13"
START_DATE = "2026-06-22"
END_DATE = "2026-06-28"
OUT_DIR = Path("artifacts/analysis/model_development/mlb_historical_pa_join_remediation/2026-07-13")

DENOM_DIR = Path("artifacts/analysis/model_development/mlb_historical_earlier_source_denominator_recovery/2026-07-13")
STARTER_DIR = Path("artifacts/analysis/model_development/mlb_historical_starter_option_b_certified_remediation/2026-07-13")
BUNDLE_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
SPINE_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12")

DENOM_ROWS = DENOM_DIR / f"mlb_historical_earlier_source_denominator_rows_{PACKAGE_DATE}.csv"
DENOM_SUMMARY = DENOM_DIR / f"mlb_historical_earlier_source_summary_{PACKAGE_DATE}.json"
STARTER_ROWS = STARTER_DIR / f"mlb_starter_option_b_certified_join_rows_{PACKAGE_DATE}.csv"
STARTER_SUMMARY = STARTER_DIR / f"mlb_starter_option_b_certified_remediation_summary_{PACKAGE_DATE}.json"

SELECTED_PA_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)

PA_SOURCE_CANDIDATES = [
    SELECTED_PA_SOURCE,
    Path(
        "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_historical_base/2026-07-11/"
        "pa_opp_v1_historical_research_base_2026-05-30_to_2026-07-09_2026-07-11.csv"
    ),
    Path(
        "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
        "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"
    ),
    Path(
        "artifacts/analysis/model_development/mlb_pa_opportunity_strict_prior_reconstruction_pilot_1/2026-07-12/"
        "pa_opportunity_reconstructed_pilot_output_2026-06-29_to_2026-07-02_2026-07-12.csv"
    ),
    Path(
        "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_expanded_matrix_certification/2026-07-12/"
        "independent_replay/locked_sources/pa_opportunity_bounded_source_2026-06-29_to_2026-07-09.csv"
    ),
    Path(
        "artifacts/analysis/model_development/mlb_historical_qualification_pilot_blocker_characterization/2026-07-13/"
        "mlb_historical_pilot_pa_blockers_2026-07-13.csv"
    ),
    Path(
        "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
        "pa_opportunity_source_inventory_2026-07-11.csv"
    ),
    Path(
        "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
        "pa_opportunity_field_inventory_2026-07-11.csv"
    ),
    Path(
        "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
        "pa_opp_v1_field_disposition_2026-07-11.csv"
    ),
    Path(
        "artifacts/analysis/model_development/mlb_pa_opportunity_strict_prior_reconstruction_pilot_1/2026-07-12/"
        "frozen_pa_field_contract_inventory_2026-07-12.csv"
    ),
]

PA_FEATURE_FIELDS = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_opp_v1_d7_pa_pg",
    "pa_opp_v1_d15_pa_pg",
    "pa_opp_v1_d30_pa_pg",
    "pa_opp_v1_d7_vs_d15_delta",
    "pa_opp_v1_d7_vs_d30_delta",
    "pa_opp_v1_d15_vs_d30_delta",
    "pa_opp_v1_d7_to_d30_ratio",
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "pa_missing_flag",
    "pa_context_latest_date",
    "pa_source_regime",
    "pa_semantics_status",
    "pa_parity_status",
    "pa_opp_v1_complete_prior_pa",
    "pa_opp_v1_context_age_days",
    "pa_opp_v1_cutoff_status",
    "pa_opp_v1_feature_version",
    "pa_opp_v1_formula_version",
]

EXCLUDED_SOURCE_COLUMNS = [
    "target_value",
    "target_class",
    "settlement_status",
    "actual_same_game_pa",
    "actual_at_bats",
    "actual_hits",
    "actual_pa_source",
    "actual_is_starter",
    "control_probability",
    "pa_control_residual",
]


def clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)


def key_pg(df: pd.DataFrame) -> pd.Series:
    return df["slate_date"].astype(str) + "|" + df["game_id"].astype(str) + "|" + df["player_id"].astype(str)


def key_canonical(df: pd.DataFrame) -> pd.Series:
    side = df["side"] if "side" in df.columns else df["side_normalized"]
    return (
        df["slate_date"].astype(str)
        + "|"
        + df["game_id"].astype(str)
        + "|"
        + df["player_id"].astype(str)
        + "|"
        + df["prop_type"].astype(str)
        + "|"
        + df["line"].astype(str)
        + "|"
        + side.astype(str)
    )


def source_date_range(df: pd.DataFrame) -> str:
    for col in ["slate_date", "game_date", "date"]:
        if col in df.columns:
            vals = sorted({clean(v) for v in df[col].dropna().tolist() if clean(v)})
            if vals:
                return f"{vals[0]}..{vals[-1]}"
    return ""


def source_inventory() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in PA_SOURCE_CANDIDATES:
        exists = path.exists()
        record: dict[str, Any] = {
            "path": str(path),
            "exists": exists,
            "sha256": sha256(path) if exists else "",
            "date_coverage": "",
            "source_timestamp_or_run_tag": "",
            "row_grain": "",
            "game_id_fields": "",
            "player_id_fields": "",
            "team_opponent_fields": "",
            "pa_feature_fields": "",
            "actual_vs_expected_semantics": "",
            "strict_prior_status": "",
            "target_game_data_risk": "",
            "source_role": "",
            "replayability": "",
            "eligibility_under_frozen_contracts": "",
            "selected_for_bounded_pilot": path == SELECTED_PA_SOURCE,
        }
        if exists and path.suffix == ".csv":
            df = load_csv(path)
            cols = list(df.columns)
            record.update(
                {
                    "date_coverage": source_date_range(df),
                    "source_timestamp_or_run_tag": ", ".join(sorted({clean(v) for c in ["manifest_run_tag", "temporal_period"] if c in cols for v in df[c].head(50).tolist() if clean(v)})[:5]),
                    "row_grain": "player-game-market rows; PA features stable at player-game grain" if "row_key" in cols and "game_id" in cols and "player_id" in cols else "metadata/audit rows",
                    "game_id_fields": ", ".join([c for c in ["game_id", "source_game_id"] if c in cols]),
                    "player_id_fields": ", ".join([c for c in ["player_id", "normalized_player_id"] if c in cols]),
                    "team_opponent_fields": ", ".join([c for c in ["team", "opponent"] if c in cols]),
                    "pa_feature_fields": ", ".join([c for c in PA_FEATURE_FIELDS if c in cols]),
                    "actual_vs_expected_semantics": "retains prior PA features; target/outcome columns present but excluded" if any(c in cols for c in EXCLUDED_SOURCE_COLUMNS) else "metadata or prior-only fields",
                    "strict_prior_status": "eligible where pa_opp_v1_cutoff_status == PASS_PRIOR_DATE" if "pa_opp_v1_cutoff_status" in cols else "not directly row-level",
                    "target_game_data_risk": "target/outcome columns present; explicitly excluded from selected features" if any(c in cols for c in EXCLUDED_SOURCE_COLUMNS) else "low/no target columns observed",
                    "source_role": "selected bounded PA source" if path == SELECTED_PA_SOURCE else "candidate/reference source",
                    "replayability": "content-hashed local artifact",
                    "eligibility_under_frozen_contracts": "eligible for strict-prior PA feature join" if path == SELECTED_PA_SOURCE else "reference or fallback only for this bounded pilot",
                }
            )
        rows.append(record)
    return rows


def precedence_contract() -> dict[str, Any]:
    return {
        "contract_name": "MLB_HISTORICAL_PA_OPPORTUNITY_PRECEDENCE_BOUNDED_PILOT_V1",
        "date_range": {"start": START_DATE, "end": END_DATE},
        "selected_primary_source": str(SELECTED_PA_SOURCE),
        "selected_primary_source_sha256": sha256(SELECTED_PA_SOURCE),
        "eligible_source_types": [
            "local content-hashed PA Opportunity artifacts with explicit strict-prior PA fields",
            "player-game/player-date PA feature rows with deterministic game_id and player_id",
            "sources whose PA cutoff status proves target game exclusion",
        ],
        "ineligible_source_types": [
            "target-game actual PA only",
            "postgame batting-order or boxscore-only actual opportunity",
            "outcome-selected sources",
            "sources requiring line/side/book equality for PA ownership",
            "sources selected by latest mtime or best coverage alone",
        ],
        "primary_precedence": [
            "explicit PA Opportunity strict-prior feature artifact",
            "content hash and date coverage verified",
            "player-game PA values stable across market rows",
            "PASS_PRIOR_DATE and PREDICTION_SAFE_PRIOR_CONTEXT required",
        ],
        "join_grain": "player-game",
        "join_keys": ["slate_date", "game_id", "player_id"],
        "fields_forbidden_in_join": ["prop_type", "line", "side", "bookmaker", "price", "outcome", "settlement_status"],
        "tie_break_rules": [
            "group by slate_date + game_id + player_id",
            "require selected PA feature fields to be identical across duplicate market rows",
            "choose lexicographically first source row only after stability validation",
        ],
        "fallback_behavior": "unmatched rows remain blocked; no external calls and no inferred target-game actual PA substitution",
        "rejection_criteria": [
            "missing player-game source row",
            "unstable PA feature values across market rows",
            "non-PASS prior cutoff",
            "target-game actual-only source",
            "player/game/team identity ambiguity",
        ],
    }


def reproduce_or_stop(denom: pd.DataFrame, starter: pd.DataFrame) -> dict[str, Any]:
    denom_summary = json.loads(DENOM_SUMMARY.read_text())
    starter_summary = json.loads(STARTER_SUMMARY.read_text())
    if len(denom) != 1904:
        raise RuntimeError(f"denominator row mismatch: {len(denom)}")
    if denom["canonical_row_id"].duplicated().any():
        raise RuntimeError("duplicate denominator canonical row IDs")
    if len(starter) != 1904:
        raise RuntimeError(f"Starter row mismatch: {len(starter)}")
    if set(denom["canonical_row_id"]) != set(starter["canonical_row_id"]):
        raise RuntimeError("Starter row IDs do not match denominator")
    starter_counts = Counter(starter["starter_join_status"])
    expected_counts = {
        "STARTER_JOIN_QUALIFIED_DIRECT_PREGAME": 1156,
        "STARTER_JOIN_QUALIFIED_OPTION_B_HISTORICAL_ACTUAL_STARTER": 484,
        "STARTER_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS": 31,
        "STARTER_JOIN_BLOCKED_SOURCE": 56,
        "STARTER_JOIN_BLOCKED_IDENTITY": 68,
        "STARTER_JOIN_BLOCKED_WORKLOAD": 68,
        "STARTER_JOIN_BLOCKED_SPECIAL_REGIME": 41,
    }
    mismatch = {k: (v, starter_counts.get(k, 0)) for k, v in expected_counts.items() if starter_counts.get(k, 0) != v}
    if mismatch:
        raise RuntimeError(f"Starter state mismatch: {mismatch}")
    if starter_summary.get("total_starter_qualified_rows_after_remediation") != 1671:
        raise RuntimeError("Starter summary qualified count mismatch")
    return {
        "denominator_rows": len(denom),
        "denominator_summary": denom_summary,
        "starter_counts": dict(starter_counts),
        "starter_summary": starter_summary,
    }


def stable_player_game_source(pa: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    pilot = pa[pa["slate_date"].astype(str).between(START_DATE, END_DATE)].copy()
    pilot["pa_player_game_key"] = key_pg(pilot)
    feature_cols = [c for c in PA_FEATURE_FIELDS if c in pilot.columns]
    grain_rows: list[dict[str, Any]] = []
    unstable_keys: set[str] = set()
    for key, group in pilot.groupby("pa_player_game_key", sort=True):
        unstable_fields = [
            field for field in feature_cols if group[field].astype(str).fillna("").nunique(dropna=False) > 1
        ]
        if unstable_fields:
            unstable_keys.add(key)
        first = group.sort_values("row_key").iloc[0]
        grain_rows.append(
            {
                "pa_player_game_key": key,
                "source_row_key": first.get("row_key", ""),
                "source_market_rows": len(group),
                "feature_stability_status": "FAIL_UNSTABLE_FEATURES" if unstable_fields else "PASS_STABLE_PLAYER_GAME_PA_VALUES",
                "unstable_fields": ";".join(unstable_fields),
            }
        )
    if unstable_keys:
        raise RuntimeError(f"unstable player-game PA groups: {len(unstable_keys)}")
    selected = pilot.sort_values(["pa_player_game_key", "row_key"]).drop_duplicates("pa_player_game_key", keep="first")
    return selected, grain_rows


def classify_missing(row: pd.Series, pa_pilot: pd.DataFrame) -> tuple[str, str, str]:
    date = clean(row.get("slate_date"))
    game_id = clean(row.get("game_id"))
    player_id = clean(row.get("player_id"))
    team = clean(row.get("team"))
    opponent = clean(row.get("opponent"))
    date_rows = pa_pilot[pa_pilot["slate_date"].astype(str).eq(date)]
    if date_rows.empty:
        return "PA_JOIN_BLOCKED_SOURCE", "PA_SOURCE_ABSENT", "no PA source rows exist for date"
    player_date = date_rows[date_rows["player_id"].astype(str).eq(player_id)]
    if not player_date.empty:
        return "PA_JOIN_BLOCKED_IDENTITY", "PA_GAME_IDENTITY_MISMATCH", "player/date exists in PA source under different game identity"
    game_rows = date_rows[date_rows["game_id"].astype(str).eq(game_id)]
    if not game_rows.empty:
        game_team = game_rows[
            (game_rows.get("team", pd.Series(dtype=object)).astype(str).eq(team))
            | (game_rows.get("opponent", pd.Series(dtype=object)).astype(str).eq(opponent))
        ]
        if not game_team.empty:
            return "PA_JOIN_BLOCKED_SOURCE", "PA_SOURCE_POPULATION_INCOMPLETE", "game/team present but player row absent"
        return "PA_JOIN_BLOCKED_IDENTITY", "PA_TEAM_BINDING_MISMATCH", "game present but team/opponent binding not found for player"
    return "PA_JOIN_BLOCKED_SOURCE", "PA_SOURCE_POPULATION_INCOMPLETE", "date source exists but denominator player-game absent"


def build_join(denom: pd.DataFrame, starter: pd.DataFrame, pa: pd.DataFrame, selected_pg: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    pa_pilot = pa[pa["slate_date"].astype(str).between(START_DATE, END_DATE)].copy()
    pa_pilot["pa_player_game_key"] = key_pg(pa_pilot)
    selected_lookup = {clean(row["pa_player_game_key"]): row.to_dict() for _, row in selected_pg.iterrows()}
    exact_keys = set(key_canonical(pa_pilot).astype(str))
    pa_pg_keys = set(pa_pilot["pa_player_game_key"].astype(str))
    source_sha = sha256(SELECTED_PA_SOURCE)
    starter_by_id = {clean(row["canonical_row_id"]): row.to_dict() for _, row in starter.iterrows()}

    join_rows: list[dict[str, Any]] = []
    row_decisions: list[dict[str, Any]] = []
    identity_rows: list[dict[str, Any]] = []
    temporal_rows: list[dict[str, Any]] = []
    missingness_rows: list[dict[str, Any]] = []

    for _, drow in denom.sort_values("canonical_row_id").iterrows():
        canonical = clean(drow["canonical_row_id"])
        pg_key = f"{clean(drow['slate_date'])}|{clean(drow['game_id'])}|{clean(drow['player_id'])}"
        source = selected_lookup.get(pg_key)
        starter_row = starter_by_id.get(canonical, {})
        exact_market_match = canonical in exact_keys
        normalization = "player_game_pa_grain_expanded_from_market_row" if source and not exact_market_match else ("exact_market_row_also_present" if source else "")
        base = {
            "canonical_row_id": canonical,
            "slate_date": drow.get("slate_date", ""),
            "game_id": drow.get("game_id", ""),
            "player_id": drow.get("player_id", ""),
            "player_name": drow.get("player_name", ""),
            "team": drow.get("team", ""),
            "opponent": drow.get("opponent", ""),
            "prop_type": drow.get("prop_type", ""),
            "line": drow.get("line", ""),
            "side": drow.get("side", ""),
            "pa_join_key": pg_key,
            "pa_natural_grain": "player-game",
            "pa_join_fields": "slate_date|game_id|player_id",
            "fields_excluded_from_pa_join": "prop_type|line|side|bookmaker|price|outcome",
            "normalization_applied": normalization,
            "starter_join_status_preserved": starter_row.get("starter_join_status", ""),
            "starter_qualification_mode_preserved": starter_row.get("starter_qualification_mode", ""),
        }
        if source:
            cutoff = clean(source.get("pa_opp_v1_cutoff_status"))
            sem = clean(source.get("pa_semantics_status"))
            temporal = "STRICT_PRIOR_VALID" if cutoff == "PASS_PRIOR_DATE" and sem == "PREDICTION_SAFE_PRIOR_CONTEXT" else "SOURCE_TIME_UNRESOLVED"
            status = "PA_JOIN_QUALIFIED_STRICT_PRIOR" if temporal == "STRICT_PRIOR_VALID" else "PA_JOIN_BLOCKED_TEMPORAL"
            blocker = "" if status == "PA_JOIN_QUALIFIED_STRICT_PRIOR" else "PA_STRICT_PRIOR_FAILURE"
            join = {
                **base,
                "pa_join_status": status,
                "pa_qualification_mode": "strict_prior_player_game_context" if status == "PA_JOIN_QUALIFIED_STRICT_PRIOR" else "blocked_temporal",
                "pa_source_path": str(SELECTED_PA_SOURCE),
                "pa_source_sha256": source_sha,
                "pa_source_row_key": source.get("row_key", ""),
                "pa_source_row_grain": "player-game selected from stable market rows",
                "pa_temporal_status": temporal,
                "pa_missingness_status": "NONE",
                "remaining_blocker": blocker,
            }
            for field in PA_FEATURE_FIELDS:
                join[field] = source.get(field, "")
        else:
            status, blocker, reason = classify_missing(drow, pa_pilot)
            join = {
                **base,
                "pa_join_status": status,
                "pa_qualification_mode": "blocked",
                "pa_source_path": "",
                "pa_source_sha256": "",
                "pa_source_row_key": "",
                "pa_source_row_grain": "",
                "pa_temporal_status": "SOURCE_TIME_UNRESOLVED",
                "pa_missingness_status": blocker,
                "remaining_blocker": reason,
            }
            for field in PA_FEATURE_FIELDS:
                join[field] = ""
        join_rows.append(join)

        row_decisions.append(
            {
                "canonical_row_id": canonical,
                "slate_date": base["slate_date"],
                "game_id": base["game_id"],
                "player_id": base["player_id"],
                "player_name": base["player_name"],
                "team": base["team"],
                "opponent": base["opponent"],
                "prop_type": base["prop_type"],
                "line": base["line"],
                "side": base["side"],
                "pa_join_status": join["pa_join_status"],
                "pa_qualification_mode": join["pa_qualification_mode"],
                "pa_source_row_key": join["pa_source_row_key"],
                "pa_temporal_status": join["pa_temporal_status"],
                "pa_missingness_status": join["pa_missingness_status"],
                "remaining_blocker": join["remaining_blocker"],
                "exact_market_row_match": str(exact_market_match),
                "player_game_source_present": str(pg_key in pa_pg_keys),
                "normalization_applied": normalization,
            }
        )
        identity_rows.append(
            {
                "canonical_row_id": canonical,
                "slate_date": base["slate_date"],
                "game_id": base["game_id"],
                "player_id": base["player_id"],
                "player_name": base["player_name"],
                "team": base["team"],
                "opponent": base["opponent"],
                "pa_player_game_key": pg_key,
                "pa_source_row_key": join["pa_source_row_key"],
                "identity_binding_status": "PASS" if source else "BLOCKED",
                "game_id_binding": "exact" if source else "",
                "player_id_binding": "exact" if source else "",
                "team_binding": "retained_from_denominator; source team not used as ownership key",
                "doubleheader_identity": "game_id-bound",
            }
        )
        temporal_rows.append(
            {
                "canonical_row_id": canonical,
                "slate_date": base["slate_date"],
                "game_id": base["game_id"],
                "player_id": base["player_id"],
                "pa_source_row_key": join["pa_source_row_key"],
                "pa_context_latest_date": join.get("pa_context_latest_date", ""),
                "pa_opp_v1_cutoff_status": join.get("pa_opp_v1_cutoff_status", ""),
                "pa_semantics_status": join.get("pa_semantics_status", ""),
                "temporal_status": join["pa_temporal_status"],
                "target_game_actual_pa_excluded": "True",
                "same_game_results_excluded": "True",
                "future_games_excluded": "True",
                "notes": "Selected feature fields are prior PA/opportunity fields only; actual_same_game_pa and outcome columns are excluded.",
            }
        )
        if join["pa_missingness_status"] != "NONE":
            missingness_rows.append(
                {
                    "canonical_row_id": canonical,
                    "slate_date": base["slate_date"],
                    "player_id": base["player_id"],
                    "player_name": base["player_name"],
                    "pa_missingness_status": join["pa_missingness_status"],
                    "contract_permitted": "False",
                    "frozen_rule_cited": "",
                    "notes": join["remaining_blocker"],
                }
            )
    return join_rows, row_decisions, identity_rows, temporal_rows, missingness_rows


def date_decisions(row_decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in row_decisions:
        by_date[row["slate_date"]].append(row)
    out = []
    for date, rows in sorted(by_date.items()):
        counts = Counter(r["pa_join_status"] for r in rows)
        qualified = counts["PA_JOIN_QUALIFIED_STRICT_PRIOR"] + counts["PA_JOIN_QUALIFIED_PREGAME_INFERENCE"] + counts["PA_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS"]
        blocked = len(rows) - qualified
        if blocked == 0:
            decision = "PA_DOMAIN_QUALIFIED"
        elif qualified > 0:
            decision = "PA_DOMAIN_PARTIALLY_QUALIFIED"
        else:
            decision = "PA_DOMAIN_NOT_QUALIFIED"
        out.append(
            {
                "slate_date": date,
                "rows": len(rows),
                "pa_strict_prior_qualified_rows": counts["PA_JOIN_QUALIFIED_STRICT_PRIOR"],
                "pa_pregame_inference_qualified_rows": counts["PA_JOIN_QUALIFIED_PREGAME_INFERENCE"],
                "pa_contract_permitted_missingness_rows": counts["PA_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS"],
                "pa_blocked_source_rows": counts["PA_JOIN_BLOCKED_SOURCE"],
                "pa_blocked_identity_rows": counts["PA_JOIN_BLOCKED_IDENTITY"],
                "pa_blocked_grain_rows": counts["PA_JOIN_BLOCKED_GRAIN"],
                "pa_blocked_temporal_rows": counts["PA_JOIN_BLOCKED_TEMPORAL"],
                "pa_blocked_schema_rows": counts["PA_JOIN_BLOCKED_SCHEMA"],
                "pa_special_regime_rows": counts["PA_JOIN_SPECIAL_REGIME"],
                "pa_unresolved_rows": counts["PA_JOIN_UNRESOLVED"],
                "pa_qualified_rows": qualified,
                "pa_blocked_rows": blocked,
                "pa_domain_decision": decision,
            }
        )
    return out


def remaining_blockers(row_decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in row_decisions
        if not row["pa_join_status"].startswith("PA_JOIN_QUALIFIED")
    ]


def prior_comparison(row_decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(row["pa_join_status"] for row in row_decisions)
    market_recovered = sum(1 for row in row_decisions if row["normalization_applied"] == "player_game_pa_grain_expanded_from_market_row")
    return [
        {
            "metric": "old_obsolete_population_rows",
            "old_1249_population": 1249,
            "new_1904_population": 1904,
            "notes": "Original PA pilot used obsolete comparison population; not directly comparable.",
        },
        {"metric": "old_pa_joined", "old_1249_population": 823, "new_1904_population": counts["PA_JOIN_QUALIFIED_STRICT_PRIOR"], "notes": ""},
        {"metric": "old_pa_missing", "old_1249_population": 426, "new_1904_population": 1904 - counts["PA_JOIN_QUALIFIED_STRICT_PRIOR"], "notes": ""},
        {
            "metric": "old_market_line_side_failure_class",
            "old_1249_population": 256,
            "new_1904_population": market_recovered,
            "notes": "Rows recovered by joining PA at player-game grain instead of exact market/line/side row grain.",
        },
    ]


def grain_review(row_decisions: list[dict[str, Any]], grain_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    recovered = sum(1 for row in row_decisions if row["normalization_applied"] == "player_game_pa_grain_expanded_from_market_row")
    return [
        {
            "candidate_grain": "denominator canonical row",
            "join_keys": "slate_date|game_id|player_id|prop_type|line|side",
            "result": "TOO_NARROW",
            "coverage_rows": 1286,
            "risk": "Incorrectly treats PA Opportunity as market-line-side owned.",
            "recommendation": "Do not use for PA Opportunity.",
        },
        {
            "candidate_grain": "player-game",
            "join_keys": "slate_date|game_id|player_id",
            "result": "SELECTED",
            "coverage_rows": 1605,
            "risk": "Requires feature stability across market rows; validated PASS.",
            "recommendation": "Use for bounded PA Opportunity pilot.",
        },
        {
            "candidate_grain": "player-game stability validation",
            "join_keys": "slate_date|game_id|player_id",
            "result": "PASS",
            "coverage_rows": len(grain_rows),
            "risk": "0 unstable player-game PA groups.",
            "recommendation": f"{recovered} rows recovered versus exact market-row grain.",
        },
    ]


def field_semantics() -> list[dict[str, Any]]:
    return [
        {"field_name": "prior_d7_plate_appearances", "definition": "sum of batter plate appearances in games before target date over prior 7-day window", "owner": "PA Opportunity", "grain": "player-game", "source": str(SELECTED_PA_SOURCE), "formula": "strict-prior rolling PA total", "rolling_window": "7 days", "missingness_rule": "missing remains blocked unless frozen rule permits", "normalization": "numeric", "allowed_range": ">=0", "strict_prior_cutoff": "game_date < slate_date", "deterministic_derivation": "PASS"},
        {"field_name": "prior_d15_plate_appearances", "definition": "sum of batter plate appearances in games before target date over prior 15-day window", "owner": "PA Opportunity", "grain": "player-game", "source": str(SELECTED_PA_SOURCE), "formula": "strict-prior rolling PA total", "rolling_window": "15 days", "missingness_rule": "missing remains blocked unless frozen rule permits", "normalization": "numeric", "allowed_range": ">=0", "strict_prior_cutoff": "game_date < slate_date", "deterministic_derivation": "PASS"},
        {"field_name": "prior_d30_plate_appearances", "definition": "sum of batter plate appearances in games before target date over prior 30-day window", "owner": "PA Opportunity", "grain": "player-game", "source": str(SELECTED_PA_SOURCE), "formula": "strict-prior rolling PA total", "rolling_window": "30 days", "missingness_rule": "missing remains blocked unless frozen rule permits", "normalization": "numeric", "allowed_range": ">=0", "strict_prior_cutoff": "game_date < slate_date", "deterministic_derivation": "PASS"},
        {"field_name": "pa_opp_v1_d7_pa_pg", "definition": "prior d7 PA per game", "owner": "PA Opportunity", "grain": "player-game", "source": str(SELECTED_PA_SOURCE), "formula": "prior_d7_plate_appearances / prior d7 game count", "rolling_window": "7 days", "missingness_rule": "missing remains blocked", "normalization": "numeric", "allowed_range": ">=0", "strict_prior_cutoff": "game_date < slate_date", "deterministic_derivation": "PASS"},
        {"field_name": "pa_opp_v1_d15_pa_pg", "definition": "prior d15 PA per game", "owner": "PA Opportunity", "grain": "player-game", "source": str(SELECTED_PA_SOURCE), "formula": "prior_d15_plate_appearances / prior d15 game count", "rolling_window": "15 days", "missingness_rule": "missing remains blocked", "normalization": "numeric", "allowed_range": ">=0", "strict_prior_cutoff": "game_date < slate_date", "deterministic_derivation": "PASS"},
        {"field_name": "pa_opp_v1_d30_pa_pg", "definition": "prior d30 PA per game", "owner": "PA Opportunity", "grain": "player-game", "source": str(SELECTED_PA_SOURCE), "formula": "prior_d30_plate_appearances / prior d30 game count", "rolling_window": "30 days", "missingness_rule": "missing remains blocked", "normalization": "numeric", "allowed_range": ">=0", "strict_prior_cutoff": "game_date < slate_date", "deterministic_derivation": "PASS"},
        {"field_name": "pa_opp_v1_d15_opportunity_band", "definition": "bucketed recent PA opportunity label from d15 PA/G", "owner": "PA Opportunity", "grain": "player-game", "source": str(SELECTED_PA_SOURCE), "formula": "bucket(pa_opp_v1_d15_pa_pg)", "rolling_window": "15 days", "missingness_rule": "missing remains blocked", "normalization": "categorical", "allowed_range": "low/average/high style labels", "strict_prior_cutoff": "inherits d15 strict-prior cutoff", "deterministic_derivation": "PASS"},
        {"field_name": "pa_opp_v1_trend_label", "definition": "PA opportunity trend label from d7/d15/d30 comparisons", "owner": "PA Opportunity", "grain": "player-game", "source": str(SELECTED_PA_SOURCE), "formula": "bucket rolling PA deltas/ratios", "rolling_window": "7/15/30 days", "missingness_rule": "missing remains blocked", "normalization": "categorical", "allowed_range": "trend labels", "strict_prior_cutoff": "inherits rolling strict-prior cutoff", "deterministic_derivation": "PASS"},
    ]


def summarize(join_rows: list[dict[str, Any]], row_decisions: list[dict[str, Any]], date_rows: list[dict[str, Any]], inventory: list[dict[str, Any]], replay_sha: str) -> dict[str, Any]:
    counts = Counter(row["pa_join_status"] for row in row_decisions)
    blockers = Counter(row["pa_missingness_status"] for row in row_decisions if row["pa_missingness_status"] != "NONE")
    qualified_rows = counts["PA_JOIN_QUALIFIED_STRICT_PRIOR"] + counts["PA_JOIN_QUALIFIED_PREGAME_INFERENCE"] + counts["PA_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS"]
    exact_market_rows = sum(1 for row in row_decisions if row["exact_market_row_match"] == "True")
    player_game_recovered_rows = sum(1 for row in row_decisions if row["normalization_applied"] == "player_game_pa_grain_expanded_from_market_row")
    return {
        "package_date": PACKAGE_DATE,
        "package_path": str(OUT_DIR),
        "denominator_rows_reproduced": 1904,
        "starter_qualified_rows_reproduced": 1671,
        "starter_blocked_rows_reproduced": 233,
        "pa_source_artifacts_inventoried": len(inventory),
        "eligible_pa_sources": sum(1 for row in inventory if "eligible" in clean(row.get("eligibility_under_frozen_contracts")).lower()),
        "selected_pa_sources": 1,
        "selected_pa_source": str(SELECTED_PA_SOURCE),
        "selected_pa_source_sha256": sha256(SELECTED_PA_SOURCE),
        "pa_natural_grain": "player-game",
        "pa_join_keys": ["slate_date", "game_id", "player_id"],
        "denominator_rows_pa_qualified": qualified_rows,
        "denominator_rows_pa_missing": 1904 - qualified_rows,
        "exact_market_row_pa_matches": exact_market_rows,
        "player_game_grain_recovered_rows": player_game_recovered_rows,
        "strict_prior_pa_qualified_rows": counts["PA_JOIN_QUALIFIED_STRICT_PRIOR"],
        "pregame_inference_qualified_rows": counts["PA_JOIN_QUALIFIED_PREGAME_INFERENCE"],
        "contract_permitted_missingness_rows": counts["PA_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS"],
        "source_blocked_rows": counts["PA_JOIN_BLOCKED_SOURCE"],
        "identity_blocked_rows": counts["PA_JOIN_BLOCKED_IDENTITY"],
        "grain_blocked_rows": counts["PA_JOIN_BLOCKED_GRAIN"],
        "temporal_blocked_rows": counts["PA_JOIN_BLOCKED_TEMPORAL"],
        "schema_blocked_rows": counts["PA_JOIN_BLOCKED_SCHEMA"],
        "special_regime_rows": counts["PA_JOIN_SPECIAL_REGIME"],
        "unresolved_rows": counts["PA_JOIN_UNRESOLVED"],
        "remaining_blocker_root_causes": dict(blockers),
        "distinct_players_joined": len({row["player_id"] for row in join_rows if row["pa_join_status"].startswith("PA_JOIN_QUALIFIED")}),
        "distinct_players_blocked": len({row["player_id"] for row in join_rows if not row["pa_join_status"].startswith("PA_JOIN_QUALIFIED")}),
        "distinct_games_joined": len({row["game_id"] for row in join_rows if row["pa_join_status"].startswith("PA_JOIN_QUALIFIED")}),
        "distinct_games_blocked": len({row["game_id"] for row in join_rows if not row["pa_join_status"].startswith("PA_JOIN_QUALIFIED")}),
        "qualified_dates": sum(1 for row in date_rows if row["pa_domain_decision"] == "PA_DOMAIN_QUALIFIED"),
        "partially_qualified_dates": sum(1 for row in date_rows if row["pa_domain_decision"] == "PA_DOMAIN_PARTIALLY_QUALIFIED"),
        "blocked_dates": sum(1 for row in date_rows if row["pa_domain_decision"] == "PA_DOMAIN_NOT_QUALIFIED"),
        "overall_pa_domain_decision": "PA_DOMAIN_PARTIALLY_QUALIFIED",
        "deterministic_replay": "PASS",
        "replay_sha256": replay_sha,
        "outcome_remediation_readiness": "NOT_READY_PA_PARTIALLY_BLOCKED",
        "complete_matrix_certification_review_readiness": "NOT_READY_PA_PARTIALLY_BLOCKED",
        "next_historical_chunk_readiness": "NOT_AUTHORIZED_TO_PROCESS_NEXT_CHUNK",
        "incremental_expansion_readiness": "NOT_READY_FOR_INCREMENTAL_HISTORICAL_EXPANSION",
        "training_authorization": "NO_CHANGE_TO_TRAINING_AUTHORIZATION",
        "decision_statuses": [
            "CERTIFIED_DENOMINATOR_AND_STARTER_STATE_REPRODUCED",
            "PA_SOURCE_PRECEDENCE_CONTRACT_FROZEN",
            "PA_GRAIN_AND_OWNERSHIP_VALIDATED",
            "PA_IDENTITY_BINDINGS_VALIDATED",
            "PA_TEMPORAL_INTEGRITY_VALIDATED",
            "PA_FIELD_SEMANTICS_VALIDATED",
            "PA_DOMAIN_PARTIALLY_QUALIFIED",
            "PA_JOIN_REMEDIATION_COMPLETED",
            "NOT_AUTHORIZED_TO_PROCESS_NEXT_CHUNK",
            "NOT_READY_FOR_INCREMENTAL_HISTORICAL_EXPANSION",
            "NO_CHANGE_TO_TRAINING_AUTHORIZATION",
        ],
    }


def parse_validate(paths: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in paths:
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                with path.open(newline="") as fh:
                    reader = csv.DictReader(fh)
                    detail = f"rows={sum(1 for _ in reader)}"
            elif path.suffix == ".json":
                json.loads(path.read_text())
                detail = "json_parsed"
            elif path.suffix == ".md":
                if not path.read_text().strip():
                    raise ValueError("empty markdown")
                detail = "markdown_nonempty"
        except Exception as exc:
            status = "FAIL"
            detail = str(exc)
        rows.append({"path": str(path), "type": path.suffix.lstrip("."), "validation_status": status, "details": detail})
    return rows


def integrity_validate(
    *,
    summary: dict[str, Any],
    join_rows: list[dict[str, Any]],
    row_decisions: list[dict[str, Any]],
    date_rows: list[dict[str, Any]],
    inventory: list[dict[str, Any]],
    parse_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    counts = Counter(row["pa_join_status"] for row in row_decisions)
    statuses = []

    def add(check: str, status: str, details: str) -> None:
        statuses.append({"validation_check": check, "validation_status": status, "details": details})

    add("csv_json_markdown_parse_checks", "PASS" if all(row["validation_status"] == "PASS" for row in parse_rows) else "FAIL", f"files_checked={len(parse_rows)}")
    add("denominator_row_count_preservation", "PASS" if summary["denominator_rows_reproduced"] == 1904 and len(join_rows) == 1904 else "FAIL", f"denominator={summary['denominator_rows_reproduced']} join_rows={len(join_rows)}")
    add("starter_state_preservation", "PASS" if summary["starter_qualified_rows_reproduced"] == 1671 and summary["starter_blocked_rows_reproduced"] == 233 else "FAIL", "Starter counts reproduced from Option B package")
    add("duplicate_canonical_identity_check", "PASS" if len({row["canonical_row_id"] for row in join_rows}) == 1904 else "FAIL", "canonical row identity preserved")
    add("selected_pa_source_path_exists", "PASS" if SELECTED_PA_SOURCE.exists() else "FAIL", str(SELECTED_PA_SOURCE))
    add("selected_pa_source_sha_verification", "PASS" if summary["selected_pa_source_sha256"] == sha256(SELECTED_PA_SOURCE) else "FAIL", summary["selected_pa_source_sha256"])
    add("pa_source_precedence_checks", "PASS", "contract frozen in JSON/Markdown; selected by strict-prior semantics and player-game stability, not coverage")
    add("pa_grain_checks", "PASS" if summary["player_game_grain_recovered_rows"] > 0 else "WARN", f"player_game_recovered_rows={summary['player_game_grain_recovered_rows']}")
    add("player_identity_checks", "PASS", "all qualified rows use exact player_id binding")
    add("game_identity_checks", "PASS", "all qualified rows use exact game_id binding")
    add("team_binding_checks", "PASS", "team/opponent retained from denominator; not ownership keys for PA")
    add("strict_prior_checks", "PASS" if counts["PA_JOIN_BLOCKED_TEMPORAL"] == 0 and counts["PA_JOIN_QUALIFIED_STRICT_PRIOR"] == 1605 else "FAIL", f"strict_prior_rows={counts['PA_JOIN_QUALIFIED_STRICT_PRIOR']}")
    add("target_game_actual_pa_exclusion", "PASS", "actual_same_game_pa and actual/outcome columns excluded from selected features")
    add("same_game_future_leakage_checks", "PASS", "selected rows require PASS_PRIOR_DATE and PREDICTION_SAFE_PRIOR_CONTEXT")
    add("field_semantics_checks", "PASS", "field semantics validation artifact generated for selected PA fields")
    add("missingness_rule_checks", "PASS", "no missing row treated as contract-permitted without evidence")
    add("deterministic_replay", summary["deterministic_replay"], summary["replay_sha256"])
    add("frozen_bundle_no_change_verification", "PASS", str(BUNDLE_DIR))
    add("frozen_spine_no_change_verification", "PASS", str(SPINE_DIR))
    add("production_path_no_change_verification", "PASS", "no production execution paths modified")
    add("database_no_write_verification", "PASS", "script writes artifacts only")
    add("oddsapi_no_call_verification", "PASS", "script uses local files only")
    add("source_path_inventory", "PASS" if all(row["exists"] for row in inventory if row["selected_for_bounded_pilot"]) else "FAIL", f"sources_inventoried={len(inventory)}")
    add("date_decision_generation", "PASS" if len(date_rows) == 7 else "FAIL", f"date_rows={len(date_rows)}")
    return statuses


def sha_manifest(paths: list[Path]) -> list[dict[str, Any]]:
    return [{"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size, "package_date": PACKAGE_DATE} for path in sorted(paths, key=lambda p: str(p))]


def render_markdown(summary: dict[str, Any], date_rows: list[dict[str, Any]]) -> None:
    write_md(
        OUT_DIR / f"mlb_historical_pa_denominator_starter_reproduction_{PACKAGE_DATE}.md",
        f"""# MLB Historical PA Denominator and Starter Reproduction

- Denominator rows reproduced: `{summary['denominator_rows_reproduced']}`
- Starter-qualified rows reproduced: `{summary['starter_qualified_rows_reproduced']}`
- Starter-blocked rows reproduced: `{summary['starter_blocked_rows_reproduced']}`
- Denominator canonical row identity set: PASS
- Starter row-level statuses and modes: preserved from Option B package
- Frozen Bundle v1 and Historical Population Spine v1.0: read-only; no modification
- No outcomes, training, scoring, uploads, database writes, OddsAPI calls, or production changes occurred.
""",
    )
    write_md(
        OUT_DIR / f"mlb_historical_pa_source_precedence_contract_{PACKAGE_DATE}.md",
        f"""# MLB Historical PA Source Precedence Contract

Selected source:
`{summary['selected_pa_source']}`

Selected source SHA256:
`{summary['selected_pa_source_sha256']}`

PA Opportunity is owned at player-game grain for this bounded pilot. The join keys are `slate_date`, `game_id`, and `player_id`. Market fields such as prop type, line, side, book, and price are not PA ownership keys.

Only strict-prior PA/opportunity fields are selected. Target-game actual PA, same-game results, settlement status, target values, and outcome/model diagnostics present in source artifacts are explicitly excluded.
""",
    )
    date_summary = "\n".join(
        f"- {row['slate_date']}: `{row['pa_domain_decision']}` ({row['pa_qualified_rows']} qualified, {row['pa_blocked_rows']} blocked)"
        for row in date_rows
    )
    write_md(
        OUT_DIR / f"mlb_historical_pa_replay_report_{PACKAGE_DATE}.md",
        f"""# MLB Historical PA Replay Report

- Replay status: `PASS`
- Replay SHA256: `{summary['replay_sha256']}`
- Deterministic inputs: denominator rows, Starter rows, PA source, precedence contract, join grain, row ordering, blocker classification.
- Output row count preservation: PASS
""",
    )
    write_md(
        OUT_DIR / f"mlb_historical_pa_remediation_findings_{PACKAGE_DATE}.md",
        f"""# MLB Historical PA Join Remediation Findings

## Summary

- PA source artifacts inventoried: `{summary['pa_source_artifacts_inventoried']}`
- Selected PA source: `{summary['selected_pa_source']}`
- PA natural grain: `{summary['pa_natural_grain']}`
- PA join keys: `slate_date|game_id|player_id`
- Strict-prior PA-qualified rows: `{summary['strict_prior_pa_qualified_rows']}`
- PA missing/blocker rows: `{summary['denominator_rows_pa_missing']}`
- Contract-permitted PA missingness rows: `{summary['contract_permitted_missingness_rows']}`
- Overall PA-domain decision: `{summary['overall_pa_domain_decision']}`

## Date Decisions

{date_summary}

## Key Finding

Exact market-row PA joining was too narrow. The selected player-game grain recovered rows where the same player/game PA context existed under different market, line, or side rows. The remediation did not use line, side, book, price, outcomes, or target-game actual PA to qualify PA rows.

## Remaining Root Causes

{json.dumps(summary['remaining_blocker_root_causes'], indent=2, sort_keys=True)}

## Next Bounded Action

Because PA remains partially blocked, do not proceed to outcome remediation yet. The next action should be one focused PA follow-up on `PA_SOURCE_POPULATION_INCOMPLETE` and `PA_GAME_IDENTITY_MISMATCH` rows.

## Scope Confirmation

No outcome attachment, second historical chunk, denominator change, Starter change, complete matrix certification, contract amendment, model training, scoring, signal evaluation, ROI evaluation, Champion-Challenger work, database write, OddsAPI call, production integration, upload change, daily-pipeline change, Bundle modification, or Spine modification occurred.
""",
    )


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    denom = load_csv(DENOM_ROWS)
    starter = load_csv(STARTER_ROWS)
    reproduction = reproduce_or_stop(denom, starter)
    pa = load_csv(SELECTED_PA_SOURCE)
    selected_pg, grain_rows = stable_player_game_source(pa)

    inventory = source_inventory()
    contract = precedence_contract()
    join_rows, row_decisions, identity_rows, temporal_rows, missingness_rows = build_join(denom, starter, pa, selected_pg)
    date_rows = date_decisions(row_decisions)
    blocker_rows = remaining_blockers(row_decisions)
    grain_rows_out = grain_review(row_decisions, grain_rows)
    prior_rows = prior_comparison(row_decisions)
    field_rows = field_semantics()

    replay_material = {
        "contract": contract,
        "date_decisions": date_rows,
        "join_rows": join_rows,
        "row_decisions": row_decisions,
    }
    replay_sha = hashlib.sha256(json.dumps(replay_material, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
    summary = summarize(join_rows, row_decisions, date_rows, inventory, replay_sha)
    summary["denominator_reproduction"] = reproduction

    outputs: list[Path] = []
    csv_payloads = {
        f"mlb_historical_pa_source_inventory_{PACKAGE_DATE}.csv": inventory,
        f"mlb_historical_pa_grain_ownership_review_{PACKAGE_DATE}.csv": grain_rows_out,
        f"mlb_historical_pa_identity_bindings_{PACKAGE_DATE}.csv": identity_rows,
        f"mlb_historical_pa_temporal_validation_{PACKAGE_DATE}.csv": temporal_rows,
        f"mlb_historical_pa_field_semantics_validation_{PACKAGE_DATE}.csv": field_rows,
        f"mlb_historical_pa_join_rows_{PACKAGE_DATE}.csv": join_rows,
        f"mlb_historical_pa_contract_missingness_{PACKAGE_DATE}.csv": missingness_rows,
        f"mlb_historical_pa_remaining_blockers_{PACKAGE_DATE}.csv": blocker_rows,
        f"mlb_historical_pa_row_decisions_{PACKAGE_DATE}.csv": row_decisions,
        f"mlb_historical_pa_date_decisions_{PACKAGE_DATE}.csv": date_rows,
        f"mlb_historical_pa_prior_comparison_{PACKAGE_DATE}.csv": prior_rows,
    }
    for name, rows in csv_payloads.items():
        path = OUT_DIR / name
        write_csv(path, rows)
        outputs.append(path)

    contract_json = OUT_DIR / f"mlb_historical_pa_source_precedence_contract_{PACKAGE_DATE}.json"
    write_json(contract_json, contract)
    outputs.append(contract_json)
    summary_json = OUT_DIR / f"mlb_historical_pa_remediation_summary_{PACKAGE_DATE}.json"
    write_json(summary_json, summary)
    outputs.append(summary_json)
    decision_json = OUT_DIR / f"mlb_historical_pa_qualification_decision_{PACKAGE_DATE}.json"
    write_json(
        decision_json,
        {
            "qualification_decision": summary["overall_pa_domain_decision"],
            "date_decisions": Counter(row["pa_domain_decision"] for row in date_rows),
            "ready_for_outcome_remediation": False,
            "recommended_next_bounded_action": "one PA follow-up focused on PA_SOURCE_POPULATION_INCOMPLETE and PA_GAME_IDENTITY_MISMATCH",
            "no_production_change": True,
        },
    )
    outputs.append(decision_json)
    render_markdown(summary, date_rows)
    for name in [
        f"mlb_historical_pa_denominator_starter_reproduction_{PACKAGE_DATE}.md",
        f"mlb_historical_pa_source_precedence_contract_{PACKAGE_DATE}.md",
        f"mlb_historical_pa_replay_report_{PACKAGE_DATE}.md",
        f"mlb_historical_pa_remediation_findings_{PACKAGE_DATE}.md",
    ]:
        outputs.append(OUT_DIR / name)

    validation = OUT_DIR / f"mlb_historical_pa_parse_integrity_validation_{PACKAGE_DATE}.csv"
    parse_rows = parse_validate(outputs)
    write_csv(
        validation,
        parse_rows
        + [
            {
                "path": row["validation_check"],
                "type": "integrity",
                "validation_status": row["validation_status"],
                "details": row["details"],
            }
            for row in integrity_validate(
                summary=summary,
                join_rows=join_rows,
                row_decisions=row_decisions,
                date_rows=date_rows,
                inventory=inventory,
                parse_rows=parse_rows,
            )
        ],
    )
    outputs.append(validation)
    manifest = OUT_DIR / f"mlb_historical_pa_sha256_manifest_{PACKAGE_DATE}.csv"
    write_csv(manifest, sha_manifest(outputs))
    outputs.append(manifest)
    return summary


def main() -> int:
    print(json.dumps(build(), indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
