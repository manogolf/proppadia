#!/usr/bin/env python3
"""Freeze the Points per-minute shadow (NHL Backlog P2.46, operator-approved 2026-11-06).

Takes candidate PER_MINUTE_ADD from ``research_nhl_points_role_change_tests.py``
(a goals + assists pair on the production inputs plus the three per-minute
rates), wraps it in the production scorer's GOALS_PLUS_ASSISTS structure, and
checks live-path parity: per-minute inputs computed from each 2026-10-16..31
Points capture must match the research path's values. Writes the artifact and
``shadow_contract.json`` under ``points_per_minute_shadow.CONTRACT_DIR``.
Nothing changes production.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_per_minute_shadow as shadow  # noqa: E402
from backend.nhl.scripts import research_nhl_points_role_change_tests as research  # noqa: E402

ARTIFACT = "nhl_points_per_minute_shadow_v1.joblib"
TOLERANCE = 1e-4  # usage minutes are stored to 3 decimals


def research_totals(production: pd.DataFrame) -> pd.DataFrame:
    """The research cache's season points and games (goals and assists as in production).

    Live scoring uses the production contract's totals, which are complete; the
    research cache misses about one late 2025-26 game for some players. Parity
    checks the code path on the research totals, and the contract records the
    difference."""
    from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base
    cache = joblib.load(base.OUT / "matrices.joblib")["season_totals"].rename(
        columns={"canonical_season": "season", "season_points": "points", "season_games": "games"})
    # Research estimated missing minutes from outcome-based games (as production); only points differ.
    out = production.drop(columns=["points"]).merge(
        cache[["player_id", "season", "points"]], on=["player_id", "season"], how="left")
    return out.fillna({"points": 0})


def parity() -> dict:
    reference = pd.read_csv(research.OUT / "per_minute_inputs_2026.csv")
    production, league = shadow.production_tables()
    totals = research_totals(production)
    usage = pd.read_csv(shadow.USAGE_PATH)
    diffs, rows = {c: [] for c in shadow.TOI_FEATURES}, 0
    for day in sorted(reference.game_date.astype(str).str[:10].unique()):
        caps = shadow.captures_for(day)
        if not caps:
            continue
        cap = caps[-1]
        features = pd.read_csv(cap / "features.csv")
        live = shadow.per_minute_inputs(
            features, pd.read_csv(cap / "source_input_01_official_outcomes_00.csv"),
            pd.read_csv(cap / "source_input_02_official_skater_logs.csv"), totals, league, usage, shadow.season_of(day))
        m = live.merge(reference, on=shadow.KEYS, suffixes=("_live", "_research"))
        rows += len(m)
        for c in shadow.TOI_FEATURES:
            rel = ((m[f"{c}_live"] - m[f"{c}_research"]).abs() / m[f"{c}_research"].abs().clip(lower=1e-9))
            diffs[c] += rel.dropna().tolist()
    last = production[production.season.eq(2025)].merge(
        totals[totals.season.eq(2025)], on=["player_id", "season"], suffixes=("_live", "_research"))
    # Goal and assist rates share every source on both paths: they must match (code-path check).
    # The points rate also uses the research cache's league points rate and season totals, which
    # miss late 2025-26 games; live uses the production contract's official values (recorded).
    return {"rows_compared": rows,
            "max_relative_diff": max(max(diffs["goal_rate_toi"]), max(diffs["assist_rate_toi"])) if rows else None,
            "points_rate_relative_diff": {"median": float(pd.Series(diffs["own_rate_toi"]).median()),
                                          "p99": float(pd.Series(diffs["own_rate_toi"]).quantile(0.99)),
                                          "max": float(max(diffs["own_rate_toi"]))} if rows else None,
            "parity_totals": "research cache season points (code-path check)",
            "live_totals": "production contract prior_season_totals (official outcomes, complete)",
            "players_2025_points_differ_live_vs_training": int((last.points_live != last.points_research).sum())}


def main() -> None:
    shadow.CONTRACT_DIR.mkdir(parents=True, exist_ok=False)
    candidates = joblib.load(research.OUT / "candidates.joblib")
    pair = candidates["PER_MINUTE_ADD"]
    features = list(pair["goals"]["feature_columns"])
    model = {"structure": "GOALS_PLUS_ASSISTS", "feature_columns": features, "fill_zero": True,
             "scaler": pair["goals"]["scaler"], "goals": pair["goals"], "assists": pair["assists"]}
    check = parity()
    if not check["rows_compared"] or check["max_relative_diff"] > TOLERANCE:
        raise SystemExit(f"POINTS_PER_MINUTE_PARITY_FAILED:{check}")
    joblib.dump(model, shadow.CONTRACT_DIR / ARTIFACT)
    contract = {
        "schema_version": "NHL_POINTS_PER_MINUTE_SHADOW_CONTRACT_V1", "model_version": shadow.MODEL_VERSION,
        "role": "SHADOW", "production_unchanged": True, "approved": "operator 2026-11-06 (\"Yes, start shadow\")",
        "fitted_artifact": ARTIFACT, "fitted_artifact_sha256": shadow.sha256(shadow.CONTRACT_DIR / ARTIFACT),
        "feature_columns": features, "per_minute_features": shadow.TOI_FEATURES, "shrinkage_games": shadow.K,
        "training": "2023-24 and 2024-25 target rows (research_nhl_points_role_change_tests.py, PER_MINUTE_ADD)",
        "research_output": str(research.OUT.relative_to(ROOT)),
        "research_candidates_sha256": shadow.sha256(research.OUT / "candidates.joblib"),
        "usage_table": str(shadow.USAGE_PATH.relative_to(ROOT)), "usage_sha256": shadow.sha256(shadow.USAGE_PATH),
        "live_parity": check,
    }
    shadow.CONTRACT_PATH.write_text(json.dumps(contract, indent=2) + "\n")
    print(json.dumps({"contract": str(shadow.CONTRACT_PATH), **check}, indent=1))


if __name__ == "__main__":
    main()
