#!/usr/bin/env python3
"""Bounded diagnostic for UBO-5 TB1.5 upload-filter thresholds."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backend.mlb.scripts.build_mlb_ubo5_tb15_human_board import implied
from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import (
    FEATURES,
    MODEL_SUPPORTED_NULL_FEATURES,
)
from backend.mlb.shared.ubo5_tb15_production_route import ARTIFACT_SHA256

ROOT = Path(__file__).resolve().parents[3]
NORMALIZED = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/normalized_refresh"
ARTIFACT = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23/original_ubo5_total_bases_multinomial.joblib"
FEASIBILITY = ROOT / "artifacts/analysis/mlb/ubo5_tb15_batting_order_feasibility/2026-07-24"
ROLE = ROOT / "artifacts/analysis/mlb/ubo5_tb15_role_envelope_pilot/2026-07-24/historical_role_envelope_validation.csv"

FILTERS = {
    "PROPOSED_STANDARD_FILTER": (200, 2.0, 0.05),
    "STRONG_FILTER": (200, 3.0, 0.08),
    "LOOSE_FILTER": (100, 1.0, 0.025),
    "EDGE_ONLY_FILTER": (100, 2.0, None),
    "POSITIVE_EDGE_BASELINE": (100, 0.0, None),
}


def markdown_table(frame: pd.DataFrame) -> str:
    def cell(value: object) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, float):
            return f"{value:.4f}"
        return str(value)

    columns = list(frame.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
    ]
    lines.extend("| " + " | ".join(cell(value) for value in row) + " |" for row in frame.itertuples(index=False, name=None))
    return "\n".join(lines)


def american_decimal(value: float) -> float:
    return 1 + (value / 100 if value > 0 else 100 / abs(value))


def add_market_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    d = frame.copy()
    d["raw_over_break_even_probability"] = d.over_price.map(lambda x: implied(int(x)))
    d["raw_under_break_even_probability"] = d.under_price.map(lambda x: implied(int(x)))
    denom = d.raw_over_break_even_probability + d.raw_under_break_even_probability
    d["no_vig_over_probability"] = d.raw_over_break_even_probability / denom
    d["over_edge_percentage_points"] = (d.ubo5_over_probability - d.no_vig_over_probability) * 100
    d["over_decimal_odds"] = d.over_price.map(american_decimal)
    d["actual_price_model_ev"] = d.ubo5_over_probability * d.over_decimal_odds - 1
    return d


def mask(d: pd.DataFrame, spec: tuple[int, float, float | None]) -> pd.Series:
    pa, edge, ev = spec
    result = d.strict_prior_pa.ge(pa)
    result &= d.over_edge_percentage_points.gt(0) if edge == 0 else d.over_edge_percentage_points.ge(edge)
    if ev is not None:
        result &= d.actual_price_model_ev.gt(0) if ev == 0 else d.actual_price_model_ev.ge(ev)
    return result


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def current_population(date: str, run_tag: str, output: Path) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    base = ROOT / f"backend/mlb/exports/model_v2/ubo5_tb15/{date}"
    audit_path = base / f"ubo5_tb15_prelineup_confirmation_audit_{run_tag}.csv"
    odds_path = ROOT / f"backend/mlb/exports/odds_history/{date}/odds_mlb_playerprops__{run_tag}.json"
    lineup_path = ROOT / (
        f"artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer/"
        f"{date}/{run_tag}/governed_lineup_capture/parsed_lineup_artifact_{date}.csv"
    )
    for path in (audit_path, odds_path, lineup_path, ARTIFACT):
        if not path.is_file():
            raise RuntimeError(f"missing bound source: {path.relative_to(ROOT)}")
    if sha256(ARTIFACT) != ARTIFACT_SHA256:
        raise RuntimeError("frozen UBO-5 artifact hash mismatch")

    audit = pd.read_csv(audit_path)
    lineup = pd.read_csv(lineup_path)
    odds = json.loads(odds_path.read_text())
    captured = str(audit.snapshot_timestamp_utc.dropna().iloc[0])
    if not captured.startswith(str(odds.get("captured_at_utc", ""))[:19]):
        raise RuntimeError("audit and BetOnline snapshot timestamp mismatch")

    lineup = lineup[
        lineup.lineup_status.eq("CONFIRMED_LINEUP")
        & lineup.pregame_validity_state.eq("VALID_PREGAME")
        & pd.to_datetime(lineup.source_timestamp, utc=True).lt(pd.to_datetime(lineup.first_pitch_timestamp, utc=True))
    ].copy()
    lineup["game_pk"] = pd.to_numeric(lineup.game_id).astype(int)
    lineup["batter_mlb_id"] = pd.to_numeric(lineup.player_id).astype(int)
    lineup["batting_order_position"] = pd.to_numeric(lineup.lineup_slot).astype(int)
    joined = lineup.merge(audit, on=["game_pk", "batter_mlb_id"], how="left", suffixes=("_lineup", ""))
    joined["ubo5_over_probability"] = [
        row.get(f"ubo5_probability_batting_{int(row.batting_order_position)}")
        for _, row in joined.iterrows()
    ]
    joined["ubo5_over_probability"] = pd.to_numeric(joined.ubo5_over_probability, errors="coerce")
    joined["over_price"] = pd.to_numeric(joined.BetOnline_over_price, errors="coerce")
    joined["under_price"] = pd.to_numeric(joined.BetOnline_under_price, errors="coerce")

    candidates = pd.DataFrame({
        "slate_date": date,
        "game_pk": joined.game_pk,
        "batter_mlb_id": joined.batter_mlb_id,
        "team": joined.team,
        "opponent": joined.opponent,
        "home_away": np.where(
            joined.game.str.split(" @ ").str[1].eq(joined.team), "home", "away"
        ),
        "prediction_timestamp_utc": joined.source_timestamp,
        "scheduled_start_utc": joined.first_pitch_timestamp,
        "lineup_certified": True,
        "lineup_certified_at_utc": joined.source_timestamp,
        "batting_order_position": joined.batting_order_position,
        "line": 1.5,
        "run_tag": run_tag,
        "opposing_starter_id": joined.opposing_starter_id,
        "source_lineage_pointer": joined.raw_response_path,
    })
    work = output / "_work"
    work.mkdir(parents=True, exist_ok=True)
    candidate_path, feature_path = work / "bound_candidates.csv", work / "bound_features.parquet"
    candidates.to_csv(candidate_path, index=False)
    subprocess.run([
        sys.executable, "-m", "backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features",
        "--normalized-root", str(NORMALIZED), "--candidate-file", str(candidate_path),
        "--output", str(feature_path),
    ], cwd=ROOT, check=True)
    features = pd.read_parquet(feature_path)
    keep = [
        "game_pk", "batter_mlb_id", "strict_prior_pa", "feature_vector_sha256",
        "feature_completeness_status", "temporal_integrity_status", "route_eligible", "exclusion_reason",
    ]
    joined = joined.merge(features[keep], on=["game_pk", "batter_mlb_id"], how="left")
    joined["player_name"] = joined.player_name.fillna(joined.player_name_lineup)
    joined["game"] = joined.game
    eligible = joined[
        joined.ubo5_over_probability.notna()
        & joined.over_price.notna() & joined.under_price.notna()
        & joined.strict_prior_pa.ge(100)
        & joined.feature_completeness_status.isin(["COMPLETE", "COMPLETE_WITH_MODEL_SUPPORTED_NULLS"])
        & joined.temporal_integrity_status.eq("PASS")
    ].copy()
    eligible = add_market_metrics(eligible)
    identity = joined[[
        "game_pk", "batter_mlb_id", "player_name", "game", "batting_order_position",
        "strict_prior_pa", "over_price", "under_price", "feature_vector_sha256",
        "feature_completeness_status", "temporal_integrity_status", "exclusion_reason",
    ]].copy()
    identity["run_tag"] = run_tag
    identity["snapshot_timestamp_utc"] = captured
    identity["exact_identity_status"] = np.where(
        identity.over_price.notna() & identity.under_price.notna(), "EXACT_TWO_SIDED_MATCH", "NO_EXACT_TWO_SIDED_MATCH"
    )
    return eligible, identity, captured


def historical_population() -> tuple[pd.DataFrame, pd.DataFrame]:
    scores = pd.read_csv(FEASIBILITY / "historical_order_sensitivity_rows.csv")
    markets = pd.read_csv(FEASIBILITY / "early_market_population_lineup_evaluation.csv")
    names = pd.read_csv(ROLE)[["slate_date", "game_pk", "batter_mlb_id", "player_name", "game"]]
    scores = scores.rename(columns={
        "history_depth_pa": "strict_prior_pa",
        "actual_probability": "ubo5_over_probability",
    })
    scores["slate_date"] = scores.game_date.astype(str)
    d = scores.merge(
        markets[markets.actual_starter & markets.strict_prior_order_available],
        on=["slate_date", "game_pk", "batter_mlb_id"],
        how="inner",
    ).merge(names.drop_duplicates(["slate_date", "game_pk", "batter_mlb_id"]),
            on=["slate_date", "game_pk", "batter_mlb_id"], how="left")
    d = d.rename(columns={"over_price": "over_price", "under_price": "under_price"})

    pa_files = sorted((NORMALIZED / "plate_appearances/season=2026").glob("*.parquet"))
    parts = []
    for path in pa_files:
        table = pq.ParquetFile(path)
        cols = [c for c in ["game_pk", "game_date", "batter", "events"] if c in table.schema_arrow.names]
        parts.append(table.read(columns=cols).to_pandas())
    pa = pd.concat(parts, ignore_index=True)
    pa["tb"] = pa.events.map({"single": 1, "double": 2, "triple": 3, "home_run": 4}).fillna(0)
    outcomes = pa.groupby(["game_pk", "batter"], as_index=False).tb.sum().rename(
        columns={"batter": "batter_mlb_id", "tb": "total_bases"}
    )
    d = d.merge(outcomes, on=["game_pk", "batter_mlb_id"], how="inner")
    d["result"] = np.where(d.total_bases > 1.5, "WIN", "LOSS")
    d["feature_completeness_status"] = "CERTIFIED_VALID_STATE_NOT_RETAINED"
    d["snapshot_type"] = "AUTHENTIC_EARLY_MARKET"
    d = add_market_metrics(d)
    audit = d[[
        "slate_date", "run_tag", "game_pk", "batter_mlb_id", "player_name", "game",
        "batting_order_position", "strict_prior_pa", "over_price", "under_price",
        "ubo5_over_probability", "total_bases", "result", "snapshot_type",
    ]].copy()
    audit["identity_status"] = "EXACT_GAME_PLAYER"
    audit["outcome_source"] = "CERTIFIED_NORMALIZED_PLATE_APPEARANCES"
    return d, audit


def summary_row(name: str, d: pd.DataFrame, baseline: int) -> dict:
    n = len(d)
    return {
        "filter": name, "eligible_baseline_rows": baseline, "retained_rows": n,
        "rows_removed": baseline - n, "volume_reduction_percentage": round(100 * (1 - n / baseline), 2) if baseline else 0,
        "plus_money_over_rows": int(d.over_price.gt(0).sum()) if n else 0,
        "favorite_price_over_rows": int(d.over_price.lt(0).sum()) if n else 0,
        "average_ubo5_over_probability": d.ubo5_over_probability.mean() if n else np.nan,
        "average_edge_percentage_points": d.over_edge_percentage_points.mean() if n else np.nan,
        "average_actual_price_model_ev": d.actual_price_model_ev.mean() if n else np.nan,
    }


def historical_row(name: str, d: pd.DataFrame, baseline: int) -> dict:
    resolved = d[d.result.isin(["WIN", "LOSS"])]
    wins, losses = int((resolved.result == "WIN").sum()), int((resolved.result == "LOSS").sum())
    units = ((resolved.over_decimal_odds - 1).where(resolved.result.eq("WIN"), -1)).sum()
    return {
        "filter": name, "snapshot_type": "AUTHENTIC_EARLY_MARKET", "rows": len(d),
        "distinct_slate_dates": d.slate_date.nunique(), "wins": wins, "losses": losses, "voids": 0,
        "win_rate": wins / len(resolved) if len(resolved) else np.nan,
        "average_odds": d.over_price.mean() if len(d) else np.nan,
        "units_at_one_unit_risk": units, "roi": units / len(resolved) if len(resolved) else np.nan,
        "average_predicted_probability": d.ubo5_over_probability.mean() if len(d) else np.nan,
        "expected_wins": d.ubo5_over_probability.sum(),
        "actual_minus_expected_wins": wins - d.ubo5_over_probability.sum(),
        "average_edge": d.over_edge_percentage_points.mean() if len(d) else np.nan,
        "median_edge": d.over_edge_percentage_points.median() if len(d) else np.nan,
        "average_model_ev": d.actual_price_model_ev.mean() if len(d) else np.nan,
        "volume_retained_percentage": 100 * len(d) / baseline if baseline else 0,
    }


def segment_rows(history: pd.DataFrame) -> pd.DataFrame:
    d = history.copy()
    d["odds_segment"] = pd.cut(
        d.over_price, [-np.inf, -0.1, 149, 199, np.inf],
        labels=["favorite price", "+100 to +149", "+150 to +199", "+200 or longer"],
    )
    d["pa_segment"] = pd.cut(
        d.strict_prior_pa, [99, 149, 199, 299, np.inf],
        labels=["100–149", "150–199", "200–299", "300+"],
    )
    d["batting_order_segment"] = pd.cut(
        d.batting_order_position, [0, 3, 6, 9], labels=["1–3", "4–6", "7–9"]
    )
    rows = []
    for filter_name, spec in FILTERS.items():
        subset = d[mask(d, spec)]
        for column in ("odds_segment", "pa_segment", "batting_order_segment", "feature_completeness_status"):
            for value, group in subset.groupby(column, observed=True):
                result = historical_row(filter_name, group, len(d))
                result.update({"segment_type": column, "segment": str(value)})
                rows.append(result)
    return pd.DataFrame(rows)


def write_report(
    out: Path, date: str, run_tag: str, captured: str, current: pd.DataFrame,
    current_summary: pd.DataFrame, historical: pd.DataFrame, historical_results: pd.DataFrame,
) -> str:
    retained = {name: current[mask(current, spec)].sort_values("over_edge_percentage_points", ascending=False)
                for name, spec in FILTERS.items()}
    decision = "INSUFFICIENT_HISTORY_TO_SELECT_FILTER"
    proposed = retained["PROPOSED_STANDARD_FILTER"]
    lines = [
        f"# UBO-5 TB 1.5 Upload-Filter Test — {date}", "",
        f"- Bound run tag: `{run_tag}`",
        f"- BetOnline snapshot timestamp: `{captured}`",
        f"- Baseline eligible rows: `{len(current)}`",
        f"- Historical population: `{len(historical)}` rows across `{historical.slate_date.nunique()}` dates",
        "- Scope: diagnostic only; no upload, routing, model, or probability change.", "",
        "## Current-slate filter summary", "",
        markdown_table(current_summary), "",
        "## Retained candidates", "",
        "| Player | Game | Batting | PA | UBO-5 Over | BOL Over | Edge | EV | Filter |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for name in ("PROPOSED_STANDARD_FILTER", "STRONG_FILTER", "LOOSE_FILTER"):
        for _, r in retained[name].iterrows():
            lines.append(
                f"| {r.player_name} | {r.game} | {int(r.batting_order_position)} | {int(r.strict_prior_pa)} | "
                f"{r.ubo5_over_probability:.2%} | {int(r.over_price):+d} | "
                f"{r.over_edge_percentage_points:+.2f} pp | {r.actual_price_model_ev:+.2%} | `{name}` |"
            )
    if not any(len(retained[name]) for name in ("PROPOSED_STANDARD_FILTER", "STRONG_FILTER", "LOOSE_FILTER")):
        lines.append("| _None_ | | | | | | | | |")
    lines += ["", "## Potential upload preview — proposed standard", "",
              "| Player | Game | Line |", "|---|---|---|"]
    for _, r in proposed.iterrows():
        lines.append(f"| {r.player_name} | {r.game} | Over 1.5 TB |")
    if proposed.empty:
        lines.append("| _None_ | | |")
    lines += [
        "", "## Historical evidence", "",
        markdown_table(historical_results), "",
        "Only four authentic early-market slate dates were available in the certified joined population. "
        "That is insufficient to choose the proposed rule over nearby thresholds or authorize automation. "
        "The current run may be observed prospectively, but no upload filter is promoted.", "",
        "## Limitations", "",
        "- Historical source artifacts retain certified valid 38-feature probabilities but not the split between complete and model-supported-null feature states.",
        "- The available historical evaluation is an authentic early-market snapshot population; distinct first-confirmed, 9:30-nearest, and final-pregame populations were not all retained for these dates.",
        "- Incumbent probability agreement was not available in the certified joined historical source and was not used as a filter.", "",
        f"`UBO5_TB15_UPLOAD_FILTER_TEST_DECISION = {decision}`", "",
        "`MLB_UBO5_TB15_UPLOAD_AUTOMATION_DECISION = NOT_AUTHORIZED_DIAGNOSTIC_ONLY`", "",
    ]
    (out / "ubo5_tb15_upload_filter_test_report.md").write_text("\n".join(lines))
    terminal = [
        f"UBO5_TB15_FILTER_TEST_BOUND_RUN_TAG = {run_tag}",
        f"UBO5_TB15_JULY25_BASELINE_ELIGIBLE_COUNT = {len(current)}",
        f"UBO5_TB15_JULY25_PROPOSED_STANDARD_COUNT = {len(proposed)}",
        f"UBO5_TB15_JULY25_STRONG_FILTER_COUNT = {len(retained['STRONG_FILTER'])}",
        f"UBO5_TB15_JULY25_LOOSE_FILTER_COUNT = {len(retained['LOOSE_FILTER'])}",
        f"UBO5_TB15_HISTORICAL_FILTER_POPULATION_COUNT = {len(historical)}",
        f"UBO5_TB15_UPLOAD_FILTER_TEST_DECISION = {decision}",
        "MLB_UBO5_TB15_UPLOAD_AUTOMATION_DECISION = NOT_AUTHORIZED_DIAGNOSTIC_ONLY",
    ]
    (out / "terminal_decision.md").write_text("\n".join(terminal) + "\n")
    return decision


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-07-25")
    ap.add_argument("--run-tag", default="local_daily_20260725T163002Z")
    args = ap.parse_args()
    out = ROOT / f"artifacts/analysis/model_development/mlb_ubo5_tb15_upload_filter_test/{args.date}"
    out.mkdir(parents=True, exist_ok=True)

    current, current_audit, captured = current_population(args.date, args.run_tag, out)
    history, history_audit = historical_population()
    summaries = pd.DataFrame([
        summary_row(name, current[mask(current, spec)], len(current)) for name, spec in FILTERS.items()
    ])
    historical_results = pd.DataFrame([
        historical_row(name, history[mask(history, spec)], len(history)) for name, spec in FILTERS.items()
    ])
    grid = []
    for pa in (100, 150, 200, 300):
        for edge in (0.0, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0):
            for ev in (0.0, 0.025, 0.05, 0.075, 0.10):
                spec = (pa, edge, ev)
                name = f"PA>={pa}|EDGE>={edge}|EV>={ev}"
                current_metrics = summary_row(name, current[mask(current, spec)], len(current))
                historical_metrics = historical_row(name, history[mask(history, spec)], len(history))
                current_metrics.update({
                    f"historical_{key}": value
                    for key, value in historical_metrics.items()
                    if key not in {"filter", "snapshot_type"}
                })
                grid.append(current_metrics)
    pd.DataFrame(grid).to_csv(out / "ubo5_tb15_upload_filter_grid.csv", index=False)

    candidate_columns = [
        "player_name", "game", "batting_order_position", "strict_prior_pa",
        "ubo5_over_probability", "over_price", "under_price", "raw_over_break_even_probability",
        "no_vig_over_probability", "over_edge_percentage_points", "actual_price_model_ev",
        "feature_completeness_status", "feature_vector_sha256",
    ]
    labeled = current[candidate_columns].copy()
    for name, spec in FILTERS.items():
        labeled[name] = mask(current, spec).values
    labeled.sort_values("over_edge_percentage_points", ascending=False).to_csv(
        out / "ubo5_tb15_july25_filter_candidates.csv", index=False
    )
    historical_results.to_csv(out / "ubo5_tb15_historical_filter_results.csv", index=False)
    segment_rows(history).to_csv(out / "ubo5_tb15_filter_segment_results.csv", index=False)
    pd.concat([
        current_audit.assign(population="JULY25_BOUND_RUN"),
        history_audit.assign(population="HISTORICAL_AUTHENTIC_EARLY_MARKET"),
    ], ignore_index=True, sort=False).to_csv(out / "ubo5_tb15_filter_identity_and_source_audit.csv", index=False)
    decision = write_report(out, args.date, args.run_tag, captured, current, summaries, history, historical_results)
    print(json.dumps({
        "run_tag": args.run_tag, "snapshot_timestamp_utc": captured, "baseline": len(current),
        "proposed": int(mask(current, FILTERS["PROPOSED_STANDARD_FILTER"]).sum()),
        "strong": int(mask(current, FILTERS["STRONG_FILTER"]).sum()),
        "loose": int(mask(current, FILTERS["LOOSE_FILTER"]).sum()),
        "historical": len(history), "decision": decision,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
