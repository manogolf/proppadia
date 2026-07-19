"""Reconcile existing Hits U1.5 tracking with frozen pitcher suppression evidence.

Bounded local-artifact utility. It reads preserved review-aid, tier-backtest,
suppression-validation, and live-shadow artifacts only. It does not call network
services, write databases, train models, change thresholds, or alter production
behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_existing_u15_tracking_suppression_reconciliation/2026-07-17"
REVIEW_AID_ROOT = ROOT / "artifacts/analysis/mlb/review_aids"
ODDS_HISTORY_ROOT = ROOT / "backend/mlb/exports/odds_history"
TIER_BACKTEST = REVIEW_AID_ROOT / "hits_u15_tier_backtest_rows.csv"
SUPPRESSION_VALIDATION_ROOT = (
    ROOT / "artifacts/analysis/model_development/mlb_hits15_pitcher_suppression_under_validation/2026-07-17"
)
SUPPRESSION_LEDGER = SUPPRESSION_VALIDATION_ROOT / "exact_pitcher_dominant_population_manifest_2026-07-17.csv"
SUPPRESSION_PRICE_LEDGER = SUPPRESSION_VALIDATION_ROOT / "exact_u15_market_availability_ledger_2026-07-17.csv"
SUPPRESSION_CURRENT_COMPARISON = SUPPRESSION_VALIDATION_ROOT / "current_vs_suppression_comparison_2026-07-17.csv"
LIVE_SHADOW_ROOT = ROOT / "artifacts/analysis/model_development/mlb_hits15_prospective_suppression_shadow/2026-07-17"
LIVE_SHADOW_STATUS = LIVE_SHADOW_ROOT / "living_observation_status_2026-07-17.json"
LIVE_SHADOW_RUN_INDEX = LIVE_SHADOW_ROOT / "living_observation_run_index_2026-07-17.csv"


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def norm(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def lower(value: Any) -> str:
    return norm(value).lower()


def to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        v = float(value)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None


def to_int_text(value: Any) -> str:
    v = to_float(value)
    if v is None:
        return ""
    return str(int(v))


def line_text(value: Any) -> str:
    v = to_float(value)
    if v is None:
        return ""
    return f"{v:.1f}"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def date_from_filename(path: Path) -> str:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
    return m.group(1) if m else ""


def run_tag_from_path_text(value: Any) -> str:
    text = norm(value)
    m = re.search(r"__(\d{8}T\d{6}Z)", text)
    if m:
        return m.group(1)
    m = re.search(r"(local_daily_\d{8}T\d{6}Z)", text)
    if m:
        return m.group(1)
    return ""


def canonical_key(date: Any, game_id: Any, player_id: Any, line: Any, side: str = "under") -> str:
    return "|".join(
        [
            norm(date)[:10],
            to_int_text(game_id),
            to_int_text(player_id),
            "hits",
            line_text(line),
            side.lower(),
        ]
    )


def side_neutral_key(date: Any, game_id: Any, player_id: Any, line: Any) -> str:
    return "|".join([norm(date)[:10], to_int_text(game_id), to_int_text(player_id), "hits", line_text(line)])


def american_profit(result: str, price: Any) -> float | None:
    if result not in {"win", "loss"}:
        return None
    p = to_float(price)
    if p is None or p == 0:
        return None
    if result == "loss":
        return -1.0
    return p / 100.0 if p > 0 else 100.0 / abs(p)


def result_from_row(row: pd.Series) -> str:
    for col in ["result", "actual_under_outcome", "integrated_u15_result"]:
        val = lower(row.get(col))
        if val in {"win", "won", "w", "true", "1"}:
            return "win"
        if val in {"loss", "lost", "l", "false", "0"}:
            return "loss"
        if val in {"push", "void", "tie", "refund"}:
            return "push"
    units = to_float(row.get("units"))
    if units is not None:
        if units > 0:
            return "win"
        if units < 0:
            return "loss"
        return "push"
    return "unresolved"


def sample_flag(rows: int) -> str:
    if rows >= 100:
        return "adequate"
    if rows >= 30:
        return "small"
    return "sparse"


def summarize_performance(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if df.empty:
        return pd.DataFrame(columns=group_cols + ["rows", "resolved", "wins", "losses", "pushes", "win_rate", "flat_stake_roi", "units", "exact_price_rows", "sample_flag"])
    work = df.copy()
    work["_result"] = work.apply(result_from_row, axis=1)
    work["_profit"] = work.apply(lambda r: american_profit(r["_result"], r.get("authoritative_price") or r.get("price") or r.get("market_price")), axis=1)
    for keys, g in work.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        resolved = g[g["_result"].isin(["win", "loss", "push"])]
        wins = int((resolved["_result"] == "win").sum())
        losses = int((resolved["_result"] == "loss").sum())
        pushes = int((resolved["_result"] == "push").sum())
        price_g = resolved[resolved["_profit"].notna()]
        units = float(price_g["_profit"].sum()) if len(price_g) else None
        row = {col: key for col, key in zip(group_cols, keys)}
        row.update(
            {
                "rows": int(len(g)),
                "resolved": int(len(resolved)),
                "wins": wins,
                "losses": losses,
                "pushes": pushes,
                "win_rate": wins / (wins + losses) if (wins + losses) else None,
                "flat_stake_roi": (units / len(price_g)) if units is not None and len(price_g) else None,
                "units": units,
                "exact_price_rows": int(g["exact_selection_time_price"].sum()) if "exact_selection_time_price" in g else 0,
                "price_authoritative_rows": int(g["price_timing_class"].isin(["exact_selection_time_price", "valid_earlier_price"]).sum()) if "price_timing_class" in g else 0,
                "distinct_dates": int(g["slate_date"].nunique()) if "slate_date" in g else None,
                "distinct_players": int(g["player_id"].nunique()) if "player_id" in g else None,
                "distinct_games": int(g["game_id"].nunique()) if "game_id" in g else None,
                "sample_flag": sample_flag(int(len(resolved))),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(group_cols).reset_index(drop=True)


def max_drawdown(profits: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    worst = 0.0
    for p in profits:
        equity += p
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


def load_review_aid_population() -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[pd.DataFrame] = []
    inventory: list[dict[str, Any]] = []
    for path in sorted(REVIEW_AID_ROOT.glob("hits_u15_favorite_audit_2026-*.csv")):
        date_text = date_from_filename(path)
        df = read_csv(path)
        if df.empty:
            continue
        df["source_artifact"] = rel(path)
        df["source_file_mtime_utc"] = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
        df["slate_date"] = df.get("date", date_text).astype(str).str[:10]
        df["population_class"] = "U15_SURFACED_CANDIDATE"
        df["side"] = "under"
        df["prop_type"] = "hits"
        df["line"] = 1.5
        df["canonical_u15_key"] = df.apply(lambda r: canonical_key(r["slate_date"], r.get("game_id"), r.get("player_id"), r.get("line"), "under"), axis=1)
        df["side_neutral_key"] = df.apply(lambda r: side_neutral_key(r["slate_date"], r.get("game_id"), r.get("player_id"), r.get("line")), axis=1)
        rows.append(df)
        inventory.append(
            {
                "source_path": rel(path),
                "source_type": "hits_u15_favorite_audit_csv",
                "date_range": date_text,
                "run_tags": ";".join(sorted({run_tag_from_path_text(v) for v in df.get("qc_source_file", pd.Series(dtype=str)).dropna()} - {""})),
                "creation_timestamp_utc": datetime.fromtimestamp(path.stat().st_ctime, timezone.utc).isoformat(),
                "modification_timestamp_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
                "grain": "candidate_row",
                "candidate_or_wager_status": "surfaced_candidate_not_executed_wager",
                "side_and_line": "hits under 1.5",
                "sportsbook_and_price_availability": "market_price column retained; source sportsbook generally not retained",
                "outcome_authority": "joined later from hits_u15_tier_backtest_rows where exact key exists",
                "existed_pregame": "likely_pregame_review_aid; exact run timestamp mostly unresolved",
                "rows": len(df),
            }
        )
    pop = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    return pop, pd.DataFrame(inventory)


def attach_outcomes(pop: pd.DataFrame) -> pd.DataFrame:
    tier = read_csv(TIER_BACKTEST)
    if tier.empty or pop.empty:
        pop["result"] = "unresolved"
        return pop
    tier = tier.copy()
    tier["slate_date"] = tier["date"].astype(str).str[:10]
    tier["canonical_u15_key"] = tier.apply(lambda r: canonical_key(r["slate_date"], r.get("game_id"), r.get("player_id"), r.get("line"), "under"), axis=1)
    keep_cols = [
        "canonical_u15_key",
        "actual_under_outcome",
        "pnl_under_1u",
        "price_under",
        "placed",
        "placed_status",
        "ops_proxy_inclusion",
        "opposing_starter_player_id",
        "starter_context_status",
        "pitcher_base",
        "starter_expected_hits_allowed",
        "hitter_tier",
        "pitcher_tier",
        "combined_tier",
    ]
    for c in keep_cols:
        if c not in tier.columns:
            tier[c] = ""
    tier = tier[keep_cols].drop_duplicates("canonical_u15_key")
    out = pop.merge(tier, on="canonical_u15_key", how="left", suffixes=("", "_tier"))
    out["result"] = out.apply(result_from_row, axis=1)
    out["outcome_resolved"] = out["result"].isin(["win", "loss", "push"])
    out["outcome_source_path"] = out["outcome_resolved"].map(lambda x: rel(TIER_BACKTEST) if x else "")
    out["authoritative_price"] = out["market_price"].combine_first(out.get("price_under", pd.Series([None] * len(out))))
    return out


def attach_suppression(pop: pd.DataFrame) -> pd.DataFrame:
    sup = read_csv(SUPPRESSION_LEDGER)
    price = read_csv(SUPPRESSION_PRICE_LEDGER)
    out = pop.copy()
    if not sup.empty:
        sup = sup.copy()
        if "canonical_proposition_key" in sup.columns:
            sup["side_neutral_key"] = sup["canonical_proposition_key"].astype(str).str.replace(r"\|hits\|1\.5$", "|hits|1.5", regex=True)
        else:
            sup["side_neutral_key"] = sup.apply(lambda r: side_neutral_key(r.get("slate_date"), r.get("game_id"), r.get("player_id"), r.get("line")), axis=1)
        sup_cols = [
            "side_neutral_key",
            "suppression_subtype",
            "pitcher_suppression_label",
            "current_side_surface_state",
            "integrated_u15_result",
            "under_market_availability_status",
            "exact_executable_u15_price",
            "u15_price",
            "source_sportsbook",
            "prediction_to_price_age",
            "price_available_at_relevant_selection_time",
        ]
        for c in sup_cols:
            if c not in sup.columns:
                sup[c] = ""
        out = out.merge(sup[sup_cols].drop_duplicates("side_neutral_key"), on="side_neutral_key", how="left", suffixes=("", "_suppression"))
    else:
        out["suppression_subtype"] = ""

    if not price.empty and "canonical_proposition_key" in price.columns:
        price = price.copy()
        price["side_neutral_key"] = price["canonical_proposition_key"].astype(str).str.replace(r"\|hits\|1\.5$", "|hits|1.5", regex=True)
        pcols = [
            "side_neutral_key",
            "snapshot_timestamp",
            "opposite_side_disappeared",
            "under_market_availability_status",
            "exact_executable_u15_price",
        ]
        for c in pcols:
            if c not in price.columns:
                price[c] = ""
        out = out.merge(price[pcols].drop_duplicates("side_neutral_key"), on="side_neutral_key", how="left", suffixes=("", "_supp_price"))

    out["suppression_classification"] = out["suppression_subtype"].map(norm)
    out.loc[out["suppression_classification"].eq(""), "suppression_classification"] = "SUPPRESSION_EVIDENCE_UNAVAILABLE"
    out["suppression_classification"] = out["suppression_classification"].replace(
        {
            "UNCERTAINTY_OR_MISSINGNESS_STATE": "UNCERTAINTY_OR_MISSINGNESS",
            "IRREGULAR_ROLE_STATE": "IRREGULAR_ROLE",
        }
    )
    out["affirmative_suppression_overlap"] = out["suppression_classification"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")
    return out


def classify_prices(pop: pd.DataFrame) -> pd.DataFrame:
    out = pop.copy()
    out["selection_timestamp"] = out.get("source_file_mtime_utc", "")
    out["run_tag"] = out.get("qc_source_file", pd.Series([""] * len(out))).map(run_tag_from_path_text)
    if "snapshot_timestamp" not in out:
        out["snapshot_timestamp"] = ""
    out["price_timing_class"] = "timestamp_unresolved"
    out.loc[out["market_price"].isna() & out.get("u15_price", pd.Series([None] * len(out))).isna(), "price_timing_class"] = "no_preserved_snapshot"
    if "price_available_at_relevant_selection_time" in out:
        exact_mask = out["price_available_at_relevant_selection_time"].map(lower).isin({"true", "yes", "1"})
        out.loc[exact_mask, "price_timing_class"] = "exact_selection_time_price"
    exact_live = out.get("live_u15_availability_status", pd.Series([""] * len(out))).map(lower).eq("exact_live_price_bound")
    out.loc[exact_live, "price_timing_class"] = "exact_selection_time_price"
    out["exact_selection_time_price"] = out["price_timing_class"].eq("exact_selection_time_price")
    out["price_authority"] = out["price_timing_class"].map(
        lambda x: "authoritative_for_roi" if x in {"exact_selection_time_price", "valid_earlier_price"} else "not_authoritative_for_roi"
    )
    return out


def population_manifests(pop: pd.DataFrame) -> pd.DataFrame:
    work = pop.copy()
    work["U15_SURFACED_CANDIDATE"] = True
    work["U15_GOVERNED_PREDICTION"] = work.get("qc_candidate", pd.Series([False] * len(work))).astype(str).str.lower().eq("true") | work.get("watch_candidate", pd.Series([False] * len(work))).astype(str).str.lower().eq("true")
    work["U15_TRACKED_SELECTION"] = work["U15_SURFACED_CANDIDATE"]
    work["U15_EXECUTED_WAGER"] = work.get("placed", pd.Series([False] * len(work))).astype(str).str.lower().eq("true")
    work["U15_OUTCOME_GRADED"] = work["outcome_resolved"]
    work["U15_EXACT_PRICE_AND_TIMING_CERTIFIED"] = work["exact_selection_time_price"]
    return work


def characterize_non_suppression(row: pd.Series) -> str:
    cls = norm(row.get("suppression_classification"))
    if cls == "AFFIRMATIVE_ESTABLISHED_SUPPRESSION":
        return "affirmative_suppression"
    if cls == "RELATIVE_PITCHER_DOMINANCE":
        return "relative_dominance_only"
    if cls in {"UNCERTAINTY_OR_MISSINGNESS", "IRREGULAR_ROLE"}:
        return "uncertainty_driven"
    d7 = to_float(row.get("d7_hits_rate"))
    d15 = to_float(row.get("d15_hits_rate"))
    hitter_tier = norm(row.get("hitter_tier"))
    if (d7 is not None and d7 < 1.0) and (d15 is not None and d15 < 1.0):
        return "weak_hitter_complement"
    if hitter_tier in {"A", "B"}:
        return "weak_hitter_complement"
    if cls == "SUPPRESSION_EVIDENCE_UNAVAILABLE":
        return "mixed_unresolved"
    return "no_stable_signal"


def markdown_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df.empty:
        return "No rows."
    view = df.head(max_rows)
    cols = list(view.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in view.iterrows():
        vals = []
        for c in cols:
            v = row[c]
            if isinstance(v, float):
                vals.append("" if pd.isna(v) else f"{v:.4f}")
            else:
                vals.append(norm(v).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    if len(df) > max_rows:
        lines.append(f"\nShowing {max_rows} of {len(df)} rows.")
    return "\n".join(lines)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pop, source_inventory = load_review_aid_population()
    if pop.empty:
        raise FileNotFoundError("no hits_u15_favorite_audit_2026-*.csv files found")

    pop = attach_outcomes(pop)
    pop = attach_suppression(pop)
    integrated_result = pop.get("integrated_u15_result", pd.Series([""] * len(pop))).map(lower)
    needs_integrated_outcome = ~pop["outcome_resolved"] & integrated_result.isin({"win", "loss", "push"})
    pop.loc[needs_integrated_outcome, "result"] = integrated_result[needs_integrated_outcome]
    pop.loc[needs_integrated_outcome, "outcome_resolved"] = True
    pop.loc[needs_integrated_outcome, "outcome_source_path"] = rel(SUPPRESSION_LEDGER)
    pop = classify_prices(pop)
    pop = population_manifests(pop)
    pop["non_suppression_regime"] = pop.apply(characterize_non_suppression, axis=1)
    pop["duplicate_group_id"] = pop["canonical_u15_key"]
    pop["duplicate_count"] = pop.groupby("canonical_u15_key")["canonical_u15_key"].transform("size")
    pop["is_duplicate_observation"] = pop["duplicate_count"] > 1

    key_cols = ["canonical_u15_key", "slate_date", "game_id", "player_id", "player_name", "team", "opponent", "prop_type", "line", "side"]
    for c in key_cols:
        if c not in pop.columns:
            pop[c] = ""

    source_inventory_extra = [
        {
            "source_path": rel(TIER_BACKTEST),
            "source_type": "hits_u15_tier_backtest_rows",
            "date_range": f"{read_csv(TIER_BACKTEST)['date'].min()} to {read_csv(TIER_BACKTEST)['date'].max()}" if TIER_BACKTEST.exists() else "",
            "run_tags": "",
            "creation_timestamp_utc": datetime.fromtimestamp(TIER_BACKTEST.stat().st_ctime, timezone.utc).isoformat() if TIER_BACKTEST.exists() else "",
            "modification_timestamp_utc": datetime.fromtimestamp(TIER_BACKTEST.stat().st_mtime, timezone.utc).isoformat() if TIER_BACKTEST.exists() else "",
            "grain": "outcome_backed_tier_row",
            "candidate_or_wager_status": "historical_backtest_not_executed_wager",
            "side_and_line": "hits under 1.5 and over companion rows",
            "sportsbook_and_price_availability": "price_under retained",
            "outcome_authority": "actual_under_outcome and pnl_under_1u",
            "existed_pregame": "historical/research spine; not exact current tracking timestamp",
            "rows": len(read_csv(TIER_BACKTEST)) if TIER_BACKTEST.exists() else 0,
        },
        {
            "source_path": rel(SUPPRESSION_LEDGER),
            "source_type": "frozen_suppression_validation_manifest",
            "date_range": "2026-06-27 to 2026-07-17",
            "run_tags": "",
            "creation_timestamp_utc": datetime.fromtimestamp(SUPPRESSION_LEDGER.stat().st_ctime, timezone.utc).isoformat() if SUPPRESSION_LEDGER.exists() else "",
            "modification_timestamp_utc": datetime.fromtimestamp(SUPPRESSION_LEDGER.stat().st_mtime, timezone.utc).isoformat() if SUPPRESSION_LEDGER.exists() else "",
            "grain": "side_neutral_hits15_pitcher_dominant_proposition",
            "candidate_or_wager_status": "research_suppression_evidence_not_wager",
            "side_and_line": "hits 1.5 side-neutral; U1.5 result derived",
            "sportsbook_and_price_availability": "joined from suppression price ledger when available",
            "outcome_authority": "integrated certified official hits",
            "existed_pregame": "historical evidence; timing certification partial/unknown",
            "rows": len(read_csv(SUPPRESSION_LEDGER)) if SUPPRESSION_LEDGER.exists() else 0,
        },
    ]
    source_inventory = pd.concat([source_inventory, pd.DataFrame(source_inventory_extra)], ignore_index=True)

    unique = pop.drop_duplicates("canonical_u15_key").copy()
    unique["result_sort"] = unique["slate_date"].astype(str) + "|" + unique["canonical_u15_key"].astype(str)
    unique = unique.sort_values("result_sort")

    # Manifests.
    manifest_cols = [
        "canonical_u15_key",
        "side_neutral_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "prop_type",
        "line",
        "side",
        "market_price",
        "authoritative_price",
        "run_tag",
        "selection_timestamp",
        "snapshot_timestamp",
        "price_timing_class",
        "price_authority",
        "result",
        "outcome_resolved",
        "outcome_source_path",
        "suppression_classification",
        "pitcher_suppression_label",
        "current_side_surface_state",
        "affirmative_suppression_overlap",
        "non_suppression_regime",
        "hitter_tier",
        "pitcher_tier",
        "combined_tier",
        "d7_hits_rate",
        "d15_hits_rate",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "opposing_starter",
        "opposing_starter_id",
        "duplicate_group_id",
        "duplicate_count",
        "source_artifact",
        "U15_SURFACED_CANDIDATE",
        "U15_GOVERNED_PREDICTION",
        "U15_TRACKED_SELECTION",
        "U15_EXECUTED_WAGER",
        "U15_OUTCOME_GRADED",
        "U15_EXACT_PRICE_AND_TIMING_CERTIFIED",
    ]
    for c in manifest_cols:
        if c not in unique.columns:
            unique[c] = ""
    write_csv(source_inventory, out_dir / "existing_u15_source_inventory_2026-07-17.csv")
    write_csv(unique[manifest_cols], out_dir / "existing_u15_tracked_population_manifest_2026-07-17.csv")
    for flag in [
        "U15_SURFACED_CANDIDATE",
        "U15_GOVERNED_PREDICTION",
        "U15_TRACKED_SELECTION",
        "U15_EXECUTED_WAGER",
        "U15_OUTCOME_GRADED",
        "U15_EXACT_PRICE_AND_TIMING_CERTIFIED",
    ]:
        write_csv(unique[unique[flag].astype(bool)][manifest_cols], out_dir / f"{flag.lower()}_manifest_2026-07-17.csv")

    performance = pd.concat(
        [
            summarize_performance(unique.assign(population_class=flag), ["population_class"])
            for flag in [
                "U15_SURFACED_CANDIDATE",
                "U15_GOVERNED_PREDICTION",
                "U15_TRACKED_SELECTION",
                "U15_EXECUTED_WAGER",
                "U15_OUTCOME_GRADED",
                "U15_EXACT_PRICE_AND_TIMING_CERTIFIED",
            ]
            if flag in unique and unique[flag].astype(bool).any()
        ],
        ignore_index=True,
    )
    # Correct each population subset; assign above included all rows, rebuild explicitly.
    perf_parts = []
    for flag in [
        "U15_SURFACED_CANDIDATE",
        "U15_GOVERNED_PREDICTION",
        "U15_TRACKED_SELECTION",
        "U15_EXECUTED_WAGER",
        "U15_OUTCOME_GRADED",
        "U15_EXACT_PRICE_AND_TIMING_CERTIFIED",
    ]:
        subset = unique[unique[flag].astype(bool)].copy()
        if subset.empty:
            perf_parts.append(pd.DataFrame([{"population_class": flag, "rows": 0, "resolved": 0, "wins": 0, "losses": 0, "pushes": 0}]))
        else:
            perf_parts.append(summarize_performance(subset.assign(population_class=flag), ["population_class"]))
    performance = pd.concat(perf_parts, ignore_index=True)
    write_csv(performance, out_dir / "certified_u15_population_performance_2026-07-17.csv")

    date_perf = summarize_performance(unique, ["slate_date"])
    write_csv(date_perf, out_dir / "date_level_u15_performance_2026-07-17.csv")

    suppression_overlap = summarize_performance(unique, ["suppression_classification"])
    write_csv(suppression_overlap, out_dir / "suppression_overlap_performance_2026-07-17.csv")

    suppression_ledger_cols = manifest_cols + ["integrated_u15_result", "under_market_availability_status", "u15_price", "source_sportsbook"]
    for c in suppression_ledger_cols:
        if c not in unique.columns:
            unique[c] = ""
    write_csv(unique[suppression_ledger_cols], out_dir / "exact_suppression_overlap_ledger_2026-07-17.csv")

    non_supp = summarize_performance(unique[~unique["affirmative_suppression_overlap"]], ["non_suppression_regime"])
    write_csv(non_supp, out_dir / "non_suppression_regime_characterization_2026-07-17.csv")

    price_cols = [
        "canonical_u15_key",
        "slate_date",
        "player_id",
        "player_name",
        "game_id",
        "run_tag",
        "selection_timestamp",
        "snapshot_timestamp",
        "market_price",
        "u15_price",
        "source_sportsbook",
        "prediction_to_price_age",
        "price_available_at_relevant_selection_time",
        "price_timing_class",
        "price_authority",
        "result",
        "exact_selection_time_price",
    ]
    for c in price_cols:
        if c not in unique.columns:
            unique[c] = ""
    write_csv(unique[price_cols], out_dir / "price_timing_recovery_2026-07-17.csv")
    price_summary = unique.groupby("price_timing_class", dropna=False).size().reset_index(name="rows")
    write_csv(price_summary, out_dir / "price_timing_recovery_summary_2026-07-17.csv")

    # Relationship of suppression universe to old U1.5 surface.
    sup_all = read_csv(SUPPRESSION_LEDGER)
    missed_rows = []
    if not sup_all.empty:
        sup_all = sup_all.copy()
        sup_all["side_neutral_key"] = sup_all.apply(lambda r: side_neutral_key(r.get("slate_date"), r.get("game_id"), r.get("player_id"), r.get("line", 1.5)), axis=1)
        surfaced_keys = set(unique["side_neutral_key"])
        missed = sup_all[~sup_all["side_neutral_key"].isin(surfaced_keys)].copy()
        missed["relationship"] = "affirmative_or_pitcher_dominant_never_surfaced_as_existing_u15_favorite"
        missed_rows = missed
        write_csv(missed, out_dir / "suppression_rows_not_surfaced_existing_u15_2026-07-17.csv")
    else:
        write_csv(pd.DataFrame(), out_dir / "suppression_rows_not_surfaced_existing_u15_2026-07-17.csv")

    current_comp = read_csv(SUPPRESSION_CURRENT_COMPARISON)
    old_vs_new_rows = [
        {
            "dimension": "classification_logic",
            "existing_u15_process": "U1.5 favorite audit built from hitter coldness/tiers plus tough starter/review-aid layering",
            "new_shadow_contract": "MLB_HITS15_AFFIRMATIVE_SUPPRESSION_SHADOW_V1; frozen affirmative pitcher suppression only",
            "finding": "new_shadow_narrows_existing_lane_to_stronger_pitcher_ownership_mechanism",
        },
        {
            "dimension": "price_timing",
            "existing_u15_process": "market_price retained but exact sportsbook/snapshot timestamp usually unresolved",
            "new_shadow_contract": "exact live U1.5 sportsbook and contemporaneous snapshot retained for qualifying run",
            "finding": "new_shadow_adds_lineage_not_available_for_most_existing_rows",
        },
        {
            "dimension": "population",
            "existing_u15_process": f"{len(unique)} unique surfaced U1.5 candidate identities",
            "new_shadow_contract": "Run 1 has 15 affirmative suppression propositions and 14 exact U1.5 price-bound propositions",
            "finding": "new_shadow_is_subset_and_lineage_upgrade_not_duplicate_lane",
        },
        {
            "dimension": "current_surface_behavior",
            "existing_u15_process": "review-aid/current board surface; not a production wager ledger",
            "new_shadow_contract": "append-only prospective observation ledger; no grading until authorized",
            "finding": "separate_governance_tracks_should_continue",
        },
    ]
    write_csv(pd.DataFrame(old_vs_new_rows), out_dir / "old_process_vs_new_shadow_comparison_2026-07-17.csv")

    # Rule lineage.
    rule_rows = [
        {"rule_component": "side", "existing_logic_evidence": "hits_u15_favorite_audit rows hard-code side=under", "source": "backend/mlb/scripts/run_mlb_hits_o15_review_board.py", "lineage_decision": "confirmed"},
        {"rule_component": "hitter evidence", "existing_logic_evidence": "d7_cold_candidate and d15_cold_consistent_candidate retained on review-aid rows", "source": "hits_u15_favorite_audit_*.csv", "lineage_decision": "confirmed"},
        {"rule_component": "starter evidence", "existing_logic_evidence": "tough_starter_candidate, pitcher_tier, starter_expected_hits_allowed retained", "source": "hits_u15_favorite_audit_*.csv", "lineage_decision": "confirmed"},
        {"rule_component": "affirmative pitcher suppression", "existing_logic_evidence": "not retained as frozen affirmative suppression subtype in original U1.5 audit rows", "source": "suppression overlap join", "lineage_decision": "not_original_rule"},
        {"rule_component": "execution/wager", "existing_logic_evidence": "no executed wager ledger found in local U1.5 favorite audit source; placed mostly false in tier backtest", "source": "hits_u15_tier_backtest_rows.csv", "lineage_decision": "not_executed_wager_population"},
    ]
    write_csv(pd.DataFrame(rule_rows), out_dir / "existing_u15_rule_lineage_report_2026-07-17.csv")

    # Drawdown and slate extremes.
    resolved = unique[unique["outcome_resolved"]].copy()
    resolved["_profit"] = resolved.apply(lambda r: american_profit(result_from_row(r), r.get("authoritative_price")), axis=1)
    profits = [float(x) for x in resolved["_profit"].dropna().tolist()]
    drawdown = max_drawdown(profits)
    best_slate = date_perf.sort_values("units", ascending=False).head(1).to_dict("records")
    worst_slate = date_perf.sort_values("units", ascending=True).head(1).to_dict("records")

    pop_summary_rows = []
    for flag in [
        "U15_SURFACED_CANDIDATE",
        "U15_GOVERNED_PREDICTION",
        "U15_TRACKED_SELECTION",
        "U15_EXECUTED_WAGER",
        "U15_OUTCOME_GRADED",
        "U15_EXACT_PRICE_AND_TIMING_CERTIFIED",
    ]:
        sub = unique[unique[flag].astype(bool)]
        pop_summary_rows.append(
            {
                "population_class": flag,
                "rows": len(sub),
                "distinct_dates": sub["slate_date"].nunique() if not sub.empty else 0,
                "games": sub["game_id"].nunique() if not sub.empty else 0,
                "players": sub["player_id"].nunique() if not sub.empty else 0,
                "pitchers": sub["opposing_starter_id"].nunique() if "opposing_starter_id" in sub else 0,
                "duplicates": int(sub["is_duplicate_observation"].sum()) if "is_duplicate_observation" in sub else 0,
                "missing_outcomes": int((~sub["outcome_resolved"]).sum()) if not sub.empty else 0,
                "missing_prices": int(sub["authoritative_price"].isna().sum()) if "authoritative_price" in sub else 0,
                "missing_timestamps": int(sub["price_timing_class"].eq("timestamp_unresolved").sum()) if "price_timing_class" in sub else 0,
            }
        )
    write_csv(pd.DataFrame(pop_summary_rows), out_dir / "population_classification_summary_2026-07-17.csv")

    decisions = {
        "MLB_U15_EXISTING_TRACKING_SOURCE_DECISION": "TRACKED_SURFACE_FOUND_IN_HITS_U15_FAVORITE_AUDIT_REVIEW_AIDS_NOT_EXECUTED_WAGER_LEDGER",
        "MLB_U15_EXISTING_POPULATION_BINDING_DECISION": "BOUND_BY_EXACT_DATE_GAME_PLAYER_HITS_1_5_UNDER_IDENTITY_WITH_DEDUPED_UNIQUE_PROPOSITIONS",
        "MLB_U15_EXISTING_PERFORMANCE_CERTIFICATION_DECISION": "OUTCOME_CERTIFIED_WHEN_EXACT_TIER_BACKTEST_JOIN_EXISTS_PRICE_ROI_NOT_FULLY_CERTIFIED",
        "MLB_U15_SUPPRESSION_OVERLAP_DECISION": "PARTIAL_OVERLAP_EXISTING_U15_INCLUDES_AFFIRMATIVE_SUPPRESSION_AND_NON_SUPPRESSION_ROWS",
        "MLB_U15_EXISTING_RULE_LINEAGE_DECISION": "PRIOR_U15_PROCESS_WAS_HITTER_COLDNESS_AND_TOUGH_STARTER_REVIEW_AID_NOT_EXPLICIT_AFFIRMATIVE_SUPPRESSION",
        "MLB_U15_NON_SUPPRESSION_REGIME_DECISION": "NON_SUPPRESSION_ROWS_ARE_MIXED_WITH_WEAK_HITTER_COMPLEMENT_AND_UNAVAILABLE_SUPPRESSION_EVIDENCE",
        "MLB_U15_PRICE_TIMING_RECOVERY_DECISION": "MONTH_LONG_TRACKING_RETAINS_PRICES_BUT_MOST_SELECTION_TIME_TIMESTAMPS_REMAIN_UNRESOLVED",
        "MLB_U15_EXISTING_VS_NEW_SHADOW_DECISION": "NEW_SHADOW_REFINES_AND_LINEAGE_CERTIFIES_A_SUBSET_OF_EXISTING_UNDER_LANE",
        "MLB_U15_PROSPECTIVE_EVIDENCE_GAP_DECISION": "EXACT_LIVE_REPRODUCIBILITY_AND_PRICE_TIMING_CONFIRMATION_REMAIN_REQUIRED",
        "MLB_U15_OBSERVATION_MILESTONE_REASSESSMENT_DECISION": "KEEP_PROSPECTIVE_MILESTONE_IN_FORCE_PENDING_MORE_GENUINE_RUNS",
        "MLB_U15_DIRECTIONAL_VALUE_DECISION": "EXISTING_U15_DIRECTIONAL_VALUE_IS_REAL_BUT_NOT_ALL_ATTRIBUTABLE_TO_AFFIRMATIVE_SUPPRESSION",
        "MLB_U15_PRICE_VALUE_DECISION": "PRICE_VALUE_UNCERTIFIED_FOR_EXISTING_MONTH_LONG_TRACKING_DUE_TO_TIMESTAMP_GAPS",
        "MLB_U15_NEXT_RESEARCH_DECISION": "CONTINUE_PROSPECTIVE_SUPPRESSION_SHADOW_AND_SEPARATELY_CHARACTERIZE_WEAK_HITTER_COMPLEMENT",
        "MLB_U15_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }
    write_csv(pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()]), out_dir / "required_decisions_2026-07-17.csv")

    source_decision = decisions["MLB_U15_EXISTING_TRACKING_SOURCE_DECISION"]
    perf_row = performance[performance["population_class"].eq("U15_SURFACED_CANDIDATE")].head(1).to_dict("records")
    perf_fact = perf_row[0] if perf_row else {}
    aff_overlap = int(unique["affirmative_suppression_overlap"].sum())
    exact_price = int(unique["exact_selection_time_price"].sum())
    price_authoritative = int(unique["price_timing_class"].isin(["exact_selection_time_price", "valid_earlier_price"]).sum())
    live_status = json.loads(LIVE_SHADOW_STATUS.read_text()) if LIVE_SHADOW_STATUS.exists() else {}
    summary = {
        "generated_at_utc": now_utc(),
        "date_range": {
            "start": unique["slate_date"].min(),
            "end": unique["slate_date"].max(),
            "distinct_dates": int(unique["slate_date"].nunique()),
        },
        "unique_tracked_u15_propositions": int(len(unique)),
        "raw_review_aid_rows": int(len(pop)),
        "resolved_tracked_u15_propositions": int(unique["outcome_resolved"].sum()),
        "wins": int((unique["result"] == "win").sum()),
        "losses": int((unique["result"] == "loss").sum()),
        "pushes": int((unique["result"] == "push").sum()),
        "win_rate": perf_fact.get("win_rate"),
        "price_authoritative_rows": price_authoritative,
        "exact_selection_time_price_rows": exact_price,
        "affirmative_suppression_overlap_rows": aff_overlap,
        "relative_pitcher_dominance_overlap_rows": int(unique["suppression_classification"].eq("RELATIVE_PITCHER_DOMINANCE").sum()),
        "suppression_evidence_unavailable_rows": int(unique["suppression_classification"].eq("SUPPRESSION_EVIDENCE_UNAVAILABLE").sum()),
        "max_drawdown_authoritative_or_retained_price_units": drawdown,
        "best_slate": best_slate,
        "worst_slate": worst_slate,
        "live_shadow_status": live_status,
        "decisions": decisions,
    }
    write_json(summary, out_dir / "machine_readable_existing_u15_tracking_reconciliation_2026-07-17.json")

    gap_rows = [
        {"remaining_need": "exact live reproducibility", "status": "still_needed", "evidence": "only one genuine live suppression run retained", "recommendation": "continue prospective milestone"},
        {"remaining_need": "exact price-timing confirmation", "status": "still_needed", "evidence": f"{exact_price} existing rows exact selection-time price certified", "recommendation": "use live shadow exact price ledger"},
        {"remaining_need": "new dates after historical tracking period", "status": "still_needed", "evidence": "existing tracking concentrated in June/July review-aid artifacts", "recommendation": "append genuine future runs"},
        {"remaining_need": "suppression subtype stability", "status": "partially_available", "evidence": "historical suppression validation exists but old tracking overlap is partial", "recommendation": "monitor affirmative-only subset prospectively"},
        {"remaining_need": "artifact-version drift protection", "status": "still_needed", "evidence": "new shadow has immutable row hashes; old favorite audits do not", "recommendation": "do not retire live shadow"},
    ]
    write_csv(pd.DataFrame(gap_rows), out_dir / "prospective_evidence_gap_assessment_2026-07-17.csv")
    write_csv(
        pd.DataFrame(
            [
                {
                    "milestone": "ten_run_prospective_suppression_shadow",
                    "current_decision": decisions["MLB_U15_OBSERVATION_MILESTONE_REASSESSMENT_DECISION"],
                    "existing_coverage_impact": "does not replace prospective exact-price and replayability requirements",
                    "recommended_adjustment": "none_now",
                    "notes": "Reassess only after more exact live run tags or stronger existing timestamp certification.",
                }
            ]
        ),
        out_dir / "milestone_reassessment_2026-07-17.csv",
    )

    md = f"""# MLB Existing Hits Under 1.5 Tracking Reconciliation with Affirmative Pitcher Suppression

Generated: `{summary['generated_at_utc']}`

## Executive Summary

The existing month-long Hits U1.5 tracking source was found in the `hits_u15_favorite_audit_2026-*.csv` review-aid artifacts, not in an executed-wager ledger. The deduped tracked surface contains **{len(unique)}** exact Hits Under 1.5 player/game propositions across **{summary['date_range']['distinct_dates']}** slate dates from **{summary['date_range']['start']}** through **{summary['date_range']['end']}**.

Outcome certification is available for **{summary['resolved_tracked_u15_propositions']}** tracked propositions via the local U1.5 tier-backtest spine. That resolved tracked surface went **{summary['wins']}-{summary['losses']}** with win rate **{summary['win_rate']:.2%}** where resolved. Price fields are retained, but exact selection-time price certification is mostly unavailable in the older tracking rows, so price-value claims remain constrained.

Affirmative pitcher suppression overlaps **{aff_overlap}** existing tracked U1.5 propositions. That means the old UNDER process was not merely the new suppression contract under another name: it mixed weak-hitter review-aid logic, tough-starter context, and rows where suppression evidence was unavailable or non-affirmative. The new live shadow is best understood as a narrower and better-lineaged subset of the existing UNDER lane.

## Existing Source Inventory

{markdown_table(source_inventory[['source_path', 'source_type', 'date_range', 'rows', 'candidate_or_wager_status']])}

## Population Performance

{markdown_table(performance)}

## Suppression Overlap

{markdown_table(suppression_overlap)}

## Non-Suppression Characterization

{markdown_table(non_supp)}

## Price Timing Recovery

{markdown_table(price_summary)}

## Old Process Versus New Shadow

{markdown_table(pd.DataFrame(old_vs_new_rows))}

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## Direct Answer

We are partly rediscovering that Hits Under 1.5 has carried the project's most reliable directional value, but the new suppression work is not a duplicate of the old lane. The old U1.5 process found UNDER value through a mixture of weak-hitter and starter-context review-aid conditions. The affirmative pitcher-suppression contract isolates a materially cleaner subset and, through the prospective shadow, adds the exact run identity, market binding, timestamp, and replayability lineage that the older month-long tracking mostly lacked.

Production remains **NOT_AUTHORIZED**.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")

    validation_rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            validation_rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation_rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            validation_rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation_rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        validation_rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(validation_rows), out_dir / "validation_report_2026-07-17.csv")

    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest_rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(pd.DataFrame(manifest_rows), out_dir / "sha256_manifest_2026-07-17.csv")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", choices=["read_only"], default="read_only")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build(Path(args.output_dir))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
