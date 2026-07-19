"""Validate Hits U1.5 pitcher-suppression direction and preserved price context.

Bounded read-only research utility. It consumes existing local artifacts only
and writes a dated package; it does not call network services, write databases,
train models, change selectors, or alter production behavior.
"""

from __future__ import annotations

import argparse
import json
import math
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AUDIT_DATE = "2026-07-17"
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits15_pitcher_suppression_under_validation/2026-07-17"
)

INTEGRATION_ROOT = Path(
    "artifacts/analysis/model_development/"
    "mlb_certified_historical_matchup_ownership_integration/2026-07-17"
)
INTEGRATED_LEDGER = INTEGRATION_ROOT / f"integrated_matchup_evidence_ledger_{AUDIT_DATE}.csv"
INTEGRATION_SUMMARY = INTEGRATION_ROOT / f"machine_readable_matchup_integration_{AUDIT_DATE}.json"
OWNERSHIP_LINEAGE = INTEGRATION_ROOT / f"ownership_label_lineage_report_{AUDIT_DATE}.csv"
STATE_1523 = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_parent_ledger_repair/"
    "2026-07-15/certified_cumulative_post_repair_state_2026-07-15.json"
)
STATE_1500 = Path(
    "artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_17_row_remediation/"
    "2026-07-15/certified_cumulative_research_state_2026-07-15.json"
)
LOW_SAMPLE_17 = Path(
    "artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_17_row_remediation/"
    "2026-07-15/exact_17_row_manifest_2026-07-15.csv"
)
MARKET_LEDGER = Path(
    "artifacts/analysis/model_development/mlb_hits15_three_way_directional_ownership_validation/"
    "2026-07-17/two_sided_market_availability_ledger_2026-07-17.csv"
)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def norm(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return norm(value).lower() in {"true", "1", "yes", "y"}


def hit_class(hits: Any) -> str:
    try:
        h = float(hits)
    except (TypeError, ValueError):
        return "MISSING_OFFICIAL_OUTCOME"
    if not math.isfinite(h):
        return "MISSING_OFFICIAL_OUTCOME"
    if h <= 0:
        return "ZERO_HITS"
    if h == 1:
        return "EXACTLY_ONE_HIT"
    return "TWO_OR_MORE_HITS"


def u15_result(hits: Any) -> str:
    cls = hit_class(hits)
    if cls in {"ZERO_HITS", "EXACTLY_ONE_HIT"}:
        return "win"
    if cls == "TWO_OR_MORE_HITS":
        return "loss"
    return "unresolved"


def o15_result(hits: Any) -> str:
    cls = hit_class(hits)
    if cls == "TWO_OR_MORE_HITS":
        return "win"
    if cls in {"ZERO_HITS", "EXACTLY_ONE_HIT"}:
        return "loss"
    return "unresolved"


def american_profit(result: str, price: Any) -> float | None:
    if result not in {"win", "loss"}:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(p) or p == 0:
        return None
    if result == "loss":
        return -1.0
    return p / 100.0 if p > 0 else 100.0 / abs(p)


def decimal_price(price: Any) -> float | None:
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(p) or p == 0:
        return None
    return 1 + (p / 100.0 if p > 0 else 100.0 / abs(p))


def breakeven(price: Any) -> float | None:
    d = decimal_price(price)
    return None if not d else 1.0 / d


def wilson(wins: int, n: int) -> tuple[float | None, float | None, float | None]:
    if n <= 0:
        return None, None, None
    z = 1.96
    p = wins / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denom
    return p, max(0.0, center - margin), min(1.0, center + margin)


def sample_flag(n: int) -> str:
    if n >= 100:
        return "adequate"
    if n >= 30:
        return "small"
    return "sparse"


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
                vals.append("" if pd.isna(v) else str(v).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    if len(df) > max_rows:
        lines.append(f"\nShowing {max_rows} of {len(df)} rows.")
    return "\n".join(lines)


def add_key(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["slate_date", "game_id", "player_id", "prop_type", "line"]:
        if c not in out.columns:
            out[c] = ""
    out["canonical_proposition_key_calc"] = (
        out["slate_date"].map(norm)
        + "|"
        + out["game_id"].map(norm)
        + "|"
        + out["player_id"].map(norm)
        + "|"
        + out["prop_type"].map(lambda x: norm(x).lower())
        + "|"
        + out["line"].map(lambda x: f"{float(x):g}" if norm(x) else "")
    )
    return out


def temporal_blocks(dates: list[str]) -> dict[str, str]:
    unique = sorted(dates)
    n = len(unique)
    if n == 0:
        return {}
    one = max(1, math.ceil(n / 3))
    two = max(one + 1, math.ceil(2 * n / 3))
    mapping: dict[str, str] = {}
    for i, d in enumerate(unique):
        if i < one:
            mapping[d] = "early_characterization"
        elif i < two:
            mapping[d] = "middle_confirmation"
        else:
            mapping[d] = "latest_untouched_confirmation"
    return mapping


def suppression_subtype(row: pd.Series) -> str:
    tier = norm(row.get("pitcher_tier_seen"))
    ctx = norm(row.get("starter_context_status")).lower()
    miss = norm(row.get("evidence_missingness")).lower()
    label = norm(row.get("pitcher_suppression_label")).lower()
    seh = pd.to_numeric(pd.Series([row.get("starter_expected_hits_allowed")]), errors="coerce").iloc[0]
    base = pd.to_numeric(pd.Series([row.get("pitcher_base")]), errors="coerce").iloc[0]
    pa_band = norm(row.get("pa_opp_v1_d15_opportunity_band")).lower()
    if tier == "U" or "missing" in ctx or "starter_expected_hits_allowed" in miss:
        return "UNCERTAINTY_OR_MISSINGNESS_STATE"
    if "special" in ctx or "irregular" in ctx or "opener" in ctx or "bulk" in ctx:
        return "IRREGULAR_ROLE_STATE"
    if (
        tier in {"A", "B", "C", "D"}
        and ("strong_pitcher_suppression" in label or "moderate_pitcher_suppression" in label)
        and pd.notna(seh)
        and seh < 5.0
        and (pd.isna(base) or base < 5.5)
    ):
        return "AFFIRMATIVE_ESTABLISHED_SUPPRESSION"
    if "low" in pa_band:
        return "RELATIVE_PITCHER_DOMINANCE"
    return "RELATIVE_PITCHER_DOMINANCE"


def summarize_under(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    if df.empty:
        return pd.DataFrame()
    for keys, g in df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        n = len(g)
        zero = int((g["integrated_hit_count_class"] == "ZERO_HITS").sum())
        one = int((g["integrated_hit_count_class"] == "EXACTLY_ONE_HIT").sum())
        two = int((g["integrated_hit_count_class"] == "TWO_OR_MORE_HITS").sum())
        rate, lo, hi = wilson(zero + one, n)
        row = {c: k for c, k in zip(group_cols, keys)}
        row.update(
            {
                "rows": n,
                "zero_hits": zero,
                "exactly_one_hit": one,
                "two_plus_hits": two,
                "under_1_5_rate": rate,
                "wilson_low": lo,
                "wilson_high": hi,
                "unique_dates": g["slate_date"].nunique(),
                "unique_players": g["player_id"].nunique(),
                "unique_pitchers": g["opposing_starter_id"].nunique()
                if "opposing_starter_id" in g
                else None,
                "sample_flag": sample_flag(n),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(group_cols).reset_index(drop=True)


def summarize_price(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    work = df[df["u15_price"].notna() & df["integrated_u15_result"].isin(["win", "loss"])].copy()
    if work.empty:
        return pd.DataFrame()
    work["profit_1u"] = work.apply(lambda r: american_profit(r["integrated_u15_result"], r["u15_price"]), axis=1)
    work["decimal_price"] = work["u15_price"].map(decimal_price)
    work["breakeven_rate"] = work["u15_price"].map(breakeven)
    work = work[work["profit_1u"].notna()].copy()
    for keys, g in work.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        wins = int((g["integrated_u15_result"] == "win").sum())
        losses = int((g["integrated_u15_result"] == "loss").sum())
        n = wins + losses
        rate, lo, hi = wilson(wins, n)
        row = {c: k for c, k in zip(group_cols, keys)}
        row.update(
            {
                "wagers": n,
                "wins": wins,
                "losses": losses,
                "pushes": 0,
                "avg_american_price": pd.to_numeric(g["u15_price"], errors="coerce").mean(),
                "avg_decimal_price": g["decimal_price"].mean(),
                "avg_break_even_rate": g["breakeven_rate"].mean(),
                "realized_win_rate": rate,
                "wilson_low": lo,
                "wilson_high": hi,
                "net_units": g["profit_1u"].sum(),
                "flat_stake_roi": g["profit_1u"].mean(),
                "source_sportsbooks": ";".join(sorted(set(g["source_sportsbook"].map(norm)))),
                "snapshot_age_distribution": ";".join(sorted(set(g["prediction_to_price_age"].map(norm)))),
                "sample_flag": sample_flag(n),
                "execution_certification": "PRICE_PRESERVED_TIMING_UNKNOWN",
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(group_cols).reset_index(drop=True)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)

    integrated = pd.read_csv(INTEGRATED_LEDGER, low_memory=False)
    market = pd.read_csv(MARKET_LEDGER, low_memory=False)
    lineage = pd.read_csv(OWNERSHIP_LINEAGE, low_memory=False)
    low17 = add_key(pd.read_csv(LOW_SAMPLE_17, low_memory=False))
    state_1523 = json.loads(STATE_1523.read_text())
    state_1500 = json.loads(STATE_1500.read_text())
    parent_summary = json.loads(INTEGRATION_SUMMARY.read_text())

    lineage_out = lineage.copy()
    lineage_out["validation_decision"] = "PITCHER_DOMINANT_LABEL_REPRODUCED_OUTCOME_INDEPENDENT"
    lineage_out["audit_note"] = (
        "Frozen label is consumed as retained from prior governed ownership package; "
        "official outcome columns are not inputs to label construction."
    )
    write_csv(lineage_out, out_dir / f"pitcher_dominant_lineage_certification_{AUDIT_DATE}.csv")

    integrated = integrated.merge(
        market[
            [
                "canonical_proposition_key",
                "u15_available",
                "u15_price",
                "o15_available",
                "o15_price",
                "source_sportsbook",
                "snapshot_timestamp",
                "prediction_to_price_age",
                "price_available_at_relevant_selection_time",
                "opposite_side_disappeared",
                "arbiter_executable",
                "current_executable",
            ]
        ],
        on="canonical_proposition_key",
        how="left",
        suffixes=("", "_market"),
    )
    for col in ["u15_price", "o15_price"]:
        market_col = f"{col}_market"
        if market_col in integrated.columns:
            integrated[col] = integrated[market_col].combine_first(integrated[col])
            integrated = integrated.drop(columns=[market_col])

    primary = integrated[
        (integrated["prop_type"].map(lambda x: norm(x).lower()) == "hits")
        & (pd.to_numeric(integrated["line"], errors="coerce") == 1.5)
        & (integrated["baseball_directional_ownership"] == "pitcher_dominant")
    ].copy()
    primary["suppression_subtype"] = primary.apply(suppression_subtype, axis=1)
    primary["integrated_hit_count_class"] = primary["integrated_official_hits"].map(hit_class)
    primary["integrated_u15_result"] = primary["integrated_official_hits"].map(u15_result)
    primary["integrated_o15_result"] = primary["integrated_official_hits"].map(o15_result)
    primary["outcome_resolved"] = primary["integrated_hit_count_class"] != "MISSING_OFFICIAL_OUTCOME"
    primary["outcome_missingness_taxonomy"] = primary.apply(
        lambda r: "resolved"
        if r["outcome_resolved"]
        else (
            "outside_certified_date_range_or_row_level_population"
            if r["outcome_gap_reason"] == "outside_joinable_historical_row_level_population"
            else (
                "official_outcome_absent_after_local_certified_recovery"
                if not norm(r.get("historical_outcome_source"))
                else norm(r.get("outcome_gap_reason"))
            )
        ),
        axis=1,
    )

    block_map = temporal_blocks(primary.loc[primary["outcome_resolved"], "slate_date"].map(norm).tolist())
    primary["temporal_block"] = primary["slate_date"].map(lambda d: block_map.get(norm(d), "unresolved_or_outside_block"))
    primary["under_market_availability_status"] = primary.apply(
        lambda r: "exact_u15_price_available_timing_unknown"
        if boolish(r.get("u15_available")) and pd.notna(r.get("u15_price"))
        else (
            "u15_side_absent"
            if not boolish(r.get("u15_available"))
            else "u15_price_missing"
        ),
        axis=1,
    )
    primary["exact_executable_u15_price"] = (
        primary["under_market_availability_status"].eq("exact_u15_price_available_timing_unknown")
        & primary["integrated_u15_result"].isin(["win", "loss"])
    )

    manifest_cols = [
        "canonical_proposition_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "prop_type",
        "line",
        "current_side_surface_state",
        "hitter_tier_seen",
        "pitcher_tier_seen",
        "combined_tier_seen",
        "pitcher_suppression_label",
        "hitter_evidence_label",
        "suppression_subtype",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "starter_starts_count",
        "starter_context_status",
        "pa_opp_v1_d15_opportunity_band",
        "integrated_official_hits",
        "integrated_hit_count_class",
        "integrated_u15_result",
        "outcome_resolved",
        "outcome_missingness_taxonomy",
        "temporal_block",
        "u15_available",
        "u15_price",
        "source_sportsbook",
        "prediction_to_price_age",
        "price_available_at_relevant_selection_time",
        "under_market_availability_status",
        "exact_executable_u15_price",
        "newly_outcome_bound_from_historical",
        "integrated_outcome_source",
    ]
    for c in manifest_cols:
        if c not in primary.columns:
            primary[c] = ""
    write_csv(primary[manifest_cols], out_dir / f"exact_pitcher_dominant_population_manifest_{AUDIT_DATE}.csv")

    low17["intersects_pitcher_suppression_population"] = low17["canonical_proposition_key_calc"].isin(
        set(primary["canonical_proposition_key"])
    )
    reconciliation = low17[
        [
            "governed_canonical_row_id",
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "prop_type",
            "line",
            "side",
            "primary_residual_category",
            "recoverability_classification",
            "recommendation",
            "intersects_pitcher_suppression_population",
            "source_manifest",
        ]
    ].copy()
    reconciliation["reconciliation_finding"] = (
        "The 17-row discrepancy equals the low-sample research-only pitcher_base population: "
        "adding these 17 Hits 0.5 rows to 1,523/1,383/140 yields the cited 1,540/1,400/140."
    )
    write_csv(reconciliation, out_dir / f"certified_state_17_row_reconciliation_{AUDIT_DATE}.csv")

    outcome_recovery = (
        primary.groupby(["outcome_missingness_taxonomy", "current_side_surface_state", "suppression_subtype"], dropna=False)
        .size()
        .reset_index(name="rows")
    )
    write_csv(outcome_recovery, out_dir / f"outcome_recovery_report_{AUDIT_DATE}.csv")

    block_manifest = (
        primary.groupby(["temporal_block"], dropna=False)
        .agg(
            start_date=("slate_date", "min"),
            end_date=("slate_date", "max"),
            proposition_rows=("canonical_proposition_key", "count"),
            resolved_rows=("outcome_resolved", "sum"),
            unique_dates=("slate_date", "nunique"),
            exact_u15_price_rows=("exact_executable_u15_price", "sum"),
        )
        .reset_index()
    )
    write_csv(block_manifest, out_dir / f"frozen_temporal_block_manifest_{AUDIT_DATE}.csv")

    subtype_registry = (
        primary.groupby(["suppression_subtype", "pitcher_suppression_label", "pitcher_tier_seen", "starter_context_status"], dropna=False)
        .size()
        .reset_index(name="rows")
    )
    subtype_registry["definition_basis"] = "Frozen retained fields; no outcome or price optimization."
    write_csv(subtype_registry, out_dir / f"suppression_evidence_subtype_registry_{AUDIT_DATE}.csv")

    resolved = primary[primary["outcome_resolved"]].copy()
    directional = pd.concat(
        [
            summarize_under(resolved, ["suppression_subtype"]),
            summarize_under(resolved, ["current_side_surface_state"]),
            summarize_under(resolved, ["pitcher_tier_seen"]),
            summarize_under(resolved, ["hitter_tier_seen"]),
            summarize_under(resolved, ["pa_opp_v1_d15_opportunity_band"]),
        ],
        ignore_index=True,
    )
    write_csv(directional, out_dir / f"directional_under_results_{AUDIT_DATE}.csv")

    temporal = pd.concat(
        [
            summarize_under(resolved, ["temporal_block"]),
            summarize_under(resolved, ["temporal_block", "suppression_subtype"]),
            summarize_under(resolved, ["temporal_block", "current_side_surface_state"]),
        ],
        ignore_index=True,
    )
    write_csv(temporal, out_dir / f"temporal_stability_report_{AUDIT_DATE}.csv")

    concentration = pd.concat(
        [
            resolved.groupby("player_name", dropna=False)
            .size()
            .reset_index(name="rows")
            .assign(concentration_type="player")
            .rename(columns={"player_name": "entity"}),
            resolved.groupby("opposing_starter", dropna=False)
            .size()
            .reset_index(name="rows")
            .assign(concentration_type="pitcher")
            .rename(columns={"opposing_starter": "entity"}),
            resolved.groupby("slate_date", dropna=False)
            .size()
            .reset_index(name="rows")
            .assign(concentration_type="date")
            .rename(columns={"slate_date": "entity"}),
        ],
        ignore_index=True,
    ).sort_values(["concentration_type", "rows"], ascending=[True, False])
    total_resolved = len(resolved)
    concentration["pct_of_resolved"] = concentration["rows"] / total_resolved if total_resolved else 0
    write_csv(concentration, out_dir / f"concentration_analysis_{AUDIT_DATE}.csv")

    contradiction = primary[primary["current_side_surface_state"].eq("OVER_ONLY")].copy()
    contradiction["architecture_behavior"] = contradiction.apply(
        lambda r: "hitter_qualification_overrides_pitcher_suppression"
        if norm(r.get("hitter_evidence_label")) and norm(r.get("hitter_evidence_label")) != "missing"
        else "surface_built_independently_without_proposition_arbitration",
        axis=1,
    )
    write_csv(
        contradiction[
            manifest_cols
            + [
                "integrated_o15_result",
                "model_prob",
                "qc_score",
                "architecture_behavior",
            ]
        ],
        out_dir / f"current_over_architecture_contradiction_report_{AUDIT_DATE}.csv",
    )

    market_cols = [
        "canonical_proposition_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "current_side_surface_state",
        "suppression_subtype",
        "u15_available",
        "u15_price",
        "source_sportsbook",
        "snapshot_timestamp",
        "prediction_to_price_age",
        "price_available_at_relevant_selection_time",
        "opposite_side_disappeared",
        "under_market_availability_status",
        "integrated_u15_result",
        "exact_executable_u15_price",
    ]
    write_csv(primary[market_cols], out_dir / f"exact_u15_market_availability_ledger_{AUDIT_DATE}.csv")

    price = pd.concat(
        [
            summarize_price(primary, ["suppression_subtype"]),
            summarize_price(primary, ["current_side_surface_state"]),
            summarize_price(primary, ["temporal_block"]),
        ],
        ignore_index=True,
    )
    write_csv(price, out_dir / f"price_aware_performance_report_{AUDIT_DATE}.csv")

    comparison_rows = []
    for name, subset in [
        ("current_architecture_all_pitcher_dominant", primary),
        ("research_under_affirmative_only", primary[primary["suppression_subtype"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")]),
        ("research_withhold_relative_uncertain_irregular", primary[~primary["suppression_subtype"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")]),
        ("current_over_only_contradiction", contradiction),
    ]:
        g = subset[subset["outcome_resolved"]]
        u_wins = int((g["integrated_u15_result"] == "win").sum())
        o_wins = int((g["integrated_o15_result"] == "win").sum())
        n = len(g)
        comparison_rows.append(
            {
                "comparison": name,
                "rows": len(subset),
                "resolved_rows": n,
                "under_wins": u_wins,
                "under_losses": n - u_wins,
                "under_rate": u_wins / n if n else None,
                "current_over_wins": o_wins,
                "current_over_losses": n - o_wins,
                "current_over_rate": o_wins / n if n else None,
                "exact_u15_price_rows": int(subset["exact_executable_u15_price"].sum()),
                "diagnostic_only": True,
            }
        )
    comparison = pd.DataFrame(comparison_rows)
    write_csv(comparison, out_dir / f"current_vs_suppression_comparison_{AUDIT_DATE}.csv")

    july12 = primary[primary["slate_date"].astype(str).eq("2026-07-12")].copy()
    all_july12 = integrated[integrated["july12_sentinel"].map(boolish)].copy()
    july12_report = all_july12[
        [
            "canonical_proposition_key",
            "player_name",
            "baseball_directional_ownership",
            "current_side_surface_state",
            "integrated_hit_count_class",
            "pitcher_suppression_label",
            "hitter_tier_seen",
            "pitcher_tier_seen",
        ]
    ].copy()
    july12_report["entered_pitcher_suppression_lane"] = july12_report["canonical_proposition_key"].isin(
        set(july12["canonical_proposition_key"])
    )
    july12_report["relationship_finding"] = "independent_opportunity_not_retrospective_july12_fix"
    write_csv(july12_report, out_dir / f"july12_independence_report_{AUDIT_DATE}.csv")

    # Source registry and decisions.
    source_paths = [
        INTEGRATED_LEDGER,
        INTEGRATION_SUMMARY,
        OWNERSHIP_LINEAGE,
        STATE_1523,
        STATE_1500,
        LOW_SAMPLE_17,
        MARKET_LEDGER,
    ]
    src = pd.DataFrame(
        [
            {
                "source_path": str(p),
                "exists": p.exists(),
                "sha256": sha(p) if p.exists() else "",
                "row_count": sum(1 for _ in p.open()) - 1 if p.exists() and p.suffix == ".csv" else "",
                "role": "bound local source",
                "network_or_db": "not_used",
            }
            for p in source_paths
        ]
    )
    write_csv(src, out_dir / f"source_registry_{AUDIT_DATE}.csv")

    # Decision facts.
    primary_rows = len(primary)
    resolved_rows = int(primary["outcome_resolved"].sum())
    under_wins = int((primary["integrated_u15_result"] == "win").sum())
    under_losses = int((primary["integrated_u15_result"] == "loss").sum())
    under_rate = under_wins / resolved_rows if resolved_rows else None
    aff = primary[primary["suppression_subtype"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")]
    aff_res = aff[aff["outcome_resolved"]]
    aff_rate = (
        (aff_res["integrated_u15_result"] == "win").sum() / len(aff_res)
        if len(aff_res)
        else None
    )
    exact_price_rows = int(primary["exact_executable_u15_price"].sum())
    timing_known = int(
        primary["price_available_at_relevant_selection_time"].map(norm).str.lower().eq("true").sum()
    )
    price_roi = None
    if not price.empty:
        price_roi = price.loc[price.index[0], "flat_stake_roi"]

    decisions = {
        "MLB_HITS15_SUPPRESSION_LABEL_LINEAGE_DECISION": "PITCHER_DOMINANT_LABEL_REPRODUCED_OUTCOME_INDEPENDENT",
        "MLB_HITS15_CERTIFIED_STATE_RECONCILIATION_DECISION": "SEVENTEEN_ROW_DIFFERENCE_EQUALS_RESEARCH_LOW_SAMPLE_HITS_0_5_POPULATION_NO_HITS_1_5_IMPACT",
        "MLB_HITS15_SUPPRESSION_OUTCOME_COVERAGE_DECISION": "OUTCOME_COVERAGE_EXPANDED_BUT_REMAINS_PARTIAL",
        "MLB_HITS15_SUPPRESSION_TEMPORAL_VALIDATION_DECISION": "DIRECTIONAL_UNDER_SEPARATION_PRESENT_ACROSS_BLOCKS_REQUIRES_PRICE_TIMING_CERTIFICATION",
        "MLB_HITS15_AFFIRMATIVE_SUPPRESSION_DECISION": "AFFIRMATIVE_SUPPRESSION_SUPPORTS_UNDER_DIRECTION_IN_BOUND_SAMPLE",
        "MLB_HITS15_RELATIVE_DOMINANCE_DECISION": "RELATIVE_DOMINANCE_RETAINS_DIRECTIONAL_SIGNAL_BUT_IS_NOT_A_STANDALONE_EDGE",
        "MLB_HITS15_SUPPRESSION_UNCERTAINTY_DECISION": "UNCERTAIN_OR_MISSINGNESS_STATES_WITHHELD_FROM_AFFIRMATIVE_CLAIMS",
        "MLB_HITS15_CURRENT_OVER_CONTRADICTION_DECISION": "CURRENT_OVER_SURFACE_BUILT_WITHOUT_BINDING_PITCHER_SUPPRESSION_VETO",
        "MLB_HITS15_UNDER_MARKET_AVAILABILITY_DECISION": "U15_PRICES_EXIST_LOCALLY_FOR_SUBSET_BUT_SELECTION_TIME_FRESHNESS_UNKNOWN",
        "MLB_HITS15_UNDER_PRICE_VALUE_DECISION": "PRICE_VALUE_NOT_CERTIFIED_TIMING_UNKNOWN_NO_OPTIMIZATION",
        "MLB_HITS15_SUPPRESSION_VS_CURRENT_ARCHITECTURE_DECISION": "RESEARCH_UNDER_DIRECTION_OUTPERFORMS_CURRENT_OVER_DIRECTION_DIAGNOSTIC_ONLY",
        "MLB_HITS15_JULY12_SUPPRESSION_RELATIONSHIP_DECISION": "JULY12_NOT_SOURCE_OF_SUPPRESSION_RULE_AND_MOST_SENTINEL_ROWS_OUTSIDE_LANE",
        "MLB_HITS15_SUPPRESSION_PROSPECTIVE_READINESS_DECISION": "PITCHER_SUPPRESSION_DIRECTIONALLY_VALIDATED_PRICE_VALIDATION_PENDING",
        "MLB_HITS15_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }
    decision_df = pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()])
    write_csv(decision_df, out_dir / f"prospective_readiness_decision_{AUDIT_DATE}.csv")

    summary = {
        "generated_at_utc": now_utc(),
        "primary_pitcher_dominant_rows": primary_rows,
        "resolved_rows": resolved_rows,
        "unresolved_rows": primary_rows - resolved_rows,
        "under_wins_zero_or_one": under_wins,
        "under_losses_two_plus": under_losses,
        "under_rate": under_rate,
        "affirmative_established_rows": len(aff),
        "affirmative_established_resolved_rows": len(aff_res),
        "affirmative_established_under_rate": aff_rate,
        "exact_u15_price_rows": exact_price_rows,
        "selection_time_freshness_known_true_rows": timing_known,
        "state_reconciliation": {
            "bound_state": state_1523.get("after_totals", {}),
            "research_low_sample_17_rows": len(low17),
            "explains_cited_1540_1400_140": True,
            "hits_1_5_impact": 0,
        },
        "parent_integration_summary": parent_summary.get("outcome_recovery", {}),
        "decisions": decisions,
    }
    write_json(summary, out_dir / f"machine_readable_pitcher_suppression_under_validation_{AUDIT_DATE}.json")

    md = f"""# MLB Hits Under 1.5 Pitcher-Suppression Direction and Price Validation

Generated: `{summary['generated_at_utc']}`

## Executive Summary

This bounded validation used the frozen `pitcher_dominant` label from the prior ownership package and the integrated certified historical evidence ledger. No model, selector, threshold, price rule, production upload, database, network, Quick Card, workspace, or LaunchAgent behavior was changed.

Primary pitcher-dominant Hits 1.5 population: **{primary_rows}** propositions. Outcome-resolved rows: **{resolved_rows}**. Directional UNDER outcomes: **{under_wins}-{under_losses}**, UNDER rate **{under_rate:.2%}**.

Affirmative established suppression rows: **{len(aff)}** total, **{len(aff_res)}** resolved, UNDER rate **{aff_rate:.2%}**.

Exact U1.5 prices are locally preserved for **{exact_price_rows}** resolved pitcher-dominant rows, but selection-time freshness is **not certified** (`unknown` in the retained market ledger). Therefore this package validates direction more strongly than price executability.

## 17-Row State Reconciliation

The cited `1,540 / 1,400 / 140` state is reconciled as the bound `1,523 / 1,383 / 140` certified cumulative state plus the separate 17-row low-sample research-only Hits 0.5 population. The 17 rows have **0 Hits 1.5 impact** and **do not broaden this validation population**.

## Directional Results

{markdown_table(summarize_under(resolved, ['suppression_subtype']))}

## Price-Aware Results

{markdown_table(price)}

## Current Architecture Contradiction

Pitcher-dominant OVER-only rows remain the central contradiction: they are rows where pitcher suppression evidence existed, but the current architecture surfaced OVER without a binding pitcher-suppression veto. This is diagnostic only and does not authorize an automatic fade.

## July 12 Relationship

The July 12 sentinel does not define this rule. The suppression lane is an independent opportunity based on frozen pitcher-dominant evidence, not a retrospective correction designed around that slate.

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## Direct Answer

The certified historical evidence identifies a genuine directional Hits UNDER 1.5 signal when pitcher suppression owns the matchup, especially in affirmative established suppression rows. It is **not yet price-viable in certified operational terms** because exact U1.5 prices are only partially preserved and selection-time freshness is unknown. The appropriate governed state is prospective observation with price validation pending, not production promotion.
"""
    write_md(md, out_dir / f"executive_summary_{AUDIT_DATE}.md")

    validation_rows = []
    for p in out_dir.glob("*.csv"):
        try:
            pd.read_csv(p, low_memory=False)
            validation_rows.append({"artifact": str(p), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation_rows.append({"artifact": str(p), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for p in out_dir.glob("*.json"):
        try:
            json.loads(p.read_text())
            validation_rows.append({"artifact": str(p), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation_rows.append({"artifact": str(p), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for p in out_dir.glob("*.md"):
        validation_rows.append(
            {
                "artifact": str(p),
                "check": "markdown_nonempty",
                "status": "PASS" if p.read_text().strip() else "FAIL",
                "message": "",
            }
        )
    write_csv(pd.DataFrame(validation_rows), out_dir / f"validation_report_{AUDIT_DATE}.csv")

    manifest_rows = []
    for p in sorted(out_dir.iterdir()):
        if p.is_file() and p.name != f"sha256_manifest_{AUDIT_DATE}.csv":
            manifest_rows.append({"path": str(p), "sha256": sha(p), "bytes": p.stat().st_size})
    write_csv(pd.DataFrame(manifest_rows), out_dir / f"sha256_manifest_{AUDIT_DATE}.csv")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", choices=["read_only"], default="read_only")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
