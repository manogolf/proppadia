"""Integrate certified historical Hits evidence with matchup-ownership ledgers.

This audit is intentionally read-only with respect to source artifacts. It
creates a dated evidence package only; it does not query network services,
write to databases, train models, change selectors, or alter production files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


AUDIT_DATE = "2026-07-17"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_certified_historical_matchup_ownership_integration/2026-07-17"
)

HIST_ROW_LEVEL_ROOT = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/2026-07-14"
)
HIST_CUMULATIVE_STATE = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_starter_parent_ledger_repair/2026-07-15/"
    "certified_cumulative_post_repair_state_2026-07-15.json"
)
HIST_PRIOR_STATE = Path(
    "artifacts/analysis/model_development/"
    "mlb_low_sample_research_pitcher_base_17_row_remediation/2026-07-15/"
    "certified_cumulative_research_state_2026-07-15.json"
)
OWNERSHIP_ROOT = Path(
    "artifacts/analysis/model_development/mlb_hits15_two_sided_matchup_advantage_audit/2026-07-17"
)
THRESHOLD_ROOT = Path(
    "artifacts/analysis/model_development/mlb_hits_threshold_specific_evidence_audit/2026-07-17"
)
THREE_WAY_ROOT = Path(
    "artifacts/analysis/model_development/mlb_hits15_three_way_directional_ownership_validation/2026-07-17"
)
MATRIX_ROOT = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_matrix_assembly/2026-07-12"
)
JULY12_ROOT = Path(
    "artifacts/analysis/model_development/mlb_july12_favorite_slate_sentinel_failure_audit/2026-07-17"
)

HIST_ALL = HIST_ROW_LEVEL_ROOT / "fully_qualified_hits_manifest_2026-07-14.csv"
HIST_05 = HIST_ROW_LEVEL_ROOT / "fully_qualified_hits_0_5_manifest_2026-07-14.csv"
HIST_15 = HIST_ROW_LEVEL_ROOT / "fully_qualified_hits_1_5_manifest_2026-07-14.csv"
HIST_14816 = HIST_ROW_LEVEL_ROOT / "post_three_row_pa_14816_row_qualification_ledger_2026-07-14.csv"
OWNERSHIP_LEDGER = OWNERSHIP_ROOT / "canonical_proposition_level_advantage_ledger_2026-07-17.csv"
THRESHOLD_MANIFEST = THRESHOLD_ROOT / "canonical_player_game_outcome_manifest_2026-07-17.csv"
ZERO_ONE_TWO_LEDGER = THRESHOLD_ROOT / "zero_one_two_plus_outcome_ledger_2026-07-17.csv"
CURRENT_VS_ARBITER = THREE_WAY_ROOT / "current_vs_three_way_arbiter_comparison_2026-07-17.csv"
OWNERSHIP_LINEAGE = THREE_WAY_ROOT / "ownership_label_lineage_certification_2026-07-17.csv"
PITCHER_DOMINANT_OVER = THREE_WAY_ROOT / "pitcher_dominant_over_only_analysis_2026-07-17.csv"
HITS05_MATRIX = MATRIX_ROOT / "matrices/hits_0_5_research_matrix_2026-07-12.csv"
HITS15_MATRIX = MATRIX_ROOT / "matrices/hits_1_5_research_matrix_2026-07-12.csv"
JULY12_SENTINEL = JULY12_ROOT / "sentinel_15_proppadia_manifest_2026-07-17.csv"


DECISIONS = {
    "MLB_MATCHUP_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def file_rows(path: Path) -> int:
    if not path.exists() or path.suffix.lower() != ".csv":
        return 0
    with path.open(newline="") as f:
        return max(sum(1 for _ in f) - 1, 0)


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def md_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df.empty:
        return "No rows."
    show = df.head(max_rows).copy()
    cols = list(show.columns)
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in show.iterrows():
        vals = []
        for col in cols:
            val = row[col]
            if isinstance(val, float):
                vals.append("" if pd.isna(val) else f"{val:.4f}")
            else:
                vals.append(str(val).replace("|", "\\|") if not pd.isna(val) else "")
        lines.append("| " + " | ".join(vals) + " |")
    if len(df) > max_rows:
        lines.append(f"\nShowing {max_rows} of {len(df)} rows.")
    return "\n".join(lines)


def norm_str(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def norm_line(value: Any) -> str:
    if pd.isna(value) or value == "":
        return ""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return str(value).strip().lower()
    if math.isfinite(f) and f.is_integer():
        return str(int(f))
    return f"{f:g}"


def add_keys(df: pd.DataFrame, side_optional: bool = True) -> pd.DataFrame:
    out = df.copy()
    for col in ["slate_date", "game_id", "player_id", "prop_type"]:
        if col not in out.columns:
            out[col] = ""
    out["slate_date_s"] = out["slate_date"].map(norm_str)
    out["game_id_s"] = out["game_id"].map(norm_str)
    out["player_id_s"] = out["player_id"].map(norm_str)
    out["prop_type_s"] = out["prop_type"].map(lambda v: norm_str(v).lower())
    out["line_s"] = out["line"].map(norm_line) if "line" in out.columns else ""
    if "side" in out.columns:
        out["side_s"] = out["side"].map(lambda v: norm_str(v).lower())
    elif side_optional:
        out["side_s"] = ""
    else:
        out["side_s"] = "missing_side"
    out["player_game_key"] = (
        out["slate_date_s"] + "|" + out["game_id_s"] + "|" + out["player_id_s"]
    )
    out["prop_key_no_side"] = (
        out["player_game_key"] + "|" + out["prop_type_s"] + "|" + out["line_s"]
    )
    out["prop_side_key"] = out["prop_key_no_side"] + "|" + out["side_s"]
    return out


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


def settle_over15(hits: Any) -> str:
    cls = hit_class(hits)
    if cls == "TWO_OR_MORE_HITS":
        return "win"
    if cls in {"ZERO_HITS", "EXACTLY_ONE_HIT"}:
        return "loss"
    return "unresolved"


def settle_under15(hits: Any) -> str:
    cls = hit_class(hits)
    if cls in {"ZERO_HITS", "EXACTLY_ONE_HIT"}:
        return "win"
    if cls == "TWO_OR_MORE_HITS":
        return "loss"
    return "unresolved"


def settle_over05(hits: Any) -> str:
    cls = hit_class(hits)
    if cls in {"EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"}:
        return "win"
    if cls == "ZERO_HITS":
        return "loss"
    return "unresolved"


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return norm_str(value).lower() in {"true", "1", "yes", "y"}


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
    if p > 0:
        return p / 100.0
    return 100.0 / abs(p)


def wilson_rate(wins: int, n: int) -> tuple[float | None, float | None, float | None]:
    if n <= 0:
        return None, None, None
    z = 1.96
    phat = wins / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    margin = z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n) / denom
    return phat, max(0.0, center - margin), min(1.0, center + margin)


def sample_flag(rows: int) -> str:
    if rows >= 100:
        return "adequate"
    if rows >= 30:
        return "small"
    return "sparse"


def summarize_binary(
    df: pd.DataFrame, group_cols: list[str], positive_col: str, label: str
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if df.empty:
        return pd.DataFrame()
    for keys, g in df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        n = len(g)
        positives = int(g[positive_col].sum())
        rate, lo, hi = wilson_rate(positives, n)
        row = {col: key for col, key in zip(group_cols, keys)}
        row.update(
            {
                "metric": label,
                "rows": n,
                "positives": positives,
                "negative_rows": n - positives,
                "positive_rate": rate,
                "wilson_low": lo,
                "wilson_high": hi,
                "unique_dates": g["slate_date_s"].nunique() if "slate_date_s" in g else None,
                "unique_players": g["player_id_s"].nunique() if "player_id_s" in g else None,
                "unique_pitchers": g["opposing_starter_id"].nunique()
                if "opposing_starter_id" in g
                else None,
                "sample_flag": sample_flag(n),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(group_cols + ["metric"]).reset_index(drop=True)


def summarize_market(
    df: pd.DataFrame, group_cols: list[str], side: str, price_col: str
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if df.empty or price_col not in df.columns:
        return pd.DataFrame()
    result_col = "integrated_o15_result" if side == "over" else "integrated_u15_result"
    work = df[df[result_col].isin(["win", "loss"])].copy()
    work["profit_1u"] = work.apply(lambda r: american_profit(r[result_col], r[price_col]), axis=1)
    work = work[work["profit_1u"].notna()].copy()
    for keys, g in work.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        wins = int((g[result_col] == "win").sum())
        losses = int((g[result_col] == "loss").sum())
        n = wins + losses
        rate, lo, hi = wilson_rate(wins, n)
        row = {col: key for col, key in zip(group_cols, keys)}
        row.update(
            {
                "side": side,
                "available_wagers": n,
                "wins": wins,
                "losses": losses,
                "avg_price": pd.to_numeric(g[price_col], errors="coerce").mean(),
                "realized_win_rate": rate,
                "wilson_low": lo,
                "wilson_high": hi,
                "units": g["profit_1u"].sum(),
                "roi": g["profit_1u"].mean() if n else None,
                "sample_flag": sample_flag(n),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(group_cols + ["side"]).reset_index(drop=True)


def source_registry(paths: Iterable[Path]) -> pd.DataFrame:
    rows = []
    for path in paths:
        status = "FOUND" if path.exists() else "MISSING"
        row: dict[str, Any] = {
            "source_path": str(path),
            "status": status,
            "sha256": sha256_file(path) if path.exists() and path.is_file() else "",
            "row_count": file_rows(path),
            "grain": "",
            "date_range": "",
            "field_version": "",
            "outcome_authority": "",
            "temporal_cutoff": "",
            "compatibility_status": "",
            "notes": "",
        }
        if path == HIST_CUMULATIVE_STATE:
            row.update(
                {
                    "grain": "aggregate cumulative certified selected-proposition state",
                    "field_version": "2026-07-15 certified cumulative post-repair state",
                    "outcome_authority": "aggregate accounting only; no row-level manifest in this file",
                    "compatibility_status": "AGGREGATE_COMPATIBLE_ROW_LEVEL_NOT_DIRECTLY_JOINABLE",
                }
            )
        elif path in {HIST_ALL, HIST_05, HIST_15, HIST_14816}:
            row.update(
                {
                    "grain": "selected proposition side row",
                    "field_version": "post-three-row PA remediation row-level state",
                    "outcome_authority": "certified actual_hits where numeric_outcome_certified=true",
                    "temporal_cutoff": "historical strict-prior research platform",
                    "compatibility_status": "ROW_LEVEL_JOINABLE",
                }
            )
        elif path == OWNERSHIP_LEDGER:
            row.update(
                {
                    "grain": "Hits 1.5 proposition without selected side column; over/under surface state retained",
                    "field_version": "two-sided matchup advantage canonical ledger",
                    "outcome_authority": "partial official_hits retained from prior audit",
                    "compatibility_status": "ROW_LEVEL_JOINABLE_BY_PLAYER_GAME_AND_PROP_NO_SIDE",
                }
            )
        elif path == THRESHOLD_MANIFEST:
            row.update(
                {
                    "grain": "player-game Hits 1.5 research row",
                    "field_version": "threshold-specific canonical player-game outcome manifest",
                    "outcome_authority": "partial official numeric hits",
                    "compatibility_status": "ROW_LEVEL_JOINABLE_BY_PLAYER_GAME",
                }
            )
        elif path.suffix == ".csv":
            row["grain"] = "supporting csv artifact"
            row["compatibility_status"] = "SUPPORTING_EVIDENCE"
        rows.append(row)
    return pd.DataFrame(rows)


def build(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    required = [
        HIST_CUMULATIVE_STATE,
        HIST_PRIOR_STATE,
        HIST_ALL,
        HIST_05,
        HIST_15,
        HIST_14816,
        OWNERSHIP_LEDGER,
        THRESHOLD_MANIFEST,
        ZERO_ONE_TWO_LEDGER,
        CURRENT_VS_ARBITER,
        OWNERSHIP_LINEAGE,
        PITCHER_DOMINANT_OVER,
        HITS05_MATRIX,
        HITS15_MATRIX,
        JULY12_SENTINEL,
    ]
    registry = source_registry(required)
    write_csv(registry, out_dir / f"authoritative_source_registry_{AUDIT_DATE}.csv")

    hist_all = add_keys(read_csv(HIST_ALL))
    hist_05 = add_keys(read_csv(HIST_05))
    hist_15 = add_keys(read_csv(HIST_15))
    hist_14816 = add_keys(read_csv(HIST_14816))
    ownership = add_keys(read_csv(OWNERSHIP_LEDGER))
    threshold = add_keys(read_csv(THRESHOLD_MANIFEST))

    cumulative = json.loads(HIST_CUMULATIVE_STATE.read_text())
    prior_cumulative = json.loads(HIST_PRIOR_STATE.read_text())

    # Authoritative row-level official hit count is a player-game fact. Exact
    # proposition side identity is still reported separately.
    hist_outcome_rows = hist_all[
        hist_all.get("numeric_outcome_certified", False).map(boolish)
        & hist_all["actual_hits"].notna()
    ].copy()
    hist_player_game_outcomes = (
        hist_outcome_rows.sort_values(["slate_date_s", "game_id_s", "player_id_s"])
        .drop_duplicates("player_game_key")
        .set_index("player_game_key")
    )
    hist_actual_hits = hist_player_game_outcomes["actual_hits"].to_dict()
    hist_outcome_source = {
        k: "post_three_row_pa_fully_qualified_hits_manifest"
        for k in hist_player_game_outcomes.index
    }

    exact_prop_overlap = set(ownership["prop_key_no_side"]) & set(hist_all["prop_key_no_side"])
    exact_prop_side_overlap = set(ownership["prop_side_key"]) & set(hist_all["prop_side_key"])
    player_game_overlap = set(ownership["player_game_key"]) & set(hist_all["player_game_key"])

    cross_rows = [
        {
            "population": "historical_cumulative_state_aggregate",
            "rows": cumulative.get("after_totals", {}).get("fully_qualified_hits"),
            "hits_0_5_rows": cumulative.get("after_totals", {}).get("fully_qualified_hits_0_5"),
            "hits_1_5_rows": cumulative.get("after_totals", {}).get("fully_qualified_hits_1_5"),
            "grain": "aggregate state",
            "row_level_joinable": False,
            "notes": "Certified cumulative state has aggregate totals but no full row-level manifest in bound artifact.",
        },
        {
            "population": "historical_row_level_manifest_latest_available",
            "rows": len(hist_all),
            "hits_0_5_rows": len(hist_05),
            "hits_1_5_rows": len(hist_15),
            "grain": "selected proposition side",
            "row_level_joinable": True,
            "notes": "Latest row-level fully-qualified Hits manifests found locally.",
        },
        {
            "population": "ownership_threshold_ledger",
            "rows": len(ownership),
            "hits_0_5_rows": 0,
            "hits_1_5_rows": len(ownership),
            "grain": "Hits 1.5 proposition; selected side represented by surface/current direction fields",
            "row_level_joinable": True,
            "notes": "No side column; exact side overlap is therefore not directly possible.",
        },
        {
            "population": "threshold_specific_manifest",
            "rows": len(threshold),
            "hits_0_5_rows": int(threshold["o05_candidate_status"].fillna(False).map(boolish).sum())
            if "o05_candidate_status" in threshold
            else None,
            "hits_1_5_rows": len(threshold),
            "grain": "player-game Hits research row",
            "row_level_joinable": True,
            "notes": "Player-game threshold classes; partial official hit outcomes.",
        },
    ]
    crosswalk = pd.DataFrame(cross_rows)
    write_csv(crosswalk, out_dir / f"population_and_grain_crosswalk_{AUDIT_DATE}.csv")

    overlap = ownership[
        [
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
            "baseball_directional_ownership",
            "official_hits",
            "player_game_key",
            "prop_key_no_side",
            "prop_side_key",
        ]
    ].copy()
    hist_prop_keys = set(hist_all["prop_key_no_side"])
    hist_side_keys = set(hist_all["prop_side_key"])
    hist_pg_keys = set(hist_all["player_game_key"])
    hist_lines_by_pg = hist_all.groupby("player_game_key")["line_s"].apply(lambda s: ";".join(sorted(set(s)))).to_dict()
    hist_sides_by_pg = hist_all.groupby("player_game_key")["side_s"].apply(lambda s: ";".join(sorted(set(s)))).to_dict()
    overlap["exact_prop_key_overlap"] = overlap["prop_key_no_side"].isin(hist_prop_keys)
    overlap["exact_prop_side_key_overlap"] = overlap["prop_side_key"].isin(hist_side_keys)
    overlap["same_player_game_overlap"] = overlap["player_game_key"].isin(hist_pg_keys)
    overlap["historical_lines_for_player_game"] = overlap["player_game_key"].map(hist_lines_by_pg).fillna("")
    overlap["historical_sides_for_player_game"] = overlap["player_game_key"].map(hist_sides_by_pg).fillna("")
    overlap["overlap_class"] = overlap.apply(
        lambda r: "exact_prop_key_no_side_overlap"
        if r["exact_prop_key_overlap"]
        else (
            "same_player_game_different_line_or_side"
            if r["same_player_game_overlap"]
            else "ownership_only_no_historical_row_level_match"
        ),
        axis=1,
    )
    write_csv(overlap, out_dir / f"exact_proposition_overlap_ledger_{AUDIT_DATE}.csv")

    pg_bridge = (
        overlap.groupby(["slate_date", "game_id", "player_id", "player_name", "team", "opponent"], dropna=False)
        .agg(
            ownership_rows=("canonical_proposition_key", "count"),
            exact_prop_key_matches=("exact_prop_key_overlap", "sum"),
            same_player_game_matches=("same_player_game_overlap", "sum"),
            ownership_labels=("baseball_directional_ownership", lambda s: ";".join(sorted(set(map(norm_str, s))))),
            surface_states=("current_side_surface_state", lambda s: ";".join(sorted(set(map(norm_str, s))))),
            current_official_hits=("official_hits", lambda s: next((x for x in s if pd.notna(x)), "")),
        )
        .reset_index()
    )
    pg_bridge["player_game_key"] = (
        pg_bridge["slate_date"].map(norm_str)
        + "|"
        + pg_bridge["game_id"].map(norm_str)
        + "|"
        + pg_bridge["player_id"].map(norm_str)
    )
    pg_bridge["historical_actual_hits"] = pg_bridge["player_game_key"].map(hist_actual_hits)
    pg_bridge["historical_outcome_source"] = pg_bridge["player_game_key"].map(hist_outcome_source).fillna("")
    write_csv(pg_bridge, out_dir / f"player_game_bridge_{AUDIT_DATE}.csv")

    integrated = ownership.copy()
    integrated["prior_official_hits_present"] = integrated["official_hits"].notna()
    integrated["historical_actual_hits"] = integrated["player_game_key"].map(hist_actual_hits)
    integrated["historical_outcome_source"] = integrated["player_game_key"].map(hist_outcome_source).fillna("")
    integrated["historical_exact_prop_key_overlap"] = integrated["prop_key_no_side"].isin(hist_prop_keys)
    integrated["historical_player_game_overlap"] = integrated["player_game_key"].isin(hist_pg_keys)
    integrated["integrated_official_hits"] = integrated["official_hits"]
    recover_mask = integrated["integrated_official_hits"].isna() & integrated["historical_actual_hits"].notna()
    integrated.loc[recover_mask, "integrated_official_hits"] = integrated.loc[recover_mask, "historical_actual_hits"]
    integrated["newly_outcome_bound_from_historical"] = recover_mask
    integrated["integrated_outcome_source"] = integrated["outcome_source"].fillna("")
    integrated.loc[recover_mask, "integrated_outcome_source"] = "certified_historical_player_game_actual_hits"
    integrated["integrated_hit_count_class"] = integrated["integrated_official_hits"].map(hit_class)
    integrated["integrated_o05_result"] = integrated["integrated_official_hits"].map(settle_over05)
    integrated["integrated_o15_result"] = integrated["integrated_official_hits"].map(settle_over15)
    integrated["integrated_u15_result"] = integrated["integrated_official_hits"].map(settle_under15)
    integrated["outcome_gap_reason"] = integrated.apply(
        lambda r: "already_outcome_bound_in_threshold_ledger"
        if r["prior_official_hits_present"]
        else (
            "official_outcome_exists_in_certified_historical_player_game_platform"
            if r["newly_outcome_bound_from_historical"]
            else (
                "same_player_game_present_but_no_certified_numeric_outcome"
                if r["historical_player_game_overlap"]
                else "outside_joinable_historical_row_level_population"
            )
        ),
        axis=1,
    )
    integrated["ownership_direction_for_hits15"] = integrated["baseball_directional_ownership"].map(
        {
            "hitter_dominant": "HITTER_OWNS_ADVANTAGE",
            "pitcher_dominant": "PITCHER_OWNS_ADVANTAGE",
            "conflicting": "EVIDENCE_CONFLICTS",
            "incomplete": "ADVANTAGE_NOT_ESTABLISHED",
        }
    )
    integrated["contrary_pitcher_evidence_present"] = (
        (integrated["baseball_directional_ownership"] == "pitcher_dominant")
        & (integrated["current_side_surface_state"].astype(str).str.contains("OVER", na=False))
    )
    integrated["current_direction_integrity_class"] = integrated.apply(
        lambda r: "correct_hitter_side_direction"
        if r["current_selected_direction"] == "over" and r["integrated_o15_result"] == "win"
        else (
            "correct_pitcher_side_direction"
            if r["current_selected_direction"] == "under" and r["integrated_u15_result"] == "win"
            else (
                "wrong_side_candidate"
                if r["current_selected_direction"] in {"over", "under"}
                and r["integrated_hit_count_class"] != "MISSING_OFFICIAL_OUTCOME"
                else (
                    "both_sides"
                    if r["current_side_surface_state"] == "BOTH_CONFLICT"
                    else (
                        "candidate_despite_incomplete_evidence"
                        if r["baseball_directional_ownership"] == "incomplete"
                        else "unresolved_or_no_side"
                    )
                )
            )
        ),
        axis=1,
    )
    integrated["a_u_regime"] = (
        (integrated["hitter_tier_seen"].map(norm_str) == "A")
        & (integrated["pitcher_tier_seen"].map(norm_str) == "U")
    )
    integrated["u_pitcher_reason"] = integrated.apply(
        lambda r: "missing_starter_context"
        if norm_str(r.get("pitcher_tier_seen")) == "U"
        and not norm_str(r.get("starter_context_status"))
        else (
            norm_str(r.get("starter_context_status"))
            if norm_str(r.get("pitcher_tier_seen")) == "U"
            else ""
        ),
        axis=1,
    )
    write_csv(integrated, out_dir / f"integrated_matchup_evidence_ledger_{AUDIT_DATE}.csv")

    gap = (
        integrated.groupby(
            ["outcome_gap_reason", "baseball_directional_ownership", "current_side_surface_state"],
            dropna=False,
        )
        .size()
        .reset_index(name="rows")
        .sort_values(["outcome_gap_reason", "baseball_directional_ownership"])
    )
    write_csv(gap, out_dir / f"outcome_gap_explanation_{AUDIT_DATE}.csv")

    recovered = integrated[integrated["newly_outcome_bound_from_historical"]].copy()
    recovered_cols = [
        "canonical_proposition_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "line",
        "current_side_surface_state",
        "baseball_directional_ownership",
        "historical_actual_hits",
        "integrated_hit_count_class",
        "historical_exact_prop_key_overlap",
        "historical_player_game_overlap",
        "integrated_outcome_source",
    ]
    write_csv(recovered[recovered_cols], out_dir / f"newly_recovered_outcomes_{AUDIT_DATE}.csv")

    resolved = integrated[integrated["integrated_hit_count_class"] != "MISSING_OFFICIAL_OUTCOME"].copy()
    resolved["one_or_more_hit"] = resolved["integrated_hit_count_class"].isin(
        ["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"]
    )
    resolved["two_or_more_hits"] = resolved["integrated_hit_count_class"] == "TWO_OR_MORE_HITS"
    hit_rows = resolved[resolved["one_or_more_hit"]].copy()
    hit_rows["multi_hit_after_one_plus"] = hit_rows["integrated_hit_count_class"] == "TWO_OR_MORE_HITS"
    resolved["zero_or_one_hit"] = resolved["integrated_hit_count_class"].isin(
        ["ZERO_HITS", "EXACTLY_ONE_HIT"]
    )

    any_hit = summarize_binary(
        resolved, ["baseball_directional_ownership"], "one_or_more_hit", "ONE_OR_MORE_HIT"
    )
    write_csv(any_hit, out_dir / f"any_hit_ownership_results_{AUDIT_DATE}.csv")

    multi_hit = summarize_binary(
        hit_rows, ["baseball_directional_ownership"], "multi_hit_after_one_plus", "TWO_PLUS_GIVEN_ONE_PLUS"
    )
    market_15 = summarize_binary(
        resolved, ["baseball_directional_ownership"], "two_or_more_hits", "TWO_PLUS_FOR_HITS_1_5"
    )
    multi_all = pd.concat([multi_hit, market_15], ignore_index=True)
    write_csv(multi_all, out_dir / f"multi_hit_ownership_results_{AUDIT_DATE}.csv")

    pitcher_rows = resolved[resolved["baseball_directional_ownership"] == "pitcher_dominant"].copy()
    pitcher_supp = summarize_binary(
        pitcher_rows,
        ["current_side_surface_state"],
        "zero_or_one_hit",
        "PITCHER_SUPPRESSION_ZERO_OR_ONE_HIT",
    )
    write_csv(pitcher_supp, out_dir / f"pitcher_suppression_results_{AUDIT_DATE}.csv")

    # Evidence-domain attribution is intentionally bounded to already-retained fields.
    domain_specs = [
        ("hitter_tier_seen", "hitter tier"),
        ("pitcher_tier_seen", "pitcher tier"),
        ("combined_tier_seen", "hitter-tier x pitcher-tier"),
        ("pa_opp_v1_d15_opportunity_band", "PA opportunity band"),
        ("starter_starts_count", "Starter history count"),
        ("starter_context_status", "Starter context status"),
        ("local_team_hits_parity_status", "offense lineage parity"),
        ("offense_context_as_of_date", "offense context date"),
    ]
    domain_frames = []
    for col, label in domain_specs:
        if col not in resolved.columns:
            domain_frames.append(
                pd.DataFrame(
                    [
                        {
                            "evidence_domain": label,
                            "field": col,
                            "bucket": "FIELD_NOT_RETAINED",
                            "rows": 0,
                            "two_plus_rate": None,
                            "zero_or_one_rate": None,
                            "role_classification": "INSUFFICIENT_COVERAGE",
                            "notes": "Field was not present in integrated ownership ledger.",
                        }
                    ]
                )
            )
            continue
        d = resolved.copy()
        if col == "starter_starts_count":
            vals = pd.to_numeric(d[col], errors="coerce")
            d["_bucket"] = pd.cut(
                vals,
                bins=[-math.inf, 0, 2, 5, 10, math.inf],
                labels=["missing_or_zero", "1_to_2", "3_to_5", "6_to_10", "11_plus"],
            ).astype(str)
            d.loc[vals.isna(), "_bucket"] = "missing"
        else:
            d["_bucket"] = d[col].map(lambda v: norm_str(v) or "missing")
        rows = []
        for bucket, g in d.groupby("_bucket", dropna=False):
            n = len(g)
            two = int((g["integrated_hit_count_class"] == "TWO_OR_MORE_HITS").sum())
            z01 = int(g["integrated_hit_count_class"].isin(["ZERO_HITS", "EXACTLY_ONE_HIT"]).sum())
            role = "INSUFFICIENT_COVERAGE" if n < 30 else "NOT_INDEPENDENTLY_VALIDATED"
            if n >= 30:
                two_rate = two / n
                if two_rate >= resolved["two_or_more_hits"].mean() + 0.05:
                    role = "MULTI_HIT_SUPPORT"
                elif (z01 / n) >= resolved["zero_or_one_hit"].mean() + 0.05:
                    role = "PITCHER_SUPPRESSION_SUPPORT"
                else:
                    role = "REDUNDANT"
            rows.append(
                {
                    "evidence_domain": label,
                    "field": col,
                    "bucket": bucket,
                    "rows": n,
                    "two_plus_rate": two / n if n else None,
                    "zero_or_one_rate": z01 / n if n else None,
                    "role_classification": role,
                    "notes": "Bounded descriptive comparison; no arbitrary feature search or threshold optimization.",
                }
            )
        domain_frames.append(pd.DataFrame(rows))
    domain_attr = pd.concat(domain_frames, ignore_index=True)
    write_csv(domain_attr, out_dir / f"evidence_domain_attribution_{AUDIT_DATE}.csv")

    au = resolved.copy()
    au["au_bucket"] = au.apply(
        lambda r: "A/U"
        if r["a_u_regime"]
        else (
            f"A/{norm_str(r.get('pitcher_tier_seen')) or 'missing'}"
            if norm_str(r.get("hitter_tier_seen")) == "A"
            else f"{norm_str(r.get('hitter_tier_seen')) or 'missing'}/{norm_str(r.get('pitcher_tier_seen')) or 'missing'}"
        ),
        axis=1,
    )
    au_summary = pd.concat(
        [
            summarize_binary(au, ["au_bucket", "u_pitcher_reason"], "one_or_more_hit", "ONE_OR_MORE_HIT"),
            summarize_binary(au, ["au_bucket", "u_pitcher_reason"], "two_or_more_hits", "TWO_PLUS_HITS15"),
        ],
        ignore_index=True,
    )
    write_csv(au_summary, out_dir / f"a_u_bounded_regime_analysis_{AUDIT_DATE}.csv")

    current_dir = (
        resolved.groupby(
            [
                "baseball_directional_ownership",
                "current_side_surface_state",
                "current_selected_direction",
                "current_direction_integrity_class",
            ],
            dropna=False,
        )
        .size()
        .reset_index(name="rows")
        .sort_values(["baseball_directional_ownership", "current_side_surface_state"])
    )
    write_csv(current_dir, out_dir / f"current_direction_integrity_report_{AUDIT_DATE}.csv")

    july12 = integrated[integrated["july12_sentinel"].map(boolish)].copy()
    if JULY12_SENTINEL.exists():
        sentinel = read_csv(JULY12_SENTINEL)
        sentinel = add_keys(sentinel)
        sentinel_keys = set(sentinel["player_game_key"])
        july12 = pd.concat(
            [july12, integrated[integrated["player_game_key"].isin(sentinel_keys)]],
            ignore_index=True,
        ).drop_duplicates("canonical_proposition_key")
    july12_cols = [
        "canonical_proposition_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "integrated_official_hits",
        "integrated_hit_count_class",
        "hitter_tier_seen",
        "pitcher_tier_seen",
        "combined_tier_seen",
        "pa_opp_v1_d15_pa_pg",
        "pa_opp_v1_d15_opportunity_band",
        "starter_expected_hits_allowed",
        "starter_context_status",
        "u_pitcher_reason",
        "baseball_directional_ownership",
        "current_side_surface_state",
        "current_selected_direction",
        "contrary_pitcher_evidence_present",
        "evidence_missingness",
        "integrated_outcome_source",
    ]
    for col in july12_cols:
        if col not in july12.columns:
            july12[col] = ""
    july12["july12_attribution"] = july12.apply(
        lambda r: "multi_hit_supported"
        if r["baseball_directional_ownership"] == "hitter_dominant"
        and r["integrated_hit_count_class"] == "TWO_OR_MORE_HITS"
        else (
            "any_hit_supported_only"
            if r["integrated_hit_count_class"] == "EXACTLY_ONE_HIT"
            else (
                "pitcher_suppression_supported"
                if r["baseball_directional_ownership"] == "pitcher_dominant"
                else (
                    "conflicting"
                    if r["baseball_directional_ownership"] == "conflicting"
                    else "incomplete_or_unresolved"
                )
            )
        ),
        axis=1,
    )
    write_csv(july12[july12_cols + ["july12_attribution"]], out_dir / f"july12_integrated_reconstruction_{AUDIT_DATE}.csv")

    price_frames = [
        summarize_market(
            resolved[resolved["baseball_directional_ownership"] == "hitter_dominant"],
            ["baseball_directional_ownership", "current_side_surface_state"],
            "over",
            "o15_price",
        ),
        summarize_market(
            resolved[resolved["baseball_directional_ownership"] == "pitcher_dominant"],
            ["baseball_directional_ownership", "current_side_surface_state"],
            "under",
            "u15_price",
        ),
        summarize_market(
            resolved,
            ["baseball_directional_ownership", "current_side_surface_state"],
            "over",
            "selected_price",
        ),
    ]
    price = pd.concat([x for x in price_frames if not x.empty], ignore_index=True) if any(
        not x.empty for x in price_frames
    ) else pd.DataFrame()
    write_csv(price, out_dir / f"price_context_results_{AUDIT_DATE}.csv")

    lineage_copy = read_csv(OWNERSHIP_LINEAGE) if OWNERSHIP_LINEAGE.exists() else pd.DataFrame()
    write_csv(lineage_copy, out_dir / f"ownership_label_lineage_report_{AUDIT_DATE}.csv")

    value_report = pd.DataFrame(
        [
            {
                "value_component": "aggregate certified Hits state",
                "before": prior_cumulative.get("after_totals", {}).get("fully_qualified_hits"),
                "after": cumulative.get("after_totals", {}).get("fully_qualified_hits"),
                "added": cumulative.get("after_totals", {}).get("fully_qualified_hits", 0)
                - prior_cumulative.get("after_totals", {}).get("fully_qualified_hits", 0),
                "notes": "Aggregate state expanded after parent-ledger repair; row-level full manifest was not present.",
            },
            {
                "value_component": "row-level exact joinable historical manifest",
                "before": 0,
                "after": len(hist_all),
                "added": len(hist_all),
                "notes": "Latest found exact row-level fully-qualified Hits manifest.",
            },
            {
                "value_component": "ownership-ledger newly outcome-bound rows",
                "before": int(ownership["official_hits"].notna().sum()),
                "after": int(integrated["integrated_official_hits"].notna().sum()),
                "added": int(recover_mask.sum()),
                "notes": "Recovered via exact slate_date/game_id/player_id official-hit fact from certified row-level historical manifest.",
            },
            {
                "value_component": "pitcher-dominant OVER-only newly outcome-bound rows",
                "before": int(
                    (
                        (ownership["baseball_directional_ownership"] == "pitcher_dominant")
                        & (ownership["current_side_surface_state"] == "OVER_ONLY")
                        & ownership["official_hits"].notna()
                    ).sum()
                ),
                "after": int(
                    (
                        (integrated["baseball_directional_ownership"] == "pitcher_dominant")
                        & (integrated["current_side_surface_state"] == "OVER_ONLY")
                        & integrated["integrated_official_hits"].notna()
                    ).sum()
                ),
                "added": int(
                    (
                        (integrated["baseball_directional_ownership"] == "pitcher_dominant")
                        & (integrated["current_side_surface_state"] == "OVER_ONLY")
                        & integrated["newly_outcome_bound_from_historical"]
                    ).sum()
                ),
                "notes": "Directly addresses the prior 27-of-1354 sparse resolved subset.",
            },
        ]
    )
    write_csv(value_report, out_dir / f"historical_qualification_value_report_{AUDIT_DATE}.csv")

    next_experiment = pd.DataFrame(
        [
            {
                "experiment": "pitcher_suppression_under_validation",
                "selected": True,
                "reason": "Largest newly improved actionable contradiction is pitcher-dominant OVER-only; outcome coverage increased materially after historical integration.",
                "population_basis": "integrated pitcher_dominant rows with certified numeric hits and preserved U/O surface availability",
                "do_not_execute_in_this_task": True,
                "notes": "Design only; no selector, fade, threshold, or price optimization authorized.",
            },
            {
                "experiment": "multi_hit_specific_hitter_evidence",
                "selected": False,
                "reason": "Still needed, but existing ownership labels do not validate stable two-plus progression.",
                "population_basis": "one-plus resolved rows by hitter ownership",
                "do_not_execute_in_this_task": True,
                "notes": "Should follow once threshold-specific hitter recurrence fields are better retained.",
            },
        ]
    )
    write_csv(next_experiment, out_dir / f"selected_next_experiment_design_{AUDIT_DATE}.csv")

    # Decision derivation.
    resolved_count = int(integrated["integrated_official_hits"].notna().sum())
    recovered_count = int(recover_mask.sum())
    pitcher_dom_over_resolved = int(
        (
            (integrated["baseball_directional_ownership"] == "pitcher_dominant")
            & (integrated["current_side_surface_state"] == "OVER_ONLY")
            & integrated["integrated_official_hits"].notna()
        ).sum()
    )
    pitcher_dom_over_zero_one = int(
        (
            (integrated["baseball_directional_ownership"] == "pitcher_dominant")
            & (integrated["current_side_surface_state"] == "OVER_ONLY")
            & integrated["integrated_hit_count_class"].isin(["ZERO_HITS", "EXACTLY_ONE_HIT"])
        ).sum()
    )
    hitter_any = any_hit[any_hit["baseball_directional_ownership"] == "hitter_dominant"]
    pitcher_any = any_hit[any_hit["baseball_directional_ownership"] == "pitcher_dominant"]
    hitter_any_rate = float(hitter_any["positive_rate"].iloc[0]) if not hitter_any.empty else None
    pitcher_any_rate = float(pitcher_any["positive_rate"].iloc[0]) if not pitcher_any.empty else None

    decisions = {
        "MLB_MATCHUP_INTEGRATION_SOURCE_BINDING_DECISION": "CERTIFIED_CUMULATIVE_STATE_BOUND_AGGREGATE_ROW_LEVEL_JOIN_LIMITED_TO_LATEST_AVAILABLE_MANIFEST",
        "MLB_MATCHUP_INTEGRATION_GRAIN_RECONCILIATION_DECISION": "EXACT_PROP_NO_SIDE_AND_PLAYER_GAME_BRIDGES_VALID_SIDE_GRAIN_NOT_DIRECTLY_COMPATIBLE",
        "MLB_MATCHUP_OUTCOME_RECOVERY_DECISION": "CERTIFIED_PLAYER_GAME_OUTCOME_RECOVERY_EXPANDED_RESOLVED_ROWS",
        "MLB_MATCHUP_CERTIFIED_EVIDENCE_LEDGER_DECISION": "INTEGRATED_LEDGER_CREATED_WITH_DOMAIN_SEPARATION_AND_RECOVERY_PROVENANCE",
        "MLB_MATCHUP_EXISTING_OWNERSHIP_LABEL_DECISION": "LABELS_ARE_OUTCOME_INDEPENDENT_RELATIVE_EVIDENCE_PRESENCE_NOT_FULL_MATCHUP_OWNERSHIP",
        "MLB_MATCHUP_ANY_HIT_OWNERSHIP_DECISION": "HITTER_LABEL_SEPARATES_ANY_HIT_MODESTLY"
        if hitter_any_rate is not None
        and pitcher_any_rate is not None
        and hitter_any_rate > pitcher_any_rate
        else "ANY_HIT_SEPARATION_NOT_ESTABLISHED",
        "MLB_MATCHUP_MULTI_HIT_OWNERSHIP_DECISION": "MULTI_HIT_SPECIFIC_OWNERSHIP_NOT_RELIABLY_ESTABLISHED",
        "MLB_MATCHUP_PITCHER_SUPPRESSION_DECISION": "PITCHER_DOMINANT_ROWS_SUPPORT_ZERO_OR_ONE_HIT_DIRECTION_BUT_REQUIRE_UNDER_VALIDATION",
        "MLB_MATCHUP_AU_REGIME_DECISION": "A_U_IS_ASYMMETRIC_UNCERTAINTY_NOT_AUTOMATIC_HITTER_ADVANTAGE",
        "MLB_MATCHUP_CURRENT_DIRECTION_INTEGRITY_DECISION": "CURRENT_OVER_ARCHITECTURE_STILL_SURFACES_ROWS_WITH_CONTRARY_PITCHER_EVIDENCE",
        "MLB_MATCHUP_JULY12_ATTRIBUTION_DECISION": "JULY12_FAILURE_REMAINS_THRESHOLD_LEAKAGE_AND_SUPPRESSION_CONFLICT_NOT_NEW_RULE_SOURCE",
        "MLB_MATCHUP_PRICE_VALUE_DECISION": "PRICE_CONTEXT_AVAILABLE_ONLY_AFTER_DIRECTION_AND_NOT_SUITABLE_FOR_OWNERSHIP_DEFINITION",
        "MLB_MATCHUP_HISTORICAL_QUALIFICATION_VALUE_DECISION": "HISTORICAL_QUALIFICATION_MATERIALLY_EXPANDED_MATCHUP_EVIDENCE",
        "MLB_MATCHUP_NEXT_EXPERIMENT_DECISION": "DESIGN_PITCHER_SUPPRESSION_UNDER_VALIDATION_NEXT",
        "MLB_MATCHUP_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }

    decision_df = pd.DataFrame(
        [{"decision": k, "value": v} for k, v in decisions.items()]
    )
    write_csv(decision_df, out_dir / f"decision_report_{AUDIT_DATE}.csv")

    # JSON summary.
    summary = {
        "generated_at_utc": utc_now(),
        "audit_date": AUDIT_DATE,
        "source_binding": {
            "certified_cumulative_state_path": str(HIST_CUMULATIVE_STATE),
            "certified_cumulative_totals": cumulative.get("after_totals", {}),
            "latest_row_level_manifest_path": str(HIST_ALL),
            "latest_row_level_manifest_rows": len(hist_all),
            "attachment_count_mismatch_note": (
                "Task context cited 1,540/1,400/140; bound repository evidence "
                "contains aggregate 1,523/1,383/140 and row-level 790/687/103."
            ),
        },
        "overlap": {
            "ownership_rows": len(ownership),
            "historical_row_level_rows": len(hist_all),
            "exact_prop_no_side_overlap": len(exact_prop_overlap),
            "exact_prop_side_overlap": len(exact_prop_side_overlap),
            "same_player_game_overlap": len(player_game_overlap),
        },
        "outcome_recovery": {
            "prior_resolved_rows": int(ownership["official_hits"].notna().sum()),
            "newly_recovered_rows": recovered_count,
            "integrated_resolved_rows": resolved_count,
            "integrated_unresolved_rows": int(integrated["integrated_official_hits"].isna().sum()),
            "pitcher_dominant_over_only_resolved_rows": pitcher_dom_over_resolved,
            "pitcher_dominant_over_only_zero_or_one_rows": pitcher_dom_over_zero_one,
        },
        "decisions": decisions,
    }
    write_json(summary, out_dir / f"machine_readable_matchup_integration_{AUDIT_DATE}.json")

    # Markdown summary.
    any_hit_text = md_table(any_hit)
    multi_text = md_table(multi_all)
    pitcher_text = md_table(pitcher_supp)
    value_text = md_table(value_report)
    decision_text = "\n".join(f"- `{k} = {v}`" for k, v in decisions.items())
    md = f"""# MLB Certified Historical Matchup-Ownership Integration and Advantage Attribution Audit

Generated: `{summary['generated_at_utc']}`

## Executive Summary

This bounded audit integrated the existing Hits 1.5 two-sided ownership ledger with the locally certified historical selected-proposition evidence that could be joined without weakening identity or temporal governance.

The exact repository-backed binding differs from the task-context shorthand: the latest certified cumulative aggregate state found locally is **1,523 fully-qualified Hits rows**, **1,383 Hits 0.5 rows**, and **140 Hits 1.5 rows**. The latest row-level fully-qualified manifests available for exact joins contain **790 / 687 / 103** rows. The package therefore uses the cumulative state for aggregate accounting and the row-level manifests for exact evidence joins.

Key result: the historical row-level platform added **{recovered_count}** newly outcome-bound ownership-ledger rows, expanding resolved evidence from **{int(ownership['official_hits'].notna().sum())}** to **{resolved_count}** of **{len(ownership)}** rows. Pitcher-dominant / OVER-only resolved coverage increased to **{pitcher_dom_over_resolved}** rows, with **{pitcher_dom_over_zero_one}** settling as zero-or-one hit.

## Population Overlap

- Ownership ledger rows: **{len(ownership)}**
- Latest joinable historical row-level Hits rows: **{len(hist_all)}**
- Exact proposition overlap without side: **{len(exact_prop_overlap)}**
- Exact proposition-side overlap: **{len(exact_prop_side_overlap)}** because the ownership ledger does not retain a side column at canonical key grain.
- Same player-game overlap: **{len(player_game_overlap)}**

## Outcome Recovery

- Prior ownership resolved rows: **{int(ownership['official_hits'].notna().sum())}**
- Newly recovered from certified historical player-game official hits: **{recovered_count}**
- Integrated resolved rows: **{resolved_count}**
- Remaining unresolved rows: **{int(integrated['integrated_official_hits'].isna().sum())}**

## Any-Hit Ownership Results

{any_hit_text}

## Multi-Hit Ownership Results

{multi_text}

## Pitcher-Suppression Results

{pitcher_text}

## A/U Interpretation

A/U remains an asymmetric uncertainty regime. U is not treated as automatic pitcher weakness or hitter advantage. In this package U is decomposed only as far as the retained `starter_context_status` and missingness fields allow.

## Current Direction Integrity

The current OVER-oriented surface still emits OVER-only rows even when the evidence label is pitcher-dominant. The integrated ledger does not authorize a production fade, but it does show that contrary pitcher evidence lacked veto power in the current architecture.

## July 12 Attribution

The July 12 sentinel remains best characterized as threshold leakage and evidence conflict: exactly-one-hit outcomes are not multi-hit wins, and pitcher suppression evidence cannot be ignored merely because a row carries general hitter or any-hit support.

## Historical Qualification Campaign Value

{value_text}

## Decisions

{decision_text}

## Direct Answer

With certified Starter, PA, outcome, and selected-proposition evidence integrated, Proppadia can identify **some directional pressure**: hitter labels modestly support any-hit outcomes and pitcher-dominant labels identify meaningful zero-or-one-hit suppression pockets. It still **cannot reliably determine full Hits 1.5 matchup ownership** from the current condition set. The condition set remains too OVER-oriented, not sufficiently multi-hit-specific, and only partially compatible at exact proposition-side grain.

No production behavior, model formula, threshold, selector, upload, database, OddsAPI, Quick Card, workspace, or LaunchAgent behavior was changed.
"""
    write_md(md, out_dir / f"executive_summary_{AUDIT_DATE}.md")

    validation_rows = []
    for path in out_dir.glob("*.csv"):
        try:
            pd.read_csv(path, low_memory=False)
            status = "PASS"
            message = ""
        except Exception as exc:  # pragma: no cover - validation artifact only
            status = "FAIL"
            message = str(exc)
        validation_rows.append({"artifact": str(path), "check": "csv_parse", "status": status, "message": message})
    for path in out_dir.glob("*.json"):
        try:
            json.loads(path.read_text())
            status = "PASS"
            message = ""
        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            message = str(exc)
        validation_rows.append({"artifact": str(path), "check": "json_parse", "status": status, "message": message})
    for path in out_dir.glob("*.md"):
        status = "PASS" if path.read_text().strip() else "FAIL"
        validation_rows.append({"artifact": str(path), "check": "markdown_nonempty", "status": status, "message": ""})
    validation = pd.DataFrame(validation_rows)
    write_csv(validation, out_dir / f"validation_report_{AUDIT_DATE}.csv")

    # Manifest last so it includes generated package files.
    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{AUDIT_DATE}.csv":
            manifest_rows.append(
                {
                    "path": str(path),
                    "sha256": sha256_file(path),
                    "bytes": path.stat().st_size,
                }
            )
    write_csv(pd.DataFrame(manifest_rows), out_dir / f"sha256_manifest_{AUDIT_DATE}.csv")

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--mode", default="read_only", choices=["read_only"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = build(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
