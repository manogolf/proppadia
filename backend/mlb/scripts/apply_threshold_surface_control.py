#!/usr/bin/env python3
"""Apply post-threshold surface controls for MLB shadow upload experiments.

This script is read-only with respect to model artifacts and existing upload variants.
It consumes an existing threshold-enhanced CSV and writes a filtered variant plus
comparison summaries inside the supplied experiment folder.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


REQUIRED_UPLOAD_COLUMNS = [
    "LEAGUE",
    "DATE",
    "HOME",
    "AWAY",
    "MARKET",
    "SELECTOR",
    "POINT",
    "SIDE",
    "WIN %",
]

KEY_COLUMNS = [
    "key_league",
    "key_date",
    "key_home",
    "key_away",
    "key_market",
    "key_selector",
    "key_point",
    "key_side",
]

MARKET_ALIASES: Dict[str, str] = {
    "batter_hits_runs_rbis": "batter_h+r+rbi",
    "batter_total_bases": "batter_bases",
    "pitcher_hits_allowed": "pitcher_hits",
    "pitcher_ks": "pitcher_strikeouts",
    "pitcher_k": "pitcher_strikeouts",
}

MARKET_TO_PROP: Dict[str, str] = {
    "batter_hits": "hits",
    "batter_runs": "runs_scored",
    "batter_rbis": "rbis",
    "batter_r+rbi": "runs_rbis",
    "batter_bases": "total_bases",
    "batter_h+r+rbi": "hits_runs_rbis",
    "batter_walks": "walks",
    "batter_strikeouts": "strikeouts_batting",
    "batter_stolen_bases": "stolen_bases",
    "batter_singles": "singles",
    "batter_doubles": "doubles",
    "batter_triples": "triples",
    "batter_home_runs": "home_runs",
    "pitcher_hits": "hits_allowed",
    "pitcher_earned_runs": "earned_runs",
    "pitcher_outs": "outs_recorded",
    "pitcher_walks": "walks_allowed",
    "pitcher_strikeouts": "strikeouts_pitching",
}

COVERAGE_RANK: Dict[str, int] = {
    "STRONG": 4,
    "GOOD": 3,
    "LIMITED": 2,
    "THIN": 1,
}


def _norm_text(value: Any) -> str:
    return str("" if value is None else value).strip()


def _norm_market(value: Any) -> str:
    raw = _norm_text(value).lower()
    return MARKET_ALIASES.get(raw, raw)


def _norm_side(value: Any) -> str:
    side = _norm_text(value).lower()
    if side in {"o", "over"}:
        return "over"
    if side in {"u", "under"}:
        return "under"
    return side


def _norm_date(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    if text.isdigit() and len(text) == 8:
        dt = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return text
    return pd.Timestamp(dt).strftime("%Y-%m-%d")


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, float) and np.isnan(value):
            return None
        text = _norm_text(value).replace(",", "")
        if text == "":
            return None
        return float(text)
    except Exception:
        return None


def _load_upload(path: Path, *, name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{name} csv missing: {path}")
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_UPLOAD_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} csv missing required columns: {missing}")
    if df.empty:
        raise RuntimeError(f"{name} csv is empty: {path}")

    out = df.copy()
    out["key_league"] = out["LEAGUE"].map(lambda v: _norm_text(v).upper())
    out["key_date"] = out["DATE"].map(_norm_date)
    out["key_home"] = out["HOME"].map(lambda v: _norm_text(v).upper())
    out["key_away"] = out["AWAY"].map(lambda v: _norm_text(v).upper())
    out["key_market"] = out["MARKET"].map(_norm_market)
    out["key_selector"] = pd.to_numeric(out["SELECTOR"], errors="coerce").astype("Int64")
    out["key_point"] = pd.to_numeric(out["POINT"], errors="coerce").round(4)
    out["key_side"] = out["SIDE"].map(_norm_side)
    out["key"] = list(zip(*(out[c] for c in KEY_COLUMNS)))
    out["win_value"] = pd.to_numeric(out["WIN %"], errors="coerce")
    out["abs_win_value"] = out["win_value"].abs()
    out["prop_type"] = out["key_market"].map(lambda m: MARKET_TO_PROP.get(str(m), "unknown"))
    out["_row_order"] = np.arange(len(out), dtype=int)
    return out


def _build_ranking_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    value_vs_market_col = None
    for candidate in ["value_vs_market", "delta_vs_market", "market_edge"]:
        if candidate in out.columns:
            series = pd.to_numeric(out[candidate], errors="coerce")
            if series.notna().any():
                value_vs_market_col = candidate
                out["rank_primary"] = series.abs()
                break
    if value_vs_market_col is None:
        out["rank_primary"] = out["abs_win_value"].fillna(0.0)

    coverage_col = None
    for candidate in ["coverage_quality_label", "coverage_quality"]:
        if candidate in out.columns:
            coverage_col = candidate
            break
    if coverage_col is None:
        out["rank_coverage"] = 0
    else:
        out["rank_coverage"] = out[coverage_col].map(lambda v: COVERAGE_RANK.get(_norm_text(v).upper(), 0))

    out["rank_abs_win"] = out["abs_win_value"].fillna(0.0)
    out["rank_targeted"] = out["is_target_market"].astype(int)
    return out


def _with_limit_per_player_prop(
    df: pd.DataFrame,
    *,
    limit: int,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    sort_cols = ["rank_primary", "rank_coverage", "rank_abs_win", "_row_order"]
    out = df.sort_values(sort_cols, ascending=[False, False, False, True]).copy()
    out["_pp_rank"] = out.groupby(["key_selector", "key_market"], dropna=False).cumcount()
    out = out[out["_pp_rank"] < int(limit)].copy()
    return out


def _select_with_surface_controls(
    base_df: pd.DataFrame,
    threshold_df: pd.DataFrame,
    *,
    target_markets: Sequence[str],
    target_rows: int,
    hard_cap: int,
    max_player_prop_rows: int,
    extreme_abs_win_cutoff: Optional[float],
    min_rows_per_target_market: int,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    target_market_set = {m.strip().lower() for m in target_markets if m.strip()}
    base_keys = set(base_df["key"].tolist())

    work = threshold_df.copy()
    stage_counts: Dict[str, int] = {"initial_threshold_rows": int(len(work))}
    work["is_target_market"] = work["key_market"].isin(target_market_set)
    work["in_base"] = work["key"].isin(base_keys)

    if extreme_abs_win_cutoff is not None:
        before = len(work)
        work = work[~(work["abs_win_value"] >= float(extreme_abs_win_cutoff))].copy()
        stage_counts["dropped_extreme_abs_win"] = int(before - len(work))
    else:
        stage_counts["dropped_extreme_abs_win"] = 0
    stage_counts["after_extreme_filter"] = int(len(work))

    # Do not allow non-target markets to expand beyond base coverage.
    before_scope = len(work)
    work = work[work["is_target_market"] | work["in_base"]].copy()
    stage_counts["dropped_non_target_expansion"] = int(before_scope - len(work))
    stage_counts["after_scope_filter"] = int(len(work))

    work = _build_ranking_columns(work)

    before_pp_limit = len(work)
    work = _with_limit_per_player_prop(work, limit=max_player_prop_rows)
    stage_counts["dropped_player_prop_limit"] = int(before_pp_limit - len(work))
    stage_counts["after_player_prop_limit"] = int(len(work))

    # Surface cap logic: preserve baseline overlap first, then fill with targeted additions.
    baseline_pool = work[work["in_base"]].copy()
    add_pool = work[(~work["in_base"]) & work["is_target_market"]].copy()

    rank_cols = ["rank_primary", "rank_coverage", "rank_abs_win", "_row_order"]
    baseline_pool = baseline_pool.sort_values(rank_cols, ascending=[False, False, False, True]).copy()
    add_pool = add_pool.sort_values(rank_cols, ascending=[False, False, False, True]).copy()

    cap_target = max(1, min(int(target_rows), int(hard_cap)))

    if len(baseline_pool) > cap_target:
        selected = baseline_pool.head(cap_target).copy()
        stage_counts["trimmed_baseline_to_target"] = int(len(baseline_pool) - len(selected))
        stage_counts["added_target_rows"] = 0
        stage_counts["added_target_rows_min_floor"] = 0
    else:
        remaining = cap_target - len(baseline_pool)
        selected_add_parts: List[pd.DataFrame] = []
        consumed_keys: set = set()

        # Ensure each targeted market gets at least a small representation when capped.
        min_floor = max(0, int(min_rows_per_target_market))
        floor_used = 0
        if min_floor > 0 and remaining > 0:
            for market in target_markets:
                market_norm = market.strip().lower()
                if not market_norm:
                    continue
                pool_market = add_pool[add_pool["key_market"] == market_norm].copy()
                if pool_market.empty:
                    continue
                take_n = min(min_floor, remaining - floor_used)
                if take_n <= 0:
                    break
                picked = pool_market.head(take_n).copy()
                if not picked.empty:
                    selected_add_parts.append(picked)
                    consumed_keys.update(picked["key"].tolist())
                    floor_used += int(len(picked))
                if floor_used >= remaining:
                    break

        if floor_used < remaining:
            remainder_pool = add_pool[~add_pool["key"].isin(consumed_keys)].copy()
            remainder_take = remaining - floor_used
            if remainder_take > 0 and not remainder_pool.empty:
                selected_add_parts.append(remainder_pool.head(remainder_take).copy())

        if selected_add_parts:
            selected_add = pd.concat(selected_add_parts, ignore_index=True)
            selected_add = selected_add.drop_duplicates(subset=["key"], keep="first")
        else:
            selected_add = add_pool.head(0).copy()

        selected = pd.concat([baseline_pool, selected_add], ignore_index=True)
        stage_counts["trimmed_baseline_to_target"] = 0
        stage_counts["added_target_rows"] = int(len(selected_add))
        stage_counts["added_target_rows_min_floor"] = int(floor_used)

    if len(selected) > int(hard_cap):
        selected = selected.sort_values(rank_cols, ascending=[False, False, False, True]).head(int(hard_cap)).copy()
    stage_counts["final_rows"] = int(len(selected))

    # Keep stable output order for readability.
    selected = selected.sort_values(["_row_order"], ascending=[True]).copy()
    return selected, stage_counts


def _distribution_rows(df: pd.DataFrame, *, variant: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    total = int(len(df))
    rows.append(
        {
            "variant": variant,
            "metric": "total_rows",
            "dimension": "all",
            "value": "all",
            "count": total,
            "share": 1.0 if total > 0 else 0.0,
        }
    )

    for value, count in df["prop_type"].value_counts(dropna=False).items():
        rows.append(
            {
                "variant": variant,
                "metric": "prop_type",
                "dimension": "prop_type",
                "value": str(value),
                "count": int(count),
                "share": float(count / total) if total > 0 else 0.0,
            }
        )
    for value, count in df["key_side"].value_counts(dropna=False).items():
        rows.append(
            {
                "variant": variant,
                "metric": "side",
                "dimension": "side",
                "value": str(value),
                "count": int(count),
                "share": float(count / total) if total > 0 else 0.0,
            }
        )
    return rows


def _pair_diff(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_name: str,
    right_name: str,
) -> pd.DataFrame:
    left_cols = KEY_COLUMNS + ["WIN %", "win_value", "prop_type"]
    right_cols = KEY_COLUMNS + ["WIN %", "win_value", "prop_type"]

    l = left[left_cols].rename(
        columns={"WIN %": f"WIN_%_{left_name}", "win_value": f"win_value_{left_name}", "prop_type": f"prop_type_{left_name}"}
    )
    r = right[right_cols].rename(
        columns={"WIN %": f"WIN_%_{right_name}", "win_value": f"win_value_{right_name}", "prop_type": f"prop_type_{right_name}"}
    )
    merged = l.merge(r, on=KEY_COLUMNS, how="outer", indicator=True)
    merged["status"] = merged["_merge"].map({"both": "shared", "left_only": f"only_{left_name}", "right_only": f"only_{right_name}"})
    merged = merged.drop(columns=["_merge"])
    return merged


def _side_share(df: pd.DataFrame, market: str) -> Dict[str, float]:
    subset = df[df["key_market"] == market].copy()
    if subset.empty:
        return {}
    counts = subset["key_side"].value_counts(dropna=False)
    total = float(counts.sum())
    return {str(k): round(float(v / total), 4) for k, v in counts.items()}


def _build_summary(
    *,
    experiment_root: Path,
    base_df: pd.DataFrame,
    weighted_df: pd.DataFrame,
    threshold_df: pd.DataFrame,
    filtered_df: pd.DataFrame,
    stage_counts: Dict[str, Any],
    target_markets: Sequence[str],
    target_rows: int,
    hard_cap: int,
    max_player_prop_rows: int,
    extreme_abs_win_cutoff: Optional[float],
    min_rows_per_target_market: int,
) -> Dict[str, Any]:
    base_keys = set(base_df["key"].tolist())
    threshold_keys = set(threshold_df["key"].tolist())
    filtered_keys = set(filtered_df["key"].tolist())

    overlap_with_base = len(filtered_keys & base_keys)
    threshold_new = threshold_keys - base_keys
    filtered_new = filtered_keys - base_keys
    retained_new_pct = float(len(filtered_new) / len(threshold_new)) if threshold_new else 0.0

    target_side: Dict[str, Dict[str, Dict[str, float]]] = {}
    for market in target_markets:
        market_norm = market.strip().lower()
        target_side[market_norm] = {
            "threshold": _side_share(threshold_df, market_norm),
            "filtered": _side_share(filtered_df, market_norm),
        }

    summary: Dict[str, Any] = {
        "experiment_root": str(experiment_root),
        "controls": {
            "target_markets": list(target_markets),
            "target_rows": int(target_rows),
            "hard_cap": int(hard_cap),
            "max_player_prop_rows": int(max_player_prop_rows),
            "drop_abs_win_pct_gte": float(extreme_abs_win_cutoff) if extreme_abs_win_cutoff is not None else None,
            "min_rows_per_target_market": int(min_rows_per_target_market),
        },
        "rows": {
            "base": int(len(base_df)),
            "weighted": int(len(weighted_df)),
            "threshold_enhanced": int(len(threshold_df)),
            "threshold_enhanced_filtered": int(len(filtered_df)),
        },
        "surface_control_stages": stage_counts,
        "overlap": {
            "filtered_overlap_with_base_count": int(overlap_with_base),
            "filtered_overlap_with_base_pct_of_filtered": float(overlap_with_base / len(filtered_df)) if len(filtered_df) else 0.0,
            "filtered_overlap_with_base_pct_of_base": float(overlap_with_base / len(base_df)) if len(base_df) else 0.0,
            "threshold_new_rows_vs_base": int(len(threshold_new)),
            "filtered_new_rows_vs_base": int(len(filtered_new)),
            "filtered_new_rows_retained_pct_vs_threshold_new": retained_new_pct,
        },
        "target_market_side_share": target_side,
        "answers": {
            "matches_base_surface_size": abs(len(filtered_df) - len(base_df)) <= 30,
            "surface_expansion_controlled": len(filtered_df) <= int(hard_cap),
            "safe_for_one_day_manual_shadow_test": len(filtered_df) <= int(hard_cap),
        },
    }
    return summary


def _summary_markdown(summary: Dict[str, Any]) -> str:
    rows = summary["rows"]
    overlap = summary["overlap"]
    controls = summary["controls"]
    answers = summary["answers"]
    target_side = summary["target_market_side_share"]

    lines: List[str] = []
    lines.append("# Threshold Shadow Summary (Surface-Controlled)")
    lines.append("")
    lines.append("## Controls")
    lines.append(f"- Target props/markets: {controls['target_markets']}")
    lines.append(f"- Target rows: {controls['target_rows']}")
    lines.append(f"- Hard cap: {controls['hard_cap']}")
    lines.append(f"- Max rows per player/prop_type: {controls['max_player_prop_rows']}")
    lines.append(f"- Drop rows where |WIN %| >= {controls['drop_abs_win_pct_gte']}")
    lines.append(f"- Min rows per targeted market before global fill: {controls['min_rows_per_target_market']}")
    lines.append("")
    lines.append("## Row Counts")
    lines.append(f"- Base: {rows['base']}")
    lines.append(f"- Weighted: {rows['weighted']}")
    lines.append(f"- Threshold enhanced (unfiltered): {rows['threshold_enhanced']}")
    lines.append(f"- Threshold enhanced (filtered): {rows['threshold_enhanced_filtered']}")
    lines.append("")
    lines.append("## Base Comparison")
    lines.append(
        f"- Row count delta vs base: {rows['threshold_enhanced_filtered'] - rows['base']}"
    )
    lines.append(
        f"- Overlap with base: {overlap['filtered_overlap_with_base_count']} "
        f"({100.0 * overlap['filtered_overlap_with_base_pct_of_filtered']:.2f}% of filtered, "
        f"{100.0 * overlap['filtered_overlap_with_base_pct_of_base']:.2f}% of base)"
    )
    lines.append(
        f"- New rows retained after filtering: {overlap['filtered_new_rows_vs_base']} / "
        f"{overlap['threshold_new_rows_vs_base']} "
        f"({100.0 * overlap['filtered_new_rows_retained_pct_vs_threshold_new']:.2f}%)"
    )
    lines.append("")
    lines.append("## Target Prop Side Distribution")
    for market, payload in target_side.items():
        lines.append(
            f"- {market}: threshold={payload.get('threshold', {})}, filtered={payload.get('filtered', {})}"
        )
    lines.append("")
    lines.append("## Questions")
    lines.append(
        f"- Does filtered threshold output match base surface size? {'Yes' if answers['matches_base_surface_size'] else 'No'}"
    )
    lines.append("- Do runs_scored and singles still show corrected bias? See target side distribution above.")
    lines.append(
        f"- Are we now controlling surface expansion? {'Yes' if answers['surface_expansion_controlled'] else 'No'}"
    )
    lines.append(
        f"- Safe for one-day manual upload shadow test? {'Yes' if answers['safe_for_one_day_manual_shadow_test'] else 'No'}"
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-root", required=True, help="Shadow experiment folder.")
    parser.add_argument(
        "--base-csv",
        default="backend/mlb/data/processed/mlb_uploads/2026-04-26/05_book_upload_base.csv",
        help="Base upload CSV path.",
    )
    parser.add_argument(
        "--weighted-csv",
        default="backend/mlb/data/processed/mlb_uploads/2026-04-26/05_book_upload_weighted.csv",
        help="Weighted upload CSV path.",
    )
    parser.add_argument(
        "--threshold-csv",
        default="threshold_enhanced_upload.csv",
        help="Threshold-enhanced upload CSV (relative to experiment root if not absolute).",
    )
    parser.add_argument("--target-rows", type=int, default=350)
    parser.add_argument("--hard-cap", type=int, default=400)
    parser.add_argument("--max-player-prop-rows", type=int, default=2)
    parser.add_argument("--drop-abs-win-pct-gte", type=float, default=500.0)
    parser.add_argument(
        "--min-rows-per-target-market",
        type=int,
        default=1,
        help="Minimum rows to keep per targeted market before global rank fill.",
    )
    parser.add_argument(
        "--target-markets",
        default="batter_runs,batter_singles",
        help="Comma-separated market keys allowed to expand.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    experiment_root = Path(args.experiment_root).resolve()
    if not experiment_root.exists():
        raise FileNotFoundError(f"experiment root missing: {experiment_root}")

    base_csv = Path(args.base_csv).resolve()
    weighted_csv = Path(args.weighted_csv).resolve()
    threshold_csv = Path(args.threshold_csv)
    if not threshold_csv.is_absolute():
        threshold_csv = experiment_root / threshold_csv
    threshold_csv = threshold_csv.resolve()

    target_markets = [v.strip().lower() for v in str(args.target_markets).split(",") if v.strip()]
    if not target_markets:
        raise RuntimeError("no target markets supplied")

    base_df = _load_upload(base_csv, name="base")
    weighted_df = _load_upload(weighted_csv, name="weighted")
    threshold_df = _load_upload(threshold_csv, name="threshold_enhanced")

    filtered_df, stage_counts = _select_with_surface_controls(
        base_df=base_df,
        threshold_df=threshold_df,
        target_markets=target_markets,
        target_rows=args.target_rows,
        hard_cap=args.hard_cap,
        max_player_prop_rows=args.max_player_prop_rows,
        extreme_abs_win_cutoff=args.drop_abs_win_pct_gte,
        min_rows_per_target_market=args.min_rows_per_target_market,
    )

    original_cols = list(pd.read_csv(threshold_csv, nrows=0).columns)
    filtered_out = filtered_df[original_cols].copy()

    out_filtered = experiment_root / "threshold_enhanced_filtered.csv"
    filtered_out.to_csv(out_filtered, index=False)

    # Comparison distributions.
    comparison_rows: List[Dict[str, Any]] = []
    comparison_rows.extend(_distribution_rows(base_df, variant="base"))
    comparison_rows.extend(_distribution_rows(threshold_df, variant="threshold_enhanced"))
    comparison_rows.extend(_distribution_rows(filtered_df, variant="threshold_enhanced_filtered"))
    comparison_df = pd.DataFrame(comparison_rows)
    comparison_df.to_csv(experiment_root / "threshold_surface_comparison.csv", index=False)

    base_vs_filtered = _pair_diff(base_df, filtered_df, left_name="base", right_name="threshold_filtered")
    base_vs_filtered.to_csv(experiment_root / "base_vs_threshold_filtered_diff.csv", index=False)

    weighted_vs_filtered = _pair_diff(weighted_df, filtered_df, left_name="weighted", right_name="threshold_filtered")
    weighted_vs_filtered.to_csv(experiment_root / "weighted_vs_threshold_filtered_diff.csv", index=False)

    only_threshold_filtered = base_vs_filtered[base_vs_filtered["status"] == "only_threshold_filtered"].copy()
    removed_from_threshold_filtered = base_vs_filtered[base_vs_filtered["status"] == "only_base"].copy()
    only_threshold_filtered.to_csv(experiment_root / "only_threshold_filtered_rows.csv", index=False)
    removed_from_threshold_filtered.to_csv(experiment_root / "removed_from_threshold_filtered_rows.csv", index=False)

    summary = _build_summary(
        experiment_root=experiment_root,
        base_df=base_df,
        weighted_df=weighted_df,
        threshold_df=threshold_df,
        filtered_df=filtered_df,
        stage_counts=stage_counts,
        target_markets=target_markets,
        target_rows=args.target_rows,
        hard_cap=args.hard_cap,
        max_player_prop_rows=args.max_player_prop_rows,
        extreme_abs_win_cutoff=args.drop_abs_win_pct_gte,
        min_rows_per_target_market=args.min_rows_per_target_market,
    )

    (experiment_root / "threshold_shadow_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    (experiment_root / "threshold_shadow_summary.md").write_text(
        _summary_markdown(summary),
        encoding="utf-8",
    )

    print(f"[threshold-surface] experiment_root={experiment_root}")
    print(f"[threshold-surface] wrote filtered_csv={out_filtered} rows={len(filtered_out)}")
    print(f"[threshold-surface] wrote comparison_csv={experiment_root / 'threshold_surface_comparison.csv'}")
    print(f"[threshold-surface] wrote summary_json={experiment_root / 'threshold_shadow_summary.json'}")
    print(f"[threshold-surface] wrote summary_md={experiment_root / 'threshold_shadow_summary.md'}")


if __name__ == "__main__":
    main()
