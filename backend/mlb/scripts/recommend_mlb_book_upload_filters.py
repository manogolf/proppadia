#!/usr/bin/env python3
"""Recommend daily MLB book-upload filters from recent post-grade performance.

Purpose:
- Keep a stable "best-of-bunch" workflow before placing wagers.
- Adapt allowed props daily using recent model + graded history.
- Emit a trimmed upload CSV (for example top 40) and a transparent JSON summary.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_MARKET_BY_PROP: dict[str, str] = {
    "hits": "batter_hits",
    "runs_scored": "batter_runs",
    "rbis": "batter_rbis",
    "runs_rbis": "batter_r+rbi",
    "total_bases": "batter_bases",
    "hits_runs_rbis": "batter_h+r+rbi",
    "walks": "batter_walks",
    "strikeouts_batting": "batter_strikeouts",
    "stolen_bases": "batter_stolen_bases",
    "singles": "batter_singles",
    "doubles": "batter_doubles",
    "triples": "batter_triples",
    "home_runs": "batter_home_runs",
    "hits_allowed": "pitcher_hits",
    "earned_runs": "pitcher_earned_runs",
    "outs_recorded": "pitcher_outs",
    "walks_allowed": "pitcher_walks",
    "strikeouts_pitching": "pitcher_strikeouts",
}

UPLOAD_MARKET_ALIASES: dict[str, str] = {
    "batter_hits_runs_rbis": "batter_h+r+rbi",
    "batter_total_bases": "batter_bases",
    "batter_runs_scored": "batter_runs",
    "pitcher_hits_allowed": "pitcher_hits",
}


def _norm_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _build_market_to_prop() -> dict[str, str]:
    out: dict[str, str] = {}
    for prop_type, market in DEFAULT_MARKET_BY_PROP.items():
        out[_norm_text(market)] = _norm_text(prop_type)
    for alias_market, canonical_market in UPLOAD_MARKET_ALIASES.items():
        key = _norm_text(alias_market)
        canonical_prop = out.get(_norm_text(canonical_market))
        if canonical_prop:
            out[key] = canonical_prop
    return out


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    x = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce")
    mask = x.notna() & w.notna() & (w > 0)
    if not mask.any():
        return None
    return float((x[mask] * w[mask]).sum() / w[mask].sum())


@dataclass
class SuggestionConfig:
    lookback_days: int
    target_rows: int
    min_model_rows: int
    min_model_win_rate_pct: float
    min_graded_rows: int
    graded_roi_floor_pct: float
    min_overs: int


def _build_prop_metrics(
    *,
    by_prop_df: pd.DataFrame,
    as_of_date: date,
    cfg: SuggestionConfig,
) -> tuple[pd.DataFrame, date]:
    if by_prop_df.empty:
        return pd.DataFrame(), as_of_date

    work = by_prop_df.copy()
    work["report_date"] = pd.to_datetime(work["report_date"], errors="coerce").dt.date
    work = work.dropna(subset=["report_date", "prop_type"])
    if work.empty:
        return pd.DataFrame(), as_of_date

    latest_date = max(work["report_date"])
    window_start = latest_date - timedelta(days=max(1, int(cfg.lookback_days)) - 1)
    work = work[work["report_date"] >= window_start]
    if work.empty:
        return pd.DataFrame(), latest_date

    rows: list[dict[str, Any]] = []
    for prop, grp in work.groupby("prop_type", dropna=False):
        prop_type = _norm_text(prop)
        if not prop_type:
            continue
        model_rows_sum = int(pd.to_numeric(grp.get("model_rows"), errors="coerce").fillna(0).sum())
        graded_rows_sum = int(pd.to_numeric(grp.get("graded_rows"), errors="coerce").fillna(0).sum())

        model_wr = _weighted_mean(grp.get("model_win_rate_pct"), grp.get("model_rows"))
        graded_wr = _weighted_mean(grp.get("graded_win_rate_pct"), grp.get("graded_rows"))
        graded_roi = _weighted_mean(grp.get("graded_roi_pct"), grp.get("graded_rows"))

        model_edge = (model_wr - 50.0) if model_wr is not None else 0.0
        graded_edge = (graded_wr - 50.0) if graded_wr is not None else 0.0
        graded_roi_component = graded_roi if graded_roi is not None else 0.0
        score = (0.60 * model_edge) + (0.25 * graded_edge) + (0.15 * graded_roi_component)

        pass_model = (model_rows_sum >= int(cfg.min_model_rows)) and (
            model_wr is not None and model_wr >= float(cfg.min_model_win_rate_pct)
        )
        if graded_rows_sum >= int(cfg.min_graded_rows):
            pass_graded = graded_roi is not None and graded_roi >= float(cfg.graded_roi_floor_pct)
        else:
            pass_graded = True
        allow = bool(pass_model and pass_graded)

        rows.append(
            {
                "prop_type": prop_type,
                "model_rows_sum": model_rows_sum,
                "model_win_rate_pct_w": None if model_wr is None else round(float(model_wr), 2),
                "graded_rows_sum": graded_rows_sum,
                "graded_win_rate_pct_w": None if graded_wr is None else round(float(graded_wr), 2),
                "graded_roi_pct_w": None if graded_roi is None else round(float(graded_roi), 2),
                "score": round(float(score), 4),
                "allow": allow,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out, latest_date

    if not out["allow"].any():
        # Safety fallback: keep flow alive with the strongest model-backed props.
        out = out.sort_values(["score", "model_rows_sum"], ascending=[False, False], kind="mergesort")
        out["allow"] = out["model_rows_sum"] > 0
    else:
        out = out.sort_values(["allow", "score", "model_rows_sum"], ascending=[False, False, False], kind="mergesort")

    return out.reset_index(drop=True), latest_date


def _select_rows(
    *,
    book_df: pd.DataFrame,
    prop_metrics: pd.DataFrame,
    cfg: SuggestionConfig,
) -> tuple[pd.DataFrame, list[str]]:
    notes: list[str] = []
    work = book_df.copy()
    work["_rank"] = range(1, len(work) + 1)
    side = work.get("SIDE")
    if side is None:
        work["SIDE"] = ""
    work["side_norm"] = work["SIDE"].astype(str).str.strip().str.lower()

    market_to_prop = _build_market_to_prop()
    work["prop_type"] = work["MARKET"].astype(str).str.strip().str.lower().map(market_to_prop)
    work["prop_type"] = work["prop_type"].fillna("")

    allow_props = set()
    score_map: dict[str, float] = {}
    if not prop_metrics.empty:
        allow_props = {str(x) for x in prop_metrics[prop_metrics["allow"]]["prop_type"].tolist() if str(x)}
        score_map = {
            str(r["prop_type"]): float(r["score"])
            for _, r in prop_metrics.iterrows()
            if str(r.get("prop_type") or "")
        }

    work["prop_score"] = work["prop_type"].map(score_map).fillna(float("-inf"))
    allow_mask = work["prop_type"].isin(allow_props) if allow_props else pd.Series([True] * len(work), index=work.index)
    preferred = work[allow_mask].sort_values(["_rank"], ascending=[True], kind="mergesort")
    fallback = work[~allow_mask].sort_values(["_rank"], ascending=[True], kind="mergesort")

    chosen_idx: list[int] = preferred.index.tolist()[: int(cfg.target_rows)]
    if len(chosen_idx) < int(cfg.target_rows):
        need = int(cfg.target_rows) - len(chosen_idx)
        chosen_idx.extend(fallback.index.tolist()[:need])
        if need > 0:
            notes.append(f"filled {need} rows from fallback (outside allowlist) to reach target_rows")

    chosen_set = set(chosen_idx)
    selected = work.loc[sorted(chosen_set, key=lambda i: int(work.loc[i, "_rank"]))].copy()

    # Optional side-balance nudge: enforce minimum overs when available.
    if int(cfg.min_overs) > 0:
        over_count = int((selected["side_norm"] == "over").sum())
        need_over = int(cfg.min_overs) - over_count
        if need_over > 0:
            over_pool = work[(work["side_norm"] == "over") & (~work.index.isin(selected.index))].sort_values(
                ["_rank"], ascending=[True], kind="mergesort"
            )
            add_idx = over_pool.index.tolist()[:need_over]
            if add_idx:
                removable = selected[selected["side_norm"] != "over"].sort_values(
                    ["_rank"], ascending=[False], kind="mergesort"
                ).index.tolist()[: len(add_idx)]
                for ridx in removable:
                    chosen_set.discard(int(ridx))
                for aidx in add_idx:
                    chosen_set.add(int(aidx))
                notes.append(
                    f"side-balance applied: added {len(add_idx)} overs (min_overs={cfg.min_overs})"
                )
            else:
                notes.append("side-balance skipped: no additional over rows available")

    final_df = work.loc[sorted(chosen_set, key=lambda i: int(work.loc[i, "_rank"]))].copy()
    final_df = final_df.head(int(cfg.target_rows)).copy()
    return final_df, notes


def main() -> int:
    ap = argparse.ArgumentParser(description="Recommend daily MLB book-upload filters from post-grade tracker history.")
    ap.add_argument("--book-upload-csv", default="backend/mlb/data/processed/mlb_book_upload.csv")
    ap.add_argument("--by-prop-tracker-csv", default="artifacts/mlb_postgrade_by_prop_daily_tracker.csv")
    ap.add_argument("--lookback-days", type=int, default=5)
    ap.add_argument("--target-rows", type=int, default=40)
    ap.add_argument("--min-model-rows", type=int, default=60)
    ap.add_argument("--min-model-win-rate-pct", type=float, default=52.0)
    ap.add_argument("--min-graded-rows", type=int, default=8)
    ap.add_argument("--graded-roi-floor-pct", type=float, default=-8.0)
    ap.add_argument("--min-overs", type=int, default=4)
    ap.add_argument("--out-csv", default="backend/mlb/data/processed/mlb_book_upload_top40_recommended.csv")
    ap.add_argument("--out-json", default="tmp/analysis/mlb_book_upload_filter_recommendation.json")
    args = ap.parse_args()

    book_path = Path(args.book_upload_csv).expanduser()
    tracker_path = Path(args.by_prop_tracker_csv).expanduser()
    out_csv = Path(args.out_csv).expanduser()
    out_json = Path(args.out_json).expanduser()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    if not book_path.exists():
        raise FileNotFoundError(f"missing book upload csv: {book_path}")
    book_df = pd.read_csv(book_path, low_memory=False)
    if book_df.empty:
        raise RuntimeError(f"book upload csv is empty: {book_path}")

    if tracker_path.exists():
        by_prop_df = pd.read_csv(tracker_path, low_memory=False)
    else:
        by_prop_df = pd.DataFrame(
            columns=["report_date", "prop_type", "rows", "model_rows", "model_win_rate_pct", "graded_rows", "graded_win_rate_pct", "graded_roi_pct"]
        )

    cfg = SuggestionConfig(
        lookback_days=max(1, int(args.lookback_days)),
        target_rows=max(1, int(args.target_rows)),
        min_model_rows=max(1, int(args.min_model_rows)),
        min_model_win_rate_pct=float(args.min_model_win_rate_pct),
        min_graded_rows=max(0, int(args.min_graded_rows)),
        graded_roi_floor_pct=float(args.graded_roi_floor_pct),
        min_overs=max(0, int(args.min_overs)),
    )

    as_of = date.today()
    prop_metrics, metrics_latest_date = _build_prop_metrics(by_prop_df=by_prop_df, as_of_date=as_of, cfg=cfg)
    selected_df, notes = _select_rows(book_df=book_df, prop_metrics=prop_metrics, cfg=cfg)

    out_cols = [c for c in book_df.columns if c in selected_df.columns]
    selected_df[out_cols].to_csv(out_csv, index=False)

    selected_side = selected_df["side_norm"].value_counts().to_dict() if "side_norm" in selected_df.columns else {}
    selected_props = (
        selected_df["prop_type"].value_counts().head(12).to_dict() if "prop_type" in selected_df.columns else {}
    )
    allowlist = (
        prop_metrics[prop_metrics["allow"]]["prop_type"].astype(str).tolist()
        if not prop_metrics.empty
        else []
    )

    prop_metrics_records: list[dict[str, Any]] = []
    if not prop_metrics.empty:
        prop_metrics_records = (
            prop_metrics.astype(object).where(pd.notna(prop_metrics), None).to_dict(orient="records")
        )

    payload = {
        "ok": True,
        "captured_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "inputs": {
            "book_upload_csv": str(book_path),
            "by_prop_tracker_csv": str(tracker_path),
        },
        "config": {
            "lookback_days": cfg.lookback_days,
            "target_rows": cfg.target_rows,
            "min_model_rows": cfg.min_model_rows,
            "min_model_win_rate_pct": cfg.min_model_win_rate_pct,
            "min_graded_rows": cfg.min_graded_rows,
            "graded_roi_floor_pct": cfg.graded_roi_floor_pct,
            "min_overs": cfg.min_overs,
        },
        "window": {
            "metrics_latest_report_date": str(metrics_latest_date),
        },
        "summary": {
            "rows_input": int(len(book_df)),
            "rows_selected": int(len(selected_df)),
            "selected_over_rows": int(selected_side.get("over", 0)),
            "selected_under_rows": int(selected_side.get("under", 0)),
            "selected_unknown_side_rows": int(selected_side.get("", 0)),
            "allowlist_props_count": int(len(allowlist)),
            "allowlist_props": allowlist,
            "selected_top_props": selected_props,
            "notes": notes,
        },
        "outputs": {
            "recommended_csv": str(out_csv),
            "recommendation_json": str(out_json),
        },
        "prop_metrics": prop_metrics_records,
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
