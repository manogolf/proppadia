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
    windows_days: list[int]
    target_rows: int
    min_model_rows: int
    min_model_win_rate_pct: float
    min_graded_rows: int
    graded_roi_floor_pct: float
    min_overs: int


def _parse_windows_days(raw: str, fallback_days: int) -> list[int]:
    vals: list[int] = []
    for part in str(raw or "").split(","):
        text = str(part or "").strip()
        if not text:
            continue
        try:
            v = int(text)
        except Exception:
            continue
        if v > 0:
            vals.append(v)
    if not vals:
        vals = [max(1, int(fallback_days))]
    return sorted(set(vals))


def _weighted_mean_from_pairs(pairs: list[tuple[float, float]]) -> float | None:
    clean = [(v, w) for (v, w) in pairs if v is not None and w is not None and w > 0]
    if not clean:
        return None
    w_sum = sum(w for _, w in clean)
    if w_sum <= 0:
        return None
    return float(sum(v * w for v, w in clean) / w_sum)


def _build_prop_metrics(
    *,
    by_prop_df: pd.DataFrame,
    as_of_date: date,
    cfg: SuggestionConfig,
) -> tuple[pd.DataFrame, date, dict[str, Any]]:
    if by_prop_df.empty:
        return pd.DataFrame(), as_of_date, {
            "requested_windows_days": list(cfg.windows_days or [cfg.lookback_days]),
            "active_windows_days": [],
            "fallback_mode": "no_tracker_rows",
            "tracker_distinct_days": 0,
        }

    work = by_prop_df.copy()
    work["report_date"] = pd.to_datetime(work["report_date"], errors="coerce").dt.date
    work = work.dropna(subset=["report_date", "prop_type"])
    if work.empty:
        return pd.DataFrame(), as_of_date, {
            "requested_windows_days": list(cfg.windows_days or [cfg.lookback_days]),
            "active_windows_days": [],
            "fallback_mode": "no_valid_dates",
            "tracker_distinct_days": 0,
        }

    latest_date = max(work["report_date"])
    tracker_distinct_days = int(work["report_date"].nunique())
    requested_windows = sorted(set(int(x) for x in (cfg.windows_days or [cfg.lookback_days]) if int(x) > 0))
    if not requested_windows:
        requested_windows = [max(1, int(cfg.lookback_days))]
    active_windows = [w for w in requested_windows if tracker_distinct_days >= int(w)]
    scoring_windows = list(active_windows) if active_windows else [int(requested_windows[0])]
    fallback_mode = "none" if active_windows else "insufficient_window_history"

    largest_window = max(requested_windows)
    keep_start = latest_date - timedelta(days=max(1, int(largest_window)) - 1)
    work = work[work["report_date"] >= keep_start].copy()
    if work.empty:
        return pd.DataFrame(), latest_date, {
            "requested_windows_days": requested_windows,
            "active_windows_days": active_windows,
            "scoring_windows_days": scoring_windows,
            "fallback_mode": "no_rows_in_window",
            "tracker_distinct_days": tracker_distinct_days,
        }

    window_weights_raw = {int(w): (1.0 / float(max(1, int(w)))) for w in scoring_windows}
    weight_sum = float(sum(window_weights_raw.values())) if window_weights_raw else 0.0
    if weight_sum <= 0:
        window_weights = {int(w): 1.0 for w in scoring_windows}
        weight_sum = float(len(window_weights)) if window_weights else 1.0
    else:
        window_weights = dict(window_weights_raw)
    window_weights = {int(w): float(v) / float(weight_sum) for w, v in window_weights.items()}

    rows: list[dict[str, Any]] = []
    for prop, grp in work.groupby("prop_type", dropna=False):
        prop_type = _norm_text(prop)
        if not prop_type:
            continue

        largest_active_for_rows = max(scoring_windows) if scoring_windows else int(cfg.lookback_days)
        rows_start = latest_date - timedelta(days=max(1, int(largest_active_for_rows)) - 1)
        grp_rows = grp[grp["report_date"] >= rows_start]
        model_rows_sum = int(pd.to_numeric(grp_rows.get("model_rows"), errors="coerce").fillna(0).sum())
        graded_rows_sum = int(pd.to_numeric(grp_rows.get("graded_rows"), errors="coerce").fillna(0).sum())

        by_window: dict[int, dict[str, Any]] = {}
        for w in requested_windows:
            w_start = latest_date - timedelta(days=max(1, int(w)) - 1)
            w_grp = grp[grp["report_date"] >= w_start]
            model_rows_w = int(pd.to_numeric(w_grp.get("model_rows"), errors="coerce").fillna(0).sum())
            graded_rows_w = int(pd.to_numeric(w_grp.get("graded_rows"), errors="coerce").fillna(0).sum())
            model_wr_w = _weighted_mean(w_grp.get("model_win_rate_pct"), w_grp.get("model_rows"))
            graded_wr_w = _weighted_mean(w_grp.get("graded_win_rate_pct"), w_grp.get("graded_rows"))
            graded_roi_w = _weighted_mean(w_grp.get("graded_roi_pct"), w_grp.get("graded_rows"))
            by_window[int(w)] = {
                "model_rows": model_rows_w,
                "graded_rows": graded_rows_w,
                "model_wr": model_wr_w,
                "graded_wr": graded_wr_w,
                "graded_roi": graded_roi_w,
            }

        model_wr = _weighted_mean_from_pairs(
            [(by_window[w]["model_wr"], window_weights.get(int(w), 0.0)) for w in scoring_windows]
        )
        graded_wr = _weighted_mean_from_pairs(
            [(by_window[w]["graded_wr"], window_weights.get(int(w), 0.0)) for w in scoring_windows]
        )
        graded_roi = _weighted_mean_from_pairs(
            [(by_window[w]["graded_roi"], window_weights.get(int(w), 0.0)) for w in scoring_windows]
        )

        model_edge = (model_wr - 50.0) if model_wr is not None else 0.0
        graded_edge = (graded_wr - 50.0) if graded_wr is not None else 0.0
        graded_roi_component = graded_roi if graded_roi is not None else 0.0
        score = (0.60 * model_edge) + (0.25 * graded_edge) + (0.15 * graded_roi_component)

        pass_model_rows = model_rows_sum >= int(cfg.min_model_rows)
        pass_model_windows = True
        if active_windows:
            for w in active_windows:
                wr_w = by_window[int(w)]["model_wr"]
                if wr_w is not None and float(wr_w) < float(cfg.min_model_win_rate_pct):
                    pass_model_windows = False
                    break
        elif model_wr is not None and float(model_wr) < float(cfg.min_model_win_rate_pct):
            pass_model_windows = False

        pass_model = bool(pass_model_rows and pass_model_windows)
        if graded_rows_sum >= int(cfg.min_graded_rows):
            pass_graded = graded_roi is not None and float(graded_roi) >= float(cfg.graded_roi_floor_pct)
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
                "active_windows_count": int(len(active_windows)),
                "fallback_mode": fallback_mode,
            }
        )
        for w in requested_windows:
            wv = by_window[int(w)]
            rows[-1][f"model_rows_{int(w)}d"] = int(wv["model_rows"])
            rows[-1][f"graded_rows_{int(w)}d"] = int(wv["graded_rows"])
            rows[-1][f"model_wr_{int(w)}d"] = None if wv["model_wr"] is None else round(float(wv["model_wr"]), 2)
            rows[-1][f"graded_wr_{int(w)}d"] = None if wv["graded_wr"] is None else round(float(wv["graded_wr"]), 2)
            rows[-1][f"graded_roi_{int(w)}d"] = None if wv["graded_roi"] is None else round(float(wv["graded_roi"]), 2)

    out = pd.DataFrame(rows)
    if out.empty:
        return out, latest_date, {
            "requested_windows_days": requested_windows,
            "active_windows_days": active_windows,
            "scoring_windows_days": scoring_windows,
            "scoring_weights": {str(k): round(float(v), 4) for k, v in window_weights.items()},
            "fallback_mode": fallback_mode,
            "tracker_distinct_days": tracker_distinct_days,
        }

    if not out["allow"].any():
        # Safety fallback: keep flow alive with the strongest model-backed props.
        out = out.sort_values(["score", "model_rows_sum"], ascending=[False, False], kind="mergesort")
        out["allow"] = out["model_rows_sum"] > 0
    else:
        out = out.sort_values(["allow", "score", "model_rows_sum"], ascending=[False, False, False], kind="mergesort")

    return out.reset_index(drop=True), latest_date, {
        "requested_windows_days": requested_windows,
        "active_windows_days": active_windows,
        "scoring_windows_days": scoring_windows,
        "scoring_weights": {str(k): round(float(v), 4) for k, v in window_weights.items()},
        "fallback_mode": fallback_mode,
        "tracker_distinct_days": tracker_distinct_days,
    }


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
    ap.add_argument(
        "--windows-days",
        default="",
        help="Comma-separated rolling windows (for example: 7,14). Early season fallback keeps flow running when full windows are unavailable.",
    )
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
        windows_days=_parse_windows_days(str(args.windows_days or ""), fallback_days=max(1, int(args.lookback_days))),
        target_rows=max(1, int(args.target_rows)),
        min_model_rows=max(1, int(args.min_model_rows)),
        min_model_win_rate_pct=float(args.min_model_win_rate_pct),
        min_graded_rows=max(0, int(args.min_graded_rows)),
        graded_roi_floor_pct=float(args.graded_roi_floor_pct),
        min_overs=max(0, int(args.min_overs)),
    )

    as_of = date.today()
    prop_metrics, metrics_latest_date, window_meta = _build_prop_metrics(by_prop_df=by_prop_df, as_of_date=as_of, cfg=cfg)
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
            "windows_days": cfg.windows_days,
            "target_rows": cfg.target_rows,
            "min_model_rows": cfg.min_model_rows,
            "min_model_win_rate_pct": cfg.min_model_win_rate_pct,
            "min_graded_rows": cfg.min_graded_rows,
            "graded_roi_floor_pct": cfg.graded_roi_floor_pct,
            "min_overs": cfg.min_overs,
        },
        "window": {
            "metrics_latest_report_date": str(metrics_latest_date),
            **window_meta,
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
