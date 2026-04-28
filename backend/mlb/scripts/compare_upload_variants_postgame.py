#!/usr/bin/env python3
"""Compare MLB upload variants (base vs weighted) against post-game outcomes.

Read-only analysis utility:
- does not alter model artifacts
- does not alter upload CSVs
- does not alter grading sources
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


UPLOAD_REQUIRED_COLUMNS = [
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

# Upload market -> canonical prop_type
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

MARKET_ALIASES: Dict[str, str] = {
    "batter_hits_runs_rbis": "batter_h+r+rbi",
    "batter_total_bases": "batter_bases",
    "pitcher_hits_allowed": "pitcher_hits",
    "pitcher_ks": "pitcher_strikeouts",
    "pitcher_k": "pitcher_strikeouts",
}

SHARED_KEY_COLS = [
    "key_league",
    "key_date",
    "key_home",
    "key_away",
    "key_market",
    "key_selector",
    "key_point",
    "key_side",
]


@dataclass
class VariantSummary:
    variant: str
    total_rows: int
    graded_rows: int
    ungraded_rows: int
    win_count: int
    loss_count: int
    push_count: int
    win_rate_ex_push: Optional[float]


def _norm_text(v: Any) -> str:
    return str(v if v is not None else "").strip()


def _norm_team(v: Any) -> str:
    return _norm_text(v).upper()


def _norm_side(v: Any) -> str:
    t = _norm_text(v).lower()
    if t in {"o", "over"}:
        return "over"
    if t in {"u", "under"}:
        return "under"
    return t


def _parse_date(v: Any) -> str:
    t = _norm_text(v)
    if not t:
        return ""
    if t.isdigit() and len(t) == 8:
        dt = pd.to_datetime(t, format="%Y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(t, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _norm_market(v: Any) -> str:
    raw = _norm_text(v).lower()
    raw = MARKET_ALIASES.get(raw, raw)
    return raw


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        t = _norm_text(v).replace(",", "")
        if not t:
            return None
        return float(t)
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        return int(float(v))
    except Exception:
        return None


def _american_to_prob(v: Any) -> Optional[float]:
    x = _to_float(v)
    if x is None:
        return None
    # Handle direct probability (0-1)
    if 0.0 < x < 1.0:
        return float(x)
    # Handle percentage (0-100)
    if 1.0 <= x <= 100.0:
        return float(x / 100.0)
    # Handle American odds
    if x == 0.0:
        return None
    if x > 0:
        return 100.0 / (x + 100.0)
    return abs(x) / (abs(x) + 100.0)


def _outcome_from_actual(*, actual_value: Optional[float], line: Optional[float], side: str) -> str:
    if actual_value is None or line is None:
        return "ungraded"
    if actual_value == line:
        return "push"
    if side == "over":
        return "win" if actual_value > line else "loss"
    if side == "under":
        return "win" if actual_value < line else "loss"
    return "ungraded"


def _normalize_upload_df(df: pd.DataFrame, *, variant: str) -> pd.DataFrame:
    missing = [c for c in UPLOAD_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"{variant}: missing required upload columns: {missing}")

    out = df.copy()
    out["variant"] = variant
    out["row_id"] = np.arange(len(out), dtype=int)

    out["key_league"] = out["LEAGUE"].map(lambda v: _norm_text(v).upper())
    out["key_date"] = out["DATE"].map(_parse_date)
    out["key_home"] = out["HOME"].map(_norm_team)
    out["key_away"] = out["AWAY"].map(_norm_team)
    out["key_market"] = out["MARKET"].map(_norm_market)
    out["key_selector"] = out["SELECTOR"].map(_to_int)
    out["key_point"] = pd.to_numeric(out["POINT"], errors="coerce").round(4)
    out["key_side"] = out["SIDE"].map(_norm_side)

    out["prop_type"] = out["key_market"].map(lambda m: MARKET_TO_PROP.get(str(m), "unknown"))
    out["win_input"] = pd.to_numeric(out["WIN %"], errors="coerce")
    out["implied_win_prob"] = out["win_input"].map(_american_to_prob)

    return out


def _load_upload_csv(path: Path, *, variant: str, expected_date: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing {variant} csv: {path}")
    raw = pd.read_csv(path)
    out = _normalize_upload_df(raw, variant=variant)
    if out.empty:
        raise RuntimeError(f"{variant}: csv is empty: {path}")
    date_values = sorted(set(x for x in out["key_date"].tolist() if x))
    if expected_date and date_values and expected_date not in date_values:
        print(
            f"[warn] {variant}: expected date={expected_date} not found in upload DATE values; observed={date_values[:5]}"
        )
    return out


def _first_non_null(series: pd.Series) -> Any:
    for v in series.tolist():
        if pd.notna(v) and _norm_text(v) != "":
            return v
    return None


def _load_grading_rows(path: Path, *, target_date: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "grading_source": str(path),
        "grading_loaded": False,
        "grading_rows_all": 0,
        "grading_rows_date": 0,
    }
    if not path.exists():
        meta["grading_note"] = f"grading rows csv not found: {path}"
        return pd.DataFrame(), meta

    header = pd.read_csv(path, nrows=0)
    header_cols = [str(c) for c in header.columns]
    required = [
        "game_date",
        "home_team_code",
        "away_team_code",
        "market_key",
        "player_id",
        "line",
        "actual_value",
        "actual_over_outcome",
        "actual_under_outcome",
    ]
    missing = [c for c in required if c not in header_cols]
    if missing:
        meta["grading_note"] = f"grading rows csv missing required columns: {missing}"
        return pd.DataFrame(), meta

    usecols = [c for c in required + ["prop_type"] if c in header_cols]
    chunks: List[pd.DataFrame] = []
    rows_all = 0
    for chunk in pd.read_csv(path, usecols=usecols, chunksize=100000):
        rows_all += int(len(chunk))
        chunk_dates = pd.to_datetime(chunk["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        chunk["key_date"] = chunk_dates.fillna("")
        chunk = chunk[chunk["key_date"] == target_date]
        if not chunk.empty:
            chunks.append(chunk)
    meta["grading_rows_all"] = rows_all

    if chunks:
        g = pd.concat(chunks, ignore_index=True)
    else:
        g = pd.DataFrame(columns=usecols + ["key_date"])

    meta["grading_rows_date"] = int(len(g))
    if g.empty:
        meta["grading_note"] = "no grading rows for target date"
        return pd.DataFrame(), meta

    g["key_home"] = g["home_team_code"].map(_norm_team)
    g["key_away"] = g["away_team_code"].map(_norm_team)
    g["key_market"] = g["market_key"].map(_norm_market)
    g["key_selector"] = pd.to_numeric(g["player_id"], errors="coerce").astype("Int64")
    g["key_point"] = pd.to_numeric(g["line"], errors="coerce").round(4)
    g["actual_value"] = pd.to_numeric(g["actual_value"], errors="coerce")
    g["actual_over_outcome"] = g["actual_over_outcome"].map(lambda v: _norm_text(v).lower())
    g["actual_under_outcome"] = g["actual_under_outcome"].map(lambda v: _norm_text(v).lower())
    g["prop_type"] = g.get("prop_type", "").map(lambda v: _norm_text(v).lower())

    keep_cols = [
        "key_date",
        "key_home",
        "key_away",
        "key_market",
        "key_selector",
        "key_point",
        "actual_value",
        "actual_over_outcome",
        "actual_under_outcome",
        "prop_type",
    ]
    g = g[keep_cols].dropna(subset=["key_selector", "key_point"]).copy()

    key_cols = ["key_date", "key_home", "key_away", "key_market", "key_selector", "key_point"]
    agg = (
        g.groupby(key_cols, dropna=False, as_index=False)
        .agg(
            actual_value=("actual_value", _first_non_null),
            actual_over_outcome=("actual_over_outcome", _first_non_null),
            actual_under_outcome=("actual_under_outcome", _first_non_null),
            prop_type_from_grade=("prop_type", _first_non_null),
            grading_rows=("actual_value", "size"),
        )
        .copy()
    )

    meta["grading_loaded"] = True
    meta["grading_distinct_keys"] = int(len(agg))
    return agg, meta


def _apply_outcomes(upload_df: pd.DataFrame, grading_df: pd.DataFrame) -> pd.DataFrame:
    out = upload_df.copy()
    key_cols = ["key_date", "key_home", "key_away", "key_market", "key_selector", "key_point"]
    if grading_df.empty:
        out["actual_value"] = np.nan
        out["actual_over_outcome"] = ""
        out["actual_under_outcome"] = ""
        out["graded_rows"] = 0
    else:
        out = out.merge(
            grading_df,
            on=key_cols,
            how="left",
        )

    def _row_outcome(r: pd.Series) -> str:
        side = _norm_side(r.get("key_side"))
        over_flag = _norm_text(r.get("actual_over_outcome")).lower()
        under_flag = _norm_text(r.get("actual_under_outcome")).lower()
        if side == "over" and over_flag in {"win", "loss", "push"}:
            return over_flag
        if side == "under" and under_flag in {"win", "loss", "push"}:
            return under_flag
        return _outcome_from_actual(
            actual_value=_to_float(r.get("actual_value")),
            line=_to_float(r.get("key_point")),
            side=side,
        )

    out["outcome"] = out.apply(_row_outcome, axis=1)
    out["is_graded"] = out["outcome"].isin(["win", "loss", "push"])
    out["is_win"] = out["outcome"].eq("win")
    out["is_loss"] = out["outcome"].eq("loss")
    out["is_push"] = out["outcome"].eq("push")

    def _y_true(v: str) -> Optional[float]:
        if v == "win":
            return 1.0
        if v == "loss":
            return 0.0
        return None

    out["y_true_win"] = out["outcome"].map(_y_true)
    out["abs_prob_error"] = np.where(
        out["y_true_win"].notna() & out["implied_win_prob"].notna(),
        (out["implied_win_prob"] - out["y_true_win"]).abs(),
        np.nan,
    )
    return out


def _variant_summary(df: pd.DataFrame, *, variant: str) -> VariantSummary:
    total = int(len(df))
    graded = int(df["is_graded"].sum())
    wins = int(df["is_win"].sum())
    losses = int(df["is_loss"].sum())
    pushes = int(df["is_push"].sum())
    wl = wins + losses
    win_rate = (wins / wl) if wl > 0 else None
    return VariantSummary(
        variant=variant,
        total_rows=total,
        graded_rows=graded,
        ungraded_rows=max(total - graded, 0),
        win_count=wins,
        loss_count=losses,
        push_count=pushes,
        win_rate_ex_push=win_rate,
    )


def _group_metrics(df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=group_cols + ["total_rows", "graded_rows", "wins", "losses", "pushes", "win_rate_ex_push"])
    g = (
        df.groupby(group_cols, dropna=False, as_index=False)
        .agg(
            total_rows=("outcome", "size"),
            graded_rows=("is_graded", "sum"),
            wins=("is_win", "sum"),
            losses=("is_loss", "sum"),
            pushes=("is_push", "sum"),
        )
        .copy()
    )
    wl = g["wins"] + g["losses"]
    g["win_rate_ex_push"] = np.where(wl > 0, g["wins"] / wl, np.nan)
    return g


def _bucket_abs_diff(v: Any) -> str:
    x = _to_float(v)
    if x is None:
        return "unknown"
    x = abs(x)
    if x == 0:
        return "0"
    if x < 10:
        return "0-10"
    if x < 25:
        return "10-25"
    if x < 50:
        return "25-50"
    return "50+"


def _shared_comparison(base_df: pd.DataFrame, weighted_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    base_cols = SHARED_KEY_COLS + [
        "prop_type",
        "WIN %",
        "win_input",
        "implied_win_prob",
        "outcome",
        "is_graded",
        "y_true_win",
    ]
    weighted_cols = SHARED_KEY_COLS + ["WIN %", "win_input", "implied_win_prob", "outcome", "is_graded", "y_true_win"]

    left = base_df[base_cols].rename(
        columns={
            "WIN %": "base_win_col",
            "win_input": "base_win_input",
            "implied_win_prob": "base_implied_win_prob",
            "outcome": "base_outcome",
            "is_graded": "base_is_graded",
            "y_true_win": "base_y_true_win",
        }
    )
    right = weighted_df[weighted_cols].rename(
        columns={
            "WIN %": "weighted_win_col",
            "win_input": "weighted_win_input",
            "implied_win_prob": "weighted_implied_win_prob",
            "outcome": "weighted_outcome",
            "is_graded": "weighted_is_graded",
            "y_true_win": "weighted_y_true_win",
        }
    )

    shared = left.merge(right, on=SHARED_KEY_COLS, how="inner")
    if shared.empty:
        return shared, {
            "shared_rows": 0,
            "shared_graded_rows": 0,
            "avg_abs_win_input_diff": None,
            "confidence_alignment": {
                "improved": 0,
                "degraded": 0,
                "tied": 0,
                "avg_abs_error_base": None,
                "avg_abs_error_weighted": None,
            },
            "abs_diff_buckets": {},
        }

    shared["win_input_diff"] = shared["weighted_win_input"] - shared["base_win_input"]
    shared["abs_win_input_diff"] = shared["win_input_diff"].abs()
    shared["abs_diff_bucket"] = shared["abs_win_input_diff"].map(_bucket_abs_diff)

    shared["y_true_win"] = np.where(
        shared["base_y_true_win"].notna(), shared["base_y_true_win"], shared["weighted_y_true_win"]
    )
    shared["is_graded"] = shared["base_is_graded"] | shared["weighted_is_graded"]

    shared["abs_error_base"] = np.where(
        shared["is_graded"] & shared["y_true_win"].notna() & shared["base_implied_win_prob"].notna(),
        (shared["base_implied_win_prob"] - shared["y_true_win"]).abs(),
        np.nan,
    )
    shared["abs_error_weighted"] = np.where(
        shared["is_graded"] & shared["y_true_win"].notna() & shared["weighted_implied_win_prob"].notna(),
        (shared["weighted_implied_win_prob"] - shared["y_true_win"]).abs(),
        np.nan,
    )

    tol = 1e-12
    improved = int(((shared["abs_error_weighted"] + tol) < shared["abs_error_base"]).sum())
    degraded = int(((shared["abs_error_weighted"] - tol) > shared["abs_error_base"]).sum())
    tied = int(
        (
            shared["abs_error_base"].notna()
            & shared["abs_error_weighted"].notna()
            & (shared["abs_error_weighted"] - shared["abs_error_base"]).abs().le(tol)
        ).sum()
    )

    meta = {
        "shared_rows": int(len(shared)),
        "shared_graded_rows": int(shared["is_graded"].sum()),
        "avg_abs_win_input_diff": (
            float(shared["abs_win_input_diff"].mean()) if shared["abs_win_input_diff"].notna().any() else None
        ),
        "confidence_alignment": {
            "improved": improved,
            "degraded": degraded,
            "tied": tied,
            "avg_abs_error_base": (
                float(shared["abs_error_base"].mean()) if shared["abs_error_base"].notna().any() else None
            ),
            "avg_abs_error_weighted": (
                float(shared["abs_error_weighted"].mean()) if shared["abs_error_weighted"].notna().any() else None
            ),
        },
        "abs_diff_buckets": (
            shared["abs_diff_bucket"].value_counts(dropna=False).sort_index().to_dict()
        ),
    }
    return shared, meta


def _build_recommendation(*, base: VariantSummary, weighted: VariantSummary, shared_meta: Dict[str, Any]) -> str:
    min_eval_rows = 50
    if base.graded_rows < min_eval_rows and weighted.graded_rows < min_eval_rows:
        return "Inconclusive due to limited graded outcomes; keep testing both variants."

    base_wr = base.win_rate_ex_push or 0.0
    weighted_wr = weighted.win_rate_ex_push or 0.0
    wr_diff_pp = (weighted_wr - base_wr) * 100.0

    improved = int(shared_meta.get("confidence_alignment", {}).get("improved", 0))
    degraded = int(shared_meta.get("confidence_alignment", {}).get("degraded", 0))

    if wr_diff_pp >= 1.0 and improved >= degraded:
        return "Keep testing weighted as primary; it shows better post-game directional performance with non-worse confidence alignment."
    if wr_diff_pp <= -1.0 and degraded > improved:
        return "Revert to base for next slate; weighted underperformed and confidence alignment degraded."
    if improved > degraded:
        return "Use weighted for continued A/B testing; confidence alignment improved but directional edge is modest."
    if degraded > improved:
        return "Prefer base for now; weighted confidence alignment weakened without clear directional gain."
    return "Inconclusive: continue parallel testing until more graded slates accumulate."


def _safe_pct(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "n/a"
    return f"{100.0 * float(v):.2f}%"


def _top_prop_delta(by_prop: pd.DataFrame) -> Dict[str, Any]:
    if by_prop.empty:
        return {}
    pivot = by_prop.pivot_table(index="prop_type", columns="variant", values="win_rate_ex_push", aggfunc="first")
    if "base" not in pivot.columns or "weighted" not in pivot.columns:
        return {}
    pivot = pivot.dropna(subset=["base", "weighted"]).copy()
    if pivot.empty:
        return {}
    pivot["delta_pp"] = (pivot["weighted"] - pivot["base"]) * 100.0
    best = pivot.sort_values("delta_pp", ascending=False).head(3)
    worst = pivot.sort_values("delta_pp", ascending=True).head(3)
    return {
        "best": [
            {"prop_type": idx, "delta_pp": float(row["delta_pp"])}
            for idx, row in best.iterrows()
        ],
        "worst": [
            {"prop_type": idx, "delta_pp": float(row["delta_pp"])}
            for idx, row in worst.iterrows()
        ],
    }


def _hits_compare(by_prop: pd.DataFrame) -> Dict[str, Any]:
    if by_prop.empty:
        return {}
    hits = by_prop[by_prop["prop_type"] == "hits"].copy()
    if hits.empty:
        return {"note": "hits prop not present in compared rows"}
    out: Dict[str, Any] = {}
    for _, r in hits.iterrows():
        out[str(r["variant"])] = {
            "total_rows": int(r["total_rows"]),
            "graded_rows": int(r["graded_rows"]),
            "win_rate_ex_push": (float(r["win_rate_ex_push"]) if pd.notna(r["win_rate_ex_push"]) else None),
        }
    return out


def _write_summary_md(
    *,
    out_path: Path,
    target_date: str,
    base: VariantSummary,
    weighted: VariantSummary,
    shared_meta: Dict[str, Any],
    grading_meta: Dict[str, Any],
    prop_delta: Dict[str, Any],
    hits_compare: Dict[str, Any],
    recommendation: str,
) -> None:
    lines: List[str] = []
    lines.append(f"# MLB Upload Variant Postgame Compare — {target_date}")
    lines.append("")
    lines.append("## Overall")
    lines.append(f"- Base win rate (ex push): {_safe_pct(base.win_rate_ex_push)} ({base.win_count}-{base.loss_count}, pushes={base.push_count})")
    lines.append(
        f"- Weighted win rate (ex push): {_safe_pct(weighted.win_rate_ex_push)} ({weighted.win_count}-{weighted.loss_count}, pushes={weighted.push_count})"
    )
    lines.append(f"- Base graded coverage: {base.graded_rows}/{base.total_rows}")
    lines.append(f"- Weighted graded coverage: {weighted.graded_rows}/{weighted.total_rows}")
    lines.append("")

    lines.append("## Shared Rows")
    lines.append(f"- Shared rows: {shared_meta.get('shared_rows', 0)}")
    lines.append(f"- Shared graded rows: {shared_meta.get('shared_graded_rows', 0)}")
    ca = shared_meta.get("confidence_alignment", {})
    lines.append(
        f"- Confidence alignment (weighted vs base abs error): improved={ca.get('improved', 0)}, degraded={ca.get('degraded', 0)}, tied={ca.get('tied', 0)}"
    )
    lines.append(
        f"- Avg abs probability error: base={ca.get('avg_abs_error_base', 'n/a')}, weighted={ca.get('avg_abs_error_weighted', 'n/a')}"
    )
    lines.append("")

    lines.append("## Props")
    if prop_delta:
        best = prop_delta.get("best", [])
        worst = prop_delta.get("worst", [])
        if best:
            lines.append("- Best weighted deltas (pp): " + ", ".join(f"{x['prop_type']} ({x['delta_pp']:+.2f})" for x in best))
        if worst:
            lines.append("- Worst weighted deltas (pp): " + ", ".join(f"{x['prop_type']} ({x['delta_pp']:+.2f})" for x in worst))
    else:
        lines.append("- Not enough comparable per-prop graded rows to compute deltas.")

    if hits_compare:
        lines.append("- Hits compare: " + json.dumps(hits_compare))
    lines.append("")

    lines.append("## Grading Source")
    lines.append(f"- Source: {grading_meta.get('grading_source')}")
    lines.append(f"- Loaded: {grading_meta.get('grading_loaded')}")
    lines.append(f"- Date rows available: {grading_meta.get('grading_rows_date')}")
    if grading_meta.get("grading_note"):
        lines.append(f"- Note: {grading_meta.get('grading_note')}")
    lines.append("")

    lines.append("## Recommendation")
    lines.append(f"- {recommendation}")
    lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")


def _json_default(v: Any) -> Any:
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    return str(v)


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare MLB upload variants postgame (base vs weighted).")
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD")
    ap.add_argument(
        "--base-csv",
        default="",
        help="Path to 05_book_upload_base.csv (default derives from --date)",
    )
    ap.add_argument(
        "--weighted-csv",
        default="",
        help="Path to 05_book_upload_weighted.csv (default derives from --date)",
    )
    ap.add_argument(
        "--out-dir",
        default="",
        help="Output directory (default artifacts/analysis/mlb/upload_variant_compare/{date})",
    )
    ap.add_argument(
        "--graded-rows-csv",
        default="tmp/mlb_base_vs_market_rows_anybook.csv",
        help="Existing reconcile/graded rows CSV used for outcomes.",
    )
    args = ap.parse_args()

    target_date = _parse_date(args.date)
    if not target_date:
        raise ValueError("--date must parse to YYYY-MM-DD")

    base_csv = (
        Path(args.base_csv).expanduser()
        if str(args.base_csv).strip()
        else Path(f"backend/mlb/data/processed/mlb_uploads/{target_date}/05_book_upload_base.csv")
    )
    weighted_csv = (
        Path(args.weighted_csv).expanduser()
        if str(args.weighted_csv).strip()
        else Path(f"backend/mlb/data/processed/mlb_uploads/{target_date}/05_book_upload_weighted.csv")
    )
    out_dir = (
        Path(args.out_dir).expanduser()
        if str(args.out_dir).strip()
        else Path(f"artifacts/analysis/mlb/upload_variant_compare/{target_date}")
    )
    graded_rows_csv = Path(args.graded_rows_csv).expanduser()

    if not weighted_csv.exists():
        raise FileNotFoundError(
            f"weighted upload CSV not found: {weighted_csv}. Build weighted variant first (for example make mlb-book-upload-variants MLB_DATE={target_date})."
        )

    base_upload = _load_upload_csv(base_csv, variant="base", expected_date=target_date)
    weighted_upload = _load_upload_csv(weighted_csv, variant="weighted", expected_date=target_date)

    grading_df, grading_meta = _load_grading_rows(graded_rows_csv, target_date=target_date)

    base_eval = _apply_outcomes(base_upload, grading_df)
    weighted_eval = _apply_outcomes(weighted_upload, grading_df)

    base_summary = _variant_summary(base_eval, variant="base")
    weighted_summary = _variant_summary(weighted_eval, variant="weighted")

    # Shared/unique keys
    base_keys = base_eval[SHARED_KEY_COLS].drop_duplicates().copy()
    weighted_keys = weighted_eval[SHARED_KEY_COLS].drop_duplicates().copy()
    only_base_keys = base_keys.merge(weighted_keys, on=SHARED_KEY_COLS, how="left", indicator=True)
    only_base_keys = only_base_keys[only_base_keys["_merge"] == "left_only"][SHARED_KEY_COLS]
    only_weighted_keys = weighted_keys.merge(base_keys, on=SHARED_KEY_COLS, how="left", indicator=True)
    only_weighted_keys = only_weighted_keys[only_weighted_keys["_merge"] == "left_only"][SHARED_KEY_COLS]

    only_base = base_eval.merge(only_base_keys, on=SHARED_KEY_COLS, how="inner")
    only_weighted = weighted_eval.merge(only_weighted_keys, on=SHARED_KEY_COLS, how="inner")

    shared_diff, shared_meta = _shared_comparison(base_eval, weighted_eval)

    by_prop = pd.concat(
        [
            _group_metrics(base_eval, ["prop_type"]).assign(variant="base"),
            _group_metrics(weighted_eval, ["prop_type"]).assign(variant="weighted"),
        ],
        ignore_index=True,
    )
    by_side = pd.concat(
        [
            _group_metrics(base_eval, ["key_side"]).rename(columns={"key_side": "side"}).assign(variant="base"),
            _group_metrics(weighted_eval, ["key_side"]).rename(columns={"key_side": "side"}).assign(variant="weighted"),
        ],
        ignore_index=True,
    )
    by_market = pd.concat(
        [
            _group_metrics(base_eval, ["key_market"]).rename(columns={"key_market": "market"}).assign(variant="base"),
            _group_metrics(weighted_eval, ["key_market"]).rename(columns={"key_market": "market"}).assign(variant="weighted"),
        ],
        ignore_index=True,
    )
    by_line = pd.concat(
        [
            _group_metrics(base_eval, ["key_point"]).rename(columns={"key_point": "line"}).assign(variant="base"),
            _group_metrics(weighted_eval, ["key_point"]).rename(columns={"key_point": "line"}).assign(variant="weighted"),
        ],
        ignore_index=True,
    )

    prop_delta = _top_prop_delta(by_prop)
    hits_compare = _hits_compare(by_prop)
    recommendation = _build_recommendation(base=base_summary, weighted=weighted_summary, shared_meta=shared_meta)

    out_dir.mkdir(parents=True, exist_ok=True)
    base_rows_path = out_dir / "base_graded_rows.csv"
    weighted_rows_path = out_dir / "weighted_graded_rows.csv"
    shared_diff_path = out_dir / "shared_row_diff.csv"
    only_base_path = out_dir / "only_base_graded.csv"
    only_weighted_path = out_dir / "only_weighted_graded.csv"
    by_prop_path = out_dir / "by_prop.csv"
    by_side_path = out_dir / "by_side.csv"
    summary_json_path = out_dir / "summary.json"
    summary_md_path = out_dir / "summary.md"

    base_eval.to_csv(base_rows_path, index=False)
    weighted_eval.to_csv(weighted_rows_path, index=False)

    # Keep all shared rows, sorted by absolute win-input difference.
    if not shared_diff.empty:
        shared_diff = shared_diff.sort_values(by="abs_win_input_diff", ascending=False, kind="mergesort")
    shared_diff.to_csv(shared_diff_path, index=False)

    only_base[only_base["is_graded"]].to_csv(only_base_path, index=False)
    only_weighted[only_weighted["is_graded"]].to_csv(only_weighted_path, index=False)
    by_prop.to_csv(by_prop_path, index=False)
    by_side.to_csv(by_side_path, index=False)

    summary_payload: Dict[str, Any] = {
        "date": target_date,
        "inputs": {
            "base_csv": str(base_csv),
            "weighted_csv": str(weighted_csv),
            "graded_rows_csv": str(graded_rows_csv),
        },
        "grading": grading_meta,
        "overall": {
            "base": base_summary.__dict__,
            "weighted": weighted_summary.__dict__,
            "which_variant_had_better_overall_win_rate": (
                "weighted"
                if (weighted_summary.win_rate_ex_push or -1.0) > (base_summary.win_rate_ex_push or -1.0)
                else "base"
                if (weighted_summary.win_rate_ex_push or -1.0) < (base_summary.win_rate_ex_push or -1.0)
                else "tie"
            ),
            "which_variant_had_better_graded_coverage": (
                "weighted"
                if weighted_summary.graded_rows > base_summary.graded_rows
                else "base"
                if weighted_summary.graded_rows < base_summary.graded_rows
                else "tie"
            ),
        },
        "shared": shared_meta,
        "unique_rows": {
            "only_base_total": int(len(only_base)),
            "only_base_graded": int(only_base["is_graded"].sum()),
            "only_base_win_rate_ex_push": (
                float((only_base["is_win"].sum() / max(int(only_base["is_win"].sum() + only_base["is_loss"].sum()), 1)))
                if int(only_base["is_win"].sum() + only_base["is_loss"].sum()) > 0
                else None
            ),
            "only_weighted_total": int(len(only_weighted)),
            "only_weighted_graded": int(only_weighted["is_graded"].sum()),
            "only_weighted_win_rate_ex_push": (
                float(
                    only_weighted["is_win"].sum()
                    / max(int(only_weighted["is_win"].sum() + only_weighted["is_loss"].sum()), 1)
                )
                if int(only_weighted["is_win"].sum() + only_weighted["is_loss"].sum()) > 0
                else None
            ),
        },
        "prop_delta_pp": prop_delta,
        "hits_compare": hits_compare,
        "market_breakdown": by_market.to_dict(orient="records"),
        "line_breakdown": by_line.to_dict(orient="records"),
        "recommendation": recommendation,
        "outputs": {
            "summary_json": str(summary_json_path),
            "summary_md": str(summary_md_path),
            "base_graded_rows_csv": str(base_rows_path),
            "weighted_graded_rows_csv": str(weighted_rows_path),
            "shared_row_diff_csv": str(shared_diff_path),
            "only_base_graded_csv": str(only_base_path),
            "only_weighted_graded_csv": str(only_weighted_path),
            "by_prop_csv": str(by_prop_path),
            "by_side_csv": str(by_side_path),
        },
    }

    summary_json_path.write_text(json.dumps(summary_payload, indent=2, default=_json_default), encoding="utf-8")
    _write_summary_md(
        out_path=summary_md_path,
        target_date=target_date,
        base=base_summary,
        weighted=weighted_summary,
        shared_meta=shared_meta,
        grading_meta=grading_meta,
        prop_delta=prop_delta,
        hits_compare=hits_compare,
        recommendation=recommendation,
    )

    print(f"[compare-upload-variants] date={target_date}")
    print(f"[compare-upload-variants] base_csv={base_csv} rows={len(base_eval)}")
    print(f"[compare-upload-variants] weighted_csv={weighted_csv} rows={len(weighted_eval)}")
    print(
        "[compare-upload-variants] graded_rows "
        f"base={base_summary.graded_rows}/{base_summary.total_rows} "
        f"weighted={weighted_summary.graded_rows}/{weighted_summary.total_rows}"
    )
    print(f"[compare-upload-variants] out_dir={out_dir}")
    if grading_meta.get("grading_note"):
        print(f"[compare-upload-variants] note={grading_meta.get('grading_note')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
