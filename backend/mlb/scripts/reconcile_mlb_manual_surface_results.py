#!/usr/bin/env python3
"""
Reconcile manual MLB broad-surface upload rows against graded results.

This script is analysis/reconciliation only:
- does not change model logic
- does not change export scripts
- does not touch LaunchAgent
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from backend.mlb.scripts import export_mlb_book_upload as ex
from backend.mlb.scripts import report_mlb_graded_wagers as rgw


POTENTIAL_JOIN_KEYS: List[List[str]] = [
    ["date_norm", "home_norm", "away_norm", "market_norm", "selector_norm", "point_norm", "side_norm"],
    ["date_norm", "home_norm", "away_norm", "market_norm", "point_norm", "side_norm"],
    ["date_norm", "market_norm", "selector_norm", "point_norm", "side_norm"],
    ["date_norm", "market_norm", "point_norm", "side_norm"],
    ["market_norm", "selector_norm", "point_norm", "side_norm"],
    ["market_norm", "point_norm", "side_norm"],
]


def _norm_text(v: Any) -> str:
    return str(v if v is not None else "").strip()


def _normalize_date_value(v: Any) -> str:
    text = _norm_text(v)
    if not text:
        return ""
    if text.isdigit() and len(text) == 8:
        dt = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _normalize_market_value(v: Any) -> str:
    return _norm_text(v).lower()


def _normalize_side_value(v: Any) -> str:
    raw = _norm_text(v).lower()
    if raw in {"over", "o"}:
        return "over"
    if raw in {"under", "u"}:
        return "under"
    return raw


def _normalize_grade_value(v: Any) -> str:
    return rgw._norm_grade(v)


def _resolve_col(df: pd.DataFrame, names: Sequence[str]) -> Optional[str]:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n in df.columns:
            return n
        key = str(n).strip().lower()
        if key in lower:
            return str(lower[key])
    return None


def _series_or_blank(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if col is None:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[col]


def _market_to_prop_map() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for prop_type, market in (ex.DEFAULT_MARKET_BY_PROP or {}).items():
        p = ex._canonical_prop_type(prop_type)
        m = _normalize_market_value(market)
        if p and m:
            out[m] = p
    for raw_alias, canonical_market in (ex.UPLOAD_MARKET_ALIASES or {}).items():
        alias_key = _normalize_market_value(raw_alias)
        canon_key = _normalize_market_value(canonical_market)
        prop = out.get(canon_key, "")
        if prop and alias_key:
            out[alias_key] = prop
    return out


def _prop_to_market_map() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for prop_type, market in (ex.DEFAULT_MARKET_BY_PROP or {}).items():
        p = ex._canonical_prop_type(prop_type)
        m = _normalize_market_value(market)
        if p and m:
            out[p] = m
    return out


def _parse_bet_market_fields(raw: pd.DataFrame) -> pd.DataFrame:
    bet_col = _resolve_col(raw, ["bet", "Bet"])
    market_col = _resolve_col(raw, ["market", "Market", "MARKET"])

    bet_series = _series_or_blank(raw, bet_col).map(_norm_text)
    market_series = _series_or_blank(raw, market_col).map(_norm_text)

    parsed = pd.DataFrame(index=raw.index)
    vals = [rgw._extract_from_bet_and_market(b, m) for b, m in zip(bet_series.tolist(), market_series.tolist())]
    parsed["player_name_parsed"] = [v[0] for v in vals]
    parsed["prop_label_parsed"] = [v[1] for v in vals]
    parsed["side_parsed"] = [v[2] for v in vals]
    parsed["line_parsed"] = [v[3] for v in vals]
    parsed["market_text"] = market_series
    return parsed


def _normalize_upload(upload_raw: pd.DataFrame) -> pd.DataFrame:
    required = ["DATE", "MARKET", "POINT", "SIDE"]
    missing = [c for c in required if c not in upload_raw.columns]
    if missing:
        raise RuntimeError(f"uploaded csv missing required columns: {missing}")

    out = upload_raw.copy()
    out["upload_row_id"] = np.arange(len(out), dtype=int)
    out["date_norm"] = out["DATE"].map(_normalize_date_value)
    out["home_norm"] = _series_or_blank(out, _resolve_col(out, ["HOME"])).map(lambda v: _norm_text(v).upper())
    out["away_norm"] = _series_or_blank(out, _resolve_col(out, ["AWAY"])).map(lambda v: _norm_text(v).upper())
    out["market_norm"] = out["MARKET"].map(_normalize_market_value)
    out["selector_norm"] = pd.to_numeric(
        _series_or_blank(out, _resolve_col(out, ["SELECTOR", "selector", "player_id"])),
        errors="coerce",
    )
    out["point_norm"] = pd.to_numeric(out["POINT"], errors="coerce").round(4)
    out["side_norm"] = out["SIDE"].map(_normalize_side_value)
    out["upload_win_pct"] = pd.to_numeric(_series_or_blank(out, _resolve_col(out, ["WIN %"])), errors="coerce")
    # Preserve canonical upload fields for downstream analysis even when raw headers vary.
    out["upload_side"] = out["side_norm"]
    out["upload_line"] = out["point_norm"]
    out["upload_price_american"] = out["upload_win_pct"]

    market_to_prop = _market_to_prop_map()
    out["upload_prop_type"] = out["market_norm"].map(lambda m: market_to_prop.get(str(m), ""))
    return out


def _normalize_graded(graded_raw: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    out = graded_raw.copy()
    out["graded_row_id"] = np.arange(len(out), dtype=int)

    colmap: Dict[str, str] = {
        "date": _resolve_col(out, ["DATE", "report_date", "event_date", "Event Date"]),
        "home": _resolve_col(out, ["HOME", "Home", "home", "home_team"]),
        "away": _resolve_col(out, ["AWAY", "Away", "away", "away_team"]),
        "market_key": _resolve_col(out, ["MARKET", "market_key"]),
        "market_text": _resolve_col(out, ["market", "Market"]),
        "selector": _resolve_col(out, ["SELECTOR", "selector", "player_id", "Player ID"]),
        "point": _resolve_col(out, ["POINT", "point", "line", "Line"]),
        "side": _resolve_col(out, ["SIDE", "side"]),
        "grade": _resolve_col(out, ["grade", "Grade", "result", "Result", "outcome", "Outcome"]),
        "roi_1u": _resolve_col(out, ["roi_1u", "ROI_1U", "profit_units", "pnl_1u", "profit_1u"]),
        "pnl": _resolve_col(out, ["pnl", "$ W/L", "Profit", "profit"]),
        "amount": _resolve_col(out, ["amount", "Amount", "Stake", "stake"]),
        "prop_type": _resolve_col(out, ["prop_type", "propType"]),
        "bet": _resolve_col(out, ["bet", "Bet"]),
        "price_american": _resolve_col(
            out,
            ["price_over_american", "american_odds", "odds_american", "odds", "price", "WIN %", "win_pct"],
        ),
    }

    parsed = _parse_bet_market_fields(out)

    side_base = _series_or_blank(out, colmap["side"]).map(_normalize_side_value)
    side_parsed = parsed["side_parsed"].map(_normalize_side_value)
    out["side_norm"] = np.where(side_base.astype(str).str.len() > 0, side_base, side_parsed)

    point_base = pd.to_numeric(_series_or_blank(out, colmap["point"]), errors="coerce")
    point_parsed = pd.to_numeric(parsed["line_parsed"], errors="coerce")
    out["point_norm"] = point_base.where(point_base.notna(), point_parsed).round(4)
    # Preserve canonical graded fields for downstream analysis when raw tool schemas differ.
    out["graded_side"] = out["side_norm"]
    out["graded_line"] = out["point_norm"]
    out["graded_price_american"] = pd.to_numeric(_series_or_blank(out, colmap["price_american"]), errors="coerce")

    out["date_norm"] = _series_or_blank(out, colmap["date"]).map(_normalize_date_value)
    out["home_norm"] = _series_or_blank(out, colmap["home"]).map(lambda v: _norm_text(v).upper())
    out["away_norm"] = _series_or_blank(out, colmap["away"]).map(lambda v: _norm_text(v).upper())
    out["selector_norm"] = pd.to_numeric(_series_or_blank(out, colmap["selector"]), errors="coerce")

    prop_base = _series_or_blank(out, colmap["prop_type"]).map(ex._canonical_prop_type)
    prop_parsed = [
        rgw._infer_prop_type(_norm_text(prop_label), _norm_text(market_text))
        for prop_label, market_text in zip(parsed["prop_label_parsed"], parsed["market_text"])
    ]
    prop_parsed = pd.Series([ex._canonical_prop_type(x) for x in prop_parsed], index=out.index)
    out["graded_prop_type"] = np.where(prop_base.astype(str).str.len() > 0, prop_base, prop_parsed)

    raw_market_key = _series_or_blank(out, colmap["market_key"]).map(_normalize_market_value)
    prop_to_market = _prop_to_market_map()

    def _pick_market_key(raw_market: str, prop_type: str) -> str:
        r = _normalize_market_value(raw_market)
        if r in ex.ALLOWED_UPLOAD_MARKETS:
            return r
        if r in ex.UPLOAD_MARKET_ALIASES:
            return _normalize_market_value(ex.UPLOAD_MARKET_ALIASES.get(r))
        p = ex._canonical_prop_type(prop_type)
        mapped = _normalize_market_value(prop_to_market.get(p, ""))
        if mapped:
            return mapped
        return ""

    out["market_norm"] = [
        _pick_market_key(raw_m, prop)
        for raw_m, prop in zip(raw_market_key.tolist(), out["graded_prop_type"].tolist())
    ]

    out["grade_norm"] = _series_or_blank(out, colmap["grade"]).map(_normalize_grade_value)
    out["is_win"] = out["grade_norm"].eq("win").astype(int)
    out["is_loss"] = out["grade_norm"].eq("loss").astype(int)
    out["is_push"] = out["grade_norm"].eq("push").astype(int)

    roi_1u = pd.to_numeric(_series_or_blank(out, colmap["roi_1u"]), errors="coerce")
    pnl = pd.to_numeric(_series_or_blank(out, colmap["pnl"]), errors="coerce")
    amt = pd.to_numeric(_series_or_blank(out, colmap["amount"]), errors="coerce")
    derived = pnl / amt.where(amt > 0)
    out["profit_units"] = roi_1u.where(roi_1u.notna(), derived)
    out.loc[out["grade_norm"].eq("push") & out["profit_units"].isna(), "profit_units"] = 0.0

    out["player_name_parsed"] = parsed["player_name_parsed"].map(_norm_text)
    out["prop_label_parsed"] = parsed["prop_label_parsed"].map(_norm_text)
    out["market_text_parsed"] = parsed["market_text"].map(_norm_text)

    for c in graded_raw.columns:
        out[f"graded__{c}"] = graded_raw[c]

    return out, colmap


def _complete_key_mask(df: pd.DataFrame, keys: Sequence[str]) -> pd.Series:
    m = pd.Series(True, index=df.index)
    for c in keys:
        s = df[c]
        m = m & s.notna()
        if str(s.dtype) == "object":
            m = m & s.astype(str).str.strip().ne("")
    return m


def _evaluate_key(upload: pd.DataFrame, graded: pd.DataFrame, keys: Sequence[str]) -> Dict[str, Any]:
    u_mask = _complete_key_mask(upload, keys)
    g_mask = _complete_key_mask(graded, keys)
    u = upload.loc[u_mask, ["upload_row_id", *keys]].copy()
    g = graded.loc[g_mask, ["graded_row_id", *keys]].copy()

    u_dup = u.duplicated(subset=list(keys), keep=False)
    g_dup = g.duplicated(subset=list(keys), keep=False)
    u_dup_keys = u.loc[u_dup, list(keys)].drop_duplicates()
    g_dup_keys = g.loc[g_dup, list(keys)].drop_duplicates()

    overlap_dup_keys = pd.merge(u_dup_keys, g_dup_keys, on=list(keys), how="inner")
    inner = pd.merge(u, g, on=list(keys), how="inner")

    return {
        "keys": list(keys),
        "u_complete_rows": int(len(u)),
        "g_complete_rows": int(len(g)),
        "u_duplicate_rows": int(u_dup.sum()),
        "g_duplicate_rows": int(g_dup.sum()),
        "u_duplicate_key_count": int(len(u_dup_keys)),
        "g_duplicate_key_count": int(len(g_dup_keys)),
        "overlap_many_to_many_key_count": int(len(overlap_dup_keys)),
        "matched_pair_rows": int(len(inner)),
        "matched_upload_rows": int(inner["upload_row_id"].nunique()),
        "matched_graded_rows": int(inner["graded_row_id"].nunique()),
        "u_dup_keys_sample": u_dup_keys.head(10),
        "g_dup_keys_sample": g_dup_keys.head(10),
        "overlap_dup_keys_sample": overlap_dup_keys.head(10),
    }


def _choose_join_key(upload: pd.DataFrame, graded: pd.DataFrame) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []
    for keys in POTENTIAL_JOIN_KEYS:
        if not all(k in upload.columns and k in graded.columns for k in keys):
            continue
        if not all(upload[k].notna().any() for k in keys):
            continue
        if not all(graded[k].notna().any() for k in keys):
            continue
        candidates.append(_evaluate_key(upload, graded, keys))

    if not candidates:
        raise RuntimeError("no viable join key candidates found from shared normalized fields")

    # Prefer: no many-to-many overlap, more key columns, more matched upload rows, fewer duplicate rows.
    ranked = sorted(
        candidates,
        key=lambda s: (
            int(s["overlap_many_to_many_key_count"] > 0),
            -int(len(s["keys"])),
            -int(s["matched_upload_rows"]),
            int(s["u_duplicate_rows"] + s["g_duplicate_rows"]),
        ),
    )
    return ranked[0]


def _format_num(v: Optional[float], decimals: int = 4) -> str:
    if v is None:
        return "NA"
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return "NA"
    return f"{float(v):.{decimals}f}"


def _summarize_eval(eval_df: pd.DataFrame) -> Dict[str, Any]:
    graded_mask = eval_df["grade_norm"].isin(["win", "loss", "push"])
    wl_mask = eval_df["grade_norm"].isin(["win", "loss"])
    wins = int(eval_df["grade_norm"].eq("win").sum())
    losses = int(eval_df["grade_norm"].eq("loss").sum())
    n_bets_graded = int(graded_mask.sum())
    win_rate = (wins / (wins + losses)) if (wins + losses) > 0 else None

    profit_series = pd.to_numeric(eval_df.loc[graded_mask, "profit_units"], errors="coerce")
    have_profit = int(profit_series.notna().sum()) > 0
    roi = float(profit_series.mean()) if have_profit else None
    total_profit_units = float(profit_series.sum()) if have_profit else None

    return {
        "n_bets_graded": n_bets_graded,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "roi": roi,
        "total_profit_units": total_profit_units,
        "have_profit": have_profit,
        "profit_rows": int(profit_series.notna().sum()),
    }


def _group_breakdown(eval_df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if group_col not in eval_df.columns:
        return pd.DataFrame(columns=[group_col, "n_bets", "win_rate", "roi"])

    work = eval_df[eval_df["grade_norm"].isin(["win", "loss", "push"])].copy()
    work[group_col] = work[group_col].map(_norm_text)
    work = work[work[group_col].str.len() > 0]
    if work.empty:
        return pd.DataFrame(columns=[group_col, "n_bets", "win_rate", "roi"])

    rows: List[Dict[str, Any]] = []
    for key, grp in work.groupby(group_col, dropna=False):
        wins = int(grp["grade_norm"].eq("win").sum())
        losses = int(grp["grade_norm"].eq("loss").sum())
        n_bets = int(len(grp))
        win_rate = (wins / (wins + losses)) if (wins + losses) > 0 else None
        p = pd.to_numeric(grp["profit_units"], errors="coerce")
        roi = float(p.mean()) if p.notna().any() else None
        rows.append(
            {
                group_col: key,
                "n_bets": n_bets,
                "win_rate": win_rate,
                "roi": roi,
            }
        )
    out = pd.DataFrame(rows)
    out = out.sort_values(["n_bets", group_col], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    return out


def _bucket_win_pct(eval_df: pd.DataFrame) -> pd.DataFrame:
    if "WIN %" not in eval_df.columns:
        return pd.DataFrame(columns=["win_pct_bucket", "n_bets", "win_rate", "roi"])
    work = eval_df[eval_df["grade_norm"].isin(["win", "loss", "push"])].copy()
    work["win_pct_num"] = pd.to_numeric(work["WIN %"], errors="coerce")
    work = work[work["win_pct_num"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=["win_pct_bucket", "n_bets", "win_rate", "roi"])

    n_unique = int(work["win_pct_num"].nunique())
    if n_unique < 4:
        return pd.DataFrame(columns=["win_pct_bucket", "n_bets", "win_rate", "roi"])

    q = min(8, n_unique)
    work["win_pct_bucket"] = pd.qcut(work["win_pct_num"], q=q, duplicates="drop")
    rows: List[Dict[str, Any]] = []
    for b, grp in work.groupby("win_pct_bucket", dropna=False):
        wins = int(grp["grade_norm"].eq("win").sum())
        losses = int(grp["grade_norm"].eq("loss").sum())
        n_bets = int(len(grp))
        win_rate = (wins / (wins + losses)) if (wins + losses) > 0 else None
        p = pd.to_numeric(grp["profit_units"], errors="coerce")
        roi = float(p.mean()) if p.notna().any() else None
        rows.append({"win_pct_bucket": str(b), "n_bets": n_bets, "win_rate": win_rate, "roi": roi})
    out = pd.DataFrame(rows)
    out = out.sort_values(["n_bets", "win_pct_bucket"], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    return out


def _print_table(title: str, df: pd.DataFrame, max_rows: int = 12) -> None:
    print(title)
    if df.empty:
        print("  (none)")
        return
    show = df.head(max_rows).copy()
    if "win_rate" in show.columns:
        show["win_rate"] = show["win_rate"].map(lambda x: None if pd.isna(x) else round(float(x), 4))
    if "roi" in show.columns:
        show["roi"] = show["roi"].map(lambda x: None if pd.isna(x) else round(float(x), 4))
    print(show.to_string(index=False))


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconcile manual MLB surface upload against graded results.")
    ap.add_argument("--uploaded-csv", required=True, help="Path to uploaded manual-surface CSV.")
    ap.add_argument("--graded-csv", required=True, help="Path to graded CSV (raw tool export or normalized rows).")
    ap.add_argument("--out-csv", required=True, help="Path to reconciliation output CSV.")
    ap.add_argument("--slate-date", default="", help="Optional reporting label (YYYY-MM-DD).")
    args = ap.parse_args()

    uploaded_csv = Path(str(args.uploaded_csv)).expanduser()
    graded_csv = Path(str(args.graded_csv)).expanduser()
    out_csv = Path(str(args.out_csv)).expanduser()

    if not uploaded_csv.exists():
        raise FileNotFoundError(f"missing --uploaded-csv: {uploaded_csv}")
    if not graded_csv.exists():
        raise FileNotFoundError(f"missing --graded-csv: {graded_csv}")

    upload_raw = pd.read_csv(uploaded_csv, low_memory=False)
    graded_raw = pd.read_csv(graded_csv, low_memory=False)

    print("[reconcile-manual-surface] loaded files")
    print(f"- uploaded_csv={uploaded_csv}")
    print(f"- graded_csv={graded_csv}")
    if args.slate_date:
        print(f"- slate_date={args.slate_date}")
    print(f"- uploaded_rows={len(upload_raw)} uploaded_cols={len(upload_raw.columns)}")
    print(f"- graded_rows={len(graded_raw)} graded_cols={len(graded_raw.columns)}")

    upload = _normalize_upload(upload_raw)
    graded, graded_colmap = _normalize_graded(graded_raw)

    best_key = _choose_join_key(upload, graded)
    key_cols = list(best_key["keys"])

    print("[reconcile-manual-surface] selected join key")
    print(f"- keys={key_cols}")
    print(f"- matched_upload_rows={best_key['matched_upload_rows']}")
    print(f"- matched_graded_rows={best_key['matched_graded_rows']}")
    print(f"- upload_duplicate_rows_on_key={best_key['u_duplicate_rows']}")
    print(f"- graded_duplicate_rows_on_key={best_key['g_duplicate_rows']}")
    print(f"- overlap_many_to_many_key_count={best_key['overlap_many_to_many_key_count']}")

    if int(best_key["u_duplicate_rows"]) > 0:
        print("[reconcile-manual-surface] upload duplicate key sample:")
        if isinstance(best_key["u_dup_keys_sample"], pd.DataFrame) and not best_key["u_dup_keys_sample"].empty:
            print(best_key["u_dup_keys_sample"].to_string(index=False))
        else:
            print("  (none)")
    if int(best_key["g_duplicate_rows"]) > 0:
        print("[reconcile-manual-surface] graded duplicate key sample:")
        if isinstance(best_key["g_dup_keys_sample"], pd.DataFrame) and not best_key["g_dup_keys_sample"].empty:
            print(best_key["g_dup_keys_sample"].to_string(index=False))
        else:
            print("  (none)")

    if int(best_key["overlap_many_to_many_key_count"]) > 0:
        print("[reconcile-manual-surface] ERROR: many-to-many join risk on selected key (overlapping duplicate keys).")
        sample = best_key.get("overlap_dup_keys_sample")
        if isinstance(sample, pd.DataFrame) and not sample.empty:
            print(sample.to_string(index=False))
        raise RuntimeError("ambiguous many-to-many join detected; aborting")

    graded_pref_cols = [c for c in graded.columns if str(c).startswith("graded__")]
    graded_keep_cols = [
        "graded_row_id",
        *key_cols,
        "grade_norm",
        "is_win",
        "is_loss",
        "is_push",
        "profit_units",
        "graded_prop_type",
        "graded_side",
        "graded_line",
        "graded_price_american",
        "side_norm",
        "point_norm",
        *graded_pref_cols,
    ]
    seen_cols: set[str] = set()
    dedup_cols: List[str] = []
    for c in graded_keep_cols:
        if c in graded.columns and c not in seen_cols:
            dedup_cols.append(c)
            seen_cols.add(c)
    graded_keep_cols = dedup_cols

    merged = pd.merge(
        upload,
        graded[graded_keep_cols],
        on=key_cols,
        how="outer",
        indicator=True,
        suffixes=("", "_graded"),
        sort=False,
    )
    merged["join_status"] = merged["_merge"].map({"left_only": "unmatched_uploaded", "right_only": "unmatched_graded", "both": "matched"})

    matched = merged[merged["_merge"].eq("both")].copy()
    matched_upload_unique = int(pd.to_numeric(matched["upload_row_id"], errors="coerce").dropna().nunique())
    matched_graded_unique = int(pd.to_numeric(matched["graded_row_id"], errors="coerce").dropna().nunique())
    unmatched_uploaded = int(len(upload_raw) - matched_upload_unique)
    unmatched_graded = int(len(graded_raw) - matched_graded_unique)

    # One upload row should ideally map to one graded row; keep first for summary metrics.
    eval_df = (
        matched.sort_values(["upload_row_id", "graded_row_id"], kind="mergesort")
        .drop_duplicates(subset=["upload_row_id"], keep="first")
        .copy()
    )
    if "SIDE" not in eval_df.columns and "side_norm" in eval_df.columns:
        eval_df["SIDE"] = eval_df["side_norm"]

    ambiguity_upload = (
        matched.groupby("upload_row_id", dropna=False)["graded_row_id"].nunique().gt(1).sum() if not matched.empty else 0
    )
    ambiguity_graded = (
        matched.groupby("graded_row_id", dropna=False)["upload_row_id"].nunique().gt(1).sum() if not matched.empty else 0
    )

    summary = _summarize_eval(eval_df)
    by_market = _group_breakdown(eval_df, "MARKET")
    by_side = _group_breakdown(eval_df, "SIDE")
    by_prop = _group_breakdown(
        eval_df.assign(prop_type_group=eval_df["upload_prop_type"].where(eval_df["upload_prop_type"].astype(str).str.len() > 0, eval_df["graded_prop_type"])),
        "prop_type_group",
    )
    by_winpct_bucket = _bucket_win_pct(eval_df)

    # Build output CSV with uploaded columns + prefixed graded columns + derived reconciliation fields.
    upload_cols = list(upload_raw.columns)
    derived_cols = [
        "join_status",
        "grade_norm",
        "is_win",
        "is_loss",
        "is_push",
        "profit_units",
        "upload_side",
        "upload_line",
        "upload_price_american",
        "graded_prop_type",
        "graded_side",
        "graded_line",
        "graded_price_american",
        "upload_prop_type",
    ]
    keep_cols = [c for c in [*upload_cols, *graded_pref_cols, *derived_cols] if c in merged.columns]
    out_df = merged[keep_cols].copy()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv, index=False)

    match_rate = (matched_upload_unique / max(1, len(upload_raw)))
    if match_rate < 0.25:
        print(
            "[reconcile-manual-surface] WARNING: suspiciously low match rate "
            f"matched_uploaded={matched_upload_unique}/{len(upload_raw)} ({match_rate:.1%})"
        )
    if int(ambiguity_upload) > 0 or int(ambiguity_graded) > 0:
        print(
            "[reconcile-manual-surface] WARNING: non-1:1 mapping detected "
            f"(upload_rows_with_multi_graded={int(ambiguity_upload)}, "
            f"graded_rows_with_multi_upload={int(ambiguity_graded)})"
        )

    if summary["n_bets_graded"] > 0 and not summary["have_profit"]:
        print(
            "[reconcile-manual-surface] WARNING: profit_units could not be derived from graded file "
            "(missing usable roi/pnl+amount fields)."
        )

    print("")
    print("RUN INFO")
    print(f"- uploaded rows: {len(upload_raw)}")
    print(f"- graded rows: {len(graded_raw)}")
    print(f"- matched rows: {int(len(matched))}")
    print(f"- unmatched uploaded rows: {unmatched_uploaded}")
    print(f"- unmatched graded rows: {unmatched_graded}")
    print(f"- output path: {out_csv}")

    print("")
    print("RESULTS")
    print(f"- n_bets graded: {summary['n_bets_graded']}")
    print(f"- win_rate: {_format_num(summary['win_rate'])}")
    print(f"- roi: {_format_num(summary['roi'])}")
    print(f"- total_profit_units: {_format_num(summary['total_profit_units'])}")

    print("")
    _print_table("BREAKDOWN BY MARKET", by_market, max_rows=15)

    if not by_prop.empty:
        print("")
        _print_table("BREAKDOWN BY PROP TYPE", by_prop, max_rows=15)

    print("")
    _print_table("BREAKDOWN BY SIDE", by_side, max_rows=10)

    if not by_winpct_bucket.empty:
        print("")
        _print_table("OPTIONAL WIN % BUCKETS", by_winpct_bucket, max_rows=12)

    print("")
    print("PASTE FOR CHATGPT")
    print(f"uploaded rows: {len(upload_raw)}")
    print(f"graded rows: {len(graded_raw)}")
    print(f"matched rows: {int(len(matched))}")
    print(f"win_rate: {_format_num(summary['win_rate'])}")
    print(f"roi: {_format_num(summary['roi'])}")
    print(f"total_profit_units: {_format_num(summary['total_profit_units'])}")
    print("MARKET breakdown:")
    if by_market.empty:
        print("(none)")
    else:
        print(by_market.head(15).to_string(index=False))
    print("SIDE breakdown:")
    if by_side.empty:
        print("(none)")
    else:
        print(by_side.head(10).to_string(index=False))
    print(f"output path: {out_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
