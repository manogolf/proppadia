#!/usr/bin/env python3
"""
Replay/evaluate MLB manual-surface opportunities from archived odds snapshots.

Scope:
- analysis utility only (no model/export/automation changes)
- strict per-snapshot matching (no cross-snapshot odds merging)
- over-side replay with edge-tier diagnostics
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from backend.mlb.scripts import export_mlb_book_upload as ex
from backend.mlb.scripts import export_mlb_book_upload_manual_surface as manual_surface
from backend.mlb.scripts.build_mlb_reconcile_rows import (
    _build_market_index,
    _build_team_name_reverse,
    _line_key,
    _load_events,
    _norm_name,
)
from backend.shared.db.pg import pg_fetchall


EDGE_BUCKETS: List[Tuple[float, float, str]] = [
    (0.05, 0.10, "0.05-0.10"),
    (0.10, 0.15, "0.10-0.15"),
    (0.15, float("inf"), "0.15+"),
]


@dataclass
class DayRollup:
    game_date: str
    n_bets: int
    win_rate: Optional[float]
    roi: Optional[float]
    total_profit_units: float


def _parse_date(s: str) -> date:
    return datetime.strptime(str(s), "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> List[date]:
    out: List[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _safe_float(v: object) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        return x
    except Exception:
        return None


def _american_to_implied_prob(price: object) -> Optional[float]:
    p = _safe_float(price)
    if p is None or p == 0:
        return None
    if p > 0:
        return 100.0 / (p + 100.0)
    return abs(p) / (abs(p) + 100.0)


def _profit_per_1u_over(*, won: int, price_over_american: object, push: int) -> Optional[float]:
    if int(push) == 1:
        return 0.0
    if int(won) == 0:
        return -1.0
    p = _safe_float(price_over_american)
    if p is None:
        return None
    if p > 0:
        return p / 100.0
    if p < 0:
        return 100.0 / abs(p)
    return None


def _edge_bucket(edge: float) -> Optional[str]:
    for lo, hi, label in EDGE_BUCKETS:
        if edge >= lo and edge < hi:
            return label
    return None


def _rollup_metrics(df: pd.DataFrame) -> Dict[str, Optional[float] | int]:
    if df.empty:
        return {
            "n_bets": 0,
            "win_rate": None,
            "roi": None,
            "total_profit_units": 0.0,
        }
    n = int(len(df))
    wins = int(df["won"].sum())
    losses = int((df["won"] == 0).sum() - int(df["push"].sum()))
    wl = wins + losses
    win_rate = (wins / wl) if wl > 0 else None
    total_profit = float(pd.to_numeric(df["profit_units"], errors="coerce").fillna(0.0).sum())
    roi = total_profit / float(n) if n > 0 else None
    return {
        "n_bets": n,
        "win_rate": win_rate,
        "roi": roi,
        "total_profit_units": total_profit,
    }


def _pct(v: Optional[float]) -> str:
    if v is None:
        return "NA"
    return f"{100.0 * float(v):.2f}%"


def _pct_num(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    return round(100.0 * float(v), 4)


def _load_player_stats_actuals(
    *,
    from_date: str,
    to_date: str,
    player_ids: Sequence[int],
) -> Dict[Tuple[int, int], Dict[str, Any]]:
    if not player_ids:
        return {}
    sql = """
    SELECT
      player_id::bigint AS player_id,
      game_id::bigint AS game_id,
      game_date::date AS game_date,
      hits,
      total_bases,
      rbis,
      runs_scored,
      strikeouts_batting,
      walks,
      singles,
      doubles,
      triples,
      home_runs,
      stolen_bases,
      strikeouts_pitching,
      walks_allowed,
      hits_allowed,
      outs_recorded,
      earned_runs
    FROM mlb.player_stats
    WHERE game_date::date BETWEEN %s::date AND %s::date
      AND player_id = ANY(%s::bigint[])
    """
    rows = pg_fetchall(sql, (str(from_date), str(to_date), list(sorted(set(int(x) for x in player_ids)))))
    out: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for r in rows or []:
        try:
            key = (int(r.get("game_id")), int(r.get("player_id")))
        except Exception:
            continue
        out[key] = dict(r)
    return out


def _actual_value_for_prop(stats_row: Dict[str, Any], prop_type: str) -> Optional[float]:
    p = ex._canonical_prop_type(prop_type)
    if not stats_row:
        return None

    def _g(k: str) -> Optional[float]:
        return _safe_float(stats_row.get(k))

    if p == "hits_runs_rbis":
        h = _g("hits")
        r = _g("runs_scored")
        rb = _g("rbis")
        if h is None or r is None or rb is None:
            return None
        return float(h + r + rb)
    if p == "runs_rbis":
        r = _g("runs_scored")
        rb = _g("rbis")
        if r is None or rb is None:
            return None
        return float(r + rb)

    direct_cols = {
        "hits": "hits",
        "total_bases": "total_bases",
        "rbis": "rbis",
        "runs_scored": "runs_scored",
        "walks": "walks",
        "strikeouts_batting": "strikeouts_batting",
        "singles": "singles",
        "doubles": "doubles",
        "triples": "triples",
        "home_runs": "home_runs",
        "stolen_bases": "stolen_bases",
        "strikeouts_pitching": "strikeouts_pitching",
        "walks_allowed": "walks_allowed",
        "hits_allowed": "hits_allowed",
        "outs_recorded": "outs_recorded",
        "earned_runs": "earned_runs",
    }
    col = direct_cols.get(p)
    if not col:
        return None
    return _g(col)


def _verify_candidate_rows_match_snapshot(
    *,
    candidate_rows: pd.DataFrame,
    odds_snapshot_json: Path,
) -> Tuple[int, pd.DataFrame]:
    if candidate_rows.empty:
        return 0, pd.DataFrame(columns=list(candidate_rows.columns))

    events = _load_events(odds_snapshot_json)
    market_idx = _build_market_index(events=events, team_name_rev=_build_team_name_reverse())

    mismatches: List[int] = []
    for i, r in candidate_rows.iterrows():
        home = str(r.get("home_team_code") or "").strip().upper()
        away = str(r.get("away_team_code") or "").strip().upper()
        market_key = str(r.get("market_key") or "").strip()
        player_name = str(r.get("player_name") or "").strip()
        line = _line_key(r.get("line"))
        bk = str(r.get("bookmaker_key") or "").strip().lower()
        if not home or not away or not market_key or not player_name or line is None or not bk:
            mismatches.append(int(i))
            continue
        idx_key = (home, away, market_key, _norm_name(player_name), float(line))
        by_book = market_idx.get(idx_key, {})
        if not by_book:
            mismatches.append(int(i))
            continue
        book_row = None
        for bk_key, payload in by_book.items():
            if str(bk_key or "").strip().lower() == bk:
                book_row = payload
                break
        if not isinstance(book_row, dict):
            mismatches.append(int(i))
            continue
        row_over = _safe_float(r.get("price_over_american"))
        row_under = _safe_float(r.get("price_under_american"))
        snap_over = _safe_float(book_row.get("over"))
        snap_under = _safe_float(book_row.get("under"))
        if row_over is not None and snap_over is not None and abs(float(row_over) - float(snap_over)) > 1e-9:
            mismatches.append(int(i))
            continue
        if row_under is not None and snap_under is not None and abs(float(row_under) - float(snap_under)) > 1e-9:
            mismatches.append(int(i))
            continue

    mismatch_df = candidate_rows.loc[mismatches].copy() if mismatches else candidate_rows.iloc[0:0].copy()
    return int(len(mismatches)), mismatch_df


def _per_date_rollup(df: pd.DataFrame) -> List[DayRollup]:
    out: List[DayRollup] = []
    if df.empty:
        return out
    for game_date, g in df.groupby("game_date", dropna=False):
        m = _rollup_metrics(g)
        out.append(
            DayRollup(
                game_date=str(game_date),
                n_bets=int(m["n_bets"]),
                win_rate=(None if m["win_rate"] is None else float(m["win_rate"])),
                roi=(None if m["roi"] is None else float(m["roi"])),
                total_profit_units=float(m["total_profit_units"]),
            )
        )
    out.sort(key=lambda x: x.game_date)
    return out


def _parse_float_csv(raw: str) -> List[float]:
    out: List[float] = []
    for tok in str(raw or "").split(","):
        t = tok.strip()
        if not t:
            continue
        out.append(float(t))
    uniq = sorted(set(out))
    return uniq


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Replay MLB manual-surface over-side historical ROI from saved snapshots.")
    ap.add_argument("--odds-root", default="backend/mlb/exports/odds_history")
    ap.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--slate-filename", default="mlb_slate_output.csv")
    ap.add_argument("--odds-filename", default="odds_mlb_playerprops.json")
    ap.add_argument("--odds-filename-fallback", default="odds_latest_compatible.json")
    ap.add_argument("--market-map-json", default="", help="Optional JSON object prop_type->market_key overrides.")
    ap.add_argument("--prop-types", default="", help="Optional prop_type allowlist CSV.")
    ap.add_argument("--require-two-sided-match", action="store_true", help="Require two-sided at matching stage.")
    ap.add_argument("--line-min-exclusive", type=float, default=0.5, help="Keep rows where line > this value.")
    ap.add_argument("--min-model-prob-over", type=float, default=0.55)
    ap.add_argument("--max-market-prob-over-novig", type=float, default=0.60)
    ap.add_argument("--edge-thresholds", default="0.05,0.10,0.15", help="CSV thresholds for cumulative diagnostics.")
    ap.add_argument("--edge-min", type=float, default=0.05, help="Minimum edge for bet-set inclusion.")
    ap.add_argument("--out-rows-csv", default="tmp/analysis/mlb_manual_surface_replay_rows.csv")
    ap.add_argument("--out-summary-json", default="tmp/analysis/mlb_manual_surface_replay_summary.json")
    ap.add_argument("--strict-duplicates", action="store_true", default=True)
    ap.add_argument("--strict-snapshot-mismatch", action="store_true", default=True)
    ap.add_argument("--max-mismatch-print", type=int, default=20)
    args = ap.parse_args(list(argv) if argv is not None else None)

    start = _parse_date(args.from_date)
    end = _parse_date(args.to_date)
    if end < start:
        raise RuntimeError("--to-date must be >= --from-date")

    odds_root = Path(str(args.odds_root)).expanduser()
    out_rows_csv = Path(str(args.out_rows_csv)).expanduser()
    out_summary_json = Path(str(args.out_summary_json)).expanduser()
    out_rows_csv.parent.mkdir(parents=True, exist_ok=True)
    out_summary_json.parent.mkdir(parents=True, exist_ok=True)

    market_map = ex._load_market_map(
        arg_json=str(args.market_map_json),
        env_json="",
    )
    prop_filter = manual_surface._parse_prop_types(str(args.prop_types or ""))
    edge_thresholds = _parse_float_csv(str(args.edge_thresholds or ""))
    if not edge_thresholds:
        raise RuntimeError("edge-thresholds resolved to empty set")

    days = _date_range(start, end)
    per_day_records: List[Dict[str, Any]] = []
    candidate_frames: List[pd.DataFrame] = []
    skipped_missing_artifacts = 0
    total_candidate_rows_in = 0
    total_matched_candidates = 0
    total_snapshot_mismatches = 0
    mismatch_examples: List[Dict[str, Any]] = []

    for d in days:
        day = d.isoformat()
        day_dir = odds_root / day
        slate_csv = day_dir / str(args.slate_filename)
        odds_json = day_dir / str(args.odds_filename)
        if not odds_json.exists() and str(args.odds_filename_fallback or "").strip():
            fallback = day_dir / str(args.odds_filename_fallback).strip()
            if fallback.exists():
                odds_json = fallback

        if not slate_csv.exists() or not odds_json.exists():
            skipped_missing_artifacts += 1
            continue

        merged = ex._normalize_slate_output(ex._load_slate_output(slate_csv))
        merged["game_date"] = pd.to_datetime(merged["game_date"], errors="coerce").dt.date
        merged = merged[merged["game_date"] == d].copy()
        if prop_filter:
            merged["prop_type"] = merged["prop_type"].map(ex._canonical_prop_type)
            merged = merged[merged["prop_type"].isin(prop_filter)].copy()
        if merged.empty:
            per_day_records.append(
                {
                    "game_date": day,
                    "candidate_rows_in": 0,
                    "matched_candidates": 0,
                    "snapshot_mismatches": 0,
                }
            )
            continue

        candidate_rows_in = int(len(merged))
        total_candidate_rows_in += candidate_rows_in
        cands = manual_surface._build_manual_candidate_rows(
            merged=merged,
            odds_snapshot_json=odds_json,
            market_map=market_map,
            require_two_sided=bool(args.require_two_sided_match),
        )
        matched_candidates = int(len(cands))
        total_matched_candidates += matched_candidates

        if not cands.empty:
            cands["game_date"] = day
            cands["odds_snapshot_file"] = str(odds_json)
            cands["slate_source_file"] = str(slate_csv)

            mismatch_count, mismatch_df = _verify_candidate_rows_match_snapshot(
                candidate_rows=cands,
                odds_snapshot_json=odds_json,
            )
            total_snapshot_mismatches += int(mismatch_count)
            if mismatch_count > 0 and len(mismatch_examples) < int(args.max_mismatch_print):
                keep_cols = [
                    "game_date",
                    "player_id",
                    "game_id",
                    "prop_type",
                    "line",
                    "market_key",
                    "bookmaker_key",
                    "price_over_american",
                    "price_under_american",
                ]
                sample = mismatch_df[keep_cols].head(int(args.max_mismatch_print)).to_dict(orient="records")
                mismatch_examples.extend(sample)

            candidate_frames.append(cands)

        per_day_records.append(
            {
                "game_date": day,
                "candidate_rows_in": candidate_rows_in,
                "matched_candidates": matched_candidates,
                "snapshot_mismatches": int(mismatch_count if matched_candidates > 0 else 0),
            }
        )

    all_candidates = pd.concat(candidate_frames, ignore_index=True) if candidate_frames else pd.DataFrame()

    if bool(args.strict_snapshot_mismatch) and int(total_snapshot_mismatches) > 0:
        raise RuntimeError(
            f"snapshot mismatch detected: count={total_snapshot_mismatches}. "
            f"example_rows={mismatch_examples[: int(args.max_mismatch_print)]}"
        )

    if all_candidates.empty:
        payload = {
            "ok": True,
            "status": "no_candidates",
            "from_date": args.from_date,
            "to_date": args.to_date,
            "requested_days": len(days),
            "skipped_missing_artifacts": int(skipped_missing_artifacts),
            "candidate_rows_in": int(total_candidate_rows_in),
            "matched_candidates": int(total_matched_candidates),
            "rows_written": 0,
            "out_rows_csv": str(out_rows_csv),
            "out_summary_json": str(out_summary_json),
            "per_day": per_day_records,
        }
        pd.DataFrame().to_csv(out_rows_csv, index=False)
        out_summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        print("\nPASTE FOR CHATGPT\nNo matched candidates in range.")
        return 0

    # Duplicate guard
    dedupe_keys = ["player_id", "game_id", "prop_type", "line"]
    dup_mask = all_candidates.duplicated(subset=dedupe_keys, keep=False)
    dup_count = int(dup_mask.sum())
    if bool(args.strict_duplicates) and dup_count > 0:
        dup_rows = all_candidates.loc[dup_mask, ["game_date", *dedupe_keys, "bookmaker_key"]].head(50).to_dict(orient="records")
        raise RuntimeError(f"duplicate keys found count={dup_count} keys={dedupe_keys} sample={dup_rows}")

    # Odds + edge derivation
    all_candidates["model_prob_over"] = pd.to_numeric(all_candidates["model_prob_over"], errors="coerce")
    all_candidates["line"] = pd.to_numeric(all_candidates["line"], errors="coerce")
    all_candidates["price_over_american"] = pd.to_numeric(all_candidates["price_over_american"], errors="coerce")
    all_candidates["price_under_american"] = pd.to_numeric(all_candidates["price_under_american"], errors="coerce")
    all_candidates["p_market_over_raw"] = all_candidates["price_over_american"].map(_american_to_implied_prob)
    all_candidates["p_market_under_raw"] = all_candidates["price_under_american"].map(_american_to_implied_prob)
    denom = all_candidates["p_market_over_raw"] + all_candidates["p_market_under_raw"]
    all_candidates["p_market_over_novig"] = (all_candidates["p_market_over_raw"] / denom).where(denom > 0)
    all_candidates["edge_over_novig"] = all_candidates["model_prob_over"] - all_candidates["p_market_over_novig"]

    # Missing odds drop accounting (for no-vig/edge)
    has_two_sided_prices = all_candidates["price_over_american"].notna() & all_candidates["price_under_american"].notna()
    has_novig = all_candidates["p_market_over_novig"].notna()
    missing_odds_mask = ~(has_two_sided_prices & has_novig)
    dropped_missing_odds = int(missing_odds_mask.sum())
    dropped_missing_odds_pct = (float(dropped_missing_odds) / float(len(all_candidates))) if len(all_candidates) > 0 else 0.0

    work = all_candidates.loc[~missing_odds_mask].copy()
    work = work[
        work["line"].notna()
        & work["model_prob_over"].notna()
        & work["p_market_over_novig"].notna()
        & (work["line"] > float(args.line_min_exclusive))
        & (work["model_prob_over"] >= float(args.min_model_prob_over))
        & (work["p_market_over_novig"] <= float(args.max_market_prob_over_novig))
    ].copy()

    # Grade from player_stats
    player_ids = work["player_id"].dropna().astype(int).unique().tolist() if not work.empty else []
    actuals_by_gp = _load_player_stats_actuals(
        from_date=args.from_date,
        to_date=args.to_date,
        player_ids=player_ids,
    )

    if not work.empty:
        actual_vals: List[Optional[float]] = []
        for _, r in work.iterrows():
            key = (int(r["game_id"]), int(r["player_id"]))
            stats_row = actuals_by_gp.get(key, {})
            actual_vals.append(_actual_value_for_prop(stats_row, str(r["prop_type"])))
        work["actual_value"] = actual_vals
        work = work[work["actual_value"].notna()].copy()

    if not work.empty:
        work["won"] = (work["actual_value"] > work["line"]).astype(int)
        work["push"] = (abs(work["actual_value"] - work["line"]) < 1e-12).astype(int)
        work.loc[work["push"] == 1, "won"] = 0
        work["profit_units"] = [
            _profit_per_1u_over(won=int(w), price_over_american=p, push=int(ps))
            for w, p, ps in zip(work["won"].tolist(), work["price_over_american"].tolist(), work["push"].tolist())
        ]
        work["profit_units"] = pd.to_numeric(work["profit_units"], errors="coerce")
        work = work[work["profit_units"].notna()].copy()

    # Edge bucket + final bet set
    if not work.empty:
        work["edge_bucket"] = [
            _edge_bucket(float(e)) if pd.notna(e) else None for e in pd.to_numeric(work["edge_over_novig"], errors="coerce").tolist()
        ]
    else:
        work["edge_bucket"] = pd.Series(dtype="object")

    bets = work[work["edge_over_novig"] >= float(args.edge_min)].copy() if not work.empty else work.copy()
    bets = bets[bets["edge_bucket"].notna()].copy() if not bets.empty else bets
    bets["side"] = "over"
    for col_name, default_val in (
        ("won", pd.Series(dtype="int64")),
        ("push", pd.Series(dtype="int64")),
        ("profit_units", pd.Series(dtype="float64")),
        ("actual_value", pd.Series(dtype="float64")),
    ):
        if col_name not in bets.columns:
            bets[col_name] = default_val

    # Date rollup
    per_day_bets = _per_date_rollup(bets)

    # Cumulative threshold diagnostics
    threshold_rows: List[Dict[str, Any]] = []
    for th in edge_thresholds:
        sub = bets[bets["edge_over_novig"] >= float(th)].copy()
        m = _rollup_metrics(sub)
        threshold_rows.append(
            {
                "edge_threshold": float(th),
                "n_bets": int(m["n_bets"]),
                "win_rate": _pct_num(m["win_rate"]),  # percent
                "roi": _pct_num(m["roi"]),  # percent
                "total_profit_units": round(float(m["total_profit_units"]), 6),
            }
        )

    # Edge-bucket table (requested)
    edge_bucket_rows: List[Dict[str, Any]] = []
    for _, _, label in EDGE_BUCKETS:
        sub = bets[bets["edge_bucket"] == label].copy()
        m = _rollup_metrics(sub)
        edge_bucket_rows.append(
            {
                "edge_bucket": label,
                "n_bets": int(m["n_bets"]),
                "win_rate": _pct_num(m["win_rate"]),  # percent
                "roi": _pct_num(m["roi"]),  # percent
                "total_profit_units": round(float(m["total_profit_units"]), 6),
            }
        )

    # Market breakdown
    market_rows: List[Dict[str, Any]] = []
    if not bets.empty:
        for market_key, g in bets.groupby("market_key", dropna=False):
            m = _rollup_metrics(g)
            market_rows.append(
                {
                    "market_key": str(market_key),
                    "n_bets": int(m["n_bets"]),
                    "win_rate": _pct_num(m["win_rate"]),
                    "roi": _pct_num(m["roi"]),
                    "total_profit_units": round(float(m["total_profit_units"]), 6),
                }
            )
        market_rows.sort(key=lambda x: (-int(x["n_bets"]), str(x["market_key"])))

    overall = _rollup_metrics(bets)

    # Write detailed rows
    keep_cols = [
        "game_date",
        "player_id",
        "game_id",
        "prop_type",
        "market_key",
        "line",
        "side",
        "model_prob_over",
        "p_market_over_raw",
        "p_market_under_raw",
        "p_market_over_novig",
        "edge_over_novig",
        "bookmaker_key",
        "price_over_american",
        "price_under_american",
        "actual_value",
        "won",
        "push",
        "profit_units",
        "edge_bucket",
        "odds_snapshot_file",
        "slate_source_file",
    ]
    out_df = bets[keep_cols].sort_values(
        by=["game_date", "edge_over_novig", "game_id", "player_id"],
        ascending=[True, False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)
    out_df.to_csv(out_rows_csv, index=False)

    payload: Dict[str, Any] = {
        "ok": True,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "requested_days": int(len(days)),
        "skipped_missing_artifacts": int(skipped_missing_artifacts),
        "candidate_rows_in": int(total_candidate_rows_in),
        "matched_candidates": int(total_matched_candidates),
        "snapshot_mismatches": int(total_snapshot_mismatches),
        "rows_total_matched_before_odds_drop": int(len(all_candidates)),
        "rows_dropped_missing_odds": int(dropped_missing_odds),
        "rows_dropped_missing_odds_pct": round(100.0 * float(dropped_missing_odds_pct), 4),
        "rows_after_filters_before_edge_min": int(len(work)),
        "rows_written": int(len(out_df)),
        "overall": {
            "n_bets": int(overall["n_bets"]),
            "win_rate": _pct_num(overall["win_rate"]),
            "roi": _pct_num(overall["roi"]),
            "total_profit_units": round(float(overall["total_profit_units"]), 6),
        },
        "edge_thresholds_cumulative": threshold_rows,
        "edge_buckets": edge_bucket_rows,
        "market_breakdown": market_rows,
        "per_day_rollup": [
            {
                "game_date": r.game_date,
                "n_bets": int(r.n_bets),
                "win_rate": _pct_num(r.win_rate),
                "roi": _pct_num(r.roi),
                "total_profit_units": round(float(r.total_profit_units), 6),
            }
            for r in per_day_bets
        ],
        "out_rows_csv": str(out_rows_csv),
        "out_summary_json": str(out_summary_json),
    }
    out_summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Console output
    print("[mlb-manual-surface-replay] RUN INFO")
    print(f"requested_days={len(days)} skipped_missing_artifacts={skipped_missing_artifacts}")
    print(f"candidate_rows_in={total_candidate_rows_in} matched_candidates={total_matched_candidates}")
    print(
        "missing_odds_drop="
        f"{dropped_missing_odds}/{len(all_candidates)} ({100.0 * dropped_missing_odds_pct:.2f}%) "
        f"snapshot_mismatches={total_snapshot_mismatches}"
    )
    print(f"rows_after_filters={len(work)} rows_written={len(out_df)}")
    print(f"out_rows_csv={out_rows_csv}")
    print(f"out_summary_json={out_summary_json}")

    print("\n[mlb-manual-surface-replay] OVERALL")
    print(
        f"n_bets={overall['n_bets']} win_rate={_pct(overall['win_rate'])} "
        f"ROI={_pct(overall['roi'])} total_profit_units={float(overall['total_profit_units']):.3f}"
    )

    print("\n[mlb-manual-surface-replay] EDGE BUCKETS")
    edge_bucket_df = pd.DataFrame(edge_bucket_rows, columns=["edge_bucket", "n_bets", "win_rate", "roi", "total_profit_units"])
    if edge_bucket_df.empty:
        print("(none)")
    else:
        print(edge_bucket_df.to_string(index=False))

    print("\n[mlb-manual-surface-replay] MARKET BREAKDOWN")
    market_df = pd.DataFrame(market_rows, columns=["market_key", "n_bets", "win_rate", "roi", "total_profit_units"])
    if market_df.empty:
        print("(none)")
    else:
        print(market_df.to_string(index=False))

    print("\nPASTE FOR CHATGPT")
    print(f"total_bets={int(overall['n_bets'])}")
    print(f"overall_roi={_pct(overall['roi'])}")
    print("edge_bucket_table:")
    if edge_bucket_df.empty:
        print("(none)")
    else:
        print(edge_bucket_df[["edge_bucket", "n_bets", "win_rate", "roi"]].to_string(index=False))
    print("market_breakdown:")
    if market_df.empty:
        print("(none)")
    else:
        print(market_df[["market_key", "n_bets", "win_rate", "roi"]].to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
