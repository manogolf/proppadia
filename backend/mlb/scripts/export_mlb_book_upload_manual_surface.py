#!/usr/bin/env python3
"""
Manual MLB book-upload exporter (broad surface).

Purpose:
- Build a strict upload CSV from slate model rows + raw odds snapshot matching.
- No policy-plan dependency.
- No prop_books gating.
- Manual-only path (not wired to LaunchAgent).

Output schema (strict):
  LEAGUE, DATE, HOME, AWAY, DOUBLEHEADER, SECTION, MARKET, SELECTOR, POINT, SIDE, WIN %
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from backend.mlb.scripts import export_mlb_book_upload as ex
from backend.mlb.scripts.build_mlb_reconcile_rows import (
    _build_market_index,
    _build_team_name_reverse,
    _choose_book,
    _line_key,
    _load_events,
    _norm_name,
)


STRICT_UPLOAD_COLUMNS: List[str] = [
    "LEAGUE",
    "DATE",
    "HOME",
    "AWAY",
    "DOUBLEHEADER",
    "SECTION",
    "MARKET",
    "SELECTOR",
    "POINT",
    "SIDE",
    "WIN %",
]


def _parse_prop_types(raw: str) -> Optional[set[str]]:
    vals = [ex._canonical_prop_type(x) for x in str(raw or "").split(",")]
    vals = [v for v in vals if v]
    return set(vals) if vals else None


def _pick_book_row_for_market(
    *,
    by_book: Dict[str, Dict[str, object]],
    require_two_sided: bool,
) -> Tuple[Optional[str], Optional[Dict[str, object]], Optional[float], Optional[float]]:
    if not by_book:
        return None, None, None, None

    if require_two_sided:
        filtered = {
            str(k): dict(v)
            for k, v in by_book.items()
            if isinstance(v, dict) and v.get("over") is not None and v.get("under") is not None
        }
        if not filtered:
            return None, None, None, None
        chosen_book, price_over, price_under, _ = _choose_book(by_book=filtered, bookmaker=None)
        if not chosen_book:
            return None, None, None, None
        return str(chosen_book), dict(filtered[str(chosen_book)]), price_over, price_under

    chosen_book, price_over, price_under, _ = _choose_book(by_book=by_book, bookmaker=None)
    if not chosen_book:
        return None, None, None, None
    return str(chosen_book), dict(by_book[str(chosen_book)]), price_over, price_under


def _build_manual_candidate_rows(
    *,
    merged: pd.DataFrame,
    odds_snapshot_json: Path,
    market_map: Dict[str, str],
    require_two_sided: bool,
) -> pd.DataFrame:
    if not odds_snapshot_json.exists():
        raise FileNotFoundError(f"missing odds snapshot json: {odds_snapshot_json}")

    events = _load_events(odds_snapshot_json)
    market_idx = _build_market_index(events=events, team_name_rev=_build_team_name_reverse())

    out_rows: List[Dict[str, object]] = []
    out_cols = [
        "league",
        "slate_date",
        "game_date",
        "game_id",
        "home_team_code",
        "away_team_code",
        "player_id",
        "player_name",
        "prop_type",
        "market_key",
        "line",
        "model_prob_over",
        "model_prob_under",
        "bookmaker_key",
        "price_over_american",
        "price_under_american",
    ]

    for _, row in merged.iterrows():
        prop_type = ex._canonical_prop_type(row.get("prop_type"))
        home = str(row.get("home_team_code") or "").strip().upper()
        away = str(row.get("away_team_code") or "").strip().upper()
        player_name = str(row.get("player_name") or "").strip()
        line = _line_key(row.get("line"))
        if not prop_type or not home or not away or not player_name or line is None:
            continue

        market_key = ex._clean_optional_str(row.get("market_key")) or market_map.get(prop_type)
        if not market_key:
            continue

        market_candidates = ex._market_candidates_for_prop(
            prop_type=prop_type,
            base_market=market_key,
        )
        if not market_candidates:
            continue

        selected_market_key: Optional[str] = None
        selected_book_key: Optional[str] = None
        selected_book_row: Optional[Dict[str, object]] = None
        selected_price_over: Optional[float] = None
        selected_price_under: Optional[float] = None

        for market_key_try in market_candidates:
            market_key_norm = str(market_key_try or "").strip()
            if not market_key_norm:
                continue
            k = (home, away, market_key_norm, _norm_name(player_name), float(line))
            by_book = market_idx.get(k, {})
            if not by_book:
                continue

            book_key, book_row, price_over, price_under = _pick_book_row_for_market(
                by_book=by_book,
                require_two_sided=bool(require_two_sided),
            )
            if not book_key or not book_row:
                continue

            selected_market_key = market_key_norm
            selected_book_key = str(book_key)
            selected_book_row = dict(book_row)
            selected_price_over = float(price_over) if price_over is not None else None
            selected_price_under = float(price_under) if price_under is not None else None
            break

        if not selected_book_key or selected_book_row is None:
            continue

        out_rows.append(
            {
                "league": row.get("league"),
                "slate_date": row.get("slate_date"),
                "game_date": row.get("game_date"),
                "game_id": row.get("game_id"),
                "home_team_code": home,
                "away_team_code": away,
                "player_id": row.get("player_id"),
                "player_name": player_name,
                "prop_type": prop_type,
                "market_key": selected_market_key or market_key,
                "line": float(line),
                "model_prob_over": float(row.get("prob_over")),
                "model_prob_under": float(row.get("prob_under")),
                "bookmaker_key": str(selected_book_key),
                "price_over_american": selected_price_over,
                "price_under_american": selected_price_under,
            }
        )

    out = pd.DataFrame(out_rows, columns=out_cols)
    if out.empty:
        return out
    return out.drop_duplicates(subset=["game_id", "player_id", "prop_type", "line"]).reset_index(drop=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse
    from datetime import datetime
    import pytz

    ap = argparse.ArgumentParser(description="Manual broad-surface MLB upload exporter (no policy gating).")
    ap.add_argument("--slate-date", default=None, help="YYYY-MM-DD (ET). Defaults to SLATE_DATE or ET today.")
    ap.add_argument("--slate-csv", required=True, help="Path to mlb_slate_output.csv")
    ap.add_argument("--odds-snapshot-json", required=True, help="Path to odds snapshot json")
    ap.add_argument("--out-csv", required=True, help="Output strict upload csv path")
    ap.add_argument("--league", default="MLB")
    ap.add_argument("--section", default="player_prop")
    ap.add_argument("--market", default="", help="Force one market key for all rows.")
    ap.add_argument("--market-map-json", default="", help="Optional JSON object prop_type->market_key overrides.")
    ap.add_argument("--prop-types", default="", help="Optional comma-separated prop_type allowlist.")
    ap.add_argument(
        "--side-mode",
        choices=["both", "over", "under"],
        default="both",
        help="Which side rows to emit (default: both).",
    )
    ap.add_argument(
        "--min-side-prob",
        type=float,
        default=0.0,
        help="Optional minimum model probability per emitted side row (0..1).",
    )
    ap.add_argument(
        "--max-abs-win-pct",
        type=int,
        default=10000,
        help="Exclude side rows where abs(WIN %)>this threshold (default: 10000).",
    )
    ap.add_argument(
        "--drop-line-0-5",
        action="store_true",
        help="Drop line=0.5 rows before matching.",
    )
    ap.add_argument(
        "--require-two-sided",
        action="store_true",
        help="Require matched odds rows to have both over and under prices.",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    min_side_prob = float(args.min_side_prob or 0.0)
    if not (0.0 <= min_side_prob <= 1.0):
        raise RuntimeError(f"--min-side-prob must be between 0 and 1; got {min_side_prob}")
    max_abs_win_pct = int(args.max_abs_win_pct or 0)
    if max_abs_win_pct <= 0:
        raise RuntimeError(f"--max-abs-win-pct must be > 0; got {max_abs_win_pct}")

    et = pytz.timezone("America/New_York")
    et_today = datetime.now(et).strftime("%Y-%m-%d")
    slate_date = (args.slate_date or os.environ.get("SLATE_DATE") or et_today).strip()
    target_date = pd.to_datetime(slate_date).date()

    slate_csv = Path(str(args.slate_csv)).expanduser()
    odds_snapshot_json = Path(str(args.odds_snapshot_json)).expanduser()
    out_csv = Path(str(args.out_csv)).expanduser()

    prop_filter = _parse_prop_types(str(args.prop_types or ""))
    market_map = ex._load_market_map(
        arg_json=str(args.market_map_json),
        env_json=str(os.environ.get("MLB_BOOK_UPLOAD_MARKET_MAP_JSON", "")),
    )

    print(f"[mlb-book-upload-manual-surface] slate_date (ET) = {slate_date}")
    print(f"[mlb-book-upload-manual-surface] slate_csv = {slate_csv}")
    print(f"[mlb-book-upload-manual-surface] odds_snapshot_json = {odds_snapshot_json}")
    print(f"[mlb-book-upload-manual-surface] out_csv = {out_csv}")

    merged = ex._normalize_slate_output(ex._load_slate_output(slate_csv))
    merged["game_date"] = pd.to_datetime(merged["game_date"]).dt.date
    merged = merged[merged["game_date"] == target_date].copy()
    if merged.empty:
        raise RuntimeError(f"zero slate rows for slate_date={slate_date}")

    if prop_filter:
        merged["prop_type"] = merged["prop_type"].map(ex._canonical_prop_type)
        merged = merged[merged["prop_type"].isin(prop_filter)].copy()
        if merged.empty:
            raise RuntimeError(f"zero rows after --prop-types filter: {sorted(prop_filter)}")

    if args.drop_line_0_5:
        merged = merged[merged["line"] != 0.5].copy()
        if merged.empty:
            raise RuntimeError("zero rows after --drop-line-0-5")

    candidate_rows_in = int(len(merged))
    candidate_rows = _build_manual_candidate_rows(
        merged=merged,
        odds_snapshot_json=odds_snapshot_json,
        market_map=market_map,
        require_two_sided=bool(args.require_two_sided),
    )
    matched_rows_out = int(len(candidate_rows))

    prop_breakdown = (
        candidate_rows["prop_type"].value_counts(dropna=False).sort_index().to_dict() if not candidate_rows.empty else {}
    )

    rows: List[Dict[str, object]] = []
    exportable_candidates = 0
    if not candidate_rows.empty:
        for _, row in candidate_rows.iterrows():
            p_over = float(row["model_prob_over"])
            if not (0.0 < p_over < 1.0):
                continue
            p_under = 1.0 - p_over

            prop_type = ex._canonical_prop_type(row["prop_type"])
            market = str(args.market).strip()
            if not market:
                market = ex._normalize_upload_market(
                    raw_market=row.get("market_key"),
                    prop_type=prop_type,
                    market_map=market_map,
                )
            else:
                market = ex._normalize_upload_market(
                    raw_market=market,
                    prop_type=prop_type,
                    market_map=market_map,
                )
            date_str = pd.to_datetime(row["game_date"]).strftime("%Y%m%d")

            base = {
                "LEAGUE": str(args.league).strip() or "MLB",
                "DATE": date_str,
                "HOME": ex._normalize_upload_team_code(row["home_team_code"]),
                "AWAY": ex._normalize_upload_team_code(row["away_team_code"]),
                "DOUBLEHEADER": "",
                "SECTION": str(args.section).strip() or "player_prop",
                "MARKET": market,
                "SELECTOR": int(row["player_id"]),
                "POINT": float(row["line"]),
            }

            emitted_for_candidate = False
            if args.side_mode in {"both", "over"} and p_over >= min_side_prob:
                odds_over = ex._prob_to_fair_american(float(p_over))
                if odds_over is not None and abs(int(odds_over)) <= max_abs_win_pct:
                    rows.append({**base, "SIDE": "over", "WIN %": int(odds_over)})
                    emitted_for_candidate = True
            if args.side_mode in {"both", "under"} and p_under >= min_side_prob:
                odds_under = ex._prob_to_fair_american(float(p_under))
                if odds_under is not None and abs(int(odds_under)) <= max_abs_win_pct:
                    rows.append({**base, "SIDE": "under", "WIN %": int(odds_under)})
                    emitted_for_candidate = True
            if emitted_for_candidate:
                exportable_candidates += 1

    out_df = pd.DataFrame(rows, columns=STRICT_UPLOAD_COLUMNS)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv, index=False)

    print(
        "[mlb-book-upload-manual-surface] summary: "
        f"candidate_rows_in={candidate_rows_in} "
        f"matched_candidates={matched_rows_out} "
        f"exportable_candidates={exportable_candidates} "
        f"rows_written={len(out_df)} "
        f"side_mode={args.side_mode} "
        f"output_path={out_csv}"
    )
    print(f"[mlb-book-upload-manual-surface] prop_type_breakdown={prop_breakdown}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
