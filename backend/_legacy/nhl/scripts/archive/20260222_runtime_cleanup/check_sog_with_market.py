#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def amer_to_implied_p(odds: float) -> Optional[float]:
    """
    American odds -> implied probability (no vig removal).
    -152 => 152/(152+100) ~ 0.603
    +180 => 100/(180+100) ~ 0.357
    """
    try:
        o = float(odds)
    except Exception:
        return None
    if o == 0:
        return None
    if o > 0:
        return 100.0 / (o + 100.0)
    return (-o) / ((-o) + 100.0)


def amer_to_profit_per_1_staked(odds: float) -> Optional[float]:
    """
    Profit if win for a $1 stake.
    -152 => win profit ~ 0.6579 (because you risk 1 to win 100/152)
    +180 => win profit = 1.8
    """
    try:
        o = float(odds)
    except Exception:
        return None
    if o == 0:
        return None
    if o > 0:
        return o / 100.0
    return 100.0 / (-o)


def ev_per_1_staked(model_p_win: float, odds: float) -> Optional[float]:
    """
    Expected value (profit) for $1 stake:
      EV = p*profit_if_win - (1-p)*1
    """
    if model_p_win is None:
        return None
    if model_p_win < 0 or model_p_win > 1:
        return None
    profit = amer_to_profit_per_1_staked(odds)
    if profit is None:
        return None
    return model_p_win * profit - (1.0 - model_p_win) * 1.0


def coerce_float(x) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() in {"none", "null", "nan"}:
        return None
    # allow "+262" / "-152" as strings
    try:
        return float(s)
    except Exception:
        # try stripping commas
        try:
            return float(s.replace(",", ""))
        except Exception:
            return None


def find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--csv",
        required=True,
        help="Path to sog_with_market.csv (pipeline output).",
    )
    ap.add_argument(
        "--out",
        default="backend/nhl/exports/check_sog_with_market_report.csv",
        help="Output report CSV path (inside repo).",
    )
    ap.add_argument(
        "--min-line",
        type=float,
        default=2.5,
        help="Only include rows with line >= this value (default: 2.5).",
    )
    ap.add_argument(
        "--min-abs-edge",
        type=float,
        default=0.10,
        help="Only include rows where abs(model_p - implied_p) >= this (default: 0.10).",
    )
    args = ap.parse_args()

    in_path = Path(args.csv)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with in_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("CSV has no header row.")
        cols = list(reader.fieldnames)

        # Try to locate common columns; tolerate your evolving schema.
        c_player = find_col(cols, ["player", "player_name", "full_name"])
        c_player_id = find_col(cols, ["player_id"])
        c_game_id = find_col(cols, ["game_id"])
        c_line = find_col(cols, ["line"])
        c_p_over = find_col(cols, ["p_over", "p_over_line", "p_over_cal", "p_over_db"])

        # Market odds columns (your file may have 1 side, or both)
        c_over_odds = find_col(cols, ["over_odds", "odds_over", "book_over_odds"])
        c_under_odds = find_col(cols, ["under_odds", "odds_under", "book_under_odds"])

        # Sometimes file has one odds column + a side/pick column
        c_odds = find_col(cols, ["odds", "book_odds", "market_odds", "price"])
        c_side = find_col(cols, ["side", "pick", "ou", "over_under"])

        # Optional implied prob column (from pipeline)
        c_implied = find_col(cols, ["implied_p", "book_p", "market_p", "p_implied"])

        rows_in = list(reader)

    if c_line is None or c_p_over is None or c_game_id is None or c_player_id is None:
        raise RuntimeError(
            "Could not find required columns. Need at least: player_id, game_id, line, p_over.\n"
            f"Found columns: {cols}"
        )

    report: List[Dict[str, str]] = []

    for r in rows_in:
        line = coerce_float(r.get(c_line))
        if line is None or line < args.min_line:
            continue

        p_over = coerce_float(r.get(c_p_over))
        if p_over is None:
            continue

        # model prob for each side at this line
        p_under = 1.0 - p_over

        # pick odds for each side
        over_odds = coerce_float(r.get(c_over_odds)) if c_over_odds else None
        under_odds = coerce_float(r.get(c_under_odds)) if c_under_odds else None

        # If only one odds column exists, try to use side column to assign it.
        if (over_odds is None and under_odds is None) and c_odds:
            one_odds = coerce_float(r.get(c_odds))
            if one_odds is not None and c_side:
                side_raw = (r.get(c_side) or "").strip().lower()
                # tolerate "o", "over", "u", "under", "o2.5", "u3.5"
                if side_raw.startswith("o") or "over" in side_raw:
                    over_odds = one_odds
                elif side_raw.startswith("u") or "under" in side_raw:
                    under_odds = one_odds

        # If still missing, we can’t EV-check; skip row
        if over_odds is None and under_odds is None:
            continue

        # Compute implied prob(s)
        implied_over = amer_to_implied_p(over_odds) if over_odds is not None else None
        implied_under = amer_to_implied_p(under_odds) if under_odds is not None else None

        # Optional: check against pipeline-implied column if it exists
        pipeline_implied = coerce_float(r.get(c_implied)) if c_implied else None

        # Compute EV(s)
        ev_over = ev_per_1_staked(p_over, over_odds) if over_odds is not None else None
        ev_under = ev_per_1_staked(p_under, under_odds) if under_odds is not None else None

        # Compute edges vs implied (simple model_p - implied_p)
        edge_over = (p_over - implied_over) if implied_over is not None else None
        edge_under = (p_under - implied_under) if implied_under is not None else None

        # Choose best side by EV if both exist; else whichever exists
        best_side = None
        best_ev = None
        best_edge = None
        best_odds = None
        best_implied = None
        best_model_p = None

        candidates: List[Tuple[str, Optional[float], Optional[float], Optional[float], Optional[float]]] = [
            ("OVER", ev_over, edge_over, over_odds, implied_over),
            ("UNDER", ev_under, edge_under, under_odds, implied_under),
        ]
        for side, ev, edge, odds, implied in candidates:
            if ev is None:
                continue
            if best_ev is None or ev > best_ev:
                best_side = side
                best_ev = ev
                best_edge = edge
                best_odds = odds
                best_implied = implied
                best_model_p = p_over if side == "OVER" else p_under

        if best_side is None:
            continue

        # Filter to “interesting” edges, like your >=10% EV/edge workflow
        if best_edge is None or abs(best_edge) < args.min_abs_edge:
            continue

        # Flag conditions
        flags = []
        if p_over < 0 or p_over > 1:
            flags.append("BAD_P_OVER_RANGE")
        if best_implied is None:
            flags.append("NO_IMPLIED")
        if pipeline_implied is not None and best_implied is not None:
            # If pipeline implied is for a specific side, it might not match our picked side;
            # we only do a weak check: if it's wildly different from *either* implied.
            diffs = []
            if implied_over is not None:
                diffs.append(abs(pipeline_implied - implied_over))
            if implied_under is not None:
                diffs.append(abs(pipeline_implied - implied_under))
            if diffs and min(diffs) > 0.02:
                flags.append("PIPELINE_IMPLIED_MISMATCH>0.02")

        # If your CSV encodes a side, verify it aligns with best_side (informational)
        if c_side:
            side_raw = (r.get(c_side) or "").strip().lower()
            if side_raw:
                encoded = "OVER" if (side_raw.startswith("o") or "over" in side_raw) else \
                          "UNDER" if (side_raw.startswith("u") or "under" in side_raw) else None
                if encoded and encoded != best_side:
                    flags.append(f"ENCODED_SIDE={encoded}_BUT_BEST={best_side}")

        report.append(
            {
                "game_date": str(r.get(find_col(list(r.keys()), ["game_date", "slate_date"]) or "") or ""),
                "game_id": str(r.get(c_game_id) or ""),
                "team": str(r.get(find_col(list(r.keys()), ["team", "team_code", "team_abbr"]) or "") or ""),
                "player": str(r.get(c_player) or ""),
                "player_id": str(r.get(c_player_id) or ""),
                "line": f"{line:.1f}",
                "p_over": f"{p_over:.6f}",
                "p_under": f"{p_under:.6f}",
                "best_side": best_side,
                "best_model_p": f"{best_model_p:.6f}" if best_model_p is not None else "",
                "best_odds": str(int(best_odds)) if best_odds is not None and float(best_odds).is_integer() else str(best_odds),
                "best_implied_p": f"{best_implied:.6f}" if best_implied is not None else "",
                "best_edge": f"{best_edge:.6f}" if best_edge is not None else "",
                "best_ev_per_$1": f"{best_ev:.6f}" if best_ev is not None else "",
                "over_odds": "" if over_odds is None else str(int(over_odds)) if float(over_odds).is_integer() else str(over_odds),
                "under_odds": "" if under_odds is None else str(int(under_odds)) if float(under_odds).is_integer() else str(under_odds),
                "implied_over": "" if implied_over is None else f"{implied_over:.6f}",
                "implied_under": "" if implied_under is None else f"{implied_under:.6f}",
                "flags": "|".join(flags),
            }
        )

    # Sort by EV desc
    report.sort(key=lambda x: float(x["best_ev_per_$1"]) if x["best_ev_per_$1"] else -999.0, reverse=True)

    fieldnames = list(report[0].keys()) if report else [
        "game_date","game_id","team","player","player_id","line",
        "p_over","p_under","best_side","best_model_p","best_odds",
        "best_implied_p","best_edge","best_ev_per_$1",
        "over_odds","under_odds","implied_over","implied_under","flags"
    ]

    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in report:
            w.writerow(row)

    print(f"✅ wrote {out_path} ({len(report)} rows kept after filters)")
    if len(report) == 0:
        print("   (No rows met filters; try lowering --min-abs-edge or check your CSV columns/odds fields.)")


if __name__ == "__main__":
    main()
