#!/usr/bin/env python3
"""Summarize downloaded grader CSV for NHL SOG analysis.

Example:
  .venv/bin/python backend/nhl/scripts/summarize_nhl_grader_csv.py \
    --in-csv /Users/jerrystrain/Downloads/8rainstation_daily_2026_03_04.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


BET_RE = re.compile(
    r"^\s*(?P<player>.+?)\s+Shots\s+On\s+Goal\s+(?P<side>Over|Under)\s+(?P<line>\d+(?:\.\d+)?)\s*$",
    re.IGNORECASE,
)
RAQ_RE = re.compile(
    r"raq\s+(?P<model>\d+(?:\.\d+)?)/(?P<stat>\d+(?:\.\d+)?|n\/a)/(?P<market>\d+(?:\.\d+)?)%?",
    re.IGNORECASE,
)


def _f(value: Any, default: float = 0.0) -> float:
    try:
        s = str(value if value is not None else "").strip().replace(",", "")
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def _norm_grade(value: Any) -> str:
    raw = str(value if value is not None else "").strip().lower()
    if raw in {"win", "w"}:
        return "win"
    if raw in {"loss", "l"}:
        return "loss"
    if raw in {"push", "p"}:
        return "push"
    if raw in {"dnp", "void", "cancelled", "canceled"}:
        return raw
    return raw or "unknown"


def _extract_side_line_player(bet: str) -> Tuple[Optional[str], Optional[float], Optional[str]]:
    m = BET_RE.match(str(bet or ""))
    if not m:
        return None, None, None
    side = str(m.group("side")).strip().lower()
    line = _f(m.group("line"), default=0.0)
    player = str(m.group("player")).strip()
    return side, line, player


def _extract_raq(notes: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    m = RAQ_RE.search(str(notes or ""))
    if not m:
        return None, None, None
    model = _f(m.group("model"), default=0.0)
    stat_raw = str(m.group("stat")).strip().lower()
    stat = None if stat_raw == "n/a" else _f(stat_raw, default=0.0)
    market = _f(m.group("market"), default=0.0)
    return model, stat, market


def _choose_slate_date(rows: Iterable[Dict[str, Any]]) -> str:
    dates: List[str] = []
    for row in rows:
        raw = str(row.get("Event Date") or "").strip()
        if not raw:
            continue
        try:
            d = datetime.strptime(raw, "%m/%d/%Y %I:%M:%S %p")
            dates.append(d.date().isoformat())
        except Exception:
            continue
    if dates:
        return sorted(dates)[-1]
    return datetime.now().date().isoformat()


def summarize(in_csv: Path, out_csv: Path, out_json: Path) -> Dict[str, Any]:
    with in_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    filtered: List[Dict[str, Any]] = []
    for row in rows:
        sport = str(row.get("Sport") or "").strip().lower()
        league = str(row.get("League") or "").strip().lower()
        market = str(row.get("Market") or "")
        if sport != "hockey":
            continue
        if league != "nhl":
            continue
        if "shots on goal" not in market.lower():
            continue

        side, line, player = _extract_side_line_player(str(row.get("Bet") or ""))
        model_pct, stat_pct, market_pct = _extract_raq(str(row.get("Notes") or ""))
        grade = _norm_grade(row.get("Grade"))
        amount = _f(row.get("Amount"))
        pnl = _f(row.get("$ W/L"))

        filtered.append(
            {
                "wager_id": row.get("Wager ID"),
                "wager_date": row.get("Wager Date"),
                "event_date": row.get("Event Date"),
                "sport": row.get("Sport"),
                "league": row.get("League"),
                "away": row.get("Away"),
                "home": row.get("Home"),
                "book": row.get("Book"),
                "bet": row.get("Bet"),
                "market": row.get("Market"),
                "player_name": player or "",
                "side": side or "",
                "line": line if line is not None else "",
                "grade": grade,
                "amount": amount,
                "pnl": pnl,
                "odds": _f(row.get("Odds"), default=0.0),
                "notes": row.get("Notes"),
                "raq_model_pct": model_pct,
                "raq_stat_pct": stat_pct,
                "raq_market_pct": market_pct,
            }
        )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "wager_id",
        "wager_date",
        "event_date",
        "sport",
        "league",
        "away",
        "home",
        "book",
        "bet",
        "market",
        "player_name",
        "side",
        "line",
        "grade",
        "amount",
        "pnl",
        "odds",
        "notes",
        "raq_model_pct",
        "raq_stat_pct",
        "raq_market_pct",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(filtered)

    grade_counts: Dict[str, int] = {}
    for row in filtered:
        g = str(row["grade"])
        grade_counts[g] = grade_counts.get(g, 0) + 1

    graded_rows = [r for r in filtered if r["grade"] in {"win", "loss", "push"}]
    wl_rows = [r for r in filtered if r["grade"] in {"win", "loss"}]
    wins = sum(1 for r in wl_rows if r["grade"] == "win")
    losses = sum(1 for r in wl_rows if r["grade"] == "loss")
    pushes = sum(1 for r in graded_rows if r["grade"] == "push")
    staked = sum(_f(r["amount"]) for r in graded_rows)
    pnl = sum(_f(r["pnl"]) for r in graded_rows)
    roi = (pnl / staked) if staked > 0 else 0.0
    win_rate = (wins / (wins + losses)) if (wins + losses) > 0 else 0.0

    by_side: Dict[str, Dict[str, float]] = {}
    for side in ("over", "under"):
        side_rows = [r for r in wl_rows if r["side"] == side]
        sw = sum(1 for r in side_rows if r["grade"] == "win")
        sl = sum(1 for r in side_rows if r["grade"] == "loss")
        srisk = sum(_f(r["amount"]) for r in side_rows)
        spnl = sum(_f(r["pnl"]) for r in side_rows)
        by_side[side] = {
            "rows": len(side_rows),
            "wins": sw,
            "losses": sl,
            "win_rate": (sw / (sw + sl)) if (sw + sl) > 0 else 0.0,
            "roi": (spnl / srisk) if srisk > 0 else 0.0,
        }

    summary = {
        "ok": True,
        "in_csv": str(in_csv),
        "out_csv": str(out_csv),
        "total_rows_in_file": len(rows),
        "nhl_sog_rows": len(filtered),
        "grade_counts": grade_counts,
        "graded_rows": len(graded_rows),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate": win_rate,
        "staked": staked,
        "pnl": pnl,
        "roi": roi,
        "by_side": by_side,
    }
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Summarize downloaded grader CSV for NHL SOG.")
    ap.add_argument("--in-csv", required=True, help="Path to downloaded grader CSV (8rainstation_daily_YYYY_MM_DD.csv)")
    ap.add_argument("--out-csv", default=None, help="Output cleaned NHL SOG graded rows CSV")
    ap.add_argument("--out-json", default=None, help="Output summary JSON")
    args = ap.parse_args()

    in_csv = Path(args.in_csv).expanduser().resolve()
    if not in_csv.exists():
        raise SystemExit(f"Missing input CSV: {in_csv}")

    stem = in_csv.stem
    if stem.startswith("8rainstation_daily_"):
        date_part = stem.replace("8rainstation_daily_", "").replace("_", "-")
    else:
        date_part = _choose_slate_date(csv.DictReader(in_csv.open("r", encoding="utf-8-sig", newline="")))

    out_csv = Path(args.out_csv) if args.out_csv else Path(f"tmp/graded/nhl_sog_graded_{date_part}.csv")
    out_json = Path(args.out_json) if args.out_json else Path(f"tmp/graded/nhl_sog_graded_{date_part}_summary.json")

    summary = summarize(in_csv, out_csv, out_json)

    print(f"[graded] in={summary['in_csv']}")
    print(f"[graded] rows_in_file={summary['total_rows_in_file']} nhl_sog_rows={summary['nhl_sog_rows']}")
    print(
        "[graded] W-L-P="
        f"{summary['wins']}-{summary['losses']}-{summary['pushes']} "
        f"win_rate={summary['win_rate']*100:.2f}% roi={summary['roi']*100:.2f}%"
    )
    print(f"[graded] wrote csv={summary['out_csv']}")
    print(f"[graded] wrote json={out_json}")


if __name__ == "__main__":
    main()
