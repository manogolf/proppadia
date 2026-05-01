#!/usr/bin/env python3
"""Summarize MLB graded wagers from 8rainstation exports.

Purpose:
- Parse daily posted MLB player-prop graded rows (the wager set that was actually placed).
- Emit a normalized row file + summary + by-prop rollup for tracker/dashboard use.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

RAQ_RE = re.compile(
    r"raq\s+(?P<model>\d+(?:\.\d+)?)/(?P<stat>\d+(?:\.\d+)?|n\/?a|na|n\.a\.?)/(?P<market>\d+(?:\.\d+)?)%?",
    re.IGNORECASE,
)
BET_RE = re.compile(
    r"^\s*(?P<player>.+?)\s+(?P<prop>.+?)\s+(?P<side>Over|Under)\s+(?P<line>\d+(?:\.\d+)?)\s*$",
    re.IGNORECASE,
)
MARKET_RE = re.compile(
    r"^\s*(?P<player>.+?)\s+(?P<prop>.+?)\s+Over/Under\s+(?P<line>\d+(?:\.\d+)?)\s*$",
    re.IGNORECASE,
)


def _to_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        t = str(v).strip().replace(",", "")
        if not t:
            return None
        return float(t)
    except Exception:
        return None


def _norm_text(v: Any) -> str:
    return str(v if v is not None else "").strip()


def _norm_grade(v: Any) -> str:
    raw = _norm_text(v).lower()
    if raw in {"w", "win"}:
        return "win"
    if raw in {"l", "loss"}:
        return "loss"
    if raw in {"p", "push"}:
        return "push"
    if raw in {"void", "cancelled", "canceled", "dnp"}:
        return raw
    return raw or "unknown"


def _extract_raq(notes: Any) -> tuple[float | None, float | None, float | None]:
    text = _norm_text(notes)
    m = RAQ_RE.search(text)
    if not m:
        return None, None, None
    model = _to_float(m.group("model"))
    stat_raw = _norm_text(m.group("stat")).lower()
    stat = None if stat_raw in {"na", "n/a", "n.a", "n.a."} else _to_float(stat_raw)
    market = _to_float(m.group("market"))
    return model, stat, market


def _extract_from_bet_and_market(bet: Any, market: Any) -> tuple[str, str, str, float | None]:
    b = _norm_text(bet)
    m = _norm_text(market)

    player = ""
    prop = ""
    side = ""
    line: float | None = None

    bm = BET_RE.match(b)
    if bm:
        player = _norm_text(bm.group("player"))
        prop = _norm_text(bm.group("prop"))
        side = _norm_text(bm.group("side")).lower()
        line = _to_float(bm.group("line"))

    if not player or not prop or line is None:
        mm = MARKET_RE.match(m)
        if mm:
            if not player:
                player = _norm_text(mm.group("player"))
            if not prop:
                prop = _norm_text(mm.group("prop"))
            if line is None:
                line = _to_float(mm.group("line"))

    return player, prop, side, line


def _infer_prop_type(prop_label: str, market: str) -> str:
    text = f"{prop_label} {market}".strip().lower()
    text = text.replace("+", " + ")
    text = " ".join(text.split())

    if "hits + runs + rbi" in text or "hits + runs + rbis" in text:
        return "hits_runs_rbis"
    if "runs + rbi" in text or "runs + rbis" in text:
        return "runs_rbis"
    if "total bases" in text:
        return "total_bases"
    if "earned runs" in text:
        return "earned_runs"
    if "outs recorded" in text or "pitcher outs" in text or "pitching outs" in text:
        return "outs_recorded"
    if "walks allowed" in text:
        return "walks_allowed"
    if "hits allowed" in text or "pitcher hits" in text:
        return "hits_allowed"
    if "runs scored" in text:
        return "runs_scored"
    if "strikeouts" in text:
        if "pitcher" in text:
            return "strikeouts_pitching"
        if "batter" in text or "hitter" in text:
            return "strikeouts_batting"
        return "strikeouts_pitching"
    if "stolen bases" in text:
        return "stolen_bases"
    if "home runs" in text:
        return "home_runs"
    if "triples" in text:
        return "triples"
    if "doubles" in text:
        return "doubles"
    if "singles" in text:
        return "singles"
    if re.search(r"\brbis\b", text):
        return "rbis"
    if re.search(r"\bhits\b", text):
        return "hits"
    if re.search(r"\bwalks\b", text):
        return "walks"
    return "unknown"


def _parse_event_date(v: Any) -> str:
    text = _norm_text(v)
    if not text:
        return ""
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except Exception:
            continue
    return ""


def _bucket_from_model_pct(model_pct: float | None) -> str:
    if model_pct is None:
        return "unknown"
    d = abs(float(model_pct) - 50.0)
    if d < 5.0:
        return "low"
    if d < 10.0:
        return "medium"
    return "high"


def _empty_outputs(out_rows_csv: Path, out_summary_json: Path, out_by_prop_csv: Path, in_csv: str) -> dict[str, Any]:
    out_rows_csv.parent.mkdir(parents=True, exist_ok=True)
    out_summary_json.parent.mkdir(parents=True, exist_ok=True)
    out_by_prop_csv.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        columns=[
            "report_date",
            "wager_id",
            "event_date",
            "book",
            "market",
            "bet",
            "player_name",
            "prop_label",
            "prop_type",
            "side",
            "line",
            "grade",
            "amount",
            "pnl",
            "roi_1u",
            "was_correct",
            "raq_model_pct",
            "raq_stat_pct",
            "raq_market_pct",
            "raq_edge_pct",
            "confidence_bucket",
        ]
    ).to_csv(out_rows_csv, index=False)

    pd.DataFrame(
        columns=[
            "prop_type",
            "rows",
            "wl_rows",
            "wins",
            "losses",
            "pushes",
            "win_rate_pct",
            "staked",
            "pnl",
            "roi_pct",
            "avg_model_pct",
        ]
    ).to_csv(out_by_prop_csv, index=False)

    payload = {
        "ok": False,
        "status": "no_data",
        "in_csv": in_csv,
        "rows_csv": str(out_rows_csv),
        "by_prop_csv": str(out_by_prop_csv),
        "summary": {
            "report_date": "",
            "rows_input": 0,
            "rows_filtered": 0,
            "graded_rows": 0,
            "wl_rows": 0,
            "wins": 0,
            "losses": 0,
            "pushes": 0,
            "win_rate_pct": None,
            "staked": 0.0,
            "pnl": 0.0,
            "roi_pct": None,
            "signal_rows": 0,
            "signal_wl_rows": 0,
            "signal_win_rate_pct": None,
        },
    }
    out_summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize MLB graded wagers from 8rainstation exports.")
    ap.add_argument("--in-csv", required=True, help="Path to MLB grader CSV (recommended: *_mlb_player_props.csv).")
    ap.add_argument("--out-rows-csv", default="tmp/analysis/mlb_graded_wagers_rows.csv")
    ap.add_argument("--out-summary-json", default="tmp/analysis/mlb_graded_wagers_summary.json")
    ap.add_argument("--out-by-prop-csv", default="tmp/analysis/mlb_graded_wagers_by_prop.csv")
    args = ap.parse_args()

    in_csv = Path(args.in_csv).expanduser()
    out_rows_csv = Path(args.out_rows_csv).expanduser()
    out_summary_json = Path(args.out_summary_json).expanduser()
    out_by_prop_csv = Path(args.out_by_prop_csv).expanduser()

    if not in_csv.exists():
        print(json.dumps({"ok": False, "status": "missing_input", "in_csv": str(in_csv)}, indent=2))
        return 2

    df = pd.read_csv(in_csv, low_memory=False)
    if df.empty:
        payload = _empty_outputs(out_rows_csv, out_summary_json, out_by_prop_csv, str(in_csv))
        print(json.dumps(payload, indent=2))
        return 0

    for c in (
        "Sport",
        "League",
        "Section",
        "Wager ID",
        "Event Date",
        "Book",
        "Market",
        "Bet",
        "Grade",
        "Amount",
        "$ W/L",
        "Notes",
    ):
        if c not in df.columns:
            df[c] = pd.NA

    sport = df["Sport"].astype(str).str.lower().str.strip()
    league = df["League"].astype(str).str.lower().str.strip()
    section = df["Section"].astype(str).str.lower().str.strip()

    mask = sport.eq("baseball") & league.eq("mlb") & section.str.contains("player") & section.str.contains("prop")
    work = df[mask].copy()

    if work.empty:
        payload = _empty_outputs(out_rows_csv, out_summary_json, out_by_prop_csv, str(in_csv))
        print(json.dumps(payload, indent=2))
        return 0

    rows: list[dict[str, Any]] = []
    for r in work.to_dict(orient="records"):
        player_name, prop_label, side, line = _extract_from_bet_and_market(r.get("Bet"), r.get("Market"))
        grade = _norm_grade(r.get("Grade"))
        amount = _to_float(r.get("Amount")) or 0.0
        pnl = _to_float(r.get("$ W/L")) or 0.0
        model_pct, stat_pct, market_pct = _extract_raq(r.get("Notes"))
        report_date = _parse_event_date(r.get("Event Date"))

        was_correct: int | None
        if grade == "win":
            was_correct = 1
        elif grade == "loss":
            was_correct = 0
        else:
            was_correct = None

        roi_1u: float | None = None
        if amount > 0:
            roi_1u = float(pnl) / float(amount)

        prop_type = _infer_prop_type(prop_label=prop_label, market=_norm_text(r.get("Market")))
        edge = (model_pct - market_pct) if (model_pct is not None and market_pct is not None) else None

        rows.append(
            {
                "report_date": report_date,
                "wager_id": _norm_text(r.get("Wager ID")),
                "event_date": _norm_text(r.get("Event Date")),
                "book": _norm_text(r.get("Book")),
                "market": _norm_text(r.get("Market")),
                "bet": _norm_text(r.get("Bet")),
                "player_name": player_name,
                "prop_label": prop_label,
                "prop_type": prop_type,
                "side": side,
                "line": line,
                "grade": grade,
                "amount": amount,
                "pnl": pnl,
                "roi_1u": roi_1u,
                "was_correct": was_correct,
                "raq_model_pct": model_pct,
                "raq_stat_pct": stat_pct,
                "raq_market_pct": market_pct,
                "raq_edge_pct": edge,
                "confidence_bucket": _bucket_from_model_pct(model_pct),
            }
        )

    out_rows_csv.parent.mkdir(parents=True, exist_ok=True)
    out_by_prop_csv.parent.mkdir(parents=True, exist_ok=True)
    out_summary_json.parent.mkdir(parents=True, exist_ok=True)

    out_df = pd.DataFrame(rows)
    if out_df.empty:
        payload = _empty_outputs(out_rows_csv, out_summary_json, out_by_prop_csv, str(in_csv))
        print(json.dumps(payload, indent=2))
        return 0

    report_date = str(pd.to_datetime(out_df["report_date"], errors="coerce").dropna().max().date()) if out_df["report_date"].notna().any() else ""

    graded_mask = out_df["grade"].isin(["win", "loss", "push"])
    wl_mask = out_df["grade"].isin(["win", "loss"])
    graded = out_df[graded_mask].copy()
    wl = out_df[wl_mask].copy()

    wins = int((out_df["grade"] == "win").sum())
    losses = int((out_df["grade"] == "loss").sum())
    pushes = int((out_df["grade"] == "push").sum())
    staked = float(pd.to_numeric(graded["amount"], errors="coerce").fillna(0).sum())
    pnl = float(pd.to_numeric(graded["pnl"], errors="coerce").fillna(0).sum())
    win_rate = (100.0 * wins / (wins + losses)) if (wins + losses) > 0 else None
    roi_pct = (100.0 * pnl / staked) if staked > 0 else None

    signal = out_df[out_df["raq_model_pct"].notna()].copy()
    signal_wl = signal[signal["grade"].isin(["win", "loss"])].copy()
    signal_wins = int((signal_wl["grade"] == "win").sum())
    signal_losses = int((signal_wl["grade"] == "loss").sum())
    signal_win_rate = (100.0 * signal_wins / (signal_wins + signal_losses)) if (signal_wins + signal_losses) > 0 else None

    by_prop = (
        out_df.assign(
            is_wl=out_df["grade"].isin(["win", "loss"]).astype(int),
            is_win=(out_df["grade"] == "win").astype(int),
            is_loss=(out_df["grade"] == "loss").astype(int),
            is_push=(out_df["grade"] == "push").astype(int),
            amount_graded=out_df["amount"].where(out_df["grade"].isin(["win", "loss", "push"]), 0.0),
            pnl_graded=out_df["pnl"].where(out_df["grade"].isin(["win", "loss", "push"]), 0.0),
        )
        .groupby("prop_type", dropna=False, as_index=False)
        .agg(
            rows=("prop_type", "size"),
            wl_rows=("is_wl", "sum"),
            wins=("is_win", "sum"),
            losses=("is_loss", "sum"),
            pushes=("is_push", "sum"),
            staked=("amount_graded", "sum"),
            pnl=("pnl_graded", "sum"),
            avg_model_pct=("raq_model_pct", "mean"),
        )
    )
    by_prop["win_rate_pct"] = by_prop.apply(
        lambda r: (100.0 * float(r["wins"]) / float(r["wins"] + r["losses"])) if (float(r["wins"]) + float(r["losses"])) > 0 else None,
        axis=1,
    )
    by_prop["roi_pct"] = by_prop.apply(
        lambda r: (100.0 * float(r["pnl"]) / float(r["staked"])) if float(r["staked"]) > 0 else None,
        axis=1,
    )
    by_prop = by_prop.sort_values(["rows", "prop_type"], ascending=[False, True], kind="mergesort")

    out_df = out_df.sort_values(["report_date", "wager_id"], ascending=[True, True], kind="mergesort")
    out_df.to_csv(out_rows_csv, index=False)
    by_prop.to_csv(out_by_prop_csv, index=False)

    payload = {
        "ok": True,
        "status": "pass",
        "in_csv": str(in_csv),
        "rows_csv": str(out_rows_csv),
        "by_prop_csv": str(out_by_prop_csv),
        "summary": {
            "report_date": report_date,
            "rows_input": int(len(df)),
            "rows_filtered": int(len(out_df)),
            "graded_rows": int(len(graded)),
            "wl_rows": int(len(wl)),
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "win_rate_pct": None if win_rate is None else round(float(win_rate), 2),
            "staked": round(float(staked), 2),
            "pnl": round(float(pnl), 2),
            "roi_pct": None if roi_pct is None else round(float(roi_pct), 2),
            "signal_rows": int(len(signal)),
            "signal_wl_rows": int(len(signal_wl)),
            "signal_win_rate_pct": None if signal_win_rate is None else round(float(signal_win_rate), 2),
        },
    }
    out_summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
