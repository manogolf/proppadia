#!/usr/bin/env python3
"""Run diagnostics on graded NHL SOG results and candidate-card calibration."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from glob import glob
from pathlib import Path
from typing import Any, Iterable


@dataclass
class GradedRow:
    date: str
    player_name: str
    side: str
    line: float
    grade: str
    amount: float
    pnl: float
    odds: float
    raq_model_pct: float | None
    raq_market_pct: float | None


@dataclass
class CardRow:
    full_name: str
    side: str
    line: float
    model_side_prob: float
    market_side_prob: float
    edge_side: float
    ev_side: float


def _f(v: Any, default: float = 0.0) -> float:
    try:
        s = str(v if v is not None else "").strip().replace(",", "")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _opt_f(v: Any) -> float | None:
    try:
        s = str(v if v is not None else "").strip().replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _norm_name(name: str) -> str:
    s = unicodedata.normalize("NFKD", str(name or "").strip().lower())
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", " ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _name_keys(name: str) -> set[str]:
    n = _norm_name(name)
    if not n:
        return set()
    parts = n.split()
    if len(parts) == 1:
        return {parts[0]}
    return {f"{parts[0]} {parts[-1]}", f"{parts[0][0]} {parts[-1]}"}


def _date_from_graded_path(path: Path) -> str:
    m = re.search(r"nhl_sog_graded_(\d{4}-\d{2}-\d{2})\.csv$", path.name)
    if m:
        return m.group(1)
    return ""


def _load_graded(paths: Iterable[Path]) -> list[GradedRow]:
    out: list[GradedRow] = []
    for path in sorted(paths):
        date = _date_from_graded_path(path)
        if not date:
            continue
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                side = str(row.get("side") or "").strip().lower()
                grade = str(row.get("grade") or "").strip().lower()
                line = _opt_f(row.get("line"))
                if side not in {"over", "under"} or line is None:
                    continue
                out.append(
                    GradedRow(
                        date=date,
                        player_name=str(row.get("player_name") or ""),
                        side=side,
                        line=float(line),
                        grade=grade,
                        amount=_f(row.get("amount")),
                        pnl=_f(row.get("pnl")),
                        odds=_f(row.get("odds")),
                        raq_model_pct=_opt_f(row.get("raq_model_pct")),
                        raq_market_pct=_opt_f(row.get("raq_market_pct")),
                    )
                )
    return out


def _load_card_index(path: Path) -> tuple[dict[tuple[str, str, float], list[CardRow]], int]:
    idx: dict[tuple[str, str, float], list[CardRow]] = defaultdict(list)
    collisions = 0
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            side = str(row.get("model_pick") or "").strip().lower()
            line = _opt_f(row.get("line"))
            if side not in {"over", "under"} or line is None:
                continue
            card = CardRow(
                full_name=str(row.get("full_name") or ""),
                side=side,
                line=float(line),
                model_side_prob=_f(row.get("model_side_prob")),
                market_side_prob=_f(row.get("market_side_prob")),
                edge_side=_f(row.get("edge_side")),
                ev_side=_f(row.get("ev_side")),
            )
            keys = _name_keys(card.full_name)
            for key in keys:
                k = (key, side, float(line))
                if idx[k]:
                    collisions += 1
                idx[k].append(card)
    return idx, collisions


def _wl_rows(rows: Iterable[GradedRow]) -> list[GradedRow]:
    return [r for r in rows if r.grade in {"win", "loss"}]


def _day_summary(rows: list[GradedRow]) -> dict[str, Any]:
    by_day: dict[str, list[GradedRow]] = defaultdict(list)
    for r in rows:
        by_day[r.date].append(r)

    out: list[dict[str, Any]] = []
    for date in sorted(by_day):
        day_rows = by_day[date]
        wl = _wl_rows(day_rows)
        wins = sum(1 for r in wl if r.grade == "win")
        losses = sum(1 for r in wl if r.grade == "loss")
        pushes = sum(1 for r in day_rows if r.grade == "push")
        staked = sum(r.amount for r in day_rows if r.grade in {"win", "loss", "push"})
        pnl = sum(r.pnl for r in day_rows if r.grade in {"win", "loss", "push"})
        out.append(
            {
                "date": date,
                "rows": len(day_rows),
                "wins": wins,
                "losses": losses,
                "pushes": pushes,
                "win_rate": (wins / (wins + losses)) if (wins + losses) else None,
                "staked": staked,
                "pnl": pnl,
                "roi": (pnl / staked) if staked else None,
            }
        )
    return {"days": out}


def _metrics_prob(rows: list[dict[str, Any]], prob_key: str) -> dict[str, Any]:
    vals: list[tuple[float, int]] = []
    for r in rows:
        p = r.get(prob_key)
        y = r.get("y")
        if not isinstance(p, (float, int)) or not isinstance(y, int):
            continue
        p = float(p)
        if not (0.0 < p < 1.0):
            continue
        vals.append((p, y))
    if not vals:
        return {"n": 0}

    n = len(vals)
    avg_p = sum(p for p, _ in vals) / n
    hit = sum(y for _, y in vals) / n
    brier = sum((p - y) ** 2 for p, y in vals) / n
    eps = 1e-12
    logloss = -sum(y * math.log(max(eps, min(1 - eps, p))) + (1 - y) * math.log(max(eps, min(1 - eps, 1 - p))) for p, y in vals) / n
    acc50 = sum((1 if p >= 0.5 else 0) == y for p, y in vals) / n
    return {
        "n": n,
        "avg_prob": avg_p,
        "hit_rate": hit,
        "gap": avg_p - hit,
        "brier": brier,
        "logloss": logloss,
        "acc50": acc50,
    }


def _calibration_bins(rows: list[dict[str, Any]], prob_key: str) -> list[dict[str, Any]]:
    bins = [(0.0, 0.40), (0.40, 0.45), (0.45, 0.50), (0.50, 0.55), (0.55, 0.60), (0.60, 0.70), (0.70, 1.01)]
    out: list[dict[str, Any]] = []
    for lo, hi in bins:
        sub = [r for r in rows if isinstance(r.get(prob_key), (int, float)) and lo <= float(r[prob_key]) < hi]
        if not sub:
            continue
        n = len(sub)
        avg_p = sum(float(r[prob_key]) for r in sub) / n
        hit = sum(int(r["y"]) for r in sub) / n
        out.append({"bin": f"{lo:.2f}-{hi:.2f}", "n": n, "avg_prob": avg_p, "hit_rate": hit, "gap": avg_p - hit})
    return out


def _build_side_line_stats(rows: list[GradedRow]) -> list[dict[str, Any]]:
    agg: dict[tuple[str, float], dict[str, Any]] = {}
    for r in rows:
        k = (r.side, r.line)
        if k not in agg:
            agg[k] = {"side": r.side, "line": r.line, "rows": 0, "wins": 0, "losses": 0, "pushes": 0, "staked": 0.0, "pnl": 0.0}
        a = agg[k]
        a["rows"] += 1
        if r.grade == "win":
            a["wins"] += 1
        elif r.grade == "loss":
            a["losses"] += 1
        elif r.grade == "push":
            a["pushes"] += 1
        if r.grade in {"win", "loss", "push"}:
            a["staked"] += r.amount
            a["pnl"] += r.pnl
    out = []
    for _, a in sorted(agg.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        wl = a["wins"] + a["losses"]
        a["win_rate"] = (a["wins"] / wl) if wl else None
        a["roi"] = (a["pnl"] / a["staked"]) if a["staked"] else None
        out.append(a)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit graded NHL SOG results and candidate-card calibration.")
    ap.add_argument("--graded-glob", default="tmp/graded/nhl_sog_graded_*.csv")
    ap.add_argument("--cards-dir", default="tmp/cards")
    ap.add_argument("--date-from", default="", help="YYYY-MM-DD inclusive")
    ap.add_argument("--date-to", default="", help="YYYY-MM-DD inclusive")
    ap.add_argument("--out-json", default="tmp/graded/nhl_sog_graded_audit.json")
    args = ap.parse_args()

    files = [Path(p) for p in sorted(glob(args.graded_glob)) if not p.endswith("_summary.csv")]
    graded = _load_graded(files)
    if args.date_from:
        graded = [r for r in graded if r.date >= str(args.date_from)]
    if args.date_to:
        graded = [r for r in graded if r.date <= str(args.date_to)]

    if not graded:
        raise SystemExit("No graded rows found for requested window.")

    wl = _wl_rows(graded)
    wins = sum(1 for r in wl if r.grade == "win")
    losses = sum(1 for r in wl if r.grade == "loss")
    pushes = sum(1 for r in graded if r.grade == "push")
    staked = sum(r.amount for r in graded if r.grade in {"win", "loss", "push"})
    pnl = sum(r.pnl for r in graded if r.grade in {"win", "loss", "push"})

    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "window": {
            "date_from": min(r.date for r in graded),
            "date_to": max(r.date for r in graded),
            "graded_rows": len(graded),
        },
        "overall": {
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "win_rate": (wins / (wins + losses)) if (wins + losses) else None,
            "staked": staked,
            "pnl": pnl,
            "roi": (pnl / staked) if staked else None,
        },
        "by_day": _day_summary(graded)["days"],
        "by_side_line": _build_side_line_stats(graded),
    }

    rows_with_model = sum(1 for r in wl if r.raq_model_pct is not None)
    rows_with_market = sum(1 for r in wl if r.raq_market_pct is not None)
    report["raq_extraction"] = {
        "wl_rows": len(wl),
        "rows_with_model_pct": rows_with_model,
        "rows_with_market_pct": rows_with_market,
        "model_coverage": (rows_with_model / len(wl)) if wl else None,
        "market_coverage": (rows_with_market / len(wl)) if wl else None,
    }

    raq_prob_rows = [
        {"y": 1 if r.grade == "win" else 0, "model_prob": (r.raq_model_pct / 100.0) if r.raq_model_pct is not None else None}
        for r in wl
        if r.raq_model_pct is not None
    ]
    report["raq_model_calibration"] = {
        "overall": _metrics_prob(raq_prob_rows, "model_prob"),
        "bins": _calibration_bins(raq_prob_rows, "model_prob"),
    }

    # Reconcile to candidate cards by date/name/side/line.
    cards_dir = Path(args.cards_dir)
    matched: list[dict[str, Any]] = []
    card_coverage: list[dict[str, Any]] = []
    for date in sorted(set(r.date for r in graded)):
        card_path = cards_dir / f"nhl_sog_card_{date}.csv"
        day_rows = [r for r in wl if r.date == date]
        if not card_path.exists():
            card_coverage.append({"date": date, "card_found": False, "wl_rows": len(day_rows), "matched_rows": 0})
            continue

        idx, collisions = _load_card_index(card_path)
        day_matched = 0
        for r in day_rows:
            found: CardRow | None = None
            for k in _name_keys(r.player_name):
                arr = idx.get((k, r.side, r.line))
                if arr:
                    found = arr[0]
                    break
            if found is None:
                continue
            day_matched += 1
            matched.append(
                {
                    "date": date,
                    "side": r.side,
                    "line": r.line,
                    "y": 1 if r.grade == "win" else 0,
                    "model_prob": found.model_side_prob,
                    "market_prob": found.market_side_prob,
                    "edge_side": found.edge_side,
                    "ev_side": found.ev_side,
                    "raq_model_prob": (r.raq_model_pct / 100.0) if r.raq_model_pct is not None else None,
                    "raq_market_prob": (r.raq_market_pct / 100.0) if r.raq_market_pct is not None else None,
                }
            )

        card_coverage.append(
            {
                "date": date,
                "card_found": True,
                "wl_rows": len(day_rows),
                "matched_rows": day_matched,
                "match_rate": (day_matched / len(day_rows)) if day_rows else None,
                "name_key_collisions": collisions,
            }
        )

    by_side: dict[str, dict[str, Any]] = {}
    for side in ("over", "under"):
        sub = [r for r in matched if r["side"] == side]
        by_side[side] = {"model": _metrics_prob(sub, "model_prob"), "market": _metrics_prob(sub, "market_prob")}

    by_line: dict[str, dict[str, Any]] = {}
    for line in sorted(set(r["line"] for r in matched)):
        key = f"{line:.1f}"
        sub = [r for r in matched if r["line"] == line]
        by_line[key] = {"model": _metrics_prob(sub, "model_prob"), "market": _metrics_prob(sub, "market_prob")}

    delta_model = [
        (r["raq_model_prob"] - r["model_prob"])
        for r in matched
        if isinstance(r.get("raq_model_prob"), (float, int))
    ]
    delta_market = [
        (r["raq_market_prob"] - r["market_prob"])
        for r in matched
        if isinstance(r.get("raq_market_prob"), (float, int))
    ]

    report["card_reconciliation"] = {
        "coverage_by_day": card_coverage,
        "matched_rows_total": len(matched),
        "model_vs_market": {
            "overall_model": _metrics_prob(matched, "model_prob"),
            "overall_market": _metrics_prob(matched, "market_prob"),
            "by_side": by_side,
            "by_line": by_line,
        },
        "raq_vs_card_delta": {
            "model_mean_delta": (sum(delta_model) / len(delta_model)) if delta_model else None,
            "model_mae": (sum(abs(x) for x in delta_model) / len(delta_model)) if delta_model else None,
            "market_mean_delta": (sum(delta_market) / len(delta_market)) if delta_market else None,
            "market_mae": (sum(abs(x) for x in delta_market) / len(delta_market)) if delta_market else None,
        },
    }

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"[audit] rows={len(graded)} wl_rows={len(wl)}")
    print(f"[audit] overall W-L-P={wins}-{losses}-{pushes} roi={(pnl / staked) * 100.0:.2f}%")
    print(f"[audit] card matched rows={len(matched)}")
    print(f"[audit] wrote {out_path}")


if __name__ == "__main__":
    main()
