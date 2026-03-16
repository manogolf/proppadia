#!/usr/bin/env python3
"""Live truth gate for NHL SOG using graded placed wagers.

Purpose:
  - Score recent placed-and-graded results by segment (side:line).
  - Produce a pass/fail gate and recommended segment disables before upload.
  - Optionally replay gate day-by-day over a historical span.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import date, timedelta
from glob import glob
from pathlib import Path
from typing import Any


GRADED_RE = re.compile(r"nhl_sog_graded_(\d{4}-\d{2}-\d{2})\.csv$")


@dataclass
class GradedRow:
    date: str
    side: str
    line: float
    grade: str
    amount: float
    pnl: float
    model_side_prob: float | None


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        s = str(v if v is not None else "").strip().replace(",", "")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _to_opt_float(v: Any) -> float | None:
    try:
        s = str(v if v is not None else "").strip().replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _date_from_path(path: Path) -> str | None:
    m = GRADED_RE.search(path.name)
    return m.group(1) if m else None


def _segment_key(side: str, line: float) -> str:
    return f"{side}:{float(line):.1f}"


def _load_graded(paths: list[Path]) -> list[GradedRow]:
    out: list[GradedRow] = []
    for path in sorted(paths):
        dt = _date_from_path(path)
        if not dt:
            continue
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                side = str(row.get("side") or "").strip().lower()
                grade = str(row.get("grade") or "").strip().lower()
                line = _to_opt_float(row.get("line"))
                if side not in {"over", "under"} or line is None:
                    continue
                if grade not in {"win", "loss", "push"}:
                    continue

                # RAQ model pct is usually p_over on the tool side.
                raq_model_pct = _to_opt_float(row.get("raq_model_pct"))
                p_side: float | None = None
                if raq_model_pct is not None:
                    p_over = raq_model_pct / 100.0
                    if 0.0 < p_over < 1.0:
                        p_side = p_over if side == "over" else (1.0 - p_over)

                out.append(
                    GradedRow(
                        date=dt,
                        side=side,
                        line=float(line),
                        grade=grade,
                        amount=_to_float(row.get("amount")),
                        pnl=_to_float(row.get("pnl")),
                        model_side_prob=p_side,
                    )
                )
    return out


def _window_dates(anchor_from: str, anchor_to: str) -> list[str]:
    start = date.fromisoformat(anchor_from)
    end = date.fromisoformat(anchor_to)
    if end < start:
        return []
    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _latest_graded_date(rows: list[GradedRow]) -> str | None:
    if not rows:
        return None
    return max(r.date for r in rows)


def _calc_stats(
    rows: list[GradedRow],
    *,
    min_segment_bets: int,
    min_segment_roi: float,
    max_calibration_gap_abs: float,
    min_overall_roi: float,
) -> dict[str, Any]:
    wl = [r for r in rows if r.grade in {"win", "loss"}]
    if not wl:
        return {
            "rows": 0,
            "overall": {
                "bets": 0,
                "wins": 0,
                "losses": 0,
                "staked": 0.0,
                "pnl": 0.0,
                "roi": None,
                "win_rate": None,
                "avg_model_side_prob": None,
                "calibration_gap": None,
            },
            "segments": [],
            "recommendation": {
                "gate_pass": False,
                "reason": "no_graded_wl_rows",
                "recommended_segment_disable": [],
                "recommended_segment_enable": [],
                "recommended_segment_disable_args": [],
            },
        }

    wins = sum(1 for r in wl if r.grade == "win")
    losses = sum(1 for r in wl if r.grade == "loss")
    staked = sum(r.amount for r in wl)
    pnl = sum(r.pnl for r in wl)
    win_rate = (wins / (wins + losses)) if (wins + losses) else None
    roi = (pnl / staked) if staked else None

    pvals = [r.model_side_prob for r in wl if isinstance(r.model_side_prob, float)]
    avg_p = (sum(pvals) / len(pvals)) if pvals else None
    calib_gap = (avg_p - win_rate) if (avg_p is not None and win_rate is not None) else None

    seg_map: dict[str, list[GradedRow]] = {}
    for r in wl:
        key = _segment_key(r.side, r.line)
        seg_map.setdefault(key, []).append(r)

    seg_rows: list[dict[str, Any]] = []
    for seg in sorted(seg_map):
        sub = seg_map[seg]
        sbets = len(sub)
        swins = sum(1 for r in sub if r.grade == "win")
        slosses = sbets - swins
        sstaked = sum(r.amount for r in sub)
        spnl = sum(r.pnl for r in sub)
        sroi = (spnl / sstaked) if sstaked else None
        swin_rate = (swins / sbets) if sbets else None
        spvals = [r.model_side_prob for r in sub if isinstance(r.model_side_prob, float)]
        savg_p = (sum(spvals) / len(spvals)) if spvals else None
        sgap = (savg_p - swin_rate) if (savg_p is not None and swin_rate is not None) else None

        if sbets < int(min_segment_bets):
            status = "insufficient"
            reason = f"bets<{int(min_segment_bets)}"
        else:
            roi_ok = (sroi is not None) and (sroi >= float(min_segment_roi))
            calib_ok = True
            if sgap is not None:
                calib_ok = abs(sgap) <= float(max_calibration_gap_abs)
            status = "pass" if (roi_ok and calib_ok) else "fail"
            fails = []
            if not roi_ok:
                fails.append(f"roi<{float(min_segment_roi):.4f}")
            if not calib_ok:
                fails.append(f"|calib_gap|>{float(max_calibration_gap_abs):.4f}")
            reason = ",".join(fails) if fails else "ok"

        seg_rows.append(
            {
                "segment": seg,
                "bets": int(sbets),
                "wins": int(swins),
                "losses": int(slosses),
                "win_rate": swin_rate,
                "staked": float(sstaked),
                "pnl": float(spnl),
                "roi": sroi,
                "avg_model_side_prob": savg_p,
                "calibration_gap": sgap,
                "status": status,
                "reason": reason,
            }
        )

    disable = [r["segment"] for r in seg_rows if r["status"] == "fail"]
    enable = [r["segment"] for r in seg_rows if r["status"] == "pass"]
    overall_ok = (roi is not None) and (roi >= float(min_overall_roi))
    gate_pass = overall_ok and (len(disable) == 0)
    if gate_pass:
        reason = "overall_ok_and_no_failed_segments"
    else:
        reasons = []
        if not overall_ok:
            reasons.append(f"overall_roi<{float(min_overall_roi):.4f}")
        if disable:
            reasons.append(f"failed_segments={len(disable)}")
        reason = ",".join(reasons) if reasons else "failed"

    return {
        "rows": int(len(wl)),
        "overall": {
            "bets": int(len(wl)),
            "wins": int(wins),
            "losses": int(losses),
            "staked": float(staked),
            "pnl": float(pnl),
            "roi": roi,
            "win_rate": win_rate,
            "avg_model_side_prob": avg_p,
            "calibration_gap": calib_gap,
        },
        "segments": seg_rows,
        "recommendation": {
            "gate_pass": bool(gate_pass),
            "reason": reason,
            "recommended_segment_disable": disable,
            "recommended_segment_enable": enable,
            "recommended_segment_disable_args": [f"--segment-disable {s}" for s in disable],
        },
    }


def _window_filter(rows: list[GradedRow], anchor_from: str, anchor_to: str, window_days: int) -> list[GradedRow]:
    all_dates = _window_dates(anchor_from, anchor_to)
    if not all_dates:
        return []
    keep = set(all_dates[-int(max(1, window_days)) :])
    return [r for r in rows if r.date in keep]


def _history(
    rows: list[GradedRow],
    *,
    anchor_from: str,
    anchor_to: str,
    window_days: int,
    min_segment_bets: int,
    min_segment_roi: float,
    max_calibration_gap_abs: float,
    min_overall_roi: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    dates = _window_dates(anchor_from, anchor_to)
    for i, day in enumerate(dates):
        if i + 1 < int(max(1, window_days)):
            continue
        start = dates[i + 1 - int(max(1, window_days))]
        sub = [r for r in rows if start <= r.date <= day]
        stats = _calc_stats(
            sub,
            min_segment_bets=min_segment_bets,
            min_segment_roi=min_segment_roi,
            max_calibration_gap_abs=max_calibration_gap_abs,
            min_overall_roi=min_overall_roi,
        )
        out.append(
            {
                "window_start": start,
                "window_end": day,
                "bets": stats["overall"]["bets"],
                "roi": stats["overall"]["roi"],
                "gate_pass": stats["recommendation"]["gate_pass"],
                "failed_segments": len(stats["recommendation"]["recommended_segment_disable"]),
                "recommended_segment_disable": stats["recommendation"]["recommended_segment_disable"],
            }
        )
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="NHL SOG live truth gate from graded placed wagers.")
    ap.add_argument("--graded-glob", default="tmp/graded/nhl_sog_graded_*.csv")
    ap.add_argument("--anchor-from", default="2026-03-04")
    ap.add_argument("--anchor-to", default="", help="Default: latest graded date found.")
    ap.add_argument("--window-days", type=int, default=7)
    ap.add_argument("--min-segment-bets", type=int, default=20)
    ap.add_argument("--min-segment-roi", type=float, default=0.0)
    ap.add_argument("--max-calibration-gap-abs", type=float, default=0.08)
    ap.add_argument("--min-overall-roi", type=float, default=0.0)
    ap.add_argument("--out-json", default="tmp/analysis/nhl_sog_live_truth_gate.json")
    ap.add_argument("--out-history-csv", default="tmp/analysis/nhl_sog_live_truth_gate_history.csv")
    ap.add_argument("--emit-history", action="store_true")
    args = ap.parse_args()

    files = [Path(p) for p in sorted(glob(args.graded_glob)) if not p.endswith("_summary.json")]
    graded = _load_graded(files)
    if not graded:
        raise SystemExit("No graded rows found.")

    latest = _latest_graded_date(graded)
    if not latest:
        raise SystemExit("Could not resolve latest graded date.")
    anchor_to = str(args.anchor_to).strip() or latest
    if anchor_to < str(args.anchor_from):
        raise SystemExit(f"anchor-to {anchor_to} is before anchor-from {args.anchor_from}")

    in_span = [r for r in graded if str(args.anchor_from) <= r.date <= anchor_to]
    if not in_span:
        raise SystemExit("No graded rows in requested anchor range.")

    recent = _window_filter(in_span, str(args.anchor_from), anchor_to, int(args.window_days))
    stats = _calc_stats(
        recent,
        min_segment_bets=int(args.min_segment_bets),
        min_segment_roi=float(args.min_segment_roi),
        max_calibration_gap_abs=float(args.max_calibration_gap_abs),
        min_overall_roi=float(args.min_overall_roi),
    )

    payload: dict[str, Any] = {
        "config": {
            "graded_glob": str(args.graded_glob),
            "anchor_from": str(args.anchor_from),
            "anchor_to": anchor_to,
            "window_days": int(args.window_days),
            "min_segment_bets": int(args.min_segment_bets),
            "min_segment_roi": float(args.min_segment_roi),
            "max_calibration_gap_abs": float(args.max_calibration_gap_abs),
            "min_overall_roi": float(args.min_overall_roi),
        },
        "window": {
            "window_start": (
                _window_dates(str(args.anchor_from), anchor_to)[-int(max(1, args.window_days))]
                if _window_dates(str(args.anchor_from), anchor_to)
                else None
            ),
            "window_end": anchor_to,
            "rows_considered": int(len(recent)),
        },
        **stats,
        "outputs": {"summary_json": str(Path(args.out_json))},
    }

    if args.emit_history:
        hist = _history(
            in_span,
            anchor_from=str(args.anchor_from),
            anchor_to=anchor_to,
            window_days=int(args.window_days),
            min_segment_bets=int(args.min_segment_bets),
            min_segment_roi=float(args.min_segment_roi),
            max_calibration_gap_abs=float(args.max_calibration_gap_abs),
            min_overall_roi=float(args.min_overall_roi),
        )
        payload["history"] = {"rows": int(len(hist)), "csv": str(Path(args.out_history_csv))}
        out_hist = Path(args.out_history_csv)
        out_hist.parent.mkdir(parents=True, exist_ok=True)
        if hist:
            keys = [
                "window_start",
                "window_end",
                "bets",
                "roi",
                "gate_pass",
                "failed_segments",
                "recommended_segment_disable",
            ]
            with out_hist.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                for row in hist:
                    rec = dict(row)
                    rec["recommended_segment_disable"] = ",".join(rec.get("recommended_segment_disable") or [])
                    w.writerow(rec)

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

