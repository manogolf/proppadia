#!/usr/bin/env python3
"""Build a unified NHL SOG truth scoreboard.

Scoreboard combines:
  1) Executable backtests for one or more threshold policies.
  2) Optional placed-stream alignment metrics from anchored reconcile output.

This makes the stream-coverage problem explicit by showing:
  - selected rows by policy
  - rows that had executable prices
  - executable match rate
alongside ROI (0c/5c) and win rate.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_STRATEGIES = [
    "baseline=tmp/nhl_sog_walkforward_threshold_history.csv",
    "exec_reconciled=tmp/analysis/walkforward_exec/nhl_sog_walkforward_exec_threshold_history.csv",
]


def _f(v: Any) -> float | None:
    try:
        if v is None or str(v).strip() == "":
            return None
        return float(v)
    except Exception:
        return None


def _parse_strategy(spec: str) -> tuple[str, Path, Path | None]:
    s = str(spec or "").strip()
    if "=" not in s:
        raise ValueError(
            f"Invalid --strategy '{spec}'. Expected NAME=THRESHOLD_HISTORY_CSV or NAME=THRESHOLD_HISTORY_CSV@ROWS_CSV"
        )
    name, rhs = s.split("=", 1)
    name = name.strip()
    rhs = rhs.strip()
    if "@" in rhs:
        fp, rows_fp = rhs.split("@", 1)
    else:
        fp, rows_fp = rhs, ""
    fp = fp.strip()
    rows_fp = rows_fp.strip()
    if not name:
        raise ValueError(f"Invalid --strategy '{spec}'. Empty strategy name.")
    if not fp:
        raise ValueError(f"Invalid --strategy '{spec}'. Empty threshold history path.")
    return name, Path(fp), (Path(rows_fp) if rows_fp else None)


def _run_cmd(cmd: list[str]) -> tuple[bool, str, str, int]:
    cp = subprocess.run(cmd, text=True, capture_output=True)
    return (cp.returncode == 0, cp.stdout or "", cp.stderr or "", int(cp.returncode))


def _collect_exec_row(
    *,
    strategy_name: str,
    threshold_history_csv: Path,
    rows_csv: Path,
    summary: dict[str, Any],
) -> dict[str, Any]:
    cfg = summary.get("config", {}) or {}
    cov = summary.get("coverage", {}) or {}
    scen = summary.get("scenarios", {}) or {}
    s0 = scen.get("slippage_0c", {}) or {}
    s5 = scen.get("slippage_5c", {}) or {}
    return {
        "stream": "executable_backtest",
        "strategy": strategy_name,
        "rows_csv": str(rows_csv),
        "threshold_history_csv": str(threshold_history_csv),
        "date_from": cfg.get("from_date"),
        "date_to": cfg.get("to_date"),
        "oot_start": cov.get("oot_start"),
        "oot_end": cov.get("oot_end"),
        "bets": s0.get("bets"),
        "wins": s0.get("wins"),
        "losses": s0.get("losses"),
        "win_rate": s0.get("win_rate"),
        "roi_0c": s0.get("roi"),
        "roi_5c": s5.get("roi"),
        "units_0c": s0.get("profit_units"),
        "units_5c": s5.get("profit_units"),
        "selected_rows": cov.get("rows_oot_selected_by_policy"),
        "executable_rows": cov.get("rows_oot_selected_with_exec_price"),
        "match_rate": cov.get("selected_exec_match_rate"),
        "status": "ok",
        "notes": "",
    }


def _collect_placed_rows(anchored_summary: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    agg = anchored_summary.get("aggregate", {}) or {}
    placed = agg.get("placed", {}) or {}
    placed_wagers = _f(placed.get("wagers")) or 0.0
    anchor_start = anchored_summary.get("anchor_start")
    anchor_end = anchored_summary.get("anchor_end")

    out.append(
        {
            "stream": "placed_truth",
            "strategy": "placed_actual",
            "rows_csv": None,
            "threshold_history_csv": None,
            "date_from": anchor_start,
            "date_to": anchor_end,
            "oot_start": anchor_start,
            "oot_end": anchor_end,
            "bets": placed.get("wagers"),
            "wins": placed.get("wins"),
            "losses": placed.get("losses"),
            "win_rate": placed.get("win_rate"),
            "roi_0c": placed.get("roi"),
            "roi_5c": None,
            "units_0c": placed.get("pnl"),
            "units_5c": None,
            "selected_rows": None,
            "executable_rows": None,
            "match_rate": 1.0 if placed_wagers > 0 else None,
            "status": "ok",
            "notes": "anchored_reconcile placed stream",
        }
    )

    for key, val in sorted(agg.items()):
        if not str(key).endswith("_alignment"):
            continue
        name = str(key).replace("_alignment", "")
        matched = _f(val.get("matched_wagers")) or 0.0
        mr = (matched / placed_wagers) if placed_wagers > 0 else None
        out.append(
            {
                "stream": "placed_alignment",
                "strategy": name,
                "rows_csv": None,
                "threshold_history_csv": None,
                "date_from": anchor_start,
                "date_to": anchor_end,
                "oot_start": anchor_start,
                "oot_end": anchor_end,
                "bets": val.get("matched_wagers"),
                "wins": val.get("wins"),
                "losses": val.get("losses"),
                "win_rate": val.get("win_rate"),
                "roi_0c": val.get("roi"),
                "roi_5c": None,
                "units_0c": val.get("pnl"),
                "units_5c": None,
                "selected_rows": placed.get("wagers"),
                "executable_rows": val.get("matched_wagers"),
                "match_rate": mr,
                "status": "ok",
                "notes": "anchored_reconcile matched subset",
            }
        )
    return out


def _sort_key(row: dict[str, Any]) -> tuple[Any, Any, Any]:
    stream = str(row.get("stream") or "")
    roi5 = _f(row.get("roi_5c"))
    roi0 = _f(row.get("roi_0c"))
    score = roi5 if roi5 is not None else roi0
    if score is None:
        score = -999.0
    return (stream, -float(score), str(row.get("strategy") or ""))


def main() -> None:
    ap = argparse.ArgumentParser(description="Build NHL SOG executable + placed truth scoreboard.")
    ap.add_argument("--rows-csv", default="tmp/nhl_sog_base_vs_betonline_rows.csv")
    ap.add_argument("--odds-root", default="backend/nhl/exports/odds_history")
    ap.add_argument("--bookmaker", default="betonlineag")
    ap.add_argument("--market-key", default="player_shots_on_goal")
    ap.add_argument("--warmup-days", type=int, default=30)
    ap.add_argument("--from-date", default="2025-10-07")
    ap.add_argument("--to-date", default="2026-03-12")
    ap.add_argument("--slippage-cents-grid", default="0,5,10")
    ap.add_argument(
        "--strategy",
        action="append",
        default=[],
        help=(
            "Repeatable NAME=THRESHOLD_HISTORY_CSV or "
            "NAME=THRESHOLD_HISTORY_CSV@ROWS_CSV. "
            "If omitted, uses baseline + exec_reconciled defaults."
        ),
    )
    ap.add_argument("--run-dir", default="tmp/analysis/scoreboard_runs")
    ap.add_argument("--anchored-summary-json", default="tmp/analysis/anchored_reconcile/anchored_reconcile_summary.json")
    ap.add_argument("--out-csv", default="tmp/analysis/sog_truth_scoreboard.csv")
    ap.add_argument("--out-json", default="tmp/analysis/sog_truth_scoreboard.json")
    args = ap.parse_args()

    strategies_raw = list(args.strategy or [])
    if not strategies_raw:
        strategies_raw = list(DEFAULT_STRATEGIES)
    strategies: list[tuple[str, Path, Path | None]] = [_parse_strategy(s) for s in strategies_raw]

    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []

    backtest_script = Path("backend/nhl/scripts/backtest_sog_primary_two_sided_execution.py")
    if not backtest_script.exists():
        raise SystemExit(f"missing backtest script: {backtest_script}")

    for name, thr_csv, rows_csv_override in strategies:
        rows_csv = Path(rows_csv_override) if rows_csv_override is not None else Path(args.rows_csv)
        if not thr_csv.exists():
            rows.append(
                {
                    "stream": "executable_backtest",
                    "strategy": name,
                    "rows_csv": str(rows_csv),
                    "threshold_history_csv": str(thr_csv),
                    "date_from": args.from_date,
                    "date_to": args.to_date,
                    "oot_start": None,
                    "oot_end": None,
                    "bets": None,
                    "wins": None,
                    "losses": None,
                    "win_rate": None,
                    "roi_0c": None,
                    "roi_5c": None,
                    "units_0c": None,
                    "units_5c": None,
                    "selected_rows": None,
                    "executable_rows": None,
                    "match_rate": None,
                    "status": "error",
                    "notes": f"missing threshold history csv: {thr_csv}",
                }
            )
            continue
        if not rows_csv.exists():
            rows.append(
                {
                    "stream": "executable_backtest",
                    "strategy": name,
                    "rows_csv": str(rows_csv),
                    "threshold_history_csv": str(thr_csv),
                    "date_from": args.from_date,
                    "date_to": args.to_date,
                    "oot_start": None,
                    "oot_end": None,
                    "bets": None,
                    "wins": None,
                    "losses": None,
                    "win_rate": None,
                    "roi_0c": None,
                    "roi_5c": None,
                    "units_0c": None,
                    "units_5c": None,
                    "selected_rows": None,
                    "executable_rows": None,
                    "match_rate": None,
                    "status": "error",
                    "notes": f"missing rows csv: {rows_csv}",
                }
            )
            continue

        out_sel = run_dir / f"{name}_selected.csv"
        out_sum = run_dir / f"{name}_summary.json"
        cmd = [
            sys.executable,
            str(backtest_script),
            "--rows-csv",
            str(rows_csv),
            "--threshold-history-csv",
            str(thr_csv),
            "--odds-root",
            str(args.odds_root),
            "--bookmaker",
            str(args.bookmaker),
            "--market-key",
            str(args.market_key),
            "--warmup-days",
            str(int(args.warmup_days)),
            "--from-date",
            str(args.from_date),
            "--to-date",
            str(args.to_date),
            "--slippage-cents-grid",
            str(args.slippage_cents_grid),
            "--out-selected-csv",
            str(out_sel),
            "--out-summary-json",
            str(out_sum),
        ]
        ok, _stdout, stderr, rc = _run_cmd(cmd)
        if (not ok) or (not out_sum.exists()):
            rows.append(
                {
                    "stream": "executable_backtest",
                    "strategy": name,
                    "rows_csv": str(rows_csv),
                    "threshold_history_csv": str(thr_csv),
                    "date_from": args.from_date,
                    "date_to": args.to_date,
                    "oot_start": None,
                    "oot_end": None,
                    "bets": None,
                    "wins": None,
                    "losses": None,
                    "win_rate": None,
                    "roi_0c": None,
                    "roi_5c": None,
                    "units_0c": None,
                    "units_5c": None,
                    "selected_rows": None,
                    "executable_rows": None,
                    "match_rate": None,
                    "status": "error",
                    "notes": f"backtest rc={rc} stderr_tail={(stderr or '').splitlines()[-1:]!r}",
                }
            )
            continue

        payload = json.loads(out_sum.read_text())
        rows.append(
            _collect_exec_row(
                strategy_name=name,
                threshold_history_csv=thr_csv,
                rows_csv=rows_csv,
                summary=payload,
            )
        )

    anchored_path = Path(args.anchored_summary_json)
    if anchored_path.exists():
        try:
            anchored_payload = json.loads(anchored_path.read_text())
            rows.extend(_collect_placed_rows(anchored_payload))
        except Exception as e:
            rows.append(
                {
                    "stream": "placed_alignment",
                    "strategy": "anchored_reconcile",
                    "rows_csv": None,
                    "threshold_history_csv": None,
                    "date_from": None,
                    "date_to": None,
                    "oot_start": None,
                    "oot_end": None,
                    "bets": None,
                    "wins": None,
                    "losses": None,
                    "win_rate": None,
                    "roi_0c": None,
                    "roi_5c": None,
                    "units_0c": None,
                    "units_5c": None,
                    "selected_rows": None,
                    "executable_rows": None,
                    "match_rate": None,
                    "status": "error",
                    "notes": f"failed to parse anchored summary: {e}",
                }
            )

    rows = sorted(rows, key=_sort_key)

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "stream",
        "strategy",
        "rows_csv",
        "threshold_history_csv",
        "date_from",
        "date_to",
        "oot_start",
        "oot_end",
        "bets",
        "wins",
        "losses",
        "win_rate",
        "roi_0c",
        "roi_5c",
        "units_0c",
        "units_5c",
        "selected_rows",
        "executable_rows",
        "match_rate",
        "status",
        "notes",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for row in rows:
            w.writerow({c: row.get(c) for c in columns})

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "config": {
            "rows_csv": str(args.rows_csv),
            "odds_root": str(args.odds_root),
            "bookmaker": str(args.bookmaker),
            "market_key": str(args.market_key),
            "warmup_days": int(args.warmup_days),
            "from_date": str(args.from_date),
            "to_date": str(args.to_date),
            "slippage_cents_grid": str(args.slippage_cents_grid),
            "strategies": [
                {
                    "name": n,
                    "threshold_history_csv": str(p),
                    "rows_csv": (str(r) if r is not None else str(args.rows_csv)),
                }
                for n, p, r in strategies
            ],
            "anchored_summary_json": str(args.anchored_summary_json),
            "run_dir": str(run_dir),
        },
        "rows": rows,
        "outputs": {"csv": str(out_csv), "json": str(out_json)},
    }
    out_json.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
