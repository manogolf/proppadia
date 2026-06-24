#!/usr/bin/env python3
"""Compare starter-only vs team-level pitcher tiers for hits 1.5 review boards."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.mlb.scripts import run_mlb_hits_15_tier_backtest as base


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/mlb/review_aids"
SUMMARY_CSV = OUT_DIR / "hits_15_pitcher_tier_source_audit.csv"
REPORT_MD = OUT_DIR / "hits_15_pitcher_tier_source_audit.md"
DETAIL_CSV = OUT_DIR / "hits_15_pitcher_tier_source_detail.csv"
SUMMARY_JSON = OUT_DIR / "hits_15_pitcher_tier_source_audit.json"
WINDOWS = ("full_history", "last_30", "last_14", "last_7", "latest_completed_slate")
TIER_ORDER = {"A": 0, "B": 1, "C": 2, "D": 3, "U": 4}


def _f(value: Any) -> float | None:
    return base._f(value)


def _pct(value: Any) -> str:
    val = _f(value)
    return "n/a" if val is None else f"{val * 100.0:.2f}%"


def _num(value: Any, digits: int = 2) -> str:
    val = _f(value)
    return "n/a" if val is None else f"{val:.{digits}f}"


def _md_table(df: pd.DataFrame, max_rows: int | None = None) -> str:
    if df.empty:
        return "_No rows._"
    work = df.head(max_rows).copy() if max_rows else df.copy()
    for col in work.columns:
        if col in {"wr", "roi", "roi_delta_vs_starter", "roi_delta_vs_team", "coverage_rate"}:
            work[col] = work[col].map(_pct)
        elif col.startswith("avg_") or col in {"units"}:
            work[col] = work[col].map(_num)
    work = work.fillna("n/a").astype(str)
    lines = [
        "| " + " | ".join(work.columns) + " |",
        "| " + " | ".join(["---"] * len(work.columns)) + " |",
    ]
    for _, row in work.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("|", "\\|") for col in work.columns) + " |")
    return "\n".join(lines)


def _team_pitcher_tier(expected: float | None, board: str) -> str:
    if expected is None:
        return "U"
    if board == "u15":
        if expected < 8.0:
            return "A"
        if expected < 8.5:
            return "B"
        if expected < 9.0:
            return "C"
        return "D"
    if expected >= 9.0:
        return "A"
    if expected >= 8.5:
        return "B"
    if expected >= 8.0:
        return "C"
    return "D"


def _starter_pitcher_tier(expected: float | None, board: str) -> str:
    return base._u15_pitcher_tier(expected) if board == "u15" else base._o15_pitcher_tier(expected)


def _hybrid_tier(starter_tier: str, team_tier: str) -> str:
    if starter_tier == "U" or team_tier == "U":
        return "U"
    # Conservative consensus: a row receives the weaker of the two support tiers.
    return starter_tier if TIER_ORDER[starter_tier] >= TIER_ORDER[team_tier] else team_tier


def _hitter_tier(row: dict[str, Any], board: str) -> str:
    d7 = _f(row.get("d7_hits_rate"))
    d15 = _f(row.get("d15_hits_rate"))
    return base._u15_hitter_tier(d7, d15) if board == "u15" else base._o15_hitter_tier(d7, d15)


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for r in rows if r.get("result") == "win")
    losses = sum(1 for r in rows if r.get("result") == "loss")
    pushes = sum(1 for r in rows if r.get("result") == "push")
    resolved = wins + losses + pushes
    units = sum(float(r.get("units") or 0.0) for r in rows if r.get("result") in {"win", "loss", "push"})

    def avg(col: str) -> float | None:
        vals = [_f(r.get(col)) for r in rows]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    if resolved == 0:
        sample_warning = "no_rows"
    elif resolved < 10:
        sample_warning = "small_sample_lt_10"
    elif resolved < 25:
        sample_warning = "small_sample_lt_25"
    else:
        sample_warning = "ok"

    return {
        "rows": len(rows),
        "resolved": resolved,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else math.nan,
        "roi": units / resolved if resolved else math.nan,
        "units": units,
        "avg_odds": avg("price"),
        "avg_d7_hits_rate": avg("d7_hits_rate"),
        "avg_d15_hits_rate": avg("d15_hits_rate"),
        "avg_starter_expected_hits_allowed": avg("starter_expected_hits_allowed"),
        "avg_team_expected_hits_allowed": avg("team_expected_hits_allowed"),
        "placed_rows": sum(1 for r in rows if r.get("placed")),
        "sample_warning": sample_warning,
    }


def _window_labels(date_text: str, latest: str) -> list[str]:
    return base._window_labels(date_text, latest)


def _load_analysis_rows(execution_root: Path, actual_root: Path) -> tuple[list[dict[str, Any]], str]:
    rows = base._load_reconcile_rows(execution_root)
    placed = base._load_placed_flags(actual_root)
    latest = max([str(r.get("date") or "") for r in rows], default="")
    out: list[dict[str, Any]] = []
    for row in rows:
        side = str(row.get("side") or "")
        if side not in {"over", "under"}:
            continue
        board = "o15" if side == "over" else "u15"
        item = dict(row)
        item["board"] = board
        item["placed"] = base._key(item.get("date"), item.get("player_id"), item.get("line"), item.get("side")) in placed
        item["placed_status"] = "placed" if item["placed"] else "unplaced"
        item["hitter_tier"] = _hitter_tier(item, board)
        starter_tier = _starter_pitcher_tier(_f(item.get("starter_expected_hits_allowed")), board)
        team_tier = _team_pitcher_tier(_f(item.get("team_expected_hits_allowed")), board)
        item["starter_pitcher_tier"] = starter_tier
        item["team_pitcher_tier"] = team_tier
        item["hybrid_pitcher_tier"] = _hybrid_tier(starter_tier, team_tier)
        item["starter_combined_tier"] = f"{item['hitter_tier']}/{starter_tier}"
        item["team_combined_tier"] = f"{item['hitter_tier']}/{team_tier}"
        item["hybrid_combined_tier"] = f"{item['hitter_tier']}/{item['hybrid_pitcher_tier']}"
        out.append(item)
    return out, latest


def _summarize(rows: list[dict[str, Any]], latest: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    modes = {
        "starter_only": ("starter_pitcher_tier", "starter_combined_tier"),
        "team_level": ("team_pitcher_tier", "team_combined_tier"),
        "hybrid_consensus": ("hybrid_pitcher_tier", "hybrid_combined_tier"),
    }
    dimensions = {
        "overall": None,
        "combined_tier": "combined",
        "hitter_tier": "hitter_tier",
        "pitcher_tier": "pitcher",
        "placed_status": "placed_status",
    }
    for board in ("o15", "u15"):
        board_rows = [r for r in rows if r.get("board") == board]
        for window in WINDOWS:
            wrows = [r for r in board_rows if window in _window_labels(str(r.get("date") or ""), latest)]
            for mode, (pitcher_col, combined_col) in modes.items():
                for dimension, field in dimensions.items():
                    if dimension == "overall":
                        groups = [("all", wrows)]
                    elif field == "combined":
                        groups = sorted({str(r.get(combined_col)) for r in wrows})
                        groups = [(g, [r for r in wrows if str(r.get(combined_col)) == g]) for g in groups]
                    elif field == "pitcher":
                        groups = sorted({str(r.get(pitcher_col)) for r in wrows})
                        groups = [(g, [r for r in wrows if str(r.get(pitcher_col)) == g]) for g in groups]
                    else:
                        values = sorted({str(r.get(field)) for r in wrows})
                        groups = [(g, [r for r in wrows if str(r.get(field)) == g]) for g in values]
                    if dimension == "overall":
                        iterable = groups
                    else:
                        iterable = groups
                    for tier, part in iterable:
                        metric = _metrics(part)
                        records.append(
                            {
                                "board": board,
                                "window": window,
                                "tier_mode": mode,
                                "dimension": dimension,
                                "tier": tier,
                                **metric,
                            }
                        )
    return pd.DataFrame(records)


def _mode_comparison(summary: pd.DataFrame) -> pd.DataFrame:
    overall = summary[(summary["dimension"] == "overall") & (summary["tier"] == "all")].copy()
    rows: list[dict[str, Any]] = []
    for (board, window), group in overall.groupby(["board", "window"], dropna=False):
        starter = group[group["tier_mode"] == "starter_only"]
        team = group[group["tier_mode"] == "team_level"]
        hybrid = group[group["tier_mode"] == "hybrid_consensus"]
        if starter.empty or team.empty or hybrid.empty:
            continue
        best = group.sort_values(["roi", "resolved"], ascending=[False, False]).head(1).iloc[0]
        rows.append(
            {
                "board": board,
                "window": window,
                "starter_roi": starter["roi"].iloc[0],
                "team_roi": team["roi"].iloc[0],
                "hybrid_roi": hybrid["roi"].iloc[0],
                "starter_wr": starter["wr"].iloc[0],
                "team_wr": team["wr"].iloc[0],
                "hybrid_wr": hybrid["wr"].iloc[0],
                "resolved": starter["resolved"].iloc[0],
                "best_mode": best["tier_mode"],
                "best_roi": best["roi"],
            }
        )
    return pd.DataFrame(rows)


def _top_combined(summary: pd.DataFrame) -> pd.DataFrame:
    rows = summary[
        (summary["dimension"] == "combined_tier")
        & (summary["window"].isin(["full_history", "last_30", "last_14", "last_7"]))
        & (summary["resolved"] >= 10)
    ].copy()
    if rows.empty:
        return rows
    return rows.sort_values(["board", "window", "roi", "resolved"], ascending=[True, True, False, False])


def _write_outputs(summary: pd.DataFrame, detail_rows: list[dict[str, Any]], latest: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    detail = pd.DataFrame(detail_rows)
    keep_detail = [
        "date",
        "board",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "side",
        "price",
        "result",
        "units",
        "d7_hits_rate",
        "d15_hits_rate",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "hitter_tier",
        "starter_pitcher_tier",
        "team_pitcher_tier",
        "hybrid_pitcher_tier",
        "starter_combined_tier",
        "team_combined_tier",
        "hybrid_combined_tier",
        "placed_status",
    ]
    detail[[c for c in keep_detail if c in detail.columns]].to_csv(DETAIL_CSV, index=False)
    summary.to_csv(SUMMARY_CSV, index=False)
    mode_cmp = _mode_comparison(summary)
    top = _top_combined(summary)

    # Add a compact machine-readable payload.
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_completed_slate": latest,
        "scope": "analysis_only_review_board_pitcher_tier_source_audit",
        "outputs": {
            "summary_csv": str(SUMMARY_CSV.relative_to(ROOT)),
            "detail_csv": str(DETAIL_CSV.relative_to(ROOT)),
            "report_md": str(REPORT_MD.relative_to(ROOT)),
            "summary_json": str(SUMMARY_JSON.relative_to(ROOT)),
        },
        "mode_comparison": mode_cmp.to_dict(orient="records"),
    }
    SUMMARY_JSON.write_text(json.dumps(payload, indent=2, default=str) + "\n")

    overall = summary[(summary["dimension"] == "overall") & (summary["tier"] == "all")].copy()
    overall = overall[
        [
            "board",
            "window",
            "tier_mode",
            "rows",
            "resolved",
            "wins",
            "losses",
            "pushes",
            "wr",
            "roi",
            "units",
            "avg_odds",
            "avg_starter_expected_hits_allowed",
            "avg_team_expected_hits_allowed",
        ]
    ]
    lines = [
        "# Hits 1.5 Pitcher Tier Source Audit",
        "",
        f"- Latest completed slate: `{latest}`",
        "- Scope: analysis only. No review-board threshold, production selector, upload, grading, or matching changes.",
        "- `starter_only` uses current board pitcher tiers from `starter_expected_hits_allowed`.",
        "- `team_level` uses starter + bullpen context (`team_expected_hits_allowed`) with analysis-only tiers: o1.5 A/B/C at `>=9.0`, `>=8.5`, `>=8.0`; u1.5 A/B/C at `<8.0`, `<8.5`, `<9.0`.",
        "- `hybrid_consensus` uses the weaker of starter-only and team-level pitcher support tiers.",
        "",
        "## Overall Mode Comparison",
        "",
        _md_table(overall),
        "",
        "## Mode Winner By Window",
        "",
        _md_table(mode_cmp),
        "",
        "## Combined Tier Comparison",
        "",
        "Top combined tiers with at least 10 resolved rows.",
        "",
        _md_table(
            top[
                [
                    "board",
                    "window",
                    "tier_mode",
                    "tier",
                    "rows",
                    "resolved",
                    "wr",
                    "roi",
                    "units",
                    "avg_odds",
                    "avg_starter_expected_hits_allowed",
                    "avg_team_expected_hits_allowed",
                    "sample_warning",
                ]
            ],
            max_rows=80,
        ),
        "",
        "## Answer",
        "",
    ]
    for board in ("o15", "u15"):
        board_cmp = mode_cmp[mode_cmp["board"] == board]
        if board_cmp.empty:
            lines.append(f"- `{board}`: insufficient rows to compare modes.")
            continue
        full = board_cmp[board_cmp["window"] == "full_history"]
        last30 = board_cmp[board_cmp["window"] == "last_30"]
        full_best = full["best_mode"].iloc[0] if not full.empty else "n/a"
        last30_best = last30["best_mode"].iloc[0] if not last30.empty else "n/a"
        lines.append(
            f"- `{board}`: full-history best mode is `{full_best}`; last-30 best mode is `{last30_best}`. "
            "Use this as review evidence only, not a threshold change."
        )
    lines.extend(
        [
            "",
            "## Files",
            "",
            f"- Summary CSV: `{SUMMARY_CSV.relative_to(ROOT)}`",
            f"- Detail CSV: `{DETAIL_CSV.relative_to(ROOT)}`",
            f"- JSON: `{SUMMARY_JSON.relative_to(ROOT)}`",
        ]
    )
    REPORT_MD.write_text("\n".join(lines) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execution-root", default="artifacts/analysis/mlb/execution_vs_model")
    parser.add_argument("--actual-root", default="backend/mlb/exports/model_v2/reconcile")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows, latest = _load_analysis_rows(ROOT / args.execution_root, ROOT / args.actual_root)
    summary = _summarize(rows, latest)
    _write_outputs(summary, rows, latest)
    mode_cmp = _mode_comparison(summary)
    print(f"latest_completed_slate={latest}")
    print(f"analysis_rows={len(rows)}")
    if not mode_cmp.empty:
        for _, row in mode_cmp[mode_cmp["window"].isin(["full_history", "last_30", "last_14", "last_7"])].iterrows():
            print(
                f"{row['board']} {row['window']} best_mode={row['best_mode']} "
                f"starter_roi={_pct(row['starter_roi'])} team_roi={_pct(row['team_roi'])} hybrid_roi={_pct(row['hybrid_roi'])}"
            )
    print(f"summary={SUMMARY_CSV.relative_to(ROOT)}")
    print(f"report={REPORT_MD.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
