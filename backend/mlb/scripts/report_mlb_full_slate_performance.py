#!/usr/bin/env python3
"""Summarize full-slate MLB model-pick performance from reconcile rows."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd


def _norm_outcome(value: Any) -> str:
    return str(value if value is not None else "").strip().lower()


def _fmt_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value * 100.0:.2f}%"


def _summarize(df: pd.DataFrame) -> dict[str, Any]:
    outcomes = df["actual_model_pick_outcome"].map(_norm_outcome)
    wins = int((outcomes == "win").sum())
    losses = int((outcomes == "loss").sum())
    pushes = int((outcomes == "push").sum())
    non_push = wins + losses
    pnl = pd.to_numeric(df.get("pnl_model_pick_1u"), errors="coerce").fillna(0.0)
    rows = int(len(df))
    return {
        "rows": rows,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "model_win_rate": (wins / non_push) if non_push else None,
        "model_roi": (float(pnl.sum()) / rows) if rows else None,
        "pnl_model_pick_1u": float(pnl.sum()),
    }


def build_summary(rows_csv: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    df = pd.read_csv(rows_csv)
    if "actual_model_pick_outcome" not in df.columns:
        raise SystemExit(f"{rows_csv} missing actual_model_pick_outcome")
    if "pnl_model_pick_1u" not in df.columns:
        raise SystemExit(f"{rows_csv} missing pnl_model_pick_1u")
    if "prop_type" not in df.columns:
        raise SystemExit(f"{rows_csv} missing prop_type")

    graded = df[df["actual_model_pick_outcome"].map(_norm_outcome).isin({"win", "loss", "push"})].copy()
    overall = _summarize(graded)

    rows = []
    for prop_type, group in graded.groupby("prop_type", dropna=False):
        row = _summarize(group)
        row["prop_type"] = str(prop_type)
        rows.append(row)
    by_prop = pd.DataFrame(rows)
    if not by_prop.empty:
        by_prop = by_prop[
            [
                "prop_type",
                "rows",
                "wins",
                "losses",
                "pushes",
                "model_win_rate",
                "model_roi",
                "pnl_model_pick_1u",
            ]
        ].sort_values(["model_roi", "model_win_rate", "rows"], ascending=[False, False, False])
    return by_prop, overall


def write_markdown(*, out_md: Path, rows_csv: Path, by_prop: pd.DataFrame, overall: dict[str, Any]) -> None:
    date_label = "unknown"
    try:
        sample = pd.read_csv(rows_csv, usecols=["game_date"], nrows=1)
        if not sample.empty:
            date_label = str(sample.iloc[0]["game_date"])
    except Exception:
        pass

    lines: list[str] = [
        f"# Full Slate Model Performance - {date_label}",
        "",
        f"Input: `{rows_csv}`",
        "",
        "Filtered to rows with `actual_model_pick_outcome` present.",
        "",
        "## Overall",
        "",
        f"- Rows: {overall['rows']:,}",
        f"- Wins/Losses/Pushes: {overall['wins']:,}-{overall['losses']:,}-{overall['pushes']:,}",
        f"- Model win rate: {_fmt_pct(overall['model_win_rate'])}",
        f"- Model ROI: {_fmt_pct(overall['model_roi'])}",
        f"- Model PnL, 1u per pick: {overall['pnl_model_pick_1u']:.4f}",
        "",
        "## By Prop",
        "",
    ]

    if by_prop.empty:
        lines.append("No resolved prop rows.")
    else:
        lines.extend(
            [
                "| prop_type | rows | wins | losses | pushes | win_rate | roi |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for _, row in by_prop.iterrows():
            lines.append(
                "| {prop_type} | {rows:,} | {wins:,} | {losses:,} | {pushes:,} | {win_rate} | {roi} |".format(
                    prop_type=row["prop_type"],
                    rows=int(row["rows"]),
                    wins=int(row["wins"]),
                    losses=int(row["losses"]),
                    pushes=int(row["pushes"]),
                    win_rate=_fmt_pct(row["model_win_rate"]),
                    roi=_fmt_pct(row["model_roi"]),
                )
            )
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows-csv", required=True)
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-by-prop-csv", required=True)
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv).expanduser()
    out_md = Path(args.out_md).expanduser()
    out_by_prop = Path(args.out_by_prop_csv).expanduser()

    by_prop, overall = build_summary(rows_csv)
    out_by_prop.parent.mkdir(parents=True, exist_ok=True)
    by_prop.to_csv(out_by_prop, index=False)
    write_markdown(out_md=out_md, rows_csv=rows_csv, by_prop=by_prop, overall=overall)

    print(f"[full-slate] rows_csv={rows_csv}")
    print(f"[full-slate] resolved_rows={overall['rows']}")
    print(f"[full-slate] model_win_rate={overall['model_win_rate']}")
    print(f"[full-slate] model_roi={overall['model_roi']}")
    print(f"[full-slate] out_md={out_md}")
    print(f"[full-slate] out_by_prop_csv={out_by_prop}")


if __name__ == "__main__":
    main()
