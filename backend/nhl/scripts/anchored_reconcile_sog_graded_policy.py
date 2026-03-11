#!/usr/bin/env python3
"""Run anchored daily reconcile of graded NHL SOG wagers vs baseline/toggle policies.

For each day in [anchor_from, anchor_to]:
  - Build baseline card from historical market snapshot
  - Build toggled card (cap over:3.5)
  - Build toggled card (disable over:3.5)
  - Match graded placed wagers to each card by (player short key, side, line)
  - Summarize alignment coverage and realized ROI on matched subset
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

GRADER_RE = re.compile(r"^nhl_sog_graded_(\d{4}-\d{2}-\d{2})\.csv$")


@dataclass(frozen=True)
class VariantSpec:
    name: str
    out_suffix: str
    extra_args: list[str]


def _norm_name(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\\s]", " ", s)
    s = re.sub(r"\\s+", " ", s).strip()
    return s


def _short_key(name: str) -> str:
    parts = _norm_name(name).split()
    if not parts:
        return ""
    return f"{parts[0][0]} {parts[-1]}"


def _parse_date(s: str) -> date:
    return date.fromisoformat(str(s))


def _date_span(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _latest_graded_date(graded_dir: Path) -> str | None:
    dates: list[str] = []
    if not graded_dir.exists():
        return None
    for fp in graded_dir.iterdir():
        m = GRADER_RE.match(fp.name)
        if m:
            dates.append(m.group(1))
    if not dates:
        return None
    return sorted(dates)[-1]


def _run_selector(
    *,
    market_csv: Path,
    policy_json: Path,
    game_date: str,
    out_csv: Path,
    out_json: Path,
    extra_args: list[str],
) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "backend/nhl/scripts/select_sog_candidates_live.py",
        "--market-csv",
        str(market_csv),
        "--policy-json",
        str(policy_json),
        "--game-date",
        str(game_date),
        "--out-csv",
        str(out_csv),
        "--out-json",
        str(out_json),
    ] + list(extra_args)
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def _prep_card(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    for need in ("full_name", "model_pick", "line"):
        if need not in df.columns:
            raise RuntimeError(f"card csv missing column '{need}': {path}")
    out = df.copy()
    out["player_key"] = out["full_name"].astype(str).map(_short_key)
    out["side"] = out["model_pick"].astype(str).str.lower().str.strip()
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out = out[out["side"].isin(["over", "under"]) & out["line"].notna()].copy()
    return out[["player_key", "side", "line"]].drop_duplicates().reset_index(drop=True)


def _prep_graded(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path).copy()
    for need in ("player_name", "side", "line", "grade", "amount", "pnl"):
        if need not in df.columns:
            raise RuntimeError(f"graded csv missing column '{need}': {path}")
    out = df.copy()
    out["side"] = out["side"].astype(str).str.lower().str.strip()
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    out["pnl"] = pd.to_numeric(out["pnl"], errors="coerce")
    out["player_key"] = out["player_name"].astype(str).map(_short_key)
    out = out[out["side"].isin(["over", "under"]) & out["line"].notna()].copy()
    return out.reset_index(drop=True)


def _subset_stats(df: pd.DataFrame, selected_col: str) -> dict[str, Any]:
    sub = df[df[selected_col] == 1].copy()
    if sub.empty:
        return {
            "matched_wagers": 0,
            "match_rate": 0.0,
            "wins": 0,
            "losses": 0,
            "staked": 0.0,
            "pnl": 0.0,
            "roi": None,
            "win_rate": None,
        }
    wins = int((sub["grade"].astype(str).str.lower() == "win").sum())
    losses = int((sub["grade"].astype(str).str.lower() == "loss").sum())
    staked = float(sub["amount"].sum())
    pnl = float(sub["pnl"].sum())
    return {
        "matched_wagers": int(len(sub)),
        "match_rate": float(len(sub) / max(1, len(df))),
        "wins": wins,
        "losses": losses,
        "staked": staked,
        "pnl": pnl,
        "roi": (float(pnl / staked) if staked else None),
        "win_rate": float(wins / len(sub)),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--anchor-from", default="2026-03-04", help="Start date YYYY-MM-DD.")
    ap.add_argument(
        "--anchor-to",
        default="",
        help="End date YYYY-MM-DD. Default: latest graded day in --graded-dir.",
    )
    ap.add_argument("--graded-dir", default="tmp/graded")
    ap.add_argument("--odds-history-root", default="backend/nhl/exports/odds_history")
    ap.add_argument("--policy-json", default="tmp/nhl_sog_walkforward_summary.json")
    ap.add_argument("--out-root", default="tmp/analysis/anchored_reconcile")
    args = ap.parse_args()

    graded_dir = Path(args.graded_dir)
    odds_root = Path(args.odds_history_root)
    policy_json = Path(args.policy_json)
    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    if not policy_json.exists():
        raise SystemExit(f"policy json not found: {policy_json}")

    anchor_from = _parse_date(args.anchor_from)
    anchor_to_raw = str(args.anchor_to).strip()
    if anchor_to_raw:
        anchor_to = _parse_date(anchor_to_raw)
    else:
        latest = _latest_graded_date(graded_dir)
        if not latest:
            raise SystemExit(f"no graded files found in {graded_dir}")
        anchor_to = _parse_date(latest)

    if anchor_to < anchor_from:
        raise SystemExit(f"anchor-to {anchor_to} is before anchor-from {anchor_from}")

    variants = [
        VariantSpec(name="baseline", out_suffix="baseline", extra_args=[]),
        VariantSpec(
            name="toggles_cap",
            out_suffix="toggles",
            extra_args=[
                "--segment-min-model-prob",
                "under:1.5=0.65",
                "--segment-max-price",
                "under:1.5=100",
                "--segment-min-ev-override",
                "over:2.5=0.15",
                "--segment-min-gap-override",
                "over:2.5=0.07",
                "--segment-min-ev-override",
                "under:2.5=0.19",
                "--segment-min-gap-override",
                "under:2.5=0.10",
                "--segment-min-ev-override",
                "under:3.5=0.20",
                "--segment-min-gap-override",
                "under:3.5=0.10",
                "--segment-max-price",
                "over:3.5=130",
            ],
        ),
        VariantSpec(
            name="toggles_disable_over35",
            out_suffix="toggles_disable_over35",
            extra_args=[
                "--segment-min-model-prob",
                "under:1.5=0.65",
                "--segment-max-price",
                "under:1.5=100",
                "--segment-min-ev-override",
                "over:2.5=0.15",
                "--segment-min-gap-override",
                "over:2.5=0.07",
                "--segment-min-ev-override",
                "under:2.5=0.19",
                "--segment-min-gap-override",
                "under:2.5=0.10",
                "--segment-min-ev-override",
                "under:3.5=0.20",
                "--segment-min-gap-override",
                "under:3.5=0.10",
                "--segment-disable",
                "over:3.5",
            ],
        ),
    ]

    day_rows: list[dict[str, Any]] = []
    all_reconcile_rows: list[pd.DataFrame] = []

    for day in _date_span(anchor_from, anchor_to):
        ds = day.isoformat()
        graded_csv = graded_dir / f"nhl_sog_graded_{ds}.csv"
        market_csv = odds_root / ds / "sog_with_market.csv"
        day_dir = out_root / ds
        day_dir.mkdir(parents=True, exist_ok=True)

        if not graded_csv.exists() or not market_csv.exists():
            day_rows.append(
                {
                    "date": ds,
                    "ok": False,
                    "reason": (
                        "missing graded csv" if not graded_csv.exists() else "missing market snapshot"
                    ),
                    "graded_csv": str(graded_csv),
                    "market_csv": str(market_csv),
                }
            )
            continue

        variant_cards: dict[str, pd.DataFrame] = {}
        card_rows: dict[str, int] = {}
        for v in variants:
            out_csv = day_dir / f"nhl_sog_card_{ds}_{v.out_suffix}.csv"
            out_json = day_dir / f"nhl_sog_card_{ds}_{v.out_suffix}_summary.json"
            _run_selector(
                market_csv=market_csv,
                policy_json=policy_json,
                game_date=ds,
                out_csv=out_csv,
                out_json=out_json,
                extra_args=v.extra_args,
            )
            cdf = _prep_card(out_csv)
            variant_cards[v.name] = cdf
            card_rows[v.name] = int(len(pd.read_csv(out_csv)))

        graded = _prep_graded(graded_csv)
        joined = graded.copy()
        for v in variants:
            mark_col = f"sel_{v.name}"
            joined = joined.merge(
                variant_cards[v.name].assign(**{mark_col: 1}),
                on=["player_key", "side", "line"],
                how="left",
            )
            joined[mark_col] = joined[mark_col].fillna(0).astype(int)

        placed_wins = int((joined["grade"].astype(str).str.lower() == "win").sum())
        placed_losses = int((joined["grade"].astype(str).str.lower() == "loss").sum())
        placed_staked = float(joined["amount"].sum())
        placed_pnl = float(joined["pnl"].sum())

        day_rec: dict[str, Any] = {
            "date": ds,
            "ok": True,
            "placed_wagers": int(len(joined)),
            "placed_staked": placed_staked,
            "placed_pnl": placed_pnl,
            "placed_roi": (float(placed_pnl / placed_staked) if placed_staked else None),
            "placed_wins": placed_wins,
            "placed_losses": placed_losses,
            "card_rows": card_rows,
        }
        for v in variants:
            day_rec[v.name] = _subset_stats(joined, f"sel_{v.name}")
        day_rows.append(day_rec)

        joined["date"] = ds
        all_reconcile_rows.append(joined)

    valid = [r for r in day_rows if r.get("ok")]

    def _agg_variant(name: str) -> dict[str, Any]:
        matched = sum(int(r[name]["matched_wagers"]) for r in valid)
        wins = sum(int(r[name]["wins"]) for r in valid)
        losses = sum(int(r[name]["losses"]) for r in valid)
        staked = float(sum(float(r[name]["staked"]) for r in valid))
        pnl = float(sum(float(r[name]["pnl"]) for r in valid))
        return {
            "matched_wagers": int(matched),
            "wins": int(wins),
            "losses": int(losses),
            "win_rate": (float(wins / matched) if matched else None),
            "staked": staked,
            "pnl": pnl,
            "roi": (float(pnl / staked) if staked else None),
        }

    placed_wagers = sum(int(r["placed_wagers"]) for r in valid)
    placed_wins = sum(int(r["placed_wins"]) for r in valid)
    placed_losses = sum(int(r["placed_losses"]) for r in valid)
    placed_staked = float(sum(float(r["placed_staked"]) for r in valid))
    placed_pnl = float(sum(float(r["placed_pnl"]) for r in valid))

    summary: dict[str, Any] = {
        "anchor_start": anchor_from.isoformat(),
        "anchor_end": anchor_to.isoformat(),
        "days_analyzed": int(len(valid)),
        "days_total_requested": int(len(day_rows)),
        "days": day_rows,
        "aggregate": {
            "placed": {
                "wagers": int(placed_wagers),
                "wins": int(placed_wins),
                "losses": int(placed_losses),
                "win_rate": (float(placed_wins / placed_wagers) if placed_wagers else None),
                "staked": placed_staked,
                "pnl": placed_pnl,
                "roi": (float(placed_pnl / placed_staked) if placed_staked else None),
            },
            "baseline_alignment": _agg_variant("baseline"),
            "toggles_cap_alignment": _agg_variant("toggles_cap"),
            "toggles_disable_over35_alignment": _agg_variant("toggles_disable_over35"),
        },
        "outputs": {
            "summary_json": str(out_root / "anchored_reconcile_summary.json"),
            "rows_csv": str(out_root / "anchored_reconcile_rows.csv"),
        },
    }

    out_summary = out_root / "anchored_reconcile_summary.json"
    out_summary.write_text(json.dumps(summary, indent=2))
    if all_reconcile_rows:
        pd.concat(all_reconcile_rows, ignore_index=True).to_csv(
            out_root / "anchored_reconcile_rows.csv",
            index=False,
        )

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

