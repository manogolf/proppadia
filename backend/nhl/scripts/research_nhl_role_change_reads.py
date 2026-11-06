"""Grade the input watch's role-change reads against outcomes (NHL Backlog P2.44).

For each slate, rebuilds the role-change section from the last Points capture
before the first puck drop, with market quotes up to one minute before it,
then scores model vs market P(Over) at SOG 1.5 and Points 0.5 by read. Use
only dates on the current models (SOG HGB from 2026-10-26, Points goals+
assists from 2026-11-01). Read-only; writes under ``tmp/``.

    ./bin/python backend/nhl/scripts/research_nhl_role_change_reads.py \
        --start 2026-10-26 --end 2026-11-05 --points-start 2026-11-01
"""
from __future__ import annotations

import argparse
import glob
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd

from backend.nhl.input_watch import ROLE_MARKET_GAP, ROOT, run_input_watch

OPS = ROOT / "artifacts/operational/nhl"


def rebuild(day: str, out: Path) -> list[dict]:
    caps = sorted(glob.glob(str(OPS / f"points_hgb_shadow/season=2026/slate_date={day}/run_id=*/phase=*/captured_at=*")),
                  key=lambda c: re.search(r"captured_at=(\S+)", c).group(1))
    if not caps:
        return []
    first = pd.to_datetime(pd.read_csv(Path(caps[0]) / "features.csv").game_start_utc, utc=True).min()
    stamp = lambda c: pd.Timestamp(re.search(r"captured_at=(\d{8}T\d{6})", c).group(1), tz="UTC")
    before = [c for c in caps if stamp(c) < first]
    if not before:
        return []
    cap = before[-1]
    run = re.search(r"run_id=([^/]+)", cap).group(1)
    result = run_input_watch(
        slate_date=day, run_id=run, prediction_run_dir=ROOT / f"backend/nhl/data/processed/daily_runs/{run}",
        points_capture_dir=Path(cap), operational_root=OPS,
        prior_date=(pd.Timestamp(day) - pd.Timedelta(days=1)).strftime("%Y-%m-%d"), output_root=out,
        now_utc=(first - pd.Timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    return [{**p, "slate_date": day} for p in result["role_changes"]["players"]]


def outcomes(days) -> pd.DataFrame:
    frames = []
    for day in days:
        paths = sorted(glob.glob(str(OPS / f"postgame_reconciliation/{day}/reconciliation=*/canonical_skater_outcomes.csv")),
                       key=os.path.getmtime)
        if paths:
            frames.append(pd.read_csv(paths[-1]).assign(slate_date=day))
    frame = pd.concat(frames)
    return frame[frame.participation_state.eq("PARTICIPATED")]


def log_loss(p: pd.Series, y: pd.Series) -> float:
    p = p.clip(1e-6, 1 - 1e-6)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean())


def grade(roles: pd.DataFrame, outs: pd.DataFrame, *, sog_start: str, points_start: str) -> pd.DataFrame:
    merged = roles.merge(outs[["slate_date", "player_id", "official_points", "official_sog"]],
                         on=["slate_date", "player_id"], how="inner")
    rows = []
    for lane, line, column, start in (("sog", 1.5, "official_sog", sog_start),
                                      ("points", 0.5, "official_points", points_start)):
        x = merged[(merged.slate_date >= start) & merged[f"{lane}_market_p_over"].notna()].copy()
        x["y"] = (x[column] > line).astype(int)
        x["model"], x["market"] = x[f"{lane}_model_p_over"], x[f"{lane}_market_p_over"]
        gap = x.model - x.market
        behind = ((x.direction == "UP") & (gap < 0)) | ((x.direction == "DOWN") & (gap > 0))
        x["read"] = np.select([gap.abs() < ROLE_MARKET_GAP, x.direction == "MIXED", behind],
                              ["agree", "mixed, gap 5+", "model behind"], "model ahead")
        groups = list(x.groupby("read")) + [("gap 5+ (all)", x[gap.abs() >= ROLE_MARKET_GAP]), ("ALL", x)]
        for read, part in groups:
            rows.append({"lane": lane, "read": read, "n": len(part), "players": part.player_id.nunique(),
                         "over_rate": part.y.mean(), "model": part.model.mean(), "market": part.market.mean(),
                         "log_loss_model": log_loss(part.model, part.y),
                         "log_loss_market": log_loss(part.market, part.y)})
    return pd.DataFrame(rows).round(4)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--points-start", required=True, help="first date on the current Points model")
    parser.add_argument("--out", type=Path, default=ROOT / "tmp/role_change_reads")
    args = parser.parse_args()
    days = pd.date_range(args.start, args.end).strftime("%Y-%m-%d")
    roles = pd.DataFrame([row for day in days for row in rebuild(day, args.out / "watch")])
    table = grade(roles, outcomes(days), sog_start=args.start, points_start=args.points_start)
    args.out.mkdir(parents=True, exist_ok=True)
    table.to_csv(args.out / "role_change_reads.csv", index=False)
    print(table.to_string(index=False))


if __name__ == "__main__":
    main()
