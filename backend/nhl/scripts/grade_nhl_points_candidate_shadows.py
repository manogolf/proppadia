#!/usr/bin/env python3
"""Grade the Points candidate shadow arms against production and the market (NHL Backlog P2.58).

Per slate: the last Points capture before the first puck drop that has arm predictions;
production's predictions from the same capture; the market's median no-vig P(Over) on
two-sided lines (the weekly evaluation's cache: quotes before the prediction); outcomes from
the postgame reconciliation (participants). Log loss at 0.5 / 1.5 / 2.5 for every arm and
production on the same player-games, the paired difference (arm minus production, negative =
better), and on two-sided Points lines the market's log loss beside them. Read-only.

    ./bin/python backend/nhl/scripts/grade_nhl_points_candidate_shadows.py --start 2026-11-16 --end 2026-11-22
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_candidate_shadows as shadow  # noqa: E402

KEYS = ["game_id", "player_id"]
LINES = (0.5, 1.5, 2.5)
OUT = ROOT / "artifacts/analysis/nhl/points_candidate_shadow_grades"
POSTGAME = ROOT / "artifacts/operational/nhl/postgame_reconciliation"


def log_loss(p: np.ndarray, y: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def slate_rows(day: str) -> pd.DataFrame:
    stamp = lambda c: pd.Timestamp(re.search(r"captured_at=(\d{8}T\d{6})", str(c)).group(1), tz="UTC")  # noqa: E731
    caps = [c for c in shadow.captures_for(day) if (c / "predictions.csv").is_file()
            and any(shadow.output_dir(c, a.name.split("=", 1)[1]).joinpath("predictions.csv").is_file()
                    for a in shadow.arms())]
    if not caps:
        return pd.DataFrame()
    first = pd.to_datetime(pd.read_csv(caps[0] / "features.csv").game_start_utc, utc=True).min()
    before = [c for c in caps if stamp(c) < first]
    if not before:
        return pd.DataFrame()
    cap = max(before, key=stamp)
    frame = pd.read_csv(cap / "predictions.csv")[KEYS + ["expected_points"]].rename(columns={"expected_points": "PRODUCTION"})
    for a in shadow.arms():
        name = a.name.split("=", 1)[1]
        f = shadow.output_dir(cap, name) / "predictions.csv"
        if f.is_file():
            frame = frame.merge(pd.read_csv(f)[KEYS + ["expected_points"]].rename(columns={"expected_points": name}),
                                on=KEYS, how="left")
    files = sorted((POSTGAME / day).glob("*/canonical_skater_outcomes.csv"))
    if not files:
        return pd.DataFrame()
    o = pd.read_csv(files[-1])
    o = o[o.participation_state.astype(str).str.upper().str.startswith("PARTICIPA")]
    frame = frame.merge(o[KEYS + ["official_points"]].astype("int64"), on=KEYS, how="inner")
    return frame.assign(slate_date=day)


def market(days: list[str]) -> pd.DataFrame:
    from backend.nhl import two_sided_lines
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    loaded: list = []

    def lc():
        if not loaded:
            loaded.append(two_sided_lines.candidate_export.load_candidates(weekly.OPERATIONAL)[0])
        return loaded[0]

    rows = []
    for day in days:
        for g, p, line, mp in (weekly._cached_two_sided_keys(day, lc).get("points") or []):
            if mp is not None:
                rows.append((int(g), int(p), float(line), float(mp)))
    return pd.DataFrame(rows, columns=KEYS + ["line", "market_p_over"])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()
    days = pd.date_range(args.start, args.end).strftime("%Y-%m-%d").tolist()
    frame = pd.concat([slate_rows(d) for d in days], ignore_index=True)
    if frame.empty:
        print("NHL_POINTS_CANDIDATE_GRADES=" + json.dumps({"status": "NO_GRADED_ROWS", "days": days}))
        return 0
    names = ["PRODUCTION"] + [a.name.split("=", 1)[1] for a in shadow.arms() if a.name.split("=", 1)[1] in frame]
    frame = frame.dropna(subset=names)
    rows = []
    for line in LINES:
        y = (frame.official_points > line).astype(float).to_numpy()
        base = log_loss(poisson.sf(int(line), frame.PRODUCTION.to_numpy()), y)
        for k in names:
            ll = log_loss(poisson.sf(int(line), frame[k].to_numpy()), y)
            rows.append({"set": "all player-games", "line": line, "model": k, "n": len(frame), "log_loss": ll.mean(),
                         "diff_vs_production_x1000": 1e3 * (ll - base).mean()})
    mk = frame.merge(market(sorted(frame.slate_date.unique())), on=KEYS, how="inner")
    for line, g in mk.groupby("line"):
        y = (g.official_points > line).astype(float).to_numpy()
        rows.append({"set": "two-sided lines", "line": line, "model": "MARKET", "n": len(g),
                     "log_loss": log_loss(g.market_p_over.to_numpy(), y).mean()})
        for k in names:
            rows.append({"set": "two-sided lines", "line": line, "model": k, "n": len(g),
                         "log_loss": log_loss(poisson.sf(int(line), g[k].to_numpy()), y).mean()})
    t = pd.DataFrame(rows)
    out = OUT / f"{args.start}_{args.end}"
    out.mkdir(parents=True, exist_ok=True)
    t.round(5).to_csv(out / "grades.csv", index=False)
    summary = {"days": days, "slates_graded": int(frame.slate_date.nunique()), "player_games": int(len(frame)),
               "two_sided_rows": int(len(mk)), "arms": names[1:]}
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print("NHL_POINTS_CANDIDATE_GRADES=" + json.dumps(summary))
    print(t.round(4).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
