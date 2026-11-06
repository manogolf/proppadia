#!/usr/bin/env python3
"""Grade the Points per-minute shadow against production and the market (P2.46).

Per slate: the last Points capture before the first puck drop that has shadow
predictions; production and shadow P(Over) at 0.5/1.5/2.5 (Poisson on expected
points); the market's median same-book no-vig P(Over) on two-sided lines quoted
up to one minute before the first puck drop (quote matching reuses the input
watch); outcomes from the postgame reconciliation (participated only); role
change as the input watch defines it. Lower log loss is better. Read-only;
writes under ``tmp/``.

    ./bin/python backend/nhl/scripts/grade_nhl_points_per_minute_shadow.py --start 2026-11-01 --end 2026-11-05
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import input_watch as watch  # noqa: E402
from backend.nhl import points_per_minute_shadow as shadow  # noqa: E402

OPS = ROOT / "artifacts/operational/nhl"
LINES = (0.5, 1.5, 2.5)


def slate_rows(day: str) -> pd.DataFrame:
    caps = [c for c in shadow.captures_for(day) if (shadow.output_dir(c) / "predictions.csv").is_file()]
    if not caps:
        return pd.DataFrame()
    first = pd.to_datetime(pd.read_csv(caps[0] / "features.csv").game_start_utc, utc=True).min()
    stamp = lambda c: pd.Timestamp(re.search(r"captured_at=(\d{8}T\d{6})", str(c)).group(1), tz="UTC")
    before = [c for c in caps if stamp(c) < first]
    if not before:
        return pd.DataFrame()
    cap = max(before, key=stamp)
    features = pd.read_csv(cap / "features.csv")
    prod = pd.read_csv(cap / "predictions.csv")[shadow.KEYS + ["expected_points"]]
    shad = pd.read_csv(shadow.output_dir(cap) / "predictions.csv")[shadow.KEYS + ["expected_points"]]
    frame = features.merge(prod, on=shadow.KEYS).merge(shad, on=shadow.KEYS, suffixes=("_prod", "_shadow"))
    # Market P(Over) per line, with quotes up to one minute before the first puck drop.
    now = (first - pd.Timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ladder = frame.assign(**{f"prob_over_{str(l).replace('.', '_')}": poisson.sf(int(l), frame.expected_points_prod)
                             for l in LINES})
    starts = {int(g): str(s).replace(" ", "T") for g, s in zip(frame.game_id, frame.game_start_utc)}
    candidates = watch._candidates(ladder, market_key="player_points", lines=watch.POINTS_LINES, slate_date=day,
                                   starts=starts, now_utc=now, run_id="grade")
    quotes, _ = watch.candidate_export.load_retained_quotes(OPS, candidates, price_both_sides=True)
    market = watch._market(watch.candidate_export.deduplicate_quotes(quotes))
    cand = pd.DataFrame(candidates).merge(market, on="candidate_key", how="left")
    cand["market_p_over"] = np.where(cand.selected_side.eq("OVER"), cand.market_p, 1 - cand.market_p)
    cand = cand.assign(game_id=cand.game_id.astype("int64"), player_id=cand.player_id.astype("int64"),
                       line=cand.market_line.astype(float))[shadow.KEYS + ["line", "market_p_over"]]
    totals, _ = shadow.production_tables()
    roles = watch.role_changes(features, pd.read_csv(shadow.USAGE_PATH), shadow.season_of(day),
                               watch.prior_points_per_game())
    frame["role_change"] = frame.player_id.isin(set(roles.player_id) if len(roles) else set())
    paths = sorted(glob.glob(str(OPS / f"postgame_reconciliation/{day}/reconciliation=*/canonical_skater_outcomes.csv")),
                   key=os.path.getmtime)
    if not paths:
        return pd.DataFrame()
    outcomes = pd.read_csv(paths[-1])
    outcomes = outcomes[outcomes.participation_state.eq("PARTICIPATED")][["player_id", "official_points"]]
    rows = frame.merge(outcomes, on="player_id", how="inner")
    rows = pd.concat([rows.assign(line=l) for l in LINES], ignore_index=True).merge(cand, on=shadow.KEYS + ["line"],
                                                                                  how="left")
    rows["y"] = (rows.official_points > rows.line).astype(int)
    rows["p_prod"] = poisson.sf(rows.line.astype(int), rows.expected_points_prod)
    rows["p_shadow"] = poisson.sf(rows.line.astype(int), rows.expected_points_shadow)
    rows["slate_date"] = day
    return rows[["slate_date", *shadow.KEYS, "line", "role_change", "y", "p_prod", "p_shadow", "market_p_over"]]


def log_loss(p: pd.Series, y: pd.Series) -> np.ndarray:
    p = p.clip(1e-6, 1 - 1e-6).to_numpy()
    return -(y.to_numpy() * np.log(p) + (1 - y.to_numpy()) * np.log(1 - p))


def summarize(rows: pd.DataFrame) -> pd.DataFrame:
    two = rows.market_p_over.notna()
    groups = {"all": rows.index == rows.index, "role change": rows.role_change, "two-sided": two,
              "two-sided, role change": two & rows.role_change, "two-sided, no role change": two & ~rows.role_change}
    out = []
    for name, mask in groups.items():
        for line in ("all lines", *LINES):
            part = rows[mask & (rows.line.eq(line) if line != "all lines" else True)]
            if part.empty:
                continue
            ll_prod, ll_shadow = log_loss(part.p_prod, part.y), log_loss(part.p_shadow, part.y)
            row = {"group": name, "line": line, "n": len(part), "slates": part.slate_date.nunique(),
                   "over_rate": part.y.mean(), "production": part.p_prod.mean(), "shadow": part.p_shadow.mean(),
                   "log_loss_production": ll_prod.mean(), "log_loss_shadow": ll_shadow.mean(),
                   "shadow_minus_production_x1000": 1e3 * (ll_shadow - ll_prod).mean()}
            if part.market_p_over.notna().all():
                ll_market = log_loss(part.market_p_over, part.y)
                row |= {"market": part.market_p_over.mean(), "log_loss_market": ll_market.mean(),
                        "market_minus_production_x1000": 1e3 * (ll_market - ll_prod).mean()}
            out.append(row)
    return pd.DataFrame(out).round(4)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "tmp/points_per_minute_shadow_grades")
    args = parser.parse_args()
    days = pd.date_range(args.start, args.end).strftime("%Y-%m-%d")
    rows = pd.concat([slate_rows(d) for d in days], ignore_index=True)
    table = summarize(rows)
    args.out.mkdir(parents=True, exist_ok=True)
    rows.to_csv(args.out / "rows.csv", index=False)
    table.to_csv(args.out / "summary.csv", index=False)
    pd.set_option("display.width", 250)
    print(table[table.line.isin(["all lines", 0.5])].to_string(index=False))


if __name__ == "__main__":
    main()
