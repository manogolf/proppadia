#!/usr/bin/env python3
"""SOG: does "a book lists this player's SOG" help V2 as an input? (NHL Backlog P2.60, 2026-11-16)

Research only, read-only (operator: "test using books listed"). Timing matters: the live model runs
in the morning, so the live flag is the morning listing (the observation the Points shadow uses for
game lines, `points_candidate_shadows.morning_observation`, by 06:30 PT). History has only the
7 pm ET snapshot (`backend/nhl/exports/odds_history`, from 2025-10-07), so training uses the
evening listing; the live window measures how often a morning listing matches the day's final one.

  A        V2 recipe (sog_hgb_features.FEATURES)
  LISTED   A + listed (0/1)
  LISTED_N A + number of books listing the player

Holdout: fit on 2025-26 rows before 2026-01-01 (the only season with listings), score 2026-01..04.
Live: fit on all 2025-26 rows, score 2026-10-25..11-14 with the morning flag (and, as a check, the
end-of-day flag), beside V2's own predictions and on two-sided lines beside the market.
"""
from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_candidate_shadows as shadow  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_listed_players as lp  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_game_lines_ppunit as glp  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_hgb_count_model as research  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_listed_players as sl  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_situation_ablation as abl  # noqa: E402
from backend.nhl.sog_hgb_features import FEATURES  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/sog_listed_input_2026-11-16"
KEYS = ["game_id", "player_id"]
SOG_MARKETS = ("player_shots_on_goal", "player_shots_on_goal_alternate")
SETS = {"A": FEATURES, "LISTED": FEATURES + ["listed"], "LISTED_N": FEATURES + ["listed_books"]}


def history_listings() -> pd.DataFrame:
    """Evening (7 pm ET snapshot) listings, every date in odds_history: slate_date, who, books."""
    rows = []
    for d in sorted(glob.glob(str(lp.ODDS / "20*"))):
        p = Path(d) / "odds_event_wrappers.json"
        if not p.is_file():
            continue
        for w in json.loads(p.read_text()):
            for b in (w.get("data") or {}).get("bookmakers", []):
                for m in b.get("markets", []):
                    if m["key"] in SOG_MARKETS:
                        for o in m["outcomes"]:
                            rows.append((Path(d).name, lp.norm(o.get("description")), b["key"]))
    t = pd.DataFrame(rows, columns=["slate_date", "who", "book"]).drop_duplicates()
    return t.groupby(["slate_date", "who"]).book.nunique().rename("books").reset_index()


def live_listings(day: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Morning listing (shadow's morning observation) and end-of-day listing (every pregame observation)."""
    def read(paths):
        parts = [pd.read_json(p, lines=True) for p in paths]
        t = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["market", "description", "bookmaker"])
        t = t[t.market.isin(SOG_MARKETS)]
        t = t.assign(who=t.description.map(lp.norm))
        return t.groupby("who").bookmaker.nunique().rename("books").reset_index().assign(slate_date=day)
    morning = shadow.morning_observation(day)
    d = shadow.ODDS_ROOT / f"season={shadow.season_of(day)}" / f"slate_date={day}"
    return read([morning] if morning else []), read(sorted(d.glob("observation=*/normalized_odds.jsonl")))


def attach(f: pd.DataFrame, listing: pd.DataFrame) -> pd.DataFrame:
    f = f.merge(listing, on=["slate_date", "who"], how="left")
    f["listed_books"] = f.books.fillna(0)
    f["listed"] = (f.listed_books > 0).astype(float)
    return f.drop(columns="books")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    logs, pos = research.pull()
    frame = research.build(logs, pos)
    frame["game_id"], frame["player_id"] = frame.game_id.astype("int64"), frame.player_id.astype("int64")
    f = abl.add_features(frame, abl.situation_table())
    import os
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SELECT player_id::bigint, full_name FROM nhl.players")
        names = dict(cur.fetchall())
    f["slate_date"] = f.game_date.dt.strftime("%Y-%m-%d")
    f["who"] = f.player_id.map(names).map(lp.norm)
    ok = f.toi_avg_10.notna() & f.shrunk_lambda_180.notna()
    hist = history_listings()
    s25 = f[f.season.eq(2025) & ok & f.slate_date.isin(set(hist.slate_date))]
    s25 = attach(s25, hist)
    match = {"history_names_matched_share": float(hist.merge(f[["slate_date", "who"]].drop_duplicates(), on=["slate_date", "who"]).shape[0] / len(hist[hist.slate_date < "2026-05-01"]))}
    cut, end = pd.Timestamp(abl.HOLDOUT_START), pd.Timestamp(abl.HOLDOUT_END)
    early, hold = s25[s25.game_date < cut], s25[(s25.game_date >= cut) & (s25.game_date < end)].copy()
    for k, cols in SETS.items():
        hold[k] = abl._fit(early, cols).predict(hold[cols].astype(float).to_numpy())
    th = glp.scores(hold, list(SETS), "A")
    th.round(5).to_csv(OUT / "holdout_2026_jan_apr.csv", index=False)
    # Live: morning and end-of-day listings.
    live = f[f.season.eq(2026)].merge(pd.read_csv(sl.LIVE_ROWS)[KEYS + ["V2"]], on=KEYS, how="inner")
    mornings, finals = zip(*(live_listings(d) for d in sorted(live.slate_date.unique())))
    morning, final = pd.concat(mornings, ignore_index=True), pd.concat(finals, ignore_index=True)
    lm, lf = attach(live, morning), attach(live, final)
    agree = {"live_rows": int(len(lm)), "morning_listed_share": float(lm.listed.mean()), "final_listed_share": float(lf.listed.mean()),
             "final_listed_also_morning": float(lm.listed[lf.listed.eq(1).to_numpy()].mean()),
             "morning_listed_also_final": float(lf.listed[lm.listed.eq(1).to_numpy()].mean()),
             "slates_without_morning_observation": [d for d in sorted(live.slate_date.unique())
                                                    if shadow.morning_observation(d) is None]}
    fit_all = s25
    models = {k: abl._fit(fit_all, cols) for k, cols in SETS.items()}
    res = {}
    for tag, lv in (("morning", lm), ("end_of_day", lf)):
        lv = lv.copy()
        for k, cols in SETS.items():
            lv[k] = models[k].predict(lv[cols].astype(float).to_numpy())
        tl = glp.scores(lv, ["V2"] + list(SETS), "V2")
        tl.round(5).to_csv(OUT / f"live_{tag}.csv", index=False)
        mk = glp.market_rows(sorted(lv.slate_date.unique())).merge(lv, on=KEYS)
        yy = (mk.shots_on_goal > mk.line).astype(float).to_numpy()
        t = mk.line.astype(int).to_numpy()
        vs = {"rows": int(len(mk)), "market": float(base.log_loss_rows(mk.market_p_over.to_numpy(), yy).mean())}
        for k in ["V2"] + list(SETS):
            vs[k] = float(base.log_loss_rows(poisson.sf(t, mk[k].to_numpy()), yy).mean())
        for k in ("LISTED", "LISTED_N"):
            d = base.paired_bootstrap(base.log_loss_rows(poisson.sf(t, mk[k].to_numpy()), yy),
                                      base.log_loss_rows(poisson.sf(t, mk.A.to_numpy()), yy), mk.slate_date.to_numpy())
            vs[f"{k}_minus_A_x1000"] = [round(d["mean_diff"] * 1e3, 2), [round(c * 1e3, 2) for c in d["ci95"]]]
        res[tag] = {"table": tl.round(4).to_dict("records"), "two_sided": vs}
    summary = {"holdout_rows": int(len(hold)), "train_rows_holdout": int(len(early)), "train_rows_live": int(len(fit_all)),
               "holdout_listed_share": float(hold.listed.mean()), **match, "live_listing_agreement": agree,
               "live_two_sided": {k: v["two_sided"] for k, v in res.items()}}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(th.round(4).to_string(index=False))
    for tag in res:
        print(tag); print(pd.DataFrame(res[tag]["table"]).to_string(index=False))
    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
