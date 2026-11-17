#!/usr/bin/env python3
"""SOG: what sits behind the listed / unlisted mid-band misses? (NHL Backlog P2.60 follow-on, 2026-11-16)

Research only, read-only (operator: "go ahead and check if there is something there"). P2.60 found,
in both the 2026-01..04 holdout and live 2026-10-25..11-14, listed players in the middle shot-rate
fifth underpredicted at Over 1.5 and unlisted players in the fourth fifth overpredicted. Here:

1. Profile: by fifth x listed, mean inputs and recent-role measures the model does not carry
   (last-game TOI, last-3 TOI, last-game SOG, PP1 last game, TOI trend) and mean residual.
2. Residual vs each role measure within the model's middle fifths (Q2-Q4): does a recent-role
   change explain the miss, with or without the listed flag?
3. Fit test: V2 recipe + the role measures (holdout fit before 2026-01-01; live fit on all
   2024-25 + 2025-26), paired log loss vs the same recipe without them, and on live two-sided lines.
   The listed flag itself is reported as a diagnostic only (it is the books' choice, not an input).
"""
from __future__ import annotations

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

OUT = ROOT / "artifacts/analysis/nhl/sog_listed_band_profile_2026-11-16"
KEYS = ["game_id", "player_id"]
ROLE = ["toi_last1", "toi_last3", "toi_trend_1v10", "sog_last1", "att_last3"]
BANDS = ["Q1 low", "Q2", "Q3", "Q4", "Q5 high"]


def role_inputs(f: pd.DataFrame) -> pd.DataFrame:
    f = f.sort_values(["player_id", "game_date", "game_id"]).copy()
    g = f.groupby("player_id")
    f["toi_last1"] = g.toi_minutes.shift(1)
    f["toi_last3"] = g.toi_minutes.transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    f["sog_last1"] = g.shots_on_goal.shift(1)
    f["att_last3"] = g.shot_attempts.transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    f["toi_trend_1v10"] = f.toi_last1 - f.toi_avg_10
    return f


def profile(f: pd.DataFrame, pred: str) -> pd.DataFrame:
    f = f.assign(p15=poisson.sf(1, f[pred].to_numpy()), y15=(f.shots_on_goal >= 2).astype(float),
                 band=pd.qcut(f[pred].rank(method="first"), 5, labels=BANDS))
    cols = ["is_defense", "toi_avg_10", "toi_avg_5", "pp_toi_avg_10", "sog60_10", "games_season"] + ROLE + ["pp1_last_game"]
    rows = []
    for (b, lst), g in f.groupby(["band", "listed"], observed=True):
        r = {"band": b, "listed": bool(lst), "n": len(g), "over15_gap": g.y15.mean() - g.p15.mean(),
             "sog_resid": (g.shots_on_goal - g[pred]).mean()}
        r.update({c: g[c].mean() for c in cols})
        rows.append(r)
    return pd.DataFrame(rows)


def residual_by_role(f: pd.DataFrame, pred: str) -> pd.DataFrame:
    f = f.assign(p15=poisson.sf(1, f[pred].to_numpy()), y15=(f.shots_on_goal >= 2).astype(float),
                 band=pd.qcut(f[pred].rank(method="first"), 5, labels=BANDS))
    mid = f[f.band.isin(["Q2", "Q3", "Q4"])]
    rows = []
    for c in ["toi_trend_1v10", "toi_last1", "sog_last1", "pp1_last_game"]:
        q = pd.qcut(mid[c].rank(method="first"), 4, labels=["low", "2", "3", "high"])
        for (k, lst), g in mid.groupby([q, "listed"], observed=True):
            rows.append({"measure": c, "quartile": str(k), "listed": bool(lst), "n": len(g), "mean": g[c].mean(),
                         "over15_gap": g.y15.mean() - g.p15.mean(),
                         "se": float(np.sqrt(g.p15.mean() * (1 - g.p15.mean()) / len(g)))})
    return pd.DataFrame(rows)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    logs, pos = research.pull()
    frame = research.build(logs, pos)
    frame["game_id"], frame["player_id"] = frame.game_id.astype("int64"), frame.player_id.astype("int64")
    f = role_inputs(abl.add_features(frame, abl.situation_table())).merge(glp.inputs(), on=KEYS, how="left")
    ok = f.toi_avg_10.notna() & f.shrunk_lambda_180.notna()
    tr = f[f.season.isin([2024, 2025]) & ok]
    cut, end = pd.Timestamp(abl.HOLDOUT_START), pd.Timestamp(abl.HOLDOUT_END)
    early, hold = tr[tr.game_date < cut], tr[(tr.game_date >= cut) & (tr.game_date < end)].copy()
    sets = {"A": FEATURES, "ROLE": FEATURES + ROLE, "ROLE_PPU": FEATURES + ROLE + shadow.PPU}
    for k, cols in sets.items():
        hold[k] = abl._fit(early, cols).predict(hold[cols].astype(float).to_numpy())
    # Listed flag (diagnostic): any book quoted the player's SOG in the 7 pm ET snapshot.
    import os
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SELECT player_id::bigint, full_name FROM nhl.players")
        names = dict(cur.fetchall())
    hold["slate_date"] = hold.game_date.dt.strftime("%Y-%m-%d")
    hold["who"] = hold.player_id.map(names).map(lp.norm)
    lst = sl.listed_sog()
    hold = hold[hold.slate_date.isin(set(lst.slate_date))]
    hold = hold.merge(lst.assign(listed=True), on=["slate_date", "who"], how="left")
    hold["listed"] = hold.listed.astype("boolean").fillna(False).astype(bool)
    profile(hold, "A").round(4).to_csv(OUT / "holdout_profile.csv", index=False)
    residual_by_role(hold, "A").round(4).to_csv(OUT / "holdout_residual_by_role.csv", index=False)
    th = glp.scores(hold.assign(game_date=hold.game_date), list(sets), "A")
    th.round(5).to_csv(OUT / "holdout_fit_test.csv", index=False)
    # Live.
    live = f[f.season.eq(2026)].merge(pd.read_csv(sl.LIVE_ROWS)[KEYS + ["V2"]], on=KEYS, how="inner").copy()
    priced = pd.read_csv(sl.PRICED)
    priced = priced[priced.lane.eq("sog")][KEYS].drop_duplicates().assign(listed=True)
    live = live.merge(priced, on=KEYS, how="left")
    live["listed"] = live.listed.astype("boolean").fillna(False).astype(bool)
    for k, cols in sets.items():
        live[k] = abl._fit(tr, cols).predict(live[cols].astype(float).to_numpy())
    profile(live, "V2").round(4).to_csv(OUT / "live_profile.csv", index=False)
    residual_by_role(live, "V2").round(4).to_csv(OUT / "live_residual_by_role.csv", index=False)
    tl = glp.scores(live, ["V2"] + list(sets), "V2")
    tl.round(5).to_csv(OUT / "live_fit_test.csv", index=False)
    live["game_date"] = pd.to_datetime(live.game_date)
    mk = glp.market_rows(sorted(live.game_date.dt.strftime("%Y-%m-%d").unique())).merge(live, on=KEYS)
    yy = (mk.shots_on_goal > mk.line).astype(float).to_numpy()
    t = mk.line.astype(int).to_numpy()
    vs = {"rows": int(len(mk)), "market": float(base.log_loss_rows(mk.market_p_over.to_numpy(), yy).mean())}
    for k in ["V2"] + list(sets):
        vs[k] = float(base.log_loss_rows(poisson.sf(t, mk[k].to_numpy()), yy).mean())
    summary = {"holdout_rows": int(len(hold)), "live_rows": int(len(live)), "live_two_sided": vs}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
    for name in ("holdout_profile", "live_profile", "holdout_residual_by_role", "live_residual_by_role",
                 "holdout_fit_test", "live_fit_test"):
        print(name); print(pd.read_csv(OUT / f"{name}.csv").round(3).to_string(index=False))
    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
