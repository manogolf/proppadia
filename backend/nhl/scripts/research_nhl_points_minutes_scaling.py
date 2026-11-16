#!/usr/bin/env python3
"""Why Points expected points do not rise enough with minutes (NHL Backlog P2.46, 2026-11-16).

Research only, read-only. Production c940ae2f underpredicts points for 17+ minute and
role-up players and overpredicts under-13-minute players (P2.46 per-minute run). Its
own training fit is already compressed (lowest predicted decile 0.159 vs 0.120 actual,
highest 1.015 vs 1.064). Two causes are tested, each as a goals + assists pair on the
same corrected training frame (P2.53, 2023-24) as production:

  BASE        the c940ae2f recipe (reproduces production)
  CAP_300     same inputs, 300 boosting rounds (production: 100, 15 leaves, l2 1.0)
  CAP_300_31  300 rounds, 31 leaves
  K10         own/goal/assist rates shrunk to the league by 10 games instead of 30
  K10_CAP_300 both

Scored on the 2025 holdout and live 2026-11-01..11-15 (frames from
research_nhl_points_per_minute_corrected.py): paired log loss at 0.5/1.5/2.5,
calibration by predicted decile and by ice-time band, and on two-sided lines against
the market's no-vig price.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import research_nhl_points_goal_assist as ga  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import research_nhl_points_per_minute_corrected as pmc  # noqa: E402
from backend.nhl.scripts import research_nhl_points_pp_corrected_refit as ppfix  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_minutes_scaling_2026-11-16"
KEYS = ["game_id", "player_id"]
FEATS = list(freeze.FEATURES)
K10 = {"own_rate_k30": "own_rate_k10", "goal_rate": "goal_rate_k10", "assist_rate": "assist_rate_k10"}
# Shot-volume inputs fell about 9% from 2023-24 to 2025-26 while scoring did not; the model
# read the drop as less scoring. REL reads them relative to the league's level.
SHOTS = ("d10_sog_per60", "attempts_d10_per60", "team_d10_sf_per_game")
REL = {c: f"{c}_rel" for c in SHOTS}


def add_relative(frame: pd.DataFrame, fallback: dict[str, float]) -> pd.DataFrame:
    """Each shot input divided by the league mean of strictly earlier slates this season
    (all rows of those slates), or by the fallback (previous season's mean) before any."""
    f = frame.copy()
    f["_day"] = pd.to_datetime(f.slate_date if "slate_date" in f else f.game_date)
    f["_season"] = pmc.season_of(f._day)
    for c in SHOTS:
        v = pd.to_numeric(f[c], errors="coerce")
        daily = f.assign(_v=v).groupby(["_season", "_day"])._v.agg(["sum", "count"]).reset_index()
        daily = daily.sort_values(["_season", "_day"])
        g = daily.groupby("_season")
        daily["_mean"] = (g["sum"].cumsum() - daily["sum"]) / (g["count"].cumsum() - daily["count"])
        daily["_mean"] = daily._mean.where(g.cumcount() >= 7)  # at least a week of slates
        daily["_mean"] = daily._mean.fillna(daily._season.map(lambda s: fallback.get((s, c))))
        f = f.merge(daily[["_season", "_day", "_mean"]], on=["_season", "_day"], how="left")
        f[REL[c]] = v / f._mean
        f = f.drop(columns="_mean")
    return f.drop(columns=["_day", "_season"])


def fit(train: pd.DataFrame, features: list[str], **kw) -> dict:
    params = {**base.HGB_KW, **kw}
    out = {"structure": "GOALS_PLUS_ASSISTS", "feature_columns": features, "fill_zero": True}
    for target in ("goals", "assists"):
        x = base.x_matrix(train, features)
        scaler = StandardScaler().fit(x)
        model = HistGradientBoostingRegressor(**params).fit(scaler.transform(x), train[target].to_numpy(float))
        out[target] = {"feature_columns": features, "scaler": scaler, "model": model}
    out["scaler"] = out["goals"]["scaler"]
    return out


def predict(model: dict, frame: pd.DataFrame) -> np.ndarray:
    return ga.predict_count(model["goals"], frame) + ga.predict_count(model["assists"], frame)


def k10_rates() -> pd.DataFrame:
    """own/goal/assist rates shrunk by 10 games, from the same research path as production's."""
    data = joblib.load(base.OUT / "matrices.joblib")
    api = pd.read_csv(own.SEASON_2022)
    totals = pd.concat([data["season_totals"], pd.DataFrame({"player_id": api.player_id, "canonical_season": 2022,
                                                              "season_points": api.points, "season_games": api.games})],
                       ignore_index=True)
    frozen = pd.read_csv(refit.FROZEN_FRAME, low_memory=False)
    frozen["game_id"] = frozen.game_id.astype("int64"); frozen["player_id"] = frozen.player_id.astype("int64")
    outcomes, _ = refit.official_outcomes(frozen)
    outcomes["game_id"] = outcomes.game_id.astype("int64"); outcomes["player_id"] = outcomes.player_id.astype("int64")
    f = ga.component_rates(own.add_own_rate(data["historical"], totals), outcomes, api)
    k = ga.K
    lg_go = (f.goal_rate * (f.pg + f.cg + k) - f.pgo - f.cgo) / k
    lg_a = (f.assist_rate * (f.pg + f.cg + k) - f.pa - f.ca) / k
    f["goal_rate_k10"] = (f.pgo + f.cgo + 10 * lg_go) / (f.pg + f.cg + 10)
    f["assist_rate_k10"] = (f.pa + f.ca + 10 * lg_a) / (f.pg + f.cg + 10)
    f["game_id"], f["player_id"] = f.game_id.astype("int64"), f.player_id.astype("int64")
    return f[KEYS + ["own_rate_k10", "goal_rate_k10", "assist_rate_k10", "own_rate_k30"]].drop_duplicates(KEYS) \
        .rename(columns={"own_rate_k30": "own_rate_k30_research"})


def frames() -> dict:
    cache = OUT / "frames.joblib"
    if cache.is_file():
        return joblib.load(cache)
    logs = ppfix.db_logs()
    logs["canonical_season"] = pmc.season_of(logs.game_date)
    _, usage = pmc.history_per_minute(logs)
    train = pd.read_csv(ppfix.OUT / "training_frame_2023_2024_pp_corrected.csv.gz", float_precision="round_trip")
    hold = pd.read_csv(freeze.OUT / "holdout_frame_2025.csv.gz", float_precision="round_trip")
    outcomes, _, _ = freeze.official()
    history = outcomes[KEYS + ["game_date"]].copy()
    history["game_date"] = pd.to_datetime(history.game_date).dt.normalize()
    history = history.merge(logs.drop(columns=["game_date", "canonical_season"]), on=KEYS, how="left")
    w = ppfix.window_means(history, hold[KEYS + ["slate_date"]].rename(columns={"slate_date": "game_date"}))
    hold = hold.merge(w, on=KEYS, how="left").assign(mean_pp_toi_last10=lambda x: x.pp_corrected)
    k10 = k10_rates()
    train, hold = train.merge(k10, on=KEYS, how="left"), hold.merge(k10, on=KEYS, how="left")
    live = pmc.role_flags(pmc.live_frame(logs), usage)
    out = {"train": train, "hold": pmc.role_flags(hold, usage), "live": live, "market": pmc.market(live)}
    joblib.dump(out, cache)
    return out


def live_k10(live: pd.DataFrame) -> pd.DataFrame:
    """Live rows carry production's k30 rates; rebuild k10 from the same totals (Bayes inversion)."""
    f = live.copy()
    # rate_k = (sum + k*mu)/(n + k): with n = own_games (prior + current games) and the k30 rate,
    # mu is not stored, so use last season's league rate from the production tables.
    totals, league = pmc.shadow.production_tables()
    season = int(pd.to_datetime(f.game_date).min().year if pd.to_datetime(f.game_date).min().month >= 9
                 else pd.to_datetime(f.game_date).min().year - 1)
    mu = league.set_index("season").loc[season - 1]
    n = f.own_games.astype(float)
    for col, new, rate in (("own_rate_k30", "own_rate_k10", mu.points), ("goal_rate", "goal_rate_k10", mu.goals),
                           ("assist_rate", "assist_rate_k10", mu.assists)):
        total = f[col] * (n + 30) - 30 * rate
        f[new] = (total + 10 * rate) / (n + 10)
    return f


def level_table(train: pd.DataFrame, hold: pd.DataFrame) -> pd.DataFrame:
    """Daily league points per player-game: training and holdout frames, and 2026 postgame outcomes."""
    parts = [x[["slate_date", "realized_points"]].assign(slate_date=lambda t: pd.to_datetime(t.slate_date))
             for x in (train, hold)]
    for day in sorted(pmc.POSTGAME.glob("2026-*")):
        files = sorted(day.glob("*/canonical_skater_outcomes.csv"))
        if files:
            o = pd.read_csv(files[-1])
            o = o[o.participation_state.astype(str).str.upper().str.startswith("PARTICIPA")]
            parts.append(pd.DataFrame({"slate_date": pd.to_datetime(o.slate_date), "realized_points": o.official_points}))
    d = pd.concat(parts).groupby("slate_date").realized_points.agg(["sum", "count"]).reset_index()
    d["season"] = pmc.season_of(d.slate_date)
    return d


def league_level(table: pd.DataFrame, day: pd.Timestamp) -> float:
    season = int(day.year if day.month >= 9 else day.year - 1)
    before = table[(table.season == season) & (table.slate_date < day)]
    if len(before) >= 7:
        return float(before["sum"].sum() / before["count"].sum())
    prev = table[table.season == season - 1]
    return float(prev["sum"].sum() / prev["count"].sum())


def calib(frame: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    rows = []
    d = frame.assign(dec=pd.qcut(frame.BASE, 10, labels=False, duplicates="drop"),
                     toi_band=pd.cut(frame.mean_toi_last10, [0, 13, 17, 20, 23, 60], labels=["<13", "13-17", "17-20", "20-23", "23+"]))
    for key in ("dec", "toi_band"):
        for v, g in d.groupby(key, observed=True):
            rows.append({"by": key, "group": str(v), "n": len(g), "actual": g.realized_points.mean(),
                         **{c: g[c].mean() for c in cols}})
    return pd.DataFrame(rows)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    fr = frames()
    train, hold = fr["train"], fr["hold"]
    live = live_k10(fr["live"])
    k10_feats = [K10.get(c, c) for c in FEATS]
    means = {}
    for name, x in (("train", train), ("hold", hold), ("live", live)):
        d = pd.to_datetime(x.slate_date if "slate_date" in x else x.game_date)
        for season in set(pmc.season_of(d)):
            for c in SHOTS:
                means[(season, c)] = float(pd.to_numeric(x[c], errors="coerce")[pmc.season_of(d) == season].mean())
    fallback = {(s + 1, c): v for (s, c), v in means.items()}
    fallback.setdefault((2023, SHOTS[0]), means[(2023, SHOTS[0])])
    for c in SHOTS:
        fallback.setdefault((2023, c), means[(2023, c)])   # first training season: its own mean
    for x in (train, hold, live):   # impute unknown attempts on the raw scale before scaling
        x["attempts_d10_per60"] = scorer.impute_unknown_attempts(x)
    train, hold, live = (add_relative(x, fallback) for x in (train, hold, live))
    as_rel = lambda x: x.assign(**{c: x[REL[c]] for c in SHOTS})  # noqa: E731  same names, relative values
    summary_rel = {f"{s}:{c}": round(v, 3) for (s, c), v in sorted(means.items())}
    specs = {"BASE": (FEATS, {}), "CAP_300": (FEATS, {"max_iter": 300}),
             "CAP_300_31": (FEATS, {"max_iter": 300, "max_leaf_nodes": 31}),
             "K10": (k10_feats, {}), "K10_CAP_300": (k10_feats, {"max_iter": 300}),
             "REL": (FEATS, {}), "REL_K10": (k10_feats, {})}
    rel_names = {"REL", "REL_K10"}
    models = {k: fit(as_rel(train) if k in rel_names else train, f, **kw) for k, (f, kw) in specs.items()}
    names = list(specs)
    for frame in (hold, live):
        rel_frame = as_rel(frame)
        for k, m in models.items():
            frame[k] = predict(m, rel_frame if k in rel_names else frame)
    # LEVEL (measurement only; a level adjustment needs operator approval): REL scaled by the
    # league's points per player-game on strictly earlier slates this season (7+ slates, else the
    # previous season's), over the training seasons' mean.
    train_level = float(train.realized_points.mean())
    league = level_table(train, hold)
    for frame in (hold, live):
        d = pd.to_datetime(frame.slate_date if "slate_date" in frame else frame.game_date).dt.normalize()
        frame["league_level"] = [league_level(league, x) for x in d]
        frame["REL_LEVEL"] = frame.REL * frame.league_level / train_level
    names.append("REL_LEVEL")
    prod = joblib.load(scorer.CONTRACT_PATH.parent / json.loads(scorer.CONTRACT_PATH.read_text())["fitted_artifact"])
    summary = {"base_vs_production_max_abs": float(np.abs(hold.BASE - scorer.score(hold, prod).expected_points).max()),
               "hyperparameters_production": base.HGB_KW, "k10_coverage_holdout": float(hold.own_rate_k10.notna().mean()),
               "season_means_of_shot_inputs": summary_rel}
    tabs = []
    for label, frame in (("holdout", hold), ("live", live)):
        masks = {k: v for k, v in pmc.groups(frame).items()
                 if k in ("all", "role_up", "role_down", "toi_under_13", "toi_17_20", "toi_20_plus")}
        for cand in names[1:]:
            t = pmc.paired(frame, cand, "BASE", masks)
            tabs.append(t.assign(part=label, model=cand)[["part", "model", "group", "line", "n", "diff_x1000", "ci_x1000"]])
        calib(frame, names).round(4).to_csv(OUT / f"{label}_calibration.csv", index=False)
        slope = {}
        for c in names:
            p = frame[c].to_numpy(); y = frame.realized_points.to_numpy()
            slope[c] = float(np.polyfit(p, y, 1)[0])
        summary[f"{label}_calibration_slope"] = slope
        summary[f"{label}_mean"] = {"actual": float(frame.realized_points.mean()), **{c: float(frame[c].mean()) for c in names}}
    paired_tab = pd.concat(tabs, ignore_index=True)
    paired_tab.to_csv(OUT / "paired_vs_base.csv", index=False)
    mk = live.merge(fr["market"], on=KEYS, how="inner")
    y = (mk.realized_points > mk.line).astype(float).to_numpy()
    k = mk.line.astype(int).to_numpy()
    mrows = []
    for name, m in {"all": np.ones(len(mk), bool), "line_0.5": mk.line.eq(0.5).to_numpy(),
                    "role_change": mk.role_change.to_numpy(bool)}.items():
        row = {"group": name, "n": int(m.sum()), "actual_over": float(y[m].mean()),
               "MARKET_ll": float(base.log_loss_rows(mk.market_p_over.to_numpy()[m], y[m]).mean())}
        for c in names:
            row[f"{c}_ll"] = float(base.log_loss_rows(poisson.sf(k, mk[c].to_numpy())[m], y[m]).mean())
        mrows.append(row)
    mt = pd.DataFrame(mrows)
    mt.to_csv(OUT / "live_two_sided_vs_market.csv", index=False)
    joblib.dump(models, OUT / "candidates.joblib")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary, indent=1, default=str))
    print(paired_tab.pivot_table(index=["part", "group", "line"], columns="model", values="diff_x1000").round(2).to_string())
    print(mt.round(4).to_string(index=False))
    print(calib(hold, names).round(3).to_string(index=False))
    print(calib(live, names).round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
