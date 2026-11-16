#!/usr/bin/env python3
"""Points ice-time and role: per-minute rates on the corrected-PP recipe (NHL Backlog P2.46 / P2.56).

Research only, read-only (research database login). The per-minute shadow (PER_MINUTE_ADD,
frozen 2026-11-06) was fitted before the PP TOI correction (P2.45) and scored against the
retired 8cda6061 production; production is now c940ae2f (corrected PP). This refits both
on the same corrected training frame (P2.53, 2023-24 seasons):

  BASE = the c940ae2f recipe (freeze_nhl_points_goals_assists.FEATURES)
  ADD  = BASE + own/goal/assist rate per minute x last-10 TOI (P2.46 test 1)

and compares them:
1. 2025 holdout: paired log loss at 0.5/1.5/2.5, all rows, role change (input-watch
   definition with corrected PP), role up/down, games played this season (the open
   P2.46 step (a)), and ice-time bands.
2. Live 2026-11-01..11-16: latest pregame Points capture per player-game, PP input
   recomputed from the corrected database, per-minute inputs as the live shadow
   computed them for that capture; official outcomes (participants). Against the
   market's no-vig price on two-sided lines (weekly evaluation cache), and on the
   input watch's biggest Points disagreements (P2.56 graded_disagreements.csv).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_per_minute_shadow as shadow  # noqa: E402
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import research_nhl_points_goal_assist as ga  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import research_nhl_points_pp_corrected_refit as ppfix  # noqa: E402
from backend.nhl.scripts import research_nhl_points_role_change_tests as rt  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_per_minute_corrected_2026-11-16"
KEYS = ["game_id", "player_id"]
TOI = list(shadow.TOI_FEATURES)
LINES = (0.5, 1.5, 2.5)
LIVE_START, LIVE_END = "2026-11-01", "2026-11-16"
DISAGREE = ROOT / "artifacts/analysis/nhl/disagreement_reasons_2026-11-16/graded_disagreements.csv"
POSTGAME = ROOT / "artifacts/operational/nhl/postgame_reconciliation"


def season_of(d: pd.Series) -> pd.Series:
    d = pd.to_datetime(d)
    return np.where(d.dt.month >= 9, d.dt.year, d.dt.year - 1)


def fit(train: pd.DataFrame, features: list[str]) -> dict:
    g, a = ga.fit_count(train, features, "goals"), ga.fit_count(train, features, "assists")
    return {"structure": "GOALS_PLUS_ASSISTS", "feature_columns": features, "fill_zero": True,
            "scaler": g["scaler"], "goals": g, "assists": a}


def role_flags(frame: pd.DataFrame, season_usage: pd.DataFrame) -> pd.DataFrame:
    prev = season_usage.assign(canonical_season=season_usage.canonical_season + 1).rename(
        columns={"s_games": "p_games_toi", "s_toi_pg": "p_toi_pg", "s_pp_pg": "p_pp_pg"})[
        ["player_id", "canonical_season", "p_games_toi", "p_toi_pg", "p_pp_pg"]]
    f = frame.assign(canonical_season=season_of(frame.slate_date)).merge(prev, on=["player_id", "canonical_season"], how="left")
    scale = f.groupby("canonical_season").apply(
        lambda t: t.mean_pp_toi_last10.mean() / t.p_pp_pg.mean() if t.p_pp_pg.mean() > 0 else 1.0, include_groups=False)
    toi_d = f.mean_toi_last10 - f.p_toi_pg
    pp_d = f.mean_pp_toi_last10 - f.p_pp_pg * f.canonical_season.map(scale)
    f["role_change"] = ((f.current_season_games_prior >= rt.ROLE_GAMES) & (f.p_games_toi >= rt.ROLE_PRIOR_GAMES)
                        & ((toi_d.abs() >= rt.ROLE_TOI) | (pp_d.abs() >= rt.ROLE_PP))).fillna(False)
    f["role_up"] = f.role_change & ((toi_d >= rt.ROLE_TOI) | (pp_d >= rt.ROLE_PP))
    f["role_down"] = f.role_change & ~f.role_up
    return f


def groups(f: pd.DataFrame) -> dict[str, np.ndarray]:
    gp = f.current_season_games_prior
    toi = f.mean_toi_last10
    return {"all": np.ones(len(f), bool), "role_change": f.role_change.to_numpy(bool),
            "role_up": f.role_up.to_numpy(bool), "role_down": f.role_down.to_numpy(bool),
            "no_role_change": ~f.role_change.to_numpy(bool),
            "games_0_4": gp.lt(5).to_numpy(), "games_5_9": gp.between(5, 9).to_numpy(),
            "games_10_19": gp.between(10, 19).to_numpy(), "games_20_plus": gp.ge(20).to_numpy(),
            "toi_under_13": toi.lt(13).to_numpy(), "toi_13_17": toi.between(13, 17, inclusive="left").to_numpy(),
            "toi_17_20": toi.between(17, 20, inclusive="left").to_numpy(), "toi_20_plus": toi.ge(20).to_numpy()}


def paired(f: pd.DataFrame, a: str, b: str, masks: dict) -> pd.DataFrame:
    out, dates = [], f.slate_date.astype(str).to_numpy()
    for name, m in masks.items():
        if m.sum() < 30:
            continue
        for line in LINES:
            y = (f.realized_points.to_numpy() > line).astype(float)
            r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), f[a].to_numpy()), y)[m],
                                      base.log_loss_rows(poisson.sf(int(line), f[b].to_numpy()), y)[m], dates[m])
            out.append({"group": name, "line": line, "n": int(m.sum()), "diff_x1000": round(r["mean_diff"] * 1e3, 2),
                        "ci_x1000": [round(c * 1e3, 2) for c in r["ci95"]],
                        "mean_actual": float(f.realized_points[m].mean()), f"mean_{a}": float(f[a][m].mean()),
                        f"mean_{b}": float(f[b][m].mean())})
    return pd.DataFrame(out)


def history_per_minute(logs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-minute inputs for historical rows (rt.build path) and season usage from corrected logs."""
    season, cur = rt.usage_tables(logs)
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
    f["lg_go"] = (f.goal_rate * (f.pg + f.cg + rt.K) - f.pgo - f.cgo) / rt.K
    f["lg_a"] = (f.assist_rate * (f.pg + f.cg + rt.K) - f.pa - f.ca) / rt.K
    f = rt.add_role_inputs(f, season, cur, totals)
    f["game_id"], f["player_id"] = f.game_id.astype("int64"), f.player_id.astype("int64")
    return f[KEYS + TOI].drop_duplicates(KEYS), season


def live_frame(logs: pd.DataFrame) -> pd.DataFrame:
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    caps = []
    for day in weekly._days(LIVE_START, LIVE_END):
        for cap in shadow.captures_for(day):
            pm = shadow.output_dir(cap) / "predictions.csv"
            if not pm.is_file():
                continue
            c = pd.read_csv(cap / "features.csv")
            p = pd.read_csv(pm)[KEYS + TOI]
            c = c.merge(p, on=KEYS, how="inner")
            c["captured_at"] = cap.name.split("=", 1)[1]
            caps.append(c)
    live = pd.concat(caps, ignore_index=True)
    live = live[pd.to_datetime(live.captured_at, format="%Y%m%dT%H%M%S%fZ", utc=True)
                < pd.to_datetime(live.game_start_utc, utc=True)]
    live = live.sort_values("captured_at").drop_duplicates(KEYS, keep="last")
    outs = []
    for day in sorted(POSTGAME.glob("2026-1[01]-*")):
        files = sorted(day.glob("*/canonical_skater_outcomes.csv"))
        if files:
            outs.append(pd.read_csv(files[-1]))
    o = pd.concat(outs, ignore_index=True)
    o = o[o.participation_state.astype(str).str.upper().str.startswith("PARTICIPA")]
    o = o[KEYS + ["official_points"]].astype("int64").rename(columns={"official_points": "realized_points"})
    live = live.merge(o, on=KEYS, how="inner")
    lt = live[KEYS + ["game_date"]].copy()
    lt["game_date"] = pd.to_datetime(lt.game_date)
    w = ppfix.window_means(logs[logs.toi_minutes.gt(0)][KEYS + ["game_date", "toi_minutes", "pp_toi_minutes"]], lt)
    live = live.merge(w, on=KEYS, how="left")
    live["mean_pp_toi_last10"] = live.pp_corrected.fillna(live.mean_pp_toi_last10)
    live["slate_date"] = live.game_date.astype(str)
    return live


def market(live: pd.DataFrame) -> pd.DataFrame:
    from backend.nhl import two_sided_lines
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    loaded: list = []

    def lc():
        if not loaded:
            loaded.append(two_sided_lines.candidate_export.load_candidates(weekly.OPERATIONAL)[0])
        return loaded[0]

    rows = []
    for day in sorted(live.slate_date.unique()):
        for g, p, line, mp in (weekly._cached_two_sided_keys(day, lc).get("points") or []):
            if mp is not None:
                rows.append((int(g), int(p), float(line), float(mp)))
    return pd.DataFrame(rows, columns=KEYS + ["line", "market_p_over"])


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    logs = ppfix.db_logs()
    logs["canonical_season"] = season_of(logs.game_date)
    pm_hist, usage = history_per_minute(logs)
    train = pd.read_csv(ppfix.OUT / "training_frame_2023_2024_pp_corrected.csv.gz", float_precision="round_trip")
    hold = pd.read_csv(freeze.OUT / "holdout_frame_2025.csv.gz", float_precision="round_trip")
    outcomes, _, _ = freeze.official()
    history = outcomes[KEYS + ["game_date"]].copy()
    history["game_date"] = pd.to_datetime(history.game_date).dt.normalize()
    history = history.merge(logs.drop(columns=["game_date", "canonical_season"]), on=KEYS, how="left")
    w = ppfix.window_means(history, hold[KEYS + ["slate_date"]].rename(columns={"slate_date": "game_date"}))
    hold = hold.merge(w, on=KEYS, how="left").assign(mean_pp_toi_last10=lambda x: x.pp_corrected)
    train = train.merge(pm_hist, on=KEYS, how="left")
    hold = hold.merge(pm_hist, on=KEYS, how="left")
    summary = {"coverage_per_minute": {"train": float(train.own_rate_toi.notna().mean()),
                                       "holdout": float(hold.own_rate_toi.notna().mean())}}
    base_m = fit(train, list(freeze.FEATURES))
    add_m = fit(train, list(freeze.FEATURES) + TOI)
    prod = joblib.load(scorer.CONTRACT_PATH.parent / json.loads(scorer.CONTRACT_PATH.read_text())["fitted_artifact"])
    hold["BASE"] = scorer.score(hold, base_m).expected_points.to_numpy()
    hold["ADD"] = scorer.score(hold, add_m).expected_points.to_numpy()
    hold["PRODUCTION"] = scorer.score(hold, prod).expected_points.to_numpy()
    summary["base_reproduces_production"] = float(np.abs(hold.BASE - hold.PRODUCTION).max())
    hold = role_flags(hold, usage)
    th = paired(hold, "ADD", "BASE", groups(hold))
    th.to_csv(OUT / "holdout_2025_add_vs_base.csv", index=False)

    live = role_flags(live_frame(logs), usage)
    live["BASE"] = scorer.score(live, prod).expected_points.to_numpy()   # production c940ae2f
    live["ADD"] = scorer.score(live, add_m).expected_points.to_numpy()
    tl = paired(live, "ADD", "BASE", groups(live))
    tl.to_csv(OUT / "live_add_vs_production.csv", index=False)
    mk = live.merge(market(live), on=KEYS, how="inner")
    mk["y"] = (mk.realized_points > mk.line).astype(float)
    k = mk.line.astype(int).to_numpy()
    probs = {"PRODUCTION": poisson.sf(k, mk.BASE), "ADD": poisson.sf(k, mk.ADD), "MARKET": mk.market_p_over.to_numpy()}
    mrows = []
    for name, m in {"all": np.ones(len(mk), bool), "role_change": mk.role_change.to_numpy(bool),
                    "no_role_change": ~mk.role_change.to_numpy(bool), "line_0.5": mk.line.eq(0.5).to_numpy(),
                    "toi_under_13": mk.mean_toi_last10.lt(13).to_numpy()}.items():
        row = {"group": name, "n": int(m.sum()), "actual_over": float(mk.y[m].mean())}
        for label, p in probs.items():
            row[f"{label}_p_over"] = float(p[m].mean())
            row[f"{label}_log_loss"] = float(base.log_loss_rows(p[m], mk.y.to_numpy()[m]).mean())
        mrows.append(row)
    tm = pd.DataFrame(mrows)
    tm.to_csv(OUT / "live_two_sided_vs_market.csv", index=False)
    td = pd.DataFrame()
    if DISAGREE.is_file():
        d = pd.read_csv(DISAGREE)
        d = d[d.lane.eq("points")].merge(live[KEYS + ["BASE", "ADD"]], on=KEYS, how="inner")
        side = np.where(d.side.eq("OVER"), 1, -1)
        kk = d.line.astype(int).to_numpy()
        po_b, po_a = poisson.sf(kk, d.BASE), poisson.sf(kk, d.ADD)
        d["prod_p_side"] = np.where(side > 0, po_b, 1 - po_b)
        d["add_p_side"] = np.where(side > 0, po_a, 1 - po_a)
        cats = d.categories.str.strip("[]").str.replace("'", "").str.split(", ")
        ex = d.assign(category=cats).explode("category")
        td = ex.groupby("category").agg(plays=("won", "size"), won=("won", "mean"), market=("market_p", "mean"),
                                        production=("prod_p_side", "mean"), per_minute=("add_p_side", "mean")).round(3)
        td = pd.concat([td, pd.DataFrame([{"plays": len(d), "won": d.won.mean(), "market": d.market_p.mean(),
                                           "production": d.prod_p_side.mean(), "per_minute": d.add_p_side.mean()}],
                                         index=["ALL"]).round(3)])
        td.to_csv(OUT / "disagreements_per_minute.csv")
    joblib.dump(add_m, OUT / "per_minute_add_corrected_candidate.joblib")
    summary.update({"train_rows": int(len(train)), "holdout_rows": int(len(hold)), "live_rows": int(len(live)),
                    "live_dates": [live.slate_date.min(), live.slate_date.max()], "two_sided_rows": int(len(mk)),
                    "role_change_share": {"holdout": float(hold.role_change.mean()), "live": float(live.role_change.mean())}})
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary, indent=1, default=str))
    for t in (th, tl):
        print(t[["group", "line", "n", "diff_x1000", "ci_x1000"]].to_string(index=False))
    print(tm.round(4).to_string(index=False)); print(td.to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
