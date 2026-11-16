#!/usr/bin/env python3
"""Points: replace raw shots per 60 with on-ice goals for, shot quality and shooting % (P2.46).

Research only, read-only (stored official play-by-play and shift charts; research login for
the 2025-26 shift charts and TOI). No model has replaced `d10_sog_per60` in the Points
model; the shot-volume drift (P2.46, 2026-11-16) makes raw shot counts a weak input.

Per player-game, from the official play-by-play (2023-24 and 2024-25:
pp_toi_historical_recovery_2026-10-16/official_sources; 2025-26:
points_attempts_2025_full_recovery_2026-10-16/official_pbp) and shift charts (2023-24,
2024-25 files; 2025-26 nhl.shiftcharts_shifts):
- ixg: individual expected goals of the player's unblocked attempts. The xG model is a
  logistic regression on 2023-24 attempts only (distance, angle, shot type, strength,
  rebound within 3 s, empty net excluded), applied unchanged to later seasons.
- onice_gf: goals for his team while he was on the ice (shift contains the goal time;
  shootout excluded); onice_ga likewise.
- isog, igoals for shooting %.
Rolling, strict prior, last 10 games with data within 120 days, per 60 of TOI:
ixg_d10_per60, onice_gf_d10_per60, plus shooting % over the last 82 games with data,
shrunk to the league rate by 100 shots. Games without play-by-play or shift charts drop out.

Candidates (goals + assists on the corrected training frame, P2.53; BASE = c940ae2f):
REPLACE_XG (d10_sog_per60 -> ixg_d10_per60), ADD_XG (+ ixg, shooting %), ADD_ONICE
(+ onice_gf), ADD_ALL, REPLACE_ALL (sog -> ixg, + onice_gf, shooting %). 2025 holdout,
paired log loss at 0.5/1.5/2.5 by group. Live needs 2026-27 play-by-play, which is not
retained (sized, not fetched).
"""
from __future__ import annotations

import glob
import gzip
import json
import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms  # noqa: E402
from backend.nhl.scripts import research_nhl_points_per_minute_corrected as pmc  # noqa: E402
from backend.nhl.scripts import research_nhl_points_pp_corrected_refit as ppfix  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_onice_xg_2026-11-16"
SRC_2324 = ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources"
PBP_2025 = ROOT / "artifacts/analysis/nhl/points_attempts_2025_full_recovery_2026-10-16/official_pbp"
# 2026-27 play-by-play fetched 2026-11-16 for the live test (operator: "Yes, fetch it"; 330 games
# through 2026-11-15, manifest pbp_2026_manifest.json).
PBP_2026 = OUT / "pbp_2026"
KEYS = ["game_id", "player_id"]
SHOT_TYPES = ("wrist", "snap", "slap", "backhand", "tip-in", "deflected", "wrap-around")


def mmss(v) -> float:
    if not v or not isinstance(v, str) or ":" not in v:
        return np.nan
    m, s = v.split(":")
    return int(m) * 60 + int(s)


def game_seconds(period: int, t: float) -> float:
    return (period - 1) * 1200 + t


def parse_pbp(p: dict) -> tuple[list, list]:
    """Unblocked attempts and goals of one game."""
    gid, home = int(p["id"]), int(p["homeTeam"]["id"])
    shots, goals = [], []
    last = {}
    for e in sorted(p.get("plays", []), key=lambda x: x.get("sortOrder", 0)):
        kind = e.get("typeDescKey")
        per = (e.get("periodDescriptor") or {})
        if per.get("periodType") == "SO" or kind not in ("shot-on-goal", "missed-shot", "goal"):
            continue
        d = e.get("details") or {}
        team = d.get("eventOwnerTeamId")
        if team is None:
            continue
        team = int(team)
        t = game_seconds(int(per.get("number", 1)), mmss(e.get("timeInPeriod")))
        code = str(e.get("situationCode") or "1551").zfill(4)
        own_home = team == home
        own_sk, opp_sk = (int(code[2]), int(code[1])) if own_home else (int(code[1]), int(code[2]))
        opp_goalie = int(code[0]) if own_home else int(code[3])
        strength = "PP" if own_sk > opp_sk else "SH" if own_sk < opp_sk else "EV"
        if kind == "goal":
            goals.append({"game_id": gid, "t": t, "team": team, "scorer": d.get("scoringPlayerId"),
                          "a1": d.get("assist1PlayerId"), "a2": d.get("assist2PlayerId"), "strength": strength,
                          "empty_net": opp_goalie == 0})
        shooter = d.get("shootingPlayerId") or d.get("scoringPlayerId")
        x, y = d.get("xCoord"), d.get("yCoord")
        if shooter is None or x is None or y is None:
            continue
        side = e.get("homeTeamDefendingSide")
        if side in ("left", "right"):
            net_x = (89 if side == "left" else -89) if own_home else (-89 if side == "left" else 89)
        else:
            net_x = 89 if x >= 0 else -89
        dist = float(np.hypot(net_x - x, y))
        angle = float(np.degrees(np.arctan2(abs(y), max(abs(net_x - x), 1e-6))))
        rebound = team in last and 0 <= t - last[team] <= 3
        last[team] = t
        shots.append({"game_id": gid, "t": t, "team": team, "shooter": int(shooter), "dist": dist, "angle": angle,
                      "shot_type": d.get("shotType") or "other", "strength": strength, "rebound": rebound,
                      "empty_net": opp_goalie == 0, "goal": kind == "goal"})
    return shots, goals


def shifts_from_json(s: dict) -> pd.DataFrame:
    rows = []
    for r in s.get("data", []):
        if r.get("typeCode") != 517 or not r.get("startTime") or not r.get("endTime"):
            continue
        per = int(r["period"])
        if per > 4:
            continue
        rows.append((int(r["gameId"]), int(r["playerId"]), int(r["teamId"]),
                     game_seconds(per, mmss(r["startTime"])), game_seconds(per, mmss(r["endTime"]))))
    return pd.DataFrame(rows, columns=["game_id", "player_id", "team_id", "start", "end"])


def shifts_2025(season: int = 2025) -> pd.DataFrame:
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT s.game_id::bigint, s.player_id::bigint, s.team_id::bigint, s.period, s.start_time, s.end_time
                         FROM nhl.shiftcharts_shifts s JOIN nhl.games g USING (game_id)
                        WHERE g.season = %s AND g.game_type = 2 AND s.period <= 4""", (season,))
        rows = cur.fetchall()
    out = [(g, p, t, game_seconds(int(per), mmss(str(a))), game_seconds(int(per), mmss(str(b))))
           for g, p, t, per, a, b in rows]
    return pd.DataFrame(out, columns=["game_id", "player_id", "team_id", "start", "end"])


def onice(goals: pd.DataFrame, shifts: pd.DataFrame) -> pd.DataFrame:
    """Per player-game goals for and against while on the ice (start < t <= end)."""
    m = shifts.merge(goals[["game_id", "t", "team"]], on="game_id")
    m = m[(m.start < m.t) & (m.t <= m.end)].drop_duplicates(["game_id", "player_id", "t", "team"])
    m["gf"] = (m.team == m.team_id).astype(int)
    m["ga"] = 1 - m.gf
    out = m.groupby(KEYS)[["gf", "ga"]].sum().rename(columns={"gf": "onice_gf", "ga": "onice_ga"})
    played = shifts[KEYS].drop_duplicates().set_index(KEYS)
    return played.join(out).fillna(0).reset_index()


def xg_design(s: pd.DataFrame) -> pd.DataFrame:
    x = pd.DataFrame({"dist": s.dist, "dist2": s.dist ** 2 / 100, "angle": s.angle, "rebound": s.rebound.astype(float),
                      "pp": s.strength.eq("PP").astype(float), "sh": s.strength.eq("SH").astype(float)})
    for t in SHOT_TYPES:
        x[f"type_{t}"] = s.shot_type.eq(t).astype(float)
    return x


def build_events() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cache = OUT / "events.joblib"
    if cache.is_file():
        import joblib
        return joblib.load(cache)
    shots, goals, shift_parts = [], [], []
    for d in sorted(SRC_2324.glob("game=*")):
        p = json.load(gzip.open(d / "pbp.json.gz"))
        if int(p.get("gameType", 2)) != 2:
            continue
        s, g = parse_pbp(p)
        shots += s; goals += g
        if (d / "shiftcharts.json.gz").is_file():
            shift_parts.append(shifts_from_json(json.load(gzip.open(d / "shiftcharts.json.gz"))))
    for f in sorted(PBP_2025.glob("game=*.play-by-play.json")):
        p = json.load(open(f))
        if int(p.get("gameType", 2)) != 2:
            continue
        s, g = parse_pbp(p)
        shots += s; goals += g
    shift_parts.append(shifts_2025())
    out = (pd.DataFrame(shots), pd.DataFrame(goals), pd.concat(shift_parts, ignore_index=True))
    import joblib
    joblib.dump(out, cache)
    return out


def per_game(shots: pd.DataFrame, goals: pd.DataFrame, shifts: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    shots = shots.copy()
    shots["season"] = shots.game_id // 1_000_000
    fit_rows = shots[shots.season.eq(2023) & ~shots.empty_net]
    model = LogisticRegression(max_iter=2000, C=1.0).fit(xg_design(fit_rows), fit_rows.goal.astype(int))
    shots["xg"] = np.where(shots.empty_net, np.nan, model.predict_proba(xg_design(shots))[:, 1])
    check = {str(s): {"attempts": int(len(g)), "goals": int(g.goal.sum()), "xg": float(g.xg.sum())}
             for s, g in shots[~shots.empty_net].groupby("season")}
    ind = shots[~shots.empty_net].groupby(["game_id", "shooter"]).agg(
        ixg=("xg", "sum"), iff=("xg", "size"), igoals=("goal", "sum")).reset_index().rename(columns={"shooter": "player_id"})
    oi = onice(goals, shifts)
    games_with_pbp = pd.DataFrame({"game_id": sorted(shots.game_id.unique())})
    return oi.merge(ind, on=KEYS, how="left"), {"xg_by_season": check, "games_with_pbp": int(len(games_with_pbp)),
                                                 "games_with_shifts": int(shifts.game_id.nunique()),
                                                 "xg_coefficients": dict(zip(xg_design(fit_rows).columns,
                                                                             np.round(model.coef_[0], 4)))}


def rolling(frame: pd.DataFrame, pg: pd.DataFrame, logs: pd.DataFrame) -> pd.DataFrame:
    """Strict-prior last-10 (120 days) per-60 rates and last-82 shrunk shooting % per target row."""
    g = pg.merge(logs[KEYS + ["game_date", "toi_minutes", "shots_on_goal"]], on=KEYS, how="inner")
    g = g[g.toi_minutes.gt(0)].copy()
    g[["ixg", "iff", "igoals"]] = g[["ixg", "iff", "igoals"]].fillna(0)
    g["game_date"] = pd.to_datetime(g.game_date)
    g = g.sort_values(["player_id", "game_date", "game_id"])
    lg_sh = float(g.igoals.sum() / max(g.shots_on_goal.sum(), 1))
    groups = {int(p): x for p, x in g.groupby("player_id", sort=False)}
    out = []
    for t in frame[KEYS + ["slate_date"]].itertuples(index=False):
        x = groups.get(int(t.player_id))
        if x is None:
            out.append((t.game_id, t.player_id, np.nan, np.nan, np.nan)); continue
        d = pd.Timestamp(t.slate_date)
        prior = x[x.game_date < d]
        h = prior[prior.game_date >= d - pd.Timedelta(days=120)].tail(10)
        toi = h.toi_minutes.sum()
        long = prior.tail(82)
        out.append((t.game_id, t.player_id,
                    h.ixg.sum() / toi * 60 if toi > 0 else np.nan,
                    h.onice_gf.sum() / toi * 60 if toi > 0 else np.nan,
                    (long.igoals.sum() + 100 * lg_sh) / (long.shots_on_goal.sum() + 100)))
    return pd.DataFrame(out, columns=KEYS + ["ixg_d10_per60", "onice_gf_d10_per60", "sh_pct_shrunk"])


def db_logs_with_sog() -> pd.DataFrame:
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT l.game_id::bigint, l.player_id::bigint, g.game_date, l.toi_minutes, l.shots_on_goal
                         FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING (game_id)
                        WHERE g.season BETWEEN 2023 AND 2026 AND g.game_type = 2""")
        logs = pd.DataFrame(cur.fetchall(), columns=["game_id", "player_id", "game_date", "toi_minutes", "shots_on_goal"])
    logs["toi_minutes"] = pd.to_numeric(logs.toi_minutes, errors="coerce").astype(float)
    logs["shots_on_goal"] = pd.to_numeric(logs.shots_on_goal, errors="coerce").astype(float)
    return logs


def live_test(shots, goals, shifts, logs, specs, models, swap) -> dict:
    """Score the candidates on live 2026-11-01..11-15 captures against production and the market."""
    import joblib
    from scipy.stats import poisson
    from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base
    from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
    s26, g26 = [], []
    for f in sorted(PBP_2026.glob("game=*.play-by-play.json")):
        a, b = parse_pbp(json.load(open(f)))
        s26 += a; g26 += b
    shots = pd.concat([shots, pd.DataFrame(s26)], ignore_index=True)
    goals = pd.concat([goals, pd.DataFrame(g26)], ignore_index=True)
    shifts = pd.concat([shifts, shifts_2025(2026)], ignore_index=True)
    pg, _ = per_game(shots, goals, shifts)   # xG still fitted on 2023-24 only
    fr = ms.frames()
    live = fr["live"].copy()
    live["slate_date"] = live.game_date.astype(str)
    roll = rolling(live[KEYS + ["slate_date"]], pg, logs)
    live = live.merge(roll, on=KEYS, how="left")
    live["attempts_d10_per60"] = scorer.impute_unknown_attempts(live)
    prod = joblib.load(scorer.CONTRACT_PATH.parent / json.loads(scorer.CONTRACT_PATH.read_text())["fitted_artifact"])
    live["PRODUCTION"] = scorer.score(live, prod).expected_points.to_numpy()
    for k, (f, sw) in specs.items():
        live[k] = ms.predict(models[k], swap(live) if sw else live)
    out = {"live_rows": int(len(live)), "coverage_ixg": float(live.ixg_d10_per60.notna().mean()),
           "base_vs_production_max_abs": float(np.abs(live.BASE - live.PRODUCTION).max()),
           "mean": {"actual": float(live.realized_points.mean()), **{k: float(live[k].mean()) for k in specs}}}
    masks = {k: v for k, v in pmc.groups(live).items()
             if k in ("all", "role_change", "role_up", "role_down", "toi_under_13", "toi_17_20", "toi_20_plus")}
    paired = pd.concat([pmc.paired(live, k, "BASE", masks).assign(model=k) for k in list(specs)[1:]], ignore_index=True)
    paired[["model", "group", "line", "n", "diff_x1000", "ci_x1000"]].to_csv(OUT / "live_vs_production.csv", index=False)
    mk = live.merge(fr["market"], on=KEYS, how="inner")
    y = (mk.realized_points > mk.line).astype(float).to_numpy()
    kk = mk.line.astype(int).to_numpy()
    rows = []
    for name, m in {"all": np.ones(len(mk), bool), "line_0.5": mk.line.eq(0.5).to_numpy(),
                    "role_change": mk.role_change.to_numpy(bool)}.items():
        row = {"group": name, "n": int(m.sum()), "MARKET": float(base.log_loss_rows(mk.market_p_over.to_numpy()[m], y[m]).mean())}
        for k in specs:
            row[k] = float(base.log_loss_rows(poisson.sf(kk, mk[k].to_numpy())[m], y[m]).mean())
        rows.append(row)
    pd.DataFrame(rows).to_csv(OUT / "live_two_sided_vs_market.csv", index=False)
    if pmc.DISAGREE.is_file():
        d = pd.read_csv(pmc.DISAGREE)
        d = d[d.lane.eq("points")].merge(live[KEYS + list(specs)], on=KEYS, how="inner")
        over = d.side.eq("OVER").to_numpy()
        row = {"plays": len(d), "won": float(d.won.mean()), "market": float(d.market_p.mean())}
        for k in specs:
            p_ = poisson.sf(d.line.astype(int).to_numpy(), d[k].to_numpy())
            row[k] = float(np.where(over, p_, 1 - p_).mean())
        pd.DataFrame([row]).to_csv(OUT / "live_biggest_points_disagreements.csv", index=False)
    return out


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    shots, goals, shifts = build_events()
    pg, info = per_game(shots, goals, shifts)
    logs = db_logs_with_sog()
    fr = ms.frames()
    train, hold = fr["train"].copy(), fr["hold"].copy()
    feats_cache = OUT / "rolling_inputs.csv.gz"
    if feats_cache.is_file():
        roll = pd.read_csv(feats_cache)
    else:
        roll = rolling(pd.concat([train[KEYS + ["slate_date"]], hold[KEYS + ["slate_date"]]]).drop_duplicates(KEYS), pg, logs)
        roll.to_csv(feats_cache, index=False, float_format="%.10g")
    train, hold = train.merge(roll, on=KEYS, how="left"), hold.merge(roll, on=KEYS, how="left")
    info["coverage"] = {c: {"train": float(train[c].notna().mean()), "holdout": float(hold[c].notna().mean())}
                        for c in ("ixg_d10_per60", "onice_gf_d10_per60", "sh_pct_shrunk")}
    info["season_means"] = {c: train.assign(s=train.season).groupby("s")[c].mean().round(4).to_dict()
                            | {2025: round(float(hold[c].mean()), 4)} for c in ("d10_sog_per60", "ixg_d10_per60", "onice_gf_d10_per60", "sh_pct_shrunk")}
    F = list(freeze.FEATURES)
    from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
    for x in (train, hold):  # impute unknown attempts from the real SOG rate before any swap
        x["attempts_d10_per60"] = scorer.impute_unknown_attempts(x)
    # REPLACE: the d10_sog_per60 slot carries ixg_d10_per60 (same column name, so the recipe is unchanged).
    swap = lambda x: x.assign(d10_sog_per60=x.ixg_d10_per60)  # noqa: E731
    specs = {"BASE": (F, False), "REPLACE_XG": (F, True),
             "ADD_XG": (F + ["ixg_d10_per60", "sh_pct_shrunk"], False),
             "ADD_ONICE": (F + ["onice_gf_d10_per60"], False),
             "ADD_ALL": (F + ["ixg_d10_per60", "sh_pct_shrunk", "onice_gf_d10_per60"], False),
             "REPLACE_ALL": (F + ["sh_pct_shrunk", "onice_gf_d10_per60"], True)}
    models = {k: ms.fit(swap(train) if sw else train, f) for k, (f, sw) in specs.items()}
    for k, (f, sw) in specs.items():
        hold[k] = ms.predict(models[k], swap(hold) if sw else hold)
    masks = {k: v for k, v in pmc.groups(hold.assign(role_change=False, role_up=False, role_down=False)).items()
             if k in ("all", "toi_under_13", "toi_13_17", "toi_17_20", "toi_20_plus", "games_0_4", "games_20_plus")}
    tabs = [pmc.paired(hold, k, "BASE", masks).assign(model=k) for k in list(specs)[1:]]
    paired = pd.concat(tabs, ignore_index=True)[["model", "group", "line", "n", "diff_x1000", "ci_x1000"]]
    paired.to_csv(OUT / "holdout_2025_vs_production.csv", index=False)
    info["holdout_mean"] = {"actual": float(hold.realized_points.mean()), **{k: float(hold[k].mean()) for k in specs}}
    toi = hold.assign(toi_band=pd.cut(hold.mean_toi_last10, [0, 13, 17, 20, 23, 60], labels=["<13", "13-17", "17-20", "20-23", "23+"]))
    toi.groupby("toi_band", observed=True)[["realized_points"] + list(specs)].mean().round(4).to_csv(OUT / "holdout_level_by_toi.csv")
    if PBP_2026.is_dir():
        info["live"] = live_test(shots, goals, shifts, logs, specs, models, swap)
    (OUT / "summary.json").write_text(json.dumps(info, indent=1, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(info, indent=1, default=str))
    print(paired.pivot_table(index=["group", "line"], columns="model", values="diff_x1000").round(2).to_string())
    print(paired[paired.group.eq("all")].to_string(index=False))
    print(pd.read_csv(OUT / "holdout_level_by_toi.csv").to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
