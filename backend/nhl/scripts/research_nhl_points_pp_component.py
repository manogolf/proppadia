#!/usr/bin/env python3
"""Points: a power-play points component (NHL Backlog P2.52 order, step 1). Research only.

Read-only; never changes production. Production Points (`c940ae2f`) underrates
top-unit PP players (3+ PP minutes: about 55% predicted vs 58-59% actual for a
point, 2025 holdout and 2026 live, P2.53). Tests whether power-play scoring
handled as its own piece fixes that, against production on the same rows:

  P0  production goals + assists model, as frozen (corrected PP TOI).
  P1  production recipe + PP inputs: shrunk PP points per PP minute, tonight's
      expected PP minutes, and their product (expected PP points).
  P2  components: production recipe fitted to non-PP goals and non-PP assists,
      plus expected PP points = PP points rate x tonight's PP minutes.

PP data per player-game come from the official `skater/powerplay` responses
captured with SHA-256 manifests (`research_nhl_sog_situation_ablation.py
--fetch`): ppPoints, ppGoals, ppTimeOnIce, team, position, date, 2023-10..2026-11.
Tonight's PP minutes = the player's share of team PP time (last 10) x the mean
of his team's PP time drawn and the opponent's PP time conceded (last 20 team
games each; team PP time = skater PP minutes / 5). Rates are shrunk toward the
league PP points rate by position (previous season; 2023-24 uses its own season,
the first in the evidence), with the prior weight chosen on the 2023-24 training
rows only. All windows strictly prior to the game.
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
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import research_nhl_points_goal_assist as ga  # noqa: E402
from backend.nhl.scripts import research_nhl_points_pp_corrected_refit as rr  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402
from backend.nhl.scripts.prepare_nhl_pp_toi_stats_api_correction import sha256_bytes  # noqa: E402
from backend.nhl.sog_hgb_features import _roll  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_pp_component_2026-11-15"
PP_EVIDENCE = ROOT / "artifacts/analysis/nhl/sog_situation_ablation_2026-11-15"
KEYS = ["game_id", "player_id"]
PP_FEATURES = ["pp_pts60_shrunk", "exp_pp_min", "exp_pp_points"]
PRIOR_GRID = (20.0, 40.0, 80.0, 160.0, 320.0, 640.0, 1280.0)
SKATERS_ON_PP = 5.0


def pp_table() -> pd.DataFrame:
    m = json.loads((PP_EVIDENCE / "official_powerplay_manifest.json").read_text())
    rows = []
    for day, entry in sorted(m.items()):
        body = (PP_EVIDENCE / "official" / "powerplay" / f"{day}.json").read_bytes()
        if sha256_bytes(body) != entry["sha256"]:
            raise RuntimeError(f"EVIDENCE_HASH_MISMATCH:{day}")
        for r in json.loads(body)["data"]:
            rows.append({"game_id": int(r["gameId"]), "player_id": int(r["playerId"]), "game_date": r["gameDate"],
                         "team": r["teamAbbrev"], "group": "D" if str(r["positionCode"]).upper() == "D" else "F",
                         "pp_min": float(r["ppTimeOnIce"] or 0) / 60, "pp_points": float(r["ppPoints"] or 0),
                         "pp_goals": float(r["ppGoals"] or 0)})
    t = pd.DataFrame(rows).drop_duplicates(KEYS)
    t["game_date"] = pd.to_datetime(t.game_date)
    t["season"] = t.game_id // 1_000_000
    return t.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)


def add_pp_features(t: pd.DataFrame, prior_minutes: float) -> pd.DataFrame:
    t = t.copy()
    lg = t.groupby(["season", "group"]).agg(p=("pp_points", "sum"), m=("pp_min", "sum")).reset_index()
    lg["lg_pp_pts60"] = lg.p / lg.m * 60
    prev = lg.assign(season=lg.season + 1)[["season", "group", "lg_pp_pts60"]]
    first = lg[lg.season == lg.season.min()][["season", "group", "lg_pp_pts60"]]
    t = t.merge(pd.concat([prev, first]).drop_duplicates(["season", "group"]), on=["season", "group"], how="left")
    t = t.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    p = "player_id"
    pts, mins = _roll(t, p, "pp_points", 60, "sum").fillna(0), _roll(t, p, "pp_min", 60, "sum").fillna(0)
    t["pp_pts60_shrunk"] = (pts + t.lg_pp_pts60 * prior_minutes / 60) / (mins + prior_minutes) * 60
    team_pp = (t.groupby(["game_id", "team"]).pp_min.sum() / SKATERS_ON_PP).rename("team_pp").reset_index()
    games = t.groupby("game_id").team.unique()
    pairs = [(g, a, [o for o in ts if o != a][0]) for g, ts in games.items() if len(ts) == 2 for a in ts]
    tg = pd.DataFrame(pairs, columns=["game_id", "team", "opp"]).merge(team_pp, on=["game_id", "team"])
    tg = tg.merge(team_pp.rename(columns={"team": "opp", "team_pp": "opp_pp"}), on=["game_id", "opp"])
    tg = tg.merge(t.drop_duplicates("game_id")[["game_id", "game_date"]], on="game_id")
    tg = tg.sort_values(["team", "game_date", "game_id"])
    tg["team_pp_min_20"] = _roll(tg, "team", "team_pp", 20, "mean")
    tg["pp_conceded_20"] = _roll(tg, "team", "opp_pp", 20, "mean")
    t = t.merge(tg[["game_id", "team", "opp", "team_pp", "team_pp_min_20"]], on=["game_id", "team"], how="left")
    t = t.merge(tg[["game_id", "team", "pp_conceded_20"]].rename(columns={"team": "opp", "pp_conceded_20": "opp_conceded_20"}),
                on=["game_id", "opp"], how="left")
    t = t.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    share = _roll(t, p, "pp_min", 10, "sum") / _roll(t, p, "team_pp", 10, "sum")
    t["exp_pp_min"] = share.where(np.isfinite(share)) * (t.team_pp_min_20 + t.opp_conceded_20) / 2
    t["exp_pp_points"] = t.pp_pts60_shrunk * t.exp_pp_min / 60
    return t


def choose_prior(t: pd.DataFrame, train_keys: pd.DataFrame) -> tuple[float, dict]:
    """Prior weight with the best Poisson log likelihood of PP points on the 2023-24 training rows."""
    out = {}
    for k in PRIOR_GRID:
        f = add_pp_features(t, k).merge(train_keys, on=KEYS)
        mu = f.exp_pp_points.clip(1e-4).fillna(1e-4)
        out[k] = float(-poisson.logpmf(f.pp_points.astype(int), mu).mean())
    return min(out, key=out.get), out


def fit_parts(train: pd.DataFrame, feats: list[str], goals: str, assists: str) -> dict:
    return {"structure": "GOALS_PLUS_ASSISTS", "feature_columns": feats,
            "goals": ga.fit_count(train, feats, goals), "assists": ga.fit_count(train, feats, assists)}


def expected(frame: pd.DataFrame, model: dict) -> np.ndarray:
    return scorer.score(frame, model).expected_points.to_numpy()


def component_targets(frame: pd.DataFrame) -> list[dict]:
    out = []
    for name, pred, actual in (("PP minutes tonight", "exp_pp_min", "pp_min"),
                               ("PP points (rate x tonight's PP minutes)", "exp_pp_points", "pp_points"),
                               ("Non-PP points (P2 fitted part)", "P2_nonpp", "nonpp_points")):
        g = frame[[pred, actual]].dropna()
        out.append({"component": name, "n": int(len(g)), "mean_pred": float(g[pred].mean()),
                    "mean_actual": float(g[actual].mean()), "mae": float((g[pred] - g[actual]).abs().mean()),
                    "corr": float(g[pred].corr(g[actual]))})
    return out


def evaluate(frame: pd.DataFrame, name: str) -> dict:
    cols = {"P0_production": "P0", "P1_pp_inputs": "P1", "P2_components": "P2"}
    res = {"rows": int(len(frame)),
           "paired_vs_P0": {k: rr.paired(frame, c, "P0") for k, c in cols.items() if c != "P0"},
           "component_targets": component_targets(frame)}
    for line in (0.5, 1.5, 2.5):
        y = (frame.realized_points > line).astype(float).to_numpy()
        res.setdefault("log_loss", {})[str(line)] = {
            k: float(rr.base.log_loss_rows(poisson.sf(int(line), frame[c].to_numpy()), y).mean()) for k, c in cols.items()}
    pb, imp = rr.bands(frame, cols)
    pb.to_csv(OUT / f"{name}_by_pp_band.csv", index=False)
    imp.to_csv(OUT / f"{name}_imp037_band.csv", index=False)
    return res


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    train = pd.read_csv(freeze.PP_OUT / "training_frame_2023_2024.csv.gz", float_precision="round_trip")
    hold = pd.read_csv(freeze.PP_OUT / "holdout_frame_2025.csv.gz", float_precision="round_trip")
    production = joblib.load(scorer.MODEL_PATH)
    base_feats = list(production["feature_columns"])
    t = pp_table()
    prior, grid = choose_prior(t, train[KEYS])
    t = add_pp_features(t, prior)
    cols = KEYS + PP_FEATURES + ["pp_points", "pp_goals", "pp_min"]
    summary: dict = {"prior_minutes_grid_logloss": grid, "prior_minutes": prior,
                     "production_identity": json.loads(scorer.CONTRACT_PATH.read_text())["model_identity_sha256"]}
    frames = {}
    for nm, f in (("train", train), ("hold", hold)):
        m = f.merge(t[cols], on=KEYS, how="left")
        summary[f"{nm}_pp_known_share"] = float(m.pp_min.notna().mean())
        m[["pp_points", "pp_goals"]] = m[["pp_points", "pp_goals"]].fillna(0)
        m["nonpp_goals"] = (m.goals - m.pp_goals).clip(lower=0)
        m["nonpp_assists"] = (m.assists - (m.pp_points - m.pp_goals)).clip(lower=0)
        m["nonpp_points"] = m.nonpp_goals + m.nonpp_assists
        frames[nm] = m
    tr, ho = frames["train"], frames["hold"]
    p1 = fit_parts(tr, base_feats + PP_FEATURES, "goals", "assists")
    p2 = fit_parts(tr, base_feats, "nonpp_goals", "nonpp_assists")

    def score_all(f: pd.DataFrame) -> pd.DataFrame:
        f = f.copy()
        f["P0"] = expected(f, production)
        f["P1"] = expected(f, p1)
        f["P2_nonpp"] = expected(f, p2)
        f["P2"] = f.P2_nonpp + f.exp_pp_points.fillna(0).clip(lower=0)
        return f

    ho = score_all(ho)
    summary["holdout"] = evaluate(ho, "holdout")

    # Live 2026-11-01..11-14: latest pregame capture per graded player-game (as in P2.53).
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    caps = []
    for day in weekly._days(rr.LIVE_START, rr.LIVE_END):
        for path in sorted((rr.CAPTURES / f"slate_date={day}").glob("run_id=*/phase=*/captured_at=*/features.csv")):
            c = pd.read_csv(path)
            c["captured_at"] = path.parent.name.split("=", 1)[1]
            caps.append(c)
    live = pd.concat(caps, ignore_index=True)
    live = live[pd.to_datetime(live.captured_at, format="%Y%m%dT%H%M%S%fZ", utc=True)
                < pd.to_datetime(live.game_start_utc, utc=True)]
    live = live.sort_values("captured_at").drop_duplicates(KEYS, keep="last")
    graded = []
    for day in weekly._days(rr.LIVE_START, rr.LIVE_END):
        s = weekly._summary(day)
        if s:
            r = weekly._cached_rows("points", day, s["summary_identity"])
            if not r.empty:
                r = r.assign(over=np.where(r.selected_side.eq("OVER"), r.grading_state.eq("WIN"), r.grading_state.ne("WIN")))
                graded.append(r[["game_id", "player_id", "line", "over"]])
    pts = (pd.concat(graded).pivot_table(index=KEYS, columns="line", values="over", aggfunc="first")
           .reindex(columns=[0.5, 1.5, 2.5]).dropna().astype(int))
    pts["realized_points"] = pts[0.5] + pts[1.5] + pts[2.5]
    live = live.merge(pts[["realized_points"]].reset_index(), on=KEYS, how="inner")
    # Live captures predate the PP repair; use corrected PP minutes, as production now does.
    lt = live[KEYS + ["game_date"]].assign(game_date=lambda d: pd.to_datetime(d.game_date))
    logs = rr.db_logs()
    w = rr.window_means(logs[logs.toi_minutes.gt(0)][KEYS + ["game_date", "toi_minutes", "pp_toi_minutes"]], lt)
    live = live.merge(w, on=KEYS, how="left")
    live["mean_pp_toi_last10"] = live.pp_corrected.fillna(live.mean_pp_toi_last10)
    live = live.merge(t[cols], on=KEYS, how="left")
    live[["pp_points", "pp_goals"]] = live[["pp_points", "pp_goals"]].fillna(0)
    live["nonpp_points"] = (live.realized_points - live.pp_points).clip(lower=0)
    live["slate_date"] = live.game_date.astype(str)
    live = score_all(live)
    summary["live"] = evaluate(live, "live")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    print(json.dumps({k: summary[k] for k in ("prior_minutes", "prior_minutes_grid_logloss")}, indent=1))
    for part in ("holdout", "live"):
        s = summary[part]
        print(part, s["rows"], json.dumps(s["paired_vs_P0"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
