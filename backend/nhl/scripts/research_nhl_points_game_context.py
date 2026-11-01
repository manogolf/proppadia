#!/usr/bin/env python3
"""Research: in-game scoring patterns and opponent goaltending for Points (P2.39, 2026-11-01).

Operator questions before freezing the own-rate Points model:
1. Timing: when in a game do goals (hence points) occur, and how much of that
   is power play and empty net?
2. Clusters: do goals bunch within a game more than chance would give, and
   is that momentum (the same team again) or game state (power plays, empty
   nets, the trailing team pressing)?
3. Opponent: does a weak defence and goalie raise a player's points beyond
   what HGB+OWN_k30 expects, and does it help as an input?

Data: official play-by-play retained for 2023-24 (PP-TOI recovery) and 2025
(attempts recovery); goalie logs with boxscore starters (complete after the
P2.41 backfill and correction); the team-game table and player teams from the
team-goals research. Read-only; nothing changes production.
"""
from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import saves_refit_model as srm  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import research_nhl_saves_refit as saves_research  # noqa: E402
from backend.nhl.scripts import research_nhl_team_goals_model as team  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/game_context_2026-11-01"
PBP_2023_24 = ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources"
PBP_2025 = ROOT / "artifacts/analysis/nhl/points_attempts_2025_full_recovery_2026-10-16/official_pbp"
KEYS = ["game_id", "player_id"]


def _sec(t: str) -> int:
    m, s = str(t).split(":")
    return int(m) * 60 + int(s)


def goals() -> pd.DataFrame:
    paths = [(p / "pbp.json.gz", True) for p in PBP_2023_24.glob("game=*")] + [(p, False) for p in PBP_2025.glob("game=*.json")]
    rows = []
    for path, gz in paths:
        if not path.is_file():
            continue
        d = json.loads(gzip.open(path).read() if gz else path.read_bytes())
        if int(d.get("gameType", 0)) != 2:
            continue
        home, away = int(d["homeTeam"]["id"]), int(d["awayTeam"]["id"])
        for e in d.get("plays", []):
            if e.get("typeDescKey") != "goal" or e["periodDescriptor"].get("periodType") == "SO":
                continue
            det, code = e.get("details", {}), str(e.get("situationCode", "1551")).zfill(4)
            a_g, a_s, h_s, h_g = int(code[0]), int(code[1]), int(code[2]), int(code[3])
            team_id = int(det.get("eventOwnerTeamId"))
            is_home = team_id == home
            own_s, opp_s = (h_s, a_s) if is_home else (a_s, h_s)
            opp_goalie = a_g if is_home else h_g
            per = int(e["periodDescriptor"]["number"])
            rows.append({"game_id": int(d["id"]), "season": int(str(d["season"])[:4]), "team_id": team_id,
                         "is_home": is_home, "period": per, "t": (per - 1) * 1200 + _sec(e["timeInPeriod"]),
                         "pp": own_s > opp_s and opp_goalie == 1, "sh": own_s < opp_s,
                         "en": opp_goalie == 0,
                         "points": 1 + int(det.get("assist1PlayerId") is not None) + int(det.get("assist2PlayerId") is not None),
                         "home_score": int(det.get("homeScore", 0)), "away_score": int(det.get("awayScore", 0))})
    g = pd.DataFrame(rows).sort_values(["game_id", "t"]).reset_index(drop=True)
    # Score margin for the scoring team before the goal.
    after = np.where(g.is_home, g.home_score - g.away_score, g.away_score - g.home_score)
    g["margin_before"] = after - 1
    return g


def timing(g: pd.DataFrame) -> dict:
    reg = g[g.period <= 3].copy()
    reg["minute"] = (reg.t // 60).astype(int)
    games = g.game_id.nunique()
    per_min = reg.groupby("minute").agg(goals=("t", "size"), en=("en", "sum"), pp=("pp", "sum"))
    per_min["goals_per_game_minute"] = per_min.goals / games
    bands = {"1st 0-20": (0, 20), "2nd 20-40": (20, 40), "3rd 40-50": (40, 50), "3rd 50-55": (50, 55),
             "3rd 55-60": (55, 60)}
    out = {}
    for k, (a, b) in bands.items():
        sub = per_min.loc[a:b - 1]
        out[k] = {"goals_per_game_per_minute": round(float(sub.goals.sum() / games / (b - a)), 4),
                  "empty_net_share": round(float(sub.en.sum() / max(sub.goals.sum(), 1)), 3),
                  "power_play_share": round(float(sub.pp.sum() / max(sub.goals.sum(), 1)), 3)}
    ot = g[g.period > 3]
    out["overtime"] = {"goals": int(len(ot)), "share_of_all_goals": round(len(ot) / len(g), 4)}
    out["all"] = {"games": int(games), "goals": int(len(g)), "goals_per_game": round(len(g) / games, 3),
                  "power_play_share": round(float(g.pp.mean()), 3), "empty_net_share": round(float(g.en.mean()), 3),
                  "short_handed_share": round(float(g.sh.mean()), 3), "points_per_goal": round(float(g.points.mean()), 3)}
    return out


def clustering(g: pd.DataFrame, window: int = 120, sims: int = 200, seed: int = 11) -> dict:
    """Next-goal-within-window rate: observed vs goal times redrawn from the league's
    own minute-by-minute goal distribution (keeps late-game and OT shape), same counts per game."""
    reg = g[g.period <= 3]
    rng = np.random.default_rng(seed)
    league_t = reg.t.to_numpy()

    def rate(frame):
        nxt = frame.groupby("game_id").t.shift(-1)
        return float(((nxt - frame.t) <= window).sum() / len(frame))

    obs = rate(reg)
    null = []
    counts = reg.groupby("game_id").size()
    for _ in range(sims):
        t = np.concatenate([np.sort(rng.choice(league_t, n)) for n in counts.to_numpy()])
        null.append(rate(pd.DataFrame({"game_id": np.repeat(counts.index, counts.to_numpy()), "t": t})))
    nxt = reg.groupby("game_id").shift(-1)
    quick = (nxt.t - reg.t) <= window
    same = quick & (nxt.team_id == reg.team_id)
    other = quick & (nxt.team_id != reg.team_id)
    pair = reg.assign(quick=quick, same=same, other=other, next_en=nxt.en, next_pp=nxt.pp)
    q = pair[pair.quick]
    return {"window_seconds": window, "goals": int(len(reg)),
            "observed_next_goal_within_window": round(obs, 4),
            "chance_same_counts_league_timing": round(float(np.mean(null)), 4),
            "chance_95pct": [round(float(np.quantile(null, .025)), 4), round(float(np.quantile(null, .975)), 4)],
            "ratio_observed_to_chance": round(obs / float(np.mean(null)), 3),
            "quick_followups_same_team_share": round(float(q.same.mean()), 3),
            "quick_followups_other_team_share": round(float(q.other.mean()), 3),
            "quick_followups_that_are_empty_net": round(float(q.next_en.mean()), 3),
            "quick_followups_that_are_power_play": round(float(q.next_pp.mean()), 3),
            "excluding_en_and_pp": round(rate(reg[~reg.en & ~reg.pp]), 4)}


def score_effects(g: pd.DataFrame) -> pd.DataFrame:
    """Share of the next goal scored by the team that is behind, by margin (non-empty-net goals)."""
    reg = g[(g.period <= 3) & ~g.en].copy()
    reg["home_margin_before"] = np.where(reg.is_home, reg.margin_before, -reg.margin_before)
    reg["scorer_trailing"] = reg.margin_before < 0
    reg["state"] = reg.home_margin_before.abs().clip(upper=3)
    tied = reg[reg.state == 0]
    rows = [{"state": "tied", "goals": int(len(tied)), "trailing_team_share": None}]
    for s in (1, 2, 3):
        sub = reg[reg.state == s]
        rows.append({"state": f"margin {s}{'+' if s == 3 else ''}", "goals": int(len(sub)),
                     "trailing_team_share": round(float(sub.scorer_trailing.mean()), 3)})
    return pd.DataFrame(rows)


def opponent_test() -> tuple[dict, pd.DataFrame]:
    """Model misses by opponent starting goalie save rate and defence; then as inputs."""
    gl = srm.save_rates(saves_research.goalie_logs())
    starters = gl[gl.start_flag.eq(True)][["game_id", "team_id", "save_rate", "goalie_shots_prior"]].drop_duplicates(["game_id", "team_id"])
    tg, meta = joblib.load(team.OUT / "team_games.joblib")
    tf = team.features(tg)[["game_id", "team_id", "opp_id", "ga_shr", "sa_shr"]]
    data = joblib.load(base.OUT / "matrices.joblib")
    api = pd.read_csv(own.SEASON_2022)
    totals = pd.concat([data["season_totals"], pd.DataFrame({"player_id": api.player_id, "canonical_season": 2022,
                                                              "season_points": api.points, "season_games": api.games})], ignore_index=True)
    hist = own.add_own_rate(data["historical"], totals).merge(meta["player_team"], on=KEYS, how="left")
    hist = hist.merge(tf, on=["game_id", "team_id"], how="left").merge(
        starters.rename(columns={"team_id": "opp_id", "save_rate": "opp_goalie_sv", "goalie_shots_prior": "opp_goalie_shots_prior"}),
        on=["game_id", "opp_id"], how="left")
    opp_def = tf[["game_id", "team_id", "ga_shr", "sa_shr"]].rename(
        columns={"team_id": "opp_id", "ga_shr": "opp_ga_shr", "sa_shr": "opp_sa_shr"})
    hist = hist.merge(opp_def, on=["game_id", "opp_id"], how="left")
    train = hist[hist.canonical_season.isin([2023, 2024])].reset_index(drop=True)
    hold = hist[hist.canonical_season.eq(2025)].reset_index(drop=True)
    k30 = own.CORE + ["own_rate_k30", "own_games"]
    m0 = own.fit_hgb(train, k30)
    hold["mu"] = own.predict(m0, hold)
    tables = []
    for col, label in (("opp_goalie_sv", "opponent starter save rate (shrunk)"), ("opp_ga_shr", "opponent goals against/game"),
                       ("opp_sa_shr", "opponent shots against/game")):
        h = hold[hold[col].notna()]
        q = pd.qcut(h[col], 5, duplicates="drop")
        t = h.groupby(q, observed=True).agg(n=("mu", "size"), actual=("realized_points", "mean"), expected=("mu", "mean"))
        t["actual_over_expected"] = (t.actual / t.expected).round(3)
        tables.append(t.reset_index().rename(columns={col: "quintile"}).assign(measure=label))
    h = hold[hold.opp_goalie_sv.notna() & hold.opp_ga_shr.notna()]
    combo = h.assign(weak=(h.opp_goalie_sv <= h.opp_goalie_sv.quantile(.2)) & (h.opp_ga_shr >= h.opp_ga_shr.quantile(.8)),
                     strong=(h.opp_goalie_sv >= h.opp_goalie_sv.quantile(.8)) & (h.opp_ga_shr <= h.opp_ga_shr.quantile(.2)))
    combo_rows = {k: {"n": int(combo[k].sum()), "actual": float(combo.loc[combo[k], "realized_points"].mean()),
                      "expected": float(combo.loc[combo[k], "mu"].mean())} for k in ("weak", "strong")}
    for k in combo_rows:
        combo_rows[k]["actual_over_expected"] = round(combo_rows[k]["actual"] / combo_rows[k]["expected"], 3)
    gains = {}
    for name, extra in (("+OPP_GOALIE", ["opp_goalie_sv", "opp_goalie_shots_prior"]), ("+OPP_DEFENCE", ["opp_ga_shr", "opp_sa_shr"]),
                        ("+BOTH", ["opp_goalie_sv", "opp_goalie_shots_prior", "opp_ga_shr", "opp_sa_shr"])):
        mm = own.fit_hgb(train, k30 + extra)
        mu = own.predict(mm, hold)
        pts, d = hold.realized_points.to_numpy(float), hold.slate_date.astype(str).to_numpy()
        gains[name] = {}
        for line in own.LINES:
            y = (pts > line).astype(float)
            r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), mu), y),
                                      base.log_loss_rows(poisson.sf(int(line), hold.mu), y), d)
            gains[name][str(line)] = {"diff_x1000": round(r["mean_diff"] * 1e3, 2), "ci": [round(c * 1e3, 2) for c in r["ci95"]]}
    cover = {c: float(hold[c].notna().mean()) for c in ("opp_goalie_sv", "opp_ga_shr")}
    return {"weak_vs_strong_opponent": combo_rows, "input_gain_vs_k30_holdout_2025": gains, "coverage": cover,
            "note": "opponent starter is the actual starter (known only postgame); a live input needs a pregame starter source"}, pd.concat(tables)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    g = goals()
    g.to_csv(OUT / "goals_2023_2025.csv.gz", index=False)
    result = {"timing": timing(g), "clustering_120s": clustering(g), "clustering_60s": clustering(g, 60)}
    se = score_effects(g)
    opp, opp_tables = opponent_test()
    result["opponent"] = opp
    se.to_csv(OUT / "score_effects.csv", index=False)
    opp_tables.to_csv(OUT / "opponent_quintiles.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps(result, indent=2, default=str) + "\n")
    pd.set_option("display.width", 220)
    print(json.dumps(result, indent=1, default=str))
    print(se.to_string(index=False))
    print(opp_tables.to_string(index=False))


if __name__ == "__main__":
    main()
