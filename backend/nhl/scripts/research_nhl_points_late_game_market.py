#!/usr/bin/env python3
"""Research: late-game situation and market-implied team scoring for Points (P2.39, 2026-11-01).

Two operator-requested tests on top of HGB+OWN_k30 (research_nhl_points_hgb_own_rate.py),
using only data already held (no new provider spend):

A. Late game. Late scoring is mostly empty-net goals, which go to the
   leading team's players who are on the ice. Inputs: the player's empty-net
   and last-five-minutes points per game (previous season plus this season so
   far, shrunk to the league by 40 games; from scorer and assist IDs in the
   retained 2023-25 play-by-play), and the pregame strength gap between his
   team and the opponent (shrunk goals-for minus goals-against). Checked
   against the 2025 market win probability (recovered T-15 moneylines).
B. Market-implied team scoring. From the 2025 daily player-prop captures
   (``backend/nhl/exports/odds_history``), each listed player's market expected
   points (-ln(1 - median same-book no-vig P(over 0.5))). For each player, his
   teammates' market total over the model's total for the same teammates
   (leaving his own price out) is a team environment factor, applied as
   mu x factor^beta with beta fitted on 2025 October-December and tested on
   January-April.

Gate: the operator's per-tier rule on the 2025 holdout (low-usage forwards
excluded). Read-only; nothing changes production.
"""
from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import psycopg
from scipy.optimize import minimize_scalar
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import market_name_identity as names  # noqa: E402
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import research_nhl_team_goals_model as team  # noqa: E402
from backend.nhl.scripts.run_nhl_points_hgb_pp_corrected_refit import research_env  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/late_game_market_2026-11-01"
PBP_2023_24 = ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources"
PBP_2025 = ROOT / "artifacts/analysis/nhl/points_attempts_2025_full_recovery_2026-10-16/official_pbp"
MONEYLINE_2025 = (ROOT / "artifacts/analysis/model_development/nhl_season_2025_historical_market_and_sog_recovery_v1"
                  / "2026-09-15/normalized_moneyline_history.csv")
ODDS_HISTORY = ROOT / "backend/nhl/exports/odds_history"
KEYS = ["game_id", "player_id"]
K_EN = 40
EXCLUDED = {"low_usage_forwards (F, TOI < 13)"}


def _sec(t: str) -> int:
    m, s = str(t).split(":")
    return int(m) * 60 + int(s)


def point_events() -> pd.DataFrame:
    """One row per player point: game, season, player, empty-net flag, last-5-minutes flag."""
    paths = [(p / "pbp.json.gz", True) for p in PBP_2023_24.glob("game=*")] + [(p, False) for p in PBP_2025.glob("game=*.json")]
    rows = []
    for path, gz in paths:
        if not path.is_file():
            continue
        d = json.loads(gzip.open(path).read() if gz else path.read_bytes())
        if int(d.get("gameType", 0)) != 2:
            continue
        home = int(d["homeTeam"]["id"])
        for e in d.get("plays", []):
            if e.get("typeDescKey") != "goal" or e["periodDescriptor"].get("periodType") == "SO":
                continue
            det, code = e.get("details", {}), str(e.get("situationCode", "1551")).zfill(4)
            is_home = int(det.get("eventOwnerTeamId")) == home
            en = (int(code[0]) if is_home else int(code[3])) == 0
            per = int(e["periodDescriptor"]["number"])
            late = per == 3 and _sec(e["timeInPeriod"]) >= 15 * 60
            for key in ("scoringPlayerId", "assist1PlayerId", "assist2PlayerId"):
                if det.get(key) is not None:
                    rows.append((int(d["id"]), int(str(d["season"])[:4]), int(det[key]), en, late))
    return pd.DataFrame(rows, columns=["game_id", "season", "player_id", "en", "late"])


def situational_rates(hist: pd.DataFrame, ev: pd.DataFrame) -> pd.DataFrame:
    """Strict-prior shrunk empty-net and late points per game for each target row."""
    per_game = ev.groupby(KEYS).agg(en_pts=("en", "sum"), late_pts=("late", "sum")).reset_index()
    h = hist[KEYS + ["canonical_season", "slate_date"]].merge(per_game, on=KEYS, how="left").fillna({"en_pts": 0, "late_pts": 0})
    h = h.sort_values(["player_id", "slate_date", "game_id"])
    season = h.groupby(["player_id", "canonical_season"]).agg(g=("game_id", "size"), en=("en_pts", "sum"), late=("late_pts", "sum")).reset_index()
    prev = season.assign(canonical_season=season.canonical_season + 1).rename(columns={"g": "pg", "en": "pen", "late": "plt"})
    h = h.merge(prev, on=["player_id", "canonical_season"], how="left").fillna({"pg": 0, "pen": 0, "plt": 0})
    by = h.groupby(["player_id", "canonical_season"], sort=False)
    h["cg"] = by.cumcount()
    h["cen"] = by.en_pts.transform(lambda v: v.shift(1).cumsum()).fillna(0)
    h["clt"] = by.late_pts.transform(lambda v: v.shift(1).cumsum()).fillna(0)
    lg = season.groupby("canonical_season").apply(lambda t: pd.Series({"en": t.en.sum() / t.g.sum(), "late": t["late"].sum() / t.g.sum()}),
                                                   include_groups=False)
    first = lg.iloc[0]
    mu_en = h.canonical_season.map(lambda s: lg.en.get(s - 1, first.en))
    mu_lt = h.canonical_season.map(lambda s: lg["late"].get(s - 1, first["late"]))
    h["en_rate"] = (h.pen + h.cen + K_EN * mu_en) / (h.pg + h.cg + K_EN)
    h["late_rate"] = (h.plt + h.clt + K_EN * mu_lt) / (h.pg + h.cg + K_EN)
    return h[KEYS + ["en_rate", "late_rate"]]


def market_win_2025() -> pd.DataFrame:
    ml = pd.read_csv(MONEYLINE_2025)
    ml = ml[(ml.market_key == "h2h") & ml.strictly_pregame.astype(bool)]
    home = ml[ml.side_orientation == "HOME"].groupby("game_id").no_vig_probability.median().rename("p_home_win")
    return home.reset_index()


def market_points_2025(players: pd.DataFrame) -> pd.DataFrame:
    """Market expected points per listed player-game from the 2025 daily prop captures.
    players: game_id, player_id, full_name (events bound to games by provider event ID)."""
    rows = []
    ml = pd.read_csv(MONEYLINE_2025, usecols=["game_id", "provider_event_id"]).drop_duplicates()
    if ml.provider_event_id.duplicated().any():
        raise RuntimeError("PROVIDER_EVENT_NOT_UNIQUE")
    event_game = dict(zip(ml.provider_event_id, ml.game_id.astype(int)))
    wanted = set(players.game_id.astype(int))
    bindings = names.load_bindings()
    for day in sorted(p.name for p in ODDS_HISTORY.iterdir() if p.is_dir() and p.name < "2026-07-01"):
        f = ODDS_HISTORY / day / "odds_event_wrappers.json"
        if not f.is_file():
            continue
        for w in json.loads(f.read_text()):
            body = w.get("data", {})
            gid = event_game.get(body.get("id"))
            if gid is None or gid not in wanted:
                continue
            roster = players[players.game_id == gid]
            slate_names = names.SlateNames(zip(roster.player_id, roster.full_name), bindings)
            prices: dict[tuple[int, str], dict[str, float]] = {}
            for b in body.get("bookmakers", []):
                for m in b.get("markets", []):
                    if m.get("key") != "player_points":
                        continue
                    for o in m.get("outcomes", []):
                        if float(o.get("point", -1)) != 0.5:
                            continue
                        pid = slate_names.player_for(o.get("description"))
                        if pid is None:
                            continue
                        price = float(o["price"])
                        imp = 100 / (price + 100) if price > 0 else -price / (-price + 100)
                        prices.setdefault((pid, b["key"]), {})[o["name"].upper()] = imp
            per_book = [(pid, v["OVER"] / (v["OVER"] + v["UNDER"])) for (pid, unused), v in prices.items() if {"OVER", "UNDER"} <= set(v)]
            if per_book:
                f2 = pd.DataFrame(per_book, columns=["player_id", "p_over"]).groupby("player_id").p_over.median().reset_index()
                f2["mu_market"] = -np.log(np.clip(1 - f2.p_over, 1e-6, 1))
                rows.append(f2.assign(game_id=gid))
    return pd.concat(rows, ignore_index=True)[["game_id", "player_id", "mu_market"]]


def ll_diff(mu_a, mu_b, pts, dates, mask=None) -> dict:
    mask = np.ones(len(pts), bool) if mask is None else mask
    out = {}
    for line in own.LINES:
        y = (pts > line).astype(float)
        r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), mu_a), y)[mask],
                                  base.log_loss_rows(poisson.sf(int(line), mu_b), y)[mask], dates[mask])
        out[str(line)] = {"diff_x1000": round(r["mean_diff"] * 1e3, 2), "ci": [round(c * 1e3, 2) for c in r["ci95"]], "n": r["n"]}
    return out


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data = joblib.load(base.OUT / "matrices.joblib")
    api = pd.read_csv(own.SEASON_2022)
    totals = pd.concat([data["season_totals"], pd.DataFrame({"player_id": api.player_id, "canonical_season": 2022,
                                                              "season_points": api.points, "season_games": api.games})], ignore_index=True)
    hist = own.add_own_rate(data["historical"], totals)
    tg, meta = joblib.load(team.OUT / "team_games.joblib")
    tf = team.features(tg)
    hist = hist.merge(meta["player_team"], on=KEYS, how="left").merge(
        tf[["game_id", "team_id", "opp_id", "gf_shr", "ga_shr", "is_home"]].rename(columns={"is_home": "team_is_home"}),
        on=["game_id", "team_id"], how="left")
    opp = tf[["game_id", "team_id", "gf_shr", "ga_shr"]].rename(columns={"team_id": "opp_id", "gf_shr": "opp_gf_shr", "ga_shr": "opp_ga_shr"})
    hist = hist.merge(opp, on=["game_id", "opp_id"], how="left")
    hist["strength_gap"] = (hist.gf_shr - hist.ga_shr) - (hist.opp_gf_shr - hist.opp_ga_shr)
    ev = point_events()
    hist = hist.merge(situational_rates(hist, ev), on=KEYS, how="left")
    train = hist[hist.canonical_season.isin([2023, 2024])].reset_index(drop=True)
    hold = hist[hist.canonical_season.eq(2025)].reset_index(drop=True)
    k30 = own.CORE + ["own_rate_k30", "own_games"]
    m0 = own.fit_hgb(train, k30)
    hold["mu"] = own.predict(m0, hold)
    pts, dates = hold.realized_points.to_numpy(float), hold.slate_date.astype(str).to_numpy()
    result = {"point_events": int(len(ev)), "en_point_share": round(float(ev.en.mean()), 4),
              "late_point_share": round(float(ev.late.mean()), 4)}
    # A. win probability residuals (2025 market) and situational inputs.
    win = market_win_2025()
    h = hold.merge(win, on="game_id", how="left")
    h["p_team_win"] = np.where(h.team_is_home == 1, h.p_home_win, 1 - h.p_home_win)
    hw = h[h.p_team_win.notna()].copy()
    hw["win_q"] = pd.qcut(hw.p_team_win, 5)
    hw["en_t"] = pd.qcut(hw.en_rate.rank(method="first"), 3, labels=["low EN", "mid EN", "high EN"])
    tab = hw.groupby(["en_t", "win_q"], observed=True).agg(n=("mu", "size"), actual=("realized_points", "mean"),
                                                          expected=("mu", "mean")).reset_index()
    tab["actual_over_expected"] = (tab.actual / tab.expected).round(3)
    tab.to_csv(OUT / "residual_by_en_and_win_prob_2025.csv", index=False)
    specs = {"+EN_LATE": ["en_rate", "late_rate"], "+STRENGTH": ["strength_gap"],
             "+EN_LATE+STRENGTH": ["en_rate", "late_rate", "strength_gap"]}
    gains, models = {}, {}
    for name, extra in specs.items():
        models[name] = own.fit_hgb(train, k30 + extra)
        hold[f"mu{name}"] = own.predict(models[name], hold)
        gains[name] = ll_diff(hold[f"mu{name}"], hold.mu, pts, dates)
    hp = {"PRODUCTION": own.predict(joblib.load(own.scorer.MODEL_PATH), hold), "HGB+OWN_k30": hold.mu.to_numpy(),
          **{f"HGB+OWN_k30{k}": hold[f"mu{k}"].to_numpy() for k in specs}}
    hold_rows, _ = own.holdout_tables(hp, hold)
    gates = own.gate(hold_rows[~hold_rows.tier.isin(EXCLUDED)])
    result["A_late_game"] = {"input_gain_vs_k30_holdout_2025": gains, "market_win_coverage": float(h.p_team_win.notna().mean())}
    # B. market-implied team scoring from teammates' props (leave own price out).
    with psycopg.connect(database_url("research", environ=research_env())) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SELECT player_id::bigint, full_name FROM nhl.players")
        pl = pd.DataFrame(cur.fetchall(), columns=["player_id", "full_name"])
    roster = hold[KEYS + ["team_id"]].merge(pl, on="player_id", how="left")
    mk = market_points_2025(roster)
    hb = hold.merge(mk, on=KEYS, how="left")
    listed = hb[hb.mu_market.notna()]
    sums = listed.groupby(["game_id", "team_id"]).agg(sm=("mu_market", "sum"), sd=("mu", "sum"), n=("mu", "size")).reset_index()
    hb = hb.merge(sums, on=["game_id", "team_id"], how="left")
    own_mk, own_md = hb.mu_market.fillna(0), np.where(hb.mu_market.notna(), hb.mu, 0)
    hb["n_others"] = hb.n - hb.mu_market.notna().astype(int)
    hb["factor"] = ((hb.sm - own_mk) / (hb.sd - own_md)).where(hb.n_others >= 3)
    has = hb.factor.notna()
    hb["month"] = pd.to_datetime(hb.slate_date).dt.month
    fit_part = has & hb.month.isin([10, 11, 12])
    test_part = has & ~hb.month.isin([10, 11, 12])

    def nll(b):
        mu = hb.mu[fit_part] * hb.factor[fit_part] ** b
        return float(-poisson.logpmf(hb.realized_points[fit_part], np.maximum(mu, 1e-9)).mean())

    beta = float(minimize_scalar(nll, bounds=(0, 2), method="bounded").x)
    hb["mu_mkt_team"] = np.where(has, hb.mu * hb.factor.fillna(1) ** beta, hb.mu)
    tp = test_part.to_numpy()
    result["B_market_team_scoring"] = {
        "prop_player_games_2025": int(len(mk)), "rows_with_factor": int(has.sum()),
        "games_with_factor": int(hb.loc[has, "game_id"].nunique()), "beta_fitted_oct_dec": round(beta, 3),
        "factor_quantiles": [round(float(q), 3) for q in hb.factor.quantile([.05, .25, .5, .75, .95])],
        "test_jan_apr_gain_vs_k30": ll_diff(hb.mu_mkt_team, hb.mu, hb.realized_points.to_numpy(float),
                                            hb.slate_date.astype(str).to_numpy(), tp),
        "test_jan_apr_unlisted_players_gain": ll_diff(hb.mu_mkt_team, hb.mu, hb.realized_points.to_numpy(float),
                                                      hb.slate_date.astype(str).to_numpy(), tp & hb.mu_market.isna().to_numpy())}
    gates.to_csv(OUT / "tier_gate_late_game_excluding_low_usage_forwards.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps(result, indent=2, default=str) + "\n")
    pd.set_option("display.width", 220)
    print(json.dumps(result, indent=1, default=str))
    print(tab.to_string(index=False))
    print(gates.to_string(index=False))


if __name__ == "__main__":
    main()
