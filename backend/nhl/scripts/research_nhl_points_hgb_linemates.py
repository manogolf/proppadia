#!/usr/bin/env python3
"""Research: linemate quality and power-play unit as Points inputs (P2.39, 2026-11-01).

Adds to the own-rate candidate HGB+OWN_k30 (research_nhl_points_hgb_own_rate.py):

- MATES: the player's two most-overlapping skater linemates in his previous
  game this season (second-by-second shared ice time from official shift
  charts, goalies excluded; 2023-24 from the shift charts retained by the
  2026-10-16 PP-TOI recovery, 2025-26 from ``nhl.shiftcharts_shifts``), scored
  by their own scoring rate going into tonight's game (``own_rate_k30``), and
  the share of his ice time spent with the top linemate.
- PP: the player's share of his team's power-play time (his PP TOI over the
  team's largest skater PP TOI that game), mean of the last 10 games.

Judged by the operator's per-tier gate on the 2025 holdout (n >= 200, no
tier-line worse than production), excluding low-usage forwards (operator
2026-11-01: excluded and revisited later), plus 2026 two-sided lines.
Read-only; nothing changes production.
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
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/linemates_2026-11-01"
SOURCES = ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources"
EXCLUDED_TIERS = {"low_usage_forwards (F, TOI < 13)"}
KEYS = ["game_id", "player_id"]


def _sec(t: str) -> int:
    m, s = str(t).split(":")
    return int(m) * 60 + int(s)


def top_mates(shifts: pd.DataFrame) -> pd.DataFrame:
    """shifts: game_id, team_id, player_id, a, b (seconds since puck drop). Top-2 skater mates."""
    out = []
    for (g, t), grp in shifts.groupby(["game_id", "team_id"], sort=False):
        players = grp.player_id.unique()
        idx = {p: i for i, p in enumerate(players)}
        m = np.zeros((int(grp.b.max()) + 1, len(players)), dtype=np.float32)
        for p, a, b in zip(grp.player_id, grp.a, grp.b):
            m[a:b, idx[p]] = 1
        o = m.T @ m
        toi = np.diag(o).copy()
        np.fill_diagonal(o, 0)
        order = np.argsort(-o, axis=1)
        for i, p in enumerate(players):
            j1, j2 = order[i, 0], order[i, 1] if len(players) > 2 else order[i, 0]
            out.append((int(g), int(t), int(p), int(toi[i]), int(players[j1]), float(o[i, j1] / toi[i]) if toi[i] else np.nan,
                        int(players[j2]), float(o[i, j2] / toi[i]) if toi[i] else np.nan))
    return pd.DataFrame(out, columns=["game_id", "team_id", "player_id", "toi_sec", "mate1", "mate1_share", "mate2", "mate2_share"])


def shifts_2023_24() -> pd.DataFrame:
    rows = []
    for folder in sorted(SOURCES.glob("game=*")):
        sc, bx = folder / "shiftcharts.json.gz", folder / "boxscore.json.gz"
        if not sc.is_file() or not bx.is_file():
            continue
        box = json.loads(gzip.open(bx).read())
        goalies = {int(g["playerId"]) for side in ("homeTeam", "awayTeam")
                   for g in (box.get("playerByGameStats") or {}).get(side, {}).get("goalies") or []}
        for r in json.loads(gzip.open(sc).read()).get("data", []):
            if r.get("typeCode") != 517 or not r.get("startTime") or not r.get("endTime") or int(r["playerId"]) in goalies:
                continue
            a = (int(r["period"]) - 1) * 1200 + _sec(r["startTime"])
            b = (int(r["period"]) - 1) * 1200 + _sec(r["endTime"])
            if b > a:
                rows.append((int(r["gameId"]), int(r["teamId"]), int(r["playerId"]), a, b))
    return pd.DataFrame(rows, columns=["game_id", "team_id", "player_id", "a", "b"])


def shifts_2025_26() -> pd.DataFrame:
    with psycopg.connect(database_url("research", environ=refit.research_env()), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT s.game_id::bigint, s.team_id, s.player_id::bigint, s.period, s.start_sec, s.end_sec
                       FROM nhl.shiftcharts_shifts s JOIN nhl.games g USING(game_id) JOIN nhl.players p USING(player_id)
                       WHERE g.season IN (2025, 2026) AND substring(g.game_id::text,5,2)='02' AND upper(p.position) <> 'G'""")
        d = pd.DataFrame(cur.fetchall(), columns=["game_id", "team_id", "player_id", "period", "start_sec", "end_sec"])
    for c_ in ("period", "start_sec", "end_sec"):
        d[c_] = pd.to_numeric(d[c_], errors="coerce")
    d = d.dropna()
    d["a"] = ((d.period - 1) * 1200 + d.start_sec).astype(int)
    d["b"] = ((d.period - 1) * 1200 + d.end_sec).astype(int)
    return d[d.b > d.a][["game_id", "team_id", "player_id", "a", "b"]]


def pp_share(logs: pd.DataFrame) -> pd.DataFrame:
    """Strict-prior mean (last 10 games) of the player's share of team PP time."""
    x = logs[["canonical_season", "game_date", "game_id", "player_id", "team_id", "pp_toi_minutes"]].copy()
    x["pp"] = pd.to_numeric(x.pp_toi_minutes, errors="coerce")
    team_max = x.groupby(["game_id", "team_id"]).pp.transform("max")
    x["share"] = (x.pp / team_max).where(team_max > 0)
    x = x.sort_values(["player_id", "game_date", "game_id"])
    by = x.groupby(["player_id", "canonical_season"], sort=False)
    x["pp_share_l10"] = by.share.transform(lambda v: v.shift(1).rolling(10, min_periods=1).mean())
    return x[KEYS + ["pp_share_l10"]]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    cache = OUT / "inputs.joblib"
    if not cache.exists():
        mates = pd.concat([top_mates(shifts_2023_24()), top_mates(shifts_2025_26())], ignore_index=True)
        logs, audit = refit.pull_history()
        joblib.dump({"mates": mates, "pp": pp_share(logs),
                     "dates": logs[["game_id", "game_date", "canonical_season"]].drop_duplicates("game_id"),
                     "login": audit["login"], "snapshot_utc": audit["transaction_timestamp_utc"]}, cache)
    inputs = joblib.load(cache)
    mates, pp, dates = inputs["mates"], inputs["pp"], inputs["dates"]
    data = joblib.load(base.OUT / "matrices.joblib")
    api = pd.read_csv(own.SEASON_2022)
    totals = pd.concat([data["season_totals"], pd.DataFrame({"player_id": api.player_id, "canonical_season": 2022,
                                                              "season_points": api.points, "season_games": api.games})],
                       ignore_index=True)
    hist = own.add_own_rate(data["historical"], totals)
    f26 = own.add_own_rate(data["2026"], totals).reset_index(drop=True)
    rate = pd.concat([hist[KEYS + ["own_rate_k30"]], f26[KEYS + ["own_rate_k30"]]]).drop_duplicates(KEYS)
    # Previous game's linemates (same season), then their own rates tonight.
    m = mates.merge(dates, on="game_id", how="left").sort_values(["player_id", "game_date", "game_id"])
    by = m.groupby(["player_id", "canonical_season"], sort=False)
    for c_ in ("mate1", "mate2", "mate1_share"):
        m[f"prev_{c_}"] = by[c_].shift(1)
    m = m[KEYS + ["prev_mate1", "prev_mate2", "prev_mate1_share"]]

    def attach(frame: pd.DataFrame) -> pd.DataFrame:
        f = frame.merge(m, on=KEYS, how="left").merge(pp, on=KEYS, how="left")
        for k in ("1", "2"):
            f = f.merge(rate.rename(columns={"player_id": f"prev_mate{k}", "own_rate_k30": f"mate{k}_rate"}),
                        on=["game_id", f"prev_mate{k}"], how="left")
        f["mate_rate_top2"] = f[["mate1_rate", "mate2_rate"]].mean(axis=1)
        f["mate_share"] = f.prev_mate1_share
        return f

    hist, f26 = attach(hist), attach(f26).reset_index(drop=True)
    rows = data["market_2026"].merge(f26[KEYS], on=KEYS, how="inner").reset_index(drop=True)
    train = hist[hist.canonical_season.isin([2023, 2024])].reset_index(drop=True)
    hold = hist[hist.canonical_season.eq(2025)].reset_index(drop=True)
    cover = {name: {c_: float(fr[c_].notna().mean()) for c_ in ("mate_rate_top2", "mate_share", "pp_share_l10")}
             for name, fr in (("train", train), ("holdout", hold), ("2026", f26))}
    k30 = own.CORE + ["own_rate_k30", "own_games"]
    specs = {"HGB+OWN_k30": k30, "HGB+OWN_k30+MATES": k30 + ["mate_rate_top2", "mate_share"],
             "HGB+OWN_k30+PP": k30 + ["pp_share_l10"],
             "HGB+OWN_k30+MATES+PP": k30 + ["mate_rate_top2", "mate_share", "pp_share_l10"]}
    models = {k: own.fit_hgb(train, f) for k, f in specs.items()}
    joblib.dump(models, OUT / "candidates.joblib")
    production = joblib.load(own.scorer.MODEL_PATH)
    hp = {"PRODUCTION": own.predict(production, hold), **{k: own.predict(v, hold) for k, v in models.items()}}
    p26 = {"PRODUCTION": own.predict(production, f26), **{k: own.predict(v, f26) for k, v in models.items()}}
    hold_rows, levels = own.holdout_tables(hp, hold)
    gate_rows = hold_rows[~hold_rows.tier.isin(EXCLUDED_TIERS)]
    gates = own.gate(gate_rows)
    market = own.market_tables(p26, rows, f26)
    step = []
    pts, d = hold.realized_points.to_numpy(float), hold.slate_date.astype(str).to_numpy()
    for tier, mask in own.tiers(hold).items():
        for label in [k for k in models if k != "HGB+OWN_k30"]:
            for line in own.LINES:
                y = (pts > line).astype(float)
                r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), hp[label]), y)[mask],
                                          base.log_loss_rows(poisson.sf(int(line), hp["HGB+OWN_k30"]), y)[mask], d[mask])
                step.append({"tier": tier, "line": line, "model": label, "n": r["n"],
                             "diff_vs_own_k30_x1000": round(r["mean_diff"] * 1e3, 2),
                             "ci_x1000": [round(c_ * 1e3, 2) for c_ in r["ci95"]]})
    step = pd.DataFrame(step)
    for name, frame in (("holdout_paired_by_tier", hold_rows), ("holdout_levels_by_tier", levels),
                        ("tier_gate_excluding_low_usage_forwards", gates), ("two_sided_2026_by_tier", market),
                        ("step_vs_own_k30", step)):
        frame.to_csv(OUT / f"{name}.csv", index=False)
    summary = {"coverage": cover, "excluded_tiers": sorted(EXCLUDED_TIERS), "candidates": specs,
               "linemate_rows": int(len(mates)), "linemate_games": int(mates.game_id.nunique()),
               "database_login": inputs["login"], "snapshot_utc": inputs["snapshot_utc"]}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    pd.set_option("display.width", 250); pd.set_option("display.max_rows", 1000); pd.set_option("display.max_colwidth", 60)
    print(json.dumps(summary, indent=1, default=str))
    print(gates.to_string(index=False))
    print(step[step.n >= own.MIN_TIER].pivot_table(index=["tier", "line"], columns="model",
                                                   values="diff_vs_own_k30_x1000").to_string())
    print(market.pivot_table(index="tier", columns="model", values="diff_vs_production_x1000").to_string())


if __name__ == "__main__":
    main()
