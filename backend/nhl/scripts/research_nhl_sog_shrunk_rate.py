#!/usr/bin/env python3
"""Research (P2.32): does a shrunk, cleaned shot rate beat production SOG?

Read-only. Rebuilds strict-prior expected shots for every regular-season
skater-game from `nhl.skater_game_logs_raw` with four recipes:
- PROD: the production formula as computed today (last 10 logged games of any
  type for the rate, missing SOG counted as 0 and shots on rows without TOI
  kept; last 10 games with TOI for exposure; d20/d5 fallbacks);
- CLEAN10: same 10-game rate, regular-season games with TOI only;
- CLEAN20: 20-game rate, same cleaning;
- SHRUNK(k): CLEAN20 rate pulled toward the prior season's league rate for the
  player's position with k minutes of prior weight.
Exposure for the cleaned recipes is the mean TOI of the last 10 cleaned games.
k is chosen on 2025-10..2025-12 and scored on 2026-01..2026-04 only.
Also pairs 2026 live production rows with shadow arms C, D and G.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg
from dotenv import dotenv_values
from scipy.stats import poisson

from backend.nhl.database_access import database_url

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/sog_shrunk_rate/2026-10-25"
K_GRID = (30, 60, 90, 120, 180, 240, 360, 480)


def pull() -> tuple[pd.DataFrame, pd.DataFrame]:
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT l.player_id, l.game_id, g.game_date, g.season, substring(g.game_id::text,5,2) AS game_type,
                              l.shots_on_goal, l.toi_minutes
                       FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING (game_id)
                       WHERE g.season IN (2024, 2025, 2026) AND g.game_date < CURRENT_DATE""")
        logs = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        cur.execute("SELECT player_id, position FROM nhl.players")
        pos = pd.DataFrame(cur.fetchall(), columns=["player_id", "position"])
    for col in ("shots_on_goal", "toi_minutes"):
        logs[col] = pd.to_numeric(logs[col], errors="coerce")
    logs["game_date"] = pd.to_datetime(logs.game_date)
    pos["group"] = np.where(pos.position.astype(str).str.upper().str.startswith("D"), "D", "F")
    return logs.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True), pos[["player_id", "group"]]


def _prior_window(frame: pd.DataFrame, value: str, n: int, how: str) -> pd.Series:
    """Strict-prior rolling sum/mean/count over each player's previous n rows."""
    g = frame.groupby("player_id", sort=False)[value]
    rolled = g.transform(lambda s: getattr(s.shift(1).rolling(n, min_periods=1), how)())
    return rolled


def features(logs: pd.DataFrame) -> pd.DataFrame:
    """Per target row: production and cleaned window sums, strictly before the game date."""
    allrows = logs.copy()
    allrows["sog0"] = allrows.shots_on_goal.fillna(0)
    allrows["toi_pos"] = allrows.toi_minutes.where(allrows.toi_minutes > 0)
    for n in (5, 10, 20):
        allrows[f"p_sog{n}"] = _prior_window(allrows, "sog0", n, "sum")
        allrows[f"p_toi{n}"] = _prior_window(allrows, "toi_pos", n, "sum")
    toi_rows = allrows[allrows.toi_pos.notna()].copy()
    for n in (5, 10, 20):
        toi_rows[f"p_toiavg{n}"] = _prior_window(toi_rows, "toi_pos", n, "mean")
    clean = allrows[(allrows.game_type == "02") & allrows.toi_pos.notna() & allrows.shots_on_goal.notna()].copy()
    for n in (10, 20):
        clean[f"c_sog{n}"] = _prior_window(clean, "shots_on_goal", n, "sum")
        clean[f"c_toi{n}"] = _prior_window(clean, "toi_pos", n, "sum")
    clean["c_toiavg10"] = _prior_window(clean, "toi_pos", 10, "mean")
    out = allrows.merge(toi_rows[["player_id", "game_id", "p_toiavg5", "p_toiavg10", "p_toiavg20"]], on=["player_id", "game_id"], how="left")
    out = out.merge(clean[["player_id", "game_id", "c_sog10", "c_toi10", "c_sog20", "c_toi20", "c_toiavg10"]], on=["player_id", "game_id"], how="left")
    # same-day rows must not see each other: windows are row-shifted, and players play once per date
    return out


def recipes(f: pd.DataFrame, league: dict, k_grid=K_GRID) -> pd.DataFrame:
    rate = lambda s, t: (s / t * 60).where(t > 0)
    prod_rate = rate(f.p_sog10, f.p_toi10).fillna(rate(f.p_sog20, f.p_toi20)).fillna(rate(f.p_sog5, f.p_toi5))
    prod_toi = f.p_toiavg10.fillna(f.p_toiavg20).fillna(f.p_toiavg5)
    out = pd.DataFrame({"PROD": prod_rate * prod_toi / 60})
    out["CLEAN10"] = rate(f.c_sog10, f.c_toi10) * f.c_toiavg10 / 60
    out["CLEAN20"] = rate(f.c_sog20, f.c_toi20) * f.c_toiavg10 / 60
    prior = f.group.map(league).fillna(np.mean(list(league.values())))
    for k in k_grid:
        shrunk = (f.c_sog20.fillna(0) + prior * k / 60) / (f.c_toi20.fillna(0) + k) * 60
        out[f"SHRUNK_{k}"] = shrunk * f.c_toiavg10 / 60
    return out


def score(y: np.ndarray, lam: np.ndarray) -> dict:
    lam = np.clip(lam, 0.02, None)
    row = {"n": int(len(y)), "mean_pred": float(lam.mean()), "mean_actual": float(y.mean()),
           "mae": float(np.abs(y - lam).mean()), "poisson_log_loss": float(-poisson.logpmf(y, lam).mean()),
           "slope": float(np.polyfit(lam, y, 1)[0])}
    for line, k in ((1.5, 2), (2.5, 3), (3.5, 4)):
        p = poisson.sf(k - 1, lam)
        o = (y >= k).astype(int)
        row[f"brier_{line}"] = float(np.mean((p - o) ** 2))
        row[f"wl_{line}"] = float(np.mean((p >= 0.5) == (o == 1)))
    return row


def league_rates(logs: pd.DataFrame, pos: pd.DataFrame, season: int) -> dict:
    s = logs[(logs.season == season) & (logs.game_type == "02") & (logs.toi_minutes > 0)].merge(pos, on="player_id", how="left")
    s["group"] = s.group.fillna("F")
    g = s.groupby("group")
    return (g.shots_on_goal.sum() / g.toi_minutes.sum() * 60).to_dict()


def season_2025(logs, pos) -> tuple[pd.DataFrame, dict]:
    f = features(logs).merge(pos, on="player_id", how="left")
    f["group"] = f.group.fillna("F")
    t = f[(f.season == 2025) & (f.game_type == "02") & (f.toi_minutes > 0) & f.shots_on_goal.notna()].copy()
    lam = recipes(t, league_rates(logs, pos, 2024))
    ok = lam.notna().all(axis=1)
    t, lam = t[ok], lam[ok]
    tune = (t.game_date < "2026-01-01").to_numpy()
    y = t.shots_on_goal.to_numpy(int)
    tune_scores = {c: score(y[tune], lam[c].to_numpy()[tune])["poisson_log_loss"] for c in lam if c.startswith("SHRUNK")}
    best = min(tune_scores, key=tune_scores.get)
    rows = []
    for name in ("PROD", "CLEAN10", "CLEAN20", best):
        for window, mask in (("tune Oct-Dec", tune), ("test Jan-Apr", ~tune)):
            rows.append({"recipe": name, "window": window, **score(y[mask], lam[name].to_numpy()[mask])})
    # paired bootstrap on the test window, clustered by date
    test = t[~tune].assign(prod=lam.PROD.to_numpy()[~tune], best=lam[best].to_numpy()[~tune])
    test["d_ll"] = (-poisson.logpmf(test.shots_on_goal, test.best.clip(0.02))) - (-poisson.logpmf(test.shots_on_goal, test["prod"].clip(0.02)))
    by_date = test.groupby("game_date").d_ll.agg(["sum", "size"])
    rng = np.random.default_rng(20261025)
    dates = by_date.index.to_numpy()
    boots = []
    for _ in range(5000):
        pick = by_date.loc[rng.choice(dates, len(dates))]
        boots.append(pick["sum"].sum() / pick["size"].sum())
    ci = [float(np.percentile(boots, 2.5)), float(np.percentile(boots, 97.5))]
    return pd.DataFrame(rows), {"best_k": best, "tune_log_loss_by_k": tune_scores,
                                "test_paired_log_loss_diff_best_minus_prod": float(test.d_ll.mean()),
                                "test_ci95_date_clustered": ci, "test_rows": int(len(test)),
                                "league_rate_prior_2024": league_rates(logs, pos, 2024)}


def season_2026_paired(logs, pos, best_k: int) -> pd.DataFrame:
    from backend.nhl.reconciliation_grades import restated_grade_path
    f = features(logs).merge(pos, on="player_id", how="left")
    f["group"] = f.group.fillna("F")
    t = f[(f.season == 2026) & (f.game_type == "02")].copy()
    lam = recipes(t, league_rates(logs, pos, 2025), k_grid=(best_k,))
    t["SHRUNK"] = lam[f"SHRUNK_{best_k}"].to_numpy()
    t["slate_date"] = t.game_date.dt.strftime("%Y-%m-%d")
    pieces = []
    for pkg in sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation").glob("2026-1*/reconciliation=*")):
        day = pkg.parent.name
        if day < "2026-10-08":
            continue
        path = restated_grade_path(pkg, "graded_resilient_sog.csv")
        if not path.is_file():
            continue
        g = pd.read_csv(path, low_memory=False)
        g = g[(g.line == 1.5) & g.official_sog.notna()]
        pieces.append(g[["slate_date", "game_id", "player_id", "contract_arm", "expected_sog", "official_sog"]])
    arms = pd.concat(pieces).drop_duplicates(["slate_date", "game_id", "player_id", "contract_arm"], keep="last")
    wide = arms.pivot_table(index=["slate_date", "game_id", "player_id"], columns="contract_arm", values="expected_sog").reset_index()
    actual = arms.drop_duplicates(["slate_date", "game_id", "player_id"])[["slate_date", "game_id", "player_id", "official_sog"]]
    wide = wide.merge(actual, on=["slate_date", "game_id", "player_id"]).merge(
        t[["slate_date", "game_id", "player_id", "SHRUNK"]], on=["slate_date", "game_id", "player_id"], how="inner")
    cols = [c for c in ("PRODUCTION_REFERENCE", "C_MULTISEASON_SHRUNK_PLAYER", "D_PLAYER_ROLE_HIERARCHICAL",
                        "G_COLD_START_TO_CURRENT_SEASON_BLEND", "SHRUNK") if c in wide]
    paired = wide.dropna(subset=cols)
    y = paired.official_sog.to_numpy(int)
    rows = [{"recipe": c, **score(y, paired[c].to_numpy())} for c in cols]
    return pd.DataFrame(rows).assign(window=f"2026 {paired.slate_date.min()}..{paired.slate_date.max()} paired")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    logs, pos = pull()
    s25, meta = season_2025(logs, pos)
    s25.to_csv(OUT / "season_2025_out_of_sample.csv", index=False)
    best_k = int(meta["best_k"].split("_")[1])
    s26 = season_2026_paired(logs, pos, best_k)
    s26.to_csv(OUT / "season_2026_paired.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps({**meta, "season_2026_rows": s26.n.tolist()}, indent=2, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(s25.round(4).to_string(index=False)); print(json.dumps(meta, indent=1, default=str)[:1500]); print(s26.round(4).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
