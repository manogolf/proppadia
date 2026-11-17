#!/usr/bin/env python3
"""Review of every running SOG shadow against current production and the market (2026-11-16).

Research only, read-only (operator: "examine sog shadows and determine which if any are beyond
being useful"). Each shadow's latest prediction made before its game started, per player-game:

  formula family (rate x TOI, started when production was the Poisson formula):
    NHL_SOG_POISSON_FORMULA_V1, NHL_SOG_SHRUNK_RATE_K180_V1, NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1,
    NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 / _25_75_V1, NHL_SOG_D10_STRUCTURAL_CORRECTION_V1
    (combined lambda)
  cold-start arms (sog_prediction_only, FINAL_PREGAME): A, B, C, D, F, G

Comparator: production NHL_SOG_COUNT_HGB_V2's own predictions for 2026-10-25..11-14 (P2.51
live_rows.csv; V2 is the current production model). Outcomes: official SOG. Market: the weekly
evaluation's two-sided no-vig P(over) per line. Metrics on the same player-games: Poisson count
log loss, binary log loss and Brier at 1.5 / 2.5 / 3.5, paired vs V2 (slate-clustered intervals),
weekly paired Brier at 1.5 (the retirement rule's measure), and log loss vs the market on
two-sided lines.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/sog_shadow_review_2026-11-16"
OPS = ROOT / "artifacts/operational/nhl"
LIVE_ROWS = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/pp_corrected_refit_2026-11-15/live_rows.csv"
KEYS = ["game_id", "player_id"]
START, END = "2026-10-25", "2026-11-14"
LINES = (1.5, 2.5, 3.5)
FORMULA = {"sog_formula_shadows": ("shadow_lambda", None), "sog_shrunk_rate_shadows": ("shadow_lambda", None),
           "sog_divergence_shadows": ("shadow_lambda", None), "sog_fixed_blend_shadows": ("shadow_lambda", None),
           "sog_structural_correction_shadows": ("combined_shadow_lambda", None)}


def run_time(path: str) -> pd.Timestamp:
    m = re.search(r"(\d{8}T\d{6})", path)
    return pd.Timestamp(m.group(1), tz="UTC")


def game_starts() -> pd.Series:
    import os
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SELECT game_id::bigint, start_time_utc FROM nhl.games WHERE season = 2026")
        s = pd.DataFrame(cur.fetchall(), columns=["game_id", "start"])
    return s.set_index("game_id").start.pipe(lambda x: pd.to_datetime(x, utc=True))


def formula_shadows(starts: pd.Series) -> pd.DataFrame:
    parts = []
    for folder, (col, _) in FORMULA.items():
        for f in (OPS / folder / "season=2026").glob("slate_date=*/run_id=*/model=*/predictions.csv"):
            day = re.search(r"slate_date=([\d-]+)", str(f)).group(1)
            if not (START <= day <= END):
                continue
            t = pd.read_csv(f, usecols=lambda c: c in KEYS + [col])
            t = t.drop_duplicates(KEYS).rename(columns={col: "lam"})
            t["model"] = re.search(r"model=([^/]+)", str(f)).group(1)
            t["run_time"] = run_time(str(f))
            parts.append(t)
    return pd.concat(parts, ignore_index=True)


DEFENSE = ROOT / "backend/nhl/data/processed/daily_runs"
DEFENSE_VARIANTS = {"lambda_blend_w0_4": "DEFENSE_SURPRISE_BLEND_W0_4", "lambda_projected_a0_4": "DEFENSE_SURPRISE_A0_4",
                    "lambda_projected_a0_5": "DEFENSE_SURPRISE_A0_5"}


def defense_surprise() -> pd.DataFrame:
    """The defense-surprise shadow written into each daily run's prediction folder (added 2026-11-16)."""
    parts = []
    for f in DEFENSE.glob("nhldaily_*/sog_predictions_wide_defense_surprise_shadow.csv"):
        t = pd.read_csv(f)
        if "game_date" not in t or t.empty:
            continue
        t = t[t.game_date.astype(str).between(START, END)]
        for col, name in DEFENSE_VARIANTS.items():
            if col in t:
                parts.append(t[KEYS + [col]].drop_duplicates(KEYS).rename(columns={col: "lam"})
                             .assign(model=name, run_time=run_time(f.parent.name)))
    return pd.concat(parts, ignore_index=True)


def cold_start() -> pd.DataFrame:
    parts = []
    for f in (OPS / "sog_prediction_only" / "season=2026").glob("slate_date=*/phase=FINAL_PREGAME/run_id=*/immutable_predictions.csv"):
        day = re.search(r"slate_date=([\d-]+)", str(f)).group(1)
        if not (START <= day <= END):
            continue
        t = pd.read_csv(f, usecols=KEYS + ["contract_arm", "expected_sog"]).drop_duplicates(KEYS + ["contract_arm"])
        t = t.rename(columns={"expected_sog": "lam", "contract_arm": "model"})
        t["model"] = "COLD_START_" + t.model.str.split("_").str[0]
        t["run_time"] = run_time(str(f))
        parts.append(t)
    return pd.concat(parts, ignore_index=True)


def latest_pregame(t: pd.DataFrame, starts: pd.Series) -> pd.DataFrame:
    t = t.assign(start=t.game_id.map(starts))
    t = t[t.run_time < t.start]
    return t.sort_values("run_time").drop_duplicates(KEYS + ["model"], keep="last")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    starts = game_starts()
    sh = latest_pregame(pd.concat([formula_shadows(starts), cold_start(), defense_surprise()], ignore_index=True), starts)
    prod = pd.read_csv(LIVE_ROWS)[KEYS + ["game_date", "shots_on_goal", "V2"]]
    wide = sh.pivot_table(index=KEYS, columns="model", values="lam").reset_index()
    m = prod.merge(wide, on=KEYS, how="inner")
    models = [c for c in wide.columns if c not in KEYS]
    m["slate_date"] = m.game_date.astype(str)
    y = m.shots_on_goal.to_numpy(int)
    rows = []
    for k in models:
        g = m[m[k].notna()]
        yk = g.shots_on_goal.to_numpy(int)
        row = {"model": k, "n": len(g), "slates": g.slate_date.nunique(), "mean_lam": g[k].mean(), "v2_mean_lam": g.V2.mean(),
               "actual": g.shots_on_goal.mean(),
               "poisson_ll_diff_x1000": 1e3 * float((-poisson.logpmf(yk, g[k].clip(0.02)) + poisson.logpmf(yk, g.V2.clip(0.02))).mean())}
        for line in LINES:
            yy = (yk > line).astype(float)
            r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), g[k].to_numpy()), yy),
                                      base.log_loss_rows(poisson.sf(int(line), g.V2.to_numpy()), yy), g.slate_date.to_numpy())
            row[f"ll_{line}_x1000"] = round(r["mean_diff"] * 1e3, 2)
            row[f"ll_{line}_ci"] = [round(c * 1e3, 2) for c in r["ci95"]]
        rows.append(row)
    paired = pd.DataFrame(rows).sort_values("poisson_ll_diff_x1000")
    paired.to_csv(OUT / "paired_vs_v2.csv", index=False)
    # Weekly paired Brier at 1.5 (retirement rule measure: better than production in any week?).
    m["week"] = pd.to_datetime(m.game_date).dt.to_period("W-SAT").astype(str)
    wk = []
    for k in models:
        for w, g in m[m[k].notna()].groupby("week"):
            yy = (g.shots_on_goal > 1.5).astype(float).to_numpy()
            bs = (poisson.sf(1, g[k]) - yy) ** 2 - (poisson.sf(1, g.V2) - yy) ** 2
            wk.append({"model": k, "week": w, "n": len(g), "brier_diff_x1000": 1e3 * bs.mean()})
    weekly = pd.DataFrame(wk)
    weekly.to_csv(OUT / "weekly_brier_1_5_vs_v2.csv", index=False)
    # Two-sided lines vs the market.
    from backend.nhl import two_sided_lines
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly_eval
    loaded: list = []

    def lc():
        if not loaded:
            loaded.append(two_sided_lines.candidate_export.load_candidates(weekly_eval.OPERATIONAL)[0])
        return loaded[0]

    mk = []
    for day in sorted(m.slate_date.unique()):
        for g_, p_, line, mp in (weekly_eval._cached_two_sided_keys(day, lc).get("sog") or []):
            if mp is not None:
                mk.append((int(g_), int(p_), float(line), float(mp)))
    mk = pd.DataFrame(mk, columns=KEYS + ["line", "market_p_over"]).merge(m, on=KEYS)
    yy = (mk.shots_on_goal > mk.line).astype(float).to_numpy()
    th = mk.line.astype(int).to_numpy()
    vs = {"rows": int(len(mk)), "market": float(base.log_loss_rows(mk.market_p_over.to_numpy(), yy).mean()),
          "V2": float(base.log_loss_rows(poisson.sf(th, mk.V2.to_numpy()), yy).mean())}
    for k in models:
        ok = mk[k].notna().to_numpy()
        vs[k] = {"rows": int(ok.sum()), "model": float(base.log_loss_rows(poisson.sf(th[ok], mk[k].to_numpy()[ok]), yy[ok]).mean()),
                 "V2_same_rows": float(base.log_loss_rows(poisson.sf(th[ok], mk.V2.to_numpy()[ok]), yy[ok]).mean()),
                 "market_same_rows": float(base.log_loss_rows(mk.market_p_over.to_numpy()[ok], yy[ok]).mean())}
    (OUT / "summary.json").write_text(json.dumps({"window": [START, END], "player_games": int(len(m)),
                                                  "vs_market_two_sided": vs}, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(paired.round(3).to_string(index=False))
    print(weekly.pivot_table(index="model", columns="week", values="brier_diff_x1000").round(2).to_string())
    print(json.dumps(vs, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
