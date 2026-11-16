#!/usr/bin/env python3
"""Points candidate: relative shot inputs + per-minute rates (NHL Backlog P2.46, 2026-11-16).

Research only, read-only; production and the running shadow are unchanged. Combines the
two input fixes measured separately on 2026-11-16:
  REL  shot-volume inputs (d10_sog_per60, attempts_d10_per60, team_d10_sf_per_game)
       divided by the league mean of earlier slates this season (previous season's mean
       before 7 slates), research_nhl_points_minutes_scaling.add_relative
  ADD  own/goal/assist rates per minute x last-10 TOI (P2.46 test 1)
Candidates on the corrected training frame (P2.53, 2023-24), goals + assists pairs:
BASE (= c940ae2f), REL, ADD, REL_ADD. Scored on the 2025 holdout, live 2026-11-01..11-15,
two-sided lines vs the market's no-vig price, and the input watch's biggest Points
disagreements (P2.56). The REL_ADD pair is fitted twice to check it is reproducible and
saved with its inputs for a shadow, if approved.
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
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms  # noqa: E402
from backend.nhl.scripts import research_nhl_points_per_minute_corrected as pmc  # noqa: E402
from backend.nhl.scripts import research_nhl_points_pp_corrected_refit as ppfix  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_rel_per_minute_candidate_2026-11-16"
KEYS = ["game_id", "player_id"]
FEATS = list(freeze.FEATURES)
TOI = list(pmc.TOI)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    fr = ms.frames()
    train, hold, live = fr["train"].copy(), fr["hold"].copy(), fr["live"].copy()
    cache = OUT / "per_minute_history.csv.gz"
    if cache.is_file():
        pm_hist = pd.read_csv(cache)
    else:
        logs = ppfix.db_logs()
        logs["canonical_season"] = pmc.season_of(logs.game_date)
        pm_hist, _ = pmc.history_per_minute(logs)
        pm_hist.to_csv(cache, index=False, float_format="%.17g")
    train, hold = train.merge(pm_hist, on=KEYS, how="left"), hold.merge(pm_hist, on=KEYS, how="left")
    means = {}
    for x in (train, hold, live):
        d = pd.to_datetime(x.slate_date if "slate_date" in x else x.game_date)
        s = pmc.season_of(d)
        for season in set(s):
            for c in ms.SHOTS:
                means[(season, c)] = float(pd.to_numeric(x[c], errors="coerce")[s == season].mean())
    fallback = {(s + 1, c): v for (s, c), v in means.items()}
    for c in ms.SHOTS:
        fallback.setdefault((2023, c), means[(2023, c)])
    for x in (train, hold, live):
        x["attempts_d10_per60"] = scorer.impute_unknown_attempts(x)
    train, hold, live = (ms.add_relative(x, fallback) for x in (train, hold, live))
    as_rel = lambda x: x.assign(**{c: x[ms.REL[c]] for c in ms.SHOTS})  # noqa: E731
    specs = {"BASE": (FEATS, False), "REL": (FEATS, True), "ADD": (FEATS + TOI, False), "REL_ADD": (FEATS + TOI, True)}
    models = {k: ms.fit(as_rel(train) if rel else train, f) for k, (f, rel) in specs.items()}
    again = ms.fit(as_rel(train), FEATS + TOI)
    for frame in (hold, live):
        r = as_rel(frame)
        for k, (f, rel) in specs.items():
            frame[k] = ms.predict(models[k], r if rel else frame)
    summary = {"training_rows": int(len(train)), "holdout_rows": int(len(hold)), "live_rows": int(len(live)),
               "per_minute_coverage": {"train": float(train.own_rate_toi.notna().mean()),
                                       "holdout": float(hold.own_rate_toi.notna().mean()),
                                       "live": float(live.own_rate_toi.notna().mean())},
               "rel_add_reproducible": bool(np.array_equal(ms.predict(again, as_rel(hold)), hold.REL_ADD.to_numpy())),
               "shot_input_season_means": {f"{s}:{c}": round(v, 4) for (s, c), v in sorted(means.items())}}
    prod = joblib.load(scorer.CONTRACT_PATH.parent / json.loads(scorer.CONTRACT_PATH.read_text())["fitted_artifact"])
    summary["base_vs_production_max_abs"] = float(np.abs(hold.BASE - scorer.score(hold, prod).expected_points).max())
    names = list(specs)
    tabs = []
    for label, frame in (("holdout", hold), ("live", live)):
        masks = {k: v for k, v in pmc.groups(frame).items()
                 if k in ("all", "role_change", "role_up", "role_down", "toi_under_13", "toi_17_20", "toi_20_plus")}
        for cand in names[1:]:
            tabs.append(pmc.paired(frame, cand, "BASE", masks).assign(part=label, model=cand))
        summary[f"{label}_mean"] = {"actual": float(frame.realized_points.mean()),
                                    **{c: float(frame[c].mean()) for c in names}}
    paired = pd.concat(tabs, ignore_index=True)[["part", "model", "group", "line", "n", "diff_x1000", "ci_x1000"]]
    paired.to_csv(OUT / "paired_vs_production.csv", index=False)
    mk = live.merge(fr["market"], on=KEYS, how="inner")
    y = (mk.realized_points > mk.line).astype(float).to_numpy()
    k = mk.line.astype(int).to_numpy()
    mrows = []
    for name, m in {"all": np.ones(len(mk), bool), "line_0.5": mk.line.eq(0.5).to_numpy(),
                    "role_change": mk.role_change.to_numpy(bool), "no_role_change": ~mk.role_change.to_numpy(bool)}.items():
        row = {"group": name, "n": int(m.sum()), "actual_over": float(y[m].mean()),
               "MARKET": float(base.log_loss_rows(mk.market_p_over.to_numpy()[m], y[m]).mean())}
        for c in names:
            row[c] = float(base.log_loss_rows(poisson.sf(k, mk[c].to_numpy())[m], y[m]).mean())
        mrows.append(row)
    market = pd.DataFrame(mrows)
    market.to_csv(OUT / "live_two_sided_log_loss_vs_market.csv", index=False)
    dis = pd.DataFrame()
    if pmc.DISAGREE.is_file():
        d = pd.read_csv(pmc.DISAGREE)
        d = d[d.lane.eq("points")].merge(live[KEYS + names], on=KEYS, how="inner")
        kk = d.line.astype(int).to_numpy()
        over = d.side.eq("OVER").to_numpy()
        row = {"plays": len(d), "won": d.won.mean(), "market": d.market_p.mean()}
        for c in names:
            p = poisson.sf(kk, d[c].to_numpy())
            row[c] = float(np.where(over, p, 1 - p).mean())
        dis = pd.DataFrame([row])
        dis.to_csv(OUT / "biggest_points_disagreements.csv", index=False)
    joblib.dump({"model": models["REL"], "feature_columns": FEATS, "relative_shot_inputs": list(ms.SHOTS),
                 "relative_rule": "value / league mean of strictly earlier slates this season (7+ slates), "
                                  "else previous season's mean", "fallback_means": fallback},
                OUT / "points_rel_candidate.joblib")
    joblib.dump({"model": models["REL_ADD"], "feature_columns": FEATS + TOI, "relative_shot_inputs": list(ms.SHOTS),
                 "relative_rule": "value / league mean of strictly earlier slates this season (7+ slates), "
                                  "else previous season's mean", "fallback_means": fallback},
                OUT / "points_rel_per_minute_candidate.joblib")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary, indent=1, default=str))
    print(paired.pivot_table(index=["part", "group", "line"], columns="model", values="diff_x1000").round(2).to_string())
    print(paired[paired.model.eq("REL_ADD")][["part", "group", "line", "diff_x1000", "ci_x1000"]].to_string(index=False))
    print(market.round(4).to_string(index=False)); print(dis.round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
