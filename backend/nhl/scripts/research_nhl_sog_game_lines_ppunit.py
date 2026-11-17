#!/usr/bin/env python3
"""SOG: do morning game lines and the PP unit add anything to V2? (P2.59 follow-on, 2026-11-16)

Research only, read-only. For Points, morning game lines (team implied goals, game total, win
probability) plus the PP unit brought the model to about market accuracy (P2.55). Same inputs for
SOG, built by the Points research (`research_nhl_points_game_totals.py` game_inputs,
`research_nhl_points_ppunit_game_scoring.py` inputs; live lines = the shadow's morning observation):

  A       V2 recipe (sog_hgb_features.FEATURES, BASE_KW)
  TOT     A + team_implied_goals, game_total, team_win_p
  PPU     A + pp1_last_game, pp1_share_last5, pp_rank_last_game
  TOT_PPU A + both

Holdout: fitted on rows before 2026-01-01 (2024-25 + early 2025-26), scored 2026-01..04 (as in the
P2.52 ablation). Live: fitted on all 2024-25 + 2025-26 rows, scored 2026-10-25..11-14 beside V2's
own predictions (player-games with every input present); on two-sided SOG lines, log loss beside the market's no-vig P(Over).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_candidate_shadows as shadow  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_hgb_count_model as research  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_situation_ablation as abl  # noqa: E402
from backend.nhl.sog_hgb_features import FEATURES  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/sog_game_lines_ppunit_2026-11-16"
TOT_DIR = ROOT / "artifacts/analysis/nhl/points_game_totals_2026-11-16"
PPU_INPUTS = ROOT / "artifacts/analysis/nhl/points_ppunit_game_scoring_2026-11-16/inputs.csv.gz"
LIVE_ROWS = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/pp_corrected_refit_2026-11-15/live_rows.csv"
KEYS = ["game_id", "player_id"]
SETS = {"A": FEATURES, "TOT": FEATURES + shadow.TOT, "PPU": FEATURES + shadow.PPU,
        "TOT_PPU": FEATURES + shadow.TOT + shadow.PPU}


def inputs() -> pd.DataFrame:
    pp = pd.read_csv(PPU_INPUTS)[KEYS + shadow.PPU]
    team = pd.read_csv(TOT_DIR / "player_team.csv.gz")
    gi = pd.read_csv(TOT_DIR / "game_inputs.csv.gz")
    return team.merge(gi, on=["game_id", "team_id"], how="left").merge(pp, on=KEYS, how="outer")[KEYS + shadow.TOT + shadow.PPU]


def scores(f: pd.DataFrame, cols: list[str], ref: str) -> pd.DataFrame:
    y = f.shots_on_goal.to_numpy(int)
    rows = []
    for c in cols:
        row = {"model": c, "n": len(f), "mean_pred": f[c].mean(), "actual": y.mean(),
               "poisson_ll": float(-poisson.logpmf(y, f[c].clip(0.02)).mean())}
        if c != ref:
            r = research.paired_ci(f, c, ref)
            row["poisson_diff_x1000"], row["ci95_x1000"] = r["mean"] * 1e3, [round(v * 1e3, 2) for v in r["ci95"]]
        for line in (1.5, 2.5, 3.5):
            yy = (y > line).astype(float)
            row[f"ll_{line}"] = float(base.log_loss_rows(poisson.sf(int(line), f[c].to_numpy()), yy).mean())
        rows.append(row)
    return pd.DataFrame(rows)


def market_rows(days: list[str]) -> pd.DataFrame:
    from backend.nhl import two_sided_lines
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    loaded: list = []

    def lc():
        if not loaded:
            loaded.append(two_sided_lines.candidate_export.load_candidates(weekly.OPERATIONAL)[0])
        return loaded[0]

    rows = []
    for day in days:
        for g, p, line, mp in (weekly._cached_two_sided_keys(day, lc).get("sog") or []):
            if mp is not None:
                rows.append((int(g), int(p), float(line), float(mp)))
    return pd.DataFrame(rows, columns=KEYS + ["line", "market_p_over"])


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    logs, pos = research.pull()
    frame = research.build(logs, pos)
    frame["game_id"], frame["player_id"] = frame.game_id.astype("int64"), frame.player_id.astype("int64")
    f = abl.add_features(frame, abl.situation_table()).merge(inputs(), on=KEYS, how="left")
    ok = f.toi_avg_10.notna() & f.shrunk_lambda_180.notna()
    tr = f[f.season.isin([2024, 2025]) & ok]
    cut, end = pd.Timestamp(abl.HOLDOUT_START), pd.Timestamp(abl.HOLDOUT_END)
    early, hold = tr[tr.game_date < cut], tr[(tr.game_date >= cut) & (tr.game_date < end)].copy()
    coverage = {"holdout": {c: float(hold[c].notna().mean()) for c in shadow.TOT + shadow.PPU}}
    for k, cols in SETS.items():
        hold[k] = abl._fit(early, cols).predict(hold[cols].astype(float).to_numpy())
    th = scores(hold, list(SETS), "A")
    th.round(5).to_csv(OUT / "holdout_2026_jan_apr.csv", index=False)
    # Live: fitted on all 2024-25 + 2025-26 rows; V2's own predictions alongside.
    live = f[f.season.eq(2026)].merge(pd.read_csv(LIVE_ROWS)[KEYS + ["V2"]], on=KEYS, how="inner").copy()
    coverage["live"] = {c: float(live[c].notna().mean()) for c in shadow.TOT + shadow.PPU}
    for k, cols in SETS.items():
        live[k] = abl._fit(tr, cols).predict(live[cols].astype(float).to_numpy())
    live["game_date"] = pd.to_datetime(live.game_date)
    # Live inputs are incomplete early in the window (morning totals and the research PP-unit build
    # start later), so scores are on player-games with every input present.
    live = live[live[shadow.TOT + shadow.PPU].notna().all(axis=1)].copy()
    coverage["live_scored_rows"], coverage["live_first_date"] = int(len(live)), str(live.game_date.min().date())
    tl = scores(live, ["V2"] + list(SETS), "V2")
    tl.round(5).to_csv(OUT / "live_2026.csv", index=False)
    mk = market_rows(sorted(live.game_date.dt.strftime("%Y-%m-%d").unique())).merge(live, on=KEYS)
    yy = (mk.shots_on_goal > mk.line).astype(float).to_numpy()
    th_ = mk.line.astype(int).to_numpy()
    vs = {"rows": int(len(mk)), "market": float(base.log_loss_rows(mk.market_p_over.to_numpy(), yy).mean())}
    for k in ["V2"] + list(SETS):
        vs[k] = float(base.log_loss_rows(poisson.sf(th_, mk[k].to_numpy()), yy).mean())
    for k in list(SETS):
        d = base.paired_bootstrap(base.log_loss_rows(poisson.sf(th_, mk[k].to_numpy()), yy),
                                  base.log_loss_rows(poisson.sf(th_, mk.V2.to_numpy()), yy),
                                  mk.game_date.dt.strftime("%Y-%m-%d").to_numpy())
        vs[f"{k}_minus_V2_x1000"] = [round(d["mean_diff"] * 1e3, 2), [round(c * 1e3, 2) for c in d["ci95"]]]
    summary = {"holdout_rows": int(len(hold)), "live_rows": int(len(live)), "coverage": coverage, "live_two_sided": vs}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(th.round(4).to_string(index=False)); print(tl.round(4).to_string(index=False))
    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
