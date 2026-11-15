#!/usr/bin/env python3
"""Points goals + assists refit on corrected PP TOI: route-B evidence (NHL Backlog P2.53).

Read-only on the database (research login); never changes production. The
production Points model (`8cda6061`, `freeze_nhl_points_goals_assists.py`) was
fitted on 2023-24 rows whose `mean_pp_toi_last10` came from PP TOI values later
corrected to the NHL stats API (P2.45). This script:

1. Recomputes `mean_pp_toi_last10` for every training (2023-24) and holdout
   (2025) row from the corrected database with the live builder's definition
   (`export_nhl_points_hgb_features.build_features`: mean over the last 10
   strict-prior official-outcome player-games within 120 days, NULL skipped),
   and checks the window by recomputing `mean_toi_last10` (TOI was not
   changed) against the stored frame.
2. Fits the production recipe (`fit_pair`) on the old and on the corrected
   training frame and scores both on the 2025 holdout with corrected PP inputs:
   paired log loss at 0.5 / 1.5 / 2.5 (slate-clustered), by PP band and the
   P(over 0.5) .45-.50 band (IMP-037).
3. Live 2026-11-01..11-14: the latest pregame capture per graded player-game,
   PP input recomputed the same way, old vs corrected model on the graded lines.
"""
from __future__ import annotations

import json
import os
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
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/pp_stats_api_refit_2026-11-15"
KEYS = ["game_id", "player_id"]
LIVE_START, LIVE_END = "2026-11-01", "2026-11-14"
CAPTURES = ROOT / "artifacts/operational/nhl/points_hgb_shadow/season=2026"


def db_logs() -> pd.DataFrame:
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT l.game_id::bigint, l.player_id::bigint, g.game_date, l.toi_minutes, l.pp_toi_minutes
                         FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING (game_id)
                        WHERE g.season BETWEEN 2022 AND 2026""")
        logs = pd.DataFrame(cur.fetchall(), columns=["game_id", "player_id", "game_date", "toi_minutes", "pp_toi_minutes"])
    for c in ("toi_minutes", "pp_toi_minutes"):
        logs[c] = pd.to_numeric(logs[c], errors="coerce").astype(float)
    logs["game_date"] = pd.to_datetime(logs.game_date)
    return logs


def window_means(history: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    """mean_toi_last10 and mean_pp_toi_last10 per target: last 10 strict-prior rows within 120 days."""
    history = history.sort_values(["player_id", "game_date", "game_id"], kind="mergesort")
    groups = {int(p): g for p, g in history.groupby("player_id", sort=False)}
    out = []
    for t in targets.itertuples(index=False):
        g = groups.get(int(t.player_id))
        if g is None:
            out.append((t.game_id, t.player_id, np.nan, np.nan))
            continue
        d = pd.Timestamp(t.game_date)
        h = g[(g.game_date < d) & (g.game_date >= d - pd.Timedelta(days=120))].tail(10)
        out.append((t.game_id, t.player_id, h.toi_minutes.mean() if len(h) else np.nan,
                    h.pp_toi_minutes.mean() if len(h) else np.nan))
    return pd.DataFrame(out, columns=KEYS + ["toi_recomputed", "pp_corrected"])


def line_loss(expected: np.ndarray, points: np.ndarray, line: float) -> np.ndarray:
    return base.log_loss_rows(poisson.sf(int(line), expected), (points > line).astype(float))


def paired(frame: pd.DataFrame, a: str, b: str) -> dict:
    out = {}
    for line in (0.5, 1.5, 2.5):
        r = base.paired_bootstrap(line_loss(frame[a].to_numpy(), frame.realized_points.to_numpy(), line),
                                  line_loss(frame[b].to_numpy(), frame.realized_points.to_numpy(), line),
                                  frame.slate_date.astype(str).to_numpy())
        out[str(line)] = {"diff_x1000": round(r["mean_diff"] * 1e3, 2), "ci": [round(c * 1e3, 2) for c in r["ci95"]]}
    return out


def bands(frame: pd.DataFrame, cols: dict[str, str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    y = (frame.realized_points >= 1).astype(float).to_numpy()
    rows = []
    f = frame.assign(pp_band=pd.cut(frame.mean_pp_toi_last10.fillna(0), [-0.01, 0.25, 1, 2, 3, 99],
                                    labels=["0-0.25", "0.25-1", "1-2", "2-3", "3+"]))
    for band, g in f.groupby("pp_band", observed=True):
        yy = (g.realized_points >= 1).astype(float).to_numpy()
        row = {"pp_band": band, "n": len(g), "actual_1plus": float(yy.mean())}
        for k, c in cols.items():
            p = poisson.sf(0, g[c].to_numpy())
            row[f"{k}_p"], row[f"{k}_ll"] = float(p.mean()), float(base.log_loss_rows(p, yy).mean())
        rows.append(row)
    imp = []
    for k, c in cols.items():
        p = poisson.sf(0, frame[c].to_numpy())
        for lo, hi in ((0.40, 0.45), (0.45, 0.50), (0.50, 0.55)):
            m = (p >= lo) & (p < hi)
            imp.append({"model": k, "band_p_over_0_5": f"{lo:.2f}-{hi:.2f}", "n": int(m.sum()),
                        "model_p_over": float(p[m].mean()) if m.any() else None,
                        "actual_over": float(y[m].mean()) if m.any() else None})
    return pd.DataFrame(rows), pd.DataFrame(imp)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    summary: dict = {}
    train = pd.read_csv(freeze.OUT / "training_frame_2023_2024.csv.gz", float_precision="round_trip")
    hold = pd.read_csv(freeze.OUT / "holdout_frame_2025.csv.gz", float_precision="round_trip")
    outcomes, _, _ = freeze.official()
    logs = db_logs()
    history = outcomes[KEYS + ["game_date"]].copy()
    history["game_date"] = pd.to_datetime(history.game_date).dt.normalize()
    history = history.merge(logs.drop(columns="game_date"), on=KEYS, how="left")
    fixed = {}
    for name, frame in (("train", train), ("hold", hold)):
        t = frame[KEYS + ["slate_date"]].rename(columns={"slate_date": "game_date"})
        w = window_means(history, t)
        m = frame.merge(w, on=KEYS, how="left", validate="one_to_one")
        toi_match = np.isclose(m.toi_recomputed, m.mean_toi_last10, atol=1e-6) | (m.toi_recomputed.isna() & m.mean_toi_last10.isna())
        pp_changed = ~(np.isclose(m.pp_corrected, m.mean_pp_toi_last10, atol=1e-6)
                       | (m.pp_corrected.isna() & m.mean_pp_toi_last10.isna()))
        summary[f"{name}_window_check"] = {"rows": int(len(m)), "toi_reproduced_share": float(toi_match.mean()),
                                           "pp_changed_rows": int(pp_changed.sum()),
                                           "pp_mean_old": float(m.mean_pp_toi_last10.mean()),
                                           "pp_mean_corrected": float(m.pp_corrected.mean())}
        fixed[name] = m
    if summary["train_window_check"]["toi_reproduced_share"] < 0.99:
        raise SystemExit(f"POINTS_WINDOW_NOT_REPRODUCED:{summary['train_window_check']}")
    feats = freeze.FEATURES
    old_train = fixed["train"][train.columns]
    new_train = fixed["train"].assign(mean_pp_toi_last10=fixed["train"].pp_corrected)[train.columns]
    old_model, new_model = freeze.fit_pair(old_train), freeze.fit_pair(new_train)
    second = freeze.fit_pair(new_train.copy())
    h = fixed["hold"].assign(mean_pp_toi_last10=fixed["hold"].pp_corrected)
    h["OLD_FIT"] = scorer.score(h, old_model).expected_points.to_numpy()
    h["CORRECTED_FIT"] = scorer.score(h, new_model).expected_points.to_numpy()
    summary["refit_reproducible"] = bool(np.array_equal(scorer.score(h, second).expected_points.to_numpy(),
                                                        h.CORRECTED_FIT.to_numpy()))
    summary["holdout"] = {"rows": int(len(h)), "paired_corrected_minus_old": paired(h, "CORRECTED_FIT", "OLD_FIT")}
    pb, imp = bands(h, {"old": "OLD_FIT", "corrected": "CORRECTED_FIT"})
    pb.to_csv(OUT / "holdout_by_pp_band.csv", index=False)
    imp.to_csv(OUT / "holdout_imp037_band.csv", index=False)

    # Live: latest pregame capture per player-game, graded lines from the weekly row cache.
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    caps = []
    for day in weekly._days(LIVE_START, LIVE_END):
        for path in sorted((CAPTURES / f"slate_date={day}").glob("run_id=*/phase=*/captured_at=*/features.csv")):
            c = pd.read_csv(path)
            c["captured_at"] = path.parent.name.split("=", 1)[1]
            caps.append(c)
    live = pd.concat(caps, ignore_index=True)
    live = live[pd.to_datetime(live.captured_at, format="%Y%m%dT%H%M%S%fZ", utc=True)
                < pd.to_datetime(live.game_start_utc, utc=True)]
    live = live.sort_values("captured_at").drop_duplicates(KEYS, keep="last")
    graded = []
    for day in weekly._days(LIVE_START, LIVE_END):
        s = weekly._summary(day)
        if s:
            r = weekly._cached_rows("points", day, s["summary_identity"])
            if not r.empty:
                r = r.assign(over=np.where(r.selected_side.eq("OVER"), r.grading_state.eq("WIN"), r.grading_state.ne("WIN")))
                graded.append(r[["game_id", "player_id", "line", "over"]])
    graded = pd.concat(graded)
    pts = (graded.pivot_table(index=KEYS, columns="line", values="over", aggfunc="first")
           .reindex(columns=[0.5, 1.5, 2.5]).dropna().astype(int))
    pts["realized_points"] = pts[0.5] + pts[1.5] + pts[2.5]
    live = live.merge(pts[["realized_points"]].reset_index(), on=KEYS, how="inner")
    lt = live[KEYS + ["game_date"]].copy()
    lt["game_date"] = pd.to_datetime(lt.game_date)
    live_hist = logs[logs.toi_minutes.gt(0)][KEYS + ["game_date", "toi_minutes", "pp_toi_minutes"]]
    w = window_means(live_hist, lt)
    live = live.merge(w, on=KEYS, how="left")
    summary["live_window_check"] = {
        "rows": int(len(live)),
        "toi_reproduced_share": float(np.isclose(live.toi_recomputed, live.mean_toi_last10, atol=0.01).mean()),
        "pp_mean_captured": float(live.mean_pp_toi_last10.mean()), "pp_mean_corrected": float(live.pp_corrected.mean())}
    live["mean_pp_toi_last10"] = live.pp_corrected.fillna(live.mean_pp_toi_last10)
    live["slate_date"] = live.game_date.astype(str)
    live["OLD_FIT"] = scorer.score(live, old_model).expected_points.to_numpy()
    live["CORRECTED_FIT"] = scorer.score(live, new_model).expected_points.to_numpy()
    summary["live"] = {"rows": int(len(live)), "paired_corrected_minus_old": paired(live, "CORRECTED_FIT", "OLD_FIT")}
    pb, imp = bands(live, {"old": "OLD_FIT", "corrected": "CORRECTED_FIT"})
    pb.to_csv(OUT / "live_by_pp_band.csv", index=False)
    imp.to_csv(OUT / "live_imp037_band.csv", index=False)
    joblib.dump(new_model, OUT / "points_goals_assists_pp_corrected_candidate.joblib")
    new_train.to_csv(OUT / "training_frame_2023_2024_pp_corrected.csv.gz", index=False, float_format="%.17g")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    print(json.dumps(summary, indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
