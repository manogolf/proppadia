#!/usr/bin/env python3
"""SOG HGB refit on corrected PP TOI: route-B evidence (NHL Backlog P2.51, steps 1-3).

Read-only on the database (research login). Same recipe and features as the
frozen V1 (`backend/nhl/sog_hgb_features.py`, BASE_KW); only the data differ.

1. Rebuilds the training frame from the corrected database with the freeze
   script's code and compares it, key by key and feature by feature, with V1's
   retained frame (`frozen_2026-10-25/training_frame_rebuilt.csv.gz`).
2. Holdout like for like: the recipe fitted before 2026-01-01 on the old frame
   and on the corrected frame, both scored on 2026-01..04 with corrected
   features. Overall, by last-10 PP TOI band, by line and side.
3. Live retrospective 2026-10-25..11-14: V2 (corrected seasons 2024-2025) vs
   frozen V1 on the graded production SOG player-games, both on strict-prior
   corrected features; plus V1 as published (from the governed rows).

Writes under OUT; prints a JSON summary.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import freeze_nhl_sog_hgb_count_model as freeze  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_hgb_count_model as research  # noqa: E402
from backend.nhl.scripts.research_nhl_sog_shrunk_rate import score  # noqa: E402
from backend.nhl.sog_hgb_features import BASE_KW, FEATURES, fit, predict  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/pp_corrected_refit_2026-11-15"
V1_DIR = freeze.OUT
HOLDOUT_START, HOLDOUT_END = "2026-01-01", "2026-05-01"
LIVE_START, LIVE_END = "2026-10-25", "2026-11-14"
PP_BANDS = [-0.01, 0.25, 1.0, 2.0, 3.0, 99.0]
PP_LABELS = ["0-0.25", "0.25-1", "1-2", "2-3", "3+"]


def compare_frames(old: pd.DataFrame, new: pd.DataFrame) -> dict:
    keys = ["game_id", "player_id"]
    merged = old.merge(new, on=keys, how="outer", suffixes=("_old", "_new"), indicator=True)
    both = merged[merged._merge == "both"]
    changed = {}
    for col in ["shots_on_goal"] + FEATURES:
        a, b = both[f"{col}_old"].astype(float), both[f"{col}_new"].astype(float)
        diff = ~(np.isclose(a, b, rtol=0, atol=1e-9) | (a.isna() & b.isna()))
        if diff.any():
            changed[col] = int(diff.sum())
    return {"old_rows": int(len(old)), "new_rows": int(len(new)), "both": int(len(both)),
            "old_only": int((merged._merge == "left_only").sum()), "new_only": int((merged._merge == "right_only").sum()),
            "changed_rows_by_column": changed}


def brier(y, lam, k):
    return float(np.mean((poisson.sf(k - 1, lam) - (y >= k)) ** 2))


def segment_table(frame: pd.DataFrame, cols: dict[str, str]) -> pd.DataFrame:
    """Log loss and Brier at 1.5 by PP band for each model column."""
    frame = frame.assign(pp_band=pd.cut(frame.pp_toi_avg_10.fillna(0), PP_BANDS, labels=PP_LABELS))
    rows = []
    for band, g in frame.groupby("pp_band", observed=True):
        y = g.shots_on_goal.to_numpy(int)
        row = {"pp_band": band, "n": len(g), "mean_actual": float(y.mean())}
        for name, col in cols.items():
            lam = g[col].clip(0.02).to_numpy()
            row[f"{name}_mean"] = float(lam.mean())
            row[f"{name}_logloss"] = float(-poisson.logpmf(y, lam).mean())
            row[f"{name}_brier15"] = brier(y, lam, 2)
        rows.append(row)
    return pd.DataFrame(rows)


def side_table(frame: pd.DataFrame, cols: dict[str, str]) -> pd.DataFrame:
    """Brier by line and the side each model picks (n >= 200 gate in route A4)."""
    rows = []
    y = frame.shots_on_goal.to_numpy(int)
    for line, k in ((1.5, 2), (2.5, 3), (3.5, 4)):
        for name, col in cols.items():
            p = poisson.sf(k - 1, frame[col].clip(0.02).to_numpy())
            o = (y >= k).astype(int)
            for side, mask in (("OVER", p >= 0.5), ("UNDER", p < 0.5)):
                if mask.sum():
                    rows.append({"line": line, "model": name, "side": side, "n": int(mask.sum()),
                                 "brier": float(np.mean((p[mask] - o[mask]) ** 2)),
                                 "win_rate": float(np.mean((p[mask] >= 0.5) == (o[mask] == 1)))})
    return pd.DataFrame(rows)


def band_check(frame: pd.DataFrame, cols: dict[str, str]) -> pd.DataFrame:
    """IMP-034 band: P(over 1.5) .45-.60 under each model's own probability."""
    y = frame.shots_on_goal.to_numpy(int)
    rows = []
    for name, col in cols.items():
        p = poisson.sf(1, frame[col].clip(0.02).to_numpy())
        for lo, hi in ((0.45, 0.5), (0.5, 0.55), (0.55, 0.6), (0.45, 0.6)):
            m = (p >= lo) & (p < hi)
            if m.sum():
                rows.append({"model": name, "band": f"{lo:.2f}-{hi:.2f}", "n": int(m.sum()),
                             "model_p": float(p[m].mean()), "actual": float((y[m] >= 2).mean())})
    return pd.DataFrame(rows)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    summary: dict = {"recipe": BASE_KW, "features": FEATURES}

    # 1. Training frame
    with gzip.open(V1_DIR / "training_frame_rebuilt.csv.gz", "rt") as fh:
        old_text = fh.read()
    contract = json.loads((V1_DIR / "shadow_contract.json").read_text())
    if hashlib.sha256(old_text.encode()).hexdigest() != contract["training_frame_sha256"]:
        raise SystemExit("V1_RETAINED_FRAME_SHA_MISMATCH")
    old = pd.read_csv(io.StringIO(old_text))
    new = freeze.training_frame(*freeze._read_history())
    new_text = freeze._frame_text(new)
    summary["frame_comparison"] = compare_frames(old, pd.read_csv(io.StringIO(new_text)))
    summary["corrected_frame_sha256"] = hashlib.sha256(new_text.encode()).hexdigest()
    summary["corrected_frame_rows"] = int(len(new))

    # 2. Holdout like for like
    dates = new[["game_id", "player_id", "game_date"]]
    old = old.merge(dates, on=["game_id", "player_id"], how="inner")
    cut = pd.Timestamp(HOLDOUT_START)
    test = new[(new.game_date >= cut) & (new.game_date < pd.Timestamp(HOLDOUT_END))].copy()
    m_old = fit(old[old.game_date < cut], BASE_KW)
    m_new = fit(new[new.game_date < cut], BASE_KW)
    test["OLD_FIT"] = predict(m_old, test)
    test["CORRECTED_FIT"] = predict(m_new, test)
    y = test.shots_on_goal.to_numpy(int)
    summary["holdout"] = {
        "window": [HOLDOUT_START, HOLDOUT_END], "rows": int(len(test)),
        "old_fit": score(y, test.OLD_FIT.to_numpy()), "corrected_fit": score(y, test.CORRECTED_FIT.to_numpy()),
        "paired_corrected_minus_old": research.paired_ci(test, "CORRECTED_FIT", "OLD_FIT"),
    }
    cols = {"old_fit": "OLD_FIT", "corrected_fit": "CORRECTED_FIT"}
    segment_table(test, cols).to_csv(OUT / "holdout_by_pp_band.csv", index=False)
    side_table(test, cols).to_csv(OUT / "holdout_by_line_side.csv", index=False)
    band_check(test, cols).to_csv(OUT / "holdout_imp034_band.csv", index=False)

    # 3. Live retrospective
    v2 = fit(new, BASE_KW)
    v1 = joblib.load(V1_DIR / freeze.ARTIFACT)
    logs, pos = research.pull()
    frame = research.build(logs, pos)
    live = frame[(frame.season == 2026) & (frame.game_date >= pd.Timestamp(LIVE_START))
                 & (frame.game_date <= pd.Timestamp(LIVE_END))].copy()
    live = live[live.toi_avg_10.notna() & live.shrunk_lambda_180.notna()]
    # Pair with the graded production SOG player-games (governed rows, line 1.5).
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    graded = []
    for day in weekly._days(LIVE_START, LIVE_END):
        s = weekly._summary(day)
        if s:
            r = weekly._cached_rows("sog", day, s["summary_identity"])
            if not r.empty:
                graded.append(r[r.line.eq(1.5)][["game_id", "player_id", "p_over"]])
    graded = pd.concat(graded).astype({"game_id": "int64", "player_id": "int64"})
    live["game_id"] = live.game_id.astype("int64")
    live["player_id"] = live.player_id.astype("int64")
    live = live.merge(graded.drop_duplicates(["game_id", "player_id"]), on=["game_id", "player_id"], how="inner")
    live["V1"] = predict(v1, live)
    live["V2"] = predict(v2, live)
    # V1 as published: the expected shots behind its governed P(over 1.5).
    live["V1_PUBLISHED"] = live.p_over.clip(1e-6, 1 - 1e-6).map(research.lambda_from_p_over_1_5)
    y = live.shots_on_goal.to_numpy(int)
    summary["live"] = {
        "window": [LIVE_START, LIVE_END], "rows": int(len(live)),
        "v1_published": score(y, live.V1_PUBLISHED.to_numpy()), "v1_corrected_inputs": score(y, live.V1.to_numpy()),
        "v2": score(y, live.V2.to_numpy()),
        "paired_v2_minus_v1_corrected_inputs": research.paired_ci(live, "V2", "V1"),
        "paired_v2_minus_v1_published": research.paired_ci(live, "V2", "V1_PUBLISHED"),
    }
    cols = {"v1_published": "V1_PUBLISHED", "v1": "V1", "v2": "V2"}
    segment_table(live, cols).to_csv(OUT / "live_by_pp_band.csv", index=False)
    side_table(live, cols).to_csv(OUT / "live_by_line_side.csv", index=False)
    band_check(live, cols).to_csv(OUT / "live_imp034_band.csv", index=False)
    live[["game_id", "player_id", "game_date", "shots_on_goal", "pp_toi_avg_10", "V1_PUBLISHED", "V1", "V2"]] \
        .to_csv(OUT / "live_rows.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    print(json.dumps(summary, indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
