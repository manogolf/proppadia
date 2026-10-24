#!/usr/bin/env python3
"""Research refit of NHL Points HGB on source-corrected history (P2.31).

Rebuilds the frozen V1 training identities (2023-24) and the 2025 holdout with
the live feature code (`export_nhl_points_hgb_features.build_features`) from
the corrected database, refits the frozen HGB recipe as research candidates,
and compares them with V1 and the market. Read-only database access through the
restricted research login. Nothing here changes production, predictions, or
model authority.

PP-TOI zero rule (research only): since 2026-01 the source stores "no PP time"
as NULL because reconciliation staging inserts NULL and the daily PP fixer only
fills positive overlaps. Here a NULL PP value is read as 0 when the skater
played (TOI > 0), had no PP shots on goal, and the team-game has positive
recorded PP time. Other NULLs stay missing. Live scoring would need the same
rule applied in the source before any candidate could run in production.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import psycopg
from scipy.stats import poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.scripts.export_nhl_points_hgb_features import (  # noqa: E402
    FEATURES, build_features, normalize_history, normalize_slate)
from backend.nhl.scripts.run_nhl_points_hgb_v2_direct_repair import KW, metric_rows  # noqa: E402
from backend.nhl.scripts.score_nhl_points_hgb_shadow import MODEL_PATH as V1_MODEL_PATH  # noqa: E402
from backend.nhl.scripts.build_nhl_player_performance_pulse import find_outcome  # noqa: E402

FROZEN_FRAME = ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz"
EXPORT = ROOT / "artifacts/analysis/nhl/all_candidate_evaluation_export/full_window_2026-10-24_both_sides"
OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/2026-10-24"
KEYS = ["game_id", "player_id"]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def research_env() -> dict[str, str]:
    from dotenv import dotenv_values
    return {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}


def pull_history() -> tuple[pd.DataFrame, dict]:
    """Regular-season skater logs and game metadata, seasons 2023-2026."""
    with psycopg.connect(database_url("research", environ=research_env()), prepare_threshold=None) as conn, conn.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("SELECT current_user, transaction_timestamp(), pg_current_snapshot()::text")
        user, ts, snap = cur.fetchone()
        cur.execute("""SELECT g.season AS canonical_season, g.game_date, g.start_time_utc,
                   g.home_team_id, g.away_team_id, l.game_id, l.player_id, l.team_id, l.is_home,
                   l.toi_minutes, l.pp_toi_minutes, l.pp_sog, l.shots_on_goal, l.shot_attempts,
                   l.goals, l.assists, l.created_at
            FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING(game_id)
            WHERE substring(g.game_id::text,5,2) = '02' AND g.season IN (2023,2024,2025,2026)
              AND g.game_date < CURRENT_DATE
            ORDER BY g.game_date, g.start_time_utc, l.game_id, l.player_id""")
        rows = cur.fetchall()
        cols = [c.name for c in cur.description]
    logs = pd.DataFrame(rows, columns=cols)
    for c in ("toi_minutes", "pp_toi_minutes", "pp_sog", "shots_on_goal", "shot_attempts", "goals", "assists"):
        logs[c] = pd.to_numeric(logs[c], errors="coerce")
    logs["game_id"] = logs.game_id.astype("int64"); logs["player_id"] = logs.player_id.astype("int64")
    logs, team_audit = bind_canonical_team(logs)
    # 2023-24 games have no stored start time. Features use game_date only
    # (strict prior days), so a 23:00 UTC placeholder affects no feature value.
    missing_start = logs.start_time_utc.isna()
    logs["start_time_utc"] = pd.to_datetime(logs.start_time_utc, utc=True)
    logs.loc[missing_start, "start_time_utc"] = pd.to_datetime(logs.loc[missing_start, "game_date"]).dt.tz_localize("UTC") + pd.Timedelta(hours=23)
    team_audit["start_time_placeholder_games"] = int(logs.loc[missing_start, "game_id"].nunique())
    return logs, {"team_binding": team_audit, "login": user, "transaction_timestamp_utc": ts.isoformat(), "snapshot": snap,
                  "isolation": "REPEATABLE READ READ ONLY", "rows": len(logs)}


def bind_canonical_team(logs: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """2023-24 logs carry legacy raw team IDs; bind each row to the game's physical side.

    Every game has exactly two raw team IDs, each with a single is_home value, so
    is_home selects the canonical home or away team. Rows already on a canonical
    side are unchanged.
    """
    logs = logs.copy()
    on_side = logs.team_id.eq(logs.home_team_id) | logs.team_id.eq(logs.away_team_id)
    sides = logs.groupby(["game_id", "team_id"]).is_home.nunique(dropna=False)
    teams = logs.groupby("game_id").team_id.nunique()
    if (sides > 1).any() or (teams != 2).any() or logs.is_home.isna().any():
        raise ValueError("LEGACY_TEAM_SIDE_BINDING_AMBIGUOUS")
    canonical = pd.Series(np.where(logs.is_home.astype(bool), logs.home_team_id, logs.away_team_id), index=logs.index)
    legacy = logs.canonical_season.isin([2023, 2024])
    # From 2025 the raw team ID is canonical and must agree with is_home.
    if (canonical[~legacy] != logs.team_id[~legacy]).any():
        raise ValueError("CANONICAL_TEAM_SIDE_CONFLICT")
    changed = canonical.ne(logs.team_id)
    logs["raw_team_id"] = logs.team_id
    logs["team_id"] = canonical.astype("int64")
    # In 2023-24 some raw IDs collide with the other team's canonical ID.
    return logs, {"rows_rebound_from_legacy_team_id": int(changed.sum()),
                  "legacy_id_collisions_resolved_by_is_home": int((changed & on_side).sum()),
                  "rebound_by_season": {int(k): int(v) for k, v in logs.loc[changed].groupby("canonical_season").size().items()}}


def apply_pp_zero_rule(logs: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    logs = logs.copy()
    before = int(logs.pp_toi_minutes.isna().sum())
    team_pp = logs.groupby(["game_id", "team_id"]).pp_toi_minutes.transform(lambda s: s.fillna(0).sum())
    zero = (logs.pp_toi_minutes.isna() & logs.toi_minutes.fillna(0).gt(0)
            & logs.pp_sog.fillna(0).eq(0) & team_pp.gt(0))
    logs.loc[zero, "pp_toi_minutes"] = 0.0
    return logs, {"null_pp_before": before, "null_read_as_zero": int(zero.sum()),
                  "null_pp_after": int(logs.pp_toi_minutes.isna().sum()),
                  "null_read_as_zero_by_season": {int(k): int(v) for k, v in logs.loc[zero].groupby("canonical_season").size().items()}}


def official_outcomes(frozen: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Points labels as the live path uses them: official outcomes, not raw logs.

    The database log goals/assists are empty, so 2023-25 use the restored
    official outcome frame and 2026 the latest reconciliation package per date.
    """
    hist = frozen[KEYS + ["game_date", "canonical_season", "goals", "assists"]].copy()
    frames = []
    for base in sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation").glob("2026-*")):
        path = find_outcome(base.name, "canonical_skater_outcomes.csv")
        if path is None:
            continue
        f = pd.read_csv(path)
        f = f[f.official_final.astype(str).str.lower().isin({"true", "t", "1"})]
        f = f[f.game_id.astype(str).str[4:6].eq("02")]
        frames.append(f.assign(game_date=base.name))
    o26 = pd.concat(frames, ignore_index=True).drop_duplicates(KEYS, keep="last")
    o26 = o26.rename(columns={"official_goals": "goals", "official_assists": "assists"})
    o26["game_id"] = o26.game_id.astype("int64"); o26["player_id"] = o26.player_id.astype("int64")
    o26["canonical_season"] = 2026
    out = pd.concat([hist, o26[KEYS + ["game_date", "canonical_season", "goals", "assists", "official_sog"]]], ignore_index=True)
    return out, {"restored_2023_2025_rows": len(hist), "reconciliation_2026_rows": len(o26)}


def history_inputs(logs: pd.DataFrame, outcomes: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    o = outcomes.merge(logs[KEYS + ["shots_on_goal", "created_at", "start_time_utc", "team_id"]], on=KEYS, how="inner")
    sog = pd.to_numeric(o.get("official_sog"), errors="coerce") if "official_sog" in o else pd.Series(np.nan, index=o.index)
    sog = sog.fillna(o.shots_on_goal)
    unknown_sog = int(sog.isna().sum())
    o["official_sog"] = sog.fillna(0.0)  # a handful of rows; recorded in the audit
    ts = pd.to_datetime(o.created_at, utc=True, errors="coerce")
    o["outcome_source_timestamp_utc"] = ts.fillna(o.start_time_utc + pd.Timedelta(hours=4)).astype(str)
    o = o.drop(columns=["shots_on_goal", "created_at", "start_time_utc"])
    log_cols = ["game_id", "player_id", "team_id", "is_home", "home_team_id", "away_team_id", "toi_minutes",
                "pp_toi_minutes", "shots_on_goal", "shot_attempts", "start_time_utc"]
    return normalize_history(logs[log_cols], o), {"history_rows": len(o), "unknown_official_sog_read_as_zero": unknown_sog,
                                                  "outcomes_without_log_row": int(len(outcomes) - len(o))}


def build_targets(history: pd.DataFrame, games: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    """targets: game_id, player_id, canonical_season, is_home. Returns features per target."""
    t = targets.merge(games, on="game_id", how="left", validate="many_to_one")
    if t.start_time_utc.isna().any():
        raise ValueError(f"TARGET_GAME_METADATA_MISSING:{int(t.start_time_utc.isna().sum())}")
    t["game_start_utc"] = pd.to_datetime(t.start_time_utc, utc=True).astype(str)
    t["is_home"] = t.is_home.map({True: True, False: False, 1: True, 0: False, "t": True, "f": False,
                                  "true": True, "false": False, 1.0: True, 0.0: False})
    out = []
    for season, group in t.groupby("canonical_season"):
        slate = normalize_slate(group[["game_id", "player_id", "game_date", "game_start_utc", "is_home",
                                       "home_team_id", "away_team_id"]], int(season))
        out.append(build_features(history, slate))
    return pd.concat(out, ignore_index=True)


def matrix(frame: pd.DataFrame, scaler: StandardScaler | None, fill_zero: bool):
    x = frame[FEATURES].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if fill_zero:
        x = x.fillna(0)
    x = x.astype(float).to_numpy()
    if scaler is None:
        scaler = StandardScaler().fit(x)
    return scaler.transform(x), scaler


def fit(frame: pd.DataFrame, fill_zero: bool):
    x, scaler = matrix(frame, None, fill_zero)
    y = frame.realized_points.to_numpy(dtype=int)
    model = HistGradientBoostingRegressor(**KW).fit(x, y)
    repeat = HistGradientBoostingRegressor(**KW).fit(x.copy(), y.copy())
    if not np.array_equal(model.predict(x), repeat.predict(x)):
        raise ValueError("REFIT_REPRODUCIBILITY_FAIL")
    return {"model": model, "scaler": scaler, "fill_zero": fill_zero, "feature_columns": FEATURES}


def predict(bundle, frame):
    x, _ = matrix(frame, bundle["scaler"], bundle.get("fill_zero", True))
    mu = np.maximum(np.asarray(bundle["model"].predict(x), dtype=float), 1e-8)
    return mu, np.column_stack([poisson.sf(k, mu) for k in (0, 1, 2)])


def score05(y_over: np.ndarray, p: np.ndarray) -> dict:
    p = np.clip(p, 1e-12, 1 - 1e-12)
    return {"n": int(len(p)), "brier": float(np.mean((p - y_over) ** 2)),
            "log_loss": float(-np.mean(y_over * np.log(p) + (1 - y_over) * np.log1p(-p))),
            "auc": float(roc_auc_score(y_over, p)) if y_over.std() else None,
            "mean_p_over": float(p.mean()), "over_rate": float(y_over.mean()),
            "selected_side_record": f"{int(((p >= .5) == (y_over == 1)).sum())}-{int(((p >= .5) != (y_over == 1)).sum())}"}


def feature_parity(rebuilt: pd.DataFrame, frozen: pd.DataFrame) -> pd.DataFrame:
    j = frozen[KEYS + FEATURES].merge(rebuilt[KEYS + FEATURES], on=KEYS, suffixes=("_frozen", "_rebuilt"))
    rows = []
    for f in FEATURES:
        a, b = pd.to_numeric(j[f + "_frozen"], errors="coerce"), pd.to_numeric(j[f + "_rebuilt"], errors="coerce")
        both = a.notna() & b.notna()
        rows.append({"feature": f, "rows": len(j), "both_present": int(both.sum()),
                     "exact_within_1e-6": int((np.abs(a - b) <= 1e-6)[both].sum()),
                     "correlation": float(a[both].corr(b[both])) if both.sum() > 2 else None,
                     "frozen_median": float(a.median()), "rebuilt_median": float(b.median()),
                     "frozen_null": int(a.isna().sum()), "rebuilt_null": int(b.isna().sum())})
    return pd.DataFrame(rows)


def targets_2026(logs: pd.DataFrame) -> pd.DataFrame:
    p = pd.read_csv(EXPORT / "candidate_predictions.csv", usecols=["candidate_key", "slate_date", "game_id", "player_id",
        "market_key", "model_family", "market_line", "probability_over", "prediction_timestamp_utc",
        "scheduled_start_utc", "outcome_status", "outcome_value"], low_memory=False)
    p = p[(p.market_key == "player_points") & (p.model_family == "hist_gradient_boosting") & (p.outcome_status == "SETTLED")]
    p = p[pd.to_numeric(p.market_line, errors="coerce").eq(0.5)]
    p = p[pd.to_datetime(p.prediction_timestamp_utc, format="ISO8601", utc=True)
          < pd.to_datetime(p.scheduled_start_utc, format="ISO8601", utc=True)]
    p = p.sort_values("prediction_timestamp_utc").groupby(KEYS, as_index=False).tail(1)
    p["game_id"] = p.game_id.astype("int64"); p["player_id"] = pd.to_numeric(p.player_id).astype("int64")
    p["realized_points"] = pd.to_numeric(p.outcome_value, errors="coerce")
    p = p.dropna(subset=["realized_points"])
    side = logs[["game_id", "player_id", "team_id", "home_team_id"]].drop_duplicates(KEYS)
    p = p.merge(side, on=KEYS, how="inner")
    p["is_home"] = p.team_id.astype(int).eq(p.home_team_id.astype(int))
    p["canonical_season"] = 2026
    return p.rename(columns={"probability_over": "v1_captured_p_over"})[
        ["candidate_key", "slate_date", "game_id", "player_id", "canonical_season", "is_home", "realized_points", "v1_captured_p_over"]]


def market_2026() -> pd.DataFrame:
    """Same-book no-vig market P(over 0.5), median across books with both sides quoted."""
    q = pd.concat(c[c.market_key.eq("player_points")] for c in pd.read_csv(
        EXPORT / "candidate_quotes.csv", chunksize=500000,
        usecols=["candidate_key", "market_key", "market_line", "quote_side", "sportsbook_key",
                 "implied_probability", "observation_timestamp_utc"]))
    q = q[pd.to_numeric(q.market_line, errors="coerce").eq(0.5)]
    q = q.sort_values("observation_timestamp_utc").groupby(["candidate_key", "sportsbook_key", "quote_side"]).tail(1)
    w = q.pivot_table(index=["candidate_key", "sportsbook_key"], columns="quote_side", values="implied_probability").dropna()
    w["nv_over"] = w["OVER"] / (w["OVER"] + w["UNDER"])
    return w.groupby("candidate_key").nv_over.median().rename("market_p_over").reset_index()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=OUT)
    out = ap.parse_args().out
    out.mkdir(parents=True, exist_ok=False)

    logs, db_audit = pull_history()
    logs_path = out / "history_logs_research_pull.csv.gz"
    logs.to_csv(logs_path, index=False, compression="gzip")
    print("pulled", db_audit["rows"], "log rows", flush=True)
    logs, pp_audit = apply_pp_zero_rule(logs)
    frozen = pd.read_csv(FROZEN_FRAME, low_memory=False)
    frozen["game_id"] = frozen.game_id.astype("int64"); frozen["player_id"] = frozen.player_id.astype("int64")
    outcomes, outcome_audit = official_outcomes(frozen)
    history, history_audit = history_inputs(logs, outcomes)
    games = logs[["game_id", "game_date", "start_time_utc", "home_team_id", "away_team_id"]].drop_duplicates("game_id")
    no_logs = ~frozen.game_id.isin(games.game_id)
    target_audit = {"frozen_targets": len(frozen), "dropped_game_without_db_logs": int(no_logs.sum()),
                    "dropped_by_season": {int(k): int(v) for k, v in frozen.loc[no_logs].groupby("canonical_season").size().items()}}
    frozen = frozen.loc[~no_logs].reset_index(drop=True)
    rebuilt = build_targets(history, games, frozen[KEYS + ["canonical_season", "is_home"]])
    rebuilt = rebuilt.merge(frozen[KEYS + ["canonical_season", "realized_points"]], on=KEYS, validate="one_to_one")
    print("rebuilt", len(rebuilt), "historical targets", flush=True)
    label_audit = {"label_source": "restored official outcome frame (frozen V1 labels)", **outcome_audit, **history_audit}
    recon = pd.read_csv(ROOT / "artifacts/analysis/nhl/points_historical_team_shots_reconstruction/2026-10-15/corrected_training_matrix.csv",
                        usecols=["game_id", "player_id", "canonical_team_id"])
    tcheck = recon.merge(logs[KEYS + ["team_id"]], on=KEYS, how="inner")
    label_team_audit = {"rows_compared": len(tcheck), "canonical_team_agrees": int((tcheck.canonical_team_id == tcheck.team_id).sum())}
    parity = feature_parity(rebuilt, frozen)
    parity.to_csv(out / "feature_parity_frozen_vs_rebuilt.csv", index=False)
    train = rebuilt[rebuilt.canonical_season.isin([2023, 2024])].reset_index(drop=True)
    hold = rebuilt[rebuilt.canonical_season.eq(2025)].reset_index(drop=True)
    frozen_hold = frozen[frozen.canonical_season.eq(2025)].reset_index(drop=True)
    train.to_csv(out / "rebuilt_training_matrix_2023_2024.csv.gz", index=False, compression="gzip")
    hold.to_csv(out / "rebuilt_holdout_matrix_2025.csv.gz", index=False, compression="gzip")

    v1 = joblib.load(V1_MODEL_PATH)
    r1 = fit(train, fill_zero=True)
    r2 = fit(train, fill_zero=False)
    joblib.dump(r1, out / "candidate_r1_fill_zero.joblib"); joblib.dump(r2, out / "candidate_r2_nan_native.joblib")

    m25 = []
    for label, bundle, frame in (("V1_FROZEN_ON_FROZEN_FEATURES", v1, frozen_hold),
                                 ("V1_FROZEN_ON_REBUILT_FEATURES", v1, hold),
                                 ("R1_REFIT_FILL_ZERO", r1, hold), ("R2_REFIT_NAN_NATIVE", r2, hold)):
        mu, probs = predict(bundle, frame)
        m25.extend(metric_rows(frame, mu, probs, label))
    pd.DataFrame(m25).to_csv(out / "holdout_2025_metrics.csv", index=False)
    print("holdout metrics written", flush=True)

    t26 = targets_2026(logs)
    f26 = build_targets(history, games, t26[KEYS + ["canonical_season", "is_home"]])
    e26 = t26.drop(columns=["is_home"]).merge(f26, on=KEYS, validate="one_to_one").merge(market_2026(), on="candidate_key", how="left")
    y = (e26.realized_points >= 1).astype(int).to_numpy()
    for label, bundle in (("v1_rebuilt", v1), ("r1", r1), ("r2", r2)):
        e26[f"{label}_p_over"] = predict(bundle, e26)[1][:, 0]
    e26["market_p_over_col"] = e26.market_p_over
    rows = []
    for subset, mask in (("all settled 0.5", np.ones(len(e26), bool)), ("with two-sided market", e26.market_p_over.notna().to_numpy())):
        for label, col in (("v1_captured", "v1_captured_p_over"), ("v1_rebuilt", "v1_rebuilt_p_over"),
                           ("r1", "r1_p_over"), ("r2", "r2_p_over"), ("market", "market_p_over")):
            m = mask & e26[col].notna().to_numpy()
            if m.sum():
                rows.append({"subset": subset, "model": label, **score05(y[m], e26.loc[m, col].to_numpy())})
    pd.DataFrame(rows).to_csv(out / "retrospective_2026_metrics.csv", index=False)

    mk = e26[e26.market_p_over.notna()].copy()
    v1_over = mk.v1_captured_p_over >= .5
    v1_sel = np.where(v1_over, mk.v1_captured_p_over, 1 - mk.v1_captured_p_over)
    mkt_sel = np.where(v1_over, mk.market_p_over, 1 - mk.market_p_over)
    mk["p2_29_cohort"] = (v1_sel > .5) & (mkt_sel < .5)
    mk["pp_bin"] = pd.cut(mk.mean_pp_toi_last10.fillna(-1), [-2, -0.5, 1, 2, 3, 4, 30],
                          labels=["missing", "<1", "1-2", "2-3", "3-4", "4+"])
    yo = (mk.realized_points >= 1).astype(int)
    cohort = []
    groups = [("market disagrees with V1 (P2.29)", mk[mk.p2_29_cohort])]
    groups += [(f"PP {b}", g) for b, g in mk.groupby("pp_bin", observed=True)]
    for name, g in groups:
        yy = yo.loc[g.index].to_numpy()
        row = {"cohort": name, "n": len(g), "over_rate": float(yy.mean()) if len(g) else None}
        for label, col in (("v1_captured", "v1_captured_p_over"), ("r1", "r1_p_over"), ("r2", "r2_p_over"), ("market", "market_p_over")):
            p = g[col].to_numpy()
            row[f"{label}_mean_p_over"] = float(p.mean())
            row[f"{label}_brier"] = float(np.mean((p - yy) ** 2))
            row[f"{label}_record"] = f"{int(((p >= .5) == (yy == 1)).sum())}-{int(((p >= .5) != (yy == 1)).sum())}"
        cohort.append(row)
    pd.DataFrame(cohort).to_csv(out / "retrospective_2026_cohorts.csv", index=False)
    e26.drop(columns=["market_p_over_col"]).to_csv(out / "retrospective_2026_rows.csv.gz", index=False, compression="gzip")

    summary = {"schema_version": "NHL_POINTS_HGB_PP_CORRECTED_REFIT_RESEARCH_V1", "database": db_audit,
               "pp_zero_rule": pp_audit, "label_audit": label_audit, "target_audit": target_audit, "team_binding_vs_2026_10_15_reconstruction": label_team_audit, "training_rows": len(train),
               "holdout_rows": len(hold), "retrospective_2026_rows": len(e26),
               "retrospective_2026_with_market": int(e26.market_p_over.notna().sum()),
               "hyperparameters": KW, "feature_construction": "export_nhl_points_hgb_features.build_features",
               "history_logs_sha256": sha(logs_path), "frozen_frame_sha256": sha(FROZEN_FRAME),
               "v1_model_sha256": sha(V1_MODEL_PATH), "production_changes": 0, "predictions_modified": False,
               "provider_calls": 0}
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
