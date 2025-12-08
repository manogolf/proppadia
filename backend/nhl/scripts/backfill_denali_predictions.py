#!/usr/bin/env python
"""
backend/nhl/scripts/backfill_denali_predictions.py

Backfill sog_denali_lr_rf predictions into nhl.predictions for a historical
date range, so calibration has more than a few hundred rows to work with.

Usage examples:

  # Backfill a single season
  python backend/nhl/scripts/backfill_denali_predictions.py --start 2023-10-01 --end 2024-07-01

  # Backfill a smaller window
  python backend/nhl/scripts/backfill_denali_predictions.py --start 2025-01-01 --end 2025-12-31 --chunk-days 30

Assumes:
  - Postgres URL in SUPABASE_DB_URL.
  - Feature view/table nhl.training_features_sog_denali with at least:
      player_id, game_id, game_date, season, team_id, opponent_id, is_home,
      and all Denali feature columns your model needs.
  - You’ll plug in your existing Denali SOG scoring code in score_denali_batch().
"""

import argparse
import os
from datetime import date, datetime, timedelta
from pathlib import Path
import joblib
import sys
import numpy as np
import psycopg
import pandas as pd
import json




MODEL_FAMILY = "sog_denali_lr_rf"
MODEL_VERSION = "denali_v1"  # Denali-specific label
PROP_NAME = "shots_on_goal"

NHL_ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = NHL_ROOT / "models"

# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------


def get_conn():
    dsn = os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL is not set in environment")
    return psycopg.connect(dsn, autocommit=False)


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def daterange_chunks(start: date, end: date, chunk_days: int):
    """
    Yield (chunk_start, chunk_end) pairs where chunk_end is inclusive.
    end is inclusive overall.
    """
    cur = start
    one_day = timedelta(days=1)
    cd = timedelta(days=chunk_days)
    while cur <= end:
        chunk_start = cur
        chunk_end = min(end, cur + cd - one_day)
        yield chunk_start, chunk_end
        cur = chunk_end + one_day


# ---------------------------------------------------------------------
# FEATURE FETCH
# ---------------------------------------------------------------------


def fetch_denali_features(conn, start: date, end: date, season: int | None = None) -> pd.DataFrame:
    """
    Pulls SOG Denali feature rows for the given date window.

    Adjust WHERE clause if your view/table uses different names.
    """
    sql = """
        SELECT
          player_id,
          game_id,
          game_date::date AS game_date,
          season,
          team_id,
          opponent_id,
          is_home,
          *
        FROM nhl.training_features_sog_denali
        WHERE game_date::date BETWEEN %s AND %s
          AND shots_on_goal IS NOT NULL  -- only completed games
    """
    params = [start, end]
    if season is not None:
        sql += " AND season = %s"
        params.append(season)

    df = pd.read_sql(sql, conn, params=params)
    return df


# ---------------------------------------------------------------------
# MODEL SCORING (YOU PLUG THIS IN)
# ---------------------------------------------------------------------


def score_denali_batch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score a batch of Denali SOG features for all standard lines (0.5, 1.5, 2.5, 3.5)
    using the same models + feature metadata used by score_sog_player_denali.py.

    Expects df to contain:
      - player_id
      - game_id
      - (optionally) season
      - all Denali feature columns used during training
    """
    if df.empty:
        print("score_denali_batch: input DataFrame is empty; nothing to score.", file=sys.stderr)
        return pd.DataFrame()

    # Where the Denali SOG models live, matching cli.py / score_sog_player_denali.py
    models_root = MODELS_DIR / "latest" / "shots_on_goal" / "sog_player_denali"
    if not models_root.exists():
        print(f"score_denali_batch: models_root does not exist: {models_root}", file=sys.stderr)
        return pd.DataFrame()

    # Standard SOG lines we train/score
    line_list = [0.5, 1.5, 2.5, 3.5]

    all_preds: list[pd.DataFrame] = []

    # Boolean features we normalize to 0/1 (must match training)
    BOOL_FEATURES = {"is_home", "b2b_flag", "hot_last5_flag"}

    for line in line_list:
        line_dir = str(line).replace(".", "_")
        model_dir = models_root / line_dir

        if not model_dir.exists():
            print(f"score_denali_batch: missing model_dir for line {line}: {model_dir}", file=sys.stderr)
            continue

        lr_path = model_dir / "lr.joblib"
        rf_path = model_dir / "rf.joblib"
        md_path = model_dir / "feature_metadata.json"

        if not lr_path.exists() or not rf_path.exists() or not md_path.exists():
            print(
                f"score_denali_batch: missing lr/rf/metadata for line {line} "
                f"(expected lr.joblib, rf.joblib, feature_metadata.json in {model_dir})",
                file=sys.stderr,
            )
            continue

        # Load metadata to get canonical feature list + model_version (if present)
        metadata = json.loads(md_path.read_text())
        feats = metadata.get("features") or []
        model_version = metadata.get("model_version") or metadata.get("feature_key") or "denali"

        if not feats:
            print(f"score_denali_batch: no features listed in metadata for line {line}", file=sys.stderr)
            continue

        # Start from the full feature df; we’ll work on a copy per line
        df_line = df.copy()

        # Normalize boolean-like columns in place
        for c in feats:
            if c in df_line.columns and c in BOOL_FEATURES:
                df_line[c] = (
                    df_line[c]
                    .replace(
                        {
                            True: 1,
                            False: 0,
                            "t": 1,
                            "f": 0,
                            "true": 1,
                            "false": 0,
                            "True": 1,
                            "False": 0,
                        }
                    )
                )

        # Ensure all expected feature columns exist
        missing = [c for c in feats if c not in df_line.columns]
        if missing:
            print(
                f"score_denali_batch: line {line} missing features {missing}; filling with 0.0",
                file=sys.stderr,
            )
            for c in missing:
                df_line[c] = 0.0

        # Build feature frame and coerce to numeric
        feat_df = df_line[feats].copy()

        # Coerce *all* feature columns to numeric in one shot
        feat_df = feat_df.apply(pd.to_numeric, errors="coerce")

        feat_df = feat_df.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        X = feat_df.to_numpy(dtype=float)

        # Load models
        lr = joblib.load(lr_path)
        rf = joblib.load(rf_path)

        # Handle feature count mismatch versus the stored pipeline/scaler
        expected_n = getattr(lr, "n_features_in_", None)
        if expected_n is None:
            # Pipeline case: look at the first step that has n_features_in_
            for step_name, step_obj in getattr(lr, "steps", []):
                if hasattr(step_obj, "n_features_in_"):
                    expected_n = step_obj.n_features_in_
                    break

        if expected_n is not None and X.shape[1] != expected_n:
            if X.shape[1] > expected_n:
                # Trim extra columns (always from the right; feats is ordered)
                X = X[:, :expected_n]
                print(
                    f"score_denali_batch: line {line} – trimming features from {len(feats)} to {expected_n} "
                    f"to satisfy model.",
                    file=sys.stderr,
                )
            else:
                # Pad with zeros if we somehow have fewer than expected
                pad_width = expected_n - X.shape[1]
                X = np.pad(X, ((0, 0), (0, pad_width)), mode="constant", constant_values=0.0)
                print(
                    f"score_denali_batch: line {line} – padding features from {X.shape[1]-pad_width} "
                    f"to {expected_n} with zeros to satisfy model.",
                    file=sys.stderr,
                )

        if X.shape[0] == 0:
            print(f"score_denali_batch: no rows to score for line {line}", file=sys.stderr)
            continue

        # Predict probabilities of Over(line)
        prob_lr = lr.predict_proba(X)[:, 1]
        prob_rf = rf.predict_proba(X)[:, 1]
        prob_blend = 0.5 * prob_lr + 0.5 * prob_rf

        # Build prediction rows
        # Build 1-D arrays for DataFrame construction
        player_id = np.asarray(df_line["player_id"]).reshape(-1)
        game_id   = np.asarray(df_line["game_id"]).reshape(-1)

        prob_lr_1d     = np.asarray(prob_lr).reshape(-1)
        prob_rf_1d     = np.asarray(prob_rf).reshape(-1)
        prob_blend_1d  = np.asarray(prob_blend).reshape(-1)

        # Use predictions as canonical; trim everything else to common min length if needed
        n_pid  = len(player_id)
        n_gid  = len(game_id)
        n_pred = len(prob_blend_1d)

        if not (n_pid == n_gid == n_pred):
            m = min(n_pid, n_gid, n_pred)
            print(
                f"score_denali_batch: line {line} – length mismatch "
                f"(player_id={n_pid}, game_id={n_gid}, preds={n_pred}); "
                f"trimming all to {m}.",
                file=sys.stderr,
            )
            player_id     = player_id[:m]
            game_id       = game_id[:m]
            prob_lr_1d    = prob_lr_1d[:m]
            prob_rf_1d    = prob_rf_1d[:m]
            prob_blend_1d = prob_blend_1d[:m]
            n_pred        = m

        # Constant columns as 1-D arrays
        prop     = np.full(n_pred, "shots_on_goal", dtype=object)
        line_arr = np.full(n_pred, line, dtype=float)

        preds_line = pd.DataFrame(
            {
                "player_id": player_id,
                "game_id": game_id,
                "prop": prop,
                "line": line_arr,
                "p_over": prob_blend_1d,
                "model_family": "sog_denali_lr_rf",
                "model_version": model_version,
            }
        )

        print(
            f"score_denali_batch: line {line} – scored {len(preds_line)} rows.",
            file=sys.stderr,
        )
        all_preds.append(preds_line)

    if not all_preds:
        print("score_denali_batch: no predictions produced for any line.", file=sys.stderr)
        return pd.DataFrame()

    preds = pd.concat(all_preds, ignore_index=True)
    print(f"score_denali_batch: total predictions produced: {len(preds)}", file=sys.stderr)
    return preds


# ---------------------------------------------------------------------
# UPSERT INTO nhl.predictions
# ---------------------------------------------------------------------


def upsert_predictions(conn, preds: pd.DataFrame, dry_run: bool = False, batch_size: int = 1000) -> int:
    """
    Upsert Denali SOG predictions into nhl.predictions row-by-row,
    committing frequently to avoid SSL timeouts.

    Expects preds with columns:
      - player_id (int)
      - game_id (int)
      - prop (text)
      - line (float)
      - p_over (float)
      - model_family (text)
      - model_version (text)

    Idempotent on (player_id, game_id, prop, line).
    """
    if preds.empty:
        print("upsert_predictions: no rows to upsert.")
        return 0

    required_cols = ["player_id", "game_id", "prop", "line", "p_over", "model_family", "model_version"]
    missing = [c for c in required_cols if c not in preds.columns]
    if missing:
        raise ValueError(f"upsert_predictions: missing required columns in preds: {missing}")

    if dry_run:
        print(f"[DRY RUN] Would upsert {len(preds)} rows into nhl.predictions (row-by-row).")
        return len(preds)

    insert_sql = """
        INSERT INTO nhl.predictions (
          player_id,
          game_id,
          prop,
          line,
          p_over,
          model_family,
          model_version,
          created_at,
          updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,now(),now())
        ON CONFLICT (player_id, game_id, prop, line)
        DO UPDATE SET
          p_over       = EXCLUDED.p_over,
          model_family = EXCLUDED.model_family,
          model_version= EXCLUDED.model_version,
          updated_at   = now();
    """

    total = 0
    cur = conn.cursor()

    # INSERT each row individually, commit every N
    for i, row in preds.iterrows():
        try:
            cur.execute(
                insert_sql,
                (
                    int(row.player_id),
                    int(row.game_id),
                    str(row.prop),
                    float(row.line),
                    float(row.p_over),
                    str(row.model_family),
                    str(row.model_version),
                ),
            )
            total += 1

            # commit frequently (avoids SSL disconnects)
            if total % 200 == 0:
                conn.commit()
                print(f"  -> committed {total} rows so far")

        except Exception as e:
            conn.rollback()
            print(f"⚠️ row {i} failed: {e}")

    conn.commit()
    cur.close()

    print(f"  -> upserted total {total} rows into nhl.predictions")
    return total


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description="Backfill Denali SOG predictions into nhl.predictions")
    ap.add_argument("--start", required=True, help="Start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    ap.add_argument("--chunk-days", type=int, default=30, help="Number of days per DB chunk")
    ap.add_argument("--season", type=int, default=None, help="Optional season filter (e.g. 2023)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to DB, just log")
    args = ap.parse_args()

    start = parse_date(args.start)
    end = parse_date(args.end)
    if end < start:
        raise SystemExit("end must be >= start")

    print(f"[backfill_denali] start={start} end={end} chunk_days={args.chunk_days} season={args.season}")
    if args.dry_run:
        print("[backfill_denali] DRY RUN mode enabled; no DB writes will occur.")

    with get_conn() as conn:
        total_inserted = 0
        for chunk_start, chunk_end in daterange_chunks(start, end, args.chunk_days):
            print(f"\n[chunk] {chunk_start}..{chunk_end}")
            df = fetch_denali_features(conn, chunk_start, chunk_end, args.season)
            if df.empty:
                print("  -> no feature rows; skipping")
                continue

            print(f"  -> fetched {len(df)} feature rows; scoring Denali...")
            preds = score_denali_batch(df)

            if preds.empty:
                print("  -> score_denali_batch returned 0 rows; skipping upsert")
                continue

            preds = preds.copy()
            preds["prop"] = PROP_NAME
            preds["model_family"] = MODEL_FAMILY
            preds["model_version"] = MODEL_VERSION

            inserted = upsert_predictions(conn, preds, dry_run=args.dry_run)
            total_inserted += inserted
            print(f"  -> upserted {inserted} rows into nhl.predictions (total={total_inserted})")

        print(f"\n[backfill_denali] Done. Total rows upserted: {total_inserted}")


if __name__ == "__main__":
    main()
