#!/usr/bin/env python3
# backend/nhl/scripts/evaluate_sog_predictions.py

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import psycopg
from sklearn.metrics import roc_auc_score, log_loss


LINES = [1.5, 2.5, 3.5]
PROP = "shots_on_goal"


def require_db_url() -> str:
    db = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db:
        raise RuntimeError("Missing SUPABASE_DB_URL (or DATABASE_URL).")
    return db


def pick_latest_truth_date(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(game_date)::text
            FROM nhl.skater_game_logs_raw
            WHERE shots_on_goal IS NOT NULL
            """
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None

@dataclass
class DayContext:
    game_date: str
    games_on_date: int
    skater_rows_date: int

def role_bucket(pos: Optional[str]) -> str:
    if not pos:
        return "UNK"
    p = pos.strip().upper()
    return "D" if p == "D" else "F"

def toi_bucket(d10_toi_min_avg: Optional[float]) -> str:
    if d10_toi_min_avg is None or np.isnan(d10_toi_min_avg):
        return "missing"
    x = float(d10_toi_min_avg)
    if x < 12:
        return "<12"
    if x < 16:
        return "12-16"
    if x < 20:
        return "16-20"
    return "20+"

def pp_bucket(pp_role_share_final: Optional[float]) -> str:
    if pp_role_share_final is None or np.isnan(pp_role_share_final):
        return "missing"
    x = float(pp_role_share_final)
    if x <= 0:
        return "0"
    if x <= 0.15:
        return "0-0.15"
    if x <= 0.35:
        return "0.15-0.35"
    return "0.35+"

def load_day_context(conn, game_date: str) -> DayContext:
    with conn.cursor() as cur:
        # games_on_date from nhl.games
        cur.execute(
            """
            SELECT COUNT(DISTINCT game_id)::int
            FROM nhl.games
            WHERE game_date = %s::date
            """,
            (game_date,),
        )
        games_on_date = int(cur.fetchone()[0] or 0)

        # skater truth rows available
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM nhl.skater_game_logs_raw
            WHERE game_date = %s::date
              AND shots_on_goal IS NOT NULL
            """,
            (game_date,),
        )
        skater_rows_date = int(cur.fetchone()[0] or 0)

    return DayContext(
        game_date=game_date,
        games_on_date=games_on_date,
        skater_rows_date=skater_rows_date,
    )


def fetch_rows(conn, game_date: str) -> List[Tuple]:
    """
    Returns tuples:
      (model_family, model_version, line, p_over, shots_on_goal, played, pos, d10_toi_min_avg, pp_role_share_final)

    shots_on_goal may be NULL (missing truth).
    played is 1 if player had any shift in shiftcharts for that game, else 0.
    pos may be NULL.
    d10_toi_min_avg / pp_role_share_final may be NULL.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH games_on_date AS (
              SELECT game_id
              FROM nhl.games
              WHERE game_date = %s::date
            ),
            played AS (
              SELECT DISTINCT ss.game_id, ss.player_id
              FROM nhl.shiftcharts_shifts ss
              JOIN games_on_date gd
                ON gd.game_id = ss.game_id
            )
            SELECT
              COALESCE(p.model_family, '') AS model_family,
              COALESCE(p.model_version, '') AS model_version,
              p.line::float8,
              p.p_over::float8,
              s.shots_on_goal::int,
              CASE WHEN pl2.player_id IS NULL THEN 0 ELSE 1 END AS played,
              pl.position::text,
              f.d10_toi_min_avg::float8,
              f.pp_role_share_final::float8
            FROM nhl.predictions p
            JOIN nhl.games g
              ON g.game_id = p.game_id
            LEFT JOIN nhl.skater_game_logs_raw s
              ON s.game_id = p.game_id
             AND s.player_id = p.player_id
            LEFT JOIN played pl2
              ON pl2.game_id = p.game_id
             AND pl2.player_id = p.player_id
            LEFT JOIN nhl.training_features_nhl_sog_enriched_pregame_v2 f
              ON f.game_id = p.game_id
             AND f.player_id = p.player_id
            LEFT JOIN nhl.players pl
              ON pl.player_id = p.player_id
            WHERE p.prop = %s
              AND g.game_date = %s::date
              AND p.line = ANY(%s)
            """,
            (game_date, PROP, game_date, LINES),
        )
        return cur.fetchall()

def compute_metrics_for_group(
    rows: np.ndarray,  # columns: line, p_over, shots
    line: float,
) -> Dict[str, Optional[float]]:
    # Separate predicted rows vs evaluable rows
    is_line = rows[:, 0] == line
    sub = rows[is_line]

    n_pred = int(sub.shape[0])

    # truth available?
    shots = sub[:, 2]
    truth_mask = ~np.isnan(shots)
    eval_rows = sub[truth_mask]

    n_eval = int(eval_rows.shape[0])

    if n_eval == 0:
        return {
            "n_pred": n_pred,
            "n_eval": 0,
            "n_pos": 0,
            "truth_coverage": 0.0 if n_pred > 0 else 0.0,
            "hit_rate": None,
            "avg_p": None,
            "auc": None,
            "logloss": None,
            "brier": None,
        }

    p = eval_rows[:, 1].astype(float)
    # y_over: shots_on_goal > line  (e.g., 2.5 -> >=3)
    thr = int(np.floor(line) + 1)   # 1.5->2, 2.5->3, 3.5->4
    y = (eval_rows[:, 2].astype(float) >= thr).astype(int)

    n_pos = int(y.sum())
    truth_coverage = (n_eval / n_pred) if n_pred > 0 else 0.0

    # Stable probability clipping
    p_clip = np.clip(p, 1e-6, 1 - 1e-6)

    hit_rate = float(y.mean())
    avg_p = float(p.mean())
    brier = float(np.mean((p - y) ** 2))

    # logloss: always compute if n_eval>0 by providing labels
    ll = float(log_loss(y, p_clip, labels=[0, 1]))

    # auc: only if both classes present
    auc = None
    if len(np.unique(y)) >= 2:
        auc = float(roc_auc_score(y, p))

    return {
        "n_pred": n_pred,
        "n_eval": n_eval,
        "n_pos": n_pos,
        "truth_coverage": float(truth_coverage),
        "hit_rate": hit_rate,
        "avg_p": avg_p,
        "auc": auc,
        "logloss": ll,
        "brier": brier,
    }


def upsert_daily_rows(conn, payload: List[Dict]) -> None:
    sql = """
    INSERT INTO nhl.eval_sog_daily (
      game_date, model_family, model_version,
      line, segment_type, segment_value,
      n_pred, n_eval, n_pos, truth_coverage,
      games_on_date, skater_rows_date, is_low_sample,
      hit_rate, avg_p, auc, logloss, brier
    )
    VALUES (
      %s, %s, %s,
      %s, %s, %s,
      %s, %s, %s, %s,
      %s, %s, %s,
      %s, %s, %s, %s, %s
    )
    ON CONFLICT (game_date, model_family, model_version, line, segment_type, segment_value)
    DO UPDATE SET
      n_pred = EXCLUDED.n_pred,
      n_eval = EXCLUDED.n_eval,
      n_pos = EXCLUDED.n_pos,
      truth_coverage = EXCLUDED.truth_coverage,
      games_on_date = EXCLUDED.games_on_date,
      skater_rows_date = EXCLUDED.skater_rows_date,
      is_low_sample = EXCLUDED.is_low_sample,
      hit_rate = EXCLUDED.hit_rate,
      avg_p = EXCLUDED.avg_p,
      auc = EXCLUDED.auc,
      logloss = EXCLUDED.logloss,
      brier = EXCLUDED.brier,
      updated_at = now()
    ;
    """

    values = [
        (
            r["game_date"],
            r["model_family"],
            r["model_version"],
            r["line"],
            r["segment_type"],
            r["segment_value"],
            r["n_pred"],
            r["n_eval"],
            r["n_pos"],
            r["truth_coverage"],
            r["games_on_date"],
            r["skater_rows_date"],
            r["is_low_sample"],
            r["hit_rate"],
            r["avg_p"],
            r["auc"],
            r["logloss"],
            r["brier"],
        )
        for r in payload
    ]

    with conn.cursor() as cur:
        cur.executemany(sql, values)
    conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--game-date", default=None, help="YYYY-MM-DD; default = latest truth date")
    ap.add_argument("--quiet", action="store_true", help="Reduce console output")
    args = ap.parse_args()

    db = require_db_url()
    conn = psycopg.connect(db, prepare_threshold=0)
    try:
        conn.prepare_threshold = 0  # type: ignore[attr-defined]
    except Exception:
        pass

    try:
        game_date = args.game_date or pick_latest_truth_date(conn)
        if not game_date:
            print("[eval_sog] no truth dates available yet; skipping")
            return

        ctx = load_day_context(conn, game_date)
        is_low_sample = ctx.games_on_date < 5

        raw = fetch_rows(conn, game_date)
        if not raw:
            print(f"[eval_sog] no predictions found for {game_date}; skipping")
            return

        # Determine distinct model groups present that day (family+version)
        groups = sorted(set((mf, mv) for (mf, mv, *_) in raw))

        payload: List[Dict] = []

        for (mf, mv) in groups:
            # Filter rows for this model group and convert to numeric array
            grp = []
            for (mf2, mv2, line, p_over, shots, played, pos, toi, pp) in raw:
                if (mf2, mv2) != (mf, mv):
                    continue
                grp.append((
                    float(line),
                    float(p_over),
                    float(shots) if shots is not None else np.nan,
                    int(played) if played is not None else 0,
                    role_bucket(pos),
                    toi_bucket(toi),
                    pp_bucket(pp),
            ))


            # numpy array for numeric cols; keep buckets separately in python list
            # numpy array for numeric cols; keep buckets separately in python list
            arr_num = np.array([(r[0], r[1], r[2]) for r in grp], dtype=float)
            arr = arr_num

            playeds = [r[3] for r in grp]
            roles   = [r[4] for r in grp]
            tois    = [r[5] for r in grp]
            pps     = [r[6] for r in grp]


            for line in LINES:
                m = compute_metrics_for_group(arr, float(line))
                payload.append({
                    "game_date": ctx.game_date,
                    "model_family": mf,
                    "model_version": mv,
                    "line": float(line),
                    "segment_type": "all",
                    "segment_value": "all",
                    "games_on_date": ctx.games_on_date,
                    "skater_rows_date": ctx.skater_rows_date,
                    "is_low_sample": bool(is_low_sample),
                    **m,
                })

            idx_played = [i for i, v in enumerate(playeds) if v == 1]
            if idx_played:
                sub = arr_num[idx_played, :]
                for line in LINES:
                    m = compute_metrics_for_group(sub, float(line))
                    payload.append({
                        "game_date": ctx.game_date,
                        "model_family": mf,
                        "model_version": mv,
                        "line": float(line),
                        "segment_type": "played",
                        "segment_value": "shiftcharts",
                        "games_on_date": ctx.games_on_date,
                        "skater_rows_date": ctx.skater_rows_date,
                        "is_low_sample": bool(is_low_sample),
                        **m,
                    })

            for role in ("D", "F", "UNK"):
                idx = [i for i, v in enumerate(roles) if v == role]
                if not idx:
                    continue
                sub = arr_num[idx, :]
                for line in LINES:
                    m = compute_metrics_for_group(sub, float(line))
                    payload.append({
                        "game_date": ctx.game_date,
                        "model_family": mf,
                        "model_version": mv,
                        "line": float(line),
                        "segment_type": "role",
                        "segment_value": role,
                        "games_on_date": ctx.games_on_date,
                        "skater_rows_date": ctx.skater_rows_date,
                        "is_low_sample": bool(is_low_sample),
                        **m,
                    })
            for b in ("<12", "12-16", "16-20", "20+", "missing"):
                idx = [i for i, v in enumerate(tois) if v == b]
                if not idx:
                    continue
                sub = arr_num[idx, :]
                for line in LINES:
                    m = compute_metrics_for_group(sub, float(line))
                    payload.append({
                        "game_date": ctx.game_date,
                        "model_family": mf,
                        "model_version": mv,
                        "line": float(line),
                        "segment_type": "toi",
                        "segment_value": b,
                        "games_on_date": ctx.games_on_date,
                        "skater_rows_date": ctx.skater_rows_date,
                        "is_low_sample": bool(is_low_sample),
                        **m,
                    })

            for b in ("0", "0-0.15", "0.15-0.35", "0.35+", "missing"):
                idx = [i for i, v in enumerate(pps) if v == b]
                if not idx:
                    continue
                sub = arr_num[idx, :]
                for line in LINES:
                    m = compute_metrics_for_group(sub, float(line))
                    payload.append({
                        "game_date": ctx.game_date,
                        "model_family": mf,
                        "model_version": mv,
                        "line": float(line),
                        "segment_type": "pp",
                        "segment_value": b,
                        "games_on_date": ctx.games_on_date,
                        "skater_rows_date": ctx.skater_rows_date,
                        "is_low_sample": bool(is_low_sample),
                        **m,
                    })

        upsert_daily_rows(conn, payload)

        if not args.quiet:
            print(f"✅ eval_sog_daily upserted for {game_date} (games={ctx.games_on_date}, low_sample={is_low_sample})")
            # Print a compact summary for the default model group (first one)
            mf0, mv0 = groups[0]
            rows0 = [
                r for r in payload
                if r["model_family"] == mf0
                and r["model_version"] == mv0
                and r["segment_type"] == "all"
                and r["segment_value"] == "all"
            ]
            for r in sorted(rows0, key=lambda x: x["line"]):
                print(
                    f"  line={r['line']:.1f}  n_eval={r['n_eval']}/{r['n_pred']} "
                    f"cov={r['truth_coverage']:.3f}  "
                    f"brier={None if r['brier'] is None else round(r['brier'],6)}  "
                    f"logloss={None if r['logloss'] is None else round(r['logloss'],6)}  "
                    f"auc={None if r['auc'] is None else round(r['auc'],6)}"
                )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
