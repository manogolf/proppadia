#!/usr/bin/env python3
"""
Generic loader for NHL predictions into nhl.predictions.

Supports two CSV shapes:
  A) LONG:  player_id,game_id,line,prob_over,... (team_id/game_date optional)
  B) WIDE:  player_id,game_id,p_over_18.5,p_over_19.5,...  (team_id/game_date optional)
           -> unpivots to LONG internally (line, prob_over)

If team_id and/or game_date are missing, this loader will fill them from:
  - team_id: nhl.roster_status (by game_id, player_id)
  - game_date: nhl.games (by game_id)

Usage examples:
  # points (long)
  python backend/nhl/scripts/load_nhl_predictions_generic.py \
    --pred-csv backend/nhl/data/processed/points_predictions_calibrated.csv \
    --project nhl --prop player_points

  # saves (wide)
  python backend/nhl/scripts/load_nhl_predictions_generic.py \
    --pred-csv backend/nhl/data/processed/saves_predictions.csv \
    --project nhl --prop goalie_saves
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg


def require_db_url() -> str:
    db = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or os.environ.get("DB")
    if not db:
        raise SystemExit("Missing DB URL. Set SUPABASE_DB_URL (preferred) or DATABASE_URL or DB.")
    return db


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "none":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "none":
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_date_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    # normalize ISO datetime -> date
    if "T" in s:
        return s.split("T", 1)[0]
    if " " in s:
        return s.split(" ", 1)[0]
    return s


def read_rows(csv_path: Path) -> Tuple[List[Dict[str, Any]], List[str]]:
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"CSV has no header: {csv_path}")
        rows = [r for r in reader]
        return rows, list(reader.fieldnames)


def chunked(seq: List[int], n: int) -> List[List[int]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def fetch_game_dates(cur, game_ids: List[int]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    if not game_ids:
        return out
    for chunk in chunked(game_ids, 5000):
        cur.execute(
            """
            SELECT game_id::bigint, game_date::date
            FROM nhl.games
            WHERE game_id = ANY(%s);
            """,
            (chunk,),
        )
        for gid, gdate in cur.fetchall():
            if gid is not None and gdate is not None:
                out[int(gid)] = str(gdate)
    return out


def fetch_team_ids(cur, pairs: List[Tuple[int, int]]) -> Dict[Tuple[int, int], int]:
    """
    Returns mapping (game_id, player_id) -> team_id from nhl.roster_status.
    Uses DISTINCT to be robust to duplicates.
    """
    out: Dict[Tuple[int, int], int] = {}
    if not pairs:
        return out

    # Split into chunks to keep query sizes sane.
    for chunk in chunked(list(range(len(pairs))), 4000):
        sub = [pairs[i] for i in chunk]
        game_ids = sorted({gid for gid, _ in sub})
        player_ids = sorted({pid for _, pid in sub})

        cur.execute(
            """
            SELECT DISTINCT game_id::bigint, player_id::bigint, team_id::int
            FROM nhl.roster_status
            WHERE game_id = ANY(%s) AND player_id = ANY(%s);
            """,
            (game_ids, player_ids),
        )
        for gid, pid, tid in cur.fetchall():
            if gid is None or pid is None or tid is None:
                continue
            out[(int(gid), int(pid))] = int(tid)

    return out


def is_wide_saves(cols: List[str]) -> bool:
    # wide if it has p_over_<line> columns and NOT a prob_over column
    has_p_over_cols = any(c.startswith("p_over_") for c in cols)
    has_prob_over = "prob_over" in cols
    return has_p_over_cols and not has_prob_over


def parse_line_from_p_over_col(col: str) -> Optional[float]:
    # examples: p_over_24.5, p_over_18.5
    if not col.startswith("p_over_"):
        return None
    suffix = col[len("p_over_") :].strip()
    return _to_float(suffix)


def main() -> None:
    ap = argparse.ArgumentParser(description="Load NHL prop prediction CSV into nhl.predictions")
    ap.add_argument("--pred-csv", required=True, help="Path to predictions CSV")
    ap.add_argument("--project", required=True, help="e.g., nhl")
    ap.add_argument("--prop", required=True, help="e.g., goalie_saves, player_points, shots_on_goal")
    args = ap.parse_args()

    csv_path = Path(args.pred_csv)
    if not csv_path.exists():
        raise SystemExit(f"Missing CSV: {csv_path}")

    rows, cols = read_rows(csv_path)

    # Always require these IDs
    base_required = {"player_id", "game_id"}
    missing_base = sorted([c for c in base_required if c not in set(cols)])
    if missing_base:
        raise SystemExit(f"CSV missing required columns: {missing_base}. Has: {cols}")

    wide_mode = is_wide_saves(cols)

    # LONG mode: need prob_over and line
    if not wide_mode:
        if "prob_over" not in cols:
            raise SystemExit(f"CSV missing 'prob_over'. Has: {cols}")
        if "line" not in cols:
            raise SystemExit(f"CSV missing 'line'. Has: {cols}")

    have_team_id = "team_id" in cols
    have_game_date = "game_date" in cols

    # Prep DB lookups only if we need them
    db = require_db_url()
    game_ids: List[int] = []
    pairs: List[Tuple[int, int]] = []

    for r in rows:
        pid = _to_int(r.get("player_id"))
        gid = _to_int(r.get("game_id"))
        if not pid or not gid:
            continue
        game_ids.append(gid)
        pairs.append((gid, pid))

    game_ids = sorted(set(game_ids))

    with psycopg.connect(db) as conn, conn.cursor() as cur:
        try:
            conn.prepare_threshold = None
        except Exception:
            pass

        game_date_map: Dict[int, str] = {}
        team_id_map: Dict[Tuple[int, int], int] = {}

        if not have_game_date:
            game_date_map = fetch_game_dates(cur, game_ids)
        if not have_team_id:
            team_id_map = fetch_team_ids(cur, pairs)

        payload: List[Dict[str, Any]] = []
        bad = 0

        # Defaults for required DB columns in nhl.predictions
        default_model_family = getattr(args, "model_family", None) or "phoenix"
        default_feature_hash = getattr(args, "feature_hash", None) or "phoenix_v2"
        default_model_version = getattr(args, "model_version", None)

        if wide_mode:
            # Unpivot wide p_over_* into (line, p_over)
            p_over_cols = [c for c in cols if c.startswith("p_over_")]
            line_cols: List[Tuple[float, str]] = []
            for c in p_over_cols:
                ln = parse_line_from_p_over_col(c)
                if ln is not None:
                    line_cols.append((ln, c))
            line_cols.sort(key=lambda t: t[0])

            if not line_cols:
                raise SystemExit(f"No parseable p_over_* columns found. Has: {cols}")

            for r in rows:
                pid = _to_int(r.get("player_id"))
                gid = _to_int(r.get("game_id"))
                if not pid or not gid:
                    bad += 1
                    continue

                for ln, c in line_cols:
                    p = _to_float(r.get(c))
                    if p is None:
                        continue  # skip blanks so we never insert NULL p_over
                    payload.append(
                        {
                            "prop": args.prop,
                            "player_id": pid,
                            "game_id": gid,
                            "line": float(ln),
                            "p_over": float(p),
                            "model_family": default_model_family,
                            "model_params": {},  # jsonb NOT NULL
                            "feature_hash": default_feature_hash,
                            "model_version": default_model_version,
                        }
                    )
        else:
            # LONG (already has line + prob_over)
            for r in rows:
                pid = _to_int(r.get("player_id"))
                gid = _to_int(r.get("game_id"))
                ln = _to_float(r.get("line"))

                p = _to_float(r.get("p_over"))
                if p is None:
                    p = _to_float(r.get("prob_over"))

                if not pid or not gid or ln is None or p is None:
                    bad += 1
                    continue

                payload.append(
                    {
                        "prop": args.prop,
                        "player_id": pid,
                        "game_id": gid,
                        "line": float(ln),
                        "p_over": float(p),
                        "model_family": default_model_family,
                        "model_params": {},  # jsonb NOT NULL
                        "feature_hash": default_feature_hash,
                        "model_version": default_model_version,
                    }
                )

        if not payload:
            raise SystemExit(f"No valid rows to load from {csv_path} (bad_rows={bad}, total_rows={len(rows)})")

        now = datetime.now(timezone.utc).isoformat()

        # IMPORTANT: adjust conflict target if your unique constraint differs.
        # IMPORTANT: adjust conflict target if your unique constraint differs.
# --- REPLACE the entire cur.execute("""...""") block that inserts into nhl.predictions with this ---

        cur.execute(
            """
            WITH src AS (
              SELECT *
              FROM jsonb_to_recordset(%s::jsonb) AS s(
                prop          text,
                player_id     bigint,
                game_id       bigint,
                line          numeric,
                p_over        double precision,
                model_family  text,
                model_params  jsonb,
                feature_hash  text,
                model_version text
              )
            )
            INSERT INTO nhl.predictions (
              prop,
              player_id,
              game_id,
              line,
              p_over,
              model_family,
              model_params,
              feature_hash,
              model_version,
              created_at,
              updated_at
            )
            SELECT
              s.prop,
              s.player_id,
              s.game_id,
              s.line::numeric(4,1),
              s.p_over,
              COALESCE(s.model_family, %s),
              COALESCE(s.model_params, '{}'::jsonb),
              COALESCE(s.feature_hash, %s),
              s.model_version,
              %s::timestamptz,
              %s::timestamptz
            FROM src s
            WHERE s.p_over IS NOT NULL
            ON CONFLICT (prop, player_id, game_id, line, feature_hash) DO UPDATE
            SET
              p_over        = EXCLUDED.p_over,
              model_family  = EXCLUDED.model_family,
              model_params  = EXCLUDED.model_params,
              model_version = EXCLUDED.model_version,
              updated_at    = EXCLUDED.updated_at;
            """,
            (
                json.dumps(payload),
                "phoenix",      # default model_family
                "phoenix_v2",   # default feature_hash (matches your table default)
                now,
                now,
            ),
        )
        conn.commit()

    print(
        f"✅ Loaded {len(payload)} rows into nhl.predictions "
        f"(project={args.project}, prop={args.prop}, wide_mode={wide_mode}, bad_rows={bad})"
    )


if __name__ == "__main__":
    main()
