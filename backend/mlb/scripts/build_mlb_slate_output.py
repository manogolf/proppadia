#!/usr/bin/env python3
"""
Build canonical MLB slate output (model-only) from calibrated wide predictions.

Purpose:
- Create one normalized MLB slate artifact that downstream consumers can share:
  - MLB market board builder (future)
  - MLB book upload exporter (operator tooling)
- Keep predictions market-independent. This file contains model probabilities only.

Default input:
- backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv

Default output:
- backend/mlb/data/processed/mlb_slate_output.csv
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]  # .../backend
DEFAULT_PRED_CSV = BASE_DIR / "mlb" / "data" / "processed" / "mlb_predictions_wide_calibrated.csv"
DEFAULT_OUT_CSV = BASE_DIR / "mlb" / "data" / "processed" / "mlb_slate_output.csv"

# External book-upload taxonomy (MARKET key) doubles as a stable cross-tool prop mapping.
DEFAULT_MARKET_BY_PROP: Dict[str, str] = {
    "hits": "batter_hits",
    "runs_scored": "batter_runs",
    "rbis": "batter_rbis",
    "runs_rbis": "batter_r+rbi",
    "total_bases": "batter_bases",
    "hits_runs_rbis": "batter_h+r+rbi",
    "walks": "batter_walks",
    "strikeouts_batting": "batter_strikeouts",
    "stolen_bases": "batter_stolen_bases",
    "singles": "batter_singles",
    "doubles": "batter_doubles",
    "triples": "batter_triples",
    "home_runs": "batter_home_runs",
    "hits_allowed": "pitcher_hits",
    "earned_runs": "pitcher_earned_runs",
    "outs_recorded": "pitcher_outs",
    "walks_allowed": "pitcher_walks",
    "strikeouts_pitching": "pitcher_strikeouts",
}

_PCOL_RE = re.compile(r"^p_over_(\d+)_([05])$")


def _canonical_prop_type(value: object) -> str:
    return str(value or "").strip().lower()


def _clean_optional_str(value: object) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _parse_lines_from_cols(cols: Iterable[str]) -> List[Tuple[str, float]]:
    out: List[Tuple[str, float]] = []
    for col in cols:
        m = _PCOL_RE.match(col)
        if not m:
            continue
        whole = int(m.group(1))
        half = int(m.group(2))
        line = float(whole) + (0.5 if half == 5 else 0.0)
        out.append((col, line))
    out.sort(key=lambda x: x[1])
    return out


def _prob_to_fair_american(prob: float) -> Optional[int]:
    if not (0.0 < prob < 1.0):
        return None
    if prob >= 0.5:
        return int(-round(100.0 * prob / (1.0 - prob)))
    return int(round(100.0 * (1.0 - prob) / prob))


def _load_market_map(arg_json: str, env_json: str) -> Dict[str, str]:
    out = dict(DEFAULT_MARKET_BY_PROP)
    raw = (arg_json or "").strip() or (env_json or "").strip()
    if not raw:
        return out
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("market map JSON must be an object")
    for key, value in payload.items():
        prop = _canonical_prop_type(key)
        val = str(value or "").strip()
        if prop and val:
            out[prop] = val
    return out


def _get_db_conn():
    import psycopg2  # type: ignore

    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("missing SUPABASE_DB_URL or DATABASE_URL")
    return psycopg2.connect(db_url)


def _table_columns(conn, *, schema: str, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name::text
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema, table),
        )
        return {str(r[0]) for r in (cur.fetchall() or [])}


def _fetch_games(conn, game_ids: List[int]) -> pd.DataFrame:
    if not game_ids:
        return pd.DataFrame(
            columns=["game_id", "game_date", "game_type", "home_team_code", "away_team_code"]
        )

    cols = _table_columns(conn, schema="mlb", table="game_info")
    game_type_expr = "game_type::text AS game_type" if "game_type" in cols else "NULL::text AS game_type"
    sql = f"""
    SELECT
      game_id::bigint AS game_id,
      game_date::date AS game_date,
      {game_type_expr},
      home_team_abbr::text AS home_team_code,
      away_team_abbr::text AS away_team_code
    FROM mlb.game_info
    WHERE game_id = ANY(%s::bigint[])
    """
    return pd.read_sql(sql, conn, params=(list(game_ids),))


def _fetch_players(conn, player_ids: List[int]) -> pd.DataFrame:
    if not player_ids:
        return pd.DataFrame(columns=["player_id", "player_name"])

    cols = _table_columns(conn, schema="mlb", table="player_ids")
    if "player_name" not in cols:
        return pd.DataFrame(columns=["player_id", "player_name"])
    sql = """
    SELECT
      player_id::bigint AS player_id,
      player_name::text AS player_name
    FROM mlb.player_ids
    WHERE player_id = ANY(%s::bigint[])
    """
    return pd.read_sql(sql, conn, params=(list(player_ids),))


def _load_predictions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing predictions file: {path}")
    print(f"[mlb-slate-output] reading wide predictions: {path}")
    return pd.read_csv(path)


def _melt_to_long(df_wide: pd.DataFrame, default_prop_type: Optional[str]) -> pd.DataFrame:
    for key in ("player_id", "game_id"):
        if key not in df_wide.columns:
            raise ValueError(f"predictions missing required column: {key}")

    col_lines = _parse_lines_from_cols(df_wide.columns)
    if not col_lines:
        raise ValueError("no p_over_* columns found in predictions input")
    prob_cols = [c for c, _ in col_lines]
    print(f"[mlb-slate-output] probability columns: {prob_cols}")

    use_prop_col = "prop_type" in df_wide.columns
    if not use_prop_col and not default_prop_type:
        raise ValueError("missing prop_type column and no --prop-type provided")

    id_cols = ["player_id", "game_id"] + (["prop_type"] if use_prop_col else [])
    for optional_col in (
        "player_name",
        "game_date",
        "game_type",
        "home_team_code",
        "away_team_code",
        "game_time",
        "team",
        "team_id",
        "opponent",
        "opponent_id",
        "is_home",
    ):
        if optional_col in df_wide.columns and optional_col not in id_cols:
            id_cols.append(optional_col)

    df_long = df_wide[id_cols + prob_cols].melt(
        id_vars=id_cols,
        value_vars=prob_cols,
        var_name="prob_col",
        value_name="prob_over",
    )
    line_map = {c: line for c, line in col_lines}
    df_long["line"] = df_long["prob_col"].map(line_map).astype(float)
    df_long = df_long.drop(columns=["prob_col"])

    if use_prop_col:
        df_long["prop_type"] = df_long["prop_type"].map(_canonical_prop_type)
    else:
        df_long["prop_type"] = _canonical_prop_type(default_prop_type)

    for c in ("player_id", "game_id", "prob_over", "line"):
        df_long[c] = pd.to_numeric(df_long[c], errors="coerce")

    if "player_name" in df_long.columns:
        df_long["player_name"] = df_long["player_name"].astype(str).str.strip()
        df_long.loc[df_long["player_name"].isin(["", "nan", "None"]), "player_name"] = None

    df_long = df_long.dropna(subset=["player_id", "game_id", "prob_over", "line"])
    df_long = df_long[df_long["prop_type"].astype(str).str.len() > 0]
    if df_long.empty:
        raise ValueError("no usable prediction rows after melt/cleanup")

    df_long["player_id"] = df_long["player_id"].astype(int)
    df_long["game_id"] = df_long["game_id"].astype(int)
    return df_long


def _enrich_with_db(df_long: pd.DataFrame) -> pd.DataFrame:
    unique_player_ids = sorted(df_long["player_id"].unique().tolist())
    need_game_cols = ["game_date", "home_team_code", "away_team_code"]
    embedded_complete = all(c in df_long.columns for c in need_game_cols) and not df_long[need_game_cols].isna().any(axis=None)

    if embedded_complete:
        print(
            f"[mlb-slate-output] using embedded game metadata for rows={len(df_long)} players={len(unique_player_ids)}"
        )
        merged = df_long.copy()
        with _get_db_conn() as conn:
            players = _fetch_players(conn, unique_player_ids)
    else:
        unique_game_ids = sorted(df_long["game_id"].unique().tolist())
        print(
            f"[mlb-slate-output] fetching metadata for games={len(unique_game_ids)} players={len(unique_player_ids)}"
        )
        with _get_db_conn() as conn:
            games = _fetch_games(conn, unique_game_ids)
            players = _fetch_players(conn, unique_player_ids)

        if games.empty:
            raise RuntimeError("no matching rows in mlb.game_info for prediction game_ids")

        merged = df_long.merge(games, on="game_id", how="left", suffixes=("", "_db"))
        for c in ("game_date", "game_type", "home_team_code", "away_team_code"):
            db_col = f"{c}_db"
            if db_col in merged.columns:
                if c in merged.columns:
                    merged[c] = merged[c].where(merged[c].notna(), merged[db_col])
                else:
                    merged[c] = merged[db_col]
                merged = merged.drop(columns=[db_col])

    merged = merged.dropna(subset=["game_date", "home_team_code", "away_team_code"])

    if "player_name" in merged.columns:
        # Keep prediction-provided name when present; backfill from player_ids.
        pred_name = merged["player_name"].copy()
        merged = merged.drop(columns=["player_name"])
        merged = merged.merge(players, on="player_id", how="left")
        merged["player_name"] = pred_name.where(pred_name.notna() & (pred_name.astype(str).str.len() > 0), merged["player_name"])
    else:
        merged = merged.merge(players, on="player_id", how="left")

    if merged.empty:
        raise RuntimeError("no rows remain after joining game metadata")
    return merged


def build_slate_output(
    *,
    df_long: pd.DataFrame,
    slate_date: str,
    strict: bool,
    drop_line_0_5: bool,
    market_map: Dict[str, str],
    pred_csv_path: Path,
) -> pd.DataFrame:
    merged = _enrich_with_db(df_long)

    merged["game_date"] = pd.to_datetime(merged["game_date"]).dt.date
    target_date = pd.to_datetime(slate_date).date()
    dates_present = sorted({d.isoformat() for d in merged["game_date"].dropna().tolist()})
    print(f"[mlb-slate-output] dates present after join: {dates_present}")

    before = len(merged)
    merged = merged[merged["game_date"] == target_date]
    after = len(merged)
    print(f"[mlb-slate-output] rows after slate filter ({slate_date}): {after}")
    if after == 0:
        raise RuntimeError(f"zero rows for slate_date={slate_date}; dates_present={dates_present}")
    if after < before:
        msg = f"filtered out {before - after} rows not on slate_date={slate_date}"
        if strict:
            raise RuntimeError(msg)
        print(f"[mlb-slate-output] WARNING: {msg}")

    if drop_line_0_5:
        before_map = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        merged = merged[merged["line"] != 0.5]
        after_map = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        print(f"[mlb-slate-output] dropped line 0.5: before={before_map} after={after_map}")

    rows: List[Dict[str, object]] = []
    generated_at_utc = datetime.now(timezone.utc).isoformat()

    present_props = sorted({_canonical_prop_type(x) for x in merged["prop_type"].tolist() if str(x or "").strip()})
    unmapped = [p for p in present_props if p not in market_map]
    if unmapped:
        raise RuntimeError(
            "unmapped prop_type(s): " + ", ".join(unmapped) +
            " (add mappings via --market-map-json / MLB_BOOK_UPLOAD_MARKET_MAP_JSON)"
        )

    for _, row in merged.iterrows():
        p_over = float(row["prob_over"])
        if not (0.0 < p_over < 1.0):
            continue
        p_under = 1.0 - p_over
        odds_over = _prob_to_fair_american(p_over)
        odds_under = _prob_to_fair_american(p_under)
        if odds_over is None or odds_under is None:
            continue

        prop_type = _canonical_prop_type(row["prop_type"])
        pick_side = "over" if p_over >= 0.5 else "under"
        pick_prob = p_over if pick_side == "over" else p_under

        rows.append(
            {
                "league": "MLB",
                "slate_date": str(slate_date),
                "game_date": pd.to_datetime(row["game_date"]).strftime("%Y-%m-%d"),
                "game_id": int(row["game_id"]),
                "game_type": _clean_optional_str(row.get("game_type")),
                "home_team_code": str(row["home_team_code"]).strip(),
                "away_team_code": str(row["away_team_code"]).strip(),
                "player_id": int(row["player_id"]),
                "player_name": _clean_optional_str(row.get("player_name")),
                "prop_type": prop_type,
                "market_key": market_map[prop_type],
                "line": float(row["line"]),
                "prob_over": round(p_over, 6),
                "prob_under": round(p_under, 6),
                "fair_odds_over_american": int(odds_over),
                "fair_odds_under_american": int(odds_under),
                "model_pick_side": pick_side,
                "model_pick_prob": round(float(pick_prob), 6),
                "prediction_source_file": str(pred_csv_path),
                "generated_at_utc": generated_at_utc,
            }
        )

    if not rows:
        raise RuntimeError("no output rows generated")

    out = pd.DataFrame(rows)
    out = out.sort_values(
        by=["game_date", "game_id", "player_name", "player_id", "prop_type", "line"],
        kind="stable",
    ).reset_index(drop=True)
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse
    import pytz

    ap = argparse.ArgumentParser(description="Build canonical MLB slate output CSV from wide predictions.")
    ap.add_argument("--slate-date", default=None, help="YYYY-MM-DD (ET). Defaults to SLATE_DATE or ET today.")
    ap.add_argument("--pred-csv", default=os.environ.get("MLB_PRED_CSV", str(DEFAULT_PRED_CSV)))
    ap.add_argument("--out-csv", default=os.environ.get("MLB_SLATE_OUTPUT_CSV", str(DEFAULT_OUT_CSV)))
    ap.add_argument("--strict", action="store_true", help="Fail if source includes rows from non-slate dates.")
    ap.add_argument("--prop-type", default=os.environ.get("MLB_SLATE_PROP_TYPE", ""), help="Fallback prop_type when wide CSV omits prop_type column.")
    ap.add_argument("--market-map-json", default="", help="Optional JSON object prop_type->market_key overrides.")
    ap.add_argument("--drop-line-0-5", action="store_true", help="Drop line 0.5 rows (default keeps them).")
    args = ap.parse_args(list(argv) if argv is not None else None)

    et = pytz.timezone("America/New_York")
    et_today = datetime.now(et).strftime("%Y-%m-%d")
    slate_date = (args.slate_date or os.environ.get("SLATE_DATE") or et_today).strip()
    pred_csv = Path(str(args.pred_csv)).expanduser()
    out_csv = Path(str(args.out_csv)).expanduser()
    prop_type_arg = _canonical_prop_type(args.prop_type)
    market_map = _load_market_map(
        arg_json=str(args.market_map_json),
        env_json=str(os.environ.get("MLB_BOOK_UPLOAD_MARKET_MAP_JSON", "")),
    )

    print(f"[mlb-slate-output] slate_date (ET) = {slate_date}")
    print(f"[mlb-slate-output] pred_csv = {pred_csv}")
    print(f"[mlb-slate-output] out_csv = {out_csv}")

    try:
        df_wide = _load_predictions(pred_csv)
        df_long = _melt_to_long(df_wide, prop_type_arg)
        out = build_slate_output(
            df_long=df_long,
            slate_date=slate_date,
            strict=bool(args.strict),
            drop_line_0_5=bool(args.drop_line_0_5),
            market_map=market_map,
            pred_csv_path=pred_csv,
        )
    except Exception as exc:
        print(f"[mlb-slate-output] ERROR: {exc}", file=sys.stderr)
        return 1

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    print(f"[mlb-slate-output] wrote {len(out)} rows to {out_csv}")
    print(
        "[mlb-slate-output] prop counts:",
        out["prop_type"].value_counts(dropna=False).sort_index().to_dict(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
