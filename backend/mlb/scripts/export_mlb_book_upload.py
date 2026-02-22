#!/usr/bin/env python3
"""
python backend/mlb/scripts/export_mlb_book_upload.py

MLB equivalent of NHL book-upload exporter:
- Reads calibrated WIDE predictions with p_over_* columns.
- Converts to long rows.
- Joins game metadata from mlb.game_info.
- Filters to slate_date (ET).
- Writes BOTH over and under rows in upload format.

Input expectations:
- Required: player_id, game_id
- Prob columns: p_over_1_5, p_over_2_5, ... (regex: p_over_<int>_<0|5>)
- Prop type:
  - preferred column: prop_type
  - fallback: --prop-type / MLB_BOOK_UPLOAD_PROP_TYPE
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]  # .../backend
PRED_CSV = Path(
    os.environ.get(
        "MLB_PRED_CSV",
        os.environ.get(
            "PRED_CSV",
            str(BASE_DIR / "mlb" / "data" / "processed" / "mlb_predictions_wide_calibrated.csv"),
        ),
    )
)
OUT_CSV = Path(
    os.environ.get(
        "MLB_BOOK_UPLOAD_OUT_CSV",
        os.environ.get(
            "OUT_CSV",
            str(BASE_DIR / "mlb" / "data" / "processed" / "mlb_book_upload.csv"),
        ),
    )
)

# External book-upload taxonomy (provided by operator).
# MARKET is the prop-type carrier in upload rows.
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
    # pitcher_win is yes/no (not over/under) and intentionally excluded here.
}

_PCOL_RE = re.compile(r"^p_over_(\d+)_([05])$")


def _canonical_prop_type(value: object) -> str:
    return str(value or "").strip().lower()


def _parse_lines_from_cols(cols: Iterable[str]) -> List[Tuple[str, float]]:
    out: List[Tuple[str, float]] = []
    for col in cols:
        match = _PCOL_RE.match(col)
        if not match:
            continue
        whole = int(match.group(1))
        half = int(match.group(2))
        line = float(whole) + (0.5 if half == 5 else 0.0)
        out.append((col, line))
    out.sort(key=lambda x: x[1])
    return out


def _get_db_conn():
    import psycopg2  # type: ignore

    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("missing SUPABASE_DB_URL or DATABASE_URL")
    return psycopg2.connect(db_url)


def _fetch_games(conn, game_ids: List[int]) -> pd.DataFrame:
    if not game_ids:
        return pd.DataFrame(columns=["game_id", "game_date", "home_team_code", "away_team_code"])
    sql = """
    SELECT
      game_id::bigint AS game_id,
      game_date::date AS game_date,
      home_team_abbr::text AS home_team_code,
      away_team_abbr::text AS away_team_code
    FROM mlb.game_info
    WHERE game_id = ANY(%s::bigint[])
    """
    return pd.read_sql(sql, conn, params=(list(game_ids),))


def _load_predictions(path: Path) -> pd.DataFrame:
    print(f"[mlb-book-upload] reading predictions from: {path}")
    if not path.exists():
        raise FileNotFoundError(f"missing predictions file: {path}")
    return pd.read_csv(path)


def _melt_to_long(df_wide: pd.DataFrame, default_prop_type: Optional[str]) -> pd.DataFrame:
    for key in ("player_id", "game_id"):
        if key not in df_wide.columns:
            raise ValueError(f"predictions missing required column: {key}")

    col_lines = _parse_lines_from_cols(df_wide.columns)
    if not col_lines:
        raise ValueError("no p_over_* columns found in predictions input")

    prob_cols = [col for col, _ in col_lines]
    print(f"[mlb-book-upload] found probability columns: {prob_cols}")

    use_prop_col = "prop_type" in df_wide.columns
    if not use_prop_col and not default_prop_type:
        raise ValueError("missing prop_type column and no --prop-type provided")

    id_cols = ["player_id", "game_id"] + (["prop_type"] if use_prop_col else [])
    df_long = df_wide[id_cols + prob_cols].melt(
        id_vars=id_cols,
        value_vars=prob_cols,
        var_name="prob_col",
        value_name="prob_over",
    )

    line_map = {col: line for col, line in col_lines}
    df_long["line"] = df_long["prob_col"].map(line_map).astype(float)
    df_long = df_long.drop(columns=["prob_col"])

    if use_prop_col:
        df_long["prop_type"] = df_long["prop_type"].map(_canonical_prop_type)
    else:
        df_long["prop_type"] = _canonical_prop_type(default_prop_type)

    df_long["player_id"] = pd.to_numeric(df_long["player_id"], errors="coerce")
    df_long["game_id"] = pd.to_numeric(df_long["game_id"], errors="coerce")
    df_long["prob_over"] = pd.to_numeric(df_long["prob_over"], errors="coerce")

    df_long = df_long.dropna(subset=["player_id", "game_id", "prob_over", "line"])
    df_long = df_long[df_long["prop_type"].astype(str).str.len() > 0]
    if df_long.empty:
        raise ValueError("no usable prediction rows after melt/cleanup")

    df_long["player_id"] = df_long["player_id"].astype(int)
    df_long["game_id"] = df_long["game_id"].astype(int)
    return df_long


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


def main() -> None:
    import argparse
    from datetime import datetime
    import pytz

    ap = argparse.ArgumentParser()
    ap.add_argument("--slate-date", default=None, help="YYYY-MM-DD (ET). Defaults to SLATE_DATE or ET today.")
    ap.add_argument("--strict", action="store_true", help="Fail if predictions contain non-slate rows.")
    ap.add_argument("--prop-type", default=os.environ.get("MLB_BOOK_UPLOAD_PROP_TYPE", ""))
    ap.add_argument("--market", default="", help="Force one market key for all rows.")
    ap.add_argument("--market-map-json", default="", help="Optional JSON object prop_type->market_key overrides.")
    ap.add_argument("--league", default="MLB")
    ap.add_argument("--section", default="player_prop")
    ap.add_argument(
        "--drop-line-0-5",
        action="store_true",
        help="Drop 0.5 lines (default keeps them for MLB).",
    )
    args = ap.parse_args()

    et = pytz.timezone("America/New_York")
    et_today = datetime.now(et).strftime("%Y-%m-%d")
    slate_date = (args.slate_date or os.environ.get("SLATE_DATE") or et_today).strip()
    prop_type_arg = _canonical_prop_type(args.prop_type)
    market_map = _load_market_map(
        arg_json=str(args.market_map_json),
        env_json=str(os.environ.get("MLB_BOOK_UPLOAD_MARKET_MAP_JSON", "")),
    )

    # Safety: fail fast when any prop_type in source lacks a market mapping.
    # (unless a single explicit --market override is provided).
    if not str(args.market).strip():
        present_props = sorted({_canonical_prop_type(x) for x in df_long["prop_type"].tolist()})
        unmapped = [p for p in present_props if p and p not in market_map]
        if unmapped:
            print(
                "[mlb-book-upload] ERROR: unmapped prop_type(s) found in predictions input: "
                + ", ".join(unmapped),
                file=sys.stderr,
            )
            print(
                "[mlb-book-upload] Add mapping via --market-map-json or MLB_BOOK_UPLOAD_MARKET_MAP_JSON "
                "or filter source to supported prop types.",
                file=sys.stderr,
            )
            sys.exit(1)

    print(f"[mlb-book-upload] slate_date (ET) = {slate_date}")
    print(f"[mlb-book-upload] using PRED_CSV = {PRED_CSV}")
    df_wide = _load_predictions(PRED_CSV)
    df_long = _melt_to_long(df_wide, prop_type_arg)

    unique_game_ids = sorted(df_long["game_id"].unique().tolist())
    print(f"[mlb-book-upload] fetching game metadata for {len(unique_game_ids)} game_ids")

    with _get_db_conn() as conn:
        games = _fetch_games(conn, unique_game_ids)

    if games.empty:
        print("ERROR: no matching rows in mlb.game_info for game_ids in predictions", file=sys.stderr)
        sys.exit(1)

    merged = df_long.merge(games, on="game_id", how="left")
    merged = merged.dropna(subset=["game_date", "home_team_code", "away_team_code"])
    if merged.empty:
        print("ERROR: no rows after joining with mlb.game_info", file=sys.stderr)
        sys.exit(1)

    merged["game_date"] = pd.to_datetime(merged["game_date"]).dt.date
    target_date = pd.to_datetime(slate_date).date()
    dates_present = sorted({d.isoformat() for d in merged["game_date"].dropna().tolist()})

    before = len(merged)
    merged = merged[merged["game_date"] == target_date]
    after = len(merged)
    print(f"[mlb-book-upload] dates present after join: {dates_present}")
    print(f"[mlb-book-upload] merged rows after date filter: {after}")

    if after == 0:
        print(
            f"ERROR: zero rows for slate_date={slate_date}. dates_present={dates_present}",
            file=sys.stderr,
        )
        sys.exit(1)

    if after < before:
        msg = f"filtered out {before - after} rows not on slate_date={slate_date}"
        if args.strict:
            print(f"ERROR: {msg}", file=sys.stderr)
            sys.exit(1)
        print(f"[mlb-book-upload] WARNING: {msg}")

    if args.drop_line_0_5:
        lines_before = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        merged = merged[merged["line"] != 0.5]
        lines_after = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        print(f"[mlb-book-upload] dropped line 0.5: before={lines_before} after={lines_after}")

    rows: List[Dict[str, object]] = []
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
        market = (
            str(args.market).strip()
            or market_map.get(prop_type)
            or f"player-{prop_type.replace('_', '-')}-ou"
        )
        date_str = pd.to_datetime(row["game_date"]).strftime("%Y%m%d")

        base = {
            "LEAGUE": str(args.league).strip() or "MLB",
            "DATE": date_str,
            "HOME": str(row["home_team_code"]).strip(),
            "AWAY": str(row["away_team_code"]).strip(),
            "DOUBLEHEADER": "",
            "SECTION": str(args.section).strip() or "player_prop",
            "MARKET": market,
            "SELECTOR": int(row["player_id"]),
            "POINT": float(row["line"]),
        }
        rows.append({**base, "SIDE": "over", "WIN %": int(odds_over)})
        rows.append({**base, "SIDE": "under", "WIN %": int(odds_under)})

    if not rows:
        print("ERROR: no output rows generated", file=sys.stderr)
        sys.exit(1)

    out_df = pd.DataFrame(rows)
    expected = 2 * len(merged)
    if len(out_df) != expected:
        raise AssertionError(f"unexpected row count: wrote {len(out_df)} expected {expected}")
    bad_sides = sorted(set(out_df["SIDE"].dropna().unique()) - {"over", "under"})
    if bad_sides:
        raise AssertionError(f"invalid SIDE values: {bad_sides}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_CSV, index=False)
    print(f"[mlb-book-upload] wrote {len(out_df)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
