#!/usr/bin/env python3
"""
Export selected NHL SOG candidates to book-upload format.

Usage:
  set -a && source backend/.env && set +a
  python backend/nhl/scripts/export_sog_candidate_book_upload.py \
    --candidates-csv tmp/cards/nhl_sog_card_2026-03-05.csv \
    --out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv

Input CSV is expected to come from:
  backend/nhl/scripts/select_sog_candidates_live.py

Output columns match the book-upload schema:
  LEAGUE, DATE, HOME, AWAY, DOUBLEHEADER, SECTION, MARKET, SELECTOR, POINT, SIDE, WIN %
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from backend.nhl.eightrain_adapter import (
    UPLOAD_COLUMNS, fair_american, load_catalogs, normalize_player_name, resolve_catalog_dir,
    validate_upload,
)


def get_db_conn():
    import psycopg2  # type: ignore

    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL (preferred) or DATABASE_URL in env.")
    return psycopg2.connect(db_url)


def fetch_games(conn, game_ids: list[int]) -> pd.DataFrame:
    if not game_ids:
        return pd.DataFrame(columns=["game_id", "game_date", "home_team_code", "away_team_code"])
    sql = """
    SELECT
      game_id::bigint AS game_id,
      game_date::date AS game_date,
      home_team_code::text AS home_team_code,
      away_team_code::text AS away_team_code
    FROM nhl.games
    WHERE game_id = ANY(%s::bigint[])
    """
    return pd.read_sql(sql, conn, params=(list(game_ids),))


def fetch_player_identities(conn, game_ids: list[int], player_ids: list[int]) -> pd.DataFrame:
    if not game_ids or not player_ids:
        return pd.DataFrame(columns=["game_id", "player_id", "player_name", "team_code"])
    sql = """
    SELECT DISTINCT ON (r.game_id, r.player_id)
      r.game_id::bigint AS game_id, r.player_id::bigint AS player_id,
      p.full_name::text AS player_name, t.team::text AS team_code
    FROM nhl.roster_status r
    JOIN nhl.players p ON p.player_id = r.player_id
    JOIN nhl.teams t ON t.team_id = r.team_id
    WHERE r.game_id = ANY(%s::bigint[]) AND r.player_id = ANY(%s::bigint[])
    ORDER BY r.game_id, r.player_id, r.asof_ts DESC
    """
    return pd.read_sql(sql, conn, params=(list(game_ids), list(player_ids)))


def prob_to_fair_american(p: float) -> Optional[int]:
    if not (0.0 < p < 1.0):
        return None
    if p >= 0.5:
        return int(-round(100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))


def _load_candidates(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing candidates CSV: {path}")
    df = pd.read_csv(path)
    required = ["game_date", "game_id", "player_id", "line", "model_pick", "model_side_prob"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Candidates CSV missing required columns: {missing}")

    df["game_date"] = df["game_date"].astype(str)
    df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["model_pick"] = df["model_pick"].astype(str).str.lower().str.strip()
    df["model_side_prob"] = pd.to_numeric(df["model_side_prob"], errors="coerce")

    df = df.dropna(subset=["game_id", "player_id", "line", "model_pick", "model_side_prob"]).copy()
    df["game_id"] = df["game_id"].astype(int)
    df["player_id"] = df["player_id"].astype(int)
    df = df[df["model_pick"].isin(["over", "under"])].copy()
    df = df[df["model_side_prob"].between(0.0, 1.0, inclusive="neither")].copy()
    return df


def _load_availability(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"availability csv not found: {path}")
    df = pd.read_csv(path)
    required = ["game_date", "game_id", "player_id", "line"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"availability csv missing required columns: {missing}")
    for c in ["game_id", "player_id", "line"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["game_date"] = df["game_date"].astype(str)
    df = df.dropna(subset=["game_date", "game_id", "player_id", "line"]).copy()
    df["game_id"] = df["game_id"].astype(int)
    df["player_id"] = df["player_id"].astype(int)
    return df[["game_date", "game_id", "player_id", "line"]].drop_duplicates().reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Export selected SOG candidates to book-upload CSV.")
    ap.add_argument("--candidates-csv", required=True, help="Output CSV from select_sog_candidates_live.py")
    ap.add_argument(
        "--out-csv",
        default="backend/nhl/data/processed/sog_candidate_book_upload.csv",
        help="Book-upload output path",
    )
    ap.add_argument(
        "--strict-date",
        action="store_true",
        help="Require all candidate rows to share one game_date.",
    )
    ap.add_argument(
        "--max-fair-favorite",
        type=int,
        default=-300,
        help=(
            "Drop rows whose fair odds are more juiced than this favorite threshold "
            "(e.g. -300 drops -301, -500; dogs are unaffected)."
        ),
    )
    ap.add_argument(
        "--skip-fair-odds-cap",
        action="store_true",
        help="Disable fair-odds favorite cap filtering.",
    )
    ap.add_argument(
        "--availability-csv",
        default="nhl/site/data/sog_with_market.csv",
        help=(
            "Optional current availability CSV (game_date,game_id,player_id,line). "
            "Rows not present here are dropped to improve upload match rate."
        ),
    )
    ap.add_argument(
        "--skip-availability-filter",
        action="store_true",
        help="Disable availability filtering.",
    )
    ap.add_argument(
        "--exclude-player-id",
        action="append",
        default=[],
        help="Player ID to drop from output (repeatable). Useful for known unmapped IDs in destination tool.",
    )
    ap.add_argument(
        "--catalog-dir", type=Path,
        default=Path("artifacts/operational/nhl/8rain_catalog/current"),
        help="Retained current model_spec.json, teams.json, and players.json catalog bundle",
    )
    args = ap.parse_args()
    args.catalog_dir = resolve_catalog_dir(args.catalog_dir)

    candidates_csv = Path(args.candidates_csv)
    out_csv = Path(args.out_csv)

    df = _load_candidates(candidates_csv)
    if df.empty:
        raise SystemExit("No usable candidate rows found.")

    game_dates = sorted(df["game_date"].dropna().unique().tolist())
    if args.strict_date and len(game_dates) != 1:
        raise SystemExit(f"--strict-date failed; found multiple dates: {game_dates}")

    # Optional hard drop of known unmapped player IDs.
    excluded_ids = {int(x) for x in (args.exclude_player_id or [])}
    if excluded_ids:
        before = len(df)
        df = df[~df["player_id"].isin(excluded_ids)].copy()
        print(f"[candidate_book_upload] excluded player_ids={sorted(excluded_ids)} dropped={before-len(df)}")
    if df.empty:
        raise SystemExit("No rows remain after player-id exclusions.")

    # Optional availability filter for better upload acceptance near lock.
    if not args.skip_availability_filter:
        avail = _load_availability(Path(args.availability_csv))
        before = len(df)
        df = df.merge(avail, on=["game_date", "game_id", "player_id", "line"], how="inner")
        print(
            f"[candidate_book_upload] availability filter kept={len(df)} dropped={before-len(df)} "
            f"using {args.availability_csv}"
        )
        if df.empty:
            raise SystemExit("No rows remain after availability filter.")

    with get_db_conn() as conn:
        game_ids = sorted(df["game_id"].unique().tolist())
        games = fetch_games(conn, game_ids)
        players = fetch_player_identities(conn, game_ids, sorted(df["player_id"].unique().tolist()))

    if games.empty:
        raise SystemExit("No nhl.games rows found for candidate game_ids.")

    merged = df.merge(games, on="game_id", how="left")
    merged = merged.merge(players, on=["game_id", "player_id"], how="left", validate="many_to_one")
    merged = merged.dropna(subset=["game_date_y", "home_team_code", "away_team_code"]).copy()
    if merged.empty:
        raise SystemExit("No rows remained after joining game metadata.")

    spec, team_map, player_map, allowed_bets = load_catalogs(args.catalog_dir)
    rows: list[dict] = []
    unmapped_players: list[int] = []
    unmapped_teams: set[str] = set()
    dropped_prob = 0
    dropped_fair_odds = 0
    for _, row in merged.iterrows():
        p = float(row["model_side_prob"])
        fair = prob_to_fair_american(p)
        if fair is None:
            dropped_prob += 1
            continue

        if (not args.skip_fair_odds_cap) and fair < 0 and fair < int(args.max_fair_favorite):
            dropped_fair_odds += 1
            continue

        # Guardrail against accidental percent-like leakage.
        if -99 < fair < 99:
            raise SystemExit(
                "Suspicious WIN % odds generated in (-99,99). "
                f"player_id={row['player_id']} game_id={row['game_id']} line={row['line']} p={p}"
            )

        home_abbr = str(row["home_team_code"]).upper()
        away_abbr = str(row["away_team_code"]).upper()
        if home_abbr not in team_map or away_abbr not in team_map:
            unmapped_teams.update(x for x in (home_abbr, away_abbr) if x not in team_map)
            continue
        selector = player_map.get((normalize_player_name(row.get("player_name")),
                                   team_map.get(str(row.get("team_code") or "").upper(), "")))
        if not selector:
            unmapped_players.append(int(row["player_id"]))
            continue
        p_over = float(row["p_over"]) if "p_over" in row and pd.notna(row["p_over"]) else (
            p if str(row["model_pick"]) == "over" else 1.0 - p
        )
        for side, side_prob in (("over", p_over), ("under", 1.0 - p_over)):
            rows.append({
                "LEAGUE": "nhl", "DATE": pd.to_datetime(row["game_date_y"]).strftime("%Y-%m-%d"),
                "HOME": team_map[home_abbr], "AWAY": team_map[away_abbr],
                "DOUBLEHEADER": "0", "SECTION": "player_prop", "MARKET": "shots_on_goal",
                "SELECTOR": selector, "POINT": float(row["line"]), "SIDE": side,
                "WIN %": fair_american(side_prob),
            })

    if not rows:
        raise SystemExit("No output rows generated.")

    out = pd.DataFrame(rows, columns=UPLOAD_COLUMNS)
    team_codes = {str(r.get("code", "")) for r in json.loads((args.catalog_dir / "teams.json").read_text()).get("data", [])}
    player_codes = {str(r.get("code", "")) for r in json.loads((args.catalog_dir / "players.json").read_text()).get("data", []) if r.get("code")}
    validate_upload(out, spec=spec, team_codes=team_codes, player_codes=player_codes)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    print(f"[candidate_book_upload] input rows={len(df)}")
    print(f"[candidate_book_upload] dropped invalid prob rows={dropped_prob}")
    print(
        f"[candidate_book_upload] dropped by fair-odds cap={dropped_fair_odds} "
        f"(max_fair_favorite={int(args.max_fair_favorite)}, skip={bool(args.skip_fair_odds_cap)})"
    )
    print(f"[candidate_book_upload] output rows={len(out)}")
    print(f"[candidate_book_upload] unmapped_player_ids={sorted(set(unmapped_players))}")
    print(f"[candidate_book_upload] unmapped_team_abbreviations={sorted(unmapped_teams)}")
    print(f"[candidate_book_upload] dates={sorted(out['DATE'].unique().tolist())}")
    print(f"[candidate_book_upload] wrote {out_csv}")


if __name__ == "__main__":
    main()
