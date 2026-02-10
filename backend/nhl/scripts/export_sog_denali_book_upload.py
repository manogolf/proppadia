#!/usr/bin/env python3
"""
python backend/nhl/scripts/export_sog_denali_book_upload.py

Reads calibrated WIDE predictions (p_over_* columns), converts to long rows,
joins nhl.games metadata, filters to slate_date (ET), (optionally) interprets
prob semantics, (optionally) drops line 0.5 (A1), runs diagnostics, and writes
book upload CSV with fair American odds for OVER only.

Key design:
- No duplicate flips.
- No always-on "repairs" (optional via --monotone-repair).
- Diagnostics are always safe to keep.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd


# -----------------------------
# Defaults / paths
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[2]  # .../backend
PRED_CSV = Path(os.environ.get(
    "PRED_CSV",
    str(BASE_DIR / "nhl" / "data" / "processed" / "sog_predictions_wide_calibrated.csv"),
))
OUT_CSV = Path(os.environ.get(
    "OUT_CSV",
    str(BASE_DIR / "nhl" / "data" / "processed" / "sog_denali_book_upload.csv"),
))


# -----------------------------
# DB helpers (same style as you had)
# -----------------------------
def get_db_conn():
    """
    Returns a DB-API connection. Uses SUPABASE_DB_URL or DATABASE_URL.
    (Keeps your existing non-SQLAlchemy usage; warning is acceptable.)
    """
    import psycopg2  # type: ignore

    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL (preferred) or DATABASE_URL in env.")
    return psycopg2.connect(db_url)


def fetch_games(conn, game_ids: List[int]) -> pd.DataFrame:
    """
    Fetch minimal game metadata for these game_ids.
    """
    if not game_ids:
        return pd.DataFrame(columns=["game_id", "game_date", "home_team_code", "away_team_code"])

    # Use ANY(%s) to pass an array via psycopg3
    sql = """
    SELECT
    game_id::bigint AS game_id,
    game_date::date AS game_date,
    home_team_code::text AS home_team_code,
    away_team_code::text AS away_team_code
    FROM nhl.games
    WHERE game_id = ANY(%s::bigint[])
"""

    if not game_ids:
        return pd.DataFrame(columns=["game_id","game_date","home_team_code","away_team_code"])

    return pd.read_sql(sql, conn, params=(list(game_ids),))

# -----------------------------
# Prediction parsing
# -----------------------------
_PCOL_RE = re.compile(r"^p_over_(\d+)_([05])$")  # p_over_1_5, p_over_2_5, etc.


def _extract_lines_from_cols(cols: Iterable[str]) -> List[Tuple[str, float]]:
    """
    Return list of (colname, line_float) for p_over_* columns that match our pattern.
    """
    out: List[Tuple[str, float]] = []
    for c in cols:
        m = _PCOL_RE.match(c)
        if not m:
            continue
        whole = int(m.group(1))
        half = int(m.group(2))
        line = float(whole) + (0.5 if half == 5 else 0.0)
        out.append((c, line))
    out.sort(key=lambda x: x[1])
    return out


def load_predictions(path: Path) -> pd.DataFrame:
    print(f"Using BASE_DIR={BASE_DIR}")
    print(f"Reading calibrated/blended SOG predictions from: {path}")
    if not path.exists():
        raise FileNotFoundError(f"Missing predictions file: {path}")
    return pd.read_csv(path)


def melt_to_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    """
    Converts wide to long with columns: player_id, game_id, line, prob_over
    """
    for k in ["player_id", "game_id"]:
        if k not in df_wide.columns:
            raise ValueError(f"Predictions file missing required column: {k}")

    col_lines = _extract_lines_from_cols(df_wide.columns)
    if not col_lines:
        raise ValueError(
            "No p_over_* columns found. Expected columns like p_over_1_5, p_over_2_5, p_over_3_5."
        )

    prob_cols = [c for c, _ in col_lines]
    print(f"Found blended probability columns: {prob_cols}")

    # Melt then map col -> line
    df_long = df_wide[["player_id", "game_id"] + prob_cols].melt(
        id_vars=["player_id", "game_id"],
        value_vars=prob_cols,
        var_name="prob_col",
        value_name="prob_over",
    )
    map_line = {c: line for c, line in col_lines}
    df_long["line"] = df_long["prob_col"].map(map_line).astype(float)
    df_long = df_long.drop(columns=["prob_col"])

    # Coerce ids
    df_long["player_id"] = pd.to_numeric(df_long["player_id"], errors="coerce").astype("Int64")
    df_long["game_id"] = pd.to_numeric(df_long["game_id"], errors="coerce").astype("Int64")
    df_long = df_long.dropna(subset=["player_id", "game_id", "line", "prob_over"])
    df_long["player_id"] = df_long["player_id"].astype(int)
    df_long["game_id"] = df_long["game_id"].astype(int)
    df_long["prob_over"] = pd.to_numeric(df_long["prob_over"], errors="coerce")
    df_long = df_long.dropna(subset=["prob_over"])

    print(f"Built long-form SOG rows: {len(df_long)} (player, game, line, prob_over)")
    return df_long


# -----------------------------
# Odds conversion
# -----------------------------
def prob_to_fair_american(p: float) -> Optional[int]:
    """
    Converts probability to fair American odds.
    Returns int odds, or None if p invalid.
    """

    if not (0.0 < p < 1.0):
        return None
    # p = implied probability of winning
    if p >= 0.5:
        # negative odds
        odds = -round(100.0 * p / (1.0 - p))
    else:
        # positive odds
        odds = round(100.0 * (1.0 - p) / p)
    return int(odds)


# -----------------------------
# Diagnostics
# -----------------------------
def print_prob_summaries(merged: pd.DataFrame) -> None:
    print(
        "[book_upload] prob_over summary by line:",
        merged.groupby("line")["prob_over"]
        .agg(["min", "median", "max", "mean"])
        .round(4)
        .to_dict(),
    )
    print(
        "[book_upload] prob_over raw min/median/max:",
        float(merged["prob_over"].min()),
        float(merged["prob_over"].median()),
        float(merged["prob_over"].max()),
    )
    n = len(merged)
    n_gt = int((merged["prob_over"] > 0.5).sum())
    print(f"[book_upload] prob_over > 0.5 count: {n_gt}/{n}")
    print("[book_upload] merged rows by line:", merged["line"].value_counts(dropna=False).sort_index().to_dict())


def monotonicity_check(
    merged: pd.DataFrame,
    lines: List[float] = [1.5, 2.5, 3.5],
    semantic: str = "over",
) -> None:
    """
    Diagnostic only: checks monotonicity expectation.

    If semantic == "over":
      P(over 1.5) >= P(over 2.5) >= P(over 3.5)

    If semantic == "under":
      P(under 1.5) <= P(under 2.5) <= P(under 3.5)
      (since under becomes more likely as the line increases)
    """
    semantic = (semantic or "over").strip().lower()
    if semantic not in ("over", "under"):
        print(f"[book_upload] monotonic check skipped; unknown semantic={semantic!r}")
        return

    wide_chk = merged.pivot_table(
        index=["player_id", "game_id"],
        columns="line",
        values="prob_over",
        aggfunc="first",
    ).reset_index()

    missing = [x for x in lines if x not in wide_chk.columns]
    if missing:
        present = [c for c in wide_chk.columns if isinstance(c, (float, int))]
        print(f"[book_upload] monotonic check skipped; missing lines {missing}. Present line cols: {sorted(present)}")
        return

    eps = 1e-12

    if semantic == "over":
        # violations are where smaller line has lower prob than larger line
        v12 = (wide_chk[1.5] + eps < wide_chk[2.5])
        v23 = (wide_chk[2.5] + eps < wide_chk[3.5])
        msg = "P(over) expected 1.5>=2.5>=3.5"
    else:
        # for under, expected ordering is increasing with line
        v12 = (wide_chk[1.5] > wide_chk[2.5] + eps)
        v23 = (wide_chk[2.5] > wide_chk[3.5] + eps)
        msg = "P(under) expected 1.5<=2.5<=3.5"

    nn = len(wide_chk)
    print(f"[book_upload] monotonic check ({msg}): v12={int(v12.sum())}/{nn}, v23={int(v23.sum())}/{nn}")

    if v12.any() or v23.any():
        ex = wide_chk[v12 | v23].head(8)[["player_id", "game_id", 1.5, 2.5, 3.5]]
        print("[book_upload] example violating rows:\n" + ex.to_string(index=False))

def monotone_repair_clip(
    merged: pd.DataFrame,
    lines: List[float] = [1.5, 2.5, 3.5],
    semantic: str = "over",
) -> pd.DataFrame:
    """
    Optional repair to enforce monotonic ordering.

    If semantic == "over": enforce p15 >= p25 >= p35 by clipping downwards.
    If semantic == "under": enforce p15 <= p25 <= p35 by clipping upwards.

    This is a "last resort" transform, so keep it off by default.
    """
    semantic = (semantic or "over").strip().lower()
    if semantic not in ("over", "under"):
        print(f"[book_upload] WARNING: monotone repair skipped; unknown semantic={semantic!r}")
        return merged

    wide = merged.pivot_table(
        index=["player_id", "game_id", "game_date", "home_team_code", "away_team_code"],
        columns="line",
        values="prob_over",
        aggfunc="first",
    ).reset_index()

    missing = [x for x in lines if x not in wide.columns]
    if missing:
        print(f"[book_upload] WARNING: monotone repair skipped; missing lines {missing}")
        return merged

    p15 = wide[1.5].astype(float)
    p25 = wide[2.5].astype(float)
    p35 = wide[3.5].astype(float)

    if semantic == "over":
        p25_fixed = p25.clip(upper=p15)
        p35_fixed = p35.clip(upper=p25_fixed)
    else:
        # under should be non-decreasing with line
        p25_fixed = p25.clip(lower=p15)
        p35_fixed = p35.clip(lower=p25_fixed)

    wide[1.5] = p15
    wide[2.5] = p25_fixed
    wide[3.5] = p35_fixed

    repaired = wide.melt(
        id_vars=["player_id", "game_id", "game_date", "home_team_code", "away_team_code"],
        value_vars=[x for x in lines if x in wide.columns],
        var_name="line",
        value_name="prob_over",
    )
    repaired["line"] = repaired["line"].astype(float)
    repaired["prob_over"] = repaired["prob_over"].astype(float)

    return repaired

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    import argparse
    from datetime import datetime
    import pytz

    ap = argparse.ArgumentParser()
    ap.add_argument("--slate-date", default=None, help="YYYY-MM-DD (ET). Defaults to SLATE_DATE or ET today.")
    ap.add_argument("--strict", action="store_true", help="Fail if predictions contain game_ids not on slate-date.")
    ap.add_argument(
        "--prob-semantic",
        choices=["over", "under"],
        default="over",
        help="Interpret p_over_* columns as P(OVER) or P(UNDER). Default: over.",
    )
    ap.add_argument(
        "--keep-line-0-5",
        action="store_true",
        help="Keep 0.5 SOG line if present (default behavior drops it).",
    )
    ap.add_argument(
        "--monotone-repair",
        choices=["off", "clip"],
        default="off",
        help="Optional repair to enforce p15>=p25>=p35 (default: off).",
    )
    args = ap.parse_args()

    et = pytz.timezone("America/New_York")
    et_today = datetime.now(et).strftime("%Y-%m-%d")
    slate_date = args.slate_date or os.environ.get("SLATE_DATE") or et_today

    print(f"[book_upload] slate_date (ET) = {slate_date}")
    print(f"[book_upload] using PRED_CSV = {PRED_CSV}")

    df_wide = load_predictions(PRED_CSV)

    # We only hard-require the 1.5/2.5/3.5 columns. 0.5 is optional (A1 drop).
    required_cols = ["p_over_1_5", "p_over_2_5", "p_over_3_5"]
    missing = [c for c in required_cols if c not in df_wide.columns]
    if missing:
        print(
            f"ERROR: {PRED_CSV} missing required columns: {missing}\n"
            f"Available p_over_* cols: {[c for c in df_wide.columns if c.startswith('p_over_')]}",
            file=sys.stderr,
        )
        sys.exit(1)

    df_long = melt_to_long(df_wide)

    unique_game_ids = sorted(df_long["game_id"].unique().tolist())
    print(f"Fetching game metadata for {len(unique_game_ids)} unique game_ids...")

    with get_db_conn() as conn:
        games = fetch_games(conn, unique_game_ids)

    if games.empty:
        print("WARNING: No matching rows in nhl.games for these game_ids. Output will be empty.", file=sys.stderr)
        return

    merged = df_long.merge(games, on="game_id", how="left")
    merged = merged.dropna(subset=["game_date", "home_team_code", "away_team_code"])
    if merged.empty:
        print("No rows after joining with nhl.games; nothing to write.")
        return

    # Guardrail: filter to slate date
    merged["game_date"] = pd.to_datetime(merged["game_date"]).dt.date
    target = pd.to_datetime(slate_date).date()

    dates_present = sorted({d.isoformat() for d in merged["game_date"].dropna().unique().tolist()})
    print(f"[book_upload] dates present after join: {dates_present}")

    before = len(merged)
    merged = merged[merged["game_date"] == target]
    after = len(merged)

    print(f"[book_upload] merged rows after date filter: {after}")
    if after == 0:
        msg = (
            f"ERROR: after filtering to slate_date={slate_date}, zero rows remain.\n"
            f"Dates present were: {dates_present}\n"
            f"This usually means {PRED_CSV} is stale or built for a different SLATE_DATE."
        )
        print(msg, file=sys.stderr)
        sys.exit(1)

    if after < before:
        msg = f"[book_upload] WARNING: filtered out {before - after} rows not on slate_date={slate_date} (kept {after})."
        if args.strict:
            print("ERROR: " + msg, file=sys.stderr)
            sys.exit(1)
        print(msg)

    # A1: drop 0.5 by default
    if not args.keep_line_0_5:
        lines_before = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        merged = merged[merged["line"] != 0.5]
        lines_after = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        print(f"[book_upload] dropped line 0.5: lines before={lines_before} after={lines_after}")

    # Prob semantic (exactly once)
    print(f"[book_upload] prob_semantic = {args.prob_semantic}")

    # From this point onward, merged["prob_over"] MUST mean P(OVER).
    semantic_for_checks = "over"

    if args.prob_semantic == "under":
        merged["prob_over"] = 1.0 - merged["prob_over"]
        print("[book_upload] prob_semantic=under → converted to P(OVER) via (1 - p)")

    # Diagnostics (safe; never mutates unless monotone-repair is enabled)
    print_prob_summaries(merged)
    monotonicity_check(merged, semantic=semantic_for_checks)

    if args.monotone_repair == "clip":
        merged = monotone_repair_clip(merged, semantic=semantic_for_checks)
        print("[book_upload] applied monotone repair: clip")
        print_prob_summaries(merged)
        monotonicity_check(merged, semantic=semantic_for_checks)

    # --- build upload rows (OVER only; fair American odds) ---
    rows = []
    first_example_printed = False

    for _, row in merged.iterrows():
        try:
            p_over = float(row["prob_over"])
        except (TypeError, ValueError):
            continue
        if not (0.0 < p_over < 1.0):
            continue

        p_under = 1.0 - p_over

        odds_over = prob_to_fair_american(p_over)
        odds_under = prob_to_fair_american(p_under)
        if odds_over is None or odds_under is None:
            continue

        # Guardrail: prevent percent-like accidental values (e.g., 80.91)
        if not isinstance(odds_over, int):
            odds_over = int(odds_over)
        if -99 < odds_over < 99:
            print(
                f"ERROR: suspicious WIN %={odds_over} (looks like percent/decimal leakage). "
                f"p_over={p_over} player_id={row['player_id']} game_id={row['game_id']} line={row['line']}",
                file=sys.stderr,
            )
            sys.exit(1)

        date_str = pd.to_datetime(row["game_date"]).strftime("%Y%m%d")

        if not first_example_printed:
            print(
                "[book_upload] example prob_over =",
                p_over,
                "fair_american(over) =",
                odds_over,
            )
            first_example_printed = True

        # --- OVER row ---
        rows.append(
            {
                "LEAGUE": "NHL",
                "DATE": date_str,
                "HOME": row["home_team_code"],
                "AWAY": row["away_team_code"],
                "DOUBLEHEADER": "",
                "SECTION": "player_prop",
                "MARKET": "player-shots_onGoal-ou",
                "SELECTOR": int(row["player_id"]),
                "POINT": float(row["line"]),
                "SIDE": "over",
                "WIN %": odds_over,
            }
        )

        # --- UNDER row ---
        rows.append(
            {
                "LEAGUE": "NHL",
                "DATE": date_str,
                "HOME": row["home_team_code"],
                "AWAY": row["away_team_code"],
                "DOUBLEHEADER": "",
                "SECTION": "player_prop",
                "MARKET": "player-shots_onGoal-ou",
                "SELECTOR": int(row["player_id"]),
                "POINT": float(row["line"]),
                "SIDE": "under",
                "WIN %": odds_under,
            }
        )

    print(f"[book_upload] output rows to write: {len(rows)}")

    out_df = pd.DataFrame(rows)

    # --- guards: shape + SIDE sanity ---
    expected = 2 * len(merged)  # we emit BOTH sides for each merged row
    if len(out_df) != expected:
        raise AssertionError(
            f"[book_upload] unexpected row count: wrote {len(out_df)} rows, expected {expected} "
            f"(2x merged rows={len(merged)}). This usually means one side didn't get appended."
        )

    bad_sides = sorted(set(out_df["SIDE"].dropna().unique()) - {"over", "under"})
    if bad_sides:
        raise AssertionError(f"[book_upload] invalid SIDE values found: {bad_sides}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_CSV, index=False)
    print(f"Wrote {len(out_df)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
