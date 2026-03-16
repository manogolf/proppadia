#!/usr/bin/env python3
"""
Build SOG CSV with market columns.

Inputs
------
--pred       backend/nhl/data/processed/sog_predictions_wide_calibrated.csv
--names backend/nhl/exports/names_<SLATE>.csv (recommended; produced by export_names_csv in cli.py)
--odds-json  nhl/site/data/odds_latest.json           (optional but recommended)
--out        nhl/site/data/sog_with_market.csv
--unmatched  nhl/site/data/unmatched_sog.csv

Env
---
SLATE_DATE=YYYY-MM-DD  (required; used to filter by game_date in --names)

Output columns (when available):
full_name, player_id, game_id, team_id, line, p_over,
price_over, p_over_mkt, edge_over, fair_over, game_date
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import re
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import unicodedata
import pandas as pd
import subprocess
from io import StringIO

# ---------- helpers ----------
def die(msg: str, code: int = 3):
    print(f"[sog_with_market] FATAL: {msg}", file=sys.stderr)
    sys.exit(code)

def _coerce_int_series(s, name: str):
    import pandas as pd
    out = pd.to_numeric(s, errors="coerce")
    bad = out.isna().sum()
    if bad:
        # we don't fail here because some pipelines may include header cruft; we do fail later if join fails
        print(f"[sog_with_market] WARN: {name} had {bad}/{len(out)} non-numeric values after coercion")
    return out.astype("Int64")  # pandas nullable int

def _pred_game_dates(pred_csv: Path) -> list[str]:
    """Return sorted unique game_date strings found in sog_predictions.csv (if column exists)."""
    if not pred_csv.exists():
        return []
    try:
        import pandas as pd
        df = pd.read_csv(pred_csv, usecols=lambda c: c in {"game_date"}, dtype={"game_date": "string"})
        if "game_date" not in df.columns:
            return []
        vals = df["game_date"].dropna().astype(str).unique().tolist()
        vals = sorted(set(v.strip() for v in vals if v and v.strip()))
        return vals
    except Exception:
        return []

def _assert_names_join(df_pred, df_names, how="left"):
    # Force key dtypes to match and be numeric
    df_pred = df_pred.copy()
    df_names = df_names.copy()

    df_pred["player_id"] = _coerce_int_series(df_pred["player_id"], "pred.player_id")
    df_pred["game_id"]   = _coerce_int_series(df_pred["game_id"],   "pred.game_id")

    df_names["player_id"] = _coerce_int_series(df_names["player_id"], "names.player_id")
    df_names["game_id"]   = _coerce_int_series(df_names["game_id"],   "names.game_id")

    # Do the merge
    before = len(df_pred)
    df = df_pred.merge(
        df_names[["player_id","game_id","full_name","team_id","team_code","game_date"]],
        on=["player_id","game_id"],
        how=how,
        suffixes=("", "_names"),
    )

    # Fail-fast: if this is broken, do not continue.
    nulls = int(df["full_name"].isna().sum()) if "full_name" in df.columns else before
    joined = before - nulls
    join_rate = joined / max(1, before)

    print(f"[sog_with_market] names join: matched full_name={joined}/{before} ({join_rate:.3%})")

    if join_rate < 0.95:
        # Print small diagnostics that actually help
        pred_keys = df_pred[["player_id","game_id"]].dropna().astype("int64").head(10).to_dict("records")
        names_keys = df_names[["player_id","game_id"]].dropna().astype("int64").head(10).to_dict("records")
        raise AssertionError(
            "Names join failed (<95% full_name matched). "
            "This usually means dtype mismatch or wrong slate names file.\n"
            f"pred dtypes: player_id={df_pred['player_id'].dtype} game_id={df_pred['game_id'].dtype}\n"
            f"names dtypes: player_id={df_names['player_id'].dtype} game_id={df_names['game_id'].dtype}\n"
            f"sample pred keys: {pred_keys}\n"
            f"sample names keys: {names_keys}\n"
        )

    return df

def _norm_name(s) -> str | None:
    """Normalize a player name for matching across sources."""
    # Treat None / empty / pandas NaN as missing
    if s is None:
        return None
    try:
        import pandas as pd
        if pd.isna(s):
            return None
    except Exception:
        pass

    # Coerce non-strings (defensive)
    if not isinstance(s, str):
        s = str(s)

    s = s.strip()
    if not s:
        return None

    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    # keep letters/numbers/spaces only (drop dots, commas, hyphens, apostrophes)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

def _initial_last(full_name: str) -> str | None:
    """
    'Brayden Schenn' -> 'b schenn'
    Returns None if we can't parse at least 2 tokens.
    """
    n = _norm_name(full_name)
    if not n:
        return None
    parts = n.split()
    if len(parts) < 2:
        return None
    first, last = parts[0], parts[-1]
    if not first or not last:
        return None
    return f"{first[0]} {last}"

def _aliases_for_name(name: str | None) -> set[str]:
    """
    Build a set of alias keys for a given name.
    Handles both 'Brayden Schenn' and 'B. Schenn' styles.
    """
    out: set[str] = set()
    n = _norm_name(name)
    if n:
        out.add(n)

    # If name looks like "b schenn" already, also add "b. schenn" normalized variants are same after _norm_name.
    # So nothing extra needed here.

    # If name is a full name, add initial+last alias too
    if name:
        il = _initial_last(name)
        if il:
            out.add(il)

    # If name is already abbreviated like "B. Schenn" -> normalized "b schenn"
    # we can also try to expand it to a "full name" via roster/names map elsewhere
    # (handled by building aliases for full_name from names.csv below)

    return out

def _pick_market_player_name(o: dict) -> str | None:
    """
    Best-effort extraction of the player name from an Odds API outcome object.
    Adjust these keys if your odds JSON uses different fields.
    """
    # common patterns: 'description', 'participant', 'name'
    for k in ("description", "participant", "name"):
        v = o.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def _is_over_outcome(o: dict) -> bool:
    """Odds API outcome can encode Over/Under in different fields."""
    for k in ("name", "description", "label", "type"):
        v = o.get(k)
        if isinstance(v, str) and v.strip().lower() == "over":
            return True
    return False

def _outcome_side(o: dict) -> str | None:
    """
    Return 'over' or 'under' if outcome dict represents that side, else None.
    Checks common OddsAPI fields where the side may appear.
    """
    if not isinstance(o, dict):
        return None

    for k in ("name", "description", "label", "type"):
        v = o.get(k)
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("over", "under"):
                return s

    return None

def _pick_market_player_name(o: dict) -> str | None:
    """
    Robust player name extraction across common Odds API schemas.

    Schema A (common): outcome.name == "Over", outcome.description == "<Player Name>"
    Schema B (also common): outcome.name == "<Player Name>", outcome.description == "Over"
    """
    name = o.get("name")
    desc = o.get("description")
    part = o.get("participant")

    name_s = name.strip() if isinstance(name, str) else ""
    desc_s = desc.strip() if isinstance(desc, str) else ""
    part_s = part.strip() if isinstance(part, str) else ""

    # If name is literally "Over/Under", player is in description/participant
    if name_s.lower() in ("over", "under"):
        return desc_s or part_s or None

    # If description is "Over/Under", player is in name/participant
    if desc_s.lower() in ("over", "under"):
        return name_s or part_s or None

    # Fallbacks
    return desc_s or part_s or (name_s or None)

def short_key(norm: str) -> str:
    """
    Build a name key like 't meier' from either 'timo meier' or 't meier'.
    This makes OddsAPI full names and our scoreboard-style 'T. Meier' align.
    """
    if not isinstance(norm, str):
        return ""
    parts = norm.split()
    if not parts:
        return ""
    last = parts[-1]
    first_initial = parts[0][0]
    return f"{first_initial} {last}"

def american_to_prob(a) -> float:
    try:
        A = float(a)
    except Exception:
        return float("nan")
    if not math.isfinite(A) or A == 0:
        return float("nan")
    return 100.0 / (A + 100.0) if A > 0 else (-A) / ((-A) + 100.0)


def is_reasonable_american_price(a: float) -> bool:
    """Guard against malformed odds values like -1 or -2.5."""
    if not isinstance(a, (int, float)) or not math.isfinite(float(a)):
        return False
    A = float(a)
    if A == 0:
        return False
    return abs(A) >= 100.0


def de_vig_two_way(p_over_raw, p_under_raw) -> tuple[float, float]:
    """
    Return no-vig over/under probabilities from two one-sided implied probs.

    Fallback behavior:
    - if only one side is available, use that side and derive complement.
    - if neither is available, return NaN, NaN.
    """
    over = float(p_over_raw) if isinstance(p_over_raw, (int, float)) and math.isfinite(float(p_over_raw)) else float("nan")
    under = float(p_under_raw) if isinstance(p_under_raw, (int, float)) and math.isfinite(float(p_under_raw)) else float("nan")

    if math.isfinite(over) and math.isfinite(under) and (over + under) > 0:
        denom = over + under
        return over / denom, under / denom

    if math.isfinite(over):
        return over, 1.0 - over
    if math.isfinite(under):
        return 1.0 - under, under
    return float("nan"), float("nan")


def prob_to_american(p) -> str:
    if not (isinstance(p, (int, float)) and 0 < p < 1):
        return ""
    return f"-{round((p / (1 - p)) * 100)}" if p >= 0.5 else f"+{round(((1 - p) / p) * 100)}"

def read_csv_required(path: Path, need_cols: set[str] | None = None) -> pd.DataFrame:
    """
    Strict CSV loader: used for required inputs (names, odds, etc.).
    Missing file or bad shape is a fatal error.
    """
    if not path.exists():
        die(f"missing CSV: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"failed reading CSV {path}: {e}")
    if need_cols:
        miss = [c for c in need_cols if c not in df.columns]
        if miss:
            die(f"{path.name} missing columns: {miss}")
    return df

def _fetch_pred_long_from_db(db_url: str, slate: str, prop: str) -> pd.DataFrame:
    """
    Pull prediction spine from nhl.predictions for a given slate (ET date),
    joining nhl.games to filter on game_date (since nhl.predictions has no game_date).

    Returns a LONG dataframe with shape:
      player_id, game_id, line, p_over, game_date
    """
    sql = f"""
WITH base AS (
  SELECT
    p.player_id::bigint AS player_id,
    p.game_id::bigint   AS game_id,
    p.line::numeric     AS line,
    p.p_over::numeric   AS p_over
  FROM nhl.predictions p
  WHERE p.prop = '{prop}'
),
joined AS (
  SELECT
    b.player_id,
    b.game_id,
    b.line,
    b.p_over,
    g.game_date::date AS game_date
  FROM base b
  JOIN nhl.games g
    ON g.game_id = b.game_id
  WHERE g.game_date::date = DATE '{slate}'
)
SELECT player_id, game_id, line, p_over, game_date
FROM joined
ORDER BY game_id, player_id, line;
"""

    # Use psql so we don't require SQLAlchemy.
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-A", "-F", ",", "-t", "-c", sql]
    try:
        res = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        stdout = (e.stdout or "").strip()
        die(f"DB spine query failed.\nSTDERR:\n{stderr}\nSTDOUT:\n{stdout}")

    txt = (res.stdout or "").strip()
    if not txt:
        die(f"DB spine returned 0 rows for prop={prop} slate={slate}")

    df = pd.read_csv(
        StringIO("player_id,game_id,line,p_over,game_date\n" + txt),
        dtype={"player_id": "Int64", "game_id": "Int64"},
    )

    # Normalize types
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["p_over"] = pd.to_numeric(df["p_over"], errors="coerce")
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date.astype(str)

    # Defensive: ensure we did not accidentally pull wrong slate
    gd = sorted(set(df["game_date"].dropna()))
    if gd and (len(gd) != 1 or gd[0] != slate):
        raise AssertionError(
            f"DB predictions game_date mismatch.\n"
            f"SLATE_DATE={slate}\n"
            f"db game_date uniques={gd[:10]} (n={len(gd)})\n"
        )

    return df

def read_pred_csv_or_exit_quiet(path: Path, need_cols: set[str] | None = None) -> pd.DataFrame:
    """
    Softer CSV loader used only for SOG predictions:
    - If missing or empty → log info and exit(0) (nothing to build for this slate).
    - If present but malformed (parse error / missing cols) → fatal (die()).
    """
    slate = os.environ.get("SLATE_DATE", "").strip()
    if not slate:
        die("SLATE_DATE is required (YYYY-MM-DD). Refusing to build sog_with_market without an explicit slate.")

    if not path.exists():
        if slate:
            print(
                f"[sog_with_market] ℹ️ predictions CSV not found at {path} for SLATE_DATE={slate}; nothing to build."
            )
        else:
            print(
                f"[sog_with_market] ℹ️ predictions CSV not found at {path}; nothing to build."
            )
        sys.exit(0)

    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"failed reading CSV {path}: {e}")

    # ---- stale prediction guard (critical) --------------------------------------
    # If this file is from a different slate, hard-fail rather than silently shipping
    # mixed-date outputs.
    try:
        if "game_date" in df.columns:
            vals = df["game_date"].dropna().astype(str).unique().tolist()
            vals = sorted(set(v.strip() for v in vals if v and v.strip()))
            if vals and slate not in vals:
                die(
                    "predictions CSV appears stale for this slate. "
                    f"SLATE_DATE={slate} pred_path={path} pred_game_dates={vals[:5]}"
                    + (" ..." if len(vals) > 5 else "")
                )
    except Exception as e:
        die(f"failed validating predictions CSV game_date against SLATE_DATE={slate}: {e} ({path})")
# ---------------------------------------------------------------------------

    if df.empty:
        if slate:
            print(
                f"[sog_with_market] ℹ️ predictions CSV has no rows for SLATE_DATE={slate}; nothing to build."
            )
        else:
            print("[sog_with_market] ℹ️ predictions CSV has no rows; nothing to build.")
        sys.exit(0)

    if need_cols:
        miss = [c for c in need_cols if c not in df.columns]
        if miss:
            die(f"{path.name} missing columns: {miss}")

    return df

def _read_predictions_from_db(db_url: str, slate: str, prop: str) -> pd.DataFrame:
    """
    Pull SOG predictions directly from DB for the given slate-day.

    We join nhl.predictions -> nhl.games to filter by slate game_date,
    because nhl.predictions itself does not have game_date.
    """
    sql = f"""
COPY (
  SELECT
    p.player_id,
    p.game_id,
    p.line,
    p.p_over
  FROM nhl.predictions p
  JOIN nhl.games g
    ON g.game_id = p.game_id
  WHERE g.game_date = DATE '{slate}'
    AND p.prop = '{prop}'
) TO STDOUT WITH CSV HEADER;
""".strip()

    try:
        proc = subprocess.run(
            ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql],
            check=True,
            text=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        die(f"DB query failed for predictions spine: {e.stderr.strip() or e.stdout.strip()}")

    out = proc.stdout.strip()
    if not out:
        # No rows is not a hard crash for the site build; treat as “nothing to do”.
        return pd.DataFrame(columns=["player_id", "game_id", "line", "p_over"])

    df = pd.read_csv(StringIO(out))

    # Coerce dtypes (important for joins and line formatting)
    df["player_id"] = _coerce_int_series(df["player_id"], "db.player_id")
    df["game_id"]   = _coerce_int_series(df["game_id"], "db.game_id")
    df["line"]      = pd.to_numeric(df["line"], errors="coerce")
    df["p_over"]    = pd.to_numeric(df["p_over"], errors="coerce")
    return df

def load_odds_json(path: Path | None) -> list | dict | None:
    # Try explicit path, then standard locations if not present
    for p in [path, Path("nhl/site/data/odds_nhl_playerprops_today.json"), Path("nhl/site/data/odds_latest.json")]:
        if p and p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    return None

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()

    # Preferred spine: DB nhl.predictions (prop=shots_on_goal for slate)
    ap.add_argument(
        "--db-url",
        default=None,
        help="DB url to pull nhl.predictions spine (else SUPABASE_DB_URL or DATABASE_URL).",
    )
    ap.add_argument(
        "--prop",
        default="shots_on_goal",
        help="nhl.predictions.prop to use as spine (default: shots_on_goal).",
    )

    # Fallback spine: calibrated wide CSV (legacy)
    ap.add_argument(
        "--pred",
        default="backend/nhl/data/processed/sog_predictions_wide_calibrated.csv",
        help="WIDE predictions CSV fallback (default: sog_predictions_wide_calibrated.csv).",
    )

    ap.add_argument(
        "--names",
        required=True,
        help="backend/nhl/exports/names_<SLATE>.csv (produced by cli.py export_names_csv)",
    )
    ap.add_argument("--odds-json", default="nhl/site/data/odds_latest.json")
    ap.add_argument("--events-json", default="nhl/site/data/events_today.json")
    ap.add_argument("--out", required=True)
    ap.add_argument("--unmatched", required=True)
    ap.add_argument(
        "--slate-date",
        dest="slate_date",
        default=None,
        help="YYYY-MM-DD (ET). If omitted, falls back to SLATE_DATE env.",
    )

    args = ap.parse_args()

    # Ensure pred is always defined even if we take an early/alternate path.
    pred: pd.DataFrame | None = None

    slate = str(args.slate_date or os.environ.get("SLATE_DATE") or "").strip()
    if not slate:
        die("Provide --slate-date YYYY-MM-DD or set SLATE_DATE env (ET).")

    db_url = args.db_url or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    print(f"[sog_with_market] SLATE_DATE (ET) = {slate}")

    names_path = Path(args.names)
    pred_path = Path(args.pred)

    if not names_path.exists() or names_path.stat().st_size == 0:
        raise AssertionError(f"[sog_with_market] expected artifact missing/empty: {names_path}")
    if (not db_url) and (not pred_path.exists() or pred_path.stat().st_size == 0):
        raise AssertionError(f"[sog_with_market] expected artifact missing/empty: {pred_path}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    unmatched_path = Path(args.unmatched)
    unmatched_path.parent.mkdir(parents=True, exist_ok=True)

    # ---------------- helpers local to main ----------------
    def _fmt_line_str(x) -> str:
        """Normalize line to the same canonical string used for joining."""
        if pd.isna(x):
            return ""
        try:
            v = float(x)
        except Exception:
            s = str(x).strip()
            return s
        # 1.0 -> "1", 1.5 -> "1.5"
        s = str(v).rstrip("0").rstrip(".")
        return s

    def _market_keys_to_accept() -> set[str]:
        # Keep existing behavior, but allow alternates if present.
        return {"player_shots_on_goal", "player_shots_on_goal_alternate"}

    # ---------------- load predictions (WIDE expected; accept tall Denali) ----------------
    # --- Spine selection -------------------------------------------------------
    # Prefer DB nhl.predictions when db_url is available; fallback to CSV otherwise.
    pred = None
    pred_long = None

    # Prefer DB nhl.predictions when db_url is available; fallback to CSV otherwise.
    if db_url:
        pred_long = _fetch_pred_long_from_db(db_url=db_url, slate=slate, prop=args.prop)
        if pred_long is None or len(pred_long) == 0:
            die(f"no rows returned from nhl.predictions for prop={args.prop} slate={slate}")
    else:
        pred = read_csv_required(Path(args.pred))
        if pred.empty:
            die("predictions CSV has no rows")
        # continue with your existing CSV parsing/pivot/melt to create pred_long below

    if not db_url:
    
        # Accept both old wide p_over_* format and new tall Denali format
        p_cols = [c for c in pred.columns if c.startswith("p_over_")]
        if not p_cols:
            # New Denali-style tall format expected:
            # columns: player_id, game_id, line, prob_over, model
            required = {"player_id", "game_id", "line", "prob_over"}
            missing_req = required - set(pred.columns)
            if missing_req:
                die(
                    "predictions CSV is neither wide p_over_* nor tall Denali format. "
                    f"Header={list(pred.columns)}, missing required={sorted(missing_req)}"
                )

            # Convert line (e.g. 0.5, 1.5, 2.5) to suffix used in p_over_* names
            def line_to_suffix(x):
                s = str(x).strip()
                # Normalize typical decimal lines like 0.5 -> 0_5
                if s.endswith(".5"):
                    s = s.replace(".5", "_5")
                # Fallback normalization: replace dot with underscore, handle +/- if they ever show up
                s = s.replace(".", "_").replace("+", "p").replace("-", "m")
                return s

            # Build a wide frame: one row per (player_id, game_id), columns p_over_{suffix}
            tmp = pred.copy()
            tmp["line_suffix"] = tmp["line"].apply(line_to_suffix)
            wide = (
                tmp.pivot_table(
                    index=["player_id", "game_id"],
                    columns="line_suffix",
                    values="prob_over",
                    aggfunc="mean",  # if dupes somehow exist, average them
                )
                .reset_index()
            )

            # Flatten the pivoted columns and rename to p_over_*
            wide.columns = [
                (f"p_over_{c}" if c not in ("player_id", "game_id") else c) for c in wide.columns
            ]
            pred = wide
            p_cols = [c for c in pred.columns if c.startswith("p_over_")]
            if not p_cols:
                die(
                    "after pivoting tall Denali predictions, no p_over_* columns were created. "
                    f"Columns={list(pred.columns)}"
                )

        # Extract numeric line from either p_over_0.5 or p_over_0_5
        def col_to_line(c: str) -> float | None:
            # p_over_12.5 or p_over_12_5
            m = re.match(r"^p_over_(\d+(?:\.\d+)?)$", c)
            if m:
                return float(m.group(1))
            m = re.match(r"^p_over_(\d+)_5$", c)
            if m:
                return float(f"{m.group(1)}.5")
            m = re.match(r"^p_over_(\d+)$", c)  # integer line (just in case)
            if m:
                return float(m.group(1))
            return None

        # Melt to long: player_id, game_id, line, p_over
        long_rows = []
        base_cols = [c for c in ["player_id", "game_id", "team_id", "full_name"] if c in pred.columns]
        for c in p_cols:
            ln = col_to_line(c)
            if ln is None:
                continue
            tmp = pred[base_cols + [c]].copy()
            tmp = tmp.rename(columns={c: "p_over"})
            tmp["line"] = ln
            long_rows.append(tmp)

        if not long_rows:
            die(f"could not interpret any p_over_* columns. Header={list(pred.columns)}")

        pred_long = pd.concat(long_rows, ignore_index=True)

    # ---- FAIL-FAST: predictions must match SLATE_DATE if they contain game_date ----
    if "game_date" in pred_long.columns:
        pred_long["game_date"] = pd.to_datetime(pred_long["game_date"], errors="coerce").dt.date.astype(str)
        gd = sorted(set(pred_long["game_date"].dropna()))
        if gd and (len(gd) != 1 or gd[0] != slate):
            raise AssertionError(
                f"Predictions game_date mismatch.\n"
                f"SLATE_DATE={slate}\n"
                f"pred_long game_date uniques={gd[:10]} (n={len(gd)})\n"
                f"This usually means sog_predictions.csv is stale (built for a different slate)."
            )


    # ---------------- load names/export CSV and filter to SLATE_DATE ----------------
    names = read_csv_required(Path(args.names))
    if "game_date" not in names.columns:
        die(f"{args.names} missing game_date column")

    # Restrict to slate
    names["game_date"] = pd.to_datetime(names["game_date"]).dt.date.astype(str)
    date_values = sorted(set(names["game_date"]))
    if slate not in date_values:
        die(f"export does not contain SLATE_DATE={slate}. Found dates: {date_values}")
    names = names[names["game_date"] == slate].copy()

    # Keep only columns we want from names to avoid collisions
    keep_name_cols = [c for c in ["player_id", "game_id", "team_id", "full_name", "game_date"] if c in names.columns]
    names = names[keep_name_cols].drop_duplicates()

    # ---------------- merge predictions + names (fill team_id/full_name if missing in pred) ----------------
    df = pred_long.merge(
        names,
        on=["player_id", "game_id"],
        how="left",
        suffixes=("", "_names"),
    )

    # ---- FAIL-FAST: verify the join by keys, not just full_name ----
    matched_keys = int(df["team_id_names"].notna().sum()) if "team_id_names" in df.columns else int(df["full_name"].notna().sum())
    total_keys = len(df)
    print(f"[sog_with_market] names join keys matched: {matched_keys}/{total_keys} ({matched_keys/max(1,total_keys):.3%})")

    if matched_keys / max(1, total_keys) < 0.95:
        # show a couple keys that didn't match
        miss = df[df.get("team_id_names", df.get("full_name")).isna()][["player_id", "game_id"]].head(10)
        raise AssertionError(
            "AssertionError: No join — wrong date or table (names join <95%).\n"
            f"SLATE_DATE={slate}\n"
            f"pred rows={len(pred_long)} names rows={len(names)}\n"
            f"sample missing keys:\n{miss.to_string(index=False)}"
        )

    # Fill team_id from names if not provided by pred
    if "team_id" in df.columns and "team_id_names" in df.columns:
        # prefer team_id from predictions; fall back to names when null
        df["team_id"] = df["team_id"].fillna(df["team_id_names"])
        df = df.drop(columns=["team_id_names"])
    elif "team_id" not in df.columns and "team_id_names" in df.columns:
        # predictions had no team_id; take it entirely from names
        df["team_id"] = df["team_id_names"]
        df = df.drop(columns=["team_id_names"])

    # Try to fill missing full_name if we got it in pred already
    if "full_name_x" in df.columns and "full_name_y" in df.columns:
        df["full_name"] = df["full_name_x"].fillna(df["full_name_y"])
        df = df.drop(columns=["full_name_x", "full_name_y"])
    elif "full_name" not in df.columns and "full_name_y" in df.columns:
        df["full_name"] = df["full_name_y"]
        df = df.drop(columns=["full_name_y"])

    # After resolving full_name, make sure NaNs don't reach alias normalizer
    if "full_name" in df.columns:
        df["full_name"] = df["full_name"].where(df["full_name"].notna(), None)

    if "full_name" in df.columns:
        n_null = int(pd.isna(df["full_name"]).sum())
        print(f"[sog_with_market] full_name nulls after merge: {n_null}/{len(df)}")

    # Fail-fast: if names didn't join, nothing downstream can match odds
    if "full_name" in df.columns:
        join_rate = 1.0 - (n_null / max(1, len(df)))
        if join_rate < 0.95:
            raise AssertionError(
                "AssertionError: names join failed (<95% full_name present). "
                "This is not an odds problem — it's a broken merge (wrong keys or dtype mismatch)."
            )

    # Normalize line string for odds join (canonical string)
    df["line_str"] = df["line"].map(_fmt_line_str)

    # ---------------- parse odds JSON (FILTERED TO SLATE EVENTS) and build median Over price by (alias_key,line_str) ----------------

    def _load_events_for_slate_ids(events_path: Path, slate_yyyy_mm_dd: str) -> set[str]:
        """
        events_today.json from Odds API is typically a LIST of event dicts.
        Keep only event IDs whose commence_time falls on SLATE_DATE in ET.
        """
        if not events_path or not events_path.exists():
            return set()
        try:
            events_raw = json.loads(events_path.read_text())
        except Exception:
            return set()

        if isinstance(events_raw, dict):
            events = events_raw.get("events", []) or []
        elif isinstance(events_raw, list):
            events = events_raw
        else:
            return set()

        et = ZoneInfo("America/New_York")
        out: set[str] = set()

        for e in events:
            if not isinstance(e, dict):
                continue
            eid = e.get("id")
            ct = e.get("commence_time")
            if not (isinstance(eid, str) and eid and isinstance(ct, str) and ct):
                continue
            try:
                dt_utc = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                dt_et = dt_utc.astimezone(et)
                if dt_et.date().isoformat() == slate_yyyy_mm_dd:
                    out.add(eid)
            except Exception:
                continue

        return out


    odds_raw = load_odds_json(Path(args.odds_json) if args.odds_json else None)
    med_prices = None

    # NEW: restrict odds to the slate's events
    events_ids = _load_events_for_slate_ids(Path(args.events_json), slate)
    if odds_raw is not None and events_ids:
        if isinstance(odds_raw, list):
            odds_raw = [e for e in odds_raw if isinstance(e, dict) and e.get("id") in events_ids]
        elif isinstance(odds_raw, dict) and isinstance(odds_raw.get("events"), list):
            odds_raw["events"] = [e for e in odds_raw["events"] if isinstance(e, dict) and e.get("id") in events_ids]

    print(f"[sog_with_market] events_ids for slate={slate}: {len(events_ids)}")
    if odds_raw is not None:
        top = odds_raw if isinstance(odds_raw, list) else odds_raw.get("events", [])
        print(f"[sog_with_market] odds events after filter: {len(top)}")

    if odds_raw is not None:
        recs: list[dict] = []
        accept_keys = _market_keys_to_accept()
        dropped_bad_price = 0

        seen_keys: set[str] = set()

        def walk(x):
            if isinstance(x, dict):
                k = x.get("key")
                if k in accept_keys:
                    seen_keys.add(k)
                    for o in x.get("outcomes", []) or []:
                        # We want BOTH sides now
                        side = _outcome_side(o)  # expects "over" / "under" or None
                        if side not in ("over", "under"):
                            continue

                        base_name = _pick_market_player_name(o) or ""
                        alias_keys = _aliases_for_name(base_name)  # full + initial-last

                        pt = o.get("point")
                        pr = o.get("price")

                        if pt is not None and pr is not None:
                            try:
                                price = float(pr)
                            except Exception:
                                continue
                            if not is_reasonable_american_price(price):
                                dropped_bad_price += 1
                                continue
                            implied_prob = american_to_prob(price)
                            if not math.isfinite(implied_prob):
                                continue
                            for alias_key in alias_keys:
                                if alias_key:
                                    recs.append(
                                        {
                                            "alias_key": alias_key,
                                            "line_str": _fmt_line_str(pt),
                                            "side": side,
                                            "price": price,
                                            "implied_prob": implied_prob,
                                        }
                                    )

                for v in x.values():
                    walk(v)

            elif isinstance(x, list):
                for it in x:
                    walk(it)

        walk(odds_raw)
        print(f"[sog_with_market] odds recs (over+under outcomes) built: {len(recs)}")
        if dropped_bad_price:
            print(f"[sog_with_market] dropped malformed american prices: {dropped_bad_price}")

        print(f"[sog_with_market] markets seen in odds (filtered to slate): {sorted(seen_keys)}")

        if recs:
            od = pd.DataFrame(recs)

            # Normalize/validate side once (before any pivot/groupby)
            if "side" in od.columns:
                od["side"] = od["side"].astype(str).str.strip().str.lower()
                od = od[od["side"].isin(["over", "under"])]

            # Expect recs rows like: alias_key, line_str, side ('over'/'under'),
            # price (american), implied_prob (side implied probability).
            if "side" in od.columns:
                med_price = (
                    od.pivot_table(
                        index=["alias_key", "line_str"],
                        columns="side",
                        values="price",
                        aggfunc="median",
                    )
                    .reset_index()
                    .rename(columns={"over": "price_over", "under": "price_under"})
                )
                med_prob = (
                    od.pivot_table(
                        index=["alias_key", "line_str"],
                        columns="side",
                        values="implied_prob",
                        aggfunc="median",
                    )
                    .reset_index()
                    .rename(columns={"over": "p_over_mkt_raw", "under": "p_under_mkt_raw"})
                )
                med_prices = med_price.merge(med_prob, on=["alias_key", "line_str"], how="outer")

                # guarantee columns exist even if pivot didn't create one of them
                if "price_over" not in med_prices.columns:
                    med_prices["price_over"] = pd.NA
                if "price_under" not in med_prices.columns:
                    med_prices["price_under"] = pd.NA
                if "p_over_mkt_raw" not in med_prices.columns:
                    med_prices["p_over_mkt_raw"] = pd.NA
                if "p_under_mkt_raw" not in med_prices.columns:
                    med_prices["p_under_mkt_raw"] = pd.NA

            else:
                # Backward compat: if recs only captured "over" rows historically
                med_prices = (
                    od.groupby(["alias_key", "line_str"], as_index=False)
                    .agg(
                        price_over=("price", "median"),
                        p_over_mkt_raw=("implied_prob", "median"),
                    )
                )
                med_prices["price_under"] = pd.NA
                med_prices["p_under_mkt_raw"] = pd.NA
        else:
            med_prices = None

    # ---------------- alias expansion (fixes "B. Schenn" <-> "Brayden Schenn") ----------------
    # Expand each prediction row into multiple alias rows derived from the canonical names export full_name.
    # Then join odds by alias_key+line_str, and collapse back to original row using median price_over/price_under.

    def _row_aliases(full_name: str | None) -> list[str]:
        if not full_name:
            return []
        return sorted(_aliases_for_name(full_name))

    # Ensure df has a stable row identity BEFORE copying/exploding
    if "_row_id" in df.columns:
        df = df.drop(columns=["_row_id"])
    df["_row_id"] = range(len(df))

    df_alias = df.copy()

    if "full_name_canonical" in df_alias.columns:
        full = df_alias["full_name_canonical"].fillna(df_alias.get("full_name"))
    else:
        full = df_alias.get("full_name")
    if full is None:
        full = pd.Series([None] * len(df_alias))

    df_alias["alias_key"] = full.map(_row_aliases)

    # explode alias list; keep rows even if aliases empty (will become NaN alias_key after explode)
    df_alias = df_alias.explode("alias_key", ignore_index=True)

    # Join odds median prices using alias_key + line_str
    if med_prices is not None:
        df_alias = df_alias.merge(med_prices, on=["alias_key", "line_str"], how="left")
    else:
        # still create the columns so downstream code never KeyErrors
        df_alias["price_over"] = pd.NA
        df_alias["price_under"] = pd.NA
        df_alias["p_over_mkt_raw"] = pd.NA
        df_alias["p_under_mkt_raw"] = pd.NA

    # Collapse exploded aliases back to original rows by taking median price across aliases
    def _median_or_na(s: pd.Series):
        s = pd.to_numeric(s, errors="coerce").dropna()
        return float(s.median()) if len(s) else pd.NA

    price_med = (
        df_alias.groupby("_row_id", as_index=False)
        .agg(
            price_over=("price_over", _median_or_na),
            price_under=("price_under", _median_or_na),
            p_over_mkt_raw=("p_over_mkt_raw", _median_or_na),
            p_under_mkt_raw=("p_under_mkt_raw", _median_or_na),
        )
    )

    # Attach the prices back onto df (original, un-exploded row shape)
    df = df.merge(price_med, on="_row_id", how="left")
    df = df.drop(columns=["_row_id"])

    # ---------------- compute p_over_mkt / edge / fair ----------------
    # model probability
    df["p_over"] = df["p_over"].astype(float)

    print("[debug] lines_present:", sorted(df["line"].dropna().unique().tolist())[:20])
    print("[debug] by-line p_over min/max:")
    for L in sorted(df["line"].dropna().unique().tolist()):
        s = df.loc[df["line"] == L, "p_over"]
        print(" ", L, "min", float(pd.to_numeric(s, errors="coerce").min()), "max", float(pd.to_numeric(s, errors="coerce").max()))

    if "price_over" not in df.columns:
        raise AssertionError("price_over missing — odds join path is broken")

    # market probability:
    # - raw one-sided implied probabilities from each side (already aggregated in prob-space)
    # - no-vig two-way normalized probabilities when both sides exist
    if "p_over_mkt_raw" not in df.columns:
        df["p_over_mkt_raw"] = df["price_over"].map(american_to_prob)
    if "p_under_mkt_raw" not in df.columns:
        df["p_under_mkt_raw"] = df["price_under"].map(american_to_prob) if "price_under" in df.columns else pd.NA

    novig = [
        de_vig_two_way(po, pu)
        for po, pu in zip(df["p_over_mkt_raw"], df["p_under_mkt_raw"])
    ]
    df["p_over_mkt_novig"] = [x[0] for x in novig]
    df["p_under_mkt_novig"] = [x[1] for x in novig]
    # Keep legacy column name; downstream selectors read p_over_mkt.
    df["p_over_mkt"] = df["p_over_mkt_novig"]

    def edge(a, b):
        if (
            isinstance(a, float)
            and isinstance(b, float)
            and math.isfinite(a)
            and math.isfinite(b)
        ):
            return a - b
        return float("nan")

    df["edge_over"] = [edge(a, b) for a, b in zip(df["p_over"], df["p_over_mkt"])]
    df["fair_over"] = df["p_over"].map(prob_to_american)

    # If market probability is unavailable, keep edge/fair blank.
    df.loc[pd.to_numeric(df["p_over_mkt"], errors="coerce").isna(), ["p_over_mkt", "edge_over", "fair_over"]] = pd.NA

    # ---------------- UNDER side ----------------
    df["p_under"] = 1.0 - df["p_over"]
    df["p_under_mkt"] = df["p_under_mkt_novig"]
    df["edge_under"] = [edge(a, b) for a, b in zip(df["p_under"], df["p_under_mkt"])]
    df["fair_under"] = df["p_under"].map(prob_to_american)

    # If market probability is unavailable, blank under-derived fields too.
    df.loc[pd.to_numeric(df["p_under_mkt"], errors="coerce").isna(), ["p_under_mkt", "edge_under", "fair_under"]] = pd.NA

    both_sides = pd.to_numeric(df["p_over_mkt_raw"], errors="coerce").notna() & pd.to_numeric(
        df["p_under_mkt_raw"], errors="coerce"
    ).notna()
    print(
        f"[sog_with_market] no-vig pairs available: {int(both_sides.sum())}/{len(df)} "
        f"({(float(both_sides.mean()) * 100.0 if len(df) else 0.0):.1f}%)"
    )

    # ---------------- unmatched report (no price) ----------------
    unmatched = df[pd.to_numeric(df["p_over_mkt"], errors="coerce").isna()].copy()
    unmatched_cols = [c for c in ["full_name", "player_id", "game_id", "team_id", "line", "p_over", "game_date"] if c in df.columns]
    unmatched[unmatched_cols].to_csv(unmatched_path, index=False)

    # ---------------- final select & write ----------------
    out_cols = [
        c
        for c in [
            "full_name",
            "player_id",
            "game_id",
            "team_id",
            "line",
            "p_over",
            "price_over",
            "price_under",
            "p_over_mkt_raw",
            "p_under_mkt_raw",
            "p_over_mkt_novig",
            "p_under_mkt_novig",
            "p_over_mkt",
            "p_under_mkt",
            "edge_over",
            "edge_under",
            "fair_over",
            "fair_under",
            "game_date",
        ]
        if c in df.columns
    ]
    df[out_cols].to_csv(out_path, index=False)

    kept = len(df)
    matched = kept - len(unmatched)
    lines_present = sorted(set(df["line"].dropna().tolist()))

    print(f"[sog_with_market] SLATE_DATE (ET) = {slate}")
    print(f"[sog_with_market] rows: {kept} | matched price: {matched}/{kept}")
    print(f"[sog_with_market] Lines present: {lines_present}")
    print(f"[sog_with_market] ✅ Wrote: {out_path}")
    print(f"[sog_with_market] Wrote unmatched to: {unmatched_path} rows={len(unmatched)}")

if __name__ == "__main__":
    main()
