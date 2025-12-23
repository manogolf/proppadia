#!/usr/bin/env python3
"""
Build SOG CSV with market columns.

Inputs
------
--pred       backend/nhl/data/processed/sog_predictions.csv
--names      exports/train_nhl_sog_v2.csv              (has full_name, team_id, game_date, etc.)
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
import argparse, json, math, os, sys, re
from pathlib import Path
import pandas as pd
import unicodedata as ud
import re
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

# ---------- helpers ----------
def die(msg: str, code: int = 3):
    print(f"[sog_with_market] FATAL: {msg}", file=sys.stderr)
    sys.exit(code)

def _norm_name(s: str | None) -> str | None:
    """Normalize a player name for matching across sources."""
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

def read_pred_csv_or_exit_quiet(path: Path, need_cols: set[str] | None = None) -> pd.DataFrame:
    """
    Softer CSV loader used only for SOG predictions:
    - If missing or empty → log info and exit(0) (nothing to build for this slate).
    - If present but malformed (parse error / missing cols) → fatal (die()).
    """
    slate = os.environ.get("SLATE_DATE", "")

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
    ap.add_argument("--pred", required=True)
    ap.add_argument("--names", required=True)
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

    slate = args.slate_date or os.environ.get("SLATE_DATE")
    if not slate:
        die("Provide --slate-date YYYY-MM-DD or set SLATE_DATE env (ET).")

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
    pred = read_csv_required(Path(args.pred))
    if pred.empty:
        die("predictions CSV has no rows")

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

        seen_keys: set[str] = set()

        def walk(x):
            if isinstance(x, dict):
                k = x.get("key")
                if k in accept_keys:
                    seen_keys.add(k)
                    for o in x.get("outcomes", []) or []:
                        if o.get("name") != "Over":
                            continue

                        base_name = _pick_market_player_name(o) or ""
                        alias_keys = _aliases_for_name(base_name)  # full + initial-last

                        pt = o.get("point")
                        pr = o.get("price")

                        if pt is not None and pr is not None:
                            for alias_key in alias_keys:
                                if alias_key:
                                    recs.append(
                                        {
                                            "alias_key": alias_key,
                                            "line_str": _fmt_line_str(pt),
                                            "price": float(pr),
                                        }
                                    )

                for v in x.values():
                    walk(v)

            elif isinstance(x, list):
                for it in x:
                    walk(it)

        walk(odds_raw)

        print(f"[sog_with_market] markets seen in odds (filtered to slate): {sorted(seen_keys)}")

        if recs:
            od = pd.DataFrame(recs)
            med_prices = (
                od.groupby(["alias_key", "line_str"], as_index=False)
                .agg(price_over=("price", "median"))
            )

    # ---------------- alias expansion (fixes "B. Schenn" <-> "Brayden Schenn") ----------------
    # Expand each prediction row into multiple alias rows derived from the canonical names export full_name.
    # Then join odds by alias_key+line_str, and collapse back to original row using median price_over.
    def _row_aliases(full_name: str | None) -> list[str]:
        if not full_name:
            return []
        return sorted(_aliases_for_name(full_name))

    df_alias = df.copy()
    df_alias["alias_key"] = df_alias.get("full_name", pd.Series([None] * len(df_alias))).map(lambda s: _row_aliases(s))

    # explode alias list; keep rows even if aliases empty (will become NaN alias_key after explode)
    df_alias = df_alias.explode("alias_key", ignore_index=True)

    # If alias_key is missing, we can't match; keep as NA
    # Join odds median prices using alias_key + line_str
    if med_prices is not None:
        df_alias = df_alias.merge(med_prices, on=["alias_key", "line_str"], how="left")
    else:
        df_alias["price_over"] = pd.NA

    # Collapse back to one row per prediction (player_id, game_id, line)
    # Use median over any matched alias prices for robustness.
    id_cols = ["player_id", "game_id", "line"]
    # Keep stable descriptive cols if present
    keep_first_cols = [c for c in ["full_name", "team_id", "p_over", "game_date", "line_str"] if c in df_alias.columns]

    def _median_or_na(s: pd.Series):
        s2 = pd.to_numeric(s, errors="coerce")
        s2 = s2[~s2.isna()]
        if len(s2) == 0:
            return pd.NA
        return float(s2.median())

    # Build collapsed frame
    grouped = df_alias.groupby(id_cols, as_index=False)

    # Start with id cols + first() of other stable fields
    collapsed = grouped[keep_first_cols].first()

    # Attach price_over median across aliases
    price_med = grouped["price_over"].agg(price_over=_median_or_na)
    df = collapsed.merge(price_med, on=id_cols, how="left")

    # ---------------- compute p_over_mkt / edge / fair ----------------
    # model probability
    df["p_over"] = df["p_over"].astype(float)

    # market probability from price if present
    df["p_over_mkt"] = df["price_over"].map(american_to_prob)

    # edge vs market prob (if we have it)
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

    # fair odds from model prob
    df["fair_over"] = df["p_over"].map(prob_to_american)

    # ---------------- unmatched report (no price) ----------------
    unmatched = df[df["price_over"].isna()].copy()
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
            "p_over_mkt",
            "edge_over",
            "fair_over",
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
