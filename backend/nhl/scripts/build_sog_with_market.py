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

# ---------- helpers ----------
def die(msg: str, code: int = 3):
    print(f"[sog_with_market] FATAL: {msg}", file=sys.stderr)
    sys.exit(code)

def norm_name(s: str) -> str:
    if not isinstance(s, str): return ""
    s = ud.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.replace("-", " ").replace(".", " ").replace("’","").replace("'", "")
    return " ".join(s.lower().split())

def american_to_prob(a) -> float:
    try:
        A = float(a)
    except Exception:
        return float("nan")
    if not math.isfinite(A) or A == 0: return float("nan")
    return 100.0 / (A + 100.0) if A > 0 else (-A) / ((-A) + 100.0)

def prob_to_american(p) -> str:
    if not (isinstance(p,(int,float)) and 0 < p < 1): return ""
    return f"-{round((p/(1-p))*100)}" if p >= 0.5 else f"+{round(((1-p)/p)*100)}"

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
    ap.add_argument("--out", required=True)
    ap.add_argument("--unmatched", required=True)
    ap.add_argument("--slate-date", dest="slate_date", default=None,
                help="YYYY-MM-DD (ET). If omitted, falls back to SLATE_DATE env.")

    args = ap.parse_args()

    slate = args.slate_date or os.environ.get("SLATE_DATE")
    if not slate:
        die("Provide --slate-date YYYY-MM-DD or set SLATE_DATE env (ET).")
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    unmatched_path = Path(args.unmatched)
    unmatched_path.parent.mkdir(parents=True, exist_ok=True)

    # --- load predictions (WIDE form expected; e.g., p_over_0.5, p_over_1.5, …) ---
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
            # Normalize typical decimal odds like 0.5 -> 0_5
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
            f"p_over_{c}" if c not in ("player_id", "game_id") else c
            for c in wide.columns
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
    base_cols = [c for c in ["player_id","game_id","team_id","full_name"] if c in pred.columns]
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

    # --- load names/export CSV and filter to SLATE_DATE ---
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
    keep_name_cols = [c for c in ["player_id","game_id","team_id","full_name","game_date"] if c in names.columns]
    names = names[keep_name_cols].drop_duplicates()

    # --- merge predictions + names (fill team_id/full_name if missing in pred) ---
    df = pred_long.merge(names, on=["player_id","game_id"], how="left", suffixes=("",""))

    # Fill team_id from names if not provided by pred
    if "team_id_x" in df.columns and "team_id_y" in df.columns:
        df["team_id"] = df["team_id_x"].fillna(df["team_id_y"])
        df = df.drop(columns=["team_id_x","team_id_y"])
    elif "team_id" not in df.columns and "team_id_y" in df.columns:
        df["team_id"] = df["team_id_y"]
        df = df.drop(columns=["team_id_y"])

    # Try to fill missing full_name if we got it in pred already
    if "full_name_x" in df.columns and "full_name_y" in df.columns:
        df["full_name"] = df["full_name_x"].fillna(df["full_name_y"])
        df = df.drop(columns=["full_name_x","full_name_y"])
    elif "full_name" not in df.columns and "full_name_y" in df.columns:
        df["full_name"] = df["full_name_y"]
        df = df.drop(columns=["full_name_y"])

    # Normalize names & line string for odds join
    df["name_norm"] = df.get("full_name","").map(norm_name)
    df["line_str"]  = df["line"].map(lambda x: str(float(x)).rstrip("0").rstrip(".") if pd.notna(x) else "")

    # --- parse odds JSON (player_shots_on_goal) and build median Over price by (name,line) ---
    odds_raw = load_odds_json(Path(args.odds_json) if args.odds_json else None)
    med_prices = None
    if odds_raw is not None:
        recs = []
        def walk(x):
            if isinstance(x, dict):
                if x.get("key") == "player_shots_on_goal":
                    for o in x.get("outcomes",[]) or []:
                        if o.get("name") != "Over":
                            continue
                        nm = norm_name(o.get("description") or o.get("player") or "")
                        pt = o.get("point")
                        pr = o.get("price")
                        if nm and pt is not None and pr is not None:
                            recs.append({"name_norm": nm, "line_str": str(pt), "price": float(pr)})
                for v in x.values(): walk(v)
            elif isinstance(x, list):
                for it in x: walk(it)
        walk(odds_raw)
        if recs:
            od = pd.DataFrame(recs)
            med_prices = (
                od.groupby(["name_norm","line_str"], as_index=False)
                  .agg(price_over=("price","median"))
            )

    # --- attach price_over (median) & compute p_over_mkt / edge / fair ---
    if med_prices is not None:
        df = df.merge(med_prices, on=["name_norm","line_str"], how="left")
    else:
        df["price_over"] = pd.NA

    # model probability
    df["p_over"] = df["p_over"].astype(float)

    # market probability from price if present
    df["p_over_mkt"] = df["price_over"].map(american_to_prob)

    # edge vs market prob (if we have it)
    def edge(a, b):
        if isinstance(a, float) and isinstance(b, float) and math.isfinite(a) and math.isfinite(b):
            return a - b
        return float("nan")
    df["edge_over"] = [edge(a,b) for a,b in zip(df["p_over"], df["p_over_mkt"])]

    # fair odds from model prob
    df["fair_over"] = df["p_over"].map(prob_to_american)

    # --- unmatched report (no price) ---
    unmatched = df[df["price_over"].isna()].copy()
    unmatched_cols = [c for c in ["full_name","player_id","game_id","team_id","line","p_over","game_date"] if c in df.columns]
    unmatched[unmatched_cols].to_csv(unmatched_path, index=False)

    # --- final select & write ---
    out_cols = [c for c in [
        "full_name","player_id","game_id","team_id",
        "line","p_over","price_over","p_over_mkt","edge_over","fair_over","game_date"
    ] if c in df.columns]
    df[out_cols].to_csv(out_path, index=False)

    kept = len(df)
    matched = kept - len(unmatched)
    lines_present = sorted(set(df["line"].dropna().tolist()))
    print(f"[sog_with_market] SLATE_DATE (ET) = {slate}")
    print(f"[sog_with_market] rows: {kept} | matched price: {matched}/{kept}")
    print(f"[sog_with_market] Lines present: {lines_present}")
    print(f"[sog_with_market] ✅ Wrote: {out_path}")
    print(f"[sog_with_market] Wrote unmatched to: {unmatched_path}  rows={len(unmatched)}")

if __name__ == "__main__":
    main()
