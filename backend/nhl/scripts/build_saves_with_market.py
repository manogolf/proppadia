#!/usr/bin/env python3
"""
Build Goalie SAVES CSV with market columns.

Inputs
------
--pred       backend/nhl/data/processed/saves_predictions.csv   (wide: p_over_18.5,...)
--names      exports/train_goalie_saves_v2.csv                  (has full_name, game_date, ids)
--odds-json  nhl/site/data/odds_latest.json                     (optional but recommended)
--out        nhl/site/data/saves_with_market.csv
--unmatched  nhl/site/data/unmatched_saves.csv

Env
---
SLATE_DATE=YYYY-MM-DD  (required; Eastern Time date)

Output columns:
full_name, player_id, game_id, team_id, line, p_over,
price_over, p_over_mkt, edge_over, fair_over, game_date
"""
from __future__ import annotations
import argparse, json, math, os, sys, re
from pathlib import Path
import pandas as pd
import unicodedata as ud

# -------------------- util --------------------

def die(msg: str, code: int = 2):
    print(f"[saves_with_market] FATAL: {msg}", file=sys.stderr)
    sys.exit(code)

# JS-identical normalizeName():
# - NFD, drop combining marks
# - keep [a-zA-Z0-9\\s'.-], drop others
# - collapse spaces, trim, lowercase
def norm_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = ud.normalize("NFD", s)
    s = "".join(ch for ch in s if ud.category(ch) != "Mn")  # strip accents
    s = s.replace(".", "")
    s = re.sub(r"[^a-zA-Z0-9\s'.-]", "", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def line_key(x) -> str:
    """Canonical text key for a line. Matches the site’s dropdown keys."""
    try:
        v = float(x)
    except Exception:
        return ""
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))        # 24
    return f"{round(v, 1)}"              # 24.5

def american_to_prob(a) -> float:
    try:
        A = float(a)
    except Exception:
        return float("nan")
    if not math.isfinite(A) or A == 0:
        return float("nan")
    return 100.0 / (A + 100.0) if A > 0 else (-A) / ((-A) + 100.0)

def prob_to_american(p) -> str:
    if not (isinstance(p,(int,float)) and 0 < p < 1):
        return ""
    return f"-{round((p/(1-p))*100)}" if p >= 0.5 else f"+{round(((1-p)/p)*100)}"

def read_csv_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        die(f"missing CSV: {path}")
    try:
        return pd.read_csv(path)
    except Exception as e:
        die(f"failed reading CSV {path}: {e}")

def load_odds_json(path: Path | None) -> list | dict | None:
    for p in [path, Path("nhl/site/data/odds_nhl_playerprops_today.json"), Path("nhl/site/data/odds_latest.json")]:
        if p and p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    return None

# -------------------- shaping --------------------

def melt_preds_wide_to_long(pred: pd.DataFrame) -> pd.DataFrame:
    """Expect columns: player_id, game_id, and p_over_XX[.5] columns."""
    need = [c for c in ["player_id","game_id"] if c not in pred.columns]
    if need:
        die(f"pred file missing columns: {need}")

    # detect p_over_* columns
    pat = re.compile(r"^p_over_(\d+(?:[._]\d+)?)$")
    pcols = [c for c in pred.columns if pat.match(str(c))]
    if not pcols:
        die("pred file lacks p_over_* probability columns (e.g., p_over_18_5, p_over_24.5)")

    # melt → long
    long = pred.melt(id_vars=["player_id","game_id"], value_vars=pcols,
                     var_name="pcol", value_name="p_over")

    # extract numeric line from pcol
    def parse_line(s: str) -> float:
        s = s.replace("p_over_", "").replace("_", ".")
        try:
            return float(s)
        except Exception:
            return float("nan")

    long["line"] = long["pcol"].map(parse_line).astype(float)
    long = long.drop(columns=["pcol"])
    # drop rows where p_over is NaN
    long = long[pd.to_numeric(long["p_over"], errors="coerce").notna()].copy()
    return long

def parse_odds_prices(raw) -> pd.DataFrame | None:
    """Return median Over price per (name_norm, line_str) for player_total_saves."""
    if raw is None:
        return None
    recs = []
    def walk(x):
        if isinstance(x, dict):
            if x.get("key") == "player_total_saves":
                for o in x.get("outcomes",[]) or []:
                    if o.get("name") != "Over":
                        continue
                    base_name = (o.get("description") or o.get("player") or "").strip()
                    pt = o.get("point")
                    pr = o.get("price")

                    # Build aliases: full normalized name + "initial last" normalized name
                    aliases = set()
                    nm_full = norm_name(base_name)
                    if nm_full:
                        aliases.add(nm_full)

                    # initial + last (e.g., "Nikita Kucherov" -> "n kucherov")
                    if nm_full:
                        parts = nm_full.split()
                        if len(parts) >= 2:
                            aliases.add(f"{parts[0][0]} {parts[-1]}")

                    if aliases and (pt is not None) and (pr is not None):
                        for nm in aliases:
                            recs.append({"name_norm": nm, "line_str": line_key(pt), "price": float(pr)})
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)
    walk(raw)
    if not recs:
        return None
    od = pd.DataFrame(recs)
    med = (
        od.groupby(["name_norm","line_str"], as_index=False)
          .agg(price_over=("price","median"))
    )
    return med

# -------------------- main --------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True)
    ap.add_argument("--names", required=True)
    ap.add_argument("--odds-json", default="nhl/site/data/odds_latest.json")
    ap.add_argument("--out", required=True)
    ap.add_argument("--unmatched", required=True)
    args = ap.parse_args()

    slate = os.environ.get("SLATE_DATE")
    if not slate:
        die("SLATE_DATE env is required (ET YYYY-MM-DD)")

    out_path = Path(args.out); out_path.parent.mkdir(parents=True, exist_ok=True)
    unmatched_path = Path(args.unmatched); unmatched_path.parent.mkdir(parents=True, exist_ok=True)

    # --- load & reshape predictions ---
    pred_wide = read_csv_required(Path(args.pred))
    long = melt_preds_wide_to_long(pred_wide)   # player_id, game_id, line, p_over

    # --- carry full_name from predictions if present (goalies may not be in names export) ---
    carry_cols = [c for c in ["player_id", "game_id", "full_name", "team_id", "game_date"] if c in pred_wide.columns]
    if "full_name" in carry_cols:
        carry = pred_wide[carry_cols].drop_duplicates(subset=["player_id", "game_id"])
        long = long.merge(carry, on=["player_id", "game_id"], how="left", suffixes=("", "_pred"))


    # --- load names (merge first, filter after) ---
    names = read_csv_required(Path(args.names))
    keep = [c for c in ["player_id","game_id","team_id","full_name","game_date"] if c in names.columns]
    names = names[keep].copy()

    keys = [k for k in ["player_id","game_id"] if k in long.columns and k in names.columns]
    if not keys:
        die("cannot merge names: missing both player_id and game_id in one of the files")
    df = long.merge(names, on=keys, how="left", suffixes=("",""))
    # Prefer full_name from predictions when present; fall back to names export
    if "full_name_x" in df.columns and "full_name_y" in df.columns:
        df["full_name"] = df["full_name_x"].fillna(df["full_name_y"])
        df = df.drop(columns=["full_name_x", "full_name_y"])


    # post-merge filter by SLATE_DATE if game_date exists
    if "game_date" in df.columns:
        df = df[df["game_date"].astype(str) == slate].copy()

    # canonical keys for join with odds
    df["name_norm"] = df.get("full_name", "").map(norm_name)
    df["line_str"]  = df["line"].map(line_key)

    # NEW: alias keys (full + initial-last) to match odds formatting differences
    def aliases_for_name(full_name: str) -> list[str]:
        base = norm_name(full_name)
        if not base:
            return [""]
        parts = base.split()
        if len(parts) >= 2:
            first, last = parts[0], parts[-1]
            initial_last = f"{first[0]} {last}" if first else last
            return list(dict.fromkeys([base, initial_last]))  # preserve order, unique
        return [base]

    df["alias_key"] = df["full_name"].map(lambda x: aliases_for_name(x))
    df = df.explode("alias_key")
    df["alias_key"] = df["alias_key"].astype(str)


    # --- odds (median Over price) ---
    odds_raw = load_odds_json(Path(args.odds_json) if args.odds_json else None)
    med_prices = parse_odds_prices(odds_raw)

    if med_prices is not None:
        # Primary exact string-key merge
        df = df.merge(med_prices, on=["name_norm","line_str"], how="left")

        # Fallback: numeric key rounded to 1 decimal (guards against weird provider float strings)
        if df["price_over"].isna().any():
            mleft = df[df["price_over"].isna()][["name_norm","line"]].copy()
            mleft["line_dec1"] = pd.to_numeric(mleft["line"], errors="coerce").round(1)

            mright = med_prices.copy()
            mright["line_dec1"] = pd.to_numeric(mright["line_str"], errors="coerce").round(1)
            mright = mright[["name_norm","line_dec1","price_over"]].drop_duplicates()

            if not mleft.empty and not mright.empty:
                df = df.merge(
                    mright,
                    left_on=["name_norm", df["line"].round(1)],
                    right_on=["name_norm","line_dec1"],
                    how="left",
                    suffixes=("","_num")
                )
                # prefer exact match if present; else numeric fallback
                df["price_over"] = df["price_over"].where(df["price_over"].notna(), df["price_over_num"])
                df = df.drop(columns=[c for c in ["key_1","line_dec1","price_over_num"] if c in df.columns])
    else:
        df["price_over"] = pd.NA

    # compute market prob, edge, fair odds
    df["p_over"] = pd.to_numeric(df["p_over"], errors="coerce")
    df["p_over_mkt"] = df["price_over"].map(american_to_prob)

    def edge(a, b):
        if isinstance(a, float) and isinstance(b, float) and math.isfinite(a) and math.isfinite(b):
            return a - b
        return float("nan")

    df["edge_over"] = [edge(a,b) for a,b in zip(df["p_over"], df["p_over_mkt"])]
    df["fair_over"] = df["p_over"].map(prob_to_american)

    # unmatched (no price)
    unmatched = df[df["price_over"].isna()].copy()
    unmatched_cols = [c for c in ["full_name","player_id","game_id","team_id","line","p_over","game_date"] if c in df.columns]
    unmatched[unmatched_cols].to_csv(unmatched_path, index=False)

    # final select
    out_cols = [c for c in [
        "full_name","player_id","game_id","team_id",
        "line","p_over","price_over","p_over_mkt","edge_over","fair_over","game_date"
    ] if c in df.columns]
    df[out_cols].to_csv(out_path, index=False)

    # logs
    kept = len(df)
    matched = kept - len(unmatched)
    lines_present = sorted(df["line"].dropna().unique().tolist())
    print(f"[saves_with_market] filter SLATE_DATE={slate}: kept {kept}")
    print(f"[saves_with_market] rows={kept}  matched_prices={matched}/{kept}")
    print(f"[saves_with_market] lines present: {lines_present}")
    print(f"[saves_with_market] ✅ wrote: {out_path}")
    print(f"[saves_with_market]     unmatched: {unmatched_path}  rows={len(unmatched)}")

if __name__ == "__main__":
    main()
