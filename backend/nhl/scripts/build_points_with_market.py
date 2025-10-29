#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math, os, re, sys
from pathlib import Path
import pandas as pd
import unicodedata as ud

def die(msg, code=2):
    print(f"[points_with_market] FATAL: {msg}", file=sys.stderr); sys.exit(code)

def norm_name(s:str)->str:
    if not isinstance(s,str): return ""
    s = ud.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.replace("-", " ").replace(".", " ").replace("’","").replace("'","")
    return " ".join(s.lower().split())

def american_to_prob(a)->float:
    try: A=float(a)
    except: return float("nan")
    if not math.isfinite(A) or A==0: return float("nan")
    return 100/(A+100) if A>0 else (-A)/((-A)+100)

def prob_to_american(p)->str:
    if not (0<p<1): return ""
    return f"-{round((p/(1-p))*100)}" if p>=0.5 else f"+{round(((1-p)/p)*100)}"

def read_csv_required(p:Path)->pd.DataFrame:
    if not p.exists(): die(f"missing CSV: {p}")
    try: return pd.read_csv(p)
    except Exception as e: die(f"failed reading CSV {p}: {e}")

def melt_preds(pred:pd.DataFrame)->pd.DataFrame:
    need = [c for c in ("player_id","game_id") if c not in pred.columns]
    if need: die(f"pred file missing columns: {need}")
    pat = re.compile(r"^p_over_(\d+(?:[._]\d+)?)$")
    pcols = [c for c in pred.columns if pat.match(str(c))]
    if not pcols: die("no p_over_* columns found in predictions")
    long = pred.melt(id_vars=["player_id","game_id"], value_vars=pcols,
                     var_name="pcol", value_name="p_over")
    def to_line(s:str)->float:
        s = s.replace("p_over_","").replace("_",".")
        try: return float(s)
        except: return float("nan")
    long["line"] = long["pcol"].map(to_line).astype(float)
    long = long.drop(columns=["pcol"])
    long = long[pd.to_numeric(long["p_over"], errors="coerce").notna()].copy()
    return long

def load_odds_json(path:Path|None):
    for p in [path, Path("nhl/site/data/odds_nhl_playerprops_today.json"), Path("nhl/site/data/odds_latest.json")]:
        if p and p.exists():
            try: return json.loads(p.read_text())
            except Exception as e:
                print(f"[points_with_market] warn: failed to read odds JSON {p}: {e}", file=sys.stderr)
    return None

def parse_points_odds(raw)->pd.DataFrame|None:
    """Return median Over price per (name_norm,line_str) and a representative full_name for display."""
    if raw is None: return None
    recs=[]
    def walk(x):
        if isinstance(x, dict):
            if x.get("key") == "player_points":
                for o in x.get("outcomes",[]) or []:
                    if o.get("name")!="Over": continue
                    disp = (o.get("description") or o.get("player") or "").strip()
                    nm = norm_name(disp)
                    pt = o.get("point"); pr = o.get("price")
                    if nm and (pt is not None) and (pr is not None):
                        recs.append({"name_norm":nm,"full_name":disp,"line_str":str(pt),"price":float(pr)})
            for v in x.values(): walk(v)
        elif isinstance(x, list):
            for it in x: walk(it)
    walk(raw)
    if not recs: return None
    od = pd.DataFrame(recs)
    # median price per player/line, plus a representative full_name (first)
    med = od.groupby(["name_norm","line_str"], as_index=False).agg(
        price_over=("price","median"),
        full_name=("full_name","first"),
    )
    return med

def main():
    ap = argparse.ArgumentParser(description="Build NHL Points site CSV with market (odds-only or predictions merge).")
    ap.add_argument("--pred", help="backend/nhl/data/processed/points_predictions.csv (optional)")
    ap.add_argument("--names", help="exports/train_nhl_points_v2.csv with name/id mapping (optional)")
    ap.add_argument("--odds-json", default="nhl/site/data/odds_latest.json", help="odds json path")
    ap.add_argument("--events-json", default="", help="(ignored; compat flag)")
    ap.add_argument("--out", required=True, help="output CSV for site: nhl/site/data/points_with_market.csv")
    ap.add_argument("--unmatched", required=True, help="output CSV: unmatched rows when merging preds↔odds")
    args = ap.parse_args()

    slate = os.environ.get("SLATE_DATE","").strip()
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    um  = Path(args.unmatched); um.parent.mkdir(parents=True, exist_ok=True)

    # Odds parsing (always)
    odds_raw = load_odds_json(Path(args.odds_json) if args.odds_json else None)
    med = parse_points_odds(odds_raw)

    # Decide mode
    have_preds = bool(args.pred and args.names and Path(args.pred).exists() and Path(args.names).exists())

    if not have_preds:
        # ---- ODDS-ONLY MODE ----
        if med is None or med.empty:
            # still write valid empty file so site code doesn't crash
            pd.DataFrame(columns=[
                "full_name","player_id","game_id","team_id","line",
                "price_over","p_over_mkt","fair_over","game_date"
            ]).to_csv(out, index=False)
            um.write_text("")  # nothing to match
            print(f"[points_with_market] odds-only: no player_points found; wrote empty {out}")
            return

        df = med.copy()
        # columns for site: we won't have ids yet; this still powers the VALUES dropdown per player
        df["line"] = df["line_str"].map(lambda s: float(s))
        df["p_over_mkt"] = df["price_over"].map(american_to_prob)
        df["fair_over"] = df["p_over_mkt"].map(prob_to_american)
        df["player_id"] = pd.NA
        df["game_id"] = pd.NA
        df["team_id"] = pd.NA
        df["game_date"] = slate or pd.NA

        keep = ["full_name","player_id","game_id","team_id","line","price_over","p_over_mkt","fair_over","game_date"]
        df[keep].to_csv(out, index=False)
        um.write_text("")  # nothing to match yet
        print(f"[points_with_market] odds-only wrote: {out}  rows={len(df)}")
        return

    # ---- PREDICTIONS MERGE MODE ----
    pred_wide = read_csv_required(Path(args.pred))
    long = melt_preds(pred_wide)

    names = read_csv_required(Path(args.names))
    # Try best-effort column set; tolerate different exports
    name_cols = [c for c in ["player_id","game_id","team_id","full_name","game_date"] if c in names.columns]
    names = names[name_cols].copy()

    keys = [k for k in ["player_id","game_id"] if k in long.columns and k in names.columns]
    if not keys:
        # If no id keys overlap, bail to name-only merge (less ideal)
        names["name_norm"] = names.get("full_name","").map(norm_name)
        long["name_norm"] = long.get("full_name","").map(norm_name) if "full_name" in long.columns else pd.NA

    df = long.merge(names, on=keys, how="left") if keys else long.merge(names, on=["name_norm"], how="left")
    if "game_date" in df.columns and slate:
        df = df[df["game_date"].astype(str) == slate].copy()

    df["name_norm"] = df.get("full_name","").map(norm_name)
    df["line_str"]  = df["line"].map(lambda x: str(float(x)).rstrip("0").rstrip(".") if pd.notna(x) else "")

    if med is not None:
        df = df.merge(med, on=["name_norm","line_str"], how="left", suffixes=("",""))
    else:
        df["price_over"] = pd.NA
        df["full_name"] = df.get("full_name")  # keep whatever we have

    df["p_over"] = pd.to_numeric(df["p_over"], errors="coerce")
    df["p_over_mkt"] = df["price_over"].map(american_to_prob)
    df["edge_over"] = df.apply(
        lambda r: r["p_over"]-r["p_over_mkt"]
        if (isinstance(r["p_over"],float) and isinstance(r["p_over_mkt"],float)
            and math.isfinite(r["p_over"]) and math.isfinite(r["p_over_mkt"]))
        else float("nan"), axis=1)
    df["fair_over"] = df["p_over"].map(prob_to_american)

    unmatched = df[df["price_over"].isna()].copy()
    um_cols = [c for c in ["full_name","player_id","game_id","team_id","line","p_over","game_date"] if c in df.columns]
    unmatched[um_cols].to_csv(um, index=False)

    keep_cols = [c for c in ["full_name","player_id","game_id","team_id","line","p_over","price_over",
                             "p_over_mkt","edge_over","fair_over","game_date"] if c in df.columns]
    df[keep_cols].to_csv(out, index=False)

    print(f"[points_with_market] slate={slate or 'n/a'} rows={len(df)} matched_prices={len(df)-len(unmatched)}/{len(df)}")
    print(f"[points_with_market] wrote: {out} | unmatched: {um} rows={len(unmatched)}")

if __name__ == "__main__":
    main()
