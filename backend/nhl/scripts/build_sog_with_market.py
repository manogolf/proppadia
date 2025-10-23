#!/usr/bin/env python3
"""
Build sog_with_market.csv by combining model predictions, the names export, and odds JSON.

- Predictions: backend/nhl/data/processed/sog_predictions.csv
  (wide columns: p_over_0.5, p_over_1.5, p_over_2.5, p_over_3.5, ...)

- Names Export: exports/train_nhl_sog_v2.csv
  (must include: full_name, player_id, game_id, team_id, game_date)

- Odds JSON: nhl/site/data/odds_nhl_playerprops_today.json (or any equivalent structure)
  (expects markets with key = "player_shots_on_goal" and outcomes Over/Under)

Outputs:
- nhl/site/data/sog_with_market.csv
- nhl/site/data/unmatched_sog.csv
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from statistics import median
from typing import Iterable, Tuple, Dict, Any, List

import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime


ET = ZoneInfo("America/New_York")


# --------------------------- helpers ---------------------------

def log(msg: str) -> None:
    print(f"[sog_with_market] {msg}")

def fail(msg: str, code: int = 3) -> "NoReturn":  # type: ignore[name-defined]
    print(f"[sog_with_market] FATAL: {msg}", file=sys.stderr)
    sys.exit(code)

def norm_name(s: str) -> str:
    """ASCII fold, normalize whitespace/punct, lower-case."""
    if not isinstance(s, str):
        return ""
    import unicodedata as ud
    s = ud.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.replace("-", " ").replace(".", " ").replace("'", "").replace("’", "")
    s = " ".join(s.lower().split())
    return s

def american_to_prob(price) -> float | None:
    try:
        p = float(price)
    except Exception:
        return None
    if p > 0:
        return 100.0 / (p + 100.0)
    if p < 0:
        return (-p) / ((-p) + 100.0)
    return None

def parse_lines_arg(lines_str: str) -> List[float]:
    if not lines_str:
        return [1.5, 2.5, 3.5]
    out = []
    for tok in lines_str.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            out.append(float(tok))
        except Exception:
            pass
    if not out:
        out = [1.5, 2.5, 3.5]
    return out


# --------------------------- odds parsing & devig ---------------------------

@dataclass
class PriceRow:
    name_norm: str
    line: float
    book: str
    price_over: float | None
    price_under: float | None
    p_over_raw: float | None
    p_under_raw: float | None
    p_over_mkt: float | None  # devigged P(Over)

def _walk_odds_collect(root: Any, lines: set[float]) -> List[PriceRow]:
    """
    Walk a (possibly nested) odds JSON payload and extract Over/Under prices
    for key == "player_shots_on_goal". Returns per-(name,line,book) rows.
    """
    rows: Dict[Tuple[str, float, str], Dict[str, float | None]] = {}

    def ensure(nm: str, ln: float, bk: str) -> Dict[str, float | None]:
        k = (nm, ln, bk)
        if k not in rows:
            rows[k] = {"over": None, "under": None}
        return rows[k]

    def visit(node: Any, book: str | None = None) -> None:
        if isinstance(node, dict):
            # detect a bookmaker-ish node to carry book identity along
            if "title" in node and "markets" in node:
                book = str(node.get("title") or node.get("key") or book or "")
            # collect market outcomes
            if node.get("key") == "player_shots_on_goal":
                for o in (node.get("outcomes") or []):
                    side = o.get("name")
                    if side not in ("Over", "Under"):
                        continue
                    try:
                        line = float(o.get("point"))
                    except Exception:
                        continue
                    if line not in lines:
                        continue
                    nm = norm_name(o.get("description") or o.get("player") or "")
                    if not nm:
                        continue
                    price = o.get("price")
                    try:
                        price = float(price)
                    except Exception:
                        price = None
                    slot = ensure(nm, line, book or "")
                    if side == "Over":
                        slot["over"] = price
                    else:
                        slot["under"] = price
            # recurse
            for v in node.values():
                if isinstance(v, (dict, list)):
                    visit(v, book)
        elif isinstance(node, list):
            for it in node:
                visit(it, book)

    visit(root, None)

    out: List[PriceRow] = []
    for (nm, ln, bk), pr in rows.items():
        po = american_to_prob(pr.get("over"))
        pu = american_to_prob(pr.get("under"))
        if po is None and pu is None:
            continue
        if po is None or pu is None:
            p_over_mkt = po if po is not None else None  # one-sided fallback
        else:
            total = po + pu
            p_over_mkt = po / total if total > 0 else None
        out.append(PriceRow(
            name_norm=nm,
            line=ln,
            book=bk,
            price_over=pr.get("over"),
            price_under=pr.get("under"),
            p_over_raw=po,
            p_under_raw=pu,
            p_over_mkt=p_over_mkt,
        ))
    return out


# --------------------------- main build ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Build sog_with_market.csv from predictions + names + odds.")
    ap.add_argument("--pred", default="backend/nhl/data/processed/sog_predictions.csv", help="Predictions CSV (wide p_over_*).")
    ap.add_argument("--names", default="exports/train_nhl_sog_v2.csv", help="Names export CSV.")
    ap.add_argument("--odds", default="nhl/site/data/odds_nhl_playerprops_today.json", help="Odds JSON file.")
    ap.add_argument("--out", default="nhl/site/data/sog_with_market.csv", help="Output CSV path.")
    ap.add_argument("--unmatched", default="nhl/site/data/unmatched_sog.csv", help="Unmatched output CSV path.")
    ap.add_argument("--lines", default="1.5,2.5,3.5", help="Comma-separated lines to keep (e.g. '1.5,2.5,3.5').")
    ap.add_argument("--slate-date", default=None, help="YYYY-MM-DD (ET). If omitted, uses ET 'today'.")
    args = ap.parse_args()

    lines = set(parse_lines_arg(args.lines))

    # Slate date (ET)
    slate_date = args.slate_date or datetime.now(ET).date().isoformat()
    log(f"SLATE_DATE (ET) = {slate_date}")

    # ---------------- predictions (wide → long) ----------------
    try:
        pred = pd.read_csv(args.pred)
    except FileNotFoundError:
        fail(f"missing predictions CSV: {args.pred}")

    pcols = [c for c in pred.columns if c.startswith("p_over_")]
    if not pcols or pred.empty:
        fail(f"predictions CSV missing p_over_* columns or has no rows. Header={list(pred.columns)}")

    # coerce IDs now to avoid dtype mismatches later
    for c in ("player_id", "game_id"):
        if c in pred.columns:
            pred[c] = pd.to_numeric(pred[c], errors="coerce").astype("Int64")

    pred_long = pred.melt(
        id_vars=["player_id", "game_id"],
        value_vars=pcols,
        var_name="k",
        value_name="p_over",
    )
    pred_long["line"] = pd.to_numeric(pred_long["k"].str.replace("p_over_", "", regex=False), errors="coerce")
    pred_long.drop(columns=["k"], inplace=True)
    pred_long = pred_long[pred_long["line"].isin(lines)].copy()

    # ---------------- names (must have full_name, player_id, game_id, team_id, game_date) ----------------
    need_cols = {"full_name", "player_id", "game_id", "team_id", "game_date"}
    try:
        names = pd.read_csv(args.names, usecols=lambda c: c in need_cols)
    except FileNotFoundError:
        fail(f"missing names CSV: {args.names}")

    missing = need_cols - set(names.columns)
    if missing:
        fail(f"{args.names} missing required columns: {sorted(missing)}")

    # type alignment
    for c in ("player_id", "game_id", "team_id"):
        names[c] = pd.to_numeric(names[c], errors="coerce").astype("Int64")
    names["game_date"] = pd.to_datetime(names["game_date"], errors="coerce").dt.date

    # Merge predictions → names to validate dates present
    m = pred_long.merge(
        names[["player_id", "game_id", "game_date"]],
        on=["player_id", "game_id"], how="left"
    )
    dates_present = sorted(set([d for d in m["game_date"].dropna().tolist()]))

    if not len(m):
        fail("predictions melted rows = 0 after line filter (check p_over_* and --lines).")
    if not dates_present:
        fail("predictions do not join to any names on (player_id,game_id). Dates present via join: []")

    # require SLATE_DATE present
    try:
        sd = pd.to_datetime(slate_date).date()
    except Exception:
        fail(f"invalid --slate-date: {slate_date}")

    if sd not in dates_present:
        fail(f"export does not contain SLATE_DATE={sd}. Found dates: {dates_present}")

    # strict keep only this slate (defensive)
    pred_named = pred_long.merge(
        names, on=["player_id", "game_id"], how="left"
    )
    before = len(pred_named)
    pred_named = pred_named[pred_named["game_date"] == sd].copy()
    after = len(pred_named)

    log(f"names rows: kept {names[names['game_date'].eq(sd)].shape[0]}/{len(names)} for {sd}")
    log(f"pred rows after merge/filter: {after}")

    pred_named["full_name"] = pred_named["full_name"].fillna("")
    pred_named["name_norm"] = pred_named["full_name"].map(norm_name)

    # ---------------- odds JSON (optional) → devig ----------------
    odds_df = pd.DataFrame()
    have_odds = False
    try:
        with open(args.odds, "r") as f:
            raw = json.load(f)
        rows = _walk_odds_collect(raw, lines)
        if rows:
            have_odds = True
            odds_df = pd.DataFrame([r.__dict__ for r in rows])
    except FileNotFoundError:
        log(f"odds JSON not found (optional): {args.odds}")
    except Exception as e:
        log(f"odds JSON parse error (treated as optional): {e}")

    if have_odds:
        # aggregate per (name,line) across books: median prices, median devig probability
        agg = (odds_df.groupby(["name_norm", "line"], as_index=False)
               .agg(price_over=("price_over", "median"),
                    price_under=("price_under", "median"),
                    p_over_mkt=("p_over_mkt", "median")))
        unique_pairs = len(agg)
        by_line = (agg.groupby("line")["name_norm"].nunique()
                      .to_dict())
        log(f"Matched-capable (unique name,line) in odds: {unique_pairs} | by line: {by_line}")
        merged = pred_named.merge(agg, on=["name_norm", "line"], how="left")
    else:
        merged = pred_named.copy()
        merged["price_over"] = pd.NA
        merged["price_under"] = pd.NA
        merged["p_over_mkt"] = pd.NA

    # ---------------- edge + outputs ----------------
    merged["edge_over"] = merged["p_over"] - merged["p_over_mkt"]

    keep = ["full_name", "player_id", "game_id", "team_id",
            "line", "p_over", "price_over", "p_over_mkt", "edge_over", "game_date"]

    out_path = args.out
    unmatched_path = args.unmatched
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(unmatched_path).parent.mkdir(parents=True, exist_ok=True)

    # defensive: filter to slate_date again
    b2 = len(merged)
    merged = merged[merged["game_date"].eq(sd)].copy()
    a2 = len(merged)
    log(f"final filter by date (defensive): kept {a2}/{b2}")

    # Save main file
    merged[keep].to_csv(out_path, index=False)

    # Save unmatched (no p_over_mkt)
    u = merged[merged["p_over_mkt"].isna()].copy()
    u[keep].to_csv(unmatched_path, index=False)

    matched = merged["p_over_mkt"].notna().sum()
    total = len(merged)
    line_list = sorted({float(l) for l in lines})
    log(f"Matched prices for {matched}/{total} rows.")
    log(f"Lines present (requested): {line_list}")
    log(f"✅ Wrote: {out_path}  rows={total}")
    log(f"Wrote unmatched to: {unmatched_path}  rows={len(u)}")

    # small sample print
    if total:
        print(merged[keep].head(12).to_string(index=False))


if __name__ == "__main__":
    from pathlib import Path
    main()
