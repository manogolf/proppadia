#!/usr/bin/env python3
"""Price the frozen NHL SOG / Points HGB baseline rows on both sides.

Inputs:
- a frozen W/L baseline package (`freeze_nhl_sog_points_wl_baseline.py`), whose
  rows carry the governed P(over), selected side and result;
- an all-candidate evaluation export run with `--price-both-sides`, which
  supplies each book's latest quote that precedes the prediction, for both
  outcomes of every production prop line.

For every baseline row and side, the best decimal price across books (and,
separately, BetOnline alone) is settled at one unit. EV uses the model's
probability for that side. "Against the model" rows are the side the model
did not pick; plus-money sides with positive model EV are the underdog-value
set a win/loss table cannot show. Read-only; writes CSVs and a summary.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

MARKETS = {
    "sog": ("sog_rows.csv", "shots_on_goal", "baseline_v1"),
    "points_hgb": ("points_hgb_rows.csv", "player_points", "NHL_POINTS_COUNT_HGB_V1"),
}
PRICE_BUCKETS = ((-100000, -300, "-300 or shorter"), (-299, -150, "-299 to -150"), (-149, -101, "-149 to -101"),
                 (100, 149, "+100 to +149"), (150, 299, "+150 to +299"), (300, 100000, "+300 or longer"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _quote_files(export: Path) -> list[Path]:
    """One export folder, or a folder of per-slate exports (slate_date=*)."""
    files = sorted(export.glob("slate_date=*/candidate_quotes.csv")) or [export / "candidate_quotes.csv"]
    return [path for path in files if path.is_file()]


def _bucket(american: float) -> str:
    return next(label for lo, hi, label in PRICE_BUCKETS if lo <= american <= hi)


def _american(decimal: float) -> float:
    return round((decimal - 1) * 100) if decimal >= 2 else round(-100 / (decimal - 1))


def priced_rows(baseline: Path, export: Path, market: str) -> pd.DataFrame:
    rows_file, market_key, model_version = MARKETS[market]
    rows = pd.read_csv(baseline / rows_file)
    quotes = pd.concat([pd.read_csv(path, low_memory=False, usecols=[
        "slate_date", "game_id", "player_id", "market_key", "market_line", "model_role", "model_version",
        "quote_side", "sportsbook_key", "american_price", "decimal_price"]) for path in _quote_files(export)],
        ignore_index=True)
    quotes = quotes[(quotes.market_key == market_key) & (quotes.model_role == "PRODUCTION")
                    & (quotes.model_version == model_version)].copy()
    quotes["line"] = pd.to_numeric(quotes.market_line, errors="coerce")
    quotes["decimal_price"] = pd.to_numeric(quotes.decimal_price, errors="coerce")
    quotes = quotes.dropna(subset=["decimal_price", "line"])
    quotes = quotes[quotes.decimal_price > 1]
    keys = ["slate_date", "game_id", "player_id", "line"]
    for frame in (rows, quotes):
        frame["game_id"] = frame.game_id.astype("int64")
        frame["player_id"] = pd.to_numeric(frame.player_id, errors="coerce").astype("Int64")
        frame["slate_date"] = frame.slate_date.astype(str)
    out = []
    for scope, subset in (("best_price", quotes), ("betonline", quotes[quotes.sportsbook_key == "betonlineag"])):
        best = subset.sort_values("decimal_price").groupby(keys + ["quote_side"], as_index=False).tail(1)
        for side in ("OVER", "UNDER"):
            priced = rows.merge(best[best.quote_side == side][keys + ["decimal_price", "sportsbook_key"]],
                                on=keys, how="inner")
            if priced.empty:
                continue
            p_side = priced.p_over if side == "OVER" else 1 - priced.p_over
            actual = priced.selected_side.where(priced.grading_state == "WIN",
                                                priced.selected_side.map({"OVER": "UNDER", "UNDER": "OVER"}))
            won = actual == side
            out.append(pd.DataFrame({
                "market": market, "price_scope": scope, "slate_date": priced.slate_date, "game_id": priced.game_id,
                "player_id": priced.player_id, "line": priced.line, "side": side,
                "relation": (priced.selected_side == side).map({True: "MODEL_PICK", False: "AGAINST_MODEL"}),
                "model_probability": p_side.round(4), "decimal_price": priced.decimal_price,
                "american_price": priced.decimal_price.map(_american), "sportsbook_key": priced.sportsbook_key,
                "model_ev": (p_side * priced.decimal_price - 1).round(4), "won": won,
                "profit": won * (priced.decimal_price - 1) - (~won) * 1.0,
            }))
    priced = pd.concat(out, ignore_index=True)
    priced["price_bucket"] = pd.Categorical(priced.american_price.map(_bucket), [b[2] for b in PRICE_BUCKETS], ordered=True)
    return priced


def summarize(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    grouped = frame.groupby(keys, observed=True)
    out = grouped.agg(n=("won", "size"), wins=("won", "sum"), profit=("profit", "sum"),
                      mean_model_ev=("model_ev", "mean")).reset_index()
    out["losses"] = out.n - out.wins
    out["win_rate"] = (out.wins / out.n).round(4)
    out["roi"] = (out.profit / out.n).round(4)
    out["profit"] = out.profit.round(2)
    out["mean_model_ev"] = out.mean_model_ev.round(4)
    return out[keys + ["n", "wins", "losses", "win_rate", "profit", "roi", "mean_model_ev"]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    priced = pd.concat([priced_rows(args.baseline, args.export, m) for m in MARKETS], ignore_index=True)
    priced.to_csv(args.out / "priced_rows.csv", index=False)
    tables = {
        "by_relation.csv": summarize(priced, ["market", "price_scope", "relation"]),
        "by_relation_price.csv": summarize(priced, ["market", "price_scope", "relation", "price_bucket"]),
        "positive_ev_by_relation_price.csv": summarize(
            priced[priced.model_ev > 0], ["market", "price_scope", "relation", "price_bucket"]),
        "plus_money_positive_ev.csv": summarize(
            priced[(priced.model_ev > 0) & (priced.american_price >= 100)], ["market", "price_scope", "line", "side"]),
    }
    for name, table in tables.items():
        table.to_csv(args.out / name, index=False)
    manifest = {"contract": "NHL_SOG_POINTS_PRICE_VIEW_V1", "baseline": str(args.baseline),
                "baseline_sha256sums": _sha(args.baseline / "SHA256SUMS"), "export": str(args.export),
                "export_quote_files_sha256": {str(path.relative_to(args.export)): _sha(path)
                                              for path in _quote_files(args.export)},
                "price_rule": "latest quote per book preceding the prediction; best decimal across books, or BetOnline only",
                "settlement": "one unit at the quoted price; EV at the model probability for that side"}
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    files = sorted(p for p in args.out.iterdir() if p.is_file())
    (args.out / "SHA256SUMS").write_text("".join(f"{_sha(p)}  {p.name}\n" for p in files))
    print(tables["by_relation.csv"].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
