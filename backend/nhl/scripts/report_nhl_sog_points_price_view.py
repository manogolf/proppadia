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

Weekly mode (``--weekly-as-of``, from 2026-11-02; run by the weekly model
evaluation schedule): the same settlement for the current production models'
governed picks, read from the weekly evaluation's per-slate row cache, priced
with each slate's retained quotes (``two_sided_lines.production_quotes``) at the
best price across all books and at the operator's books. Its subject is the
model's underdog side: sides whose model fair odds are plus money (the model
gives them under 50%), banded by those fair odds, the 8rain fair-odds filter
set to +100 or longer. It collects until every band has enough positive-EV
picks at the operator's books, and rewrites ``docs/nhl/plus_money_price_view.md``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]

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


def settle_sides(rows: pd.DataFrame, best: pd.DataFrame, *, market: str, scope: str) -> list[pd.DataFrame]:
    """Settle both sides of every graded row at one unit, at ``best``'s price for that side."""
    keys = ["slate_date", "game_id", "player_id", "line"]
    out = []
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
    return out


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
        out.extend(settle_sides(rows, best, market=market, scope=scope))
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


# --- Weekly plus-money view (2026-11-02) -------------------------------------
# The operator's books (BetOnline, Novig, Kalshi; Pinnacle from 2026-11-02).
MY_BOOKS = ("betonlineag", "novig", "kalshi", "pinnacle")
PLUS_BANDS = ((100, 129, "+100 to +129"), (130, 169, "+130 to +169"), (170, 249, "+170 to +249"),
              (250, 100000, "+250 or longer"))
# Model fair odds for the side. Plus money (model under 50%) uses the price bands;
# the favorite side keeps the 8rain fair-odds floor at -133 (57.1%).
FAIR_BANDS = ((0.5, 0.5714, "-100 to -132"), (0.5714, 0.6667, "-133 to -200"), (0.6667, 1.01, "-200 or shorter"))
SOG_HGB_START = "2026-10-25"   # production SOG became NHL_SOG_COUNT_HGB_V1
LANE_MARKETS = {"sog": "shots_on_goal", "points": "player_points"}
# Positive-EV underdog sides at the operator's books, per lane and fair-odds band:
# about +/-10 points of return noise at 100 picks. A lane is ready when every band is.
READY_PER_BAND = 100
# Single lines the operator watches on their own, besides each market as a whole:
# (readiness name, market, line, side, heading).
FOCUS_LINES = (("sog over 1.5", "sog", 1.5, "OVER", "SOG Over 1.5"),)
# Focus lines split the first plus band in three; the other bands are unchanged.
FOCUS_BANDS = ((100, 109, "+100 to +109"), (110, 119, "+110 to +119"), (120, 129, "+120 to +129")) + PLUS_BANDS[1:]
WEEKLY_OUT = ROOT / "artifacts/analysis/nhl/price_view"
WEEKLY_DOC = ROOT / "docs/nhl/plus_money_price_view.md"


def _band(value: float, bands) -> str | None:
    return next((label for lo, hi, label in bands if lo <= value <= hi), None)


def fair_american(p: float) -> float:
    return round(100 * (1 - p) / p) if p < 0.5 else round(-100 * p / (1 - p))


def _fair_band(p: float) -> str:
    if p < 0.5:
        return _band(max(fair_american(p), 100), PLUS_BANDS)
    return next(label for lo, hi, label in FAIR_BANDS if lo <= p < hi)


def current_points_identity() -> str:
    from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
    return json.loads(scorer.CONTRACT_PATH.read_text())["model_identity_sha256"]


def points_identity(summary: dict) -> str | None:
    return (((summary.get("models") or {}).get("points") or {}).get("authority_context") or {}) \
        .get("production", {}).get("fitted_model_identity_sha256")


def lane_days(summaries: dict[str, dict], as_of: str) -> dict[str, list[str]]:
    """Slates scored by the current production model of each lane."""
    identity = current_points_identity()
    return {"points": sorted(d for d, s in summaries.items() if d <= as_of and points_identity(s) == identity),
            "sog": sorted(d for d in summaries if SOG_HGB_START <= d <= as_of)}


def best_prices(quotes: list[dict]) -> pd.DataFrame:
    """Best decimal price per player line and side, across all books and at the operator's books."""
    frame = pd.DataFrame(quotes)
    columns = ["lane", "game_id", "player_id", "line", "quote_side", "price_scope", "decimal_price", "sportsbook_key"]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    lanes = {market: lane for lane, market in LANE_MARKETS.items()}
    frame = frame[frame.market_key.isin(lanes)].copy()
    frame["lane"] = frame.market_key.map(lanes)
    frame["decimal_price"] = pd.to_numeric(frame.decimal_price, errors="coerce")
    frame["line"] = pd.to_numeric(frame.market_line, errors="coerce")
    frame["game_id"] = pd.to_numeric(frame.game_id, errors="coerce")
    frame["player_id"] = pd.to_numeric(frame.player_id, errors="coerce")
    frame = frame.dropna(subset=["decimal_price", "line", "game_id", "player_id"])
    frame = frame[frame.decimal_price > 1].astype({"game_id": "int64", "player_id": "int64"})
    keys = ["lane", "game_id", "player_id", "line", "quote_side"]
    out = []
    for scope, subset in (("all_books", frame), ("my_books", frame[frame.sportsbook_key.isin(MY_BOOKS)])):
        best = subset.sort_values(["decimal_price", "sportsbook_key"]).groupby(keys, as_index=False).tail(1)
        out.append(best.assign(price_scope=scope))
    return pd.concat(out, ignore_index=True)[columns] if out else pd.DataFrame(columns=columns)


def _cached_prices(day: str, load_candidates) -> pd.DataFrame:
    from backend.nhl import two_sided_lines
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    snapshots = len(list((weekly.OPERATIONAL / "odds_observations" / "season=2026" / f"slate_date={day}").glob("observation=*")))
    path = weekly.CACHE / "prices" / f"{day}_{snapshots}_v1.csv"
    if path.is_file():
        return pd.read_csv(path)
    _, quotes = two_sided_lines.production_quotes(weekly.OPERATIONAL, day, candidates=load_candidates())
    prices = best_prices(quotes)
    path.parent.mkdir(parents=True, exist_ok=True)
    prices.to_csv(path, index=False)
    return prices


def weekly_priced(rows: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """Settle one lane-day's graded rows on both sides at both price scopes."""
    out = []
    for lane in LANE_MARKETS:
        lane_rows = rows[rows.lane == lane]
        for scope in ("all_books", "my_books"):
            best = prices[(prices.lane == lane) & (prices.price_scope == scope)].assign(
                slate_date=lane_rows.slate_date.iloc[0] if len(lane_rows) else "")
            if lane_rows.empty or best.empty:
                continue
            out.extend(settle_sides(lane_rows, best, market=lane, scope=scope))
    if not out:
        return pd.DataFrame()
    priced = pd.concat(out, ignore_index=True)
    priced["plus_band"] = priced.american_price.map(lambda a: _band(a, PLUS_BANDS))
    priced["fair_american"] = priced.model_probability.map(fair_american)
    priced["fair_band"] = priced.model_probability.map(_fair_band)
    priced["focus_band"] = priced.model_probability.map(
        lambda p: _band(max(fair_american(p), 100), FOCUS_BANDS) if p < 0.5 else None)
    priced["price_bucket"] = priced.american_price.map(lambda a: _band(a, PRICE_BUCKETS) or "-100")
    return priced


def underdogs(priced: pd.DataFrame) -> pd.DataFrame:
    """Sides whose model fair odds are plus money (the model gives them under 50%)."""
    return priced[priced.model_probability < 0.5]


def readiness(priced: pd.DataFrame) -> pd.DataFrame:
    """Positive-EV underdog sides at the operator's books per lane and fair-odds band."""
    target = underdogs(priced)
    target = target[(target.price_scope == "my_books") & (target.model_ev > 0)]
    rows = [{"market": lane, "fair_band": band, "picks": int(((target.market == lane) & (target.fair_band == band)).sum()),
             "needed": READY_PER_BAND} for lane in LANE_MARKETS for _, _, band in PLUS_BANDS]
    for name, market, line, side, heading in FOCUS_LINES:
        focus = target[(target.market == market) & (target.line == line) & (target.side == side)]
        rows += [{"market": name, "fair_band": band, "picks": int((focus.focus_band == band).sum()),
                  "needed": READY_PER_BAND} for _, _, band in FOCUS_BANDS]
    out = pd.DataFrame(rows)
    out["ready"] = out.picks >= out.needed
    return out


def lane_readiness(ready: pd.DataFrame) -> list[dict]:
    """One entry per lane for the console: picks in the thinnest band, and whether every band is ready."""
    return [{"market": lane, "picks": int(g.picks.min()), "needed": READY_PER_BAND, "ready": bool(g.ready.all())}
            for lane, g in ready.groupby("market", sort=False)]


def _md_table(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    if frame.empty:
        return ["", "No settled plus-money picks yet."]
    head = ["", "| " + " | ".join(columns) + " | Picks | W-L | Win % | Model % | Return |",
            "|" + "---|" * (len(columns) + 5)]
    body = [f"| {' | '.join(str(getattr(r, c)) for c in columns)} | {r.n} | {r.wins}-{r.losses} | "
            f"{100 * r.win_rate:.1f} | {100 * r.mean_probability:.1f} | {100 * r.roi:+.1f}% |"
            for r in frame.itertuples()]
    return head + body


def plus_summary(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = summarize(frame, keys)
    mean_p = frame.groupby(keys, observed=True).model_probability.mean().reset_index(name="mean_probability")
    return out.merge(mean_p, on=keys)


def render_weekly(as_of: str, priced: pd.DataFrame, days: dict[str, list[str]], ready: pd.DataFrame) -> str:
    dogs = underdogs(priced)
    keys = ["market", "line", "side", "fair_band"]
    lines = [f"# NHL plus-money price view (through {as_of})", "",
             "Rebuilt weekly with the model evaluation. The subject is the model's underdog side: every side of a "
             "graded production pick whose model fair odds are plus money (the model gives it under 50%), "
             "banded by those fair odds, as the 8rain fair-odds filter would show it at +100 or longer. Each side "
             "is settled at one unit at the best quoted price before the prediction, at the operator's books "
             "(BetOnline, Novig, Kalshi, Pinnacle) and across all books (members' view). Positive EV means the "
             "book's price is longer than the model's fair price. Every side and price is in `priced_rows.csv` "
             f"under `artifacts/analysis/nhl/price_view/weekly_{as_of}/`. Collection only: no filter is chosen "
             "from these tables until the readiness counts are met.", "",
             f"- Points: goals + assists model (`{current_points_identity()[:8]}…`), "
             f"{len(days['points'])} slates ({days['points'][0] if days['points'] else '—'} to "
             f"{days['points'][-1] if days['points'] else '—'}).",
             f"- SOG: `NHL_SOG_COUNT_HGB_V1`, {len(days['sog'])} slates from {SOG_HGB_START}.", "",
             "## Readiness", "",
             f"Positive-EV underdog sides at the operator's books; a market is ready for evaluation when every "
             f"fair-odds band has {READY_PER_BAND}.", "",
             "| Market | Fair odds | Picks | Needed | Ready |", "|---|---|---|---|---|"]
    lines += [f"| {r.market} | {r.fair_band} | {r.picks} | {r.needed} | {'yes' if r.ready else 'no'} |"
              for r in ready.itertuples()]
    for scope, title in (("my_books", "operator's books"), ("all_books", "all books")):
        sub = dogs[(dogs.price_scope == scope) & (dogs.model_ev > 0)]
        lines += ["", f"## Underdog sides with positive EV — {title}", "", "By fair odds:"]
        lines += _md_table(plus_summary(sub, ["market", "side", "fair_band"]), ["market", "side", "fair_band"])
        lines += ["", "By line, side and fair odds:"]
        lines += _md_table(plus_summary(sub, keys), keys)
    for name, market, line, side, heading in FOCUS_LINES:
        focus = dogs[(dogs.market == market) & (dogs.line == line) & (dogs.side == side)]
        lines += ["", f"## {heading}: model-underdog sides by fair odds", "",
                  "The same sides for this one line, tracked on their own (readiness above)."]
        for scope, title, positive in (("my_books", "Positive EV, operator's books", True),
                                       ("all_books", "Positive EV, all books", True),
                                       ("my_books", "Any EV, operator's books", False)):
            sub = focus[(focus.price_scope == scope) & ((focus.model_ev > 0) | (not positive))]
            lines += ["", f"{title}:"]
            sub = sub.drop(columns="fair_band").rename(columns={"focus_band": "fair_band"})
            lines += _md_table(plus_summary(sub, ["fair_band"]), ["fair_band"])
    every = dogs[dogs.price_scope == "my_books"]
    lines += ["", "## Every underdog side, any EV — operator's books", "",
              "Context: the same sides without the positive-EV condition."]
    lines += _md_table(plus_summary(every, ["market", "side", "fair_band"]), ["market", "side", "fair_band"])
    lines += ["", "Return is profit per unit at the stated price; Model % is the model's mean probability for the "
              "side. Small cells move by chance: about +/-10 points of return at 100 picks.", ""]
    return "\n".join(lines)


def run_weekly(as_of: str) -> dict:
    from backend.nhl import two_sided_lines
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    summaries = {d: s for d in weekly._days("2026-10-01", as_of) if (s := weekly._summary(d))}
    days = lane_days(summaries, as_of)
    loaded: list = []

    def load_candidates() -> list:
        if not loaded:
            loaded.append(two_sided_lines.candidate_export.load_candidates(weekly.OPERATIONAL)[0])
        return loaded[0]

    frames = []
    for day in sorted(set(days["points"]) | set(days["sog"])):
        rows = [weekly._cached_rows(lane, day, summaries[day]["summary_identity"]).assign(lane=lane)
                for lane in LANE_MARKETS if day in days[lane]]
        rows = pd.concat(rows, ignore_index=True)
        if rows.empty:
            continue
        rows = rows[rows.grading_state.isin(["WIN", "LOSS"])].copy()
        rows["slate_date"] = rows.slate_date.astype(str)
        rows["game_id"] = rows.game_id.astype("int64")
        rows["player_id"] = rows.player_id.astype("int64")
        rows["line"] = pd.to_numeric(rows.line).astype(float)
        priced = weekly_priced(rows, _cached_prices(day, load_candidates))
        if not priced.empty:
            frames.append(priced)
    priced = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=[
        "market", "price_scope", "relation", "side", "line", "american_price", "model_probability",
        "plus_band", "fair_band", "focus_band", "price_bucket", "won", "profit", "model_ev"])
    out = WEEKLY_OUT / f"weekly_{as_of}"
    out.mkdir(parents=True, exist_ok=True)
    priced.to_csv(out / "priced_rows.csv", index=False)
    ready = readiness(priced)
    ready.to_csv(out / "readiness.csv", index=False)
    plus = priced[priced.american_price >= 100]
    summarize(underdogs(priced).assign(positive_ev=lambda f: f.model_ev > 0),
              ["market", "price_scope", "positive_ev", "line", "side", "fair_band"]).to_csv(
        out / "underdog_fair_bands.csv", index=False)
    summarize(plus, ["market", "price_scope", "relation", "side", "fair_band", "plus_band"]).to_csv(
        out / "plus_price_bands.csv", index=False)
    summarize(priced, ["market", "price_scope", "relation", "price_bucket"]).to_csv(
        out / "by_relation_price.csv", index=False)
    WEEKLY_DOC.write_text(render_weekly(as_of, priced, days, ready))
    summary = {"contract": "NHL_PLUS_MONEY_PRICE_VIEW_WEEKLY_V1", "as_of": as_of,
               "points_model_identity_sha256": current_points_identity(), "sog_model": "NHL_SOG_COUNT_HGB_V1",
               "slates": days, "my_books": list(MY_BOOKS), "ready_per_band": READY_PER_BAND,
               "readiness": lane_readiness(ready), "readiness_by_band": ready.to_dict(orient="records"), "doc": str(WEEKLY_DOC.relative_to(ROOT)),
               "price_rule": "each book's latest quote preceding the prediction (EARLY/FINAL_PREGAME); best decimal per side"}
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print("NHL_PLUS_MONEY_PRICE_VIEW=" + json.dumps({"as_of": as_of, "readiness": summary["readiness"]}))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--export", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--weekly-as-of", help="weekly plus-money view through this slate date")
    args = parser.parse_args()
    if args.weekly_as_of:
        run_weekly(args.weekly_as_of)
        return 0
    if not (args.baseline and args.export and args.out):
        parser.error("--baseline, --export and --out are required without --weekly-as-of")
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
