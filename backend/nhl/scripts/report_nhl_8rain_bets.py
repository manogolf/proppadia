#!/usr/bin/env python3
"""The operator's 8rain paper bets: record by market, side, line, fair odds, EV and price.

Reads the daily 8rain downloads (``8rainstation_daily_*.csv``; read-only), keeps
NHL rows, drops repeated wager IDs (a re-download repeats the day's bets), and
parses each bet's player, market, side and line. 8rain's "No-Vig" column is our
uploaded fair price (it equals "Station Odds"), "Odds" is the price taken and
"Positive EV" the model's EV at that price. Where the weekly plus-money price
view priced the same pick (current production SOG and Points models), the price
taken is compared with the best price at the operator's books before the
prediction.

The output is the operator's own betting record, so it is written only under the
git-ignored ``artifacts/analysis/nhl/8rain_bets/`` (``report.md``, ``bets.csv``,
``summary.json``), never under ``docs/``.

  ./bin/python backend/nhl/scripts/report_nhl_8rain_bets.py [--downloads ~/Downloads]
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/8rain_bets"
NAMES = ROOT / "backend/nhl/exports/daily/names"
PRICE_VIEW = ROOT / "artifacts/analysis/nhl/price_view"
PROP_RE = re.compile(r"^(?P<side>Over|Under)\s+(?P<player>.+?)\s+(?P<market>Shots On Goal|Points|Saves)\s+"
                     r"(?P<line>\d+(?:\.\d+)?)$", re.IGNORECASE)
MARKETS = {"shots on goal": "sog", "points": "points", "saves": "saves"}
EV_BANDS = [-1e9, 0, 0.02, 0.05, 0.10, 0.20, 1e9]
EV_LABELS = ["negative", "0-2%", "2-5%", "5-10%", "10-20%", "20%+"]
FAIR_ORDER = ["beyond -500", "-300 to -500", "-200 to -300", "-133 to -200", "-100 to -132",
              "+100 to +129", "+130 to +169", "+170 to +249", "+250 or longer", "unknown"]


def probability(american: float) -> float:
    a = float(american)
    return -a / (-a + 100) if a < 0 else 100 / (a + 100)


def fair_band(american: float) -> str:
    """8rain-style fair-odds bands: -133 to -200 includes both ends, +100 to +129 includes +100."""
    a = float(american)
    if a >= 0:
        v = max(a, 100)
        return "+100 to +129" if v < 130 else "+130 to +169" if v < 170 else "+170 to +249" if v < 250 else "+250 or longer"
    v = max(-a, 100)
    return ("-100 to -132" if v < 133 else "-133 to -200" if v <= 200 else "-200 to -300" if v <= 300
            else "-300 to -500" if v <= 500 else "beyond -500")


def load_downloads(paths: list[Path]) -> pd.DataFrame:
    frames = [pd.read_csv(path, dtype=str).assign(source_file=path.name) for path in sorted(paths)]
    if not frames:
        raise SystemExit("NO_8RAIN_DOWNLOADS_FOUND")
    raw = pd.concat(frames, ignore_index=True)
    # A re-download (e.g. "_2026_10_24.1.csv") repeats the day's bets; keep one row per wager.
    return raw.drop_duplicates("Wager ID", keep="last").reset_index(drop=True)


def parse_bets(raw: pd.DataFrame) -> pd.DataFrame:
    nhl = raw[raw.League.str.upper().eq("NHL")].copy()
    parsed = nhl.Bet.fillna("").str.strip().str.extract(PROP_RE)
    nhl["market"] = parsed.market.str.lower().map(MARKETS)
    nhl.loc[nhl.Market.fillna("").str.lower().eq("h2h"), "market"] = "moneyline"
    nhl["market"] = nhl.market.fillna("other")
    nhl["side"] = parsed.side.str.upper()
    nhl["player"] = parsed.player
    nhl["line"] = pd.to_numeric(parsed.line, errors="coerce")
    nhl["slate_date"] = pd.to_datetime(nhl["Event Date"], format="%m/%d/%Y %H:%M:%S").dt.date.astype(str)
    for column, name in (("Odds", "taken"), ("No-Vig", "fair"), ("Positive EV", "ev"), ("Amount", "stake"),
                         ("$ W/L", "pnl"), ("Closing Line Value", "clv"), ("CLV Venue %", "clv_venue_pct")):
        nhl[name] = pd.to_numeric(nhl[column], errors="coerce")
    nhl["grade"] = nhl.Grade.str.upper().map({"W": "WIN", "L": "LOSS", "P": "PUSH"}).fillna("OPEN")
    nhl["p_fair"] = nhl.fair.map(lambda a: probability(a) if pd.notna(a) else np.nan)
    nhl["p_taken"] = nhl.taken.map(lambda a: probability(a) if pd.notna(a) else np.nan)
    nhl["edge"] = nhl.p_fair - nhl.p_taken
    nhl["fair_band"] = pd.Categorical(nhl.fair.map(lambda a: fair_band(a) if pd.notna(a) else "unknown"),
                                      FAIR_ORDER, ordered=True)
    nhl["model_side"] = np.where(nhl.p_fair >= 0.5, "model favorite", "model underdog")
    nhl["price_type"] = np.where(nhl.taken >= 100, "plus money", "minus money")
    nhl["ev_band"] = pd.cut(nhl.ev, EV_BANDS, labels=EV_LABELS, right=False)
    return nhl.rename(columns={"Wager ID": "wager_id", "Book": "book", "Bet": "bet"})


def attach_prediction_prices(bets: pd.DataFrame) -> pd.DataFrame:
    """The best price at the operator's books before the prediction, from the latest weekly price view."""
    from backend.nhl.eightrain_adapter import normalize_player_name
    views = sorted(PRICE_VIEW.glob("weekly_*/priced_rows.csv"))
    out = bets.assign(prediction_price=np.nan)
    if not views:
        return out
    priced = pd.read_csv(views[-1])
    priced = priced[priced.price_scope.eq("my_books")]
    ids = []
    for day in sorted(set(bets.slate_date)):
        path = NAMES / f"names_{day}.csv"
        if path.is_file():
            names = pd.read_csv(path, usecols=["player_id", "full_name", "game_date"])
            ids.append(names.assign(slate_date=day))
    if not ids:
        return out
    names = pd.concat(ids)
    names["key"] = names.full_name.map(normalize_player_name)
    names = names.drop_duplicates(["slate_date", "key"], keep=False)  # same-name players bind to nobody
    out["key"] = out.player.fillna("").map(normalize_player_name)
    out = out.merge(names[["slate_date", "key", "player_id"]], on=["slate_date", "key"], how="left")
    view = priced.rename(columns={"market": "view_market", "american_price": "prediction_price_view"})
    out = out.merge(view[["slate_date", "player_id", "view_market", "line", "side", "prediction_price_view"]],
                    left_on=["slate_date", "player_id", "market", "line", "side"],
                    right_on=["slate_date", "player_id", "view_market", "line", "side"], how="left")
    out["prediction_price"] = out.prediction_price_view
    return out.drop(columns=["key", "view_market", "prediction_price_view"])


def record(frame: pd.DataFrame) -> pd.Series:
    settled = frame[frame.grade.isin(["WIN", "LOSS", "PUSH"])]
    wins, losses = int(settled.grade.eq("WIN").sum()), int(settled.grade.eq("LOSS").sum())
    pushes = int(settled.grade.eq("PUSH").sum())
    staked = settled.stake.sum()
    decided = settled[settled.grade.isin(["WIN", "LOSS"])]
    return pd.Series({
        "bets": len(settled), "W-L-P": f"{wins}-{losses}-{pushes}",
        "win %": round(100 * wins / (wins + losses), 1) if wins + losses else np.nan,
        "model %": round(100 * decided.p_fair.mean(), 1) if len(decided) else np.nan,
        "price %": round(100 * decided.p_taken.mean(), 1) if len(decided) else np.nan,
        "mean EV": round(100 * settled.ev.mean(), 1) if len(settled) else np.nan,
        "staked": round(staked, 2), "P/L": round(settled.pnl.sum(), 2),
        "ROI %": round(100 * settled.pnl.sum() / staked, 1) if staked else np.nan,
    })


def table(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    return frame.groupby(keys, dropna=False, observed=True).apply(record, include_groups=False).reset_index()


def markdown(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return ["", "None yet."]
    cols = list(df.columns)
    return ["", "| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)] + [
        "| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |" for row in df.itertuples(index=False)]


def build(bets: pd.DataFrame, files: list[str]) -> tuple[str, dict]:
    props = bets[bets.market.isin(["sog", "points", "saves"])]
    plus = props[props.price_type.eq("plus money")]
    compared = props.dropna(subset=["prediction_price"])
    moved = compared.assign(better=compared.taken.map(lambda a: 1 / probability(a))
                            > compared.prediction_price.map(lambda a: 1 / probability(a)))
    clv = props.dropna(subset=["clv"])
    first, last = bets.slate_date.min(), bets.slate_date.max()
    lines = [f"# 8rain paper bets — NHL ({first} to {last})", "",
             f"From {len(files)} downloads ({', '.join(files)}); repeated wager IDs counted once. "
             "Fair odds are the model's (8rain's No-Vig column); EV is at the price taken; ROI is P/L over "
             "stake. Small groups move by chance: about +/-10 points of ROI at 100 bets.", "",
             "## By market"]
    lines += markdown(table(bets, ["market"]))
    lines += ["", "## Props by side and line"] + markdown(table(props, ["market", "side", "line"]))
    lines += ["", "## Props by model fair odds"] + markdown(table(props, ["market", "model_side", "fair_band"]))
    lines += ["", "## Props by EV at the price taken"] + markdown(table(props, ["market", "ev_band"]))
    lines += ["", "## Plus-money prices taken",
              "Plus money at the book, split by whether the model also had that side as an underdog."]
    lines += markdown(table(plus, ["market", "model_side", "side"]))
    lines += ["", "## Price taken vs the price before the prediction",
              "Same pick in the weekly plus-money price view (current SOG and Points models; best price at the "
              f"operator's books before the prediction). {len(compared)} of {len(props)} prop bets matched; "
              f"price taken was better on {int(moved.better.sum())}, worse or equal on {int((~moved.better).sum())}."]
    lines += ["", "## Closing-line value",
              f"8rain recorded closing-line value on {len(clv)} prop bets; mean {100 * clv.clv.mean():+.2f}% "
              f"({int((clv.clv > 0).sum())} positive, {int((clv.clv < 0).sum())} negative)." if len(clv) else
              "No closing-line values recorded yet."]
    lines += ["", "## By book"] + markdown(table(bets, ["book"]))
    summary = {"first_slate": first, "last_slate": last, "files": files, "nhl_bets": int(len(bets)),
               "overall": record(bets).to_dict(), "plus_money": record(plus).to_dict() if len(plus) else None,
               "matched_prediction_prices": int(len(compared)), "clv_bets": int(len(clv))}
    return "\n".join(lines) + "\n", summary


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--downloads", type=Path, default=Path.home() / "Downloads")
    args = ap.parse_args()
    paths = sorted(args.downloads.expanduser().glob("8rainstation_daily_*.csv"))
    bets = attach_prediction_prices(parse_bets(load_downloads(paths)))
    report, summary = build(bets, [p.name for p in paths])
    OUT.mkdir(parents=True, exist_ok=True)
    bets.to_csv(OUT / "bets.csv", index=False)
    (OUT / "report.md").write_text(report)
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print(f"NHL_8RAIN_BET_REPORT={OUT.relative_to(ROOT) / 'report.md'} bets={summary['nhl_bets']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
