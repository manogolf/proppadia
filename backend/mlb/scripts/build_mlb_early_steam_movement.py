#!/usr/bin/env python3
"""Build MLB early-steam movement rows from local daily OddsAPI snapshots."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _clean_text(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _snapshot_tag(path: Path) -> str:
    return path.stem


def _date_from_path(path: Path) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", str(path))
    return match.group(1) if match else ""


def _american_implied(price: Any) -> float:
    odds = pd.to_numeric(price, errors="coerce")
    if pd.isna(odds) or float(odds) == 0:
        return np.nan
    odds = float(odds)
    if odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    return 100.0 / (odds + 100.0)


def _read_snapshot(path: Path) -> pd.DataFrame:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    captured_at = pd.to_datetime(data.get("captured_at_utc"), errors="coerce", utc=True)
    tag = _snapshot_tag(path)
    rows: list[dict[str, Any]] = []
    for event in data.get("events", []) or []:
        event_id = event.get("id")
        home_team = event.get("home_team")
        away_team = event.get("away_team")
        commence_time = event.get("commence_time")
        for bookmaker in event.get("bookmakers", []) or []:
            bookmaker_key = bookmaker.get("key")
            for market in bookmaker.get("markets", []) or []:
                market_key = market.get("key")
                for outcome in market.get("outcomes", []) or []:
                    side = _clean_text(outcome.get("name")).lower()
                    if side not in {"over", "under"}:
                        continue
                    rows.append(
                        {
                            "event_id": event_id,
                            "home_team": home_team,
                            "away_team": away_team,
                            "commence_time": commence_time,
                            "player": outcome.get("description"),
                            "market": market_key,
                            "line": outcome.get("point"),
                            "side": side,
                            "bookmaker_key": bookmaker_key,
                            "snapshot_tag": tag,
                            "captured_at_utc": captured_at,
                            "price": outcome.get("price"),
                        }
                    )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    return out


def build_movement(snapshot_paths: list[Path], *, slate_date: str) -> pd.DataFrame:
    if len(snapshot_paths) < 2:
        raise SystemExit(f"Need at least 2 local_daily snapshots; found {len(snapshot_paths)}.")

    frames = [_read_snapshot(path) for path in snapshot_paths]
    raw = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True, sort=False)
    if raw.empty:
        return pd.DataFrame()
    raw = raw.dropna(subset=["captured_at_utc", "line", "price"]).copy()
    if raw.empty:
        return pd.DataFrame()

    snapshot_order = (
        raw[["snapshot_tag", "captured_at_utc"]]
        .drop_duplicates()
        .sort_values(["captured_at_utc", "snapshot_tag"], kind="mergesort")
        .reset_index(drop=True)
    )
    if len(snapshot_order) < 2:
        raise SystemExit(f"Need at least 2 distinct local_daily snapshots; found {len(snapshot_order)}.")
    first_tag = snapshot_order.loc[0, "snapshot_tag"]
    second_tag = snapshot_order.loc[1, "snapshot_tag"]
    first_ts = snapshot_order.loc[0, "captured_at_utc"]
    second_ts = snapshot_order.loc[1, "captured_at_utc"]

    keys = ["event_id", "home_team", "away_team", "player", "market", "line", "side", "bookmaker_key"]
    first = raw[raw["snapshot_tag"].eq(first_tag)].copy()
    second = raw[raw["snapshot_tag"].eq(second_tag)].copy()
    merged = first.merge(second, on=keys, how="inner", suffixes=("_first", "_second"))
    if merged.empty:
        return pd.DataFrame()

    first_price = pd.to_numeric(merged["price_first"], errors="coerce")
    second_price = pd.to_numeric(merged["price_second"], errors="coerce")
    first_imp = first_price.map(_american_implied)
    second_imp = second_price.map(_american_implied)
    out = pd.DataFrame(
        {
            "date": slate_date,
            "event_id": merged["event_id"],
            "home_team": merged["home_team"],
            "away_team": merged["away_team"],
            "player": merged["player"],
            "market": merged["market"],
            "line": merged["line"],
            "side": merged["side"],
            "bookmaker_key": merged["bookmaker_key"],
            "n_snapshots": int(len(snapshot_order)),
            "first_snapshot_tag": first_tag,
            "second_snapshot_tag": second_tag,
            "open_tag": first_tag,
            "close_tag": second_tag,
            "first_captured_at_utc": first_ts,
            "second_captured_at_utc": second_ts,
            "open_captured_at_utc": first_ts,
            "close_captured_at_utc": second_ts,
            "first_price": first_price,
            "second_price": second_price,
            "open_price": first_price,
            "close_price": second_price,
            "movement": second_price - first_price,
            "first_imp": first_imp,
            "second_imp": second_imp,
            "open_imp": first_imp,
            "close_imp": second_imp,
            "imp_move_early": second_imp - first_imp,
            "imp_move": second_imp - first_imp,
        }
    )
    return out.sort_values(["market", "side", "line", "bookmaker_key", "player"], na_position="last")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Slate date YYYY-MM-DD.")
    parser.add_argument("--odds-history-root", default="backend/mlb/exports/odds_history")
    parser.add_argument("--snapshot-glob", default="", help="Optional explicit snapshot glob.")
    parser.add_argument("--out-csv", default="")
    args = parser.parse_args()

    date_text = _clean_text(args.date)
    day_dir = Path(args.odds_history_root) / date_text
    pattern = args.snapshot_glob or str(day_dir / "odds_mlb_playerprops__local_daily_*.json")
    paths = sorted(Path(p) for p in __import__("glob").glob(pattern))
    missing_date = [p for p in paths if _date_from_path(p) and _date_from_path(p) != date_text]
    if missing_date:
        raise SystemExit(f"Snapshot glob resolved files outside --date={date_text}: {missing_date[:3]}")
    if len(paths) < 2:
        raise SystemExit(f"Need at least 2 local_daily snapshots for {date_text}; found {len(paths)} using {pattern}")

    out_csv = Path(args.out_csv or f"tmp/mlb_line_movement_{date_text}_mixedbook_imp.csv")
    movement = build_movement(paths, slate_date=date_text)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    movement.to_csv(out_csv, index=False)

    print(
        "[mlb-early-steam-movement] "
        f"date={date_text} snapshots={len(paths)} rows={len(movement)} out_csv={out_csv}"
    )
    if not movement.empty:
        early = movement[pd.to_numeric(movement["imp_move_early"], errors="coerce").between(0.02, 0.05, inclusive="both")]
        print(
            "[mlb-early-steam-movement] "
            f"first={movement['first_snapshot_tag'].iloc[0]} second={movement['second_snapshot_tag'].iloc[0]} "
            f"early_steam_rows_002_005={len(early)}"
        )
        if not early.empty:
            print(early.groupby(["market", "side"]).size().sort_values(ascending=False).head(30).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
