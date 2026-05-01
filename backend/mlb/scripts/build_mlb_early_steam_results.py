#!/usr/bin/env python3
"""Join MLB early movement rows to reconcile outcomes while preserving snapshots.

Examples:
  python -m backend.mlb.scripts.build_mlb_early_steam_results \
    --movement-csv 'tmp/mlb_line_movement_*_imp.csv' \
    --reconcile-csv 'tmp/mlb_reconcile_rows_*_full_slate_mixedbook.csv' \
    --out-csv tmp/mlb_early_steam_multiday_results.csv
"""

from __future__ import annotations

import argparse
import glob
import re
from pathlib import Path
from typing import Any, Iterable, List, Sequence

import numpy as np
import pandas as pd


DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")


def _norm_text(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _norm_lower(value: Any) -> str:
    return _norm_text(value).lower()


def _resolve_col(df: pd.DataFrame, names: Sequence[str]) -> str:
    columns = {str(c).strip().lower(): str(c) for c in df.columns}
    for name in names:
        key = str(name).strip().lower()
        if key in columns:
            return columns[key]
    return ""


def _expand_inputs(patterns: Iterable[str]) -> List[Path]:
    paths: List[Path] = []
    for raw in patterns:
        text = _norm_text(raw)
        if not text:
            continue
        matches = sorted(glob.glob(text))
        if matches:
            paths.extend(Path(m) for m in matches)
        else:
            paths.append(Path(text))
    return list(dict.fromkeys(paths))


def _date_from_path(path: Path) -> str:
    match = DATE_RE.search(str(path))
    return match.group(1) if match else ""


def _read_movement(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    date_col = _resolve_col(df, ["date", "game_date", "slate_date"])
    if date_col:
        dates = pd.to_datetime(df[date_col], errors="coerce").dt.date.astype("string")
    else:
        inferred = _date_from_path(path)
        if not inferred:
            raise SystemExit(f"Could not infer date for movement CSV: {path}")
        dates = pd.Series([inferred] * len(df), index=df.index, dtype="string")

    player_col = _resolve_col(df, ["player", "player_name"])
    market_col = _resolve_col(df, ["market", "market_key"])
    line_col = _resolve_col(df, ["line"])
    side_col = _resolve_col(df, ["side", "selected_side"])
    imp_col = _resolve_col(df, ["imp_move_early", "imp_move"])
    if not all([player_col, market_col, line_col, side_col, imp_col]):
        raise SystemExit(
            f"Movement CSV missing required columns: {path}. "
            "Expected player/player_name, market/market_key, line, side, and imp_move/imp_move_early."
        )

    out = pd.DataFrame(
        {
            "player": df[player_col].map(_norm_lower),
            "market": df[market_col].map(_norm_lower),
            "line": pd.to_numeric(df[line_col], errors="coerce"),
            "side": df[side_col].map(_norm_lower),
            "imp_move_early": pd.to_numeric(df[imp_col], errors="coerce"),
            "date": dates.astype(str),
            "first_price": _numeric_col(df, ["first_price", "first_odds", "open_price", "opening_price"]),
            "second_price": _numeric_col(df, ["second_price", "second_odds", "close_price", "closing_price"]),
            "first_snapshot_tag": _text_col(df, ["first_snapshot_tag", "open_tag", "opening_snapshot_tag"]),
            "second_snapshot_tag": _text_col(df, ["second_snapshot_tag", "close_tag", "closing_snapshot_tag"]),
            "first_captured_at_utc": _text_col(df, ["first_captured_at_utc", "open_captured_at_utc"]),
            "second_captured_at_utc": _text_col(df, ["second_captured_at_utc", "close_captured_at_utc"]),
        }
    )
    out["movement_source_file"] = str(path)
    return out


def _numeric_col(df: pd.DataFrame, names: Sequence[str]) -> pd.Series:
    col = _resolve_col(df, names)
    if col:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(np.nan, index=df.index, dtype="float64")


def _text_col(df: pd.DataFrame, names: Sequence[str]) -> pd.Series:
    col = _resolve_col(df, names)
    if col:
        return df[col].map(_norm_text)
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def _read_reconcile(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    date_col = _resolve_col(df, ["game_date", "date", "slate_date"])
    player_col = _resolve_col(df, ["player_name", "player"])
    market_col = _resolve_col(df, ["market_key", "market"])
    line_col = _resolve_col(df, ["line"])
    if not all([date_col, player_col, market_col, line_col]):
        raise SystemExit(
            f"Reconcile CSV missing required columns: {path}. "
            "Expected game_date/date, player_name/player, market_key/market, and line."
        )

    out = df.copy()
    out["date"] = pd.to_datetime(out[date_col], errors="coerce").dt.date.astype("string").astype(str)
    out["__player_key"] = out[player_col].map(_norm_lower)
    out["__market_key"] = out[market_col].map(_norm_lower)
    out["__line_key"] = pd.to_numeric(out[line_col], errors="coerce").round(3)
    out["reconcile_source_file"] = str(path)
    return out


def _selected_outcome_and_pnl(rows: pd.DataFrame) -> pd.DataFrame:
    side = rows["side"].map(_norm_lower)
    over_outcome_col = _resolve_col(rows, ["actual_over_outcome"])
    under_outcome_col = _resolve_col(rows, ["actual_under_outcome"])
    over_pnl_col = _resolve_col(rows, ["pnl_over_1u"])
    under_pnl_col = _resolve_col(rows, ["pnl_under_1u"])

    over_outcome = rows[over_outcome_col].map(_norm_lower) if over_outcome_col else pd.Series([""] * len(rows), index=rows.index)
    under_outcome = rows[under_outcome_col].map(_norm_lower) if under_outcome_col else pd.Series([""] * len(rows), index=rows.index)
    over_pnl = pd.to_numeric(rows[over_pnl_col], errors="coerce") if over_pnl_col else pd.Series(np.nan, index=rows.index)
    under_pnl = pd.to_numeric(rows[under_pnl_col], errors="coerce") if under_pnl_col else pd.Series(np.nan, index=rows.index)

    rows["outcome"] = np.where(side.eq("over"), over_outcome, np.where(side.eq("under"), under_outcome, ""))
    rows["pnl"] = np.where(side.eq("over"), over_pnl, np.where(side.eq("under"), under_pnl, np.nan))
    return rows


def build_results(
    movement_paths: Iterable[Path],
    reconcile_paths: Iterable[Path],
    *,
    min_imp_move: float,
    max_imp_move: float,
) -> pd.DataFrame:
    movement_frames = [_read_movement(p) for p in movement_paths]
    reconcile_frames = [_read_reconcile(p) for p in reconcile_paths]
    if not movement_frames:
        raise SystemExit("No movement CSVs found.")
    if not reconcile_frames:
        raise SystemExit("No reconcile CSVs found.")

    movement = pd.concat(movement_frames, ignore_index=True)
    reconcile = pd.concat(reconcile_frames, ignore_index=True)

    movement = movement[movement["imp_move_early"].between(float(min_imp_move), float(max_imp_move), inclusive="both")].copy()
    movement["__player_key"] = movement["player"].map(_norm_lower)
    movement["__market_key"] = movement["market"].map(_norm_lower)
    movement["__line_key"] = pd.to_numeric(movement["line"], errors="coerce").round(3)

    merged = movement.merge(
        reconcile,
        on=["date", "__player_key", "__market_key", "__line_key"],
        how="inner",
        suffixes=("", "_reconcile"),
    )
    merged = _selected_outcome_and_pnl(merged)

    # Preserve the legacy leading movement columns, now with source snapshot provenance.
    lead_cols = [
        "player",
        "market",
        "line",
        "side",
        "first_price",
        "second_price",
        "first_snapshot_tag",
        "second_snapshot_tag",
        "first_captured_at_utc",
        "second_captured_at_utc",
        "imp_move_early",
    ]
    drop_cols = {"__player_key", "__market_key", "__line_key", "reconcile_source_file"}
    remaining = [c for c in merged.columns if c not in set(lead_cols) | drop_cols]
    out = merged[lead_cols + remaining].copy()
    return out.sort_values(["date", "market", "side", "line", "bookmaker_key", "player"], na_position="last")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--movement-csv", action="append", default=[], help="Movement CSV path or glob. Repeatable.")
    ap.add_argument("--reconcile-csv", action="append", default=[], help="Reconcile CSV path or glob. Repeatable.")
    ap.add_argument("--out-csv", default="tmp/mlb_early_steam_multiday_results.csv")
    ap.add_argument("--min-imp-move", type=float, default=0.02)
    ap.add_argument("--max-imp-move", type=float, default=0.05)
    ap.add_argument(
        "--no-preserve-existing-dates",
        action="store_true",
        help="Overwrite the output with rebuilt rows only. Default preserves existing dates not rebuilt.",
    )
    args = ap.parse_args()

    movement_patterns = args.movement_csv or ["tmp/mlb_line_movement_*_imp.csv"]
    reconcile_patterns = args.reconcile_csv or ["tmp/mlb_reconcile_rows_*_full_slate_mixedbook.csv"]
    movement_paths = _expand_inputs(movement_patterns)
    reconcile_paths = _expand_inputs(reconcile_patterns)
    missing = [str(p) for p in movement_paths + reconcile_paths if not p.exists()]
    if missing:
        raise SystemExit("Missing input file(s): " + ", ".join(missing))

    rebuilt = build_results(
        movement_paths,
        reconcile_paths,
        min_imp_move=args.min_imp_move,
        max_imp_move=args.max_imp_move,
    )
    out_csv = Path(args.out_csv)
    out = rebuilt
    preserved_rows = 0
    if out_csv.exists() and not args.no_preserve_existing_dates:
        existing = pd.read_csv(out_csv)
        if "date" in existing.columns and "date" in rebuilt.columns:
            rebuilt_dates = set(rebuilt["date"].dropna().astype(str).unique())
            preserved = existing[~existing["date"].astype(str).isin(rebuilt_dates)].copy()
            preserved_rows = len(preserved)
            out = pd.concat([preserved, rebuilt], ignore_index=True, sort=False)
            sort_cols = [c for c in ["date", "market", "side", "line", "bookmaker_key", "player"] if c in out.columns]
            if sort_cols:
                out = out.sort_values(sort_cols, na_position="last")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    print(
        "[mlb-early-steam-results] "
        f"movement_files={len(movement_paths)} reconcile_files={len(reconcile_paths)} "
        f"rebuilt_rows={len(rebuilt)} preserved_rows={preserved_rows} rows={len(out)} out_csv={out_csv}"
    )
    if not out.empty:
        print(
            "[mlb-early-steam-results] "
            f"dates={out['date'].min()}..{out['date'].max()} "
            f"first_price_populated={int(out['first_price'].notna().sum())} "
            f"second_price_populated={int(out['second_price'].notna().sum())}"
        )


if __name__ == "__main__":
    main()
