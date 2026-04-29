#!/usr/bin/env python3
"""Build a prop-policy hybrid MLB book upload from base and weighted variants."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd


UPLOAD_COLUMNS = [
    "LEAGUE",
    "DATE",
    "HOME",
    "AWAY",
    "DOUBLEHEADER",
    "SECTION",
    "MARKET",
    "SELECTOR",
    "POINT",
    "SIDE",
    "WIN %",
]

KEY_COLUMNS = [c for c in UPLOAD_COLUMNS if c != "WIN %"]

MARKET_TO_PROP: Dict[str, str] = {
    "batter_hits": "hits",
    "batter_singles": "singles",
    "batter_bases": "total_bases",
    "batter_h+r+rbi": "hits_runs_rbis",
    "pitcher_strikeouts": "strikeouts_pitching",
    "pitcher_outs": "outs_recorded",
}

PROP_POLICY: Dict[str, str] = {
    "total_bases": "weighted",
    "hits": "base",
    "singles": "base",
}


def _load_upload(path: Path, *, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing {label} upload CSV: {path}")
    df = pd.read_csv(path)
    missing = [c for c in UPLOAD_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label}: missing upload columns: {missing}")
    return df[UPLOAD_COLUMNS].copy()


def _key_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in KEY_COLUMNS:
        out[f"__key_{col}"] = out[col].astype(str).str.strip()
    return out


def _assert_no_duplicate_keys(df: pd.DataFrame, *, label: str) -> None:
    dupes = df[df.duplicated(KEY_COLUMNS, keep=False)]
    if not dupes.empty:
        sample = dupes[KEY_COLUMNS].head(10).to_dict(orient="records")
        raise RuntimeError(f"{label}: duplicate upload keys detected; sample={sample}")


def _prop_for_market(market: object) -> str:
    return MARKET_TO_PROP.get(str(market or "").strip(), "unknown")


def build_hybrid(*, base_csv: Path, weighted_csv: Path, out_csv: Path) -> pd.DataFrame:
    base = _load_upload(base_csv, label="base")
    weighted = _load_upload(weighted_csv, label="weighted")
    _assert_no_duplicate_keys(base, label="base")
    _assert_no_duplicate_keys(weighted, label="weighted")

    weighted_keyed = _key_frame(weighted)
    weighted_lookup = {
        tuple(row[f"__key_{col}"] for col in KEY_COLUMNS): row
        for _, row in weighted_keyed.iterrows()
    }

    hybrid_rows: List[dict] = []
    counts: Dict[str, int] = {"base": 0, "weighted": 0, "weighted_missing_fallback_base": 0}
    by_prop_source: Dict[str, Dict[str, int]] = {}

    for _, base_row in base.iterrows():
        prop_type = _prop_for_market(base_row["MARKET"])
        source = PROP_POLICY.get(prop_type, "base")
        key = tuple(str(base_row[col]).strip() for col in KEY_COLUMNS)
        selected = base_row
        actual_source = "base"
        if source == "weighted":
            weighted_row = weighted_lookup.get(key)
            if weighted_row is not None:
                selected = weighted_row
                actual_source = "weighted"
            else:
                counts["weighted_missing_fallback_base"] += 1
        counts[actual_source] += 1
        by_prop_source.setdefault(prop_type, {"base": 0, "weighted": 0})
        by_prop_source[prop_type][actual_source] += 1
        hybrid_rows.append({col: selected[col] for col in UPLOAD_COLUMNS})

    hybrid = pd.DataFrame(hybrid_rows, columns=UPLOAD_COLUMNS)
    _assert_no_duplicate_keys(hybrid, label="hybrid")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    hybrid.to_csv(out_csv, index=False)

    print("[upload] hybrid policy:")
    print("[upload]   total_bases -> weighted")
    print("[upload]   hits -> base")
    print("[upload]   singles -> base")
    print("[upload]   others -> base")
    print(
        "[upload] hybrid rows "
        f"base_input={len(base)} weighted_input={len(weighted)} output={len(hybrid)} "
        f"base_selected={counts['base']} weighted_selected={counts['weighted']} "
        f"weighted_missing_fallback_base={counts['weighted_missing_fallback_base']}"
    )
    for prop_type in sorted(by_prop_source):
        source_counts = by_prop_source[prop_type]
        print(
            "[upload] hybrid prop "
            f"{prop_type}: base={source_counts.get('base', 0)} weighted={source_counts.get('weighted', 0)}"
        )
    print(f"[upload] wrote hybrid upload: {out_csv}")
    return hybrid


def main() -> int:
    ap = argparse.ArgumentParser(description="Build MLB hybrid upload from base and weighted upload variants.")
    ap.add_argument("--base-csv", required=True)
    ap.add_argument("--weighted-csv", required=True)
    ap.add_argument("--out-csv", required=True)
    args = ap.parse_args()

    build_hybrid(
        base_csv=Path(args.base_csv).expanduser(),
        weighted_csv=Path(args.weighted_csv).expanduser(),
        out_csv=Path(args.out_csv).expanduser(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
