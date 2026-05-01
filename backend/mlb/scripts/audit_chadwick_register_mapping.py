#!/usr/bin/env python3
"""Audit Chadwick Register Retrosheet -> MLBAM mapping coverage.

Read-only utility. No database writes.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_REGISTER = "backend/mlb/data/raw/retrosheet/chadwick_register/people.csv"
DEFAULT_OUT = "tmp/chadwick_register_mapping_audit.csv"

RECENT_PITCHER_NAME_HINTS = [
    ("cole", "gerrit"),
    ("scherzer", "max"),
    ("verlander", "justin"),
    ("de grom", "jacob"),
    ("degrom", "jacob"),
    ("strider", "spencer"),
    ("burnes", "corbin"),
    ("wheeler", "zack"),
    ("skubal", "tarik"),
    ("gausman", "kevin"),
    ("ober", "bailey"),
    ("ragans", "cole"),
]


def _clean_series(series: pd.Series) -> pd.Series:
    text = series.fillna("").astype(str).str.strip()
    return text.mask(text.str.lower().isin({"nan", "none", "null"}), "")


def _duplicate_nonblank_count(series: pd.Series) -> int:
    clean = _clean_series(series)
    clean = clean[clean.ne("")]
    return int(clean.duplicated(keep=False).sum())


def _sample_recent_pitchers(df: pd.DataFrame) -> pd.DataFrame:
    if not {"name_last", "name_first", "key_retro", "key_mlbam"}.issubset(df.columns):
        return pd.DataFrame()

    work = df.copy()
    work["name_last_norm"] = _clean_series(work["name_last"]).str.lower()
    work["name_first_norm"] = _clean_series(work["name_first"]).str.lower()
    work["key_retro"] = _clean_series(work["key_retro"])
    work["key_mlbam"] = _clean_series(work["key_mlbam"])

    samples = []
    for last, first in RECENT_PITCHER_NAME_HINTS:
        exact = work[
            work["name_last_norm"].eq(last)
            & work["name_first_norm"].eq(first)
            & work["key_retro"].ne("")
            & work["key_mlbam"].ne("")
        ].copy()
        if not exact.empty:
            samples.append(exact.head(1))
    if not samples:
        return pd.DataFrame()

    out = pd.concat(samples, ignore_index=True)
    cols = [
        c
        for c in [
            "name_first",
            "name_last",
            "key_retro",
            "key_mlbam",
            "mlb_played_first",
            "mlb_played_last",
            "pro_played_first",
            "pro_played_last",
        ]
        if c in out.columns
    ]
    return out[cols]


def _print_metric(label: str, value: int) -> None:
    print(f"{label}: {value:,}")


def build_audit(register_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(register_csv, dtype=str)
    required = {"key_retro", "key_mlbam"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"Chadwick Register missing required columns: {missing}")

    key_retro = _clean_series(df["key_retro"])
    key_mlbam = _clean_series(df["key_mlbam"])
    both = key_retro.ne("") & key_mlbam.ne("")

    metrics = pd.DataFrame(
        [
            {"metric": "total_rows", "value": int(len(df))},
            {"metric": "rows_with_key_retro", "value": int(key_retro.ne("").sum())},
            {"metric": "rows_with_key_mlbam", "value": int(key_mlbam.ne("").sum())},
            {"metric": "rows_with_both_key_retro_and_key_mlbam", "value": int(both.sum())},
            {"metric": "duplicate_key_retro_rows", "value": _duplicate_nonblank_count(key_retro)},
            {"metric": "duplicate_key_mlbam_rows", "value": _duplicate_nonblank_count(key_mlbam)},
            {"metric": "unique_key_retro", "value": int(key_retro[key_retro.ne("")].nunique())},
            {"metric": "unique_key_mlbam", "value": int(key_mlbam[key_mlbam.ne("")].nunique())},
        ]
    )
    samples = _sample_recent_pitchers(df)
    return metrics, samples


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register-csv", default=DEFAULT_REGISTER)
    parser.add_argument("--out-csv", default=DEFAULT_OUT)
    parser.add_argument("--no-out-csv", action="store_true")
    args = parser.parse_args()

    register_csv = Path(args.register_csv)
    if not register_csv.exists():
        raise SystemExit(f"Chadwick Register CSV not found: {register_csv}")

    metrics, samples = build_audit(register_csv)

    print(f"[chadwick-register-audit] register_csv={register_csv}")
    for _, row in metrics.iterrows():
        _print_metric(str(row["metric"]), int(row["value"]))

    if not samples.empty:
        print("[chadwick-register-audit] sample mapped active/recent MLB pitchers")
        print(samples.to_string(index=False))
    else:
        print("[chadwick-register-audit] sample mapped active/recent MLB pitchers: none found by name hints")

    if not args.no_out_csv:
        out_csv = Path(args.out_csv)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        metrics.to_csv(out_csv, index=False)
        print(f"[chadwick-register-audit] out_csv={out_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
