#!/usr/bin/env python3
"""
Audit the inference feature contract for the Denali pairings ordinal SOG model.

Compares:
  - required feature names from model metadata.json
  - columns in daily exports: backend/nhl/exports/daily/sog_features/sog_features_<date>_denali.csv

Flags features as:
  - MISSING (not present in export)
  - ALL_NULL (present but all values null/blank)
  - CONSTANT (present but nunique <= 1 among non-null)
  - OK

Outputs:
  - console summary
  - CSV report (one row per feature per slate + overall rollup)

Usage examples:
  python backend/nhl/scripts/audit_ordinal_feature_contract.py \
    --metadata backend/nhl/models/latest/shots_on_goal/sog_player_denali_pairings_ordinal_v1__no_shiftcounts/ge_2/metadata.json \
    --exports-dir backend/nhl/exports/daily/sog_features \
    --pattern "sog_features_*_denali.csv" \
    --last-n 7

Notes:
  - This does NOT apply scorer aliasing/defaults.
  - It is intentionally strict and purely observational.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class FeatureStat:
    feature: str
    slate_date: str
    status: str  # MISSING | ALL_NULL | CONSTANT | OK
    nonnull_rate: Optional[float]
    nunique_nonnull: Optional[int]
    example_values: Optional[str]


def _extract_required_features(metadata: dict) -> List[str]:
    """
    Try common keys. We keep this permissive so it works with your existing metadata variants.
    """
    candidates = [
        "required_features",
        "features",
        "feature_names",
        "model_features",
        "training_features",
        "columns",
        "x_columns",
    ]
    for k in candidates:
        v = metadata.get(k)
        if isinstance(v, list) and v and all(isinstance(x, str) for x in v):
            return v

    # Sometimes nested
    for parent_k in ["model", "artifacts", "training", "data"]:
        parent = metadata.get(parent_k)
        if isinstance(parent, dict):
            for k in candidates:
                v = parent.get(k)
                if isinstance(v, list) and v and all(isinstance(x, str) for x in v):
                    return v

    raise KeyError(
        "Could not find feature list in metadata.json. "
        "Searched keys: required_features/features/feature_names/model_features/training_features/columns/x_columns "
        "at top-level and under model/artifacts/training/data."
    )


def _list_recent_exports(exports_dir: Path, pattern: str, last_n: int) -> List[Tuple[str, Path]]:
    files = sorted(exports_dir.glob(pattern))
    dated: List[Tuple[str, Path]] = []
    for p in files:
        m = DATE_RE.search(p.name)
        if not m:
            continue
        dated.append((m.group(1), p))

    # sort by date string (YYYY-MM-DD safe lexicographically)
    dated.sort(key=lambda t: t[0])
    if last_n > 0:
        dated = dated[-last_n:]
    return dated


def _analyze_one_export(required: List[str], slate_date: str, csv_path: Path) -> List[FeatureStat]:
    df = pd.read_csv(csv_path)
    cols = set(df.columns)

    out: List[FeatureStat] = []
    for f in required:
        if f not in cols:
            out.append(
                FeatureStat(
                    feature=f,
                    slate_date=slate_date,
                    status="MISSING",
                    nonnull_rate=None,
                    nunique_nonnull=None,
                    example_values=None,
                )
            )
            continue

        s = df[f]
        nonnull = s.notna()
        nonnull_rate = float(nonnull.mean()) if len(s) else 0.0

        s_nonnull = s[nonnull]
        if s_nonnull.empty:
            out.append(
                FeatureStat(
                    feature=f,
                    slate_date=slate_date,
                    status="ALL_NULL",
                    nonnull_rate=nonnull_rate,
                    nunique_nonnull=0,
                    example_values=None,
                )
            )
            continue

        nunique = int(s_nonnull.nunique(dropna=True))
        if nunique <= 1:
            # grab up to 3 examples (stringify safely)
            examples = s_nonnull.astype(str).head(3).tolist()
            out.append(
                FeatureStat(
                    feature=f,
                    slate_date=slate_date,
                    status="CONSTANT",
                    nonnull_rate=nonnull_rate,
                    nunique_nonnull=nunique,
                    example_values=" | ".join(examples),
                )
            )
            continue

        examples = s_nonnull.astype(str).head(3).tolist()
        out.append(
            FeatureStat(
                feature=f,
                slate_date=slate_date,
                status="OK",
                nonnull_rate=nonnull_rate,
                nunique_nonnull=nunique,
                example_values=" | ".join(examples),
            )
        )

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--metadata", required=True, help="Path to model metadata.json containing training feature list.")
    ap.add_argument("--exports-dir", default="backend/nhl/exports/daily/sog_features", help="Daily export directory.")
    ap.add_argument("--pattern", default="sog_features_*_denali.csv", help="Glob pattern under exports-dir.")
    ap.add_argument("--last-n", type=int, default=7, help="How many most recent slates to scan (by date in filename).")
    ap.add_argument("--out-dir", default="backend/nhl/exports/audits", help="Where to write the audit CSVs.")
    args = ap.parse_args()

    meta_path = Path(args.metadata)
    exports_dir = Path(args.exports_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    metadata = json.loads(meta_path.read_text())
    required = _extract_required_features(metadata)
    required = sorted(set(required))

    dated_exports = _list_recent_exports(exports_dir, args.pattern, args.last_n)
    if not dated_exports:
        raise SystemExit(f"No exports found in {exports_dir} matching {args.pattern}")

    all_rows: List[FeatureStat] = []
    for slate_date, csv_path in dated_exports:
        rows = _analyze_one_export(required, slate_date, csv_path)
        all_rows.extend(rows)

    # --- per-slate report ---
    df = pd.DataFrame([r.__dict__ for r in all_rows])
    per_slate_path = out_dir / "audit_ordinal_features_per_slate.csv"
    df.to_csv(per_slate_path, index=False)

    # --- overall rollup: worst status seen across slates ---
    # priority: MISSING > ALL_NULL > CONSTANT > OK
    pri = {"MISSING": 3, "ALL_NULL": 2, "CONSTANT": 1, "OK": 0}
    df["status_pri"] = df["status"].map(pri).fillna(0).astype(int)

    roll = (
        df.sort_values(["feature", "status_pri"], ascending=[True, False])
        .groupby("feature", as_index=False)
        .first()
        .drop(columns=["status_pri"])
    )
    roll_path = out_dir / "audit_ordinal_features_rollup.csv"
    roll.to_csv(roll_path, index=False)

    # --- console summary ---
    counts = roll["status"].value_counts().to_dict()
    print("=== Ordinal feature contract audit ===")
    print(f"metadata: {meta_path}")
    print(f"exports : {exports_dir} ({len(dated_exports)} slates scanned)")
    print(f"out     : {out_dir}")
    print("")
    print("Rollup counts (worst status across scanned slates):")
    for k in ["MISSING", "ALL_NULL", "CONSTANT", "OK"]:
        print(f"  {k:9s}: {counts.get(k, 0)}")

    # list the “bad” features explicitly (this is your first-domino list)
    bad = roll[roll["status"].isin(["MISSING", "ALL_NULL", "CONSTANT"])].copy()
    bad = bad.sort_values(["status", "feature"])
    print("")
    print("Features NOT reliably produced (worst-case across scanned slates):")
    for _, r in bad.iterrows():
        print(f"- {r['feature']}  [{r['status']}]  nonnull_rate={r['nonnull_rate']}  nunique={r['nunique_nonnull']}")

    print("")
    print(f"Wrote:\n  {roll_path}\n  {per_slate_path}")


if __name__ == "__main__":
    main()
