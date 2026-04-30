#!/usr/bin/env python3
"""Add user-facing regime context fields to MLB prop regime signals.

This is a decision-support artifact transformer only. It does not touch model
training, prediction generation, calibration, upload generation, or ranking.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd


DEFAULT_IN_CSV = Path("artifacts/analysis/mlb/prop_regime_validation/prop_regime_combined_signal.csv")
DEFAULT_OUT_CSV = DEFAULT_IN_CSV
DEFAULT_SUMMARY_CSV = Path(
    "artifacts/analysis/mlb/prop_regime_validation/prop_regime_context_validation_summary.csv"
)
DEFAULT_SUMMARY_MD = Path(
    "artifacts/analysis/mlb/prop_regime_validation/prop_regime_context_validation_summary.md"
)

LONG_TERM_SCORE = {
    "HOT": 2.0,
    "NEUTRAL": 0.0,
    "COLD": -2.0,
}
RECENT_DB_SCORE = {
    "HOT": 1.0,
    "NEUTRAL": 0.0,
    "COLD": -1.0,
}
EXECUTION_SCORE = {
    "HOT": 1.0,
    "SOFT HOT": 0.5,
    "NEUTRAL": 0.0,
    "COOLING": -0.5,
    "COLD": -1.0,
}


def _norm_regime(value: object) -> str:
    text = str(value or "").strip().upper().replace("_", " ")
    if text in {"", "NAN", "NONE"}:
        return "INSUFFICIENT"
    return text


def _score(mapping: dict[str, float], value: object) -> float:
    return float(mapping.get(_norm_regime(value), 0.0))


def _label(score: float) -> str:
    if score >= 2.0:
        return "Strong environment"
    if score >= 1.0:
        return "Favorable environment"
    if score >= 0.0:
        return "Mixed / neutral environment"
    if score >= -1.0:
        return "Unstable environment"
    return "Unfavorable environment"


def _phrase_long(regime: str) -> str:
    if regime == "HOT":
        return "Long-term model-pick results have been positive"
    if regime == "COLD":
        return "Long-term model-pick results have been negative"
    if regime == "NEUTRAL":
        return "Long-term model-pick results are neutral"
    return "Long-term model-pick context is limited"


def _phrase_recent(regime: str) -> str:
    if regime == "HOT":
        return "recent DB-backed results have improved"
    if regime == "COLD":
        return "recent DB-backed results have weakened"
    if regime == "NEUTRAL":
        return "recent DB-backed results are neutral"
    return "recent DB-backed sample is limited"


def _phrase_execution(regime: str) -> str:
    if regime == "HOT":
        return "rolling model-pick trend is positive"
    if regime == "SOFT HOT":
        return "rolling model-pick trend has improved recently"
    if regime == "COOLING":
        return "rolling model-pick trend has cooled"
    if regime == "COLD":
        return "rolling model-pick trend is negative"
    if regime == "NEUTRAL":
        return "rolling model-pick trend is neutral"
    return "rolling model-pick trend sample is limited"


def _explanation(row: pd.Series) -> str:
    long_term = _norm_regime(row.get("long_term_regime"))
    recent = _norm_regime(row.get("recent_regime"))
    execution = _norm_regime(row.get("execution_regime"))
    return f"{_phrase_long(long_term)}; {_phrase_recent(recent)}; {_phrase_execution(execution)}."


def _markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "(none)"
    rows = ["| " + " | ".join(map(str, df.columns)) + " |"]
    rows.append("| " + " | ".join(["---"] * len(df.columns)) + " |")
    for _, row in df.iterrows():
        vals = []
        for col in df.columns:
            value = row[col]
            if pd.isna(value):
                text = ""
            elif isinstance(value, float):
                text = f"{value:.3f}".rstrip("0").rstrip(".")
            else:
                text = str(value)
            vals.append(text.replace("|", "\\|"))
        rows.append("| " + " | ".join(vals) + " |")
    return "\n".join(rows)


def add_context_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["long_term_regime"] = out["long_term_regime"].map(_norm_regime)
    out["recent_regime"] = out["recent_regime"].map(_norm_regime)
    out["execution_regime"] = out["execution_regime"].map(_norm_regime)

    out["regime_context_score"] = (
        out["long_term_regime"].map(lambda x: _score(LONG_TERM_SCORE, x))
        + out["recent_regime"].map(lambda x: _score(RECENT_DB_SCORE, x))
        + out["execution_regime"].map(lambda x: _score(EXECUTION_SCORE, x))
    )
    out["regime_context_label"] = out["regime_context_score"].map(_label)
    out["regime_context_explanation"] = out.apply(_explanation, axis=1)
    return out


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in-csv", default=str(DEFAULT_IN_CSV))
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--summary-csv", default=str(DEFAULT_SUMMARY_CSV))
    ap.add_argument("--summary-md", default=str(DEFAULT_SUMMARY_MD))
    args = ap.parse_args(argv)

    in_csv = Path(args.in_csv)
    out_csv = Path(args.out_csv)
    summary_csv = Path(args.summary_csv)
    summary_md = Path(args.summary_md)

    df = pd.read_csv(in_csv)
    out = add_context_fields(df)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    summary_cols = [
        "prop_type",
        "long_term_regime",
        "recent_regime",
        "execution_regime",
        "regime_context_score",
        "regime_context_label",
        "regime_context_explanation",
    ]
    summary = out[summary_cols].copy()
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_csv, index=False)

    lines = [
        "# Prop Regime Context Validation Summary",
        "",
        "User-facing labels describe signal environment only. They are not calls to action.",
        "",
        _markdown_table(summary),
        "",
    ]
    summary_md.write_text("\n".join(lines), encoding="utf-8")

    print(f"[prop-regime-context] wrote {out_csv}")
    print(f"[prop-regime-context] wrote {summary_csv}")
    print(f"[prop-regime-context] wrote {summary_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
