#!/usr/bin/env python3
"""Export Quick Card hits rows to the external tool upload schema.

This adapter does not use rank-bucket mapping. It converts the Quick Card's
own confidence/score field into the upload WIN % column and reuses the proven
MLB book-upload formatting helpers for market and team codes.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.mlb.scripts import export_mlb_book_upload as book_upload


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

WIN_VALUE_COLUMNS = [
    "quick_card_win_pct",
    "win_pct",
    "confidence",
    "model_prob",
    "score",
    "rank_score",
]


def _clean(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _compact_date(value: Any) -> str:
    key = _date_key(value)
    return key.replace("-", "") if key else ""


def _num(value: Any) -> float | None:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(val) if pd.notna(val) else None


def _format_point(value: Any) -> Any:
    val = _num(value)
    if val is None:
        return ""
    if abs(val - round(val)) < 1e-9:
        return int(round(val))
    return val


def _norm_prop(value: Any) -> str:
    text = _clean(value).lower().replace(" ", "_")
    aliases = {
        "pitcher_outs": "outs_recorded",
        "pitching_outs": "outs_recorded",
        "outs_recorded": "outs_recorded",
        "pitcher_strikeouts": "strikeouts_pitching",
        "strikeouts_pitching": "strikeouts_pitching",
    }
    return aliases.get(text, text)


def _upload_market(value: Any) -> str:
    return book_upload._normalize_upload_market(
        raw_market="",
        prop_type=_norm_prop(value),
        market_map=book_upload.DEFAULT_MARKET_BY_PROP,
    )


def _pick_win_column(df: pd.DataFrame) -> str | None:
    for col in WIN_VALUE_COLUMNS:
        if col in df.columns and pd.to_numeric(df[col], errors="coerce").notna().any():
            return col
    print("Skipping Quick Card upload: no WIN % available")
    print("Available columns:")
    for col in df.columns:
        print(f"- {col}")
    return None


def _format_win_value(value: Any) -> Any:
    val = _num(value)
    if val is None:
        return ""
    if 0.0 <= val <= 1.0:
        return round(val, 6)
    # Historical Quick Card variants may already carry 0-100 percentages.
    return round(val / 100.0, 6)


def export_quick_card(input_csv: Path, out_csv: Path, date_value: str, diagnostics_csv: Path | None = None) -> dict[str, Any]:
    if not input_csv.exists():
        raise SystemExit(f"missing Quick Card hits input: {input_csv}")
    df = pd.read_csv(input_csv, low_memory=False)
    if df.empty:
        if out_csv.exists():
            out_csv.unlink()
        print("Skipping Quick Card upload: no WIN % available")
        print("Available columns:")
        for col in df.columns:
            print(f"- {col}")
        return {"rows": 0, "win_column": "", "missing_home": 0, "missing_away": 0, "missing_win": 0, "skipped": True}

    required = {"date", "player_id", "prop_type", "side", "line"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"{input_csv} missing required columns: {missing}. Available columns: {list(df.columns)}")

    home_col = "home_upload" if "home_upload" in df.columns else "home_team_code"
    away_col = "away_upload" if "away_upload" in df.columns else "away_team_code"
    if home_col not in df.columns or away_col not in df.columns:
        raise SystemExit(
            f"{input_csv} missing HOME/AWAY source columns. Available columns: {list(df.columns)}"
        )

    win_col = _pick_win_column(df)
    if win_col is None:
        if out_csv.exists():
            out_csv.unlink()
        return {"rows": 0, "win_column": "", "missing_home": 0, "missing_away": 0, "missing_win": 0, "skipped": True}
    date_series = df["date"].map(_date_key)
    if date_value:
        df = df[date_series.eq(date_value)].copy()
        date_series = df["date"].map(_date_key)

    exported_decimal = df[win_col].map(_format_win_value)
    upload = pd.DataFrame(
        {
            "LEAGUE": "MLB",
            "DATE": date_series.map(_compact_date),
            "HOME": df[home_col].map(book_upload._normalize_upload_team_code),
            "AWAY": df[away_col].map(book_upload._normalize_upload_team_code),
            "DOUBLEHEADER": "",
            "SECTION": "player_prop",
            "MARKET": df["prop_type"].map(_upload_market),
            "SELECTOR": pd.to_numeric(df["player_id"], errors="coerce").astype("Int64"),
            "POINT": df["line"].map(_format_point),
            "SIDE": df["side"].astype(str).str.strip().str.lower(),
            "WIN %": exported_decimal,
        }
    )[UPLOAD_COLUMNS]
    diagnostics = df.copy()
    diagnostics["win_pct_raw_source"] = pd.to_numeric(df[win_col], errors="coerce")
    diagnostics["win_pct_exported_decimal"] = pd.to_numeric(exported_decimal, errors="coerce")
    diagnostics["exported_side"] = upload["SIDE"].values
    diagnostics["probability_semantics"] = "P(exported SIDE wins)"

    home_missing = upload["HOME"].isna() | upload["HOME"].astype(str).str.strip().eq("")
    away_missing = upload["AWAY"].isna() | upload["AWAY"].astype(str).str.strip().eq("")
    win_missing = upload["WIN %"].isna() | upload["WIN %"].astype(str).str.strip().eq("")
    if home_missing.any() or away_missing.any() or win_missing.any():
        sample = upload[home_missing | away_missing | win_missing].head(10)
        print("Quick Card upload has missing required values; sample:", file=sys.stderr)
        print(sample.to_string(index=False), file=sys.stderr)
        raise SystemExit(3)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    upload.to_csv(out_csv, index=False)
    if diagnostics_csv is not None:
        diag_cols = [
            "date",
            "player_id",
            "player_name",
            "prop_type",
            "side",
            "line",
            "score",
            "rank_score",
            "win_pct_raw_source",
            "win_pct_exported_decimal",
            "exported_side",
            "probability_semantics",
        ]
        diagnostics_csv.parent.mkdir(parents=True, exist_ok=True)
        diagnostics[[c for c in diag_cols if c in diagnostics.columns]].to_csv(diagnostics_csv, index=False)
    return {
        "rows": int(len(upload)),
        "win_column": win_col,
        "missing_home": int(home_missing.sum()),
        "missing_away": int(away_missing.sum()),
        "missing_win": int(win_missing.sum()),
        "skipped": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Quick Card hits rows to external tool upload CSV.")
    parser.add_argument("--date", required=True, help="Slate date YYYY-MM-DD.")
    parser.add_argument("--in-csv", type=Path, default=None)
    parser.add_argument("--out-csv", type=Path, default=None)
    parser.add_argument("--diagnostics-csv", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    date_value = _date_key(args.date)
    if not date_value:
        raise SystemExit("--date YYYY-MM-DD is required")
    if args.in_csv:
        in_csv = args.in_csv
    else:
        dated_in = Path(f"backend/mlb/exports/model_v2/lanes/today/{date_value}/quick_card_hits_{date_value}.csv")
        legacy_in = Path(f"backend/mlb/exports/model_v2/lanes/today/quick_card_hits_{date_value}.csv")
        in_csv = dated_in if dated_in.exists() else legacy_in
    upload_dir = Path(f"backend/mlb/exports/model_v2/upload/{date_value}")
    out_csv = args.out_csv or upload_dir / f"quick_card_tool_upload_{date_value}.csv"
    diagnostics_csv = args.diagnostics_csv or upload_dir / f"quick_card_tool_upload_diagnostics_{date_value}.csv"
    summary = export_quick_card(in_csv, out_csv, date_value, diagnostics_csv)
    if summary.get("skipped"):
        return
    print(f"Wrote {out_csv}")
    print(
        "summary "
        f"rows={summary['rows']} win_column={summary['win_column']} "
        f"missing_home={summary['missing_home']} missing_away={summary['missing_away']} "
        f"missing_win={summary['missing_win']}"
    )


if __name__ == "__main__":
    main()
