#!/usr/bin/env python3
"""
backend/nhl/scripts/reconcile_sog_predictions.py

Evidence-first reconciler for NHL SOG predictions.

- Uses sog_predictions.csv as the spine (wide format).
- Unpivots to long rows by line (0.5/1.5/2.5/3.5) WITHOUT renaming any existing columns.
- Optionally overlays:
    - sog_predictions_wide_calibrated.csv (wide calibrated)
    - sog_predictions_{1_5,2_5,3_5}.csv (line-specific)
- Writes:
    - observed_long__run_{...}.csv
    - overlay__run_{...}.csv
    - summary__run_{...}.json
    - report__run_{...}.md
    - log__run_{...}.jsonl

Joins ONLY on IDs + game_date + line:
  (player_id, game_id, game_date, line)

No semantics are assumed for any text team column (name/team/etc): we only report what exists.

Run example:
  python backend/nhl/scripts/reconcile_sog_predictions.py \
    --spine backend/nhl/data/processed/sog_predictions.csv \
    --calibrated backend/nhl/data/processed/sog_predictions_wide_calibrated.csv \
    --linefiles \
      backend/nhl/data/processed/sog_predictions_1_5.csv \
      backend/nhl/data/processed/sog_predictions_2_5.csv \
      backend/nhl/data/processed/sog_predictions_3_5.csv \
    --outdir backend/nhl/data/processed/reconcile
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore


ET_TZ = "America/New_York"


# ----------------------------
# Logging (JSONL)
# ----------------------------
@dataclass
class Event:
    ts_et: str
    level: str
    event: str
    data: dict

def now_et_iso() -> str:
    if ZoneInfo is None:
        # fallback: local time; still explicit in report that ET zoneinfo missing
        return datetime.now().isoformat(timespec="seconds")
    return datetime.now(ZoneInfo(ET_TZ)).isoformat(timespec="seconds")

def run_stamp() -> str:
    # YYYYMMDD_HHMM_ET
    if ZoneInfo is None:
        dt = datetime.now()
    else:
        dt = datetime.now(ZoneInfo(ET_TZ))
    return dt.strftime("%Y%m%d_%H%M_ET")

def write_jsonl(path: Path, events: List[Event]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e.__dict__, ensure_ascii=False) + "\n")

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def safe_stat(path: Path) -> dict:
    try:
        st = path.stat()
        return {
            "exists": True,
            "size": st.st_size,
            "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
        }
    except FileNotFoundError:
        return {"exists": False}


# ----------------------------
# File fingerprinting
# ----------------------------
def read_header(path: Path) -> str:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        row = next(reader)
        return ",".join(row)

def fingerprint_file(path: Path) -> dict:
    info = {"path": str(path), **safe_stat(path)}
    if info.get("exists"):
        hdr = read_header(path)
        info["header"] = hdr
        info["header_sha1"] = sha1_text(hdr)
    return info


# ----------------------------
# Data helpers
# ----------------------------
JOIN_COLS = ["player_id", "game_id", "game_date", "line"]

def coerce_core_types(df: pd.DataFrame) -> pd.DataFrame:
    # We coerce only known ID/date-ish columns if present; do not rename.
    for c in ["player_id", "game_id", "team_id", "opponent_id"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    if "is_home" in df.columns:
        # allow 0/1, True/False
        df["is_home"] = df["is_home"].astype("Int64") if pd.api.types.is_numeric_dtype(df["is_home"]) else df["is_home"]
    if "game_date" in df.columns:
        # keep as string to avoid timezone shifts; just standardize to YYYY-MM-DD if parseable
        gd = pd.to_datetime(df["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df["game_date"] = gd.fillna(df["game_date"].astype(str))
    return df

def discover_text_team_cols(df: pd.DataFrame) -> List[str]:
    # Evidence-only: detect candidate text columns; no semantics assumed.
    candidates = []
    for c in ["name", "team", "teams", "team_code", "abbr", "triCode", "tricode"]:
        if c in df.columns:
            candidates.append(c)
    # Also: any obvious 3-letter-ish text column not in above list
    for c in df.columns:
        if c in candidates:
            continue
        if df[c].dtype == object and any(k in c.lower() for k in ["team", "abbr", "code", "tri"]):
            candidates.append(c)
    # de-dupe preserving order
    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out

def text_col_stats(df: pd.DataFrame, col: str, topn: int = 20) -> dict:
    s = df[col].astype("string")
    nonnull = s.dropna()
    total = len(s)
    nn = len(nonnull)
    lens = nonnull.str.len()
    is_len3 = (lens == 3)
    is_upper_alpha = nonnull.str.fullmatch(r"[A-Z]{3}").fillna(False)
    top = nonnull.value_counts(dropna=True).head(topn).to_dict()
    return {
        "col": col,
        "total_rows": total,
        "nonnull_rows": nn,
        "pct_null": round((total - nn) / total * 100, 3) if total else None,
        "pct_len_3": round(is_len3.mean() * 100, 3) if nn else None,
        "pct_upper_AZ3": round(is_upper_alpha.mean() * 100, 3) if nn else None,
        "top_values": top,
    }

def detect_line_columns(cols: Iterable[str]) -> List[str]:
    # Identify which p_over_*_{line} exist in the wide file.
    # We consider these families:
    #   p_over_{line}, p_over_lr_{line}, p_over_rf_{line}
    # where line is like 0_5,1_5,2_5,3_5
    out = []
    for c in cols:
        if c.startswith("p_over_") and any(c.endswith(suf) for suf in ("_0_5", "_1_5", "_2_5", "_3_5")):
            out.append(c)
    return out

def parse_line_suffix(col: str) -> Optional[float]:
    # expects ..._{0_5,1_5,2_5,3_5}
    for suf, val in [("_0_5", 0.5), ("_1_5", 1.5), ("_2_5", 2.5), ("_3_5", 3.5)]:
        if col.endswith(suf):
            return val
    return None

def unpivot_wide_to_long(
    df_wide: pd.DataFrame,
    source_file: str,
    events: List[Event],
    value_families: Tuple[str, ...] = ("p_over", "p_over_lr", "p_over_rf"),
) -> pd.DataFrame:
    """
    Converts wide p_over_*_{line} columns into long rows.
    Keeps all original columns untouched; adds:
      - line (float)
      - raw_value / raw_lr_value / raw_rf_value (as available)
      - raw_source_col / raw_lr_source_col / raw_rf_source_col (exact column name used)
      - source_file
    """
    required = ["player_id", "game_id", "game_date"]
    missing = [c for c in required if c not in df_wide.columns]
    if missing:
        events.append(Event(now_et_iso(), "ERROR", "SPINE_MISSING_REQUIRED_COLS", {"missing": missing, "source_file": source_file}))
        raise ValueError(f"Missing required columns in spine: {missing}")

    # Identify available line suffixes by presence of any p_over_{line} column
    line_suffixes = []
    for suf, val in [("0_5", 0.5), ("1_5", 1.5), ("2_5", 2.5), ("3_5", 3.5)]:
        if any(col.endswith(f"_{suf}") and col.startswith("p_over") for col in df_wide.columns):
            line_suffixes.append((suf, val))

    if not line_suffixes:
        events.append(Event(now_et_iso(), "ERROR", "NO_LINE_COLUMNS_FOUND", {"source_file": source_file}))
        raise ValueError("No p_over*_{line} columns found in wide file.")

    # Build long rows by concatenating per-line slices
    chunks = []
    base_cols = list(df_wide.columns)  # keep all
    for suf, line_val in line_suffixes:
        chunk = df_wide.copy()

        chunk["line"] = float(line_val)

        # For each family, map column -> value + source col name.
        # Do not rename original cols; add new ones for overlay use.
        fam_map = {
            "p_over": ("raw_value", f"p_over_{suf}"),
            "p_over_lr": ("raw_lr_value", f"p_over_lr_{suf}"),
            "p_over_rf": ("raw_rf_value", f"p_over_rf_{suf}"),
        }
        for fam, (outcol, incol) in fam_map.items():
            if fam in value_families:
                if incol in df_wide.columns:
                    chunk[outcol] = pd.to_numeric(df_wide[incol], errors="coerce")
                    chunk[outcol + "_source_col"] = incol
                else:
                    chunk[outcol] = pd.NA
                    chunk[outcol + "_source_col"] = pd.NA

        chunk["source_file"] = source_file
        chunk["source_shape"] = "wide"
        chunks.append(chunk)

    df_long = pd.concat(chunks, ignore_index=True)
    df_long = coerce_core_types(df_long)

    events.append(Event(now_et_iso(), "INFO", "UNPIVOT_OK", {
        "source_file": source_file,
        "rows_wide": int(len(df_wide)),
        "rows_long": int(len(df_long)),
        "lines_emitted": [lv for _, lv in line_suffixes],
    }))
    return df_long

def normalize_linefile_long(df_line: pd.DataFrame, source_file: str, events: List[Event]) -> pd.DataFrame:
    """
    Takes a line-specific file (e.g. sog_predictions_2_5.csv) and produces a long df
    with join keys and values in:
      - linefile_value / linefile_lr_value / linefile_rf_value
      - *_source_col capturing which column was used (verbatim)
    Keeps all original columns untouched; adds only new columns.
    """
    required = ["player_id", "game_id", "game_date"]
    missing = [c for c in required if c not in df_line.columns]
    if missing:
        events.append(Event(now_et_iso(), "WARN", "LINEFILE_MISSING_REQUIRED_COLS", {"missing": missing, "source_file": source_file}))
        # still attempt, but will likely fail joins
    if "line" not in df_line.columns:
        events.append(Event(now_et_iso(), "WARN", "LINEFILE_MISSING_LINE_COL", {"source_file": source_file}))

    df = df_line.copy()
    df = coerce_core_types(df)

    # Determine which suffix this file uses by scanning for p_over_*_{x_5}
    suffix = None
    for suf in ["0_5", "1_5", "2_5", "3_5"]:
        if f"p_over_{suf}" in df.columns or any(c.endswith(f"_{suf}") for c in df.columns if c.startswith("p_over_")):
            suffix = suf
            break

    # If line exists, trust it; else derive from suffix if found
    if "line" in df.columns:
        df["line"] = pd.to_numeric(df["line"], errors="coerce")
    elif suffix is not None:
        df["line"] = float({"0_5": 0.5, "1_5": 1.5, "2_5": 2.5, "3_5": 3.5}[suffix])
    else:
        df["line"] = pd.NA

    # Identify the exact columns present; don't assume they exist
    # Use same families but store into linefile_* columns
    fam_map = [
        ("linefile_value", [f"p_over_{suffix}"] if suffix else []),
        ("linefile_lr_value", [f"p_over_lr_{suffix}"] if suffix else []),
        ("linefile_rf_value", [f"p_over_rf_{suffix}"] if suffix else []),
    ]
    for outcol, candidates in fam_map:
        found = None
        for c in candidates:
            if c and c in df.columns:
                found = c
                break
        if found:
            df[outcol] = pd.to_numeric(df[found], errors="coerce")
            df[outcol + "_source_col"] = found
        else:
            df[outcol] = pd.NA
            df[outcol + "_source_col"] = pd.NA

    df["source_file"] = source_file
    df["source_shape"] = "linefile"
    events.append(Event(now_et_iso(), "INFO", "LINEFILE_LOAD_OK", {
        "source_file": source_file,
        "rows": int(len(df)),
        "suffix_detected": suffix,
        "has_line_col": "line" in df_line.columns,
    }))
    return df

def key_dupes(df: pd.DataFrame, key_cols: List[str]) -> Tuple[int, pd.DataFrame]:
    if not all(c in df.columns for c in key_cols):
        return 0, pd.DataFrame()
    grp = df.groupby(key_cols, dropna=False).size().reset_index(name="n")
    dup = grp[grp["n"] > 1].sort_values("n", ascending=False)
    return int(dup["n"].sum() - len(dup)) if len(dup) else 0, dup

def unique_stats(series: pd.Series) -> dict:
    s = pd.to_numeric(series, errors="coerce")
    s = s.dropna()
    if s.empty:
        return {"n": 0, "unique": 0, "min": None, "mean": None, "max": None, "std": None}
    return {
        "n": int(len(s)),
        "unique": int(s.nunique()),
        "min": float(s.min()),
        "mean": float(s.mean()),
        "max": float(s.max()),
        "std": float(s.std(ddof=0)),
    }


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spine", required=True, help="Path to sog_predictions.csv (wide)")
    ap.add_argument("--calibrated", default="", help="Path to sog_predictions_wide_calibrated.csv (wide)")
    ap.add_argument("--linefiles", nargs="*", default=[], help="Paths to sog_predictions_{1_5,2_5,3_5}.csv")
    ap.add_argument("--outdir", default="backend/nhl/data/processed/reconcile", help="Output directory")
    ap.add_argument("--max_mismatch_samples", type=int, default=20, help="How many mismatch examples to include in report")
    args = ap.parse_args()

    events: List[Event] = []
    run_ts = now_et_iso()
    stamp = run_stamp()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    spine_path = Path(args.spine)
    cal_path = Path(args.calibrated) if args.calibrated else None
    line_paths = [Path(p) for p in args.linefiles]

    # Fingerprints
    inputs_fp = {
        "run_ts_et": run_ts,
        "spine": fingerprint_file(spine_path),
        "calibrated": fingerprint_file(cal_path) if cal_path else {"provided": False},
        "linefiles": [fingerprint_file(p) for p in line_paths],
    }
    events.append(Event(run_ts, "INFO", "INPUT_FINGERPRINTS", inputs_fp))

    # Load spine
    try:
        df_spine = pd.read_csv(spine_path)
        events.append(Event(now_et_iso(), "INFO", "READ_OK", {"file": str(spine_path), "rows": int(len(df_spine))}))
    except Exception as e:
        events.append(Event(now_et_iso(), "ERROR", "READ_FAIL", {"file": str(spine_path), "error": str(e)}))
        write_jsonl(outdir / f"sog_log__run_{stamp}.jsonl", events)
        return 2

    df_spine = coerce_core_types(df_spine)

    # Team-text evidence for spine
    spine_text_cols = discover_text_team_cols(df_spine)
    events.append(Event(now_et_iso(), "INFO", "TEXT_TEAM_COLS_DETECTED", {"file": str(spine_path), "cols": spine_text_cols}))
    spine_text_stats = [text_col_stats(df_spine, c) for c in spine_text_cols] if spine_text_cols else []

    # Unpivot spine -> observed long
    try:
        observed_long = unpivot_wide_to_long(df_spine, source_file=str(spine_path), events=events)
    except Exception as e:
        events.append(Event(now_et_iso(), "ERROR", "UNPIVOT_FAIL", {"file": str(spine_path), "error": str(e)}))
        write_jsonl(outdir / f"sog_log__run_{stamp}.jsonl", events)
        return 3

    # Duplicates check (observed long)
    dup_count, dup_df = key_dupes(observed_long, JOIN_COLS)
    if dup_count:
        events.append(Event(now_et_iso(), "WARN", "DUPLICATE_JOIN_KEYS", {"source": "spine_unpivot", "dup_count": dup_count, "top": dup_df.head(20).to_dict(orient="records")}))
    else:
        events.append(Event(now_et_iso(), "INFO", "NO_DUPLICATE_JOIN_KEYS", {"source": "spine_unpivot"}))

    # Load + unpivot calibrated (optional)
    cal_long = None
    cal_text_stats = []
    if cal_path and cal_path.exists():
        try:
            df_cal = pd.read_csv(cal_path)
            df_cal = coerce_core_types(df_cal)
            cal_text_cols = discover_text_team_cols(df_cal)
            events.append(Event(now_et_iso(), "INFO", "TEXT_TEAM_COLS_DETECTED", {"file": str(cal_path), "cols": cal_text_cols}))
            cal_text_stats = [text_col_stats(df_cal, c) for c in cal_text_cols] if cal_text_cols else []

            cal_long = unpivot_wide_to_long(df_cal, source_file=str(cal_path), events=events)
            # Rename overlay columns ONLY (additive) so we can distinguish in overlay file.
            # We do not rename original columns; these are new columns that exist only in overlay outputs.
            cal_long = cal_long.rename(columns={
                "raw_value": "cal_value",
                "raw_lr_value": "cal_lr_value",
                "raw_rf_value": "cal_rf_value",
                "raw_value_source_col": "cal_value_source_col",
                "raw_lr_value_source_col": "cal_lr_value_source_col",
                "raw_rf_value_source_col": "cal_rf_value_source_col",
                "source_file": "cal_source_file",
                "source_shape": "cal_source_shape",
            })
        except Exception as e:
            events.append(Event(now_et_iso(), "ERROR", "CAL_READ_OR_UNPIVOT_FAIL", {"file": str(cal_path), "error": str(e)}))
            cal_long = None
    else:
        events.append(Event(now_et_iso(), "INFO", "CALIBRATED_NOT_PROVIDED_OR_MISSING", {"file": str(cal_path) if cal_path else ""}))

    # Load linefiles (optional) and normalize
    line_long_all = []
    line_text_stats_all = []
    for lp in line_paths:
        if not lp.exists():
            events.append(Event(now_et_iso(), "WARN", "LINEFILE_MISSING", {"file": str(lp)}))
            continue
        try:
            df_lf = pd.read_csv(lp)
            df_lf = coerce_core_types(df_lf)
            lf_text_cols = discover_text_team_cols(df_lf)
            events.append(Event(now_et_iso(), "INFO", "TEXT_TEAM_COLS_DETECTED", {"file": str(lp), "cols": lf_text_cols}))
            line_text_stats_all.extend([text_col_stats(df_lf, c) for c in lf_text_cols] if lf_text_cols else [])
            line_long_all.append(normalize_linefile_long(df_lf, source_file=str(lp), events=events))
        except Exception as e:
            events.append(Event(now_et_iso(), "ERROR", "LINEFILE_READ_FAIL", {"file": str(lp), "error": str(e)}))

    line_long = pd.concat(line_long_all, ignore_index=True) if line_long_all else None
    if line_long is None:
        events.append(Event(now_et_iso(), "INFO", "NO_LINEFILES_LOADED", {}))

    # Build overlay dataframe: start from observed_long keys
    overlay = observed_long.copy()

    # Add run metadata
    overlay["run_ts_et"] = run_ts
    overlay["run_stamp"] = stamp

    # Overlay calibration values by join key
    if cal_long is not None:
        # Keep only necessary overlay cols + join keys
        cal_keep = [c for c in cal_long.columns if c in JOIN_COLS or c.startswith("cal_")]
        cal_merge = cal_long[cal_keep].drop_duplicates(subset=JOIN_COLS, keep="last")
        before = len(overlay)
        overlay = overlay.merge(cal_merge, on=JOIN_COLS, how="left")
        after = len(overlay)
        events.append(Event(now_et_iso(), "INFO", "CAL_OVERLAY_MERGE_OK", {"rows_before": int(before), "rows_after": int(after)}))
    else:
        overlay["cal_value"] = pd.NA
        overlay["cal_lr_value"] = pd.NA
        overlay["cal_rf_value"] = pd.NA

    # Overlay linefile values by join key
    if line_long is not None:
        lf_keep = [c for c in line_long.columns if c in JOIN_COLS or c.startswith("linefile_") or c in ("source_file", "source_shape")]
        lf_merge = line_long[lf_keep].drop_duplicates(subset=JOIN_COLS, keep="last").rename(columns={
            "source_file": "linefile_source_file",
            "source_shape": "linefile_source_shape",
        })
        before = len(overlay)
        overlay = overlay.merge(lf_merge, on=JOIN_COLS, how="left")
        after = len(overlay)
        events.append(Event(now_et_iso(), "INFO", "LINEFILE_OVERLAY_MERGE_OK", {"rows_before": int(before), "rows_after": int(after)}))
    else:
        overlay["linefile_value"] = pd.NA
        overlay["linefile_lr_value"] = pd.NA
        overlay["linefile_rf_value"] = pd.NA

    # Differences (additive, does not rename originals)
    overlay["diff_raw_vs_cal"] = pd.to_numeric(overlay.get("raw_value"), errors="coerce") - pd.to_numeric(overlay.get("cal_value"), errors="coerce")
    overlay["diff_raw_vs_linefile"] = pd.to_numeric(overlay.get("raw_value"), errors="coerce") - pd.to_numeric(overlay.get("linefile_value"), errors="coerce")

    # Coverage metrics
    n_total = len(overlay)
    cal_cov = float(pd.to_numeric(overlay.get("cal_value"), errors="coerce").notna().mean() * 100) if n_total else 0.0
    lf_cov = float(pd.to_numeric(overlay.get("linefile_value"), errors="coerce").notna().mean() * 100) if n_total else 0.0

    # Unique / squash stats per line
    per_line = {}
    for line_val in sorted(overlay["line"].dropna().unique()):
        sub = overlay[overlay["line"] == line_val]
        per_line[str(line_val)] = {
            "rows": int(len(sub)),
            "raw": unique_stats(sub.get("raw_value")),
            "cal": unique_stats(sub.get("cal_value")) if "cal_value" in sub.columns else {"n": 0, "unique": 0, "min": None, "mean": None, "max": None, "std": None},
        }

    # Mismatch samples (raw vs cal, raw vs linefile)
    mismatch_samples = {}
    if "diff_raw_vs_cal" in overlay.columns:
        d = pd.to_numeric(overlay["diff_raw_vs_cal"], errors="coerce").abs()
        worst = overlay.loc[d.sort_values(ascending=False).head(args.max_mismatch_samples).index, JOIN_COLS + ["raw_value", "cal_value", "diff_raw_vs_cal"]]
        mismatch_samples["raw_vs_cal_worst"] = worst.to_dict(orient="records")
    if "diff_raw_vs_linefile" in overlay.columns:
        d = pd.to_numeric(overlay["diff_raw_vs_linefile"], errors="coerce").abs()
        worst = overlay.loc[d.sort_values(ascending=False).head(args.max_mismatch_samples).index, JOIN_COLS + ["raw_value", "linefile_value", "diff_raw_vs_linefile"]]
        mismatch_samples["raw_vs_linefile_worst"] = worst.to_dict(orient="records")

    # Write outputs
    observed_path = outdir / f"sog_observed_long__run_{stamp}.csv"
    overlay_path = outdir / f"sog_overlay__run_{stamp}.csv"
    summary_path = outdir / f"sog_summary__run_{stamp}.json"
    report_path = outdir / f"sog_report__run_{stamp}.md"
    log_path = outdir / f"sog_log__run_{stamp}.jsonl"

    observed_long.to_csv(observed_path, index=False)
    overlay.to_csv(overlay_path, index=False)

    summary = {
        "run_ts_et": run_ts,
        "run_stamp": stamp,
        "inputs": inputs_fp,
        "rows_total_overlay": int(n_total),
        "calibration_coverage_pct": round(cal_cov, 3),
        "linefile_coverage_pct": round(lf_cov, 3),
        "duplicates_in_spine_unpivot": int(dup_count),
        "per_line_stats": per_line,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    # Report (markdown, evidence-first)
    lines = []
    lines.append(f"# SOG Reconcile Report — run {stamp}")
    lines.append("")
    lines.append(f"- run_ts_et: `{run_ts}`")
    lines.append(f"- spine: `{spine_path}`")
    if cal_path:
        lines.append(f"- calibrated: `{cal_path}`")
    if line_paths:
        lines.append(f"- linefiles: {', '.join(f'`{p}`' for p in line_paths)}")
    lines.append("")
    lines.append("## Input fingerprints")
    lines.append("```json")
    lines.append(json.dumps(inputs_fp, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## Join behavior (declared)")
    lines.append("- Join keys used: `player_id + game_id + game_date + line`")
    lines.append("- `line` is derived from the *presence of* `p_over_*_{0_5,1_5,2_5,3_5}` columns in the spine.")
    lines.append("- No joins are performed on any text team/name columns.")
    lines.append("")
    lines.append("## Text team/name column evidence (no semantics assumed)")
    def render_text_stats(title: str, stats: List[dict]) -> None:
        lines.append(f"### {title}")
        if not stats:
            lines.append("- (no detected candidate text columns)")
            lines.append("")
            return
        for st in stats:
            lines.append(f"- **{st['col']}**: null={st['pct_null']}% len3={st['pct_len_3']}% upper[A-Z]3={st['pct_upper_AZ3']}%")
            # Top values (compact)
            top = st["top_values"]
            if top:
                top_items = list(top.items())[:10]
                lines.append(f"  - top: " + ", ".join([f"{k}:{v}" for k, v in top_items]))
        lines.append("")
    render_text_stats("spine", spine_text_stats)
    render_text_stats("calibrated", cal_text_stats)
    render_text_stats("linefiles", line_text_stats_all)
    lines.append("## Coverage")
    lines.append(f"- overlay rows (post-unpivot): **{n_total}**")
    lines.append(f"- calibration coverage: **{round(cal_cov,3)}%**")
    lines.append(f"- linefile coverage: **{round(lf_cov,3)}%**")
    lines.append("")
    lines.append("## Duplicate join-key check")
    lines.append(f"- duplicates in spine unpivot by (player_id, game_id, game_date, line): **{dup_count}**")
    if dup_count and not dup_df.empty:
        lines.append("")
        lines.append("Top duplicate keys:")
        lines.append("```json")
        lines.append(json.dumps(dup_df.head(20).to_dict(orient="records"), indent=2))
        lines.append("```")
    lines.append("")
    lines.append("## Per-line squash / uniqueness stats")
    for ln, st in per_line.items():
        lines.append(f"### line = {ln}")
        lines.append(f"- rows: {st['rows']}")
        lines.append(f"- raw: n={st['raw']['n']} unique={st['raw']['unique']} min={st['raw']['min']} mean={st['raw']['mean']} max={st['raw']['max']} std={st['raw']['std']}")
        lines.append(f"- cal: n={st['cal']['n']} unique={st['cal']['unique']} min={st['cal']['min']} mean={st['cal']['mean']} max={st['cal']['max']} std={st['cal']['std']}")
        # Simple collapse flag (evidence-only; threshold is visible)
        collapse = False
        if st["cal"]["n"] and st["cal"]["unique"] <= 10 and st["rows"] >= 500:
            collapse = True
        if collapse:
            lines.append(f"- **FLAG:** calibrated uniqueness suspiciously low (unique<=10 with rows>=500)")
        lines.append("")
    lines.append("## Worst mismatches (samples)")
    lines.append("```json")
    lines.append(json.dumps(mismatch_samples, indent=2))
    lines.append("```")
    report_path.write_text("\n".join(lines), encoding="utf-8")

    events.append(Event(now_et_iso(), "INFO", "WRITE_OK", {
        "observed_long": str(observed_path),
        "overlay": str(overlay_path),
        "summary": str(summary_path),
        "report": str(report_path),
        "log": str(log_path),
    }))
    write_jsonl(log_path, events)

    print(f"✅ wrote:\n- {observed_path}\n- {overlay_path}\n- {summary_path}\n- {report_path}\n- {log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
