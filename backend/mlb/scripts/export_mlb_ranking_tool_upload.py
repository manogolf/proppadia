#!/usr/bin/env python3
"""Convert rank-model MLB output into external price-finding tool upload rows.

The upload probability is an empirical historical win rate by rank bucket, not
the raw rank score. Reporting/export only; no model logic changes and no DB
writes.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from backend.mlb.shared.market_audit_context import MARKET_AUDIT_CONTEXT_COLUMNS, add_market_audit_context
from backend.mlb.scripts import export_mlb_book_upload as book_upload
from backend.mlb.scripts import tool_upload_8rain


DEFAULT_HISTORY_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_OUT_ROOT = Path("backend/mlb/exports/model_v2/upload")

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

PATCH_1A_CONTEXT_COLUMNS = [
    "game_time",
    "time_of_day_bucket",
    "game_day_of_week",
    "is_home",
    "team",
    "team_id",
    "opponent",
    "opponent_id",
]
BVP_CONTEXT_COLUMNS = [
    "bvp_plate_appearances",
    "bvp_at_bats",
    "bvp_hits",
    "bvp_total_bases",
    "bvp_avg",
    "bvp_slg",
    "bvp_payload_present",
    "bvp_source",
]
ROLLING_CONTEXT_COLUMNS = [
    "rolling_result_avg_7",
    "d7_hits",
    "d15_hits",
    "d30_hits",
    "d7_total_bases",
    "d15_total_bases",
    "d30_total_bases",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
    "d30_hits_runs_rbis",
    "d7_strikeouts_batting",
    "d15_strikeouts_batting",
    "d30_strikeouts_batting",
    "d7_hits_allowed",
    "d15_hits_allowed",
    "d30_hits_allowed",
]
PASSIVE_CONTEXT_COLUMNS = [
    *PATCH_1A_CONTEXT_COLUMNS,
    *BVP_CONTEXT_COLUMNS,
    *ROLLING_CONTEXT_COLUMNS,
    *MARKET_AUDIT_CONTEXT_COLUMNS,
]
BVP_DIRECT_CONTEXT_COLUMNS = [
    "bvp_plate_appearances",
    "bvp_at_bats",
    "bvp_hits",
    "bvp_total_bases",
]
BVP_DEFAULT_SOURCE = "prop_features_precomputed"


def _archive_versioned_csv(path: Path, run_tag: str | None = None) -> Path:
    tag = run_tag or tool_upload_8rain.upload_run_tag()
    archived = path.with_name(f"{path.stem}__{tag}{path.suffix}")
    if archived != path:
        shutil.copy2(path, archived)
    return archived


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
    prop_type = _norm_prop(value)
    return book_upload._normalize_upload_market(
        raw_market="",
        prop_type=prop_type,
        market_map=book_upload.DEFAULT_MARKET_BY_PROP,
    )


def _norm_side(value: Any) -> str:
    text = _clean(value).lower()
    if text.startswith("o"):
        return "over"
    if text.startswith("u"):
        return "under"
    return text


def _num(value: Any) -> float:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(val) if pd.notna(val) else np.nan


def _line_key(value: Any) -> str:
    val = _num(value)
    if pd.isna(val):
        return ""
    return f"{val:.3f}".rstrip("0").rstrip(".")


def _format_point(value: Any) -> Any:
    val = _num(value)
    if pd.isna(val):
        return ""
    if abs(val - round(val)) < 1e-9:
        return int(round(val))
    return val


def _rank_percentile_0_1(series: pd.Series) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    # Accept either 0-1 or 0-100 input.
    if vals.dropna().gt(1.0).any():
        vals = vals / 100.0
    return vals.clip(0.0, 1.0)


def _rank_bucket(value: Any, bucket_size: float) -> str:
    p = _num(value)
    if pd.isna(p):
        return ""
    if p > 1.0:
        p = p / 100.0
    p = min(max(p, 0.0), 1.0)
    size = float(bucket_size)
    low = math.floor(p / size) * size
    high = min(low + size, 1.0)
    if p >= 1.0:
        low = max(0.0, 1.0 - size)
        high = 1.0
    return f"{low:.2f}-{high:.2f}"


def _line_bucket(value: Any) -> str:
    return _line_key(value)


def _discover_history_files(root: Path, from_date: str | None, to_date: str | None) -> list[Path]:
    files: list[tuple[str, Path]] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        if not _date_key(date):
            continue
        if from_date and date < from_date:
            continue
        if to_date and date > to_date:
            continue
        files.append((date, path))
    return [p for _, p in sorted(files)]


def _load_rank_csv(path: Path, date_arg: str | None) -> tuple[pd.DataFrame, str]:
    if not path.exists():
        raise SystemExit(f"missing ranking CSV: {path}")
    df = pd.read_csv(path, low_memory=False)
    required = {"player_name", "prop_type", "side", "line", "rank_score", "rank_position", "rank_percentile"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"{path} missing required columns: {missing}")
    out = df.copy()
    if "date" not in out.columns:
        if not date_arg:
            raise SystemExit("ranking CSV lacks date column; pass --date YYYY-MM-DD")
        out["date"] = date_arg
    out["date"] = out["date"].map(_date_key)
    if date_arg:
        out = out[out["date"].eq(_date_key(date_arg))].copy()
    if out.empty:
        raise SystemExit("ranking CSV has no rows after date filter")
    date_value = sorted(out["date"].dropna().unique())[-1]
    out["prop_type"] = out["prop_type"].map(_norm_prop)
    out["side"] = out["side"].map(_norm_side)
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["rank_percentile"] = _rank_percentile_0_1(out["rank_percentile"])
    out["rank_bucket"] = out["rank_percentile"].map(lambda v: _rank_bucket(v, 0.10))
    out["line_bucket"] = out["line"].map(_line_bucket)
    return out, str(date_value)


def _load_history(paths: Iterable[Path]) -> pd.DataFrame:
    frames = []
    required = {
        "game_date",
        "player_name",
        "prop_type",
        "line",
        "model_pick_side",
        "model_pick_prob",
        "actual_over_outcome",
        "actual_under_outcome",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[ranking-upload] skip {path}: missing {missing}")
            continue
        df["source_reconcile_file"] = str(path)
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible historical reconcile rows found.")
    return pd.concat(frames, ignore_index=True)


def _history_to_rank_rows(history: pd.DataFrame, bucket_size: float) -> pd.DataFrame:
    out = history.copy()
    out["date"] = out["game_date"].map(_date_key)
    out["prop_type"] = out["prop_type"].map(_norm_prop)
    out["side"] = out["model_pick_side"].map(_norm_side)
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["line_bucket"] = out["line"].map(_line_bucket)
    if "rank_score" in out.columns:
        out["rank_score_hist"] = pd.to_numeric(out["rank_score"], errors="coerce")
    else:
        out["rank_score_hist"] = pd.to_numeric(out["model_pick_prob"], errors="coerce")
    if "rank_percentile" in out.columns:
        out["rank_percentile_hist"] = _rank_percentile_0_1(out["rank_percentile"])
    else:
        # Percentile by slate, high score = better rank. pct=True gives ascending percentile,
        # so rank descending and invert to make strongest rows approach 1.0.
        out["rank_percentile_hist"] = out.groupby("date")["rank_score_hist"].rank(pct=True, method="average")
    out["rank_bucket"] = out["rank_percentile_hist"].map(lambda v: _rank_bucket(v, bucket_size))
    outcome = np.where(
        out["side"].eq("over"),
        out["actual_over_outcome"].map(lambda v: _clean(v).lower()),
        out["actual_under_outcome"].map(lambda v: _clean(v).lower()),
    )
    out["outcome_norm"] = pd.Series(outcome, index=out.index)
    out = out[out["outcome_norm"].isin(["win", "loss"])].copy()
    out["win"] = out["outcome_norm"].eq("win").astype(float)
    out = out[out["prop_type"].ne("") & out["side"].isin(["over", "under"]) & out["line_bucket"].ne("") & out["rank_bucket"].ne("")]
    return out


def _build_lookup(history_rows: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["prop_type", "side", "line_bucket", "rank_bucket"]
    lookup = (
        history_rows.groupby(group_cols, dropna=False, observed=True)
        .agg(bets=("win", "size"), actual_win_rate=("win", "mean"))
        .reset_index()
    )
    lookup = lookup.rename(columns={"bets": "sample_size"})
    return lookup


def _fair_american(prob: float) -> int | None:
    if pd.isna(prob) or prob <= 0.0 or prob >= 1.0:
        return None
    if prob > 0.5:
        return int(round(-(prob / (1.0 - prob)) * 100.0))
    return int(round(((1.0 - prob) / prob) * 100.0))


def _format_win_value(prob: Any, win_format: str) -> Any:
    p = _num(prob)
    if pd.isna(p):
        return ""
    if win_format == "american":
        odds = _fair_american(p)
        return "" if odds is None else int(odds)
    if win_format == "pct":
        return round(p * 100.0, 4)
    return round(p, 6)


def _fill_bvp_lineage_wrappers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if not all(col in out.columns for col in BVP_DIRECT_CONTEXT_COLUMNS):
        return out
    direct_present = out[BVP_DIRECT_CONTEXT_COLUMNS].notna().any(axis=1)
    for col in ("bvp_payload_present", "bvp_source", "bvp_avg", "bvp_slg"):
        if col not in out.columns:
            out[col] = pd.NA

    ab = pd.to_numeric(out["bvp_at_bats"], errors="coerce")
    hits = pd.to_numeric(out["bvp_hits"], errors="coerce")
    total_bases = pd.to_numeric(out["bvp_total_bases"], errors="coerce")
    valid_ab = direct_present & ab.gt(0)
    out.loc[valid_ab & out["bvp_avg"].isna(), "bvp_avg"] = hits.loc[valid_ab] / ab.loc[valid_ab]
    out.loc[valid_ab & out["bvp_slg"].isna(), "bvp_slg"] = total_bases.loc[valid_ab] / ab.loc[valid_ab]
    out.loc[direct_present, "bvp_payload_present"] = True
    out.loc[~direct_present & out["bvp_payload_present"].isna(), "bvp_payload_present"] = False
    out.loc[direct_present & out["bvp_source"].isna(), "bvp_source"] = BVP_DEFAULT_SOURCE
    return out


def _merge_lookup(current: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    keys = ["prop_type", "side", "line_bucket", "rank_bucket"]
    merged = current.merge(lookup, on=keys, how="left")
    return merged.rename(columns={"actual_win_rate": "empirical_win_pct"})


def _write_outputs(
    rows: pd.DataFrame,
    *,
    date_value: str,
    out_csv: Path,
    diagnostics_csv: Path,
    win_format: str,
    allow_low_sample: bool,
    known_unresolved_players_csv: Path | None = None,
    allow_8rain_public_catalog_fetch: bool = False,
) -> dict[str, Any]:
    work = rows.copy()
    work["uploaded_win_value"] = work["empirical_win_pct"].map(lambda p: _format_win_value(p, win_format))
    work["win_format"] = win_format
    work["mapped_bucket_win_rate"] = work["empirical_win_pct"]
    work["mapper_sample_size"] = work["sample_size"]
    work["win_pct_raw_source"] = work["empirical_win_pct"]
    work["win_pct_exported_decimal"] = pd.to_numeric(work["empirical_win_pct"], errors="coerce").round(6)
    work["exported_side"] = work["side"].astype(str).str.strip().str.lower()
    work["probability_semantics"] = "P(exported SIDE wins)"
    work = _fill_bvp_lineage_wrappers(work)
    work = add_market_audit_context(work, side_col="exported_side", probability_col="win_pct_exported_decimal")

    diag_cols = [
        "date",
        "player_id",
        "player_name",
        "prop_type",
        "side",
        "line",
        "source_lane",
        "rank_score",
        "rank_position",
        "rank_percentile",
        "rank_bucket",
        "mapped_bucket_win_rate",
        "mapper_sample_size",
        "win_pct_raw_source",
        "win_pct_exported_decimal",
        "exported_side",
        "probability_semantics",
        "empirical_win_pct",
        "sample_size",
        "win_format",
        "uploaded_win_value",
        *PASSIVE_CONTEXT_COLUMNS,
    ]
    diagnostics_csv.parent.mkdir(parents=True, exist_ok=True)
    work[[c for c in diag_cols if c in work.columns]].to_csv(diagnostics_csv, index=False)

    upload_work = work[work["empirical_win_pct"].notna()].copy()
    if not allow_low_sample:
        upload_work = upload_work[pd.to_numeric(upload_work["sample_size"], errors="coerce").ge(50)].copy()
    home_col = "home_upload" if "home_upload" in upload_work.columns else "home_team_code"
    away_col = "away_upload" if "away_upload" in upload_work.columns else "away_team_code"
    home_source_col = "home_team_code" if "home_team_code" in upload_work.columns else home_col
    away_source_col = "away_team_code" if "away_team_code" in upload_work.columns else away_col

    upload = pd.DataFrame(
        {
            "LEAGUE": "mlb",
            "DATE": _date_key(date_value),
            "HOME": upload_work[home_col].map(book_upload._normalize_upload_team_code) if home_col in upload_work.columns else "",
            "AWAY": upload_work[away_col].map(book_upload._normalize_upload_team_code) if away_col in upload_work.columns else "",
            "DOUBLEHEADER": 0,
            "SECTION": "player_prop",
            "MARKET": upload_work["prop_type"].map(_upload_market),
            "SELECTOR": (
                pd.to_numeric(upload_work["player_id"], errors="coerce").astype("Int64")
                if "player_id" in upload_work.columns
                else upload_work["player_name"]
            ),
            "POINT": upload_work["line"].map(_format_point),
            "SIDE": upload_work["side"].str.lower(),
            "WIN %": upload_work["uploaded_win_value"],
            "HOME_SOURCE": upload_work[home_source_col] if home_source_col in upload_work.columns else "",
            "AWAY_SOURCE": upload_work[away_source_col] if away_source_col in upload_work.columns else "",
        }
    )
    for col in PASSIVE_CONTEXT_COLUMNS:
        if col in upload_work.columns:
            upload[col] = upload_work[col].to_numpy()
    catalog = tool_upload_8rain.load_catalog()
    run_tag = tool_upload_8rain.upload_run_tag()
    generated_at = tool_upload_8rain.generated_at_utc()
    try:
        upload, validation_summary = tool_upload_8rain.prepare_player_prop_upload(
            upload,
            catalog=catalog,
            source_rows_before=len(upload_work),
            allow_public_catalog_fetch=allow_8rain_public_catalog_fetch,
        )
    except ValueError as exc:
        tool_upload_8rain.write_prepare_failure_diagnostics(exc, diagnostics_csv)
        raise
    tool_upload_8rain.write_unknown_event_exclusions(validation_summary, diagnostics_csv)
    validation_summary = tool_upload_8rain.with_artifact_status(
        validation_summary,
        status=str(validation_summary.get("upload_status") or "success"),
        run_tag=run_tag,
        generated_at=generated_at,
    )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    upload.to_csv(out_csv, index=False)
    _archive_versioned_csv(out_csv, run_tag=run_tag)
    summary_json = diagnostics_csv.with_name(f"{diagnostics_csv.stem}_summary.json")
    summary_json.write_text(json.dumps(validation_summary, indent=2) + "\n", encoding="utf-8")
    event_diag = diagnostics_csv.with_name(f"{diagnostics_csv.stem}_event_diagnostics.csv")
    pd.DataFrame(validation_summary.get("event_diagnostics_rows", [])).to_csv(event_diag, index=False)
    unresolved_csv = out_csv.parent / f"unresolved_player_candidates_{date_value}.csv"
    player_summary = tool_upload_8rain.write_unresolved_player_candidates(
        source_rows=upload_work,
        source_name="ranking_tool_upload",
        out_csv=unresolved_csv,
        player_map=tool_upload_8rain.build_player_map(catalog),
    )
    known_ids = tool_upload_8rain._known_unresolved_player_ids(known_unresolved_players_csv)
    validation_summary.update(
        {
            "total_upload_rows": int(len(upload)),
            "expected_paired_rows": int(len(upload_work) * 2),
            "players_using_mlbam_selector": int(player_summary["players_using_mlbam_selector"]),
            "known_tool_unresolved_players": int(len(known_ids)),
            "rows_likely_to_fail_selector_resolution": int(player_summary["rows_likely_to_fail_selector_resolution"]),
            "unresolved_player_candidates_csv": str(unresolved_csv),
        }
    )
    summary_json.write_text(json.dumps(validation_summary, indent=2) + "\n", encoding="utf-8")
    return validation_summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Export rank-model MLB rows for external price-finding tool.")
    parser.add_argument("--rank-csv", type=Path, required=True, help="Daily ranking model CSV.")
    parser.add_argument("--date", default="", help="Slate date YYYY-MM-DD; optional if rank CSV has date.")
    parser.add_argument("--history-root", type=Path, default=DEFAULT_HISTORY_ROOT)
    parser.add_argument("--from-date", default="", help="Historical reconcile start date.")
    parser.add_argument("--to-date", default="", help="Historical reconcile end date. Defaults to day before --date when provided.")
    parser.add_argument("--out-csv", type=Path, default=None)
    parser.add_argument("--diagnostics-csv", type=Path, default=None)
    parser.add_argument("--win-format", choices=["pct", "decimal", "american"], default="decimal")
    parser.add_argument("--allow-low-sample", action="store_true")
    parser.add_argument(
        "--allow-8rain-public-catalog-fetch",
        action="store_true",
        default=tool_upload_8rain.public_catalog_fetch_allowed(),
        help="Opt in to documented public 8rain catalog GET requests. Default is cache-only.",
    )
    parser.add_argument("--rank-bucket-size", type=float, default=0.10)
    parser.add_argument("--known-unresolved-players-csv", type=Path, default=None)
    args = parser.parse_args()

    current, date_value = _load_rank_csv(args.rank_csv, args.date or None)
    to_date = args.to_date or ""
    if not to_date and date_value:
        to_date = (pd.Timestamp(date_value) - pd.Timedelta(days=1)).date().isoformat()

    history_paths = _discover_history_files(args.history_root, args.from_date or None, to_date or None)
    history = _load_history(history_paths)
    history_rows = _history_to_rank_rows(history, float(args.rank_bucket_size))
    lookup = _build_lookup(history_rows)

    current["rank_bucket"] = current["rank_percentile"].map(lambda v: _rank_bucket(v, float(args.rank_bucket_size)))
    current["line_bucket"] = current["line"].map(_line_bucket)
    merged = _merge_lookup(current, lookup)

    default_out_root = DEFAULT_OUT_ROOT / date_value
    default_out = default_out_root / f"ranking_tool_upload_{date_value}.csv"
    default_diag = default_out_root / f"ranking_tool_upload_diagnostics_{date_value}.csv"
    out_csv = args.out_csv or default_out
    diagnostics_csv = args.diagnostics_csv or default_diag
    validation_summary = _write_outputs(
        merged,
        date_value=date_value,
        out_csv=out_csv,
        diagnostics_csv=diagnostics_csv,
        win_format=args.win_format,
        allow_low_sample=bool(args.allow_low_sample),
        known_unresolved_players_csv=args.known_unresolved_players_csv,
        allow_8rain_public_catalog_fetch=bool(args.allow_8rain_public_catalog_fetch),
    )

    kept = merged if args.allow_low_sample else merged[pd.to_numeric(merged["sample_size"], errors="coerce").ge(50)]
    print(f"Wrote {out_csv}")
    print(f"Wrote {diagnostics_csv}")
    print(
        "summary "
        f"input_rows={len(current)} matched_buckets={int(merged['empirical_win_pct'].notna().sum())} "
        f"selected_rows={len(kept)} uploaded_rows={validation_summary['rows_after_pairing']} "
        f"win_format={args.win_format} "
        f"public_catalog_fetch_allowed={str(bool(validation_summary.get('public_catalog_fetch_allowed'))).lower()} "
        f"public_catalog_fetch_attempted={str(bool(validation_summary.get('public_catalog_fetch_attempted'))).lower()} "
        f"public_catalog_fetch_succeeded={str(bool(validation_summary.get('public_catalog_fetch_succeeded'))).lower()} "
        f"cache_only_mode={str(bool(validation_summary.get('cache_only_mode'))).lower()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
