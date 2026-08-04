#!/usr/bin/env python3
"""Export Quick Card hits rows to the external tool upload schema.

This adapter does not use rank-bucket mapping. It converts the Quick Card's
own confidence/score field into the upload WIN % column and reuses the proven
MLB book-upload formatting helpers for market and team codes.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.mlb.shared.market_audit_context import MARKET_AUDIT_CONTEXT_COLUMNS, add_market_audit_context
from backend.mlb.scripts import export_mlb_book_upload as book_upload
from backend.mlb.scripts import tool_upload_8rain


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

WIN_VALUE_COLUMNS = [
    "quick_card_win_pct",
    "win_pct",
    "confidence",
    "model_prob",
    "score",
    "rank_score",
]


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


def export_quick_card(
    input_csv: Path,
    out_csv: Path,
    date_value: str,
    diagnostics_csv: Path | None = None,
    known_unresolved_players_csv: Path | None = None,
    allow_8rain_public_catalog_fetch: bool = False,
) -> dict[str, Any]:
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
    home_source_col = "home_team_code" if "home_team_code" in df.columns else home_col
    away_source_col = "away_team_code" if "away_team_code" in df.columns else away_col
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
    df = add_market_audit_context(df, side_col="side", probability_col="score")

    exported_decimal = df[win_col].map(_format_win_value)
    upload = pd.DataFrame(
        {
            "LEAGUE": "mlb",
            "DATE": date_series,
            "HOME": df[home_col].map(book_upload._normalize_upload_team_code),
            "AWAY": df[away_col].map(book_upload._normalize_upload_team_code),
            "DOUBLEHEADER": 0,
            "SECTION": "player_prop",
            "MARKET": df["prop_type"].map(_upload_market),
            "SELECTOR": pd.to_numeric(df["player_id"], errors="coerce").astype("Int64"),
            "POINT": df["line"].map(_format_point),
            "SIDE": df["side"].astype(str).str.strip().str.lower(),
            "WIN %": exported_decimal,
            "HOME_SOURCE": df[home_source_col] if home_source_col in df.columns else "",
            "AWAY_SOURCE": df[away_source_col] if away_source_col in df.columns else "",
        }
    )
    for col in PASSIVE_CONTEXT_COLUMNS:
        if col in df.columns:
            upload[col] = df[col].to_numpy()
    catalog = tool_upload_8rain.load_catalog()
    run_tag = tool_upload_8rain.upload_run_tag()
    generated_at = tool_upload_8rain.generated_at_utc()
    try:
        upload, validation_summary = tool_upload_8rain.prepare_player_prop_upload(
            upload,
            catalog=catalog,
            source_rows_before=len(df),
            allow_public_catalog_fetch=allow_8rain_public_catalog_fetch,
        )
    except ValueError as exc:
        tool_upload_8rain.write_prepare_failure_diagnostics(exc, diagnostics_csv or out_csv.with_name(f"{out_csv.stem}_diagnostics.csv"))
        raise
    if diagnostics_csv is not None:
        tool_upload_8rain.write_unknown_event_exclusions(validation_summary, diagnostics_csv)
    else:
        validation_summary["unknown_event_exclusions_csv"] = ""
    validation_summary = tool_upload_8rain.with_artifact_status(
        validation_summary,
        status=str(validation_summary.get("upload_status") or "success"),
        run_tag=run_tag,
        generated_at=generated_at,
    )
    diagnostics = df.copy()
    diagnostics["win_pct_raw_source"] = pd.to_numeric(df[win_col], errors="coerce")
    diagnostics["win_pct_exported_decimal"] = pd.to_numeric(exported_decimal, errors="coerce")
    diagnostics["exported_side"] = df["side"].astype(str).str.strip().str.lower()
    diagnostics["probability_semantics"] = "P(exported SIDE wins)"
    diagnostics = _fill_bvp_lineage_wrappers(diagnostics)
    diagnostics = add_market_audit_context(
        diagnostics,
        side_col="exported_side",
        probability_col="win_pct_exported_decimal",
    )

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
    archived_upload_csv = _archive_versioned_csv(out_csv, run_tag=run_tag)
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
            *PASSIVE_CONTEXT_COLUMNS,
        ]
        diagnostics_csv.parent.mkdir(parents=True, exist_ok=True)
        diagnostics[[c for c in diag_cols if c in diagnostics.columns]].to_csv(diagnostics_csv, index=False)
        archived_diagnostics_csv = _archive_versioned_csv(diagnostics_csv, run_tag=run_tag)
        summary_json = diagnostics_csv.with_name(f"{diagnostics_csv.stem}_summary.json")
        event_diag = diagnostics_csv.with_name(f"{diagnostics_csv.stem}_event_diagnostics.csv")
        pd.DataFrame(validation_summary.get("event_diagnostics_rows", [])).to_csv(event_diag, index=False)
        unresolved_csv = out_csv.parent / f"unresolved_player_candidates_{date_value}.csv"
        player_summary = tool_upload_8rain.write_unresolved_player_candidates(
            source_rows=df,
            source_name="quick_card_tool_upload",
            out_csv=unresolved_csv,
            player_map=tool_upload_8rain.build_player_map(catalog),
        )
        known_ids = tool_upload_8rain._known_unresolved_player_ids(known_unresolved_players_csv)
        validation_summary.update(
            {
                "total_upload_rows": int(len(upload)),
                "expected_paired_rows": int(len(df) * 2),
                "players_using_mlbam_selector": int(player_summary["players_using_mlbam_selector"]),
                "known_tool_unresolved_players": int(len(known_ids)),
                "rows_likely_to_fail_selector_resolution": int(player_summary["rows_likely_to_fail_selector_resolution"]),
                "unresolved_player_candidates_csv": str(unresolved_csv),
            }
        )
        summary_json.write_text(json.dumps(validation_summary, indent=2) + "\n", encoding="utf-8")
    else:
        archived_diagnostics_csv = None
        summary_json = None
    return {
        "rows": int(len(upload)),
        "win_column": win_col,
        "missing_home": int(home_missing.sum()),
        "missing_away": int(away_missing.sum()),
        "missing_win": int(win_missing.sum()),
        "archived_upload_csv": str(archived_upload_csv),
        "archived_diagnostics_csv": str(archived_diagnostics_csv) if archived_diagnostics_csv else "",
        "validation_summary_json": str(summary_json) if summary_json else "",
        "public_catalog_fetch_allowed": bool(validation_summary.get("public_catalog_fetch_allowed")),
        "public_catalog_fetch_attempted": bool(validation_summary.get("public_catalog_fetch_attempted")),
        "public_catalog_fetch_succeeded": bool(validation_summary.get("public_catalog_fetch_succeeded")),
        "public_catalog_endpoint_used": str(validation_summary.get("public_catalog_endpoint_used") or ""),
        "cache_only_mode": bool(validation_summary.get("cache_only_mode")),
        "skipped": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Quick Card hits rows to external tool upload CSV.")
    parser.add_argument("--date", required=True, help="Slate date YYYY-MM-DD.")
    parser.add_argument("--in-csv", type=Path, default=None)
    parser.add_argument("--out-csv", type=Path, default=None)
    parser.add_argument("--diagnostics-csv", type=Path, default=None)
    parser.add_argument("--known-unresolved-players-csv", type=Path, default=None)
    parser.add_argument(
        "--allow-8rain-public-catalog-fetch",
        action="store_true",
        default=tool_upload_8rain.public_catalog_fetch_allowed(),
        help="Opt in to documented public 8rain catalog GET requests. Default is cache-only.",
    )
    return parser.parse_args()


def main() -> None:
    from backend.mlb.shared.model_authority import assert_predictive_model_qualified

    assert_predictive_model_qualified("production_quick_card_upload")
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
    summary = export_quick_card(
        in_csv,
        out_csv,
        date_value,
        diagnostics_csv,
        args.known_unresolved_players_csv,
        bool(args.allow_8rain_public_catalog_fetch),
    )
    if summary.get("skipped"):
        return
    print(f"Wrote {out_csv}")
    print(
        "summary "
        f"rows={summary['rows']} win_column={summary['win_column']} "
        f"missing_home={summary['missing_home']} missing_away={summary['missing_away']} "
        f"missing_win={summary['missing_win']} "
        f"public_catalog_fetch_allowed={str(bool(summary.get('public_catalog_fetch_allowed'))).lower()} "
        f"public_catalog_fetch_attempted={str(bool(summary.get('public_catalog_fetch_attempted'))).lower()} "
        f"public_catalog_fetch_succeeded={str(bool(summary.get('public_catalog_fetch_succeeded'))).lower()} "
        f"cache_only_mode={str(bool(summary.get('cache_only_mode'))).lower()}"
    )


if __name__ == "__main__":
    main()
