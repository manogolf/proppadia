#!/usr/bin/env python3
"""Audit model-v2 tool upload rows against their parent selector/slate datasets."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts.tool_upload_8rain import TEAM_CODE_BY_ABBR


MARKET_TO_PROP = {
    "batter_hits": "hits",
    "batter_runs": "runs_scored",
    "batter_rbis": "rbis",
    "batter_bases": "total_bases",
    "batter_total_bases": "total_bases",
    "batter_h+r+rbi": "hits_runs_rbis",
    "batter_hits_runs_rbis": "hits_runs_rbis",
    "batter_walks": "walks",
    "batter_strikeouts": "strikeouts_batting",
    "pitcher_hits": "hits_allowed",
    "pitcher_hits_allowed": "hits_allowed",
    "pitcher_strikeouts": "strikeouts_pitching",
    "pitcher_outs": "outs_recorded",
    "outs_recorded": "outs_recorded",
    "outs recorded": "outs_recorded",
}

TEAM_ALIASES = {
    "AZ": "ARI",
    "ARI": "ARI",
    "ATH": "OAK",
    "OAK": "OAK",
    "CHW": "CWS",
    "CWS": "CWS",
    "SF": "SF",
    "SFG": "SF",
    "SD": "SD",
    "SDP": "SD",
    "KC": "KC",
    "KCR": "KC",
    "TB": "TB",
    "TBR": "TB",
    "WAS": "WSH",
    "WSH": "WSH",
}
TEAM_ALIASES.update({slug.upper(): TEAM_ALIASES.get(abbr.upper(), abbr.upper()) for abbr, slug in TEAM_CODE_BY_ABBR.items()})


def _norm_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return str(value).strip()


def _norm_key(value: Any) -> str:
    return _norm_text(value).lower()


def _norm_side(value: Any) -> str:
    side = _norm_key(value)
    if side in {"o", "over"}:
        return "over"
    if side in {"u", "under"}:
        return "under"
    return side


def _norm_team(value: Any) -> str:
    team = _norm_text(value).upper()
    return TEAM_ALIASES.get(team, team)


def _date_key(value: Any) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""
    if raw.isdigit() and len(raw) == 8:
        dt = pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(raw, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _to_id_text(value: Any) -> str:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return ""
        return str(int(float(value)))
    except Exception:
        return _norm_text(value)


def _market_to_prop(value: Any) -> str:
    market = _norm_key(value)
    return MARKET_TO_PROP.get(market, market)


def _line(value: Any) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return np.nan
    return round(float(parsed), 4)


def _load_upload(path: Path, source_file: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"DATE", "HOME", "AWAY", "MARKET", "SELECTOR", "POINT", "SIDE"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"{path} missing required upload columns: {missing}")
    out = df.copy()
    out["source_file"] = source_file
    out["upload_row_number"] = np.arange(len(out), dtype=int) + 1
    out["date_norm"] = out["DATE"].map(_date_key)
    out["player_id_norm"] = out["SELECTOR"].map(_to_id_text)
    out["prop_type_norm"] = out["MARKET"].map(_market_to_prop)
    out["line_norm"] = out["POINT"].map(_line)
    out["side_norm"] = out["SIDE"].map(_norm_side)
    out["home_norm"] = out["HOME"].map(_norm_team)
    out["away_norm"] = out["AWAY"].map(_norm_team)
    return out


def _prepare_lane(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    out = df.copy()
    out["date_norm"] = out.get("date", "").map(_date_key)
    out["player_id_norm"] = out.get("player_id", "").map(_to_id_text)
    out["prop_type_norm"] = out.get("prop_type", "").map(_norm_key)
    out["line_norm"] = out.get("line", "").map(_line)
    out["side_norm"] = out.get("side", "").map(_norm_side)
    home_source = "home_upload" if "home_upload" in out.columns else "home_team_code"
    away_source = "away_upload" if "away_upload" in out.columns else "away_team_code"
    out["home_norm"] = out.get(home_source, "").map(_norm_team)
    out["away_norm"] = out.get(away_source, "").map(_norm_team)
    return out


def _prepare_slate(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    out = df.copy()
    date_col = "game_date" if "game_date" in out.columns else "slate_date"
    out["date_norm"] = out.get(date_col, "").map(_date_key)
    out["player_id_norm"] = out.get("player_id", "").map(_to_id_text)
    out["prop_type_norm"] = out.get("prop_type", "").map(_norm_key)
    out["line_norm"] = out.get("line", "").map(_line)
    out["side_norm"] = out.get("model_pick_side", "").map(_norm_side)
    out["home_norm"] = out.get("home_team_code", "").map(_norm_team)
    out["away_norm"] = out.get("away_team_code", "").map(_norm_team)
    return out


def _count_exact(parent: pd.DataFrame, row: pd.Series) -> int:
    if parent.empty:
        return 0
    work = _filter_expected_parent_lane(parent, row)
    mask = (
        (work["date_norm"] == row["date_norm"])
        & (work["player_id_norm"] == row["player_id_norm"])
        & (work["prop_type_norm"] == row["prop_type_norm"])
        & (work["line_norm"] == row["line_norm"])
        & (work["side_norm"] == row["side_norm"])
        & (work["home_norm"] == row["home_norm"])
        & (work["away_norm"] == row["away_norm"])
    )
    return int(mask.sum())


def _count_without(parent: pd.DataFrame, row: pd.Series, omit: str) -> int:
    if parent.empty:
        return 0
    parent = _filter_expected_parent_lane(parent, row)
    cols = ["date_norm", "player_id_norm", "prop_type_norm", "line_norm", "side_norm", "home_norm", "away_norm"]
    cols.remove(omit)
    mask = pd.Series(True, index=parent.index)
    for col in cols:
        mask &= parent[col] == row[col]
    return int(mask.sum())


def _expected_parent_lane(row: pd.Series) -> str:
    source = _norm_key(row.get("source_file", ""))
    if source == "quick_card_tool_upload":
        return "quick_card_hits"
    if source == "ranking_tool_upload":
        return "ranking_lanes_excluding_quick_card"
    return ""


def _filter_expected_parent_lane(parent: pd.DataFrame, row: pd.Series) -> pd.DataFrame:
    if "source_lane" not in parent.columns:
        return parent
    expected = _expected_parent_lane(row)
    if expected == "quick_card_hits":
        return parent[parent["source_lane"].map(_norm_key) == "quick_card_hits"]
    if expected == "ranking_lanes_excluding_quick_card":
        return parent[parent["source_lane"].map(_norm_key) != "quick_card_hits"]
    return parent


def _audit_rows(upload: pd.DataFrame, lane: pd.DataFrame, slate: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in upload.iterrows():
        lane_exact = _count_exact(lane, row)
        slate_exact = _count_exact(slate, row)
        exact = lane_exact == 1
        duplicate = lane_exact > 1
        missing = lane_exact == 0
        mismatched_line = missing and _count_without(lane, row, "line_norm") > 0
        mismatched_side = missing and _count_without(lane, row, "side_norm") > 0
        mismatched_home_away = missing and (
            _count_without(lane, row, "home_norm") > 0 or _count_without(lane, row, "away_norm") > 0
        )
        present_in_slate = slate_exact > 0
        rows.append(
            {
                "source_file": row["source_file"],
                "upload_row_number": int(row["upload_row_number"]),
                "date": row["date_norm"],
                "selector": row.get("SELECTOR", ""),
                "player_id": row["player_id_norm"],
                "market": row.get("MARKET", ""),
                "prop_type": row["prop_type_norm"],
                "point": row.get("POINT", np.nan),
                "line": row["line_norm"],
                "side": row["side_norm"],
                "home": row.get("HOME", ""),
                "away": row.get("AWAY", ""),
                "home_norm": row["home_norm"],
                "away_norm": row["away_norm"],
                "expected_parent_lane": _expected_parent_lane(row),
                "exact_match_found": bool(exact),
                "match_count": int(lane_exact),
                "mismatched_line": bool(mismatched_line),
                "mismatched_side": bool(mismatched_side),
                "mismatched_home_away": bool(mismatched_home_away),
                "missing_row": bool(missing),
                "duplicate_parent_match": bool(duplicate),
                "slate_exact_match_found": bool(slate_exact == 1),
                "slate_match_count": int(slate_exact),
                "present_in_slate": bool(present_in_slate),
                "integrity_status": "exact_match"
                if exact
                else ("duplicate_parent_match" if duplicate else "missing_from_lane_parent"),
            }
        )
    return pd.DataFrame(rows)


def _summary(df: pd.DataFrame, *, date_value: str, out_csv: Path, lane_path: Path, slate_path: Path) -> dict[str, Any]:
    return {
        "date": date_value,
        "out_csv": str(out_csv),
        "lane_selector_csv": str(lane_path),
        "slate_csv": str(slate_path),
        "total_upload_rows": int(len(df)),
        "exact_matches": int(df["exact_match_found"].sum()),
        "mismatches": int((~df["exact_match_found"]).sum()),
        "missing_rows": int(df["missing_row"].sum()),
        "duplicate_parent_matches": int(df["duplicate_parent_match"].sum()),
        "mismatched_line": int(df["mismatched_line"].sum()),
        "mismatched_side": int(df["mismatched_side"].sum()),
        "mismatched_home_away": int(df["mismatched_home_away"].sum()),
        "present_in_slate": int(df["present_in_slate"].sum()),
        "by_source_file": {
            str(source): {
                "total_upload_rows": int(len(group)),
                "exact_matches": int(group["exact_match_found"].sum()),
                "mismatches": int((~group["exact_match_found"]).sum()),
                "missing_rows": int(group["missing_row"].sum()),
            }
            for source, group in df.groupby("source_file", dropna=False)
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--ranking-upload-csv", default="")
    parser.add_argument("--quick-card-upload-csv", default="")
    parser.add_argument("--lane-selector-csv", default="")
    parser.add_argument("--slate-csv", default="")
    parser.add_argument("--out-csv", default="")
    parser.add_argument("--summary-json", default="")
    args = parser.parse_args()

    date_value = _date_key(args.date)
    if not date_value:
        raise SystemExit(f"Invalid --date: {args.date}")

    upload_root = Path("backend/mlb/exports/model_v2/upload")
    upload_date_root = upload_root / date_value
    if args.ranking_upload_csv:
        ranking_path = Path(args.ranking_upload_csv)
    else:
        dated_ranking = upload_date_root / f"ranking_tool_upload_{date_value}.csv"
        legacy_ranking = upload_root / f"ranking_tool_upload_{date_value}.csv"
        ranking_path = dated_ranking if dated_ranking.exists() else legacy_ranking
    if args.quick_card_upload_csv:
        quick_path = Path(args.quick_card_upload_csv)
    else:
        dated_quick = upload_date_root / f"quick_card_tool_upload_{date_value}.csv"
        legacy_quick = upload_root / f"quick_card_tool_upload_{date_value}.csv"
        quick_path = dated_quick if dated_quick.exists() else legacy_quick
    if args.lane_selector_csv:
        lane_path = Path(args.lane_selector_csv)
    else:
        dated_lane = Path(f"backend/mlb/exports/model_v2/lanes/today/{date_value}/hits_lane_selector_{date_value}.csv")
        legacy_lane = Path(f"backend/mlb/exports/model_v2/lanes/today/hits_lane_selector_{date_value}.csv")
        lane_path = dated_lane if dated_lane.exists() else legacy_lane
    slate_path = Path(args.slate_csv or f"backend/mlb/exports/odds_history/{date_value}/mlb_slate_output.csv")
    reconcile_date_root = Path("backend/mlb/exports/model_v2/reconcile") / date_value
    out_csv = Path(args.out_csv or reconcile_date_root / f"upload_integrity_{date_value}.csv")
    summary_json = Path(args.summary_json or reconcile_date_root / f"upload_integrity_{date_value}_summary.json")

    for path in [ranking_path, quick_path, lane_path, slate_path]:
        if not path.exists():
            raise SystemExit(f"Missing required input: {path}")

    upload = pd.concat(
        [
            _load_upload(ranking_path, "ranking_tool_upload"),
            _load_upload(quick_path, "quick_card_tool_upload"),
        ],
        ignore_index=True,
    )
    lane = _prepare_lane(lane_path)
    slate = _prepare_slate(slate_path)
    audit = _audit_rows(upload, lane, slate)
    summary = _summary(audit, date_value=date_value, out_csv=out_csv, lane_path=lane_path, slate_path=slate_path)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(out_csv, index=False)
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {out_csv}")
    print(f"Wrote {summary_json}")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
