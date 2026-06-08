#!/usr/bin/env python3
"""Reconcile actual graded wager rows by source/category."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.mlb.shared.team_name_map import teamIdMap
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

PROP_TO_MARKET = {v: k for k, v in MARKET_TO_PROP.items()}
PROP_TO_BET_LABEL = {
    "hits": "Hits",
    "hits_allowed": "Hits Allowed",
    "total_bases": "Total Bases",
    "runs_scored": "Runs",
    "rbis": "RBIs",
}

NOTE_TO_SOURCE_CATEGORY = {
    "v2": "v2_ranking",
    "qc": "quick_card",
    "v1": "v1_steam",
    "steam": "v1_steam",
    "8r": "eight_r",
}

UPLOAD_TO_CANON_TEAM = {
    "AZ": "ARI",
    "ARI": "ARI",
    "ATH": "OAK",
    "OAK": "OAK",
    "CWS": "CWS",
    "CHW": "CWS",
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
UPLOAD_TO_CANON_TEAM.update(
    {
        slug.upper(): UPLOAD_TO_CANON_TEAM.get(abbr.upper(), abbr.upper())
        for abbr, slug in TEAM_CODE_BY_ABBR.items()
    }
)

OLD_SIDE_MIDDLE_BET_RE = re.compile(
    r"^(?P<player>.+?)\s+"
    r"(?P<label>Hits Allowed|Hits|Total Bases|Runs|RBIs?)\s+"
    r"(?P<side>Over|Under)\s+"
    r"(?P<line>-?\d+(?:\.\d+)?)$",
    re.IGNORECASE,
)
NEW_SIDE_FIRST_OVER_UNDER_BET_RE = re.compile(
    r"^(?P<side>Over|Under)\s+"
    r"(?P<player>.+?)\s+"
    r"(?P<label>Hits Allowed|Hits|Total Bases|Runs|RBIs?)\s+"
    r"Over/Under\s+"
    r"(?P<line>-?\d+(?:\.\d+)?)$",
    re.IGNORECASE,
)


def _norm_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return str(value).strip()


def _norm_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _norm_text(value).lower()).strip()


def _norm_key(value: Any) -> str:
    return _norm_text(value).lower().strip()


def _norm_side(value: Any) -> str:
    side = _norm_key(value)
    if side in {"o", "over"}:
        return "over"
    if side in {"u", "under"}:
        return "under"
    return side


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


def _line(value: Any) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return np.nan
    return round(float(parsed), 4)


def _to_float(value: Any) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _norm_team(value: Any) -> str:
    raw = _norm_text(value).upper()
    return UPLOAD_TO_CANON_TEAM.get(raw, raw)


def _team_name_reverse() -> dict[str, str]:
    out: dict[str, str] = {}
    for info in teamIdMap.values():
        abbr = _norm_team(info.get("abbr"))
        full = _norm_name(info.get("fullName"))
        if full and abbr:
            out[full] = abbr
    out[_norm_name("Athletics")] = "OAK"
    out[_norm_name("Arizona Diamondbacks")] = "ARI"
    out[_norm_name("Washington Nationals")] = "WSH"
    out[_norm_name("Chicago White Sox")] = "CWS"
    out[_norm_name("Kansas City Royals")] = "KC"
    out[_norm_name("San Diego Padres")] = "SD"
    out[_norm_name("San Francisco Giants")] = "SF"
    out[_norm_name("Tampa Bay Rays")] = "TB"
    return out


def _market_to_prop(value: Any) -> str:
    market = _norm_key(value)
    return MARKET_TO_PROP.get(market, market)


def _prop_from_bet_label(value: Any) -> str:
    label = _norm_key(value)
    if label == "hits":
        return "hits"
    if label == "hits allowed":
        return "hits_allowed"
    if label == "total bases":
        return "total_bases"
    if label == "runs":
        return "runs_scored"
    if label in {"rbi", "rbis"}:
        return "rbis"
    return label.replace(" ", "_")


def _result_from_grade(value: Any) -> str:
    grade = _norm_key(value)
    if grade in {"w", "win", "won"}:
        return "win"
    if grade in {"l", "loss", "lost"}:
        return "loss"
    if grade in {"p", "push", "void", "refund"}:
        return "push"
    return "unresolved"


def _source_category_from_note(value: Any) -> str:
    note = _norm_key(value)
    for token in note.split():
        if token in NOTE_TO_SOURCE_CATEGORY:
            return NOTE_TO_SOURCE_CATEGORY[token]
    return "misc_unclassified"


def _expected_upload_category(source_category: Any) -> str:
    category = _norm_key(source_category)
    if category == "v2_ranking":
        return "v2_ranking"
    if category == "quick_card":
        return "quick_card"
    if category == "v1_steam":
        return "v1_steam"
    return ""


def _american_profit(price: Any, won: bool) -> float | None:
    px = _to_float(price)
    if px is None:
        return None
    if not won:
        return -1.0
    if px > 0:
        return px / 100.0
    if px < 0:
        return 100.0 / abs(px)
    return None


def _load_upload(path: Path, date_value: str, source_category: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    out = df.copy()
    out["date_norm"] = out["DATE"].map(_date_key)
    out = out[out["date_norm"].eq(date_value)].copy()
    out["player_id_norm"] = out["SELECTOR"].map(_to_id_text)
    out["player_name_norm"] = out["SELECTOR"].map(_norm_name)
    out["prop_type_norm"] = out["MARKET"].map(_market_to_prop)
    out["line_norm"] = out["POINT"].map(_line)
    out["side_norm"] = out["SIDE"].map(_norm_side)
    out["home_norm"] = out["HOME"].map(_norm_team)
    out["away_norm"] = out["AWAY"].map(_norm_team)
    out["upload_win_prob"] = pd.to_numeric(out.get("WIN %"), errors="coerce")
    out["upload_source_category"] = source_category
    out["upload_source_file"] = str(path)
    out["upload_row_id"] = [f"{source_category}:{path.stem}:{i}" for i in range(len(out))]
    return out


def _unique_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for path in paths:
        key = str(path)
        if key in seen or not path.exists():
            continue
        seen.add(key)
        out.append(path)
    return out


def _discover_upload_paths(
    *,
    upload_root: Path,
    date_value: str,
    prefix: str,
    explicit: str = "",
    legacy_name: str = "",
) -> list[Path]:
    if explicit:
        return _unique_paths([Path(p.strip()) for p in explicit.split(",") if p.strip()])

    upload_date_root = upload_root / date_value
    paths: list[Path] = []
    if upload_date_root.exists():
        paths.extend(
            p
            for p in sorted(upload_date_root.glob(f"{prefix}*.csv"))
            if "diagnostics" not in p.name.lower()
        )
    if legacy_name:
        paths.append(upload_root / legacy_name)
    return _unique_paths(paths)


def _load_upload_many(paths: list[Path], date_value: str, source_category: str) -> pd.DataFrame:
    frames = [_load_upload(path, date_value, source_category) for path in paths]
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    dedupe_cols = [
        "date_norm",
        "player_id_norm",
        "player_name_norm",
        "prop_type_norm",
        "line_norm",
        "side_norm",
        "home_norm",
        "away_norm",
        "upload_source_category",
    ]
    return combined.drop_duplicates(subset=[c for c in dedupe_cols if c in combined.columns]).copy()


def _load_timestamped_upload_many(paths: list[Path], date_value: str, source_category: str) -> pd.DataFrame:
    frames = []
    for path in paths:
        if "__" not in path.stem:
            continue
        try:
            frame = _load_upload(path, date_value, source_category)
        except Exception:
            continue
        frame["matched_upload_run_tag"] = _upload_run_tag_from_path(path)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _upload_run_tag_from_path(path: Path | str) -> str:
    stem = Path(path).stem
    if "__" not in stem:
        return ""
    return stem.rsplit("__", 1)[-1]


def _upload_source_from_path(path: Path | str) -> str:
    name = Path(path).name
    if name.startswith("ranking_tool_upload"):
        return "ranking_tool"
    if name.startswith("quick_card_tool_upload"):
        return "quick_card_tool"
    return ""


def _load_v1_steam_upload(path: Path, date_value: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    out = df.copy()
    date_col = "date" if "date" in out.columns else "DATE"
    market_col = "market_key" if "market_key" in out.columns else "MARKET"
    side_col = "side" if "side" in out.columns else "SIDE"
    line_col = "line" if "line" in out.columns else "POINT"
    player_col = "player_name" if "player_name" in out.columns else "SELECTOR"
    price_col = "current_price" if "current_price" in out.columns else "price"
    out["date_norm"] = out[date_col].map(_date_key)
    out = out[out["date_norm"].eq(date_value)].copy()
    out["player_id_norm"] = ""
    out["player_name"] = out[player_col].map(_norm_text)
    out["player_name_norm"] = out[player_col].map(_norm_name)
    out["prop_type_norm"] = out[market_col].map(_market_to_prop)
    out["line_norm"] = out[line_col].map(_line)
    out["side_norm"] = out[side_col].map(_norm_side)
    out["home_norm"] = ""
    out["away_norm"] = ""
    out["upload_win_prob"] = np.nan
    out["price_uploaded_or_reconcile"] = pd.to_numeric(out.get(price_col), errors="coerce")
    out["upload_source_category"] = "v1_steam"
    out["upload_source_file"] = str(path)
    out["upload_row_id"] = [f"v1_steam:{path.stem}:{i}" for i in range(len(out))]
    return out


def _prepare_reconcile(path: Path) -> pd.DataFrame:
    rec = pd.read_csv(path, low_memory=False)
    rec = rec.copy()
    rec["date_norm"] = rec["game_date"].map(_date_key)
    rec["player_id_norm"] = rec["player_id"].map(_to_id_text)
    rec["player_name_norm"] = rec["player_name"].map(_norm_name)
    rec["prop_type_norm"] = rec["prop_type"].map(_norm_key)
    rec["line_norm"] = rec["line"].map(_line)
    rec["home_norm"] = rec["home_team_code"].map(_norm_team)
    rec["away_norm"] = rec["away_team_code"].map(_norm_team)
    return _filter_two_sided_valid_prices(rec)


def _filter_two_sided_valid_prices(df: pd.DataFrame) -> pd.DataFrame:
    required = {"price_over_american", "price_under_american"}
    if df.empty or not required.issubset(df.columns):
        return df
    over = pd.to_numeric(df["price_over_american"], errors="coerce")
    under = pd.to_numeric(df["price_under_american"], errors="coerce")
    mask = over.notna() & under.notna() & over.abs().ge(100) & under.abs().ge(100)
    if "book_count_two_sided" in df.columns:
        mask &= pd.to_numeric(df["book_count_two_sided"], errors="coerce").fillna(0).ge(2)
    return df.loc[mask].copy()


def _attach_upload_outcomes(upload: pd.DataFrame, rec: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    grouped = {
        key: group
        for key, group in rec.groupby(["date_norm", "player_id_norm", "prop_type_norm", "line_norm"], dropna=False)
    }
    for _, row in upload.iterrows():
        side = row["side_norm"]
        matches = grouped.get((row["date_norm"], row["player_id_norm"], row["prop_type_norm"], row["line_norm"]), pd.DataFrame())
        if matches.empty:
            enriched = row.to_dict()
            enriched.update(
                {
                    "upload_result": "missing",
                    "upload_pnl_1u": np.nan,
                    "price_uploaded_or_reconcile": np.nan,
                    "player_name": row.get("player_name", ""),
                    "player_name_norm": row.get("player_name_norm", ""),
                }
            )
            rows.append(enriched)
            continue
        price_col = "price_over_american" if side == "over" else "price_under_american"
        result_col = "actual_over_outcome" if side == "over" else "actual_under_outcome"
        pnl_col = "pnl_over_1u" if side == "over" else "pnl_under_1u"
        price_sort = pd.to_numeric(matches[price_col], errors="coerce").fillna(-999999)
        match = matches.loc[int(price_sort.idxmax())]
        result = _norm_key(match.get(result_col, ""))
        pnl = _to_float(match.get(pnl_col))
        if pnl is None and result in {"win", "loss"}:
            pnl = _american_profit(match.get(price_col), result == "win")
        enriched = row.to_dict()
        enriched.update(
            {
                "upload_result": result if result else "unresolved",
                "upload_pnl_1u": pnl,
                "price_uploaded_or_reconcile": match.get(price_col, np.nan),
                "player_name": match.get("player_name", ""),
                "player_name_norm": _norm_name(match.get("player_name", "")) or row.get("player_name_norm", ""),
                "reconcile_bookmaker_key": match.get("bookmaker_key", ""),
                "actual_value": match.get("actual_value", np.nan),
            }
        )
        rows.append(enriched)
    return pd.DataFrame(rows)


def _load_actual(path: Path, date_value: str, rec: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_csv(path)
    actual = df.copy()
    actual["note"] = actual.get("Notes", pd.Series("", index=actual.index)).map(_norm_key)
    actual["source_category"] = actual["note"].map(_source_category_from_note)
    actual["date_norm"] = actual["Event Date"].map(_date_key)
    actual = actual[actual["date_norm"].eq(date_value)].copy()
    team_rev = _team_name_reverse()
    actual["home_norm"] = actual["Home"].map(lambda x: team_rev.get(_norm_name(x), _norm_team(x)))
    actual["away_norm"] = actual["Away"].map(lambda x: team_rev.get(_norm_name(x), _norm_team(x)))
    parsed = actual["Bet"].map(_parse_bet)
    parsed_df = pd.DataFrame(list(parsed), index=actual.index)
    actual = pd.concat([actual, parsed_df], axis=1)
    actual["actual_wager_result"] = actual["Grade"].map(_result_from_grade)
    actual["raw_grade"] = actual.get("Grade", pd.Series("", index=actual.index)).map(_norm_text)
    actual["parsed_result"] = actual["actual_wager_result"]
    actual["actual_pnl"] = pd.to_numeric(actual.get("$ W/L"), errors="coerce")
    actual["actual_amount"] = pd.to_numeric(actual.get("Amount"), errors="coerce")
    actual["actual_pnl_units"] = actual["actual_pnl"] / actual["actual_amount"].replace(0, np.nan)
    actual["price_actually_bet"] = pd.to_numeric(actual.get("Odds"), errors="coerce")
    actual["actual_row_id"] = [f"actual_{i}" for i in range(len(actual))]
    actual = _attach_actual_player_ids(actual, rec)
    return actual


def _parse_bet(value: Any) -> dict[str, Any]:
    text = _norm_text(value)
    match = NEW_SIDE_FIRST_OVER_UNDER_BET_RE.match(text)
    pattern_used = "new_side_first_over_under" if match else ""
    if not match:
        match = OLD_SIDE_MIDDLE_BET_RE.match(text)
        pattern_used = "old_side_middle" if match else ""
    if not match:
        return {
            "bet_raw": text,
            "parse_pattern_used": "failed",
            "parsed_player_name": "",
            "parsed_player_name_norm": "",
            "parsed_side": "",
            "parsed_line": np.nan,
            "parsed_prop_type": "",
            "prop_type_norm": "",
            "side_norm": "",
            "line_norm": np.nan,
            "parse_ok": False,
        }
    player = match.group("player")
    prop_type = _prop_from_bet_label(match.group("label"))
    side = _norm_side(match.group("side"))
    line = _line(match.group("line"))
    return {
        "bet_raw": text,
        "parse_pattern_used": pattern_used,
        "parsed_player_name": player,
        "parsed_player_name_norm": _norm_name(player),
        "parsed_side": side,
        "parsed_line": line,
        "parsed_prop_type": prop_type,
        "prop_type_norm": prop_type,
        "side_norm": side,
        "line_norm": line,
        "parse_ok": True,
    }


def _attach_actual_player_ids(actual: pd.DataFrame, rec: pd.DataFrame) -> pd.DataFrame:
    out = actual.copy()
    lookup_cols = ["date_norm", "player_name_norm", "prop_type_norm", "line_norm"]
    rec_lookup = rec.drop_duplicates(lookup_cols)[lookup_cols + ["player_id_norm", "player_name"]].copy()
    merged = out.merge(
        rec_lookup,
        how="left",
        left_on=["date_norm", "parsed_player_name_norm", "prop_type_norm", "line_norm"],
        right_on=lookup_cols,
        suffixes=("", "_rec"),
    )
    merged["player_id_norm"] = merged["player_id_norm"].fillna("")
    missing = merged["player_id_norm"].astype(str).eq("")
    if missing.any():
        rec_name_lookup = rec[["date_norm", "player_name_norm", "player_id_norm", "player_name"]].dropna().copy()
        rec_name_lookup = rec_name_lookup[
            rec_name_lookup["player_id_norm"].astype(str).ne("")
        ].drop_duplicates(["date_norm", "player_name_norm", "player_id_norm"])
        unique_rec_names = rec_name_lookup.groupby(["date_norm", "player_name_norm"]).filter(
            lambda group: group["player_id_norm"].nunique() == 1
        )
        if not unique_rec_names.empty:
            fallback = merged.loc[missing].drop(columns=["player_id_norm", "player_name_rec"], errors="ignore").merge(
                unique_rec_names.drop_duplicates(["date_norm", "player_name_norm"]),
                how="left",
                left_on=["date_norm", "parsed_player_name_norm"],
                right_on=["date_norm", "player_name_norm"],
                suffixes=("", "_rec_name"),
            )
            merged.loc[missing, "player_id_norm"] = fallback["player_id_norm"].fillna("").to_numpy()

    missing = merged["player_id_norm"].astype(str).eq("")
    if missing.any():
        slate_lookup = _load_slate_player_lookup(sorted(set(merged.loc[missing, "date_norm"].dropna().astype(str))))
        if not slate_lookup.empty:
            fallback = merged.loc[missing].drop(columns=["player_id_norm", "player_name_rec"], errors="ignore").merge(
                slate_lookup,
                how="left",
                left_on=["date_norm", "parsed_player_name_norm", "prop_type_norm", "line_norm"],
                right_on=["date_norm", "player_name_norm", "prop_type_norm", "line_norm"],
                suffixes=("", "_slate"),
            )
            merged.loc[missing, "player_id_norm"] = fallback["player_id_norm"].fillna("").to_numpy()
    return merged


def _load_slate_player_lookup(dates: list[str]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for date_value in dates:
        day_dir = Path("backend/mlb/exports/odds_history") / date_value
        if not day_dir.exists():
            continue
        for path in sorted(day_dir.glob("mlb_slate_output*.csv")):
            try:
                df = pd.read_csv(
                    path,
                    usecols=["slate_date", "player_id", "player_name", "prop_type", "line"],
                    low_memory=False,
                )
            except Exception:
                continue
            df = df.copy()
            df["date_norm"] = df["slate_date"].map(_date_key)
            df["player_id_norm"] = df["player_id"].map(_to_id_text)
            df["player_name_norm"] = df["player_name"].map(_norm_name)
            df["prop_type_norm"] = df["prop_type"].map(_norm_key)
            df["line_norm"] = df["line"].map(_line)
            rows.append(
                df[
                    [
                        "date_norm",
                        "player_name_norm",
                        "prop_type_norm",
                        "line_norm",
                        "player_id_norm",
                    ]
                ]
            )
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True)
    combined = combined[
        combined["date_norm"].astype(str).ne("")
        & combined["player_name_norm"].astype(str).ne("")
        & combined["player_id_norm"].astype(str).ne("")
    ]
    return combined.drop_duplicates(
        ["date_norm", "player_name_norm", "prop_type_norm", "line_norm", "player_id_norm"]
    )


def _key(df: pd.DataFrame) -> pd.Series:
    return (
        df["date_norm"].astype(str)
        + "|"
        + df["player_id_norm"].astype(str)
        + "|"
        + df["prop_type_norm"].astype(str)
        + "|"
        + df["line_norm"].astype(str)
        + "|"
        + df["side_norm"].astype(str)
    )


def _name_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["date_norm"].astype(str)
        + "|"
        + df["player_name_norm"].astype(str)
        + "|"
        + df["prop_type_norm"].astype(str)
        + "|"
        + df["line_norm"].astype(str)
        + "|"
        + df["side_norm"].astype(str)
    )


def _name_side_line_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["date_norm"].astype(str)
        + "|"
        + df["player_name_norm"].astype(str)
        + "|"
        + df["side_norm"].astype(str)
        + "|"
        + df["line_norm"].astype(str)
    )


def _name_line_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["date_norm"].astype(str)
        + "|"
        + df["player_name_norm"].astype(str)
        + "|"
        + df["line_norm"].astype(str)
    )


def _name_prop_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["date_norm"].astype(str)
        + "|"
        + df["player_name_norm"].astype(str)
        + "|"
        + df["prop_type_norm"].astype(str)
    )


def _build_upload_index(upload: pd.DataFrame) -> dict[str, dict[str, list[dict[str, Any]]]]:
    index: dict[str, dict[str, list[dict[str, Any]]]] = {}
    if upload.empty:
        return index
    work = upload.copy()
    work["common_key"] = _key(work)
    work["name_key"] = _name_key(work)
    for _, row in work.iterrows():
        category = str(row.get("upload_source_category") or "")
        if not category:
            continue
        index.setdefault(category, {})
        for key_col in ("common_key", "name_key"):
            key = str(row.get(key_col) or "")
            if key and "nan" not in key.lower():
                index[category].setdefault(key, []).append(row.to_dict())
    return index


def _match_uploads_for_actual(row: pd.Series, upload_index: dict[str, dict[str, list[dict[str, Any]]]]) -> tuple[list[dict[str, Any]], str]:
    expected = _expected_upload_category(row.get("source_category"))
    categories = [expected] if expected else []
    if not categories:
        categories = ["v2_ranking", "quick_card", "v1_steam"]
    keys = [str(row.get("common_key") or ""), str(row.get("name_key") or "")]
    matches: list[dict[str, Any]] = []
    strategy = ""
    for category in categories:
        for key in keys:
            if not key or "nan" in key.lower():
                continue
            found = upload_index.get(category, {}).get(key, [])
            if found:
                matches.extend(found)
                strategy = f"{category}:{'id_key' if key == keys[0] else 'name_key'}"
    if matches or expected:
        deduped = {str(m.get("upload_row_id") or id(m)): m for m in matches}
        return list(deduped.values()), strategy
    for category in ("v2_ranking", "quick_card", "v1_steam"):
        for key in keys:
            if not key or "nan" in key.lower():
                continue
            found = upload_index.get(category, {}).get(key, [])
            if found:
                matches.extend(found)
                strategy = f"{category}:{'id_key' if key == keys[0] else 'name_key'}"
    deduped = {str(m.get("upload_row_id") or id(m)): m for m in matches}
    return list(deduped.values()), strategy


def _build_comparison(upload: pd.DataFrame, actual: pd.DataFrame) -> pd.DataFrame:
    upload = upload.copy()
    actual = actual.copy()
    if not upload.empty:
        upload["common_key"] = _key(upload)
        upload["name_key"] = _name_key(upload)
    actual["common_key"] = _key(actual)
    actual["name_key"] = _name_key(actual)
    upload_index = _build_upload_index(upload)

    rows: list[dict[str, Any]] = []
    matched_upload_ids: set[str] = set()
    for _, row in actual.iterrows():
        matches, strategy = _match_uploads_for_actual(row, upload_index)
        expected_category = _expected_upload_category(row.get("source_category"))
        expected_match_count = sum(
            1 for m in matches if str(m.get("upload_source_category") or "") == expected_category
        )
        matched_categories = sorted({str(m.get("upload_source_category") or "") for m in matches if m.get("upload_source_category")})
        for m in matches:
            if m.get("upload_row_id"):
                matched_upload_ids.add(str(m["upload_row_id"]))
        best = matches[0] if matches else {}
        out = {
            "row_type": "actual_wager",
            "date": row.get("date_norm"),
            "wager_id": row.get("Wager ID"),
            "note": row.get("note"),
            "source_category": row.get("source_category"),
            "expected_upload_category": expected_category,
            "matched_expected_upload": bool(expected_category and expected_match_count > 0),
            "matched_any_upload": bool(matches),
            "matched_upload_categories": ",".join(matched_categories),
            "match_count": int(len(matches)),
            "expected_match_count": int(expected_match_count),
            "ambiguous_match": bool(len(matches) > 1 or expected_match_count > 1),
            "match_strategy": strategy,
            "matched_upload_row_id": best.get("upload_row_id", ""),
            "matched_upload_source_file": best.get("upload_source_file", ""),
            "bet": row.get("Bet"),
            "bet_raw": row.get("bet_raw"),
            "parse_pattern_used": row.get("parse_pattern_used"),
            "parse_ok": row.get("parse_ok"),
            "parsed_player_name": row.get("parsed_player_name"),
            "parsed_side": row.get("parsed_side"),
            "parsed_line": row.get("parsed_line"),
            "parsed_prop_type": row.get("parsed_prop_type"),
            "raw_grade": row.get("raw_grade"),
            "parsed_result": row.get("parsed_result"),
            "book": row.get("Book"),
            "player_name": row.get("player_name") or row.get("parsed_player_name"),
            "player_id": row.get("player_id_norm"),
            "prop_type": row.get("prop_type_norm"),
            "line": row.get("line_norm"),
            "side": row.get("side_norm"),
            "home": row.get("home_norm"),
            "away": row.get("away_norm"),
            "result": row.get("actual_wager_result"),
            "units": row.get("actual_pnl_units"),
            "amount": row.get("actual_amount"),
            "price": row.get("price_actually_bet"),
        }
        rows.append(out)

    if not upload.empty:
        for _, row in upload[~upload["upload_row_id"].astype(str).isin(matched_upload_ids)].iterrows():
            rows.append(
                {
                    "row_type": "upload_not_wagered",
                    "date": row.get("date_norm"),
                    "wager_id": "",
                    "note": "",
                    "source_category": row.get("upload_source_category"),
                    "expected_upload_category": row.get("upload_source_category"),
                    "matched_expected_upload": False,
                    "matched_any_upload": False,
                    "matched_upload_categories": "",
                    "match_count": 0,
                    "expected_match_count": 0,
                    "ambiguous_match": False,
                    "match_strategy": "",
                    "matched_upload_row_id": row.get("upload_row_id"),
                    "matched_upload_source_file": row.get("upload_source_file"),
                    "bet": "",
                    "bet_raw": "",
                    "parse_pattern_used": "",
                    "parse_ok": np.nan,
                    "parsed_player_name": "",
                    "parsed_side": "",
                    "parsed_line": np.nan,
                    "parsed_prop_type": "",
                    "raw_grade": "",
                    "parsed_result": row.get("upload_result"),
                    "book": "",
                    "player_name": row.get("player_name"),
                    "player_id": row.get("player_id_norm"),
                    "prop_type": row.get("prop_type_norm"),
                    "line": row.get("line_norm"),
                    "side": row.get("side_norm"),
                    "home": row.get("home_norm"),
                    "away": row.get("away_norm"),
                    "result": row.get("upload_result"),
                    "units": row.get("upload_pnl_1u"),
                    "amount": np.nan,
                    "price": row.get("price_uploaded_or_reconcile"),
                }
            )
    return pd.DataFrame(rows)


def _summary_for(group: pd.DataFrame, result_col: str, pnl_col: str, price_col: str = "price") -> dict[str, Any]:
    resolved = group[group[result_col].isin(["win", "loss", "push"])].copy()
    wins = int((resolved[result_col] == "win").sum())
    losses = int((resolved[result_col] == "loss").sum())
    pushes = int((resolved[result_col] == "push").sum())
    profit = float(pd.to_numeric(resolved[pnl_col], errors="coerce").fillna(0).sum())
    risk = wins + losses + pushes
    prices = pd.to_numeric(group.get(price_col, pd.Series(dtype=float)), errors="coerce")
    return {
        "bets": int(len(group)),
        "resolved_count": int(len(resolved)),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "unresolved": int((~group[result_col].isin(["win", "loss", "push"])).sum()),
        "win_rate": wins / (wins + losses) if wins + losses else None,
        "roi": profit / risk if risk else None,
        "units": profit,
        "avg_price": float(prices.mean()) if prices.notna().any() else None,
    }


def _records_for_json(df: pd.DataFrame, limit: int = 25) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return json.loads(df.head(limit).to_json(orient="records"))


def _load_overlap_snapshot(path: Path, date_value: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    snap = pd.read_csv(path, low_memory=False)
    if snap.empty:
        return snap
    out = snap.copy()
    out["date_norm"] = out.get("snapshot_date", pd.Series("", index=out.index)).map(_date_key)
    out = out[out["date_norm"].eq(date_value)].copy()
    if out.empty:
        return out
    out["player_id_norm"] = out.get("player_id", pd.Series("", index=out.index)).map(_to_id_text)
    out["player_name_norm"] = out.get("player_name", pd.Series("", index=out.index)).map(_norm_name)
    out["prop_type_norm"] = out.get("prop_type", pd.Series("", index=out.index)).map(_market_to_prop)
    out["line_norm"] = out.get("line", pd.Series("", index=out.index)).map(_line)
    out["side_norm"] = out.get("side", pd.Series("", index=out.index)).map(_norm_side)
    out["snapshot_common_key"] = _key(out)
    out.loc[out["player_id_norm"].astype(str).eq(""), "snapshot_common_key"] = ""
    out["snapshot_name_key"] = _name_key(out)
    keep = [
        "snapshot_common_key",
        "snapshot_name_key",
        "overlap_flag",
        "formation_bucket",
    ]
    out = out[[c for c in keep if c in out.columns]].copy()
    out["formation_bucket"] = out.get("formation_bucket", pd.Series("unmatched_snapshot", index=out.index)).map(_norm_key)
    raw_overlap = out.get("overlap_flag", pd.Series(False, index=out.index)).astype(str).str.lower()
    out["overlap_flag"] = raw_overlap.isin({"1", "1.0", "true", "yes"}) | out["formation_bucket"].eq("overlap")
    return out.drop_duplicates(["snapshot_common_key", "formation_bucket"], keep="last")


def _candidate_field(df: pd.DataFrame, *names: str) -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series("", index=df.index)


def _prepare_candidate_frame(df: pd.DataFrame, date_value: str, source: str, date_col: str = "date") -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["candidate_source"] = source
    out["date_norm"] = _candidate_field(out, date_col, "snapshot_date", "date").map(_date_key)
    out = out[out["date_norm"].eq(date_value)].copy()
    if out.empty:
        return out
    out["player_id_norm"] = _candidate_field(out, "player_id", "SELECTOR").map(_to_id_text)
    out["player_name"] = _candidate_field(out, "player_name", "player", "parsed_player_name", "SELECTOR").map(_norm_text)
    out["player_name_norm"] = out["player_name"].map(_norm_name)
    out["prop_type_norm"] = _candidate_field(out, "prop_type", "MARKET").map(_market_to_prop)
    out["side_norm"] = _candidate_field(out, "side", "SIDE").map(_norm_side)
    out["line_norm"] = _candidate_field(out, "line", "POINT").map(_line)
    out["formation_bucket"] = _candidate_field(out, "formation_bucket").map(_norm_key)
    out["common_key"] = _key(out)
    out.loc[out["player_id_norm"].astype(str).eq(""), "common_key"] = ""
    out["name_key"] = _name_key(out)
    out["name_line_key"] = _name_line_key(out)
    out["name_prop_key"] = _name_prop_key(out)
    out["name_side_line_key"] = _name_side_line_key(out)
    return out


def _load_snapshot_candidates(snapshot_csv: Path, date_value: str) -> pd.DataFrame:
    if not snapshot_csv.exists():
        return pd.DataFrame()
    try:
        snap = pd.read_csv(snapshot_csv, low_memory=False)
    except Exception:
        return pd.DataFrame()
    return _prepare_candidate_frame(snap, date_value, "snapshot", date_col="snapshot_date")


def _load_registry_candidates(date_value: str) -> pd.DataFrame:
    paths = [
        Path(f"artifacts/analysis/mlb/research_gap_analysis/daily_candidate_registry/mlb_v2_daily_candidate_registry_{date_value}.csv"),
        Path("artifacts/analysis/mlb/research_gap_analysis/mlb_v2_daily_candidate_registry.csv"),
    ]
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        prepared = _prepare_candidate_frame(df, date_value, f"registry:{path.name}", date_col="date")
        if not prepared.empty:
            frames.append(prepared)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates(["common_key", "name_key", "formation_bucket"], keep="last")


def _load_lane_candidates(date_value: str) -> pd.DataFrame:
    lane_root = Path("backend/mlb/exports/model_v2/lanes/today") / date_value
    paths = [
        (lane_root / f"hits_lane_selector_{date_value}_ranking_upload_input.csv", "lane:ranking_upload_input"),
        (lane_root / f"quick_card_hits_{date_value}.csv", "lane:quick_card_hits"),
        (lane_root / f"hits_lane_selector_{date_value}.csv", "lane:selector"),
    ]
    frames: list[pd.DataFrame] = []
    for path, source in paths:
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        prepared = _prepare_candidate_frame(df, date_value, source, date_col="date")
        if not prepared.empty:
            frames.append(prepared)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates(["common_key", "name_key", "candidate_source"], keep="last")


def _upload_candidates_for_diagnostics(comp: pd.DataFrame, date_value: str) -> pd.DataFrame:
    upload = comp[comp.get("row_type", pd.Series("", index=comp.index)).eq("upload_not_wagered")].copy()
    actual_upload_matches = comp[
        comp.get("row_type", pd.Series("", index=comp.index)).eq("actual_wager")
        & comp.get("matched_any_upload", pd.Series(False, index=comp.index)).astype(bool)
    ].copy()
    if not actual_upload_matches.empty:
        actual_upload_matches["upload_source_category"] = actual_upload_matches.get("matched_upload_categories", "")
        actual_upload_matches["upload_source_file"] = actual_upload_matches.get("matched_upload_source_file", "")
    frames = [df for df in [upload, actual_upload_matches] if not df.empty]
    if not frames:
        return pd.DataFrame()
    candidates = pd.concat(frames, ignore_index=True, sort=False)
    prepared = _prepare_candidate_frame(candidates, date_value, "upload", date_col="date")
    if prepared.empty:
        return prepared
    prepared["candidate_source"] = prepared.get("upload_source_file", pd.Series("upload", index=prepared.index)).fillna("upload").astype(str)
    return prepared.drop_duplicates(["common_key", "name_key", "candidate_source"], keep="last")


def _find_candidate(actual: pd.Series, candidates: pd.DataFrame) -> tuple[pd.Series | None, str]:
    if candidates.empty:
        return None, ""
    checks = [
        ("exact_primary_id", "common_key"),
        ("exact_secondary_name", "name_key"),
        ("same_date_player_id", "player_id_norm"),
        ("same_date_name_line", "name_line_key"),
        ("same_date_name_prop", "name_prop_key"),
        ("same_date_name_side_line_ignore_prop", "name_side_line_key"),
        ("same_date_name", "player_name_norm"),
    ]
    for strategy, col in checks:
        if col not in candidates.columns:
            continue
        if col == "player_id_norm":
            key = str(actual.get("player_id_norm") or "")
            mask = candidates["date_norm"].astype(str).eq(str(actual.get("date_norm") or "")) & candidates[col].astype(str).eq(key)
            mask &= key != ""
        elif col == "player_name_norm":
            key = str(actual.get("player_name_norm") or "")
            mask = candidates["date_norm"].astype(str).eq(str(actual.get("date_norm") or "")) & candidates[col].astype(str).eq(key)
            mask &= key != ""
        else:
            key = str(actual.get(col) or "")
            if col in {"name_key", "name_line_key", "name_prop_key", "name_side_line_key"} and not str(actual.get("player_name_norm") or ""):
                continue
            mask = candidates[col].astype(str).eq(key) & (key != "") & (~pd.Series([key]).astype(str).str.contains("nan", case=False).iloc[0])
        found = candidates.loc[mask]
        if not found.empty:
            return found.iloc[0], strategy
    return None, ""


def _candidate_payload(candidate: pd.Series | None, strategy: str, prefix: str) -> dict[str, Any]:
    if candidate is None:
        return {
            f"closest_{prefix}_match_strategy": "",
            f"closest_{prefix}_player_id": "",
            f"closest_{prefix}_player_name": "",
            f"closest_{prefix}_prop_type": "",
            f"closest_{prefix}_side": "",
            f"closest_{prefix}_line": "",
            f"closest_{prefix}_formation_bucket": "",
            f"closest_{prefix}_source": "",
        }
    return {
        f"closest_{prefix}_match_strategy": strategy,
        f"closest_{prefix}_player_id": candidate.get("player_id_norm", ""),
        f"closest_{prefix}_player_name": candidate.get("player_name", ""),
        f"closest_{prefix}_prop_type": candidate.get("prop_type_norm", ""),
        f"closest_{prefix}_side": candidate.get("side_norm", ""),
        f"closest_{prefix}_line": candidate.get("line_norm", ""),
        f"closest_{prefix}_formation_bucket": candidate.get("formation_bucket", ""),
        f"closest_{prefix}_source": candidate.get("candidate_source", ""),
    }


def _mismatch_reason(actual: pd.Series, snap: pd.Series | None, snap_strategy: str, registry: pd.Series | None, lane: pd.Series | None, upload: pd.Series | None) -> tuple[str, str]:
    source_category = _norm_key(actual.get("source_category"))
    if source_category not in {"v2_ranking", "quick_card"}:
        return "actual wager source category not represented in snapshot", "No overlap formation bucket is expected for this source category."
    if not _norm_text(actual.get("player_id_norm")) and not _norm_text(actual.get("player_name_norm")):
        return "player_id missing on actual wager row", "Attach player identity from graded wager/reconcile parser before snapshot enrichment."
    if snap is not None:
        if snap_strategy == "same_date_player_id":
            fields = []
            for label, col in [("prop_type", "prop_type_norm"), ("side", "side_norm"), ("line", "line_norm")]:
                if str(actual.get(col)) != str(snap.get(col)):
                    fields.append(label)
            reason = f"{'/'.join(fields)} mismatch" if fields else "player_id mismatch but player_name matches"
            return reason, "Exact snapshot enrichment requires date + player_id/name + prop_type + side + line."
        if snap_strategy in {"same_date_name_line", "same_date_name_prop"}:
            return "player_name normalization mismatch", "Inspect player identity/name normalization or prop/side/line differences against snapshot."
    if registry is not None:
        return "candidate exists in registry but not snapshot", "Refresh overlap snapshot capture before reconcile or investigate registry/snapshot drift."
    if lane is not None:
        return "candidate exists in selected-side lane artifact but not snapshot", "Refresh overlap snapshot capture from selected-side formation artifacts."
    if upload is not None:
        return "candidate exists only in upload file", "This wager matched a timestamped upload but was not present in formation snapshot."
    return "formation snapshot missing candidate", "No matching selected-side formation candidate was found in snapshot, registry, lane, or upload diagnostics."


def _build_overlap_unmatched_diagnostics(
    enriched: pd.DataFrame,
    snapshot_csv: Path,
    date_value: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    empty_columns = [
        "date",
        "wager_id",
        "note",
        "source_category",
        "actual_player_id",
        "actual_player_name",
        "actual_prop_type",
        "actual_side",
        "actual_line",
        "bet",
        "mismatch_reason",
        "suggested_fix",
    ]
    unmatched_mask = (
        enriched.get("_trace_source", pd.Series("unmatched", index=enriched.index)).astype(str).eq("unmatched")
        if "_trace_source" in enriched.columns
        else enriched.get("formation_bucket", pd.Series("", index=enriched.index)).eq("unmatched_snapshot")
    )
    actual = enriched[
        enriched.get("row_type", pd.Series("", index=enriched.index)).eq("actual_wager")
        & unmatched_mask
    ].copy()
    if actual.empty:
        return pd.DataFrame(columns=empty_columns), {
            "unmatched_by_reason": {},
            "registry_candidate_matches": 0,
            "upload_candidate_matches": 0,
            "lane_candidate_matches": 0,
        }

    actual["date_norm"] = actual.get("date", pd.Series("", index=actual.index)).map(_date_key)
    actual["player_id_norm"] = actual.get("player_id", pd.Series("", index=actual.index)).map(_to_id_text)
    actual["player_name_norm"] = actual.get("player_name", pd.Series("", index=actual.index)).map(_norm_name)
    actual["prop_type_norm"] = actual.get("prop_type", pd.Series("", index=actual.index)).map(_market_to_prop)
    actual["side_norm"] = actual.get("side", pd.Series("", index=actual.index)).map(_norm_side)
    actual["line_norm"] = actual.get("line", pd.Series("", index=actual.index)).map(_line)
    actual["common_key"] = _key(actual)
    actual.loc[actual["player_id_norm"].astype(str).eq(""), "common_key"] = ""
    actual["name_key"] = _name_key(actual)
    actual["name_line_key"] = _name_line_key(actual)
    actual["name_prop_key"] = _name_prop_key(actual)
    actual["name_side_line_key"] = _name_side_line_key(actual)

    snapshot_candidates = _load_snapshot_candidates(snapshot_csv, date_value)
    registry_candidates = _load_registry_candidates(date_value)
    lane_candidates = _load_lane_candidates(date_value)
    upload_candidates = _upload_candidates_for_diagnostics(enriched, date_value)

    rows: list[dict[str, Any]] = []
    registry_matches = 0
    lane_matches = 0
    upload_matches = 0
    for _, row in actual.iterrows():
        snap_candidate, snap_strategy = _find_candidate(row, snapshot_candidates)
        registry_candidate, registry_strategy = _find_candidate(row, registry_candidates)
        lane_candidate, lane_strategy = _find_candidate(row, lane_candidates)
        upload_candidate, upload_strategy = _find_candidate(row, upload_candidates)
        registry_matches += int(registry_candidate is not None)
        lane_matches += int(lane_candidate is not None)
        upload_matches += int(upload_candidate is not None)
        reason, suggested_fix = _mismatch_reason(row, snap_candidate, snap_strategy, registry_candidate, lane_candidate, upload_candidate)
        payload = {
            "date": row.get("date", ""),
            "wager_id": row.get("wager_id", ""),
            "note": row.get("note", ""),
            "source_category": row.get("source_category", ""),
            "actual_player_id": row.get("player_id", ""),
            "actual_player_name": row.get("player_name", ""),
            "actual_player_name_norm": row.get("player_name_norm", ""),
            "actual_prop_type": row.get("prop_type", ""),
            "actual_side": row.get("side", ""),
            "actual_line": row.get("line", ""),
            "bet": row.get("bet", ""),
            "matched_any_upload": row.get("matched_any_upload", False),
            "matched_upload_categories": row.get("matched_upload_categories", ""),
            "matched_upload_source_file": row.get("matched_upload_source_file", ""),
            "mismatch_reason": reason,
            "suggested_fix": suggested_fix,
        }
        payload.update(_candidate_payload(snap_candidate, snap_strategy, "snapshot"))
        payload.update(_candidate_payload(registry_candidate, registry_strategy, "registry"))
        payload.update(_candidate_payload(lane_candidate, lane_strategy, "lane"))
        payload.update(_candidate_payload(upload_candidate, upload_strategy, "upload"))
        rows.append(payload)

    diag = pd.DataFrame(rows)
    return diag, {
        "unmatched_by_reason": {str(k): int(v) for k, v in diag["mismatch_reason"].value_counts(dropna=False).to_dict().items()},
        "registry_candidate_matches": int(registry_matches),
        "upload_candidate_matches": int(upload_matches),
        "lane_candidate_matches": int(lane_matches),
    }


def _build_timestamped_upload_index(timestamped_upload: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    if timestamped_upload.empty:
        return index
    work = timestamped_upload.copy()
    work["common_key"] = _key(work)
    work.loc[work["player_id_norm"].astype(str).eq(""), "common_key"] = ""
    work["name_key"] = _name_key(work)
    for _, row in work.iterrows():
        payload = row.to_dict()
        for key_col in ("common_key", "name_key"):
            key = str(row.get(key_col) or "")
            if key and "nan" not in key.lower():
                index.setdefault(key, []).append(payload)
    return index


def _upload_bucket_from_categories(categories: set[str]) -> str:
    has_ranking = "v2_ranking" in categories
    has_quick = "quick_card" in categories
    if has_ranking and has_quick:
        return "overlap_upload"
    if has_ranking:
        return "ranking_only_upload"
    if has_quick:
        return "quick_card_only_upload"
    return "unmatched_snapshot"


def _apply_timestamped_upload_provenance(
    comp: pd.DataFrame,
    timestamped_upload: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = comp.copy()
    out["matched_upload_file"] = out.get("matched_upload_source_file", pd.Series("", index=out.index)).fillna("")
    out["matched_upload_run_tag"] = out["matched_upload_file"].map(_upload_run_tag_from_path)
    formation = out.get("formation_bucket", pd.Series("unmatched_snapshot", index=out.index)).astype(str)
    out["_trace_source"] = np.where(formation.ne("unmatched_snapshot"), "upload_match", "unmatched")
    actual_mask = out.get("row_type", pd.Series("", index=out.index)).eq("actual_wager")
    external_mask = actual_mask & out.get("source_category", pd.Series("", index=out.index)).astype(str).isin(
        ["eight_r", "8r"]
    )
    if external_mask.any():
        out.loc[external_mask, "formation_bucket"] = "eight_r"
        out.loc[external_mask, "_trace_source"] = "external_source"
        out.loc[external_mask, "overlap_flag"] = False

    summary = {
        "external_source_rows": int(external_mask.sum()),
        "eight_r_external_rows": int(external_mask.sum()),
        "truly_unmatched_rows": 0,
        "still_unmatched_rows": 0,
        "overlap_upload_rows": 0,
        "ranking_only_upload_rows": 0,
        "quick_card_only_upload_rows": 0,
    }
    if timestamped_upload.empty or not actual_mask.any():
        truly_unmatched = int((actual_mask & out["_trace_source"].eq("unmatched")).sum())
        summary["truly_unmatched_rows"] = truly_unmatched
        summary["still_unmatched_rows"] = truly_unmatched
        return out, summary

    work = out.loc[actual_mask].copy()
    work["date_norm"] = work.get("date", pd.Series("", index=work.index)).map(_date_key)
    work["player_id_norm"] = work.get("player_id", pd.Series("", index=work.index)).map(_to_id_text)
    work["player_name_norm"] = work.get("player_name", pd.Series("", index=work.index)).map(_norm_name)
    work["prop_type_norm"] = work.get("prop_type", pd.Series("", index=work.index)).map(_market_to_prop)
    work["side_norm"] = work.get("side", pd.Series("", index=work.index)).map(_norm_side)
    work["line_norm"] = work.get("line", pd.Series("", index=work.index)).map(_line)
    work["common_key"] = _key(work)
    work.loc[work["player_id_norm"].astype(str).eq(""), "common_key"] = ""
    work["name_key"] = _name_key(work)

    upload_index = _build_timestamped_upload_index(timestamped_upload)
    for idx, row in work.iterrows():
        if str(out.loc[idx, "_trace_source"]) == "upload_match":
            continue
        matches: list[dict[str, Any]] = []
        for key in [str(row.get("common_key") or ""), str(row.get("name_key") or "")]:
            if not key or "nan" in key.lower():
                continue
            matches.extend(upload_index.get(key, []))
            if matches:
                break
        if not matches:
            continue
        categories = {str(m.get("upload_source_category") or "") for m in matches if m.get("upload_source_category")}
        bucket = _upload_bucket_from_categories(categories)
        if bucket == "unmatched_snapshot":
            continue
        files = sorted({str(m.get("upload_source_file") or "") for m in matches if m.get("upload_source_file")})
        run_tags = sorted({_upload_run_tag_from_path(file) for file in files if _upload_run_tag_from_path(file)})
        out.loc[idx, "formation_bucket"] = bucket
        out.loc[idx, "_trace_source"] = "upload_match"
        out.loc[idx, "matched_upload_file"] = ",".join(files)
        out.loc[idx, "matched_upload_run_tag"] = ",".join(run_tags)
        out.loc[idx, "overlap_flag"] = False

    actual = out.loc[actual_mask].copy()
    summary.update(
        {
            "external_source_rows": int(actual["_trace_source"].eq("external_source").sum()),
            "eight_r_external_rows": int((
                actual["_trace_source"].eq("external_source")
                & actual.get("source_category", pd.Series("", index=actual.index)).astype(str).eq("eight_r")
            ).sum()),
            "truly_unmatched_rows": int(actual["_trace_source"].eq("unmatched").sum()),
            "still_unmatched_rows": int(actual["_trace_source"].eq("unmatched").sum()),
            "overlap_upload_rows": int(actual["formation_bucket"].eq("overlap_upload").sum()),
            "ranking_only_upload_rows": int(actual["formation_bucket"].eq("ranking_only_upload").sum()),
            "quick_card_only_upload_rows": int(actual["formation_bucket"].eq("quick_card_only_upload").sum()),
        }
    )
    return out, summary


def _apply_reconcile_trace_bucket(comp: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = comp.copy()
    out["reconcile_trace_bucket"] = ""
    actual_mask = out.get("row_type", pd.Series("", index=out.index)).eq("actual_wager")
    source = out.get("_trace_source", pd.Series("unmatched", index=out.index)).astype(str)
    upload_match = actual_mask & source.eq("upload_match")
    external = actual_mask & source.eq("external_source")
    unmatched = actual_mask & ~(upload_match | external)
    out.loc[upload_match, "reconcile_trace_bucket"] = "upload_match"
    out.loc[external, "reconcile_trace_bucket"] = "external_source"
    out.loc[unmatched, "reconcile_trace_bucket"] = "unmatched"
    return out, {
        "upload_match_rows": int(upload_match.sum()),
        "external_source_rows": int(external.sum()),
        "truly_unmatched_rows": int(unmatched.sum()),
    }


def _enrich_with_overlap_snapshot(comp: pd.DataFrame, snapshot_csv: Path, date_value: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = comp.copy()
    out["overlap_flag"] = False
    out["formation_bucket"] = "unmatched_snapshot"

    actual_mask = out["row_type"].eq("actual_wager") if "row_type" in out.columns else pd.Series(False, index=out.index)
    diagnostics: dict[str, Any] = {
        "snapshot_csv": str(snapshot_csv),
        "snapshot_available": bool(snapshot_csv.exists()),
        "actual_wager_rows": int(actual_mask.sum()),
        "snapshot_matched_rows": 0,
        "rows_matched_to_snapshot": 0,
        "rows_unmatched_to_snapshot": int(actual_mask.sum()),
        "overlap_rows_among_actual_wagers": 0,
        "ranking_only_rows_among_actual_wagers": 0,
        "quick_card_only_rows_among_actual_wagers": 0,
    }

    snapshot = _load_overlap_snapshot(snapshot_csv, date_value)
    diagnostics["snapshot_rows_for_date"] = int(len(snapshot))
    if snapshot.empty or not actual_mask.any():
        diagnostics["reason"] = "missing_or_empty_snapshot" if snapshot.empty else "no_actual_wager_rows"
        return out, diagnostics

    work = out.loc[actual_mask].copy()
    work["date_norm"] = work.get("date", pd.Series("", index=work.index)).map(_date_key)
    work["player_id_norm"] = work.get("player_id", pd.Series("", index=work.index)).map(_to_id_text)
    work["player_name_norm"] = work.get("player_name", pd.Series("", index=work.index)).map(_norm_name)
    work["prop_type_norm"] = work.get("prop_type", pd.Series("", index=work.index)).map(_market_to_prop)
    work["line_norm"] = work.get("line", pd.Series("", index=work.index)).map(_line)
    work["side_norm"] = work.get("side", pd.Series("", index=work.index)).map(_norm_side)
    work["_comp_index"] = work.index
    work["common_key"] = _key(work)
    work.loc[work["player_id_norm"].astype(str).eq(""), "common_key"] = ""
    work["name_key"] = _name_key(work)

    by_id = snapshot[["snapshot_common_key", "overlap_flag", "formation_bucket"]].rename(
        columns={
            "snapshot_common_key": "common_key",
            "overlap_flag": "snapshot_overlap_flag",
            "formation_bucket": "snapshot_formation_bucket",
        }
    )
    matched = work.merge(by_id, on="common_key", how="left")
    missing = matched["snapshot_formation_bucket"].isna()
    if missing.any():
        by_name = snapshot[["snapshot_name_key", "overlap_flag", "formation_bucket"]].rename(
            columns={
                "snapshot_name_key": "name_key",
                "overlap_flag": "snapshot_overlap_flag",
                "formation_bucket": "snapshot_formation_bucket",
            }
        )
        fallback = work.loc[work["_comp_index"].isin(matched.loc[missing, "_comp_index"])].merge(
            by_name,
            on="name_key",
            how="left",
        )
        fallback = fallback.dropna(subset=["snapshot_formation_bucket"]).drop_duplicates("_comp_index", keep="last")
        if not fallback.empty:
            fill_map = fallback.set_index("_comp_index")
            idx = matched["_comp_index"].isin(fill_map.index) & matched["snapshot_formation_bucket"].isna()
            matched.loc[idx, "snapshot_formation_bucket"] = matched.loc[idx, "_comp_index"].map(fill_map["snapshot_formation_bucket"])
            matched.loc[idx, "snapshot_overlap_flag"] = matched.loc[idx, "_comp_index"].map(fill_map["snapshot_overlap_flag"])

    matched = matched.drop_duplicates("_comp_index", keep="last").set_index("_comp_index")
    hit = matched["snapshot_formation_bucket"].notna()
    if hit.any():
        out.loc[matched.index[hit], "formation_bucket"] = matched.loc[hit, "snapshot_formation_bucket"].astype(str)
        out.loc[matched.index[hit], "overlap_flag"] = matched.loc[hit, "snapshot_overlap_flag"].map(bool)

    actual = out.loc[actual_mask].copy()
    matched_actual = actual[actual["formation_bucket"].ne("unmatched_snapshot")]
    diagnostics.update(
        {
            "rows_matched_to_snapshot": int(len(matched_actual)),
            "snapshot_matched_rows": int(len(matched_actual)),
            "rows_unmatched_to_snapshot": int(len(actual) - len(matched_actual)),
            "overlap_rows_among_actual_wagers": int(actual["formation_bucket"].eq("overlap").sum()),
            "ranking_only_rows_among_actual_wagers": int(actual["formation_bucket"].eq("ranking_only").sum()),
            "quick_card_only_rows_among_actual_wagers": int(actual["formation_bucket"].eq("quick_card_only").sum()),
            "source": "overlap_daily_snapshot",
        }
    )
    return out, diagnostics


def _build_summary(comp: pd.DataFrame, upload: pd.DataFrame, actual: pd.DataFrame, date_value: str) -> dict[str, Any]:
    actual_rows = comp[comp["row_type"].eq("actual_wager")].copy()
    upload_only = comp[comp["row_type"].eq("upload_not_wagered")].copy()
    by_source = {
        str(name): _summary_for(group, "result", "units", "price")
        for name, group in actual_rows.groupby("source_category", dropna=False)
    }
    note_tagged = actual_rows[
        actual_rows["source_category"].astype(str).isin(["v2_ranking", "quick_card", "v1_steam", "eight_r"])
    ].copy()
    note_tagged_with_expected = note_tagged[note_tagged["expected_upload_category"].astype(str).ne("")]
    note_tagged_expected_unmatched = note_tagged_with_expected[~note_tagged_with_expected["matched_expected_upload"]]
    wagered_not_in_upload = actual_rows[
        actual_rows["expected_upload_category"].astype(str).ne("") & ~actual_rows["matched_expected_upload"]
    ]
    upload_not_wagered_by_source = {
        str(name): int(len(group)) for name, group in upload_only.groupby("source_category", dropna=False)
    }
    ambiguous = actual_rows[actual_rows["ambiguous_match"]]
    return {
        "date": date_value,
        "upload_rows": int(len(upload)),
        "actual_wager_rows": int(len(actual_rows)),
        "overall_actual_wagers": _summary_for(actual_rows, "result", "units", "price"),
        "by_source_category": by_source,
        "note_tagged_wagers_without_expected_upload_match": int(len(note_tagged_expected_unmatched)),
        "wagered_rows_not_in_expected_upload": int(len(wagered_not_in_upload)),
        "upload_rows_not_wagered": int(len(upload_only)),
        "upload_rows_not_wagered_by_source": upload_not_wagered_by_source,
        "duplicate_or_ambiguous_matches": int(len(ambiguous)),
        "wagers_in_graded_download_not_matched_to_any_upload": int((~actual_rows["matched_any_upload"]).sum()),
        "matched_any_upload": int(actual_rows["matched_any_upload"].sum()),
        "matched_expected_upload": int(actual_rows["matched_expected_upload"].sum()),
        "wagered_unmatched_sample": _records_for_json(wagered_not_in_upload),
        "upload_not_wagered_sample": _records_for_json(upload_only),
        "ambiguous_match_sample": _records_for_json(ambiguous),
    }


def _explain(upload_only_wins: int, upload_only_losses: int, actual_only_losses: int, mismatch_count: int) -> str:
    parts = []
    if upload_only_wins or upload_only_losses:
        parts.append(
            f"ranking upload had {upload_only_wins} wins and {upload_only_losses} losses that were not actually bet"
        )
    if actual_only_losses:
        parts.append(f"actual v2 wagers included {actual_only_losses} losses that were not in the ranking upload")
    if mismatch_count:
        parts.append(f"{mismatch_count} actual-only rows share player/prop with upload but differ by side or line")
    if not parts:
        return "No upload-vs-actual population gap found; investigate grading or key normalization."
    return "; ".join(parts) + "."


def _write_md(path: Path, summary: dict[str, Any]) -> None:
    overall = summary["overall_actual_wagers"]
    overall_wr = "n/a" if overall["win_rate"] is None else f"{overall['win_rate']:.2%}"
    overall_roi = "n/a" if overall["roi"] is None else f"{overall['roi']:.2%}"
    lines = [
        f"# Actual Wagers By Source: {summary['date']}",
        "",
        "## Headline",
        f"- Actual wagers: `{overall['bets']}`",
        f"- Record: `{overall['wins']}-{overall['losses']}-{overall['pushes']}`; unresolved `{overall['unresolved']}`",
        f"- Win rate: `{overall_wr}`; ROI `{overall_roi}`; units `{overall['units']:.3f}`",
        "",
        "## By Source Category",
    ]
    for name, stats in summary["by_source_category"].items():
        wr = "n/a" if stats["win_rate"] is None else f"{stats['win_rate']:.2%}"
        roi = "n/a" if stats["roi"] is None else f"{stats['roi']:.2%}"
        avg_price = "n/a" if stats["avg_price"] is None else f"{stats['avg_price']:.1f}"
        lines.append(
            f"- `{name}`: bets `{stats['bets']}`, resolved `{stats['resolved_count']}`, "
            f"record `{stats['wins']}-{stats['losses']}-{stats['pushes']}`, win rate `{wr}`, "
            f"ROI `{roi}`, units `{stats['units']:.3f}`, avg price `{avg_price}`"
        )
    lines.extend(
        [
            "",
            "## Validation Checks",
            f"- Note-tagged wagers without expected upload match: `{summary['note_tagged_wagers_without_expected_upload_match']}`",
            f"- Wagered rows not in expected upload: `{summary['wagered_rows_not_in_expected_upload']}`",
            f"- Wagers in graded download not matched to any upload: `{summary['wagers_in_graded_download_not_matched_to_any_upload']}`",
            f"- Upload rows not wagered: `{summary['upload_rows_not_wagered']}`",
            f"- Duplicate/ambiguous matches: `{summary['duplicate_or_ambiguous_matches']}`",
            "",
            "## Reconcile Trace",
            f"- Actual wager rows: `{summary.get('overlap_enrichment', {}).get('actual_wager_rows', 0)}`",
            f"- Upload matches: `{summary.get('overlap_enrichment', {}).get('upload_match_rows', 0)}`",
            f"- External source rows: `{summary.get('overlap_enrichment', {}).get('external_source_rows', 0)}`",
            f"- Truly unmatched rows: `{summary.get('overlap_enrichment', {}).get('truly_unmatched_rows', 0)}`",
            "",
            "## Upload Rows Not Wagered By Source",
        ]
    )
    for name, count in summary["upload_rows_not_wagered_by_source"].items():
        lines.append(f"- `{name}`: `{count}`")
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--upload-csv", default="", help="Deprecated alias for --ranking-upload-csv.")
    parser.add_argument("--ranking-upload-csv", default="", help="Optional comma-separated CSV paths; defaults to all dated ranking_tool_upload*.csv files.")
    parser.add_argument("--quick-card-upload-csv", default="", help="Optional comma-separated CSV paths; defaults to all dated quick_card_tool_upload*.csv files.")
    parser.add_argument("--v1-steam-upload-csv", default="")
    parser.add_argument("--graded-csv", default="")
    parser.add_argument("--reconcile-csv", default="")
    parser.add_argument("--out-csv", default="")
    parser.add_argument("--summary-json", default="")
    parser.add_argument("--summary-md", default="")
    parser.add_argument("--overlap-snapshot-csv", default="artifacts/analysis/mlb/research_gap_analysis/overlap_daily_snapshot.csv")
    parser.add_argument("--overlap-enrichment-diagnostics-json", default="")
    args = parser.parse_args()

    date_value = _date_key(args.date)
    if not date_value:
        raise SystemExit(f"Invalid --date: {args.date}")
    compact_under = date_value.replace("-", "_")

    upload_root = Path("backend/mlb/exports/model_v2/upload")
    ranking_upload_paths = _discover_upload_paths(
        upload_root=upload_root,
        date_value=date_value,
        prefix="ranking_tool_upload",
        explicit=args.ranking_upload_csv or args.upload_csv,
        legacy_name=f"ranking_tool_upload_{date_value}.csv",
    )
    quick_card_upload_paths = _discover_upload_paths(
        upload_root=upload_root,
        date_value=date_value,
        prefix="quick_card_tool_upload",
        explicit=args.quick_card_upload_csv,
        legacy_name=f"quick_card_tool_upload_{date_value}.csv",
    )
    if args.v1_steam_upload_csv:
        v1_steam_upload_csv = Path(args.v1_steam_upload_csv)
    else:
        v1_steam_upload_csv = Path(f"backend/mlb/exports/v1_wagers/{date_value}/wagers.csv")
    graded_csv = Path(args.graded_csv or f"/Users/jerrystrain/Downloads/8rainstation_daily_{compact_under}.csv")
    reconcile_csv = Path(args.reconcile_csv or f"artifacts/analysis/mlb/execution_vs_model/{date_value}/reconcile_rows.csv")
    reconcile_date_root = Path("backend/mlb/exports/model_v2/reconcile") / date_value
    out_csv = Path(args.out_csv or reconcile_date_root / f"actual_wagers_by_source_{date_value}.csv")
    summary_json = Path(args.summary_json or reconcile_date_root / f"actual_wagers_by_source_{date_value}_summary.json")
    summary_md = Path(args.summary_md or reconcile_date_root / f"actual_wagers_by_source_{date_value}_summary.md")
    overlap_snapshot_csv = Path(args.overlap_snapshot_csv)
    overlap_enrichment_diagnostics_json = Path(
        args.overlap_enrichment_diagnostics_json
        or reconcile_date_root / f"actual_wagers_by_source_{date_value}_overlap_enrichment.json"
    )
    overlap_unmatched_diagnostics_csv = (
        reconcile_date_root / f"actual_wagers_by_source_{date_value}_overlap_unmatched_diagnostics.csv"
    )

    if not ranking_upload_paths:
        raise SystemExit(f"Missing required input: {upload_root / date_value / f'ranking_tool_upload_{date_value}.csv'}")
    if not quick_card_upload_paths:
        raise SystemExit(f"Missing required input: {upload_root / date_value / f'quick_card_tool_upload_{date_value}.csv'}")
    for path in [*ranking_upload_paths, *quick_card_upload_paths, graded_csv, reconcile_csv]:
        if not path.exists():
            raise SystemExit(f"Missing required input: {path}")

    rec = _prepare_reconcile(reconcile_csv)
    upload_frames = [
        _attach_upload_outcomes(_load_upload_many(ranking_upload_paths, date_value, "v2_ranking"), rec),
        _attach_upload_outcomes(_load_upload_many(quick_card_upload_paths, date_value, "quick_card"), rec),
    ]
    timestamped_upload_frames = [
        _load_timestamped_upload_many(ranking_upload_paths, date_value, "v2_ranking"),
        _load_timestamped_upload_many(quick_card_upload_paths, date_value, "quick_card"),
    ]
    if v1_steam_upload_csv.exists():
        v1_upload = _load_v1_steam_upload(v1_steam_upload_csv, date_value)
        v1_upload["parsed_player_name"] = v1_upload["player_name"]
        v1_upload["parsed_player_name_norm"] = v1_upload["player_name_norm"]
        v1_upload = _attach_actual_player_ids(v1_upload, rec)
        v1_upload["player_name"] = v1_upload.get("parsed_player_name", "")
        upload_frames.append(v1_upload)
    upload = pd.concat(upload_frames, ignore_index=True)
    timestamped_upload = pd.concat(
        [frame for frame in timestamped_upload_frames if not frame.empty],
        ignore_index=True,
    ) if any(not frame.empty for frame in timestamped_upload_frames) else pd.DataFrame()
    actual = _load_actual(graded_csv, date_value, rec)
    comp = _build_comparison(upload, actual)
    comp, overlap_enrichment = _enrich_with_overlap_snapshot(comp, overlap_snapshot_csv, date_value)
    comp, timestamped_upload_summary = _apply_timestamped_upload_provenance(comp, timestamped_upload)
    overlap_enrichment.update(timestamped_upload_summary)
    comp, trace_summary = _apply_reconcile_trace_bucket(comp)
    overlap_enrichment.update(trace_summary)
    unmatched_diagnostics, unmatched_summary = _build_overlap_unmatched_diagnostics(comp, overlap_snapshot_csv, date_value)
    overlap_enrichment.update(unmatched_summary)
    overlap_enrichment["overlap_unmatched_diagnostics_csv"] = str(overlap_unmatched_diagnostics_csv)
    public_enrichment_keys = [
        "actual_wager_rows",
        "upload_match_rows",
        "external_source_rows",
        "eight_r_external_rows",
        "truly_unmatched_rows",
        "still_unmatched_rows",
        "unmatched_by_reason",
        "overlap_unmatched_diagnostics_csv",
    ]
    overlap_enrichment = {key: overlap_enrichment.get(key) for key in public_enrichment_keys if key in overlap_enrichment}
    summary = _build_summary(comp, upload, actual, date_value)
    summary.update(
        {
            "ranking_upload_csv": ",".join(str(p) for p in ranking_upload_paths),
            "quick_card_upload_csv": ",".join(str(p) for p in quick_card_upload_paths),
            "ranking_upload_csvs": [str(p) for p in ranking_upload_paths],
            "quick_card_upload_csvs": [str(p) for p in quick_card_upload_paths],
            "v1_steam_upload_csv": str(v1_steam_upload_csv) if v1_steam_upload_csv.exists() else "",
            "graded_csv": str(graded_csv),
            "reconcile_csv": str(reconcile_csv),
            "out_csv": str(out_csv),
            "overlap_enrichment": overlap_enrichment,
            "overlap_enrichment_diagnostics_json": str(overlap_enrichment_diagnostics_json),
        }
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    comp.drop(columns=["_trace_source", "matched_upload_source", "upload_only_match_flag", "formation_bucket_source"], errors="ignore").to_csv(out_csv, index=False)
    unmatched_diagnostics.to_csv(overlap_unmatched_diagnostics_csv, index=False)
    overlap_enrichment_diagnostics_json.write_text(json.dumps(overlap_enrichment, indent=2, sort_keys=True) + "\n")
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    _write_md(summary_md, summary)

    print(f"Wrote {out_csv}")
    print(f"Wrote {overlap_unmatched_diagnostics_csv}")
    print(f"Wrote {overlap_enrichment_diagnostics_json}")
    print(f"Wrote {summary_json}")
    print(f"Wrote {summary_md}")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
