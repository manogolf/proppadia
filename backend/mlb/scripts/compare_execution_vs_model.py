#!/usr/bin/env python3
"""Compare executed betting results against model/reconcile decision correctness."""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from backend.mlb.scripts import report_mlb_graded_wagers as rgw
from backend.mlb.shared.probability_calibration import calibrate_probability, load_calibrator


RESOLVED = {"win", "loss", "push"}
PROP_LABEL_TO_TYPE: Dict[str, str] = {
    "hits + runs + rbis": "hits_runs_rbis",
    "hits + runs + rbi": "hits_runs_rbis",
    "hits runs rbis": "hits_runs_rbis",
    "hits runs rbi": "hits_runs_rbis",
    "total bases": "total_bases",
    "earned runs": "earned_runs",
    "hits allowed": "hits_allowed",
    "walks allowed": "walks_allowed",
    "runs scored": "runs_scored",
    "rbis": "rbis",
    "rbi": "rbis",
    "walks": "walks",
    "batter strikeouts": "strikeouts_batting",
    "pitcher strikeouts": "strikeouts_pitching",
    "singles": "singles",
    "strikeouts": "strikeouts_pitching",
    "ks": "strikeouts_pitching",
    "outs": "outs_recorded",
    "outs recorded": "outs_recorded",
    "hits": "hits",
}
PROP_LABEL_PATTERN = "|".join(
    re.escape(k).replace("\\ ", r"\s+").replace("\\+", r"\+")
    for k in sorted(PROP_LABEL_TO_TYPE, key=len, reverse=True)
)


def _norm_text(v: Any) -> str:
    return str(v if v is not None else "").strip()


def _norm_name(v: Any) -> str:
    text = unicodedata.normalize("NFKD", _norm_text(v))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    keep: List[str] = []
    for ch in text:
        if ch.isalnum() or ch.isspace():
            keep.append(ch)
    return " ".join("".join(keep).split())


def _norm_side(v: Any) -> str:
    raw = _norm_text(v).lower()
    if raw in {"o", "over"}:
        return "over"
    if raw in {"u", "under"}:
        return "under"
    return raw


def _norm_result(v: Any) -> str:
    return rgw._norm_grade(v)


def _parse_date(v: Any) -> str:
    text = _norm_text(v)
    if not text:
        return ""
    if text.isdigit() and len(text) == 8:
        dt = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _parse_wager_timestamp_utc(v: Any) -> str:
    text = _norm_text(v)
    if not text:
        return ""
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return ""
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None:
        ts = ts.tz_localize(ZoneInfo("America/Los_Angeles"))
    return ts.tz_convert("UTC").isoformat()


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        text = _norm_text(v).replace(",", "").replace("%", "")
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _resolve_col(df: pd.DataFrame, names: Sequence[str]) -> Optional[str]:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for name in names:
        if name in df.columns:
            return str(name)
        key = str(name).strip().lower()
        if key in lower:
            return str(lower[key])
    return None


def _series_or_blank(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if col is None:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[col]


def _line_series(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    return pd.to_numeric(_series_or_blank(df, col), errors="coerce").round(1)


def _american_to_pnl_1u(*, odds: Optional[float], result: str) -> Optional[float]:
    if result == "push":
        return 0.0
    if result == "loss":
        return -1.0
    if result != "win" or odds is None or odds == 0:
        return None
    if odds > 0:
        return odds / 100.0
    return 100.0 / abs(odds)


def _american_to_implied_probability(odds: Any) -> Optional[float]:
    x = _to_float(odds)
    if x is None or x == 0:
        return None
    if x > 0:
        return 100.0 / (x + 100.0)
    return abs(x) / (abs(x) + 100.0)


def _edge_bucket(edge: Any) -> str:
    x = _to_float(edge)
    if x is None:
        return "unknown"
    pp = x * 100.0
    if pp < -10:
        return "< -10pp"
    if pp < -5:
        return "-10 to -5pp"
    if pp < 0:
        return "-5 to 0pp"
    if pp < 5:
        return "0 to +5pp"
    if pp < 10:
        return "+5 to +10pp"
    return ">= +10pp"


def _calibrated_edge_bucket(edge: Any) -> str:
    x = _to_float(edge)
    if x is None:
        return "unknown"
    pp = x * 100.0
    if pp < 0:
        return "< 0pp"
    if pp < 5:
        return "0-5pp"
    if pp < 10:
        return "5-10pp"
    if pp < 15:
        return "10-15pp"
    if pp < 20:
        return "15-20pp"
    return "> 20pp"


def _odds_bucket(odds: Any) -> str:
    x = _to_float(odds)
    if x is None:
        return "unknown"
    if x <= -200:
        return "<= -200"
    if x < -150:
        return "-199 to -151"
    if x < -110:
        return "-150 to -111"
    if x <= 110:
        return "-110 to +110"
    if x <= 150:
        return "+111 to +150"
    if x <= 200:
        return "+151 to +200"
    return "> +200"


def _canonical_tool_prop(v: Any) -> str:
    raw = " ".join(_norm_text(v).lower().replace("+", " + ").split())
    if not raw:
        return ""
    if raw in PROP_LABEL_TO_TYPE:
        return PROP_LABEL_TO_TYPE[raw]
    inferred = rgw._infer_prop_type(raw, raw)
    inferred_norm = _norm_text(inferred).lower()
    return "" if inferred_norm == "unknown" else inferred_norm


def _extract_player_prop_line(text: Any) -> Tuple[str, str, Optional[float]]:
    raw = " ".join(_norm_text(text).split())
    if not raw:
        return "", "", None
    raw = re.sub(r"\s+\((?:pitching|batting)\)", "", raw, flags=re.IGNORECASE)
    pattern = (
        rf"^(?P<player>.+?)\s+(?P<prop>{PROP_LABEL_PATTERN})"
        rf"(?:\s+Over/Under)?(?:\s+(?P<line>\d+(?:\.\d+)?))?\s*$"
    )
    m = re.match(pattern, raw, flags=re.IGNORECASE)
    if not m:
        return "", "", None
    return _norm_text(m.group("player")), _canonical_tool_prop(m.group("prop")), _to_float(m.group("line"))


def _extract_tool_fields(row: pd.Series, bet_col: Optional[str], market_col: Optional[str]) -> Tuple[str, str, str, Optional[float]]:
    bet = _norm_text(row.get(bet_col)) if bet_col else ""
    market = _norm_text(row.get(market_col)) if market_col else ""
    side = ""
    line: Optional[float] = None
    player, prop_type, market_line = _extract_player_prop_line(market)

    bm = re.match(r"^(?P<body>.+?)\s+(?P<side>Over|Under)\s+(?P<line>\d+(?:\.\d+)?)\s*$", bet, flags=re.IGNORECASE)
    if bm:
        side = _norm_side(bm.group("side"))
        line = _to_float(bm.group("line"))
        bet_player, bet_prop, _ = _extract_player_prop_line(bm.group("body"))
        player = bet_player or player
        prop_type = bet_prop or prop_type

    if line is None:
        line = market_line
    if not player or not prop_type or line is None or not side:
        fallback_player, fallback_prop, fallback_side, fallback_line = rgw._extract_from_bet_and_market(bet, market)
        player = player or fallback_player
        prop_type = prop_type or _canonical_tool_prop(fallback_prop)
        side = side or _norm_side(fallback_side)
        line = line if line is not None else fallback_line
    return player, prop_type, side, line


def _normalize_tool_results(raw: pd.DataFrame, *, default_date: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    out = raw.copy()
    out["tool_row_id"] = np.arange(len(out), dtype=int)

    colmap = {
        "date": _resolve_col(out, ["date", "DATE", "game_date", "event_date", "Event Date"]),
        "wager_date": _resolve_col(out, ["wager_date", "Wager Date", "placed_at", "Placed At"]),
        "player_id": _resolve_col(out, ["player_id", "PLAYER_ID", "selector", "SELECTOR", "Player ID"]),
        "player": _resolve_col(out, ["player", "Player", "player_name", "PLAYER", "Name"]),
        "prop_type": _resolve_col(out, ["prop_type", "propType", "PROP_TYPE"]),
        "market": _resolve_col(out, ["market", "Market", "MARKET"]),
        "bet": _resolve_col(out, ["bet", "Bet"]),
        "line": _resolve_col(out, ["line", "Line", "POINT", "point"]),
        "side": _resolve_col(out, ["side", "Side", "SIDE"]),
        "odds": _resolve_col(out, ["odds", "Odds", "american_odds", "price", "Price", "WIN %", "win_pct"]),
        "model_fair_price": _resolve_col(out, ["No-Vig", "no_vig", "model_fair_price", "fair_price"]),
        "station_odds": _resolve_col(out, ["Station Odds", "station_odds"]),
        "closing_no_vig": _resolve_col(out, ["Closing No-Vig", "closing_no_vig"]),
        "unboosted_odds": _resolve_col(out, ["Unboosted Odds", "unboosted_odds"]),
        "result": _resolve_col(out, ["result", "Result", "grade", "Grade", "outcome", "Outcome"]),
        "pnl": _resolve_col(out, ["pnl", "Pnl", "profit", "Profit", "$ W/L", "pnl_1u", "profit_units", "roi_1u"]),
        "roi": _resolve_col(out, ["roi", "ROI", "roi_pct", "ROI %"]),
        "stake": _resolve_col(out, ["stake", "Stake", "amount", "Amount"]),
    }

    parsed = [_extract_tool_fields(row, colmap["bet"], colmap["market"]) for _, row in out.iterrows()]
    parsed_player = pd.Series([p[0] for p in parsed], index=out.index)
    parsed_prop_type = pd.Series([p[1] for p in parsed], index=out.index)
    parsed_side = pd.Series([p[2] for p in parsed], index=out.index)
    parsed_line = pd.Series([p[3] for p in parsed], index=out.index)

    date_series = _series_or_blank(out, colmap["date"]).map(_parse_date)
    if default_date:
        date_series = date_series.where(date_series.astype(str).str.len() > 0, default_date)
    out["date_norm"] = date_series
    out["wager_timestamp_utc"] = _series_or_blank(out, colmap["wager_date"]).map(_parse_wager_timestamp_utc)

    player_base = _series_or_blank(out, colmap["player"]).map(_norm_text)
    out["player_name_norm"] = np.where(player_base.astype(str).str.len() > 0, player_base, parsed_player).astype(str)
    out["player_name_key"] = pd.Series(out["player_name_norm"], index=out.index).map(_norm_name)

    prop_base = _series_or_blank(out, colmap["prop_type"]).map(_canonical_tool_prop)
    out["prop_type_norm"] = np.where(prop_base.astype(str).str.len() > 0, prop_base, parsed_prop_type)
    out["prop_type_norm"] = np.where(
        pd.Series(out["prop_type_norm"], index=out.index).astype(str).str.len() > 0,
        out["prop_type_norm"],
        [
            _extract_player_prop_line(row.get(colmap["market"]) if colmap["market"] else "")[1]
            for _, row in out.iterrows()
        ],
    )
    out["prop_type_norm"] = pd.Series(out["prop_type_norm"], index=out.index).map(lambda v: _norm_text(v).lower())

    side_base = _series_or_blank(out, colmap["side"]).map(_norm_side)
    side_parsed = parsed_side.map(_norm_side)
    out["side_norm"] = np.where(side_base.astype(str).str.len() > 0, side_base, side_parsed)

    line_base = _line_series(out, colmap["line"])
    out["line_norm"] = line_base.where(line_base.notna(), pd.to_numeric(parsed_line, errors="coerce").round(1))

    player_id = pd.to_numeric(_series_or_blank(out, colmap["player_id"]), errors="coerce")
    out["player_id_norm"] = player_id.astype("Int64")
    out["player_join_key"] = np.where(
        out["player_id_norm"].notna(),
        "id:" + out["player_id_norm"].astype(str),
        "name:" + out["player_name_key"],
    )

    out["bet_odds"] = pd.to_numeric(_series_or_blank(out, colmap["odds"]), errors="coerce")
    out["odds"] = out["bet_odds"]
    out["model_fair_price"] = pd.to_numeric(_series_or_blank(out, colmap["model_fair_price"]), errors="coerce")
    out["station_odds"] = pd.to_numeric(_series_or_blank(out, colmap["station_odds"]), errors="coerce")
    out["closing_no_vig_price"] = pd.to_numeric(_series_or_blank(out, colmap["closing_no_vig"]), errors="coerce")
    out["unboosted_odds"] = pd.to_numeric(_series_or_blank(out, colmap["unboosted_odds"]), errors="coerce")
    out["implied_prob_from_bet_odds"] = out["bet_odds"].map(_american_to_implied_probability)
    out["bet_result"] = _series_or_blank(out, colmap["result"]).map(_norm_result)
    out["bet_win"] = out["bet_result"].eq("win")
    out["bet_loss"] = out["bet_result"].eq("loss")
    out["bet_push"] = out["bet_result"].eq("push")

    pnl = pd.to_numeric(_series_or_blank(out, colmap["pnl"]), errors="coerce")
    stake = pd.to_numeric(_series_or_blank(out, colmap["stake"]), errors="coerce")
    roi = pd.to_numeric(_series_or_blank(out, colmap["roi"]), errors="coerce")
    roi_units = np.where(roi.abs() > 2, roi / 100.0, roi)
    pnl_units = pnl.where(stake.isna() | stake.le(0), pnl / stake)
    derived_pnl = [
        _american_to_pnl_1u(odds=_to_float(odds), result=str(result))
        for odds, result in zip(out["bet_odds"].tolist(), out["bet_result"].tolist())
    ]
    out["pnl"] = pnl_units.where(pnl_units.notna(), pd.Series(roi_units, index=out.index))
    out["pnl"] = out["pnl"].where(out["pnl"].notna(), pd.Series(derived_pnl, index=out.index))

    for c in raw.columns:
        out[f"tool__{c}"] = raw[c]

    required = ["date_norm", "prop_type_norm", "line_norm", "side_norm", "player_join_key", "bet_result"]
    complete = out[required].notna().all(axis=1)
    complete = complete & out["date_norm"].astype(str).str.len().gt(0)
    complete = complete & out["prop_type_norm"].astype(str).str.len().gt(0)
    complete = complete & out["side_norm"].astype(str).str.len().gt(0)
    complete = complete & out["player_join_key"].astype(str).str.replace("name:", "", regex=False).str.len().gt(0)
    meta = {
        "tool_columns": [str(c) for c in raw.columns],
        "tool_colmap": colmap,
        "tool_rows": int(len(out)),
        "tool_complete_key_rows": int(complete.sum()),
        "tool_sample_values": {
            "player": out["player_name_norm"].head(10).tolist(),
            "prop_type": out["prop_type_norm"].head(10).tolist(),
            "line": out["line_norm"].head(10).tolist(),
            "side": out["side_norm"].head(10).tolist(),
        },
        "unique_tool_prop_names": sorted(set(x for x in out["prop_type_norm"].dropna().astype(str).tolist() if x)),
    }
    return out.loc[complete].copy(), meta


def _book_is_betonline(value: Any) -> bool:
    text = re.sub(r"[^a-z0-9]+", "", _norm_text(value).lower())
    return text in {"betonline", "betonlineag"} or "betonline" in text


def _wager_export_counts(raw: pd.DataFrame) -> Dict[str, Any]:
    sport_col = _resolve_col(raw, ["Sport", "sport"])
    league_col = _resolve_col(raw, ["League", "league"])
    book_col = _resolve_col(raw, ["Book", "book", "bookmaker", "bookmaker_key"])
    grade_col = _resolve_col(raw, ["Grade", "grade", "Result", "result", "outcome", "Outcome"])

    mlb_mask = pd.Series(True, index=raw.index)
    if sport_col is not None:
        mlb_mask &= raw[sport_col].map(lambda v: _norm_text(v).lower()).eq("baseball")
    if league_col is not None:
        mlb_mask &= raw[league_col].map(lambda v: _norm_text(v).upper()).eq("MLB")

    betonline_mask = mlb_mask.copy()
    if book_col is not None:
        betonline_mask &= raw[book_col].map(_book_is_betonline)

    non_push_mask = betonline_mask.copy()
    if grade_col is not None:
        non_push_mask &= raw[grade_col].map(_norm_result).ne("push")

    return {
        "raw_rows_loaded": int(len(raw)),
        "mlb_rows": int(mlb_mask.sum()),
        "mlb_betonline_rows": int(betonline_mask.sum()),
        "mlb_betonline_non_push_rows": int(non_push_mask.sum()),
        "wager_guard_columns": {
            "sport": sport_col,
            "league": league_col,
            "book": book_col,
            "grade": grade_col,
        },
    }


def _guard_expected_count(*, label: str, actual: int, expected: int) -> None:
    if expected <= 0:
        return
    if actual != expected:
        raise RuntimeError(
            f"{label} count mismatch: loaded {actual}, expected {expected}. "
            "Stopping before reconciliation to avoid partial coverage."
        )


def _nonblank_count(series: pd.Series) -> int:
    return int(series.notna().sum() and series.fillna("").astype(str).str.strip().ne("").sum())


def _reconcile_source_mtime_guard(*, rec_path: Path, rec_raw: pd.DataFrame, target_date: str) -> Dict[str, Any]:
    if not rec_path.exists():
        raise FileNotFoundError(f"reconcile csv not found: {rec_path}")

    rec_mtime = datetime.fromtimestamp(rec_path.stat().st_mtime)
    today = datetime.now().date()
    if rec_mtime.date() == today:
        return {
            "reconcile_mtime": rec_mtime.isoformat(),
            "reconcile_mtime_guard": "same_day_as_run",
            "source_slate_mtime": None,
        }

    date_col = _resolve_col(rec_raw, ["game_date", "date", "slate_date"])
    source_col = _resolve_col(rec_raw, ["slate_source_file"])
    source_paths: List[Path] = []
    if date_col is not None and source_col is not None:
        dates = rec_raw[date_col].map(_parse_date)
        for raw_path in rec_raw.loc[dates.eq(target_date), source_col].dropna().astype(str).unique().tolist():
            p = Path(raw_path).expanduser()
            if p.exists():
                source_paths.append(p)

    default_slate = Path(f"backend/mlb/exports/odds_history/{target_date}/mlb_slate_output.csv")
    if default_slate.exists() and default_slate not in source_paths:
        source_paths.append(default_slate)

    if not source_paths:
        raise RuntimeError(
            f"Could not verify reconcile freshness for {rec_path}: no source slate artifact paths were found "
            f"for date {target_date}, and the reconcile file was not produced today."
        )

    newest_source_mtime = max(datetime.fromtimestamp(p.stat().st_mtime) for p in source_paths)
    if rec_mtime < newest_source_mtime:
        raise RuntimeError(
            f"Stale reconcile artifact: {rec_path} mtime={rec_mtime.isoformat()} is older than source slate "
            f"artifact mtime={newest_source_mtime.isoformat()} for {target_date}. Rebuild date-scoped reconcile rows "
            "before execution comparison."
        )

    return {
        "reconcile_mtime": rec_mtime.isoformat(),
        "reconcile_mtime_guard": "newer_than_source_slate",
        "source_slate_mtime": newest_source_mtime.isoformat(),
    }


def _upstream_outcome_counts(target_date: str) -> Dict[str, Any]:
    try:
        from backend.shared.db.pg import pg_fetchone

        row = pg_fetchone(
            """
            SELECT
              (
                SELECT count(*)::int
                FROM mlb.model_training_props
                WHERE game_date::date = %s::date
                  AND lower(trim(coalesce(prop_source, ''))) = 'mlb_api'
                  AND NULLIF(btrim(prop_value::text), '') IS NOT NULL
              ) AS model_training_props_values,
              (
                SELECT count(*)::int
                FROM mlb.player_stats
                WHERE game_date::date = %s::date
                  AND (
                    coalesce(at_bats,0) > 0
                    OR coalesce(hits,0) > 0
                    OR coalesce(walks,0) > 0
                    OR coalesce(runs_scored,0) > 0
                    OR coalesce(rbis,0) > 0
                    OR coalesce(outs_recorded,0) > 0
                    OR coalesce(strikeouts_pitching,0) > 0
                    OR coalesce(hits_allowed,0) > 0
                    OR coalesce(earned_runs,0) > 0
                  )
              ) AS player_stats_participation_rows
            """,
            (target_date, target_date),
        ) or {}
        mtp = int(row.get("model_training_props_values") or 0)
        ps = int(row.get("player_stats_participation_rows") or 0)
        return {
            "checked": True,
            "model_training_props_values": mtp,
            "player_stats_participation_rows": ps,
            "upstream_outcomes_exist": bool(mtp > 0 or ps > 0),
            "error": None,
        }
    except Exception as e:
        return {
            "checked": False,
            "model_training_props_values": None,
            "player_stats_participation_rows": None,
            "upstream_outcomes_exist": False,
            "error": f"{type(e).__name__}: {e}",
        }


def _validate_reconcile_input(rec_raw: pd.DataFrame, *, rec_path: Path, target_date: str) -> Dict[str, Any]:
    date_col = _resolve_col(rec_raw, ["game_date", "date", "slate_date"])
    if date_col is None:
        raise RuntimeError("reconcile csv missing a date column (expected game_date/date/slate_date)")
    dates = rec_raw[date_col].map(_parse_date)
    target_mask = dates.eq(target_date)
    if not bool(target_mask.any()):
        observed = sorted(set(x for x in dates.dropna().astype(str).tolist() if x))[:10]
        raise RuntimeError(
            f"reconcile csv does not contain MLB_DATE={target_date}; observed_dates_sample={observed}. "
            "Use a fresh date-scoped reconcile file."
        )

    target_rows = rec_raw.loc[target_mask].copy()
    freshness = _reconcile_source_mtime_guard(rec_path=rec_path, rec_raw=rec_raw, target_date=target_date)

    outcome_cols = [
        c
        for c in ["actual_value", "actual_over_outcome", "actual_under_outcome", "actual_model_pick_outcome"]
        if c in target_rows.columns
    ]
    outcome_nonblank = {c: _nonblank_count(target_rows[c]) for c in outcome_cols}
    all_outcomes_empty = bool(outcome_cols) and all(v == 0 for v in outcome_nonblank.values())
    upstream_counts = _upstream_outcome_counts(target_date) if all_outcomes_empty else {
        "checked": False,
        "model_training_props_values": None,
        "player_stats_participation_rows": None,
        "upstream_outcomes_exist": False,
        "error": None,
    }
    if all_outcomes_empty and upstream_counts.get("upstream_outcomes_exist"):
        raise RuntimeError(
            f"Stale reconcile artifact: {rec_path} has {len(target_rows)} rows for {target_date} but all outcome "
            "columns are empty, while upstream outcomes exist "
            f"(model_training_props_values={upstream_counts.get('model_training_props_values')}, "
            f"player_stats_participation_rows={upstream_counts.get('player_stats_participation_rows')}). "
            "Rebuild date-scoped reconcile rows before execution comparison."
        )

    return {
        **freshness,
        "reconcile_date_col": date_col,
        "reconcile_target_rows": int(len(target_rows)),
        "reconcile_outcome_nonblank": outcome_nonblank,
        "reconcile_all_outcomes_empty": all_outcomes_empty,
        "upstream_outcome_check": upstream_counts,
    }


def _normalize_reconcile(raw: pd.DataFrame, *, target_date: str, calibration_json: str = "") -> pd.DataFrame:
    required = [
        "game_date",
        "player_id",
        "player_name",
        "prop_type",
        "line",
        "model_pick_side",
        "actual_model_pick_outcome",
    ]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise RuntimeError(f"reconcile csv missing required columns: {missing}")

    out = raw.copy()
    out["date_norm"] = out["game_date"].map(_parse_date)
    if target_date:
        out = out[out["date_norm"] == target_date].copy()
    out["player_id_norm"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out["player_name_key"] = out["player_name"].map(_norm_name)
    out["prop_type_norm"] = out["prop_type"].map(lambda v: _norm_text(v).lower())
    out["line_norm"] = pd.to_numeric(out["line"], errors="coerce").round(1)
    out["model_pick_side_norm"] = out["model_pick_side"].map(_norm_side)
    out["actual_model_pick_outcome_norm"] = out["actual_model_pick_outcome"].map(_norm_result)
    out["actual_over_outcome_norm"] = _series_or_blank(out, _resolve_col(out, ["actual_over_outcome"])).map(_norm_result)
    out["actual_under_outcome_norm"] = _series_or_blank(out, _resolve_col(out, ["actual_under_outcome"])).map(_norm_result)
    out["model_pick_prob"] = pd.to_numeric(_series_or_blank(out, _resolve_col(out, ["model_pick_prob"])), errors="coerce")
    out["model_prob_over"] = pd.to_numeric(_series_or_blank(out, _resolve_col(out, ["model_prob_over"])), errors="coerce")
    out["model_prob_under"] = pd.to_numeric(_series_or_blank(out, _resolve_col(out, ["model_prob_under"])), errors="coerce")
    calibrator = load_calibrator(calibration_json)
    min_prop_samples = int((calibrator or {}).get("min_prop_samples") or 200)
    if calibrator:
        out["raw_model_prob_over"] = out["model_prob_over"]
        out["raw_model_prob_under"] = out["model_prob_under"]
        out["model_prob_over"] = [
            calibrate_probability(calibrator, prop_type=prop, raw_prob=prob, min_prop_samples=min_prop_samples)
            for prop, prob in zip(out["prop_type_norm"], out["raw_model_prob_over"])
        ]
        out["model_prob_under"] = [
            calibrate_probability(calibrator, prop_type=prop, raw_prob=prob, min_prop_samples=min_prop_samples)
            for prop, prob in zip(out["prop_type_norm"], out["raw_model_prob_under"])
        ]
        out["model_pick_prob_raw"] = out["model_pick_prob"]
        out["model_pick_prob"] = np.where(
            out["model_pick_side_norm"].eq("over"),
            out["model_prob_over"],
            np.where(out["model_pick_side_norm"].eq("under"), out["model_prob_under"], out["model_pick_prob"]),
        )
    else:
        out["raw_model_prob_over"] = out["model_prob_over"]
        out["raw_model_prob_under"] = out["model_prob_under"]
        out["model_pick_prob_raw"] = out["model_pick_prob"]

    expanded: List[pd.DataFrame] = []
    for side in ("over", "under"):
        s = out.copy()
        s["side_norm"] = side
        s["bet_side_model_prob"] = s["model_prob_over"] if side == "over" else s["model_prob_under"]
        s["bet_side_raw_model_prob"] = s["raw_model_prob_over"] if side == "over" else s["raw_model_prob_under"]
        s["bet_side_actual_outcome"] = s["actual_over_outcome_norm"] if side == "over" else s["actual_under_outcome_norm"]
        expanded.append(s)
    rec = pd.concat(expanded, ignore_index=True)

    model_outcome = rec["actual_model_pick_outcome_norm"]
    fallback_model_outcome = np.where(
        rec["model_pick_side_norm"].eq("over"),
        rec["actual_over_outcome_norm"],
        np.where(rec["model_pick_side_norm"].eq("under"), rec["actual_under_outcome_norm"], ""),
    )
    rec["model_pick_outcome"] = np.where(model_outcome.isin(RESOLVED), model_outcome, fallback_model_outcome)
    rec["model_correct"] = rec["model_pick_outcome"].eq("win")
    rec["model_wrong"] = rec["model_pick_outcome"].eq("loss")
    rec["model_push"] = rec["model_pick_outcome"].eq("push")
    rec["model_graded"] = rec["model_pick_outcome"].isin(RESOLVED)
    rec["raw_model_prob"] = rec["bet_side_raw_model_prob"]
    rec["calibrated_model_prob"] = rec["bet_side_model_prob"]
    rec["model_prob"] = rec["calibrated_model_prob"]
    rec["calibration_applied"] = bool(calibrator)

    rec["player_join_key_id"] = np.where(rec["player_id_norm"].notna(), "id:" + rec["player_id_norm"].astype(str), "")
    rec["player_join_key_name"] = "name:" + rec["player_name_key"]
    rec["snapshot_run_tag"] = _series_or_blank(rec, _resolve_col(rec, ["snapshot_run_tag"])).map(_norm_text)
    rec["snapshot_time_utc"] = _series_or_blank(rec, _resolve_col(rec, ["snapshot_time_utc"])).map(_norm_text)
    return rec


STRICT_JOIN_COLS = ["date_norm", "prop_type_norm", "line_norm", "side_norm", "player_join_key"]
RELAXED_JOIN_COLS = ["date_norm", "prop_type_norm", "side_norm", "player_join_key"]


def _reconcile_join_frame(rec: pd.DataFrame) -> pd.DataFrame:
    rec_id = rec[rec["player_join_key_id"].astype(str).str.len().gt(3)].copy()
    rec_id["player_join_key"] = rec_id["player_join_key_id"]
    rec_name = rec.copy()
    rec_name["player_join_key"] = rec_name["player_join_key_name"]
    rec_join = pd.concat([rec_id, rec_name], ignore_index=True)
    return rec_join.sort_values(["date_norm", "prop_type_norm", "line_norm", "side_norm", "player_join_key"])


def _join_on(tool: pd.DataFrame, rec_join: pd.DataFrame, join_cols: List[str], *, join_label: str) -> pd.DataFrame:
    rec_cols = join_cols + [
        "line_norm",
        "game_id",
        "player_id",
        "player_name",
        "model_pick_side_norm",
        "model_pick_outcome",
        "model_correct",
        "model_wrong",
        "model_push",
        "model_graded",
        "model_prob",
        "raw_model_prob",
        "calibrated_model_prob",
        "calibration_applied",
        "model_pick_prob",
        "model_pick_prob_raw",
        "bet_side_actual_outcome",
        "snapshot_run_tag",
        "snapshot_time_utc",
    ]
    rec_cols = list(dict.fromkeys(rec_cols))
    merged = tool.merge(rec_join[rec_cols], on=join_cols, how="left", indicator=True, suffixes=("", "_reconcile"))
    merged["matched_reconcile"] = merged["_merge"].eq("both")
    merged["join_strategy"] = np.where(merged["matched_reconcile"], join_label, "")
    merged = merged.drop(columns=["_merge"])
    if "tool_row_id" in merged.columns:
        merged = _select_snapshot_match_per_wager(merged)
    merged["edge"] = pd.to_numeric(merged.get("model_prob"), errors="coerce") - pd.to_numeric(
        merged.get("implied_prob_from_bet_odds"), errors="coerce"
    )
    merged["calibrated_edge"] = pd.to_numeric(merged.get("calibrated_model_prob"), errors="coerce") - pd.to_numeric(
        merged.get("implied_prob_from_bet_odds"), errors="coerce"
    )
    merged["edge_bucket"] = merged["edge"].map(_edge_bucket)
    merged["calibrated_edge_bucket"] = merged["calibrated_edge"].map(_calibrated_edge_bucket)
    merged["bet_odds_bucket"] = merged["bet_odds"].map(_odds_bucket)
    merged["model_correct_bet_lost"] = merged["model_correct"].eq(True) & merged["bet_loss"].eq(True)
    merged["model_wrong_bet_won"] = merged["model_wrong"].eq(True) & merged["bet_win"].eq(True)
    return merged


def _select_snapshot_match_per_wager(merged: pd.DataFrame) -> pd.DataFrame:
    selected: List[pd.Series] = []
    for _, group in merged.groupby("tool_row_id", sort=False, dropna=False):
        matches = group[group["matched_reconcile"]].copy()
        if matches.empty:
            row = group.iloc[0].copy()
            row["snapshot_match_policy"] = ""
            row["snapshot_age_minutes"] = np.nan
            selected.append(row)
            continue

        wager_ts = pd.to_datetime(matches.get("wager_timestamp_utc"), errors="coerce", utc=True)
        snap_ts = pd.to_datetime(matches.get("snapshot_time_utc"), errors="coerce", utc=True)
        matches["__wager_ts"] = wager_ts
        matches["__snap_ts"] = snap_ts
        matches["__age_min"] = (matches["__wager_ts"] - matches["__snap_ts"]).dt.total_seconds() / 60.0
        valid_time = matches["__wager_ts"].notna() & matches["__snap_ts"].notna()

        if bool(valid_time.any()):
            prior = matches[valid_time & matches["__age_min"].ge(0)].copy()
            if not prior.empty:
                pick = prior.sort_values(["__age_min"]).iloc[0].copy()
                pick["snapshot_match_policy"] = "latest_prior"
            else:
                later = matches[valid_time].copy()
                pick = later.sort_values(["__age_min"], ascending=False).iloc[0].copy()
                pick["snapshot_match_policy"] = "fallback_next"
        else:
            pick = matches.iloc[0].copy()
            pick["snapshot_match_policy"] = "unknown_snapshot_time"
            pick["__age_min"] = np.nan

        pick["snapshot_age_minutes"] = pick.get("__age_min", np.nan)
        selected.append(pick.drop(labels=[c for c in ["__wager_ts", "__snap_ts", "__age_min"] if c in pick.index]))

    return pd.DataFrame(selected).reset_index(drop=True)


def _join_execution_to_model(tool: pd.DataFrame, rec: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    rec_join = _reconcile_join_frame(rec)
    strict = _join_on(tool, rec_join, STRICT_JOIN_COLS, join_label="strict")
    strict_matches = int(strict["matched_reconcile"].sum())
    relaxed_matches = 0
    merged = strict
    if strict_matches == 0:
        relaxed = _join_on(tool, rec_join, RELAXED_JOIN_COLS, join_label="relaxed_without_line")
        relaxed_matches = int(relaxed["matched_reconcile"].sum())
        if relaxed_matches > strict_matches:
            merged = relaxed
            if "line_norm_reconcile" in merged.columns:
                merged["reconcile_line_norm"] = merged["line_norm_reconcile"]
    diag = {
        "strict_join_matches": strict_matches,
        "relaxed_without_line_matches": relaxed_matches,
        "join_strategy_used": "relaxed_without_line" if merged is not strict else "strict",
    }
    return merged, diag


def _summary(df: pd.DataFrame, *, meta: Dict[str, Any]) -> Dict[str, Any]:
    matched = df[df["matched_reconcile"]].copy()
    graded_model = matched[matched["model_graded"].eq(True)].copy()
    graded_bets = df[df["bet_result"].isin(RESOLVED)].copy()
    matched_graded_bets = matched[matched["bet_result"].isin(RESOLVED)].copy()

    model_wl = graded_model["model_correct"].sum() + graded_model["model_wrong"].sum()
    bet_wl = matched_graded_bets["bet_win"].sum() + matched_graded_bets["bet_loss"].sum()
    pnl = pd.to_numeric(matched_graded_bets["pnl"], errors="coerce")
    bet_odds = pd.to_numeric(matched_graded_bets.get("bet_odds"), errors="coerce")
    implied = pd.to_numeric(matched_graded_bets.get("implied_prob_from_bet_odds"), errors="coerce")
    model_prob = pd.to_numeric(matched_graded_bets.get("model_prob"), errors="coerce")
    raw_model_prob = pd.to_numeric(matched_graded_bets.get("raw_model_prob"), errors="coerce")
    calibrated_model_prob = pd.to_numeric(matched_graded_bets.get("calibrated_model_prob"), errors="coerce")
    edge = pd.to_numeric(matched_graded_bets.get("edge"), errors="coerce")
    bets_with_pnl = int(pnl.notna().sum())
    pnl_sum = float(pnl.fillna(0).sum()) if len(pnl) else 0.0

    def _rate(num: Any, den: Any) -> Optional[float]:
        den_i = int(den)
        return float(num) / den_i if den_i > 0 else None

    return {
        **meta,
        "matched_rows": int(df["matched_reconcile"].sum()),
        "unmatched_rows": int((~df["matched_reconcile"]).sum()),
        "graded_tool_rows": int(len(graded_bets)),
        "matched_graded_tool_rows": int(len(matched_graded_bets)),
        "model_accuracy": _rate(int(graded_model["model_correct"].sum()), model_wl),
        "model_correct": int(graded_model["model_correct"].sum()),
        "model_wrong": int(graded_model["model_wrong"].sum()),
        "model_push": int(graded_model["model_push"].sum()),
        "bet_win_rate": _rate(int(matched_graded_bets["bet_win"].sum()), bet_wl),
        "bet_wins": int(matched_graded_bets["bet_win"].sum()),
        "bet_losses": int(matched_graded_bets["bet_loss"].sum()),
        "bet_pushes": int(matched_graded_bets["bet_push"].sum()),
        "pnl": pnl_sum,
        "bets_with_pnl": bets_with_pnl,
        "roi": (pnl_sum / bets_with_pnl) if bets_with_pnl > 0 else None,
        "avg_bet_odds": float(bet_odds.mean()) if bet_odds.notna().any() else None,
        "avg_implied_probability_from_bet_odds": float(implied.mean()) if implied.notna().any() else None,
        "avg_model_probability": float(model_prob.mean()) if model_prob.notna().any() else None,
        "avg_raw_model_probability": float(raw_model_prob.mean()) if raw_model_prob.notna().any() else None,
        "avg_calibrated_model_probability": (
            float(calibrated_model_prob.mean()) if calibrated_model_prob.notna().any() else None
        ),
        "avg_edge": float(edge.mean()) if edge.notna().any() else None,
        "calibration_applied": bool(matched_graded_bets.get("calibration_applied", pd.Series(dtype=bool)).eq(True).any()),
        "model_correct_bet_lost": int(matched_graded_bets["model_correct_bet_lost"].sum()),
        "model_wrong_bet_won": int(matched_graded_bets["model_wrong_bet_won"].sum()),
    }


def _bucket_summary(df: pd.DataFrame) -> pd.DataFrame:
    work = df[df["matched_reconcile"] & df["bet_result"].isin(RESOLVED)].copy()
    if work.empty:
        return pd.DataFrame(
            columns=[
                "edge_bucket",
                "bets",
                "bet_wins",
                "bet_losses",
                "bet_pushes",
                "bet_win_rate",
                "pnl",
                "roi",
                "model_correct",
                "model_wrong",
                "model_push",
                "model_accuracy",
                "avg_bet_odds",
                "avg_implied_probability_from_bet_odds",
                "avg_model_probability",
                "avg_raw_model_probability",
                "avg_calibrated_model_probability",
                "avg_edge",
            ]
        )
    g = (
        work.groupby("edge_bucket", dropna=False, as_index=False)
        .agg(
            bets=("bet_result", "size"),
            bet_wins=("bet_win", "sum"),
            bet_losses=("bet_loss", "sum"),
            bet_pushes=("bet_push", "sum"),
            pnl=("pnl", "sum"),
            model_correct=("model_correct", "sum"),
            model_wrong=("model_wrong", "sum"),
            model_push=("model_push", "sum"),
            avg_bet_odds=("bet_odds", "mean"),
            avg_implied_probability_from_bet_odds=("implied_prob_from_bet_odds", "mean"),
            avg_model_probability=("model_prob", "mean"),
            avg_raw_model_probability=("raw_model_prob", "mean"),
            avg_calibrated_model_probability=("calibrated_model_prob", "mean"),
            avg_edge=("edge", "mean"),
        )
        .copy()
    )
    bet_wl = g["bet_wins"] + g["bet_losses"]
    model_wl = g["model_correct"] + g["model_wrong"]
    g["bet_win_rate"] = np.divide(
        g["bet_wins"].to_numpy(dtype=float),
        bet_wl.to_numpy(dtype=float),
        out=np.full(len(g), np.nan, dtype=float),
        where=bet_wl.to_numpy() > 0,
    )
    g["roi"] = np.where(g["bets"] > 0, g["pnl"] / g["bets"], np.nan)
    g["model_accuracy"] = np.divide(
        g["model_correct"].to_numpy(dtype=float),
        model_wl.to_numpy(dtype=float),
        out=np.full(len(g), np.nan, dtype=float),
        where=model_wl.to_numpy() > 0,
    )
    order = ["< -10pp", "-10 to -5pp", "-5 to 0pp", "0 to +5pp", "+5 to +10pp", ">= +10pp", "unknown"]
    g["__order"] = g["edge_bucket"].map(lambda v: order.index(v) if v in order else len(order))
    return g.sort_values(["__order", "edge_bucket"]).drop(columns=["__order"])


def _odds_distribution(df: pd.DataFrame) -> pd.DataFrame:
    work = df[df["matched_reconcile"] & df["bet_result"].isin(RESOLVED)].copy()
    if work.empty:
        return pd.DataFrame(columns=["bet_odds_bucket", "bets", "avg_bet_odds", "wins", "losses", "pushes", "win_rate", "pnl", "roi"])
    g = (
        work.groupby("bet_odds_bucket", dropna=False, as_index=False)
        .agg(
            bets=("bet_result", "size"),
            avg_bet_odds=("bet_odds", "mean"),
            wins=("bet_win", "sum"),
            losses=("bet_loss", "sum"),
            pushes=("bet_push", "sum"),
            pnl=("pnl", "sum"),
        )
        .copy()
    )
    wl = g["wins"] + g["losses"]
    g["win_rate"] = np.where(wl > 0, g["wins"] / wl, np.nan)
    g["roi"] = np.where(g["bets"] > 0, g["pnl"] / g["bets"], np.nan)
    order = ["<= -200", "-199 to -151", "-150 to -111", "-110 to +110", "+111 to +150", "+151 to +200", "> +200", "unknown"]
    g["__order"] = g["bet_odds_bucket"].map(lambda v: order.index(v) if v in order else len(order))
    return g.sort_values(["__order", "bet_odds_bucket"]).drop(columns=["__order"])


def _calibrated_edge_quality(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    work = df[
        df["matched_reconcile"]
        & df["bet_result"].isin({"win", "loss"})
        & pd.to_numeric(df.get("calibrated_edge"), errors="coerce").notna()
    ].copy()
    cols = [
        "edge_bucket",
        "bets",
        "bet_wins",
        "bet_losses",
        "bet_win_rate",
        "pnl",
        "roi",
        "avg_calibrated_edge",
    ]
    if work.empty:
        return pd.DataFrame(columns=cols), {
            "calibrated_edge_monotonicity": "insufficient_data",
            "calibrated_edge_spearman": None,
            "calibrated_edge_spearman_n": 0,
        }

    work["edge_bucket"] = work["calibrated_edge"].map(_calibrated_edge_bucket)
    work["actual_outcome_i"] = work["bet_win"].astype(int)
    g = (
        work.groupby("edge_bucket", dropna=False, as_index=False)
        .agg(
            bets=("bet_result", "size"),
            bet_wins=("bet_win", "sum"),
            bet_losses=("bet_loss", "sum"),
            pnl=("pnl", "sum"),
            avg_calibrated_edge=("calibrated_edge", "mean"),
        )
        .copy()
    )
    wl = g["bet_wins"] + g["bet_losses"]
    g["bet_win_rate"] = np.where(wl > 0, g["bet_wins"] / wl, np.nan)
    g["roi"] = np.where(g["bets"] > 0, g["pnl"] / g["bets"], np.nan)
    order = ["< 0pp", "0-5pp", "5-10pp", "10-15pp", "15-20pp", "> 20pp", "unknown"]
    g["__order"] = g["edge_bucket"].map(lambda v: order.index(v) if v in order else len(order))
    g = g.sort_values(["__order", "edge_bucket"]).drop(columns=["__order"])
    g = g[cols]

    ordered_rates = g.loc[g["edge_bucket"].isin(order[:-1]) & g["bet_win_rate"].notna(), "bet_win_rate"].tolist()
    if len(ordered_rates) < 2:
        monotonicity = "insufficient_data"
    elif all(float(a) <= float(b) for a, b in zip(ordered_rates, ordered_rates[1:])):
        monotonicity = "monotonic"
    elif all(float(a) >= float(b) for a, b in zip(ordered_rates, ordered_rates[1:])):
        monotonicity = "inverted"
    else:
        monotonicity = "flat_or_mixed"

    spearman = work[["calibrated_edge", "actual_outcome_i"]].corr(method="spearman").iloc[0, 1]
    diag = {
        "calibrated_edge_monotonicity": monotonicity,
        "calibrated_edge_spearman": None if pd.isna(spearman) else float(spearman),
        "calibrated_edge_spearman_n": int(len(work)),
    }
    return g, diag


def _write_summary_md(path: Path, summary: Dict[str, Any]) -> None:
    def pct(v: Any) -> str:
        return "n/a" if v is None else f"{100.0 * float(v):.2f}%"

    lines = [
        "# MLB Execution vs Model",
        "",
        "Pricing interpretation: `bet_odds` is the actual wager price from the tool `Odds` column. `model_fair_price` / no-vig fields are diagnostics only and are not used for wager-price ROI or edge buckets.",
        "",
        f"- Tool rows: {summary.get('tool_rows', 0)}",
        f"- Complete-key tool rows: {summary.get('tool_complete_key_rows', 0)}",
        f"- Matched reconcile rows: {summary.get('matched_rows', 0)}",
        f"- Unmatched rows: {summary.get('unmatched_rows', 0)}",
        f"- Model accuracy: {pct(summary.get('model_accuracy'))} ({summary.get('model_correct', 0)}-{summary.get('model_wrong', 0)}, pushes={summary.get('model_push', 0)})",
        f"- Bet win rate: {pct(summary.get('bet_win_rate'))} ({summary.get('bet_wins', 0)}-{summary.get('bet_losses', 0)}, pushes={summary.get('bet_pushes', 0)})",
        f"- ROI: {pct(summary.get('roi'))} (pnl={summary.get('pnl', 0)})",
        f"- Average bet odds: {summary.get('avg_bet_odds', 'n/a')}",
        f"- Average implied probability from bet odds: {pct(summary.get('avg_implied_probability_from_bet_odds'))}",
        f"- Calibration applied: {summary.get('calibration_applied', False)}",
        f"- Average raw model probability: {pct(summary.get('avg_raw_model_probability'))}",
        f"- Average calibrated model probability: {pct(summary.get('avg_calibrated_model_probability'))}",
        f"- Average model probability used for edge: {pct(summary.get('avg_model_probability'))}",
        f"- Average edge: {pct(summary.get('avg_edge'))}",
        f"- Calibrated edge monotonicity: {summary.get('calibrated_edge_monotonicity', 'n/a')}",
        f"- Calibrated edge Spearman: {summary.get('calibrated_edge_spearman', 'n/a')} (n={summary.get('calibrated_edge_spearman_n', 0)})",
        f"- Model correct but bet lost: {summary.get('model_correct_bet_lost', 0)}",
        f"- Model wrong but bet won: {summary.get('model_wrong_bet_won', 0)}",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare execution-layer tool results against MLB reconcile/model outcomes.")
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD")
    ap.add_argument("--tool-results-csv", required=True, help="Daily tool result download CSV")
    ap.add_argument("--reconcile-csv", default="tmp/mlb_base_vs_market_rows_anybook.csv")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-md", default="")
    ap.add_argument("--unmatched-tool-csv", default="")
    ap.add_argument("--unmatched-reconcile-csv", default="")
    ap.add_argument("--edge-bucket-csv", default="")
    ap.add_argument("--calibrated-edge-bucket-csv", default="")
    ap.add_argument("--odds-distribution-csv", default="")
    ap.add_argument(
        "--calibration-json",
        default="",
        help="Optional MLB probability calibration JSON. When set, edge/model_prob use calibrated probabilities.",
    )
    ap.add_argument(
        "--expected-raw-tool-rows",
        type=int,
        default=0,
        help="Optional guardrail: stop if the loaded wager export row count does not match this value.",
    )
    ap.add_argument(
        "--expected-mlb-betonline-rows",
        type=int,
        default=0,
        help="Optional guardrail: stop if MLB/BetOnline wager rows do not match this value.",
    )
    ap.add_argument(
        "--expected-mlb-betonline-non-push-rows",
        type=int,
        default=0,
        help="Optional guardrail: stop if MLB/BetOnline non-push rows do not match this value.",
    )
    args = ap.parse_args()

    target_date = _parse_date(args.date)
    if not target_date:
        raise ValueError("--date must parse to YYYY-MM-DD")

    tool_path = Path(args.tool_results_csv).expanduser()
    rec_path = Path(args.reconcile_csv).expanduser()
    out_csv = Path(args.out_csv or f"artifacts/analysis/mlb/execution_vs_model/{target_date}/execution_vs_model.csv").expanduser()
    out_json = Path(args.out_json or f"artifacts/analysis/mlb/execution_vs_model/{target_date}/summary.json").expanduser()
    out_md = Path(args.out_md or f"artifacts/analysis/mlb/execution_vs_model/{target_date}/summary.md").expanduser()
    default_out_dir = out_csv.parent
    unmatched_tool_csv = Path(args.unmatched_tool_csv or default_out_dir / "unmatched_tool_rows.csv").expanduser()
    unmatched_reconcile_csv = Path(args.unmatched_reconcile_csv or default_out_dir / "unmatched_reconcile_rows.csv").expanduser()
    edge_bucket_csv = Path(args.edge_bucket_csv or default_out_dir / "edge_bucket_summary.csv").expanduser()
    calibrated_edge_bucket_csv = Path(
        args.calibrated_edge_bucket_csv or default_out_dir / "edge_bucket_summary_calibrated.csv"
    ).expanduser()
    odds_distribution_csv = Path(args.odds_distribution_csv or default_out_dir / "odds_distribution.csv").expanduser()

    tool_raw = pd.read_csv(tool_path)
    wager_counts = _wager_export_counts(tool_raw)
    print(f"[execution-vs-model] raw_rows_loaded={wager_counts['raw_rows_loaded']}")
    print(
        "[execution-vs-model] graded_wager_rows "
        f"mlb={wager_counts['mlb_rows']} "
        f"mlb_betonline={wager_counts['mlb_betonline_rows']} "
        f"mlb_betonline_non_push={wager_counts['mlb_betonline_non_push_rows']} "
        f"columns={wager_counts['wager_guard_columns']}"
    )
    _guard_expected_count(
        label="raw tool/wager export",
        actual=int(wager_counts["raw_rows_loaded"]),
        expected=int(args.expected_raw_tool_rows or 0),
    )
    _guard_expected_count(
        label="MLB BetOnline wager",
        actual=int(wager_counts["mlb_betonline_rows"]),
        expected=int(args.expected_mlb_betonline_rows or 0),
    )
    _guard_expected_count(
        label="MLB BetOnline non-push wager",
        actual=int(wager_counts["mlb_betonline_non_push_rows"]),
        expected=int(args.expected_mlb_betonline_non_push_rows or 0),
    )
    rec_raw = pd.read_csv(rec_path)
    reconcile_input_meta = _validate_reconcile_input(rec_raw, rec_path=rec_path, target_date=target_date)
    tool, meta = _normalize_tool_results(tool_raw, default_date=target_date)
    meta.update(wager_counts)
    meta.update(reconcile_input_meta)
    rec = _normalize_reconcile(rec_raw, target_date=target_date, calibration_json=str(args.calibration_json or ""))
    merged, join_diag = _join_execution_to_model(tool, rec)
    meta.update(join_diag)
    meta["unique_reconcile_prop_type"] = sorted(set(x for x in rec["prop_type_norm"].dropna().astype(str).tolist() if x))

    mismatch_sample = (
        tool.loc[~tool["player_name_key"].isin(set(rec["player_name_key"].dropna().astype(str))), ["player_name_norm", "prop_type_norm", "line_norm", "side_norm"]]
        .drop_duplicates()
        .head(10)
        .to_dict(orient="records")
    )
    meta["sample_player_mismatches"] = mismatch_sample

    output_cols = [
        "date_norm",
        "wager_timestamp_utc",
        "player_name_norm",
        "player_id_norm",
        "prop_type_norm",
        "line_norm",
        "side_norm",
        "bet_odds",
        "model_fair_price",
        "station_odds",
        "closing_no_vig_price",
        "unboosted_odds",
        "implied_prob_from_bet_odds",
        "bet_result",
        "bet_win",
        "pnl",
        "model_correct",
        "model_pick_side_norm",
        "model_pick_outcome",
        "model_prob",
        "raw_model_prob",
        "calibrated_model_prob",
        "calibration_applied",
        "model_pick_prob",
        "model_pick_prob_raw",
        "edge",
        "calibrated_edge",
        "edge_bucket",
        "calibrated_edge_bucket",
        "bet_odds_bucket",
        "bet_side_actual_outcome",
        "model_correct_bet_lost",
        "model_wrong_bet_won",
        "matched_reconcile",
        "join_strategy",
        "snapshot_run_tag",
        "snapshot_time_utc",
        "snapshot_age_minutes",
        "snapshot_match_policy",
        "tool_row_id",
    ]
    passthrough_cols = [c for c in merged.columns if c.startswith("tool__")]
    out = merged[[c for c in output_cols if c in merged.columns] + passthrough_cols].copy()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    unmatched_tool = merged[~merged["matched_reconcile"]].copy()
    unmatched_tool_csv.parent.mkdir(parents=True, exist_ok=True)
    unmatched_tool.to_csv(unmatched_tool_csv, index=False)

    matched_keys = merged.loc[merged["matched_reconcile"], RELAXED_JOIN_COLS].drop_duplicates()
    rec_unmatched = rec.copy()
    rec_unmatched["player_join_key"] = rec_unmatched["player_join_key_name"]
    if not matched_keys.empty:
        rec_unmatched = rec_unmatched.merge(matched_keys, on=RELAXED_JOIN_COLS, how="left", indicator=True)
        rec_unmatched = rec_unmatched[rec_unmatched["_merge"] == "left_only"].drop(columns=["_merge"])
    unmatched_reconcile_csv.parent.mkdir(parents=True, exist_ok=True)
    rec_unmatched.to_csv(unmatched_reconcile_csv, index=False)

    edge_bucket = _bucket_summary(merged)
    edge_bucket_csv.parent.mkdir(parents=True, exist_ok=True)
    edge_bucket.to_csv(edge_bucket_csv, index=False)
    calibrated_edge_bucket, calibrated_edge_diag = _calibrated_edge_quality(merged)
    calibrated_edge_bucket_csv.parent.mkdir(parents=True, exist_ok=True)
    calibrated_edge_bucket.to_csv(calibrated_edge_bucket_csv, index=False)
    odds_distribution = _odds_distribution(merged)
    odds_distribution_csv.parent.mkdir(parents=True, exist_ok=True)
    odds_distribution.to_csv(odds_distribution_csv, index=False)

    summary = _summary(merged, meta={**meta, "date": target_date, "tool_results_csv": str(tool_path), "reconcile_csv": str(rec_path)})
    summary.update(calibrated_edge_diag)
    summary["unmatched_reconcile_rows"] = int(len(rec_unmatched))
    if "snapshot_match_policy" in merged.columns:
        summary["snapshot_match_policy_counts"] = (
            merged["snapshot_match_policy"].fillna("").astype(str).value_counts().to_dict()
        )
    if "snapshot_age_minutes" in merged.columns:
        ages = pd.to_numeric(merged.loc[merged["matched_reconcile"], "snapshot_age_minutes"], errors="coerce")
        summary["avg_snapshot_age_minutes"] = float(ages.mean()) if ages.notna().any() else None
    summary["outputs"] = {
        "execution_vs_model_csv": str(out_csv),
        "summary_json": str(out_json),
        "summary_md": str(out_md),
        "unmatched_tool_rows_csv": str(unmatched_tool_csv),
        "unmatched_reconcile_rows_csv": str(unmatched_reconcile_csv),
        "edge_bucket_summary_csv": str(edge_bucket_csv),
        "edge_bucket_summary_calibrated_csv": str(calibrated_edge_bucket_csv),
        "odds_distribution_csv": str(odds_distribution_csv),
    }
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    _write_summary_md(out_md, summary)

    print(f"[execution-vs-model] date={target_date}")
    print(
        "[execution-vs-model] reconcile_input "
        f"path={rec_path} "
        f"target_rows={summary.get('reconcile_target_rows')} "
        f"mtime={summary.get('reconcile_mtime')} "
        f"freshness={summary.get('reconcile_mtime_guard')} "
        f"outcome_nonblank={summary.get('reconcile_outcome_nonblank')}"
    )
    print(f"[execution-vs-model] tool_columns={meta.get('tool_columns')}")
    print(f"[execution-vs-model] tool_sample_values={meta.get('tool_sample_values')}")
    print(f"[execution-vs-model] unique_tool_prop_names={meta.get('unique_tool_prop_names')}")
    print(f"[execution-vs-model] unique_reconcile_prop_type={meta.get('unique_reconcile_prop_type')}")
    print(f"[execution-vs-model] sample_player_mismatches={meta.get('sample_player_mismatches')}")
    print(
        "[execution-vs-model] join_diagnostics "
        f"strict={summary.get('strict_join_matches')} "
        f"relaxed_without_line={summary.get('relaxed_without_line_matches')} "
        f"used={summary.get('join_strategy_used')}"
    )
    print(f"[execution-vs-model] tool_rows={summary['tool_rows']} complete_key_rows={summary['tool_complete_key_rows']}")
    print(f"[execution-vs-model] matched={summary['matched_rows']} unmatched={summary['unmatched_rows']}")
    print(
        "[execution-vs-model] model_accuracy="
        f"{summary['model_accuracy']} bet_win_rate={summary['bet_win_rate']} roi={summary['roi']}"
    )
    print(
        "[execution-vs-model] pricing "
        f"avg_bet_odds={summary.get('avg_bet_odds')} "
        f"avg_implied_prob={summary.get('avg_implied_probability_from_bet_odds')} "
        f"avg_model_prob={summary.get('avg_model_probability')} "
        f"avg_edge={summary.get('avg_edge')}"
    )
    print(
        "[execution-vs-model] disconnects "
        f"model_correct_bet_lost={summary['model_correct_bet_lost']} "
        f"model_wrong_bet_won={summary['model_wrong_bet_won']}"
    )
    print(f"[execution-vs-model] out_csv={out_csv}")
    print(f"[execution-vs-model] unmatched_tool_csv={unmatched_tool_csv}")
    print(f"[execution-vs-model] unmatched_reconcile_csv={unmatched_reconcile_csv}")
    print(f"[execution-vs-model] edge_bucket_csv={edge_bucket_csv}")
    print(
        "[execution-vs-model] calibrated_edge_quality "
        f"monotonicity={summary.get('calibrated_edge_monotonicity')} "
        f"spearman={summary.get('calibrated_edge_spearman')} "
        f"n={summary.get('calibrated_edge_spearman_n')}"
    )
    print(f"[execution-vs-model] calibrated_edge_bucket_csv={calibrated_edge_bucket_csv}")
    print(f"[execution-vs-model] odds_distribution_csv={odds_distribution_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
