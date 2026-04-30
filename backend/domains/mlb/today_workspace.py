"""Domain helpers for MLB /today workspace."""

from __future__ import annotations

import csv
from datetime import date, datetime
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from backend.domains.mlb.repository.today_workspace_repository import (
    fetch_today_prop_availability as repo_fetch_today_prop_availability,
    fetch_today_workspace_last_updated as repo_fetch_today_workspace_last_updated,
    fetch_today_workspace_rows as repo_fetch_today_workspace_rows,
)

ET = ZoneInfo("America/New_York")
logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[3]
PROP_REGIME_CONTEXT_CSV = (
    REPO_ROOT / "artifacts/analysis/mlb/prop_regime_validation/prop_regime_combined_signal.csv"
)
DEPLOYED_PROP_REGIME_CONTEXT_CSV = (
    REPO_ROOT / "backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv"
)


def _int_or_none(value: Any) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _display_prop(prop_type: Any) -> str:
    prop = str(prop_type or "").strip().lower()
    labels = {
        "hits": "Hits",
        "total_bases": "Total Bases",
        "hits_runs_rbis": "HRRBI",
        "strikeouts_pitching": "Pitcher Ks",
        "strikeouts_batting": "Batter Ks",
        "outs_recorded": "Outs Recorded",
        "earned_runs": "Earned Runs",
        "walks_allowed": "Walks Allowed",
        "hits_allowed": "Hits Allowed",
        "runs_scored": "Runs",
        "rbis": "RBIs",
        "rbi": "RBIs",
        "home_runs": "Home Runs",
        "walks": "Walks",
        "doubles": "Doubles",
    }
    return labels.get(prop, prop.replace("_", " ").title() if prop else "")


def _resolve_requested_slate_date(slate_date: Optional[str]) -> str:
    if slate_date:
        # Router validates format; keep this as a defensive guard.
        date.fromisoformat(str(slate_date))
        return str(slate_date)
    return datetime.now(ET).date().isoformat()


def _load_prop_regime_context() -> Dict[str, Dict[str, Any]]:
    source_path = PROP_REGIME_CONTEXT_CSV
    if not source_path.exists():
        logger.warning(
            "MLB prop regime context source missing at %s; cwd=%s; trying deployed fallback %s",
            source_path,
            Path.cwd(),
            DEPLOYED_PROP_REGIME_CONTEXT_CSV,
        )
        source_path = DEPLOYED_PROP_REGIME_CONTEXT_CSV
    if not source_path.exists():
        logger.warning(
            "MLB prop regime context unavailable; missing primary=%s fallback=%s cwd=%s",
            PROP_REGIME_CONTEXT_CSV,
            DEPLOYED_PROP_REGIME_CONTEXT_CSV,
            Path.cwd(),
        )
        return {}
    if source_path.stat().st_size <= 0:
        logger.warning("MLB prop regime context source is empty: %s", source_path)
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    with source_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            prop_type = str(row.get("prop_type") or "").strip().lower()
            if not prop_type:
                continue
            regime_label = row.get("regime_context_label")
            has_regime_context = bool(str(regime_label or "").strip())
            out[prop_type] = {
                "prop_type": prop_type,
                "display_prop": _display_prop(prop_type),
                "regime_context_score": row.get("regime_context_score"),
                "regime_context_label": regime_label,
                "regime_context_explanation": row.get("regime_context_explanation"),
                "long_term_regime": row.get("long_term_regime"),
                "recent_db_regime": row.get("recent_db_regime") or row.get("recent_regime"),
                "execution_regime": row.get("execution_regime"),
                "latest_regime_date": row.get("latest_usable_date"),
                "long_term_sample_rows": _int_or_none(row.get("long_term_30d_rows")),
                "recent_sample_rows": _int_or_none(row.get("recent_window_rows")),
                "trend_sample_rows": _int_or_none(row.get("trend_window_rows")),
                "recent_window_days": _int_or_none(row.get("recent_window_days")),
                "trend_window_days": _int_or_none(row.get("trend_window_days")),
                "trend_direction": row.get("trend_direction") or row.get("execution_regime"),
                "trend_metric_recent": row.get("trend_metric_recent"),
                "trend_metric_prior": row.get("trend_metric_prior"),
                "trend_metric_delta": row.get("trend_metric_delta"),
                "trend_prior_window_days": _int_or_none(row.get("trend_prior_window_days")),
                "trend_prior_sample_rows": _int_or_none(row.get("trend_prior_sample_rows")),
                "regime_context_available": has_regime_context,
            }
    if not out:
        logger.warning("MLB prop regime context source loaded zero rows from %s", source_path)
    else:
        logger.info("MLB prop regime context loaded %s rows from %s", len(out), source_path)
    return out


def _apply_regime_context(row: Dict[str, Any], context_by_prop: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(row)
    prop_type = str(out.get("prop_type") or "").strip().lower()
    context = context_by_prop.get(prop_type, {})

    out["regime_context_score"] = context.get("regime_context_score")
    out["regime_context_label"] = context.get("regime_context_label")
    out["regime_context_explanation"] = context.get("regime_context_explanation")
    out["long_term_regime"] = context.get("long_term_regime")
    out["recent_db_regime"] = context.get("recent_db_regime")
    out["execution_regime"] = context.get("execution_regime")
    out["latest_regime_date"] = context.get("latest_regime_date")
    out["long_term_sample_rows"] = context.get("long_term_sample_rows")
    out["recent_sample_rows"] = context.get("recent_sample_rows")
    out["trend_sample_rows"] = context.get("trend_sample_rows")
    out["recent_window_days"] = context.get("recent_window_days")
    out["trend_window_days"] = context.get("trend_window_days")
    out["trend_direction"] = context.get("trend_direction")
    out["trend_metric_recent"] = context.get("trend_metric_recent")
    out["trend_metric_prior"] = context.get("trend_metric_prior")
    out["trend_metric_delta"] = context.get("trend_metric_delta")
    out["trend_prior_window_days"] = context.get("trend_prior_window_days")
    out["trend_prior_sample_rows"] = context.get("trend_prior_sample_rows")
    out["regime_context_available"] = bool(context.get("regime_context_available"))
    out["regime_context_missing_reason"] = None if context else f"No regime context row found for prop_type={prop_type}."
    return out


def _build_regime_context_by_prop(
    rows: list[Dict[str, Any]], context_by_prop: Dict[str, Dict[str, Any]]
) -> list[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for row in rows:
        prop_type = str(row.get("prop_type") or "").strip().lower()
        if prop_type:
            counts[prop_type] = counts.get(prop_type, 0) + 1

    out = []
    for prop_type in sorted(counts, key=lambda p: (_display_prop(p).lower(), p)):
        context = context_by_prop.get(prop_type, {})
        has_context = bool(context.get("regime_context_available"))
        out.append(
            {
                "prop_type": prop_type,
                "display_prop": context.get("display_prop") or _display_prop(prop_type),
                "regime_context_score": context.get("regime_context_score"),
                "regime_context_label": context.get("regime_context_label"),
                "regime_context_explanation": context.get("regime_context_explanation"),
                "long_term_regime": context.get("long_term_regime"),
                "recent_db_regime": context.get("recent_db_regime"),
                "execution_regime": context.get("execution_regime"),
                "latest_regime_date": context.get("latest_regime_date"),
                "today_rows": counts[prop_type],
                "long_term_sample_rows": context.get("long_term_sample_rows"),
                "recent_sample_rows": context.get("recent_sample_rows"),
                "trend_sample_rows": context.get("trend_sample_rows"),
                "recent_window_days": context.get("recent_window_days"),
                "trend_window_days": context.get("trend_window_days"),
                "trend_direction": context.get("trend_direction"),
                "trend_metric_recent": context.get("trend_metric_recent"),
                "trend_metric_prior": context.get("trend_metric_prior"),
                "trend_metric_delta": context.get("trend_metric_delta"),
                "trend_prior_window_days": context.get("trend_prior_window_days"),
                "trend_prior_sample_rows": context.get("trend_prior_sample_rows"),
                "regime_context_available": has_context,
                "regime_context_missing_reason": None
                if context
                else f"No regime context row found for prop_type={prop_type}.",
                "row_count": counts[prop_type],
            }
        )
    return out


def fetch_today_workspace_rows(
    *,
    slate_date: Optional[str] = None,
    prop_type: Optional[str] = None,
    team: Optional[str] = None,
    side: Optional[str] = None,
    timing_signal: Optional[str] = None,
    player_id: Optional[int] = None,
    player_query: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> Dict[str, Any]:
    requested_slate_date = _resolve_requested_slate_date(slate_date)
    rows = repo_fetch_today_workspace_rows(
        slate_date=requested_slate_date,
        prop_type=prop_type,
        team=team,
        side=side,
        timing_signal=timing_signal,
        player_id=player_id,
        player_query=player_query,
        limit=limit,
        offset=offset,
    )
    last_updated = repo_fetch_today_workspace_last_updated(slate_date=requested_slate_date)
    total = int(rows[0].get("total_rows") or 0) if rows else 0
    context_by_prop = _load_prop_regime_context()
    cleaned = []
    for r in rows:
        row = dict(r)
        row.pop("total_rows", None)
        cleaned.append(_apply_regime_context(row, context_by_prop))
    is_ready = len(cleaned) > 0
    return {
        "ok": True,
        "count": len(cleaned),
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "requested_slate_date": requested_slate_date,
        "active_slate_date": requested_slate_date if is_ready else None,
        "is_ready": is_ready,
        "last_updated": last_updated,
        "regime_context_by_prop": _build_regime_context_by_prop(cleaned, context_by_prop),
        "rows": cleaned,
    }


def fetch_today_prop_availability(
    *,
    slate_date: Optional[str] = None,
    player_id: int,
    prop_type: str,
) -> Dict[str, Any]:
    requested_slate_date = _resolve_requested_slate_date(slate_date)
    details = repo_fetch_today_prop_availability(
        slate_date=requested_slate_date,
        player_id=int(player_id),
        prop_type=str(prop_type),
    )
    return {
        "ok": True,
        "requested_slate_date": requested_slate_date,
        "player_id": int(player_id),
        "prop_type": str(prop_type).strip().lower(),
        **details,
    }
