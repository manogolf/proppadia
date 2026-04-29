"""Domain helpers for MLB /today workspace."""

from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from backend.domains.mlb.repository.today_workspace_repository import (
    fetch_today_prop_availability as repo_fetch_today_prop_availability,
    fetch_today_workspace_last_updated as repo_fetch_today_workspace_last_updated,
    fetch_today_workspace_rows as repo_fetch_today_workspace_rows,
)

ET = ZoneInfo("America/New_York")
REPO_ROOT = Path(__file__).resolve().parents[3]
PROP_REGIME_CONTEXT_CSV = (
    REPO_ROOT / "artifacts/analysis/mlb/prop_regime_validation/prop_regime_combined_signal.csv"
)


def _resolve_requested_slate_date(slate_date: Optional[str]) -> str:
    if slate_date:
        # Router validates format; keep this as a defensive guard.
        date.fromisoformat(str(slate_date))
        return str(slate_date)
    return datetime.now(ET).date().isoformat()


def _load_prop_regime_context() -> Dict[str, Dict[str, Any]]:
    if not PROP_REGIME_CONTEXT_CSV.exists():
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    with PROP_REGIME_CONTEXT_CSV.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            prop_type = str(row.get("prop_type") or "").strip().lower()
            if not prop_type:
                continue
            out[prop_type] = {
                "regime_context_score": row.get("regime_context_score"),
                "regime_context_label": row.get("regime_context_label"),
                "regime_context_explanation": row.get("regime_context_explanation"),
            }
    return out


def _decision_ui_from_regime_context(regime_context_label: Any) -> str:
    label = str(regime_context_label or "").strip()
    if label in {"Strong environment", "Favorable environment"}:
        return "FAVORABLE"
    if label == "Mixed / neutral environment":
        return "NEUTRAL"
    return "MONITOR"


def _apply_regime_context(row: Dict[str, Any], context_by_prop: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(row)
    prop_type = str(out.get("prop_type") or "").strip().lower()
    context = context_by_prop.get(prop_type, {})
    regime_label = context.get("regime_context_label")
    regime_explanation = context.get("regime_context_explanation")

    out["market_decision_label"] = out.get("decision_label")
    out["market_decision_reason"] = out.get("decision_reason")
    out["regime_context_score"] = context.get("regime_context_score")
    out["regime_context_label"] = regime_label
    out["regime_context_explanation"] = regime_explanation
    out["decision_label"] = _decision_ui_from_regime_context(regime_label)
    out["decision_reason"] = (
        str(regime_explanation).strip()
        if regime_explanation is not None and str(regime_explanation).strip()
        else "Regime context is unavailable for this prop."
    )
    return out


def fetch_today_workspace_rows(
    *,
    slate_date: Optional[str] = None,
    prop_type: Optional[str] = None,
    team: Optional[str] = None,
    side: Optional[str] = None,
    timing_signal: Optional[str] = None,
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
