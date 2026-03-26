from __future__ import annotations

import os
import secrets
from typing import Literal, Optional

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from backend.app.services.nhl.prop_resolution_service import resolve_nhl_pending_props
from backend.app.services.shared.mlb_prod12_job_service import (
    get_prod12_cycle_status,
    resolve_prod12_artifact,
    start_prod12_cycle,
)
from backend.app.services.shared.render_deploy_service import (
    fetch_latest_deploy,
    fetch_service_metrics,
    trigger_redeploy,
)

router = APIRouter(prefix="/ops", tags=["ops"])


class RedeployRequest(BaseModel):
    clear_cache: bool = False


class NhlResolveRequest(BaseModel):
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    dry_run: bool = True
    only_past_games: bool = True
    outcome: str = "dnp"


class Prod12TriggerRequest(BaseModel):
    run_mode: Literal["daily", "weekly", "full", "auto"] = "daily"
    weekly_day_utc: Optional[int] = None
    mlb_base_url: Optional[str] = None
    mlb_weekly_base_url: Optional[str] = None
    mlb_daily_base_url: Optional[str] = None
    mlb_weekly_phase2_enabled: Optional[int] = None
    mlb_daily_stat_derived_enabled: Optional[int] = None
    mlb_weekly_prop_sequence_enabled: Optional[int] = None
    mlb_weekly_prop_sequence: Optional[str] = None
    mlb_weekly_prop_sequence_continue_on_error: Optional[int] = None
    mlb_weekly_prop_sequence_sleep_sec: Optional[int] = None
    mlb_stat_days_ago: Optional[int] = None
    mlb_stat_from_date: Optional[str] = None
    mlb_stat_to_date: Optional[str] = None
    mlb_stat_max_games: Optional[int] = None
    mlb_stat_skip_existing_dates: Optional[int] = None
    mlb_stat_derived_days: Optional[int] = None
    mlb_stat_derived_min: Optional[int] = None
    mlb_season_require_regular: Optional[int] = None
    mlb_prod12_prop_types: Optional[str] = None
    mlb_date: Optional[str] = None
    mlb_predict_sample: Optional[int] = None
    mlb_predict_min_success: Optional[int] = None
    mlb_prod12_daily_prop_types: Optional[str] = None
    mlb_replay_sample: Optional[int] = None
    mlb_replay_min_success: Optional[int] = None
    mlb_replay_retry_attempts: Optional[int] = None
    mlb_replay_retry_backoff_ms: Optional[int] = None
    mlb_replay_max_predict_p95_ms: Optional[int] = None
    mlb_candidate_min_total: Optional[int] = None
    mlb_prod12_min_lift_pct: Optional[float] = None
    mlb_prod12_max_prop_drop_pct: Optional[float] = None


def _require_ops_token(header_value: Optional[str]) -> None:
    expected = str(os.getenv("OPS_API_TOKEN") or "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="OPS_API_TOKEN is not configured")
    incoming = str(header_value or "").strip()
    if not incoming or not secrets.compare_digest(incoming, expected):
        raise HTTPException(status_code=403, detail="forbidden")


@router.get("/render/deploy-status", summary="Ops: latest Render deploy status")
def render_deploy_status(
    x_ops_token: Optional[str] = Header(default=None, alias="X-Ops-Token"),
):
    _require_ops_token(x_ops_token)
    try:
        return fetch_latest_deploy()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.post("/render/redeploy", summary="Ops: trigger Render redeploy")
def render_redeploy(
    body: RedeployRequest,
    x_ops_token: Optional[str] = Header(default=None, alias="X-Ops-Token"),
):
    _require_ops_token(x_ops_token)
    try:
        return trigger_redeploy(clear_cache=bool(body.clear_cache))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.get("/render/metrics", summary="Ops: Render CPU/memory metrics")
def render_metrics(
    window_minutes: int = 360,
    resolution_seconds: int = 60,
    x_ops_token: Optional[str] = Header(default=None, alias="X-Ops-Token"),
):
    _require_ops_token(x_ops_token)
    try:
        return fetch_service_metrics(
            window_minutes=int(window_minutes),
            resolution_seconds=int(resolution_seconds),
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.post("/nhl/resolve-props", summary="Ops: resolve NHL pending props in nhl.user_props")
def resolve_nhl_props(
    body: NhlResolveRequest,
    x_ops_token: Optional[str] = Header(default=None, alias="X-Ops-Token"),
):
    _require_ops_token(x_ops_token)
    try:
        return resolve_nhl_pending_props(
            from_date=body.from_date,
            to_date=body.to_date,
            dry_run=bool(body.dry_run),
            only_past_games=bool(body.only_past_games),
            outcome=body.outcome,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.post("/mlb/prod12/trigger", summary="Ops: trigger detached MLB prod12 cycle")
def trigger_mlb_prod12_cycle(
    body: Prod12TriggerRequest,
    x_ops_token: Optional[str] = Header(default=None, alias="X-Ops-Token"),
):
    _require_ops_token(x_ops_token)
    weekly_day_utc = body.weekly_day_utc
    if weekly_day_utc is not None and not (1 <= int(weekly_day_utc) <= 7):
        raise HTTPException(status_code=400, detail="weekly_day_utc must be between 1 and 7")
    env_overrides = {
        "MLB_CRON_RUN_MODE": body.run_mode,
        "MLB_CRON_WEEKLY_DAY_UTC": weekly_day_utc,
        "MLB_BASE_URL": body.mlb_base_url,
        "MLB_WEEKLY_BASE_URL": body.mlb_weekly_base_url,
        "MLB_DAILY_BASE_URL": body.mlb_daily_base_url,
        "MLB_WEEKLY_PHASE2_ENABLED": body.mlb_weekly_phase2_enabled,
        "MLB_DAILY_STAT_DERIVED_ENABLED": body.mlb_daily_stat_derived_enabled,
        "MLB_WEEKLY_PROP_SEQUENCE_ENABLED": body.mlb_weekly_prop_sequence_enabled,
        "MLB_WEEKLY_PROP_SEQUENCE": body.mlb_weekly_prop_sequence,
        "MLB_WEEKLY_PROP_SEQUENCE_CONTINUE_ON_ERROR": body.mlb_weekly_prop_sequence_continue_on_error,
        "MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC": body.mlb_weekly_prop_sequence_sleep_sec,
        "MLB_STAT_DAYS_AGO": body.mlb_stat_days_ago,
        "MLB_STAT_FROM_DATE": body.mlb_stat_from_date,
        "MLB_STAT_TO_DATE": body.mlb_stat_to_date,
        "MLB_STAT_MAX_GAMES": body.mlb_stat_max_games,
        "MLB_STAT_SKIP_EXISTING_DATES": body.mlb_stat_skip_existing_dates,
        "MLB_STAT_DERIVED_DAYS": body.mlb_stat_derived_days,
        "MLB_STAT_DERIVED_MIN": body.mlb_stat_derived_min,
        "MLB_SEASON_REQUIRE_REGULAR": body.mlb_season_require_regular,
        "MLB_PROD12_PROP_TYPES": body.mlb_prod12_prop_types,
        "MLB_DATE": body.mlb_date,
        "MLB_PREDICT_SAMPLE": body.mlb_predict_sample,
        "MLB_PREDICT_MIN_SUCCESS": body.mlb_predict_min_success,
        "MLB_PROD12_DAILY_PROP_TYPES": body.mlb_prod12_daily_prop_types,
        "MLB_REPLAY_SAMPLE": body.mlb_replay_sample,
        "MLB_REPLAY_MIN_SUCCESS": body.mlb_replay_min_success,
        "MLB_REPLAY_RETRY_ATTEMPTS": body.mlb_replay_retry_attempts,
        "MLB_REPLAY_RETRY_BACKOFF_MS": body.mlb_replay_retry_backoff_ms,
        "MLB_REPLAY_MAX_PREDICT_P95_MS": body.mlb_replay_max_predict_p95_ms,
        "MLB_CANDIDATE_MIN_TOTAL": body.mlb_candidate_min_total,
        "MLB_PROD12_MIN_LIFT_PCT": body.mlb_prod12_min_lift_pct,
        "MLB_PROD12_MAX_PROP_DROP_PCT": body.mlb_prod12_max_prop_drop_pct,
    }
    try:
        payload = start_prod12_cycle(triggered_by="ops_api", env_overrides=env_overrides)
        if payload.get("status") == "already_running":
            raise HTTPException(status_code=409, detail=payload)
        return payload
    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.get("/mlb/prod12/status", summary="Ops: show MLB prod12 detached cycle status + log tail")
def mlb_prod12_cycle_status(
    tail_lines: int = 80,
    x_ops_token: Optional[str] = Header(default=None, alias="X-Ops-Token"),
):
    _require_ops_token(x_ops_token)
    safe_tail = max(0, min(int(tail_lines), 400))
    try:
        return get_prod12_cycle_status(tail_lines=safe_tail)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.get("/mlb/prod12/artifact", summary="Ops: download MLB prod12 artifact")
def mlb_prod12_artifact(
    kind: Literal["book_upload", "predictions_wide", "slate_output", "archive_manifest"] = "book_upload",
    mlb_date: Optional[str] = None,
    x_ops_token: Optional[str] = Header(default=None, alias="X-Ops-Token"),
):
    _require_ops_token(x_ops_token)
    try:
        artifact = resolve_prod12_artifact(artifact_kind=kind, mlb_date=mlb_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e

    path = artifact["path"]
    if not artifact.get("exists"):
        raise HTTPException(
            status_code=404,
            detail={
                "error": "artifact_missing",
                "kind": artifact.get("kind"),
                "mlb_date": artifact.get("mlb_date"),
                "path": str(path),
            },
        )

    media_type = "application/json" if str(path).endswith(".json") else "text/csv"
    filename = str(path.name)
    if kind == "book_upload":
        filename = f"mlb_book_upload_{artifact.get('mlb_date')}.csv"
    return FileResponse(path=str(path), media_type=media_type, filename=filename)
