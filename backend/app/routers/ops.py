from __future__ import annotations

import os
import secrets
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from backend.app.services.nhl.prop_resolution_service import resolve_nhl_pending_props
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


@router.post("/nhl/resolve-props", summary="Ops: resolve NHL pending props in player_props")
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
