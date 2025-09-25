# backend/app/routes/api/score_prop.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pathlib import Path
from typing import Optional, Dict, Any, List
from app.services.model_registry import resolve_feature_spec_path
from app.security.commit_token import mint_commit_token

import os, json, math, time
import time, base64, hmac, hashlib
import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

router = APIRouter()

# ---------- request schema ----------
class ScoreReq(BaseModel):
    prop_type: str
    line: float
    # You can pass either a fully-prepared features dict (preferred),
    # or the legacy tuple (player_id, game_date) + have the caller precompute features via /prepareProp
    features: Optional[Dict[str, Any]] = None
    player_id: Optional[int] = None
    game_date: Optional[str] = None


# ---------- helpers ----------
def _base_models_dir() -> Path:
    # Honor MODEL_DIR or MODELS_DIR; default to props layout
    base = os.getenv("MODEL_DIR") or os.getenv("MODELS_DIR") or "/var/data/models/props"
    return Path(base).resolve()

def _prop_dir(prop: str) -> Path:
    # /var/data/models/props/latest/<prop>/
    return _base_models_dir() / "latest" / prop

def _resolve_version_from_latest(md: Path) -> str:
    # md is /.../props/latest/<prop>. We want the real target of 'latest'
    try:
        return md.parent.resolve().name  # vYYYYMMDD after following symlink
    except Exception:
        return "unknown"

def _find_artifacts(prop: str):
    """
    Return (model_path, zero_model_or_None, calibrators_or_None, features_json_or_None, model_dir)
    Prefer new numeric-only names; fall back to older names.
    """
    md = _prop_dir(prop)
    if not md.is_dir():
        raise HTTPException(500, f"Model directory not found: {md}")

    # model (prefer *poisson*.joblib; fall back to lambda/any)
    candidates = [
        *md.glob("*poisson*.joblib"),
        md / "zip_lambda.joblib",
        *md.glob("*lambda*.joblib"),
        *md.glob("*.joblib"),
    ]
    model = next((p for p in candidates if p.exists()), None)
    if model is None:
        listing = sorted(p.name for p in md.glob("*"))
        raise HTTPException(500, f"No model found in {md}; saw: {listing}")

    # optional zero model (ZIP); new flow won’t have one
    zero = next((p for p in [md / "zip_zero.joblib", *md.glob("*zero*.joblib")] if p.exists()), None)

    # features & calibrators (prefix-aware or generic)
    feat = next((p for p in [*md.glob("*_features_v1.json"), md / f"features_{prop}_v1.json"] if p.exists()), None)
    cal  = next((p for p in [*md.glob("*_calibrators_v1.json"), md / f"calibrators_{prop}_v1.json"] if p.exists()), None)

    return model, zero, cal, feat, md

def _load_feature_spec(path: Path) -> List[str]:
    """
    Trainer writes either a list or {"features":[...]}.
    """
    try:
        obj = json.loads(path.read_text())
        if isinstance(obj, dict) and "features" in obj:
            return list(map(str, obj["features"]))
        if isinstance(obj, list):
            return list(map(str, obj))
    except Exception:
        pass
    raise HTTPException(500, f"Invalid features file: {path}")

def _align_numeric_row(features: Dict[str, Any], spec: List[str]) -> pd.DataFrame:
    """
    Keep strictly numeric values; coerce; missing -> 0.0; extra keys ignored.
    """
    row: Dict[str, float] = {}
    for col in spec:
        v = features.get(col, 0.0)
        try:
            row[col] = float(v)
        except Exception:
            row[col] = 0.0
    return pd.DataFrame([row], columns=spec)

def _apply_calibrator_scalar(p: float, cal_path: Path, line: float) -> float:
    """
    Use bag for the requested line key if available; else median of first bag; else identity.
    """
    try:
        obj = json.loads(cal_path.read_text())
        if not isinstance(obj, dict):
            return p
        lines = obj.get("lines") or {}
        key = str(line).replace(".", "_")
        bag = lines.get(key)
        if not bag:
            # fallback to first available bag
            if lines:
                bag = next(iter(lines.values()))
            else:
                return p
        preds = []
        for cal in bag:
            t = cal.get("type", "identity")
            if t == "isotonic":
                x = np.asarray(cal.get("x", []), dtype=float)
                y = np.asarray(cal.get("y", []), dtype=float)
                if x.size >= 2:
                    preds.append(float(np.interp(p, x, y, left=y[0], right=y[-1])))
                else:
                    preds.append(p)
            elif t == "platt":
                z = cal.get("coef", 1.0) * p + cal.get("intercept", 0.0)
                preds.append(float(1.0 / (1.0 + math.exp(-z))))
            else:
                preds.append(p)
        return float(np.median(np.array(preds))) if preds else p
    except Exception:
        return p

def _zip_tail_over(line: float, pi: float, lam: float) -> float:
    """
    P(Y > line) under ZIP (pi = P(structural zero)). If pi==0 → plain Poisson tail.
    """
    k = int(math.floor(line))
    lam = max(1e-9, min(1e9, float(lam)))
    if pi <= 0.0:
        return float(1.0 - poisson.cdf(k, lam))
    t = k + 1
    e = math.exp(-lam)
    p0 = pi + (1.0 - pi) * e
    tail = 1.0 - p0
    pk = (1.0 - pi) * e * lam  # k=1
    for i in range(1, t):
        if i > 1:
            pk *= lam / i
        tail -= pk
    return float(max(0.0, min(1.0, tail)))

def _b64e(b: bytes) -> str:
    # URL-safe base64, no padding
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("utf-8")

def _secret_bytes() -> bytes:
    s = os.getenv("COMMIT_TOKEN_SECRET") or "dev-unsafe"
    return s.encode("utf-8")

def _issue_commit_token(payload: dict) -> str:
    # v1.<payload_b64>.<sig_b64> — HMAC over the BASE64 STRING
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_b64 = _b64e(payload_bytes)
    sig = hmac.new(_secret_bytes(), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = _b64e(sig)
    return f"v1.{payload_b64}.{sig_b64}"

# ---------- route ----------
# backend/app/routes/api/score_prop.py (route replacement only)

@router.post("/api/score-prop")
def score_prop(req: ScoreReq):

    # Discover model + artifact-local feature/calibrator files
    model_path, zero_path, cal_path, feat_path, md = _find_artifacts(req.prop_type)
    version = _resolve_version_from_latest(md)

    # Require features unless you’ve built a separate server-side feature builder
    if not req.features:
        raise HTTPException(
            400,
            "Provide 'features' (from /api/prepareProp). "
            "The scorer aligns to the trained numeric feature spec and fills missing with 0.0."
        )

    if not feat_path:
        raise HTTPException(
            500,
            f"Features file not found alongside model for '{req.prop_type}' in {md}"
        )

    # Load model(s)
    pipe = joblib.load(model_path)
    zero_pipe = joblib.load(zero_path) if zero_path else None

    # Align features to spec (strict numeric)
    spec = _load_feature_spec(feat_path)
    X = _align_numeric_row(req.features, spec)

    # Predict lambda (and optional zero prob)
    try:
        lam = float(np.clip(pipe.predict(X)[0], 1e-9, 1e9))
    except Exception as e:
        # helpful debug
        raise HTTPException(
            500,
            f"predict failed: {e} | n_spec={len(spec)} | model={model_path.name}"
        )

    if zero_pipe is not None:
        try:
            pi = float(zero_pipe.predict_proba(X)[0, 1])
        except Exception:
            pi = 0.0
    else:
        pi = 0.0

    p_over_raw = _zip_tail_over(req.line, pi, lam)
    p_over = _apply_calibrator_scalar(p_over_raw, cal_path, req.line) if (cal_path and cal_path.exists()) else p_over_raw

    # Build commit token payload — includes the full features dict (no hash)
    import time
    commit_payload = {
        "ts": int(time.time()),         # REQUIRED by verifier
        "v": 1,
        "prop_type": req.prop_type,     # REQUIRED
        "features": dict(req.features), # REQUIRED

        # extra context (OK to include)
        "line": float(req.line),
        "model_version": version,
        "mu": lam,
        "p_over": p_over,
        "artifact": model_path.name,
        "feat_file": feat_path.name,

        # convenience fields
        "player_id": req.features.get("player_id"),
        "game_id": req.features.get("game_id"),
        "game_date": req.features.get("game_date"),
        "team_id": req.features.get("team_id"),
        "team_abbr": req.features.get("team"),
    }
    commit_token = mint_commit_token(
        prob=float(p_over),
        prop_type=req.prop_type,
        features=dict(req.features),
        ttl_seconds=600,
    )

    return {
        "prop_type": req.prop_type,
        "line": req.line,
        "mu": lam,
        "p_over": p_over,
        "p_under": 1.0 - p_over,
        "p_over_raw": p_over_raw,
        "used_zero_model": bool(zero_path),
        "used_model": True,
        "model_version": version,
        "artifact_dir": str(md),
        "model_file": model_path.name,
        "zero_model_file": zero_path.name if zero_path else None,
        "calibrators_file": cal_path.name if cal_path else None,
        "features_file": feat_path.name,
        "commit_token": commit_token,
    }
