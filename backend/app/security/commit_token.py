# backend/app/security/commit_token.py

from __future__ import annotations
import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict

# -------- base64 helpers (URL-safe, no padding) --------
def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("utf-8").rstrip("=")

def _b64d(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)

# -------- small env helpers --------
def _get_secret_bytes(override: str | None) -> bytes:
    secret = override or os.getenv("PROP_COMMIT_SECRET", "dev-secret-change-me")
    return secret.encode("utf-8")

def _get_ttl_seconds(override: int | None) -> int:
    try:
        return int(override if override is not None else os.getenv("PROP_COMMIT_TTL_SEC", "600"))
    except Exception:
        return 600

# -------- API --------
def mint_commit_token(
    *, prob: float, prop_type: str, features: dict,
    ttl_seconds: int = 600, secret: str | None = None, version: str = "v1",
) -> str:
    """
    Return a versioned token: "{version}.{payload_b64}.{sig_b64}"
    We sign the BASE64 STRING (payload_b64) with HMAC-SHA256.
    """
    now = int(time.time())
    payload_obj = {
        "features": features,
        "prob": prob,
        "prop_type": prop_type,
        "ts": now,
        "exp": now + int(ttl_seconds),
    }
    payload_b64 = _b64e(json.dumps(payload_obj, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(_get_secret_bytes(secret), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = _b64e(sig)
    return f"{version}.{payload_b64}.{sig_b64}"

def verify_commit_token(
    token: str, *, ttl_seconds: int | None = None, secret: str | None = None,
) -> Dict[str, Any]:
    """
    Validate signature + TTL and return the payload dict.
    Raises ValueError on invalid/expired tokens.
    """
    if not token or token.count(".") != 2:
        raise ValueError("Malformed token")

    version, payload_b64, sig_b64 = token.split(".", 2)
    if version != "v1":
        raise ValueError("Unsupported token version")

    # Verify HMAC over the BASE64 STRING
    expected_sig = hmac.new(_get_secret_bytes(secret), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    got_sig = _b64d(sig_b64)
    if not hmac.compare_digest(got_sig, expected_sig):
        raise ValueError("Bad signature")

    # Decode payload and check TTL
    try:
        payload = json.loads(_b64d(payload_b64).decode("utf-8"))
    except Exception:
        raise ValueError("Bad payload")

    ts = int(payload.get("ts", 0))
    ttl = _get_ttl_seconds(ttl_seconds)
    if ts <= 0 or (int(time.time()) - ts) > ttl:
        raise ValueError("Token expired")

    if "features" not in payload or "prop_type" not in payload:
        raise ValueError("Missing fields")

    return payload
