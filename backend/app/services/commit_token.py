# backend/app/security/commit_token.py


from __future__ import annotations
import os, json, time, hmac, hashlib, base64
from typing import Any, Dict, Optional


# === helpers ================================================================

def _b64e(b: bytes) -> str:
    """URL-safe base64 without padding."""
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("utf-8")

def _b64d(s: str) -> bytes:
    """URL-safe base64 decoder that tolerates missing padding."""
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))

def _get_secret_bytes(explicit: Optional[str] = None) -> bytes:
    secret = explicit or os.getenv("COMMIT_TOKEN_SECRET", "")
    if not secret:
        raise ValueError("COMMIT_TOKEN_SECRET not set")
    return secret.encode("utf-8")

def _get_ttl_seconds(explicit: Optional[int] = None) -> int:
    if explicit is not None:
        return int(explicit)
    env = os.getenv("COMMIT_TOKEN_TTL_SECONDS")
    return int(env) if env else 30 * 60  # default 30m

# === public API =============================================================

def mint_commit_token(
    payload: Dict[str, Any],
    *,
    ttl_seconds: int | None = None,
    secret: str | None = None,
) -> str:
    """
    Create a token: v1.<base64url(json_payload) >.<base64url(HMAC_SHA256 over that base64 string)>
    - Adds `ts` if missing.
    - TTL is enforced at verify-time via ts + ttl.
    """
    body = dict(payload)
    if "ts" not in body:
        body["ts"] = int(time.time())

    # Canonical JSON (stable, no spaces) → base64url (no padding)
    payload_json = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    payload_b64 = _b64e(payload_json)

    # HMAC **over the base64 string bytes** (must match verifier)
    sig = hmac.new(_get_secret_bytes(secret), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = _b64e(sig)

    return f"v1.{payload_b64}.{sig_b64}"

def verify_commit_token(
    token: str,
    *,
    ttl_seconds: int | None = None,
    secret: str | None = None,
) -> Dict[str, Any]:
    """
    Validate signature + TTL and return the payload dict.
    Token format: v1.<base64url(json_payload)>.<base64url(HMAC over that base64 string)>
    """
    if not token or token.count(".") != 2:
        raise ValueError("Malformed token")

    version, payload_b64, sig_b64 = token.split(".", 2)
    if version != "v1":
        raise ValueError("Unsupported token version")

    # Verify signature **over the base64 string** (must match mint)
    expected_sig = hmac.new(_get_secret_bytes(secret), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    got_sig = _b64d(sig_b64)
    if not hmac.compare_digest(got_sig, expected_sig):
        raise ValueError("Bad signature")

    # Decode payload + TTL check (based on `ts`)
    try:
        payload = json.loads(_b64d(payload_b64).decode("utf-8"))
    except Exception:
        raise ValueError("Bad payload")

    ts = int(payload.get("ts", 0))
    ttl = _get_ttl_seconds(ttl_seconds)
    if ts <= 0 or (int(time.time()) - ts) > ttl:
        raise ValueError("Token expired")

    # Minimal required fields for our flow
    if "features" not in payload or "prop_type" not in payload:
        raise ValueError("Missing fields")

    return payload
