"""Signed commit-token helpers for MLB prop workflow."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import binascii
from typing import Any, Dict


def _b64u_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64u_decode(s: str) -> bytes:
    pad = "=" * ((4 - len(s) % 4) % 4)
    try:
        return base64.urlsafe_b64decode((s + pad).encode("ascii"))
    except (binascii.Error, ValueError) as e:
        raise ValueError("invalid token encoding") from e


def _secret() -> str:
    return (
        os.getenv("PROPPADIA_COMMIT_TOKEN_SECRET")
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or "local-dev-commit-secret"
    )


def sign_commit_payload(payload: Dict[str, Any], ttl_seconds: int = 1800) -> str:
    body = dict(payload)
    body.setdefault("iat", int(time.time()))
    body.setdefault("exp", int(time.time()) + int(ttl_seconds))
    raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    body_part = _b64u_encode(raw)
    sig = hmac.new(_secret().encode("utf-8"), body_part.encode("ascii"), hashlib.sha256).digest()
    return f"{body_part}.{_b64u_encode(sig)}"


def verify_commit_token(token: str) -> Dict[str, Any]:
    try:
        body_part, sig_part = token.split(".", 1)
    except ValueError as e:
        raise ValueError("invalid token format") from e

    expected = hmac.new(_secret().encode("utf-8"), body_part.encode("ascii"), hashlib.sha256).digest()
    got = _b64u_decode(sig_part)
    if not hmac.compare_digest(expected, got):
        raise ValueError("invalid token signature")

    try:
        payload = json.loads(_b64u_decode(body_part).decode("utf-8"))
    except Exception as e:
        raise ValueError("invalid token payload") from e
    exp = int(payload.get("exp", 0))
    if exp and int(time.time()) > exp:
        raise ValueError("commit token expired")
    return payload
