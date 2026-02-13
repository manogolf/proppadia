"""Shared response validators for smoke/post-deploy checks."""

from __future__ import annotations

from typing import Any, Callable, Tuple


Validator = Callable[[Any], Tuple[bool, str]]


def expect_ok(body: Any) -> Tuple[bool, str]:
    if not isinstance(body, dict):
        return False, "body is not object"
    return body.get("ok") is True, "expects ok=true"


def expect_ping_sport(sport: str) -> Validator:
    def _validate(body: Any) -> Tuple[bool, str]:
        if not isinstance(body, dict):
            return False, "ping body is not object"
        return body.get("ok") is True and body.get("sport") == sport, f"expects ok=true,sport={sport}"

    return _validate


def expect_predict_probability_and_token(body: Any) -> Tuple[bool, str]:
    if not isinstance(body, dict):
        return False, "predict body is not object"
    has_prob = isinstance(body.get("probability"), (int, float))
    has_token = isinstance(body.get("commit_token"), str) and "." in body.get("commit_token", "")
    return bool(has_prob and has_token), "expects probability + commit_token"


def expect_ok_count_rows(body: Any) -> Tuple[bool, str]:
    if not isinstance(body, dict):
        return False, "expects object"
    rows = body.get("rows")
    count = body.get("count")
    ok = body.get("ok") is True and isinstance(rows, list) and isinstance(count, int)
    return ok, "expects ok=true,count=int,rows=list"


def expect_list_or_error_object(body: Any) -> Tuple[bool, str]:
    if isinstance(body, list):
        return True, "expects list payload"
    if isinstance(body, dict) and body.get("ok") is False and isinstance(body.get("error"), str):
        return True, "allows structured db error payload"
    return False, "expects list payload (or {ok:false,error} object)"
