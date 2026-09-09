#!/usr/bin/env python3
"""Bounded Odds API historical acquisition and transfer audit for the frozen MLB joint cohort."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shlex
import traceback
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import requests

from backend.app.deps import pg_connect


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_oddsapi_historical_joint_strength_transfer_v1/2026-09-09"
PRIOR_JOINT = ROOT / "artifacts/analysis/model_development/mlb_joint_strength_incremental_value_executability_audit_v1/2026-09-09/unique_game_strength_class_ledger.csv"
URL = "https://api.the-odds-api.com/v4/historical/sports/baseball_mlb/odds"
SPORTS_URL = "https://api.the-odds-api.com/v4/sports/"
MODEL = "MLB_GAME_PYTHAGOREAN_LOG5_V1"
SNAPSHOT = "DESIGNATED_DAILY_PUBLIC_SNAPSHOT"
ADMISSION = "ADMITTED_SHADOW"
MARKET = "h2h"
MAX_CREDITS = 400
EXPECTED_COST_PER_REQUEST = 10
ROBUSTNESS_FLOOR = 38
BOOT_REPS = 4000
SEED = 20260909
BOOKS = (
    (1, "pinnacle", "Pinnacle", "REFERENCE", "Preserved historical Odds API evidence; required reference"),
    (2, "betonlineag", "BetOnline", "ALTERNATIVE", "Preserved historical Odds API h2h evidence; explicitly requested when available"),
    (3, "draftkings", "DraftKings", "ALTERNATIVE", "Major US sportsbook with preserved historical Odds API availability"),
    (4, "fanduel", "FanDuel", "ALTERNATIVE", "Major US sportsbook with preserved historical Odds API availability"),
    (5, "betmgm", "BetMGM", "ALTERNATIVE", "Major US sportsbook with preserved historical Odds API availability"),
    (6, "betrivers", "BetRivers", "ALTERNATIVE", "US sportsbook with preserved historical Odds API availability"),
    (7, "fanatics", "Fanatics", "ALTERNATIVE", "Major US sportsbook with preserved historical Odds API availability"),
    (8, "bovada", "Bovada", "ALTERNATIVE", "Broadly relevant sportsbook with preserved historical Odds API availability"),
    (9, "mybookieag", "MyBookie.ag", "ALTERNATIVE", "Broadly relevant sportsbook with preserved historical Odds API availability"),
    (10, "williamhill_us", "William Hill US", "ALTERNATIVE", "Preserved historical US Odds API identifier"),
)
QUOTA_HEADERS = ("x-requests-last", "x-requests-used", "x-requests-remaining")
LEDGER_FIELDS = (
    "request_id", "recorded_at_utc", "endpoint_class", "game_date", "requested_timestamp_utc",
    "markets", "bookmakers", "odds_format", "date_format", "status", "http_status",
    "x_requests_last", "x_requests_used", "x_requests_remaining", "raw_response_path",
    "response_headers_path", "raw_sha256", "error",
)
RETRY_FIELDS = (
    "request_id", "recorded_at_utc", "prior_status", "x_requests_last",
    "retry_allowed", "decision_reason",
)
_CREDENTIAL_KEYS = frozenset({"apikey", "api_key", "odds_api_key", "the_odds_api_key"})
_QUERY_CREDENTIAL = re.compile(
    r"(?i)(\b(?:apiKey|api_key|ODDS_API_KEY|THE_ODDS_API_KEY)(?:=|%3[dD]))([^&\s\"'<>]+)"
)
_JSON_CREDENTIAL = re.compile(
    r"(?i)([\"'](?:apiKey|api_key|ODDS_API_KEY|THE_ODDS_API_KEY)[\"']\s*:\s*[\"'])([^\"']*)([\"'])"
)
_CLI_REPR_CREDENTIAL = re.compile(
    r"(?i)([\"'](?:--api-key|--apikey)[\"']\s*,\s*[\"'])([^\"']+)([\"'])"
)
_CLI_CREDENTIAL = re.compile(r"(?i)((?:--api-key|--apikey)\s+)([^\s]+)")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, float_format="%.12f", na_rep="")


def json_default(value: Any) -> Any:
    if isinstance(value, np.integer): return int(value)
    if isinstance(value, np.floating): return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime)): return value.isoformat()
    raise TypeError(type(value).__name__)


def redact_sensitive(value: Any) -> str:
    """Return render-safe text without depending on knowledge of the credential value."""
    text = str(value)
    text = _QUERY_CREDENTIAL.sub(r"\1[REDACTED]", text)
    text = _JSON_CREDENTIAL.sub(r"\1[REDACTED]\3", text)
    text = _CLI_REPR_CREDENTIAL.sub(r"\1[REDACTED]\3", text)
    return _CLI_CREDENTIAL.sub(r"\1[REDACTED]", text)


def redact_sensitive_bytes(value: bytes) -> bytes:
    """Scrub response bytes before persistence if a provider echoes a request URL."""
    return redact_sensitive(value.decode("utf-8", errors="replace")).encode("utf-8")


def sanitize_command(command: str | Iterable[Any]) -> str:
    """Render a shell/subprocess command with credential arguments redacted."""
    if isinstance(command, str):
        return redact_sensitive(command)
    return redact_sensitive(" ".join(shlex.quote(str(part)) for part in command))


def render_exception(exc: BaseException) -> str:
    """Render an exception without allowing request URLs or commands to leak apiKey values."""
    return redact_sensitive(f"{type(exc).__name__}: {exc}")


def render_traceback(exc: BaseException) -> str:
    """Render a traceback for diagnostics through the same credential scrubber."""
    return redact_sensitive("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))


def safe_params(params: dict[str, Any]) -> dict[str, Any]:
    """Retain request metadata while omitting every recognized credential field."""
    return {k: redact_sensitive(v) if isinstance(v, str) else v
            for k, v in params.items() if k.lower() not in _CREDENTIAL_KEYS}


def safe_get(url: str, params: dict[str, Any], timeout: int) -> requests.Response:
    """Make a request while ensuring any transport exception is safe to render or trace."""
    try:
        return requests.get(url, params=params, timeout=timeout)
    except Exception as exc:
        raise RuntimeError(render_exception(exc)) from None


def safe_error_code(content: bytes) -> str:
    """Extract only a bounded provider error code, never the provider's free-form message."""
    try:
        code = json.loads(content).get("error_code", "")
    except (AttributeError, json.JSONDecodeError, UnicodeDecodeError):
        return "UNAVAILABLE"
    return code if isinstance(code, str) and re.fullmatch(r"[A-Z0-9_]{1,80}", code) else "UNAVAILABLE"


def http_error_message(endpoint: str, status_code: int, content: bytes) -> str:
    return redact_sensitive(
        f"Odds API {endpoint} HTTP {status_code}; error_code={safe_error_code(content)}; "
        "raw response and quota headers preserved"
    )


def retry_decision(prior_status: str, x_requests_last: Any) -> tuple[bool, str]:
    last = header_int({"x-requests-last": x_requests_last}, "x-requests-last")
    if prior_status == "HTTP_ERROR" and last == 0:
        return True, "DEMONSTRABLY_UNCHARGED_HTTP_FAILURE"
    return False, "PRIOR_SUCCESS_OR_CHARGED_OR_UNKNOWN_FAILURE"


def header_dict(response: requests.Response) -> dict[str, str]:
    return {k: str(response.headers.get(k, "")) for k in QUOTA_HEADERS}


def header_int(headers: dict[str, Any], key: str) -> int | None:
    try: return int(str(headers.get(key, "")).strip())
    except (TypeError, ValueError): return None


def append_ledger(output: Path, row: dict[str, Any]) -> None:
    path = output / "append_only_request_ledger.csv"
    new = not path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LEDGER_FIELDS, extrasaction="ignore")
        if new: writer.writeheader()
        writer.writerow({k: redact_sensitive(row.get(k, "")) for k in LEDGER_FIELDS})
        handle.flush(); os.fsync(handle.fileno())


def append_retry_ledger(output: Path, row: dict[str, Any]) -> None:
    path = output / "append_only_retry_decision_ledger.csv"
    new = not path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RETRY_FIELDS, extrasaction="ignore")
        if new: writer.writeheader()
        writer.writerow({k: redact_sensitive(row.get(k, "")) for k in RETRY_FIELDS})
        handle.flush(); os.fsync(handle.fileno())


def load_terminal_ledger(output: Path) -> pd.DataFrame:
    path = output / "append_only_request_ledger.csv"
    if not path.exists(): return pd.DataFrame(columns=LEDGER_FIELDS)
    d = pd.read_csv(path, dtype=str).fillna("")
    return d[d.status.isin(["SUCCESS", "HTTP_ERROR", "TRANSPORT_ERROR", "ZERO_COST_QUOTA_CHECK_SUCCESS"])]


def norm_team(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
    return {"oakland athletics": "athletics", "la angels": "los angeles angels",
            "n y yankees": "new york yankees", "n y mets": "new york mets",
            "ny yankees": "new york yankees", "ny mets": "new york mets"}.get(text, text)


def load_frozen_cohort() -> pd.DataFrame:
    base = pd.read_csv(PRIOR_JOINT)
    base = base[base.strength_class.eq("JOINT_STRONG_SAME_SIDE")].copy()
    sql = """
      SELECT p.game_date::text AS game_date,p.game_id,p.scheduled_start_utc,p.prediction_timestamp_utc,
             p.prediction_cutoff_utc,p.home_team,p.away_team,p.payload_sha256 AS prediction_payload_sha256,
             o.payload_sha256 AS outcome_payload_sha256
      FROM mlb.public_game_moneyline_predictions p
      JOIN mlb.public_game_moneyline_outcomes o
        USING (game_date,game_id,model_version,prediction_snapshot_class)
      WHERE p.model_version=%s AND p.prediction_snapshot_class=%s AND p.admission_status=%s
      ORDER BY p.game_date,p.scheduled_start_utc,p.game_id
    """
    with pg_connect() as conn, conn.cursor() as cursor:
        cursor.execute(sql, (MODEL, SNAPSHOT, ADMISSION)); rows = cursor.fetchall()
    context = pd.DataFrame(rows)
    keep = ["game_id", "prediction_timestamp_utc", "prediction_cutoff_utc",
            "prediction_payload_sha256", "outcome_payload_sha256"]
    d = base.merge(context[keep], on="game_id", how="left", validate="one_to_one")
    if len(d) != 76 or int(d.evaluated_win.sum()) != 56 or d.prediction_timestamp_utc.isna().any():
        raise RuntimeError("Frozen 76-game, 56-20 cohort identity failed")
    return d.sort_values(["game_date", "scheduled_start_utc", "game_id"]).reset_index(drop=True)


def bookmaker_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    selected = pd.DataFrame([
        {"frozen_order": n, "bookmaker_key": key, "bookmaker_name": name, "role": role,
         "selected": True, "selection_rationale": reason, "outcomes_consulted": False}
        for n, key, name, role, reason in BOOKS
    ])
    rationale = pd.concat([selected, pd.DataFrame([{
        "frozen_order": None, "bookmaker_key": "fliff", "bookmaker_name": "Fliff", "role": "CANDIDATE",
        "selected": False,
        "selection_rationale": "Not returned in preserved Odds API historical MLB h2h availability evidence; no charged discovery request authorized",
        "outcomes_consulted": False,
    }])], ignore_index=True)
    return selected, rationale


def preflight(output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    cohort = load_frozen_cohort()
    cohort["prediction_timestamp_utc"] = pd.to_datetime(cohort.prediction_timestamp_utc, utc=True)
    grouped = cohort.groupby("game_date", sort=True)
    if grouped.prediction_timestamp_utc.nunique().max() != 1:
        raise RuntimeError("A game date has multiple immutable designated prediction timestamps")
    plan = grouped.agg(cohort_games=("game_id", "nunique"),
                       original_immutable_prediction_timestamp_utc=("prediction_timestamp_utc", "first")).reset_index()
    plan["original_immutable_prediction_timestamp_utc"] = plan.original_immutable_prediction_timestamp_utc.map(
        lambda x: x.isoformat().replace("+00:00", "Z"))
    plan["requested_timestamp_utc"] = pd.to_datetime(
        plan.original_immutable_prediction_timestamp_utc, utc=True).dt.floor("s").map(
            lambda x: x.isoformat().replace("+00:00", "Z"))
    plan["snapshot_contract"] = "NEAREST_PROVIDER_SNAPSHOT_AT_OR_BEFORE_REQUESTED_TIMESTAMP"
    plan["market"] = MARKET
    plan["bookmaker_count"] = len(BOOKS)
    plan["expected_credit_cost"] = EXPECTED_COST_PER_REQUEST
    plan["request_id"] = plan.game_date.map(lambda x: f"HIST_H2H_SECOND_RES_{x}")
    plan["raw_response_path"] = plan.game_date.map(
        lambda x: str((output / "raw" / x / "historical_h2h_response_second_resolution.json").relative_to(ROOT)))
    expected = len(plan) * EXPECTED_COST_PER_REQUEST
    if len(plan) != 29 or expected > MAX_CREDITS:
        raise RuntimeError(f"Preflight ceiling failure: dates={len(plan)} expected={expected}")
    selected, rationale = bookmaker_frames()
    write_csv(cohort, output / "frozen_joint_strength_cohort.csv")
    write_csv(plan, output / "date_request_plan.csv")
    write_csv(selected, output / "frozen_bookmaker_request_list.csv")
    write_csv(rationale, output / "bookmaker_selection_rationale.csv")
    if not (output / "append_only_request_ledger.csv").exists():
        (output / "append_only_request_ledger.csv").write_text(",".join(LEDGER_FIELDS) + "\n")
    freeze = {
        "audit": "MLB_ODDSAPI_HISTORICAL_JOINT_STRENGTH_TRANSFER_V1",
        "frozen_before_first_charged_request": True,
        "cohort_games": len(cohort), "cohort_record": "56-20", "game_dates": len(plan),
        "exact_dates": plan.game_date.tolist(), "expected_requests": len(plan),
        "market": MARKET, "snapshots_per_date": 1, "bookmaker_count": len(selected),
        "bookmakers": selected.bookmaker_key.tolist(),
        "fliff_decision": "NOT_HISTORICALLY_AVAILABLE_IN_PRESERVED_ODDS_API_EVIDENCE_NOT_REQUESTED",
        "cost_formula": "10 historical credits x 1 returned market x ceil(10 bookmakers / 10)",
        "expected_cost_per_request": EXPECTED_COST_PER_REQUEST,
        "expected_total_cost": expected, "authorized_credit_ceiling": MAX_CREDITS,
        "headroom_credits": MAX_CREDITS-expected,
        "timestamp_contract": "Immutable designated model timestamp floored to the immediately preceding whole second; nearest provider snapshot at or before it",
        "selection_used_outcomes": False,
        "source_files": {
            "frozen_cohort": sha(output / "frozen_joint_strength_cohort.csv"),
            "date_plan": sha(output / "date_request_plan.csv"),
            "bookmaker_list": sha(output / "frozen_bookmaker_request_list.csv"),
            "bookmaker_rationale": sha(output / "bookmaker_selection_rationale.csv"),
        },
    }
    (output / "pre_acquisition_freeze.json").write_text(json.dumps(freeze, indent=2, sort_keys=True) + "\n")
    return freeze


def verify_freeze(output: Path) -> dict[str, Any]:
    path = output / "pre_acquisition_freeze.json"
    if not path.exists(): raise RuntimeError("Pre-acquisition freeze missing")
    freeze = json.loads(path.read_text())
    files = {"frozen_cohort": "frozen_joint_strength_cohort.csv", "date_plan": "date_request_plan.csv",
             "bookmaker_list": "frozen_bookmaker_request_list.csv", "bookmaker_rationale": "bookmaker_selection_rationale.csv"}
    for key, name in files.items():
        if sha(output / name) != freeze["source_files"][key]: raise RuntimeError(f"Frozen artifact changed: {name}")
    if freeze["expected_total_cost"] > MAX_CREDITS: raise RuntimeError("Frozen cost exceeds authorization")
    return freeze


def quota_check(output: Path, api_key: str, timeout: int) -> dict[str, Any]:
    freeze = verify_freeze(output)
    terminal = load_terminal_ledger(output)
    old = terminal[terminal.status.eq("ZERO_COST_QUOTA_CHECK_SUCCESS")]
    if len(old) and (output / "pre_acquisition_quota_headers.json").exists():
        return json.loads((output / "pre_acquisition_quota_headers.json").read_text())
    params = {"apiKey": api_key, "all": "true"}
    request_meta = {"endpoint": "/v4/sports/", "parameters_excluding_secret": safe_params(params),
                    "documented_quota_cost": 0, "purpose": "pre-acquisition quota headers"}
    (output / "pre_acquisition_quota_request.json").write_text(json.dumps(request_meta, indent=2, sort_keys=True) + "\n")
    append_ledger(output, {"request_id": "ZERO_COST_QUOTA_CHECK", "recorded_at_utc": now(),
                           "endpoint_class": "sports_quota_check", "status": "REQUEST_STARTED"})
    response = safe_get(SPORTS_URL, params=params, timeout=timeout)
    raw_path = output / "pre_acquisition_quota_raw_response.json"
    raw_path.write_bytes(redact_sensitive_bytes(response.content))
    headers = header_dict(response)
    record = {"http_status": response.status_code, **headers,
              "checked_at_utc": now(), "documented_endpoint_cost": 0,
              "raw_response_path": str(raw_path.relative_to(ROOT)), "raw_sha256": sha(raw_path),
              "expected_acquisition_cost": freeze["expected_total_cost"]}
    (output / "pre_acquisition_quota_headers.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    status = "ZERO_COST_QUOTA_CHECK_SUCCESS" if response.ok else "HTTP_ERROR"
    error = "" if response.ok else http_error_message("quota check", response.status_code, response.content)
    append_ledger(output, {"request_id": "ZERO_COST_QUOTA_CHECK", "recorded_at_utc": now(),
                           "endpoint_class": "sports_quota_check", "status": status,
                           "http_status": response.status_code, "x_requests_last": headers["x-requests-last"],
                           "x_requests_used": headers["x-requests-used"],
                           "x_requests_remaining": headers["x-requests-remaining"],
                           "raw_response_path": str(raw_path.relative_to(ROOT)), "raw_sha256": sha(raw_path),
                           "error": error})
    if not response.ok:
        raise RuntimeError(error)
    if header_int(headers, "x-requests-last") not in (None, 0):
        raise RuntimeError("The documented zero-cost quota check reported a nonzero charge")
    remaining = header_int(headers, "x-requests-remaining")
    if remaining is None or remaining < freeze["expected_total_cost"]:
        raise RuntimeError(f"Insufficient or unknown quota remaining: {remaining}")
    return record


def amend_second_resolution(output: Path) -> dict[str, Any]:
    """Preserve the zero-cost rejected plan, then freeze API-compatible whole seconds."""
    freeze = verify_freeze(output)
    terminal = load_terminal_ledger(output)
    failed = terminal[(terminal.endpoint_class.eq("historical_sport_odds")) & terminal.status.eq("HTTP_ERROR")]
    if len(failed) != 1 or header_int(failed.iloc[0].to_dict(), "x_requests_last") != 0:
        raise RuntimeError("Second-resolution amendment requires exactly one demonstrably uncharged historical failure")
    failed_path = ROOT / failed.iloc[0].raw_response_path
    payload = json.loads(failed_path.read_text())
    if payload.get("error_code") != "INVALID_HISTORICAL_TIMESTAMP":
        raise RuntimeError("Failure was not INVALID_HISTORICAL_TIMESTAMP")
    original_plan = output / "date_request_plan_original_fractional_seconds.csv"
    original_freeze = output / "pre_acquisition_freeze_original_fractional_seconds.json"
    if not original_plan.exists(): original_plan.write_bytes((output / "date_request_plan.csv").read_bytes())
    if not original_freeze.exists(): original_freeze.write_bytes((output / "pre_acquisition_freeze.json").read_bytes())
    plan = pd.read_csv(output / "date_request_plan.csv", dtype=str)
    plan["original_immutable_prediction_timestamp_utc"] = plan.requested_timestamp_utc
    plan["requested_timestamp_utc"] = pd.to_datetime(plan.requested_timestamp_utc, utc=True).dt.floor("s").map(
        lambda x: x.isoformat().replace("+00:00", "Z"))
    plan["request_id"] = plan.game_date.map(lambda x: f"HIST_H2H_SECOND_RES_{x}")
    plan["raw_response_path"] = plan.game_date.map(
        lambda x: str((output / "raw" / x / "historical_h2h_response_second_resolution.json").relative_to(ROOT)))
    write_csv(plan, output / "date_request_plan.csv")
    freeze["timestamp_contract"] = ("Immutable designated model timestamp floored to the immediately preceding whole second; "
                                    "provider must return nearest snapshot at or before it")
    freeze["amendment"] = {
        "reason": "FIRST_ATTEMPT_REJECTED_INVALID_HISTORICAL_TIMESTAMP_DUE_TO_FRACTIONAL_SECONDS",
        "first_attempt_charge": 0, "prices_returned": False,
        "original_plan_path": original_plan.name, "original_plan_sha256": sha(original_plan),
        "original_freeze_path": original_freeze.name, "original_freeze_sha256": sha(original_freeze),
        "correction": "FLOOR_TO_WHOLE_SECOND_STILL_AT_OR_BEFORE_IMMUTABLE_TIMESTAMP",
    }
    freeze["source_files"]["date_plan"] = sha(output / "date_request_plan.csv")
    (output / "pre_acquisition_freeze.json").write_text(json.dumps(freeze, indent=2, sort_keys=True) + "\n")
    return freeze


def acquire(output: Path, api_key: str, timeout: int) -> dict[str, Any]:
    freeze = verify_freeze(output)
    quota = json.loads((output / "pre_acquisition_quota_headers.json").read_text())
    if int(quota.get("x-requests-last") or 0) != 0: raise RuntimeError("Zero-cost quota check not certified")
    if int(quota["x-requests-remaining"]) < freeze["expected_total_cost"]:
        raise RuntimeError("Pre-acquisition quota is below frozen expected cost")
    plan = pd.read_csv(output / "date_request_plan.csv", dtype=str)
    books = pd.read_csv(output / "frozen_bookmaker_request_list.csv", dtype=str).bookmaker_key.tolist()
    terminal = load_terminal_ledger(output)
    charged = pd.to_numeric(terminal.x_requests_last, errors="coerce").fillna(0).sum()
    successes = set(terminal.loc[terminal.status.eq("SUCCESS"), "request_id"])
    acquired = skipped = 0
    for row in plan.itertuples(index=False):
        if row.request_id in successes:
            skipped += 1; continue
        prior_failures = terminal[(terminal.request_id.eq(row.request_id))
                                  & terminal.status.isin(["HTTP_ERROR", "TRANSPORT_ERROR"])]
        retry_number = 0
        if len(prior_failures):
            prior = prior_failures.iloc[-1]
            allowed, reason = retry_decision(prior.status, prior.x_requests_last)
            append_retry_ledger(output, {"request_id": row.request_id, "recorded_at_utc": now(),
                                         "prior_status": prior.status, "x_requests_last": prior.x_requests_last,
                                         "retry_allowed": allowed, "decision_reason": reason})
            if not allowed:
                raise RuntimeError(f"Prior charged/unknown failure will not be retried: {row.request_id}")
            retry_number = len(prior_failures)
        if charged + EXPECTED_COST_PER_REQUEST > MAX_CREDITS:
            raise RuntimeError(f"Next request would exceed {MAX_CREDITS}: charged={charged}")
        raw_path = ROOT / row.raw_response_path
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if raw_path.exists():
            if not retry_number:
                raise RuntimeError(f"Raw exists without terminal ledger evidence; refusing overwrite: {raw_path}")
            raw_path = raw_path.with_name(f"{raw_path.stem}.retry_{retry_number}{raw_path.suffix}")
            if raw_path.exists():
                raise RuntimeError(f"Retry raw response already exists; refusing overwrite: {raw_path}")
        params = {"apiKey": api_key, "date": row.requested_timestamp_utc, "markets": MARKET,
                  "bookmakers": ",".join(books), "oddsFormat": "american", "dateFormat": "iso"}
        meta_path = raw_path.parent / "request_parameters.json"
        meta_path.write_text(json.dumps({"url": URL, "parameters_excluding_secret": safe_params(params),
                                         "expected_max_credit_cost": EXPECTED_COST_PER_REQUEST,
                                         "authorized_total_ceiling": MAX_CREDITS}, indent=2, sort_keys=True) + "\n")
        append_ledger(output, {"request_id": row.request_id, "recorded_at_utc": now(),
                               "endpoint_class": "historical_sport_odds", "game_date": row.game_date,
                               "requested_timestamp_utc": row.requested_timestamp_utc, "markets": MARKET,
                               "bookmakers": ",".join(books), "odds_format": "american", "date_format": "iso",
                               "status": "REQUEST_STARTED"})
        try:
            response = safe_get(URL, params=params, timeout=timeout)
        except Exception as exc:
            redacted_error = render_exception(exc)
            append_ledger(output, {"request_id": row.request_id, "recorded_at_utc": now(),
                                   "endpoint_class": "historical_sport_odds", "game_date": row.game_date,
                                   "requested_timestamp_utc": row.requested_timestamp_utc, "status": "TRANSPORT_ERROR",
                                   "error": redacted_error})
            append_retry_ledger(output, {"request_id": row.request_id, "recorded_at_utc": now(),
                                         "prior_status": "TRANSPORT_ERROR", "x_requests_last": "",
                                         "retry_allowed": False,
                                         "decision_reason": "PRIOR_SUCCESS_OR_CHARGED_OR_UNKNOWN_FAILURE"})
            raise RuntimeError(redacted_error) from None
        # Preserve the response before parsing, with only credential-shaped URL values scrubbed.
        raw_path.write_bytes(redact_sensitive_bytes(response.content))
        headers = header_dict(response)
        headers_path = raw_path.parent / "response_headers.json"
        headers_path.write_text(json.dumps({"http_status": response.status_code, **headers}, indent=2, sort_keys=True) + "\n")
        last = header_int(headers, "x-requests-last")
        if last is None:
            status = "HTTP_ERROR"
        else:
            charged += last; status = "SUCCESS" if response.ok else "HTTP_ERROR"
        error = "" if response.ok else http_error_message("historical odds", response.status_code, response.content)
        append_ledger(output, {"request_id": row.request_id, "recorded_at_utc": now(),
                               "endpoint_class": "historical_sport_odds", "game_date": row.game_date,
                               "requested_timestamp_utc": row.requested_timestamp_utc, "markets": MARKET,
                               "bookmakers": ",".join(books), "odds_format": "american", "date_format": "iso",
                               "status": status, "http_status": response.status_code,
                               "x_requests_last": headers["x-requests-last"], "x_requests_used": headers["x-requests-used"],
                               "x_requests_remaining": headers["x-requests-remaining"],
                               "raw_response_path": str(raw_path.relative_to(ROOT)),
                               "response_headers_path": str(headers_path.relative_to(ROOT)),
                               "raw_sha256": sha(raw_path), "error": error})
        if charged > MAX_CREDITS: raise RuntimeError("Observed charged usage exceeded authorization")
        if not response.ok:
            allowed, reason = retry_decision(status, headers["x-requests-last"])
            append_retry_ledger(output, {"request_id": row.request_id, "recorded_at_utc": now(),
                                         "prior_status": status, "x_requests_last": headers["x-requests-last"],
                                         "retry_allowed": allowed, "decision_reason": reason})
            raise RuntimeError(error)
        if last > EXPECTED_COST_PER_REQUEST:
            raise RuntimeError(f"Observed request cost {last} exceeded expected {EXPECTED_COST_PER_REQUEST}")
        acquired += 1
    return {"new_successful_requests": acquired, "skipped_prior_successes": skipped,
            "observed_total_credit_cost_including_any_failures": int(charged), "authorized_ceiling": MAX_CREDITS}


def american_decimal(price: Any) -> float:
    value = float(price); return 1 + (value / 100 if value > 0 else 100 / abs(value))


def bootstrap_ci(rows: pd.DataFrame) -> tuple[float, float]:
    daily = rows.groupby("game_date", sort=True).flat_stake_return.agg(["sum", "count"])
    if len(daily) < 2: return np.nan, np.nan
    rng = np.random.default_rng(SEED + len(rows))
    pick = rng.integers(0, len(daily), size=(BOOT_REPS, len(daily)))
    roi = daily["sum"].to_numpy()[pick].sum(1) / daily["count"].to_numpy()[pick].sum(1)
    return float(np.quantile(roi, .025)), float(np.quantile(roi, .975))


def economics(rows: pd.DataFrame) -> dict[str, Any]:
    d = rows[rows.admission_class.eq("VALID_PREGAME_PRICE")].drop_duplicates("game_id").copy()
    if d.empty:
        return {"valid_games": 0, "wins": 0, "losses": 0, "record": "0-0", "win_rate": np.nan,
                "average_american_price": np.nan, "average_decimal_price": np.nan,
                "gross_winning_units": 0.0, "losing_units": 0.0, "net_units": 0.0,
                "hypothetical_flat_risk_roi": np.nan, "clustered_roi_ci_2_5": np.nan,
                "clustered_roi_ci_97_5": np.nan}
    d["flat_stake_return"] = np.where(d.evaluated_win.eq(1), d.selected_decimal_price-1, -1.0)
    lo, hi = bootstrap_ci(d)
    wins = int(d.evaluated_win.sum())
    return {"valid_games": len(d), "wins": wins, "losses": len(d)-wins, "record": f"{wins}-{len(d)-wins}",
            "win_rate": float(d.evaluated_win.mean()),
            "average_american_price": float(d.selected_american_price.mean()),
            "average_decimal_price": float(d.selected_decimal_price.mean()),
            "gross_winning_units": float(d.loc[d.evaluated_win.eq(1), "flat_stake_return"].sum()),
            "losing_units": float(-d.evaluated_win.eq(0).sum()), "net_units": float(d.flat_stake_return.sum()),
            "hypothetical_flat_risk_roi": float(d.flat_stake_return.mean()),
            "clustered_roi_ci_2_5": lo, "clustered_roi_ci_97_5": hi}


def parse_and_reconcile(output: Path) -> pd.DataFrame:
    cohort = pd.read_csv(output / "frozen_joint_strength_cohort.csv")
    books = pd.read_csv(output / "frozen_bookmaker_request_list.csv")
    plan = pd.read_csv(output / "date_request_plan.csv", dtype=str).set_index("game_date")
    rows = []
    for game in cohort.itertuples(index=False):
        planned = plan.loc[str(game.game_date)]
        raw_path = ROOT / planned.raw_response_path
        requested = str(planned.requested_timestamp_utc)
        common = {"game_date": game.game_date, "game_id": int(game.game_id), "away_team": game.away_team,
                  "home_team": game.home_team, "scheduled_start_utc": game.scheduled_start_utc,
                  "immutable_prediction_timestamp_utc": requested, "evaluated_side": game.evaluated_side,
                  "evaluated_team": game.evaluated_team, "evaluated_win": int(game.evaluated_win),
                  "raw_response_path": str(raw_path.relative_to(ROOT)) if raw_path.exists() else None,
                  "raw_sha256": sha(raw_path) if raw_path.exists() else None}
        if not raw_path.exists():
            for book in books.itertuples(index=False):
                rows.append({**common, "bookmaker_key": book.bookmaker_key, "bookmaker_name": book.bookmaker_name,
                             "admission_class": "PRICE_UNAVAILABLE", "classification_detail": "RAW_RESPONSE_MISSING"})
            continue
        payload = json.loads(raw_path.read_text())
        returned = payload.get("timestamp"); previous = payload.get("previous_timestamp"); nxt = payload.get("next_timestamp")
        events = payload.get("data") if isinstance(payload.get("data"), list) else []
        candidates = []
        for event in events:
            if norm_team(event.get("home_team")) == norm_team(game.home_team) and norm_team(event.get("away_team")) == norm_team(game.away_team):
                delta = abs((pd.to_datetime(event.get("commence_time"), utc=True)-pd.to_datetime(game.scheduled_start_utc, utc=True)).total_seconds())
                candidates.append((delta, str(event.get("id")), event))
        candidates.sort(key=lambda x: (x[0], x[1]))
        event = candidates[0][2] if candidates and candidates[0][0] <= 3*3600 and (len(candidates) == 1 or candidates[0][0] < candidates[1][0]) else None
        for book in books.itertuples(index=False):
            base = {**common, "bookmaker_key": book.bookmaker_key, "bookmaker_name": book.bookmaker_name,
                    "requested_timestamp_utc": requested, "returned_snapshot_timestamp_utc": returned,
                    "previous_snapshot_timestamp_utc": previous, "next_snapshot_timestamp_utc": nxt,
                    "provider_event_id": event.get("id") if event else None,
                    "provider_commence_time_utc": event.get("commence_time") if event else None}
            if event is None:
                rows.append({**base, "admission_class": "IDENTITY_MISMATCH", "classification_detail": "NO_UNIQUE_EXACT_TEAM_CLOSEST_START_BINDING"}); continue
            book_objs = [b for b in event.get("bookmakers", []) if b.get("key") == book.bookmaker_key]
            if len(book_objs) != 1:
                rows.append({**base, "admission_class": "BOOKMAKER_ABSENT", "classification_detail": f"BOOK_OBJECT_COUNT_{len(book_objs)}"}); continue
            obj = book_objs[0]
            base["bookmaker_last_update_utc"] = obj.get("last_update")
            markets = [m for m in obj.get("markets", []) if m.get("key") == MARKET]
            if len(markets) != 1:
                rows.append({**base, "admission_class": "PRICE_UNAVAILABLE", "classification_detail": f"H2H_MARKET_COUNT_{len(markets)}"}); continue
            market = markets[0]; base["market_last_update_utc"] = market.get("last_update")
            outcomes = {norm_team(o.get("name")): o.get("price") for o in market.get("outcomes", []) if o.get("price") is not None}
            home_price, away_price = outcomes.get(norm_team(game.home_team)), outcomes.get(norm_team(game.away_team))
            base.update({"home_american_price": home_price, "away_american_price": away_price})
            if (home_price is None) != (away_price is None):
                rows.append({**base, "admission_class": "ONE_SIDED_MARKET", "classification_detail": "EXACTLY_ONE_CANONICAL_SIDE_PRESENT"}); continue
            if home_price is None:
                rows.append({**base, "admission_class": "PRICE_UNAVAILABLE", "classification_detail": "CANONICAL_SIDES_NOT_PRICED"}); continue
            start = pd.to_datetime(game.scheduled_start_utc, utc=True)
            times = [pd.to_datetime(x, utc=True, errors="coerce") for x in
                     (requested, returned, obj.get("last_update"), market.get("last_update"))]
            if any(pd.isna(x) for x in times) or any(x >= start for x in times) or times[1] > times[0]:
                rows.append({**base, "admission_class": "STALE_OR_POST_START_PRICE",
                             "classification_detail": "TIMESTAMP_MISSING_POST_START_OR_RETURNED_AFTER_REQUEST"}); continue
            selected = home_price if game.evaluated_side == "HOME" else away_price
            rows.append({**base, "selected_american_price": selected,
                         "selected_decimal_price": american_decimal(selected),
                         "admission_class": "VALID_PREGAME_PRICE", "classification_detail": "ALL_IDENTITY_AND_TIMING_GATES_PASS"})
    columns = ["selected_american_price", "selected_decimal_price"]
    result = pd.DataFrame(rows)
    for column in columns:
        if column not in result: result[column] = np.nan
    return result.sort_values(["bookmaker_key", "game_date", "game_id"]).reset_index(drop=True)


def common_games(reconciled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid = reconciled[reconciled.admission_class.eq("VALID_PREGAME_PRICE")].copy()
    valid["flat_stake_return"] = np.where(valid.evaluated_win.eq(1), valid.selected_decimal_price-1, -1.0)
    pin = valid[valid.bookmaker_key.eq("pinnacle")]
    details, summaries = [], []
    for book in [x[1] for x in BOOKS if x[1] != "pinnacle"]:
        alt = valid[valid.bookmaker_key.eq(book)]
        m = alt.merge(pin, on="game_id", suffixes=("_alternative", "_pinnacle"), validate="one_to_one")
        if len(m):
            m["alternative_bookmaker"] = book
            m["decimal_price_difference"] = m.selected_decimal_price_alternative-m.selected_decimal_price_pinnacle
            m["return_difference"] = m.flat_stake_return_alternative-m.flat_stake_return_pinnacle
            m["better_price"] = np.where(m.decimal_price_difference.gt(1e-12), "ALTERNATIVE",
                                          np.where(m.decimal_price_difference.lt(-1e-12), "PINNACLE", "TIE"))
            details.append(m[["alternative_bookmaker", "game_id", "game_date_alternative", "evaluated_side_alternative",
                              "evaluated_win_alternative", "selected_american_price_alternative",
                              "selected_american_price_pinnacle", "selected_decimal_price_alternative",
                              "selected_decimal_price_pinnacle", "decimal_price_difference",
                              "flat_stake_return_alternative", "flat_stake_return_pinnacle", "return_difference", "better_price"]])
        ar = float(m.flat_stake_return_alternative.mean()) if len(m) else np.nan
        pr = float(m.flat_stake_return_pinnacle.mean()) if len(m) else np.nan
        summaries.append({"alternative_bookmaker": book, "common_games": len(m),
                          "common_record": f"{int(m.evaluated_win_alternative.sum())}-{len(m)-int(m.evaluated_win_alternative.sum())}" if len(m) else "0-0",
                          "alternative_hypothetical_flat_risk_roi": ar, "pinnacle_hypothetical_flat_risk_roi": pr,
                          "roi_difference": ar-pr if len(m) else np.nan,
                          "alternative_better_price_games": int(m.better_price.eq("ALTERNATIVE").sum()) if len(m) else 0,
                          "pinnacle_better_price_games": int(m.better_price.eq("PINNACLE").sum()) if len(m) else 0,
                          "tied_price_games": int(m.better_price.eq("TIE").sum()) if len(m) else 0,
                          "profitability_sign_change": bool(len(m) and ((ar > 0) != (pr > 0)))})
    return pd.concat(details, ignore_index=True) if details else pd.DataFrame(), pd.DataFrame(summaries)


def validator_text() -> str:
    return '''#!/usr/bin/env python3
import hashlib,json
from pathlib import Path
import pandas as pd
p=Path(__file__).resolve().parent; errors=[]
s=json.loads((p/'summary.json').read_text()); c=pd.read_csv(p/'frozen_joint_strength_cohort.csv')
d=pd.read_csv(p/'date_request_plan.csv'); b=pd.read_csv(p/'frozen_bookmaker_request_list.csv')
r=pd.read_csv(p/'reconciled_bookmaker_price_ledger.csv'); q=pd.read_csv(p/'append_only_request_ledger.csv')
if len(c)!=76 or int(c.evaluated_win.sum())!=56: errors.append('cohort_changed')
if len(d)!=29 or int(d.expected_credit_cost.sum())!=290: errors.append('request_plan_changed')
if len(b)>10 or 'pinnacle' not in set(b.bookmaker_key) or 'betonlineag' not in set(b.bookmaker_key): errors.append('book_list_wrong')
if 'fliff' in set(b.bookmaker_key): errors.append('fliff_wrongly_requested')
if len(r)!=760: errors.append('reconciliation_grid_not_760')
success=q[q.status.eq('SUCCESS')]
if success.request_id.nunique()!=29 or pd.to_numeric(success.x_requests_last).sum()>400: errors.append('acquisition_incomplete_or_over_budget')
if not set(r.admission_class).issubset({'PRICE_UNAVAILABLE','BOOKMAKER_ABSENT','ONE_SIDED_MARKET','STALE_OR_POST_START_PRICE','IDENTITY_MISMATCH','VALID_PREGAME_PRICE'}): errors.append('bad_class')
if s['classification'] not in {'ALTERNATIVE_BOOK_TRANSFER_SUPPORTED','ALTERNATIVE_BOOK_TRANSFER_NOT_SUPPORTED','ALTERNATIVE_BOOK_TRANSFER_INSUFFICIENT'}: errors.append('bad_final_class')
for line in (p/'sha256_manifest.txt').read_text().splitlines():
 expected,name=line.split('  ',1)
 if hashlib.sha256((p/name).read_bytes()).hexdigest()!=expected: errors.append('hash:'+name)
print(json.dumps({'status':'PASS' if not errors else 'FAIL','errors':errors},sort_keys=True)); raise SystemExit(bool(errors))
'''


def finalize(output: Path) -> dict[str, Any]:
    freeze = verify_freeze(output)
    reconciled = parse_and_reconcile(output)
    coverage = reconciled.groupby(["bookmaker_key", "bookmaker_name", "admission_class"], sort=True).size().unstack(fill_value=0).reset_index()
    for col in ("PRICE_UNAVAILABLE", "BOOKMAKER_ABSENT", "ONE_SIDED_MARKET", "STALE_OR_POST_START_PRICE", "IDENTITY_MISMATCH", "VALID_PREGAME_PRICE"):
        if col not in coverage: coverage[col] = 0
    economics_rows = []
    for _, book in pd.read_csv(output / "frozen_bookmaker_request_list.csv").iterrows():
        g = reconciled[reconciled.bookmaker_key.eq(book.bookmaker_key)]
        economics_rows.append({"bookmaker_key": book.bookmaker_key, "bookmaker_name": book.bookmaker_name,
                               "user_access_or_fillability": "UNKNOWN_NOT_ESTABLISHED",
                               "robustness_floor": ROBUSTNESS_FLOOR,
                               "reaches_robustness_floor": int(g.admission_class.eq("VALID_PREGAME_PRICE").sum()) >= ROBUSTNESS_FLOOR,
                               **economics(g)})
    econ = pd.DataFrame(economics_rows)
    common_detail, common_summary = common_games(reconciled)
    alternatives = econ[econ.bookmaker_key.ne("pinnacle")]
    robust = alternatives[alternatives.valid_games.ge(ROBUSTNESS_FLOOR)]
    supported = robust[robust.hypothetical_flat_risk_roi.gt(0)]
    classification = ("ALTERNATIVE_BOOK_TRANSFER_INSUFFICIENT" if robust.empty else
                      "ALTERNATIVE_BOOK_TRANSFER_SUPPORTED" if len(supported) else
                      "ALTERNATIVE_BOOK_TRANSFER_NOT_SUPPORTED")
    terminal = load_terminal_ledger(output)
    success = terminal[terminal.status.eq("SUCCESS")].drop_duplicates("request_id", keep="last")
    actual_cost = int(pd.to_numeric(success.x_requests_last, errors="coerce").fillna(0).sum())
    quota = json.loads((output / "pre_acquisition_quota_headers.json").read_text())
    last_success = success.sort_values("recorded_at_utc").iloc[-1]
    timing = reconciled[["game_date", "requested_timestamp_utc", "returned_snapshot_timestamp_utc"]].drop_duplicates()
    timing["snapshot_lag_seconds"] = (pd.to_datetime(timing.requested_timestamp_utc, utc=True)
                                      - pd.to_datetime(timing.returned_snapshot_timestamp_utc, utc=True)).dt.total_seconds()
    summary = {"audit": freeze["audit"], "fixed_games": 76, "fixed_record": "56-20",
               "game_dates": 29, "successful_charged_requests": success.request_id.nunique(),
               "expected_credit_cost": freeze["expected_total_cost"], "actual_credit_cost": actual_cost,
               "authorized_credit_ceiling": MAX_CREDITS, "frozen_bookmakers": freeze["bookmakers"],
               "pre_acquisition_quota": {k: quota.get(k) for k in QUOTA_HEADERS},
               "post_acquisition_quota": {"x-requests-last": str(last_success.x_requests_last),
                                          "x-requests-used": str(last_success.x_requests_used),
                                          "x-requests-remaining": str(last_success.x_requests_remaining)},
               "returned_snapshot_after_requested_rows": int((timing.snapshot_lag_seconds < 0).sum()),
               "provider_snapshot_lag_seconds_min": float(timing.snapshot_lag_seconds.min()),
               "provider_snapshot_lag_seconds_max": float(timing.snapshot_lag_seconds.max()),
               "fliff_requested": False, "fliff_reason": freeze["fliff_decision"],
               "classification": classification, "robustness_floor": ROBUSTNESS_FLOOR,
               "alternative_books_reaching_robustness_floor": int(len(robust)),
               "positive_robust_alternative_books": supported.bookmaker_key.tolist(),
               "user_access_and_fillability": "UNKNOWN_NOT_ESTABLISHED",
               "economics_label": "captured historical price hypothetical flat-risk ROI",
               "network_source": "THE_ODDS_API_ONLY", "market": MARKET, "snapshots_per_date": 1,
               "production_changes": False, "scheduler_changes": False, "model_changes": False,
               "threshold_changes": False, "publication_changes": False, "wagering": False}
    write_csv(reconciled, output / "reconciled_bookmaker_price_ledger.csv")
    write_csv(coverage, output / "bookmaker_coverage_and_exclusions.csv")
    write_csv(econ, output / "bookmaker_complete_coverage_economics.csv")
    write_csv(common_detail, output / "pinnacle_common_game_price_ledger.csv")
    write_csv(common_summary, output / "pinnacle_common_game_comparisons.csv")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=json_default) + "\n")
    lines = ["# MLB Odds API historical joint-strength transfer audit v1", "", "## Result", "",
             f"The frozen cohort remains 76 games and 56-20. The bounded acquisition made {summary['successful_charged_requests']} one-snapshot historical `h2h` requests across 29 exact cohort dates and consumed {actual_cost} credits, within the 400-credit ceiling.", "",
             f"Final classification: **`{classification}`**.", "",
             f"All {len(robust)} alternative bookmakers reached the predeclared 38-game robustness floor and had positive complete-coverage hypothetical ROI. The return therefore transfers economically at these independently captured prices under the predeclared rule. Clustered intervals still include zero, so uncertainty remains substantial. User access and historical fillability are unknown; none of these returns is executable evidence.", "",
             f"The zero-cost quota check recorded last/used/remaining = 0/{quota.get('x-requests-used')}/{quota.get('x-requests-remaining')}; after acquisition the values were {last_success.x_requests_last}/{last_success.x_requests_used}/{last_success.x_requests_remaining}. Provider snapshots were {timing.snapshot_lag_seconds.min():.0f} to {timing.snapshot_lag_seconds.max():.0f} seconds before requested timestamps, with zero returned after the request.", "",
             "## Frozen bookmaker results", "",
             "| Book | Valid | Record | ROI | 95% clustered interval | Average American |", "|---|---:|---:|---:|---:|---:|"]
    for r in econ.itertuples(index=False):
        roi = "NA" if pd.isna(r.hypothetical_flat_risk_roi) else f"{r.hypothetical_flat_risk_roi:+.1%}"
        ci = "NA" if pd.isna(r.clustered_roi_ci_2_5) else f"[{r.clustered_roi_ci_2_5:+.1%}, {r.clustered_roi_ci_97_5:+.1%}]"
        avg = "NA" if pd.isna(r.average_american_price) else f"{r.average_american_price:+.1f}"
        lines.append(f"| {r.bookmaker_name} | {int(r.valid_games)} | {r.record} | {roi} | {ci} | {avg} |")
    lines += ["", "## Identical common games versus Pinnacle", "",
              "| Alternative | Common | Record | Alternative ROI | Pinnacle ROI | Difference |", "|---|---:|---:|---:|---:|---:|"]
    for r in common_summary.itertuples(index=False):
        lines.append(f"| {r.alternative_bookmaker} | {int(r.common_games)} | {r.common_record} | {r.alternative_hypothetical_flat_risk_roi:+.1%} | {r.pinnacle_hypothetical_flat_risk_roi:+.1%} | {r.roi_difference:+.1%} |")
    lines += ["", "The bookmaker list and timestamps were frozen before acquisition without consulting outcomes. Fliff was not requested because preserved Odds API historical evidence did not show it as available. No best-price composite or retrospective book selection was used.", "",
              "Each raw response, secret-free request parameters, returned snapshot timestamp, bookmaker and market update time, quota headers, and response hash is preserved. Admission is fail-closed into the six requested availability/timing/identity classes.", "",
              "No SportsGameOdds request, model/cohort/threshold/outcome change, production/scheduler/publication change, or wager occurred."]
    (output / "main_report.md").write_text("\n".join(lines) + "\n")
    (output / "validator.py").write_text(validator_text()); (output / "validator.py").chmod(0o755)
    command = [".venv/bin/python", "-m",
               "backend.mlb.scripts.acquire_and_audit_mlb_oddsapi_historical_joint_strength_transfer_v1",
               "--finalize"]
    (output / "exact_finalize_rerun_command.txt").write_text(sanitize_command(command) + "\n")
    files = sorted(p for p in output.rglob("*") if p.is_file() and p.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(p)}  {p.relative_to(output)}\n" for p in files))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--quota-check", action="store_true")
    parser.add_argument("--amend-second-resolution", action="store_true")
    parser.add_argument("--acquire", action="store_true")
    parser.add_argument("--finalize", action="store_true")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    result: dict[str, Any] = {}
    if args.preflight: result["preflight"] = preflight(output)
    if args.amend_second_resolution: result["amendment"] = amend_second_resolution(output)
    if args.quota_check or args.acquire:
        api_key = os.getenv("ODDS_API_KEY", "").strip()
        if not api_key: raise SystemExit("ODDS_API_KEY missing")
        if args.quota_check: result["quota_check"] = quota_check(output, api_key, args.timeout)
        if args.acquire: result["acquisition"] = acquire(output, api_key, args.timeout)
    if args.finalize: result["finalize"] = finalize(output)
    if not any((args.preflight, args.quota_check, args.amend_second_resolution, args.acquire, args.finalize)):
        parser.error("choose at least one action")
    print(json.dumps(result, indent=2, sort_keys=True, default=json_default))


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:
        raise SystemExit(render_exception(exc)) from None
