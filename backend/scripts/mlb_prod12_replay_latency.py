#!/usr/bin/env python3
"""Measure prepare/predict replay latency for prod12 props on a historical date."""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.scripts.api_client_utils import ClientAdapter, HttpClient, InProcessClient, safe_json

DEFAULT_PROP_TYPES = (
    "hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,"
    "strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis"
)
FALLBACK_PLAYER_IDS: tuple[int, ...] = (660271, 592450, 545361)


def _split_csv(raw: str) -> list[str]:
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def _obj(resp) -> dict[str, Any]:
    body = safe_json(resp)
    return body if isinstance(body, dict) else {"_raw": str(body)}


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return round(values[0], 2)
    ranked = sorted(values)
    pos = (len(ranked) - 1) * (pct / 100.0)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return round(ranked[lo], 2)
    frac = pos - lo
    return round((ranked[lo] * (1 - frac)) + (ranked[hi] * frac), 2)


def _stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "avg_ms": None, "p50_ms": None, "p95_ms": None, "max_ms": None}
    return {
        "count": len(values),
        "avg_ms": round(sum(values) / len(values), 2),
        "p50_ms": _percentile(values, 50),
        "p95_ms": _percentile(values, 95),
        "max_ms": round(max(values), 2),
    }


def _load_players(client: ClientAdapter, limit: int) -> list[dict[str, int]]:
    resp = client.request("GET", "/api/players", params={"limit": int(limit)})
    body = safe_json(resp)
    out: list[dict[str, int]] = []
    if resp.status_code == 200 and isinstance(body, list):
        for row in body:
            if not isinstance(row, dict):
                continue
            pid = row.get("player_id")
            tid = row.get("team_id")
            if pid is None or tid is None:
                continue
            try:
                out.append({"player_id": int(pid), "team_id": int(tid)})
            except Exception:
                continue
    if out:
        return out[:limit]
    for pid in FALLBACK_PLAYER_IDS:
        if len(out) >= int(limit):
            break
        lookup_resp = client.request("GET", "/api/players/lookup", params={"player_id": int(pid)})
        lookup = safe_json(lookup_resp)
        if lookup_resp.status_code != 200 or not isinstance(lookup, dict):
            continue
        if not lookup.get("ok") or not lookup.get("found"):
            continue
        if lookup.get("team_id") is None or lookup.get("player_id") is None:
            continue
        try:
            out.append({"player_id": int(lookup["player_id"]), "team_id": int(lookup["team_id"])})
        except Exception:
            continue
    return out[:limit]


def collect_latency(
    client: ClientAdapter,
    *,
    game_date: str,
    sample_size: int,
    require_min_success: int,
    prop_types: list[str],
    max_predict_p95_ms: float,
    allow_sparse: bool,
    retry_attempts: int,
    retry_backoff_ms: int,
) -> dict[str, Any]:
    players = _load_players(client, max(1, int(sample_size)))
    chosen_props = prop_types or _split_csv(DEFAULT_PROP_TYPES)

    prepare_ms: list[float] = []
    predict_ms: list[float] = []
    per_prop: dict[str, dict[str, Any]] = {
        prop: {
            "attempts": 0,
            "prepare_success": 0,
            "predict_success": 0,
            "prepare_ms": [],
            "predict_ms": [],
            "fallback_predict_count": 0,
            "failure_count": 0,
        }
        for prop in chosen_props
    }
    failures: list[dict[str, Any]] = []
    transport_retry_count = 0
    transport_error_count = 0
    predict_success = 0
    prepare_success = 0

    def _request_with_retry(method: str, path: str, *, json_payload: dict[str, Any]) -> tuple[Any | None, int]:
        nonlocal transport_retry_count, transport_error_count
        attempts = max(1, int(retry_attempts))
        last_resp = None
        tries = 0
        while tries < attempts:
            tries += 1
            try:
                resp = client.request(method, path, json=json_payload)
            except Exception:
                transport_error_count += 1
                if tries < attempts:
                    transport_retry_count += 1
                    time.sleep(max(0, int(retry_backoff_ms)) / 1000.0)
                    continue
                return None, tries

            last_resp = resp
            if int(getattr(resp, "status_code", 0)) >= 500 and tries < attempts:
                transport_retry_count += 1
                time.sleep(max(0, int(retry_backoff_ms)) / 1000.0)
                continue
            return resp, tries
        return last_resp, tries

    for row in players:
        for prop_type in chosen_props:
            lane = per_prop[prop_type]
            lane["attempts"] += 1
            prep_req = {
                "player_id": int(row["player_id"]),
                "team_id": int(row["team_id"]),
                "game_date": game_date,
                "prop_type": prop_type,
                "prop_value": 1.5,
                "over_under": "over",
            }

            t0 = time.perf_counter()
            prep_resp, _ = _request_with_retry("POST", "/api/prepareProp", json_payload=prep_req)
            prep_t = (time.perf_counter() - t0) * 1000.0
            prepare_ms.append(prep_t)
            lane["prepare_ms"].append(prep_t)
            if prep_resp is None:
                lane["failure_count"] += 1
                failures.append(
                    {
                        "player_id": row["player_id"],
                        "prop_type": prop_type,
                        "stage": "prepareProp",
                        "status": 0,
                        "detail": {"error": "transport_failure"},
                    }
                )
                continue
            prep_body = _obj(prep_resp)
            if prep_resp.status_code != 200 or not prep_body.get("ok"):
                lane["failure_count"] += 1
                failures.append(
                    {
                        "player_id": row["player_id"],
                        "prop_type": prop_type,
                        "stage": "prepareProp",
                        "status": prep_resp.status_code,
                        "detail": prep_body,
                    }
                )
                continue

            prepare_success += 1
            lane["prepare_success"] += 1
            features = prep_body.get("features") or {}
            pred_req = {"prop_type": prop_type, "features": features}

            t1 = time.perf_counter()
            pred_resp, _ = _request_with_retry("POST", "/api/predict", json_payload=pred_req)
            pred_t = (time.perf_counter() - t1) * 1000.0
            if pred_resp is None:
                lane["failure_count"] += 1
                failures.append(
                    {
                        "player_id": row["player_id"],
                        "prop_type": prop_type,
                        "stage": "predict",
                        "status": 0,
                        "detail": {"error": "transport_failure"},
                    }
                )
                continue
            predict_ms.append(pred_t)
            lane["predict_ms"].append(pred_t)
            pred_body = _obj(pred_resp)
            token = pred_body.get("commit_token")
            if pred_resp.status_code != 200 or not token:
                lane["failure_count"] += 1
                failures.append(
                    {
                        "player_id": row["player_id"],
                        "prop_type": prop_type,
                        "stage": "predict",
                        "status": pred_resp.status_code,
                        "detail": pred_body,
                    }
                )
                continue

            predict_success += 1
            lane["predict_success"] += 1
            if str(pred_body.get("model") or "") == "heuristic_fallback_v1":
                lane["fallback_predict_count"] += 1

    per_prop_summary: dict[str, dict[str, Any]] = {}
    for prop_type, lane in per_prop.items():
        per_prop_summary[prop_type] = {
            "attempts": lane["attempts"],
            "prepare_success": lane["prepare_success"],
            "predict_success": lane["predict_success"],
            "failure_count": lane["failure_count"],
            "fallback_predict_count": lane["fallback_predict_count"],
            "prepare_latency": _stats([float(v) for v in lane["prepare_ms"]]),
            "predict_latency": _stats([float(v) for v in lane["predict_ms"]]),
        }

    predict_p95 = _stats(predict_ms).get("p95_ms")
    require_success = max(0, int(require_min_success))
    warnings: list[str] = []
    if not players:
        warnings.append("no_players_loaded")
    if (len(players) * len(chosen_props)) == 0:
        if allow_sparse:
            ok = True
            warnings.append("sparse_window_no_attempts_allowed")
        else:
            ok = False
    else:
        ok = bool(predict_success >= require_success and predict_p95 is not None and float(predict_p95) <= float(max_predict_p95_ms))

    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ok": ok,
        "status": "pass" if ok else "fail",
        "game_date": game_date,
        "sample_requested": int(sample_size),
        "sample_loaded": len(players),
        "prop_types": chosen_props,
        "require_min_success": require_success,
        "max_predict_p95_ms": float(max_predict_p95_ms),
        "attempts": int(len(players) * len(chosen_props)),
        "prepare_success": prepare_success,
        "predict_success": predict_success,
        "summary_latency": {
            "prepare": _stats(prepare_ms),
            "predict": _stats(predict_ms),
        },
        "transport": {
            "retry_attempts": int(retry_attempts),
            "retry_backoff_ms": int(retry_backoff_ms),
            "retry_count": int(transport_retry_count),
            "error_count": int(transport_error_count),
        },
        "allow_sparse": bool(allow_sparse),
        "per_prop": per_prop_summary,
        "failure_count": len(failures),
        "failures": failures[:10],
        "warnings": warnings,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Historical replay latency report for MLB prod12 props.")
    ap.add_argument("--base-url", default=None, help="Use running backend URL instead of in-process app.")
    ap.add_argument("--date", default="2025-08-15")
    ap.add_argument("--sample-size", type=int, default=10)
    ap.add_argument("--require-min-success", type=int, default=3)
    ap.add_argument("--prop-types", default=DEFAULT_PROP_TYPES)
    ap.add_argument("--max-predict-p95-ms", type=float, default=4000.0)
    ap.add_argument("--allow-sparse", action="store_true", help="Allow pass when no players/attempts are available.")
    ap.add_argument("--retry-attempts", type=int, default=2, help="Transient transport/5xx retry attempts per request.")
    ap.add_argument("--retry-backoff-ms", type=int, default=350, help="Backoff between retry attempts.")
    ap.add_argument("--output", default="")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    prop_types = _split_csv(args.prop_types)
    client: ClientAdapter = HttpClient(args.base_url) if args.base_url else InProcessClient()
    payload = collect_latency(
        client,
        game_date=str(args.date),
        sample_size=int(args.sample_size),
        require_min_success=int(args.require_min_success),
        prop_types=prop_types,
        max_predict_p95_ms=float(args.max_predict_p95_ms),
        allow_sparse=bool(args.allow_sparse),
        retry_attempts=int(args.retry_attempts),
        retry_backoff_ms=int(args.retry_backoff_ms),
    )

    if str(args.output).strip():
        out = Path(str(args.output).strip())
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
