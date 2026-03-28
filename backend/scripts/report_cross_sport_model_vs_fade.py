#!/usr/bin/env python3
"""Cross-sport guardrail: model vs fade summary for NHL + MLB."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _to_int(v: Any) -> int | None:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _to_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _eval_sport(
    *,
    sport: str,
    payload: dict[str, Any] | None,
    min_bets: int,
    max_fade_minus_model_delta: float,
    required: bool,
) -> dict[str, Any]:
    if payload is None:
        status = "missing" if required else "skip_missing"
        return {
            "sport": sport,
            "status": status,
            "required": bool(required),
            "bets": None,
            "model_roi": None,
            "fade_roi": None,
            "delta_fade_minus_model": None,
            "reason": "summary_missing_or_invalid",
        }

    overall = payload.get("overall") or {}
    if sport.lower() == "mlb":
        bets = _to_int(overall.get("paired_bets") if overall.get("paired_bets") is not None else overall.get("model_bets"))
        model_roi = _to_float(overall.get("model_roi_1u"))
        fade_roi = _to_float(overall.get("fade_roi_1u"))
        delta = _to_float(overall.get("delta_fade_minus_model_1u"))
    else:
        bets = _to_int(overall.get("model_bets"))
        model_roi = _to_float(overall.get("model_roi"))
        fade_roi = _to_float(overall.get("fade_roi"))
        delta = _to_float(overall.get("delta_fade_minus_model"))

    if bets is None or delta is None:
        return {
            "sport": sport,
            "status": "invalid",
            "required": bool(required),
            "bets": bets,
            "model_roi": model_roi,
            "fade_roi": fade_roi,
            "delta_fade_minus_model": delta,
            "reason": "missing_bets_or_delta",
        }

    if bets < int(min_bets):
        return {
            "sport": sport,
            "status": "insufficient",
            "required": bool(required),
            "bets": int(bets),
            "model_roi": model_roi,
            "fade_roi": fade_roi,
            "delta_fade_minus_model": delta,
            "reason": f"bets<{int(min_bets)}",
        }

    if delta > float(max_fade_minus_model_delta):
        return {
            "sport": sport,
            "status": "fail",
            "required": bool(required),
            "bets": int(bets),
            "model_roi": model_roi,
            "fade_roi": fade_roi,
            "delta_fade_minus_model": delta,
            "reason": f"fade_minus_model_delta>{float(max_fade_minus_model_delta):.6f}",
        }

    return {
        "sport": sport,
        "status": "pass",
        "required": bool(required),
        "bets": int(bets),
        "model_roi": model_roi,
        "fade_roi": fade_roi,
        "delta_fade_minus_model": delta,
        "reason": "ok",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--nhl-json", default="tmp/analysis/nhl_model_vs_fade_summary.json")
    ap.add_argument("--mlb-json", default="tmp/analysis/mlb_model_vs_fade_summary.json")
    ap.add_argument("--nhl-min-bets", type=int, default=20)
    ap.add_argument("--mlb-min-bets", type=int, default=30)
    ap.add_argument("--max-fade-minus-model-delta", type=float, default=0.0)
    ap.add_argument("--require-nhl", action="store_true")
    ap.add_argument("--require-mlb", action="store_true")
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--out-json", default="tmp/analysis/cross_sport_model_vs_fade_summary.json")
    args = ap.parse_args()

    nhl_payload = _load_json(Path(args.nhl_json).expanduser())
    mlb_payload = _load_json(Path(args.mlb_json).expanduser())

    sports = [
        _eval_sport(
            sport="nhl",
            payload=nhl_payload,
            min_bets=int(args.nhl_min_bets),
            max_fade_minus_model_delta=float(args.max_fade_minus_model_delta),
            required=bool(args.require_nhl),
        ),
        _eval_sport(
            sport="mlb",
            payload=mlb_payload,
            min_bets=int(args.mlb_min_bets),
            max_fade_minus_model_delta=float(args.max_fade_minus_model_delta),
            required=bool(args.require_mlb),
        ),
    ]

    failures = [
        s for s in sports if s["status"] in {"fail", "invalid"} or (s["status"] == "missing" and bool(s.get("required")))
    ]
    status = "fail" if failures else "pass"
    payload = {
        "status": status,
        "ok": status == "pass",
        "inputs": {
            "nhl_json": str(Path(args.nhl_json).expanduser()),
            "mlb_json": str(Path(args.mlb_json).expanduser()),
            "nhl_min_bets": int(args.nhl_min_bets),
            "mlb_min_bets": int(args.mlb_min_bets),
            "max_fade_minus_model_delta": float(args.max_fade_minus_model_delta),
            "require_nhl": bool(args.require_nhl),
            "require_mlb": bool(args.require_mlb),
        },
        "sports": sports,
        "failures": failures,
    }

    out_json = Path(args.out_json).expanduser()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))

    if args.strict and status != "pass":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
