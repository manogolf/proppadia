#!/usr/bin/env python3
"""Append MLB prod12 phase-2 readiness summary to JSONL history."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from backend.scripts import mlb_candidate_eval
from backend.shared.scripts.json_check_runner import run_json_check


def _load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {"ok": False, "status": "fail", "error": f"missing_file:{path}"}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"ok": False, "status": "fail", "error": f"invalid_json:{path}:{type(exc).__name__}:{exc}"}
    if not isinstance(payload, dict):
        return {"ok": False, "status": "fail", "error": f"invalid_payload_type:{path}"}
    return payload


def collect_phase2_snapshot(
    *,
    manifest_path: Path,
    replay_path: Path,
    baseline_path: str,
    baseline_dir: str,
    source_table: str,
    window_mode: str,
    window_days: int,
    games_back: int,
    prop_types: str,
    required_props: str,
    min_candidate_total: int,
    min_overall_lift_pct: float,
    max_prop_drop_pct: float,
) -> dict[str, Any]:
    manifest_payload = _load_json_file(manifest_path)
    replay_payload = _load_json_file(replay_path)

    candidate_args: list[str] = [
        "--baseline-path",
        str(baseline_path),
        "--baseline-dir",
        str(baseline_dir),
        "--source-table",
        str(source_table),
        "--window-days",
        str(int(window_days)),
        "--games-back",
        str(int(games_back)),
        "--prop-types",
        str(prop_types),
        "--required-props",
        str(required_props),
        "--min-candidate-total",
        str(int(min_candidate_total)),
        "--min-overall-lift-pct",
        str(float(min_overall_lift_pct)),
        "--max-prop-drop-pct",
        str(float(max_prop_drop_pct)),
    ]
    if str(window_mode).strip():
        candidate_args.extend(["--window-mode", str(window_mode).strip()])

    candidate_rc, candidate_payload = run_json_check(mlb_candidate_eval.main, candidate_args)

    checks = {
        "release_manifest": {
            "ok": bool(manifest_payload.get("ok")),
            "status": manifest_payload.get("status"),
            "payload": manifest_payload,
        },
        "replay_latency": {
            "ok": bool(replay_payload.get("ok")),
            "status": replay_payload.get("status"),
            "payload": replay_payload,
        },
        "candidate_eval": {
            "ok": bool(candidate_payload.get("ok")),
            "status": candidate_payload.get("status"),
            "exit_code": int(candidate_rc),
            "payload": candidate_payload,
        },
    }

    failures = [name for name, check in checks.items() if not bool(check.get("ok"))]
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ok": len(failures) == 0,
        "status": "pass" if len(failures) == 0 else "fail",
        "failures": failures,
        "checks": checks,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Append prod12 phase-2 readiness summary to JSONL history.")
    ap.add_argument("--output", default="artifacts/mlb_prod12_phase2_history.jsonl")
    ap.add_argument("--manifest-path", default="artifacts/releases/mlb_prod12_release_manifest.json")
    ap.add_argument("--replay-path", default="artifacts/releases/mlb_prod12_replay_latency.json")
    ap.add_argument("--baseline-path", default="")
    ap.add_argument("--baseline-dir", default="artifacts/season_baselines")
    ap.add_argument("--source-table", choices=["player_props", "model_training_props"], default="model_training_props")
    ap.add_argument("--window-mode", choices=["", "days", "games"], default="")
    ap.add_argument("--window-days", type=int, default=120)
    ap.add_argument("--games-back", type=int, default=30)
    ap.add_argument("--prop-types", default="")
    ap.add_argument("--required-props", default="")
    ap.add_argument("--min-candidate-total", type=int, default=-1)
    ap.add_argument("--min-overall-lift-pct", type=float, default=0.25)
    ap.add_argument("--max-prop-drop-pct", type=float, default=3.5)
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    payload = collect_phase2_snapshot(
        manifest_path=Path(args.manifest_path),
        replay_path=Path(args.replay_path),
        baseline_path=str(args.baseline_path),
        baseline_dir=str(args.baseline_dir),
        source_table=str(args.source_table),
        window_mode=str(args.window_mode),
        window_days=int(args.window_days),
        games_back=int(args.games_back),
        prop_types=str(args.prop_types),
        required_props=str(args.required_props),
        min_candidate_total=int(args.min_candidate_total),
        min_overall_lift_pct=float(args.min_overall_lift_pct),
        max_prop_drop_pct=float(args.max_prop_drop_pct),
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, default=str))
        fh.write("\n")

    summary = {
        "captured_at": payload.get("captured_at"),
        "status": payload.get("status"),
        "ok": payload.get("ok"),
        "output": str(out_path),
        "failures": payload.get("failures") or [],
    }
    print(json.dumps(summary, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
