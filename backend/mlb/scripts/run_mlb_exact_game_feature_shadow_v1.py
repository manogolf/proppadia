#!/usr/bin/env python3
"""Run the prospective exact-game feature shadow writer once for a wrapper run."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from backend.mlb.prospective_exact_game_feature_shadow_writer_v1 import (
    OUTPUT_ROOT,
    ShadowWriterError,
    build_shadow,
    load_read_only_snapshot,
    publish_create_only,
    sha256_path,
    verify_package,
)
from backend.mlb.season_transition.game_phase_authority_v1 import HashedProposalAuthority


ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = ROOT / "backend/mlb/contracts/mlb_2026_prospective_exact_game_feature_shadow_writer_v1.json"


def _repo_path(value: object, code: str) -> Path:
    raw = str(value or "")
    if not raw or raw.startswith("/") or ".." in Path(raw).parts:
        raise ShadowWriterError(code, raw)
    path = (ROOT / raw).resolve()
    try:
        path.relative_to(ROOT)
    except ValueError:
        raise ShadowWriterError(code, raw) from None
    if not path.is_file():
        raise ShadowWriterError(code, raw)
    return path


def _schedule_source(slate_date: str, wrapper_started_at_utc: str) -> tuple[Path, dict[str, object]]:
    directory = ROOT / "artifacts/ops/mlb_public_game_moneyline_history_schedules" / slate_date
    started = wrapper_started_at_utc.replace("Z", "+00:00")
    from datetime import datetime, timezone
    threshold = datetime.fromisoformat(started).astimezone(timezone.utc)
    matches: list[tuple[Path, dict[str, object]]] = []
    for selection_path in sorted(directory.glob("*.selection.json")) if directory.is_dir() else []:
        selection = json.loads(selection_path.read_text())
        source = dict(selection.get("history_schedule") or {})
        try:
            retrieved = datetime.fromisoformat(str(source.get("retrieved_at_utc") or "").replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            continue
        horizon = source.get("date_horizon") or {}
        if retrieved >= threshold and str(horizon.get("end_date")) == slate_date:
            matches.append((selection_path, source))
    if len(matches) != 1:
        raise ShadowWriterError("CURRENT_RUN_IMMUTABLE_SCHEDULE_SOURCE_NOT_UNIQUE", f"count={len(matches)}")
    selection_path, source = matches[0]
    schedule = _repo_path(source.get("source_path"), "IMMUTABLE_SCHEDULE_SOURCE_MISSING")
    actual = sha256_path(schedule)
    if actual != source.get("source_sha256"):
        raise ShadowWriterError("IMMUTABLE_SCHEDULE_SOURCE_HASH_MISMATCH")
    selection_sha256 = sha256_path(selection_path)
    source["selection_receipt_path"] = str(selection_path.relative_to(ROOT))
    source["selection_receipt_sha256"] = selection_sha256
    return schedule, source


def _commit() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def _existing_run_status(
    *,
    output: Path,
    slate_date: str,
    run_identity: str,
    wrapper_started_at_utc: str,
    code_commit: str,
    contract_sha256: str,
) -> str | None:
    if not output.exists():
        return None
    if not output.is_dir() or verify_package(output)["status"] != "PASS":
        raise ShadowWriterError("IMMUTABLE_RUN_IDENTITY_CONFLICT", str(output))
    receipt = json.loads((output / "snapshot_receipt.json").read_text())
    expected = {
        "slate_date": slate_date,
        "run_identity": run_identity,
        "wrapper_started_at_utc": wrapper_started_at_utc.replace("+00:00", "Z"),
        "code_commit": code_commit,
        "contract_sha256": contract_sha256,
    }
    if any(str(receipt.get(key)) != str(value) for key, value in expected.items()):
        raise ShadowWriterError("IMMUTABLE_RUN_IDENTITY_CONFLICT", str(output))
    retained = receipt.get("schedule_source") or {}
    for path_key, hash_key in (("source_path", "source_sha256"), ("selection_receipt_path", "selection_receipt_sha256")):
        path = _repo_path(retained.get(path_key), "IMMUTABLE_EXISTING_SOURCE_MISSING")
        if sha256_path(path) != retained.get(hash_key):
            raise ShadowWriterError("IMMUTABLE_RUN_IDENTITY_CONFLICT", f"{output}:{path_key}")
    return "SHADOW_SNAPSHOT_ALREADY_EXISTS_IDENTICAL"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", required=True)
    parser.add_argument("--run-identity", required=True)
    parser.add_argument("--wrapper-started-at-utc", required=True)
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    args = parser.parse_args()
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        print("EXACT_GAME_SHADOW_DATABASE_URL_MISSING", file=sys.stderr)
        return 78
    try:
        code_commit = _commit()
        contract_sha256 = sha256_path(CONTRACT_PATH)
        output = args.output_root / args.slate_date / args.run_identity
        existing = _existing_run_status(
            output=output,
            slate_date=args.slate_date,
            run_identity=args.run_identity,
            wrapper_started_at_utc=args.wrapper_started_at_utc,
            code_commit=code_commit,
            contract_sha256=contract_sha256,
        )
        if existing:
            print(f"{existing} slate_date={args.slate_date} run_identity={args.run_identity} output={output}")
            return 0
        schedule_path, source = _schedule_source(args.slate_date, args.wrapper_started_at_utc)
        snapshot = load_read_only_snapshot(dsn)
        result = build_shadow(
            slate_date=args.slate_date,
            run_identity=args.run_identity,
            wrapper_started_at_utc=args.wrapper_started_at_utc,
            snapshot=snapshot,
            schedule_payload=json.loads(schedule_path.read_text()),
            schedule_source=source,
            phase_authority=HashedProposalAuthority(),
            code_commit=code_commit,
            contract_sha256=contract_sha256,
            interpreter=str(Path(sys.executable).resolve()),
        )
        status, output = publish_create_only(
            root=args.output_root,
            slate_date=args.slate_date,
            run_identity=args.run_identity,
            result=result,
        )
        validation = verify_package(output)
        if validation["status"] != "PASS":
            raise ShadowWriterError("SHADOW_PACKAGE_MANIFEST_FAILED", ",".join(validation["failures"]))
        print(
            f"{status} slate_date={args.slate_date} run_identity={args.run_identity} "
            f"admitted={len(result.admitted)} rejected={len(result.rejected)} output={output}"
        )
        return 0
    except (ShadowWriterError, OSError, ValueError, json.JSONDecodeError) as exc:
        code = getattr(exc, "code", type(exc).__name__)
        print(f"EXACT_GAME_SHADOW_FAILED code={code} detail={exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
