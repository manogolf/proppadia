#!/usr/bin/env python3
"""Operator CLI for the MLB phase-authority DB chain.

--verify     read the chain (restricted ingest login) and verify it fully.
--publish    re-publish games from a retained selection receipt, for example
             after a failed daily publication.
Both --verify and --publish use the restricted ingest login (MLB_INGEST_DATABASE_URL).
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import backend.supabase.supabase_utils  # noqa: F401 - loads backend/.env
from backend.mlb.season_transition.game_phase_authority_v1 import HashedProposalAuthority
from backend.mlb.season_transition.phase_authority_db_chain_v1 import (
    PhaseChainError,
    fetch_chain_rows,
    ingest_connect,
    load_config,
    publish_runtime_records,
    verify_chain,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import REPO_ROOT
from backend.mlb.season_transition.runtime_schedule_authority_v1 import RuntimeScheduleAuthority


def _verify() -> dict:
    config = load_config()
    with ingest_connect() as conn:
        rows = fetch_chain_rows(conn, str(config["chain_id"]))
    chain = verify_chain(rows, config=config, base=HashedProposalAuthority())
    return {"status": "DB_CHAIN_VERIFIED", "entry_count": chain.entry_count,
            "head_entry_sha256": chain.head_sha256, "game_pks": sorted(chain.game_pks),
            "extension_game_pks": sorted(chain.records)}


def _publish(receipt_path: Path, game_pks: list[int]) -> dict:
    raw = receipt_path.read_bytes()
    receipt = json.loads(raw)
    schedule = receipt.get("history_schedule") or {}
    runtime = RuntimeScheduleAuthority(
        source_path=REPO_ROOT / str(schedule["source_path"]),
        expected_source_sha256=str(schedule["source_sha256"]),
        base=HashedProposalAuthority(),
    )
    accepted = [int(item["game_pk"]) for item in
                (receipt.get("phase_authority_binding") or {}).get("decisions") or []
                if item.get("authority_decision") == "EXACT_GAME_AUTHORITY_ACCEPTED"]
    selected = game_pks or [pk for pk in accepted if runtime.is_runtime_exact(pk)]
    if set(selected) - set(accepted):
        raise PhaseChainError("DB_CHAIN_GAME_NOT_ACCEPTED_IN_RECEIPT")
    return publish_runtime_records(
        runtime, selected, selection_receipt_sha256=hashlib.sha256(raw).hexdigest())


def main() -> int:
    parser = argparse.ArgumentParser()
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--verify", action="store_true")
    actions.add_argument("--publish", type=Path, metavar="SELECTION_RECEIPT")
    parser.add_argument("--game-pk", type=int, action="append", default=[])
    args = parser.parse_args()
    try:
        result = _verify() if args.verify else _publish(args.publish, args.game_pk)
    except Exception as exc:
        print(json.dumps({"status": "BLOCKED", "error": getattr(exc, "code", type(exc).__name__)}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
