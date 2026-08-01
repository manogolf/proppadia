#!/usr/bin/env python3
"""Pure prospective-lineage certification predicates."""
from __future__ import annotations
import hashlib
from datetime import datetime
from pathlib import Path

def observation_admissible(observed: datetime | None, governing_capture: datetime | None,
                           first_pitch: datetime | None, ingestion_completed: datetime | None,
                           governing_run_started: datetime | None) -> bool:
    return bool(observed is not None and governing_capture is not None
                and first_pitch is not None and ingestion_completed is not None
                and governing_run_started is not None
                and observed <= governing_capture and observed < first_pitch
                and ingestion_completed <= governing_run_started)

def payload_hash_certified(path: Path | None, expected: str | None) -> bool:
    return bool(path and expected and path.is_file()
                and hashlib.sha256(path.read_bytes()).hexdigest() == expected)

def exact_identity_certified(*, game_pk: int | None, player_mlb_id: int | None,
                             event_candidate_count: int, normalized_candidate_count: int) -> bool:
    return bool(game_pk and player_mlb_id and event_candidate_count == 1
                and normalized_candidate_count == 1)

def total_bases(singles: int, doubles: int, triples: int, home_runs: int) -> int:
    return singles + 2*doubles + 3*triples + 4*home_runs
