#!/usr/bin/env python3
"""Build local-only retained identity reconciliation for the V1 hardening."""
from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
PRIOR = ROOT / "docs/contracts/mlb_2026_postseason_collection_identity_readiness_audit_v1"
SOURCE_MANIFEST = ROOT / (
    "docs/contracts/mlb_2026_canonical_phase_source_completion_v1/"
    "canonical_backfill_proposal/retained_source_manifest.jsonl"
)
PROPOSAL = ROOT / (
    "docs/contracts/mlb_2026_canonical_phase_source_completion_v1/"
    "canonical_backfill_proposal/canonical_game_phase_backfill_proposal.jsonl"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normalize_team(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def instant(value: object):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else None
    except (TypeError, ValueError):
        return None


def write_csv(name: str, rows: list[dict], fields: list[str]) -> None:
    with (OUT / name).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def load_authority() -> set[int]:
    return {
        int(json.loads(line)["game_pk"])
        for line in PROPOSAL.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def schedule_index(authority: set[int]):
    index = defaultdict(list)
    verified = 0
    for line in SOURCE_MANIFEST.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        path = ROOT / row["source_path"]
        if sha256(path) != row["source_sha256"]:
            raise RuntimeError(f"schedule hash mismatch: {row['source_path']}")
        verified += 1
        payload = json.loads(path.read_text(encoding="utf-8"))
        for day in payload.get("dates", []):
            for game in day.get("games", []):
                game_pk = int(game["gamePk"])
                if game_pk not in authority:
                    continue
                teams = game.get("teams", {})
                away = normalize_team((teams.get("away", {}).get("team") or {}).get("name"))
                home = normalize_team((teams.get("home", {}).get("team") or {}).get("name"))
                index[(away, home)].append({
                    "game_pk": game_pk,
                    "start": game.get("gameDate"),
                    "game_number": game.get("gameNumber"),
                    "source_path": row["source_path"],
                    "source_sha256": row["source_sha256"],
                })
    return index, verified


def raw_events(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("events", payload.get("data", []))
    return payload if isinstance(payload, list) else []


def player_reconciliation(index) -> list[dict]:
    state = defaultdict(lambda: {
        "lane": "GENERAL_PLAYER_PROP_SNAPSHOT", "mappings": set(), "candidates": set(),
        "paths": set(), "hashes": set(), "observations": 0, "matched": 0,
        "ambiguous_observations": 0,
    })
    for path in sorted((ROOT / "backend/mlb/exports/odds_history").glob("2026-*/odds_mlb_playerprops*.json")):
        source_hash = sha256(path)
        rel = str(path.relative_to(ROOT))
        for event in raw_events(path):
            event_id = str(event.get("id") or "").strip()
            if not event_id:
                continue
            entry = state[event_id]
            entry["observations"] += 1
            entry["paths"].add(rel)
            entry["hashes"].add(source_hash)
            if any(str(book.get("key") or "").lower() == "betonlineag" for book in event.get("bookmakers", [])):
                entry["lane"] = "BETONLINE_PLAYER_PROPS"
            candidates = index.get((normalize_team(event.get("away_team")), normalize_team(event.get("home_team"))), [])
            commence = instant(event.get("commence_time"))
            game_number = event.get("game_number") or event.get("gameNumber")
            selected = set()
            for candidate in candidates:
                start = instant(candidate["start"])
                if commence is None or start is None or abs((start - commence).total_seconds()) > 600:
                    continue
                if game_number is not None and int(candidate.get("game_number") or 0) != int(game_number):
                    continue
                selected.add(int(candidate["game_pk"]))
            entry["candidates"].update(selected)
            if len(selected) == 1:
                entry["mappings"].update(selected)
                entry["matched"] += 1
            elif len(selected) > 1:
                entry["ambiguous_observations"] += 1
    rows = []
    for event_id, entry in sorted(state.items()):
        mappings = sorted(entry["mappings"])
        if len(mappings) == 1:
            classification = "RECONSTRUCTABLE_BUT_PREVIOUSLY_UNBOUND"
            reason = "EXACT_TEAM_START_AND_AUTHORITY_REPLAY_UNIQUE"
        elif len(mappings) > 1 or entry["ambiguous_observations"]:
            classification = "AMBIGUOUS"
            reason = "RETAINED_OBSERVATION_HAS_MULTIPLE_EXACT_TEAM_START_CANDIDATES"
        else:
            classification = "UNAVAILABLE"
            reason = "NO_RETAINED_EXACT_TEAM_START_AUTHORITY_BINDING"
        rows.append({
            "lane": entry["lane"], "provider": "THE_ODDS_API",
            "provider_event_id": event_id,
            "historical_classification": classification,
            "exact_game_pk": mappings[0] if len(mappings) == 1 else "",
            "candidate_game_pks": "|".join(map(str, sorted(entry["candidates"]))),
            "observation_count": entry["observations"], "matched_observation_count": entry["matched"],
            "provider_source_file_count": len(entry["paths"]),
            "provider_source_hash_count": len(entry["hashes"]),
            "reason_code": reason,
            "historical_rows_modified": 0,
        })
    return rows


def pinnacle_reconciliation(authority: set[int]) -> list[dict]:
    with (PRIOR / "provider_event_to_game_pk_audit.csv").open(newline="", encoding="utf-8") as handle:
        prior = [row for row in csv.DictReader(handle) if row["lane"] == "PINNACLE_MAIN_MARKETS"]
    rows = []
    for row in prior:
        game_pk = int(row["exact_game_pk"])
        classification = "RECONSTRUCTABLE_BUT_PREVIOUSLY_UNBOUND" if game_pk in authority else "UNAVAILABLE"
        rows.append({
            "lane": "PINNACLE_MAIN_MARKETS", "provider": "THE_ODDS_API",
            "provider_event_id": row["provider_event_id"],
            "historical_classification": classification,
            "exact_game_pk": game_pk if game_pk in authority else "",
            "candidate_game_pks": row["candidate_game_pks"],
            "observation_count": row["observation_count"], "matched_observation_count": row["observation_count"],
            "provider_source_file_count": row["source_file_count"],
            "provider_source_hash_count": row["source_sha256_count"],
            "reason_code": "EXISTING_EXACT_MAPPING_PRESERVED_SCHEDULE_HASH_WAS_UNBOUND",
            "historical_rows_modified": 0,
        })
    return rows


def main() -> None:
    authority = load_authority()
    index, schedule_files = schedule_index(authority)
    player = player_reconciliation(index)
    pinnacle = pinnacle_reconciliation(authority)
    rows = sorted(player + pinnacle, key=lambda row: (row["lane"], row["provider_event_id"]))
    fields = [
        "lane", "provider", "provider_event_id", "historical_classification", "exact_game_pk",
        "candidate_game_pks", "observation_count", "matched_observation_count",
        "provider_source_file_count", "provider_source_hash_count", "reason_code",
        "historical_rows_modified",
    ]
    write_csv("historical_reconciliation.csv", rows, fields)
    blocked = [row for row in rows if row["historical_classification"] in {"AMBIGUOUS", "UNAVAILABLE"}]
    write_csv("blocked_event_ledger.csv", blocked, fields)
    player_counts = Counter(row["historical_classification"] for row in player)
    pinnacle_counts = Counter(row["historical_classification"] for row in pinnacle)
    summary = {
        "authority_game_pks": len(authority),
        "verified_schedule_source_files": schedule_files,
        "player_prop_events": len(player),
        "player_prop_classifications": dict(sorted(player_counts.items())),
        "betonline_events": sum(row["lane"] == "BETONLINE_PLAYER_PROPS" for row in player),
        "pinnacle_events": len(pinnacle),
        "pinnacle_classifications": dict(sorted(pinnacle_counts.items())),
        "pinnacle_unique_event_game_pairs": len({(row["provider_event_id"], row["exact_game_pk"]) for row in pinnacle}),
        "blocked_events": len(blocked),
        "historical_rows_modified": 0,
        "network_requests": 0,
        "paid_requests": 0,
        "paid_credits": 0,
    }
    (OUT / "reconciliation_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
