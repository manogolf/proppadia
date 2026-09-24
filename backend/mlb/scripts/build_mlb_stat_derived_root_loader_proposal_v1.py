#!/usr/bin/env python3
"""Build the retained-input-only 824785/824784 root-loader proposal.

The utility is deliberately offline.  It reads immutable repository evidence,
never imports a database adapter, and emits no operational authorization.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from backend.mlb.stat_derived_exact_game_loader_v1 import (
    CONTRACT_VERSION,
    FEATURE_CONTRACT_VERSION,
    INSERT_NEW_EXACT_FACT,
    QUARANTINE_CONFLICTING_LEGACY_IDENTITY,
    RELOCATE_MATCHING_MISDATED_FACT,
    UNPROVABLE_FAIL_CLOSED,
    AcceptedGameV1,
    ExactGameMutationPlannerV1,
    MutationIntentV1,
    SourceEvidenceV1,
    accepted_games_from_retained_schedule,
    assert_no_legacy_derived_write,
    canonical_json_bytes,
    content_sha256,
    validate_terminal_feed,
)


ROOT = Path(__file__).resolve().parents[3]
FOUNDATION_FIXTURE = ROOT / "backend/mlb/tests/fixtures/exact_game_stat_derived_v1/retained_and_synthetic_cases.json"
DESIGN_ROOT = ROOT / "artifacts/analysis/model_development/mlb_2026_stat_derived_exact_game_grain_correction_design_v1/2026-09-24"
DESIGN_PROPOSAL = DESIGN_ROOT / "offline_mutation_proposal.csv"
DESIGN_SUMMARY = DESIGN_ROOT / "summary.json"
PHASE_DESCRIPTOR = ROOT / "backend/mlb/season_transition/authority_snapshots/v1/descriptor.json"
DEFAULT_OUTPUT = ROOT / "docs/contracts/mlb_2026_stat_derived_root_loader_correction_v1"

BATTER_PROP_TYPES = (
    "hits", "strikeouts_batting", "home_runs", "rbis", "runs_rbis",
    "runs_scored", "total_bases", "walks", "stolen_bases", "singles",
    "doubles", "triples", "hits_runs_rbis",
)
PITCHER_PROP_TYPES = (
    "strikeouts_pitching", "outs_recorded", "earned_runs", "hits_allowed", "walks_allowed",
)


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def observed_from_feed(feed: Mapping[str, Any]) -> str:
    raw = str((feed.get("metaData") or {}).get("timeStamp") or "")
    try:
        return datetime.strptime(raw, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        raise RuntimeError("RETAINED_FEED_OBSERVATION_TIME_MISSING") from None


def number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def integer(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def stat_int(value: Any) -> int:
    return int(integer(value) or 0)


def ip_to_outs(value: Any) -> int | None:
    if value is None:
        return None
    parts = str(value).split(".")
    try:
        whole = int(parts[0])
    except ValueError:
        return None
    fraction = parts[1] if len(parts) > 1 else "0"
    return whole * 3 + (1 if fraction == "1" else 2 if fraction == "2" else 0)


def hash01(value: str) -> float:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:8], 16) / 0xFFFFFFFF


def stat_for_prop(stats: Mapping[str, Any], prop: str) -> float | None:
    batting = stats.get("batting") or {}
    pitching = stats.get("pitching") or {}
    if prop == "hits": return number(batting.get("hits"))
    if prop == "strikeouts_batting": return number(batting.get("strikeOuts") or batting.get("strikeouts"))
    if prop == "home_runs": return number(batting.get("homeRuns") or batting.get("home_runs"))
    if prop == "rbis": return number(batting.get("rbi") or batting.get("rbis"))
    if prop == "runs_scored": return number(batting.get("runs"))
    if prop == "runs_rbis": return (number(batting.get("runs")) or 0.0) + (number(batting.get("rbi") or batting.get("rbis")) or 0.0)
    if prop == "walks": return number(batting.get("baseOnBalls") or batting.get("walks"))
    if prop == "stolen_bases": return number(batting.get("stolenBases") or batting.get("stolen_bases"))
    if prop == "doubles": return number(batting.get("doubles"))
    if prop == "triples": return number(batting.get("triples"))
    if prop == "total_bases": return number(batting.get("totalBases") or batting.get("total_bases"))
    if prop == "singles":
        return ((number(batting.get("hits")) or 0.0) - (number(batting.get("doubles")) or 0.0)
                - (number(batting.get("triples")) or 0.0) - (number(batting.get("homeRuns") or batting.get("home_runs")) or 0.0))
    if prop == "hits_runs_rbis":
        return ((number(batting.get("hits")) or 0.0) + (number(batting.get("runs")) or 0.0)
                + (number(batting.get("rbi") or batting.get("rbis")) or 0.0))
    if prop == "strikeouts_pitching": return number(pitching.get("strikeOuts") or pitching.get("strikeouts"))
    if prop == "outs_recorded": return number(pitching.get("outs")) or ip_to_outs(pitching.get("inningsPitched"))
    if prop == "earned_runs": return number(pitching.get("earnedRuns") or pitching.get("earned_runs"))
    if prop == "hits_allowed": return number(pitching.get("hits") or pitching.get("hits_allowed"))
    if prop == "walks_allowed": return number(pitching.get("baseOnBalls") or pitching.get("walks_allowed") or pitching.get("walks"))
    return None


def team_starter(players: Mapping[str, Any]) -> set[int]:
    candidates: list[tuple[int, float, int, int]] = []
    for player in players.values():
        person = player.get("person") or {}
        pid = integer(person.get("id"))
        if pid is None:
            continue
        stats = player.get("stats") or {}
        pitching = stats.get("pitching") or {}
        position = str((player.get("position") or {}).get("abbreviation") or "").upper()
        if not pitching and position not in {"P", "SP", "RP"}:
            continue
        starts = number(pitching.get("gamesStarted")) or 0.0
        outs = integer(pitching.get("outs"))
        if outs is None:
            outs = ip_to_outs(pitching.get("inningsPitched"))
        pitches = integer(pitching.get("numberOfPitches") or pitching.get("pitchesThrown") or pitching.get("pitches")) or 0
        candidates.append((pid, starts, int(outs or 0), pitches))
    explicit = {pid for pid, starts, _, _ in candidates if starts > 0}
    if explicit:
        return explicit
    with_outs = [row for row in candidates if row[2] > 0]
    if not with_outs:
        return set()
    return {sorted(with_outs, key=lambda row: (row[2], row[3], -row[0]), reverse=True)[0][0]}


def extract_feed_facts(game: AcceptedGameV1, feed: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    stats_rows: list[dict[str, Any]] = []
    training_rows: list[dict[str, Any]] = []
    game_data = feed.get("gameData") or {}
    teams = game_data.get("teams") or {}
    box_teams = ((feed.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}
    for side in ("away", "home"):
        players = (box_teams.get(side) or {}).get("players") or {}
        starters = team_starter(players)
        team = teams.get(side) or {}
        opponent = teams.get("home" if side == "away" else "away") or {}
        for player in players.values():
            person = player.get("person") or {}
            pid = integer(person.get("id"))
            if pid is None:
                continue
            stats = player.get("stats") or {}
            batting = stats.get("batting") or {}
            pitching = stats.get("pitching") or {}
            position = str((player.get("position") or {}).get("abbreviation") or "").upper()
            is_pitcher = bool(pitching) or position in {"P", "SP", "RP"}
            is_starter = (number(pitching.get("gamesStarted")) or 0.0) > 0 or position == "SP" or pid in starters
            if not batting and not is_pitcher:
                continue
            hits = stat_int(batting.get("hits"))
            doubles = stat_int(batting.get("doubles"))
            triples = stat_int(batting.get("triples"))
            homers = stat_int(batting.get("homeRuns") or batting.get("home_runs"))
            payload = {
                "player_id": pid,
                "game_id": game.game_pk,
                "game_date": game.operational_date,
                "team_id": integer(team.get("id")),
                "opponent_team_id": integer(opponent.get("id")),
                "is_home": side == "home",
                "position": position or None,
                "plate_appearances": stat_int(batting.get("plateAppearances") or batting.get("plate_appearances")),
                "at_bats": stat_int(batting.get("atBats") or batting.get("at_bats")),
                "hits": hits,
                "total_bases": stat_int(batting.get("totalBases") or batting.get("total_bases")),
                "rbis": stat_int(batting.get("rbi") or batting.get("rbis")),
                "runs_scored": stat_int(batting.get("runs")),
                "singles": max(0, hits - doubles - triples - homers),
                "doubles": doubles,
                "triples": triples,
                "home_runs": homers,
                "outs_recorded": integer(pitching.get("outs")) or ip_to_outs(pitching.get("inningsPitched")) or 0,
                "is_starter": bool(is_starter),
            }
            stats_rows.append(payload)
            props: list[str] = []
            if batting:
                props.extend(BATTER_PROP_TYPES)
            if is_pitcher and is_starter:
                props.extend(PITCHER_PROP_TYPES)
            for prop in sorted(set(props)):
                result = stat_for_prop(stats, prop)
                if result is None:
                    continue
                seed = hash01(f"line-{pid}-{game.game_pk}-{prop}")
                line = round(((result - 0.5) if seed < 0.5 else (result + 0.5)) * 2) / 2
                line = max(0.5, line)
                side_value = "over" if hash01(f"ou-{pid}-{game.game_pk}-{prop}") < 0.5 else "under"
                outcome = "win" if ((side_value == "over" and result > line) or (side_value == "under" and result < line)) else "loss"
                training_rows.append({
                    "player_id": pid,
                    "game_id": game.game_pk,
                    "prop_type": prop,
                    "prop_source": "mlb_api",
                    "game_date": game.operational_date,
                    "prop_value": float(result),
                    "line": float(line),
                    "over_under": side_value,
                    "outcome": outcome,
                    "is_home": side == "home",
                })
    return (
        sorted(stats_rows, key=lambda row: (row["player_id"], row["game_id"])),
        sorted(training_rows, key=lambda row: (row["player_id"], row["prop_type"])),
    )


def write_json(path: Path, value: Any) -> None:
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def build(output_dir: Path) -> dict[str, Any]:
    fixture = load_json(FOUNDATION_FIXTURE)
    design_summary = load_json(DESIGN_SUMMARY)
    descriptor_sha = file_sha256(PHASE_DESCRIPTOR)
    if descriptor_sha != "543fda06d3c066bb6f0608ee8c05987829216fa441459a8fc08b4f87ee8c245a":
        raise RuntimeError("PHASE_AUTHORITY_DESCRIPTOR_HASH_MISMATCH")

    retained = fixture["retained_sources"]
    sources: dict[str, SourceEvidenceV1] = {}
    for name in ("september_22_schedule", "september_23_schedule"):
        item = retained[name]
        path = ROOT / item["path"]
        if file_sha256(path) != item["sha256"]:
            raise RuntimeError(f"RETAINED_SOURCE_HASH_MISMATCH:{name}")
        timestamp = "2026-09-22T23:30:04Z" if name == "september_22_schedule" else "2026-09-23T23:30:02Z"
        sources[name] = SourceEvidenceV1(item["path"], item["sha256"], timestamp, "RETAINED_STATSAPI_SCHEDULE")
    sources["phase"] = SourceEvidenceV1(
        str(PHASE_DESCRIPTOR.relative_to(ROOT)), descriptor_sha, "2026-09-24T00:00:00Z",
        "CANONICAL_PHASE_AUTHORITY_DESCRIPTOR",
    )
    design_sha = file_sha256(DESIGN_PROPOSAL)
    sources["design"] = SourceEvidenceV1(
        str(DESIGN_PROPOSAL.relative_to(ROOT)), design_sha,
        design_summary["generated_at_utc"], "RETAINED_READ_ONLY_DATABASE_SNAPSHOT_PROPOSAL",
    )

    accepted = {
        item.game_pk: item
        for item in accepted_games_from_retained_schedule(
            fixture["retained_schedule_payload"],
            schedule_sources=(sources["september_22_schedule"], sources["september_23_schedule"], sources["phase"]),
            phase_by_game_pk={824785: "REGULAR_SEASON", 824784: "REGULAR_SEASON", 824912: "REGULAR_SEASON", 824459: "REGULAR_SEASON", 824460: "REGULAR_SEASON"},
            phase_authority_descriptor_sha256=descriptor_sha,
        )
    }
    intents: list[MutationIntentV1] = []
    affected_players: dict[int, set[int]] = {824785: set(), 824784: set()}

    feed_sources: dict[int, SourceEvidenceV1] = {}
    feeds: dict[int, dict[str, Any]] = {}
    for game_pk, fixture_key in ((824785, "game_824785_final_feed"), (824784, "game_824784_final_feed")):
        item = retained[fixture_key]
        path = ROOT / item["path"]
        if file_sha256(path) != item["sha256"]:
            raise RuntimeError(f"RETAINED_SOURCE_HASH_MISMATCH:{fixture_key}")
        feed = load_json(path)
        source = SourceEvidenceV1(item["path"], item["sha256"], observed_from_feed(feed), "RETAINED_PLAYABLE_TERMINAL_FEED")
        validate_terminal_feed(accepted[game_pk], feed, source)
        feed_sources[game_pk] = source
        feeds[game_pk] = feed

    design_rows = list(csv.DictReader(DESIGN_PROPOSAL.open(newline="", encoding="utf-8")))
    for row in design_rows:
        if row["game_pk"] != "824785":
            continue
        relation = row["relation"]
        if relation == "bounded_recovery_plan":
            continue
        player_id = integer(row["player_id"])
        if player_id is not None:
            affected_players[824785].add(player_id)
        if relation == "game_info":
            key = {"game_id": 824785}
        elif relation == "player_stats":
            key = {"player_id": player_id, "game_id": 824785}
        else:
            key = {"id": row["row_identity"]}
        current = {
            "row_identity": row["row_identity"],
            "game_date": row["current_date"],
            "substantive_payload_sha256": row["substantive_payload_sha256"],
        }
        proposed = dict(current, game_date=row["corrected_date"])
        force = RELOCATE_MATCHING_MISDATED_FACT
        reason = "ACCEPTED_MAKEUP_APPEARANCE_RELOCATES_DATE_ATTRIBUTE_ONLY"
        if relation == "player_derived_stats":
            proposed = None
            force = QUARANTINE_CONFLICTING_LEGACY_IDENTITY
            reason = "LEGACY_PLAYER_DATE_EVIDENCE_RETAINED_UNCHANGED_AND_EXCLUDED_FROM_EXACT_USE"
        intents.append(MutationIntentV1(
            relation=relation,
            exact_key=key,
            authoritative_game_pk=824785,
            accepted_operational_date="2026-09-23",
            source_evidence=(sources["september_22_schedule"], sources["september_23_schedule"], feed_sources[824785], sources["phase"], sources["design"]),
            relationship_evidence={"postponed_appearance": "2026-09-22", "playable_makeup_appearance": "2026-09-23", "same_game_pk": 824785},
            reason=reason,
            rollback_identity=key,
            current_value=current,
            proposed_value=proposed,
            current_substantive_hash=row["substantive_payload_sha256"],
            proposed_substantive_hash=row["substantive_payload_sha256"] if proposed else None,
            force_operation=force,
            collision_status=row["collision_status"],
        ))

    stats_785, training_785 = extract_feed_facts(accepted[824785], feeds[824785])
    if len(stats_785) != 49 or len(training_785) != 188 or affected_players[824785] != {row["player_id"] for row in stats_785}:
        raise RuntimeError("GAME_824785_RETAINED_POPULATION_MISMATCH")
    for player_id in sorted(affected_players[824785]):
        key = {"player_id": player_id, "game_pk": 824785, "contract_version": FEATURE_CONTRACT_VERSION}
        intents.append(MutationIntentV1(
            relation="player_game_feature_state_v1",
            exact_key=key,
            authoritative_game_pk=824785,
            accepted_operational_date="2026-09-23",
            source_evidence=(feed_sources[824785], sources["phase"], sources["design"]),
            relationship_evidence={"historical_feature_input_cutoff": "NOT_RETAINED"},
            reason="HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE",
            rollback_identity=key,
            current_value=None,
            proposed_value=None,
            force_operation=UNPROVABLE_FAIL_CLOSED,
            collision_status="NO_MUTATION_ALLOWED_MISSING_IMMUTABLE_CUTOFF",
        ))

    stats_784, training_784 = extract_feed_facts(accepted[824784], feeds[824784])
    if len(stats_784) != 46 or len(training_784) != 160:
        raise RuntimeError("GAME_824784_RETAINED_POPULATION_MISMATCH")
    affected_players[824784] = {row["player_id"] for row in stats_784}
    source_tuple_784 = (sources["september_23_schedule"], feed_sources[824784], sources["phase"], sources["design"])
    game_info_payload = {
        "game_id": 824784,
        "game_date": accepted[824784].operational_date,
        "game_time": accepted[824784].scheduled_start_utc,
        "game_number": accepted[824784].game_number,
        "double_header": accepted[824784].double_header,
        "team_ids": list(accepted[824784].team_ids),
        "phase": accepted[824784].phase,
        "authority_hash": accepted[824784].authority_hash,
    }
    intents.append(MutationIntentV1(
        relation="game_info", exact_key={"game_id": 824784}, authoritative_game_pk=824784,
        accepted_operational_date="2026-09-23", source_evidence=source_tuple_784,
        relationship_evidence={"split_doubleheader_sibling": 824785, "game_number": 2},
        reason="MISSING_EXACT_GAME_FACT_FROM_RETAINED_PLAYABLE_TERMINAL_FEED",
        rollback_identity={"game_id": 824784}, current_value=None, proposed_value=game_info_payload,
        force_operation=INSERT_NEW_EXACT_FACT, collision_status="RETAINED_BOUNDARY_REPORTS_RELATION_ABSENT",
    ))
    for row in stats_784:
        key = {"player_id": row["player_id"], "game_id": 824784}
        intents.append(MutationIntentV1(
            relation="player_stats", exact_key=key, authoritative_game_pk=824784,
            accepted_operational_date="2026-09-23", source_evidence=source_tuple_784,
            relationship_evidence={"split_doubleheader_sibling": 824785, "game_number": 2},
            reason="MISSING_EXACT_PLAYER_GAME_FACT_FROM_RETAINED_PLAYABLE_TERMINAL_FEED",
            rollback_identity=key, current_value=None, proposed_value=row,
            force_operation=INSERT_NEW_EXACT_FACT, collision_status="RETAINED_BOUNDARY_REPORTS_RELATION_ABSENT",
        ))
    for row in training_784:
        key = {"player_id": row["player_id"], "game_id": 824784, "prop_type": row["prop_type"], "prop_source": "mlb_api"}
        intents.append(MutationIntentV1(
            relation="model_training_props", exact_key=key, authoritative_game_pk=824784,
            accepted_operational_date="2026-09-23", source_evidence=source_tuple_784,
            relationship_evidence={"split_doubleheader_sibling": 824785, "game_number": 2},
            reason="MISSING_EXACT_PLAYER_GAME_STAT_OUTCOME_FROM_RETAINED_PLAYABLE_TERMINAL_FEED",
            rollback_identity=key, current_value=None, proposed_value=row,
            force_operation=INSERT_NEW_EXACT_FACT, collision_status="RETAINED_BOUNDARY_REPORTS_RELATION_ABSENT",
        ))
    for player_id in sorted(affected_players[824784]):
        key = {"player_id": player_id, "game_pk": 824784, "contract_version": FEATURE_CONTRACT_VERSION}
        intents.append(MutationIntentV1(
            relation="player_game_feature_state_v1", exact_key=key, authoritative_game_pk=824784,
            accepted_operational_date="2026-09-23", source_evidence=source_tuple_784,
            relationship_evidence={"historical_feature_input_cutoff": "NOT_RETAINED", "split_doubleheader_sibling": 824785},
            reason="HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE",
            rollback_identity=key, current_value=None, proposed_value=None,
            force_operation=UNPROVABLE_FAIL_CLOSED, collision_status="NO_MUTATION_ALLOWED_MISSING_IMMUTABLE_CUTOFF",
        ))

    plan = ExactGameMutationPlannerV1().build(intents)
    assert_no_legacy_derived_write(plan)
    operation_counts = Counter(row.operation for row in plan.proposals)
    relation_counts = Counter(row.relation for row in plan.proposals)
    game_counts = Counter(row.authoritative_game_pk for row in plan.proposals)
    overlap = affected_players[824785] & affected_players[824784]
    summary = {
        "contract": CONTRACT_VERSION,
        "status": "OFFLINE_PROPOSAL_ONLY_NOT_AUTHORIZED",
        "database_connections": 0,
        "database_writes": 0,
        "api_requests": 0,
        "pipeline_runs": 0,
        "plan_sha256": plan.plan_sha256,
        "source_set_sha256": plan.source_set_sha256,
        "expected_state_sha256": plan.expected_state_sha256,
        "proposal_rows": len(plan.proposals),
        "operation_counts": dict(sorted(operation_counts.items())),
        "relation_counts": dict(sorted(relation_counts.items())),
        "game_counts": {str(key): value for key, value in sorted(game_counts.items())},
        "game_824785": {
            "accepted_operational_date": accepted[824785].operational_date,
            "player_stats_relocations": 49,
            "training_relocations": 188,
            "game_info_relocations": 1,
            "legacy_derived_quarantines": 49,
            "feature_states_blocked_missing_cutoff": 49,
        },
        "game_824784": {
            "accepted_operational_date": accepted[824784].operational_date,
            "game_info_inserts": 1,
            "player_stats_inserts": 46,
            "training_inserts": 160,
            "legacy_derived_writes": 0,
            "feature_states_blocked_missing_cutoff": 46,
        },
        "player_population": {
            "824785": 49,
            "824784": 46,
            "appeared_in_both": len(overlap),
            "824785_only": len(affected_players[824785] - overlap),
            "824784_only": len(affected_players[824784] - overlap),
        },
        "matching_rows": operation_counts.get("MATCH_EXISTING_EXACT_FACT", 0),
        "conflicting_rows": operation_counts.get("BLOCK_CONFLICTING_PAYLOAD", 0),
        "corrected_or_missing_exact_facts": operation_counts[RELOCATE_MATCHING_MISDATED_FACT] + operation_counts[INSERT_NEW_EXACT_FACT],
        "historical_cutoff_reconstruction": "PROHIBITED",
        "authorization_artifact_created": False,
        "operational_activation": "NOT_AUTHORIZED",
        "stat_derived_retry": "BLOCKED_PENDING_AUTHORIZED_RECONCILIATION",
        "separate_totals_defect": "OFFICIAL_FINAL_SOURCE_COUNT_824462_2",
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "contract.json", {
        "contract": CONTRACT_VERSION,
        "loader_module": "backend.mlb.stat_derived_exact_game_loader_v1",
        "operational_activation": False,
        "default_mode": "DRY_RUN_NO_MUTATION",
        "required_authorization": "SEPARATELY_GENERATED_PLAN_HASH_BOUND_EXPIRING_ARTIFACT",
        "relation_grains": {
            "game_info": "EXACT_GAME_PK_FACT",
            "player_stats": "EXACT_PLAYER_GAME_POSTGAME_FACT",
            "model_training_props": "EXACT_PLAYER_GAME_STAT_OUTCOME",
            "player_game_feature_state_v1": "PROSPECTIVE_STRICT_PRIOR_EXACT_PLAYER_GAME_STATE",
            "player_derived_stats": "LEGACY_PLAYER_DATE_EVIDENCE_READ_ONLY",
        },
        "legacy_derived_writes": False,
        "date_only_identity": False,
        "identity_by_maximum_game_id": False,
        "historical_cutoff_reconstruction": False,
        "shared_finality_contract": "MLB_SHARED_PLAYABLE_TERMINAL_V1",
        "phase_authority": "MLB_CANONICAL_GAME_PHASE_AUTHORITY_V1",
        "operation_classes": sorted({row.operation for row in plan.proposals} | {"MATCH_EXISTING_EXACT_FACT", "BLOCK_CONFLICTING_PAYLOAD", "NO_ACTION"}),
    })
    write_json(output_dir / "summary.json", summary)
    with (output_dir / "offline_mutation_proposal.jsonl.gz").open("wb") as raw_handle:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw_handle, mtime=0) as handle:
            for proposal in plan.proposals:
                handle.write(canonical_json_bytes(proposal.to_dict()) + b"\n")
    with (output_dir / "affected_key_ledger.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = ["game_pk", "player_id", "in_game_824785", "in_game_824784", "population_class"]
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for player_id in sorted(affected_players[824785] | affected_players[824784]):
            in_785 = player_id in affected_players[824785]
            in_784 = player_id in affected_players[824784]
            writer.writerow({
                "game_pk": "824785|824784" if in_785 and in_784 else "824785" if in_785 else "824784",
                "player_id": player_id,
                "in_game_824785": str(in_785).lower(),
                "in_game_824784": str(in_784).lower(),
                "population_class": "BOTH_GAMES" if in_785 and in_784 else "GAME_824785_ONLY" if in_785 else "GAME_824784_ONLY",
            })
    with (output_dir / "legacy_quarantine_specification.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = ["relation", "legacy_row_id", "player_id", "game_pk_label", "disposition", "write_allowed"]
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in plan.proposals:
            if row.relation == "player_derived_stats":
                writer.writerow({
                    "relation": row.relation,
                    "legacy_row_id": row.exact_key["id"],
                    "player_id": next(item["player_id"] for item in design_rows if item["relation"] == "player_derived_stats" and item["row_identity"] == row.exact_key["id"]),
                    "game_pk_label": row.authoritative_game_pk,
                    "disposition": row.operation,
                    "write_allowed": "false",
                })
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    summary = build(args.output_dir)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
