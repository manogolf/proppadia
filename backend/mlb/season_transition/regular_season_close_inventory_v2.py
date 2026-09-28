"""Offline reconciliation of the 2026 MLB regular-season close blockers.

V2 adds exact-gamePk retained live-feed observations to the unchanged V1
phase-authority population. It has no network or database dependencies.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from backend.mlb.season_transition import regular_season_close_inventory_v1 as v1
from backend.mlb.season_transition.game_phase_authority_v1 import load_v1_authority

ROOT = v1.REPO_ROOT
PACKAGE = ROOT / "docs/contracts/mlb_2026_authoritative_regular_season_close_inventory_v2"
INVENTORY = "authoritative_regular_season_games.jsonl"
EVIDENCE = "retained_live_feed_evidence.jsonl"
SCHEDULE_EVIDENCE = "retained_schedule_relationship_evidence.jsonl"
MANIFEST = "reconciliation_manifest.json"
REPORT = "validation_report.json"
EVIDENCE_ROOTS = (
    "artifacts/analysis/mlb/player_stats_completeness",
    "artifacts/ops/mlb_stat_derived_natural_run_evidence_v1",
)
SCHEDULE_ROOT = "artifacts/ops/mlb_public_game_moneyline_history_schedules"
CONTRACT = "MLB_2026_REGULAR_SEASON_CLOSE_INVENTORY_RECONCILIATION_V2"
RETAINED_RELATIONSHIP_GAME_PK = 824785
RETAINED_RELATIONSHIP_FEED_SHA256 = (
    "61bdfdaae620c95da70b0e0940d854d8a789a0e1ef2e0e5bb325b7de41d8406b"
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_bytes(b"".join(v1.canonical_json_bytes(row) + b"\n" for row in rows))


def _load_pinned_feeds() -> tuple[list[dict[str, Any]], dict[int, list[v1.SourceObservation]]]:
    evidence_path = PACKAGE / EVIDENCE
    if not evidence_path.exists():
        raise v1.CloseInventoryError("V2_RETAINED_EVIDENCE_MANIFEST_MISSING")
    records = _read_jsonl(evidence_path)
    # The exact blocker ledger is pinned by V1's validation report, not reconstructed from dates.
    blockers = {int(value) for value in json.loads(
        (v1.DEFAULT_PACKAGE_PATH / "scheduled_not_final_game_pks.json").read_text())}
    allowed_ids = blockers | {RETAINED_RELATIONSHIP_GAME_PK}
    grouped: dict[int, list[v1.SourceObservation]] = {}
    for record in records:
        relative = record["path"]
        if not any(relative.startswith(prefix + "/") for prefix in EVIDENCE_ROOTS):
            raise v1.CloseInventoryError("V2_EVIDENCE_PATH_OUTSIDE_RETAINED_ROOT")
        path = ROOT / relative
        if sha256(path) != record["sha256"]:
            raise v1.CloseInventoryError(f"V2_RETAINED_EVIDENCE_HASH_MISMATCH:{relative}")
        payload = json.loads(path.read_text())
        game = v1._normalize_live_feed_game(payload)
        game_pk = int(game.get("gamePk") or 0)
        if game_pk != int(record["game_pk"]) or game_pk not in allowed_ids:
            raise v1.CloseInventoryError(f"V2_RETAINED_EVIDENCE_GAMEPK_MISMATCH:{relative}")
        grouped.setdefault(game_pk, []).append(v1.SourceObservation(
            relative, record["sha256"], game, "STATSAPI_LIVE_GAME_FEED",
            v1._statsapi_feed_timestamp(payload),
        ))
    return records, grouped


def _load_pinned_schedules() -> tuple[list[dict[str, Any]], dict[int, list[v1.SourceObservation]]]:
    evidence_path = PACKAGE / SCHEDULE_EVIDENCE
    records = _read_jsonl(evidence_path) if evidence_path.exists() else []
    blockers = {int(value) for value in json.loads(
        (v1.DEFAULT_PACKAGE_PATH / "scheduled_not_final_game_pks.json").read_text())}
    grouped: dict[int, list[v1.SourceObservation]] = {}
    loaded_sources: dict[str, tuple[str, dict[str, Any]]] = {}
    for record in records:
        relative = record["path"]
        if not relative.startswith("artifacts/ops/mlb_public_game_moneyline_history_schedules/"):
            raise v1.CloseInventoryError("V2_SCHEDULE_EVIDENCE_PATH_OUTSIDE_RETAINED_ROOT")
        if relative not in loaded_sources:
            path = ROOT / relative
            digest = sha256(path)
            if digest != record["sha256"]:
                raise v1.CloseInventoryError(f"V2_RETAINED_SCHEDULE_HASH_MISMATCH:{relative}")
            loaded_sources[relative] = (digest, json.loads(path.read_text()))
        digest, payload = loaded_sources[relative]
        if digest != record["sha256"]:
            raise v1.CloseInventoryError(f"V2_RETAINED_SCHEDULE_HASH_MISMATCH:{relative}")
        matches = [game for day in payload.get("dates", []) for game in day.get("games", [])
                   if int(game.get("gamePk") or 0) == int(record["game_pk"])]
        if not matches or int(record["game_pk"]) not in blockers:
            raise v1.CloseInventoryError(f"V2_RETAINED_SCHEDULE_GAMEPK_MISMATCH:{relative}")
        for game in matches:
            if game.get("gameType") != "R" or str(game.get("season")) != "2026":
                raise v1.CloseInventoryError(f"V2_SCHEDULE_IDENTITY_INVALID:{record['game_pk']}")
            grouped.setdefault(int(record["game_pk"]), []).append(v1.SourceObservation(
                relative, digest, game, "STATSAPI_SCHEDULE_RESPONSE", None))
    return records, grouped


def build_inventory() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows, summary = v1.build_authoritative_inventory(root=ROOT)
    evidence_records, feeds = _load_pinned_feeds()
    schedule_records, retained_schedules = _load_pinned_schedules()
    authority = {r.game_pk: r for r in load_v1_authority(root=ROOT).records}
    blocker_ids = set(summary["scheduled_not_final_game_pks"])
    blocker_authority = [authority[pk] for pk in sorted(blocker_ids)]
    schedule_observations, _ = v1._load_verified_observations(
        blocker_authority,
        source_manifest_paths=(v1.DEFAULT_SOURCE_MANIFEST_PATH,
                               v1.DEFAULT_DISPOSITION_SUPPLEMENT_MANIFEST_PATH),
        root=ROOT,
    )
    found = set(feeds)
    if not found <= (blocker_ids | {RETAINED_RELATIONSHIP_GAME_PK}):
        raise v1.CloseInventoryError("V2_FEED_NOT_IN_BLOCKER_LEDGER")
    by_pk = {row["game_pk"]: row for row in rows}
    transition_conflicts: dict[int, str] = {}
    for game_pk in sorted(blocker_ids):
        observations = feeds.get(game_pk, [])
        prior = [*schedule_observations.get(game_pk, []), *retained_schedules.get(game_pk, [])]
        merged = v1._deduplicate_observations([*prior, *observations])
        classified = v1.classify_authoritative_game(authority[game_pk], merged)
        # Exact gamePk, regular-season type/season, stable teams, and one
        # consistent terminal outcome establish identity across appearances.
        # officialDate changes alone are not contradictory: the terminal
        # playable feed's officialDate is the played date, while every retained
        # schedule appearance remains in the row's evidence history.
        schedules = [o.game for o in merged if o.source_kind == "STATSAPI_SCHEDULE_RESPONSE"]
        for obs in observations:
            g = obs.game
            if (int(g.get("gamePk") or 0) != game_pk or g.get("gameType") != "R"
                    or int(g.get("season") or 0) != 2026):
                raise v1.CloseInventoryError(f"V2_FEED_IDENTITY_INVALID:{game_pk}")
        terminal_feeds = [o for o in observations
                          if v1._terminal_kind(v1._status(o.game)) == "FINAL"]
        identity_sources = [*schedules, *(o.game for o in observations)]
        team_pairs = set()
        incomplete_team_identity = False
        for source in identity_sources:
            teams = source.get("teams") or {}
            pair = tuple(((teams.get(side) or {}).get("team") or {}).get("id")
                         for side in ("away", "home"))
            if all(value is not None for value in pair):
                team_pairs.add(pair)
            else:
                incomplete_team_identity = True
        evidence_conflicts: list[str] = []
        if incomplete_team_identity:
            evidence_conflicts.append("INCOMPLETE_AWAY_HOME_TEAM_IDENTITY")
        if len(team_pairs) > 1:
            evidence_conflicts.append("CONFLICTING_AWAY_HOME_TEAM_IDENTITY")
        if len(terminal_feeds) > 1:
            outcomes = {json.dumps(v1._score_outcome(o.game), sort_keys=True)
                        for o in terminal_feeds}
            if len(outcomes) != 1:
                evidence_conflicts.append("MULTIPLE_INCOMPATIBLE_TERMINAL_FEED_OUTCOMES")
        accepted = not evidence_conflicts and classified["close_disposition"] in {
            "FINAL", "POSTPONED_RESCHEDULED_IDENTITY_RESOLVED",
            "SUSPENDED_RESUMED_IDENTITY_RESOLVED"}
        if accepted:
            final_dates = {str(o.game.get("officialDate")) for o in terminal_feeds
                           if o.game.get("officialDate")}
            if len(final_dates) > 1:
                evidence_conflicts.append("CONFLICTING_TERMINAL_OFFICIAL_DATE")
                accepted = False
        if accepted:
            classified["played_official_date"] = next(iter(final_dates), None)
            by_pk[game_pk] = classified
        else:
            reason = ";".join(evidence_conflicts) or classified["disposition_reason"]
            transition_conflicts[game_pk] = reason
            row = by_pk[game_pk]
            row["close_disposition"] = "UNRESOLVED_IDENTITY_OR_STATUS"
            row["disposition_reason"] = reason
            if observations:
                row["required_evidence"] = [
                    "One consistent authoritative playable terminal feed for this exact gamePk, with gameType=R, season=2026, matching teams, officialDate, final status, and result."
                ]
            else:
                row["required_evidence"] = [
                    "Retained authoritative exact-gamePk schedule appearance or live feed documenting gameType=R, season=2026, matching teams, and an allowed terminal disposition (Final or an allowed cancellation/postponement/resumption outcome)."
                ]
    unresolved = sorted(set(transition_conflicts))
    rows = [by_pk[r["game_pk"]] for r in rows]
    counts = {key: sum(r["close_disposition"] == key for r in rows)
              for key in sorted(v1.DISPOSITIONS)}
    summary.update({
        "contract_name": CONTRACT,
        "disposition_counts": counts,
        "scheduled_not_final_game_pks": [],
        "unresolved_game_pks": unresolved,
        "reconciliation": {
            "prior_blockers": len(blocker_ids),
            "exact_gamepk_live_feed_ids": len(found & blocker_ids),
            "additional_relationship_feed_ids": len(found - blocker_ids),
            "accepted_terminal_from_blockers": sum(
                by_pk[pk]["close_disposition"] in {
                    "FINAL", "POSTPONED_RESCHEDULED_IDENTITY_RESOLVED",
                    "SUSPENDED_RESUMED_IDENTITY_RESOLVED"} for pk in blocker_ids),
            "unresolved": len(unresolved),
            "live_feed_evidence_records": len(evidence_records),
            "retained_schedule_evidence_records": len(schedule_records),
            "required_evidence_by_unresolved_game_pk": {str(pk): by_pk[pk]["required_evidence"] for pk in unresolved},
        },
    })
    return rows, summary


def manifest(rows: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, Any]:
    evidence_path = PACKAGE / EVIDENCE
    evidence_hash = sha256(evidence_path)
    body = {
        "contract_name": CONTRACT, "population_size": len(rows),
        "date_transition_interpretation": (
            "Exact gamePk plus gameType=R, season=2026, matching away/home teams, "
            "and one consistent authoritative playable terminal outcome link "
            "schedule appearances to a single game. officialDate changes alone "
            "do not conflict; a terminal live feed's officialDate is played_official_date."
        ),
        "authority_contract": v1.CONTRACT_NAME,
        "authority_manifest_sha256": summary["authority"]["source_manifest_sha256"],
        "prior_inventory_manifest_sha256": v1.EXPECTED_CLOSE_INVENTORY_MANIFEST_SHA256,
        "retained_live_feed_evidence_path": EVIDENCE,
        "retained_live_feed_evidence_sha256": evidence_hash,
        "retained_live_feed_evidence_records": summary["reconciliation"]["live_feed_evidence_records"],
        "retained_schedule_relationship_evidence_path": SCHEDULE_EVIDENCE,
        "retained_schedule_relationship_evidence_sha256": sha256(PACKAGE / SCHEDULE_EVIDENCE),
        "retained_schedule_relationship_evidence_records": summary["reconciliation"]["retained_schedule_evidence_records"],
        "disposition_counts": summary["disposition_counts"],
        "unresolved_game_pks": summary["unresolved_game_pks"],
        "inventory_sha256": hashlib.sha256(b"".join(v1.canonical_json_bytes(r)+b"\n" for r in rows)).hexdigest(),
    }
    body["manifest_sha256"] = v1.canonical_sha256(body)
    return body


def initialize_package() -> dict[str, Any]:
    """Pin bounded retained feeds and schedule appearances for the blocker set."""
    prior = json.loads((v1.DEFAULT_PACKAGE_PATH / "scheduled_not_final_game_pks.json").read_text())
    blocker_ids = {int(value) for value in prior}
    evidence_ids = blocker_ids | {RETAINED_RELATIONSHIP_GAME_PK}
    records: dict[str, dict[str, Any]] = {}
    for retained_root in EVIDENCE_ROOTS:
        base = ROOT / retained_root
        for path in sorted(base.rglob("*.json")):
            if "live_feed" not in path.name.lower():
                continue
            try:
                payload = json.loads(path.read_text())
                game = v1._normalize_live_feed_game(payload)
                game_pk = int(game.get("gamePk") or 0)
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                continue
            if game_pk in evidence_ids:
                rel = path.relative_to(ROOT).as_posix()
                digest = sha256(path)
                if digest == RETAINED_RELATIONSHIP_FEED_SHA256 and game_pk != RETAINED_RELATIONSHIP_GAME_PK:
                    raise v1.CloseInventoryError("V2_824785_FEED_GAMEPK_MISMATCH")
                records[rel] = {"path": rel, "sha256": digest, "game_pk": game_pk}
    if not any(row["sha256"] == RETAINED_RELATIONSHIP_FEED_SHA256
               and row["game_pk"] == RETAINED_RELATIONSHIP_GAME_PK
               for row in records.values()):
        raise v1.CloseInventoryError("V2_824785_FINAL_FEED_EVIDENCE_MISSING")
    ordered = [records[key] for key in sorted(records)]
    _write_jsonl(PACKAGE / EVIDENCE, ordered)
    target_pks = evidence_ids
    schedule_records_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    schedule_root = ROOT / SCHEDULE_ROOT
    for path in sorted(schedule_root.rglob("*.json")):
        if path.name.endswith(".selection.json"):
            continue
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        matched = {int(game.get("gamePk") or 0)
                   for day in payload.get("dates", [])
                   for game in day.get("games", [])
                   if int(game.get("gamePk") or 0) in target_pks}
        if not matched:
            continue
        relative = path.relative_to(ROOT).as_posix()
        digest = sha256(path)
        for pk in sorted(matched):
            schedule_records_by_key[(relative, pk)] = {
                "path": relative, "sha256": digest, "game_pk": pk}
    schedule_records = [schedule_records_by_key[key]
                        for key in sorted(schedule_records_by_key)]
    if not {823489, 824703, 824705, 824785} <= {
            record["game_pk"] for record in schedule_records}:
        raise v1.CloseInventoryError("V2_EXPECTED_SCHEDULE_GAMEPK_SET_MISMATCH")
    _write_jsonl(PACKAGE / SCHEDULE_EVIDENCE, schedule_records)
    rows, summary = build_inventory()
    _write_jsonl(PACKAGE / INVENTORY, rows)
    built_manifest = manifest(rows, summary)
    (PACKAGE / MANIFEST).write_text(json.dumps(built_manifest, indent=2, sort_keys=True) + "\n")
    report = validate_package()
    (PACKAGE / REPORT).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def validate_package() -> dict[str, Any]:
    rows, summary = build_inventory()
    expected_bytes = b"".join(v1.canonical_json_bytes(r) + b"\n" for r in rows)
    stored = (PACKAGE / INVENTORY).read_bytes()
    stored_manifest = json.loads((PACKAGE / MANIFEST).read_text())
    rebuilt_manifest = manifest(rows, summary)
    authority = load_v1_authority(root=ROOT)
    expected_pks = {r.game_pk for r in authority.records if r.season_phase == "REGULAR_SEASON"}
    row_pks = [r["game_pk"] for r in rows]
    postseason = [r["game_pk"] for r in rows if r.get("authoritative_raw_game_type") != "R"]
    checks = {
        "regular_season_population_2430": len(rows) == 2430 and set(row_pks) == expected_pks,
        "unique_gamepk_population": len(row_pks) == len(set(row_pks)),
        "postseason_excluded": not postseason,
        "inventory_rebuild_deterministic": expected_bytes == stored,
        "manifest_rebuild_deterministic": rebuilt_manifest == stored_manifest,
        "blocker_reconciliation_accounts_for_all_88": summary["reconciliation"]["accepted_terminal_from_blockers"] + summary["reconciliation"]["unresolved"] == 88,
        "all_population_rows_have_disposition": sum(summary["disposition_counts"].values()) == 2430,
    }
    report = {"contract_name": CONTRACT, "checks": checks,
              "integrity_passed": all(checks.values()), "population_counts": summary["population_counts"],
              "disposition_counts": summary["disposition_counts"],
              "exact_unresolved_game_pks": summary["unresolved_game_pks"],
              "reconciliation": summary["reconciliation"]}
    return report
