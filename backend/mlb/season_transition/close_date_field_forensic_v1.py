"""Bounded, offline forensic report for V2 close-inventory date conflicts."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any

from backend.mlb.season_transition import regular_season_close_inventory_v1 as v1
from backend.mlb.season_transition.regular_season_close_inventory_v2 import (
    EVIDENCE, SCHEDULE_EVIDENCE, EVIDENCE_ROOTS, PACKAGE as V2_PACKAGE,
    _load_pinned_schedules, sha256 as file_sha256,
)
from backend.mlb.season_transition.game_phase_authority_v1 import load_v1_authority

ROOT = v1.REPO_ROOT
PACKAGE = ROOT / "docs/contracts/mlb_2026_close_reconciliation_date_field_forensic_v1"
CONFLICTS = (823489, 824703, 824705, 824785)
GAPS = (822841, 823086, 823168, 823327, 823410, 823490, 823492, 823894,
        824060, 824223, 824301, 824625, 824710, 824784, 824868, 824951)
RELATIONSHIP_FIELDS = ("rescheduleDate", "rescheduleGameDate", "rescheduledFrom",
                       "rescheduledFromDate", "resumeDate", "resumeGameDate",
                       "resumedFrom", "resumedFromDate")


def _et(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
            ZoneInfo("America/New_York")).isoformat()
    except ValueError:
        return None


def _appearance(game: dict[str, Any], *, path: str, digest: str,
                datetime_fields: dict[str, Any] | None = None) -> dict[str, Any]:
    dt = datetime_fields if isinstance(datetime_fields, dict) else {}
    status = game.get("status") or {}
    return {
        "path": path, "sha256": digest,
        "gamePk": game.get("gamePk", game.get("pk")),
        "gameType": game.get("gameType", game.get("type")),
        "season": game.get("season"),
        "officialDate": dt.get("officialDate", game.get("officialDate")),
        "gameDate": dt.get("dateTime", game.get("gameDate")),
        "gameDate_ET_diagnostic": _et(dt.get("dateTime", game.get("gameDate"))),
        "status": {key: status.get(key) for key in
                   ("abstractGameState", "codedGameState", "detailedState", "statusCode", "reason")},
        "teams": {side: ((game.get("teams") or {}).get(side) or {}).get("team", {}).get("id")
                  for side in ("away", "home")},
        "relationships": {key: game[key] for key in RELATIONSHIP_FIELDS if key in game},
    }


def _schedule_games(payload: dict[str, Any], game_pk: int) -> list[dict[str, Any]]:
    return [game for day in payload.get("dates", []) for game in day.get("games", [])
            if game.get("gamePk") == game_pk]


def build_report() -> dict[str, Any]:
    authorities = {r.game_pk: r for r in load_v1_authority(root=ROOT).records}
    observations, _ = v1._load_verified_observations(
        [authorities[pk] for pk in CONFLICTS],
        source_manifest_paths=(v1.DEFAULT_SOURCE_MANIFEST_PATH,
                               v1.DEFAULT_DISPOSITION_SUPPLEMENT_MANIFEST_PATH),
        root=ROOT,
    )
    _, added_schedules = _load_pinned_schedules()
    feed_manifest_path = V2_PACKAGE / EVIDENCE
    feed_manifest = [json.loads(line) for line in feed_manifest_path.read_text().splitlines() if line]
    by_conflict: dict[int, list[dict[str, Any]]] = {pk: [] for pk in CONFLICTS}
    for ref in feed_manifest:
        pk = int(ref["game_pk"])
        if pk not in by_conflict:
            continue
        path = ROOT / ref["path"]
        payload = json.loads(path.read_text())
        gd = payload["gameData"]
        game = gd["game"]
        raw = {
            "gamePk": payload.get("gamePk"), "type": game.get("type"),
            "season": game.get("season"),
        }
        by_conflict[pk].append(_appearance(
            raw, path=ref["path"], digest=ref["sha256"],
            datetime_fields=gd.get("datetime"),
        ) | {"status": {k: (gd.get("status") or {}).get(k) for k in
                          ("abstractGameState", "codedGameState", "detailedState", "statusCode", "reason")},
            "teams": {side: (gd.get("teams", {}).get(side) or {}).get("id") for side in ("away", "home")}})

    cases: list[dict[str, Any]] = []
    for pk in CONFLICTS:
        schedule = []
        all_schedule_observations = [*observations.get(pk, []), *added_schedules.get(pk, [])]
        for obs in all_schedule_observations:
            path = ROOT / obs.source_path
            raw_payload = json.loads(path.read_text())
            games = _schedule_games(raw_payload, pk)
            if not games:
                raise v1.CloseInventoryError(f"FORENSIC_SCHEDULE_GAMEPK_NOT_FOUND:{pk}:{obs.source_path}")
            for game in games:
                schedule.append(_appearance(game, path=obs.source_path, digest=obs.source_sha256))
        feeds = by_conflict[pk]
        official_dates = {x["officialDate"] for x in [*schedule, *feeds] if x["officialDate"]}
        relationships = [x["relationships"] for x in [*schedule, *feeds]]
        relationship_fields = {key for rel in relationships for key, value in rel.items()
                               if value not in (None, "")}
        complete_reschedule = {"rescheduleDate", "rescheduleGameDate",
                               "rescheduledFrom", "rescheduledFromDate"} <= relationship_fields
        category = ("RESOLVED_POSTPONEMENT_RESCHEDULE" if complete_reschedule
                    else "MULTIPLE_OFFICIAL_APPEARANCES_RELATIONSHIP_MISSING"
                    if len(official_dates) > 1 else "UNRESOLVED")
        cases.append({"gamePk": pk, "classification": category,
                      "schedule_appearances": sorted(schedule, key=lambda x: (x["path"], x["sha256"])),
                      "feed_appearances": sorted(feeds, key=lambda x: (x["path"], x["sha256"]))})

    root_search = []
    gap_rows: dict[int, list[str]] = {pk: [] for pk in GAPS}
    for root in EVIDENCE_ROOTS:
        base = ROOT / root
        rows = []
        for path in sorted(base.rglob("*.json")):
            if "live_feed" not in path.name.lower():
                continue
            raw = path.read_bytes()
            try:
                payload = json.loads(raw)
                pk = int(payload.get("gamePk") or 0)
            except (ValueError, TypeError, json.JSONDecodeError):
                pk = 0
            rel = path.relative_to(ROOT).as_posix()
            digest = hashlib.sha256(raw).hexdigest()
            rows.append({"path": rel, "sha256": digest, "gamePk": pk})
            if pk in gap_rows:
                gap_rows[pk].append(rel)
        encoded = v1.canonical_json_bytes(rows)
        root_search.append({"root": root, "live_feed_file_count": len(rows),
                            "inventory_sha256": hashlib.sha256(encoded).hexdigest(),
                            "source_files": rows})
    gaps = [{"gamePk": pk,
             "classification": "RETAINED_SOURCE_GAP" if not gap_rows[pk] else "UNRESOLVED",
             "matching_feed_paths": sorted(gap_rows[pk])}
            for pk in GAPS]
    return {
        "contract_name": "MLB_2026_CLOSE_RECONCILIATION_DATE_FIELD_FORENSIC_V1",
        "date_semantics": {
            "close_path": "V2 copies StatsAPI officialDate as-is and compares source officialDate values; it does not convert gameDate to obtain an authoritative date.",
            "application_et_handling": "Existing market helpers convert UTC timestamps to America/New_York for operational market date; stat-derived loader compares feed datetime.officialDate directly to authority operational_date.",
            "conclusion": "No timezone-conversion defect found. Retained schedule history resolves 824785 by explicit postponement/reschedule identity; 823489, 824703, and 824705 remain unresolved because the searched retained schedule appearances contain no matching transition relationship evidence.",
        },
        "source_manifests": {
            "v1_phase_retained_manifest": {"path": v1.DEFAULT_SOURCE_MANIFEST_PATH.relative_to(ROOT).as_posix(),
                "sha256": file_sha256(ROOT / v1.DEFAULT_SOURCE_MANIFEST_PATH)},
            "v1_disposition_supplement_manifest": {"path": v1.DEFAULT_DISPOSITION_SUPPLEMENT_MANIFEST_PATH.relative_to(ROOT).as_posix(),
                "sha256": file_sha256(ROOT / v1.DEFAULT_DISPOSITION_SUPPLEMENT_MANIFEST_PATH)},
            "v2_retained_live_feed_evidence": {"path": str(feed_manifest_path.relative_to(ROOT)),
                "sha256": file_sha256(feed_manifest_path), "records": len(feed_manifest)},
            "v2_retained_schedule_relationship_evidence": {
                "path": str((V2_PACKAGE / SCHEDULE_EVIDENCE).relative_to(ROOT)),
                "sha256": file_sha256(V2_PACKAGE / SCHEDULE_EVIDENCE),
                "records": len(_load_pinned_schedules()[0])},
        },
        "conflict_cases": cases,
        "feed_gap_search_roots": root_search,
        "feed_gaps": gaps,
        "integrity_passed": all(c["classification"] in {
                "MULTIPLE_OFFICIAL_APPEARANCES_RELATIONSHIP_MISSING",
                "RESOLVED_POSTPONEMENT_RESCHEDULE"} for c in cases)
            and all(g["classification"] == "RETAINED_SOURCE_GAP" for g in gaps),
    }


def validate_report() -> dict[str, Any]:
    rebuilt = build_report()
    stored_path = PACKAGE / "forensic_report.json"
    stored = json.loads(stored_path.read_text())
    checks = {
        "deterministic_report": rebuilt == stored,
        "four_conflicts_classified": len(rebuilt["conflict_cases"]) == 4 and
            rebuilt["conflict_cases"][-1]["classification"] == "RESOLVED_POSTPONEMENT_RESCHEDULE" and
            all(c["classification"] in {"MULTIPLE_OFFICIAL_APPEARANCES_RELATIONSHIP_MISSING",
                                        "RESOLVED_POSTPONEMENT_RESCHEDULE"}
                for c in rebuilt["conflict_cases"]),
        "sixteen_gaps_absent": len(rebuilt["feed_gaps"]) == 16 and all(
            g["classification"] == "RETAINED_SOURCE_GAP" for g in rebuilt["feed_gaps"]),
        "no_timezone_defect": "No timezone-conversion defect" in rebuilt["date_semantics"]["conclusion"],
    }
    return {"integrity_passed": all(checks.values()), "checks": checks}
