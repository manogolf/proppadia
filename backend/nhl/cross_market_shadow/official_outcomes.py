"""Read and verify immutable official outcomes for the cross-market revision lane."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from backend.nhl.official_request_journal import (
    canonical_game_set_hash,
    request_run_tree_fingerprint,
)


FINAL_STATES = {"FINAL", "OFF"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_manifest(package: Path) -> set[str]:
    manifest = package / "SHA256SUMS"
    if not manifest.is_file():
        raise ValueError("CROSS_MARKET_OUTCOME_MANIFEST_MISSING")
    seen: set[str] = set()
    for line in manifest.read_text().splitlines():
        digest, name = line.split("  ", 1)
        if name in seen:
            raise ValueError("CROSS_MARKET_OUTCOME_MANIFEST_DUPLICATE")
        seen.add(name)
        target = package / name
        if not target.is_file() or _sha256(target) != digest:
            raise ValueError(f"CROSS_MARKET_OUTCOME_MANIFEST_MISMATCH:{name}")
    required = {"canonical_admitted_slate.csv", "canonical_game_outcomes.csv", "summary.json"}
    if not required.issubset(seen):
        raise ValueError("CROSS_MARKET_OUTCOME_MANIFEST_INCOMPLETE")
    actual = {path.name for path in package.iterdir() if path.is_file()
              and path.name not in {"SHA256SUMS", "RUN_COMPLETE.json"}}
    if actual != seen:
        raise ValueError("CROSS_MARKET_OUTCOME_MANIFEST_FILE_SET_MISMATCH")
    complete_path = package / "RUN_COMPLETE.json"
    summary = json.loads((package / "summary.json").read_text())
    complete = json.loads(complete_path.read_text()) if complete_path.is_file() else {}
    if (complete.get("status") != "COMPLETE" or summary.get("status") != "COMPLETE"
            or complete.get("substantive_identity") != summary.get("substantive_identity")
            or summary.get("slate_date") != package.parent.name):
        raise ValueError("CROSS_MARKET_OUTCOME_RUN_INCOMPLETE")
    return seen


def _verify_preserved_request(package: Path, outcome_root: Path, slate_date: str,
                              game_ids: set[int]) -> list[dict[str, Any]]:
    journal_path = package / "official_request_journal.jsonl"
    rows = [json.loads(line) for line in journal_path.read_text().splitlines() if line.strip()]
    def valid_direct(row: dict[str, Any]) -> bool:
        return bool(
            row.get("event_kind") == "NETWORK_ATTEMPT"
            and row.get("authority_boundary")
            and row.get("http_status") == 200
            and row.get("final_disposition") == "SUCCESS"
            and row.get("response_preserved")
        )

    def valid_reuse(row: dict[str, Any]) -> bool:
        return bool(
            row.get("event_kind") == "PRESERVED_RESPONSE_REUSE"
            and row.get("cross_run_reuse")
            and row.get("source_role") == "AUTHORITY_RESPONSE_SOURCE"
            and row.get("source_run_id")
            and row.get("source_journal_sha256")
            and row.get("source_response_index_sha256")
            and row.get("source_response_object_sha256") == row.get("response_sha256")
            and row.get("http_status") == 200
            and row.get("final_disposition") == "PRESERVED_RESPONSE_REUSE"
            and row.get("response_preserved")
        )

    candidates = [row for row in rows if row.get("endpoint_family") in {"SCHEDULE", "BOXSCORE"}
                  and row.get("resource_identity", {}).get("slate_date") == slate_date
                  and (valid_direct(row) or valid_reuse(row))]
    unique_candidates: dict[tuple[str, str], dict[str, Any]] = {}
    for row in candidates:
        key = (str(row["endpoint_family"]), json.dumps(
            row.get("resource_identity") or {}, sort_keys=True, separators=(",", ":")))
        previous = unique_candidates.get(key)
        if previous is not None and previous.get("response_sha256") != row.get("response_sha256"):
            raise ValueError("CROSS_MARKET_OFFICIAL_REQUEST_RESPONSE_CONFLICT")
        unique_candidates.setdefault(key, row)
    selected_candidate_ids = {id(row) for row in unique_candidates.values()}
    rows = [row for row in rows if row.get("endpoint_family") not in {"SCHEDULE", "BOXSCORE"}
            or id(row) in selected_candidate_ids]
    candidates = list(unique_candidates.values())
    reused = [row for row in candidates if valid_reuse(row)]
    if reused:
        source_ids = {str(row["source_run_id"]) for row in reused}
        if len(source_ids) != 1:
            raise ValueError("CROSS_MARKET_OFFICIAL_REQUEST_RUN_MISMATCH")
        source_id = next(iter(source_ids))
        receipt_path = outcome_root / "acquisition_receipts" / slate_date / f"{source_id}.json"
        source_root = outcome_root / "request_runs" / slate_date / source_id
        try:
            receipt = json.loads(receipt_path.read_text())
            if (receipt.get("contract_version") != "NHL_AUTHORITY_ROSTER_ACQUISITION_V1"
                    or receipt.get("status") != "COMPLETE"
                    or receipt.get("run_id") != source_id
                    or receipt.get("slate_date") != slate_date
                    or set(map(int, receipt.get("game_ids") or [])) != game_ids):
                raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_SOURCE_RECEIPT_INVALID")
            source_journal_path = source_root / "official_request_journal.jsonl"
            source_cache = source_root / "preserved_responses"
            if (not source_journal_path.is_file() or not source_cache.is_dir()
                    or _sha256(source_journal_path) != receipt.get("journal_sha256")
                    or request_run_tree_fingerprint(
                        source_root, repository_root=outcome_root.parents[3]
                    ) != receipt.get("tree_fingerprint")):
                raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_SOURCE_INVALID")
            source_rows = [json.loads(line) for line in source_journal_path.read_text().splitlines()
                           if line.strip()]
            if any(row.get("run_id") != source_id for row in source_rows):
                raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_SOURCE_INVALID")
            expected_hash = canonical_game_set_hash(game_ids)
            expected_resources = {
                ("SCHEDULE", json.dumps({"slate_date": slate_date}, sort_keys=True,
                                         separators=(",", ":"))),
                *(('BOXSCORE', json.dumps(
                    {"slate_date": slate_date, "game_id": gid}, sort_keys=True,
                    separators=(",", ":"))) for gid in sorted(game_ids)),
            }
            source_responses = {}
            for source_row in source_rows:
                family = source_row.get("endpoint_family")
                identity = source_row.get("resource_identity") or {}
                resource = (family, json.dumps(identity, sort_keys=True, separators=(",", ":")))
                if resource not in expected_resources:
                    continue
                if (source_row.get("canonical_game_set_hash") != expected_hash
                        or not source_row.get("authority_boundary")
                        or source_row.get("event_kind") != "NETWORK_ATTEMPT"
                        or source_row.get("final_disposition") != "SUCCESS"
                        or source_row.get("http_status") != 200
                        or not source_row.get("response_preserved")):
                    continue
                token = hashlib.sha256(json.dumps(
                    {"endpoint_family": family, "identity": identity},
                    sort_keys=True, separators=(",", ":"),
                ).encode()).hexdigest()
                index_path = source_cache / "index" / f"{token}.json"
                if not index_path.is_file():
                    continue
                index = json.loads(index_path.read_text())
                obj_path = source_cache / "objects" / str(index.get("object_name") or "")
                if (index.get("endpoint_family") != family or index.get("identity") != identity
                        or not obj_path.is_file() or _sha256(obj_path) != source_row.get("response_sha256")
                        or index.get("response_sha256") != source_row.get("response_sha256")):
                    continue
                source_responses[resource] = {
                    "object_sha256": _sha256(obj_path),
                    "index_sha256": _sha256(index_path),
                    "response_bytes": obj_path.stat().st_size,
                }
            if set(source_responses) != expected_resources:
                raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_SOURCE_INVALID")
            for row in reused:
                identity_key = (row["endpoint_family"], json.dumps(
                    row["resource_identity"], sort_keys=True, separators=(",", ":")))
                source_response = source_responses.get(identity_key)
                if (source_response is None
                        or source_response["object_sha256"] != row.get("source_response_object_sha256")
                        or source_response["index_sha256"] != row.get("source_response_index_sha256")):
                    raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_REUSE_MISMATCH")
        except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
            if isinstance(error, ValueError) and str(error).startswith("CROSS_MARKET_"):
                raise
            raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_SOURCE_INVALID") from error

    schedule = [row for row in candidates if row.get("endpoint_family") == "SCHEDULE"]
    successful_boxes = [row for row in candidates if row.get("endpoint_family") == "BOXSCORE"]
    boxes = {int(row.get("resource_identity", {}).get("game_id", -1)): row
             for row in successful_boxes}
    if (len(schedule) != 1 or len(successful_boxes) != len(game_ids)
            or set(boxes) != game_ids):
        raise ValueError("CROSS_MARKET_OFFICIAL_REQUEST_EVIDENCE_INCOMPLETE")
    run_ids = {str(row.get("run_id") or "") for row in [schedule[0], *boxes.values()]}
    if len(run_ids) != 1 or not next(iter(run_ids)):
        raise ValueError("CROSS_MARKET_OFFICIAL_REQUEST_RUN_MISMATCH")
    request_root = outcome_root / "request_runs" / slate_date / next(iter(run_ids))
    for row in [schedule[0], *boxes.values()]:
        row_request_root = (
            outcome_root / "request_runs" / slate_date / str(row["source_run_id"])
            if valid_reuse(row) else request_root
        )
        cache = row_request_root / "preserved_responses"
        if not cache.is_dir():
            raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_CACHE_MISSING")
        identity = row.get("resource_identity") or {}
        token = hashlib.sha256(json.dumps(
            {"endpoint_family": row["endpoint_family"], "identity": identity},
            sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()
        index_path = cache / "index" / f"{token}.json"
        if not index_path.is_file():
            raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_INDEX_MISSING")
        index = json.loads(index_path.read_text())
        digest = row.get("response_sha256")
        object_path = cache / "objects" / str(index.get("object_name") or "")
        if (index.get("endpoint_family") != row.get("endpoint_family")
                or index.get("identity") != identity
                or index.get("response_sha256") != digest or not object_path.is_file()
                or _sha256(object_path) != digest):
            raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_HASH_MISMATCH")
        try:
            payload = json.loads(object_path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError("CROSS_MARKET_OFFICIAL_RESPONSE_INVALID_JSON") from error
        if row.get("endpoint_family") == "BOXSCORE":
            raw_id = payload.get("id") or payload.get("gameId") or payload.get("gamePk")
            if raw_id is None or int(raw_id) != int(identity.get("game_id", -1)):
                raise ValueError("CROSS_MARKET_OFFICIAL_BOXSCORE_IDENTITY_MISMATCH")
        elif row.get("endpoint_family") == "SCHEDULE":
            parent_dates = {str(day.get("date") or "") for day in payload.get("gameWeek", []) or []}
            top_dates = {str(game.get("gameDate") or "") for game in payload.get("games", []) or []}
            if identity.get("slate_date") not in parent_dates | top_dates:
                raise ValueError("CROSS_MARKET_OFFICIAL_SCHEDULE_DATE_MISMATCH")
    return rows


def _official_schedule_games(payload: dict[str, Any], slate_date: str) -> dict[int, dict[str, Any]]:
    games: dict[int, dict[str, Any]] = {}
    candidates: list[dict[str, Any]] = []
    for day in payload.get("gameWeek", []) or []:
        if str(day.get("date") or "") == slate_date:
            candidates.extend(day.get("games") or [])
    candidates.extend(game for game in payload.get("games", []) or []
                      if str(game.get("gameDate") or "") == slate_date)
    for raw in candidates:
        game_id = raw.get("id") or raw.get("gameId") or raw.get("gamePk")
        if game_id is None:
            raise ValueError("CROSS_MARKET_OFFICIAL_GAME_ID_MISSING")
        game_id = int(game_id)
        if game_id in games:
            if games[game_id] != raw:
                raise ValueError("CROSS_MARKET_OFFICIAL_SCHEDULE_DUPLICATE_CONFLICT")
            continue
        games[game_id] = raw
    return games


def load_official_outcomes(outcome_root: Path) -> pd.DataFrame:
    """Load verified reconciliation artifacts; never make an external request."""
    columns = ["canonical_season", "game_id", "game_type_code", "scheduled_start_time_utc",
               "home_team_id", "away_team_id", "final_home_goals", "final_away_goals",
               "game_status", "score_source", "score_status", "score_identity_qualified",
               "score_observed_at_utc", "outcome_artifact", "outcome_artifact_sha256",
               "outcome_request_journal", "official_request_run_id",
               "official_score_endpoint", "official_schedule_response_sha256",
               "official_schedule_response_path"]
    records: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    if not outcome_root.is_dir():
        return pd.DataFrame(columns=columns)
    for day_root in sorted(path for path in outcome_root.iterdir() if path.is_dir()):
        try:
            slate_date = pd.Timestamp(day_root.name).date().isoformat()
        except (ValueError, TypeError):
            continue
        packages = sorted(day_root.glob("reconciliation=*"))
        if not packages:
            continue
        if len(packages) != 1:
            raise ValueError("CROSS_MARKET_MULTIPLE_RECONCILIATIONS_FOR_SLATE")
        package = packages[0]
        manifest_files = _verify_manifest(package)
        slate = pd.read_csv(package / "canonical_admitted_slate.csv")
        if not pd.to_numeric(slate.game_type_code, errors="coerce").eq(2).any():
            continue
        if "official_request_journal.jsonl" not in manifest_files:
            raise ValueError("CROSS_MARKET_OFFICIAL_REQUEST_JOURNAL_NOT_PRESERVED")
        outcomes = pd.read_csv(package / "canonical_game_outcomes.csv")
        if slate.game_id.duplicated().any() or outcomes.game_id.duplicated().any():
            raise ValueError("CROSS_MARKET_DUPLICATE_OFFICIAL_OUTCOME_IDENTITY")
        if set(slate.game_id.astype(int)) != set(outcomes.game_id.astype(int)):
            raise ValueError("CROSS_MARKET_OUTCOME_CANONICAL_GAME_SET_MISMATCH")
        journal = _verify_preserved_request(
            package, outcome_root, slate_date, set(outcomes.game_id.astype(int)),
        )
        schedule_row = next(row for row in journal if row.get("endpoint_family") == "SCHEDULE"
                            and row.get("final_disposition") in {"SUCCESS", "PRESERVED_RESPONSE_REUSE"})
        identity = schedule_row["resource_identity"]
        token = hashlib.sha256(json.dumps(
            {"endpoint_family": "SCHEDULE", "identity": identity},
            sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()
        schedule_cache = (outcome_root / "request_runs" / slate_date /
                          str(schedule_row.get("source_run_id") or schedule_row["run_id"]) /
                          "preserved_responses")
        index = json.loads((schedule_cache /
                             "index" / f"{token}.json").read_text())
        raw_payload = json.loads((schedule_cache / "objects" /
                                  str(index["object_name"])).read_text())
        official = _official_schedule_games(raw_payload, slate_date)
        joined = slate.merge(outcomes, on=["canonical_season", "slate_date", "game_id",
                                           "game_type_code", "home_team_id", "away_team_id"],
                             how="inner", validate="one_to_one")
        if len(joined) != len(outcomes):
            raise ValueError("CROSS_MARKET_OUTCOME_IDENTITY_ORIENTATION_MISMATCH")
        for row in joined.itertuples(index=False):
            gid = int(row.game_id)
            if gid in seen_ids:
                raise ValueError("CROSS_MARKET_DUPLICATE_OFFICIAL_OUTCOME_EVIDENCE")
            seen_ids.add(gid)
            raw = official.get(gid)
            if raw is None:
                raise ValueError("CROSS_MARKET_OFFICIAL_SCHEDULE_GAME_MISSING")
            home, away = raw.get("homeTeam") or {}, raw.get("awayTeam") or {}
            state = str(raw.get("gameState") or raw.get("gameScheduleState") or "").upper()
            if state not in FINAL_STATES:
                raise ValueError("CROSS_MARKET_OFFICIAL_GAME_NOT_FINAL")
            if int(home.get("id") or -1) != int(row.home_team_id) or int(away.get("id") or -1) != int(row.away_team_id):
                raise ValueError("CROSS_MARKET_OFFICIAL_TEAM_ORIENTATION_MISMATCH")
            if pd.isna(row.official_final_home_goals) or pd.isna(row.official_final_away_goals):
                raise ValueError("CROSS_MARKET_OFFICIAL_OUTCOME_SCORE_MISSING")
            if (pd.isna(home.get("score")) or pd.isna(away.get("score"))
                    or int(home["score"]) < 0 or int(away["score"]) < 0
                    or int(home["score"]) == int(away["score"])
                    or int(home["score"]) != int(row.official_final_home_goals)
                    or int(away["score"]) != int(row.official_final_away_goals)):
                raise ValueError("CROSS_MARKET_OFFICIAL_SCORE_MISMATCH_OR_INVALID")
            observed = pd.to_datetime(row.outcome_source_timestamp_utc, utc=True, errors="coerce")
            if (pd.isna(observed) or str(row.outcome_source) != "NHL_OFFICIAL_GAMECENTER"
                    or not bool(row.official_final)
                    or str(row.outcome_conflict_status) != "NO_CONFLICT"):
                raise ValueError("CROSS_MARKET_OFFICIAL_OUTCOME_NOT_QUALIFIED")
            completion = json.loads((package / "RUN_COMPLETE.json").read_text())
            completed_at = pd.to_datetime(completion.get("completed_at_utc"), utc=True, errors="coerce")
            if pd.isna(completed_at) or observed > completed_at:
                raise ValueError("CROSS_MARKET_OUTCOME_CAPTURE_TIMESTAMP_MISMATCH")
            request_ends = pd.to_datetime(
                [request.get("request_end_utc") for request in journal
                 if request.get("resource_identity", {}).get("game_id") in (None, gid)
                 and request.get("caller_stage") == "POSTGAME_AUTHORITY"
                 and request.get("final_disposition") in {"SUCCESS", "PRESERVED_RESPONSE_REUSE"}],
                utc=True, errors="coerce",
            )
            if request_ends.isna().any() or observed < request_ends.max():
                raise ValueError("CROSS_MARKET_OUTCOME_OBSERVATION_PRECEDES_OFFICIAL_CAPTURE")
            records.append({
                "canonical_season": int(row.canonical_season), "game_id": gid,
                "game_type_code": int(row.game_type_code),
                "scheduled_start_time_utc": row.scheduled_start_time_utc,
                "home_team_id": int(row.home_team_id), "away_team_id": int(row.away_team_id),
                "final_home_goals": int(row.official_final_home_goals),
                "final_away_goals": int(row.official_final_away_goals),
                "game_status": state, "score_source": "OFFICIAL_NHL_FINAL_SCORE",
                "score_status": "QUALIFIED", "score_identity_qualified": True,
                "score_observed_at_utc": observed.isoformat(),
                "outcome_artifact": str(package / "canonical_game_outcomes.csv"),
                "outcome_artifact_sha256": _sha256(package / "canonical_game_outcomes.csv"),
                "outcome_request_journal": str(package / "official_request_journal.jsonl"),
                "official_request_run_id": str(
                    schedule_row.get("source_run_id") or schedule_row["run_id"]),
                "official_score_endpoint": f"/v1/schedule/{slate_date}",
                "official_schedule_response_sha256": str(schedule_row["response_sha256"]),
                "official_schedule_response_path": str(
                    schedule_cache / "objects" / str(index["object_name"])
                ),
            })
    return pd.DataFrame(records, columns=columns)
