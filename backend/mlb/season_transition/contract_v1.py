"""MLB 2026 regular-season close and postseason transition contract V1.

This module is deliberately pure and offline.  It never calls MLB, a database,
or a paid provider.  Runtime lanes may use it to classify already-retained
schedule records; the close command uses it to fail closed over a frozen input
inventory.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping


CONTRACT_NAME = "MLB_2026_REGULAR_SEASON_CLOSE_AND_POSTSEASON_DATA_PLAN_V1"
REPO_ROOT = Path(__file__).resolve().parents[3]
PHASES = frozenset({"PRESEASON", "REGULAR_SEASON", "POSTSEASON"})
POSTSEASON_ROUNDS = frozenset(
    {
        "WILD_CARD",
        "DIVISION_SERIES",
        "LEAGUE_CHAMPIONSHIP_SERIES",
        "WORLD_SERIES",
        "PLAYOFFS_UNSPECIFIED",
        "CHAMPIONSHIP_UNSPECIFIED",
    }
)

# MLB StatsAPI schedule.gameType / gameData.game.type semantics.  Special
# games are recognized but intentionally receive no normalized phase, keeping
# them out of both regular-season and postseason evaluation.
GAME_TYPE_CONTRACT: dict[str, dict[str, str | None]] = {
    "S": {"label": "Spring Training", "phase": "PRESEASON", "postseason_round": None},
    "E": {"label": "Exhibition", "phase": "PRESEASON", "postseason_round": None},
    "I": {"label": "Intrasquad", "phase": "PRESEASON", "postseason_round": None},
    "R": {"label": "Regular Season", "phase": "REGULAR_SEASON", "postseason_round": None},
    "F": {"label": "Wild Card / First Round", "phase": "POSTSEASON", "postseason_round": "WILD_CARD"},
    "D": {"label": "Division Series", "phase": "POSTSEASON", "postseason_round": "DIVISION_SERIES"},
    "L": {"label": "League Championship Series", "phase": "POSTSEASON", "postseason_round": "LEAGUE_CHAMPIONSHIP_SERIES"},
    "W": {"label": "World Series", "phase": "POSTSEASON", "postseason_round": "WORLD_SERIES"},
    "P": {"label": "Playoffs (round unspecified)", "phase": "POSTSEASON", "postseason_round": "PLAYOFFS_UNSPECIFIED"},
    "C": {"label": "Championship (round unspecified)", "phase": "POSTSEASON", "postseason_round": "CHAMPIONSHIP_UNSPECIFIED"},
    "A": {"label": "All-Star Game", "phase": None, "postseason_round": None},
    "N": {"label": "Nineteenth Century Series", "phase": None, "postseason_round": None},
}

RETROSHEET_TYPE_CONTRACT: dict[str, tuple[str, str | None]] = {
    "regular": ("REGULAR_SEASON", None),
    "exhibition": ("PRESEASON", None),
    "wildcard": ("POSTSEASON", "WILD_CARD"),
    "divisionseries": ("POSTSEASON", "DIVISION_SERIES"),
    "lcs": ("POSTSEASON", "LEAGUE_CHAMPIONSHIP_SERIES"),
    "worldseries": ("POSTSEASON", "WORLD_SERIES"),
    "playoff": ("POSTSEASON", "PLAYOFFS_UNSPECIFIED"),
    "championship": ("POSTSEASON", "CHAMPIONSHIP_UNSPECIFIED"),
    "allstar": ("", None),
}

REQUIRED_CLOSE_LANES = (
    "CANONICAL_SCHEDULE",
    "MONEYLINE",
    "RAW_TOTALS",
    "TOTALS_C",
    "FULL_BOARD_HITS",
    "BVP",
    "FEATURE_LINEAGE",
    "PINNACLE",
    "BETONLINE",
    "AGREEMENT_STUDY",
    "DAILY_REPORTS_OPS_INDEXES",
    "GRADING",
    "RESEARCH_EXPORT",
)

ALLOWED_CLOSE_DISPOSITIONS = frozenset(
    {
        "FINAL",
        "POSTPONED_AUTHORITATIVELY_DISPOSED",
        "CANCELLED",
        "EXPLICITLY_UNRESOLVED",
    }
)


class PhaseContractError(ValueError):
    """Raised when authoritative source phase is absent or conflicting."""


@dataclass(frozen=True)
class PhaseClassification:
    source: str
    raw_game_type: str
    source_label: str
    season: int
    phase: str | None
    postseason_round: str | None
    source_round: str | None
    season_name: str | None
    eligible_for_phase_evaluation: bool
    decision: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def normalize_source_game_type(
    raw_game_type: Any,
    *,
    season: int,
    source: str = "MLB_STATSAPI",
    source_round: Any = None,
) -> PhaseClassification:
    """Normalize an authoritative raw type without using calendar dates."""
    source_name = _clean(source).upper()
    raw = "" if raw_game_type is None else str(raw_game_type)
    if raw == "":
        raise PhaseContractError("AUTHORITATIVE_GAME_TYPE_MISSING")
    try:
        season_number = int(season)
    except (TypeError, ValueError):
        raise PhaseContractError("AUTHORITATIVE_SEASON_MISSING") from None
    if season_number <= 0:
        raise PhaseContractError("AUTHORITATIVE_SEASON_MISSING")
    if source_name not in {"MLB_STATSAPI", "RETROSHEET"}:
        raise PhaseContractError(f"UNKNOWN_GAME_TYPE_SOURCE:{source_name}")

    if source_name == "RETROSHEET":
        key = raw.casefold()
        if key not in RETROSHEET_TYPE_CONTRACT:
            raise PhaseContractError(f"UNKNOWN_RETROSHEET_GAME_TYPE:{raw}")
        phase, round_name = RETROSHEET_TYPE_CONTRACT[key]
        label = raw
        phase = phase or None
    else:
        # StatsAPI values are a one-character wire contract.  Do not trim,
        # case-fold, substitute, or otherwise mutate the retained source byte.
        key = raw
        if key not in GAME_TYPE_CONTRACT:
            raise PhaseContractError(f"UNKNOWN_STATSAPI_GAME_TYPE:{raw}")
        spec = GAME_TYPE_CONTRACT[key]
        phase = spec["phase"]
        round_name = spec["postseason_round"]
        label = str(spec["label"])

    raw_round = None if source_round is None else str(source_round)
    eligible = phase in PHASES
    decision = "CLASSIFIED_FROM_AUTHORITATIVE_SOURCE_TYPE" if eligible else "SPECIAL_GAME_EXCLUDED_FAIL_CLOSED"
    return PhaseClassification(
        source=source_name,
        raw_game_type=raw,
        source_label=label,
        season=season_number,
        phase=phase,
        postseason_round=round_name,
        source_round=raw_round,
        season_name=f"MLB_{season_number}_{phase}" if phase else None,
        eligible_for_phase_evaluation=eligible,
        decision=decision,
    )


def classify_schedule_game(game: Mapping[str, Any], *, season: int | None = None) -> PhaseClassification:
    """Classify one retained StatsAPI schedule/feed record and reject conflicts."""
    schedule_value = game.get("gameType")
    schedule_type = "" if schedule_value is None else str(schedule_value)
    game_data = game.get("gameData") or {}
    feed_value = (game_data.get("game") or {}).get("type")
    feed_type = "" if feed_value is None else str(feed_value)
    observed = {value for value in (schedule_type, feed_type) if value}
    if len(observed) > 1:
        raise PhaseContractError(f"CONFLICTING_AUTHORITATIVE_GAME_TYPES:{','.join(sorted(observed))}")
    raw_type = next(iter(observed), "")
    season_values = [
        value
        for value in (season, game.get("season"), (game_data.get("game") or {}).get("season"))
        if value not in (None, "")
    ]
    try:
        observed_seasons = {int(value) for value in season_values}
    except (TypeError, ValueError):
        raise PhaseContractError("AUTHORITATIVE_SEASON_MISSING") from None
    if not observed_seasons:
        raise PhaseContractError("AUTHORITATIVE_SEASON_MISSING")
    if len(observed_seasons) > 1:
        raise PhaseContractError(
            "CONFLICTING_AUTHORITATIVE_SEASONS:"
            + ",".join(str(value) for value in sorted(observed_seasons))
        )
    season_number = next(iter(observed_seasons))
    source_round = (
        game.get("seriesDescription")
        if "seriesDescription" in game
        else (game_data.get("game") or {}).get("typeDescription")
    )
    return normalize_source_game_type(
        raw_type,
        season=season_number,
        source="MLB_STATSAPI",
        source_round=source_round,
    )


def _check(checks: list[dict[str, Any]], code: str, passed: bool, detail: str) -> None:
    checks.append({"check": code, "status": "PASS" if passed else "FAIL", "detail": detail})


def validate_close_inventory(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a frozen, read-only season-close inventory.

    This validates evidence supplied by an inventory builder; it does not query
    live systems or infer phase from a date.
    """
    checks: list[dict[str, Any]] = []
    _check(checks, "contract_identity", payload.get("contract_name") == CONTRACT_NAME, str(payload.get("contract_name")))
    _check(checks, "season_identity", payload.get("season") == 2026, str(payload.get("season")))

    games = list(payload.get("canonical_regular_season_games") or [])
    _check(checks, "canonical_regular_population_nonempty", bool(games), f"rows={len(games)}")
    seen: set[int] = set()
    duplicate_games = 0
    invalid_games: list[str] = []
    for row in games:
        try:
            game_pk = int(row.get("game_pk"))
        except (TypeError, ValueError):
            invalid_games.append("missing_game_pk")
            continue
        if game_pk in seen:
            duplicate_games += 1
        seen.add(game_pk)
        try:
            phase = normalize_source_game_type(row.get("source_game_type"), season=2026)
        except PhaseContractError as exc:
            invalid_games.append(f"{game_pk}:{exc}")
            continue
        if phase.phase != "REGULAR_SEASON" or row.get("season_phase") != "REGULAR_SEASON":
            invalid_games.append(f"{game_pk}:NOT_REGULAR_SEASON")
        disposition = _clean(row.get("close_disposition")).upper()
        if disposition not in ALLOWED_CLOSE_DISPOSITIONS:
            invalid_games.append(f"{game_pk}:INVALID_DISPOSITION:{disposition}")
        if disposition == "POSTPONED_AUTHORITATIVELY_DISPOSED" and not _clean(row.get("authoritative_disposition_ref")):
            invalid_games.append(f"{game_pk}:POSTPONEMENT_DISPOSITION_REF_MISSING")
        if disposition == "EXPLICITLY_UNRESOLVED":
            if not _clean(row.get("unresolved_reason")) or not _clean(row.get("operator_acknowledgement")):
                invalid_games.append(f"{game_pk}:UNRESOLVED_ACKNOWLEDGEMENT_MISSING")
    _check(checks, "canonical_game_identity_unique", duplicate_games == 0, f"duplicates={duplicate_games}")
    _check(checks, "regular_game_phase_and_disposition", not invalid_games, "|".join(invalid_games) or "all rows valid")

    lanes = payload.get("lanes") or {}
    missing_lanes = [name for name in REQUIRED_CLOSE_LANES if name not in lanes]
    _check(checks, "all_required_lanes_present", not missing_lanes, ",".join(missing_lanes) or "all present")
    for name in REQUIRED_CLOSE_LANES:
        lane = lanes.get(name) or {}
        _check(checks, f"{name}:manifest", lane.get("manifest_status") == "PASS", str(lane.get("manifest_status")))
        _check(checks, f"{name}:ledger", lane.get("ledger_status") == "PASS", str(lane.get("ledger_status")))
        for counter in (
            "ungraded_eligible_predictions",
            "duplicate_prediction_identities",
            "duplicate_outcome_identities",
            "post_start_violations",
            "outcome_leakage_violations",
            "postseason_rows_in_regular_outputs",
        ):
            value = lane.get(counter)
            _check(checks, f"{name}:{counter}", value == 0, f"value={value!r}")

    required_sections = (
        "proper_scores",
        "bvp_acquisition_identity_status",
        "feature_lineage_health",
        "market_coverage",
        "agreement_study_progress",
        "api_credit_accounting",
        "outstanding_unresolved_rows",
        "model_qualification_publication_status",
        "source_config_identities",
    )
    for section in required_sections:
        present = section in payload
        populated = present and (section == "outstanding_unresolved_rows" or bool(payload.get(section)))
        detail = "populated" if populated else "empty" if present else "missing"
        _check(checks, f"freeze_section:{section}", populated, detail)

    identities = payload.get("source_config_identities") or []
    bad_hashes = [
        str(row.get("identity") or "unnamed")
        for row in identities
        if re.fullmatch(r"[0-9a-f]{64}", _clean(row.get("sha256"))) is None
    ]
    _check(checks, "source_config_sha256", bool(identities) and not bad_hashes, ",".join(bad_hashes) or f"rows={len(identities)}")

    qualification = payload.get("model_qualification_publication_status") or {}
    frozen_false = all(
        qualification.get(field) is False
        for field in ("model_promoted", "published", "wagering_authorized")
    )
    _check(checks, "no_promotion_publication_wagering", frozen_false, json.dumps(qualification, sort_keys=True))

    passed = all(row["status"] == "PASS" for row in checks)
    return {
        "contract_name": CONTRACT_NAME,
        "decision": "REGULAR_SEASON_CLOSE_AUTHORIZED" if passed else "REGULAR_SEASON_CLOSE_BLOCKED",
        "passed": passed,
        "check_count": len(checks),
        "failed_checks": [row["check"] for row in checks if row["status"] == "FAIL"],
        "checks": checks,
    }


def validate_fixture_suite(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    for source_file in payload.get("retained_source_files") or []:
        relative_path = Path(str(source_file.get("path") or ""))
        path = REPO_ROOT / relative_path
        expected_hash = _clean(source_file.get("sha256"))
        actual_hash = sha256_file(path) if path.is_file() else "MISSING"
        _check(
            checks,
            f"retained_source_sha256:{relative_path}",
            actual_hash == expected_hash,
            actual_hash,
        )

    inventory = payload.get("retained_inventory_expectations") or {}
    statsapi_types: Counter[str] = Counter()
    statsapi_statuses: Counter[str] = Counter()
    statsapi_relationships: Counter[str] = Counter()
    for relative in inventory.get("statsapi_schedule_paths") or []:
        schedule = json.loads((REPO_ROOT / str(relative)).read_text(encoding="utf-8"))
        for block in schedule.get("dates") or []:
            for game in block.get("games") or []:
                statsapi_types[_clean(game.get("gameType")) or "<MISSING>"] += 1
                statsapi_statuses[_clean((game.get("status") or {}).get("detailedState")) or "<MISSING>"] += 1
                for field in ("rescheduledFrom", "rescheduleDate", "resumeDate", "resumedFrom"):
                    if field in game:
                        statsapi_relationships[field] += 1
    for check_name, actual, expected in (
        ("retained_statsapi_game_types", statsapi_types, inventory.get("statsapi_game_types") or {}),
        ("retained_statsapi_statuses", statsapi_statuses, inventory.get("statsapi_detailed_states") or {}),
        ("retained_statsapi_relationships", statsapi_relationships, inventory.get("statsapi_relationship_fields") or {}),
    ):
        _check(checks, check_name, dict(actual) == expected, json.dumps(dict(actual), sort_keys=True))

    retrosheet_path = inventory.get("retrosheet_gameinfo_path")
    if retrosheet_path:
        with (REPO_ROOT / str(retrosheet_path)).open(newline="", encoding="utf-8-sig") as handle:
            retrosheet_rows = list(csv.DictReader(handle))
        retrosheet_types = Counter(_clean(row.get("gametype")) or "<MISSING>" for row in retrosheet_rows)
        suspended_count = sum(bool(_clean(row.get("suspend"))) for row in retrosheet_rows)
        _check(
            checks,
            "retained_retrosheet_game_types",
            dict(retrosheet_types) == (inventory.get("retrosheet_game_types") or {}),
            json.dumps(dict(retrosheet_types), sort_keys=True),
        )
        _check(
            checks,
            "retained_retrosheet_suspended_rows",
            suspended_count == inventory.get("retrosheet_suspended_rows"),
            str(suspended_count),
        )

    for case in payload.get("phase_cases") or []:
        case_id = str(case.get("case_id"))
        try:
            result = normalize_source_game_type(
                case.get("raw_game_type"),
                season=int(case.get("season")),
                source=str(case.get("source")),
                source_round=case.get("source_round"),
            )
            actual = {"phase": result.phase, "postseason_round": result.postseason_round, "decision": result.decision}
            passed = all(actual.get(key) == value for key, value in (case.get("expected") or {}).items())
            _check(checks, case_id, passed, json.dumps(actual, sort_keys=True))
        except (PhaseContractError, TypeError, ValueError) as exc:
            expected_error = str((case.get("expected") or {}).get("error") or "")
            _check(checks, case_id, bool(expected_error and str(exc).startswith(expected_error)), str(exc))

    mixed = payload.get("mixed_report_case") or {}
    regular_ids: list[int] = []
    for row in mixed.get("games") or []:
        try:
            result = normalize_source_game_type(row.get("raw_game_type"), season=int(row.get("season")), source=str(row.get("source", "MLB_STATSAPI")))
        except PhaseContractError:
            continue
        if result.phase == "REGULAR_SEASON":
            regular_ids.append(int(row["game_pk"]))
    _check(
        checks,
        "mixed_database_regular_report_exclusion",
        regular_ids == list(mixed.get("expected_regular_game_pks") or []),
        f"actual={regular_ids}",
    )

    strict_prior = payload.get("future_strict_prior_case") or {}
    event_phase = normalize_source_game_type(strict_prior.get("event_game_type"), season=int(strict_prior.get("season"))).phase
    available = event_phase == "POSTSEASON" and strict_prior.get("event_date") < strict_prior.get("feature_as_of_date")
    contaminates = event_phase == "POSTSEASON" and strict_prior.get("regular_evaluation_membership") is True
    _check(checks, "postseason_event_future_strict_prior_available", available, f"available={available}")
    _check(checks, "postseason_event_regular_evaluation_excluded", not contaminates, f"contaminates={contaminates}")

    premature_payload = dict(payload.get("premature_close_inventory") or {})
    if premature_payload.pop("fixture_fill_passing_lanes", False):
        clean_lane = {
            "manifest_status": "PASS",
            "ledger_status": "PASS",
            "ungraded_eligible_predictions": 0,
            "duplicate_prediction_identities": 0,
            "duplicate_outcome_identities": 0,
            "post_start_violations": 0,
            "outcome_leakage_violations": 0,
            "postseason_rows_in_regular_outputs": 0,
        }
        premature_payload["lanes"] = {name: dict(clean_lane) for name in REQUIRED_CLOSE_LANES}
        for section in (
            "proper_scores",
            "bvp_acquisition_identity_status",
            "feature_lineage_health",
            "market_coverage",
            "agreement_study_progress",
            "api_credit_accounting",
        ):
            premature_payload[section] = {"status": "FIXTURE_PASS"}
        premature_payload["outstanding_unresolved_rows"] = []
        premature_payload["model_qualification_publication_status"] = {
            "model_promoted": False,
            "published": False,
            "wagering_authorized": False,
        }
        premature_payload["source_config_identities"] = [
            {"identity": "deterministic_fixture", "sha256": "0" * 64}
        ]
    premature = validate_close_inventory(premature_payload)
    _check(checks, "regular_season_close_attempted_too_early", not premature["passed"], premature["decision"])
    _check(
        checks,
        "premature_close_has_active_game_failure",
        "regular_game_phase_and_disposition" in premature["failed_checks"],
        ",".join(premature["failed_checks"]),
    )

    passed = all(row["status"] == "PASS" for row in checks)
    canonical = json.dumps(checks, sort_keys=True, separators=(",", ":")).encode()
    return {
        "contract_name": CONTRACT_NAME,
        "passed": passed,
        "decision": "TRANSITION_CONTRACT_VALIDATED" if passed else "TRANSITION_CONTRACT_VALIDATION_FAILED",
        "checks": checks,
        "validation_sha256": hashlib.sha256(canonical).hexdigest(),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifest_lines(paths: Iterable[Path], *, root: Path) -> list[str]:
    return [f"{sha256_file(path)}  {path.resolve().relative_to(root.resolve())}" for path in sorted(paths)]
