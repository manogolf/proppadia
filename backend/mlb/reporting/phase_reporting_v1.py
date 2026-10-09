"""Fail-closed phase partitions for MLB operational reporting.

This module is deliberately read-only.  It joins reporting identities to the
canonical file authority by exact ``gamePk`` and exposes one shared status
payload for the Ops Brief and daily index.  It never infers phase from a date;
dates are used only to describe a subset after a row has been proven regular
season.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    HashedProposalAuthority,
    REPO_ROOT,
    load_v1_authority,
)
from backend.mlb.season_transition.regular_season_close_inventory_v1 import (
    CloseInventoryError,
    DEFAULT_PACKAGE_PATH as CLOSE_PACKAGE_PATH,
    INVENTORY_FILENAME,
    validate_close_inventory_package,
)


CONTRACT_NAME = "MLB_2026_OPS_BRIEF_AND_DAILY_INDEX_PHASE_GATING_V1"
LATE_SEASON_START = date(2026, 9, 1)
AGREEMENT_PACKAGE = REPO_ROOT / (
    "docs/contracts/mlb_2026_agreement_study_phase_gating_v1"
)
AGREEMENT_RECONCILIATION = AGREEMENT_PACKAGE / "retained_reconciliation.json"
AGREEMENT_POPULATION = AGREEMENT_PACKAGE / "retained_gamepk_population.csv"
AGREEMENT_STALENESS = AGREEMENT_PACKAGE / "summary_staleness_report.json"
AGREEMENT_MANIFEST = AGREEMENT_PACKAGE / "sha256_manifest.txt"


class PhaseReportingError(RuntimeError):
    """A reporting source cannot be safely assigned to a phase partition."""


@dataclass(frozen=True)
class PartitionedRows:
    regular_season: tuple[Mapping[str, Any], ...]
    late_season_regular_season: tuple[Mapping[str, Any], ...]
    postseason: tuple[Mapping[str, Any], ...]
    excluded_preseason: tuple[Mapping[str, Any], ...]

    def counts(self) -> dict[str, int]:
        return {
            "REGULAR_SEASON": len(self.regular_season),
            "LATE_SEASON_REGULAR_SEASON": len(self.late_season_regular_season),
            "POSTSEASON": len(self.postseason),
            "EXCLUDED_PRESEASON": len(self.excluded_preseason),
        }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise PhaseReportingError(f"REPORT_SOURCE_MISSING:{path}") from exc
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PhaseReportingError(f"REPORT_SOURCE_MISSING_OR_MALFORMED:{path}") from exc
    if not isinstance(value, dict):
        raise PhaseReportingError(f"REPORT_SOURCE_NOT_OBJECT:{path}")
    return value


def verify_sha256_manifest(package: Path, manifest_path: Path) -> dict[str, str]:
    """Verify every governed package entry without writing the package."""

    try:
        lines = manifest_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise PhaseReportingError(f"REPORT_MANIFEST_MISSING:{manifest_path}") from exc
    verified: dict[str, str] = {}
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        parts = line.split("  ", 1)
        if len(parts) != 2 or len(parts[0]) != 64:
            raise PhaseReportingError(
                f"REPORT_MANIFEST_MALFORMED:{manifest_path}:{line_number}"
            )
        expected, relative = parts
        path = package / relative
        actual = sha256_file(path)
        if actual != expected:
            raise PhaseReportingError(f"REPORT_MANIFEST_HASH_MISMATCH:{relative}")
        verified[relative] = actual
    if not verified:
        raise PhaseReportingError(f"REPORT_MANIFEST_EMPTY:{manifest_path}")
    return verified


def _exact_game_pk(row: Mapping[str, Any], field: str) -> int:
    value = row.get(field)
    if value is None or isinstance(value, bool):
        raise PhaseReportingError("REPORT_GAME_PK_MISSING")
    try:
        game_pk = int(str(value).strip())
    except (TypeError, ValueError):
        raise PhaseReportingError(f"REPORT_GAME_PK_INVALID:{value!r}") from None
    if game_pk <= 0 or str(value).strip() != str(game_pk):
        raise PhaseReportingError(f"REPORT_GAME_PK_INVALID:{value!r}")
    return game_pk


def partition_exact_game_pk_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    authority: CanonicalGamePhaseAuthority,
    game_pk_field: str = "gamePk",
    scheduled_date_field: str | None = None,
    identity_fields: Sequence[str] | None = None,
) -> PartitionedRows:
    """Partition rows using only an exact-gamePk authority lookup.

    Repeated rows for one game are legitimate for player and market ledgers.
    When ``identity_fields`` are supplied, duplicate row identities fail closed.
    Authority backends remain responsible for detecting duplicate or conflicting
    game identities in their own source population.
    """

    regular: list[Mapping[str, Any]] = []
    late: list[Mapping[str, Any]] = []
    postseason: list[Mapping[str, Any]] = []
    excluded: list[Mapping[str, Any]] = []
    seen_identities: set[tuple[str, ...]] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise PhaseReportingError("REPORT_ROW_INVALID")
        game_pk = _exact_game_pk(row, game_pk_field)
        if identity_fields:
            identity = tuple(str(row.get(field)) for field in identity_fields)
            if identity in seen_identities:
                raise PhaseReportingError(f"REPORT_DUPLICATE_IDENTITY:{identity!r}")
            seen_identities.add(identity)
        try:
            record = authority.lookup_exact(game_pk)
        except GamePhaseAuthorityError as exc:
            raise PhaseReportingError(f"REPORT_AUTHORITY_REJECTED:{exc}") from exc
        phase = record.season_phase
        if phase == "REGULAR_SEASON":
            regular.append(row)
            if scheduled_date_field:
                raw_date = str(row.get(scheduled_date_field) or "")[:10]
                try:
                    scheduled = date.fromisoformat(raw_date)
                except ValueError:
                    scheduled = None
                if scheduled is not None and scheduled >= LATE_SEASON_START:
                    late.append(row)
        elif phase == "POSTSEASON":
            postseason.append(row)
        elif phase == "PRESEASON":
            excluded.append(row)
        elif phase is None:
            raise PhaseReportingError(
                f"REPORT_AUTHORITY_SPECIAL_OR_UNCLASSIFIED:{game_pk}"
            )
        else:
            raise PhaseReportingError(f"REPORT_AUTHORITY_PHASE_UNKNOWN:{phase!r}")
    return PartitionedRows(tuple(regular), tuple(late), tuple(postseason), tuple(excluded))


def _agreement_control(authority: CanonicalGamePhaseAuthority) -> dict[str, Any]:
    verified = verify_sha256_manifest(AGREEMENT_PACKAGE, AGREEMENT_MANIFEST)
    reconciliation = _load_json(AGREEMENT_RECONCILIATION)
    staleness = _load_json(AGREEMENT_STALENESS)
    try:
        with AGREEMENT_POPULATION.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    except OSError as exc:
        raise PhaseReportingError(
            f"REPORT_SOURCE_MISSING:{AGREEMENT_POPULATION}"
        ) from exc
    partitioned = partition_exact_game_pk_rows(
        rows,
        authority=authority,
        game_pk_field="gamePk",
        scheduled_date_field="game_date",
        identity_fields=("gamePk",),
    )
    live_counts = reconciliation.get("live_counts")
    if not isinstance(live_counts, Mapping):
        raise PhaseReportingError("AGREEMENT_LIVE_COUNTS_MISSING")
    risk_rows = sum(int(row.get("risk_rows") or 0) for row in rows)
    if risk_rows != int(live_counts.get("risk_rows") or -1):
        raise PhaseReportingError("AGREEMENT_LEDGER_COUNT_MISMATCH")
    if staleness.get("decision") != "COMMITTED_DAILY_SUMMARY_STALE_DO_NOT_REWRITE":
        raise PhaseReportingError("AGREEMENT_SUMMARY_STALENESS_UNPROVEN")
    return {
        "authority": "LIVE_IMMUTABLE_LEDGER",
        "ledger_path": reconciliation.get("ledger", {}).get("path"),
        "ledger_sha256": reconciliation.get("ledger", {}).get("prechange_sha256"),
        "live_counts": dict(live_counts),
        "phase_game_pk_counts": partitioned.counts(),
        "phase_risk_row_counts": {
            "REGULAR_SEASON": risk_rows,
            "POSTSEASON": 0,
        },
        "historical_56_20": "PRESERVED_SEPARATE_NOT_PROSPECTIVE_EVIDENCE",
        "stale_summary": {
            "path": staleness.get("committed_summary_path"),
            "decision": staleness.get("decision"),
            "counts": staleness.get("committed_summary_counts"),
            "may_override_ledger": False,
        },
        "package_manifest_path": str(AGREEMENT_MANIFEST.relative_to(REPO_ROOT)),
        "package_manifest_sha256": sha256_file(AGREEMENT_MANIFEST),
        "verified_manifest_entries": len(verified),
    }


def _close_control(authority: HashedProposalAuthority) -> dict[str, Any]:
    # The close inventory is pinned to approved V1 authority.  Operational
    # reporting may use a later active snapshot, but it must not silently
    # widen or replace the frozen close population.
    close_authority = load_v1_authority()
    report = validate_close_inventory_package(authority=close_authority)
    inventory_path = CLOSE_PACKAGE_PATH / INVENTORY_FILENAME
    try:
        rows = [
            json.loads(line)
            for line in inventory_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except (OSError, json.JSONDecodeError) as exc:
        raise PhaseReportingError("CLOSE_INVENTORY_MISSING_OR_MALFORMED") from exc
    partitioned = partition_exact_game_pk_rows(
        rows,
        authority=close_authority,
        game_pk_field="game_pk",
        scheduled_date_field="scheduled_start",
        identity_fields=("game_pk",),
    )
    if partitioned.postseason or partitioned.excluded_preseason:
        raise PhaseReportingError("CLOSE_INVENTORY_PHASE_CONTAMINATION")
    late_dispositions = Counter(
        str(row.get("close_disposition"))
        for row in partitioned.late_season_regular_season
    )
    return {
        "decision": report["decision"],
        "authority_contract": "APPROVED_V1_CLOSE_AUTHORITY",
        "authority_proposal_sha256": close_authority.metadata.proposal_sha256,
        "check_only": report["check_only"],
        "close_package_created": report["close_package_created"],
        "canonical_regular_season_game_pks": report["expected_count"],
        "accepted_disposition_game_pks": (
            report["expected_count"]
            - len(report["scheduled_not_final_game_pks"])
            - len(report["unresolved_game_pks"])
        ),
        "outstanding_game_pks": len(report["scheduled_not_final_game_pks"]),
        "unresolved_game_pks": len(report["unresolved_game_pks"]),
        "disposition_counts": report["disposition_counts"],
        "manifest_sha256": report["manifest_sha256"],
        "inventory_path": str(inventory_path.relative_to(REPO_ROOT)),
        "inventory_sha256": sha256_file(inventory_path),
        "late_season_regular_season": {
            "definition": "authoritative REGULAR_SEASON and scheduled date >= 2026-09-01",
            "game_pks": len(partitioned.late_season_regular_season),
            "disposition_counts": dict(sorted(late_dispositions.items())),
            "certification_effect": "DESCRIPTIVE_ONLY_NOT_INDEPENDENTLY_CERTIFYING",
        },
    }


def build_current_phase_reporting_control(
    *,
    report_date: str,
    authority: HashedProposalAuthority | None = None,
) -> dict[str, Any]:
    """Build the shared, source-hashed Ops Brief/daily-index control payload."""

    try:
        authority = authority or HashedProposalAuthority()
        metadata = authority.metadata
    except GamePhaseAuthorityError as exc:
        raise PhaseReportingError(f"PHASE_AUTHORITY_INVALID:{exc}") from exc
    try:
        authority.require_supported_window(report_date, report_date)
        freshness = "CURRENT_FOR_REPORT_DATE"
    except GamePhaseAuthorityError as exc:
        raise PhaseReportingError(f"PHASE_AUTHORITY_STALE:{exc}") from exc
    try:
        close = _close_control(authority)
        agreement = _agreement_control(authority)
    except (GamePhaseAuthorityError, CloseInventoryError) as exc:
        raise PhaseReportingError(f"PHASE_REPORTING_EVIDENCE_INVALID:{exc}") from exc
    postseason_count = int(metadata.phase_counts.get("POSTSEASON", 0))
    classifications = {
        "regular_season_close_readiness": close["decision"],
        "late_season_regular_season_completeness": (
            "BLOCKED_OUTSTANDING_GAMES"
            if close["outstanding_game_pks"] or close["unresolved_game_pks"]
            else "COMPLETE_DESCRIPTIVE_ONLY"
        ),
        "postseason_collection_status": "CODE_READY_OPERATIONALLY_UNVALIDATED",
        "postseason_evaluation_status": (
            "NO_AUTHORITATIVE_POSTSEASON_EVIDENCE"
            if postseason_count == 0
            else "SEPARATE_SHADOW_PARTITION_AVAILABLE"
        ),
        "game_type_integrity": (
            "EXACT_GAME_PK_AUTHORITY_VERIFIED"
            if freshness == "CURRENT_FOR_REPORT_DATE"
            else "AUTHORITY_STALE_FAIL_CLOSED"
        ),
        "outstanding_games": str(close["outstanding_game_pks"]),
        "requests_and_credits": "REPORTING_USED_ZERO_REQUESTS_ZERO_PAID_CREDITS",
        "agreement_study_status": "SEPARATION_EVIDENCE_INSUFFICIENT",
        "model_selector_publication_status": "NO_QUALIFIED_MLB_MODEL_SELECTOR_RANKING_QUICK_CARD_UNAVAILABLE",
        "offseason_readiness": "BLOCKED_UNTIL_REGULAR_SEASON_CLOSE_CONTRACT_PASSES",
    }
    return {
        "contract_name": CONTRACT_NAME,
        "report_date": report_date,
        "authority": metadata.to_dict(),
        "authority_freshness": freshness,
        "canonical_population": {
            "total": metadata.proposal_count,
            "REGULAR_SEASON": int(metadata.phase_counts.get("REGULAR_SEASON", 0)),
            "POSTSEASON": postseason_count,
            "PRESEASON": int(metadata.phase_counts.get("PRESEASON", 0)),
            "missing": metadata.missing_count,
            "unknown": metadata.unknown_count,
            "conflicting": metadata.conflicting_count,
            "duplicate_identities": metadata.duplicate_identity_count,
        },
        "partitions": {
            "REGULAR_SEASON": "AUTHORITATIVE_ONLY",
            "LATE_SEASON_REGULAR_SEASON": "DESCRIPTIVE_SUBSET_OF_REGULAR_SEASON",
            "POSTSEASON": (
                "NO_AUTHORITATIVE_EVIDENCE"
                if postseason_count == 0
                else "SEPARATE_SHADOW_ONLY"
            ),
            "UNAVAILABLE_OR_BLOCKED": "FAIL_CLOSED",
        },
        "close": close,
        "agreement": agreement,
        "historical_56_20": "SEPARATE_HISTORICAL_COHORT_NOT_CURRENT_PROSPECTIVE_EVIDENCE",
        "prediction_quality_effect": "NONE_REPORTING_MEMBERSHIP_ONLY",
        "market_roi_effect": "NONE_NO_MARKET_OR_ROI_RECOMPUTATION",
        "classifications": classifications,
    }


def build_unavailable_phase_reporting_control(
    *, report_date: str, reason: str
) -> dict[str, Any]:
    """Return a bounded fail-closed status; never invent replacement counts."""

    return {
        "contract_name": CONTRACT_NAME,
        "report_date": report_date,
        "status": "UNAVAILABLE_OR_BLOCKED",
        "reason": reason,
        "partitions": {
            "REGULAR_SEASON": "UNAVAILABLE",
            "LATE_SEASON_REGULAR_SEASON": "UNAVAILABLE",
            "POSTSEASON": "UNAVAILABLE",
            "UNAVAILABLE_OR_BLOCKED": "FAIL_CLOSED",
        },
        "classifications": {
            "regular_season_close_readiness": "UNAVAILABLE_OR_BLOCKED",
            "late_season_regular_season_completeness": "UNAVAILABLE_OR_BLOCKED",
            "postseason_collection_status": "UNAVAILABLE_OR_BLOCKED",
            "postseason_evaluation_status": "UNAVAILABLE_OR_BLOCKED",
            "game_type_integrity": "FAIL_CLOSED",
            "outstanding_games": "UNAVAILABLE_OR_BLOCKED",
            "requests_and_credits": "REPORTING_USED_ZERO_REQUESTS_ZERO_PAID_CREDITS",
            "agreement_study_status": "UNAVAILABLE_OR_BLOCKED",
            "model_selector_publication_status": "NO_QUALIFIED_MLB_MODEL_SELECTOR_RANKING_QUICK_CARD_UNAVAILABLE",
            "offseason_readiness": "UNAVAILABLE_OR_BLOCKED",
        },
    }


def render_phase_reporting_markdown(control: Mapping[str, Any]) -> list[str]:
    """Render one identical control surface for both governed reports."""

    if control.get("status") == "UNAVAILABLE_OR_BLOCKED":
        return [
            "## Canonical Game Phase Control",
            "",
            "- Status: `UNAVAILABLE_OR_BLOCKED`",
            f"- Reason: `{control.get('reason') or 'UNKNOWN'}`",
            "- All regular-season, late-season, postseason, close, metric, market, ROI, and agreement totals are fail-closed.",
            "- Model/selector/publication: `NO_QUALIFIED_MLB_MODEL_SELECTOR_RANKING_QUICK_CARD_UNAVAILABLE`",
            "",
        ]
    population = control["canonical_population"]
    close = control["close"]
    late = close["late_season_regular_season"]
    agreement = control["agreement"]
    stale = agreement["stale_summary"]
    classifications = control["classifications"]
    lines = [
        "## Canonical Game Phase Control",
        "",
        f"- Authority: `{control['authority']['backend']}`; records hash `{control['authority']['authority_records_sha256']}`; freshness `{control['authority_freshness']}`.",
        f"- Population: `{population['total']}` total = `{population['REGULAR_SEASON']}` REGULAR_SEASON + `{population['PRESEASON']}` PRESEASON + `{population['POSTSEASON']}` POSTSEASON.",
        f"- Integrity exceptions: missing `{population['missing']}`, unknown `{population['unknown']}`, conflicting `{population['conflicting']}`, duplicate identities `{population['duplicate_identities']}`.",
        "- Partition rule: exact `gamePk` authority only; no date, month, filename, market, model, or status phase inference.",
        "- Unpartitioned aggregate metrics elsewhere in this report are `LEGACY_UNPARTITIONED_NOT_CERTIFICATION_EVIDENCE`; they cannot enter regular-season or postseason totals.",
        "",
        "### Regular-Season Close",
        "",
        f"- Checker: `{close['decision']}` (check-only `{str(close['check_only']).lower()}`; close package created `{str(close['close_package_created']).lower()}`).",
        f"- Exact canonical population: `{close['canonical_regular_season_game_pks']}`; accepted dispositions `{close['accepted_disposition_game_pks']}`; scheduled-not-final `{close['outstanding_game_pks']}`; unresolved `{close['unresolved_game_pks']}`.",
        f"- Manifest hash: `{close['manifest_sha256']}`.",
        "",
        "### Phase Partitions",
        "",
        f"- REGULAR_SEASON: `{population['REGULAR_SEASON']}` authoritative games; regular evaluation only.",
        f"- LATE_SEASON_REGULAR_SEASON: `{late['game_pks']}` games; {late['certification_effect']}; dispositions `{json.dumps(late['disposition_counts'], sort_keys=True)}`.",
        f"- POSTSEASON: `{population['POSTSEASON']}` authoritative games; `{control['partitions']['POSTSEASON']}`. Empty means no authoritative evidence, not omission or synthetic proof.",
        "- PRESEASON: excluded from both evaluation cohorts. Special/unclassified events: `UNAVAILABLE_OR_BLOCKED` and fail closed.",
        "",
        "### Agreement and Governance",
        "",
        f"- Current agreement authority: `{agreement['authority']}`; risk rows `{agreement['live_counts']['risk_rows']}`, outcomes `{agreement['live_counts']['outcomes']}`, price cells `{agreement['live_counts']['bookmaker_price_cells']}`.",
        f"- Committed summary: `{stale['decision']}`; path `{stale['path']}`; may override ledger `{str(stale['may_override_ledger']).lower()}`.",
        f"- Historical 56-20: `{control['historical_56_20']}`.",
        f"- Quality effect: `{control['prediction_quality_effect']}`; market/ROI effect: `{control['market_roi_effect']}`.",
        f"- Model/selector/publication: `{classifications['model_selector_publication_status']}`.",
        "",
    ]
    return lines


def render_phase_reporting_classification_footer(
    control: Mapping[str, Any],
) -> list[str]:
    """End each generated review with the ten required explicit decisions."""

    classifications = control.get("classifications")
    if not isinstance(classifications, Mapping):
        classifications = build_unavailable_phase_reporting_control(
            report_date=str(control.get("report_date") or ""),
            reason="CLASSIFICATIONS_MISSING",
        )["classifications"]
    ordered = (
        ("Regular-season close readiness", "regular_season_close_readiness"),
        ("Late-season regular-season completeness", "late_season_regular_season_completeness"),
        ("Postseason collection status", "postseason_collection_status"),
        ("Postseason evaluation status", "postseason_evaluation_status"),
        ("Game-type integrity", "game_type_integrity"),
        ("Outstanding games", "outstanding_games"),
        ("Requests and credits", "requests_and_credits"),
        ("Agreement-study status", "agreement_study_status"),
        ("Model/selector/publication status", "model_selector_publication_status"),
        ("Offseason readiness", "offseason_readiness"),
    )
    lines = ["", "## MLB Phase Reporting Classifications", ""]
    lines.extend(
        f"- {label}: `{classifications.get(key) or 'UNAVAILABLE_OR_BLOCKED'}`"
        for label, key in ordered
    )
    lines.append("")
    return lines
