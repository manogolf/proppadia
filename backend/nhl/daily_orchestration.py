"""Run-bound state and append-only receipts for the comprehensive NHL daily job."""
from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from backend.nhl.daily_capture import sha256_file, verify_package


UTC = timezone.utc
EASTERN = ZoneInfo("America/New_York")
RECEIPT_SCHEMA = "NHL_COMPREHENSIVE_DAILY_RUN_RECEIPT_V3"
DATABASE_WRITE_STATUSES = (
    "NONE", "COMMITTED", "ROLLED_BACK", "POSSIBLE_UNQUANTIFIED", "UNKNOWN",
)
_DATABASE_WRITE_STATUS_PRECEDENCE = {
    "NONE": 0,
    "ROLLED_BACK": 1,
    "UNKNOWN": 2,
    "POSSIBLE_UNQUANTIFIED": 3,
    "COMMITTED": 4,
}
LEGACY_SOG_TOI_REASON = (
    "LEGACY_SOG_SEASON_TOI_UNAVAILABLE_FOR_ROSTER_SKATER_WITHOUT_"
    "QUALIFYING_SAME_SEASON_SHIFT_HISTORY"
)
LANE_NAMES = (
    "shared_prerequisites",
    "roster",
    "legacy_sog",
    "sog_fixed_blend_shadows",
    "points",
    "points_hgb_shadow",
    "saves",
    "cold_start_sog_reference",
    "odds",
    "sog_attachment",
    "points_attachment",
    "saves_attachment",
    "research_integrity",
)

_URI_CREDENTIAL_RE = re.compile(
    r"\bpostgres(?:ql)?(?:\+[a-z0-9]+)?:\/\/[^\s'\"<>]+", re.I
)


def redact_sensitive_text(value: object) -> str:
    """Remove PostgreSQL connection URIs before diagnostic text reaches logs/receipts."""
    return _URI_CREDENTIAL_RE.sub("<redacted-db-uri>", str(value))


def safe_called_process_error(error: BaseException, command: Iterable[object] | None = None):
    """Copy subprocess failure metadata with command and streams safe to serialize."""
    import subprocess

    if not isinstance(error, subprocess.CalledProcessError):
        return RuntimeError(redact_sensitive_text(error))
    safe_command = redact_sensitive_text(
        " ".join(str(part) for part in (command if command is not None else error.cmd))
    )

    def safe_stream(stream):
        if stream is None:
            return None
        if isinstance(stream, bytes):
            return redact_sensitive_text(stream.decode("utf-8", errors="replace")).encode("utf-8")
        return redact_sensitive_text(stream)

    return subprocess.CalledProcessError(
        error.returncode,
        safe_command,
        output=safe_stream(error.output),
        stderr=safe_stream(error.stderr),
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _iso_et(value: datetime) -> str:
    return value.astimezone(EASTERN).isoformat()


def artifact_identity(path: Path) -> dict[str, Any]:
    path = Path(path)
    if not path.is_file():
        raise RuntimeError(f"CURRENT_RUN_ARTIFACT_MISSING:{path}")
    return {
        "path": str(path.resolve()),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
    }


def legacy_sog_toi_population_diagnostic(
    *, population_rows: int, null_5v5: int, null_season_5v5: int,
    maximum_null_ratio: float = 0.20,
) -> dict[str, Any]:
    population_rows = int(population_rows)
    null_5v5 = int(null_5v5)
    null_season_5v5 = int(null_season_5v5)
    if population_rows < 0 or min(null_5v5, null_season_5v5) < 0:
        raise ValueError("LEGACY_SOG_TOI_COUNTS_NEGATIVE")
    if max(null_5v5, null_season_5v5) > population_rows:
        raise ValueError("LEGACY_SOG_TOI_NULL_COUNT_EXCEEDS_POPULATION")
    ratio = max(null_5v5, null_season_5v5) / population_rows if population_rows else 0.0
    return {
        "population_rows": population_rows,
        "null_szn_toi_per_game_5on5": null_5v5,
        "null_season_5on5_icetime_per_game": null_season_5v5,
        "null_ratio": ratio,
        "maximum_null_ratio": float(maximum_null_ratio),
        "legacy_population_gate_would_block": (
            population_rows > 0 and ratio > float(maximum_null_ratio)
        ),
    }


@dataclass
class LaneResult:
    name: str
    blocking: bool
    status: str = "NOT_STARTED"
    reason: str | None = None
    started_at_utc: str | None = None
    ended_at_utc: str | None = None
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    # Compatibility projection: True means committed writes are proven, False
    # means zero committed writes are proven, and None means the status cannot
    # truthfully be reduced to a boolean.
    database_rows_written: bool | None = False
    database_write_status: str = "NONE"
    database_write_children: list[dict[str, Any]] = field(default_factory=list)
    database_stage_summary: dict[str, Any] = field(default_factory=dict)
    provider_requests: int = 0
    credits_consumed: int | None = 0
    error_type: str | None = None
    error_message: str | None = None


class DailyRunRecorder:
    """In-memory run ledger finalized as one create-only package."""

    def __init__(self, *, run_id: str, command: list[str], phase: str,
                 started_at: datetime | None = None) -> None:
        self.run_id = run_id
        self.command = list(command)
        self.phase = str(phase).upper()
        self.started_at = started_at or _utc_now()
        self.ended_at: datetime | None = None
        self.slate_date: str | None = None
        self.canonical_season: int | None = None
        self.canonical_game_ids: list[int] = []
        self.canonical_game_set_hash: str | None = None
        self.eligible_pregame_game_ids: list[int] = []
        self.started_excluded_game_ids: list[int] = []
        self.feature_input_cutoff_utc: str | None = None
        self.roster_observation: dict[str, Any] | None = None
        self.odds_observation: dict[str, Any] | None = None
        self.prior_learning: dict[str, Any] | None = None
        self.children: list[dict[str, Any]] = []
        self.failure: dict[str, Any] | None = None
        # Runtime-only continuation state. It is intentionally excluded from
        # the receipt because it contains live Python objects.
        self.independent_context: dict[str, Any] | None = None
        self.lanes = {
            name: LaneResult(name=name, blocking=name in {"shared_prerequisites", "roster"})
            for name in LANE_NAMES
        }

    def lane(self, name: str) -> LaneResult:
        if name not in self.lanes:
            raise KeyError(f"UNKNOWN_DAILY_LANE:{name}")
        return self.lanes[name]

    def start_lane(self, name: str, *, inputs: Iterable[Mapping[str, Any]] = ()) -> None:
        lane = self.lane(name)
        if lane.started_at_utc is None:
            lane.started_at_utc = _iso_utc(_utc_now())
        lane.status = "RUNNING"
        lane.inputs.extend(dict(value) for value in inputs)

    def finish_lane(
        self, name: str, *, status: str = "COMPLETE", reason: str | None = None,
        outputs: Iterable[Mapping[str, Any]] = (), database_rows_written: bool | None = None,
        database_stage_summary: Mapping[str, Any] | None = None,
        provider_requests: int | None = None, credits_consumed: int | None = None,
    ) -> None:
        lane = self.lane(name)
        if lane.started_at_utc is None:
            lane.started_at_utc = _iso_utc(_utc_now())
        lane.status = status
        lane.reason = reason
        lane.ended_at_utc = _iso_utc(_utc_now())
        lane.outputs.extend(dict(value) for value in outputs)
        if database_rows_written is not None:
            # Once child-level evidence exists it is authoritative. Legacy
            # boolean callers cannot upgrade POSSIBLE_UNQUANTIFIED to a false
            # claim of quantified committed writes.
            if not lane.database_write_children:
                requested_status = "COMMITTED" if database_rows_written else "NONE"
                self._merge_lane_database_write_status(lane, requested_status)
        if database_stage_summary is not None:
            lane.database_stage_summary.update(dict(database_stage_summary))
        if provider_requests is not None:
            lane.provider_requests = int(provider_requests)
        if credits_consumed is not None:
            lane.credits_consumed = int(credits_consumed)

    def fail_lane(self, name: str, error: BaseException, *, blocking: bool | None = None) -> None:
        lane = self.lane(name)
        if lane.started_at_utc is None:
            lane.started_at_utc = _iso_utc(_utc_now())
        if blocking is not None:
            lane.blocking = bool(blocking)
        lane.status = "FAILED_BLOCKING" if lane.blocking else "FAILED_NONBLOCKING"
        lane.reason = redact_sensitive_text(f"{type(error).__name__}:{error}")
        lane.error_type = type(error).__name__
        lane.error_message = redact_sensitive_text(error)
        lane.ended_at_utc = _iso_utc(_utc_now())

    def record_child(self, value: Mapping[str, Any]) -> None:
        # Only the explicit bounded contract is admitted to the parent receipt.
        allowed = {
            "lane", "command_identity", "exit_status", "duration_ms", "status",
            "schema_version", "normalizer_counts", "roster_observation",
            "roster_manifest_sha256", "database_stage_summary",
            "database_write_capable", "database_write_status",
            "transaction_disposition", "database_row_counts",
            "database_row_counts_complete", "error_type",
        }
        child = {key: value[key] for key in allowed if key in value}
        status = self._classify_child_database_write(child)
        child["database_write_status"] = status
        child.setdefault("database_write_capable", False)
        child.setdefault(
            "transaction_disposition",
            "UNKNOWN" if child["database_write_capable"] else "NOT_APPLICABLE",
        )
        child.setdefault("database_row_counts", {})
        child.setdefault("database_row_counts_complete", False)
        self.children.append(child)
        lane_name = child.get("lane")
        if lane_name in self.lanes and child.get("database_write_capable"):
            event = {
                key: child[key]
                for key in (
                    "command_identity", "exit_status", "transaction_disposition",
                    "database_write_status", "database_row_counts",
                )
            }
            event["database_row_counts_complete"] = child[
                "database_row_counts_complete"]
            event["lane"] = str(lane_name)
            lane = self.lane(str(lane_name))
            lane.database_write_children.append(event)
            self._merge_lane_database_write_status(lane, status)

    @staticmethod
    def _classify_child_database_write(child: Mapping[str, Any]) -> str:
        if not bool(child.get("database_write_capable")):
            return "NONE"
        explicit = child.get("database_write_status")
        if explicit is not None and explicit not in DATABASE_WRITE_STATUSES:
            raise ValueError(f"DATABASE_WRITE_STATUS_INVALID:{explicit}")
        exit_status = int(child.get("exit_status", 1))
        row_counts = child.get("database_row_counts")
        counts = []
        all_counts_quantified = isinstance(row_counts, Mapping) and bool(row_counts)
        if isinstance(row_counts, Mapping):
            for value in row_counts.values():
                if isinstance(value, bool):
                    all_counts_quantified = False
                    continue
                if isinstance(value, int) and value >= 0:
                    counts.append(value)
                else:
                    all_counts_quantified = False

        if exit_status == 0:
            if (
                child.get("database_row_counts_complete") is True
                and all_counts_quantified
                and all(value == 0 for value in counts)
            ):
                return "NONE"
            if counts and any(value > 0 for value in counts):
                return "COMMITTED"
            if explicit == "NONE":
                # NONE without affirmative zero counts is not proof of no write.
                return "POSSIBLE_UNQUANTIFIED"
            if explicit in {"ROLLED_BACK", "UNKNOWN"}:
                return explicit
            return "POSSIBLE_UNQUANTIFIED"

        if explicit == "ROLLED_BACK" or child.get("transaction_disposition") == "ROLLED_BACK":
            return "ROLLED_BACK"
        return "UNKNOWN"

    @staticmethod
    def _merge_lane_database_write_status(lane: LaneResult, status: str) -> None:
        if status not in DATABASE_WRITE_STATUSES:
            raise ValueError(f"DATABASE_WRITE_STATUS_INVALID:{status}")
        if _DATABASE_WRITE_STATUS_PRECEDENCE[status] > _DATABASE_WRITE_STATUS_PRECEDENCE[
                lane.database_write_status]:
            lane.database_write_status = status
        lane.database_rows_written = {
            "NONE": False,
            "ROLLED_BACK": False,
            "COMMITTED": True,
            "POSSIBLE_UNQUANTIFIED": None,
            "UNKNOWN": None,
        }[lane.database_write_status]

    def database_write_status(self) -> str:
        return max(
            (lane.database_write_status for lane in self.lanes.values()),
            key=_DATABASE_WRITE_STATUS_PRECEDENCE.__getitem__,
        )

    def set_canonical(self, *, slate_date: str, season: int,
                      game_ids: Iterable[int], game_set_hash: str) -> None:
        self.slate_date = slate_date
        self.canonical_season = int(season)
        self.canonical_game_ids = sorted({int(value) for value in game_ids})
        self.canonical_game_set_hash = game_set_hash

    def set_pregame_eligibility(self, *, eligible_game_ids: Iterable[int],
                                started_excluded_game_ids: Iterable[int],
                                cutoff_utc: str) -> None:
        self.eligible_pregame_game_ids = sorted(map(int, eligible_game_ids))
        self.started_excluded_game_ids = sorted(map(int, started_excluded_game_ids))
        self.feature_input_cutoff_utc = str(cutoff_utc)

    def classification(self) -> str:
        if any(lane.status == "FAILED_BLOCKING" for lane in self.lanes.values()):
            return "FAILED_BLOCKING"
        warning_statuses = {
            "BLOCKED_LANE_LOCAL", "FAILED_NONBLOCKING", "COMPLETE_WITH_BOUNDED_LIMITS",
            "SKIPPED_UPSTREAM_LANE_BLOCKED", "READY_WITH_ODDS_WARNING",
            "FAILED_NONBLOCKING_INTEGRITY",
        }
        if any(lane.status in warning_statuses for lane in self.lanes.values()):
            return "READY_WITH_BOUNDED_LANE_WARNING"
        return "READY"

    def payload(self) -> dict[str, Any]:
        ended = self.ended_at or _utc_now()
        return {
            "schema_version": RECEIPT_SCHEMA,
            "fitted_model_identity_contract": "NHL_FITTED_MODEL_IDENTITY_V1",
            "parent_daily_run_id": self.run_id,
            "command": self.command,
            "resolved_phase": self.phase,
            "started_at_utc": _iso_utc(self.started_at),
            "started_at_et": _iso_et(self.started_at),
            "ended_at_utc": _iso_utc(ended),
            "ended_at_et": _iso_et(ended),
            "operational_timezone": "America/New_York",
            "slate_date": self.slate_date,
            "canonical_season": self.canonical_season,
            "canonical_game_ids": self.canonical_game_ids,
            "canonical_game_count": len(self.canonical_game_ids),
            "canonical_game_set_hash": self.canonical_game_set_hash,
            "eligible_pregame_game_ids": self.eligible_pregame_game_ids,
            "eligible_pregame_game_count": len(self.eligible_pregame_game_ids),
            "started_excluded_game_ids": self.started_excluded_game_ids,
            "started_excluded_game_count": len(self.started_excluded_game_ids),
            "started_exclusion_reason": (
                "GAME_ALREADY_STARTED" if self.started_excluded_game_ids else None),
            "feature_cutoff_utc": self.feature_input_cutoff_utc,
            "lanes": {name: asdict(self.lanes[name]) for name in LANE_NAMES},
            "roster_observation": self.roster_observation,
            "odds_observation": self.odds_observation,
            "prior_day_learning": self.prior_learning,
            "child_summaries": self.children,
            "database_write_status": self.database_write_status(),
            "database_write_children": [
                event
                for name in LANE_NAMES
                for event in self.lanes[name].database_write_children
            ],
            "failure": self.failure,
            "final_classification": self.classification(),
        }

    def finalize(self, root: Path) -> Path:
        self.ended_at = self.ended_at or _utc_now()
        root = Path(root)
        final = root / f"run_id={self.run_id}"
        staging = root / f".run_id={self.run_id}.{uuid.uuid4().hex}.incomplete"
        if final.exists():
            raise RuntimeError(f"DAILY_RUN_RECEIPT_EXISTS:{final}")
        staging.mkdir(parents=True, exist_ok=False)
        try:
            receipt = staging / "parent_receipt.json"
            receipt.write_text(json.dumps(self.payload(), indent=2, sort_keys=True) + "\n")
            marker = staging / "RUN_COMPLETE.json"
            marker.write_text(json.dumps({
                "schema_version": RECEIPT_SCHEMA,
                "parent_daily_run_id": self.run_id,
                "final_classification": self.classification(),
                "completed_at_utc": _iso_utc(self.ended_at),
            }, indent=2, sort_keys=True) + "\n")
            files = sorted(path for path in staging.iterdir() if path.is_file())
            (staging / "SHA256SUMS").write_text(
                "".join(f"{sha256_file(path)}  {path.name}\n" for path in files)
            )
            verify_package(staging)
            root.mkdir(parents=True, exist_ok=True)
            staging.replace(final)
            return final
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise


def verify_roster_observation_reuse(
    path: Path, *, season: int, slate_date: str, phase: str,
    canonical_game_ids: Iterable[int], canonical_game_set_hash: str,
) -> dict[str, Any]:
    """Validate a retained roster package without modifying or relabeling it."""
    path = Path(path).resolve()
    manifest_sha256 = verify_package(path)
    marker_path = path / "RUN_COMPLETE.json"
    summary_path = path / "observation_summary.json"
    if not marker_path.is_file() or not summary_path.is_file():
        raise RuntimeError("ROSTER_REUSE_PACKAGE_INCOMPLETE")
    marker = json.loads(marker_path.read_text())
    summary = json.loads(summary_path.read_text())
    checks = {
        "complete_marker": marker.get("status") == "COMPLETE",
        "season": int(summary.get("season", -1)) == int(season),
        "slate_date": summary.get("slate_date") == slate_date,
        "phase": str(summary.get("phase", "")).upper() == str(phase).upper(),
        "canonical_game_set_hash": summary.get("canonical_game_set_hash") == canonical_game_set_hash,
        "canonical_game_ids": sorted(map(int, summary.get("canonical_game_ids") or []))
        == sorted({int(value) for value in canonical_game_ids}),
        "complete_per_game_coverage": summary.get("complete_per_game_coverage") is True,
        "strictly_prestart": summary.get("strictly_prestart") is True,
        "zero_conflicts": int(summary.get("conflict_count", -1)) == 0,
        "no_missing_teams": not summary.get("missing_teams"),
        "no_unexpected_teams": not summary.get("unexpected_teams"),
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    if failed:
        raise RuntimeError(f"ROSTER_REUSE_VALIDATION_FAILED:{','.join(failed)}")
    return {
        "path": str(path),
        "manifest_sha256": manifest_sha256,
        "source_parent_daily_run_id": summary.get("parent_daily_run_id"),
        "observation_timestamp_utc": summary.get("observation_timestamp_utc"),
        "snapshot_row_count": int(summary.get("snapshot_row_count", 0)),
        "canonical_game_set_hash": canonical_game_set_hash,
        "reuse_mode": "EXPLICIT_VERIFIED_IMMUTABLE_SOURCE",
    }
