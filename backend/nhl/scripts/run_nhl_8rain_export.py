#!/usr/bin/env python3
"""Run the governed raw NHL 8rain export sequence for one explicit package."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.scripts.export_nhl_8rain_upload import verify_package_manifest


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "bin" / "python"
OBSERVATION_ROOT = ROOT / "artifacts/operational/nhl/odds_observations"
CATALOG_ROOT = ROOT / "artifacts/operational/nhl/8rain_catalog"
CROSS_MARKET_ROOT = ROOT / "artifacts/operational/nhl/cross_market_shadow"

PACKAGE_REQUIRED_FILES = (
    "SHA256SUMS", "daily_execution_status.json", "schedule_event_identity.csv",
    "v2_immutable_predictions.csv", "puck_line_v1_immutable_predictions.csv",
    "raw_market_response.json",
)


def resolve_package_argument(cli_package: str | None, env_package: str | None) -> Path:
    value = cli_package or env_package
    if not value:
        raise ValueError("PACKAGE_REQUIRED: pass --package or set NHL_CROSS_MARKET_PACKAGE")
    return Path(value).expanduser().resolve()


def current_et_slate() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


def _manifest_names(package: Path) -> set[str]:
    manifest = package / "SHA256SUMS"
    names: set[str] = set()
    for line in manifest.read_text(encoding="utf-8").splitlines():
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            raise ValueError("PACKAGE_MANIFEST_MALFORMED")
        names.add(parts[1].lstrip("* "))
    return names


def _capture_timestamp(status: dict) -> datetime:
    value = pd.to_datetime(status.get("run_timestamp_utc"), utc=True, errors="coerce")
    if pd.isna(value):
        raise ValueError("CAPTURE_TIMESTAMP_MISSING_OR_INVALID")
    return value.to_pydatetime()


def validate_latest_capture_package(package: Path, *, current_slate: str) -> datetime:
    """Validate a successful operational capture and its required reference outputs."""
    package = Path(package).expanduser().resolve()
    manifest_hash, state_hash = validate_package(package, current_slate=current_slate)
    del manifest_hash
    status = json.loads((package / "daily_execution_status.json").read_text(encoding="utf-8"))
    run_type = package.parent.name.removeprefix("run_type=")
    if run_type not in {"MIDDAY", "FINAL_PREGAME", "REFRESH"}:
        raise ValueError(f"CAPTURE_PACKAGE_PATH_RUN_TYPE_NOT_OPERATIONAL:{run_type}")
    if package.name != f"state={state_hash}":
        raise ValueError("CAPTURE_PACKAGE_PATH_STATE_MISMATCH")
    if status.get("run_type") != run_type:
        raise ValueError(f"CAPTURE_STATUS_RUN_TYPE_MISMATCH:{status.get('run_type')}")
    capture_states = {
        str(status.get(key) or "").upper()
        for key in ("status", "classification", "completion_status")
    }
    if capture_states & {
        "FAILED", "FAILED_CLOSED", "ABORTED", "INCOMPLETE", "NOOP",
        "PARTIAL", "PARTIAL_CAPTURE", "NOT_COMPLETE",
    }:
        raise ValueError("CAPTURE_STATUS_FAILED")
    if status.get("mode") != "SHADOW_RESEARCH_ONLY":
        raise ValueError(f"CAPTURE_STATUS_NOT_COMPLETE:{status.get('mode')}")
    if any(int(status.get(key) or 0) <= 0 for key in (
        "scheduled_games", "v2_predictions_created", "puck_line_v1_predictions_created",
    )):
        raise ValueError("CAPTURE_REFERENCE_OUTPUT_COUNTS_EMPTY")
    required_manifest_entries = set(PACKAGE_REQUIRED_FILES) - {"SHA256SUMS"}
    missing_manifest_entries = sorted(required_manifest_entries - _manifest_names(package))
    if missing_manifest_entries:
        raise ValueError(
            "CAPTURE_REQUIRED_FILES_NOT_MANIFESTED:" + ",".join(missing_manifest_entries))
    schedule = pd.read_csv(package / "schedule_event_identity.csv")
    if not {"game_id", "game_date"}.issubset(schedule.columns):
        raise ValueError("CAPTURE_CANONICAL_SLATE_SCHEMA_INVALID")
    if "slate_date" in schedule and sorted(
        schedule.slate_date.dropna().astype(str).unique()) != [current_slate]:
        raise ValueError("CAPTURE_CANONICAL_SLATE_DATE_MISMATCH")
    if "canonical_season" in schedule and set(
        pd.to_numeric(schedule.canonical_season, errors="coerce").dropna().astype(int)
    ) != {2026}:
        raise ValueError("CAPTURE_CANONICAL_SEASON_MISMATCH")
    schedule_ids = set(pd.to_numeric(schedule.game_id, errors="coerce").dropna().astype(int))
    if not schedule_ids or schedule.game_id.duplicated().any():
        raise ValueError("CAPTURE_CANONICAL_SLATE_EMPTY_OR_DUPLICATED")
    if len(schedule_ids) != int(status["scheduled_games"]):
        raise ValueError("CAPTURE_CANONICAL_SLATE_COUNT_MISMATCH")
    for filename, count_key in (
        ("v2_immutable_predictions.csv", "v2_predictions_created"),
        ("puck_line_v1_immutable_predictions.csv", "puck_line_v1_predictions_created"),
    ):
        frame = pd.read_csv(package / filename)
        if frame.empty or "game_id" not in frame or frame.game_id.isna().any():
            raise ValueError(f"CAPTURE_REFERENCE_OUTPUT_EMPTY_OR_MALFORMED:{filename}")
        prediction_ids = set(pd.to_numeric(frame.game_id, errors="coerce").dropna().astype(int))
        if frame.game_id.duplicated().any() or not prediction_ids.issubset(schedule_ids):
            raise ValueError(f"CAPTURE_REFERENCE_OUTPUT_IDENTITY_INVALID:{filename}")
        if len(frame) != int(status[count_key]):
            raise ValueError(f"CAPTURE_REFERENCE_OUTPUT_COUNT_MISMATCH:{filename}")
        if "slate_date" in frame and sorted(frame.slate_date.dropna().astype(str).unique()) != [current_slate]:
            raise ValueError(f"CAPTURE_REFERENCE_OUTPUT_SLATE_MISMATCH:{filename}")
    return _capture_timestamp(status)


# Backward-compatible name retained for callers/tests from the earlier selector.
validate_latest_refresh_package = validate_latest_capture_package


def select_latest_capture_package(
    *, current_slate: str, cross_market_root: Path = CROSS_MARKET_ROOT,
) -> tuple[Path, list[str]]:
    """Select the newest valid operational capture for this ET slate."""
    slate_root = Path(cross_market_root) / "season=2026" / f"slate_date={current_slate}"
    candidates = sorted(
        path for run_type in ("MIDDAY", "FINAL_PREGAME", "REFRESH")
        for path in (slate_root / f"run_type={run_type}").glob("state=*")
        if path.is_dir()
    )
    if not candidates:
        raise ValueError(f"CURRENT_DAY_CAPTURE_PACKAGE_MISSING:{current_slate}")
    valid: list[tuple[datetime, Path]] = []
    skipped: list[str] = []
    for package in candidates:
        try:
            captured_at = validate_latest_capture_package(package, current_slate=current_slate)
        except Exception as error:
            skipped.append(f"{package}: {type(error).__name__}:{error}")
            continue
        valid.append((captured_at, package.resolve()))
    if not valid:
        reasons = " | ".join(skipped)
        raise ValueError(f"NO_VALID_CURRENT_DAY_CAPTURE_PACKAGE:{current_slate}:{reasons}")
    valid.sort(key=lambda item: (item[0], item[1].name))
    return valid[-1][1], skipped


# Backward-compatible Python API and CLI flag naming from the original workflow.
select_latest_refresh_package = select_latest_capture_package


def resolve_package_selection(
    cli_package: str | None, *, latest_refresh: bool = False, latest_capture: bool = False,
    env_package: str | None,
    current_slate: str, cross_market_root: Path = CROSS_MARKET_ROOT,
) -> tuple[Path, list[str]]:
    """Apply CLI precedence and return the resolved package plus operator notices."""
    notices: list[str] = []
    if cli_package:
        if latest_refresh or latest_capture:
            notices.append("An automatic selector and --package were both supplied; explicit --package wins.")
        return resolve_package_argument(cli_package, env_package), notices
    if latest_refresh or latest_capture:
        package, skipped = select_latest_capture_package(
            current_slate=current_slate, cross_market_root=cross_market_root)
        notices.extend(f"Skipped invalid current-day capture: {item}" for item in skipped)
        return package, notices
    return resolve_package_argument(None, env_package), notices


def validate_package(package: Path, *, current_slate: str) -> tuple[str, str]:
    if not package.exists():
        raise ValueError(f"PACKAGE_NOT_FOUND:{package}")
    if not package.is_dir():
        raise ValueError(f"PACKAGE_NOT_DIRECTORY:{package}")
    required = (
        "SHA256SUMS", "daily_execution_status.json", "schedule_event_identity.csv",
        "v2_immutable_predictions.csv", "puck_line_v1_immutable_predictions.csv",
        "raw_market_response.json",
    )
    missing = [name for name in required if not (package / name).is_file()]
    if missing:
        raise ValueError(f"PACKAGE_REQUIRED_FILES_MISSING:{','.join(missing)}")
    try:
        package_manifest_sha256 = verify_package_manifest(package)
        status = json.loads((package / "daily_execution_status.json").read_text())
        schedule = pd.read_csv(package / "schedule_event_identity.csv")
    except SystemExit as error:
        raise ValueError(f"PACKAGE_MANIFEST_INVALID:{error}") from error
    except Exception as error:
        raise ValueError(f"PACKAGE_MALFORMED:{type(error).__name__}:{error}") from error
    slate = str(status.get("slate_date") or "")
    schedule_dates = sorted(schedule.game_date.dropna().astype(str).unique())
    if slate != current_slate or schedule_dates != [current_slate]:
        raise ValueError(
            f"PACKAGE_SLATE_MISMATCH:expected={current_slate}:status={slate}:schedule={schedule_dates}"
        )
    state_hash = str(status.get("substantive_state_sha256") or "")
    if not state_hash:
        raise ValueError("PACKAGE_STATE_IDENTITY_MISSING")
    return package_manifest_sha256, state_hash


def _run(command: list[str], *, label: str, cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    if result.returncode:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"{label}_FAILED:{detail}")
    return result


def _latest_observation(slate: str) -> Path:
    matches = sorted(OBSERVATION_ROOT.glob(f"season=*/slate_date={slate}/observation=*"))
    matches = [path for path in matches if path.is_dir()]
    if not matches:
        raise ValueError(f"CURRENT_SLATE_ODDS_OBSERVATION_MISSING:{slate}")
    return matches[-1].resolve()


def _json_stdout(result: subprocess.CompletedProcess[str], *, label: str) -> dict:
    try:
        value = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError) as error:
        raise RuntimeError(f"{label}_OUTPUT_NOT_JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError(f"{label}_OUTPUT_NOT_OBJECT")
    return value


def validate_export_report(export: dict, *, state_hash: str, manifest_hash: str) -> None:
    if export.get("package_state_sha256") != state_hash:
        raise RuntimeError("EXPORT_PACKAGE_STATE_MISMATCH")
    if export.get("package_manifest_sha256") != manifest_hash:
        raise RuntimeError("EXPORT_PACKAGE_MANIFEST_MISMATCH")
    validation = export.get("validation") or {}
    if (
        int(validation.get("rows", 0)) <= 0
        or int(validation.get("duplicate_rows", -1)) != 0
        or int(validation.get("pair_failures", -1)) != 0
        or int(validation.get("probability_pair_failures", -1)) != 0
    ):
        raise RuntimeError(f"EXPORT_VALIDATION_FAILED:{validation}")


def run_export(package: Path, *, current_slate: str | None = None,
               retained_catalog_dir: Path | None = None) -> dict:
    if not PYTHON.is_file():
        raise RuntimeError(f"QUALIFIED_PYTHON_MISSING:{PYTHON}")
    slate = current_slate or current_et_slate()
    package = Path(package).expanduser().resolve()
    package_manifest_sha256, state_hash = validate_package(package, current_slate=slate)
    print(f"Resolved package:\n{package}")
    print(f"Resolved state:\n{state_hash[:12]}")
    print(f"Slate:\n{slate}")

    if retained_catalog_dir is None:
        catalog_result = _run(
            [str(PYTHON), "backend/nhl/scripts/refresh_nhl_8rain_catalog.py"],
            label="CATALOG_REFRESH",
        )
        catalog_lines = (catalog_result.stdout or "").splitlines()
        if not catalog_lines:
            raise RuntimeError("CATALOG_REFRESH_PATH_MISSING")
        catalog_dir = Path(catalog_lines[0]).resolve()
    else:
        catalog_dir = Path(retained_catalog_dir).expanduser().resolve()
        if not catalog_dir.is_dir() or any(
            not (catalog_dir / name).is_file()
            for name in ("model_spec.json", "teams.json", "players.json")
        ):
            raise RuntimeError(f"RETAINED_CATALOG_INVALID:{catalog_dir}")
    if not catalog_dir.is_dir() or catalog_dir.parent != CATALOG_ROOT.resolve():
        raise RuntimeError(f"CATALOG_REFRESH_PATH_INVALID:{catalog_dir}")

    observation = _latest_observation(slate)
    names_csv = ROOT / "backend/nhl/exports/daily/names" / f"names_{slate}.csv"
    if not names_csv.is_file():
        raise ValueError(f"CURRENT_SLATE_CANONICAL_NAMES_MISSING:{names_csv}")
    output_dir = ROOT / "tmp/cards/nhl_8rain_raw" / slate
    combined_props = output_dir / f"nhl_8rain_raw_props_{slate}.csv"
    selection = _json_stdout(_run([
        str(PYTHON), "backend/nhl/scripts/select_nhl_points_saves_8rain_candidates.py",
        "--mode", "raw", "--slate-date", slate, "--names-csv", str(names_csv),
        "--package-dir", str(package), "--catalog-dir", str(catalog_dir),
        "--odds-observation-dir", str(observation), "--out-dir", str(output_dir),
        "--combined-props-csv", str(combined_props),
    ], label="RAW_REFERENCE_SELECTION"), label="RAW_REFERENCE_SELECTION")
    if selection.get("mode") != "RAW_PREDICTION_COLLECTION" or selection.get("challengers_included") is not False:
        raise RuntimeError("RAW_REFERENCE_SELECTION_CONTRACT_FAILED")

    archive_dir = ROOT / "artifacts/operational/nhl/8rain_uploads" / slate
    export = _json_stdout(_run([
        str(PYTHON), "backend/nhl/scripts/export_nhl_8rain_upload.py",
        "--package-dir", str(package), "--catalog-dir", str(catalog_dir),
        "--date", slate, "--props-csv", str(combined_props),
        "--immutable-output-dir", str(archive_dir),
    ], label="EIGHTRAIN_EXPORT"), label="EIGHTRAIN_EXPORT")
    validate_export_report(
        export, state_hash=state_hash, manifest_hash=package_manifest_sha256)
    csv_path = Path(export.get("csv_path", "")).resolve()
    lineage_path = csv_path.with_name(f"{csv_path.stem}_lineage.json")
    if not csv_path.is_file() or not lineage_path.is_file():
        raise RuntimeError("IMMUTABLE_EXPORT_ARTIFACT_MISSING")
    validation = export["validation"]

    mapping_exclusions = sum(
        int((selection.get("lanes", {}).get(lane) or {}).get("mapping_only_excluded_predictions", 0))
        for lane in ("points", "saves")
    ) + int((selection.get("sog") or {}).get("mapping_only_excluded_predictions", 0))
    market_rows = export.get("rows_by_market") or {}
    return {
        "slate": slate,
        "state_prefix": state_hash[:12],
        "phase": json.loads((package / "daily_execution_status.json").read_text()).get("run_type"),
        "rows": int(validation["rows"]),
        "pairs": int(validation["market_pairs"]),
        "markets": {
            "Moneyline": int(market_rows.get("h2h", 0)),
            "Puck Line": int(market_rows.get("spread", 0)),
            "SOG": int(market_rows.get("shots_on_goal", 0)),
            "Points": int(market_rows.get("points", 0)),
            "Saves": int(market_rows.get("saves", 0)),
        },
        "mapping_exclusions": mapping_exclusions,
        "csv_path": str(csv_path),
        "lineage_path": str(lineage_path),
        "validation": "PASS",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", help="Immutable current-slate cross-market package directory")
    parser.add_argument(
        "--latest-refresh", action="store_true",
        help="Resolve the newest valid successful current-ET-day operational capture (legacy alias)",
    )
    parser.add_argument(
        "--latest-capture", action="store_true",
        help="Resolve the newest valid successful current-ET-day operational capture",
    )
    parser.add_argument(
        "--retained-catalog-dir", type=Path,
        help="Use an existing local catalog package instead of refreshing the public catalog",
    )
    args = parser.parse_args(argv)
    try:
        slate = current_et_slate()
        package, notices = resolve_package_selection(
            args.package, latest_refresh=args.latest_refresh, latest_capture=args.latest_capture,
            env_package=os.environ.get("NHL_CROSS_MARKET_PACKAGE"), current_slate=slate,
        )
        for notice in notices:
            print(notice, file=sys.stderr)
        summary = run_export(
            package, current_slate=slate, retained_catalog_dir=args.retained_catalog_dir)
    except Exception as error:
        print(f"NHL_8RAIN_EXPORT_FAILED: {error}", file=sys.stderr)
        return 1
    print("NHL 8RAIN EXPORT READY")
    print(f"Slate: {summary['slate']}")
    print(f"Package: {summary['state_prefix']}")
    print(f"Phase: {summary['phase']}")
    print(f"Rows: {summary['rows']}")
    print(f"Pairs: {summary['pairs']}")
    for market, rows in summary["markets"].items():
        print(f"{market}: {rows}")
    print(f"Mapping exclusions: {summary['mapping_exclusions']}")
    print(f"Validation: {summary['validation']}")
    print(f"CSV: {summary['csv_path']}")
    print(f"Lineage: {summary['lineage_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
