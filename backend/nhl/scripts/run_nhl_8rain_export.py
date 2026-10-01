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


def resolve_package_argument(cli_package: str | None, env_package: str | None) -> Path:
    value = cli_package or env_package
    if not value:
        raise ValueError("PACKAGE_REQUIRED: pass --package or set NHL_CROSS_MARKET_PACKAGE")
    return Path(value).expanduser().resolve()


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


def run_export(package: Path) -> dict:
    if not PYTHON.is_file():
        raise RuntimeError(f"QUALIFIED_PYTHON_MISSING:{PYTHON}")
    slate = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    package_manifest_sha256, state_hash = validate_package(package, current_slate=slate)

    catalog_result = _run(
        [str(PYTHON), "backend/nhl/scripts/refresh_nhl_8rain_catalog.py"],
        label="CATALOG_REFRESH",
    )
    catalog_lines = (catalog_result.stdout or "").splitlines()
    if not catalog_lines:
        raise RuntimeError("CATALOG_REFRESH_PATH_MISSING")
    catalog_dir = Path(catalog_lines[0]).resolve()
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
    args = parser.parse_args(argv)
    try:
        package = resolve_package_argument(args.package, os.environ.get("NHL_CROSS_MARKET_PACKAGE"))
        summary = run_export(package)
    except Exception as error:
        print(f"NHL_8RAIN_EXPORT_FAILED: {error}", file=sys.stderr)
        return 1
    print("NHL 8RAIN EXPORT READY")
    print(f"Slate: {summary['slate']}")
    print(f"Package: {summary['state_prefix']}")
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
