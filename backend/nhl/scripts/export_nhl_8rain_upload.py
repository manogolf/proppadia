#!/usr/bin/env python3
"""Export NHL reference predictions and selected prop candidates to 8rain CSV."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.eightrain_adapter import (
    UPLOAD_COLUMNS, ambiguous_player_bindings, ambiguous_player_names, build_rows, load_catalogs,
    resolve_catalog_dir, validate_upload, classify_export_date,
    unique_player_codes_by_name,
)


def immutable_export_paths(
    output_dir: Path, *, slate_date: str, package_state_sha256: str,
    exported_at_et: datetime,
) -> tuple[Path, Path]:
    """Build an ET-timestamped CSV and adjacent lineage path for one package."""
    if exported_at_et.tzinfo is None:
        raise ValueError("EXPORT_TIMESTAMP_MUST_BE_TIMEZONE_AWARE")
    et_timestamp = exported_at_et.astimezone(ZoneInfo("America/New_York"))
    stamp = et_timestamp.strftime("%Y%m%dT%H%M%S%f%Z")
    state_prefix = re.sub(r"[^A-Za-z0-9]", "", str(package_state_sha256))[:12]
    if len(state_prefix) != 12:
        raise ValueError("PACKAGE_STATE_IDENTITY_INVALID")
    stem = f"nhl_8rain_raw_manual_upload_{slate_date}_{stamp}_{state_prefix}"
    output_dir = Path(output_dir)
    return output_dir / f"{stem}.csv", output_dir / f"{stem}_lineage.json"


def verify_package_manifest(package_dir: Path) -> str:
    manifest = package_dir / "SHA256SUMS"
    if not manifest.is_file():
        raise SystemExit("cross-market package SHA256SUMS is missing")
    expected: dict[str, str] = {}
    for line in manifest.read_text().splitlines():
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            raise SystemExit("cross-market package SHA256SUMS is malformed")
        expected[parts[1].lstrip("* ")] = parts[0]
    for name, digest in expected.items():
        path = package_dir / name
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise SystemExit(f"cross-market package integrity failure: {name}")
    if not expected:
        raise SystemExit("cross-market package SHA256SUMS is empty")
    return hashlib.sha256(manifest.read_bytes()).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--package-dir", required=True, type=Path,
                    help="Immutable cross-market REFRESH package for the slate")
    ap.add_argument("--catalog-dir", required=True, type=Path,
                    help="Directory containing model_spec.json, teams.json, players.json")
    ap.add_argument("--date", required=True, help="Canonical slate date YYYY-MM-DD")
    ap.add_argument("--props-csv", type=Path, help="Already policy-selected prop candidate rows")
    ap.add_argument("--names-csv", type=Path, help="Canonical player/team mapping for candidates")
    ap.add_argument("--capture-receipt", type=Path, help="Optional cross-market capture receipt for lineage")
    ap.add_argument("--prop-market", choices=["shots_on_goal", "points", "saves"])
    ap.add_argument("--out-csv", type=Path,
                    help="Explicit output path; use --immutable-output-dir for the operational raw flow")
    ap.add_argument("--immutable-output-dir", type=Path,
                    help="Create a unique ET-timestamped CSV and adjacent lineage file in this directory")
    ap.add_argument("--report-json", type=Path)
    ap.add_argument("--test-only", action="store_true",
                    help="Allow a non-current slate for schema/importer testing; lineage is marked non-operational")
    args = ap.parse_args()

    if (args.out_csv is None) == (args.immutable_output_dir is None):
        raise SystemExit("provide exactly one of --out-csv or --immutable-output-dir")

    export_classification = classify_export_date(
        args.date,
        current_date=datetime.now(ZoneInfo("America/Los_Angeles")).date(),
        test_only=args.test_only,
    )
    if args.test_only and args.report_json is None:
        raise SystemExit("--report-json is required with --test-only so the artifact is marked non-operational")

    catalog_dir = resolve_catalog_dir(args.catalog_dir)
    package_manifest_sha256 = verify_package_manifest(args.package_dir)
    package_status = json.loads((args.package_dir / "daily_execution_status.json").read_text())
    exported_at_et = datetime.now(ZoneInfo("America/New_York"))
    if args.immutable_output_dir is not None:
        args.out_csv, generated_report_path = immutable_export_paths(
            args.immutable_output_dir, slate_date=args.date,
            package_state_sha256=str(package_status.get("substantive_state_sha256") or ""),
            exported_at_et=exported_at_et,
        )
        if args.report_json is None:
            args.report_json = generated_report_path
    if args.out_csv.exists():
        raise SystemExit(f"EXPORT_OUTPUT_ALREADY_EXISTS:{args.out_csv}")
    spec, team_map, player_map, allowed_bets = load_catalogs(catalog_dir)
    props = None
    if args.props_csv:
        props = pd.read_csv(args.props_csv)
        if "full_name" in props and "player_name" not in props:
            props = props.rename(columns={"full_name": "player_name"})
        if "market" not in props:
            if not args.prop_market:
                raise SystemExit("--prop-market is required when the candidate file has no market column")
            props["market"] = args.prop_market
        if "model_pick" not in props and "selected_side" in props:
            props["model_pick"] = props["selected_side"].astype(str).str.lower()
        if "model_side_prob" not in props:
            if "model_side_prob" not in props and "p_over" in props:
                props["model_side_prob"] = props.apply(
                    lambda r: float(r.p_over) if str(r.model_pick).lower() == "over" else 1.0-float(r.p_over), axis=1)
        if "team" not in props:
            if args.names_csv is None:
                raise SystemExit("--names-csv is required to bind player candidates to NHL teams")
            names = pd.read_csv(args.names_csv)
            names = names.rename(columns={"team_code": "team"})
            props = props.merge(names[["game_id", "player_id", "team"]].drop_duplicates(),
                                on=["game_id", "player_id"], how="left", validate="many_to_one")
        if "player_name" not in props or props.player_name.isna().any():
            raise SystemExit("candidate player identity missing; no player slug fallback is allowed")
        if "run_id" not in props:
            props["run_id"] = ""
        if "model_identity" not in props:
            props["model_identity"] = "POLICY_SELECTED_CANDIDATE"

    out, diagnostics = build_rows(
        package_dir=args.package_dir, spec=spec, team_map=team_map,
        player_map=player_map, allowed_bets=allowed_bets, prop_candidates=props,
        ambiguous_player_keys=ambiguous_player_bindings(catalog_dir),
        unique_player_code_by_name=unique_player_codes_by_name(catalog_dir),
        ambiguous_player_names=ambiguous_player_names(catalog_dir))
    canonical_dates = set(pd.read_csv(args.package_dir / "schedule_event_identity.csv").game_date.astype(str))
    if canonical_dates != {args.date}:
        raise SystemExit(f"package slate mismatch: expected {args.date}, got {sorted(canonical_dates)}")
    team_codes = {str(r.get("code", "")) for r in json.loads((catalog_dir / "teams.json").read_text()).get("data", [])}
    player_codes = {str(r.get("code", "")) for r in json.loads((catalog_dir / "players.json").read_text()).get("data", []) if r.get("code")}
    validation = validate_upload(out, spec=spec, team_codes=team_codes, player_codes=player_codes)
    if not out.DATE.eq(args.date).all():
        raise SystemExit("UPLOAD_DATE_DOES_NOT_MATCH_REQUESTED_SLATE")

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, columns=UPLOAD_COLUMNS)
    catalog_hashes = {name: hashlib.sha256((catalog_dir / name).read_bytes()).hexdigest()
                      for name in ("model_spec.json", "teams.json", "players.json")}
    source_hashes = {
        name: hashlib.sha256((args.package_dir / name).read_bytes()).hexdigest()
        for name in ("v2_immutable_predictions.csv", "puck_line_v1_immutable_predictions.csv", "raw_market_response.json")
    }
    capture_receipt = json.loads(args.capture_receipt.read_text()) if args.capture_receipt else {}
    source_run_ids = sorted({str(value) for column in ("parent_daily_run_id", "run_id")
                             if column in (props.columns if props is not None else [])
                             for value in props[column].dropna().astype(str) if value}) if props is not None else []
    source_observation_ids = sorted({str(value) for column in ("odds_observation_id",)
                                    if column in (props.columns if props is not None else [])
                                    for value in props[column].dropna().astype(str) if value}) if props is not None else []
    source_observation_timestamps = sorted({str(value) for column in ("capture_timestamp_utc",)
                                            if column in (props.columns if props is not None else [])
                                            for value in props[column].dropna().astype(str) if value}) if props is not None else []
    report = {
        "slate_date": args.date, "csv_path": str(args.out_csv),
        "export_timestamp_et": exported_at_et.isoformat(timespec="microseconds"),
        "export_classification": export_classification,
        "csv_sha256": hashlib.sha256(args.out_csv.read_bytes()).hexdigest(),
        "columns": UPLOAD_COLUMNS, "validation": validation,
        "rows_by_market": out.groupby("MARKET").size().to_dict(),
        "catalog_sha256": catalog_hashes, "catalog_dir": str(catalog_dir),
        "package_manifest_sha256": package_manifest_sha256,
        "package_dir": str(args.package_dir), "package_state_sha256": package_status.get("substantive_state_sha256"),
        "package_run_timestamp_utc": package_status.get("run_timestamp_utc"),
        "source_daily_run_ids": source_run_ids,
        "source_odds_observation_ids": source_observation_ids,
        "source_odds_observation_timestamps_utc": source_observation_timestamps,
        "market_capture_id": capture_receipt.get("capture_id") or (args.capture_receipt.stem if args.capture_receipt else None),
        "market_capture_timestamp_utc": capture_receipt.get("invocation_timestamp_utc"),
        "market_snapshot_sha256": source_hashes["raw_market_response.json"],
        "source_artifact_sha256": source_hashes,
        "reference_model_identities": sorted({x["model_identity"] for x in diagnostics["provenance"]}),
        "capture_receipt_path": str(args.capture_receipt) if args.capture_receipt else None,
        "capture_receipt_sha256": hashlib.sha256(args.capture_receipt.read_bytes()).hexdigest() if args.capture_receipt else None,
        **diagnostics,
    }
    if args.report_json:
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "provenance"}, indent=2))


if __name__ == "__main__":
    main()
