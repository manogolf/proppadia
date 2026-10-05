#!/usr/bin/env python3
"""Replay SOG market attachments from retained NHL evidence, without providers."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.nhl.daily_capture import (
    canonical_game_set_hash, load_canonical_slate, sha256_file, verify_package,
)
from backend.nhl.attachment_integrity import validate_odds_observation
from backend.nhl.performance_summary import _verify_daily_receipt
from backend.nhl.sog_attachment_integrity import (
    audit_sog_attachment, retain_sog_attachment_package,
)
from backend.nhl.postgame_learning import verify_reconciliation_package


ROOT = Path(__file__).resolve().parents[3]
DAILY_ROOT = ROOT / "artifacts/operational/nhl/daily_runs"
ODDS_ROOT = ROOT / "artifacts/operational/nhl/odds_observations"
ROSTER_ROOT = ROOT / "artifacts/operational/nhl/roster_observations"
SLATE_ROOT = ROOT / "artifacts/operational/nhl/slates"
ARCHIVE_ROOT = ROOT / "backend/nhl/exports/odds_history"
RECONSTRUCTION_ROOT = ROOT / "artifacts/operational/nhl/sog_market_coverage_reconstructions"
BUILDER = ROOT / "backend/nhl/scripts/build_sog_with_market.py"


def _sha(path: Path) -> str:
    return sha256_file(Path(path))


def _roster_names(roster_dir: Path, pred_path: Path, slate: str, out_path: Path,
                  grade_path: Path) -> None:
    predictions = pd.read_csv(pred_path)
    if not {"player_id", "game_id", "team_id"}.issubset(predictions.columns):
        raise RuntimeError("SOG_PREDICTION_IDENTITY_COLUMNS_MISSING")
    wanted = predictions[["player_id", "game_id", "team_id"]].drop_duplicates(
        ["player_id", "game_id"])
    rows = []
    snapshot_path = roster_dir / "roster_snapshot.jsonl"
    if not snapshot_path.is_file():
        raise RuntimeError("RETAINED_ROSTER_SNAPSHOT_MISSING")
    snapshot = pd.read_json(snapshot_path, lines=True)
    required = {"player_id", "game_id", "first_name", "last_name", "team"}
    if not required.issubset(snapshot.columns):
        raise RuntimeError("RETAINED_ROSTER_IDENTITY_COLUMNS_MISSING")
    merged = wanted.merge(snapshot, on=["player_id", "game_id"], how="left",
                          validate="one_to_one")
    missing = merged.first_name.isna() | merged.last_name.isna()
    if missing.any():
        grade = pd.read_csv(grade_path)
        if not {"player_id", "game_id", "player_name"}.issubset(grade.columns):
            raise RuntimeError("RETAINED_ROSTER_IDENTITY_INCOMPLETE")
        fallback = grade[["player_id", "game_id", "player_name"]].drop_duplicates(
            ["player_id", "game_id"])
        merged = merged.merge(fallback, on=["player_id", "game_id"], how="left",
                              validate="one_to_one")
        still_missing = missing & merged.player_name.isna()
        if still_missing.any():
            raise RuntimeError("RETAINED_ROSTER_AND_CANONICAL_GRADE_IDENTITIES_INCOMPLETE")
        merged.loc[missing, "first_name"] = merged.loc[missing, "player_name"].astype(str)
        merged.loc[missing, "last_name"] = ""
        merged.loc[missing, "team"] = ""
    names = pd.DataFrame({
        "player_id": merged.player_id.astype("int64"),
        "game_id": merged.game_id.astype("int64"),
        "team_id": merged.team_id.astype("int64"),
        "full_name": (merged.first_name.astype(str).str.strip() + " "
                      + merged.last_name.astype(str).str.strip()),
        "team_code": merged.team.astype(str),
        "game_date": slate,
    })
    if names.duplicated(["player_id", "game_id"]).any():
        raise RuntimeError("RETAINED_ROSTER_NAMES_DUPLICATED")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    names.to_csv(out_path, index=False)


def _eligible_receipts(slate: str) -> list[tuple[Path, dict, str]]:
    candidates = []
    for receipt_path in DAILY_ROOT.glob("run_id=*/parent_receipt.json"):
        try:
            receipt, manifest_sha = _verify_daily_receipt(receipt_path, slate)
        except Exception:
            continue
        lane = (receipt.get("lanes") or {}).get("sog_attachment") or {}
        old_attachment = next((item for item in lane.get("outputs", [])
                               if str(item.get("path", "")).endswith("sog_with_market.csv")), None)
        if lane.get("status") != "COMPLETE" or not old_attachment:
            continue
        # Prefer a receipt whose attachment still matches the retained archive,
        # while retaining earlier complete runs as a valid fallback when the
        # latest run's odds were captured after a game started.
        archive = ARCHIVE_ROOT / slate / "sog_with_market.csv"
        archive_matches = archive.is_file() and old_attachment.get("sha256") == _sha(archive)
        prediction_lane = (receipt.get("lanes") or {}).get("legacy_sog") or {}
        pred = next((item for item in prediction_lane.get("outputs", [])
                     if str(item.get("path", "")).endswith("sog_predictions_wide_calibrated.csv")), None)
        odds = receipt.get("odds_observation") or {}
        roster = receipt.get("roster_observation") or {}
        if not pred or not odds.get("path") or not odds.get("manifest_sha256") or not roster.get("path"):
            continue
        try:
            pred_path = Path(pred["path"])
            if _sha(pred_path) != pred["sha256"]:
                continue
            candidates.append((receipt_path, receipt, manifest_sha, archive_matches))
        except OSError:
            continue
    candidates.sort(key=lambda row: (
        bool(row[3]), str(row[1].get("ended_at_utc") or "")), reverse=True)
    if not candidates:
        raise RuntimeError("NO_COMPLETE_DAILY_RECEIPT_BINDS_RETAINED_SOG_INPUTS")
    return candidates


def _verify_sources(receipt_path: Path, receipt: dict, slate: str,
                    receipt_manifest_sha: str) -> tuple[Path, Path, Path, Path, str, str, str, str]:
    run_id = str(receipt["parent_daily_run_id"])
    prediction_lane = receipt["lanes"]["legacy_sog"]
    prediction_identity = next(item for item in prediction_lane["outputs"]
                               if str(item.get("path", "")).endswith(
                                   "sog_predictions_wide_calibrated.csv"))
    pred_path = Path(prediction_identity["path"]).resolve()
    if _sha(pred_path) != prediction_identity["sha256"]:
        raise RuntimeError("RETAINED_SOG_PREDICTION_HASH_MISMATCH")
    odds = receipt["odds_observation"]
    odds_dir = Path(odds["path"]).resolve()
    odds_manifest_sha = verify_package(odds_dir)
    if odds_manifest_sha != odds["manifest_sha256"]:
        raise RuntimeError("RETAINED_ODDS_MANIFEST_HASH_MISMATCH")
    odds_summary = json.loads((odds_dir / "observation_summary.json").read_text())
    if (odds_summary.get("slate_date") != slate
            or odds_summary.get("parent_daily_run_id") != run_id):
        raise RuntimeError("RETAINED_ODDS_RUN_OR_SLATE_MISMATCH")
    roster = receipt["roster_observation"]
    roster_dir = Path(roster["path"]).resolve()
    roster_manifest_sha = verify_package(roster_dir)
    if roster_manifest_sha != roster.get("manifest_sha256"):
        raise RuntimeError("RETAINED_ROSTER_MANIFEST_HASH_MISMATCH")
    roster_summary = json.loads((roster_dir / "observation_summary.json").read_text())
    if (roster_summary.get("slate_date") != slate
            or int(roster_summary.get("season", -1)) != 2026
            or roster_summary.get("canonical_game_set_hash") != receipt.get(
                "canonical_game_set_hash")):
        raise RuntimeError("RETAINED_ROSTER_RUN_OR_SLATE_MISMATCH")
    slate_dir = SLATE_ROOT / slate
    games = load_canonical_slate(
        slate_date=slate,
        raw_schedule_path=slate_dir / "raw_schedule_response.json",
        slate_health_path=slate_dir / "slate_health.json",
    )
    prediction = pd.read_csv(pred_path)
    game_ids = sorted(pd.to_numeric(prediction.game_id, errors="raise").astype(int).unique())
    if game_ids != sorted(game.game_id for game in games):
        raise RuntimeError("CANONICAL_SLATE_PREDICTION_GAME_SET_MISMATCH")
    game_hash = canonical_game_set_hash(game_ids)
    if game_hash != receipt.get("canonical_game_set_hash"):
        raise RuntimeError("CANONICAL_SLATE_RECEIPT_HASH_MISMATCH")
    observed_at = datetime.fromisoformat(
        str(odds_summary.get("observation_timestamp_utc", "")).replace("Z", "+00:00"))
    eligible_games = [game for game in games if observed_at < datetime.fromisoformat(
        game.start_time_utc.replace("Z", "+00:00"))]
    if not eligible_games:
        raise RuntimeError("RETAINED_ODDS_HAS_NO_GAME_SPECIFIC_PRESTART_COVERAGE")
    validate_odds_observation(
        observation_dir=odds_dir, odds_json=odds_dir / "raw_response.json",
        expected_manifest_sha256=odds_manifest_sha,
        expected_parent_daily_run_id=run_id, expected_slate_date=slate,
        expected_season=2026, expected_phase=receipt.get("resolved_phase"),
        expected_game_set_hash=game_hash,
    )
    reconciliation_day = ROOT / "artifacts/operational/nhl/postgame_reconciliation" / slate
    packages = list(reconciliation_day.glob("reconciliation=*"))
    if len(packages) != 1:
        raise RuntimeError("CANONICAL_SOG_IDENTITY_PACKAGE_CARDINALITY_INVALID")
    reconciliation = packages[0]
    summary = verify_reconciliation_package(reconciliation, slate)
    grade_path = reconciliation / "graded_sog.csv"
    if not grade_path.is_file():
        raise RuntimeError("CANONICAL_SOG_IDENTITY_GRADE_MISSING")
    return (pred_path, odds_dir, roster_dir, grade_path, prediction_identity["sha256"],
            odds_manifest_sha, roster_manifest_sha, _sha(grade_path))


def reconstruct(slate: str) -> dict:
    candidates = _eligible_receipts(slate)
    errors = []
    for receipt_path, receipt, receipt_manifest_sha, archive_matches in candidates:
        run_id = str(receipt["parent_daily_run_id"])
        try:
            (pred_path, odds_dir, roster_dir, identity_grade_path, pred_sha, odds_sha,
             roster_sha, identity_grade_sha) = _verify_sources(
                receipt_path, receipt, slate, receipt_manifest_sha)
            with tempfile.TemporaryDirectory(prefix="nhl_sog_reconstruction_") as temporary:
                tmp = Path(temporary)
                names = tmp / "names.csv"
                attached = tmp / "sog_with_market.csv"
                unmatched = tmp / "unmatched_sog.csv"
                _roster_names(roster_dir, pred_path, slate, names, identity_grade_path)
                command = [
                    sys.executable, str(BUILDER), "--pred", str(pred_path),
                    "--pred-only", "--names", str(names),
                    "--odds-json", str(odds_dir / "raw_response.json"),
                    "--events-json", str(odds_dir / "events_response.json"),
                    "--out", str(attached), "--unmatched", str(unmatched),
                    "--slate-date", slate,
                ]
                env = dict(os.environ, SLATE_DATE=slate)
                subprocess.run(command, cwd=ROOT, env=env, check=True,
                               text=True, capture_output=True)
                integrity = audit_sog_attachment(
                    prediction_path=pred_path, attachment_path=attached,
                    unmatched_path=unmatched, slate_date=slate,
                    parent_daily_run_id=run_id, odds_observation_path=odds_dir,
                    odds_observation_manifest_sha256=odds_sha,
                    names_path=names, reconstructed=True,
                    source_daily_receipt_manifest_sha256=receipt_manifest_sha,
                    canonical_game_starts_utc={int(game.game_id): game.start_time_utc
                                               for game in load_canonical_slate(
                                                   slate_date=slate,
                                                   raw_schedule_path=SLATE_ROOT / slate / "raw_schedule_response.json",
                                                   slate_health_path=SLATE_ROOT / slate / "slate_health.json")},
                )
                odds_lineage = validate_odds_observation(
                    observation_dir=odds_dir, odds_json=odds_dir / "raw_response.json",
                    expected_manifest_sha256=odds_sha,
                    expected_parent_daily_run_id=run_id, expected_slate_date=slate,
                    expected_season=2026, expected_phase=receipt.get("resolved_phase"),
                    expected_game_set_hash=receipt.get("canonical_game_set_hash"),
                )
                integrity.update(odds_lineage)
                integrity["odds_observation_phase"] = receipt.get("resolved_phase")
                integrity["odds_observation_season"] = 2026
                if integrity["status"] != "PASS":
                    raise RuntimeError("RECONSTRUCTED_SOG_ATTACHMENT_INTEGRITY_FAILED")
                archived = ARCHIVE_ROOT / slate / "sog_with_market.csv"
                if archive_matches:
                    old = pd.read_csv(archived)
                    new = pd.read_csv(attached)
                    status = lambda frame: {
                        (str(row.game_date), int(row.game_id), int(row.player_id), str(row.line)): bool(
                            pd.notna(pd.to_numeric(getattr(row, "p_over_mkt", None), errors="coerce")))
                        for row in frame.itertuples(index=False)
                    }
                    if status(old) != status(new):
                        raise RuntimeError("RECONSTRUCTION_DOES_NOT_MATCH_RECEIPT_BOUND_ARCHIVE")
                    integrity["source_archive_attachment_sha256"] = _sha(archived)
                    integrity["source_archive_attachment_receipt_sha256"] = next(
                        item["sha256"] for item in receipt["lanes"]["sog_attachment"]["outputs"]
                        if str(item.get("path", "")).endswith("sog_with_market.csv"))
                integrity["source_roster_observation_manifest_sha256"] = roster_sha
                integrity["source_daily_receipt_path"] = str(receipt_path.resolve())
                identity_payload = {
                    "contract": "NHL_SOG_ATTACHMENT_RECONSTRUCTION_V2",
                    "slate_date": slate, "parent_daily_run_id": run_id,
                    "prediction_sha256": pred_sha, "odds_manifest_sha256": odds_sha,
                    "roster_manifest_sha256": roster_sha,
                    "canonical_grade_identity_sha256": identity_grade_sha,
                    "daily_receipt_manifest_sha256": receipt_manifest_sha,
                    "names_sha256": _sha(names), "builder_sha256": _sha(BUILDER),
                    "integrity_contract_sha256": _sha(
                        ROOT / "backend/nhl/sog_attachment_integrity.py"),
                    "reconstruction_script_sha256": _sha(Path(__file__).resolve()),
                }
                identity = hashlib.sha256(json.dumps(
                    identity_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                integrity["reconstruction_identity"] = identity
                integrity["reconstruction_contract"] = identity_payload["contract"]
                integrity["reconstruction_source_hashes"] = identity_payload
                final_dir = (RECONSTRUCTION_ROOT / "season=2026" / f"slate_date={slate}"
                             / f"reconstruction={identity[:20]}")
                if final_dir.exists():
                    prior = json.loads((final_dir / "sog_attachment_integrity.json").read_text())
                    if prior.get("reconstruction_identity") != identity:
                        raise RuntimeError("RECONSTRUCTION_IDENTITY_COLLISION")
                    verify_package(final_dir)
                    return prior
                report, _ = retain_sog_attachment_package(
                    package_path=final_dir, integrity=integrity,
                    attachment_path=attached, unmatched_path=unmatched,
                    names_path=names,
                )
                return json.loads(report.read_text())
        except Exception as error:
            errors.append(f"{run_id}:{type(error).__name__}:{error}")
    raise RuntimeError("SOG_RECONSTRUCTION_UNAVAILABLE:" + ";".join(errors))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slates", nargs="+", default=[
        "2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02", "2026-10-03"])
    args = parser.parse_args()
    failed = False
    for slate in args.slates:
        try:
            report = reconstruct(slate)
            counts = report["counts"]
            print(json.dumps({
                "slate_date": slate, "status": report["status"],
                "prediction_rows": counts["prediction_row_count"],
                "matched": counts["matched_count"],
                "unmatched": counts["unmatched_count"],
                "ambiguous": counts["ambiguous_count"],
                "prediction_sha256": report["prediction_artifact_sha256"],
                "odds_manifest_sha256": report["odds_observation_manifest_sha256"],
            }, sort_keys=True))
        except Exception as error:
            failed = True
            print(json.dumps({"slate_date": slate, "status":
                              "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE",
                              "reason": str(error)}, sort_keys=True))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
