"""Build a create-only audit ledger for retained NHL scoring runs.

This audit deliberately proves only what the original receipt and its package
manifest bind. A nearby Git commit is never treated as the source state of a
run unless the receipt explicitly binds that commit.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RECEIPTS = ROOT / "artifacts/operational/nhl/daily_runs"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/nhl/fitted_model_identity_reconstruction/2026-10-08"
DATE_START = "2026-09-29"
DATE_END = "2026-10-08"
LANES = {
    "sog": ("legacy_sog", "sog_predictions_wide_calibrated.csv"),
    "points": ("points", "points_predictions.csv"),
    "saves": ("saves", "saves_predictions.csv"),
}
CLASSIFICATION = "HISTORICAL_REPOSITORY_STATE_UNPROVEN"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_package(receipt_path: Path) -> bool:
    manifest = receipt_path.parent / "SHA256SUMS"
    if not manifest.is_file():
        return False
    for line in manifest.read_text().splitlines():
        if not line.strip():
            continue
        expected, relpath = line.split(maxsplit=1)
        relpath = relpath.lstrip("*")
        target = (manifest.parent / relpath).resolve()
        if not target.is_file() or sha256(target) != expected:
            return False
    return True


def repo_relative(path: str) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(ROOT).as_posix()
    except ValueError:
        return path


def pulse_transition_counts() -> dict:
    path = (ROOT / "artifacts/analysis/nhl/player_performance_pulse/2026-10-07_final/"
            "revisions/as_of=2026-10-08/player_state_transitions.csv")
    counts = {lane: 0 for lane in LANES}
    with path.open(newline="") as stream:
        for row in csv.DictReader(stream):
            if (row.get("lane") in counts
                    and DATE_START <= row.get("game_n_date", "") <= DATE_END
                    and DATE_START <= row.get("game_n1_date", "") <= DATE_END):
                counts[row["lane"]] += 1
    return counts


def collect_rows() -> tuple[list[dict], dict]:
    rows: list[dict] = []
    receipts_seen = 0
    package_integrity_failures = 0
    prediction_hash_failures = 0
    for receipt_path in sorted(RECEIPTS.glob("run_id=*/parent_receipt.json")):
        receipt = json.loads(receipt_path.read_text())
        slate_date = receipt.get("slate_date")
        if not slate_date or not DATE_START <= slate_date <= DATE_END:
            continue
        receipts_seen += 1
        package_ok = verify_package(receipt_path)
        if not package_ok:
            package_integrity_failures += 1
        run_id = receipt_path.parent.name.split("=", 1)[1]
        for lane, (receipt_lane, expected_name) in LANES.items():
            section = receipt.get("lanes", {}).get(receipt_lane, {})
            prediction = next((item for item in section.get("outputs", [])
                               if Path(item.get("path", "")).name == expected_name), None)
            if prediction is None:
                continue
            raw_path = Path(prediction["path"])
            prediction_path = repo_relative(str(raw_path))
            on_disk_path = raw_path if raw_path.is_absolute() else ROOT / raw_path
            actual_sha = sha256(on_disk_path) if on_disk_path.is_file() else None
            expected_sha = prediction.get("sha256")
            hash_matches = actual_sha is not None and actual_sha == expected_sha
            if not hash_matches:
                prediction_hash_failures += 1
            child_commands = sorted({child.get("command_identity", "")
                                     for child in section.get("database_write_children", [])
                                     if child.get("command_identity")})
            inputs = section.get("inputs", [])
            rows.append({
                "slate_date": slate_date,
                "run_id": run_id,
                "lane": lane,
                "prediction_artifact_path": prediction_path,
                "prediction_sha256": expected_sha,
                "prediction_sha256_verified_on_disk": hash_matches,
                "receipt_package_integrity_verified": package_ok,
                "repository_commit": None,
                "repository_source_state": None,
                "reconstruction_classification": CLASSIFICATION,
                "fitted_model_identity_sha256": None,
                "component_manifest": [],
                "scoring_configuration": None,
                "retained_lane_inputs": inputs,
                "retained_lane_commands": child_commands,
                "evidence_sources": [
                    repo_relative(str(receipt_path)),
                    repo_relative(str(receipt_path.parent / "SHA256SUMS")),
                    prediction_path,
                    "Git history for NHL scorer sources and model artifact paths",
                    "/Users/jerrystrain/Library/LaunchAgents/com.proppadia.nhl.morning-orchestration.plist",
                    "/Users/jerrystrain/Library/LaunchAgents/com.proppadia.nhl.mainline-cross-market-shadow.plist",
                ],
                "unresolved_reason": (
                    "Receipt has no exact source commit/hash; Git model artifacts are not tracked; "
                    "historical scorer/configuration state cannot be bound to this execution."
                    if package_ok and hash_matches else
                    "Prediction or receipt package integrity failed; exact identity reconstruction is blocked."
                ),
                "pulse_comparability_eligible": False,
            })
    rows.sort(key=lambda row: (row["slate_date"], row["run_id"], row["lane"]))
    lane_counts = {lane: sum(row["lane"] == lane for row in rows) for lane in LANES}
    return rows, {
        "date_start": DATE_START,
        "date_end": DATE_END,
        "retained_receipts_examined": receipts_seen,
        "receipts_with_exact_repository_commit": 0,
        "git_tracked_nhl_model_artifact_paths": 0,
        "launchagent_definitions_examined": [
            "/Users/jerrystrain/Library/LaunchAgents/com.proppadia.nhl.morning-orchestration.plist",
            "/Users/jerrystrain/Library/LaunchAgents/com.proppadia.nhl.mainline-cross-market-shadow.plist",
        ],
        "scored_lane_runs_in_ledger": len(rows),
        "scored_lane_runs_by_lane": lane_counts,
        "receipt_package_integrity_failures": package_integrity_failures,
        "prediction_hash_failures": prediction_hash_failures,
        "identity_proven_by_lane": {lane: 0 for lane in LANES},
        "identity_partially_recovered_by_lane": {lane: 0 for lane in LANES},
        "repository_state_unproven_by_lane": lane_counts,
        "same_model_pairs_recovered_by_lane": {lane: 0 for lane in LANES},
        "historical_pulse_transitions_reconsidered_by_lane": pulse_transition_counts(),
        "historical_pulse_transitions_upgraded_same_model_by_lane": {lane: 0 for lane in LANES},
        "historical_pulse_transitions_upgraded_model_changed_by_lane": {lane: 0 for lane in LANES},
        "historical_pulse_transitions_still_unresolved_by_lane": pulse_transition_counts(),
        "note": (
            "Receipt/output hashes establish retained artifact integrity, not which exact source/model "
            "bytes or scoring configuration executed. LaunchAgent definitions identify entrypoints "
            "and working directory but do not bind a run to a checkout commit. No retrospective identity is pulse eligible. "
            "Original receipts and prediction values are not modified."
        ),
    }


def write_package(output: Path) -> None:
    if output.exists():
        raise FileExistsError(f"create-only output already exists: {output}")
    rows, summary = collect_rows()
    output.mkdir(parents=True)
    fields = list(rows[0]) if rows else []
    with (output / "ledger.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, sort_keys=True, separators=(",", ":"),
                                             allow_nan=False) if isinstance(value, (dict, list))
                             else value for key, value in row.items()})
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True,
                                                     allow_nan=False) + "\n")
    (output / "README.md").write_text(
        "# Retrospective NHL fitted model identity reconstruction\n\n"
        f"Scope: slate dates {DATE_START} through {DATE_END}. This create-only ledger records "
        "the retained prediction and receipt evidence for each scored lane run. It does not "
        "upgrade model comparability because no receipt binds an exact repository/source state, "
        "and the fitted model binaries are not Git tracked. A Git commit near a run timestamp "
        "is not execution proof.\n\n"
        "The prediction SHA-256 is copied from the intact original receipt and independently "
        "checked against the retained prediction file. Receipt package integrity is independently "
        "checked against its SHA256SUMS. No original receipt, prediction, feature, or outcome "
        "artifact is modified. Since no row has a proven fitted identity, the pulse was not "
        "changed to consume this ledger.\n"
    )
    files = sorted(path for path in output.iterdir() if path.is_file())
    (output / "SHA256SUMS").write_text("".join(f"{sha256(path)}  {path.name}\n" for path in files))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    write_package(args.output)
    print(args.output)


if __name__ == "__main__":
    main()
