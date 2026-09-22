from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = Path(__file__).resolve().parent
FREEZE = ROOT / "docs/contracts/mlb_2026_stat_derived_model_training_phase_population_freeze_v1"
EXPECTED_HEAD = "08c661a9c9108795514d3a882a5fdc2a80508cee"
EXPECTED_INTERPRETER = "/Users/jerrystrain/Projects/proppadia/.venv/bin/python"
LOCK = ROOT / "backend/mlb/data/research/dh_forward_validation/v1/rolling_forward_evidence_status_v1.json.publish.lock"
ALLOWED_CATEGORIES = {"training", "validation", "evaluation", "research", "reporting", "unused/legacy"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, text=True, capture_output=True
    ).stdout


checks: list[str] = []


def check(condition: bool, name: str) -> None:
    if not condition:
        raise AssertionError(name)
    checks.append(name)


def main() -> int:
    check(Path(sys.executable).resolve() == Path(EXPECTED_INTERPRETER).resolve(), "canonical interpreter")
    check(
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", EXPECTED_HEAD, "HEAD"],
            cwd=ROOT,
            capture_output=True,
        ).returncode == 0,
        "starting HEAD retained in ancestry",
    )
    tracked_changes = set(git("diff", "--name-only").splitlines())
    in_scope_tracked = {
        path for path in tracked_changes
        if path.startswith("backend/mlb/")
        or path.startswith("docs/contracts/mlb_2026_stat_derived_")
    }
    check(not in_scope_tracked, "no in-scope tracked changes")
    check(not git("diff", "--cached", "--name-only").strip(), "staged worktree clean")
    check(not git("diff", "--check").strip() and not git("diff", "--cached", "--check").strip(), "git diff checks")

    venv = ROOT / ".venv"
    vstat = venv.lstat()
    check(venv.is_symlink() and os.readlink(venv) == "/Users/jerrystrain/Projects/.proppadia-py311-scipy1152-macos12-arm64-candidate", "venv target")
    check((vstat.st_size, int(vstat.st_mtime), vstat.st_ino) == (78, 1790025129, 137012602), "venv unchanged")
    lstat = LOCK.lstat()
    check((lstat.st_size, int(lstat.st_mtime), lstat.st_ino, sha256(LOCK)) == (0, 1789577504, 135377680, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"), "publish lock unchanged")

    status = git("status", "--porcelain=v1", "--untracked-files=all").splitlines()
    untracked_status = [line for line in status if line.startswith("?? ")]
    allowed_prefixes = (
        "?? .venv",
        "?? backend/mlb/data/research/dh_forward_validation/v1/rolling_forward_evidence_status_v1.json.publish.lock",
        "?? docs/contracts/mlb_2026_stat_derived_model_training_phase_population_freeze_v1/",
        "?? docs/contracts/mlb_2026_stat_derived_training_phase_correction_design_v1/",
    )
    check(all(any(line.startswith(prefix) for prefix in allowed_prefixes) for line in untracked_status), "only protected and two review packages untracked")
    check(any(line.startswith(allowed_prefixes[2]) for line in untracked_status) and any(line.startswith(allowed_prefixes[3]) for line in untracked_status), "both packages remain untracked")

    freeze_manifest = {}
    for line in (FREEZE / "sha256_manifest.txt").read_text().splitlines():
        digest, rel = line.split("  ", 1)
        freeze_manifest[rel] = digest
    freeze_files = {str(p.relative_to(FREEZE)) for p in FREEZE.rglob("*") if p.is_file() and p.name != "sha256_manifest.txt"}
    check(set(freeze_manifest) == freeze_files, "freeze manifest coverage")
    check(all(sha256(FREEZE / rel) == digest for rel, digest in freeze_manifest.items()), "freeze manifest hashes")
    freeze_report = json.loads((FREEZE / "validation_report.json").read_text())
    check(freeze_report["status"] == "PASS" and freeze_report["checks_passed"] == 61 and freeze_report["checks_failed"] == 0, "freeze validation report")

    storage = read_csv(PACKAGE / "package_storage_report.csv")
    file_rows = [row for row in storage if row["path"] != "TOTAL"]
    total = [row for row in storage if row["path"] == "TOTAL"]
    check(len(file_rows) == 19 and len(total) == 1, "storage report inventory")
    check(sum(int(row["bytes"]) for row in file_rows) == 244578450 and int(total[0]["bytes"]) == 244578450, "storage total")
    check(all((FREEZE / row["path"]).stat().st_size == int(row["bytes"]) for row in file_rows), "storage individual sizes")
    check(all(sha256(FREEZE / row["path"]) == row["sha256"] for row in file_rows), "storage individual hashes")
    ledger = next(row for row in file_rows if row["path"] == "affected_unprovable_row_ledger.csv")
    check(ledger["bytes"] == "153352113" and ledger["storage_recommendation"] == "DO_NOT_STAGE_PENDING_REPOSITORY_STORAGE_REVIEW", "large ledger storage guard")
    check(subprocess.run(["git", "ls-files", "--error-unmatch", str((FREEZE / ledger["path"]).relative_to(ROOT))], cwd=ROOT, capture_output=True).returncode != 0, "large ledger not tracked")

    contamination = {row["source_identity"]: row for row in read_csv(PACKAGE / "contamination_by_source.csv")}
    check(len(contamination) == 11, "contamination source inventory")
    operational = contamination["mlb.model_training_props::2026"]
    check((operational["total_rows"], operational["distinct_game_pks"]) == ("600766", "2812"), "operational total")
    check((operational["regular_rows"], operational["regular_game_pks"]) == ("459604", "2341"), "operational regular")
    check((operational["preseason_rows"], operational["preseason_game_pks"]) == ("141162", "471"), "operational preseason")
    check((operational["preseason_S_rows"], operational["preseason_S_game_pks"], operational["preseason_E_rows"], operational["preseason_E_game_pks"]) == ("132609", "440", "8553", "31"), "operational source types")
    check(all(operational[k] == "0" for k in ("postseason_rows", "special_unknown_rows", "missing_authority_rows", "rows_lacking_game_pk", "duplicate_identity_conflicts", "source_conflicts")), "operational zero violations")
    full = contamination["tmp/mlb_base_vs_market_rows_anybook_full.csv::2026"]
    one = contamination["tmp/mlb_base_vs_market_rows_anybook_one_sided_rows.csv::2026"]
    check((full["regular_rows"], full["preseason_rows"], full["preseason_S_rows"], full["preseason_E_rows"]) == ("69652", "869", "734", "135"), "full artifact phase split")
    check((one["regular_rows"], one["preseason_rows"], one["preseason_S_rows"], one["preseason_E_rows"]) == ("55252", "16516", "15517", "999"), "one-sided artifact phase split")

    restatement = json.loads((PACKAGE / "retained_evaluation_restatement.json").read_text())
    check(len(restatement["artifacts"]) == 2, "two restatable artifacts")
    check(all(row["exact_game_pk_complete"] and row["restatement"] == "FILTER_RETAINED_ROWS_ONLY" for row in restatement["artifacts"]), "deterministic restatement method")
    check(all(row["manufacture_policy"] == "NEVER_FILL_MISSING_PREDICTIONS_OUTCOMES_OR_PRICES" for row in restatement["artifacts"]), "no manufactured values")

    consumers = read_csv(PACKAGE / "affected_consumer_map.csv")
    check(len(consumers) == 89 and len({row["path"] for row in consumers}) == 89, "consumer map cardinality")
    check({row["category"] for row in consumers}.issubset(ALLOWED_CATEGORIES) and {row["category"] for row in consumers} == ALLOWED_CATEGORIES, "consumer categories")
    check(Counter(row["category"] for row in consumers) == Counter({"reporting": 21, "validation": 19, "research": 18, "unused/legacy": 16, "evaluation": 13, "training": 2}), "consumer category counts")
    direct_sources = {row["path"] for row in read_csv(FREEZE / "direct_consumer_source_inventory.csv")}
    check(direct_sources.issubset({row["path"] for row in consumers}), "all frozen direct references dispositioned")
    check(all((ROOT / row["path"]).exists() for row in consumers), "consumer paths exist")
    trainer = next(row for row in consumers if row["path"] == "backend/mlb/model_trainer.py")
    check(trainer["category"] == "training" and trainer["eligibility_policy"] == "REQUIRED_BEFORE_MEMBERSHIP", "central trainer cutover")
    evaluator = next(row for row in consumers if row["path"] == "backend/mlb/scripts/evaluate_hits_model_candidates.py")
    check(evaluator["eligibility_policy"] == "SHARED_GATE_ALREADY_PRESENT", "existing pilot preserved")
    producer = next(row for row in consumers if row["path"] == "backend/mlb/scripts/insert_mlb_stat_derived.py")
    check(producer["eligibility_policy"] == "RAW_OBSERVATION_ONLY_DO_NOT_FILTER", "observation producer preserved")

    models = read_csv(PACKAGE / "model_lineage_inventory.csv")
    check(len(models) == 544 and len({row["artifact_path"] for row in models}) == 544, "model lineage inventory cardinality")
    check(Counter(row["lineage_classification"] for row in models) == Counter({"INPUT_MEMBERSHIP_UNPROVABLE": 537, "NOT_APPLICABLE": 3, "RECONSTRUCTABLE_BUT_UNBOUND": 3, "PROVEN_REGULAR_INPUT": 1}), "model lineage classifications")
    expected_binaries = {str(p.relative_to(ROOT)) for p in (ROOT / "models_out").rglob("*.joblib") if "nhl" not in p.parts}
    expected_binaries |= {str(p.relative_to(ROOT)) for p in (ROOT / "artifacts/mlb_models_bundle").rglob("*.joblib")}
    expected_binaries.add("backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib")
    extensions = {".joblib", ".pkl", ".pickle", ".onnx", ".pt"}
    expected_binaries |= {
        str(p.relative_to(ROOT))
        for p in (ROOT / "artifacts/analysis/model_development").rglob("*")
        if p.is_file() and p.suffix.lower() in extensions and "mlb" in str(p.relative_to(ROOT)).lower()
    }
    actual_binaries = {row["artifact_path"] for row in models if row["artifact_kind"] in {"MODEL_BINARY", "RESEARCH_MODEL_BINARY"}}
    check(actual_binaries == expected_binaries and len(actual_binaries) == 538, "every in-scope model binary inventoried")
    check(all((ROOT / row["artifact_path"]).stat().st_size == int(row["bytes"]) for row in models), "model inventory paths and sizes")
    check(not any(row["artifact_kind"] in {"MODEL_BINARY", "RESEARCH_MODEL_BINARY"} and row["lineage_classification"] == "PROVEN_REGULAR_INPUT" for row in models), "no model binary overclaimed")
    residual = next(row for row in models if row["artifact_path"].endswith("hits_residual_ranker.joblib"))
    check(residual["lineage_classification"] == "RECONSTRUCTABLE_BUT_UNBOUND", "residual ranker classification")
    hits_pilot = next(row for row in models if row["artifact_path"].endswith("frozen_hits_review_population.csv"))
    check(hits_pilot["lineage_classification"] == "PROVEN_REGULAR_INPUT", "bounded Hits result classification")

    contract = json.loads((PACKAGE / "future_training_manifest_contract.json").read_text())
    check(contract["certification_rule"] == "NO_MODEL_OR_RESULT_CERTIFICATION_WITHOUT_A_VALID_COMPLETE_BINDING", "future certification rule")
    required_groups = {"run_identity", "code", "input_population", "phase_authority", "source_data", "learning_contract", "windows", "exclusions", "outputs", "binding"}
    check(set(contract["required_fields"]) == required_groups, "future manifest groups")
    mandatory = {item for values in contract["required_fields"].values() for item in values}
    check({"run_id", "repository_commit", "exact_game_pk_list", "row_count", "deterministic_row_sha256", "phase_contract_sha256", "source_artifact_sha256s", "feature_contract_sha256", "target_definition", "training_from", "evaluation_through", "model_artifact_sha256", "result_artifact_sha256s", "manifest_sha256"}.issubset(mandatory), "future manifest minimum fields")

    shared = (PACKAGE / "shared_eligibility_contract.md").read_text()
    check("exact gamePk" in shared and "season_phase == \"REGULAR_SEASON\"" in shared, "exact regular admission")
    check("There is no missing-type-to-`R` path" in shared and "No calendar value" in shared, "zero fallback or date inference")
    check("Membership-only guarantee" in shared and "unchanged" in shared, "membership-only invariant")
    check("Future database sidecar backend" in shared and "configuration-only" in shared, "backend-neutral transition")

    sequence = (PACKAGE / "correction_sequence.md").read_text()
    check(all(name in sequence for name in ("Eligibility interface implementation", "Selector cutover", "Frozen-population comparison", "Retained evaluation restatement", "Future manifest binding", "Model-specific retraining decision")), "bounded correction stages")
    check(sequence.count("Rollback:") >= 5, "stage rollback coverage")
    threshold = (PACKAGE / "retraining_evidence_threshold.md").read_text()
    check("does not by itself prove" in threshold and "Creation date" in threshold, "retraining evidence threshold")

    evidence = read_csv(PACKAGE / "source_evidence_manifest.csv")
    check(len(evidence) == 22, "source evidence count")
    check(all((ROOT / row["path"]).stat().st_size == int(row["bytes"]) and sha256(ROOT / row["path"]) == row["sha256"] for row in evidence), "source evidence hashes")

    readme = (PACKAGE / "README.md").read_text()
    check("TRAINING_PHASE_CORRECTION_DESIGN_READY" in readme, "README classification")
    check("No existing model binary is classified `PROVEN_REGULAR_INPUT`" in readme, "lineage conclusion")
    check("did not change source code" in readme and "did not retrain" in readme, "design-only boundary")

    validation = json.loads((PACKAGE / "validation_report.json").read_text())
    check(validation["status"] == "PASS" and validation["classification"] == "TRAINING_PHASE_CORRECTION_DESIGN_READY", "validation disposition")

    package_manifest = {}
    for line in (PACKAGE / "sha256_manifest.txt").read_text().splitlines():
        digest, rel = line.split("  ", 1)
        package_manifest[rel] = digest
    package_files = {str(p.relative_to(PACKAGE)) for p in PACKAGE.rglob("*") if p.is_file() and p.name != "sha256_manifest.txt"}
    check(set(package_manifest) == package_files, "package manifest coverage")
    check(all(sha256(PACKAGE / rel) == digest for rel, digest in package_manifest.items()), "package manifest hashes")

    print(json.dumps({
        "status": "PASS",
        "checks_passed": len(checks),
        "checks_failed": 0,
        "checks": checks,
        "classification": "TRAINING_PHASE_CORRECTION_DESIGN_READY",
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({
            "status": "FAIL",
            "checks_passed": len(checks),
            "checks_failed": 1,
            "failure": str(exc),
        }, indent=2, sort_keys=True))
        raise
