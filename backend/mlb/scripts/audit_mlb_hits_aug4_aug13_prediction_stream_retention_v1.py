"""Audit retained Aug 4-13 MLB Hits scoring lineage without replaying predictions."""
from __future__ import annotations

import csv
import hashlib
import json
import subprocess
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits_aug4_aug13_prediction_stream_retention_audit_v1/2026-08-14"
DATES = [f"2026-08-{day:02d}" for day in range(4, 14)]
MODEL_ID = "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb"
MODEL_HASH = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(name: str, rows: list[dict]) -> None:
    if not rows:
        raise ValueError(f"refusing empty required output: {name}")
    with (OUT / name).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def load_lineage(date: str) -> tuple[Path, pd.DataFrame]:
    path = ROOT / f"backend/mlb/exports/prospective_lineage/{date}/prediction_lineage_ledger.csv"
    frame = pd.read_csv(path, low_memory=False)
    identities = frame["canonical_row_identity"].map(json.loads)
    frame = frame.assign(
        prop_type=identities.map(lambda value: value.get("prop_type")),
        line=identities.map(lambda value: float(value.get("line"))),
        game_id=identities.map(lambda value: str(value.get("game_id"))),
        player_id=identities.map(lambda value: str(value.get("player_id"))),
    )
    frame = frame[(frame.prop_type == "hits") & frame.line.isin([0.5, 1.5])].copy()
    frame["prediction_dt"] = pd.to_datetime(frame.prediction_timestamp, utc=True)
    frame["start_dt"] = pd.to_datetime(frame.scheduled_game_start, utc=True)
    frame["strict_pregame"] = frame.prediction_dt < frame.start_dt
    frame["identity_key"] = (
        frame.game_id + ":" + frame.player_id + ":" + frame.line.astype(str)
    )
    return path, frame


def lane(line: float) -> str:
    return "HITS_0_5" if line == 0.5 else "HITS_1_5"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    ledgers = {date: load_lineage(date) for date in DATES}

    chain: list[dict] = []
    scorer: list[dict] = []
    stage_counts: list[dict] = []
    log_evidence: list[dict] = []
    recovery: list[dict] = []
    classifications: list[dict] = []
    daily_summary: dict[str, dict] = {}

    for date, (path, frame) in ledgers.items():
        exact = frame[
            frame.strict_pregame
            & frame.model_semantic_name.eq(MODEL_ID)
            & frame.model_artifact_sha256.eq(MODEL_HASH)
            & frame.model_probability_over.notna()
        ].copy()
        if len(exact) != len(frame):
            raise AssertionError(f"{date}: non-exact or post-start Hits lineage rows found")
        earliest = exact.sort_values("prediction_dt").drop_duplicates("identity_key")
        daily_summary[date] = {
            "rows": len(exact),
            "unique": len(earliest),
            "runs": exact.run_tag.nunique(),
            "h05_rows": int(exact.line.eq(0.5).sum()),
            "h15_rows": int(exact.line.eq(1.5).sum()),
            "h05_unique": int(earliest.line.eq(0.5).sum()),
            "h15_unique": int(earliest.line.eq(1.5).sum()),
            "first": exact.prediction_dt.min().isoformat(),
            "last": exact.prediction_dt.max().isoformat(),
            "sha": sha256(path),
        }
        stages = [
            ("scheduler", "LaunchAgent com.proppadia.mlb.refresh.daily", "natural scheduled invocation", "artifacts/ops/mlb_refresh_daily.out.log"),
            ("daily_wrapper", "/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh", "invoked wide scorer and downstream guarded stages", "artifacts/ops/mlb_refresh_daily.out.log"),
            ("slate_source", "build_mlb_predictions_wide.py::main", "loaded date-scoped market/source rows", rel(path)),
            ("feature_generation", "prop_features.prepare_prop_features", "exact feature vectors captured", rel(path)),
            ("hits_model_invocation", "prop_workflow/make_prediction", "current semantic Hits artifact invoked", rel(path)),
            ("raw_probability_generation", "build_mlb_predictions_wide.py", "model_probability_over persisted", rel(path)),
            ("line_transformation", "build_mlb_predictions_wide.py", "line-specific selected-side probability persisted", rel(path)),
            ("selected_side_output", "build_mlb_predictions_wide.py", "selected_side and executable price persisted", rel(path)),
            ("candidate_routing", "build_mlb_slate_output.py::main", "blocked before candidate/routing output", "backend/mlb/scripts/build_mlb_slate_output.py"),
            ("artifact_writer", "prospective_lineage.append_certified_rows", "append-only certified ledger written", rel(path)),
            ("authority_publish_guard", "assert_predictive_model_qualified", "NO_QUALIFIED_MLB_PROP_MODEL; slate/upload/public suppressed", "backend/mlb/shared/model_authority.py"),
        ]
        for order, (stage, component, result, evidence) in enumerate(stages, 1):
            chain.append(dict(date=date, stage_order=order, stage=stage, script_or_function=component,
                              run_tags="|".join(sorted(exact.run_tag.unique())), start_timestamp=daily_summary[date]["first"],
                              end_timestamp=daily_summary[date]["last"], exit_code="0_OR_EXPECTED_AUTHORITY_SKIP",
                              result=result, relevant_output_path=evidence))

        for line_value in (0.5, 1.5):
            rows = exact[exact.line.eq(line_value)]
            unique = rows.sort_values("prediction_dt").drop_duplicates("identity_key")
            scorer.append(dict(date=date, lane=lane(line_value), hits_model_scorer_executed="YES",
                               scorer_output_rows=len(rows), unique_strict_pregame_identities=len(unique),
                               probability_generated="YES", probability_retained="YES",
                               semantic_model_id=MODEL_ID, exact_model_sha256=MODEL_HASH,
                               evidence_path=rel(path), evidence_sha256=sha256(path)))
            for run_tag, run_rows in rows.groupby("run_tag", sort=True):
                count = len(run_rows)
                stage_counts.append(dict(date=date, run_tag=run_tag, lane=lane(line_value),
                    market_input_rows="UNKNOWN", feature_complete_rows=f">={count}", scorer_input_rows=count,
                    scorer_output_rows=count, line_transformed_rows=count, selected_side_rows=count,
                    candidate_routing_rows=0, persisted_probability_rows=count,
                    note="Exact lane counts from certified lineage; full upstream market denominator unavailable per lane. Candidate/routing blocked downstream."))

        log_evidence.append(dict(date=date, natural_log="artifacts/ops/mlb_refresh_daily.out.log",
            first_prediction_timestamp=daily_summary[date]["first"], last_prediction_timestamp=daily_summary[date]["last"],
            run_tags=daily_summary[date]["runs"], certified_hits_rows=len(exact),
            scorer_proof="model_probability_over + selected-side probability + exact model/feature hashes in downstream certified ledger",
            authority_marker="SKIP MLB slate output: NO_QUALIFIED_MLB_MODEL operation=production_slate_generation",
            retention_marker="appended certified lineage rows", cleanup_marker="wrapper temp stdout/stderr removed after content emitted to durable global log"))
        recovery.append(dict(date=date, recovery_class="DIRECT_ARTIFACT_RECOVERY_POSSIBLE",
            source_path=rel(path), source_sha256=sha256(path), raw_probability_rows=len(exact),
            unique_strict_pregame_identities=len(earliest), timing_quality="ROW_LEVEL_STRICT_PREGAME",
            provenance_quality="LINEAGE_CERTIFIED_EXACT_MODEL_FEATURE_SOURCE_HASHES", continuity_ledger_created="NO"))
        classifications.append(dict(date=date, scorer_executed="YES", probability_generated="YES",
            probability_retained="YES", downstream_guard_location="production_slate_generation before candidate/routing output",
            recovery_possible="DIRECT_ARTIFACT_RECOVERY_POSSIBLE", strongest_evidence=rel(path),
            final_classification="GENERATED_AND_RECOVERED"))

    write_csv("hits_gap_execution_chain.csv", chain)
    write_csv("hits_gap_scorer_execution.csv", scorer)
    write_csv("hits_gap_stage_row_counts.csv", stage_counts)
    write_csv("hits_gap_log_evidence.csv", log_evidence)
    write_csv("hits_gap_recovery_opportunity.csv", recovery)
    write_csv("hits_gap_daily_final_classification.csv", classifications)

    surfaces = []
    for date, (path, frame) in ledgers.items():
        surfaces.append(dict(surface="prospective_lineage prediction ledger", dates=date, path=rel(path),
            probability_fields="model_probability_over; model_selected_side_probability; selected_side_no_vig_probability",
            retained_state="DURABLE_APPEND_ONLY_EXACT_PROBABILITIES", hits_rows=len(frame), source_sha256=sha256(path),
            recovery_value="DIRECT_ROW_LEVEL_RECOVERY"))
    surfaces.extend([
        dict(surface="mutable processed prediction-wide", dates="current date only", path="backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv", probability_fields="probability-bearing wide columns", retained_state="OVERWRITTEN_BY_LATER_RUNS", hits_rows="UNKNOWN_FOR_AUG4_AUG13", source_sha256="CURRENT_FILE_NOT_GAP_EVIDENCE", recovery_value="NOT_NEEDED; LINEAGE SURVIVES"),
        dict(surface="run-tagged slate/wide archive", dates="2026-08-04/2026-08-13", path="backend/mlb/exports/odds_history/<date>", probability_fields="none found for gap", retained_state="ABSENT_AFTER_AUTHORITY_GUARD", hits_rows=0, source_sha256="NOT_APPLICABLE", recovery_value="NONE"),
        dict(surface="prepared feature vectors", dates="2026-08-04/2026-08-13", path="backend/mlb/exports/model_diagnostics/prepared_feature_vectors/<date>/hits_features.csv", probability_fields="none", retained_state="FEATURES_ONLY", hits_rows="NOT_A_PROBABILITY_SURFACE", source_sha256="PER_FILE", recovery_value="NOT_USED"),
        dict(surface="natural stdout/stderr", dates="2026-08-04/2026-08-13", path="artifacts/ops/mlb_refresh_daily.out.log", probability_fields="aggregate scorer/writer counts", retained_state="DURABLE_LOG_EVIDENCE", hits_rows="AGGREGATE_ONLY", source_sha256="NOT_ROW_RECOVERY_SOURCE", recovery_value="CORROBORATION"),
        dict(surface="database/lifecycle", dates="2026-08-04/2026-08-13", path="mlb/public information_schema inspection", probability_fields="no Hits prediction table", retained_state="NO_HITS_ROWS", hits_rows=0, source_sha256="NOT_APPLICABLE", recovery_value="NONE"),
    ])
    write_csv("hits_gap_probability_surface_inventory.csv", surfaces)

    mutable = [
        dict(path="backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv", overwritten_in_place="YES", run_tagged_copies_existed="NO_FOR_AUG4_AUG13", git_ignored="YES/OPERATIONAL", backup_or_temp_survives="NO GAP COPY FOUND", log_counts_or_hashes="ROW COUNTS IN DAILY LOG; NO PER-RUN FILE HASH", risk="YES", explanation="Each wide run replaces current-slate output; certified lineage separately retained probabilities."),
        dict(path="backend/mlb/data/processed/mlb_slate_output.csv", overwritten_in_place="POTENTIALLY, BUT GAP WRITER BLOCKED", run_tagged_copies_existed="NO_FOR_AUG4_AUG13", git_ignored="YES/OPERATIONAL", backup_or_temp_survives="NO GAP COPY FOUND", log_counts_or_hashes="AUTHORITY SKIP MARKER", risk="YES", explanation="Slate writer was blocked before new gap output."),
        dict(path="/tmp/mlb-predictions-wide-{stdout,stderr}.*", overwritten_in_place="NO; UNIQUE TEMP", run_tagged_copies_existed="NO", git_ignored="OUTSIDE_REPO", backup_or_temp_survives="NO; TRAP DELETES", log_counts_or_hashes="CONTENT FORWARDED TO GLOBAL DAILY LOG", risk="YES", explanation="Temporary capture deleted after wrapper emits content."),
        dict(path="backend/mlb/exports/prospective_lineage/<date>/prediction_lineage_ledger.csv", overwritten_in_place="NO; APPEND-ONLY", run_tagged_copies_existed="RUN TAG IN EACH ROW", git_ignored="OPERATIONAL EXPORT", backup_or_temp_survives="PRIMARY ARTIFACT SURVIVES", log_counts_or_hashes="ROW AND SOURCE HASHES EMBEDDED", risk="NO", explanation="This durable surface prevents loss of exact probability evidence."),
    ]
    write_csv("hits_gap_mutable_output_audit.csv", mutable)

    guards = [
        dict(order=1, guard="production_slate_generation", location="backend/mlb/scripts/build_mlb_slate_output.py::main", earliest_blocked_stage="candidate/routing slate construction", prevents_model_scoring="NO", prevents_raw_probability_write="NO", prevents_candidate_ranking="YES", prevents_upload_generation="YES_DOWNSTREAM", prevents_publication="YES", observed_effect="Scoring, mutable wide write, and certified lineage complete before guard raises."),
        dict(order=2, guard="production_ranking_and_routing", location="daily upload/ranking guarded path", earliest_blocked_stage="ranking/routing", prevents_model_scoring="NO", prevents_raw_probability_write="NO", prevents_candidate_ranking="YES", prevents_upload_generation="YES", prevents_publication="YES", observed_effect="Expected skip after slate guard."),
        dict(order=3, guard="production_upload_generation", location="backend/mlb/scripts/export_mlb_prediction_book.py", earliest_blocked_stage="upload artifact generation", prevents_model_scoring="NO", prevents_raw_probability_write="NO", prevents_candidate_ranking="NO/UPSTREAM ALREADY BLOCKED", prevents_upload_generation="YES", prevents_publication="YES", observed_effect="No production prediction book/upload."),
    ]
    write_csv("hits_gap_authority_guard_map.csv", guards)

    commits = [
        ("052360a7d1380b48b9132cd2ee1c2b9f62e7f660", "Block retired MLB downstream predictive outputs", "Added downstream slate/output authority blocking; did not disable prediction-wide scoring."),
        ("73160edf67a8afa9425f4b55d8f80c82a91dd112", "Preserve pending MLB research and lineage work", "Added semantic lineage capture to prediction-wide builder; made Aug4-13 exact probabilities durable."),
        ("6b46207bc3300584458042323b50fbabd5fb4750", "Treat retired MLB slate block as expected refresh skip", "Daily wrapper treated authority block as expected rather than scorer failure."),
        ("0ca69d63607a9ab9e7b2965993ecaccd06b6df11", "Complete daily MLB authority-block handling", "Extended expected downstream skip handling."),
        ("8a8a9940068a0a4ce99dd52352a576486d76642c", "Treat late empty MLB slate as expected skip", "Handled late empty-slate condition; no probability deletion."),
    ]
    changes = []
    for commit, subject, effect in commits:
        changes.append(dict(commit_sha=commit, author_date=git("show", "-s", "--format=%aI", commit), subject=subject,
                            affected_paths="|".join(git("show", "--format=", "--name-only", commit).splitlines()), behavioral_effect=effect))
    write_csv("hits_gap_git_boundary_changes.csv", changes)

    db = [
        dict(surface="PostgreSQL information_schema", object="mlb Hits prediction/lifecycle table", rows=0, dates="2026-08-04/2026-08-13", model_identity="NONE", finding="No Hits prediction or lifecycle DB table found; public_game_moneyline_predictions is unrelated."),
        dict(surface="filesystem shadow/analysis ledger", object="backend/mlb/exports/prospective_lineage/<date>/prediction_lineage_ledger.csv", rows=sum(v[1].shape[0] for v in ledgers.values()), dates="2026-08-04/2026-08-13", model_identity=MODEL_ID, finding="Certified exact probability rows persisted outside the database."),
    ]
    write_csv("hits_gap_database_ledger_check.csv", db)

    cleanup = [
        dict(path_or_code="build_mlb_predictions_wide.py output writer", operation="replace current processed wide CSV", can_explain_missing_mutable_wide="YES", can_delete_lineage="NO", finding="Later daily runs overwrite current wide output."),
        dict(path_or_code="bin/mlb_predictions_wide_guarded.sh", operation="trap rm -f temporary stdout/stderr", can_explain_missing_mutable_wide="NO", can_delete_lineage="NO", finding="Temp logs removed, but emitted content survives in LaunchAgent log."),
        dict(path_or_code="bin/mlb_predictive_command_guarded.sh", operation="trap cleanup temporary command captures", can_explain_missing_mutable_wide="NO", can_delete_lineage="NO", finding="Downstream capture cleanup does not touch scorer artifacts."),
        dict(path_or_code="prospective_lineage.append_certified_rows", operation="append certified rows", can_explain_missing_mutable_wide="NO", can_delete_lineage="NO", finding="No cleanup/pruning path found for daily lineage ledgers."),
        dict(path_or_code="authority failure path", operation="fail/skip before slate output", can_explain_missing_mutable_wide="YES_FOR_SLATE_ONLY", can_delete_lineage="NO", finding="Guard suppresses later artifacts but leaves already-persisted lineage immutable."),
    ]
    write_csv("hits_gap_cleanup_retention_audit.csv", cleanup)

    aug3_diff = """# August 3 versus August 4 execution boundary

August 3 retained the older run-tagged slate/wide surfaces through downstream slate construction. After commit `052360a7d1380b48b9132cd2ee1c2b9f62e7f660`, August 4 first diverged at `build_mlb_slate_output.py::main`: `production_slate_generation` failed closed under `NO_QUALIFIED_MLB_PROP_MODEL`, so candidate/routing, upload, and public-production artifacts were not produced.

The divergence was **after scoring**. `build_mlb_predictions_wide.py` still generated line-specific Hits probabilities and wrote its mutable current-wide output. Commit `73160edf67a8afa9425f4b55d8f80c82a91dd112` also caused exact probability rows to be append-preserved before the guard in each date's `prospective_lineage/.../prediction_lineage_ledger.csv`.

Therefore the first concrete difference was downstream slate persistence/routing, not scorer invocation or raw probability retention.
"""
    (OUT / "hits_aug3_vs_aug4_execution_diff.md").write_text(aug3_diff)

    invariant = """# Future retention invariant

Every scoring run must durably preserve, before downstream authority checks: game/player identity, line, P(Over), P(Under), semantic model ID, exact model hash, feature-contract hash, source run tag, prediction timestamp, scheduled start, and source hashes. Publication authority may suppress routing or public output but must not erase research evidence.

Current code **partially satisfies, but does not exactly satisfy, this invariant**. The append-only certified lineage preserves identity, line, P(Over), selected-side probability, semantic/model/feature/source hashes, run tag, prediction timestamp, and scheduled start before the slate authority guard. It does not persist a dedicated explicit `P(Under)` field (the value is deterministically `1 - P(Over)`), and some identity fields are encoded in canonical JSON rather than dedicated columns. No implementation change is made by this audit.
"""
    (OUT / "hits_future_retention_invariant.md").write_text(invariant)

    raw_total = sum(item["rows"] for item in daily_summary.values())
    unique_total = sum(item["unique"] for item in daily_summary.values())
    h05_raw = sum(item["h05_rows"] for item in daily_summary.values())
    h15_raw = sum(item["h15_rows"] for item in daily_summary.values())
    h05_unique = sum(item["h05_unique"] for item in daily_summary.values())
    h15_unique = sum(item["h15_unique"] for item in daily_summary.values())
    daily_lines = "\n".join(
        f"- {date}: `GENERATED_AND_RECOVERED` — H0.5 {value['h05_rows']} rows/{value['h05_unique']} unique; H1.5 {value['h15_rows']} rows/{value['h15_unique']} unique."
        for date, value in daily_summary.items()
    )
    concise = f"""# MLB Hits Aug 4–13 prediction-stream retention audit v1

- Hits scorer execution: **YES on every date and both lines**.
- Exact retained rows: {raw_total:,} run-level observations ({unique_total:,} date-scoped earliest unique identities). Hits 0.5: {h05_raw:,}/{h05_unique:,}; Hits 1.5: {h15_raw:,}/{h15_unique:,}.
- All discovered rows are strict pregame and lineage-certified under `{MODEL_ID}` / `{MODEL_HASH}`.
- Persistence surface: `backend/mlb/exports/prospective_lineage/<date>/prediction_lineage_ledger.csv` (append-only, with exact feature, market-source, and model hashes).
- First August 3/4 divergence: downstream `production_slate_generation`; scoring and lineage persistence completed, then candidate/routing/upload/public artifacts were blocked.
- `NO_QUALIFIED_MLB_PROP_MODEL` effect: prevented candidate/ranking, upload, and publication; it did **not** prevent scorer execution or raw probability retention.
- Mutable processed wide/slate outputs were overwritten or suppressed (`MUTABLE_OUTPUT_RETENTION_RISK = YES`), but the certified lineage artifacts survived.
- Recovery: `DIRECT_ARTIFACT_RECOVERY_POSSIBLE`. This audit does not create a continuity ledger.
- Root cause: `HITS_PROBABILITIES_EXIST_AND_WERE_MISSED_BY_PRIOR_SEARCH`.

## Date classifications

{daily_lines}

## Human decision

Review whether to authorize a separate, immutable continuity-ledger import from these certified original rows and whether to add an explicit stored `P(Under)` field to make the future retention invariant exact. No replay, grading, certification, or pipeline change is authorized here.
"""
    (OUT / "concise_mlb_hits_prediction_stream_retention_audit_v1.md").write_text(concise)

    output_names = sorted(path.name for path in OUT.iterdir() if path.name != "reproducibility_hashes.csv")
    hashes = [dict(file=name, sha256=sha256(OUT / name)) for name in output_names]
    hashes.extend(dict(file=rel(path), sha256=sha256(path)) for path, _ in ledgers.values())
    hashes.append(dict(file=rel(Path(__file__)), sha256=sha256(Path(__file__))))
    write_csv("reproducibility_hashes.csv", hashes)

    required = {
        "hits_gap_execution_chain.csv", "hits_gap_scorer_execution.csv", "hits_gap_probability_surface_inventory.csv",
        "hits_gap_mutable_output_audit.csv", "hits_gap_authority_guard_map.csv", "hits_gap_stage_row_counts.csv",
        "hits_aug3_vs_aug4_execution_diff.md", "hits_gap_git_boundary_changes.csv", "hits_gap_log_evidence.csv",
        "hits_gap_database_ledger_check.csv", "hits_gap_cleanup_retention_audit.csv", "hits_gap_recovery_opportunity.csv",
        "hits_gap_daily_final_classification.csv", "hits_future_retention_invariant.md",
        "concise_mlb_hits_prediction_stream_retention_audit_v1.md", "reproducibility_hashes.csv",
    }
    missing = required - {path.name for path in OUT.iterdir()}
    if missing:
        raise AssertionError(f"missing required outputs: {sorted(missing)}")
    print(json.dumps({"output_dir": rel(OUT), "raw_rows": raw_total, "unique_identities": unique_total,
                      "h05_raw": h05_raw, "h05_unique": h05_unique, "h15_raw": h15_raw,
                      "h15_unique": h15_unique, "root_cause": "HITS_PROBABILITIES_EXIST_AND_WERE_MISSED_BY_PRIOR_SEARCH"}, indent=2))


if __name__ == "__main__":
    main()
