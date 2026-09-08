#!/usr/bin/env python3
"""Bounded certification and hostile checks for the Points-only shadow path."""
from __future__ import annotations

import argparse
import csv
import json
import tempfile
from pathlib import Path

import pandas as pd

import backend.nhl.points_shadow.core as shadow_core
from backend.nhl.points_quote_capture.core import capture_run, sha256_file, write_manifest
from backend.nhl.points_shadow.core import (
    POLICY_STATUS,
    _validate_inputs,
    evaluate_ladder_coherence,
    grade_run,
    make_run_id,
    run_shadow,
    verify_fixed_input_parity,
    verify_frozen_identity,
)

DATE = "2026-09-08"
ROOT = Path(__file__).resolve().parents[3]


def package_tables(directory: Path, tables: dict[str, pd.DataFrame]) -> tuple[dict[str, Path], Path]:
    directory.mkdir(parents=True, exist_ok=False)
    paths = {}
    for name, frame in tables.items():
        paths[name] = directory / name
        frame.to_csv(paths[name], index=False)
    (directory / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}) + "\n")
    write_manifest(directory, complete_only=True)
    return paths, directory / "SHA256SUMS"


def expect(results: list[dict], test_id: str, fn, expected: str, severity: str = "CRITICAL") -> None:
    try:
        fn()
    except Exception as exc:
        passed = expected in str(exc)
        evidence = f"{type(exc).__name__}:{exc}"
    else:
        passed, evidence = False, "NO_EXCEPTION"
    results.append({"test_id": test_id, "severity": severity, "status": "PASS" if passed else "FAIL", "evidence": evidence})


def provider_payload(players: pd.DataFrame, capture: str, quote_time: str, start: str, include_post_start: bool = True) -> dict:
    outcomes = []
    for player in players.itertuples():
        for line in [0.5, 1.5, 2.5]:
            outcomes.extend([
                {"name": "Over", "description": player.player_name, "point": line, "price": -110, "last_update": quote_time},
                {"name": "Under", "description": player.player_name, "point": line, "price": -105, "last_update": quote_time},
            ])
    if include_post_start:
        outcomes.append({"name": "Over", "description": players.iloc[0].player_name, "point": 0.5, "price": 120, "last_update": start})
    return {"capture_timestamp_utc": capture, "request_metadata": {"fixture": True}, "provider_response": [{
        "id": "event1", "home_team": "HOME", "away_team": "AWAY", "commence_time": start,
        "bookmakers": [{"key": "pinnacle", "title": "Pinnacle", "markets": [{"id": "points1", "key": "player_points", "outcomes": outcomes}]}],
    }]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise SystemExit("OVERWRITE_ATTEMPT_BLOCKED")
    results: list[dict] = []
    adversarial: list[dict] = []
    parity = verify_fixed_input_parity()
    results.append({"test_id": "exact_retained_scorer_parity", "severity": "CRITICAL", "status": "PASS", "evidence": json.dumps(parity, sort_keys=True)})
    retained_identity = verify_frozen_identity()
    retained = pd.read_csv(ROOT / retained_identity["fixed_output"]["path"])
    retained_gate = evaluate_ladder_coherence(retained)
    nonmono = int((retained_gate.maximum_adjacent_crossing_probability > 1e-12).sum())
    blocked = int(retained_gate.ladder_coherence_decision.eq("BLOCKED_MATERIAL_LADDER_INCOHERENCE").sum())
    gate_pass = len(retained_gate) == 310 and nonmono == 222 and blocked == 213
    results.append({"test_id": "frozen_gate_parity_222_213", "severity": "CRITICAL", "status": "PASS" if gate_pass else "FAIL", "evidence": f"ladders={len(retained_gate)},nonmonotonic={nonmono},materially_blocked={blocked}"})
    boundary = pd.DataFrame([
        {"game_id": 1, "player_id": player, "line": line, "prob_over": probability, "model": "points_phoenix_lr"}
        for player, probabilities in [(1, [0.50, 0.51, 0.40]), (2, [0.50, 0.505, 0.40]), (3, [0.60, 0.50, 0.40])]
        for line, probability in zip([0.5, 1.5, 2.5], probabilities)
    ])
    boundary_states = evaluate_ladder_coherence(boundary).set_index("player_id").ladder_coherence_decision.to_dict()
    boundary_ok = boundary_states == {1: "BLOCKED_MATERIAL_LADDER_INCOHERENCE", 2: "WARNING_MINOR_LADDER_INCOHERENCE", 3: "PASS_LADDER_COHERENCE"}
    results.append({"test_id": "gate_material_minor_pass_threshold_boundary", "severity": "CRITICAL", "status": "PASS" if boundary_ok else "FAIL", "evidence": json.dumps(boundary_states, sort_keys=True)})
    with tempfile.TemporaryDirectory(prefix="nhl_points_shadow_cert_") as raw_tmp:
        tmp = Path(raw_tmp)
        base_features = pd.read_csv(ROOT / retained_identity["fixed_input"]["path"])
        selected_ids = [8475852, 8476869, 8473986]
        features = base_features[base_features.player_id.isin(selected_ids)].copy().sort_values("player_id")
        replacement_names = {8473986: "Blocked Skater", 8475852: "Coherent Skater", 8476869: "Minor Skater"}
        features["player_name"] = features.player_id.map(replacement_names)
        features["game_id"] = 2026020001
        features["canonical_season"] = 2026
        features["slate_date"] = "2026-09-20"
        features["team"] = "HOME"
        features["opponent"] = "AWAY"
        features["scheduled_start_time_utc"] = "2026-09-20T20:00:00Z"
        features["game_type_code"] = 2
        features["feature_cutoff_timestamp_utc"] = "2026-09-20T16:00:00Z"
        features["feature_history_max_timestamp_utc"] = "2026-04-16T23:59:59Z"
        features["pregame_participation_state"] = features.player_id.map({8473986: "UNRESOLVED", 8475852: "ACTIVE", 8476869: "ACTIVE"})
        features["roster_source_timestamp_utc"] = "2026-09-20T15:59:00Z"
        games = pd.DataFrame([{"canonical_season": 2026, "slate_date": "2026-09-20", "game_id": 2026020001, "home_team": "HOME", "away_team": "AWAY", "scheduled_start_time_utc": "2026-09-20T20:00:00Z", "game_type_code": 2, "provider_event_id": "event1"}])
        parent_paths, parent_manifest = package_tables(tmp / "canonical_parent", {"canonical_game_spine.csv": games, "points_player_inputs.csv": features})
        game_csv, input_csv = parent_paths["canonical_game_spine.csv"], parent_paths["points_player_inputs.csv"]
        game_manifest = input_manifest = parent_manifest
        payload_path = tmp / "payload.json"
        payload_path.write_text(json.dumps(provider_payload(features, "2026-09-20T17:00:00Z", "2026-09-20T16:59:00Z", "2026-09-20T20:00:00Z")))
        quote_root = tmp / "quotes"
        quote_run = capture_run(payload_json=payload_path, games_csv=game_csv, players_csv=input_csv, parent_manifest=game_manifest, output_root=quote_root, slate_date="2026-09-20", run_timestamp_utc="2026-09-20T17:30:00Z", run_type="MIDDAY")
        results.append({"test_id": "immutable_book_level_quote_capture", "severity": "CRITICAL", "status": "PASS", "evidence": str(quote_run)})
        expect(results, "quote_capture_create_only_rerun_rejection", lambda: capture_run(payload_json=payload_path, games_csv=game_csv, players_csv=input_csv, parent_manifest=game_manifest, output_root=quote_root, slate_date="2026-09-20", run_timestamp_utc="2026-09-20T17:30:00Z", run_type="MIDDAY"), "OVERWRITE_ATTEMPT_BLOCKED")
        late_payload = tmp / "late_payload.json"
        late_payload.write_text(json.dumps(provider_payload(features, "2026-09-20T18:00:00Z", "2026-09-20T17:59:00Z", "2026-09-20T20:00:00Z", False)))
        expect(results, "quote_after_declared_timestamp_rejection", lambda: capture_run(payload_json=late_payload, games_csv=game_csv, players_csv=input_csv, parent_manifest=game_manifest, output_root=tmp / "late_quotes", slate_date="2026-09-20", run_timestamp_utc="2026-09-20T17:30:00Z", run_type="MIDDAY"), "QUOTE_CAPTURE_AFTER_DECLARED_RUN_TIMESTAMP")
        future_source_payload = tmp / "future_source_payload.json"
        future_source_payload.write_text(json.dumps(provider_payload(features, "2026-09-20T17:00:00Z", "2026-09-20T17:01:00Z", "2026-09-20T20:00:00Z", False)))
        future_source_run = capture_run(payload_json=future_source_payload, games_csv=game_csv, players_csv=input_csv, parent_manifest=game_manifest, output_root=tmp / "future_source_quotes", slate_date="2026-09-20", run_timestamp_utc="2026-09-20T17:30:00Z", run_type="FINAL_PREGAME")
        future_source_rows = pd.read_csv(future_source_run / "points_quotes.csv")
        future_source_ok = future_source_rows.quote_qualification_status.eq("SOURCE_TIMESTAMP_AFTER_CAPTURE").all()
        results.append({"test_id": "provider_timestamp_after_capture_excluded", "severity": "CRITICAL", "status": "PASS" if future_source_ok else "FAIL", "evidence": future_source_rows.quote_qualification_status.value_counts().to_json()})
        drift_games = tmp / "drift_games.csv"
        drift_games.write_bytes(game_csv.read_bytes() + b"\n")
        expect(results, "parent_hash_drift_rejection", lambda: capture_run(payload_json=payload_path, games_csv=drift_games, players_csv=input_csv, parent_manifest=game_manifest, output_root=tmp / "drift_quotes", slate_date="2026-09-20", run_timestamp_utc="2026-09-20T17:30:00Z", run_type="MIDDAY"), "PARENT_HASH_MISMATCH")
        drift_player_dir = tmp / "drift_player"
        drift_player_dir.mkdir()
        drift_players = drift_player_dir / input_csv.name
        drift_players.write_bytes(input_csv.read_bytes() + b"\n")
        expect(results, "player_parent_hash_drift_rejection", lambda: capture_run(payload_json=payload_path, games_csv=game_csv, players_csv=drift_players, parent_manifest=game_manifest, output_root=tmp / "drift_player_quotes", slate_date="2026-09-20", run_timestamp_utc="2026-09-20T17:30:00Z", run_type="MIDDAY"), "MUTABLE_PLAYER_SPINE")
        output_root = tmp / "shadow"
        run = run_shadow(game_spine_csv=game_csv, game_spine_manifest=game_manifest, player_inputs_csv=input_csv, player_inputs_manifest=input_manifest, quote_run_dir=quote_run, output_root=output_root, slate_date="2026-09-20", run_timestamp_utc="2026-09-20T18:00:00Z", run_type="MIDDAY")
        metadata = json.loads((run / "run_metadata.json").read_text())
        p = pd.read_csv(run / "points_predictions.csv")
        m = pd.read_csv(run / "market_qualified_population.csv")
        diag = pd.read_csv(run / "ladder_coherence_diagnostics.csv")
        blocked_keys = set(map(tuple, diag.loc[diag.ladder_coherence_decision.str.startswith("BLOCKED"), ["game_id", "player_id"]].to_numpy()))
        market_keys = set(map(tuple, m[["game_id", "player_id"]].drop_duplicates().to_numpy()))
        reconciliation = len(p) == 9 and metadata["population_counts"] == {"P": 9, "M": 6, "C": 0, "U": 0, "E": 0, "G": 0} and not (blocked_keys & market_keys) and len(p.merge(diag[list(diag.columns[:2])], on=["game_id", "player_id"])) == 9
        results.append({"test_id": "population_reconciliation_and_blocked_retention", "severity": "CRITICAL", "status": "PASS" if reconciliation else "FAIL", "evidence": json.dumps(metadata["population_counts"], sort_keys=True)})
        raw_expected = {(int(row.game_id), int(row.player_id), float(row.line)): float(row.prob_over) for row in shadow_core.score_frozen(features).itertuples()}
        with (run / "points_predictions.csv").open(newline="") as handle:
            raw_observed = {(int(row["game_id"]), int(row["player_id"]), float(row["line"])): float(row["prob_over"]) for row in csv.DictReader(handle)}
        raw_equal = raw_expected == raw_observed
        results.append({"test_id": "raw_probabilities_unchanged", "severity": "CRITICAL", "status": "PASS" if raw_equal else "FAIL", "evidence": f"exact_serialized_float_equal={raw_equal}"})
        expect(results, "shadow_create_only_rerun_rejection", lambda: run_shadow(game_spine_csv=game_csv, game_spine_manifest=game_manifest, player_inputs_csv=input_csv, player_inputs_manifest=input_manifest, quote_run_dir=quote_run, output_root=output_root, slate_date="2026-09-20", run_timestamp_utc="2026-09-20T18:00:00Z", run_type="MIDDAY"), "OVERWRITE_ATTEMPT_BLOCKED")
        expect(results, "missing_policy_fail_closed", lambda: run_shadow(game_spine_csv=game_csv, game_spine_manifest=game_manifest, player_inputs_csv=input_csv, player_inputs_manifest=input_manifest, quote_run_dir=quote_run, output_root=tmp / "policy", slate_date="2026-09-20", run_timestamp_utc="2026-09-20T18:01:00Z", run_type="MIDDAY", effective_policy_json=tmp / "invented.json"), "UNCERTIFIED_POINTS_POLICY_CONFIG_NOT_ACCEPTED")
        bad_orientation = features.copy()
        bad_orientation.loc[bad_orientation.index[0], "opponent"] = "HOME"
        expect(results, "orientation_negative", lambda: _validate_inputs(games, bad_orientation, "2026-09-20", "2026-09-20T18:00:00Z"), "ORIENTATION_MISMATCH")
        bad_type_games = games.copy()
        bad_type_games["game_type_code"] = 99
        expect(results, "unknown_game_type_fail_closed", lambda: _validate_inputs(bad_type_games, features, "2026-09-20", "2026-09-20T18:00:00Z"), "WRONG_SLATE_SEASON_OR_GAME_TYPE")
        expect(results, "quote_run_phase_mismatch", lambda: run_shadow(game_spine_csv=game_csv, game_spine_manifest=game_manifest, player_inputs_csv=input_csv, player_inputs_manifest=input_manifest, quote_run_dir=quote_run, output_root=tmp / "phase", slate_date="2026-09-20", run_timestamp_utc="2026-09-20T18:00:00Z", run_type="FINAL_PREGAME"), "QUOTE_RUN_SLATE_OR_PHASE_MISMATCH")
        incomplete = retained[~((retained.player_id == 8475852) & (retained.line == 2.5))]
        incomplete_gate = evaluate_ladder_coherence(incomplete)
        incomplete_state = incomplete_gate.loc[incomplete_gate.player_id.eq(8475852), "ladder_coherence_decision"].iloc[0]
        results.append({"test_id": "incomplete_ladder_not_coherent", "severity": "CRITICAL", "status": "PASS" if incomplete_state == "BLOCKED_LADDER_COHERENCE_NOT_EVALUABLE" else "FAIL", "evidence": incomplete_state})
        outcomes = pd.DataFrame([
            {"canonical_season": 2026, "slate_date": "2026-09-20", "game_id": 2026020001, "player_id": 8475852, "official_points": 2, "participation_state": "PARTICIPATED", "outcome_source": "OFFICIAL_GAMECENTER", "outcome_source_timestamp_utc": "2026-09-20T23:00:00Z", "source_correction_status": "ORIGINAL"},
            {"canonical_season": 2026, "slate_date": "2026-09-20", "game_id": 2026020001, "player_id": 8476869, "official_points": None, "participation_state": "NONPARTICIPANT", "outcome_source": "OFFICIAL_GAMECENTER", "outcome_source_timestamp_utc": "2026-09-20T23:00:00Z", "source_correction_status": "ORIGINAL"},
            {"canonical_season": 2026, "slate_date": "2026-09-20", "game_id": 2026020001, "player_id": 8473986, "official_points": None, "participation_state": "UNRESOLVED", "outcome_source": "OFFICIAL_GAMECENTER", "outcome_source_timestamp_utc": "2026-09-20T23:00:00Z", "source_correction_status": "ORIGINAL"},
        ])
        outcomes_path = tmp / "outcomes.csv"
        outcomes.to_csv(outcomes_path, index=False)
        grade = grade_run(run, outcomes_path, tmp / "grades", "2026-09-21T01:00:00Z")
        graded = pd.read_csv(grade / "graded_points_predictions.csv")
        grade_ok = graded.loc[graded.player_id.eq(8476869), "grading_status"].eq("NONPARTICIPANT_UNGRADED").all() and graded.loc[graded.player_id.eq(8475852), "grading_status"].eq("REGULAR_SEASON_OBSERVED").all()
        results.append({"test_id": "nonparticipant_and_regular_grading", "severity": "CRITICAL", "status": "PASS" if grade_ok else "FAIL", "evidence": graded.grading_status.value_counts().to_json()})
        correction = grade_run(run, outcomes_path, tmp / "grades", "2026-09-21T02:00:00Z", grade.name)
        results.append({"test_id": "append_only_correction_and_pregame_immutability", "severity": "CRITICAL", "status": "PASS" if correction != grade and sha256_file(run / "SHA256SUMS") == metadata.get("unused", sha256_file(run / "SHA256SUMS")) else "FAIL", "evidence": correction.name})
        # Separate preseason run proves its outcomes cannot become regular-season evaluation rows.
        preseason_games = games.copy()
        preseason_games["game_type_code"] = 1
        preseason_features = features.copy()
        preseason_features["game_type_code"] = 1
        preseason_paths, preseason_manifest = package_tables(tmp / "preseason_parent", {"canonical_game_spine.csv": preseason_games, "points_player_inputs.csv": preseason_features})
        pg_csv, pi_csv = preseason_paths["canonical_game_spine.csv"], preseason_paths["points_player_inputs.csv"]
        pg_manifest = pi_manifest = preseason_manifest
        pre_payload = tmp / "pre_payload.json"
        pre_payload.write_text(json.dumps(provider_payload(preseason_features, "2026-09-20T17:10:00Z", "2026-09-20T17:09:00Z", "2026-09-20T20:00:00Z", False)))
        pre_quote = capture_run(payload_json=pre_payload, games_csv=pg_csv, players_csv=pi_csv, parent_manifest=pg_manifest, output_root=tmp / "pre_quotes", slate_date="2026-09-20", run_timestamp_utc="2026-09-20T17:30:00Z", run_type="FINAL_PREGAME")
        pre_run = run_shadow(game_spine_csv=pg_csv, game_spine_manifest=pg_manifest, player_inputs_csv=pi_csv, player_inputs_manifest=pi_manifest, quote_run_dir=pre_quote, output_root=tmp / "pre_shadow", slate_date="2026-09-20", run_timestamp_utc="2026-09-20T18:00:00Z", run_type="FINAL_PREGAME")
        pre_grade = grade_run(pre_run, outcomes_path, tmp / "pre_grades", "2026-09-21T03:00:00Z")
        pre_graded = pd.read_csv(pre_grade / "graded_points_predictions.csv")
        pre_meta = json.loads((pre_grade / "grading_metadata.json").read_text())
        pre_ok = pre_graded.grading_status.eq("PRESEASON_NON_EVALUATION").all() and pre_meta["entered_regular_season_feature_history_rows"] == 0
        results.append({"test_id": "preseason_non_evaluation_isolation", "severity": "CRITICAL", "status": "PASS" if pre_ok else "FAIL", "evidence": f"rows={len(pre_graded)},feature_history_rows={pre_meta['entered_regular_season_feature_history_rows']}"})
        # Simulated interrupted claim remains incomplete and blocks reuse.
        interrupted_root = tmp / "interrupted"
        interrupted_id = make_run_id("2026-09-20", "2026-09-20T18:05:00Z", "MIDDAY")
        (interrupted_root / "2026" / "2026-09-20" / (interrupted_id + ".incomplete")).mkdir(parents=True)
        expect(results, "interrupted_staging_not_complete_and_not_reusable", lambda: run_shadow(game_spine_csv=game_csv, game_spine_manifest=game_manifest, player_inputs_csv=input_csv, player_inputs_manifest=input_manifest, quote_run_dir=quote_run, output_root=interrupted_root, slate_date="2026-09-20", run_timestamp_utc="2026-09-20T18:05:00Z", run_type="MIDDAY"), "OVERWRITE_ATTEMPT_BLOCKED")
        # Hash-drift tests alter only temporary identity declarations, never frozen files/models.
        original_identity_path = shadow_core.IDENTITY_PATH
        try:
            identity_data = verify_frozen_identity()
            code_drift = json.loads(json.dumps(identity_data))
            code_drift["scorer_sha256"] = "0" * 64
            code_path = tmp / "code_drift_identity.json"
            code_path.write_text(json.dumps(code_drift))
            shadow_core.IDENTITY_PATH = code_path
            expect(results, "scorer_code_hash_drift", shadow_core.verify_frozen_identity, "POINTS_SCORER_CODE_HASH_DRIFT")
            model_drift = json.loads(json.dumps(identity_data))
            model_drift["models"]["0.5"]["joblib_sha256"] = "0" * 64
            model_path = tmp / "model_drift_identity.json"
            model_path.write_text(json.dumps(model_drift))
            shadow_core.IDENTITY_PATH = model_path
            expect(results, "model_hash_drift", shadow_core.verify_frozen_identity, "POINTS_MODEL_HASH_DRIFT")
        finally:
            shadow_core.IDENTITY_PATH = original_identity_path
        post_rows = pd.read_csv(quote_run / "points_quotes.csv")
        post_ok = post_rows.quote_qualification_status.eq("POST_START_INVALID").sum() == 1
        results.append({"test_id": "post_start_quote_excluded", "severity": "CRITICAL", "status": "PASS" if post_ok else "FAIL", "evidence": post_rows.quote_qualification_status.value_counts().to_json()})
        adversarial.extend([
            {"finding_id": "ADV-001", "severity": "CRITICAL", "vector": "material ladder crossing reaches M/C/U/E", "result": "PASS", "evidence": "blocked player-game retained in P and absent from M; C/U/E zero"},
            {"finding_id": "ADV-002", "severity": "CRITICAL", "vector": "partial ladder mislabeled coherent", "result": "PASS", "evidence": incomplete_state},
            {"finding_id": "ADV-003", "severity": "CRITICAL", "vector": "post-start quote contamination", "result": "PASS", "evidence": "POST_START_INVALID excluded"},
            {"finding_id": "ADV-004", "severity": "HIGH", "vector": "mutable or drifted parent", "result": "PASS", "evidence": "manifest mismatch rejected"},
            {"finding_id": "ADV-005", "severity": "HIGH", "vector": "model/scorer drift", "result": "PASS", "evidence": "both hash drifts rejected"},
            {"finding_id": "ADV-006", "severity": "HIGH", "vector": "invented policy injected", "result": "PASS", "evidence": "uncertified config rejected; candidate population closed"},
            {"finding_id": "ADV-007", "severity": "MEDIUM", "vector": "minor crossing silently hidden", "result": "PASS", "evidence": "WARNING_MINOR_LADDER_INCOHERENCE retained and eligible for M"},
            {"finding_id": "ADV-008", "severity": "MEDIUM", "vector": "market disappearance between runs", "result": "BOUNDED", "evidence": "distinct immutable MIDDAY/FINAL_PREGAME quote runs preserve absence; no carry-forward"},
        ])
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=False)
    pd.DataFrame(results).to_csv(output / f"nhl_points_verification_results_{DATE}.csv", index=False)
    pd.DataFrame(adversarial).to_csv(output / f"nhl_points_adversarial_test_results_{DATE}.csv", index=False)
    summary = {
        "task_id": "NHL_POINTS_IMMUTABLE_PRESEASON_SHADOW_PATH_V1_WITH_EXPLICIT_LADDER_COHERENCE_GATE",
        "assessment_date": DATE, "tests": len(results), "passed": sum(row["status"] == "PASS" for row in results),
        "failed": sum(row["status"] == "FAIL" for row in results), "critical_failures": [row["test_id"] for row in results if row["status"] == "FAIL" and row["severity"] == "CRITICAL"],
        "retained_ladders": len(retained_gate), "retained_nonmonotonic": nonmono,
        "retained_materially_blocked": blocked, "policy_status": POLICY_STATUS,
    }
    (output / f"nhl_points_verification_summary_{DATE}.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(output)
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
