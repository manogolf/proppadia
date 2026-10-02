from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from backend.nhl import cli
from backend.nhl.daily_capture import canonical_game_set_hash, sha256_file, verify_package
from backend.nhl.daily_orchestration import (
    DailyRunRecorder,
    LANE_NAMES,
    LEGACY_SOG_TOI_REASON,
    legacy_sog_toi_population_diagnostic,
    verify_roster_observation_reuse,
)


UTC = timezone.utc
SLATE = "2026-09-24"
GAME_IDS = list(range(2026010037, 2026010048))
GAME_HASH = "92d828be583187109116de1eccda70c2bf8563288ec6ad39ce3976da43a9eec3"
RETAINED_ROSTER = Path(
    "artifacts/operational/nhl/roster_observations/season=2026/"
    "slate_date=2026-09-24/observation=20260924T174037.492521Z_353e5992af746639"
)


def recorder() -> DailyRunRecorder:
    value = DailyRunRecorder(
        run_id="test-run", command=["python", "-m", "backend.nhl.cli", "daily"],
        phase="EARLY", started_at=datetime(2026, 9, 24, 17, tzinfo=UTC))
    value.set_canonical(
        slate_date=SLATE, season=2026, game_ids=GAME_IDS, game_set_hash=GAME_HASH)
    value.finish_lane("shared_prerequisites")
    value.finish_lane("roster")
    return value


def finish_unowned_lanes(value: DailyRunRecorder) -> None:
    for name in LANE_NAMES:
        if value.lane(name).status == "NOT_STARTED":
            value.finish_lane(name, status="NOT_AVAILABLE_EXTERNAL_OWNER")


def write_prediction(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "player_id,game_id,game_date,p_over_0_5,p_over_18_5\n"
        "1,2026010037,2026-09-24,0.5,0.5\n")


def fake_prepare_scoring_input(**kwargs):
    path = Path(kwargs["output_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("fixture\n")
    return {"path": str(path.resolve()), "row_count": 1}


def fake_validate_prediction_output(**_kwargs):
    return {
        "validated_prediction_identity": True,
        "natural_identity_count": 1,
        "conditional_prediction_count": 1,
    }


def fake_attachment_audit(**kwargs):
    attachment = Path(kwargs["attachment_path"])
    return {
        "schema_version": "NHL_ATTACHMENT_INTEGRITY_V1",
        "lane": kwargs["lane"],
        "status": "PASS",
        "prediction_artifact_sha256": kwargs["expected_prediction_sha256"],
        "attachment_sha256": sha256_file(attachment),
        "odds_observation_manifest_sha256": kwargs["expected_odds_manifest_sha256"],
        "counts": {
            "prediction_row_count": 1, "attachment_row_count": 1,
            "unique_prediction_key_count": 1, "unique_attachment_key_count": 1,
            "duplicate_prediction_key_count": 0, "duplicate_attachment_key_count": 0,
            "missing_prediction_key_count": 0, "extra_attachment_key_count": 0,
            "matched_count": 0, "unmatched_count": 1, "ambiguous_count": 0,
        },
        "checks": {"fixture": True},
    }


def fake_odds_lineage(**kwargs):
    return {
        "odds_observation_path": str(Path(kwargs["observation_dir"]).resolve()),
        "odds_observation_manifest_sha256": kwargs["expected_manifest_sha256"],
        "odds_raw_response_sha256": sha256_file(Path(kwargs["odds_json"])),
    }


def test_212_of_713_records_the_retired_legacy_sog_population_gate():
    gate = legacy_sog_toi_population_diagnostic(
        population_rows=713, null_5v5=212, null_season_5v5=212)
    assert gate == {
        "population_rows": 713,
        "null_szn_toi_per_game_5on5": 212,
        "null_season_5on5_icetime_per_game": 212,
        "null_ratio": 212 / 713,
        "maximum_null_ratio": 0.20,
        "legacy_population_gate_would_block": True,
    }
    value = recorder()
    value.finish_lane("legacy_sog", status="BLOCKED_LANE_LOCAL", reason=LEGACY_SOG_TOI_REASON)
    value.finish_lane("points")
    value.finish_lane("saves")
    finish_unowned_lanes(value)
    assert value.classification() == "READY_WITH_BOUNDED_LANE_WARNING"


def test_legacy_sog_below_threshold_has_no_retired_gate_warning():
    gate = legacy_sog_toi_population_diagnostic(
        population_rows=713, null_5v5=142, null_season_5v5=142)
    assert gate["legacy_population_gate_would_block"] is False
    value = recorder()
    for name in LANE_NAMES:
        if value.lane(name).status == "NOT_STARTED":
            value.finish_lane(name)
    assert value.classification() == "READY"


def test_parent_receipt_is_append_only_and_manifest_verified(tmp_path):
    value = recorder()
    for name in LANE_NAMES:
        if value.lane(name).status == "NOT_STARTED":
            value.finish_lane(name)
    package = value.finalize(tmp_path)
    assert verify_package(package) == sha256_file(package / "SHA256SUMS")
    receipt = json.loads((package / "parent_receipt.json").read_text())
    assert receipt["final_classification"] == "READY"
    assert set(receipt["lanes"]) == set(LANE_NAMES)
    with pytest.raises(RuntimeError, match="RECEIPT_EXISTS"):
        value.finalize(tmp_path)


@pytest.mark.parametrize("classification", [
    "READY", "READY_WITH_BOUNDED_LANE_WARNING", "FAILED_BLOCKING",
])
def test_parent_receipt_finalizes_all_classifications(tmp_path, classification):
    value = recorder()
    if classification == "READY_WITH_BOUNDED_LANE_WARNING":
        value.finish_lane("legacy_sog", status="BLOCKED_LANE_LOCAL", reason=LEGACY_SOG_TOI_REASON)
    elif classification == "FAILED_BLOCKING":
        value.fail_lane("shared_prerequisites", RuntimeError("canonical slate corrupt"), blocking=True)
    for name in LANE_NAMES:
        if value.lane(name).status == "NOT_STARTED":
            value.finish_lane(name)
    package = value.finalize(tmp_path / classification)
    payload = json.loads((package / "parent_receipt.json").read_text())
    assert payload["final_classification"] == classification
    assert verify_package(package)


def test_exact_retained_roster_fixture_is_reusable():
    reference = verify_roster_observation_reuse(
        RETAINED_ROSTER, season=2026, slate_date=SLATE, phase="EARLY",
        canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH)
    assert reference["reuse_mode"] == "EXPLICIT_VERIFIED_IMMUTABLE_SOURCE"
    assert reference["source_parent_daily_run_id"] == "nhldaily_20260924T173759595605Z_2d9d4dfc"
    assert reference["manifest_sha256"] == "400e5339cd13c16a8057426584416c5dd8eb71322a4646d23944016aac419303"


def test_roster_reuse_validation_is_read_only_and_provider_free():
    before = {
        path.relative_to(RETAINED_ROSTER).as_posix(): sha256_file(path)
        for path in RETAINED_ROSTER.iterdir() if path.is_file()
    }
    siblings_before = sorted(path.name for path in RETAINED_ROSTER.parent.iterdir())
    with patch("requests.Session.request") as provider_request:
        verify_roster_observation_reuse(
            RETAINED_ROSTER, season=2026, slate_date=SLATE, phase="EARLY",
            canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH)
    after = {
        path.relative_to(RETAINED_ROSTER).as_posix(): sha256_file(path)
        for path in RETAINED_ROSTER.iterdir() if path.is_file()
    }
    assert after == before
    assert sorted(path.name for path in RETAINED_ROSTER.parent.iterdir()) == siblings_before
    provider_request.assert_not_called()


def test_roster_reuse_disables_all_comprehensive_roster_fetch_paths():
    with patch.object(cli, "run") as child:
        fetched = cli._refresh_all_team_rosters_for_daily(
            slate=SLATE, reuse_roster_observation=RETAINED_ROSTER)
    assert fetched is False
    child.assert_not_called()
    assert cli._roster_refresh_environment(
        slate_date="2026-09-23", reuse_roster_observation=RETAINED_ROSTER,
    ) == {"SLATE_DATE": "2026-09-23", "NHL_FETCH_DISABLE": "1"}


def test_roster_reuse_tampered_manifest_fails(tmp_path):
    target = tmp_path / "roster"
    shutil.copytree(RETAINED_ROSTER, target)
    (target / "roster_snapshot.jsonl").write_text("tampered\n")
    with pytest.raises(RuntimeError, match="MANIFEST_MISMATCH"):
        verify_roster_observation_reuse(
            target, season=2026, slate_date=SLATE, phase="EARLY",
            canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH)


@pytest.mark.parametrize("override,expected", [
    ({"season": 2025}, "season"),
    ({"slate_date": "2026-09-25"}, "slate_date"),
    ({"phase": "REFRESH"}, "phase"),
    ({"canonical_game_set_hash": "bad"}, "canonical_game_set_hash"),
    ({"complete_per_game_coverage": False}, "complete_per_game_coverage"),
    ({"conflict_count": 1}, "zero_conflicts"),
    ({"strictly_prestart": False}, "strictly_prestart"),
])
def test_roster_reuse_metadata_mismatch_fails_closed(tmp_path, override, expected):
    target = tmp_path / "roster"
    shutil.copytree(RETAINED_ROSTER, target)
    summary_path = target / "observation_summary.json"
    summary = json.loads(summary_path.read_text())
    summary.update(override)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    files = sorted(path for path in target.iterdir() if path.is_file() and path.name != "SHA256SUMS")
    (target / "SHA256SUMS").write_text(
        "".join(f"{sha256_file(path)}  {path.name}\n" for path in files))
    with pytest.raises(RuntimeError, match=expected):
        verify_roster_observation_reuse(
            target, season=2026, slate_date=SLATE, phase="EARLY",
            canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH)


def test_reuse_preflight_failure_stops_before_daily_implementation(tmp_path, monkeypatch):
    bad = tmp_path / "bad"
    shutil.copytree(RETAINED_ROSTER, bad)
    (bad / "roster_snapshot.jsonl").write_text("tampered\n")
    monkeypatch.setenv("NHL_DAILY_RECEIPT_ROOT", str(tmp_path / "receipts"))
    with patch.object(cli, "_cmd_daily_impl") as implementation:
        with pytest.raises(RuntimeError, match="MANIFEST_MISMATCH"):
            cli.cmd_daily(
                with_odds=True, odds_phase="EARLY", reuse_roster_observation=bad)
    implementation.assert_not_called()
    package = next((tmp_path / "receipts").glob("run_id=*"))
    receipt = json.loads((package / "parent_receipt.json").read_text())
    assert receipt["final_classification"] == "FAILED_BLOCKING"


def test_reuse_argument_is_explicit():
    args = cli.build_arg_parser().parse_args([
        "daily", "--with-odds", "--reuse-roster-observation", str(RETAINED_ROSTER)])
    assert args.reuse_roster_observation == RETAINED_ROSTER


def test_successful_child_structured_summary_is_retained():
    value = recorder()
    completed = subprocess.CompletedProcess(
        ["child"], 0,
        stdout=("human line\nNHL_CHILD_SUMMARY_JSON=" + json.dumps({
            "schema_version": "NHL_ROSTER_CHILD_SUMMARY_V1",
            "normalizer_counts": {"source_rows": 789},
            "roster_observation": "/immutable/roster",
            "roster_manifest_sha256": "a" * 64,
        }) + "\n"), stderr="")
    with patch.object(cli, "_ACTIVE_DAILY_RECORDER", value), patch.object(
            cli.sp, "run", return_value=completed):
        cli.run(["child"])
    assert value.children[0]["normalizer_counts"] == {"source_rows": 789}
    assert "human line" not in json.dumps(value.children)


def test_blocked_sog_continues_points_saves_odds_without_sog_consumers(tmp_path):
    value = recorder()
    value.finish_lane("legacy_sog", status="BLOCKED_LANE_LOCAL", reason=LEGACY_SOG_TOI_REASON)
    proc, exports, models, site, archive = (
        tmp_path / "proc", tmp_path / "exports", tmp_path / "models",
        tmp_path / "site", tmp_path / "archive")
    (models / "latest/goalie_saves").mkdir(parents=True)
    exports.mkdir(); site.mkdir()
    (exports / "train_goalie_saves_v2.csv").write_text("fixture\n")
    (exports / "train_nhl_points_v2.csv").write_text("fixture\n")
    names = tmp_path / "names.csv"
    names.write_text("player_id,game_id,team_id,team_code,full_name,game_date\n1,2026010037,1,NJD,A,2026-09-24\n")
    odds_dir = tmp_path / "odds"
    odds_dir.mkdir()
    for name, body in (("raw_response.json", "[]\n"), ("events_response.json", "[]\n"),
                       ("request_plan.json", "{}\n")):
        (odds_dir / name).write_text(body)
    odds = SimpleNamespace(
        classification="CAPTURED_VALID_EMPTY", observation_dir=odds_dir,
        manifest_sha256="b" * 64,
        summary={"network_attempt_count": 1, "credits_consumed": 0, "maximum_credits": 8})

    def fake_run(command, **_kwargs):
        values = list(map(str, command))
        if "--out" in values:
            out = Path(values[values.index("--out") + 1])
            if "score_nhl_saves_with_lineage.py" in values[1] or "score_nhl_points_with_lineage.py" in values[1]:
                write_prediction(out)
            elif "build_saves_with_market.py" in values[1] or "build_points_with_market.py" in values[1]:
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text("player_id,game_date\n1,2026-09-24\n")
                unmatched = Path(values[values.index("--unmatched") + 1])
                unmatched.write_text("player_id\n")
                if "--ambiguous" in values:
                    Path(values[values.index("--ambiguous") + 1]).write_text("prediction_index\n")
        return subprocess.CompletedProcess(values, 0, "", "")

    with patch.multiple(
        cli, PROC_DIR=proc, EXPORTS_DIR=exports, MODELS_DIR=models, SITE_DIR=site,
        EXPORTS_ODDS_HISTORY_DIR=archive,
    ), patch.object(cli, "run", side_effect=fake_run), patch.object(
        cli, "export_names_csv", return_value=names), patch.object(
        cli, "run_optional_odds_observation", return_value=odds), patch.object(
        cli, "prepare_scoring_input", side_effect=fake_prepare_scoring_input), patch.object(
        cli, "validate_prediction_output", side_effect=fake_validate_prediction_output), patch.object(
        cli, "validate_odds_observation", side_effect=fake_odds_lineage), patch.object(
        cli, "audit_attachment_files", side_effect=fake_attachment_audit), patch.object(
        cli, "refresh_sog_residual_dataset") as residual, patch.object(
        cli, "refresh_sog_reconcile_artifacts") as reconcile, patch.object(
        cli, "build_sog") as build_sog:
        cli._run_independent_daily_lanes(
            recorder=value, db="fixture", slate=SLATE, with_odds=True,
            odds_phase="EARLY", daily_run_id="test-run", canonical_games=[],
            saves_export_ready=True, points_export_ready=True,
            legacy_sog_prediction=None)
    assert value.lane("points").status == "COMPLETE"
    assert value.lane("saves").status == "COMPLETE"
    assert value.lane("odds").provider_requests == 1
    assert value.lane("sog_attachment").status == "SKIPPED_UPSTREAM_LANE_BLOCKED"
    assert value.lane("points_attachment").status == "COMPLETE"
    assert value.lane("saves_attachment").status == "COMPLETE"
    assert value.classification() == "READY_WITH_BOUNDED_LANE_WARNING"
    prediction_paths = {
        Path(output["path"])
        for lane_name in ("points", "saves")
        for output in value.lane(lane_name).outputs
    }
    assert all(path.parent == proc / "daily_runs" / "test-run" for path in prediction_paths)
    build_sog.assert_not_called()
    residual.assert_not_called()
    reconcile.assert_not_called()


def test_cold_start_sog_is_reference_only_and_creates_nothing(tmp_path):
    value = recorder()
    package = (
        tmp_path / "artifacts/operational/nhl/sog_prediction_only/season=2026"
        f"/slate_date={SLATE}/phase=EARLY/run_id=existing"
    )
    status_dir = tmp_path / f"artifacts/operational/nhl/sog_prediction_only/status/{SLATE}"
    package.mkdir(parents=True)
    status_dir.mkdir(parents=True)
    package_payload = package / "prediction.json"
    package_payload.write_text("{}\n")
    (package / "SHA256SUMS").write_text(
        f"{sha256_file(package_payload)}  prediction.json\n")
    (status_dir / "prediction_existing.json").write_text("{}\n")
    before = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    with patch.object(cli, "ROOT", tmp_path):
        cli._reference_cold_start_sog(value, SLATE)
    after = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    assert after == before
    assert value.lane("cold_start_sog_reference").status == "REFERENCED_EXTERNAL_OWNER"
    assert len(value.lane("cold_start_sog_reference").outputs) == 2


@pytest.mark.parametrize("failed_lane", ["points", "saves"])
def test_prediction_lane_failure_does_not_suppress_other_lane_or_odds(tmp_path, failed_lane):
    value = recorder()
    value.finish_lane("legacy_sog", status="BLOCKED_LANE_LOCAL", reason=LEGACY_SOG_TOI_REASON)
    proc, exports, models, site = tmp_path / "proc", tmp_path / "exports", tmp_path / "models", tmp_path / "site"
    (models / "latest/goalie_saves").mkdir(parents=True)
    exports.mkdir(); site.mkdir()
    (exports / "train_goalie_saves_v2.csv").write_text("fixture\n")
    (exports / "train_nhl_points_v2.csv").write_text("fixture\n")
    names = tmp_path / "names.csv"
    names.write_text("player_id,game_id,team_id,team_code,full_name,game_date\n1,2026010037,1,NJD,A,2026-09-24\n")
    odds_called = []

    def fake_run(command, **_kwargs):
        values = list(map(str, command))
        script = values[1] if len(values) > 1 else ""
        if failed_lane == "points" and "score_nhl_points_with_lineage.py" in script:
            raise RuntimeError("points failed")
        if failed_lane == "saves" and "score_nhl_saves_with_lineage.py" in script:
            raise RuntimeError("saves failed")
        if "--out" in values:
            out = Path(values[values.index("--out") + 1])
            if "score_" in script:
                write_prediction(out)
            elif "build_" in script:
                out.write_text("player_id,game_date\n1,2026-09-24\n")
                Path(values[values.index("--unmatched") + 1]).write_text("player_id\n")
                if "--ambiguous" in values:
                    Path(values[values.index("--ambiguous") + 1]).write_text("prediction_index\n")
        return subprocess.CompletedProcess(values, 0, "", "")

    def no_odds(**_kwargs):
        odds_called.append(True)
        return None

    with patch.multiple(cli, PROC_DIR=proc, EXPORTS_DIR=exports, MODELS_DIR=models, SITE_DIR=site,
                        EXPORTS_ODDS_HISTORY_DIR=tmp_path / "archive"), patch.object(
        cli, "run", side_effect=fake_run), patch.object(
        cli, "export_names_csv", return_value=names), patch.object(
        cli, "run_optional_odds_observation", side_effect=no_odds), patch.object(
        cli, "prepare_scoring_input", side_effect=fake_prepare_scoring_input), patch.object(
        cli, "validate_prediction_output", side_effect=fake_validate_prediction_output), patch.object(
        cli, "audit_attachment_files", side_effect=fake_attachment_audit):
        cli._run_independent_daily_lanes(
            recorder=value, db="fixture", slate=SLATE, with_odds=False,
            odds_phase="EARLY", daily_run_id="test-run", canonical_games=[],
            saves_export_ready=True, points_export_ready=True, legacy_sog_prediction=None)
    other = "saves" if failed_lane == "points" else "points"
    assert value.lane(failed_lane).status == "FAILED_NONBLOCKING"
    assert value.lane(other).status == "COMPLETE"
    assert odds_called == [True]


def test_odds_failure_is_warning_and_preserves_current_predictions(tmp_path):
    value = recorder()
    value.finish_lane("legacy_sog", status="BLOCKED_LANE_LOCAL", reason=LEGACY_SOG_TOI_REASON)
    proc, exports, models, site = (
        tmp_path / "proc", tmp_path / "exports", tmp_path / "models", tmp_path / "site")
    (models / "latest/goalie_saves").mkdir(parents=True)
    exports.mkdir(); site.mkdir()
    (exports / "train_goalie_saves_v2.csv").write_text("fixture\n")
    (exports / "train_nhl_points_v2.csv").write_text("fixture\n")
    names = tmp_path / "names.csv"
    names.write_text(
        "player_id,game_id,team_id,team_code,full_name,game_date\n"
        "1,2026010037,1,NJD,A,2026-09-24\n")

    def fake_run(command, **_kwargs):
        values = list(map(str, command))
        script = values[1] if len(values) > 1 else ""
        if "--out" in values:
            out = Path(values[values.index("--out") + 1])
            if "score_" in script:
                write_prediction(out)
            elif "build_" in script:
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text("player_id,game_date\n1,2026-09-24\n")
                Path(values[values.index("--unmatched") + 1]).write_text("player_id\n")
                if "--ambiguous" in values:
                    Path(values[values.index("--ambiguous") + 1]).write_text("prediction_index\n")
        return subprocess.CompletedProcess(values, 0, "", "")

    with patch.multiple(
        cli, PROC_DIR=proc, EXPORTS_DIR=exports, MODELS_DIR=models, SITE_DIR=site,
        EXPORTS_ODDS_HISTORY_DIR=tmp_path / "archive",
    ), patch.object(cli, "run", side_effect=fake_run), patch.object(
        cli, "export_names_csv", return_value=names), patch.object(
        cli, "run_optional_odds_observation", side_effect=RuntimeError("provider failed")), patch.object(
        cli, "prepare_scoring_input", side_effect=fake_prepare_scoring_input), patch.object(
        cli, "validate_prediction_output", side_effect=fake_validate_prediction_output), patch.object(
        cli, "audit_attachment_files", side_effect=fake_attachment_audit):
        cli._run_independent_daily_lanes(
            recorder=value, db="fixture", slate=SLATE, with_odds=True,
            odds_phase="EARLY", daily_run_id="test-run", canonical_games=[],
            saves_export_ready=True, points_export_ready=True,
            legacy_sog_prediction=None)
    assert value.lane("odds").status == "FAILED_NONBLOCKING"
    assert value.lane("points").status == "COMPLETE"
    assert value.lane("saves").status == "COMPLETE"
    assert value.lane("points_attachment").status == "COMPLETE"
    assert value.lane("saves_attachment").status == "COMPLETE"
    assert value.classification() == "READY_WITH_BOUNDED_LANE_WARNING"


def test_unexpected_legacy_sog_exception_uses_independent_continuation(tmp_path, monkeypatch):
    monkeypatch.setenv("NHL_DAILY_RECEIPT_ROOT", str(tmp_path))

    def fail_legacy(**kwargs):
        value = kwargs["recorder"]
        value.finish_lane("shared_prerequisites")
        value.finish_lane("roster")
        value.start_lane("legacy_sog")
        value.independent_context = {
            "db": "fixture", "slate": SLATE, "with_odds": False,
            "odds_phase": "EARLY", "daily_run_id": "test-run",
            "canonical_games": [], "saves_export_ready": True,
            "points_export_ready": True, "legacy_sog_prediction": None,
        }
        cli._ACTIVE_DAILY_LANE = "legacy_sog"
        raise RuntimeError("legacy scorer failed")

    def continue_independent(*, recorder, **_kwargs):
        for lane_name in LANE_NAMES:
            if recorder.lane(lane_name).status == "NOT_STARTED":
                recorder.finish_lane(lane_name)

    with patch.object(cli, "_cmd_daily_impl", side_effect=fail_legacy), patch.object(
        cli, "_run_independent_daily_lanes", side_effect=continue_independent) as continuation:
        receipt_path = cli.cmd_daily(with_odds=False)
    continuation.assert_called_once()
    receipt = json.loads((receipt_path / "parent_receipt.json").read_text())
    assert receipt["lanes"]["legacy_sog"]["status"] == "FAILED_NONBLOCKING"
    assert receipt["final_classification"] == "READY_WITH_BOUNDED_LANE_WARNING"


def test_current_prediction_hash_required_before_attachment(tmp_path):
    prediction = tmp_path / "points.csv"
    write_prediction(prediction)
    with patch.object(cli, "run") as child:
        with pytest.raises(AssertionError, match="hash mismatch"):
            cli.build_points(
                SLATE, pred_path=prediction, expected_pred_sha256="0" * 64)
    child.assert_not_called()


def test_archive_excludes_blocked_legacy_sog_fixed_files(tmp_path):
    site, archive = tmp_path / "site", tmp_path / "archive"
    site.mkdir()
    for name in ("sog_with_market.csv", "unmatched_sog.csv", "points_with_market.csv", "unmatched_points.csv"):
        (site / name).write_text(name + "\n")
    with patch.object(cli, "SITE_DIR", site), patch.object(cli, "EXPORTS_ODDS_HISTORY_DIR", archive):
        cli.archive_site_artifacts(SLATE, completed_lanes={"points"})
    day = archive / SLATE
    assert (day / "points_with_market.csv").exists()
    assert not (day / "sog_with_market.csv").exists()


def test_cmd_daily_finalizes_receipt_after_blocking_exception(tmp_path, monkeypatch):
    monkeypatch.setenv("NHL_DAILY_RECEIPT_ROOT", str(tmp_path))
    with patch.object(cli, "_cmd_daily_impl", side_effect=RuntimeError("canonical failure")):
        with pytest.raises(RuntimeError, match="canonical failure"):
            cli.cmd_daily(with_odds=False)
    package = next(tmp_path.glob("run_id=*"))
    receipt = json.loads((package / "parent_receipt.json").read_text())
    assert receipt["final_classification"] == "FAILED_BLOCKING"
    assert verify_package(package)
