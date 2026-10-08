from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from backend.nhl.daily_capture import CanonicalGame, canonical_game_set_hash, sha256_file
from backend.nhl import cli
from backend.nhl.daily_orchestration import DailyRunRecorder, LEGACY_SOG_TOI_REASON
from backend.nhl.prediction_lineage import (
    LINEAGE_COLUMNS,
    POINTS_LINES,
    SAVES_LINES,
    expand_wide_predictions,
    prepare_scoring_input,
    validate_prediction_output,
    wide_line_column,
)
from backend.nhl.scripts import (
    load_nhl_predictions_generic,
    score_nhl_points_with_lineage,
    score_nhl_saves_with_lineage,
)


SLATE = "2026-09-24"
RUN_ID = "nhldaily_lineage_test"
CUTOFF = "2026-09-24T19:45:00Z"
GAME_IDS = list(range(2026010037, 2026010048))


def games(game_ids=GAME_IDS, *, slate=SLATE):
    return [
        CanonicalGame(
            game_id=game_id,
            start_time_utc=f"{slate}T20:{index:02d}:00Z",
            home_team=f"H{index}", away_team=f"A{index}",
            home_team_id=100 + index, away_team_id=200 + index,
        )
        for index, game_id in enumerate(game_ids)
    ]


def lineage_hash(game_ids=GAME_IDS):
    return canonical_game_set_hash(game_ids)


def prepare(tmp_path: Path, frame: pd.DataFrame, *, canonical_games=None, allow_partial=False):
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "source.csv"
    output = tmp_path / "run" / "scoring_input.csv"
    frame.to_csv(source, index=False)
    identity = prepare_scoring_input(
        source_path=source, output_path=output,
        canonical_games=canonical_games or games(), slate=SLATE,
        parent_daily_run_id=RUN_ID, feature_input_cutoff_utc=CUTOFF,
        expected_game_set_hash=lineage_hash(
            [game.game_id for game in (canonical_games or games())]),
        allow_partial_slate=allow_partial,
    )
    return pd.read_csv(output), identity, output


def points_predictions(scoring_input: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for source in scoring_input.to_dict("records"):
        for line in POINTS_LINES:
            rows.append({
                **{column: source[column] for column in ["player_id", "game_id", *LINEAGE_COLUMNS]},
                "line": line, "prob_over": 0.5, "model": "points_phoenix_lr",
            })
    return pd.DataFrame(rows)


def saves_predictions(scoring_input: pd.DataFrame) -> pd.DataFrame:
    output = scoring_input[["player_id", "game_id", *LINEAGE_COLUMNS]].copy()
    for line in SAVES_LINES:
        output[wide_line_column(line)] = 0.5
    return output


def validate(path: Path, lane: str, canonical_games, lines):
    return validate_prediction_output(
        path=path, lane=lane, canonical_games=canonical_games, slate=SLATE,
        parent_daily_run_id=RUN_ID, feature_input_cutoff_utc=CUTOFF,
        expected_game_set_hash=lineage_hash([game.game_id for game in canonical_games]),
        expected_lines=lines,
    )


def test_points_input_with_verified_game_date(tmp_path):
    frame = pd.DataFrame({
        "player_id": range(1, 12), "game_id": GAME_IDS,
        "game_date": [SLATE] * 11, "feature": range(11),
    })
    output, identity, _ = prepare(tmp_path, frame)
    assert set(output.game_date) == {SLATE}
    assert identity["canonical_game_set_hash"] == lineage_hash()


def test_points_input_without_date_is_joined_by_exact_game_id(tmp_path):
    frame = pd.DataFrame({"player_id": range(1, 12), "game_id": list(reversed(GAME_IDS))})
    output, _, _ = prepare(tmp_path, frame)
    assert set(output.game_date) == {SLATE}
    assert output.loc[0, "game_id"] == GAME_IDS[-1]
    assert output.loc[0, "home_team_id"] == 110


@pytest.mark.parametrize("bad_game_id,error", [
    (None, "NULL_OR_INVALID"),
    (9999999999, "NONCANONICAL_GAMES"),
])
def test_null_or_unknown_game_id_is_rejected(tmp_path, bad_game_id, error):
    frame = pd.DataFrame({"player_id": range(1, 12), "game_id": GAME_IDS})
    frame.loc[0, "game_id"] = bad_game_id
    with pytest.raises(RuntimeError, match=error):
        prepare(tmp_path, frame)


def test_source_date_conflict_is_rejected(tmp_path):
    frame = pd.DataFrame({
        "player_id": range(1, 12), "game_id": GAME_IDS, "game_date": [SLATE] * 11})
    frame.loc[3, "game_date"] = "2026-09-23"
    with pytest.raises(RuntimeError, match="GAME_DATE_CONFLICT"):
        prepare(tmp_path, frame)


def test_extra_noncanonical_game_is_rejected(tmp_path):
    frame = pd.DataFrame({"player_id": range(1, 13), "game_id": [*GAME_IDS, 2026019999]})
    with pytest.raises(RuntimeError, match="NONCANONICAL_GAMES"):
        prepare(tmp_path, frame)


def test_partial_and_mixed_slate_inputs_are_rejected(tmp_path):
    partial = pd.DataFrame({"player_id": range(1, 11), "game_id": GAME_IDS[:-1]})
    with pytest.raises(RuntimeError, match="PARTIAL_SLATE"):
        prepare(tmp_path, partial)
    mixed_games = games()
    mixed_games[-1] = CanonicalGame(
        GAME_IDS[-1], "2026-09-25T20:00:00Z", "H", "A",
        home_team_id=1, away_team_id=2)
    full = pd.DataFrame({"player_id": range(1, 12), "game_id": GAME_IDS})
    with pytest.raises(RuntimeError, match="CANONICAL_GAME_DATE_MISMATCH"):
        prepare(tmp_path, full, canonical_games=mixed_games)


@pytest.mark.parametrize("kind", ["player", "goalie"])
def test_same_identity_in_multiple_canonical_games_is_not_collapsed(tmp_path, kind):
    canonical = games(GAME_IDS[:2])
    frame = pd.DataFrame({"player_id": [7, 7], "game_id": GAME_IDS[:2]})
    output, identity, _ = prepare(tmp_path, frame, canonical_games=canonical)
    assert len(output) == 2
    assert identity["identity_count"] == 2


def test_september24_fixture_points_and_saves_exact_populations(tmp_path):
    points_source = Path("backend/nhl/exports/train_nhl_points_v2.csv")
    saves_source = Path("backend/nhl/exports/train_goalie_saves_v2.csv")
    points_input_path = tmp_path / "run" / "points_input.csv"
    saves_input_path = tmp_path / "run" / "saves_input.csv"
    for source, output in ((points_source, points_input_path), (saves_source, saves_input_path)):
        prepare_scoring_input(
            source_path=source, output_path=output, canonical_games=games(), slate=SLATE,
            parent_daily_run_id=RUN_ID, feature_input_cutoff_utc=CUTOFF,
            expected_game_set_hash=lineage_hash())
    points_input = pd.read_csv(points_input_path)
    saves_input = pd.read_csv(saves_input_path)
    assert len(points_input) == 798
    assert len(saves_input) == 76

    points_path, saves_path = tmp_path / "points.csv", tmp_path / "saves.csv"
    points_predictions(points_input).to_csv(points_path, index=False)
    saves_predictions(saves_input).to_csv(saves_path, index=False)
    points_identity = validate(points_path, "points", games(), POINTS_LINES)
    saves_identity = validate(saves_path, "saves", games(), SAVES_LINES)
    assert points_identity["conditional_prediction_count"] == 2394
    assert points_identity["natural_identity_count"] == 798
    assert saves_identity["conditional_prediction_count"] == 988
    assert saves_identity["natural_identity_count"] == 76


def test_saves_wide_and_13_line_expansion_preserve_lineage(tmp_path):
    canonical = games(GAME_IDS[:1])
    scoring_input, _, _ = prepare(
        tmp_path, pd.DataFrame({"player_id": [30], "game_id": GAME_IDS[:1]}),
        canonical_games=canonical)
    wide = saves_predictions(scoring_input)
    expanded = expand_wide_predictions(wide, lines=SAVES_LINES)
    assert len(expanded) == 13
    assert set(expanded.game_date) == {SLATE}
    assert set(expanded.parent_daily_run_id) == {RUN_ID}
    assert set(expanded.canonical_game_set_hash) == {lineage_hash(GAME_IDS[:1])}


def test_points_scorer_preserves_lineage_through_three_lines(tmp_path, monkeypatch):
    canonical = games(GAME_IDS[:1])
    scoring_input, _, input_path = prepare(
        tmp_path, pd.DataFrame({"player_id": [40], "game_id": GAME_IDS[:1], "x": [1.0]}),
        canonical_games=canonical)
    model_root = tmp_path / "models"
    model_root.mkdir()
    out = tmp_path / "points_predictions.csv"

    def frozen_scorer(command, check):
        assert check is True
        source = pd.read_csv(command[command.index("--features-csv") + 1])
        unbound = []
        for row in source.to_dict("records"):
            for line in POINTS_LINES:
                unbound.append({
                    "player_id": row["player_id"], "game_id": row["game_id"],
                    "line": line, "prob_over": 0.5, "model": "points_phoenix_lr",
                })
        pd.DataFrame(unbound).to_csv(command[command.index("--out") + 1], index=False)
        return subprocess.CompletedProcess(command, 0)

    captured_identity = {}

    def fake_fitted_model_identity(**kwargs):
        captured_identity.update(kwargs)
        return {"test_identity": True}

    monkeypatch.setattr(score_nhl_points_with_lineage.subprocess, "run", frozen_scorer)
    monkeypatch.setattr(score_nhl_points_with_lineage, "fitted_model_identity", fake_fitted_model_identity)
    monkeypatch.setattr(sys, "argv", [
        "score_nhl_points_with_lineage.py", "--features-csv", str(input_path),
        "--model-root", str(model_root), "--out", str(out)])
    score_nhl_points_with_lineage.main()
    result = pd.read_csv(out)
    assert len(result) == 3
    assert set(result.line) == set(POINTS_LINES)
    assert set(result.game_date) == {SLATE}
    assert set(result.parent_daily_run_id) == {RUN_ID}
    assert ("feature_construction", Path(score_nhl_points_with_lineage.__file__).resolve().parents[1]
            / "sql" / "export_points.sql") in captured_identity["components"]
    assert captured_identity["scoring_configuration"]["feature_contract_version"] == "POINTS_PLAYER_HISTORY_CROSS_SEASON_V2"
    assert captured_identity["scoring_configuration"]["feature_contract_sha256"]


def test_saves_scorer_wide_output_preserves_lineage(tmp_path, monkeypatch):
    canonical = games(GAME_IDS[:1])
    _, _, input_path = prepare(
        tmp_path, pd.DataFrame({"player_id": [50], "game_id": GAME_IDS[:1], "x": [1.0]}),
        canonical_games=canonical)
    model_dir = tmp_path / "saves_model"
    model_dir.mkdir()
    (model_dir / "MODEL_INDEX.json").write_text(json.dumps({
        "family": "poisson", "params": {}, "metrics_holdout": {},
        "metrics_holdout_calibrated": {},
    }))
    (model_dir / "MODEL_ARTIFACT.json").write_text(json.dumps({
        "sklearn_poisson": {"feature_order": ["x"], "coef": [0.0], "intercept": 3.0},
        "calibration": {},
    }))
    feature_json = tmp_path / "features.json"
    feature_json.write_text(json.dumps({"goalie_saves": ["x"]}))
    out = tmp_path / "saves_predictions.csv"
    monkeypatch.setattr(sys, "argv", [
        "score_nhl_saves_with_lineage.py", "--model-dir", str(model_dir), "--csv", str(input_path),
        "--feature-json", str(feature_json), "--feature-key", "goalie_saves",
        "--line", ",".join(map(str, SAVES_LINES)), "--out", str(out)])
    score_nhl_saves_with_lineage.main()
    result = pd.read_csv(out)
    assert set(LINEAGE_COLUMNS).issubset(result.columns)
    assert result.loc[0, "game_date"] == SLATE
    assert result.loc[0, "parent_daily_run_id"] == RUN_ID


@pytest.mark.parametrize("mutation,error", [
    ("missing_date", "LINEAGE_COLUMNS_MISSING"),
    ("wrong_date", "GAME_DATE_CONFLICT"),
    ("duplicate", "DUPLICATE_NATURAL_KEY"),
    ("missing_line", "LINE_SET_MISMATCH"),
])
def test_fail_closed_output_gate(tmp_path, mutation, error):
    canonical = games(GAME_IDS[:1])
    scoring_input, _, _ = prepare(
        tmp_path, pd.DataFrame({"player_id": [60], "game_id": GAME_IDS[:1]}),
        canonical_games=canonical)
    predictions = points_predictions(scoring_input)
    if mutation == "missing_date":
        predictions = predictions.drop(columns=["game_date"])
    elif mutation == "wrong_date":
        predictions.loc[0, "game_date"] = "2026-09-23"
    elif mutation == "duplicate":
        predictions = pd.concat([predictions, predictions.iloc[[0]]], ignore_index=True)
    elif mutation == "missing_line":
        predictions = predictions.iloc[:-1]
    path = tmp_path / f"{mutation}.csv"
    predictions.to_csv(path, index=False)
    with pytest.raises(RuntimeError, match=error):
        validate(path, "points", canonical, POINTS_LINES)


def test_run_local_output_does_not_modify_stale_or_observer_artifacts(tmp_path):
    stale = tmp_path / "fixed" / "points_predictions.csv"
    observer = tmp_path / "observer" / "MIDDAY" / "prediction.csv"
    stale.parent.mkdir(parents=True)
    observer.parent.mkdir(parents=True)
    stale.write_bytes(b"stale-fixed-file\n")
    observer.write_bytes(b"natural-observer-package\n")
    canonical = games(GAME_IDS[:1])
    prepare(
        tmp_path / "work",
        pd.DataFrame({"player_id": [70], "game_id": GAME_IDS[:1]}),
        canonical_games=canonical)
    assert stale.read_bytes() == b"stale-fixed-file\n"
    assert observer.read_bytes() == b"natural-observer-package\n"


def test_frozen_natural_observer_scorers_remain_byte_identical():
    contracts = (
        Path("backend/nhl/points_shadow/frozen_points_v1.json"),
        Path("backend/nhl/saves_shadow/frozen_saves_v1.json"),
    )
    for contract_path in contracts:
        contract = json.loads(contract_path.read_text())
        assert sha256_file(Path(contract["scorer_path"])) == contract["scorer_sha256"]


def test_loader_hash_gate_runs_before_database_connection(tmp_path, monkeypatch):
    prediction = tmp_path / "prediction.csv"
    prediction.write_text("player_id,game_id,game_date,line,prob_over\n1,2,2026-09-24,0.5,0.5\n")
    connected = []
    monkeypatch.setattr(
        load_nhl_predictions_generic.psycopg, "connect",
        lambda *_args, **_kwargs: connected.append(True))
    monkeypatch.setattr(sys, "argv", [
        "load_nhl_predictions_generic.py", "--pred-csv", str(prediction),
        "--project", "nhl", "--prop", "player_points",
        "--expected-sha256", "0" * 64,
    ])
    with pytest.raises(SystemExit, match="hash mismatch"):
        load_nhl_predictions_generic.main()
    assert connected == []


def _recorder(game_ids) -> DailyRunRecorder:
    value = DailyRunRecorder(
        run_id=RUN_ID, command=["python", "-m", "backend.nhl.cli", "daily"],
        phase="EARLY", started_at=datetime(2026, 9, 24, 19, 45, tzinfo=timezone.utc))
    value.set_canonical(
        slate_date=SLATE, season=2026, game_ids=game_ids,
        game_set_hash=lineage_hash(game_ids))
    value.finish_lane("shared_prerequisites")
    value.finish_lane("roster")
    value.finish_lane(
        "legacy_sog", status="BLOCKED_LANE_LOCAL", reason=LEGACY_SOG_TOI_REASON)
    return value


def test_parent_orchestration_uses_only_validated_run_local_artifacts(tmp_path):
    canonical = games(GAME_IDS[:1])
    value = _recorder(GAME_IDS[:1])
    proc, exports, models, site = (
        tmp_path / "proc", tmp_path / "exports", tmp_path / "models", tmp_path / "site")
    exports.mkdir(); site.mkdir(); (models / "latest/goalie_saves").mkdir(parents=True)
    pd.DataFrame({"player_id": [80], "game_id": GAME_IDS[:1], "x": [1.0]}).to_csv(
        exports / "train_nhl_points_v2.csv", index=False)
    pd.DataFrame({
        "player_id": [90], "game_id": GAME_IDS[:1], "game_date": [SLATE], "x": [1.0],
    }).to_csv(exports / "train_goalie_saves_v2.csv", index=False)
    child_commands = []
    attachments = {}

    def fake_run(command, **_kwargs):
        values = list(map(str, command))
        child_commands.append(values)
        script = values[1] if len(values) > 1 else ""
        if "score_nhl_points_with_lineage.py" in script:
            source = pd.read_csv(values[values.index("--features-csv") + 1])
            points_predictions(source).to_csv(values[values.index("--out") + 1], index=False)
        elif "score_nhl_saves_with_lineage.py" in script:
            source = pd.read_csv(values[values.index("--csv") + 1])
            saves_predictions(source).to_csv(values[values.index("--out") + 1], index=False)
        return subprocess.CompletedProcess(values, 0, "", "")

    def attachment(name):
        def build(_slate, **kwargs):
            attachments[name] = kwargs
            (site / f"{name}_with_market.csv").write_text("player_id,game_date\n1,2026-09-24\n")
            (site / f"unmatched_{name}.csv").write_text("player_id\n")
        return build

    def cold_reference(recorder, _slate):
        recorder.finish_lane("cold_start_sog_reference", status="NOT_AVAILABLE_EXTERNAL_OWNER")

    with patch.multiple(
        cli, PROC_DIR=proc, EXPORTS_DIR=exports, MODELS_DIR=models, SITE_DIR=site,
        EXPORTS_ODDS_HISTORY_DIR=tmp_path / "archive",
    ), patch.object(cli, "run", side_effect=fake_run), patch.object(
        cli, "run_optional_odds_observation", return_value=None), patch.object(
        cli, "build_points", side_effect=attachment("points")), patch.object(
        cli, "build_saves", side_effect=attachment("saves")), patch.object(
        cli, "_reference_cold_start_sog", side_effect=cold_reference), patch.object(
        cli, "archive_site_artifacts"):
        cli._run_independent_daily_lanes(
            recorder=value, db="postgresql://offline.invalid/denied", slate=SLATE,
            with_odds=False, odds_phase="EARLY", daily_run_id=RUN_ID,
            canonical_games=canonical, saves_export_ready=True,
            points_export_ready=True, legacy_sog_prediction=None)

    run_dir = proc / "daily_runs" / RUN_ID
    loader_paths = [
        Path(command[command.index("--pred-csv") + 1])
        for command in child_commands if "--pred-csv" in command]
    assert loader_paths == [run_dir / "saves_predictions.csv", run_dir / "points_predictions.csv"]
    assert Path(attachments["points"]["pred_path"]).parent == run_dir
    assert Path(attachments["saves"]["pred_path"]).parent == run_dir
    assert attachments["points"]["expected_pred_sha256"] == value.lane("points").outputs[0]["sha256"]
    assert value.lane("points").outputs[0]["validated_prediction_identity"] is True
    assert value.lane("saves").outputs[0]["conditional_prediction_count"] == 13

    receipt_dir = value.finalize(tmp_path / "receipts")
    receipt = json.loads((receipt_dir / "parent_receipt.json").read_text())
    recorded = receipt["lanes"]["points"]["outputs"][0]
    assert recorded["parent_daily_run_id"] == RUN_ID
    assert recorded["canonical_game_set_hash"] == lineage_hash(GAME_IDS[:1])
    assert recorded["sha256"] == attachments["points"]["expected_pred_sha256"]


def test_missing_game_date_blocks_lane_before_database_or_attachment(tmp_path):
    canonical = games(GAME_IDS[:1])
    value = _recorder(GAME_IDS[:1])
    proc, exports, models, site = (
        tmp_path / "proc", tmp_path / "exports", tmp_path / "models", tmp_path / "site")
    exports.mkdir(); site.mkdir(); models.mkdir()
    pd.DataFrame({"player_id": [100], "game_id": GAME_IDS[:1], "x": [1.0]}).to_csv(
        exports / "train_nhl_points_v2.csv", index=False)
    loader_called = []
    attachment_called = []

    def fake_run(command, **_kwargs):
        values = list(map(str, command))
        script = values[1] if len(values) > 1 else ""
        if "score_nhl_points_with_lineage.py" in script:
            source = pd.read_csv(values[values.index("--features-csv") + 1])
            broken = points_predictions(source).drop(columns=["game_date"])
            broken.to_csv(values[values.index("--out") + 1], index=False)
        if "load_nhl_predictions_generic.py" in script:
            loader_called.append(values)
        return subprocess.CompletedProcess(values, 0, "", "")

    def cold_reference(recorder, _slate):
        recorder.finish_lane("cold_start_sog_reference", status="NOT_AVAILABLE_EXTERNAL_OWNER")

    with patch.multiple(
        cli, PROC_DIR=proc, EXPORTS_DIR=exports, MODELS_DIR=models, SITE_DIR=site,
        EXPORTS_ODDS_HISTORY_DIR=tmp_path / "archive",
    ), patch.object(cli, "run", side_effect=fake_run), patch.object(
        cli, "run_optional_odds_observation", return_value=None), patch.object(
        cli, "build_points", side_effect=lambda *_args, **_kwargs: attachment_called.append(True)), patch.object(
        cli, "_reference_cold_start_sog", side_effect=cold_reference), patch.object(
        cli, "archive_site_artifacts"):
        cli._run_independent_daily_lanes(
            recorder=value, db="postgresql://offline.invalid/denied", slate=SLATE,
            with_odds=False, odds_phase="EARLY", daily_run_id=RUN_ID,
            canonical_games=canonical, saves_export_ready=False,
            points_export_ready=True, legacy_sog_prediction=None)
    assert value.lane("points").status == "FAILED_NONBLOCKING"
    assert "PREDICTION_LINEAGE_COLUMNS_MISSING" in value.lane("points").reason
    assert loader_called == []
    assert attachment_called == []
    assert value.lane("points_attachment").status == "SKIPPED_UPSTREAM_LANE_BLOCKED"
