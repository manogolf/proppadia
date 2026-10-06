from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

from backend.nhl.attachment_integrity import (
    AttachmentIntegrityError,
    audit_attachment_files,
    validate_attachment_frame,
    validate_odds_observation,
)
from backend.nhl.daily_capture import sha256_file
from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl.scripts import build_saves_with_market as saves


SLATE = "2026-09-24"
RUN_ID = "nhldaily_attachment_test"
RETAINED_RUN_ID = "nhldaily_20260924T203800000000Z_operator_refresh_canary"
RETAINED_PREDICTION = Path(
    f"backend/nhl/data/processed/daily_runs/{RETAINED_RUN_ID}/saves_predictions.csv")
RETAINED_PREDICTION_SHA256 = (
    "6dbae0edfd0aa00625f69f8e0ea9eaa0f420d2a14bdfad99108b0870b8a5c67e")


def prediction_frame(count: int = 1) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "full_name": f"Goalie {index}", "player_id": 1000 + index,
            "game_id": 2026010037 + (index % 11), "team_id": 10 + index,
            "line": 18.5, "p_over": 0.5, "game_date": SLATE,
        }
        for index in range(count)
    ])


def odds_candidate(
    *, alias: str, identity: str, market: str, price: float,
    alias_type: str = "NORMALIZED_FULL_NAME", rank: int = 1,
) -> dict[str, object]:
    return {
        "normalized_alias": alias,
        "alias_value": alias,
        "alias_type": alias_type,
        "alias_rank": rank,
        "provider_player_identity": identity,
        "provider_player_name": identity,
        "market_identity": market,
        "line_str": "18.5",
        "price_over": price,
        # Production normalization always includes both price columns; one
        # side may be absent from the observed market and remains null.
        "price_under": None,
        "source_quote_count": 1,
    }


def reduce_one(name: str, odds_rows: list[dict[str, object]]):
    frame = prediction_frame()
    frame.loc[0, "full_name"] = name
    candidates = saves.build_match_candidates(frame, pd.DataFrame(odds_rows))
    return saves.reduce_match_candidates(frame, candidates)


def attachment_frame(count: int = 1) -> pd.DataFrame:
    frame = prediction_frame(count)
    frame["attachment_status"] = "UNMATCHED"
    frame["price_over"] = pd.NA
    return frame


def write_prediction(path: Path, *, parent: str = RUN_ID) -> None:
    pd.DataFrame({
        "player_id": [1], "game_id": [2026010037], "game_date": [SLATE],
        "parent_daily_run_id": [parent], "p_over_18_5": [0.5],
    }).to_csv(path, index=False)


def create_odds_observation(tmp_path: Path, *, parent: str = RUN_ID):
    directory = tmp_path / "observation=fixture"
    directory.mkdir()
    (directory / "raw_response.json").write_text("[]\n")
    (directory / "observation_summary.json").write_text(json.dumps({
        "parent_daily_run_id": parent,
        "slate_date": SLATE,
        "classification": "CAPTURED_VALID_EMPTY",
    }) + "\n")
    (directory / "RUN_COMPLETE.json").write_text("{}\n")
    files = sorted(directory.iterdir())
    (directory / "SHA256SUMS").write_text("".join(
        f"{sha256_file(path)}  {path.name}\n" for path in files))
    return directory, sha256_file(directory / "SHA256SUMS")


def test_retained_988_unmatched_predictions_produce_exactly_988_rows(tmp_path, monkeypatch):
    assert sha256_file(RETAINED_PREDICTION) == RETAINED_PREDICTION_SHA256
    names = Path("backend/nhl/exports/daily/names/names_2026-09-24.csv")
    out, unmatched = tmp_path / "saves.csv", tmp_path / "unmatched.csv"
    ambiguous, report = tmp_path / "ambiguous.csv", tmp_path / "integrity.json"
    monkeypatch.setenv("SLATE_DATE", SLATE)
    monkeypatch.setattr(sys, "argv", [
        "build_saves_with_market.py", "--pred", str(RETAINED_PREDICTION),
        "--names", str(names), "--out", str(out), "--unmatched", str(unmatched),
        "--ambiguous", str(ambiguous), "--integrity-report", str(report),
        "--strict-current-run", "--parent-run-id", RETAINED_RUN_ID,
        "--expected-pred-sha256", RETAINED_PREDICTION_SHA256,
    ])
    saves.main()
    result = pd.read_csv(out)
    integrity = json.loads(report.read_text())
    assert len(result) == len(pd.read_csv(unmatched)) == 988
    assert result.attachment_status.eq("UNMATCHED").all()
    assert integrity["counts"]["unique_attachment_key_count"] == 988
    assert integrity["counts"]["duplicate_attachment_key_count"] == 0


def test_full_name_alias_only_matches():
    output, _ = reduce_one("Alex Lyon", [odds_candidate(
        alias="alex lyon", identity="alex lyon", market="market-1", price=-110)])
    assert output.loc[0, "attachment_status"] == "MATCHED"
    assert output.loc[0, "price_over"] == -110
    assert pd.isna(output.loc[0, "price_under"])
    assert output.loc[0, "matched_alias_type"] == "AUTHORITATIVE_FULL_NAME"


def test_initial_last_alias_only_matches():
    output, _ = reduce_one("Alex Lyon", [odds_candidate(
        alias="a lyon", identity="a lyon", market="market-1", price=105)])
    assert output.loc[0, "attachment_status"] == "MATCHED"
    assert output.loc[0, "matched_alias_type"] == "INITIAL_LAST"


def test_both_aliases_same_market_and_price_collapse():
    odds = [
        odds_candidate(alias="alex lyon", identity="alex lyon", market="same", price=-105),
        odds_candidate(alias="a lyon", identity="alex lyon", market="same", price=-105),
    ]
    output, ambiguous = reduce_one("Alex Lyon", odds)
    assert len(output) == 1
    assert output.loc[0, "attachment_status"] == "MATCHED"
    assert output.loc[0, "matched_alias_type"] == "AUTHORITATIVE_FULL_NAME"
    assert output.loc[0, "matched_alias_types"] == (
        "AUTHORITATIVE_FULL_NAME|INITIAL_LAST|NORMALIZED_FULL_NAME")
    assert ambiguous.empty


def test_aliases_with_conflicting_prices_are_ambiguous():
    odds = [
        odds_candidate(alias="alex lyon", identity="alex lyon", market="one", price=-110),
        odds_candidate(alias="a lyon", identity="alex lyon", market="two", price=120),
    ]
    output, ambiguous = reduce_one("Alex Lyon", odds)
    assert output.loc[0, "attachment_status"] == "AMBIGUOUS_ALIAS_MATCH"
    assert pd.isna(output.loc[0, "price_over"])
    assert len(ambiguous) == 2


def test_aliases_with_different_provider_identities_are_ambiguous():
    odds = [
        odds_candidate(alias="alex lyon", identity="alex lyon", market="one", price=-110),
        odds_candidate(alias="a lyon", identity="andrew lyon", market="two", price=-110),
    ]
    output, ambiguous = reduce_one("Alex Lyon", odds)
    assert output.loc[0, "attachment_status"] == "AMBIGUOUS_ALIAS_MATCH"
    assert set(ambiguous.provider_player_identity) == {"alex lyon", "andrew lyon"}


def test_two_goalies_with_same_initial_and_last_are_not_rank_resolved():
    odds = [
        odds_candidate(alias="j smith", identity="john smith", market="john", price=-105),
        odds_candidate(alias="j smith", identity="james smith", market="james", price=-105),
    ]
    output, _ = reduce_one("Jordan Smith", odds)
    assert output.loc[0, "attachment_status"] == "AMBIGUOUS_ALIAS_MATCH"
    assert pd.isna(output.loc[0, "price_over"])


@pytest.mark.parametrize("prediction,provider", [
    ("José Théodore", "Jose Theodore"),
    ("A.J. O’Connor-Smith Jr.", "AJ OConnor Smith"),
])
def test_accents_punctuation_hyphens_apostrophes_and_suffixes_match(prediction, provider):
    alias = saves.norm_name(provider)
    output, _ = reduce_one(prediction, [odds_candidate(
        alias=alias, identity=alias, market="market", price=-101)])
    assert output.loc[0, "attachment_status"] == "MATCHED"


def test_missing_odds_produces_one_unmatched_row():
    output, ambiguous = reduce_one("Alex Lyon", [])
    assert len(output) == 1
    assert output.loc[0, "attachment_status"] == "UNMATCHED"
    assert pd.isna(output.loc[0, "price_over"])
    assert ambiguous.empty


def test_exact_prediction_attachment_key_set_equality():
    result = validate_attachment_frame(
        prediction_frame=prediction_frame(3), attachment_frame=attachment_frame(3))
    assert result["counts"]["attachment_row_count"] == 3
    assert result["checks"]["prediction_attachment_key_set_equal"] is True


def test_duplicate_output_is_rejected_before_completion():
    attachment = pd.concat([attachment_frame(), attachment_frame()], ignore_index=True)
    with pytest.raises(AttachmentIntegrityError, match="attachment_keys_unique"):
        validate_attachment_frame(prediction_frame=prediction_frame(), attachment_frame=attachment)


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_missing_or_extra_output_is_rejected(mutation):
    predictions = prediction_frame(2)
    attachment = attachment_frame(2)
    if mutation == "missing":
        attachment = attachment.iloc[:1]
    else:
        extra = attachment.iloc[[0]].copy()
        extra["player_id"] = 9999
        attachment = pd.concat([attachment, extra], ignore_index=True)
    with pytest.raises(AttachmentIntegrityError, match="prediction_attachment_key_set_equal"):
        validate_attachment_frame(prediction_frame=predictions, attachment_frame=attachment)


def test_ambiguous_result_with_selected_price_is_rejected():
    attachment = attachment_frame()
    attachment.loc[0, "attachment_status"] = "AMBIGUOUS_ALIAS_MATCH"
    attachment.loc[0, "price_over"] = -110
    with pytest.raises(AttachmentIntegrityError, match="ambiguous_prices_absent"):
        validate_attachment_frame(prediction_frame=prediction_frame(), attachment_frame=attachment)


def test_mutable_odds_input_is_rejected(tmp_path):
    directory, manifest = create_odds_observation(tmp_path)
    mutable = tmp_path / "odds_latest.json"
    mutable.write_text("[]\n")
    with pytest.raises(AttachmentIntegrityError, match="IMMUTABLE_OBSERVATION"):
        validate_odds_observation(
            observation_dir=directory, odds_json=mutable,
            expected_manifest_sha256=manifest,
            expected_parent_daily_run_id=RUN_ID, expected_slate_date=SLATE)


@pytest.mark.parametrize("wrong", ["parent", "manifest"])
def test_current_observation_parent_and_manifest_hashes_are_enforced(tmp_path, wrong):
    directory, manifest = create_odds_observation(tmp_path)
    with pytest.raises(AttachmentIntegrityError):
        validate_odds_observation(
            observation_dir=directory, odds_json=directory / "raw_response.json",
            expected_manifest_sha256=("0" * 64 if wrong == "manifest" else manifest),
            expected_parent_daily_run_id=("other" if wrong == "parent" else RUN_ID),
            expected_slate_date=SLATE)


def test_future_receipt_cannot_label_duplicate_attachment_complete(tmp_path):
    prediction = tmp_path / "prediction.csv"
    attachment = tmp_path / "attachment.csv"
    write_prediction(prediction)
    duplicate = attachment_frame()
    duplicate.loc[:, ["player_id", "game_id"]] = [1, 2026010037]
    pd.concat([duplicate, duplicate], ignore_index=True).to_csv(attachment, index=False)
    recorder = DailyRunRecorder(run_id=RUN_ID, command=["fixture"], phase="REFRESH")
    recorder.start_lane("saves_attachment")
    with pytest.raises(AttachmentIntegrityError):
        audit_attachment_files(
            lane="saves", prediction_path=prediction, attachment_path=attachment,
            expected_prediction_sha256=sha256_file(prediction),
            expected_parent_daily_run_id=RUN_ID,
            expected_odds_manifest_sha256=None)
    recorder.finish_lane("saves_attachment", status="FAILED_NONBLOCKING_INTEGRITY")
    assert recorder.lane("saves_attachment").status != "COMPLETE"
    assert recorder.classification() == "READY_WITH_BOUNDED_LANE_WARNING"


def test_research_integrity_independently_detects_duplication(tmp_path):
    prediction = tmp_path / "prediction.csv"
    attachment = tmp_path / "attachment.csv"
    write_prediction(prediction)
    duplicate = attachment_frame()
    duplicate.loc[:, ["player_id", "game_id"]] = [1, 2026010037]
    pd.concat([duplicate, duplicate], ignore_index=True).to_csv(attachment, index=False)
    with pytest.raises(AttachmentIntegrityError, match="duplicate_attachment_key_count"):
        audit_attachment_files(
            lane="saves", prediction_path=prediction, attachment_path=attachment,
            expected_prediction_sha256=sha256_file(prediction),
            expected_parent_daily_run_id=RUN_ID,
            expected_odds_manifest_sha256=None)


def test_points_attachment_retains_one_row_per_prediction_key(tmp_path):
    prediction = tmp_path / "points.csv"
    attachment = tmp_path / "points_with_market.csv"
    pd.DataFrame({
        "player_id": [1, 1, 1], "game_id": [2026010037] * 3,
        "game_date": [SLATE] * 3, "parent_daily_run_id": [RUN_ID] * 3,
        "line": [0.5, 1.5, 2.5], "prob_over": [0.5] * 3,
    }).to_csv(prediction, index=False)
    prediction_sha = sha256_file(prediction)
    pd.DataFrame({
        "player_id": [1, 1, 1], "game_id": [2026010037] * 3,
        "game_date": [SLATE] * 3, "line": [0.5, 1.5, 2.5],
        "price_over": [pd.NA] * 3, "parent_daily_run_id": [RUN_ID] * 3,
        "prediction_artifact_sha256": [prediction_sha] * 3,
        "odds_observation_manifest_sha256": [""] * 3,
    }).to_csv(attachment, index=False)
    result = audit_attachment_files(
        lane="points", prediction_path=prediction, attachment_path=attachment,
        expected_prediction_sha256=prediction_sha,
        expected_parent_daily_run_id=RUN_ID,
        expected_odds_manifest_sha256=None)
    assert result["counts"]["prediction_row_count"] == 3
    assert result["counts"]["attachment_row_count"] == 3
    assert result["counts"]["duplicate_attachment_key_count"] == 0


def test_retained_refresh_evidence_hashes_remain_unchanged():
    expected = {
        RETAINED_PREDICTION: RETAINED_PREDICTION_SHA256,
        Path(f"artifacts/operational/nhl/daily_runs/run_id={RETAINED_RUN_ID}/parent_receipt.json"):
            "ee28d2ebb117a184273801cd88229922dae7dd41e2595899df2897e55ebeae4b",
        Path("backend/nhl/exports/odds_history/2026-09-24/saves_with_market.csv"):
            "d939b5037a2acd939a667667d1ac61ae21283e8fcf6a4b55b9870059f8ab6ce4",
    }
    assert {path: sha256_file(path) for path in expected} == expected
