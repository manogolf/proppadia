import csv
import json
from copy import deepcopy
from pathlib import Path

import pytest

from backend.mlb.scripts import reconcile_mlb_prospective_lineage_outcomes as r


def prediction():
    return {
        "prediction_timestamp": "2026-08-14T10:00:00+00:00",
        "scheduled_game_start": "2026-08-14T20:00:00+00:00",
        "lineage_status": "LINEAGE_CERTIFIED",
        "bookmaker_key": "book",
        "selected_side": "over",
        "model_semantic_name": "model",
        "model_artifact_sha256": "a" * 64,
        "model_probability_over": ".6",
        "model_selected_side_probability": ".6",
        "canonical_row_identity": json.dumps(
            {
                "game_date": "2026-08-14",
                "game_id": 1,
                "player_id": 2,
                "prop_type": "hits",
                "line": 0.5,
            }
        ),
    }


def outcome(*, game_id=1, player_id=2, actual_value=1, outcome_status="CANONICAL_RESOLVED_OFFICIAL_PLAYER_STAT"):
    line = 0.5
    return {
        "canonical_identity": f"{game_id}:{player_id}:hits:{line:g}",
        "game_date": "2026-08-14",
        "game_id": game_id,
        "player_id": player_id,
        "prop_type": "hits",
        "line": line,
        "selected_side": "over",
        "prediction_timestamp": "2026-08-14T10:00:00+00:00",
        "scheduled_game_start": "2026-08-14T20:00:00+00:00",
        "model_semantic_name": "model",
        "model_artifact_sha256": "a" * 64,
        "model_probability_over": 0.6,
        "model_selected_side_probability": 0.6,
        "prediction_lineage_status": "LINEAGE_CERTIFIED",
        "actual_value": actual_value,
        "selected_side_outcome": "win" if actual_value not in (None, "") else "",
        "outcome_status": outcome_status,
        "actual_sample_rows": 1 if actual_value not in (None, "") else 0,
        "actual_distinct_values": 1 if actual_value not in (None, "") else 0,
        "outcome_contract": "MLB_API_CANONICAL_ACTUAL_WITH_PLAYER_STATS_FALLBACK",
    }


def summary(outcome_sha="b" * 64):
    return {
        "date": "2026-08-14",
        "decision": "CANONICAL_PROSPECTIVE_OUTCOME_RECONCILIATION_COMPLETE",
        "prediction_ledger": "ledger.csv",
        "prediction_ledger_sha256": "c" * 64,
        "completeness_sha256": "d" * 64,
        "frozen_identities": 1,
        "resolved": 1,
        "unresolved": 0,
        "duplicate_identities": 0,
        "by_prop": {"hits": {"predictions": 1, "resolved": 1, "unresolved": 0}},
        "by_prop_line": {"hits:0.5": {"predictions": 1, "resolved": 1, "unresolved": 0}},
        "outcome_csv": "out.csv",
        "outcome_csv_sha256": outcome_sha,
    }


def write_ledger(path: Path, rows):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_freezes_earliest_strict_pregame_identity(tmp_path):
    early = prediction()
    late = {**early, "prediction_timestamp": "2026-08-14T11:00:00+00:00"}
    post = {**early, "prediction_timestamp": "2026-08-14T21:00:00+00:00"}
    path = tmp_path / "ledger.csv"
    write_ledger(path, [late, post, early])
    frozen = r.freeze_predictions(path)
    assert len(frozen) == 1
    assert frozen[0]["prediction"]["prediction_timestamp"] == early["prediction_timestamp"]


def test_resolves_existing_outcome_and_leaves_nonappearance_unresolved():
    item = {
        "identity": json.loads(prediction()["canonical_row_identity"]),
        "prediction": prediction(),
        "_order": ("", ""),
    }
    resolved = r.reconcile_rows(
        [item], {(1, 2, "hits"): {"actual_value": 1, "sample_rows": 1, "distinct_actual_values": 1}}
    )[0]
    unresolved = r.reconcile_rows([item], {})[0]
    assert resolved["outcome_status"] == "CANONICAL_RESOLVED_OFFICIAL_PLAYER_STAT"
    assert resolved["selected_side_outcome"] == "win"
    assert unresolved["outcome_status"] == "UNRESOLVED_NO_OFFICIAL_APPEARANCE_OR_ELIGIBLE_OUTCOME"
    assert unresolved["actual_value"] == ""


def test_conflicting_outcome_fails_closed():
    item = {
        "identity": json.loads(prediction()["canonical_row_identity"]),
        "prediction": prediction(),
        "_order": ("", ""),
    }
    row = r.reconcile_rows(
        [item], {(1, 2, "hits"): {"actual_value": 1, "sample_rows": 2, "distinct_actual_values": 2}}
    )[0]
    assert row["outcome_status"] == "UNRESOLVED_CANONICAL_OUTCOME_CONFLICT"
    assert row["actual_value"] == ""


def test_first_write_and_identical_second_pass_are_idempotent(tmp_path):
    path = tmp_path / "out.csv"
    first = r.write_immutable_csv(path, [outcome()])
    before = path.read_bytes()
    assert r.write_immutable_csv(path, [outcome()]) == first
    assert path.read_bytes() == before


def test_reordered_rows_are_same_canonical_outcome_set(tmp_path):
    path = tmp_path / "out.csv"
    rows = [outcome(game_id=2), outcome(game_id=1)]
    first = r.write_immutable_csv(path, rows)
    before = path.read_bytes()
    assert r.write_immutable_csv(path, list(reversed(rows))) == first
    assert path.read_bytes() == before


def test_incidental_metadata_changes_are_ignored(tmp_path):
    path = tmp_path / "out.csv"
    r.write_immutable_csv(path, [outcome()])
    proposed = {**outcome(), "generated_at_utc": "2026-08-20T15:30:00Z"}
    before = path.read_bytes()
    r.write_immutable_csv(path, [proposed])
    assert path.read_bytes() == before


def test_true_outcome_change_fails_closed_without_mutation(tmp_path):
    path = tmp_path / "out.csv"
    r.write_immutable_csv(path, [outcome(actual_value=1)])
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="IMMUTABLE_OUTCOME_SIDECAR_CONFLICT"):
        r.write_immutable_csv(path, [outcome(actual_value=0)])
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "proposed",
    [
        [outcome(game_id=1)],
        [outcome(game_id=1), outcome(game_id=2), outcome(game_id=3)],
    ],
    ids=["missing_identity", "extra_identity"],
)
def test_missing_or_extra_identity_fails_closed(proposed, tmp_path):
    path = tmp_path / "out.csv"
    r.write_immutable_csv(path, [outcome(game_id=1), outcome(game_id=2)])
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="IMMUTABLE_OUTCOME_SIDECAR_CONFLICT"):
        r.write_immutable_csv(path, proposed)
    assert path.read_bytes() == before


def test_duplicate_identity_fails_closed():
    with pytest.raises(RuntimeError, match="DUPLICATE_CANONICAL_OUTCOME_IDENTITIES"):
        r.canonical_outcome_set_sha256([outcome(), outcome()])


def test_null_and_numeric_normalization_produce_stable_hash():
    left = outcome(actual_value=None, outcome_status="UNRESOLVED_NO_OFFICIAL_APPEARANCE_OR_ELIGIBLE_OUTCOME")
    right = deepcopy(left)
    right.update(
        {
            "game_id": "1.0",
            "player_id": "2.00",
            "line": "0.5000",
            "model_probability_over": ".6000",
            "model_selected_side_probability": "0.60",
            "actual_value": "null",
            "selected_side_outcome": None,
            "actual_sample_rows": "0.0",
            "actual_distinct_values": "0.00",
        }
    )
    assert r.canonical_outcome_set_sha256([left]) == r.canonical_outcome_set_sha256([right])


def test_stable_hash_is_order_independent_and_material_field_sensitive():
    rows = [outcome(game_id=2), outcome(game_id=1)]
    assert r.canonical_outcome_set_sha256(rows) == r.canonical_outcome_set_sha256(list(reversed(rows)))
    changed = deepcopy(rows)
    changed[0]["outcome_contract"] = "DIFFERENT_OFFICIAL_SOURCE_CONTRACT"
    assert r.canonical_outcome_set_sha256(rows) != r.canonical_outcome_set_sha256(changed)


def test_summary_input_hash_changes_are_no_op_but_aggregate_change_conflicts(tmp_path):
    path = tmp_path / "summary.json"
    initial = summary()
    assert r.write_immutable_summary(path, initial) == "CANONICAL_PROSPECTIVE_OUTCOME_RECONCILIATION_COMPLETE"
    before = path.read_bytes()
    regenerated_input = {**initial, "completeness_sha256": "e" * 64, "prediction_ledger_sha256": "f" * 64}
    assert r.write_immutable_summary(path, regenerated_input) == "IMMUTABLE_OUTCOME_SUMMARY_ALREADY_CURRENT"
    assert path.read_bytes() == before

    changed = deepcopy(initial)
    changed["resolved"] = 0
    changed["unresolved"] = 1
    with pytest.raises(RuntimeError, match="IMMUTABLE_OUTCOME_SUMMARY_CONFLICT"):
        r.write_immutable_summary(path, changed)
    assert path.read_bytes() == before
