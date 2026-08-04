import json

import pytest

from backend.mlb.shared import model_authority as authority


def test_no_qualified_model_status():
    payload = authority.authority()
    assert payload["authority_status"] == "NO_QUALIFIED_MLB_MODEL"
    assert payload["data_only_processes"] == "ALLOWED"


def test_predictive_operations_fail_closed():
    with pytest.raises(authority.MLBPredictiveModelBlocked, match=authority.BLOCKED_STATUS):
        authority.assert_predictive_model_qualified("test_prediction")


def test_retired_semantic_ids_are_blocked():
    payload = json.loads(authority.AUTHORITY_PATH.read_text())
    assert set(payload["blocked_semantic_model_ids"]) == {
        "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb",
        "MLB_STRIKEOUTS_PITCHING_SEMANTIC_V1_a3ee2428e5d0",
        "MLB_TOTAL_BASES_SEMANTIC_V1_b6cdb5379aa9",
    }
