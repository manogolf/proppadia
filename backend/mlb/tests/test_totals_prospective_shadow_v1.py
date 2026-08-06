from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd
import pytest

from backend.mlb.scripts.run_mlb_totals_prospective_shadow_v1 import dynamic_environment, probability_fields
from backend.mlb.totals_predictions.prospective_shadow_v1 import append_context, append_prediction, canonical_identity, connect_ledger, contexts_for_date, counts, payload_hash, rows_for_date


def sample_payload():
    return {"game_date":"2026-08-06","game_pk":1,"scheduled_start_utc":"2026-08-06T23:00:00Z","prediction_timestamp_utc":"2026-08-06T22:00:00Z",
        "model_hash":"abc","feature_state_hash":"def","schedule_source_sha256":"ghi","expected_total":8.5,"context_quality_state":"TOTALS_CONTEXT_COMPLETE"}


def test_ledger_is_unique_idempotent_append_only_and_outcome_separated(tmp_path):
    connection=connect_ledger(tmp_path/"ledger.sqlite3");payload=sample_payload()
    assert append_prediction(connection,payload)=="APPENDED_NEW"
    context={"model_features":{"league_total":8.5},"away_starter_state":{"prior_starts":5}}
    assert append_context(connection,canonical_identity("2026-08-06",1),context,payload_hash(context),payload["prediction_timestamp_utc"])=="APPENDED_NEW"
    assert append_prediction(connection,payload)=="EXISTING_IMMUTABLE"
    assert append_context(connection,canonical_identity("2026-08-06",1),context,payload_hash(context),payload["prediction_timestamp_utc"])=="EXISTING_IMMUTABLE"
    assert len(rows_for_date(connection,"2026-08-06"))==1
    assert len(contexts_for_date(connection,"2026-08-06"))==1
    assert counts(connection)=={"prediction_rows":1,"context_rows":1,"outcome_rows":0,"duplicate_prediction_identities":0,"duplicate_outcome_identities":0}
    with pytest.raises(sqlite3.IntegrityError,match="APPEND_ONLY_PREDICTION_LEDGER"):
        connection.execute("UPDATE totals_shadow_predictions SET model_hash='changed'")
    connection.rollback()
    with pytest.raises(sqlite3.IntegrityError,match="APPEND_ONLY_PREDICTION_LEDGER"):
        connection.execute("DELETE FROM totals_shadow_predictions")


def test_prediction_ledger_rejects_outcome_fields(tmp_path):
    connection=connect_ledger(tmp_path/"ledger.sqlite3");payload=sample_payload();payload["final_total"]=9
    with pytest.raises(ValueError,match="OUTCOME_FIELD_FORBIDDEN"):
        append_prediction(connection,payload)


def test_market_line_probability_preserves_push_mass():
    values=probability_fields(8.5,.12944479977012996,8.0)
    assert np.isclose(values["p_over_market_line"]+values["p_under_market_line"]+values["push_probability_at_market_line"],1)
    assert values["model_minus_market_total"]==.5
    assert set(f"p_over_{x}" for x in ("6_5","7_5","8_5","9_5","10_5","11_5"))<=set(values)


def test_dynamic_environment_uses_only_prior_dates():
    history={"core":pd.DataFrame({"game_date":["2026-08-04","2026-08-05","2026-08-06"],"final_total":[8,10,99]})}
    result=dynamic_environment(history,"2026-08-06")
    assert result["season_to_date_league_rpg"]==9
    assert result["season_history_depth"]==2
    assert result["latest_included_game_date"]=="2026-08-05"
