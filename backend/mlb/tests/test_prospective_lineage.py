import csv
from pathlib import Path

import pytest

from backend.mlb.shared import prospective_lineage as p


def certified_row():
    identity={"game_date":"2026-08-04","game_id":1,"player_id":2,"prop_type":"hits","line":0.5,"selected_side":"over","bookmaker_key":"book","snapshot_run_tag":"run"}
    row={k:"x" for k in p.MANDATORY}
    row.update({"canonical_row_identity":p.canonical_json(identity),"selected_side":"over","price_over_american":-120,
                "price_under_american":100,"model_artifact_sha256":"a"*64,"feature_vector_sha256":"b"*64,
                "feature_schema_sha256":"c"*64,"configuration_sha256":"d"*64})
    return row


def test_validator_certifies_complete_row_and_blocks_missing_model_hash():
    row=certified_row()
    assert p.validate(row)[0] == "LINEAGE_CERTIFIED"
    row["model_artifact_sha256"]=""
    assert p.validate(row)[0] == "LINEAGE_BLOCKED_MISSING_MODEL_HASH"


def test_append_only_rejects_duplicate_identity(tmp_path: Path):
    path=tmp_path/"ledger.csv"; row=certified_row()
    assert p.append_rows(path,[row]) == 1
    with pytest.raises(ValueError, match="duplicate"):
        p.append_rows(path,[row])
    with path.open() as f: assert len(list(csv.DictReader(f))) == 1


def test_canonical_hash_is_order_invariant():
    assert p.hash_value({"b":2,"a":1}) == p.hash_value({"a":1,"b":2})


def test_adjacent_line_coherence_is_observational_only():
    rows=[]
    for line, prob in ((.5,.7),(1.5,.4)):
        row=certified_row(); ident=__import__('json').loads(row["canonical_row_identity"]); ident["line"]=line
        row["canonical_row_identity"]=p.canonical_json(ident); row["model_probability_over"]=prob; rows.append(row)
    p.annotate_distribution_coherence(rows)
    assert {r["distribution_coherence_status"] for r in rows} == {"COHERENT"}
