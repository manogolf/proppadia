import csv,json
from pathlib import Path

import pytest

from backend.mlb.scripts import reconcile_mlb_prospective_lineage_outcomes as r


def prediction():
 return {'prediction_timestamp':'2026-08-14T10:00:00+00:00','scheduled_game_start':'2026-08-14T20:00:00+00:00','lineage_status':'LINEAGE_CERTIFIED','bookmaker_key':'book','selected_side':'over','model_semantic_name':'model','model_artifact_sha256':'a'*64,'model_probability_over':'.6','model_selected_side_probability':'.6','canonical_row_identity':json.dumps({'game_date':'2026-08-14','game_id':1,'player_id':2,'prop_type':'hits','line':.5})}


def write_ledger(path:Path,rows):
 with path.open('w',newline='') as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)


def test_freezes_earliest_strict_pregame_identity(tmp_path):
 early=prediction();late={**early,'prediction_timestamp':'2026-08-14T11:00:00+00:00'};post={**early,'prediction_timestamp':'2026-08-14T21:00:00+00:00'}
 path=tmp_path/'ledger.csv';write_ledger(path,[late,post,early])
 frozen=r.freeze_predictions(path)
 assert len(frozen)==1 and frozen[0]['prediction']['prediction_timestamp']==early['prediction_timestamp']


def test_resolves_existing_outcome_and_leaves_nonappearance_unresolved():
 item={'identity':json.loads(prediction()['canonical_row_identity']),'prediction':prediction(),'_order':('','')}
 resolved=r.reconcile_rows([item],{(1,2,'hits'):{'actual_value':1,'sample_rows':1,'distinct_actual_values':1}})[0]
 unresolved=r.reconcile_rows([item],{})[0]
 assert resolved['outcome_status']=='CANONICAL_RESOLVED_OFFICIAL_PLAYER_STAT' and resolved['selected_side_outcome']=='win'
 assert unresolved['outcome_status']=='UNRESOLVED_NO_OFFICIAL_APPEARANCE_OR_ELIGIBLE_OUTCOME' and unresolved['actual_value']==''


def test_conflicting_outcome_fails_closed():
 item={'identity':json.loads(prediction()['canonical_row_identity']),'prediction':prediction(),'_order':('','')}
 row=r.reconcile_rows([item],{(1,2,'hits'):{'actual_value':1,'sample_rows':2,'distinct_actual_values':2}})[0]
 assert row['outcome_status']=='UNRESOLVED_CANONICAL_OUTCOME_CONFLICT' and row['actual_value']==''


def test_outcome_sidecar_is_immutable(tmp_path):
 path=tmp_path/'out.csv';rows=[{'identity':'a','actual':1}]
 first=r.write_immutable_csv(path,rows);assert r.write_immutable_csv(path,rows)==first
 with pytest.raises(RuntimeError,match='IMMUTABLE_OUTCOME_SIDECAR_CONFLICT'):r.write_immutable_csv(path,[{'identity':'a','actual':2}])
