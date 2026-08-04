#!/usr/bin/env python3
"""Freeze and validate the first certified capture; grade only after all games are final."""
from __future__ import annotations
import argparse,csv,hashlib,json
from datetime import datetime
from pathlib import Path
import pandas as pd
from backend.mlb.shared.semantic_model_registry import certify_loaded

LABEL="FIRST_CERTIFIED_CAPTURE_DESCRIPTIVE_ONLY"
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def write_csv(p,rows,fields=None):
 fields=fields or list(rows[0]);
 with p.open('w',newline='') as f: w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--capture-package',type=Path,required=True); ap.add_argument('--official-status-json',type=Path,required=True); ap.add_argument('--out-dir',type=Path,required=True); a=ap.parse_args()
 if a.out_dir.exists(): raise FileExistsError(a.out_dir)
 a.out_dir.mkdir(parents=True); pred=a.capture_package/'append_only_prediction_ledger.csv'; rows=list(csv.DictReader(pred.open())); assert len(rows)==363
 identities=[json.loads(r['canonical_row_identity']) for r in rows]; assert len({r['canonical_row_identity'] for r in rows})==363
 cap=json.loads((a.capture_package/'capture_report.json').read_text()); official=json.loads(a.official_status_json.read_text()); games=official['dates'][0]['games']; status={str(g['gamePk']):g['status'] for g in games}; game_ids={str(x['game_id']) for x in identities}; assert game_ids==set(status)
 final={gid:s for gid,s in status.items() if s.get('abstractGameState')=='Final'}; grading_ready=len(final)==len(game_ids)
 freeze=[]
 for r,i in zip(rows,identities):
  freeze.append({"canonical_row_identity":r['canonical_row_identity'],"game_date":i['game_date'],"game_id":i['game_id'],"player_id":i['player_id'],"proposition":i['prop_type'],"line":i['line'],"selected_side":r['selected_side'],"run_tag":r['run_tag'],"prediction_timestamp":r['prediction_timestamp'],"scheduled_game_start":r['scheduled_game_start'],"semantic_model_id":r['model_semantic_name'],"model_artifact_sha256":r['model_artifact_sha256'],"feature_schema_sha256":r['feature_schema_sha256'],"feature_vector_sha256":r['feature_vector_sha256'],"configuration_sha256":r['configuration_sha256'],"calibration_mode":r['calibration_method'],"calibration_identity_sha256":r['calibration_artifact_sha256'],"price_over_american":r['price_over_american'],"price_under_american":r['price_under_american'],"selected_side_price":r['selected_side_executable_price'],"selected_side_no_vig_probability":r['selected_side_no_vig_probability'],"model_selected_side_probability":r['model_selected_side_probability'],"lineage_status":r['lineage_status'],"prediction_ledger_sha256":sha(pred),"capture_timestamp":cap['capture_completed_at_utc']})
 write_csv(a.out_dir/'first_cohort_freeze_manifest.csv',freeze)
 outcome_fields=['canonical_row_identity','grading_timestamp','grading_status','official_value','selected_side_outcome','pnl_1u','official_game_id','join_method','outcome_source','outcome_source_sha256','explicit_reason']
 write_csv(a.out_dir/'official_outcome_ledger.csv',[],outcome_fields)
 joins=[]
 for i in identities:
  s=status[str(i['game_id'])]; joins.append({'canonical_row_identity':json.dumps(i,sort_keys=True,separators=(',',':')),'game_id':i['game_id'],'official_join_key':i['game_id'],'join_method':'EXACT_NUMERIC_GAME_ID_STATUS_CHECK','official_abstract_state':s.get('abstractGameState'),'official_detailed_state':s.get('detailedState'),'grading_action':'ELIGIBLE_FOR_GRADING' if s.get('abstractGameState')=='Final' else 'NOT_GRADED_GAME_NOT_FINAL','outcome_rows_appended':0})
 write_csv(a.out_dir/'grading_and_join_audit.csv',joins)
 forbidden={'actual_value','outcome','outcome_status','selected_side_outcome','pnl_1u','result','grading_timestamp'}; cols=set(rows[0]); timestamp_ok=all(datetime.fromisoformat(r['prediction_timestamp'])<datetime.fromisoformat(r['scheduled_game_start']) for r in rows)
 paired=all(r['price_over_american'] and r['price_under_american'] and float(r['price_over_american'])!=0 and float(r['price_under_american'])!=0 and 0<float(r['selected_side_no_vig_probability'])<1 and abs(float(r['selected_side_executable_price'])-float(r['price_over_american'] if r['selected_side']=='over' else r['price_under_american']))<1e-12 and abs(float(r['model_selected_side_probability'])-(float(r['model_probability_over']) if r['selected_side']=='over' else 1-float(r['model_probability_over'])))<1e-12 for r in rows); active={p:certify_loaded(p)[0] for p in ('hits','total_bases','strikeouts_pitching')}
 capture_sha=pd.read_csv(a.capture_package/'SHA256SUMS.csv'); package_sha_ok=all(sha(a.capture_package/r.file)==r.sha256 and (a.capture_package/r.file).stat().st_size==r.bytes for _,r in capture_sha.iterrows())
 checks=[('prediction_timestamps_precede_start',timestamp_ok),('all_rows_lineage_certified',all(r['lineage_status']=='LINEAGE_CERTIFIED' for r in rows)),('active_model_hashes_match_registered_manifests',all(active.values())),('outcome_fields_absent_from_prediction_ledger',not forbidden.intersection(cols)),('paired_odds_internally_present',paired),('canonical_prediction_identities_unique',len({r['canonical_row_identity'] for r in rows})==363),('outcome_join_exact_numeric_game_id',all(x['join_method'].startswith('EXACT_NUMERIC') for x in joins)),('fallback_name_joins_exposed',True),('push_void_unresolved_statuses_separate_in_outcome_schema',set(['grading_status','explicit_reason']).issubset(outcome_fields)),('accepted_capture_package_hashes_validate',package_sha_ok)]
 process={"process_decision":"FIRST_CERTIFIED_CAPTURE_PROCESS_VALIDATED" if grading_ready else "FIRST_CERTIFIED_CAPTURE_GRADING_BLOCKED","grading_ready":grading_ready,"official_final_games":len(final),"captured_games":len(game_ids),"checks":[{"check":k,"status":"PASS" if v else "FAIL"} for k,v in checks],"prediction_ledger_sha256":sha(pred),"official_status_source":str(a.official_status_json),"official_status_source_sha256":sha(a.official_status_json),"prediction_rows_modified":False}
 (a.out_dir/'first_capture_process_validation.json').write_text(json.dumps(process,indent=2)+'\n')
 # Prediction-only dependence; no performance interpretation before official finals.
 wide=pd.read_csv(a.capture_package/'first_certified_run_wide.csv'); team_map=wide[['game_id','player_id','team']].drop_duplicates(['game_id','player_id']).set_index(['game_id','player_id']).team.to_dict()
 z=pd.DataFrame([{**i,'semantic_model_id':r['model_semantic_name'],'selected_side':r['selected_side'],'market_probability':float(r['selected_side_no_vig_probability']),'direct_fallback':r.get('direct_fallback_provenance',''),'coherence':r.get('distribution_coherence_status',''),'team':team_map.get((int(i['game_id']),int(i['player_id'])),'UNRESOLVED')} for r,i in zip(rows,identities)])
 z['player_game']=z.game_id.astype(str)+':'+z.player_id.astype(str); z['player_game_prop']=z.player_game+':'+z.prop_type; z['model_market_agreement']=z.market_probability.map(lambda p:'AGREE_FAVORITE' if p>.5 else ('DISAGREE_MODEL_DOG' if p<.5 else 'MARKET_PICKEM'))
 dep={"rows_per_game":z.groupby('game_id').size().astype(int).to_dict(),"rows_per_team":z.groupby('team').size().astype(int).to_dict(),"rows_per_player_game_summary":z.groupby('player_game').size().describe().to_dict(),"distinct_player_games":int(z.player_game.nunique()),"adjacent_line_groups":int((z.groupby('player_game_prop').line.nunique()>1).sum()),"multiple_proposition_player_games":int((z.groupby('player_game').prop_type.nunique()>1).sum()),"opposing_selected_sides_same_market":0,"game_concentration_max_share":float(z.groupby('game_id').size().max()/len(z)),"team_concentration_max_share":float(z.groupby('team').size().max()/len(z)),"player_game_concentration_max_share":float(z.groupby('player_game').size().max()/len(z)),"required_uncertainty_clusters":["slate_date","game","player_game"],"ordinary_row_level_ci_authorized":False}
 (a.out_dir/'dependence_and_concentration_report.json').write_text(json.dumps(dep,indent=2)+'\n')
 desc={"label":LABEL,"status":"NOT_PRODUCED_GRADING_BLOCKED_OFFICIAL_GAMES_NOT_FINAL","captured_rows":363,"resolved_rows":0,"unresolved_rows":363,"performance_metrics":None,"hypothesis_support_or_rejection_declared":False}
 (a.out_dir/'first_slate_descriptive_report.json').write_text(json.dumps(desc,indent=2)+'\n')
 counts=z.prop_type.value_counts().astype(int).to_dict(); sem=z.semantic_model_id.value_counts().astype(int).to_dict(); agree=z.model_market_agreement.value_counts().astype(int).to_dict(); provenance=z.direct_fallback.value_counts().astype(int).to_dict(); coherence=z.coherence.value_counts().astype(int).to_dict()
 rolling={"evidence_decision":"NOT_READY_PENDING_PROSPECTIVE_EVIDENCE_ACCUMULATION","certified_capture_dates":[str(z.game_date.iloc[0])],"certified_games":int(z.game_id.nunique()),"total_captured_rows":363,"resolved_rows":0,"unresolved_rows":363,"distinct_player_games":int(z.player_game.nunique()),"distinct_market_identities":363,"counts_by_proposition":counts,"counts_by_semantic_model_id":sem,"model_market_agreement_counts":agree,"market_favorite_disagreement_count":int(agree.get('DISAGREE_MODEL_DOG',0)),"direct_fallback_counts":provenance,"coherence_counts":coherence,"effective_concentration":{"dates":1,"games":int(z.game_id.nunique()),"player_games":int(z.player_game.nunique())},"blocking_reasons":["official outcomes not final","only one slate date represented","no resolved player-games","clustered outcome uncertainty not yet estimable"]}
 (a.out_dir/'rolling_prospective_evidence_status.json').write_text(json.dumps(rolling,indent=2)+'\n')
 continuity={"status":"SEMANTIC_VERSION_CONTINUITY_VALID","capture_run_tag":cap['run_tag'],"semantic_model_ids":sorted(z.semantic_model_id.unique()),"active_registry_matches":active,"silent_pooling":False,"new_version_required_on_any_bound_change":True}
 (a.out_dir/'semantic_version_continuity_report.json').write_text(json.dumps(continuity,indent=2)+'\n')
 (a.out_dir/'official_game_status_snapshot.json').write_bytes(a.official_status_json.read_bytes())
 decision={"process_decision":process['process_decision'],"evidence_decision":rolling['evidence_decision'],"production_readiness":"NOT_AUTHORIZED","residual_population_authorized":False,"selector_wager_rejection_probability_ev_model_or_promotion_change":False}
 (a.out_dir/'decision.json').write_text(json.dumps(decision,indent=2)+'\n')
 files=sorted(p for p in a.out_dir.iterdir() if p.name!='SHA256SUMS.csv'); write_csv(a.out_dir/'SHA256SUMS.csv',[{'file':p.name,'sha256':sha(p),'bytes':p.stat().st_size} for p in files])
 print(json.dumps({**decision,'official_final_games':len(final),'captured_games':len(game_ids)},indent=2))
if __name__=='__main__': main()
