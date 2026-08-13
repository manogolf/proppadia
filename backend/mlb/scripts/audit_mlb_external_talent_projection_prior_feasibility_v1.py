#!/usr/bin/env python3
"""Read-only external talent projection prior feasibility audit."""
from __future__ import annotations
import hashlib,json
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_external_talent_projection_prior_feasibility_v1/2026-08-12';POP=ROOT/'artifacts/analysis/model_development/mlb_pa_outcome_prediction_foundation_v1/2026-08-12/pa_population_manifest.csv'
FG='https://www.fangraphs.com/projections?type=steamer';FGL='https://library.fangraphs.com/principles/projections/';FGNEWS='https://blogs.fangraphs.com/instagraphs/introducing-new-steamer-split-projections-and-more/';BP='https://www.baseballprospectus.com/pecota-projections/'
def sh(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 OUT.mkdir(parents=True,exist_ok=True);p=pd.read_csv(POP);bat=sorted(p.batter_id.unique());pit=sorted(p.starter_id.unique())
 inv=[{'source_system':'NONE','path_or_reference':'repository-wide rg inventory','projection_vintage':'NONE','batter_coverage':'0 retained projection rows','pitcher_coverage':'0 retained projection rows','fields':'NONE','raw_source_retained':'NO','historical_provenance':'NO','finding':'No Steamer, ZiPS, THE BAT/X, ATC, PECOTA, Depth Charts, preseason, ROS, or archived player projection dataset found.'}]
 pd.DataFrame(inv).to_csv(OUT/'external_projection_repository_inventory.csv',index=False)
 systems=[
  ('Steamer','YES','YES','YES: official 2026 preseason surface dated 2026-02-08','NO documented dated in-season archive','FanGraphs HTML; full data export members-only','public table / restricted export','FanGraphs ID in player URL; export ID schema must be verified','PA|H|1B|2B|3B|HR|BB|HBP|SO|AVG|OBP|SLG|ISO|BABIP|wOBA','GS|IP|H|HR|SO|BB|K/9|BB/9|HR/9|AVG|BABIP|ERA|FIP','HIGH',FG),
  ('ZiPS','YES','YES','YES: listed as 2026 preseason','NO documented dated in-season archive','FanGraphs table/export','public table / restricted export','FanGraphs ID; crosswalk required','standard batter projection fields','standard pitcher projection fields','MEDIUM',FG),
  ('Depth Charts','YES','YES','YES but continuously updated playing time makes exact vintage essential','NO dated snapshot retained','FanGraphs table/export','public table / restricted export','FanGraphs ID; crosswalk required','Steamer/ZiPS rates plus staff playing time','Steamer/ZiPS rates plus staff playing time','MEDIUM',FG),
  ('ATC','YES','YES','YES: listed as 2026 preseason','NO dated snapshot retained','FanGraphs table/export','public table / restricted export','FanGraphs ID; crosswalk required','standard batter fields / ensemble','standard pitcher fields / ensemble','MEDIUM',FG),
  ('THE BAT / THE BAT X','YES','THE BAT yes; THE BAT X hitter-focused','YES: listed as 2026 preseason','NO dated snapshot retained','FanGraphs table; vendor variants elsewhere','public table / restricted export; methodology proprietary','FanGraphs ID; crosswalk required','standard fields; THE BAT X includes Statcast','THE BAT standard pitcher fields; X limitation','MEDIUM',FG),
  ('PECOTA','YES','YES','not verified as recoverable raw 2026 preseason vintage','not verified','Baseball Prospectus','paid/restricted','source-specific IDs/crosswalk unknown','batter projections likely compatible but schema not probed','pitcher projections likely compatible but schema not probed','LOW',BP)]
 cols=['system','batter_available','pitcher_available','preseason_2026_recoverable','archived_inseason_recoverable','format','access','player_ids','batter_fields','pitcher_fields','provenance_quality','source_reference'];S=pd.DataFrame(systems,columns=cols);S.to_csv(OUT/'external_projection_source_feasibility.csv',index=False)
 temporal=pd.DataFrame([
  {'source':'FanGraphs Steamer 2026 Preseason','class':'A_PRESEASON_STATIC','vintage_evidence':'Page search snapshot Updated 2026-02-08 03:44 ET','historically_safe':'YES if exact frozen export matches vintage','usable_now':'NO','reason':'No raw dated export/hash retained; live page is mutable.'},
  {'source':'FanGraphs 2026 preseason families','class':'A_PRESEASON_STATIC','vintage_evidence':'Official page separates preseason from updated in-season projections','historically_safe':'POTENTIALLY','usable_now':'NO','reason':'Exact source file/vintage not retained.'},
  {'source':'Current RoS/update projections','class':'C_CURRENT_REST_OF_SEASON_ONLY','vintage_evidence':'Current live surface','historically_safe':'NO','usable_now':'NO','reason':'Must never be applied backward.'},
  {'source':'Any undated projection export','class':'D_UNKNOWN_VINTAGE','vintage_evidence':'none','historically_safe':'NO','usable_now':'NO','reason':'Unknown vintage.'}]);temporal.to_csv(OUT/'external_projection_temporal_validity.csv',index=False)
 fields=[]
 for ent,vals in [('BATTER',['PA','H','1B','2B','3B','HR','BB','HBP','SO','AVG','OBP','SLG','ISO','BABIP','wOBA']),('STARTER',['GS','IP','H','HR','SO','BB','K/9','BB/9','HR/9','AVG','BABIP','ERA','FIP'])]:
  for f in vals:fields.append({'preferred_source':'Steamer 2026 preseason','entity':ent,'field':f,'available_on_official_surface':'YES','PA_component_use':{'SO':'STRIKEOUT','K/9':'STRIKEOUT proxy','BB':'WALK_HBP partial','BB/9':'WALK_HBP proxy','HBP':'WALK_HBP completion','HR':'HOME_RUN','HR/9':'HOME_RUN proxy','H':'HIT','AVG':'HIT proxy','1B':'SINGLE','2B':'DOUBLE_TRIPLE','3B':'DOUBLE_TRIPLE','ISO':'XBH/power','BABIP':'contact-in-play','wOBA':'overall talent','FIP':'pitcher run-prevention proxy','ERA':'pitcher run-prevention proxy','PA':'playing-time/reliability','IP':'workload','GS':'starter role','OBP':'REACH_BASE','SLG':'power'}.get(f,'supporting')})
 pd.DataFrame(fields).to_csv(OUT/'external_projection_field_contract.csv',index=False)
 mapping=[]
 for ent,ids in [('BATTER',bat),('STARTER',pit)]:
  for pid in ids:mapping.append({'entity':ent,'mlbam_id':pid,'projection_source':'Steamer 2026 preseason','fangraphs_id':'UNRESOLVED','mapping_status':'UNMAPPED_NOT_TESTED','ambiguity':'UNKNOWN','reason':'No frozen raw export or retained MLBAM↔FanGraphs crosswalk; name-only match prohibited.'})
 pd.DataFrame(mapping).to_csv(OUT/'external_projection_player_mapping.csv',index=False)
 cov=[]
 for ent,total in [('BATTER',len(bat)),('STARTER',len(pit))]:
  for period in ['ALL','OPENING_WEEK','APRIL','MAY','JUNE','JULY']:cov.append({'source':'Steamer 2026 preseason','entity':ent,'period':period,'population_players':total,'exact_mapped':0,'ambiguous':0,'unmapped_or_unverified':total,'player_coverage_pct':'NOT_MEASURABLE','pa_weighted_coverage_pct':'NOT_MEASURABLE','game_coverage_pct':'NOT_MEASURABLE','reason':'Raw vintage/export unavailable; no projection rows may be presumed.'})
 pd.DataFrame(cov).to_csv(OUT/'external_projection_pa_population_coverage.csv',index=False)
 novelty=pd.DataFrame([
  {'system':'Steamer','independent_true_talent':'HIGH','historical_rate_transformation':'YES but independently modeled','playing_time_projection':'YES','aging_regression':'YES per FanGraphs projection-system description','ensemble':'NO','novelty_vs_proppadia':'HIGH'},
  {'system':'ZiPS','independent_true_talent':'HIGH','historical_rate_transformation':'independent advanced methodology','playing_time_projection':'YES','aging_regression':'YES/general projection methodology','ensemble':'NO','novelty_vs_proppadia':'HIGH'},
  {'system':'Depth Charts','independent_true_talent':'HIGH','historical_rate_transformation':'Steamer/ZiPS blend','playing_time_projection':'FanGraphs staff','aging_regression':'in component systems','ensemble':'YES','novelty_vs_proppadia':'HIGH'},
  {'system':'ATC','independent_true_talent':'HIGH','historical_rate_transformation':'weighted projection ensemble','playing_time_projection':'YES','aging_regression':'in component systems','ensemble':'YES','novelty_vs_proppadia':'HIGH'},
  {'system':'THE BAT/X','independent_true_talent':'HIGH','historical_rate_transformation':'proprietary; X includes Statcast','playing_time_projection':'YES','aging_regression':'UNKNOWN','ensemble':'NO','novelty_vs_proppadia':'HIGH'},
  {'system':'PECOTA','independent_true_talent':'HIGH','historical_rate_transformation':'proprietary projection','playing_time_projection':'YES','aging_regression':'YES broadly documented','ensemble':'NO','novelty_vs_proppadia':'HIGH'}]);novelty.to_csv(OUT/'external_projection_novelty_scorecard.csv',index=False)
 pa_map=pd.DataFrame([
  ('P_K','SO/PA','direct batter projection; pitcher K% or K/9 converted with BF/IP assumptions','FEASIBLE'),('P_BB_HBP','(BB+HBP)/PA','pitcher BB proxy; pitcher HBP may be absent','PARTIAL'),('P_HR','HR/PA','pitcher HR/9 requires BF/IP conversion','FEASIBLE_WITH_CONVERSION'),('P_HIT','H/PA or AVG with AB/PA reconciliation','pitcher H/allowed BF or AVG allowed','FEASIBLE'),('P_XBH','(2B+3B+HR)/PA','pitcher XBH allowed unavailable on dashboard','BATTER_DIRECT_PITCHER_LIMITED'),('P_SINGLE','1B/PA','pitcher singles allowed unavailable','BATTER_DIRECT_PITCHER_LIMITED'),('P_OTHER_OUT','1-sum mutually exclusive mapped classes','requires denominator reconciliation and exact taxonomy','RESIDUAL_ONLY')],columns=['target','batter_derivation','pitcher_derivation','status']);pa_map.to_csv(OUT/'external_projection_pa_mapping_contract.csv',index=False)
 pd.DataFrame([
  {'projection_field':'K% or K/9','allowed_component':'STRIKEOUT','mapping':'direct K% preferred; K/9 requires projected BF/IP','limitation':'dashboard may expose K/9 rather than K%'},
  {'projection_field':'BB% or BB/9','allowed_component':'WALK_HBP','mapping':'walk probability proxy','limitation':'HBP may be absent'},
  {'projection_field':'HR/9','allowed_component':'HOME_RUN','mapping':'convert using projected BF/IP','limitation':'BF may require H+BB+outs reconstruction'},
  {'projection_field':'BABIP / AVG allowed / H','allowed_component':'HIT','mapping':'contact-result proxy','limitation':'no direct singles/XBH allowed split'},
  {'projection_field':'FIP/ERA','allowed_component':'global pitcher talent','mapping':'feature only, not a PA class probability','limitation':'cannot uniquely decompose taxonomy'}]).to_csv(OUT/'external_projection_pitcher_mapping_contract.csv',index=False)
 score=[]
 vals={'Steamer':['HIGH','UNKNOWN','UNKNOWN','HIGH','LOW','HIGH','LOW','MEDIUM'],'ZiPS':['MEDIUM','UNKNOWN','UNKNOWN','HIGH','LOW','HIGH','LOW','MEDIUM'],'Depth Charts':['MEDIUM','UNKNOWN','UNKNOWN','HIGH','LOW','HIGH','LOW','MEDIUM'],'ATC':['MEDIUM','UNKNOWN','UNKNOWN','HIGH','LOW','HIGH','LOW','MEDIUM'],'THE BAT/X':['MEDIUM','UNKNOWN','UNKNOWN','MEDIUM','LOW','HIGH','LOW','MEDIUM'],'PECOTA':['LOW','UNKNOWN','UNKNOWN','MEDIUM','LOW','HIGH','LOW','HIGH']}
 for s,v in vals.items():score.append(dict(zip(['system','temporal_validity','batter_coverage','starter_coverage','pa_field_compatibility','player_id_mapping','methodological_independence','reproducibility','acquisition_effort'],[s]+v)))
 pd.DataFrame(score).to_csv(OUT/'external_projection_feasibility_scorecard.csv',index=False)
 (OUT/'external_projection_reproducibility_contract.md').write_text(f"""# External projection reproducibility contract

Before modeling, retain the exact raw 2026 preseason export, download timestamp, page/source reference, stated vintage, SHA-256, field dictionary, FanGraphs-to-MLBAM crosswalk with mapping disposition, license/use notes, and coverage report. Never silently replace it with a live or RoS projection. Current audit date: 2026-08-12.

Preferred source: FanGraphs Steamer 2026 preseason. Official search evidence identifies an update timestamp of 2026-02-08 03:44 ET, but no frozen raw file is retained locally. FanGraphs marks data export members-only. Therefore the visible table is evidence that the candidate existed, not a reproducible modeling input.

References: {FG}; {FGL}; {FGNEWS}
""")
 text=f"""# MLB External Talent Projection Prior Feasibility v1

`EXTERNAL_TALENT_PROJECTION_PRIOR_PARTIALLY_READY`  
`EXTERNAL_PROJECTION_PA_EXPERIMENT = NOT_READY`

- No external MLB projection rows or historical vintages are retained locally.
- Preferred candidate: FanGraphs Steamer 2026 preseason, official surface dated 2026-02-08 03:44 ET. Batter and pitcher schemas support K, BB, hit, XBH/HR, BABIP, workload, and residual PA mapping; pitcher XBH decomposition and HBP are partial.
- Exact coverage cannot be certified against 597 batters, 316 starters, or 68,865 PAs because the frozen export is absent and data export is members-only. All 913 identities remain unverified; name-only matching was not used.
- Steamer is methodologically independent of Proppadia's pooled history and is suitable in principle for `preseason prior -> strict-prior 2026 update`.
- Exact next step: obtain and retain the authentic 2026 preseason Steamer batter/pitcher export with its vintage and hash, build a stable-ID crosswalk, and rerun coverage. Do not fit `MLB_EXTERNAL_PROJECTION_PRIOR_PA_CHALLENGER_V1` until that gate passes.
""";(OUT/'concise_mlb_external_talent_projection_prior_feasibility_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sh(x)}  {x.name}\n' for x in files));print(json.dumps({'batters':len(bat),'starters':len(pit),'pa':len(p),'preferred':'FanGraphs Steamer 2026 preseason','decision':'EXTERNAL_TALENT_PROJECTION_PRIOR_PARTIALLY_READY','experiment':'NOT_READY'},indent=2))
if __name__=='__main__':main()
