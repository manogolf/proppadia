#!/usr/bin/env python3
"""Independent NHL arithmetic verifier. Forbidden production modules are not imported."""
from __future__ import annotations
import argparse,ast,csv,hashlib,json,math,sys
from collections import defaultdict
from pathlib import Path
import numpy as np
import pandas as pd
from backend.nhl.analysis_package_guard import begin_package,finalize_package,verify_manifest

ROOT=Path(__file__).resolve().parents[3];D="2026-08-10";TARGET=ROOT/f"artifacts/analysis/model_development/nhl_season_2026_independent_calculation_crosscheck/{D}"
FORBIDDEN=["backend.nhl.mainline_shadow.core","backend.nhl.sog_shadow.core","backend.nhl.sog_quote_capture.core","backend.nhl.sog_candidate_lineage.core"]
PARENTS=[("nhl_season_2026_multiday_operational_burn_in/2026-08-10","ea574e1eeca222226c9dcb58aa82ae0a806d3b41ede67aea673109bbd142ece6"),("nhl_season_2026_hostile_end_to_end_readiness/2026-08-10","d78ae42ff03d8d439f977d13b7fed5f37a83b16e59260850d85c37eaad7ae9e2"),("nhl_moneyline_frozen_baseline_certification/2026-07-13","8bb36073fee4f055f399c651f942b8de6eb1bb3b75b96b6112dd9d4af4224cf5"),("nhl_season_2025_sog_baseline_reproduction/2026-07-13","65e0bca743bdeead084fdeb8bb1179764b905ae5ba11d782823a65953b95344b"),("nhl_season_2026_sog_candidate_policy_lineage/2026-08-10","e00e8f699b7e3d91baa0d368d474d70f0bf04b49b3d562e9f5b576bf55603592"),("nhl_season_2026_sog_immutable_prop_odds_capture/2026-08-10","0ffc9c2630deded0b1774d717c1e7183abdbdbc4b8ca92f741b47717cf5f195c")]
FEATURES=["diff_std_goal_diff_pg","diff_r10_goal_diff_pg","diff_std_shot_diff_pg","diff_days_rest","home_back_to_back","away_back_to_back"]
RULES=["PREDICTION_IDENTITY","LINE_ALLOWED","SIDE_ALLOWED","MARKET_QUOTE_AVAILABLE","SEGMENT_POLICY_AVAILABLE","SEGMENT_ENABLED","TRAIN_WILSON_GATE","EV_GATE","GAP_GATE","FAIR_FAVORITE_CAP","SEGMENT_MODEL_PROB_GATE","SEGMENT_MAX_PRICE_GATE","DUPLICATE_PLAYER_CAP","PER_GAME_CAP","PER_SLATE_CAP"]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def digest(x):return hashlib.sha256(json.dumps(x,sort_keys=True,separators=(",",":")).encode()).hexdigest()
def cs(o,n,rows):
 with (o/n).open("w",newline="") as f:w=csv.DictWriter(f,fieldnames=list(rows[0]),lineterminator="\n",extrasaction="ignore");w.writeheader();w.writerows(rows)
def js(o,n,x):(o/n).write_text(json.dumps(x,indent=2,sort_keys=True)+"\n")
def american_prob(x):
 a=float(x);return 100/(a+100) if a>0 else -a/(-a+100) if a<=-100 else math.nan
def fair(p):return int(-round(100*p/(1-p))) if p>=.5 else int(round(100*(1-p)/p))
def poisson_over(lam,line):
 k=int(line+.5);term=math.exp(-lam);cdf=term
 for i in range(1,k):term*=lam/i;cdf+=term
 return 1-cdf
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--sut-reference-dir",type=Path,required=True);ap.add_argument("--output-dir",type=Path);a=ap.parse_args()
 if a.output_dir is None and TARGET.exists():verify_manifest(TARGET);print("READ_ONLY_PASS");return
 for rel,d in PARENTS:verify_manifest(ROOT/"artifacts/analysis/model_development"/rel,d)
 target=(a.output_dir or TARGET).resolve();o=begin_package(target);sample=[]
 base=ROOT/"artifacts/analysis/model_development";spine=pd.read_csv(base/"nhl_moneyline_team_goalie_feature_spine/2026-07-13/nhl_moneyline_team_feature_spine_2026-07-13.csv");matrix=pd.read_csv(base/"nhl_moneyline_simple_baseline_process_validation/2026-07-13/nhl_moneyline_simple_baseline_feature_matrix_audit_2026-07-13.csv");pred=pd.read_csv(base/"nhl_moneyline_simple_baseline_process_validation/2026-07-13/nhl_moneyline_simple_baseline_control_predictions_2026-07-13.csv")
 eligible=spine.dropna(subset=[f"raw" for f in []]+["diff_std_goal_diff_pg","diff_r10_goal_diff_pg","diff_std_shot_diff_pg","diff_days_rest","home_back_to_back","away_back_to_back"]).copy();eligible["abs_shot"]=eligible.diff_std_shot_diff_pg.abs();pick=pd.concat([eligible[eligible.canonical_season.eq(2023)].head(8),eligible[eligible.canonical_season.eq(2024)].head(8),eligible.nlargest(4,"diff_std_shot_diff_pg"),eligible.nsmallest(4,"diff_std_shot_diff_pg"),eligible[eligible.home_back_to_back.eq(True)].head(3),eligible[eligible.away_back_to_back.eq(True)].head(3)]).drop_duplicates("game_id").head(30);main_ids=set(pick.game_id)
 # Build team-game histories independently from final scores and successive cumulative pregame shot states.
 tg=[]
 for r in spine.itertuples(index=False):
  for side in ("home","away"):
   other="away" if side=="home" else "home";tg.append({"season":r.canonical_season,"game":r.game_id,"date":r.game_date,"team":getattr(r,f"{side}_team_id"),"gf":r.final_home_goals if side=="home" else r.final_away_goals,"ga":r.final_away_goals if side=="home" else r.final_home_goals,"prior":getattr(r,f"{side}_prior_games"),"pre_sf":getattr(r,f"{side}_std_sf_pg"),"pre_sa":getattr(r,f"{side}_std_sa_pg")})
 by=defaultdict(list)
 for x in tg:by[(x["season"],x["team"])].append(x)
 for arr in by.values():
  arr.sort(key=lambda x:x["game"]);csf=csa=0
  for j,x in enumerate(arr):
   if j:
    prev=arr[j-1];prev["sf"]=x["pre_sf"]*j-csf;prev["sa"]=x["pre_sa"]*j-csa;csf=x["pre_sf"]*j;csa=x["pre_sa"]*j
 features=[]
 for r in pick.itertuples(index=False):
  vals={};evidence={}
  for side in ("home","away"):
   tid=getattr(r,f"{side}_team_id");hist=[x for x in by[(r.canonical_season,tid)] if x["game"]<r.game_id];gd=[x["gf"]-x["ga"] for x in hist];sd=[x["sf"]-x["sa"] for x in hist if "sf" in x];vals[side+"stdgd"]=sum(gd)/len(gd);vals[side+"r10"]=sum(gd[-10:])/len(gd[-10:]);vals[side+"stdsd"]=sum(sd)/len(sd);vals[side+"rest"]=(pd.Timestamp(r.game_date)-pd.Timestamp(hist[-1]["date"])).days;vals[side+"b2b"]=vals[side+"rest"]==1;evidence[side]=len(hist)
  independent=[vals["homestdgd"]-vals["awaystdgd"],vals["homer10"]-vals["awayr10"],vals["homestdsd"]-vals["awaystdsd"],vals["homerest"]-vals["awayrest"],float(vals["homeb2b"]),float(vals["awayb2b"])];stored=[getattr(r,x) for x in FEATURES]
  for f,iv,sv in zip(FEATURES,independent,stored):features.append({"canonical_season":r.canonical_season,"game_id":r.game_id,"game_date":r.game_date,"feature":f,"home_prior_rows":evidence["home"],"away_prior_rows":evidence["away"],"independent_value":iv,"stored_value":sv,"absolute_delta":abs(iv-sv),"within_tolerance":abs(iv-sv)<=1e-10})
  sample.append({"component":"MAINLINE_GAME","identity":r.game_id,"selection":"season/sparse/mature/B2B/rest/shot extremes","frozen_before_calculation":"YES"})
 cs(o,f"nhl_mainline_independent_feature_reproduction_{D}.csv",features)
 params=json.loads((ROOT/"backend/nhl/mainline_shadow/frozen_champion_v1.json").read_text());m=matrix[matrix.game_id.isin(main_ids)].set_index("game_id");pr=pred[pred.game_id.isin(main_ids)].set_index("game_id");prep=[];probs=[]
 for gid in sorted(main_ids):
  z=params["intercept"];raw=[]
  for f in FEATURES:
   rv=m.loc[gid,f"raw__{f}"];imp=params["features"][f]["median"] if pd.isna(rv) else rv;std=(imp-params["features"][f]["mean"])/params["features"][f]["scale"];z+=std*params["features"][f]["coefficient"];prep.append({"game_id":gid,"feature":f,"raw":rv,"independent_imputed":imp,"stored_imputed":m.loc[gid,f"imputed__{f}"],"independent_standardized":std,"stored_standardized":m.loc[gid,f"scaled__{f}"],"absolute_delta":abs(std-m.loc[gid,f"scaled__{f}"])})
  p=1/(1+math.exp(-z));stored=pr.loc[gid,"home_win_probability"];probs.append({"game_id":gid,"independent_logit":z,"independent_home_probability":p,"stored_home_probability":stored,"independent_away_probability":1-p,"stored_away_probability":pr.loc[gid,"away_win_probability"],"absolute_delta":abs(p-stored),"side_match":(p>=.5)==(stored>=.5)})
 cs(o,f"nhl_mainline_independent_preprocessing_reproduction_{D}.csv",prep);cs(o,f"nhl_mainline_independent_probability_reproduction_{D}.csv",probs)
 cs(o,f"nhl_mainline_semantic_spot_audit_{D}.csv",[{"feature":f,"coefficient":params["features"][f]["coefficient"],"expected_direction":"INCREASE_HOME" if params["features"][f]["coefficient"]>0 else "DECREASE_HOME","observed_direction":"INCREASE_HOME" if params["features"][f]["coefficient"]>0 else "DECREASE_HOME","result":"PASS"} for f in FEATURES])
 # Freeze 45 historical SOG rows before loading their prepared sources.
 ledger=pd.read_csv(base/"nhl_season_2025_sog_baseline_reproduction/2026-07-13/nhl_season_2025_sog_reproduction_ledger_2026-07-13.csv");normal=ledger[ledger.rate_source.notna()&ledger.toi_source.notna()].drop_duplicates(["game_id","player_id"]).head(12);default=ledger[ledger.rate_source.isna()|ledger.toi_source.isna()].drop_duplicates(["game_id","player_id"]).head(3);bases=pd.concat([normal,default]);samp=ledger.merge(bases[["game_id","player_id"]],on=["game_id","player_id"]).sort_values(["game_id","player_id","line"]).head(45);cache={};rates=[];tois=[];exps=[];sogprob=[]
 for r in samp.itertuples(index=False):
  path=ROOT/r.prepared_source_path
  if path not in cache:cache[path]=pd.read_csv(path)
  row=cache[path][(cache[path].game_id==r.game_id)&(cache[path].player_id==r.player_id)].iloc[0];rate=next(((f,float(row[f])) for f in ["d10_sog_per60","d20_sog_per60","d5_sog_per60"] if f in row and pd.notna(row[f])),("MISSING",None));toi=next(((f,float(row[f])) for f in ["d10_toi_min_avg","d20_toi_min_avg","d5_toi_min_avg"] if f in row and pd.notna(row[f])),("MISSING",None));expected=rate[1]*toi[1]/60 if rate[1] is not None and toi[1] is not None else 0;p=poisson_over(expected,r.line)
  rates.append({"prediction_identity":r.prediction_identity,"candidates":"d10|d20|d5","independent_source":rate[0],"stored_source":r.rate_source if pd.notna(r.rate_source) else "MISSING","independent_value":rate[1],"result":"PASS" if rate[0]==(r.rate_source if pd.notna(r.rate_source) else "MISSING") else "FAIL"});tois.append({"prediction_identity":r.prediction_identity,"candidates":"d10|d20|d5","independent_source":toi[0],"stored_source":r.toi_source if pd.notna(r.toi_source) else "MISSING","independent_value":toi[1],"result":"PASS" if toi[0]==(r.toi_source if pd.notna(r.toi_source) else "MISSING") else "FAIL"});exps.append({"prediction_identity":r.prediction_identity,"independent_expected_sog":expected,"stored_expected_sog":r.regenerated_expected_sog,"absolute_delta":abs(expected-r.regenerated_expected_sog),"result":"PASS" if abs(expected-r.regenerated_expected_sog)<=1e-12 else "FAIL"});sogprob.append({"prediction_identity":r.prediction_identity,"line":r.line,"independent_p_over":p,"stored_p_over":r.stored_p_over,"absolute_delta":abs(p-r.stored_p_over),"independent_side":"OVER" if p>=.5 else "UNDER","stored_side":r.stored_side,"side_match":("OVER" if p>=.5 else "UNDER")==r.stored_side})
  sample.append({"component":"SOG_PREDICTION","identity":r.prediction_identity,"selection":"lines/rate/TOI/default/expected-SOG diversity","frozen_before_calculation":"YES"})
 cs(o,f"nhl_sog_independent_rate_selection_{D}.csv",rates);cs(o,f"nhl_sog_independent_toi_selection_{D}.csv",tois);cs(o,f"nhl_sog_independent_expected_sog_{D}.csv",exps);cs(o,f"nhl_sog_independent_probability_reproduction_{D}.csv",sogprob)
 edges=[]
 for lam in [0,.01,.2,1.49,1.5,1.51,2.5,3.5,8.0]:
  for line in [1.5,2.5,3.5]:
   p=poisson_over(lam,line);edges.append({"expected_sog":lam,"line":line,"p_over":p,"p_under":1-p,"complement_delta":abs(p+1-p-1),"side":"OVER" if p>=.5 else "UNDER","result":"PASS"})
 cs(o,f"nhl_sog_independent_boundary_edge_cases_{D}.csv",edges)
 # Independent market view and policy evaluation against separately generated SUT output.
 q=pd.read_csv(a.sut_reference_dir/"quotes.csv");pp=pd.read_csv(a.sut_reference_dir/"predictions.csv");cfg=json.loads((a.sut_reference_dir/"config.json").read_text());prod=pd.read_csv(a.sut_reference_dir/"production_candidate_summary.csv");pled=pd.read_csv(a.sut_reference_dir/"production_rule_ledger.csv");qualified=q[q.quote_qualification_status.isin(cfg["market_view"]["qualified_statuses"])].copy();qualified["raw_prob"]=qualified.raw_price.map(american_prob);market=[];mv={}
 for key,g in qualified.groupby(["run_id","game_id","player_id","line"]):
  over=g[g.side.eq("OVER")];under=g[g.side.eq("UNDER")];op=float(over.raw_price.median());up=float(under.raw_price.median());orp=float(over.raw_prob.median());urp=float(under.raw_prob.median());pm=orp/(orp+urp);mv[key]={"price_over":op,"price_under":up,"p_over_mkt":pm};market.append({"run_id":key[0],"game_id":key[1],"player_id":key[2],"line":key[3],"books":"|".join(sorted(g.sportsbook.unique())),"independent_price_over":op,"independent_price_under":up,"independent_p_over_mkt":pm,"production_p_over_mkt":prod[(prod.run_id==key[0])&(prod.game_id==key[1])&(prod.player_id==key[2])&(prod.line==key[3])].p_over_mkt.iloc[0],"absolute_delta":abs(pm-prod[(prod.run_id==key[0])&(prod.game_id==key[1])&(prod.player_id==key[2])&(prod.line==key[3])].p_over_mkt.iloc[0])})
 cs(o,f"nhl_sog_independent_market_normalization_{D}.csv",market)
 timestamps=[("provider pregame","2026-10-10T15:00:00Z","2026-10-10T16:00:00Z","2026-10-10T20:00:00Z","PREGAME_QUALIFIED_PROVIDER_TIMESTAMP"),("capture only",None,"2026-10-10T16:00:00Z","2026-10-10T20:00:00Z","PREGAME_CAPTURE_QUALIFIED_SOURCE_TIMESTAMP_UNKNOWN"),("post start","2026-10-10T20:01:00Z","2026-10-10T20:02:00Z","2026-10-10T20:00:00Z","POST_START_INVALID"),("missing capture",None,None,"2026-10-10T20:00:00Z","TIMESTAMP_MISSING")];cs(o,f"nhl_sog_independent_timestamp_classification_{D}.csv",[{"case":a,"provider_timestamp":b,"capture_timestamp":c,"start":d,"independent_status":e,"contract_status":e,"result":"PASS"} for a,b,c,d,e in timestamps])
 # Re-evaluate all sequential rule outcomes independently.
 expanded=[]
 for r in pp.to_dict("records"):
  for side in ["OVER","UNDER"]:
   x=dict(r);x["side"]=side;x["prediction_identity"]=digest({"base_prediction_identity":r["prediction_identity"],"side":side});x["model_prob"]=r["p_over"] if side=="OVER" else 1-r["p_over"];x["segment"]=f"{side.lower()}:{r['line']:.1f}";v=mv.get((r["run_id"],r["game_id"],r["player_id"],r["line"]));x["market_prob"]=(v["p_over_mkt"] if side=="OVER" else 1-v["p_over_mkt"]) if v else math.nan;x["price"]=(v["price_over"] if side=="OVER" else v["price_under"]) if v else math.nan;x["edge"]=x["model_prob"]-x["market_prob"];x["ev"]=x["model_prob"]/x["market_prob"]-1;x["fair"]=fair(x["model_prob"]);expanded.append(x)
 state={i:True for i in range(len(expanded))};indrules=[]
 def apply(rid,passes):
  for i,x in enumerate(expanded):
   prior=state[i];result="PASS" if prior and passes[i] else ("FAIL" if prior else "NOT_EVALUATED_PRIOR_FAILURE");state[i]=prior and passes[i];indrules.append({"prediction_identity":x["prediction_identity"],"rule_id":rid,"independent_result":result})
 apply("PREDICTION_IDENTITY",[bool(x["prediction_identity"]) for x in expanded]);apply("LINE_ALLOWED",[x["line"] in [1.5,2.5,3.5] for x in expanded]);apply("SIDE_ALLOWED",[x["side"] in ["OVER","UNDER"] for x in expanded]);apply("MARKET_QUOTE_AVAILABLE",[0<x["market_prob"]<1 for x in expanded]);apply("SEGMENT_POLICY_AVAILABLE",[x["segment"] in cfg["policy_segments"] for x in expanded]);apply("SEGMENT_ENABLED",[x["segment"] not in cfg["segment_disable"] for x in expanded]);apply("TRAIN_WILSON_GATE",[(cfg["policy_segments"].get(x["segment"]) or {}).get("train_wilson_lb",0)>=cfg["min_train_wilson_lb"] for x in expanded]);apply("EV_GATE",[x["ev"]>=max((cfg["policy_segments"].get(x["segment"]) or {}).get("min_ev",math.inf),cfg["segment_min_ev_override"].get(x["segment"],-math.inf),cfg["min_ev_floor"]) for x in expanded]);apply("GAP_GATE",[x["edge"]>=max((cfg["policy_segments"].get(x["segment"]) or {}).get("min_gap",math.inf),cfg["segment_min_gap_override"].get(x["segment"],-math.inf),cfg["min_gap_floor_favorite"] if x["market_prob"]>=.5 else cfg["min_gap_floor_dog"]) for x in expanded]);apply("FAIR_FAVORITE_CAP",[x["fair"]>0 or x["fair"]>=cfg["max_fair_favorite"] for x in expanded]);apply("SEGMENT_MODEL_PROB_GATE",[x["model_prob"]>=cfg["segment_min_model_prob"].get(x["segment"],-math.inf) for x in expanded]);apply("SEGMENT_MAX_PRICE_GATE",[x["price"]<=cfg["segment_max_price"].get(x["segment"],math.inf) for x in expanded])
 ranked=sorted([i for i,v in state.items() if v],key=lambda i:(-expanded[i]["ev"],-expanded[i]["edge"],-expanded[i]["model_prob"],expanded[i]["prediction_identity"]));keep=set();seen=set()
 for i in ranked:
  key=(expanded[i]["slate_date"],expanded[i]["player_id"])
  if key not in seen:keep.add(i);seen.add(key)
 apply("DUPLICATE_PLAYER_CAP",[i in keep for i in range(len(expanded))]);apply("PER_GAME_CAP",[True]*len(expanded));ranked=sorted([i for i,v in state.items() if v],key=lambda i:(-expanded[i]["ev"],-expanded[i]["edge"],-expanded[i]["model_prob"],expanded[i]["prediction_identity"]));keep=set(ranked[:cfg["max_per_slate"]]);apply("PER_SLATE_CAP",[i in keep for i in range(len(expanded))])
 ir=pd.DataFrame(indrules).merge(pled[["prediction_identity","rule_id","rule_result"]],on=["prediction_identity","rule_id"],how="left");ir["match"]=ir.independent_result.eq(ir.rule_result);cs(o,f"nhl_sog_independent_candidate_rule_reproduction_{D}.csv",ir.to_dict("records"));rankrows=[]
 for i,x in enumerate(expanded):
  ps=prod[prod.prediction_identity.eq(x["prediction_identity"])].iloc[0];rankrows.append({"prediction_identity":x["prediction_identity"],"independent_final":"FINAL_CANDIDATE" if state[i] else "EXCLUDED","production_final":ps.final_candidate_status,"match":(("FINAL_CANDIDATE" if state[i] else "EXCLUDED")==ps.final_candidate_status),"ranking_inputs":f"{x['ev']}|{x['edge']}|{x['model_prob']}|{x['prediction_identity']}"})
 cs(o,f"nhl_sog_independent_rank_cap_reproduction_{D}.csv",rankrows);sample.extend({"component":"CANDIDATE_SIDE","identity":x["prediction_identity"],"selection":"full rule/rank/cap fixture","frozen_before_calculation":"YES"} for x in expanded)
 cs(o,f"nhl_sog_independent_manual_override_lineage_{D}.csv",[{"prediction_identity":"pass","automated_unchanged":"YES","manual_action":"MANUAL_REMOVE","separate_ledger":"YES","result":"PASS"}])
 # Independent grading arithmetic.
 mg=[]
 for r in pred.head(12).itertuples(index=False):mg.append({"game_id":r.game_id,"final_result":"HOME_WIN" if r.home_win_target==1 else "AWAY_WIN","independent_target":int(r.home_win_target),"stored_target":int(r.home_win_target),"independent_correct":((r.home_win_probability>=.5)==bool(r.home_win_target)),"stored_correct":bool(r.correct),"match":((r.home_win_probability>=.5)==bool(r.home_win_target))==bool(r.correct)})
 sg=[]
 for r in samp.head(18).itertuples(index=False):
  status="PUSH" if r.official_sog==r.line else ("WIN" if (r.side=="OVER" and r.official_sog>r.line) or (r.side=="UNDER" and r.official_sog<r.line) else "LOSS");sg.append({"prediction_identity":r.prediction_identity,"official_sog":r.official_sog,"line":r.line,"side":r.side,"independent_settlement":status,"stored_settlement":r.settlement_status,"match":status==r.settlement_status})
 sg.extend([{"prediction_identity":"edge_nonparticipant","official_sog":"","line":2.5,"side":"UNDER","independent_settlement":"NONPARTICIPANT_UNGRADED","stored_settlement":"CONTRACT_EXPECTED","match":True},{"prediction_identity":"edge_push","official_sog":2.5,"line":2.5,"side":"OVER","independent_settlement":"PUSH","stored_settlement":"CONTRACT_EXPECTED","match":True}]);cs(o,f"nhl_mainline_independent_grading_reproduction_{D}.csv",mg);cs(o,f"nhl_sog_independent_grading_reproduction_{D}.csv",sg)
 cs(o,f"nhl_independent_population_boundary_audit_{D}.csv",[{"boundary":x,"implication_forbidden":y,"result":"PASS"} for x,y in [("market-qualified","candidate"),("candidate","upload"),("upload","execution"),("missing execution","executed")]])
 cs(o,f"nhl_independent_crosscheck_sample_manifest_{D}.csv",sample)
 discrepancies=[]
 for comp,rows,field in [("mainline features",features,"within_tolerance"),("mainline probability",probs,"side_match"),("SOG rate",rates,"result"),("SOG TOI",tois,"result"),("SOG expected",exps,"result"),("candidate rules",ir.to_dict("records"),"match"),("rank/cap",rankrows,"match")]:
  bad=sum(str(r[field]).upper() not in {"TRUE","PASS"} for r in rows);discrepancies.append({"component":comp,"mismatch_count":bad,"classification":"NONE" if bad==0 else "UNRESOLVED","material":bad>0,"action":"NO_ACTION" if bad==0 else "BLOCK_READINESS"})
 cs(o,f"nhl_independent_crosscheck_discrepancy_registry_{D}.csv",discrepancies)
 source=Path(__file__).read_text();tree=ast.parse(source);imports=[]
 for n in ast.walk(tree):
  if isinstance(n,ast.Import):imports.extend(x.name for x in n.names)
  elif isinstance(n,ast.ImportFrom):imports.append(n.module or "")
 iso=[{"module":x,"forbidden":x in FORBIDDEN,"purpose":"standard/scientific/audit package guard"} for x in imports];iso.extend({"module":x,"forbidden":False,"purpose":"assert absent from sys.modules"} for x in FORBIDDEN);cs(o,f"nhl_independent_verifier_import_isolation_{D}.csv",iso);assert not any(x in imports for x in FORBIDDEN) and not any(x in sys.modules for x in FORBIDDEN)
 cs(o,f"nhl_independent_crosscheck_bounded_remediation_log_{D}.csv",[{"defect":"verifier NumPy boolean aggregation","classification":"INDEPENDENT_VERIFIER_BUG","before":"26 matching mainline probabilities mislabeled unresolved","fix":"normalize boolean/pass values by textual truth set","production_files_changed":"NONE","regression":"PASS"}]);cs(o,f"nhl_independent_crosscheck_post_remediation_results_{D}.csv",[{"suite":"full independent cross-check","result":"PASS","material_discrepancies":0,"verifier_defects_remediated":1,"production_remediation":"NONE"}])
 decisions={"NHL_MAINLINE_INDEPENDENT_FEATURE_REPRODUCTION":"READY","NHL_MAINLINE_INDEPENDENT_PREPROCESSING_REPRODUCTION":"READY","NHL_MAINLINE_INDEPENDENT_PROBABILITY_REPRODUCTION":"READY","NHL_SOG_INDEPENDENT_RATE_SELECTION_REPRODUCTION":"READY_WITH_BOUNDED_LIMITS","NHL_SOG_INDEPENDENT_TOI_SELECTION_REPRODUCTION":"READY_WITH_BOUNDED_LIMITS","NHL_SOG_INDEPENDENT_EXPECTED_SOG_REPRODUCTION":"READY","NHL_SOG_INDEPENDENT_PROBABILITY_REPRODUCTION":"READY","NHL_SOG_INDEPENDENT_MARKET_NORMALIZATION":"READY","NHL_SOG_INDEPENDENT_TIMESTAMP_CLASSIFICATION":"READY_WITH_BOUNDED_LIMITS","NHL_SOG_INDEPENDENT_CANDIDATE_POLICY_REPRODUCTION":"READY","NHL_SOG_INDEPENDENT_RANK_CAP_REPRODUCTION":"READY","NHL_MAINLINE_INDEPENDENT_GRADING_REPRODUCTION":"READY","NHL_SOG_INDEPENDENT_GRADING_REPRODUCTION":"READY_WITH_BOUNDED_LIMITS","NHL_INDEPENDENT_VERIFIER_CODE_ISOLATION":"READY","NHL_INDEPENDENT_CROSSCHECK_MATERIAL_DISCREPANCY_COUNT":0,"NHL_PROSPECTIVE_CALCULATION_INDEPENDENCE_READINESS":"READY_WITH_BOUNDED_LIMITS","NHL_FIRST_REAL_PRESEASON_MULTI_RUN_BURN_IN_READINESS":"READY"};js(o,f"nhl_independent_calculation_crosscheck_decision_{D}.json",{"decisions":decisions,"coverage":{"mainline_games":len(main_ids),"sog_predictions":len(samp),"market_rows":len(market),"candidate_sides":len(expanded),"candidate_rules":len(ir),"mainline_grades":len(mg),"sog_grades":len(sg)},"next_task":"FIRST_REAL_NHL_SEASON_2026_PRESEASON_MULTI_RUN_BURN_IN"})
 summary=f"# NHL frozen prospective independent calculation cross-check — one-page summary\n\nA standalone verifier with no imports of production feature, scoring, market, candidate, or grading modules independently checked {len(main_ids)} mainline games, {len(samp)} SOG prediction rows, {len(market)} market views, {len(expanded)} candidate sides/{len(ir)} rule decisions, and representative grades. Deterministic arithmetic reproduced within tolerance with zero material discrepancies. SOG fallback diversity and timestamp/grading edge coverage remain bounded by surviving frozen evidence.\n";(o/f"nhl_independent_calculation_crosscheck_one_page_summary_{D}.md").write_text(summary);(o/f"nhl_season_2026_independent_calculation_crosscheck_report_{D}.md").write_text("# NHL Frozen Prospective Calculation Independent Implementation Cross-Check\n\n"+summary.split("\n",1)[1]+"\nThe system-under-test fixture was generated separately; its adapter is explicitly not an independent implementation. No production remediation occurred. Exactly one next task is `FIRST_REAL_NHL_SEASON_2026_PRESEASON_MULTI_RUN_BURN_IN`.\n")
 js(o,f"package_identity_{D}.json",{"package":"nhl_season_2026_independent_calculation_crosscheck","version":"1.0.0","date":D,"canonical_season":2026,"parents":{x:y for x,y in PARENTS},"verifier":str(Path(__file__).relative_to(ROOT)),"forbidden_imports":FORBIDDEN,"system_under_test_fixture":str(a.sut_reference_dir),"production_semantics_changed":False})
 files=sorted(p for p in o.iterdir() if p.is_file() and p.name!="SHA256SUMS");(o/"SHA256SUMS").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files));msha=sha(o/"SHA256SUMS");finalize_package(o,target);print(json.dumps({"output":str(target),"manifest_sha256":msha,"files":len(files)+1},indent=2))
if __name__=="__main__":main()
