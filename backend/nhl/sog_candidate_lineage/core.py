"""Deterministic shadow-only candidate decisions with normalized rule lineage."""
from __future__ import annotations
import hashlib,json,math
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

POLICY_NAME="NHL_SOG_STEP4A_DEFAULT_DAILY_POLICY"
POLICY_VERSION="v1_lineage"
QUALIFIED={"PREGAME_QUALIFIED_PROVIDER_TIMESTAMP","PREGAME_CAPTURE_QUALIFIED_SOURCE_TIMESTAMP_UNKNOWN"}
RULES=[
 (10,"PREDICTION_IDENTITY","PREDICTION_ELIGIBILITY"),(20,"LINE_ALLOWED","LINE_FILTER"),(30,"SIDE_ALLOWED","SIDE_FILTER"),
 (40,"MARKET_QUOTE_AVAILABLE","MARKET_AVAILABILITY"),(50,"SEGMENT_POLICY_AVAILABLE","PREDICTION_ELIGIBILITY"),
 (60,"SEGMENT_ENABLED","SIDE_FILTER"),(70,"TRAIN_WILSON_GATE","CONFIDENCE_FILTER"),(80,"EV_GATE","CONFIDENCE_FILTER"),
 (90,"GAP_GATE","CONFIDENCE_FILTER"),(100,"FAIR_FAVORITE_CAP","PRICE_FILTER"),(110,"SEGMENT_MODEL_PROB_GATE","CONFIDENCE_FILTER"),
 (120,"SEGMENT_MAX_PRICE_GATE","PRICE_FILTER"),(130,"DUPLICATE_PLAYER_CAP","DUPLICATE_SUPPRESSION"),
 (140,"PER_GAME_CAP","SLATE_CAP"),(150,"PER_SLATE_CAP","SLATE_CAP"),
]

def canonical(obj:Any)->str:return json.dumps(obj,sort_keys=True,separators=(",",":"),ensure_ascii=False)
def digest(obj:Any)->str:return hashlib.sha256(canonical(obj).encode()).hexdigest()
def file_hash(path:Path)->str:return hashlib.sha256(path.read_bytes()).hexdigest()
def fair(p:float)->int|None:
 if not 0<p<1:return None
 return int(-round(100*p/(1-p))) if p>=.5 else int(round(100*(1-p)/p))
def american_prob(x:Any)->float:
 try:a=float(x)
 except:return np.nan
 return 100/(a+100) if a>0 else -a/(-a+100) if a<=-100 else np.nan

def default_config(policy_segments:dict[str,dict[str,Any]])->dict[str,Any]:
 return {"policy_name":POLICY_NAME,"policy_version":POLICY_VERSION,"source_path":"backend/nhl/scripts/select_sog_candidates_live.py",
  "authority":"docs/NHL SOG Command Deck.md Step 4a Default daily upload (recommended)","policy_segments":policy_segments,
  "market_view":{"method":"LEGACY_SEMANTIC_MEDIAN_ELIGIBLE_BOOK_QUOTES","price_aggregation":"median American price per side","probability_aggregation":"median raw implied probability per side then two-sided proportional no-vig","qualified_statuses":sorted(QUALIFIED)},
  "min_train_wilson_lb":0.0,"min_ev_floor":0.0,"min_gap_floor_favorite":0.0,"min_gap_floor_dog":0.0,
  "max_per_player":1,"max_per_game":0,"max_per_slate":0,"max_fair_favorite":-300,
  "segment_min_ev_override":{"over:2.5":0.15,"under:2.5":0.19},"segment_min_gap_override":{"over:2.5":0.07,"under:2.5":0.10},
  "segment_min_model_prob":{"under:1.5":0.65},"segment_max_price":{"under:1.5":100.0,"over:3.5":130.0},
  "segment_disable":[],"segment_alpha":{},"ranking":[["ev_side","DESC"],["edge_side","DESC"],["model_side_prob","DESC"],["prediction_identity","ASC"]],
  "matchup_confirmation":{"enabled":True,"selection_effect":"DESCRIPTIVE_ONLY"},"fallback":"FAIL_CLOSED_NO_ALTERNATE_SELECTOR",
  "manual_override":"SEPARATE_APPEND_ONLY_LEDGER_ONLY","mode":"SHADOW_CANDIDATE_OBSERVATION_ONLY"}

def effective_config(policy_segments:dict[str,dict[str,Any]],overrides:dict[str,Any]|None=None)->dict[str,Any]:
 cfg=default_config(policy_segments); supplied=overrides or {}; allowed=set(cfg)-{"policy_name","policy_version","source_path","authority","mode"}
 unknown=set(supplied)-allowed
 if unknown:raise ValueError(f"unrecorded/unsupported overrides: {sorted(unknown)}")
 cfg.update(supplied); cfg["base_policy_manifest_hash"]=digest({"name":POLICY_NAME,"version":POLICY_VERSION,"rules":RULES,"source":cfg["source_path"]}); cfg["effective_config_hash"]=digest(cfg); return cfg

def market_view(quotes:pd.DataFrame)->pd.DataFrame:
 q=quotes[quotes.quote_qualification_status.isin(QUALIFIED)&quotes.canonical_prop_type.eq("shots_on_goal")].copy()
 q["line"]=pd.to_numeric(q.line,errors="coerce"); q["raw_implied_prob"]=q.raw_price.map(american_prob); q["side"]=q.side.str.upper()
 keys=["run_id","game_id","player_id","line"]
 z=q.groupby(keys+["side"],dropna=False).agg(median_price=("raw_price","median"),median_raw_prob=("raw_implied_prob","median"),quote_count=("sportsbook","size"),sportsbooks=("sportsbook",lambda x:"|".join(sorted(set(map(str,x))))),quote_payload_hashes=("raw_payload_sha256",lambda x:"|".join(sorted(set(map(str,x)))))).reset_index()
 if z.empty:return pd.DataFrame(columns=keys+["price_over","price_under","p_over_mkt","market_snapshot_identity","quote_count_over","quote_count_under"])
 price=z.pivot(index=keys,columns="side",values="median_price").reset_index().rename(columns={"OVER":"price_over","UNDER":"price_under"}); prob=z.pivot(index=keys,columns="side",values="median_raw_prob").reset_index(); count=z.pivot(index=keys,columns="side",values="quote_count").reset_index().rename(columns={"OVER":"quote_count_over","UNDER":"quote_count_under"}); meta=z.groupby(keys).agg(sportsbooks=("sportsbooks",lambda x:"|".join(sorted(set("|".join(x).split("|"))))),quote_payload_hashes=("quote_payload_hashes",lambda x:"|".join(sorted(set("|".join(x).split("|")))))).reset_index(); out=price.merge(prob,on=keys,how="outer",suffixes=("","_prob")).merge(count,on=keys,how="outer").merge(meta,on=keys)
 over=pd.to_numeric(out.get("OVER"),errors="coerce"); under=pd.to_numeric(out.get("UNDER"),errors="coerce"); denom=over+under; out["p_over_mkt"]=np.where(over.notna()&under.notna()&(denom>0),over/denom,np.where(over.notna(),over,np.where(under.notna(),1-under,np.nan))); out["market_snapshot_identity"]=out.apply(lambda r:digest({k:(None if pd.isna(r.get(k)) else r.get(k)) for k in keys+['sportsbooks','quote_payload_hashes']}),axis=1); return out

def evaluate(predictions:pd.DataFrame,quotes:pd.DataFrame,cfg:dict[str,Any],decision_timestamp_utc:str,manual_actions:pd.DataFrame|None=None)->tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
 if digest({k:v for k,v in cfg.items() if k!="effective_config_hash"})!=cfg["effective_config_hash"]:raise ValueError("policy config hash invalid")
 p=predictions.copy(); required={"canonical_season","slate_date","run_id","prediction_identity","game_id","player_id","prop_type","line","model_version","p_over"}
 if required-set(p):raise ValueError(f"prediction schema missing {sorted(required-set(p))}")
 sides=[]
 for r in p.to_dict("records"):
  for side in ["OVER","UNDER"]:
   x=dict(r); x["base_prediction_identity"]=r["prediction_identity"]; x["prediction_identity"]=digest({"base_prediction_identity":r["prediction_identity"],"side":side}); x["side"]=side;x["model_side_prob_raw"]=float(r["p_over"]) if side=="OVER" else 1-float(r["p_over"]);x["segment"]=f"{side.lower()}:{float(r['line']):.1f}";sides.append(x)
 s=pd.DataFrame(sides); mv=market_view(quotes); s=s.merge(mv,on=["run_id","game_id","player_id","line"],how="left"); s["market_side_prob"]=np.where(s.side.eq("OVER"),s.p_over_mkt,1-s.p_over_mkt);s["price_side"]=np.where(s.side.eq("OVER"),s.get("price_over"),s.get("price_under")); alpha=s.segment.map(lambda x:float(cfg["segment_alpha"].get(x,1)));s["model_side_prob"]=s.market_side_prob+alpha*(s.model_side_prob_raw-s.market_side_prob);s["edge_side"]=s.model_side_prob-s.market_side_prob;s["ev_side"]=s.model_side_prob/s.market_side_prob-1;s["fair_american"]=s.model_side_prob.map(fair)
 ledger=[]; state={i:True for i in s.index}
 def rule(order:int,rid:str,stage:str,passes:pd.Series,input_col:str,value:Any,reason:str):
  for i,row in s.iterrows():
   prior=state[i]; passed=bool(passes.get(i,False)) if prior else False; result="PASS" if prior and passed else ("FAIL" if prior else "NOT_EVALUATED_PRIOR_FAILURE"); state[i]=prior and passed
   ledger.append({"canonical_season":row.canonical_season,"slate_date":row.slate_date,"run_id":row.run_id,"prediction_identity":row.prediction_identity,"game_id":row.game_id,"player_id":row.player_id,"prop_type":row.prop_type,"line":row.line,"side":row.side,"model_version":row.model_version,"market_snapshot_identity":row.get("market_snapshot_identity"),"candidate_policy_name":cfg["policy_name"],"candidate_policy_version":cfg["policy_version"],"candidate_policy_hash":cfg["effective_config_hash"],"rule_id":rid,"rule_order":order,"policy_stage":stage,"input_value":row.get(input_col),"threshold_or_rule_value":canonical(value),"rule_result":result,"failure_reason":reason if result=="FAIL" else "","decision_timestamp_utc":decision_timestamp_utc})
 rule(10,"PREDICTION_IDENTITY","PREDICTION_ELIGIBILITY",s.prediction_identity.notna()&~s.prediction_identity.duplicated(keep=False),"prediction_identity","unique and present","PREDICTION_IDENTITY_INVALID")
 rule(20,"LINE_ALLOWED","LINE_FILTER",pd.to_numeric(s.line,errors="coerce").isin([1.5,2.5,3.5]),"line",[1.5,2.5,3.5],"LINE_FAILURE")
 rule(30,"SIDE_ALLOWED","SIDE_FILTER",s.side.isin(["OVER","UNDER"]),"side",["OVER","UNDER"],"SIDE_FAILURE")
 rule(40,"MARKET_QUOTE_AVAILABLE","MARKET_AVAILABILITY",s.market_side_prob.between(0,1,inclusive="neither"),"market_side_prob",cfg["market_view"],"MISSING_OR_AMBIGUOUS_QUOTE")
 seg=s.segment.map(cfg["policy_segments"]); rule(50,"SEGMENT_POLICY_AVAILABLE","PREDICTION_ELIGIBILITY",seg.notna(),"segment",sorted(cfg["policy_segments"]),"SEGMENT_POLICY_MISSING")
 rule(60,"SEGMENT_ENABLED","SIDE_FILTER",~s.segment.isin(cfg["segment_disable"]),"segment",cfg["segment_disable"],"SEGMENT_DISABLED")
 wilson=s.segment.map(lambda x:(cfg["policy_segments"].get(x)or{}).get("train_wilson_lb")); rule(70,"TRAIN_WILSON_GATE","CONFIDENCE_FILTER",pd.to_numeric(wilson,errors="coerce").fillna(0)>=float(cfg["min_train_wilson_lb"]),"segment",cfg["min_train_wilson_lb"],"TRAIN_WILSON_FAILURE")
 evthr=s.segment.map(lambda x:max(float((cfg["policy_segments"].get(x)or{}).get("min_ev",math.inf)),float(cfg["segment_min_ev_override"].get(x,-math.inf)),float(cfg["min_ev_floor"]))); rule(80,"EV_GATE","CONFIDENCE_FILTER",s.ev_side>=evthr,"ev_side",evthr.to_dict(),"PREDICTION_EV_FAILURE")
 gapthr=s.segment.map(lambda x:max(float((cfg["policy_segments"].get(x)or{}).get("min_gap",math.inf)),float(cfg["segment_min_gap_override"].get(x,-math.inf)))); floors=np.where(s.market_side_prob>=.5,float(cfg["min_gap_floor_favorite"]),float(cfg["min_gap_floor_dog"])); gapthr=np.maximum(gapthr.astype(float),floors); rule(90,"GAP_GATE","CONFIDENCE_FILTER",s.edge_side>=gapthr,"edge_side",list(map(float,gapthr)),"PREDICTION_GAP_FAILURE")
 rule(100,"FAIR_FAVORITE_CAP","PRICE_FILTER",s.fair_american.map(lambda x:x is not None and (x>0 or x>=int(cfg["max_fair_favorite"]))),"fair_american",cfg["max_fair_favorite"],"FAIR_PRICE_FAILURE")
 minprob=s.segment.map(lambda x:float(cfg["segment_min_model_prob"].get(x,-math.inf)));rule(110,"SEGMENT_MODEL_PROB_GATE","CONFIDENCE_FILTER",s.model_side_prob>=minprob,"model_side_prob",cfg["segment_min_model_prob"],"MODEL_PROB_FAILURE")
 maxprice=s.segment.map(lambda x:float(cfg["segment_max_price"].get(x,math.inf)));rule(120,"SEGMENT_MAX_PRICE_GATE","PRICE_FILTER",pd.to_numeric(s.price_side,errors="coerce")<=maxprice,"price_side",cfg["segment_max_price"],"PRICE_FAILURE")
 passed=s[pd.Series(state)].copy(); passed=passed.sort_values(["ev_side","edge_side","model_side_prob","prediction_identity"],ascending=[False,False,False,True]);passed["pre_cap_rank"]=range(1,len(passed)+1);keep=set(passed.groupby(["slate_date","player_id"],dropna=False).head(int(cfg["max_per_player"])).index) if int(cfg["max_per_player"])>0 else set(passed.index);rule(130,"DUPLICATE_PLAYER_CAP","DUPLICATE_SUPPRESSION",pd.Series(s.index.isin(keep),index=s.index),"player_id",cfg["max_per_player"],"DUPLICATE_COLLISION")
 passed=s[pd.Series(state)].sort_values(["ev_side","edge_side","model_side_prob","prediction_identity"],ascending=[False,False,False,True]);keep=set(passed.groupby(["slate_date","game_id"]).head(int(cfg["max_per_game"])).index) if int(cfg["max_per_game"])>0 else set(passed.index);rule(140,"PER_GAME_CAP","SLATE_CAP",pd.Series(s.index.isin(keep),index=s.index),"game_id",cfg["max_per_game"],"EXCLUDED_BY_GAME_CAP")
 passed=s[pd.Series(state)].sort_values(["ev_side","edge_side","model_side_prob","prediction_identity"],ascending=[False,False,False,True]);keep=set(passed.head(int(cfg["max_per_slate"])).index) if int(cfg["max_per_slate"])>0 else set(passed.index);rule(150,"PER_SLATE_CAP","SLATE_CAP",pd.Series(s.index.isin(keep),index=s.index),"slate_date",cfg["max_per_slate"],"EXCLUDED_BY_SLATE_CAP")
 final=s.copy();final["final_candidate_status"]=["FINAL_CANDIDATE" if state[i] else "EXCLUDED" for i in s.index];final["candidate_policy_hash"]=cfg["effective_config_hash"];final["manual_override_status"]="NO_MANUAL_ACTION";final["candidate_rank"]=final.sort_values(["ev_side","edge_side","model_side_prob","prediction_identity"],ascending=[False,False,False,True]).groupby("slate_date").cumcount()+1
 manual=manual_actions.copy() if manual_actions is not None else pd.DataFrame(columns=["prediction_identity","side","manual_override_status","operator_id","reason","timestamp_utc"])
 allowed={"MANUAL_ADD","MANUAL_REMOVE","MANUAL_PRICE_OVERRIDE","MANUAL_LINE_OVERRIDE","MANUAL_SIDE_OVERRIDE"}
 if len(manual) and not set(manual.manual_override_status).issubset(allowed):raise ValueError("invalid manual action")
 return final,pd.DataFrame(ledger),manual
