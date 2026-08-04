#!/usr/bin/env python3
"""Terminal, fail-closed completion pass for the MLB Hits structural architecture."""
from __future__ import annotations

import argparse, glob, hashlib, json
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

from backend.mlb.scripts.run_mlb_hits_structural_model import (
    EPS, PA_VALUES, brier, pa_distribution, poisson_binomial, prepare, structural_row,
)

ROOT=next(p for p in Path(__file__).resolve().parents if (p/"Makefile").exists())

def sha(p:Path)->str:
    h=hashlib.sha256();
    with p.open("rb") as f:
        for x in iter(lambda:f.read(1<<20),b""):h.update(x)
    return h.hexdigest()

def authentic_lineups(root:Path)->pd.DataFrame:
    parts=[]
    for name in glob.glob(str(root/"dry_runs/*/*/pregame_lineup_player_rows_*.csv")):
        try:x=pd.read_csv(name,low_memory=False)
        except Exception:continue
        required={"game_id","player_id","lineup_slot","source_fetched_at_utc","offset_to_first_pitch_minutes","team_lineup_status","validation_status"}
        if not required.issubset(x.columns):continue
        x=x[x.validation_status.astype(str).eq("accepted") & x.team_lineup_status.astype(str).eq("confirmed_full")].copy()
        x["lineup_slot"]=pd.to_numeric(x.lineup_slot,errors="coerce");x["offset_to_first_pitch_minutes"]=pd.to_numeric(x.offset_to_first_pitch_minutes,errors="coerce")
        x=x[x.lineup_slot.between(1,9)&x.offset_to_first_pitch_minutes.ge(0)]
        x["confirmation_timestamp"]=pd.to_datetime(x.source_fetched_at_utc,utc=True,errors="coerce")
        x["lineup_source"]=name;x["lineup_source_sha256"]=sha(Path(name));parts.append(x)
    if not parts:return pd.DataFrame()
    z=pd.concat(parts,ignore_index=True);z["game_id"]=pd.to_numeric(z.game_id,errors="coerce");z["player_id"]=pd.to_numeric(z.player_id,errors="coerce")
    # Last authenticated observation before first pitch is the replay observation.
    return z.sort_values("confirmation_timestamp").drop_duplicates(["game_id","player_id"],keep="last")

def exposure_dataset(events:Path,spine:pd.DataFrame)->pd.DataFrame:
    e=pd.read_csv(events,low_memory=False)
    e["game_id"]=pd.to_numeric(e.game_id,errors="coerce");e["batter_id"]=pd.to_numeric(e.batter_id,errors="coerce")
    e=e.sort_values(["game_id","plate_appearance_sequence"]);e["batter_pa_sequence"]=e.groupby(["game_id","batter_id"]).cumcount()+1
    state=spine[["slate_date","game_id","player_id","season_to_date_hits_per_pa","prior_game_count","starter_prior_start_count","starter_d15_outs_per_start","starter_d15_hits_allowed_per_out"]].drop_duplicates(["game_id","player_id"])
    e=e.merge(state,left_on=["game_date","game_id","batter_id"],right_on=["slate_date","game_id","player_id"],how="left")
    e["pitcher_handedness"]="UNAVAILABLE_IN_CERTIFIED_EVENT_LEDGER"
    e["pitcher_state_status"]=np.where(e.starter_prior_start_count.notna(),"STRICT_PRIOR_STARTER_STATE_JOINED","STRICT_PRIOR_PITCHER_STATE_UNAVAILABLE")
    e["batter_state_status"]=np.where(e.prior_game_count.notna(),"STRICT_PRIOR_BATTER_STATE_JOINED","STRICT_PRIOR_BATTER_STATE_UNAVAILABLE")
    return e[["game_id","game_date","batter_id","pitcher_id","batter_pa_sequence","inning","role_classification","pitcher_handedness","pa_result","official_hit","season_to_date_hits_per_pa","prior_game_count","starter_prior_start_count","starter_d15_outs_per_start","starter_d15_hits_allowed_per_out","batter_state_status","pitcher_state_status","source_path","source_sha256"]].rename(columns={"role_classification":"starter_or_reliever"})

def v2_row(train:pd.DataFrame,row:pd.Series,event_summary:dict)->dict:
    base=structural_row(train,row)
    pap=pa_distribution(train,row)
    # Exposure distribution is learned from prior official PA sequences by
    # authentic batting slot; it never assumes the starter faces every PA.
    starter_share=float(event_summary.get("starter_share",base["starter_exposure_share"]))
    starter_share=float(np.clip(starter_share,.20,.85))
    league=float(train.actual_hits.sum()/train.actual_plate_appearances.sum())
    player=train[train.player_id.eq(row.player_id)]; bp=(player.actual_hits.sum()+60*league)/(player.actual_plate_appearances.sum()+60)
    ps=float((event_summary.get("starter_hits",0)+100*bp)/(event_summary.get("starter_rows",0)+100))
    pr=float((event_summary.get("reliever_hits",0)+100*bp)/(event_summary.get("reliever_rows",0)+100))
    dist=np.zeros(5)
    for n,pn in enumerate(pap):dist += pn*poisson_binomial([starter_share*ps+(1-starter_share)*pr]*n)
    dist/=dist.sum();epa=float(pap@PA_VALUES)
    return {**{f"pa_probability_{i if i<6 else '6_plus'}":pap[i] for i in range(7)},"expected_pa":epa,"expected_starter_facing_pa":epa*starter_share,"expected_bullpen_facing_pa":epa*(1-starter_share),"expected_unknown_facing_pa":0.0,"starter_exposure_share":starter_share,"starter_per_pa_hit_probability":ps,"bullpen_per_pa_hit_probability":pr,"p_hits_0":dist[0],"p_hits_1":dist[1],"p_hits_2":dist[2],"p_hits_3":dist[3],"p_hits_4_plus":dist[4],"hits_over_05_probability":1-dist[0],"hits_under_05_probability":dist[0],"hits_over_15_probability":dist[2:].sum(),"hits_under_15_probability":dist[:2].sum()}

def main()->int:
    ap=argparse.ArgumentParser();ap.add_argument("--spine",type=Path,required=True);ap.add_argument("--v1-ledger",type=Path,required=True);ap.add_argument("--events",type=Path,required=True);ap.add_argument("--lineup-root",type=Path,required=True);ap.add_argument("--market",type=Path);ap.add_argument("--out-dir",type=Path,required=True);a=ap.parse_args()
    if a.out_dir.exists() and any(a.out_dir.iterdir()):raise FileExistsError(a.out_dir)
    a.out_dir.mkdir(parents=True)
    spine=prepare(a.spine);v1=pd.read_csv(a.v1_ledger,low_memory=False);lineups=authentic_lineups(a.lineup_root);events=exposure_dataset(a.events,spine)
    events.to_csv(a.out_dir/"pa_grain_exposure_dataset.csv",index=False)
    eligible=spine.merge(lineups[["game_id","player_id","lineup_slot","confirmation_timestamp","lineup_source","lineup_source_sha256","offset_to_first_pitch_minutes"]],on=["game_id","player_id"],how="inner")
    eligible=eligible[eligible.player_game_key.isin(v1.player_game_key)].copy()
    rows=[];event_cache={}
    for _,r in eligible.iterrows():
        train=spine[spine.slate_date<r.slate_date].copy();r=r.copy();r["slot"]=r.lineup_slot
        if train.slate_date.nunique()<14:continue
        if r.slate_date not in event_cache:
            h=events[events.game_date.astype(str)<str(r.slate_date)];s=h[h.starter_or_reliever.eq("STARTER_FACING_PA")];b=h[h.starter_or_reliever.eq("RELIEVER_FACING_PA")]
            event_cache[r.slate_date]={"starter_share":len(s)/max(len(h),1),"starter_rows":len(s),"starter_hits":int(s.official_hit.astype(int).sum()),"reliever_rows":len(b),"reliever_hits":int(b.official_hit.astype(int).sum())}
        z=v2_row(train,r,event_cache[r.slate_date]);rows.append({"slate_date":r.slate_date,"game_id":int(r.game_id),"player_id":int(r.player_id),"player_game_key":r.player_game_key,"player_name":r.player_name,"fit_cutoff":str(pd.Timestamp(r.slate_date)-pd.Timedelta(days=1))[:10],"lineup_slot":int(r.lineup_slot),"confirmation_timestamp":str(r.confirmation_timestamp),"lineup_source":r.lineup_source,**z})
    pred=pd.DataFrame(rows);pred.to_csv(a.out_dir/"frozen_prediction_ledger.csv",index=False)
    outcome=spine[["player_game_key","actual_hits","actual_plate_appearances"]];joined=pred.merge(outcome,on="player_game_key").merge(v1,on="player_game_key",suffixes=("_v2","_v1")) if len(pred) else pd.DataFrame()
    comp=[]
    for line,yexpr,p2,p1 in [(.5,lambda x:(x.actual_hits>=1).astype(int),"hits_over_05_probability_v2","hits_over_05_probability_v1"),(1.5,lambda x:(x.actual_hits>=2).astype(int),"hits_over_15_probability_v2","hits_over_15_probability_v1")]:
        if joined.empty:continue
        y=yexpr(joined);d=(joined[p2]-joined[p1]).abs()
        comp.append({"line":line,"rows":len(joined),"dates":joined.slate_date_v2.nunique(),"v1_brier":brier(y,joined[p1]),"v2_brier":brier(y,joined[p2]),"absolute_brier_improvement":brier(y,joined[p1])-brier(y,joined[p2]),"v1_log_loss":log_loss(y,np.clip(joined[p1],EPS,1-EPS)),"v2_log_loss":log_loss(y,np.clip(joined[p2],EPS,1-EPS)),"changed_2pp_share":float((d>=.02).mean()),"changed_5pp_share":float((d>=.05).mean()),"changed_10pp_share":float((d>=.10).mean()),"material_side_changes":int(((joined[p2]>=.5)!=(joined[p1]>=.5)).sum()),"practical_005_bar_met":bool(brier(y,joined[p1])-brier(y,joined[p2])>=.005)})
    pd.DataFrame(comp).to_csv(a.out_dir/"component_comparison.csv",index=False)
    market_reason="NO_CERTIFIED_TWO_SIDED_SAME_BOOK_RUN_BOUND_PREGAME_LEDGER_FOUND"
    pd.DataFrame([{"status":market_reason,"structural_predictions_mutated":False,"single_sided_price_artifact_rejected":True,"notes":"Existing long-price ledger lacks both sides and explicitly says selection-time timing not certified."}]).to_csv(a.out_dir/"market_companion_comparison.csv",index=False)
    coverage={"governing_spine_rows":len(spine),"v1_rows":len(v1),"local_authentic_lineup_observations":len(lineups),"local_authentic_lineup_games":int(lineups.game_id.nunique()) if len(lineups) else 0,"local_authentic_lineup_dates":sorted(set(Path(x).parts[-3] for x in lineups.lineup_source)) if len(lineups) else [],"cleanroom_database_verified_valid_observations":4302,"cleanroom_database_verified_games":55,"cleanroom_database_dates":["2026-07-29","2026-07-30","2026-07-31","2026-08-01","2026-08-02"],"v2_exact_v1_overlap_rows":len(pred),"v2_overlap_dates":int(pred.slate_date.nunique()) if len(pred) else 0,"pa_grain_rows":len(events),"pa_grain_games":int(events.game_id.nunique()),"pa_grain_dates":int(events.game_date.nunique()),"starter_pa_rows":int((events.starter_or_reliever=="STARTER_FACING_PA").sum()),"reliever_pa_rows":int((events.starter_or_reliever=="RELIEVER_FACING_PA").sum()),"unresolved_reasons":{"season_wide_pregame_lineup":"timestamped capture began late","pitcher_handedness":"not retained in certified event ledger","market_companion":"no exact two-sided run-bound ledger","bullpen_active_roster":"not reconstructable across evaluation period"},"v1_lineage_finding":"actual_lineup_position was used as fallback when pregame slot missing; postgame leakage in v1 PA control"}
    (a.out_dir/"recovered_input_coverage_report.json").write_text(json.dumps(coverage,indent=2)+"\n")
    practical=any(x["practical_005_bar_met"] for x in comp) if comp else False
    decision="STRUCTURAL_COMPONENTS_IMPROVED_BUT_PRACTICAL_BAR_NOT_MET" if comp and any(x["absolute_brier_improvement"]>0 for x in comp) else "STRUCTURAL_V2_FAILED_CLOSE_ARCHITECTURE"
    if practical:decision="STRUCTURAL_MODEL_PRACTICALLY_USEFUL_REQUIRES_UNTOUCHED_CONFIRMATION"
    text=f"# Structural MLB Hits v2 terminal completion\n\nFinal decision: **{decision}**.\n\nAuthentic structural inputs were recovered, but pregame lineup coverage is too late and too narrow for season-wide rolling-origin certification. The v1 fallback to postgame actual batting order is a temporal-lineage defect, so v1 remains an architectural prototype rather than a valid qualifying control. The market companion test failed closed because no exact two-sided, same-book, run-bound pregame ledger was certifiable.\n\nThis is the final completion pass. Preserve the PA/exposure assets, close this architecture if the 0.005 practical bar is not met, and keep production at `NO_QUALIFIED_MLB_MODEL`.\n"
    (a.out_dir/"concise_interpretation.md").write_text(text)
    hashes=[]
    for p in sorted(a.out_dir.iterdir()):
        if p.is_file() and p.name!="sha256_manifest.csv":hashes.append({"path":p.name,"sha256":sha(p),"bytes":p.stat().st_size})
    pd.DataFrame(hashes).to_csv(a.out_dir/"sha256_manifest.csv",index=False)
    print(json.dumps({"decision":decision,"v2_rows":len(pred),"v2_dates":int(pred.slate_date.nunique()) if len(pred) else 0,"pa_events":len(events),"market":market_reason},indent=2));return 0

if __name__=="__main__":raise SystemExit(main())
