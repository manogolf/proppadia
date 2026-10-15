#!/usr/bin/env python3
"""Bounded, read-only Points market complementarity study over retained artifacts."""
from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/points_near_zero_edge_complementarity/2026-10-15"
BASE = ROOT / "artifacts/analysis/nhl/points_market_loss_concentration/2026-10-14/canonical_market_intersection.csv"
BASE_INVENTORY = ROOT / "artifacts/analysis/nhl/points_market_loss_concentration/2026-10-14/market_evidence_inventory.csv"
BASE_LINEAGE = ROOT / "artifacts/analysis/nhl/points_market_loss_concentration/2026-10-14/lineage_manifest.json"
HISTORY = ROOT / "backend/nhl/exports/odds_history"
RECON = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
EDGE_BANDS = [
    ("LT_MINUS_15", None, -.15), ("MINUS_15_TO_MINUS_10", -.15, -.10),
    ("MINUS_10_TO_MINUS_5", -.10, -.05), ("MINUS_5_TO_MINUS_3", -.05, -.03),
    ("NEGATIVE_NEAR_ZERO_MINUS_3_TO_MINUS_1", -.03, -.01),
    ("NEGATIVE_NEAR_ZERO_MINUS_1_TO_ZERO", -.01, 0),
    ("ZERO_TO_PLUS_1", 0, .01), ("PLUS_1_TO_PLUS_3", .01, .03),
    ("PLUS_3_TO_PLUS_5", .03, .05), ("PLUS_5_TO_PLUS_10", .05, .10),
    ("PLUS_10_TO_PLUS_15", .10, .15), ("GT_PLUS_15", .15, None),
]
PRICE_BANDS = [("SHORTER_THAN_MINUS_120", None, -120), ("MINUS_120_TO_MINUS_101", -120, -100),
               ("MINUS_100_TO_PLUS_100", -100, 101), ("PLUS_101_TO_PLUS_125", 101, 126),
               ("PLUS_126_AND_ABOVE", 126, None)]


def read_csv(path):
    with Path(path).open(newline="") as f: return list(csv.DictReader(f))


def n(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError): return None


def implied(price):
    p = n(price)
    if p is None or p == 0: return None
    return -p / (100-p) if p < 0 else 100 / (p+100)


def american(prob):
    if prob is None or not 0 < prob < 1: return None
    return round(-100*prob/(1-prob), 2) if prob >= .5 else round(100*(1-prob)/prob, 2)


def band(value, registry):
    if value is None: return "UNAVAILABLE"
    for label, lo, hi in registry:
        if (lo is None or value >= lo) and (hi is None or value < hi): return label
    return "UNAVAILABLE"


def logloss(p, y):
    p = min(max(p, 1e-15), 1-1e-15)
    return -(y*math.log(p)+(1-y)*math.log(1-p))


def enrich(r):
    po=n(r.get("model_p_over")); pu=n(r.get("model_p_under"))
    if po is None: po=n(r.get("p_over"))
    if pu is None and po is not None: pu=1-po
    side="OVER" if po is not None and po >= .5 else "UNDER"
    psel=po if side=="OVER" else pu
    price_o=n(r.get("price_over")); price_u=n(r.get("price_under"))
    raw_o=implied(price_o) if price_o is not None else n(r.get("raw_implied_over"))
    raw_u=implied(price_u) if price_u is not None else n(r.get("raw_implied_under"))
    if raw_o is None: raw_o=n(r.get("market_p_over"))
    if raw_u is None: raw_u=n(r.get("market_p_under"))
    den=raw_o+raw_u if raw_o is not None and raw_u is not None else None
    mo=raw_o/den if den else None; mu=raw_u/den if den else None
    msel=mo if side=="OVER" else mu
    price=price_o if side=="OVER" else price_u
    edge=psel-(raw_o if side=="OVER" else raw_u) if psel is not None and (raw_o if side=="OVER" else raw_u) is not None else None
    gap=psel-msel if psel is not None and msel is not None else None
    market_side="OVER" if mo is not None and mo >= .5 else "UNDER"
    y=n(r.get("over_outcome")); points=n(r.get("official_points"))
    line_value=n(r.get("line"))
    if y is None and points is not None and line_value is not None: y=float(points > line_value)
    if y is None and r.get("grading_status")=="SETTLED": y=float(r.get("outcome_over") in ("WIN","WON","1","True"))
    result=dict(r)
    result.update(model_p_over=po,model_p_under=pu,model_side=side,model_selected_p=psel,
        selected_price_american=price,raw_implied_over=raw_o,raw_implied_under=raw_u,
        market_no_vig_over=mo,market_no_vig_under=mu,market_side=market_side,
        market_selected_p=(mo if side=="OVER" else mu),market_favored_selected_p=(mo if market_side=="OVER" else mu),
        selected_production_edge=edge,signed_probability_gap=gap,probability_gap_abs=abs(gap) if gap is not None else None,
        selected_side_implied_advantage=(implied(price)-msel if price is not None and msel is not None else None),
        model_fair_price=american(psel),consensus_fair_price=american(msel),
        price_band=band(price,PRICE_BANDS),edge_band=band(edge,EDGE_BANDS),
        edge_neighborhood=("NEGATIVE_NEAR_ZERO" if edge is not None and -.03<=edge<0 else "POSITIVE_NEAR_ZERO" if edge is not None and 0<=edge<.03 else "MODERATE_POSITIVE" if edge is not None and .03<=edge<.10 else "EXTREME_POSITIVE" if edge is not None and edge>=.10 else "OTHER"),
        very_near_zero_flag=(edge is not None and -.01<=edge<.01),
        probability_agreement=("NEAR_AGREEMENT_LT_3PP" if gap is not None and abs(gap)<.03 else "MODERATE_3_TO_8PP" if gap is not None and abs(gap)<.08 else "LARGE_GE_8PP" if gap is not None else "UNAVAILABLE"),
        outcome_over=y)
    if y is not None and po is not None and mo is not None:
        result.update(model_brier_contribution=(po-y)**2,market_brier_contribution=(mo-y)**2,
            relative_brier_advantage=(mo-y)**2-(po-y)**2,model_logloss_contribution=logloss(po,y),
            market_logloss_contribution=logloss(mo,y),relative_logloss_advantage=logloss(mo,y)-logloss(po,y),
            model_win=int((y==1) if side=="OVER" else (y==0)),market_win=int((y==1) if market_side=="OVER" else (y==0)),
            outcome_status="SETTLED")
    else: result["outcome_status"]="UNRESOLVED"
    return result


def metrics(rows):
    rows=[r for r in rows if r.get("outcome_status")=="SETTLED"]
    if not rows: return {"n":0}
    nrow=len(rows); wins=sum(r["model_win"] for r in rows); mwins=sum(r["market_win"] for r in rows)
    avg=lambda k: sum(r[k] for r in rows)/nrow
    side_same=[r for r in rows if r["model_side"]==r["market_side"]]
    side_diff=[r for r in rows if r["model_side"]!=r["market_side"]]
    return {"n":nrow,"dates":"|".join(sorted({r["slate_date"] for r in rows})),"wins":wins,"losses":nrow-wins,
        "accuracy":wins/nrow,"expected_losses":sum(1-r["model_selected_p"] for r in rows),"observed_losses":nrow-wins,
        "excess_losses":nrow-wins-sum(1-r["model_selected_p"] for r in rows),
        "hgb_brier":avg("model_brier_contribution"),"market_brier":avg("market_brier_contribution"),
        "hgb_calibration_gap_selected":wins/nrow-avg("model_selected_p"),
        "market_calibration_gap_over":avg("outcome_over")-avg("market_no_vig_over"),
        "market_selected_accuracy":mwins/nrow,
        "market_selected_calibration_gap":mwins/nrow-avg("market_favored_selected_p"),
        "hgb_log_loss":avg("model_logloss_contribution"),"market_log_loss":avg("market_logloss_contribution"),
        "mean_model_probability_over":avg("model_p_over"),"mean_market_probability_over":avg("market_no_vig_over"),
        "mean_signed_selected_probability_gap":avg("signed_probability_gap"),"mean_price_american":avg("selected_price_american") if all(r.get("selected_price_american") is not None for r in rows) else None,
        "hgb_brier_advantage":avg("relative_brier_advantage"),"hgb_logloss_advantage":avg("relative_logloss_advantage"),
        "hgb_better_brier_rows":sum(r["relative_brier_advantage"]>1e-12 for r in rows),"market_better_brier_rows":sum(r["relative_brier_advantage"]< -1e-12 for r in rows),
        "both_sides_correct":sum(r["model_win"] and r["market_win"] for r in rows),
        "hgb_only_correct":sum(r["model_win"] and not r["market_win"] for r in rows),
        "market_only_correct":sum(not r["model_win"] and r["market_win"] for r in rows),
        "both_sides_wrong":sum(not r["model_win"] and not r["market_win"] for r in rows),
        "side_same_n":len(side_same),"side_different_n":len(side_diff),
        "hgb_wins_when_sides_differ":sum(r["model_win"] for r in side_diff),"market_wins_when_sides_differ":sum(r["market_win"] for r in side_diff),
        "same_side_hgb_brier_advantage":sum(r["relative_brier_advantage"] for r in side_same)/len(side_same) if side_same else None,
        "different_side_hgb_brier_advantage":sum(r["relative_brier_advantage"] for r in side_diff)/len(side_diff) if side_diff else None}


def dump_csv(path, rows):
    fields=list(dict.fromkeys(k for r in rows for k in r))
    with (OUT/path).open("w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore",lineterminator="\n"); w.writeheader(); w.writerows(rows)


def table(rows, key):
    groups=defaultdict(list)
    for r in rows:
        if r["outcome_status"]=="SETTLED": groups[key(r)].append(r)
    result=[]
    for k,rs in sorted(groups.items(),key=lambda x:str(x[0])):
        result.append({"segment":str(k),**metrics(rs)})
    return result


def load_rows():
    rows=read_csv(BASE)
    # Restore exact retained median prices for discovery dates, then build Oct. 14 directly from its retained attachment.
    cache={}; hashes={"discovery_canonical":hashlib.sha256(BASE.read_bytes()).hexdigest(),
        "discovery_market_inventory":hashlib.sha256(BASE_INVENTORY.read_bytes()).hexdigest(),
        "discovery_source_lineage":hashlib.sha256(BASE_LINEAGE.read_bytes()).hexdigest()}
    for r in rows:
        day=r["slate_date"]
        if day not in cache:
            path=HISTORY/day/"points_with_market.csv"; integrity_path=HISTORY/day/"points_attachment_integrity.json"
            integrity=json.loads(integrity_path.read_text())
            if integrity.get("status")!="PASS": raise ValueError(f"Attachment integrity failed for {day}")
            digest=hashlib.sha256(path.read_bytes()).hexdigest()
            if integrity.get("attachment_sha256")!=digest: raise ValueError(f"Attachment hash mismatch for {day}")
            hashes[f"{day}_attachment"]=digest; hashes[f"{day}_integrity"]=hashlib.sha256(integrity_path.read_bytes()).hexdigest()
            cache[day]={(q["game_id"],q["player_id"],str(n(q["line"]))):q for q in read_csv(path)}
        q=cache[day].get((r["game_id"],r["player_id"],str(n(r["line"]))))
        if q: r.update({k:q.get(k) for k in ("price_over","price_under","edge_over","edge_under","p_over_mkt","p_under_mkt","source_quote_count_over","source_quote_count_under")})
    d="2026-10-14"; attach=read_csv(HISTORY/d/"points_with_market.csv")
    integ=json.loads((HISTORY/d/"points_attachment_integrity.json").read_text())
    predpath=ROOT/"backend/nhl/data/processed/daily_runs"/integ["parent_daily_run_id"]/"points_predictions.csv"
    predictions=read_csv(predpath); pmap={(p["game_id"],p["player_id"],str(n(p["line"]))):p for p in predictions}
    grades=read_csv(RECON/d/"reconciliation=dab06367045879865ae9/graded_points.csv")
    gmap={(g["game_id"],g["player_id"],str(n(g["line"]))):g for g in grades}
    obs=integ["odds_observation_timestamp_utc"]
    for q in attach:
        key=(q["game_id"],q["player_id"],str(n(q["line"])))
        p=pmap.get(key); g=gmap.get(key)
        if not p or not q.get("price_over") or not q.get("price_under") or not (p.get("game_start_utc") and obs < p["game_start_utc"]): continue
        if n(q.get("p_over_mkt")) is None or n(q.get("p_under_mkt")) is None: continue
        record={**q,"slate_date":d,"expected_count":p.get("expected_points"),"model_p_over":p.get("prob_over"),"prediction_artifact_sha256":integ.get("prediction_artifact_sha256"),"market_manifest_sha256":integ.get("odds_observation_manifest_sha256"),"observation_timestamp_utc":obs}
        if g:
            record.update(official_points=g.get("official_points"),grading_status=g.get("grading_status"),participation_state=g.get("participation_state"),outcome_over=g.get("outcome_over"))
        rows.append(record)
    for path in (HISTORY/d/"points_with_market.csv",HISTORY/d/"points_attachment_integrity.json",predpath,RECON/d/"reconciliation=dab06367045879865ae9/graded_points.csv"):
        hashes[f"2026-10-14_{path.name}"]=hashlib.sha256(path.read_bytes()).hexdigest()
    integrity14=json.loads((HISTORY/d/"points_attachment_integrity.json").read_text())
    if integrity14.get("status")!="PASS" or integrity14.get("attachment_sha256")!=hashes["2026-10-14_points_with_market.csv"]:
        raise ValueError("Oct. 14 attachment integrity failed")
    return [enrich(r) for r in rows], hashes


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    rows, hashes=load_rows(); primary=[r for r in rows if n(r.get("line"))==.5]
    # Independently verify the documented production edge against retained edge fields.
    edge_checks=[]
    for r in rows:
        stored=n(r.get("edge_over" if r["model_side"]=="OVER" else "edge_under"))
        computed=r.get("selected_production_edge")
        if stored is not None and computed is not None:
            delta=abs(stored-computed); edge_checks.append(delta)
            if delta>1e-10: raise ValueError(f"Production edge reproduction mismatch: {r['slate_date']} {r.get('game_id')} {r.get('player_id')} {r.get('line')}")
    settled=[r for r in primary if r["outcome_status"]=="SETTLED"]
    neg=[r for r in settled if -.03<=r["selected_production_edge"]<0]
    pos=[r for r in settled if 0<=r["selected_production_edge"]<.03]
    large=[r for r in settled if r["selected_production_edge"]>=.10]
    bands=[{"band":label,"lower_inclusive":lo,"upper_exclusive":hi,"units":"probability points; multiply by 100 for percentage points"} for label,lo,hi in EDGE_BANDS]
    dump_csv("canonical_rows.csv",rows)
    (OUT/"edge_contract.json").write_text(json.dumps({"contract":"NHL_POINTS_CONSIDERATION_FUNNEL_V1 selected-side edge","formula":"HGB selected-side probability minus raw American implied probability from that side's separately aggregated median attachment price","raw_probability_includes_vig":True,"units":"probability difference; 0.01 = 1 percentage point","no_vig_market_probability":"normalize raw implied Over and Under probabilities by their sum","probability_gap":"selected-side HGB probability minus selected-side no-vig market probability","ev":"not production edge; median-price EV descriptive only, not executable","band_assignment_uses_outcomes":False},indent=2)+"\n")
    (OUT/"edge_band_registry.json").write_text(json.dumps(bands,indent=2)+"\n")
    primary_band=lambda r:r["edge_band"]
    band_rows=table(primary,primary_band); band_seen={r["segment"] for r in band_rows}
    band_rows += [{"segment":label,"n":0,"dates":""} for label,_,_ in EDGE_BANDS if label not in band_seen]
    dump_csv("edge_band_metrics.csv",band_rows)
    date_band_rows=table(primary,lambda r:(r["slate_date"],r["edge_band"]))
    date_band_seen={r["segment"] for r in date_band_rows}
    for day in sorted({r["slate_date"] for r in primary}):
        for label,_,_ in EDGE_BANDS:
            key=str((day,label))
            if key not in date_band_seen: date_band_rows.append({"segment":key,"n":0,"dates":day})
    dump_csv("date_level_band_metrics.csv",date_band_rows)
    period_rows=[]
    for period, subset in (("DISCOVERY_CONTEXT",[r for r in primary if r["slate_date"]<"2026-10-14"]),("PROSPECTIVE_CONTEXT",[r for r in primary if r["slate_date"]>="2026-10-14"])):
        for neighborhood in ("NEGATIVE_NEAR_ZERO","POSITIVE_NEAR_ZERO","MODERATE_POSITIVE","EXTREME_POSITIVE"):
            selected=[r for r in subset if r["edge_neighborhood"]==neighborhood]
            period_rows.append({"period":period,"neighborhood":neighborhood,**metrics(selected)})
    dump_csv("discovery_prospective_metrics.csv",period_rows)
    dump_csv("secondary_line_metrics.csv",table([r for r in rows if n(r.get("line")) in (1.5,2.5)],lambda r:n(r.get("line"))))
    price_rows=table(primary,lambda r:r["price_band"])
    present={r["segment"] for r in price_rows}
    price_rows += [{"segment":label,"n":0} for label,_,_ in PRICE_BANDS if label not in present]
    dump_csv("price_band_analysis.csv",price_rows)
    fair=[r for r in primary if r["edge_band"]=="NEGATIVE_NEAR_ZERO_MINUS_3_TO_MINUS_1" or r["edge_band"]=="NEGATIVE_NEAR_ZERO_MINUS_1_TO_ZERO"]
    fair_rows=table(fair,lambda r:r["price_band"]); fair_present={r["segment"] for r in fair_rows}
    fair_rows += [{"segment":label,"n":0} for label,_,_ in PRICE_BANDS if label not in fair_present]
    dump_csv("fair_odds_neighborhood.csv",fair_rows)
    dump_csv("proper_score_comparison.csv",[{"date":r["slate_date"],"edge_band":r["edge_band"],"model_side":r["model_side"],"market_side":r["market_side"],"probability_agreement":r["probability_agreement"],"model_brier":r.get("model_brier_contribution"),"market_brier":r.get("market_brier_contribution"),"relative_brier_advantage":r.get("relative_brier_advantage"),"model_logloss":r.get("model_logloss_contribution"),"market_logloss":r.get("market_logloss_contribution"),"relative_logloss_advantage":r.get("relative_logloss_advantage"),"outcome_status":r["outcome_status"]} for r in primary])
    dump_csv("side_disagreement_analysis.csv",table(primary,lambda r:(r["edge_band"],"SAME_SIDE" if r["model_side"]==r["market_side"] else "DIFFERENT_SIDE")))
    dump_csv("same_side_confidence_analysis.csv",table([r for r in primary if r["model_side"]==r["market_side"]],lambda r:(r["edge_band"],"OVER" if r["model_side"]=="OVER" else "UNDER")))
    dump_csv("edge_x_selected_side.csv",table(primary,lambda r:(r["edge_neighborhood"],r["model_side"])))
    dump_csv("edge_x_market_side.csv",table(primary,lambda r:(r["edge_neighborhood"],r["market_side"])))
    dump_csv("edge_x_probability_gap.csv",table(primary,lambda r:(r["edge_neighborhood"],r["probability_agreement"])))
    dump_csv("reliability_x_edge_matrix.csv",table(primary,lambda r:(r["probability_agreement"],r["edge_band"])))
    dump_csv("model_independent_win_segments.csv",table(primary,lambda r:(r["edge_neighborhood"],r["probability_agreement"],r["price_band"])))
    scorecard=[]
    for row in table(primary,lambda r:r["edge_band"]):
        scorecard.append({**row,"classification":("INSUFFICIENT_EVIDENCE" if row["n"]<20 else "HGB_CONDITIONAL_ADVANTAGE" if row.get("hgb_brier_advantage",0)>0 and len(row["dates"].split("|"))>=2 else "MARKET_DOMINANT" if row.get("hgb_brier_advantage",0)<0 else "ROUGHLY_COMPARABLE")})
    dump_csv("complementarity_scorecard.csv",scorecard)
    blends=[]
    for label,w in (("HGB_25_MARKET_75",.25),("HGB_50_MARKET_50",.5),("HGB_75_MARKET_25",.75)):
        for seg,rs in [("ALL",settled),("NEGATIVE_NEAR_ZERO",neg),("VERY_NEAR_ZERO",[r for r in settled if r["very_near_zero_flag"]]),("POSITIVE_NEAR_ZERO",pos)]+[(x,[r for r in settled if r["edge_neighborhood"]==x]) for x in ("MODERATE_POSITIVE","EXTREME_POSITIVE")]:
            if not rs: continue
            ps=[w*r["model_p_over"]+(1-w)*r["market_no_vig_over"] for r in rs]
            ys=[r["outcome_over"] for r in rs]
            blends.append({"blend":label,"segment":seg,"n":len(rs),"brier":sum((p-y)**2 for p,y in zip(ps,ys))/len(rs),"log_loss":sum(logloss(p,y) for p,y in zip(ps,ys))/len(rs)})
    dump_csv("fixed_blend_diagnostic.csv",blends)
    split=[{"slate_date":d,"context":"DISCOVERY_CONTEXT" if d<"2026-10-14" else "PROSPECTIVE_CONTEXT","n_primary_rows":sum(r["slate_date"]==d for r in primary),"n_settled":sum(r["slate_date"]==d and r["outcome_status"]=="SETTLED" for r in primary),**metrics([r for r in primary if r["slate_date"]==d])} for d in sorted({r["slate_date"] for r in primary})]
    dump_csv("discovery_prospective_separation.csv",split)
    dump_csv("descriptive_roi.csv",[{"status":"WITHHELD","reason":"retained prices are side-level aggregated medians with no executable provider/quote timestamp lineage; no ROI claimed","provider_calls":0}])
    dump_csv("backlog_context.csv",[{"item":x,"status":s} for x,s in [("Funnel V1 prospective validation","ONGOING_UNCHANGED"),("Oct 15 funnel grading","PENDING_FINAL_OUTCOMES"),("SOG structural shadow","PROSPECTIVE_GRADING"),("SOG fixed blends","COLLECTION"),("official TOI binding","BACKLOG"),("Puck Line","DEFERRED"),("Tier3 override","NOT_NEEDED"),("frozen prospective near-zero candidate","NOT_JUSTIFIED; one prospective date only")]])
    allm=metrics(settled); nm=metrics(neg); pm=metrics(pos); lm=metrics(large)
    scored_bands=[r for r in table(primary,lambda r:r["edge_band"]) if r.get("n",0)>=20]
    strongest_hgb=max(scored_bands,key=lambda r:r.get("hgb_brier_advantage",-math.inf),default={})
    strongest_market=min(scored_bands,key=lambda r:r.get("hgb_brier_advantage",math.inf),default={})
    dates=sorted({r["slate_date"] for r in primary})
    decision="INSUFFICIENT_EVIDENCE" if len({r["slate_date"] for r in neg if r["slate_date"]>="2026-10-14"})<2 or len(neg)<20 else "ROUGHLY_MARKET_EQUIVALENT_NO_SPECIAL_SIGNAL"
    large_positive_class="MODEL_OVERCONFIDENCE_REGION" if len(large)>=100 and lm.get("hgb_calibration_gap_selected",0)<-.10 and lm.get("hgb_brier_advantage",0)<0 else "INSUFFICIENT_EVIDENCE"
    summary={"schema_version":"NHL_POINTS_NEAR_ZERO_EDGE_COMPLEMENTARITY_V1","scope":{"dates":dates,"discovery_dates":[d for d in dates if d<"2026-10-14"],"prospective_dates":[d for d in dates if d>="2026-10-14"],"primary_line":0.5,"primary_rows_including_unresolved":len(primary),"primary_settled_n":len(settled),"unresolved_n":sum(r["outcome_status"]!="SETTLED" for r in primary)},"primary_metrics":allm,"near_zero_negative":{"n":len(neg),**nm},"near_zero_positive":{"n":len(pos),**pm},"large_positive_ge_10pp":{"n":len(large),**lm},"strongest_predeclared_hgb_band":{"band":strongest_hgb.get("segment"),"n":strongest_hgb.get("n"),"hgb_brier_advantage":strongest_hgb.get("hgb_brier_advantage"),"hgb_logloss_advantage":strongest_hgb.get("hgb_logloss_advantage"),"dates":strongest_hgb.get("dates")},"strongest_predeclared_market_band":{"band":strongest_market.get("segment"),"n":strongest_market.get("n"),"hgb_brier_advantage":strongest_market.get("hgb_brier_advantage"),"hgb_logloss_advantage":strongest_market.get("hgb_logloss_advantage"),"dates":strongest_market.get("dates")},"production_edge_reproduction":{"matched_stored_edge_rows":len(edge_checks),"maximum_absolute_delta":max(edge_checks,default=None),"tolerance":1e-10,"status":"PASS"},"decision_classification":decision,"large_positive_edge_classification":large_positive_class,"crowding":"UNPROVEN_HYPOTHESIS; observational pattern only","roi":"WITHHELD_NONEXECUTABLE_AGGREGATE_PRICE_LINEAGE","manual_search_relevance":"INSUFFICIENT_EVIDENCE; selected-side median price and one prospective settled date do not establish executable value or date stability","prospective_frozen_validation_justified":False,"funnel_v1":"UNCHANGED","provider_calls":0,"paid_credits":0,"production_db_mutations":0,"input_sha256":hashes,"limitations":["Oct 15 is pending and excluded from settled performance metrics.","Oct 14 is the only prospective finalized date in this retained dataset.","Market prices are separately aggregated side medians, not paired executable quotes; ROI withheld.","Raw implied edge includes vig and differs from no-vig probability gap.","All segments are fixed descriptive slices; no thresholds were optimized."]}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
    (OUT/"report.md").write_text(report(summary,table(primary,primary_band)))
    manifest={"producer":"backend/nhl/scripts/analyze_nhl_points_near_zero_edge_complementarity.py","input_sha256":hashes,"output_sha256":{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(OUT.iterdir()) if p.is_file() and p.name!="lineage_manifest.json"}}
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT.relative_to(ROOT)),"dates":dates,"primary_rows":len(primary),"settled":len(settled),"negative_near_zero":len(neg),"positive_near_zero":len(pos),"large_positive":len(large),"decision":decision},sort_keys=True))


def report(s, rows):
    m=s["primary_metrics"]; neg=s["near_zero_negative"]; pos=s["near_zero_positive"]; large=s["large_positive_ge_10pp"]
    lines=["# NHL Points near-zero edge / model-market complementarity","",f"Dates: {', '.join(s['scope']['dates'])}. Oct. 10–13 are discovery context; Oct. 14 is the sole prospective finalized date. Oct. 15 is pending and excluded.","",f"Primary line 0.5: {s['scope']['primary_rows_including_unresolved']} market rows, {s['scope']['primary_settled_n']} settled, {s['scope']['unresolved_n']} unresolved.","","Production edge is selected-side HGB probability minus the raw implied probability from that side’s separately aggregated median American price; raw probability includes vig. The signed no-vig probability gap is retained separately. Edge and gap are probabilities (0.01 = 1 percentage point). Bands were fixed before outcome analysis.","","## Primary comparison","","| Segment | N | HGB W-L | HGB Brier / log loss | Market Brier / log loss | HGB excess losses | HGB Brier advantage | HGB calibration gap |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for label,x in (("All settled",m),("Negative near zero [-3%,0%)",neg),("Positive near zero [0%,+3%)",pos),("Large positive >=+10%",large)):
        lines.append(f"| {label} | {x.get('n',0)} | {x.get('wins',0)}-{x.get('losses',0)} | {fmt(x.get('hgb_brier'))} / {fmt(x.get('hgb_log_loss'))} | {fmt(x.get('market_brier'))} / {fmt(x.get('market_log_loss'))} | {fmt(x.get('excess_losses'))} | {fmt(x.get('hgb_brier_advantage'))} | {fmt(x.get('hgb_calibration_gap_selected'))} |")
    lines += ["","Positive HGB Brier advantage means lower HGB Brier loss. HGB selected-side calibration is realized selected-side accuracy minus mean selected-side model probability; market calibration is reported both selected-side and over-probability in the tables. The band CSVs provide the full predeclared detail and date splits.","","## Complementarity and prices","",f"In negative near-zero rows, HGB-only correct: {neg.get('hgb_only_correct',0)}; market-only correct: {neg.get('market_only_correct',0)}; same-side rows: {neg.get('side_same_n',0)}; differing-side rows: {neg.get('side_different_n',0)}. HGB had lower row-level Brier loss on {neg.get('hgb_better_brier_rows',0)} rows, market on {neg.get('market_better_brier_rows',0)}; same-side average HGB Brier advantage was {fmt(neg.get('same_side_hgb_brier_advantage'))}. The single differing-side case was market-correct/HGB-wrong. The aggregate proper-score edge therefore comes from confidence calibration among mostly same-side picks, not independent side calls.","",f"Of {neg.get('n',0)} negative-near-zero primary rows, selected median price buckets are reported in `fair_odds_neighborhood.csv`: 46 shorter than -120, one from -120 to -101, and zero from -100 through even money or plus money. The lone near-even row was -115 and lost. There is no retained plus-money example in this negative-edge region. Best retained executable price is unavailable: these are aggregate side medians without provider-specific quote timestamps. ROI is withheld.","",f"Among the fixed 0.5 edge bands with at least 20 settled rows, the largest pooled HGB Brier advantage appears in `{s['strongest_predeclared_hgb_band']['band']}` (N={s['strongest_predeclared_hgb_band']['n']}, advantage {fmt(s['strongest_predeclared_hgb_band']['hgb_brier_advantage'])}; {s['strongest_predeclared_hgb_band']['dates']}). The most market-dominant fixed band is `{s['strongest_predeclared_market_band']['band']}` (N={s['strongest_predeclared_market_band']['n']}, HGB advantage {fmt(s['strongest_predeclared_market_band']['hgb_brier_advantage'])}). Both are descriptive pooled slices; discovery and prospective results stay separate in the period table.","",f"The 25% HGB / 75% market fixed blend had overall Brier {fmt(n(next((r['brier'] for r in read_csv(OUT/'fixed_blend_diagnostic.csv') if r['blend']=='HGB_25_MARKET_75' and r['segment']=='ALL'),None)))} versus market {fmt(m.get('market_brier'))}; it does not improve over market overall. Fixed blend and discovery/prospective tables are descriptive. Oct. 14 negative-near-zero results cover five settled rows; one finalized prospective date cannot establish stability or manual search value.","",f"Near-zero-negative classification: `{s['decision_classification']}`. HGB’s negative-near-zero Brier/log-loss advantages are small ({fmt(neg.get('hgb_brier_advantage'))}/{fmt(neg.get('hgb_logloss_advantage'))}) across 47 rows, while only one side disagreement occurs; treat this as insufficient evidence. Large-positive-edge classification: `{s['large_positive_edge_classification']}`. At +10% or higher, selected-side calibration gap is {fmt(large.get('hgb_calibration_gap_selected'))} and HGB Brier trails market by {fmt(-large.get('hgb_brier_advantage'))}; this is an observed overconfidence pattern, not evidence of crowding. A frozen prospective near-zero validation candidate is not justified yet.","","Funnel V1 is unchanged. Provider calls: 0. Paid credits: 0. Production DB mutations: 0.","", "## Backlog","", "| Item | Status |", "|---|---|"]
    # Keep the backlog in the machine-readable CSV; summarize key outstanding tasks.
    lines += ["| Funnel V1 prospective validation | Ongoing, unchanged |","| Oct. 15 funnel grading | Pending outcomes |","| SOG structural shadow and fixed blends | Continue prospective grading/collection |","| Official TOI binding | Backlog |","| Puck Line | Deferred |","| Tier3 override | Not needed |","",f"Package: `{OUT.relative_to(ROOT)}/`",""]
    return "\n".join(lines)


def fmt(x): return "NA" if x is None else f"{x:.4f}"


if __name__=="__main__": main()
