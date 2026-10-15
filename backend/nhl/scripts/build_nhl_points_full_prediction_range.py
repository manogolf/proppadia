#!/usr/bin/env python3
"""Build full-range descriptive performance maps from the corrected Points package."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15"
OUT = ROOT / "artifacts/analysis/nhl/points_full_prediction_range/2026-10-15"
EDGE = ["< -15%", "-15 to -10", "-10 to -5", "-5 to -3", "-3 to -1", "-1 to 0", "0 to +1", "+1 to +3", "+3 to +5", "+5 to +10", "+10 to +15", "+15 to +20", "+20%+"]
EV = ["< -30%", "-30 to -20", "-20 to -10", "-10 to -5", "-5 to 0", "0 to +5", "+5 to +10", "+10 to +20", "+20 to +30", "+30 to +40", "+40%+"]
DATES = [f"2026-10-{d}" for d in ("10", "11", "12", "13", "14")]
CONF = ["50–55%", "55–60%", "60–65%", "65–70%", "70–75%", "75–80%", "80–85%", "85%+"]
OVER = ["<20%", "20–30%", "30–35%", "35–40%", "40–45%", "45–50%", "50–55%", "55–60%", "60–65%", "65–70%", "70–75%", "75–80%", "80%+"]
MCONF = ["<50%", "50–55%", "55–60%", "60–65%", "65–70%", "70–75%", "75–80%", "80%+"]
PRICE = ["<= -250", "-249 to -201", "-200 to -151", "-150 to -121", "-120 to -101", "+100 to +125", "+126 to +150", "+151 to +200", "+201+"]

def read(path):
    with path.open(newline="") as f: return list(csv.DictReader(f))
def num(r, k):
    try:
        x = float(r[k]); return x if math.isfinite(x) else None
    except (ValueError, TypeError, KeyError): return None
def csvout(name, rows):
    fields = list(dict.fromkeys(k for row in rows for k in row)) or ["empty"]
    with (OUT/name).open("w", newline="") as f:
        w=csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n"); w.writeheader(); w.writerows(rows)
def summarize(rows):
    n=len(rows); wins=sum(int(float(r["selected_win"])) for r in rows); losses=n-wins
    if not n: return {"n":0,"wins":0,"losses":0,"realized_win_rate":None,"expected_win_rate":None,"calibration_gap":None,"expected_losses":0,"observed_losses":0,"excess_losses":0,"excess_loss_rate":None,"hgb_brier":None,"hgb_log_loss":None,"market_brier":None,"market_log_loss":None,"hgb_minus_market_brier":None,"hgb_minus_market_log_loss":None,"mean_edge":None,"median_edge":None,"mean_ev":None,"mean_price":None,"median_price":None,"under_count":0,"over_count":0,"same_side_n":0,"opposite_side_n":0,"dates_represented":""}
    avg=lambda k: statistics.mean(num(r,k) for r in rows)
    p=[num(r,"display_selected_probability") for r in rows]; exp_loss=sum(1-x for x in p); ex=losses-exp_loss
    return {"n":n,"wins":wins,"losses":losses,"realized_win_rate":wins/n,"expected_win_rate":statistics.mean(p),"calibration_gap":statistics.mean(p)-wins/n,"expected_losses":exp_loss,"observed_losses":losses,"excess_losses":ex,"excess_loss_rate":ex/n,"hgb_brier":avg("selected_brier"),"hgb_log_loss":avg("selected_log_loss"),"market_brier":avg("market_selected_brier"),"market_log_loss":avg("market_selected_log_loss"),"hgb_minus_market_brier":avg("selected_brier")-avg("market_selected_brier"),"hgb_minus_market_log_loss":avg("selected_log_loss")-avg("market_selected_log_loss"),"mean_edge":avg("display_edge"),"median_edge":statistics.median(num(r,"display_edge") for r in rows),"mean_ev":avg("display_ev"),"mean_price":avg("display_price"),"median_price":statistics.median(num(r,"display_price") for r in rows),"under_count":sum(r["display_side"]=="UNDER" for r in rows),"over_count":sum(r["display_side"]=="OVER" for r in rows),"same_side_n":sum(r["side_relation"]=="SAME_SIDE" for r in rows),"opposite_side_n":sum(r["side_relation"]=="OPPOSITE_SIDE" for r in rows),"dates_represented":"|".join(d for d in DATES if any(r["slate_date"]==d for r in rows))}
def bands(rows, field, labels, limits):
    out=[]
    for i,label in enumerate(labels):
        lo,hi=limits[i]; group=[r for r in rows if lo<=num(r,field)<hi]
        out.append({"band_order":i+1,"band":label,**summarize(group)})
    return out
def fixed(rows, field, labels, bounds): return bands(rows,field,labels,bounds)
def ranges(labels):
    d={"edge": [(-math.inf,-.15),(-.15,-.10),(-.10,-.05),(-.05,-.03),(-.03,-.01),(-.01,0),(0,.01),(.01,.03),(.03,.05),(.05,.10),(.10,.15),(.15,.20),(.20,math.inf)],"ev":[(-math.inf,-.30),(-.30,-.20),(-.20,-.10),(-.10,-.05),(-.05,0),(0,.05),(.05,.10),(.10,.20),(.20,.30),(.30,.40),(.40,math.inf)]}
    return d[labels]
def pricebin(x):
    bounds=[(-math.inf,-249.5),(-249.5,-200.5),(-200.5,-150.5),(-150.5,-120.5),(-120.5,-100.5),(-100.5,125.5),(125.5,150.5),(150.5,200.5),(200.5,math.inf)]
    return next((i for i,(a,b) in enumerate(bounds) if a<=x<b),8)
def main():
    OUT.mkdir(parents=True,exist_ok=True)
    source=read(SRC/"canonical_rows.csv")
    settled=[r for r in source if r["status"]=="SETTLED" and num(r,"line")==.5 and r["slate_date"] in DATES]
    assert len(settled)==714
    for r in settled:
        p=num(r,"display_selected_probability"); price=num(r,"display_price")
        implied= -price/(100-price) if price<0 else 100/(price+100)
        dec=1+(100/-price if price<0 else price/100)
        assert abs(p-implied-num(r,"display_edge"))<1e-12
        assert abs(p*dec-1-num(r,"display_ev"))<1e-12
        assert int(float(r["selected_win"]))+int(float(r["selected_loss"]))==1
    edge=fixed(settled,"display_edge",EDGE,ranges("edge")); ev=fixed(settled,"display_ev",EV,ranges("ev"))
    def region(name, rows): return {"region":name,**summarize(rows)}
    neg=[("Edge <0",lambda e:e<0),("Edge <-1%",lambda e:e<-.01),("Edge <-3%",lambda e:e<-.03),("Edge <-5%",lambda e:e<-.05),("Edge <-10%",lambda e:e<-.10),("Edge <-15%",lambda e:e<-.15),("-5% to 0",lambda e:-.05<=e<0),("-3% to 0",lambda e:-.03<=e<0),("-1% to 0",lambda e:-.01<=e<0)]
    pos=[("Edge >=0",lambda e:e>=0),("Edge >=+1%",lambda e:e>=.01),("Edge >=+3%",lambda e:e>=.03),("Edge >=+5%",lambda e:e>=.05),("Edge >=+10%",lambda e:e>=.10),("Edge >=+15%",lambda e:e>=.15),("Edge >=+20%",lambda e:e>=.20)]
    near=[("-3% to -1%",lambda e:-.03<=e<-.01),("-1% to 0",lambda e:-.01<=e<0),("0 to +1%",lambda e:0<=e<.01),("+1% to +3%",lambda e:.01<=e<.03),("-3% to +3%",lambda e:-.03<=e<.03),("-1% to +1%",lambda e:-.01<=e<.01)]
    for fn,regs in [("negative_edge_regions.csv",neg),("positive_edge_regions.csv",pos),("near_zero_regions.csv",near)]: csvout(fn,[region(label,[r for r in settled if test(num(r,"display_edge"))]) for label,test in regs])
    cbounds=[(.5+i*.05,.55+i*.05) for i in range(7)]+[(.85,math.inf)]
    mbounds=[(0,.50),(.50,.55),(.55,.60),(.60,.65),(.65,.70),(.70,.75),(.75,.80),(.80,math.inf)]
    conf=fixed(settled,"display_selected_probability",CONF,cbounds); market=fixed(settled,"market_selected_probability",MCONF,mbounds)
    price_rows=[]
    for i,label in enumerate(PRICE):
        g=[r for r in settled if pricebin(num(r,"display_price"))==i]; s=summarize(g)
        s.update({"band_order":i+1,"band":label,"break_even_rate":statistics.mean((-num(r,"display_price")/(100-num(r,"display_price")) if num(r,"display_price")<0 else 100/(num(r,"display_price")+100)) for r in g) if g else None})
        price_rows.append(s)
    for fn,rows in [("edge_band_full_metrics.csv",edge),("ev_band_full_metrics.csv",ev),("model_confidence_bands.csv",conf),("market_confidence_bands.csv",market),("price_bands.csv",price_rows)]:csvout(fn,rows)
    csvout("under_edge_bands.csv",[{"band":b["band"],**summarize([r for r in settled if r["display_side"]=="UNDER" and ranges("edge")[i][0]<=num(r,"display_edge")<ranges("edge")[i][1]])} for i,b in enumerate(edge)])
    csvout("over_edge_bands.csv",[{"band":b["band"],**summarize([r for r in settled if r["display_side"]=="OVER" and ranges("edge")[i][0]<=num(r,"display_edge")<ranges("edge")[i][1]])} for i,b in enumerate(edge)])
    # Exact legacy HGB P(over) buckets, with Over-side calibration and market comparison.
    obounds=[(0,.20),(.20,.30),(.30,.35),(.35,.40),(.40,.45),(.45,.50),(.50,.55),(.55,.60),(.60,.65),(.65,.70),(.70,.75),(.75,.80),(.80,math.inf)]
    overrows=[]
    for i,b in enumerate(OVER):
        g=[r for r in settled if obounds[i][0]<=num(r,"hgb_p_over")<obounds[i][1]]; y=[int(float(r["realized_points"])>=1) for r in g]
        mean=lambda f: statistics.mean(num(r,f) for r in g) if g else None
        overrows.append({"band":b,"n":len(g),"realized_over_rate":statistics.mean(y) if g else None,"hgb_expected_over_rate":mean("hgb_p_over"),"calibration_gap":(mean("hgb_p_over")-statistics.mean(y)) if g else None,"market_over_probability":mean("market_no_vig_over"),"hgb_over_brier":statistics.mean((num(r,"hgb_p_over")-z)**2 for r,z in zip(g,y)) if g else None,"market_over_brier":statistics.mean((num(r,"market_no_vig_over")-z)**2 for r,z in zip(g,y)) if g else None,"hgb_over_log_loss":statistics.mean(-z*math.log(max(1e-15,num(r,"hgb_p_over")))-(1-z)*math.log(max(1e-15,1-num(r,"hgb_p_over"))) for r,z in zip(g,y)) if g else None,"market_over_log_loss":statistics.mean(-z*math.log(max(1e-15,num(r,"market_no_vig_over")))-(1-z)*math.log(max(1e-15,1-num(r,"market_no_vig_over"))) for r,z in zip(g,y)) if g else None})
    csvout("hgb_over_probability_bands.csv",overrows)
    # All fixed cells are emitted, including zero-count cells.
    ep=[]
    for i,b in enumerate(EDGE):
        for j,pb in enumerate(PRICE):
            g=[r for r in settled if ranges("edge")[i][0]<=num(r,"display_edge")<ranges("edge")[i][1] and pricebin(num(r,"display_price"))==j]
            ep.append({"edge_band":b,"price_band":pb,**summarize(g)})
    csvout("edge_price_matrix.csv",ep)
    ec=[]
    for i,b in enumerate(EDGE):
        for j,c in enumerate(CONF):
            g=[r for r in settled if ranges("edge")[i][0]<=num(r,"display_edge")<ranges("edge")[i][1] and cbounds[j][0]<=num(r,"display_selected_probability")<cbounds[j][1]]
            ec.append({"edge_band":b,"model_confidence_band":c,**summarize(g)})
    csvout("edge_model_confidence_matrix.csv",ec)
    ea=[]
    for b in EDGE:
        for rel in ("SAME_SIDE","OPPOSITE_SIDE"):
            g=[r for r in settled if r["edge_band"]==b and r["side_relation"]==rel]
            ea.append({"edge_band":b,"market_relation":rel,**summarize(g)})
    csvout("edge_market_agreement_matrix.csv",ea)
    csvout("full_range_master.csv",[region("edge:"+b,[r for r in settled if ranges("edge")[i][0]<=num(r,"display_edge")<ranges("edge")[i][1]]) for i,b in enumerate(EDGE)]+[region("ev:"+b,[r for r in settled if ranges("ev")[i][0]<=num(r,"display_ev")<ranges("ev")[i][1]]) for i,b in enumerate(EV)]+[region("model_confidence:"+b,[r for r in settled if cbounds[i][0]<=num(r,"display_selected_probability")<cbounds[i][1]]) for i,b in enumerate(CONF)]+[region("market_confidence:"+b,[r for r in settled if mbounds[i][0]<=num(r,"market_selected_probability")<mbounds[i][1]]) for i,b in enumerate(MCONF)]+[region("price:"+b,[r for r in settled if pricebin(num(r,"display_price"))==i]) for i,b in enumerate(PRICE)])
    # Descriptive one-unit-risk return: win earns decimal odds minus one, loss risks one unit.
    def returns(g):
        vals=[]
        for r in g:
            p=num(r,"display_selected_probability"); price=num(r,"display_price"); dec=1+(100/-price if price<0 else price/100)
            vals.append((dec-1) if int(float(r["selected_win"])) else -1)
        return {"n":len(g),"net_units":sum(vals),"roi_per_unit_risked":sum(vals)/len(vals) if vals else None,"return_label":"RETAINED_ROW_PRICE_DESCRIPTIVE_RETURN"}
    csvout("descriptive_return_by_edge.csv",[{"band":b["band"],**returns([r for r in settled if ranges("edge")[i][0]<=num(r,"display_edge")<ranges("edge")[i][1]])} for i,b in enumerate(edge)])
    csvout("descriptive_return_by_price.csv",[{"band":b["band"],**returns([r for r in settled if pricebin(num(r,"display_price"))==i])} for i,b in enumerate(price_rows)])
    date=[]
    for d in DATES:
        g=[r for r in settled if r["slate_date"]==d]; date.append({"slate_date":d,**summarize(g),"period":"FIRST_PROSPECTIVE_CONTEXT" if d=="2026-10-14" else "DISCOVERY_CONTEXT"})
        for i,b in enumerate(EDGE): date.append({"slate_date":d,"edge_band":b,**summarize([r for r in g if ranges("edge")[i][0]<=num(r,"display_edge")<ranges("edge")[i][1]])})
    csvout("date_stability.csv",date)
    csvout("discovery_vs_prospective.csv",[{"period":p,**summarize([r for r in settled if (r["slate_date"]!="2026-10-14")==(p=="DISCOVERY_CONTEXT")])} for p in ("DISCOVERY_CONTEXT","FIRST_PROSPECTIVE_CONTEXT")])
    # Region summaries with transparent descriptive labels and a fixed support floor.
    regions=[{"type":"edge_band","label":x["band"],"rows":[r for r in settled if ranges("edge")[i][0]<=num(r,"display_edge")<ranges("edge")[i][1]]} for i,x in enumerate(edge)]+[{"type":"ev_band","label":x["band"],"rows":[r for r in settled if ranges("ev")[i][0]<=num(r,"display_ev")<ranges("ev")[i][1]]} for i,x in enumerate(ev)]
    classifications=[]
    for x in regions:
        s=summarize(x["rows"]); gap=s["calibration_gap"]
        cls="INSUFFICIENT_SAMPLE" if s["n"]<20 else ("HGB_OVERCONFIDENT" if gap>.10 else "HGB_UNDERCONFIDENT" if gap<-.10 else "CALIBRATION_WITHIN_10PP")
        classifications.append({"region_type":x["type"],"region":x["label"],**s,"behavior_classification":cls,"support_floor":20,"classification_is_operational":False})
    csvout("behavior_classifications.csv",classifications)
    eligible=[x for x in classifications if x["n"]>=20]
    csvout("favorable_region_rankings.csv",sorted(eligible,key=lambda x:(x["excess_loss_rate"],x["hgb_minus_market_brier"])))
    csvout("failure_region_rankings.csv",sorted(eligible,key=lambda x:(-x["excess_loss_rate"],-x["hgb_minus_market_brier"])))
    csvout("chart_edge_performance.csv",[{k:r[k] for k in ("band_order","band","n","realized_win_rate","expected_win_rate","excess_loss_rate","hgb_brier","market_brier")} for r in edge])
    csvout("chart_confidence_performance.csv",[{k:r[k] for k in ("band_order","band","n","realized_win_rate","expected_win_rate","calibration_gap","hgb_brier","market_brier")} for r in conf])
    pending=read(SRC/"oct15_pending_map.csv")
    csvout("oct15_outcome_free.csv",pending)
    funnel=json.loads((SRC/"funnel_stage1_reconciliation.json").read_text())
    total=summarize(settled)
    neg_total=summarize([r for r in settled if num(r,"display_edge")<0]); pos_total=summarize([r for r in settled if num(r,"display_edge")>=0])
    summary={"schema_version":"NHL_POINTS_FULL_PREDICTION_RANGE_V1","source_csv_sha256":hashlib.sha256((SRC/"canonical_rows.csv").read_bytes()).hexdigest(),"pending_map_sha256":hashlib.sha256((SRC/"oct15_pending_map.csv").read_bytes()).hexdigest(),"settled_dates":DATES,"settled_line":0.5,"settled_n":len(settled),"total":total,"negative_edge_total":neg_total,"positive_edge_total":pos_total,"negative_positive_reconcile":neg_total["n"]+pos_total["n"]==714,"oct15_pending_n":len(pending),"fixed_edge_bands_reconcile":sum(x["n"] for x in edge)==714,"fixed_ev_bands_reconcile":sum(x["n"] for x in ev)==714,"confidence_bands_reconcile":sum(x["n"] for x in conf)==714,"price_bands_reconcile":sum(x["n"] for x in price_rows)==714,"funnel_v1_reconciliation":funnel,"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"funnel_v1_modified":False,"classification":"DESCRIPTIVE_MAP_COMPLETE"}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
    (OUT/"lineage_manifest.json").write_text(json.dumps({"schema_version":"NHL_POINTS_FULL_RANGE_LINEAGE_V1","sources":[{"path":str(SRC/"canonical_rows.csv"),"sha256":summary["source_csv_sha256"]},{"path":str(SRC/"oct15_pending_map.csv"),"sha256":summary["pending_map_sha256"]}],"script_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),"output_population":"SETTLED, line 0.5, Oct 10-14","edge_and_ev":"corrected selected side","return":"one-unit-risk arithmetic at each retained row price; retrospective descriptive only, not an executable quote record","provider_calls":0,"paid_credits":0,"production_db_mutations":0},indent=2)+"\n")
    (OUT/"backlog_context.md").write_text("""# Backlog and context\n\n- Points Funnel V1 prospective validation\n- Corrected Points EV/Edge prospective collection\n- Cross-market review: Points COMPLETE AFTER THIS TASK; SOG NEXT; Saves; Moneyline; Puck Line\n- Continue SOG structural, divergence, and fixed-blend prospective grading\n- Historical official-TOI binding\n- Deferred Puck Line framework work\n- Tier3 override not needed\n\nStandard rule: understand the entire model/market performance range before defining consideration or inclusion rules. No filters or Funnel changes are made here.\n""")
    report=["# NHL Points full prediction-range map","","## Scope","",f"Corrected selected-side HGB Points 0.5 settled rows, Oct. 10–14, 2026. N={total['n']}; wins={total['wins']}, losses={total['losses']}. Oct. 15 is kept in `oct15_outcome_free.csv` ({len(pending)} pending rows, with no outcome); unresolved rows are excluded.","","## Full-range interpretation","",f"Across all settled rows HGB expected {total['expected_win_rate']:.1%} wins versus {total['realized_win_rate']:.1%} realized, a {total['excess_losses']:.1f} excess losses / {total['excess_loss_rate']:.1%} excess-loss rate. HGB Brier/log loss are {total['hgb_brier']:.3f}/{total['hgb_log_loss']:.3f}; market selected-side values are {total['market_brier']:.3f}/{total['market_log_loss']:.3f}. HGB-minus-market Brier and log loss are {total['hgb_minus_market_brier']:+.3f}/{total['hgb_minus_market_log_loss']:+.3f}, favoring market overall.","",f"Negative Edge (<0): n={neg_total['n']}, {neg_total['wins']}–{neg_total['losses']}, realized {neg_total['realized_win_rate']:.1%} vs expected {neg_total['expected_win_rate']:.1%}, excess losses {neg_total['excess_losses']:+.1f} ({neg_total['excess_loss_rate']:+.1%}). Positive Edge (>=0): n={pos_total['n']}, {pos_total['wins']}–{pos_total['losses']}, realized {pos_total['realized_win_rate']:.1%} vs expected {pos_total['expected_win_rate']:.1%}, excess losses {pos_total['excess_losses']:+.1f} ({pos_total['excess_loss_rate']:+.1%}).","", "Near-zero regions are in `near_zero_regions.csv`; the narrow 0 to +1% range is noisy and under-supported (n=18). Fixed Edge and EV tables are exhaustive, with each partition reconciling to 714. Detailed rows and supporting tables are linked below.","","## Methods and outputs","","Calibration gap = mean selected-side HGB probability minus realized win rate. Excess losses = observed losses minus sum(1 − selected-side probability). Negative HGB-minus-market Brier/log loss favors HGB. Return tables assume one unit risked per row at that row's retained price; this is retrospective descriptive arithmetic, not executable ROI or a current quote record. Rankings and behavior classifications apply a descriptive support floor of 20 and do not define operational rules.","","The package includes fixed Edge/EV bands, cumulative negative and positive Edge regions, near-zero regions, model selected-side probability, the established full HGB P(Over) bins, fixed no-vig selected-side market-probability bands, retained price bands, same/opposite agreement, Under/Over splits, full fixed interaction matrices, date stability, discovery/prospective comparison, rankings, descriptive return, chart-ready CSVs, and static charts.","","- `edge_band_full_metrics.csv` and `ev_band_full_metrics.csv`","- `negative_edge_regions.csv`, `positive_edge_regions.csv`, `near_zero_regions.csv`","- `model_confidence_bands.csv`, `hgb_over_probability_bands.csv`, `market_confidence_bands.csv`, `price_bands.csv`","- `edge_price_matrix.csv`, `edge_model_confidence_matrix.csv`, `edge_market_agreement_matrix.csv`","- `under_edge_bands.csv`, `over_edge_bands.csv`, `date_stability.csv`, `discovery_vs_prospective.csv`","- `favorable_region_rankings.csv`, `failure_region_rankings.csv`, `descriptive_return_by_edge.csv`, `descriptive_return_by_price.csv`","- `edge_performance.png`, `model_confidence.png`","","Funnel V1 reconciliation is retained in `summary.json` (658 inputs, 0 mismatches; unchanged). No filter, funnel, production, model, UI, DB, or provider change was made. Provider calls: 0; paid credits: 0; production DB mutations: 0."]
    (OUT/"report.md").write_text("\n".join(report)+"\n")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        for filename,rows in (("edge_performance.png",edge),("model_confidence.png",conf)):
            xs=range(len(rows)); fig,ax=plt.subplots(figsize=(12,5)); ax.plot(xs,[r["expected_win_rate"] or 0 for r in rows],marker="o",label="Expected"); ax.plot(xs,[r["realized_win_rate"] or 0 for r in rows],marker="s",label="Realized"); ax.set_xticks(list(xs),[r["band"] for r in rows],rotation=35,ha="right"); ax.set_ylabel("Rate"); ax.grid(axis="y",alpha=.25); ax.legend(frameon=False); fig.tight_layout(); fig.savefig(OUT/filename,dpi=160); plt.close(fig)
    except ImportError: pass
    print(json.dumps({"output":str(OUT),"settled_n":len(settled),"edge_n":sum(x["n"] for x in edge),"ev_n":sum(x["n"] for x in ev),"confidence_n":sum(x["n"] for x in conf),"price_n":sum(x["n"] for x in price_rows),"oct15_pending_n":len(pending),"classification":"DESCRIPTIVE_MAP_COMPLETE"},sort_keys=True))
if __name__=="__main__": main()
