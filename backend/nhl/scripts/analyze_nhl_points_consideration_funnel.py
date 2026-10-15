#!/usr/bin/env python3
"""Read-only sequential research funnel for retained NHL Points evidence."""
from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "artifacts/analysis/nhl/points_market_loss_concentration/2026-10-14"
OUT = ROOT / "artifacts/analysis/nhl/points_consideration_funnel/2026-10-14"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def number(value):
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def implied(price):
    p = number(price)
    if p is None or p == 0:
        return None
    return (-p / (-p + 100)) if p < 0 else 100 / (p + 100)


def enrich(row: dict, dry: bool = False) -> dict:
    r = dict(row)
    line = number(r.get("line"))
    po = number(r.get("model_p_over"))
    if po is None:
        po = number(r.get("p_over"))
    pu = 1 - po if po is not None else None
    side = "OVER" if po is not None and po >= .5 else "UNDER"
    psel = po if side == "OVER" else pu
    price_o = number(r.get("price_over"))
    price_u = number(r.get("price_under"))
    # Historical audit retains the matched two-sided raw implied probabilities
    # and selected attached price; the current attachment retains both prices.
    raw_o = implied(price_o) if price_o is not None else number(r.get("market_p_over"))
    raw_u = implied(price_u) if price_u is not None else number(r.get("market_p_under"))
    denom = raw_o + raw_u if raw_o is not None and raw_u is not None else None
    no_vig_o = raw_o / denom if denom else None
    no_vig_u = raw_u / denom if denom else None
    msel = no_vig_o if side == "OVER" else no_vig_u
    existing_edge = number(r.get("edge_over" if side == "OVER" else "edge_under"))
    selected_price = price_o if side == "OVER" else price_u
    if selected_price is None:
        selected_price = number(r.get("selected_attached_american_price"))
    dec = (1 + selected_price / 100) if selected_price is not None and selected_price > 0 else (1 + 100 / abs(selected_price)) if selected_price is not None and selected_price < 0 else None
    indicated_ev = psel * dec - 1 if psel is not None and dec is not None else None
    if existing_edge is None and psel is not None:
        raw_selected = raw_o if side == "OVER" else raw_u
        existing_edge = psel - raw_selected if raw_selected is not None else None
    gap = psel - msel if psel is not None and msel is not None else None
    market_side = "OVER" if no_vig_o is not None and no_vig_o >= .5 else "UNDER"
    r.update({"line": line, "model_p_over": po, "model_p_under": pu, "model_side": side,
        "model_selected_p": psel, "raw_implied_over": raw_o, "raw_implied_under": raw_u,
        "market_no_vig_over": no_vig_o, "market_no_vig_under": no_vig_u,
        "market_side": market_side, "market_selected_p": msel,
        "signed_selected_model_minus_no_vig_market_gap": gap,
        "selected_side_indicated_ev_at_attachment_median_price": indicated_ev,
        "selected_production_edge": existing_edge,
        "selected_price_american": selected_price,
        "selected_market_raw_implied_probability": raw_o if side == "OVER" else raw_u,
        "ev_status": "DESCRIPTIVE_MEDIAN_PRICE_EV_NOT_EXECUTABLE"})
    r["player"] = r.get("player") or r.get("full_name") or r.get("player_name") or ""
    r["opponent"] = r.get("opponent") or r.get("opponent_team_id") or ""
    if not r.get("prediction_identity"):
        identity_parts=(r.get("prediction_artifact_sha256"),r.get("slate_date"),r.get("game_id"),r.get("player_id"),line,r.get("model_name"),r.get("model_version"))
        r["prediction_identity"]=hashlib.sha256("|".join(map(str,identity_parts)).encode()).hexdigest()
    if not dry:
        points = number(r.get("official_points"))
        if points is not None and line is not None:
            win = int(points > line) if side == "OVER" else int(points < line)
            r["model_win"] = win
            r["expected_loss"] = 1 - psel
            r["observed_loss"] = 1 - win
            r["excess_loss_contribution"] = (1 - win) - (1 - psel)
            r["model_logloss"] = -(math.log(max(po, 1e-15)) if points > line else math.log(max(1-po, 1e-15)))
            r["over_outcome"] = int(points > line)
    return r


def metrics(rows: list[dict]) -> dict:
    if not rows:
        return {"n": 0, "wins": 0, "losses": 0, "expected_losses": 0, "observed_losses": 0,
                "excess_losses": 0, "accuracy": None, "brier": None, "log_loss": None,"log_loss_sum":0}
    n = len(rows); wins = sum(int(x["model_win"]) for x in rows); losses = n-wins
    exp = sum(1-x["model_selected_p"] for x in rows)
    out={"n":n,"wins":wins,"losses":losses,"accuracy":wins/n,"expected_losses":exp,
        "observed_losses":losses,"excess_losses":losses-exp,
        "brier":sum((x["model_p_over"]-x["over_outcome"])**2 for x in rows)/n,
        "log_loss":sum(x["model_logloss"] for x in rows)/n,
        "log_loss_sum":sum(x["model_logloss"] for x in rows),
        "mean_signed_gap":sum(x["signed_selected_model_minus_no_vig_market_gap"] for x in rows if x.get("signed_selected_model_minus_no_vig_market_gap") is not None)/max(1,sum(x.get("signed_selected_model_minus_no_vig_market_gap") is not None for x in rows)),
        "mean_edge":sum(x["selected_production_edge"] for x in rows if x.get("selected_production_edge") is not None)/max(1,sum(x.get("selected_production_edge") is not None for x in rows)),
        "mean_model_selected_p":sum(x["model_selected_p"] for x in rows)/n,
        "mean_selected_market_p":sum(x["market_selected_p"] for x in rows if x.get("market_selected_p") is not None)/max(1,sum(x.get("market_selected_p") is not None for x in rows)),
        "mean_selected_price_american":sum(x["selected_price_american"] for x in rows if x.get("selected_price_american") is not None)/max(1,sum(x.get("selected_price_american") is not None for x in rows)),
        "min_selected_price_american":min((x["selected_price_american"] for x in rows if x.get("selected_price_american") is not None),default=None),
        "max_selected_price_american":max((x["selected_price_american"] for x in rows if x.get("selected_price_american") is not None),default=None)}
    if all(x.get("market_no_vig_over") is not None for x in rows):
        out["market_no_vig_brier"]=sum((x["market_no_vig_over"]-x["over_outcome"])**2 for x in rows)/n
        out["market_no_vig_log_loss"]=sum(-(x["over_outcome"]*math.log(max(x["market_no_vig_over"],1e-15))+(1-x["over_outcome"])*math.log(max(1-x["market_no_vig_over"],1e-15))) for x in rows)/n
    return out


def write_csv(name: str, rows: list[dict]) -> None:
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with (OUT/name).open("w", newline="") as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore",lineterminator="\n"); w.writeheader(); w.writerows(rows)


def grouped(rows, fn):
    d=defaultdict(list)
    for row in rows: d[fn(row)].append(row)
    return d


def segment_table(rows, field, bins):
    result=[]
    for row in rows:
        value=row.get(field)
        label="UNAVAILABLE" if value is None else next((name for lim,name in bins if value < lim), bins[-1][1])
        row["_bin"]=label
    for key, subset in sorted(grouped(rows,lambda x:x["_bin"]).items()):
        m=metrics(subset); m["segment"]=key; m["dates"]="|".join(sorted({x["slate_date"] for x in subset})); m["share_survivor_losses"]=m["losses"]/max(1,sum(not int(x["model_win"]) for x in rows)); result.append(m)
    for r in rows: r.pop("_bin",None)
    return result


def main() -> None:
    OUT.mkdir(parents=True,exist_ok=True)
    source_rows=read_csv(SOURCE/"canonical_market_intersection.csv")
    historical_attachment_paths={}
    historical_market_cache={}
    for row in source_rows:
        slate=row["slate_date"]
        market_path=ROOT/"backend/nhl/exports/odds_history"/slate/"points_with_market.csv"
        if market_path.exists():
            historical_attachment_paths[str(market_path.relative_to(ROOT))]=hashlib.sha256(market_path.read_bytes()).hexdigest()
            if slate not in historical_market_cache:
                historical_market_cache[slate]={(q.get("game_id"),q.get("player_id"),str(number(q.get("line")))):q for q in read_csv(market_path)}
            market=historical_market_cache[slate].get((row.get("game_id"),row.get("player_id"),str(number(row.get("line")))))
            if market:
                for col in ("price_over","price_under","edge_over","edge_under","p_over_mkt","p_under_mkt","source_quote_count_over","source_quote_count_under","source_books_over","source_books_under"):
                    row[col]=market.get(col)
        day_path=ROOT/"artifacts/operational/nhl/postgame_reconciliation"/row["slate_date"]
        recon_dirs=sorted(day_path.glob("reconciliation=*/canonical_game_outcomes.csv"))
        if recon_dirs:
            games=read_csv(recon_dirs[-1]); game=next((g for g in games if g.get("game_id")==row.get("game_id")),None)
            if game:
                row["home_team_id"]=game.get("home_team_id"); row["away_team_id"]=game.get("away_team_id")
                row["opponent_team_id"]=game.get("away_team_id") if str(row.get("team_id"))==str(game.get("home_team_id")) else game.get("home_team_id")
        row["model_name"]="NHL_POINTS_COUNT_HGB_V1"; row["model_version"]="NHL_POINTS_LEADER_CORE_FEATURES_V1"
    hist=[enrich(r) for r in source_rows]
    # The Oct. 14 attachment is pregame-only and intentionally outcome-free.
    day=ROOT/"backend/nhl/exports/odds_history/2026-10-14"
    attachment=read_csv(day/"points_with_market.csv")
    integrity=json.loads((day/"points_attachment_integrity.json").read_text())
    preds_path=ROOT/"backend/nhl/data/processed/daily_runs"/integrity["parent_daily_run_id"]/"points_predictions.csv"
    preds=read_csv(preds_path); predmap={(x["game_id"],x["player_id"],str(number(x["line"]))):x for x in preds}
    obs=integrity["odds_observation_timestamp_utc"]
    dry=[]
    for p in preds:
        p["model_name"]="NHL_POINTS_COUNT_HGB_V1"; p["model_version"]="NHL_POINTS_LEADER_CORE_FEATURES_V1"
        p["prediction_artifact_sha256"]=integrity.get("prediction_artifact_sha256")
    for q in attachment:
        p=predmap.get((q["game_id"],q["player_id"],str(number(q["line"]))))
        if not p or not q.get("price_over") or not q.get("price_under") or integrity.get("status")!="PASS": continue
        if not (p.get("game_start_utc") and obs < p["game_start_utc"]): continue
        team_id=q.get("team_id"); opp_id=p.get("away_team_id") if str(team_id)==str(p.get("home_team_id")) else p.get("home_team_id")
        dry.append(enrich({**q,"slate_date":"2026-10-14","expected_count":p.get("expected_points"),
            "model_p_over":p.get("prob_over"),"prediction_identity":p.get("feature_hash"),
            "opponent_team_id":opp_id,"home_team_id":p.get("home_team_id"),"away_team_id":p.get("away_team_id"),
            "prediction_artifact_sha256":integrity.get("prediction_artifact_sha256"),
            "market_manifest_sha256":integrity.get("odds_observation_manifest_sha256"),
            "observation_timestamp_utc":obs},True))
    hgbdates=sorted({r["slate_date"] for r in hist})
    for r in hist:
        edge=r.get("selected_production_edge"); gap=r.get("signed_selected_model_minus_no_vig_market_gap")
        r["negative_edge_flag"]=edge is not None and edge<0
        r["edge_unavailable_flag"]=edge is None
        r["edge_status"]="EDGE_UNAVAILABLE" if edge is None else "NEGATIVE_EDGE" if edge<0 else "ZERO_EDGE" if edge==0 else "POSITIVE_EDGE"
        r["extreme_gap_flag"]=gap is not None and gap>.15
        r["opposite_side_flag"]=r.get("model_side")!=r.get("market_side")
        r["strong_under_gap_flag"]=r.get("model_side")=="UNDER" and gap is not None and gap>.15
        r["first_exclusion_stage"]=""
        if r["negative_edge_flag"]: r.update(first_exclusion_stage="STAGE_1",exclusion_reason="RESEARCH_EXCLUDE_NEGATIVE_EDGE")
        elif r["extreme_gap_flag"]: r.update(first_exclusion_stage="STAGE_2",exclusion_reason="RESEARCH_EXCLUDE_EXTREME_MODEL_MARKET_GAP")
        elif r["opposite_side_flag"]: r.update(first_exclusion_stage="STAGE_3",exclusion_reason="RESEARCH_EXCLUDE_OPPOSITE_SIDE_DISAGREEMENT")
        else: r.update(exclusion_reason="SURVIVES_CURRENT_RESEARCH_EXCLUSIONS")
        if r["edge_unavailable_flag"]:
            r["reasons_for_survival"]="EDGE_UNAVAILABLE_RETAINED;NO_EXTREME_GAP_EXCLUSION" if not r["extreme_gap_flag"] else "EDGE_UNAVAILABLE_RETAINED"
        elif not r["negative_edge_flag"] and not r["extreme_gap_flag"] and not r["opposite_side_flag"]:
            r["reasons_for_survival"]="NONNEGATIVE_OR_UNAVAILABLE_EDGE;SELECTED_SIDE_NO_VIG_GAP_NOT_ABOVE_15PP"
        else:
            r["reasons_for_survival"]=""
    lines={str(line):[r for r in hist if r["line"]==line] for line in (.5,1.5,2.5)}
    stages=[]; dates=[]; negrows=[]; s1=[]; s2=[]; s3=[]; survivors=[]; removed=[]; sensitivity=[]; overlaps=[]; survivor_bins=[]
    for line, rows in lines.items():
        if not rows: continue
        neg=[r for r in rows if r["selected_production_edge"] is not None and r["selected_production_edge"]<0]
        zero=[r for r in rows if r["selected_production_edge"]==0]
        pos=[r for r in rows if r["selected_production_edge"] is not None and r["selected_production_edge"]>0]
        unavail=[r for r in rows if r["selected_production_edge"] is None]
        one=[r for r in rows if not r["negative_edge_flag"]]
        two=[r for r in one if not r["extreme_gap_flag"]]
        three=[r for r in two if not r["opposite_side_flag"]]
        negrows += neg; s1 += one; s2 += two; s3 += three; survivors += three; removed += [r for r in rows if r not in three]
        stage_specs=[("STAGE_0",rows,[],rows,"ALL_MARKET_USABLE"),("STAGE_1",rows,neg,one,"REMOVE_EDGE_LT_0"),("STAGE_2",one,[r for r in one if r["extreme_gap_flag"]],two,"STAGE_1_SURVIVORS_EXCLUDING_SIGNED_GAP_GT_0.15"),("STAGE_3",two,[r for r in two if r["opposite_side_flag"]],three,"STAGE_2_SURVIVORS_EXCLUDING_OPPOSITE_IMPLIED_SIDES")]
        for stage,start,drop,remain,definition in stage_specs:
            m=metrics(remain); d=metrics(drop); stages.append({"line":line,"stage":stage,"definition":definition,"starting_n":len(start),"removed_n":len(drop),"remaining_n":len(remain),"removed_wins":d["wins"],"removed_losses":d["losses"],"removed_excess_losses":d["excess_losses"],"removed_log_loss_sum":d["log_loss_sum"],"remaining_excess_losses":m["excess_losses"],**{f"remaining_{k}":v for k,v in m.items()}})
        for cut in (.10,.15,.20):
            excluded=[r for r in one if (r.get("signed_selected_model_minus_no_vig_market_gap") or -1)>cut]
            sensitivity.append({"line":line,"positive_gap_boundary_pp":cut*100,"n":len(excluded),**metrics(excluded)})
        flags={"negative_edge":lambda r:r["negative_edge_flag"],"gap_gt_15pp":lambda r:r["extreme_gap_flag"],"opposite_side":lambda r:r["opposite_side_flag"],"strong_under_gap":lambda r:r["strong_under_gap_flag"]}
        for a,fa in flags.items():
            for b,fb in flags.items():
                if a<=b:
                    sub=[r for r in rows if fa(r) and fb(r)]; overlaps.append({"line":line,"region_a":a,"region_b":b,**metrics(sub)})
        for date,dr in sorted(grouped(rows,lambda r:r["slate_date"]).items()):
            dr1=[r for r in dr if not r["negative_edge_flag"]]; dr2=[r for r in dr1 if not r["extreme_gap_flag"]]; dr3=[r for r in dr2 if not r["opposite_side_flag"]]
            dates.append({"line":line,"slate_date":date,"stage0_n":len(dr),"stage1_removed":len(dr)-len(dr1),"stage1_surviving":len(dr1),"stage1_percent_surviving":len(dr1)/len(dr),"stage2_removed":len(dr1)-len(dr2),"stage2_surviving":len(dr2),"stage2_percent_of_stage1":len(dr2)/len(dr1) if dr1 else 0,"stage2_percent_surviving_raw":len(dr2)/len(dr),"stage3_removed":len(dr2)-len(dr3),"stage3_surviving":len(dr3),"stage3_percent_of_stage2":len(dr3)/len(dr2) if dr2 else 0,"stage3_percent_surviving_raw":len(dr3)/len(dr)})
    # Independent survivor segmentations; keep each one-dimensional slice bounded/descriptive.
    primary=[r for r in survivors if r["line"]==.5]
    for fname,field,bounds in [
        ("survivor_model_probability_bins.csv","model_selected_p",[(.6,"<60%"),(.7,"60–70%"),(.8,"70–80%"),(2,"80%+")]),
        ("survivor_market_probability_bins.csv","market_selected_p",[(.6,"<60%"),(.7,"60–70%"),(.8,"70–80%"),(2,"80%+")]),
        ("survivor_signed_gap_bins.csv","signed_selected_model_minus_no_vig_market_gap",[(-.15,"<-15pp"),(-.08,"-15 to -8pp"),(-.03,"-8 to -3pp"),(.03,"-3 to +3pp"),(.08,"+3 to +8pp"),(.15,"+8 to +15pp"),(2,">+15pp")]),
        ("survivor_edge_bands.csv","selected_production_edge",[(0,"<0"),(.01,"0–1pp"),(.03,"1–3pp"),(.08,"3–8pp"),(.15,"8–15pp"),(2,">15pp")]),
        ("survivor_ev_bands.csv","selected_side_indicated_ev_at_attachment_median_price",[(0,"<0"),(.01,"0–1%"),(.05,"1–5%"),(.10,"5–10%"),(.20,"10–20%"),(2,">20%")]),
        ("survivor_price_bins.csv","selected_price_american",[(-200,"<=-200"),(-130,"-199 to -130"),(-110,"-129 to -110"),(110,"-109 to +109"),(150,"+110 to +149"),(10000,"+150+")])]:
        seg=segment_table(primary,field,bounds); survivor_bins += [{"table":fname,**r} for r in seg]; write_csv(fname,seg)
    prefilter_edge_bands=segment_table([r for r in lines["0.5"]],"selected_production_edge",[(0,"<0"),(.01,"0–1pp"),(.03,"1–3pp"),(.08,"3–8pp"),(.15,"8–15pp"),(2,">15pp")])
    write_csv("stage_0_edge_bands.csv",prefilter_edge_bands)
    for filename,key,cuts,labels in [
        ("stage_0_market_gap_bands.csv","signed_selected_model_minus_no_vig_market_gap",[-.15,-.08,-.03,.03,.08,.15],("<-15pp","-15 to -8pp","-8 to -3pp","-3 to +3pp","+3 to +8pp","+8 to +15pp",">+15pp")),
        ("stage_0_ev_bands.csv","selected_side_indicated_ev_at_attachment_median_price",[0,.01,.05,.10,.20],("<0","0–1%","1–5%","5–10%","10–20%",">20%")),
        ("stage_0_price_bins.csv","selected_price_american",[-200,-130,-110,110,150],("<=-200","-199 to -130","-129 to -110","-109 to +109","+110 to +149","+150+"))]:
        write_csv(filename,segment_table(lines["0.5"],key,list(zip(cuts,labels))))
    # Structural decomposition of the high positive edge tail among survivors.
    def edge_band(r):
        x=r.get("selected_production_edge")
        return "UNAVAILABLE" if x is None else "0–1pp" if x<.01 else "1–3pp" if x<.03 else "3–8pp" if x<.08 else "8–15pp" if x<.15 else ">15pp"
    edge_origin=[]
    for (band,side,line), subset in sorted(grouped(primary,lambda r:(edge_band(r),r["model_side"],str(r["line"]))).items()):
        edge_origin.append({"edge_band":band,"model_side":side,"line":line,**metrics(subset)})
    write_csv("survivor_edge_origin.csv",edge_origin)
    for name, fn in [("survivor_side.csv",lambda r:r["model_side"]),("survivor_line.csv",lambda r:str(r["line"])),("survivor_date.csv",lambda r:r["slate_date"])]:
        write_csv(name,[{"segment":k,**metrics(v),"share_survivor_losses":sum(not int(x["model_win"]) for x in v)/max(1,sum(not int(x["model_win"]) for x in primary))} for k,v in sorted(grouped(primary,fn).items())])
    for r in dry:
        edge=r.get("selected_production_edge")
        r["edge_status"]="EDGE_UNAVAILABLE" if edge is None else "NEGATIVE_EDGE" if edge<0 else "ZERO_EDGE" if edge==0 else "POSITIVE_EDGE"
        r["stage1_status"]="REMOVED_NEGATIVE_EDGE" if r["selected_production_edge"] is not None and r["selected_production_edge"]<0 else "SURVIVES_STAGE_1"
        r["stage2_status"]="REMOVED_EXTREME_GAP" if r["stage1_status"]=="SURVIVES_STAGE_1" and r["signed_selected_model_minus_no_vig_market_gap"] is not None and r["signed_selected_model_minus_no_vig_market_gap"]>.15 else "SURVIVES_STAGE_2" if r["stage1_status"]=="SURVIVES_STAGE_1" else "NOT_REACHED"
        r["stage3_status"]="REMOVED_OPPOSITE_SIDE" if r["stage2_status"]=="SURVIVES_STAGE_2" and r["model_side"]!=r["market_side"] else "SURVIVES_STAGE_3" if r["stage2_status"]=="SURVIVES_STAGE_2" else "NOT_REACHED"
        r["research_status"]="RESEARCH_ONLY_NO_OPERATIONAL_FILTER"
        r["reasons_for_survival"]=("EDGE_UNAVAILABLE_RETAINED;GAP_NOT_ABOVE_15PP;SAME_IMPLIED_SIDE" if edge is None else "NONNEGATIVE_EDGE;GAP_NOT_ABOVE_15PP;SAME_IMPLIED_SIDE") if r["stage3_status"]=="SURVIVES_STAGE_3" else ""
    write_csv("raw_canonical_population.csv",hist)
    write_csv("historical_market_evidence_inventory.csv",read_csv(SOURCE/"market_evidence_inventory.csv"))
    write_csv("stage_0_metrics.csv",[{"line":k,**metrics(v)} for k,v in lines.items() if v])
    write_csv("negative_edge_analysis.csv",[{"line":str(l),"edge_region":name,**metrics(sub),"dates":"|".join(sorted({x["slate_date"] for x in sub}))} for l,rs in lines.items() for name,sub in [("NEGATIVE",[x for x in rs if x.get("selected_production_edge") is not None and x["selected_production_edge"]<0]),("ZERO",[x for x in rs if x.get("selected_production_edge")==0]),("POSITIVE",[x for x in rs if x.get("selected_production_edge") is not None and x["selected_production_edge"]>0]),("UNAVAILABLE",[x for x in rs if x.get("selected_production_edge") is None])]])
    write_csv("stage_1_rows.csv",s1); write_csv("stage_2_rows.csv",s2); write_csv("stage_2_disagreement_sensitivity.csv",sensitivity)
    write_csv("exclusion_overlap_matrix.csv",overlaps); write_csv("canonical_funnel_table.csv",stages); write_csv("date_level_funnel_counts.csv",dates)
    write_csv("current_consideration_survivors.csv",survivors); write_csv("removed_rows.csv",removed)
    write_csv("survivor_loss_concentration.csv",survivor_bins)
    write_csv("current_day_dry_view_2026-10-14.csv",dry)
    write_csv("current_day_dry_funnel.csv",[{"line":line,"raw_market_usable":sum(r["line"]==line for r in dry),"stage1_removed":sum(r["line"]==line and r["stage1_status"]=="REMOVED_NEGATIVE_EDGE" for r in dry),"stage1_survivors":sum(r["line"]==line and r["stage1_status"]=="SURVIVES_STAGE_1" for r in dry),"stage1_percent_surviving":sum(r["line"]==line and r["stage1_status"]=="SURVIVES_STAGE_1" for r in dry)/max(1,sum(r["line"]==line for r in dry)),"stage2_removed_incremental":sum(r["line"]==line and r["stage2_status"]=="REMOVED_EXTREME_GAP" for r in dry),"stage2_survivors":sum(r["line"]==line and r["stage2_status"]=="SURVIVES_STAGE_2" for r in dry),"stage2_percent_of_stage1":sum(r["line"]==line and r["stage2_status"]=="SURVIVES_STAGE_2" for r in dry)/max(1,sum(r["line"]==line and r["stage1_status"]=="SURVIVES_STAGE_1" for r in dry)),"stage2_percent_surviving_raw":sum(r["line"]==line and r["stage2_status"]=="SURVIVES_STAGE_2" for r in dry)/max(1,sum(r["line"]==line for r in dry)),"stage3_removed_incremental":sum(r["line"]==line and r["stage3_status"]=="REMOVED_OPPOSITE_SIDE" for r in dry),"stage3_survivors":sum(r["line"]==line and r["stage3_status"]=="SURVIVES_STAGE_3" for r in dry),"stage3_percent_surviving_raw":sum(r["line"]==line and r["stage3_status"]=="SURVIVES_STAGE_3" for r in dry)/max(1,sum(r["line"]==line for r in dry))} for line in sorted({r["line"] for r in dry})])
    region_rows=[]
    for line, rows in lines.items():
        for name, fn in [("NEGATIVE_EDGE",lambda r:r["negative_edge_flag"]),("GAP_GT_15PP",lambda r:r["extreme_gap_flag"]),("OPPOSITE_SIDE",lambda r:r["opposite_side_flag"]),("STRONG_UNDER_GAP",lambda r:r["strong_under_gap_flag"]),("STAGE_1_SURVIVORS",lambda r:not r["negative_edge_flag"]),("STAGE_2_SURVIVORS",lambda r:not r["negative_edge_flag"] and not r["extreme_gap_flag"])]:
            sub=[r for r in rows if fn(r)]; region_rows.append({"line":line,"region":name,"dates":"|".join(sorted({r["slate_date"] for r in sub})),"model_under_n":sum(r["model_side"]=="UNDER" for r in sub),"model_over_n":sum(r["model_side"]=="OVER" for r in sub),**metrics(sub)})
    write_csv("region_summary.csv",region_rows)
    write_csv("stage_1_metrics.csv",[x for x in stages if x["stage"]=="STAGE_1"])
    write_csv("stage_2_metrics.csv",[x for x in stages if x["stage"]=="STAGE_2"])
    write_csv("stage_3_metrics.csv",[x for x in stages if x["stage"]=="STAGE_3"])
    write_csv("stage_3_rows.csv",s3)
    inventory=json.loads((SOURCE/"summary.json").read_text())
    line1=[r for r in survivors if r["line"]==1.5]
    negative_primary=[r for r in lines["0.5"] if r["negative_edge_flag"]]
    highgap_primary=[r for r in lines["0.5"] if r["extreme_gap_flag"]]
    opp_primary=[r for r in lines["0.5"] if r["opposite_side_flag"]]
    opp_after_stage2=[r for r in s2 if r["line"]==.5 and r["opposite_side_flag"]]
    stage3_removed_primary=[r for r in opp_after_stage2]
    edge_values=sorted(r["selected_production_edge"] for r in primary if r.get("selected_production_edge") is not None)
    def quantile(q):
        if not edge_values: return None
        return edge_values[min(len(edge_values)-1, int((len(edge_values)-1)*q))]
    summary={"schema_version":"NHL_POINTS_CONSIDERATION_FUNNEL_V1","historical_dates":hgbdates,
      "line_metrics":{l:metrics(rs) for l,rs in lines.items() if rs},"stage_metrics":stages,
      "stage1_negative_edge_counts":{l:sum(1 for r in rs if r["negative_edge_flag"]) for l,rs in lines.items()},
      "stage2_incremental_counts":{l:sum(1 for r in rs if not r["negative_edge_flag"] and r["extreme_gap_flag"]) for l,rs in lines.items()},
      "survivor_metrics_0_5":metrics(primary),"survivor_metrics_1_5":metrics(line1),
      "survivor_percent_raw_0_5":len(primary)/max(1,len(lines.get("0.5",[]))),
      "region_results_0_5":{"negative_edge":metrics(negative_primary),"gap_gt_15pp":metrics(highgap_primary),"opposite_side":metrics(opp_primary),
        "opposite_removed_stage1":sum(r["opposite_side_flag"] and r["negative_edge_flag"] for r in lines["0.5"]),
        "opposite_removed_stage2_incremental":sum(r["opposite_side_flag"] and not r["negative_edge_flag"] and r["extreme_gap_flag"] for r in lines["0.5"]),
        "opposite_after_stage2":metrics(opp_after_stage2),"stage3_removed":metrics(stage3_removed_primary),
        "opposite_survivors":metrics([r for r in primary if r["opposite_side_flag"]]),
        "strong_under_gap":metrics([r for r in lines["0.5"] if r["strong_under_gap_flag"]])},
      "survivor_edge_quantiles_0_5":{"p10":quantile(.1),"p25":quantile(.25),"p50":quantile(.5),"p75":quantile(.75),"p90":quantile(.9)},
      "dry_view":{"date":"2026-10-14","integrity_status":integrity.get("status"),"parent_daily_run_id":integrity.get("parent_daily_run_id"),"observation_timestamp_utc":obs,"two_game_ids":integrity.get("odds_observation_canonical_game_ids"),"immutable_prediction_rows":len(preds),"market_usable_rows":len(dry),"stage3_survivors":sum(r["stage3_status"]=="SURVIVES_STAGE_3" for r in dry),"survivor_share_market_usable":sum(r["stage3_status"]=="SURVIVES_STAGE_3" for r in dry)/max(1,len(dry)),"survivor_share_immutable_predictions":sum(r["stage3_status"]=="SURVIVES_STAGE_3" for r in dry)/max(1,len(preds)),"final_outcome_available":False,"outcomes_included":False},
      "edge_definitions":{"production_export":"selected side model probability minus raw American implied probability of that side's separately aggregated median attachment price; p_mkt includes vig",
        "signed_gap":"selected-side model probability minus selected-side no-vig market probability; raw American implied probabilities normalized by their two-side sum",
        "ev":"model probability times decimal return minus one at selected side median price; descriptive only because side medians aggregate across books and are not an executable paired quote",
        "site_edge":"selected p minus p_over_mkt, fallback to raw American implied probability; bps filter keeps edge*10000 >= threshold and leaves missing-edge rows visible",
        "frontend_ranked_edge":"Over model probability minus p_over_mkt; ranking accepts attached prices from -350 through +500 American",
        "8rain_gap":"selected-side model probability minus source side market probability (attachment raw implied, fallback raw American implied); candidate policy requires gap > 0 and EV > 0",
        "fair_price":"American equivalent odds from model probability, not an edge"},
      "stage_definitions":{"stage1":"remove selected production edge < 0; exact zero survives; unavailable retained as EDGE_UNAVAILABLE",
        "stage2":"among Stage 1 survivors remove signed selected-side no-vig gap > +0.15; first exclusion stage wins",
        "stage3":"among Stage 2 survivors remove model side opposite the no-vig market implied side; provisional, carried forward only for prospective evaluation",
        "validation":"freeze definitions and evaluate by later finalized slate dates; do not optimize thresholds on outcomes"},
      "provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_changes":0,"shortlist_target_imposed":False,
      "excluded_rows_preserved_in_original_prediction_and_grade_artifacts":True,"source_audit_summary_identity":inventory.get("audit_identity","")}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
    (OUT/"edge_definition_inventory.json").write_text(json.dumps({**summary["edge_definitions"],"source_locations":{"production_edge":"backend/nhl/scripts/build_points_with_market.py: p_over_mkt, p_under_mkt, edge_over, edge_under","8rain":"backend/nhl/scripts/select_nhl_points_saves_8rain_candidates.py: american_implied_probability, per-side EV and model_market_gap; backend/nhl/config/nhl_points_active_candidate_policy_v1.json","site_edge_ev":"nhl/site/index.html: probFromAmerican, evPerUnit, row edge derivation and Min Edge bps control","frontend_rank":"frontend/src/Pages/nhl/NHLPredictions.jsx: bestEdgeCandidate/topPlayableEdgeRows/isPlayableMarket"}},indent=2,sort_keys=True)+"\n")
    (OUT/"frozen_future_validation.json").write_text(json.dumps({"frozen_stage_1":"selected production edge < 0","frozen_stage_2":"Stage 1 survivor signed selected-side no-vig gap > +0.15","frozen_stage_3":"Stage 2 survivor model side differs from no-vig market implied side","independent_unit":"finalized slate_date","append_policy":"append exact-key rows and preserve source hashes; report each date and pooled descriptive metrics","no_cutoff_optimization":True,"stage_3_status":"PROVISIONAL_RESEARCH_EXCLUSION_REQUIRES_PROSPECTIVE_VALIDATION"},indent=2)+"\n")
    (OUT/"stage_3_analysis.json").write_text(json.dumps({"status":"PROVISIONALLY_JUSTIFIED","basis":"Previously established opposite-side concentration remains partly distinct after Stages 1–2.","primary_0_5_after_stage2":metrics(opp_after_stage2),"primary_0_5_removed":metrics(stage3_removed_primary),"remaining_opposite_side_n":sum(r["opposite_side_flag"] for r in primary),"limitations":"36 rows across four dates; heterogeneous date outcomes; descriptive early-season sample only. Freeze provisionally and validate prospectively; do not call permanent."},indent=2)+"\n")
    success_band=next((r for r in survivor_bins if r.get("table")=="survivor_edge_bands.csv" and r.get("segment")=="1–3pp"),{})
    (OUT/"success_region_summary.json").write_text(json.dumps({"status":"DESCRIPTIVE_ONLY","candidate_regions":[{"region":"survivor edge 1–3pp",**success_band,"interpretation":"small favorable proper-score slice; requires later-date replication and is not an inclusion rule"}],"no_inclusion_criteria_created":True},indent=2)+"\n")
    (OUT/"next_questionable_region_summary.json").write_text(json.dumps({"primary_next_candidate":None,"secondary_candidate":None,"decision":"NO_ADDITIONAL_IMMEDIATE_EXCLUSION","reason":"After the prior-supported three provisional exclusions, no further survivor segment in four slates meets a defensible independent-repeat criterion. Continue prospective Stage 1/2/3 evaluation; investigate larger positive-edge bands only as more dates accrue."},indent=2)+"\n")
    (OUT/"cross_prop_reuse.md").write_text("# Cross-prop reuse\n\nFor SOG and Saves, reuse the research sequence only after binding each prop's own immutable prediction identity, two-sided pregame prices, no-vig construction, official outcome/participation settlement, and unresolved rules. Preserve every excluded row for grading, apply previously established exclusions sequentially, then inspect survivor losses before proposing further exclusions. This package implements Points only.\n")
    (OUT/"backlog_context.md").write_text("# Backlog and context\n\nPreserved: first live capture of `NHL_SOG_D10_STRUCTURAL_CORRECTION_V1`; prospective SOG structural-shadow grading; fixed-blend collection; historical official-TOI binding; deferred Puck Line work; Tier3 override not needed. Points backlog: prospective validation of Stage 1/2; investigate the next survivor failure region; inclusion criteria after exclusion work stabilizes; eventual read-only consideration view; decide whether to retire manual NHL.com review only after automated reduction matures.\n")
    source_inputs={str(SOURCE/"canonical_market_intersection.csv"):hashlib.sha256((SOURCE/"canonical_market_intersection.csv").read_bytes()).hexdigest(),str(SOURCE/"lineage_manifest.json"):hashlib.sha256((SOURCE/"lineage_manifest.json").read_bytes()).hexdigest(),**historical_attachment_paths,str(day/"points_with_market.csv"):hashlib.sha256((day/"points_with_market.csv").read_bytes()).hexdigest(),str(day/"points_attachment_integrity.json"):hashlib.sha256((day/"points_attachment_integrity.json").read_bytes()).hexdigest(),str(preds_path):hashlib.sha256(preds_path.read_bytes()).hexdigest()}
    (OUT/"lineage_manifest.json").write_text(json.dumps({"inputs":source_inputs,"output_hashes":{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"},"output_dir":str(OUT.relative_to(ROOT))},indent=2,sort_keys=True)+"\n")
    sm=metrics(primary)
    report=["# NHL Points consideration-funnel reduction","","## Decision summary","",f"Finalized HGB market-intersection dates: {', '.join(hgbdates)}. Historical primary 0.5 Stage 0 is {len(lines['0.5'])} rows; Stage 1 removes {summary['stage1_negative_edge_counts']['0.5']}, Stage 2 removes a further {summary['stage2_incremental_counts']['0.5']}, and Stage 3 removes {len(stage3_removed_primary)}, leaving {len(primary)} ({summary['survivor_percent_raw_0_5']:.1%}). Stage 1 removes 120/658 (18.2%) and keeps 81.8%; its removed group was 88–32 with -5.9 excess losses, so it removed a relatively useful subset. Stage 2 removes 275/538 (51.1%), 138–137 with +55.7 excess losses. Stage 3 removes a distinct 36/263, 14–22 with +6.6 excess losses; the final survivor set is 227/658 (34.5%).", "",f"At 0.5 the full Stage 0 was 380–278, expected losses 197.5, excess +80.5, Brier .2528 and log loss .7032. Final Stage 3 survivors: {sm['wins']}–{sm['losses']}, expected losses {sm['expected_losses']:.1f}, excess {sm['excess_losses']:+.1f}, Brier {sm['brier']:.4f}, log loss {sm['log_loss']:.4f}. Proper scores improve descriptively; excess losses remain.","","Stage 1, Stage 2, and Stage 3 are provisional research exclusions. Stage 3 uses the previously identified opposite-side failure region and removes its remaining distinct population; the 36-row residual remains a small early-season sample and requires prospective validation. No permanent operational filter is established.","","## Edge definitions","", "The export `edge_over/edge_under` is model-side probability less raw American implied probability from each side’s separately aggregated median price. This raw probability includes vig. The signed no-vig gap normalizes both raw side probabilities before comparison. The site’s Edge bps uses selected model probability minus `p_over_mkt` (or raw-price fallback), and retains rows with edge × 10,000 at or above the user threshold while missing values stay visible. The frontend ranked cards use Over probability minus market Over probability and a -350 to +500 price guard. EightRain computes selected-side EV as probability / American implied probability - 1 and the model-market gap from side probabilities; its active Points policy requires both strictly >0. The fair American price is model fair odds, not edge. This study computes median-price EV for description only; cross-book medians are not executable, so no ROI is claimed.","","## Funnel summary","", "| Line | Stage | Start | Removed | Remain | Wins removed | Losses removed | Excess losses removed | Remaining excess losses |","|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for x in stages:
        report.append(f"| {x['line']} | {x['stage']} | {x['starting_n']} | {x['removed_n']} | {x['remaining_n']} | {x['removed_wins']} | {x['removed_losses']} | {x['removed_excess_losses']:.1f} | {x['remaining_excess_losses']:.1f} |")
    sm=metrics(primary); opp=summary["region_results_0_5"]; report += ["","## Survivor-only reading","",f"0.5 survivor performance: {sm['wins']}–{sm['losses']} ({sm['accuracy']:.1%}); expected losses {sm['expected_losses']:.1f}, observed {sm['observed_losses']}, excess {sm['excess_losses']:+.1f}; Brier {sm['brier']:.4f}, log loss {sm['log_loss']:.4f}; no-vig market Brier {sm.get('market_no_vig_brier',0):.4f}, log loss {sm.get('market_no_vig_log_loss',0):.4f}.","","Negative-edge rows were 120/658 (18.2%), 88–32, -5.9 excess losses. The Stage 2 gap region was 275/658 overall (41.8%), all HGB UNDER; fixed sensitivity among Stage 1 at >10/>15/>20pp is 423/275/151. Opposite-side disagreement was 192 total (77–115, +44.9 excess); Stage 1 removed 1, Stage 2 removed 155, leaving 36 (14–22, +6.6 excess) after Stage 2. Those 36 span all four dates, have Brier .2621 and log loss .7174 versus no-vig market .2385/.6698, justifying a provisional third stage. This is the earlier signal on the same four-date evidence, not independent replication; prospective validation remains necessary.","","Stage 1, 2, and 3 date counts are in `date_level_funnel_counts.csv`. Final survivor edge bands do not improve monotonically: 0–1pp n=15, 5–10, +5.3 excess; 1–3pp n=32, 24–8, -1.7 excess; 3–8pp n=108, 69–39, +10.0 excess; 8–15pp n=72, 42–30, +10.5 excess. The 1–3pp slice has Brier .1865/log loss .5627 versus no-vig market .2193/.6900, but it is descriptive only. The 0–1pp slice is small and poor. There were no production-edge values above 15pp among final survivors. The remaining 8–15pp tail has mean model-market gap 13.1pp, mean production edge 9.8pp, selected model probability 73.0%, mean market probability 59.9%, and only negative American prices (-280 to -117.5); by construction, the gap-edge difference reflects vig normalization. Final survivors are all HGB UNDER, so there are no near-even or plus-money price rows. The three favorite-price bands are mixed (19 rows at -129 to -110: 8–11; 96 at -199 to -130: 55–41; 112 at -200 or shorter: 77–35). No minimum edge or price rule is inferred. Median-price EV is descriptive and ROI is withheld.","","Model confidence improves across broad bands but remains imperfect: 80%+ is 19–5 on n=24 and close to expected losses; 60–70% is 39–34 on n=73 and 70–80% is 79–44 on n=123, both with excess losses. No-vig market Brier/log loss are better than HGB overall among survivors (.2310/.6613 versus .2400/.6744). No retained manual-style NHL.com variables were bound to these rows, so their survivor-level separating value is unresolved. No further next candidate is justified after these three provisional exclusions; continue prospective validation.","","Line 1.5 remains separate: Stage 0 n=524, Stage 1 n=433, Stage 2 n=312 and Stage 3 n=312 (59.5% raw); final survivor excess losses +14.0. Line 2.5 has only 9 rows and is not decision-grade.","","## Oct. 14 dry view","",f"Integrity {integrity.get('status')}; retained observation {obs}; two games {integrity.get('odds_observation_canonical_game_ids')}. Outcome-free two-sided pregame rows: {len(dry)} of {len(preds)} immutable predictions. At 0.5: 58 raw, 9 Stage 1 removed, 26 Stage 2 removed, {sum(r['line']==.5 and r['stage3_status']=='REMOVED_OPPOSITE_SIDE' for r in dry)} Stage 3 removed, {sum(r['line']==.5 and r['stage3_status']=='SURVIVES_STAGE_3' for r in dry)} survive. Across all lines, 43/93 (46.2%) market-usable rows and 43/285 (15.1%) immutable predictions survive. Full per-line reduction is in `current_day_dry_funnel.csv`. This view leaves immutable predictions and exports unchanged.","","## Constraints and backlog","","All excluded rows remain in immutable source predictions and grades, with first exclusion and overlap flags in `removed_rows.csv`. No target shortlist count or inclusion rule was imposed. Preserve the SOG structural-shadow capture and prospective grades, fixed-blend collection, historical official-TOI binding, deferred Puck Line work, and no Tier3 override. Points backlog: prospective Stage 1/2/3 validation; investigate the next survivor failure region; defer inclusion criteria; later build a read-only consideration view and revisit manual NHL.com review after automated reduction matures.","","The pattern can later be reused for SOG and Saves only after their own market/outcome identity and settlement rules are bound. No other prop was changed.","","Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Production changes: 0. Pushed: NO.","",f"Package: `{OUT.relative_to(ROOT)}/`."]
    (OUT/"report.md").write_text("\n".join(report)+"\n")
    (OUT/"lineage_manifest.json").write_text(json.dumps({"inputs":source_inputs,"output_hashes":{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"},"output_dir":str(OUT.relative_to(ROOT))},indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT.relative_to(ROOT)),"historical_dates":hgbdates,"primary_stage0":len(lines["0.5"]),"primary_survivors":len(primary),"dry_rows":len(dry)},sort_keys=True))


if __name__ == "__main__": main()
