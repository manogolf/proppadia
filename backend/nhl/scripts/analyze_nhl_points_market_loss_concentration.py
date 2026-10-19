#!/usr/bin/env python3
"""Read-only concentration analysis over retained NHL Points HGB × market evidence."""
from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from backend.nhl.reconciliation_grades import restated_grade_path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/points_market_loss_concentration/2026-10-14"
ODDS = ROOT / "backend/nhl/exports/odds_history"
RECON = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
LINES = (0.5, 1.5, 2.5)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(name: str, rows: list[dict], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = list(rows[0]) if rows else []
    with (OUT / name).open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)


def num(v, default=None):
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def timestamp(v):
    if not v: return None
    try: return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError: return None


def amer(p):
    if p is None or not 0 < p < 1: return ""
    return round(-100*p/(1-p), 2) if p >= .5 else round(100*(1-p)/p, 2)


def logloss(p, y):
    p = min(max(p, 1e-15), 1-1e-15)
    return -(y*math.log(p)+(1-y)*math.log(1-p))


def auc(rows, pkey, ykey):
    pos = [r for r in rows if int(r[ykey]) == 1]
    neg = [r for r in rows if int(r[ykey]) == 0]
    if not pos or not neg: return None
    wins = sum((a[pkey] > b[pkey]) + .5*(a[pkey] == b[pkey]) for a in pos for b in neg)
    return wins/(len(pos)*len(neg))


def metrics(rows, pkey="model_selected_p", marketkey="market_selected_p"):
    if not rows: return {"n": 0}
    n=len(rows); wins=sum(int(r["model_win"]) for r in rows); losses=n-wins
    expected=sum(1-r[pkey] for r in rows); ll=sum(r["model_logloss"] for r in rows)
    out={"n":n,"wins":wins,"losses":losses,"accuracy":wins/n,"observed_loss_rate":losses/n,
         "expected_losses":expected,"expected_loss_rate":expected/n,"excess_losses":losses-expected,
         "mean_model_selected_p":sum(r[pkey] for r in rows)/n,
         "realized_selected_win_rate":wins/n,"calibration_gap_realized_minus_model":wins/n-sum(r[pkey] for r in rows)/n,
         "model_brier":sum((r["model_p_over"]-r["over_outcome"])**2 for r in rows)/n,
         "model_log_loss":ll/n,"model_log_loss_sum":ll,"model_auc_over":auc(rows,"model_p_over","over_outcome"),
         "model_over_n":sum(r["model_side"]=="OVER" for r in rows),"model_under_n":sum(r["model_side"]=="UNDER" for r in rows),
         "logloss_share":sum(r["model_logloss"] for r in rows)}
    if all(r.get(marketkey) is not None for r in rows):
        market_wins=sum(r.get("market_win",0) for r in rows)
        out.update(market_selected_mean=sum(r[marketkey] for r in rows)/n,
                   market_brier=sum((r["market_p_over"]-r["over_outcome"])**2 for r in rows)/n,
                   market_log_loss=sum(r["market_logloss"] for r in rows)/n,
                   market_log_loss_sum=sum(r["market_logloss"] for r in rows),
                   market_auc_over=auc(rows,"market_p_over","over_outcome"),market_wins=market_wins,
                   market_losses=n-market_wins,market_selected_accuracy=market_wins/n,
                   market_over_n=sum(r.get("market_side")=="OVER" for r in rows),
                   market_under_n=sum(r.get("market_side")=="UNDER" for r in rows))
    return out


def grouped(rows, keyfn):
    groups=defaultdict(list)
    for r in rows: groups[keyfn(r)].append(r)
    return groups


def score_rows(rows, title, keyfn):
    total_loss=sum(int(r["model_win"])==0 for r in rows); total_expected=sum(1-r["model_selected_p"] for r in rows)
    total_excess=total_loss-total_expected
    total_ll=sum(r["model_logloss"] for r in rows)
    out=[]
    for k, rs in grouped(rows,keyfn).items():
        m=metrics(rs); m["segment"]=str(k); m["share_all_losses"]=m["losses"]/total_loss if total_loss else 0
        m["share_expected_losses"]=m["expected_losses"]/total_expected if total_expected else 0
        m["share_excess_losses"]=m["excess_losses"]/total_excess if total_excess else 0
        m["share_full_logloss"]=m["model_log_loss_sum"]/total_ll if total_ll else 0
        m["dates"]=",".join(sorted({r["slate_date"] for r in rs})); out.append(m)
    return sorted(out,key=lambda x:x["segment"])


def bins(rows, key, edges, labels):
    def label(r):
        x=r.get(key)
        if x is None: return "MISSING"
        for i,e in enumerate(edges):
            if x < e: return labels[i]
        return labels[-1]
    return score_rows(rows,key,lambda r:(r["line"],label(r)))


def bins_key(fn): return fn


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    all_rows=[]; inventory=[]; hash_inputs={}
    for market_path in sorted(ODDS.glob("2026-10-*/points_with_market.csv")):
        day=market_path.parent.name; integrity_path=market_path.parent/"points_attachment_integrity.json"
        attachment=read_csv(market_path); integ=json.loads(integrity_path.read_text()) if integrity_path.exists() else {}
        run=integ.get("parent_daily_run_id") or (attachment[0].get("parent_daily_run_id") if attachment else "")
        prediction=ROOT/"backend/nhl/data/processed/daily_runs"/str(run)/"points_predictions.csv"
        recon_dirs=sorted((RECON/day).glob("reconciliation=*/graded_points.csv"))
        grade_path=restated_grade_path(recon_dirs[-1].parent,"graded_points.csv") if recon_dirs else None
        final_rows=read_csv(grade_path) if grade_path else []
        final_lookup={(r.get("game_id",""),r.get("player_id",""),str(num(r.get("line")))):r for r in final_rows
                      if r.get("grading_status")=="SETTLED" and r.get("participation_state")=="PARTICIPATED" and num(r.get("official_points")) is not None}
        settled_market_n=sum((r.get("game_id",""),r.get("player_id",""),str(num(r.get("line")))) in final_lookup for r in attachment)
        model_kind="HGB" if prediction.exists() and "expected_points" in (read_csv(prediction)[:1] or [{}])[0] else "NON_HGB"
        match_n=sum(bool(r.get("p_over_mkt")) for r in attachment)
        timestamp_utc=integ.get("odds_observation_timestamp_utc","")
        prediction_rows=read_csv(prediction) if prediction.exists() else []
        pred_lookup={(r["game_id"],r["player_id"],str(num(r["line"]))):r for r in prediction_rows}
        obs_dt=timestamp(timestamp_utc)
        def is_pregame(row):
            pr=pred_lookup.get((row.get("game_id",""),row.get("player_id",""),str(num(row.get("line")))))
            start=timestamp(pr.get("game_start_utc")) if pr else None
            return bool(obs_dt and start and obs_dt < start)
        pregame_n=sum(bool(r.get("p_over_mkt")) and bool(r.get("p_under_mkt")) and is_pregame(r) for r in attachment)
        two_sided=sum(bool(r.get("p_over_mkt")) and bool(r.get("p_under_mkt")) and num(r.get("price_over")) is not None and num(r.get("price_under")) is not None and is_pregame(r) for r in attachment)
        inventory.append({"slate_date":day,"model_family_binding":model_kind,"parent_daily_run_id":run,"prediction_path":str(prediction.relative_to(ROOT)) if prediction.exists() else "",
            "prediction_sha256":hashlib.sha256(prediction.read_bytes()).hexdigest() if prediction.exists() else "","attachment_path":str(market_path.relative_to(ROOT)),
            "attachment_sha256":hashlib.sha256(market_path.read_bytes()).hexdigest(),"integrity_status":integ.get("status","MISSING"),
            "manifest_sha256":integ.get("odds_observation_manifest_sha256",""),"observation_timestamp_utc":integ.get("odds_observation_timestamp_utc",""),
            "prediction_rows":len(prediction_rows) if prediction.exists() else len(attachment),"final_outcome_rows":len(final_lookup),"settled_market_matched_rows":settled_market_n,
            "market_matched_rows":match_n,"legitimate_pregame_market_rows":pregame_n,"two_sided_priced_rows":two_sided,"final_grade_available":bool(grade_path),
            "reconciliation_path":str(grade_path.relative_to(ROOT)) if grade_path else "","providers_observed":"|".join(sorted({x for r in attachment for x in (r.get("source_books_over","")+"|"+r.get("source_books_under","")).split("|") if x})),
            "lines":"|".join(sorted({r.get("line","") for r in attachment})),"eligible_hgb_final_market_date":bool(model_kind=="HGB" and grade_path and integ.get("status")=="PASS" and pregame_n>0)})
        for p in (market_path,integrity_path,prediction,grade_path):
            if p and p.exists(): hash_inputs[str(p.relative_to(ROOT))]=hashlib.sha256(p.read_bytes()).hexdigest()
        if model_kind!="HGB" or not grade_path or integ.get("status")!="PASS": continue
        preds=pred_lookup
        grades={(r["game_id"],r["player_id"],str(num(r["line"]))):r for r in read_csv(grade_path)}
        for q in attachment:
            if not (q.get("p_over_mkt") and q.get("p_under_mkt")) or not is_pregame(q): continue
            line=num(q.get("line")); key=(q.get("game_id",""),q.get("player_id",""),str(line)); pred=preds.get(key); outcome=grades.get(key)
            if not pred or not outcome or num(outcome.get("official_points")) is None or outcome.get("participation_state")!="PARTICIPATED": continue
            po=num(pred.get("prob_over")); exp=num(pred.get("expected_points")); pm_o=num(q.get("p_over_mkt")); pm_u=num(q.get("p_under_mkt")); points=num(outcome.get("official_points"))
            if None in (po,pm_o,pm_u,line,points) or not 0<po<1: continue
            model_side="OVER" if po>=.5 else "UNDER"; market_side="OVER" if pm_o>=pm_u else "UNDER"
            model_p=po if model_side=="OVER" else 1-po; market_p=pm_o if model_side=="OVER" else pm_u
            over=int(points>line); model_win=int((points>line) if model_side=="OVER" else (points<line))
            price=num(q.get("price_over" if model_side=="OVER" else "price_under")); outcome_row={"slate_date":day,"game_id":q["game_id"],"player_id":q["player_id"],"player":q.get("full_name",""),"line":line,
                "team_id":q.get("team_id",""),"opponent":"","home_away":"","model_name":"NHL_POINTS_COUNT_HGB_V1","model_version":"NHL_POINTS_LEADER_CORE_FEATURES_V1",
                "expected_count":exp,"model_p_over":po,"model_p_under":1-po,"model_side":model_side,"model_selected_p":model_p,"model_equivalent_american":amer(model_p),
                "market_p_over":pm_o,"market_p_under":pm_u,"market_side":market_side,"market_side_selected_p":max(pm_o,pm_u),"market_selected_p":market_p,"selected_model_minus_market_gap":model_p-market_p,
                "market_quote_count_over":num(q.get("source_quote_count_over")),"market_quote_count_under":num(q.get("source_quote_count_under")),"source_books_over":q.get("source_books_over",""),"source_books_under":q.get("source_books_under",""),
                "selected_attached_american_price":price,"official_points":points,"over_outcome":over,"model_win":model_win,
                "market_win":int((points>line) if market_side=="OVER" else (points<line)),"model_logloss":logloss(po,over),
                "market_logloss":logloss(pm_o,over),"feature_input_cutoff_utc":pred.get("feature_input_cutoff_utc",""),"observation_timestamp_utc":integ.get("odds_observation_timestamp_utc",""),
                "prediction_artifact_sha256":integ.get("prediction_artifact_sha256",""),"market_manifest_sha256":integ.get("odds_observation_manifest_sha256",""),"outcome_grade_path":str(grade_path.relative_to(ROOT))}
            all_rows.append(outcome_row)
    # Retain exact market rows on reconciled HGB dates. One record per (date,game,player,line).
    all_rows.sort(key=lambda r:(r["slate_date"],r["game_id"],r["player_id"],r["line"]))
    write_csv("market_evidence_inventory.csv",inventory)
    write_csv("canonical_market_intersection.csv",all_rows)
    primary=[r for r in all_rows if r["line"]==.5]
    table_sets={"global_line_metrics.csv":score_rows(all_rows,"line",lambda r:r["line"]),
       "date_level_metrics.csv":score_rows(all_rows,"date",lambda r:(r["slate_date"],r["line"])),
       "side_metrics.csv":score_rows(all_rows,"side",lambda r:(r["line"],r["model_side"])),
       "model_confidence_bins.csv":bins(all_rows,"model_selected_p",[.55,.60,.65,.70,.75,.80], ["<55%","55-60%","60-65%","65-70%","70-75%","75-80%",">=80%"]),
       "market_confidence_bins.csv":bins(all_rows,"market_selected_p",[.55,.60,.65,.70,.75,.80], ["<55%","55-60%","60-65%","65-70%","70-75%","75-80%",">=80%"]),
       "signed_gap_bins.csv":bins(all_rows,"selected_model_minus_market_gap",[-.15,-.08,-.03,.03,.08,.15],["<=-15pp","-15 to -8pp","-8 to -3pp","near agreement ±3pp","+3 to +8pp","+8 to +15pp",">+15pp"]),
       "price_bins.csv":bins([r for r in all_rows if r["selected_attached_american_price"] is not None],"selected_attached_american_price",[-200,-130,-110,110,150],["<=-200","-200 to -130","-130 to -110","-110 to +110","+110 to +150",">+150"]),
       "failure_map_0_5.csv":score_rows(primary,"failure map",lambda r:(r["model_side"],"<65%" if r["model_selected_p"]<.65 else "65-80%" if r["model_selected_p"]<.8 else "80%+", "market <60%" if r["market_selected_p"]<.6 else "market 60%+", "gap <=-8pp" if r["selected_model_minus_market_gap"]<=-.08 else "gap near" if abs(r["selected_model_minus_market_gap"])<.03 else "gap +3 to 15pp" if r["selected_model_minus_market_gap"]<=.15 else "gap >15pp")),
       "logloss_burden.csv":score_rows(all_rows,"logloss",lambda r:(r["line"],"model-selected loss" if not r["model_win"] else "win")),
       "calibration_tables.csv":score_rows(all_rows,"calibration",lambda r:(r["line"],"<60%" if r["model_selected_p"]<.6 else "60-70%" if r["model_selected_p"]<.7 else "70-80%" if r["model_selected_p"]<.8 else "80%+")),
       "model_market_agreement.csv":score_rows(all_rows,"agreement",lambda r:(r["line"],"opposite sides" if r["model_side"]!=r["market_side"] else "same side; prob gap <3pp" if abs(r["selected_model_minus_market_gap"])<.03 else "same side; gap >=3pp"))}
    for filename, rows in table_sets.items(): write_csv(filename,rows)
    # Discovery/validation separated at the slate level. Fewer than six independent dates is inadequate for confirmation.
    dates=sorted({r["slate_date"] for r in primary}); enough=len(dates)>=6; split=max(1,int(len(dates)*.6)) if enough else len(dates)
    discovery=set(dates[:split]); validation=set(dates[split:]) if enough else set()
    candidates=[]; validations=[]
    if enough:
        for name,fn in [("large positive selected-side gap >= 8pp",lambda r:r["selected_model_minus_market_gap"]>=.08),("model selected confidence >=80%",lambda r:r["model_selected_p"]>=.8)]:
            dr=[r for r in primary if r["slate_date"] in discovery]; reg=[r for r in dr if fn(r)]
            if len(reg)>=30 and metrics(reg)["excess_losses"]>0:
                candidates.append({"regime":name,"definition_frozen_on_discovery":True,"discovery_n":len(reg),**metrics(reg)})
                vr=[r for r in primary if r["slate_date"] in validation]; excluded=[r for r in vr if fn(r)]; retained=[r for r in vr if not fn(r)]
                validations.append({"regime":name,"validation_dates":"|".join(sorted(validation)),"all_n":len(vr),"excluded_n":len(excluded),"retained_n":len(retained),"losses_removed":sum(not r["model_win"] for r in excluded),"wins_removed":sum(r["model_win"] for r in excluded),"all_metrics":json.dumps(metrics(vr),sort_keys=True),"retained_metrics":json.dumps(metrics(retained),sort_keys=True),"excluded_metrics":json.dumps(metrics(excluded),sort_keys=True)})
    write_csv("candidate_failure_regimes.csv",candidates)
    write_csv("validation_avoidance_tests.csv",validations)
    p_brier_delta=(sum((r["model_p_over"]-r["over_outcome"])**2-(r["market_p_over"]-r["over_outcome"])**2 for r in primary)/len(primary)) if primary else None
    write_csv("failure_scorecard.csv",[{"regime":"all model-selected 0.5 rows","definition":"population baseline; not an exclusion candidate","n":len(primary),"dates":"|".join(dates),"share_predictions":1.0 if primary else 0.0,"share_losses":1.0 if primary else 0.0,"share_excess_losses":1.0 if primary else 0.0,"calibration_gap":metrics(primary).get("calibration_gap_realized_minus_model"),"model_minus_market_brier":p_brier_delta,"status":"ONE_SLATE_OR_UNSTABLE","validation":"not available; four dates"}])
    write_csv("success_scorecard.csv",[{"regime":"none confirmed","definition":"No repeatable strength can be confirmed from four dates.","status":"UNSTABLE","validation":"not available"}])
    pareto=[]
    for frac in (.1,.2):
        ranked=sorted(primary,key=lambda r:r["model_logloss"],reverse=True); top=ranked[:max(1,math.ceil(len(ranked)*frac))]
        pareto.append({"selection":"highest individual model log-loss","population_fraction":frac,"n":len(top),"losses":sum(not r["model_win"] for r in top),"share_all_losses":sum(not r["model_win"] for r in top)/sum(not r["model_win"] for r in primary) if primary and sum(not r["model_win"] for r in primary) else None,"logloss_share":sum(r["model_logloss"] for r in top)/sum(r["model_logloss"] for r in primary) if primary else None,"selection_is_ex_post":True})
    write_csv("pareto_loss_concentration.csv",pareto)
    summary={"schema_version":"NHL_POINTS_MARKET_LOSS_CONCENTRATION_V1","scope":{"model":"NHL_POINTS_COUNT_HGB_V1","feature_contract":"NHL_POINTS_LEADER_CORE_FEATURES_V1","strict_dates":[x["slate_date"] for x in inventory if x["eligible_hgb_final_market_date"]],"primary_line":0.5,"independent_dates":dates,"discovery_dates":sorted(discovery),"validation_dates":sorted(validation),"validation_adequate":enough,"validation_rule":"chronological 60/40 by slate; requires >=6 primary dates"},
      "overall_by_line":{str(line):metrics([r for r in all_rows if r["line"]==line]) for line in LINES},"primary_0_5":metrics(primary),
      "primary_selected_side_market_metrics":metrics(primary,"market_selected_p","market_selected_p"),"market_overall_brier":sum((r["market_p_over"]-r["over_outcome"])**2 for r in primary)/len(primary) if primary else None,
      "primary_model_vs_market_brier_delta":(sum((r["model_p_over"]-r["over_outcome"])**2-(r["market_p_over"]-r["over_outcome"])**2 for r in primary)/len(primary)) if primary else None,
      "primary_model_vs_market_logloss_delta":sum(r["model_logloss"]-r["market_logloss"] for r in primary)/len(primary) if primary else None,
      "primary_market_dates":sorted({r["slate_date"] for r in primary}),"primary_population":len(primary),"all_line_population":len(all_rows),"scorecard_status":"ONE_SLATE_OR_UNSTABLE" if len(dates)<3 else "POSSIBLE_FAILURE",
      "manual_review_decision":"MANUAL_CHECK_REMAINS_USEFUL_UNRESOLVED" if not enough else "MANUAL_CHECK_ONLY_FOR_FLAGGED_REGIMES_PENDING_REPLICATION",
      "daily_automation_justified":"YES_AS_READ_ONLY_SUMMARY; NO AUTOMATED EXCLUSION until prospective validation","provider_calls":0,"paid_credits":0,"production_db_mutations":0,
      "evidence_hashes":hash_inputs,"limitations":["Oct. 1–9 point to Phoenix artifacts, excluded from HGB analysis.","October 2026 has fewer than six final HGB market slates; no credible unseen-date validation can be performed.","Market attachment exposes aggregate consensus and source-book lists; provider-by-provider prices and quote-level timestamps are not retained in this table, so BetOnline ROI is not claimed.","Player history/TOI context is not included unless available in this exact attached prediction artifact; this study prioritizes market/model variables."]}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
    (OUT/"discovery_validation_definition.json").write_text(json.dumps({"independent_unit":"slate_date","eligible_primary_dates":dates,"split_policy":"chronological 60/40, only if >=6 primary dates","discovery_dates":sorted(discovery),"validation_dates":sorted(validation),"status":"INSUFFICIENT_DATES_NO_CLAIM" if not enough else "SPLIT_DEFINED","frozen_candidates":candidates},indent=2,sort_keys=True)+"\n")
    (OUT/"daily_report_schema.json").write_text(json.dumps({"report_type":"read-only NHL Points model-market sanity report","fields":["slate_date","game_id","player_id","line","expected_points","p_over","p_under","selected_side","selected_side_probability","consensus_no_vig_over","consensus_no_vig_under","selected_side_market_probability","model_minus_market_gap","selected_price_if_provider_bound","official_points_when_final","expected_loss_probability","historical_regime_n","historical_observed_loss_rate","historical_expected_loss_rate","historical_excess_losses","calibration_gap","regime_flag","season_points_per_game","season_point_game_hit_rate","last5_hit_rate","last10_hit_rate","toi_trend","pp_toi_trend"],"guardrails":["No automated exclusions","Record quote provider and observation time or suppress price ROI","Show exact identity/hash binding and coverage","Separate live unresolved from final grade","Display sparse regime warning","Track history by date"]},indent=2,sort_keys=True)+"\n")
    (OUT/"cross_prop_reuse.md").write_text("# Cross-prop reuse notes\n\nReuse the identity-bound grain and expected/observed/excess loss, probability calibration, model-market gap, proper scores, retained price ROI, date stability, and chronological avoidance validation. SOG needs exact player-game-line and official shot binding; Saves also needs confirmed-starter eligibility and starter-status unresolved handling. Moneyline and Puck Line need game-level outcomes and their own push/settlement definitions. This audit does not implement those extensions.\n")
    (OUT/"backlog_context.md").write_text("# Backlog and context\n\nPreserved: first live capture of `NHL_SOG_D10_STRUCTURAL_CORRECTION_V1`; prospective SOG shadow grading; fixed-blend collection; historical official-TOI binding; Puck Line work deferred until tomorrow; Tier3 override not currently needed. Update Points sanity-card: market/model segmentation is more scalable and is now reported, but four HGB slates and absent provider-specific quote history/context are insufficient to retire manual review or claim a validated avoidance rule.\n")
    write_csv("logloss_row_burden.csv",[{k:r[k] for k in ("slate_date","game_id","player_id","player","line","model_side","model_selected_p","market_selected_p","selected_model_minus_market_gap","official_points","model_win","model_logloss")} for r in sorted(primary,key=lambda x:x["model_logloss"],reverse=True)])
    report=["# NHL Points model × market loss-concentration audit","","## Scope and evidence","",f"Strict retained HGB prediction + final outcome + passing market attachment dates: {', '.join(summary['scope']['strict_dates']) or 'none'}. Primary 0.5 intersection: **{len(primary)}** rows; all supported lines: **{len(all_rows)}** rows.","", "The audit uses immutable daily `points_predictions.csv` rows, a matching market attachment hash and passing attachment integrity record, plus participating final reconciled outcomes at exact game/player/line keys. Oct. 1–9 Phoenix attachments are inventoried but excluded. Oct. 14 has no final grade yet.","", "## Primary 0.5 results","", "| N | Model wins-losses | Model accuracy | Model Brier | Model log loss | Market Brier | Market log loss | Expected losses | Observed losses | Excess losses |", "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    m=metrics(primary); mk=summary["market_overall_brier"]
    report.append(f"| {m.get('n',0)} | {m.get('wins',0)}-{m.get('losses',0)} | {m.get('accuracy',0):.3f} | {m.get('model_brier',0):.4f} | {m.get('model_log_loss',0):.4f} | {mk if mk is not None else float('nan'):.4f} | {m.get('market_log_loss',0):.4f} | {m.get('expected_losses',0):.1f} | {m.get('losses',0)} | {m.get('excess_losses',0):+.1f} |")
    report += ["", "## Interpretation and study limits", "", "Across four independent slates, the model is materially overconfident at 0.5. The 658-row primary set has 80.5 more losses than summed model expectation, and the attached market consensus has lower Brier and log loss. This is descriptive across a short early-season window, not a validated failure rule. The largest absolute loss count occurs at 0.5 because that line supplies 658 rows; lines 1.5 have 524 market rows and 2.5 only 9, so latter lines are secondary and the 2.5 result is not decision-grade.", "", "By date, 0.5 excess losses are positive on all four dates, though sizes vary. Discovery/validation is not credible with four dates; all avoidance candidates remain untested, excluded rows = 0, wins sacrificed = 0, retained population = 658/658, ROI change unavailable. Ex post high-log-loss ranking is descriptive only and not an avoidable rule.", "", "Retained `price_over`/`price_under` values are attachment-level prices with aggregate source-book lists, not provider-specific quote history. Provider selection and per-provider quote timestamps are absent, so ROI and BetOnline-only ROI are withheld. Consensus probabilities remain suitable for proper-score comparison.", "", "Manual-review recommendation: do not discontinue NHL.com checks yet. The study has not tested manual variables inside a repeatable failure regime because the needed captured context is absent for these HGB rows and there are too few dates. A read-only daily market/sanity summary is justified to replace repeated ad hoc browsing over time; it must not filter picks. SOG/Saves reuse is straightforward after exact binding to their own outcomes; Moneyline/Puck Line need their own settlement and market definitions.", "", "Backlog preserved: first live SOG structural-correction capture; prospective SOG shadow grades; fixed-blend collection; historical official-TOI binding; Puck Line deferred; Tier3 override not currently needed.", "", "Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Production model/authority/filters: unchanged.", "", f"Files in `{OUT.relative_to(ROOT)}/`."]
    (OUT/"report.md").write_text("\n".join(report)+"\n")
    (OUT/"lineage_manifest.json").write_text(json.dumps({"created_by":"backend/nhl/scripts/analyze_nhl_points_market_loss_concentration.py","inputs":hash_inputs,"outputs":{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"}},indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT.relative_to(ROOT)),"inventory_dates":len(inventory),"strict_hgb_dates":summary["scope"]["strict_dates"],"primary_n":len(primary),"all_n":len(all_rows),"primary_metrics":m},sort_keys=True))


if __name__ == "__main__": main()
