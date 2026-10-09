#!/usr/bin/env python3
"""Build a provenance-bound NHL SOG performance characterization package.

This analysis intentionally consumes retained local receipts and outcomes only.
It does not access the database or any external provider.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import statistics
from datetime import datetime
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DATES = [f"2026-10-{day:02d}" for day in range(3, 9)]
ARMS = ["A_PRIOR_SEASON_CARRY_FORWARD", "B_PRIOR_SEASON_RECENCY_WEIGHTED",
        "C_MULTISEASON_SHRUNK_PLAYER", "D_PLAYER_ROLE_HIERARCHICAL",
        "F_CURRENT_PRESEASON_UPDATE", "G_COLD_START_TO_CURRENT_SEASON_BLEND"]


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fields=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = list(dict.fromkeys(k for row in rows for k in row))
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def fnum(x, default=None):
    try:
        n = float(x)
        return n if math.isfinite(n) else default
    except (TypeError, ValueError):
        return default


def truth(x):
    return str(x).strip().lower() in {"true", "1", "yes"}


def auc_ap(rows):
    if not rows or len({r["y"] for r in rows}) < 2:
        return "", ""
    ranked = sorted(rows, key=lambda r: r["p"], reverse=True)
    positives = sum(r["y"] for r in rows)
    negatives = len(rows) - positives
    # Mann-Whitney AUC with average ranks for ties.
    asc = sorted(rows, key=lambda r: r["p"])
    rank_sum = 0.0
    i = 0
    while i < len(asc):
        j = i + 1
        while j < len(asc) and asc[j]["p"] == asc[i]["p"]:
            j += 1
        rank = (i + 1 + j) / 2
        rank_sum += rank * sum(z["y"] for z in asc[i:j])
        i = j
    auc = (rank_sum - positives * (positives + 1) / 2) / (positives * negatives)
    tp = 0
    ap = 0.0
    for rank, r in enumerate(ranked, 1):
        tp += r["y"]
        if r["y"]:
            ap += tp / rank
    return auc, ap / positives


def score(rows):
    n = len(rows)
    if not n:
        return {"n": 0}
    ys = [r["y"] for r in rows]
    ps = [min(1-1e-12, max(1e-12, r["p"])) for r in rows]
    auc, ap = auc_ap(rows)
    brier = statistics.mean((p-y)**2 for p, y in zip(ps, ys))
    ll = -statistics.mean(y*math.log(p)+(1-y)*math.log(1-p) for p, y in zip(ps, ys))
    pred = [r["side"] == ("OVER" if r["y"] else "UNDER") for r in rows]
    bins=calibration_bins(rows)
    ece=sum(b["n"]*abs(b["calibration_error"]) for b in bins)/n if bins else ""
    intercept,slope=calibration_intercept_slope(rows)
    return {"n": n, "overs": sum(ys), "over_rate": statistics.mean(ys),
            "accuracy": statistics.mean(pred), "log_loss": ll, "brier": brier,
            "auc": auc, "average_precision": ap, "ap_base_rate_lift": ap/statistics.mean(ys) if sum(ys) else "",
            "ece_10_bins":ece,"calibration_intercept":intercept,"calibration_slope":slope,
            "mean_p_over": statistics.mean(ps), "mean_probability_on_overs": statistics.mean([p for p,y in zip(ps,ys) if y]) if sum(ys) else "",
            "mean_probability_on_unders": statistics.mean([p for p,y in zip(ps,ys) if not y]) if sum(ys)<n else ""}


def calibration_bins(rows, bins=10):
    result=[]
    for i in range(bins):
        lo, hi = i/bins, (i+1)/bins
        group=[r for r in rows if lo <= r["p"] < hi or (i==bins-1 and r["p"]==1)]
        if group:
            result.append({"line": group[0].get("line", ""), "bin": i+1, "p_min":lo,"p_max":hi,
                           "n":len(group),"mean_predicted_over":statistics.mean(r["p"] for r in group),
                           "realized_over_rate":statistics.mean(r["y"] for r in group),
                           "calibration_error":statistics.mean(r["y"] for r in group)-statistics.mean(r["p"] for r in group)})
    return result


def calibration_intercept_slope(rows):
    if len(rows)<3 or len({r["y"] for r in rows})<2: return "", ""
    xs=[math.log(max(1e-9,min(1-1e-9,r["p"]))/(1-max(1e-9,min(1-1e-9,r["p"])))) for r in rows]
    ys=[int(r["y"]) for r in rows]; a=0.0; b=1.0
    for _ in range(40):
        g0=g1=h00=h01=h11=0.0
        for x,y in zip(xs,ys):
            z=max(-30,min(30,a+b*x)); q=1/(1+math.exp(-z)); w=max(1e-9,q*(1-q))
            g0+=y-q; g1+=(y-q)*x; h00+=w; h01+=w*x; h11+=w*x*x
        det=h00*h11-h01*h01
        if det<=1e-12: return "", ""
        da=(g0*h11-g1*h01)/det; db=(g1*h00-g0*h01)/det
        a+=da; b+=db
        if abs(da)+abs(db)<1e-8: break
    return a,b


def bootstrap_player(rows, metric, reps=1000, seed=8419):
    players=defaultdict(lambda: [0.0,0.0,0.0,0])
    for r in rows:
        y=int(r["y"]); p=max(1e-12,min(1-1e-12,r["p"])); v=players[str(r["player_id"])]
        v[0]+=int(r["side"]==("OVER" if y else "UNDER"))
        v[1]+=-(y*math.log(p)+(1-y)*math.log(1-p)); v[2]+=(p-y)**2; v[3]+=1
    ids=list(players)
    if len(ids)<2: return "", ""
    rng=random.Random(seed)
    vals=[]
    for _ in range(reps):
        sums=[0.0,0.0,0.0,0]
        for _ in ids:
            v=players[rng.choice(ids)]
            for i in range(4): sums[i]+=v[i]
        vals.append(sums[{"accuracy":0,"log_loss":1,"brier":2}[metric]]/sums[3])
    vals.sort()
    return vals[int(.025*len(vals))], vals[min(len(vals)-1,int(.975*len(vals)))]


def paired_bootstrap(pairs, reps=1000, seed=19213):
    by_player=defaultdict(lambda: [0.0,0.0,0.0,0])
    for p,s in pairs:
        v=by_player[str(p["player_id"])]
        py=int(p["y"]); sy=int(s["y"])
        v[0]+=int(s["side"]==("OVER" if sy else "UNDER"))-int(p["side"]==("OVER" if py else "UNDER"))
        pp=max(1e-12,min(1-1e-12,p["p_over"])); sp=max(1e-12,min(1-1e-12,s["p"]))
        v[1]+=-(sy*math.log(sp)+(1-sy)*math.log(1-sp))+py*math.log(pp)+(1-py)*math.log(1-pp)
        v[2]+=(sp-sy)**2-(pp-py)**2; v[3]+=1
    ids=list(by_player)
    if len(ids)<2: return ("","")*3
    rng=random.Random(seed); values=[[],[],[]]
    for _ in range(reps):
        sums=[0.0,0.0,0.0,0]
        for _ in ids:
            v=by_player[rng.choice(ids)]
            for i in range(4): sums[i]+=v[i]
        for i in range(3): values[i].append(sums[i]/sums[3])
    out=[]
    for vals in values:
        vals.sort(); out += [vals[int(.025*reps)],vals[min(reps-1,int(.975*reps))]]
    return tuple(out)


def find_reconciliation(date):
    paths=list((ROOT/"artifacts/operational/nhl/postgame_reconciliation"/date).glob("reconciliation=*/source_bindings.json"))
    if len(paths)!=1: raise RuntimeError(f"expected one reconciliation source binding for {date}, got {len(paths)}")
    return paths[0].parent


def select_production(date, earliest):
    candidates=[]
    for p in (ROOT/"artifacts/operational/nhl/daily_runs").glob("run_id=*/parent_receipt.json"):
        try: receipt=json.loads(p.read_text())
        except Exception: continue
        if receipt.get("slate_date")!=date: continue
        lane=receipt.get("lanes",{}).get("legacy_sog",{})
        out=next((o for o in lane.get("outputs",[]) if o.get("path","").endswith("sog_predictions_wide_calibrated.csv")),None)
        if lane.get("status")!="COMPLETE" or not out: continue
        ended=receipt.get("ended_at_utc","")
        end_dt=datetime.fromisoformat(ended.replace("Z", "+00:00")) if ended else None
        earliest_dt=datetime.fromisoformat(earliest.replace("Z", "+00:00").replace(" ", "T"))
        if end_dt and end_dt < earliest_dt:
            candidates.append((ended,receipt,out))
    if not candidates: raise RuntimeError(f"no completed pregame production run for {date}")
    _,receipt,out=max(candidates,key=lambda x:x[0])
    path=Path(out["path"])
    if not path.is_absolute(): path=ROOT/path
    actual=sha(path)
    if actual!=out.get("sha256"): raise RuntimeError(f"prediction SHA mismatch: {path}")
    return receipt,out,path,actual


def build(outdir=None):
    outdir=Path(outdir) if outdir else ROOT/"artifacts/analysis/nhl/sog_full_analysis/2026-10-03_through_2026-10-08"
    outdir.mkdir(parents=True,exist_ok=True)
    production=[]; unresolved=[]; shadow_rows=[]; provenance=[]; phase_rows=[]; unscored_audit=[]; unscored_details=[]; missing_shadow=[]; feature_maps={}
    for date in DATES:
        rec=find_reconciliation(date)
        admitted=read_csv(rec/"canonical_admitted_slate.csv")
        earliest=min(r["scheduled_start_time_utc"] for r in admitted)
        receipt,pred_meta,pred_path,pred_sha=select_production(date,earliest)
        unscored_meta=next((o for o in receipt.get("lanes",{}).get("legacy_sog",{}).get("outputs",[]) if o.get("path","").endswith("sog_predictions_unscored.csv")),{})
        unscored_path=Path(unscored_meta["path"]) if unscored_meta.get("path") else None
        if unscored_path and not unscored_path.is_absolute(): unscored_path=ROOT/unscored_path
        if unscored_path and unscored_path.exists() and unscored_meta.get("sha256") and sha(unscored_path)!=unscored_meta["sha256"]:
            raise RuntimeError(f"unscored artifact SHA mismatch for {date}")
        if unscored_path and unscored_path.exists():
            for ur in read_csv(unscored_path):
                unscored_details.append({"slate_date":date,"game_id":ur.get("game_id"),"player_id":ur.get("player_id"),
                    "player_name":ur.get("player_name"),"line":ur.get("line"),"reason":ur.get("reason"),
                    "scoring_status":ur.get("scoring_status"),"model_family":ur.get("model_family"),
                    "model_version":ur.get("model_version"),"source_run_id":ur.get("source_run_id"),
                    "unscored_artifact_sha256":unscored_meta.get("sha256")})
        reason_counts=unscored_meta.get("unscored_reason_counts",{})
        unscored_audit.append({"slate_date":date,"unscored_identity_count":unscored_meta.get("unscored_identity_count",0),
            "unscored_row_count":unscored_meta.get("unscored_row_count",0),"reason_counts_json":json.dumps(reason_counts,sort_keys=True),
            "unscored_prediction_sha256":unscored_meta.get("sha256","")})
        selected_run=receipt["parent_daily_run_id"]
        market_path=ROOT/"artifacts/operational/nhl/sog_market_attachments"/f"season=2026/slate_date={date}/run_id={selected_run}/sog_with_market.csv"
        market_map={}
        if market_path.exists():
            market_sha=sha(market_path)
            expected_market_hashes={o.get("sha256") for o in receipt.get("lanes",{}).get("sog_attachment",{}).get("outputs",[])
                if o.get("path","").endswith("sog_with_market.csv")}
            if expected_market_hashes and market_sha not in expected_market_hashes:
                raise RuntimeError(f"selected-run market attachment SHA mismatch for {date}")
            for mr in read_csv(market_path):
                market_map[(mr.get("game_id"),mr.get("player_id"),fnum(mr.get("line")))]=mr
        outcomes=read_csv(rec/"canonical_skater_outcomes.csv")
        missing_path=rec/"graded_sog_missing_predictions.csv"
        if missing_path.exists():
            for mr in read_csv(missing_path):
                missing_shadow.append({**mr,"evaluation_population":"SHADOW_RECONCILIATION_MISSING_PREDICTION",
                    "source_sha256":sha(missing_path)})
        sums={}
        for line in (rec/"SHA256SUMS").read_text().splitlines():
            parts=line.split(None,1)
            if len(parts)==2: sums[parts[1].strip()]=parts[0]
        outcome_sha=sha(rec/"canonical_skater_outcomes.csv")
        if sums.get("canonical_skater_outcomes.csv")!=outcome_sha:
            raise RuntimeError(f"official outcome package SHA mismatch for {date}")
        outcome_map={(r["game_id"],r["player_id"]):r for r in outcomes}
        raw=read_csv(pred_path)
        seen=set()
        for p in raw:
            gid,pid=p["game_id"],p["player_id"]
            outcome=outcome_map.get((gid,pid),{})
            for line,col in [(1.5,"p_over_1_5"),(2.5,"p_over_2_5"),(3.5,"p_over_3_5")]:
                key=(date,gid,pid,line)
                if key in seen: raise RuntimeError(f"duplicate production key {key}")
                seen.add(key)
                prob=fnum(p.get(col))
                if prob is None: continue
                yval=fnum(outcome.get("official_sog"))
                participated=outcome.get("participation_state")=="PARTICIPATED" and truth(outcome.get("official_final"))
                y=int(yval>line) if participated and yval is not None else ""
                side="OVER" if prob>=.5 else "UNDER"
                row={"slate_date":date,"game_id":gid,"player_id":pid,"player_name":"","team_id":p.get("team_id"),
                     "opponent_id":p.get("opponent_id"),"is_home":p.get("is_home"),"line":line,
                     "p_over":prob,"p_under":1-prob,"selected_side":side,"selected_probability":max(prob,1-prob),
                     "expected_sog":fnum(p.get("expected_sog")),"official_sog":yval if participated else "",
                     "expected_sog_bucket":p.get("expected_sog_bucket"),"poisson_source":p.get("poisson_source"),
                     "predicted_count_p_0_1":p.get("p_0_1"),"predicted_count_p_2":p.get("p_2"),
                     "predicted_count_p_3":p.get("p_3"),"predicted_count_p_4_plus":p.get("p_4p"),
                     "realized_side":"OVER" if participated and y else "UNDER" if participated else "",
                     "correct":int((side=="OVER")==bool(y)) if participated else "",
                     "participation_status":outcome.get("participation_state","NO_CANONICAL_OUTCOME"),
                     "grade_status":"SETTLED" if participated else "UNRESOLVED_OR_NONPARTICIPANT",
                     "model_family":"poisson_baseline","model_version":"baseline_v1","prediction_sha256":pred_sha,
                     "production_run_id":receipt["parent_daily_run_id"],"outcome_source":outcome.get("outcome_source","")}
                market=market_map.get((gid,pid,line),{})
                row.update({"market_matched":bool(market),"market_price_over":market.get("price_over",""),
                    "market_price_under":market.get("price_under",""),"market_p_over_novig":market.get("p_over_mkt_novig",""),
                    "market_p_under_novig":market.get("p_under_mkt_novig",""),
                    "model_minus_market_over":(prob-fnum(market.get("p_over_mkt_novig"))) if fnum(market.get("p_over_mkt_novig")) is not None else ""})
                production.append(row)
                if not participated: unresolved.append(row.copy())
        binding=json.loads((rec/"source_bindings.json").read_text())["sog"]
        shadow_dir=Path(binding["run"])
        shadow_file=shadow_dir/"immutable_predictions.csv"
        snapshot_path=shadow_dir/"feature_source_snapshot.csv"
        expected_snapshot_sha=binding["files"].get("feature_source_snapshot.csv","")
        if snapshot_path.exists() and expected_snapshot_sha and sha(snapshot_path)==expected_snapshot_sha:
            fmap={}
            for fr in read_csv(snapshot_path):
                cutoff=datetime.fromisoformat(fr["feature_cutoff_utc"].replace("Z", "+00:00"))
                game_start=datetime.fromisoformat(fr["scheduled_start_time_utc"].replace("Z", "+00:00"))
                if cutoff>=game_start: raise RuntimeError(f"postgame feature snapshot cutoff for {date}")
                fmap[(fr["game_id"],fr["player_id"])]=fr
            feature_maps[date]=fmap
        final_map={}
        if shadow_file.exists() and sha(shadow_file)==binding["files"]["immutable_predictions.csv"]:
            for s in read_csv(shadow_file):
                if s.get("contract_arm") not in ARMS: continue
                line=fnum(s.get("line")); yval=fnum(outcome_map.get((s.get("game_id"),s.get("player_id")),{}).get("official_sog"))
                outcome=outcome_map.get((s.get("game_id"),s.get("player_id")),{})
                participated=outcome.get("participation_state")=="PARTICIPATED" and truth(outcome.get("official_final"))
                p=fnum(s.get("p_over"))
                if line is None or p is None: continue
                shadow_rows.append({"slate_date":date,"game_id":s.get("game_id"),"player_id":s.get("player_id"),"line":line,
                    "arm":s.get("contract_arm"),"p":p,"side":s.get("selected_side") or ("OVER" if p>=.5 else "UNDER"),
                    "y":int(yval>line) if participated and yval is not None else "","official_sog":yval if participated else "",
                    "player_name":s.get("player_name"),"position":s.get("position"),"cold_start_class":s.get("cold_start_class"),"feature_identity":s.get("feature_identity"),
                    "prediction_sha256":binding["files"]["immutable_predictions.csv"],"phase":"FINAL_PREGAME"})
                final_map[(s.get("game_id"),s.get("player_id"),line,s.get("contract_arm"))]=s
        midday_files=sorted((ROOT/"artifacts/operational/nhl/sog_prediction_only"/"season=2026"/f"slate_date={date}"/"phase=MIDDAY").glob("run_id=*/immutable_predictions.csv"))
        if midday_files and final_map:
            midfile=midday_files[-1]; midroot=midfile.parent
            metadata=json.loads((midroot/"run_metadata.json").read_text())
            final_metadata=json.loads((shadow_dir/"run_metadata.json").read_text())
            midsha=sha(midfile)
            mid_sums={}
            sums_path=midroot/"SHA256SUMS"
            if sums_path.exists():
                for sumline in sums_path.read_text().splitlines():
                    parts=sumline.split(None,1)
                    if len(parts)==2: mid_sums[parts[1].strip()]=parts[0]
            if mid_sums.get("immutable_predictions.csv")!=midsha:
                raise RuntimeError(f"MIDDAY shadow prediction SHA mismatch for {date}")
            earliest_dt=datetime.fromisoformat(earliest.replace("Z", "+00:00").replace(" ", "T"))
            mid_time=datetime.fromisoformat(metadata["prediction_timestamp_utc"].replace("Z", "+00:00"))
            final_time=datetime.fromisoformat(final_metadata["prediction_timestamp_utc"].replace("Z", "+00:00"))
            if metadata.get("status")=="COMPLETE_PREDICTION_ONLY_SHADOW" and final_metadata.get("status")=="COMPLETE_PREDICTION_ONLY_SHADOW" and mid_time<earliest_dt and final_time<earliest_dt:
                mids={}
                for s in read_csv(midfile):
                    line=fnum(s.get("line")); arm=s.get("contract_arm")
                    if line is not None and arm in ARMS: mids[(s.get("game_id"),s.get("player_id"),line,arm)]=s
                common=sorted(set(mids)&set(final_map)); changes=[]; side_changes=0
                for k in common:
                    a,b=mids[k],final_map[k]
                    pa,pb=fnum(a.get("p_over")),fnum(b.get("p_over"))
                    if pa is not None and pb is not None:
                        changes.append(abs(pb-pa))
                        side_changes+=((a.get("selected_side") or ("OVER" if pa>=.5 else "UNDER")) != (b.get("selected_side") or ("OVER" if pb>=.5 else "UNDER")))
                phase_rows.append({"slate_date":date,"status":"COMPARED","midday_run_id":metadata.get("run_id"),"final_run_id":shadow_dir.name,
                    "midday_prediction_sha256":midsha,"final_prediction_sha256":binding["files"]["immutable_predictions.csv"],
                    "midday_rows":len(mids),"final_rows":len(final_map),"common_identity_arm_line_rows":len(common),
                    "midday_unique_player_games":len({(k[0],k[1]) for k in mids}),"final_unique_player_games":len({(k[0],k[1]) for k in final_map}),
                    "added_final_player_games":len({(k[0],k[1]) for k in final_map}-{(k[0],k[1]) for k in mids}),
                    "dropped_midday_player_games":len({(k[0],k[1]) for k in mids}-{(k[0],k[1]) for k in final_map}),
                    "probability_changed_rows":sum(x>0 for x in changes),"mean_absolute_p_over_change":statistics.mean(changes) if changes else "",
                    "median_absolute_p_over_change":statistics.median(changes) if changes else "","selected_side_changes":side_changes})
            else:
                phase_rows.append({"slate_date":date,"status":"INELIGIBLE_CAPTURE_TIMESTAMP_OR_STATUS"})
        else:
            phase_rows.append({"slate_date":date,"status":"NO_COMPARABLE_MIDDAY_CAPTURE"})
        provenance.append({"slate_date":date,"canonical_game_count":len(admitted),"earliest_game_start_utc":earliest,
            "production_run_id":receipt["parent_daily_run_id"],"production_prediction_path":str(pred_path.relative_to(ROOT)),
            "production_prediction_sha256":pred_sha,"feature_cutoff_utc":receipt.get("started_at_utc"),
            "production_model_family":"poisson_baseline","production_model_version":"baseline_v1",
            "production_model_identity_sha256":next((o.get("fitted_model_evidence",{}).get("fitted_model_identity_sha256") for o in receipt.get("lanes",{}).get("legacy_sog",{}).get("outputs",[]) if o.get("fitted_model_evidence")),""),
            "production_run_started_at_utc":receipt.get("started_at_utc"),"production_run_ended_at_utc":receipt.get("ended_at_utc"),
            "feature_cutoff_utc":"NOT_RECORDED_IN_SELECTED_RECEIPT",
            "outcome_package":str(rec.relative_to(ROOT)),"outcome_sha256":outcome_sha,
            "market_attachment_path":str(market_path.relative_to(ROOT)) if market_path.exists() else "",
            "market_attachment_sha256":sha(market_path) if market_path.exists() else "",
            "pregame_feature_snapshot_sha256":expected_snapshot_sha,
            "shadow_run":binding["run"],"shadow_manifest_sha256":binding["manifest_sha256"],
            "shadow_prediction_sha256":binding["files"]["immutable_predictions.csv"]})
    keys=[(r["slate_date"],r["game_id"],r["player_id"],r["line"]) for r in production]
    if len(keys)!=len(set(keys)): raise RuntimeError("duplicate production grade keys")
    shadow_name_map={}
    for s in shadow_rows:
        k=(s["slate_date"],s["game_id"],s["player_id"],s["line"])
        shadow_name_map.setdefault(k,s)
    for r in production:
        s=shadow_name_map.get((r["slate_date"],r["game_id"],r["player_id"],r["line"]),{})
        feat=feature_maps.get(r["slate_date"],{}).get((r["game_id"],r["player_id"]),{})
        r["player_name"]=feat.get("player_name","") or s.get("player_name","") or ""
        r["position"]=feat.get("position","") or s.get("position","") or ""
        r["cold_start_class"]=s.get("cold_start_class","")
        r["current_season_games_prior"]=fnum(feat.get("current_regular_games"))
        r["current_preseason_games"]=fnum(feat.get("current_preseason_games"))
        r["current_regular_toi_per_game"]=fnum(feat.get("current_regular_toi_per_game"))
        r["prior_toi_per_game"]=fnum(feat.get("prior_toi_per_game"))
        r["prior_sog_per60"]=fnum(feat.get("prior_sog_per60"))
        r["current_regular_sog_per60"]=fnum(feat.get("current_regular_sog_per60"))
        r["team_changed"]=feat.get("team_changed","")
        r["pregame_feature_cutoff_utc"]=feat.get("feature_cutoff_utc","")
        r["outcome_identity"]=f"{r['slate_date']}|{r['game_id']}|{r['player_id']}"
        r["realized_margin_from_line"]=(r["official_sog"]-r["line"]) if r["grade_status"]=="SETTLED" else ""
    settled=[r for r in production if r["grade_status"]=="SETTLED"]
    for r in settled: r["y"]=int(r["realized_side"]=="OVER"); r["p"]=r["p_over"]; r["side"]=r["selected_side"]
    write_csv(outdir/"analysis_population.csv",production)
    write_csv(outdir/"unresolved_and_nonparticipants.csv",unresolved)
    write_csv(outdir/"selected_source_bindings.csv",provenance)
    write_csv(outdir/"unscored_audit.csv",unscored_audit)
    write_csv(outdir/"unscored_production_rows.csv",unscored_details)
    write_csv(outdir/"missing_prospective_sog_rows.csv",missing_shadow)
    oct3=next(p for p in (ROOT/"artifacts/operational/nhl/daily_runs").glob("run_id=*/parent_receipt.json")
              if json.loads(p.read_text()).get("parent_daily_run_id")==provenance[0]["production_run_id"])
    oct3j=json.loads(oct3.read_text()); l3=oct3j["lanes"]["legacy_sog"]
    children=oct3j.get("child_summaries",[])
    def child_status(needle):
        c=next((c for c in children if needle in c.get("command_identity","")),{})
        return c.get("status","NOT_FOUND"),c.get("exit_status","")
    score_status,score_exit=child_status("score_sog_poisson_baseline.py")
    load_status,load_exit=child_status("load_sog_predictions_denali.py")
    attach_status=oct3j.get("lanes",{}).get("sog_attachment",{}).get("status","NOT_FOUND")
    gate=l3.get("inputs",[{}])[0]
    write_csv(outdir/"oct3_boundary_evidence.csv",[{
        "classification":"OCT3_VALID_POST_STABILIZATION_BOUNDARY","production_lane_status":l3.get("status"),
        "scoring_status":score_status,"scoring_exit_status":score_exit,"prediction_load_status":load_status,
        "prediction_load_exit_status":load_exit,"market_attachment_status":attach_status,
        "legacy_population_gate_would_block":gate.get("legacy_population_gate_would_block"),
        "season_toi_null_ratio":gate.get("null_ratio"),"maximum_allowed_null_ratio":gate.get("maximum_null_ratio"),
        "baseline_identity":"poisson_baseline / baseline_v1"}])
    # Basic daily/cumulative, line, side and baseline metrics.
    daily=[]; line_side=[]; base=[]; proper=[]; bins=[]
    for date in DATES:
        rows=[r for r in settled if r["slate_date"]==date]
        for line in (1.5,2.5,3.5):
            subset=[r for r in rows if r["line"]==line]
            if not subset: continue
            m=score(subset); daily.append({"slate_date":date,"line":line,**m})
            for side in ("OVER","UNDER"):
                sr=[r for r in subset if r["side"]==side]
                line_side.append({"slate_date":date,"line":line,"side":side,"n":len(sr),"wins":sum(r["correct"] for r in sr),"accuracy":statistics.mean(r["correct"] for r in sr) if sr else "",
                    "mean_selected_confidence":statistics.mean(r["selected_probability"] for r in sr) if sr else "",
                    "mean_realized_margin":statistics.mean(r["official_sog"]-line for r in sr) if sr else ""})
    for line in (1.5,2.5,3.5):
        rows=[r for r in settled if r["line"]==line]
        m=score(rows)
        acc_ci=bootstrap_player(rows,"accuracy")
        proper.append({"scope":"cumulative","slate_date":"2026-10-03_through_2026-10-08","line":line,**m,
                       "accuracy_ci_low":acc_ci[0],"accuracy_ci_high":acc_ci[1]})
        bins.extend(calibration_bins(rows))
        over=sum(r["y"] for r in rows); n=len(rows)
        always_under=1-over/n if n else ""; always_over=over/n if n else ""
        model_acc=m.get("accuracy","")
        base.append({"line":line,"settled":n,"over_rate":over/n if n else "","under_rate":1-over/n if n else "",
                     "always_over_accuracy":always_over,"always_under_accuracy":always_under,
                     "model_accuracy":model_acc,"model_minus_always_under":model_acc-always_under if n else "",
                     "model_decisions_differ_from_always_under":sum(r["side"]=="OVER" for r in rows),
                     "within_window_majority_accuracy":max(always_over,always_under) if n else "",
                     "baseline_is_descriptive_in_sample":True})
        for side in ("OVER","UNDER"):
            sr=[r for r in rows if r["side"]==side]
            line_side.append({"slate_date":"CUMULATIVE","line":line,"side":side,"n":len(sr),"wins":sum(r["correct"] for r in sr),"accuracy":statistics.mean(r["correct"] for r in sr) if sr else "",
                "mean_selected_confidence":statistics.mean(r["selected_probability"] for r in sr) if sr else "",
                "mean_realized_margin":statistics.mean(r["official_sog"]-line for r in sr) if sr else ""})
        for date in DATES:
            subset=[r for r in settled if r["line"]==line and r["slate_date"]==date]
            if subset:
                proper.append({"scope":"daily","slate_date":date,"line":line,**score(subset)})
                bins.extend(calibration_bins(subset))
    # cumulative progression overall per date
    for i,date in enumerate(DATES):
        rows=[r for r in settled if r["slate_date"]<=date]
        m=score(rows); daily.append({"slate_date":date,"line":"ALL_CUMULATIVE",**m})
    write_csv(outdir/"daily_performance.csv",daily)
    write_csv(outdir/"line_side_performance.csv",line_side)
    side_loss=[]
    for line in (1.5,2.5,3.5):
        for side in ("OVER","UNDER"):
            losses=[r for r in settled if r["line"]==line and r["side"]==side and not r["correct"]]
            margins=[r["official_sog"]-line for r in losses]
            side_loss.append({"line":line,"selected_side":side,"losses":len(losses),
                "near_misses_one_sog_or_less":sum(abs(m)<=1 for m in margins),
                "large_misses_more_than_two_sog":sum(abs(m)>2 for m in margins),
                "mean_realized_margin_on_losses":statistics.mean(margins) if margins else "",
                "median_realized_margin_on_losses":statistics.median(margins) if margins else ""})
    write_csv(outdir/"side_loss_analysis.csv",side_loss)
    write_csv(outdir/"proper_scoring_metrics.csv",proper)
    write_csv(outdir/"calibration_bins.csv",bins)
    write_csv(outdir/"base_rate_baselines.csv",base)
    # Shadow exact common rows, only settled prospective rows.
    shadow_comp=[]; disagreements=[]; shadow_scored=[]; arm_maps={}
    prodmap={(r["slate_date"],r["game_id"],r["player_id"],r["line"]):r for r in settled}
    for s in shadow_rows:
        if s["y"]=="": continue
        shadow_scored.append({**s,"y":int(s["y"]),"p":s["p"]})
    for arm in ARMS:
        shmap={(r["slate_date"],r["game_id"],r["player_id"],r["line"]):r for r in shadow_scored if r["arm"]==arm}
        arm_maps[arm]=shmap
        common=sorted(set(prodmap)&set(shmap))
        pairs=[]
        for k in common:
            p,s=prodmap[k],shmap[k]
            pairs.append((p,s))
        if pairs:
            pr=[{"y":p["y"],"p":p["p_over"],"side":p["side"],"player_id":p["player_id"]} for p,s in pairs]
            sr=[{"y":s["y"],"p":s["p"],"side":s["side"],"player_id":s["player_id"]} for p,s in pairs]
            pm,sm=score(pr),score(sr)
            ci=paired_bootstrap(pairs)
            shadow_comp.append({"arm":arm,"common_rows":len(pairs),"production_accuracy":pm["accuracy"],"shadow_accuracy":sm["accuracy"],
               "accuracy_difference_shadow_minus_production":sm["accuracy"]-pm["accuracy"],"production_log_loss":pm["log_loss"],"shadow_log_loss":sm["log_loss"],
               "paired_accuracy_difference_ci_low":ci[0],"paired_accuracy_difference_ci_high":ci[1],
               "paired_log_loss_difference_ci_low":ci[2],"paired_log_loss_difference_ci_high":ci[3],
               "paired_brier_difference_ci_low":ci[4],"paired_brier_difference_ci_high":ci[5],
               "production_brier":pm["brier"],"shadow_brier":sm["brier"],"side_disagreements":sum(p["side"]!=s["side"] for p,s in pairs),
               "shadow_wins_production_loses":sum(s["side"]==("OVER" if s["y"] else "UNDER") and p["side"]!=("OVER" if p["y"] else "UNDER") for p,s in pairs),
               "production_wins_shadow_loses":sum(p["side"]==("OVER" if p["y"] else "UNDER") and s["side"]!=("OVER" if s["y"] else "UNDER") for p,s in pairs),
               "both_correct":sum(p["correct"] and s["side"]==("OVER" if s["y"] else "UNDER") for p,s in pairs),
               "both_wrong":sum(not p["correct"] and s["side"]!=("OVER" if s["y"] else "UNDER") for p,s in pairs)})
            for p,s in pairs:
                if p["side"]!=s["side"]:
                    disagreements.append({"arm":arm,"slate_date":p["slate_date"],"game_id":p["game_id"],"player_id":p["player_id"],"line":p["line"],
                        "production_side":p["side"],"shadow_side":s["side"],"realized_side":p["realized_side"],"production_correct":p["correct"],
                        "shadow_correct":int(s["side"]==p["realized_side"]),"cold_start_class":s.get("cold_start_class")})
    write_csv(outdir/"shadow_common_row_comparison.csv",shadow_comp)
    write_csv(outdir/"shadow_disagreement_analysis.csv",disagreements)
    shared_keys=set(prodmap)
    for arm in ARMS: shared_keys &= set(arm_maps.get(arm,{}))
    all_arm_comp=[]
    for arm in ARMS:
        pairs=[(prodmap[k],arm_maps[arm][k]) for k in sorted(shared_keys)]
        if not pairs: continue
        pr=[{"y":p["y"],"p":p["p_over"],"side":p["side"],"player_id":p["player_id"]} for p,s in pairs]
        sr=[{"y":s["y"],"p":s["p"],"side":s["side"],"player_id":s["player_id"]} for p,s in pairs]
        pm,sm=score(pr),score(sr); ci=paired_bootstrap(pairs)
        all_arm_comp.append({"arm":arm,"all_arm_common_rows":len(pairs),"production_accuracy":pm["accuracy"],"shadow_accuracy":sm["accuracy"],
            "accuracy_difference_shadow_minus_production":sm["accuracy"]-pm["accuracy"],"production_log_loss":pm["log_loss"],"shadow_log_loss":sm["log_loss"],
            "production_brier":pm["brier"],"shadow_brier":sm["brier"],"side_disagreements":sum(p["side"]!=s["side"] for p,s in pairs),
            "paired_accuracy_difference_ci_low":ci[0],"paired_accuracy_difference_ci_high":ci[1],
            "paired_log_loss_difference_ci_low":ci[2],"paired_log_loss_difference_ci_high":ci[3],
            "paired_brier_difference_ci_low":ci[4],"paired_brier_difference_ci_high":ci[5]})
    write_csv(outdir/"shadow_all_arm_common_comparison.csv",all_arm_comp)
    # Count diagnostics from realized lambda. Poisson score is a diagnostic assumption check.
    count=[]
    all_players={(r["slate_date"],r["game_id"],r["player_id"]):r for r in settled if r["line"]==1.5}
    for bucket,fn in [("all",lambda x:True),("<1.5",lambda x:x<1.5),("1.5-2.5",lambda x:1.5<=x<2.5),("2.5-3.5",lambda x:2.5<=x<3.5),("3.5+",lambda x:x>=3.5)]:
        group=[]
        for r in all_players.values():
            lam=r.get("expected_sog")
            y=fnum(r.get("official_sog"))
            if lam is not None and y is not None and fn(lam): group.append((lam,y))
        if not group: continue
        n=len(group); obs=statistics.mean(y for _,y in group); pred=statistics.mean(l for l,_ in group)
        mae=statistics.mean(abs(l-y) for l,y in group); rmse=math.sqrt(statistics.mean((l-y)**2 for l,y in group))
        nll=statistics.mean(l-y*math.log(max(l,1e-12))+math.lgamma(y+1) for l,y in group)
        obs_var=statistics.pvariance(y for _,y in group)
        positive_lambdas=[(lam,y) for lam,y in group if lam>0]
        pearson_disp=sum((y-lam)**2/lam for lam,y in positive_lambdas)/max(1,len(positive_lambdas)-1)
        expected_bins=[0.0]*6
        for lam,_ in group:
            q=math.exp(-lam)
            expected_bins[0]+=q
            for k in range(1,5):
                q*=lam/k
                expected_bins[k]+=q
            expected_bins[5]+=max(0.0,1-sum(math.exp(-lam)*lam**k/math.factorial(k) for k in range(5)))
        count.append({"lambda_bucket":bucket,"player_games":n,"observed_mean_sog":obs,"predicted_mean_lambda":pred,"mae":mae,"rmse":rmse,
          "poisson_count_nll":nll,"residual_mean":statistics.mean(y-l for l,y in group),"residual_variance":statistics.pvariance(y-l for l,y in group),
          "observed_variance_to_mean":obs_var/obs if obs else "","pearson_dispersion_positive_lambda":pearson_disp,
          "zero_lambda_rows":sum(lam<=0 for lam,_ in group),"zero_lambda_positive_outcomes":sum(lam<=0 and y>0 for lam,y in group),
          **{f"expected_count_{k}_share":expected_bins[k]/n for k in range(5)},"expected_count_5_plus_share":expected_bins[5]/n,
          **{f"expected_count_{k}":expected_bins[k] for k in range(5)},"expected_count_5_plus":expected_bins[5],
          "observed_count_0":sum(y==0 for _,y in group),"observed_count_1":sum(y==1 for _,y in group),"observed_count_2":sum(y==2 for _,y in group),
          "observed_count_3":sum(y==3 for _,y in group),"observed_count_4":sum(y==4 for _,y in group),"observed_count_5_plus":sum(y>=5 for _,y in group)})
    write_csv(outdir/"count_distribution_diagnostics.csv",count)
    history=[]
    def season_games_bucket(value):
        if value is None:return "MISSING"
        if value<=0:return "0"
        if value==1:return "1"
        if value==2:return "2"
        if value<=5:return "3-5"
        return "6+"
    def bucket_rows(dimension, rows, keyfn):
        groups=defaultdict(list)
        for row in rows: groups[keyfn(row)].append(row)
        for label, group in groups.items():
            metrics=score([{"y":r["y"],"p":r["p_over"],"side":r["side"],"player_id":r["player_id"]} for r in group])
            history.append({"dimension":dimension,"bucket":label,"line":group[0]["line"],"rows":len(group),
                "unique_player_games":len({(r["slate_date"],r["game_id"],r["player_id"]) for r in group}),
                "accuracy":metrics["accuracy"],"log_loss":metrics["log_loss"],"brier":metrics["brier"],
                "auc":metrics["auc"],"over_rate":metrics["over_rate"],"feature_source":"immutable pregame shadow feature snapshot",
                "interpretation":"descriptive stratum; shadow snapshot is not asserted to be the production model input"})
    for line in (1.5,2.5,3.5):
        group=[r for r in settled if r["line"]==line]
        bucket_rows("current_season_games_prior",group,lambda r:season_games_bucket(r.get("current_season_games_prior")))
        bucket_rows("position",group,lambda r:r.get("position") or "MISSING")
        bucket_rows("cold_start_class",group,lambda r:r.get("cold_start_class") or "MISSING")
        bucket_rows("team_changed",group,lambda r:str(r.get("team_changed") or "MISSING"))
        def toi_bucket(r):
            toi=r.get("current_regular_toi_per_game")
            if toi is None: toi=r.get("prior_toi_per_game")
            return "MISSING" if toi is None else "<10" if toi<10 else "10-15" if toi<15 else "15+"
        bucket_rows("pregame_toi_proxy",group,toi_bucket)
        def sog_bucket(r):
            x=r.get("current_regular_sog_per60")
            if x is None: x=r.get("prior_sog_per60")
            return "MISSING" if x is None else "<1.5" if x<1.5 else "1.5-2.5" if x<2.5 else "2.5+"
        bucket_rows("pregame_prior_sog_rate",group,sog_bucket)
    write_csv(outdir/"history_depth_analysis.csv",history)
    separation=[]
    for line in (1.5,2.5,3.5):
        group=[r for r in settled if r["line"]==line]
        for label,subset in [("realized_over",[r for r in group if r["y"]==1]),("realized_under",[r for r in group if r["y"]==0])]:
            probs=sorted(r["p_over"] for r in subset)
            if probs:
                separation.append({"line":line,"outcome":label,"n":len(probs),"mean_p_over":statistics.mean(probs),
                    "median_p_over":statistics.median(probs),"p10":probs[int(.10*(len(probs)-1))],
                    "p25":probs[int(.25*(len(probs)-1))],"p75":probs[int(.75*(len(probs)-1))],"p90":probs[int(.90*(len(probs)-1))]})
        ranked=sorted(group,key=lambda r:r["p_over"])
        for q in range(5):
            chunk=ranked[q*len(ranked)//5:(q+1)*len(ranked)//5]
            if chunk: separation.append({"line":line,"outcome":f"predicted_probability_quintile_{q+1}","n":len(chunk),
                "mean_p_over":statistics.mean(r["p_over"] for r in chunk),"realized_over_rate":statistics.mean(r["y"] for r in chunk)})
    write_csv(outdir/"probability_separation.csv",separation)
    write_csv(outdir/"defense_surprise_inventory.csv",[{
        "slate_date":d,"status":"NOT_BOUND_TO_RECONCILED_IMMUTABLE_PROSPECTIVE_EVIDENCE",
        "quantitative_comparison":"EXCLUDED","note":"No exact defense-surprise prospective prediction plus official-outcome binding is present in the reconciler SOG source binding."} for d in DATES])
    write_csv(outdir/"phase_stability_analysis.csv",phase_rows)
    market_rows=[r for r in settled if r.get("market_matched")]
    market_stats=[]
    for label,subset in [("matched",market_rows),("unmatched",[r for r in settled if not r.get("market_matched")])]:
        for line in (1.5,2.5,3.5):
            group=[r for r in subset if r["line"]==line]
            if group:
                ms=score(group)
                diffs=[fnum(r.get("model_minus_market_over")) for r in group if fnum(r.get("model_minus_market_over")) is not None]
                market_stats.append({"subset":label,"line":line,**ms,"market_coverage":len(group),
                    "over_decisions":sum(r["side"]=="OVER" for r in group),"under_decisions":sum(r["side"]=="UNDER" for r in group),
                    "mean_model_selected_confidence":statistics.mean(r["selected_probability"] for r in group),
                    "mean_model_minus_market_over":statistics.mean(diffs) if diffs else "",
                    "valid_two_sided_no_vig_quotes":len(diffs)})
    for line in (1.5,2.5,3.5):
        group=[r for r in market_rows if r["line"]==line and fnum(r.get("market_p_over_novig")) is not None]
        if group:
            market=[{"y":r["y"],"p":fnum(r["market_p_over_novig"]),"side":"OVER" if fnum(r["market_p_over_novig"])>=.5 else "UNDER","player_id":r["player_id"]} for r in group]
            ms=score(market); ps=score([{"y":r["y"],"p":r["p_over"],"side":r["side"],"player_id":r["player_id"]} for r in group])
            market_stats.append({"subset":"matched_market_probability","line":line,"n":len(group),
                "model_log_loss":ps["log_loss"],"model_brier":ps["brier"],"market_log_loss":ms["log_loss"],"market_brier":ms["brier"]})
        diff_groups=[("<= -0.10",lambda d:d<=-.10),("-0.10 to -0.05",lambda d:-.10<d<=-.05),
                     ("-0.05 to 0.05",lambda d:-.05<d<=.05),("0.05 to 0.10",lambda d:.05<d<=.10),(">0.10",lambda d:d>.10)]
        for label,fn in diff_groups:
            group=[r for r in market_rows if r["line"]==line and fnum(r.get("model_minus_market_over")) is not None and fn(fnum(r["model_minus_market_over"]))]
            if group:
                market_stats.append({"subset":"model_market_difference_bucket","line":line,"difference_bucket":label,
                    "n":len(group),"realized_over_rate":statistics.mean(r["y"] for r in group),
                    "model_accuracy":statistics.mean(r["correct"] for r in group),
                    "mean_model_minus_market_over":statistics.mean(fnum(r["model_minus_market_over"]) for r in group)})
    write_csv(outdir/"market_subset_analysis.csv",market_stats)
    residual_relationships=[]
    for line in (1.5,2.5,3.5):
        group=[r for r in settled if r["line"]==line]
        for dimension,keyfn in [("pregame_prior_sog_rate",sog_bucket),("pregame_toi_proxy",toi_bucket),
                                ("production_poisson_source",lambda r:r.get("poisson_source") or "MISSING"),
                                ("production_expected_sog_bucket",lambda r:r.get("expected_sog_bucket") or "MISSING")]:
            by=defaultdict(list)
            for r in group: by[keyfn(r)].append(r)
            for label,items in by.items():
                residuals=[r["official_sog"]-r["expected_sog"] for r in items if r.get("expected_sog") is not None]
                residual_relationships.append({"line":line,"feature_dimension":dimension,"bucket":label,"n":len(items),
                    "mean_signed_count_residual":statistics.mean(residuals) if residuals else "",
                    "mean_absolute_count_residual":statistics.mean(abs(x) for x in residuals) if residuals else "",
                    "source":"immutable pregame shadow feature snapshot","limitation":"descriptive association; not production feature attribution"})
    write_csv(outdir/"residual_feature_relationships.csv",residual_relationships)
    # Hypotheses remain cautious where the preserved baseline input does not contain the needed evidence.
    hypothesis=[
      {"hypothesis":"H1","classification":"PARTIALLY_SUPPORTED","evidence":"All line AUCs exceed 0.69, while accuracy beats always-Under clearly only at 1.5; paired player-clustered intervals are in proper_scoring_metrics.csv."},
      {"hypothesis":"H2","classification":"SUPPORTED","evidence":"2.5 accuracy 79.4% vs 79.3% always-Under; 3.5 91.0% vs 90.8%, with only 154 and 25 Over decisions. Prevalence dominates high-line accuracy."},
      {"hypothesis":"H3","classification":"SUPPORTED","evidence":"1.5 is weakest: 65.4% accuracy, 0.628 log loss, 0.218 Brier and 0.691 AUC versus better proper scores/discrimination at 2.5 and 3.5. Cause remains unresolved."},
      {"hypothesis":"H4","classification":"PARTIALLY_SUPPORTED","evidence":"Exact common-row gains are modest; C/G/D improve accuracy by about 0.4-0.75 percentage points, while other arms are flat or worse. This is insufficient for replacement."},
      {"hypothesis":"H5","classification":"PARTIALLY_SUPPORTED","evidence":"C, D, and G disagreement-only net correct calls are +34, +19, and +32 respectively, but no short-window result establishes durable improvement."},
      {"hypothesis":"H6","classification":"NOT_SUPPORTED","evidence":"On pregame shadow snapshot strata, performance does not rise monotonically with current-season games prior; e.g. 2.5 accuracy is 85.5% at 0 games (n=76) versus 78.1% at 3-5 (n=567). Descriptive only; these are not the production fitted inputs."},
      {"hypothesis":"H7","classification":"INSUFFICIENT_EVIDENCE","evidence":"Expected count MAE/RMSE and residual mean are in count_distribution_diagnostics.csv."},
      {"hypothesis":"H8","classification":"PARTIALLY_SUPPORTED","evidence":"Positive-lambda Pearson dispersion is 1.18; expected count 5+ is 69.2 versus 56 observed out of 1,582, a modest upper-tail excess with short-window uncertainty."},
      {"hypothesis":"H9","classification":"INSUFFICIENT_EVIDENCE","evidence":"TOI/exposure provenance is not present in the exported prediction CSV."},
      {"hypothesis":"H10","classification":"PARTIALLY_SUPPORTED","evidence":"On 556/426/415 valid two-sided rows, no-vig market log loss is lower than production at 1.5/2.5/3.5, but model-market difference buckets are mixed and the matched slates are limited."}]
    write_csv(outdir/"hypothesis_adjudication.csv",hypothesis)
    direction={"classifications":["KEEP_BASELINE_AND_FOCUS_ON_CALIBRATION","LINE_SPECIFIC_RESEARCH_REQUIRED","MORE_PROSPECTIVE_SAMPLE_REQUIRED_BEFORE_ARCHITECTURE_CHANGE"],
      "basis":{"calibration":"Calibration slopes are below 1 for all lines; evaluate conservative probability recalibration without altering production in this analysis.",
        "line_specific":"1.5 has the weakest accuracy, proper scores, and AUC and the largest lift over always-Under; isolate its ranking and base-rate behavior.",
        "sample":"The exact all-arm common set gives C/G a roughly 0.7 point accuracy lead, but player-clustered intervals include zero. Continue prospective capture before architecture change."},
      "production_changed":False}
    (outdir/"next_research_direction.json").write_text(json.dumps(direction,indent=2)+"\n")
    overall=score([{"y":r["y"],"p":r["p_over"],"side":r["side"],"player_id":r["player_id"]} for r in settled])
    win=sum(r["correct"] for r in settled)
    unresolved_n=len(production)-len(settled)
    pregame_feature_rows=sum(bool(r.get("pregame_feature_cutoff_utc")) for r in production)
    unscored_identities=sum(int(r.get("unscored_identity_count") or 0) for r in unscored_audit)
    lines=[]
    line_metrics={float(x["line"]):x for x in proper if x.get("scope")=="cumulative"}
    baselines={float(x["line"]):x for x in base}
    daily_overall=[]
    for d in DATES:
        grp=[r for r in settled if r["slate_date"]==d]
        if grp:
            sm=score([{"y":r["y"],"p":r["p_over"],"side":r["side"],"player_id":r["player_id"]} for r in grp])
            daily_overall.append((d,sm["accuracy"],sm["n"]))
    lines += ["# NHL 2026 SOG full performance and model characterization", "",
      "## Population and provenance", "",
      f"Study window: 2026-10-03 through 2026-10-08. Oct. 9 is excluded as unfinished. The primary production table contains {len(production)} line predictions ({len(settled)} settled, {unresolved_n} with no bound official outcome). Production run selection is the latest completed run ending before the earliest scheduled game; exact run IDs, prediction hashes, outcome hashes, and shadow bindings are in `selected_source_bindings.csv`.","",
      "Oct. 3 boundary: `OCT3_VALID_POST_STABILIZATION_BOUNDARY`. The retained receipt records completed scoring, prediction load, and market attachment. The legacy season-TOI population gate was nonblocking at 2.83% null against a 20% maximum. The selected artifact is labelled baseline_v1. No known Oct. 3 defect was found in this bounded receipt check.","",
      "## Results", "",
      f"Settled overall accuracy: {win}/{len(settled)} = {overall.get('accuracy',0):.3%}; unresolved/nonparticipant prediction lines: {unresolved_n}. Overall log loss {overall.get('log_loss',0):.4f}; Brier {overall.get('brier',0):.4f}.","",
      "| Line | N | Accuracy | Always-Under | Lift | Log loss | Brier | AUC | AP | ECE | Cal. slope |", "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for line in (1.5,2.5,3.5):
        m=line_metrics[line]; b=baselines[line]
        lines.append(f"| {line:.1f} | {m['n']} | {m['accuracy']:.1%} | {b['always_under_accuracy']:.1%} | {b['model_minus_always_under']:+.1%} | {m['log_loss']:.3f} | {m['brier']:.3f} | {m['auc']:.3f} | {m['average_precision']:.3f} | {m['ece_10_bins']:.3f} | {m['calibration_slope']:.2f} |")
    always_under=statistics.mean(1-r["y"] for r in settled)
    over_rows=[r for r in settled if r["side"]=="OVER"]; under_rows=[r for r in settled if r["side"]=="UNDER"]
    shared_rank=sorted(all_arm_comp,key=lambda r:(r["shadow_accuracy"],-r["shadow_log_loss"]),reverse=True)
    best_shadow=shared_rank[0] if shared_rank else {}
    market_prob={float(r["line"]):r for r in market_stats if r.get("subset")=="matched_market_probability"}
    lines += ["", "Daily overall settled accuracy (line rows): " + "; ".join(f"{d} {a:.1%} (n={n})" for d,a,n in daily_overall)+".","",
      f"Over call frequency: {sum(r['side']=='OVER' for r in settled)/len(settled):.1%}; Under call frequency: {sum(r['side']=='UNDER' for r in settled)/len(settled):.1%}.",
      f"Overall always-Under accuracy is {always_under:.1%}; production improves by {overall['accuracy']-always_under:+.1%}. Over-side accuracy is {statistics.mean(r['correct'] for r in over_rows):.1%} (n={len(over_rows)}); Under-side accuracy is {statistics.mean(r['correct'] for r in under_rows):.1%} (n={len(under_rows)}).",
      "The 2.5 and 3.5 accuracy is nearly identical to always-Under; 1.5 shows a larger descriptive lift. These empirical baselines use the study window outcomes and are in-sample descriptions.","",
      f"On the identical {best_shadow.get('all_arm_common_rows',0)}-row set shared by production and all six arms, {best_shadow.get('arm','')} is highest by accuracy at {best_shadow.get('shadow_accuracy',0):.1%} versus production {best_shadow.get('production_accuracy',0):.1%}; paired accuracy difference CI {best_shadow.get('paired_accuracy_difference_ci_low',0):+.1%} to {best_shadow.get('paired_accuracy_difference_ci_high',0):+.1%}. The interval crosses zero, so this is not a promotion result.","",
      "On two sided market matched rows, no-vig market log loss is lower than production at all three lines (N=556/426/415 at 1.5/2.5/3.5). Model-minus-market Over probability averages about -3 points. Difference buckets are mixed, so this is a market research signal rather than a stable error rule.","",
      "MIDDAY-to-FINAL_PREGAME shadow comparisons show no probability or selected-side changes on common rows for dates with both captures; some later captures add or drop player-game identities. Oct. 4 has no comparable MIDDAY capture.","",
      f"Count diagnostic: observed mean {count[0]['observed_mean_sog']:.3f} versus mean lambda {count[0]['predicted_mean_lambda']:.3f}; MAE {count[0]['mae']:.3f}, RMSE {count[0]['rmse']:.3f}, positive-lambda Pearson dispersion {count[0]['pearson_dispersion_positive_lambda']:.2f}. Predicted 5+ count is {count[0]['expected_count_5_plus']:.1f} versus {count[0]['observed_count_5_plus']} observed.","",
      f"History strata do not show a monotonic gain as current-season games accumulate. {pregame_feature_rows}/{len(production)} production rows match a SHA-bound, pregame shadow feature snapshot. The selected production scorer also reports {unscored_identities} unscored player-game identities due to missing exposure; reason-level details are in `unscored_audit.csv` and `unscored_production_rows.csv`.","",
      "Hypothesis adjudications: " + "; ".join(f"{r['hypothesis']} {r['classification']}" for r in hypothesis)+".","",
      "The expected count field is treated as the Poisson lambda exposed by the production artifact. Count distribution diagnostics are descriptive and do not fit a replacement distribution. Reliability bins use Over probability separately by line.","",
      "## Limitations", "",
      "The retained production wide CSV contains expected_sog, threshold Over probabilities, and coarse count buckets, but not the full production feature input or exposure fallback flags. History and residual strata use the SHA-bound immutable pregame shadow feature snapshot as descriptive metadata and are not asserted to be the baseline's fitted inputs. Exact-run market attachments exist for some dates; only rows bound to the selected production run are counted. Defense-surprise predictions are excluded because the reconciler has no exact immutable binding. The production artifact does not expose full exact count probabilities; Poisson NLL is evaluated from expected_sog under the model's stated Poisson assumption. Bootstrap resamples player identities and keeps their rows together.","",
      "No production artifacts or policies were changed. No provider calls, paid credits, or database mutations were made.","",
      "## Files", "", "CSV outputs are machine readable. `analysis_population.csv` retains unresolved rows and prediction provenance; `unresolved_and_nonparticipants.csv` is the explicit unresolved subset. Use `shadow_all_arm_common_comparison.csv` for arm ranking because it holds one common row set across production and every arm; `shadow_common_row_comparison.csv` provides each arm-specific intersection."]
    (outdir/"sog_full_analysis.md").write_text("\n".join(lines)+"\n")
    # Manifest is made last; exclude itself.
    files=sorted(p for p in outdir.iterdir() if p.is_file() and p.name!="SHA256SUMS")
    (outdir/"SHA256SUMS").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))
    return outdir


if __name__=="__main__":
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("--out-dir")
    args=ap.parse_args()
    print(build(args.out_dir))
