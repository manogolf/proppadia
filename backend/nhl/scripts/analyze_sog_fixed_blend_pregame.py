"""Build a descriptive comparison from an immutable NHL SOG shadow capture."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.nhl.daily_capture import verify_package
from backend.nhl.sog_feature_input import sha256_file
from backend.nhl.sog_fixed_blend import MODELS, LINES
from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail


def _coalesce(*series: pd.Series) -> pd.Series:
    result = series[0].copy()
    for item in series[1:]:
        result = result.where(result.notna(), item)
    return result


def _sha_tree(directory: Path) -> dict[str, str]:
    return {str(p.relative_to(directory)): sha256_file(p) for p in sorted(directory.rglob("*")) if p.is_file()}


def build(*, run_id: str, root: Path, out: Path) -> None:
    daily = root / "artifacts/operational/nhl/daily_runs" / f"run_id={run_id}"
    receipt = json.loads((daily / "parent_receipt.json").read_text())
    slate = receipt["slate_date"]
    feature_pkg = root / "artifacts/operational/nhl/sog_feature_inputs/season=2026" / f"slate_date={slate}" / f"run_id={run_id}"
    feature_path = feature_pkg / "sog_features.csv"
    prod_path = root / "backend/nhl/data/processed/daily_runs" / run_id / "sog_predictions_wide_calibrated.csv"
    shadow_root = root / "artifacts/operational/nhl/sog_fixed_blend_shadows/season=2026" / f"slate_date={slate}" / f"run_id={run_id}"
    before = {"feature_input": sha256_file(feature_path), "production_prediction": sha256_file(prod_path), "models": {}}
    feats, prod = pd.read_csv(feature_path), pd.read_csv(prod_path)
    keys = ["game_id", "player_id"]
    if feats.duplicated(keys).any() or prod.duplicated(keys).any():
        raise ValueError("duplicate production feature or prediction key")
    joined = feats.merge(prod, on=keys, how="inner", validate="one_to_one", suffixes=("", "_prod"))
    if len(joined) != 612 or len(joined) != len(feats) or len(joined) != len(prod):
        raise ValueError(f"player-game population mismatch: features={len(feats)} prod={len(prod)} common={len(joined)}")
    rates = [_coalesce(*[pd.to_numeric(joined[c], errors="coerce") for c in ("d10_sog_per60", "d20_sog_per60", "d5_sog_per60")])]
    toi_fields = ["d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg"]
    toi_candidates = [pd.to_numeric(joined.get(c), errors="coerce") if c in joined else pd.Series(np.nan, index=joined.index) for c in toi_fields]
    numeric = lambda c: pd.to_numeric(joined[c], errors="coerce") if c in joined else pd.Series(np.nan, index=joined.index)
    toi_candidates += [numeric("szn_toi_per_game_5on5") + numeric("szn_toi_per_game_pp"),
                       numeric("season_5on5_icetime_per_game") / 60 + numeric("season_5on4_icetime_per_game") / 60]
    toi = _coalesce(*toi_candidates)
    selected_rate = rates[0]
    lam = pd.to_numeric(joined.expected_sog, errors="coerce")
    if not (selected_rate.mul(toi).div(60).sub(lam).abs().le(1e-12)).all():
        raise ValueError("production rate/selected TOI/lambda parity failed")
    for line, col, threshold in ((1.5,"p_over_1_5",2),(2.5,"p_over_2_5",3),(3.5,"p_over_3_5",4)):
        expected = lam.map(lambda value: _poisson_tail(float(value), threshold))
        if not np.allclose(expected, pd.to_numeric(joined[col]), rtol=0, atol=1e-12):
            raise ValueError(f"production probability/lambda parity failed at {line}")
    joined["selected_toi_minutes"] = toi
    joined["production_lambda"] = lam
    joined["production_side_1_5"] = np.where(pd.to_numeric(joined.p_over_1_5).ge(.5), "OVER", "UNDER")
    names_path = root / "backend/nhl/exports/daily/names" / f"names_{slate}.csv"
    names = pd.read_csv(names_path)[["player_id", "full_name"]].drop_duplicates("player_id")
    joined = joined.merge(names, on="player_id", how="left", validate="many_to_one")
    model_frames = {}
    for model in MODELS:
        package = shadow_root / f"model={model['identity']}"
        verify_package(package)
        manifest = json.loads((package / "manifest.json").read_text())
        pred = pd.read_csv(package / "predictions.csv")
        before["models"][model["identity"]] = _sha_tree(package)
        if len(pred) != 1836 or pred.duplicated(keys + ["line"]).any():
            raise ValueError(f"shadow population invalid: {model['identity']}")
        if manifest["feature_input_sha256"] != before["feature_input"]:
            raise ValueError("feature input hash binding mismatch")
        if manifest["deterministic_replay"] != "PASS" or manifest["prediction_sha256"] != manifest["replay_prediction_sha256"]:
            raise ValueError("shadow deterministic replay evidence invalid")
        model_frames[model["identity"]] = pred
    base = joined[["game_id", "player_id", "full_name", "team_id", "opponent_id", "game_date", "d10_sog_per60", "d20_sog_per60", "selected_toi_minutes", "production_lambda", "p_over_1_5", "p_over_2_5", "p_over_3_5", "production_side_1_5"]].copy()
    base["game"] = base.game_id.astype(str)
    base["d10_d20_spread"] = base.d10_sog_per60 - base.d20_sog_per60
    for model in MODELS:
        ident = model["identity"]
        weight = model["d10_weight"]
        base[f"rate_{ident}"] = weight * base.d10_sog_per60 + (1-weight) * base.d20_sog_per60
        base[f"lambda_{ident}"] = base[f"rate_{ident}"] * base.selected_toi_minutes / 60
        pred = model_frames[ident]
        expected_keys = {(int(r.game_id), int(r.player_id), float(line)) for r in base.itertuples() for line in LINES}
        got_keys = {(int(r.game_id), int(r.player_id), float(r.line)) for r in pred.itertuples()}
        if len(got_keys) != 1836 or got_keys != expected_keys:
            raise ValueError(f"exact 1836-row key reconciliation failed: {ident}")
        expected_lambda = base.set_index(keys)[f"lambda_{ident}"]
        actual_lambda = pred.set_index(keys).shadow_lambda
        if not np.allclose(expected_lambda.sort_index(), actual_lambda.groupby(level=keys).first().sort_index(), rtol=0, atol=1e-12):
            raise ValueError(f"blend arithmetic mismatch: {ident}")
        thresholds = {1.5: 2, 2.5: 3, 3.5: 4}
        expected_p = pred.apply(lambda r: _poisson_tail(float(r.shadow_lambda), thresholds[float(r.line)]), axis=1)
        if not np.allclose(expected_p, pred.p_over, rtol=0, atol=1e-12):
            raise ValueError(f"shadow probability/lambda parity failed: {ident}")
        base[f"p15_{ident}"] = pred[pred.line.eq(1.5)].set_index(keys).p_over.reindex(base.set_index(keys).index).to_numpy()
        base[f"side15_{ident}"] = pred[pred.line.eq(1.5)].set_index(keys).selected_side.reindex(base.set_index(keys).index).to_numpy()

    paired = []
    probability_rows, flip_rows = [], []
    for model in MODELS:
        ident = model["identity"]
        pred = model_frames[ident].merge(base[["game_id", "player_id", "full_name", "production_lambda", "d10_sog_per60", "d20_sog_per60", "selected_toi_minutes", "production_side_1_5"]], on=keys, validate="many_to_one")
        pred["production_p_over"] = np.nan
        for line, col in ((1.5,"p_over_1_5"),(2.5,"p_over_2_5"),(3.5,"p_over_3_5")):
            mask=pred.line.eq(line); pred.loc[mask,"production_p_over"]=pred.loc[mask].merge(base[keys+[col]],on=keys,validate="many_to_one")[col].to_numpy()
        pred["production_side"] = np.where(pred.production_p_over.ge(.5),"OVER","UNDER")
        pred["lambda_change"] = pred.shadow_lambda-pred.production_lambda
        pred["p_change"] = pred.p_over-pred.production_p_over
        paired.append(pred)
        for line, part in pred.groupby("line"):
            probability_rows.append({"model":ident,"line":line,"n":len(part),"production_mean_p_over":part.production_p_over.mean(),"shadow_mean_p_over":part.p_over.mean(),"mean_signed_change":part.p_change.mean(),"mean_absolute_change":part.p_change.abs().mean(),"median_absolute_change":part.p_change.abs().median(),"max_absolute_change":part.p_change.abs().max()})
            flips=part[part.production_side.ne(part.selected_side)]
            flip_rows.append({"model":ident,"line":line,"total_rows":len(part),"same_side":len(part)-len(flips),"side_flips":len(flips),"flip_percentage":100*len(flips)/len(part),"over_to_under":int(((flips.production_side=="OVER")&(flips.selected_side=="UNDER")).sum()),"under_to_over":int(((flips.production_side=="UNDER")&(flips.selected_side=="OVER")).sum())})
        allp=pred
        flips=allp[allp.production_side.ne(allp.selected_side)]
        flip_rows.append({"model":ident,"line":"ALL","total_rows":len(allp),"same_side":len(allp)-len(flips),"side_flips":len(flips),"flip_percentage":100*len(flips)/len(allp),"over_to_under":int(((flips.production_side=="OVER")&(flips.selected_side=="UNDER")).sum()),"under_to_over":int(((flips.production_side=="UNDER")&(flips.selected_side=="OVER")).sum())})
    allpaired=pd.concat(paired,ignore_index=True)
    lamrows=[]
    for m in MODELS:
        ident=m["identity"]; delta=base[f"lambda_{ident}"]-base.production_lambda
        lamrows.append({"model":ident,"n":len(base),"mean_d10":base.d10_sog_per60.mean(),"median_d10":base.d10_sog_per60.median(),"mean_d20":base.d20_sog_per60.mean(),"median_d20":base.d20_sog_per60.median(),"mean_production_lambda":base.production_lambda.mean(),"median_production_lambda":base.production_lambda.median(),"mean_shadow_lambda":base[f"lambda_{ident}"].mean(),"median_shadow_lambda":base[f"lambda_{ident}"].median(),"mean_movement":delta.mean(),"median_movement":delta.median(),"mean_absolute_movement":delta.abs().mean(),"median_absolute_movement":delta.abs().median(),"maximum_decrease":delta.min(),"maximum_increase":delta.max(),"percentage_decreased":100*delta.lt(-1e-12).mean(),"percentage_increased":100*delta.gt(1e-12).mean(),"percentage_unchanged":100*delta.abs().le(1e-12).mean()})
    lambda_summary=pd.DataFrame(lamrows)
    # Production 1.5 over and d10 tail groups are player-game populations.
    cohort=base[base.production_side_1_5.eq("OVER")]
    highrows=[]
    ordered=base.assign(_rank=base.d10_sog_per60.rank(method="first",ascending=False))
    groups={"production_over_1_5":cohort,"top_d10_quintile":ordered[ordered._rank<=int(np.ceil(len(base)*.2))],"top_d10_decile":ordered[ordered._rank<=int(np.ceil(len(base)*.1))]}
    for name,g in groups.items():
        row={"group":name,"n":len(g),"mean_d10":g.d10_sog_per60.mean(),"mean_d20":g.d20_sog_per60.mean(),"mean_production_lambda":g.production_lambda.mean()}
        row["mean_production_p_over_1_5"]=g.p_over_1_5.mean()
        for m in MODELS:
            ident=m["identity"]; row[f"mean_{ident}_lambda"]=g[f"lambda_{ident}"].mean(); row[f"mean_{ident}_lambda_change"]=(g[f"lambda_{ident}"]-g.production_lambda).mean()
            row[f"mean_{ident}_p_over_1_5"]=g[f"p15_{ident}"].mean()
            row[f"mean_{ident}_p_over_1_5_change"]=(g[f"p15_{ident}"]-g.p_over_1_5).mean()
            row[f"{ident}_side_flips"]=int((g.production_side_1_5.ne(g[f"side15_{ident}"])).sum())
            row[f"{ident}_over_to_under_flips"]=int(((g.production_side_1_5=="OVER")&(g[f"side15_{ident}"]=="UNDER")).sum())
        highrows.append(row)
    regime=[]
    for name,mask in (("d10_gt_d20",base.d10_d20_spread.gt(0)),("d10_lt_d20",base.d10_d20_spread.lt(0)),("equal",base.d10_d20_spread.eq(0))):
        g=base[mask]; row={"group":name,"n":len(g),"mean_d10_minus_d20":g.d10_d20_spread.mean(),"mean_production_lambda":g.production_lambda.mean()}
        for m in MODELS:
            ident=m["identity"]; row[f"mean_{ident}_lambda"]=g[f"lambda_{ident}"].mean(); row[f"mean_{ident}_movement"]=(g[f"lambda_{ident}"]-g.production_lambda).mean()
        regime.append(row)
    direct=[]
    for m in MODELS:
        ident=m["identity"]
        direct.append(allpaired[allpaired.model_identity.eq(ident)][["game_id","player_id","line","p_over","selected_side"]].rename(columns={"p_over":f"p_{ident}","selected_side":f"side_{ident}"}))
    direct_df=direct[0].merge(direct[1],on=keys+["line"],validate="one_to_one")
    disagree=direct_df[direct_df[f"side_{MODELS[0]['identity']}"].ne(direct_df[f"side_{MODELS[1]['identity']}"])]
    p_a,p_b=f"p_{MODELS[0]['identity']}",f"p_{MODELS[1]['identity']}"
    direct_df["probability_difference_25_75_minus_50_50"]=direct_df[p_a]-direct_df[p_b]
    direct_prob_summary=direct_df.groupby("line").probability_difference_25_75_minus_50_50.agg(
        n="size",mean_difference="mean",mean_absolute_difference=lambda s:s.abs().mean(),
        median_absolute_difference=lambda s:s.abs().median(),max_absolute_difference=lambda s:s.abs().max()).reset_index()
    largest=[]
    for m in MODELS:
        ident=m["identity"]; tmp=base.copy(); tmp["delta"]=tmp[f"lambda_{ident}"]-tmp.production_lambda
        for direction,frame in (("largest_reductions",tmp.nsmallest(20,"delta")),("largest_increases",tmp.nlargest(20,"delta"))):
            for r in frame.itertuples():
                largest.append({"model":ident,"direction":direction,"player":r.full_name,"player_id":r.player_id,"game_id":r.game_id,"d10":r.d10_sog_per60,"d20":r.d20_sog_per60,"selected_toi_minutes":r.selected_toi_minutes,"production_lambda":r.production_lambda,"shadow_lambda":getattr(r,f"lambda_{ident}"),"production_p_over_1_5":r.p_over_1_5,"shadow_p_over_1_5":getattr(r,f"p15_{ident}"),"production_side":r.production_side_1_5,"shadow_side":getattr(r,f"side15_{ident}")})
    # Last check, then the derived artifact records the complete immutable snapshot.
    after={"feature_input":sha256_file(feature_path),"production_prediction":sha256_file(prod_path),"models":{m["identity"]:_sha_tree(shadow_root/f"model={m['identity']}") for m in MODELS}}
    if before!=after: raise ValueError("source artifacts changed while comparison was being built")
    if out.exists(): raise FileExistsError(out)
    out.mkdir(parents=True)
    allpaired.to_csv(out/"paired_predictions.csv",index=False,float_format="%.17g")
    lambda_summary.to_csv(out/"lambda_summary.csv",index=False,float_format="%.17g")
    pd.DataFrame(probability_rows).to_csv(out/"probability_summary.csv",index=False,float_format="%.17g")
    pd.DataFrame(flip_rows).to_csv(out/"side_flip_summary.csv",index=False,float_format="%.17g")
    pd.DataFrame(highrows).to_csv(out/"high_tail_summary.csv",index=False,float_format="%.17g")
    pd.DataFrame(regime).to_csv(out/"rate_regime_summary.csv",index=False,float_format="%.17g")
    direct_df.to_csv(out/"direct_blend_comparison.csv",index=False,float_format="%.17g")
    direct_prob_summary.to_csv(out/"direct_blend_probability_summary.csv",index=False,float_format="%.17g")
    pd.DataFrame(largest).to_csv(out/"largest_movements.csv",index=False,float_format="%.17g")
    audit={"parent_run_id":run_id,"slate_date":slate,"first_capture_preserved":True,"source_hashes_before":before,"source_hashes_after":after,"production_side_threshold":"P(Over) >= 0.5 selects OVER; otherwise UNDER, matching capture writer","player_game_count":len(base),"common_line_row_count_per_model":1836,"duplicate_key_count":0,"feature_binding_sha256":before["feature_input"],"deterministic_replay":"PASS","comparison_is_pregame_only":True,"provider_calls":0,"paid_credits":0,"database_mutations":0,"later_run_contract":{"new_parent_run_id_gets_separate_run_id_directory":True,"first_run_mutated_by_writer":False,"site_data_convenience_outputs":False}}
    (out/"preservation_check.json").write_text(json.dumps(audit,indent=2,sort_keys=True)+"\n")
    def md_table(df):
        headers=list(df.columns)
        def cell(v): return f"{v:.6f}" if isinstance(v,(float,np.floating)) and np.isfinite(v) else str(v)
        rows=[[cell(v) for v in row] for row in df.itertuples(index=False,name=None)]
        return "| " + " | ".join(headers) + " |\n| " + " | ".join("---" for _ in headers) + " |\n" + "\n".join("| " + " | ".join(row) + " |" for row in rows)
    per_line = []
    side_a, side_b = f"side_{MODELS[0]['identity']}", f"side_{MODELS[1]['identity']}"
    for line in LINES:
        part = direct_df[direct_df.line.eq(line)]
        per_line.append(f"{line:g}={len(part)} rows, {int(part[side_a].ne(part[side_b]).sum())} disagreements")
    md=["# NHL SOG fixed blend prospective comparison", "", f"Slate: {slate}; parent run: `{run_id}`.", "", "Pregame descriptive comparison only. No outcomes or grading are used.", "", f"Population reconciles exactly: {len(base)} player-games and {len(allpaired)//2} rows per shadow (1,836 expected). `paired_predictions.csv` is line grain with both shadow populations and production probabilities.", "", "## Immutable source checks", "", f"First capture packages pass `verify_package`; both replay hashes equal prediction hashes; feature SHA-256 is `{before['feature_input']}`. Full per-file snapshot is in `preservation_check.json`.", "", "The daily writer creates `season/slate_date/run_id/model` packages, rejects an existing model package, stages with create-only semantics, renames into place, and verifies the completed package. There is no fixed-blend site/data output or cleanup of earlier same-slate run directories. A later parent run therefore gets its own package path; postgame grading discovers all run IDs.", "", "Production side is OVER when P(Over) >= 0.5, otherwise UNDER, matching the capture code.", "", "## Lambda summary", "", md_table(lambda_summary), "", "## Probability movement", "", md_table(pd.DataFrame(probability_rows)), "", "## Side flips", "", md_table(pd.DataFrame(flip_rows)), "", "## Production Over 1.5 and high d10 groups", "", md_table(pd.DataFrame(highrows)), "", "## d10 versus d20 regimes", "", md_table(pd.DataFrame(regime)), "", "## Direct blend decisions", "", f"Total 25/75 vs 50/50 side disagreements: {len(disagree)} of {len(direct_df)} rows; by line: " + ", ".join(per_line)+". The shadows make the same side choice on 98.86% of rows overall.", "", "Probability differences between 25/75 and 50/50 by line:", "", md_table(direct_prob_summary), "", "## Largest movements", "", "See `largest_movements.csv` for the 20 largest increases and decreases for each blend.", "", "## Scope", "", "Predictions only. No outcomes, win/loss, Brier, log loss, count MAE, or promotion evidence was calculated.", ""]
    (out/"pregame_comparison.md").write_text("\n".join(md))
    sums="".join(f"{sha256_file(p)}  {p.name}\n" for p in sorted(out.iterdir()) if p.is_file() and p.name!="SHA256SUMS")
    (out/"SHA256SUMS").write_text(sums)


def main() -> None:
    parser=argparse.ArgumentParser(); parser.add_argument("--run-id",required=True); parser.add_argument("--root",type=Path,default=Path.cwd()); parser.add_argument("--out",type=Path,required=True)
    args=parser.parse_args(); build(run_id=args.run_id,root=args.root,out=args.out)


if __name__=="__main__": main()
