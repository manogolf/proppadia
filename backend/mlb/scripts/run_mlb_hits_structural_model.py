#!/usr/bin/env python3
"""Research-only rolling-origin structural MLB Hits model.

This model never reads a model artifact or market probability while producing
baseball probabilities.  PA, pitcher exposure, per-PA hit probability, and the
coherent hit-count distribution are explicit, separately auditable layers.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
PA_VALUES = np.array([0, 1, 2, 3, 4, 5, 6], dtype=float)
EPS = 1e-6


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def clip(x):
    return np.clip(np.asarray(x, dtype=float), EPS, 1 - EPS)


def american_probability(x: float) -> float:
    return -x / (-x + 100) if x < 0 else 100 / (x + 100)


def poisson_binomial(ps: list[float], size: int = 5) -> np.ndarray:
    out = np.zeros(size, dtype=float)
    out[0] = 1.0
    for p in ps:
        nxt = np.zeros_like(out)
        nxt[0] = out[0] * (1 - p)
        for k in range(1, size - 1):
            nxt[k] = out[k] * (1 - p) + out[k - 1] * p
        nxt[-1] = out[-1] + out[-2] * p
        out = nxt
    return out / out.sum()


def rps(actual: np.ndarray, probs: np.ndarray) -> float:
    obs = np.eye(probs.shape[1])[actual]
    return float(np.mean(np.sum((np.cumsum(probs, axis=1)[:, :-1] - np.cumsum(obs, axis=1)[:, :-1]) ** 2, axis=1)))


def brier(y, p) -> float:
    return float(np.mean((np.asarray(p) - np.asarray(y)) ** 2))


def calibration_bins(frame: pd.DataFrame, probability: str, outcome: str, group: str) -> pd.DataFrame:
    x = frame[[probability, outcome]].dropna().copy()
    if x.empty:
        return pd.DataFrame()
    x["band"] = pd.cut(x[probability], np.arange(0, 1.0001, .05), include_lowest=True)
    z = x.groupby("band", observed=True).agg(rows=(outcome, "size"), mean_probability=(probability, "mean"), observed_rate=(outcome, "mean")).reset_index()
    z["group"] = group
    z["absolute_error"] = (z.mean_probability - z.observed_rate).abs()
    return z


def load_market(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    x = pd.read_csv(path, low_memory=False)
    needed = {"player_game_key", "line", "side", "price"}
    if not needed.issubset(x.columns):
        return pd.DataFrame()
    x = x[x.line.isin([.5, 1.5]) & x.side.astype(str).str.lower().isin(["over", "under"])].copy()
    if "source_capture_timestamp" in x:
        x["timestamp"] = pd.to_datetime(x.source_capture_timestamp, utc=True, errors="coerce")
        x = x.sort_values("timestamp")
    x = x.drop_duplicates(["player_game_key", "line", "side"], keep="first")
    p = x.pivot(index=["player_game_key", "line"], columns="side", values="price").reset_index()
    if not {"over", "under"}.issubset(p.columns):
        return pd.DataFrame()
    p = p.dropna(subset=["over", "under"])
    oi = p.over.astype(float).map(american_probability)
    ui = p.under.astype(float).map(american_probability)
    p["market_probability"] = oi / (oi + ui)
    return p


def prepare(path: Path) -> pd.DataFrame:
    d = pd.read_csv(path, low_memory=False)
    d["slate_date"] = d.slate_date.astype(str).str[:10]
    numeric = ["game_id", "player_id", "actual_plate_appearances", "actual_hits", "batting_order_position", "actual_lineup_position", "is_home", "season_to_date_hits_per_pa", "season_to_date_pa_per_game", "prior_game_count", "starter_prior_start_count", "starter_d15_outs_per_start", "starter_d15_hits_allowed_per_out", "team_offense_d15_hits_per_game"]
    for col in numeric:
        d[col] = pd.to_numeric(d.get(col), errors="coerce")
    d = d[d.actual_plate_appearances.notna() & d.actual_hits.notna()].copy()
    d["pa_bucket"] = d.actual_plate_appearances.clip(0, 6).astype(int)
    d["slot"] = d.batting_order_position.fillna(d.actual_lineup_position).clip(1, 9)
    d["actual_o05"] = (d.actual_hits >= 1).astype(int)
    d["actual_o15"] = (d.actual_hits >= 2).astype(int)
    return d


def pa_distribution(train: pd.DataFrame, row: pd.Series) -> np.ndarray:
    # Hierarchical Dirichlet pooling: lineup slot/home cell -> slot -> league.
    league = np.bincount(train.pa_bucket, minlength=7).astype(float) + 1.0
    league /= league.sum()
    slot = int(row.slot) if pd.notna(row.slot) else 0
    home = int(row.is_home) if pd.notna(row.is_home) else -1
    a = train[train.slot.eq(slot)] if slot else train.iloc[:0]
    b = a[a.is_home.eq(home)] if home >= 0 else a.iloc[:0]
    slot_counts = np.bincount(a.pa_bucket, minlength=7).astype(float)
    cell_counts = np.bincount(b.pa_bucket, minlength=7).astype(float)
    slot_prior = slot_counts + 30 * league
    slot_prior /= slot_prior.sum()
    result = cell_counts + 20 * slot_prior
    return result / result.sum()


def beta_mean(success: float, trials: float, league: float, strength: float) -> tuple[float, float]:
    a = max(success, 0) + league * strength
    b = max(trials - success, 0) + (1 - league) * strength
    return a / (a + b), a * b / ((a + b) ** 2 * (a + b + 1))


def structural_row(train: pd.DataFrame, row: pd.Series) -> dict:
    pap = pa_distribution(train, row)
    expected_pa = float(pap @ PA_VALUES)
    pa_sd = float(np.sqrt(pap @ ((PA_VALUES - expected_pa) ** 2)))
    league_hits = float(train.actual_hits.sum() / max(train.actual_plate_appearances.sum(), 1))
    player = train[train.player_id.eq(row.player_id)]
    batter_p, batter_var = beta_mean(player.actual_hits.sum(), player.actual_plate_appearances.sum(), league_hits, 60)

    # Pregame starter form is already strict-prior in the source spine. Convert
    # hits/out to an approximate hit/BF rate and pool heavily toward league.
    starter_raw = row.starter_d15_hits_allowed_per_out / 1.42 if pd.notna(row.starter_d15_hits_allowed_per_out) else league_hits
    starter_p = float(np.clip(.70 * batter_p + .30 * np.clip(starter_raw, .02, .45), .02, .50))
    bullpen_p = float(np.clip(.85 * batter_p + .15 * league_hits, .02, .50))
    starter_outs = float(row.starter_d15_outs_per_start) if pd.notna(row.starter_d15_outs_per_start) else 15.0
    expected_bf = np.clip(starter_outs * 1.42, 9, 30)
    starter_share = float(np.clip(expected_bf / 38.0, .25, .78))
    starter_pa = expected_pa * starter_share
    bullpen_pa = expected_pa - starter_pa

    hit_dist = np.zeros(5, dtype=float)
    for n, pn in enumerate(pap):
        if pn == 0:
            continue
        ps = [starter_share * starter_p + (1 - starter_share) * bullpen_p] * n
        hit_dist += pn * poisson_binomial(ps)
    hit_dist /= hit_dist.sum()
    return {
        **{f"pa_probability_{i if i < 6 else '6_plus'}": float(pap[i]) for i in range(7)},
        "expected_pa": expected_pa,
        "pa_uncertainty_sd": pa_sd,
        "expected_starter_facing_pa": starter_pa,
        "expected_bullpen_facing_pa": bullpen_pa,
        "starter_exposure_share": starter_share,
        "starter_per_pa_hit_probability": starter_p,
        "bullpen_per_pa_hit_probability": bullpen_p,
        "batter_true_hit_probability": batter_p,
        "per_pa_probability_variance": batter_var,
        "p_hits_0": float(hit_dist[0]), "p_hits_1": float(hit_dist[1]),
        "p_hits_2": float(hit_dist[2]), "p_hits_3": float(hit_dist[3]),
        "p_hits_4_plus": float(hit_dist[4]),
        "expected_hits": float(hit_dist @ np.array([0, 1, 2, 3, 4])),
        "hits_over_05_probability": float(1 - hit_dist[0]),
        "hits_under_05_probability": float(hit_dist[0]),
        "hits_over_15_probability": float(hit_dist[2:].sum()),
        "hits_under_15_probability": float(hit_dist[:2].sum()),
    }


def rolling_predictions(d: pd.DataFrame, minimum_dates: int) -> pd.DataFrame:
    rows = []
    for date in sorted(d.slate_date.unique()):
        train = d[d.slate_date < date]
        test = d[d.slate_date == date]
        if train.slate_date.nunique() < minimum_dates or len(train) < 500:
            continue
        league_o05 = (train.actual_hits >= 1).mean()
        league_o15 = (train.actual_hits >= 2).mean()
        for _, row in test.iterrows():
            z = structural_row(train, row)
            rate = row.season_to_date_hits_per_pa if pd.notna(row.season_to_date_hits_per_pa) else train.actual_hits.sum() / train.actual_plate_appearances.sum()
            epa = row.season_to_date_pa_per_game if pd.notna(row.season_to_date_pa_per_game) else train.actual_plate_appearances.mean()
            q = float(np.clip(rate, .01, .60)); n = int(np.clip(round(epa), 0, 6))
            control = poisson_binomial([q] * n)
            rows.append({
                "slate_date": date, "game_id": int(row.game_id), "player_id": int(row.player_id),
                "player_game_key": row.player_game_key, "player_name": row.player_name,
                "fit_cutoff": str(pd.Timestamp(date) - pd.Timedelta(days=1))[:10],
                "training_rows": len(train), "training_dates": train.slate_date.nunique(),
                "actual_plate_appearances": int(row.actual_plate_appearances), "actual_hits": int(row.actual_hits),
                "actual_o05": int(row.actual_o05), "actual_o15": int(row.actual_o15),
                "batting_order_position": row.slot, "base_rate_o05": league_o05, "base_rate_o15": league_o15,
                "opportunity_control_o05": 1 - control[0], "opportunity_control_o15": control[2:].sum(), **z,
            })
        print(f"structural {date}: train={len(train)} score={len(test)}", flush=True)
    return pd.DataFrame(rows)


def evaluate(pred: pd.DataFrame, market: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows = []
    month_rows = []
    for line, ycol, pcol, bcol, ocol in [(.5, "actual_o05", "hits_over_05_probability", "base_rate_o05", "opportunity_control_o05"), (1.5, "actual_o15", "hits_over_15_probability", "base_rate_o15", "opportunity_control_o15")]:
        for name, col in [("line_specific_expanding_base_rate", bcol), ("expected_pa_x_batter_rate_control", ocol), ("structural_baseball", pcol)]:
            y = pred[ycol].to_numpy(); p = clip(pred[col])
            rows.append({"line": line, "model": name, "rows": len(pred), "dates": pred.slate_date.nunique(), "brier": brier(y, p), "log_loss": float(log_loss(y, p)), "calibration_error": float(abs(p.mean() - y.mean())), "mean_probability": float(p.mean()), "observed_rate": float(y.mean())})
            q = pred.assign(month=pred.slate_date.str[:7], loss=(p-y)**2).groupby("month", as_index=False).agg(rows=(ycol, "size"), brier=("loss", "mean"))
            q["line"], q["model"] = line, name
            month_rows.append(q)
        g = pred.copy(); g["loss_difference"] = (clip(g[pcol])-g[ycol])**2 - (clip(g[ocol])-g[ycol])**2
        structural = rows[-1]
        structural["absolute_brier_improvement_vs_opportunity"] = -float(g.loss_difference.mean())
        structural["relative_brier_improvement_vs_opportunity"] = -float(g.loss_difference.mean()) / brier(g[ycol], g[ocol])
        structural["percentage_dates_improved"] = float((g.groupby("slate_date").loss_difference.mean() < 0).mean())
        structural["mean_absolute_probability_change"] = float(np.mean(np.abs(g[pcol]-g[ocol])))
        structural["material_5pp_decisions"] = int((np.abs(g[pcol]-g[ocol]) >= .05).sum())
    companion = pd.DataFrame()
    if not market.empty:
        parts = []
        for line, pcol, ycol in [(.5, "hits_over_05_probability", "actual_o05"), (1.5, "hits_over_15_probability", "actual_o15")]:
            z = pred.merge(market[market.line.eq(line)], on="player_game_key", how="inner")
            if z.empty: continue
            # Transparent expanding one-parameter convex blend, fit on prior dates only.
            for date in sorted(z.slate_date.unique()):
                tr, te = z[z.slate_date < date], z[z.slate_date == date]
                if len(tr) < 100: continue
                grid = np.linspace(0, 1, 21)
                losses = [log_loss(tr[ycol], clip(w*tr[pcol]+(1-w)*tr.market_probability)) for w in grid]
                w = float(grid[int(np.argmin(losses))])
                te = te.copy(); te["combined_probability"] = w*te[pcol]+(1-w)*te.market_probability; te["structural_weight"] = w; te["line"] = line
                parts.append(te)
        if parts:
            c = pd.concat(parts, ignore_index=True)
            rec=[]
            for line, g in c.groupby("line"):
                ycol = "actual_o05" if line == .5 else "actual_o15"; pcol = "hits_over_05_probability" if line == .5 else "hits_over_15_probability"
                for name,col in [("market_alone","market_probability"),("structural_alone",pcol),("market_plus_structural","combined_probability")]:
                    rec.append({"line":line,"model":name,"rows":len(g),"brier":brier(g[ycol],g[col]),"log_loss":float(log_loss(g[ycol],clip(g[col]))),"mean_structural_weight":float(g.structural_weight.mean())})
            companion=pd.DataFrame(rec)
    return pd.DataFrame(rows), pd.concat(month_rows, ignore_index=True), companion


def component_reports(pred: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pa_cols = [f"pa_probability_{i if i < 6 else '6_plus'}" for i in range(7)]
    pp = pred[pa_cols].to_numpy(); actual = pred.actual_plate_appearances.clip(0, 6).astype(int).to_numpy()
    pa = [{"slice":"all", "rows":len(pred), "multiclass_log_loss":float(log_loss(actual, clip(pp), labels=list(range(7)))), "ranked_probability_score":rps(actual, pp), "expected_pa_mae":float(np.mean(abs(pred.expected_pa-pred.actual_plate_appearances))), "actual_count_coverage":1.0}]
    for slot,g in pred.groupby("batting_order_position"):
        pa.append({"slice":f"batting_order_{int(slot)}","rows":len(g),"expected_pa_mean":g.expected_pa.mean(),"actual_pa_mean":g.actual_plate_appearances.mean(),"expected_pa_bias":g.expected_pa.mean()-g.actual_plate_appearances.mean()})
    # Every reconstructed PA receives its game's pregame mixture probability.
    event=[]
    for role,pcol in [("STARTER","starter_per_pa_hit_probability"),("BULLPEN","bullpen_per_pa_hit_probability")]:
        w = pred.expected_starter_facing_pa if role == "STARTER" else pred.expected_bullpen_facing_pa
        p = pred[pcol]; observed = pred.actual_hits.sum()/pred.actual_plate_appearances.sum()
        event.append({"context":role,"expected_pa":float(w.sum()),"mean_predicted_hit_probability":float(np.average(p,weights=np.maximum(w,EPS))),"all_pa_observed_hit_rate_reference":float(observed),"likelihood_status":"GAME_LEVEL_CONTEXT_RECONSTRUCTION_NO_AUTHENTIC_PA_ROLE_JOIN"})
    count=[]
    for k,col in [(0,"p_hits_0"),(1,"p_hits_1")]:
        count.append({"bucket":str(k),"predicted_rate":pred[col].mean(),"observed_rate":float((pred.actual_hits==k).mean())})
    count.append({"bucket":"2+","predicted_rate":pred.hits_over_15_probability.mean(),"observed_rate":float((pred.actual_hits>=2).mean())})
    return pd.DataFrame(pa),pd.DataFrame(event),pd.DataFrame(count)


def main() -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument("--spine", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--market", type=Path)
    ap.add_argument("--recent-count-ledger", type=Path, help="comparison-only frozen count-HGB ledger")
    ap.add_argument("--minimum-prior-dates", type=int, default=14)
    args=ap.parse_args(); out=args.out_dir
    if out.exists() and any(out.iterdir()):
        raise FileExistsError(f"refusing to overwrite immutable result: {out}")
    out.mkdir(parents=True)
    d=prepare(args.spine); market=load_market(args.market)
    pred=rolling_predictions(d,args.minimum_prior_dates)
    # Freeze baseball predictions before any market or outcome-specific evaluation artifact.
    prediction_cols=[c for c in pred.columns if not c.startswith("actual_")]
    pred[prediction_cols].to_csv(out/"frozen_structural_prediction_ledger.csv",index=False)
    pred[["slate_date","game_id","player_id","player_game_key","actual_plate_appearances","actual_hits","actual_o05","actual_o15"]].to_csv(out/"outcome_ledger.csv",index=False)
    metrics,monthly,companion=evaluate(pred,market)
    if args.recent_count_ledger and args.recent_count_ledger.exists():
        old=pd.read_csv(args.recent_count_ledger,low_memory=False)
        old=old[old.candidate.eq("count_hgb")].copy()
        bench=[]
        for line,ycol in [(.5,"actual_o05"),(1.5,"actual_o15")]:
            z=pred.merge(old[old.line.eq(line)][["player_game_key","predicted_over_probability"]].drop_duplicates("player_game_key"),on="player_game_key",how="inner")
            if not z.empty:
                bench.append({"line":line,"model":"recent_count_hgb_comparison_only","rows":len(z),"dates":z.slate_date.nunique(),"brier":brier(z[ycol],z.predicted_over_probability),"log_loss":float(log_loss(z[ycol],clip(z.predicted_over_probability))),"calibration_error":float(abs(z.predicted_over_probability.mean()-z[ycol].mean())),"mean_probability":float(z.predicted_over_probability.mean()),"observed_rate":float(z[ycol].mean())})
        if bench: metrics=pd.concat([metrics,pd.DataFrame(bench)],ignore_index=True,sort=False)
    pa,perpa,count=component_reports(pred)
    metrics.to_csv(out/"prop_comparison_report.csv",index=False); monthly.to_csv(out/"monthly_stability.csv",index=False)
    pa.to_csv(out/"pa_component_evaluation.csv",index=False); perpa.to_csv(out/"per_pa_component_evaluation.csv",index=False); count.to_csv(out/"hit_count_component_evaluation.csv",index=False)
    companion.to_csv(out/"market_companion_test.csv",index=False)
    cal=pd.concat([calibration_bins(pred,"hits_over_05_probability","actual_o05","hits_o05"),calibration_bins(pred,"hits_over_15_probability","actual_o15","hits_o15")],ignore_index=True);cal.to_csv(out/"fixed_band_calibration.csv",index=False)
    spec={"identity":"MLB_HITS_STRUCTURAL_V1","production_status":"NO_QUALIFIED_MLB_MODEL","market_in_baseball_model":False,"pa_distribution":"hierarchical Dirichlet full P(PA=0..5,6+), rolling strict-prior","pitcher_exposure":"starter workload-derived starter/bullpen PA share","per_pa_hit_model":"beta-binomial partial pooling of batter ability plus pooled opponent context","hit_distribution":"PA-mixture of Poisson-binomial distributions","coherence_invariants":["P(H>=2)<=P(H>=1)","threshold complements sum to one"],"limitations":["pregame lineup history sparse in governing spine","bullpen identity/availability unavailable","PA-role likelihood uses game-level reconstruction"]}
    (out/"structural_model_specification.json").write_text(json.dumps(spec,indent=2)+"\n")
    coherent=bool(((pred.hits_over_15_probability<=pred.hits_over_05_probability+1e-12)&((pred.hits_over_05_probability+pred.hits_under_05_probability-1).abs()<1e-10)&((pred.hits_over_15_probability+pred.hits_under_15_probability-1).abs()<1e-10)).all())
    sm=metrics[metrics.model.eq("structural_baseball")].copy()
    controls=metrics[metrics.model.isin(["line_specific_expanding_base_rate","expected_pa_x_batter_rate_control"])]
    best=controls.groupby("line").brier.min()
    improvement_vs_best=np.array([best.loc[r.line]-r.brier for _,r in sm.iterrows()])
    practical=bool((improvement_vs_best>.002).all() and (sm.percentage_dates_improved.fillna(0)>.55).all())
    if not coherent: decision="STRUCTURAL_MLB_HITS_MODEL_FAILED"
    elif practical and not companion.empty and all(companion[companion.model.eq("market_plus_structural")].set_index("line").brier < companion[companion.model.eq("market_alone")].set_index("line").brier): decision="STRUCTURAL_MODEL_MARKET_INCREMENTAL_SIGNAL_FOUND"
    elif practical and not companion.empty: decision="STRUCTURAL_BASEBALL_MODEL_FOUND_NOT_MARKET_INCREMENTAL"
    elif practical: decision="STRUCTURAL_MODEL_PRACTICALLY_USEFUL_REQUIRES_UNTOUCHED_CONFIRMATION"
    elif (sm.absolute_brier_improvement_vs_opportunity.fillna(0)>0).any(): decision="STRUCTURAL_COMPONENT_SIGNAL_FOUND_MODEL_INCOMPLETE"
    else: decision="STRUCTURAL_MLB_HITS_MODEL_FAILED"
    report=f"""# MLB Hits structural model v1\n\nFinal decision: **{decision}**\n\nThis is an independent baseball-process model. It creates one coherent hit-count distribution from a full PA distribution, explicit starter/bullpen exposure, and partially pooled per-PA hit probabilities. Market data enters only the post-freeze companion test.\n\nProduction remains **NO_QUALIFIED_MLB_MODEL**. No promotion, selector, EV, wager, ranking, upload, routing, or staking action is authorized.\n\nKnown limitation: authentic PA-level starter/reliever role could not be joined into this run's governing strict-prior player-game spine, so the exposure component is evaluated as a game-level reconstruction and is not yet independently certified at PA grain.\n"""
    (out/"final_comparison_report.md").write_text(report)
    (out/"terminal_decision.md").write_text(f"FINAL_DECISION = {decision}\nPRODUCTION_STATUS = NO_QUALIFIED_MLB_MODEL\n")
    hashes=[]
    for p in sorted(out.iterdir()):
        if p.is_file() and p.name!="sha256_manifest.csv": hashes.append({"path":p.name,"sha256":sha256(p),"bytes":p.stat().st_size})
    pd.DataFrame(hashes).to_csv(out/"sha256_manifest.csv",index=False)
    print(json.dumps({"decision":decision,"prediction_rows":len(pred),"dates":pred.slate_date.nunique(),"coherent":coherent,"market_rows":len(market)},indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
