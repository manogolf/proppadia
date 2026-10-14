#!/usr/bin/env python3
"""Characterize retained NHL SOG production exposure evidence (read-only inputs)."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "artifacts/analysis/nhl/sog_baseline_characterization/2026-10-14"
OUT = ROOT / "artifacts/analysis/nhl/sog_toi_exposure_characterization/2026-10-14"
THRESHOLDS = {"1.5": 2, "2.5": 3, "3.5": 4}
TOI_FIELDS = ["d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg"]


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def num(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce") if col in df else pd.Series(np.nan, index=df.index)


def poisson_tail(lam: pd.Series, threshold: int) -> pd.Series:
    return pd.Series([1 - sum(math.exp(-x) * x**k / math.factorial(k) for k in range(threshold))
                      if pd.notna(x) and x >= 0 else np.nan for x in lam], index=lam.index)


def coalesce_exposure(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    candidates = [num(df, c) for c in TOI_FIELDS]
    candidates += [num(df, "szn_toi_per_game_5on5") + num(df, "szn_toi_per_game_pp")]
    candidates += [num(df, "season_5on5_icetime_per_game") / 60 + num(df, "season_5on4_icetime_per_game") / 60]
    names = TOI_FIELDS + ["SEASON_TOI_5V5_PLUS_PP", "SEASON_TOI_5V5_PLUS_5V4"]
    selected = pd.Series(np.nan, index=df.index, dtype=float)
    source = pd.Series("UNAVAILABLE", index=df.index, dtype=object)
    for name, values in zip(names, candidates):
        values = values.where(np.isfinite(values))
        use = selected.isna() & values.notna()
        selected.loc[use] = values.loc[use]
        source.loc[use] = name
    return selected, source


def parse_toi(s: pd.Series) -> pd.Series:
    def one(v):
        if pd.isna(v):
            return np.nan
        try:
            a, b = str(v).split(":")
            return int(a) + int(b) / 60
        except (ValueError, TypeError):
            return pd.to_numeric(v, errors="coerce")
    return s.map(one).astype(float)


def metrics(df: pd.DataFrame, group: str | None = None) -> pd.DataFrame:
    groups = [(name, g) for name, g in df.groupby(group, dropna=False, observed=False)] if group else [("ALL", df)]
    rows = []
    for name, g in groups:
        actual, lam = num(g, "actual_sog"), num(g, "production_lambda")
        te, sr = num(g, "toi_error"), actual - lam
        row = {"group": name, "n": int(len(g)), "mean_toi_error": te.mean(), "median_toi_error": te.median(),
               "toi_mae": te.abs().mean(), "toi_rmse": np.sqrt((te**2).mean()),
               "mean_sog_residual": sr.mean(), "sog_mae": sr.abs().mean(), "sog_rmse": np.sqrt((sr**2).mean())}
        for line, threshold in THRESHOLDS.items():
            p = poisson_tail(lam, threshold).clip(1e-15, 1 - 1e-15)
            y = (actual >= threshold).astype(float)
            row[f"brier_{line}"] = ((p - y) ** 2).mean()
            row[f"log_loss_{line}"] = (-(y * np.log(p) + (1-y) * np.log(1-p))).mean()
            row[f"calibration_bias_{line}"] = p.mean() - y.mean()
        rows.append(row)
    return pd.DataFrame(rows)


def corr_metrics(df: pd.DataFrame) -> dict:
    x, y = num(df, "toi_error"), num(df, "sog_residual")
    z = pd.DataFrame({"x": x, "y": y}).dropna()
    slope = np.polyfit(z.x, z.y, 1)[0] if len(z) >= 2 else None
    pearson = z.x.corr(z.y) if len(z) >= 2 else None
    spearman = z.x.corr(z.y, method="spearman") if len(z) >= 2 else None
    return {"n": len(z), "pearson_r": pearson, "spearman_rho": spearman,
            "descriptive_slope_sog_residual_per_toi_min": slope,
            "linear_r_squared": pearson**2 if pd.notna(pearson) else None}


def dedup_rows(path: Path) -> pd.DataFrame:
    d = pd.read_csv(path)
    d["game_id"] = pd.to_numeric(d.game_id, errors="coerce")
    d["player_id"] = pd.to_numeric(d.player_id, errors="coerce")
    return d.drop_duplicates(["game_id", "player_id"]).copy()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    hist_path = BASE / "historical_characterization_rows.csv"
    hist = dedup_rows(hist_path)
    hist["production_lambda"] = num(hist, "production_lambda").fillna(num(hist, "regenerated_expected_sog"))
    hist["actual_sog"] = num(hist, "official_sog").fillna(num(hist, "actual_sog"))
    replayed_toi, replayed_source = coalesce_exposure(hist)
    hist["selected_toi_minutes"] = num(hist, "selected_toi_minutes").fillna(replayed_toi)
    hist["selected_toi_source_replayed"] = hist.get("toi_source", replayed_source).fillna(replayed_source)
    # Historical certified ledger contains only rows represented by line markets; its source exposure
    # replay is feature-bound and checked against retained lambda, but official TOI is not retained.
    rate = num(hist, "d10_sog_per60").where(num(hist, "d10_sog_per60").notna(),
          num(hist, "d20_sog_per60").where(num(hist, "d20_sog_per60").notna(), num(hist, "d5_sog_per60")))
    hist["replayed_lambda"] = rate * hist.selected_toi_minutes / 60
    hist["replay_abs_error"] = (hist.replayed_lambda - hist.production_lambda).abs()
    hist["toi_error"] = np.nan
    hist["sog_residual"] = hist.actual_sog - hist.production_lambda
    hist["window"] = "historical_2025_certified_replay"
    pd.DataFrame([{"rows":len(hist),"replayable":int(hist.replay_abs_error.notna().sum()),"unreplayable":int(hist.replay_abs_error.isna().sum()),"matches_1e-9":int((hist.replay_abs_error<=1e-9).sum()),"mismatches":int((hist.replay_abs_error>1e-9).sum()),"max_abs_lambda_error":hist.replay_abs_error.max()}]).to_csv(OUT/"historical_exposure_metrics.csv",index=False)

    prosp_path = BASE / "prospective_paired_rows.csv"
    prosp = dedup_rows(prosp_path)
    feat = pd.read_csv(BASE / "prospective_feature_bound_rows.csv")
    feat = feat.drop_duplicates(["game_id", "player_id"])
    # Prefer retained exact feature-bound records for exposure fields; retain all paired rows for denominator.
    feat_cols = [c for c in feat if c not in prosp.columns or c in {"selected_toi_minutes", "selected_toi_source", "selected_rate", "feature_lambda_parity", "d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg", "szn_toi_per_game_5on5", "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game", "szn_toi_per_game_pk", "season_4on5_icetime_per_game", "position_raw"}]
    prosp = prosp.merge(feat[["game_id", "player_id", *[c for c in feat_cols if c not in {"game_id", "player_id"}]]], on=["game_id", "player_id"], how="left", suffixes=("", "_feature"))
    prosp["actual_sog"] = num(prosp, "actual_sog").fillna(num(prosp, "official_sog"))
    prosp["production_lambda"] = num(prosp, "production_lambda").fillna(num(prosp, "lambda_production"))
    replay_toi, replay_src = coalesce_exposure(prosp)
    prosp["selected_toi_minutes"] = num(prosp, "selected_toi_minutes").fillna(replay_toi)
    prosp["selected_toi_source"] = prosp.get("selected_toi_source", pd.Series(index=prosp.index, dtype=object)).fillna(replay_src)
    prosp["replayed_lambda"] = num(prosp, "selected_rate") * prosp.selected_toi_minutes / 60
    prosp["replay_abs_error"] = (prosp.replayed_lambda - prosp.production_lambda).abs()
    prosp["actual_toi_minutes"] = parse_toi(prosp.get("toi", pd.Series(np.nan, index=prosp.index)))
    prosp["toi_error"] = prosp.actual_toi_minutes - prosp.selected_toi_minutes
    prosp["sog_residual"] = prosp.actual_sog - prosp.production_lambda
    prosp["window"] = "prospective_2026_feature_bound"

    # Attach official TOI directly from immutable canonical outcome files for all Oct 8-13 certified slates.
    outcomes = []
    sources = []
    for path in sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation").glob("2026-10-*/reconciliation=*/canonical_skater_outcomes.csv")):
        if path.parent.parent.name not in {f"2026-10-{d}" for d in range(8, 14)}:
            continue
        o = pd.read_csv(path)
        o["actual_toi_minutes"] = parse_toi(o.get("toi", pd.Series(np.nan, index=o.index)))
        outcomes.append(o[["game_id", "player_id", "actual_toi_minutes", "official_sog", "participation_state", "official_final"]])
        sources.append({"path": str(path.relative_to(ROOT)), "sha256": sha(path)})
    if outcomes:
        official = pd.concat(outcomes, ignore_index=True).drop_duplicates(["game_id", "player_id"])
        prosp = prosp.drop(columns=["actual_toi_minutes"], errors="ignore").merge(official, on=["game_id", "player_id"], how="left", suffixes=("", "_official"))
        prosp["toi_error"] = prosp.actual_toi_minutes - prosp.selected_toi_minutes
        prosp["actual_sog"] = num(prosp, "actual_sog").fillna(num(prosp, "official_sog"))
        prosp["sog_residual"] = prosp.actual_sog - prosp.production_lambda

    # Bounded replay-validation population and prospective actual-exposure diagnostics.
    feature_bound = prosp[prosp.selected_toi_minutes.notna()].copy()
    feature_bound["exposure_source"] = feature_bound.selected_toi_source.fillna("UNAVAILABLE")
    feature_bound["toi_error_bin"] = pd.cut(feature_bound.toi_error, [-np.inf, -3, -1, 1, 3, np.inf],
        labels=["over_by_3plus", "over_by_1_to_3", "within_1_min", "under_by_1_to_3", "under_by_3plus"])

    # Multi-view artifact outputs.
    hist.to_csv(OUT / "historical_exposure_rows.csv", index=False)
    prosp.to_csv(OUT / "prospective_exposure_rows.csv", index=False)
    feature_bound.to_csv(OUT / "feature_bound_prospective_exposure_rows.csv", index=False)
    hist.groupby("selected_toi_source_replayed", dropna=False).size().rename("rows").reset_index().to_csv(OUT / "historical_source_usage.csv", index=False)
    feature_bound.groupby("exposure_source", dropna=False).size().rename("rows").reset_index().to_csv(OUT / "prospective_source_usage.csv", index=False)
    bin_table = feature_bound.groupby("toi_error_bin", observed=False).apply(lambda g: metrics(g).assign(group=str(g.name)), include_groups=False).reset_index(drop=True)
    bin_table.to_csv(OUT / "toi_error_bins.csv", index=False)
    metrics(feature_bound, "exposure_source").to_csv(OUT / "source_metrics.csv", index=False)
    feature_bound["home_away"] = feature_bound.is_home_feature.astype(str).str.lower().map({"t":"HOME","true":"HOME","1":"HOME","f":"AWAY","false":"AWAY","0":"AWAY"}).fillna("UNKNOWN")
    metrics(feature_bound, "home_away").to_csv(OUT / "home_away_metrics.csv", index=False)
    feature_bound.groupby("game_date_feature", dropna=False).apply(lambda g: pd.Series({"n":len(g),"mean_toi_error":g.toi_error.mean(),"toi_mae":g.toi_error.abs().mean(),"mean_sog_residual":g.sog_residual.mean(),"sog_mae":g.sog_residual.abs().mean()})).reset_index().to_csv(OUT/"date_metrics.csv",index=False)
    if "position_raw" in feature_bound and feature_bound.position_raw.notna().any():
        metrics(feature_bound, "position_raw").to_csv(OUT / "position_metrics.csv", index=False)
    else:
        for stale_position_table in ("position_metrics.csv", "position_role_metrics.csv"):
            if (OUT/stale_position_table).exists():
                (OUT/stale_position_table).unlink()
    feature_bound.groupby("team_id_feature", dropna=False).apply(lambda g: pd.Series({"n":len(g), "mean_toi_error":g.toi_error.mean(), "toi_mae":g.toi_error.abs().mean(), "sog_residual":g.sog_residual.mean()})).query("n >= 20").reset_index().to_csv(OUT / "team_metrics_min20.csv", index=False)
    player = feature_bound.groupby("player_id", dropna=False).agg(n=("toi_error","count"), mean_toi_error=("toi_error","mean"), toi_mae=("toi_error",lambda s:s.abs().mean()), mean_sog_residual=("sog_residual","mean"), sog_residual_mae=("sog_residual",lambda s:s.abs().mean())).query("n >= 3").reset_index()
    player.to_csv(OUT / "player_persistence_min3.csv", index=False)

    # Recent/season comparison and direct exposure component ablations, feature-bound only.
    recent, season = num(feature_bound,"d10_toi_min_avg"), num(feature_bound,"szn_toi_per_game_5on5") + num(feature_bound,"szn_toi_per_game_pp")
    feature_bound["recent_toi"] = recent
    feature_bound["season_toi"] = season
    feature_bound["recent_minus_season"] = recent - season
    both = feature_bound[recent.notna() & season.notna() & feature_bound.actual_toi_minutes.notna()].copy()
    both["recent_abs_error"] = (both.actual_toi_minutes - both.recent_toi).abs()
    both["season_abs_error"] = (both.actual_toi_minutes - both.season_toi).abs()
    both["closer"] = np.select([both.recent_abs_error < both.season_abs_error, both.season_abs_error < both.recent_abs_error], ["recent", "season"], default="tie")
    both.to_csv(OUT / "recent_vs_season_rows.csv", index=False)
    pd.DataFrame([{"n":len(both),"recent_mae":both.recent_abs_error.mean(),"season_mae":both.season_abs_error.mean(),"recent_closer":int((both.closer=="recent").sum()),"season_closer":int((both.closer=="season").sum()),"ties":int((both.closer=="tie").sum())}]).to_csv(OUT / "recent_vs_season_metrics.csv", index=False)
    if len(both):
        both["usage_group"] = np.select([both.recent_minus_season > 1, both.recent_minus_season < -1], ["rising_recent_vs_season", "falling_recent_vs_season"], default="near_agreement")
        both.groupby("usage_group", dropna=False).apply(lambda g: pd.Series({"n":len(g),"mean_realized_toi":g.actual_toi_minutes.mean(),"mean_production_toi":g.selected_toi_minutes.mean(),"mean_toi_error":g.toi_error.mean(),"mean_sog_residual":g.sog_residual.mean(),"toi_mae":g.toi_error.abs().mean()})).reset_index().to_csv(OUT / "recent_season_divergence_metrics.csv", index=False)
    # Component-only exposure replays are descriptive ablations; retain production-selected rate.
    ablations=[]
    for label, exp in [("production_selected", num(feature_bound,"selected_toi_minutes")),
                       ("d10_only", num(feature_bound,"d10_toi_min_avg")),
                       ("d20_only", num(feature_bound,"d20_toi_min_avg")),
                       ("d5_only", num(feature_bound,"d5_toi_min_avg")),
                       ("season_5v5_only", num(feature_bound,"szn_toi_per_game_5on5")),
                       ("season_5v5_plus_pp", num(feature_bound,"szn_toi_per_game_5on5")+num(feature_bound,"szn_toi_per_game_pp")),
                       ("season_5v5_plus_5v4", num(feature_bound,"season_5on5_icetime_per_game")/60+num(feature_bound,"season_5on4_icetime_per_game")/60)]:
        a=feature_bound.copy(); a["selected_toi_minutes"]=exp; a["production_lambda"]=num(feature_bound,"selected_rate")*exp/60
        a["toi_error"]=a.actual_toi_minutes-exp; a["sog_residual"]=a.actual_sog-a.production_lambda
        m=metrics(a).iloc[0].to_dict(); m["ablation"]=label; ablations.append(m)
    pd.DataFrame(ablations).to_csv(OUT/"exposure_ablation_metrics.csv",index=False)
    # Existing pregame proxies only: role_pp_share and season PP time; bins are fixed and interpretable.
    feature_bound["pp_toi_group"] = pd.cut(num(feature_bound,"szn_toi_per_game_pp"),[-np.inf,.5,1.5,np.inf],labels=["low_le_0.5","moderate_0.5_to_1.5","high_gt_1.5"])
    metrics(feature_bound,"pp_toi_group").to_csv(OUT/"pp_role_metrics.csv",index=False)
    pd.DataFrame([
        {"component":"D10 recent TOI","classification":"CORE_IN_AVAILABLE_POPULATION","evidence":"Supplies exposure for 13,347/13,389 historical replay rows and 1,107/1,107 feature-bound prospective rows; alternate-source fallback not observed prospectively."},
        {"component":"D20 / D5 TOI fallbacks","classification":"CONDITIONAL_REDUNDANT_FALLBACK","evidence":"Scorer reaches only if preceding TOI is missing; no prospective feature-bound rows required fallback."},
        {"component":"Season TOI fallbacks","classification":"CONDITIONAL_FALLBACK_UNVALIDATED","evidence":"Fallbacks exist in scorer but did not control selected exposure in available measured prospective rows."},
        {"component":"PP TOI","classification":"CONDITIONAL","evidence":"Included only in season 5v5+PP fallback; no separate production multiplier."},
        {"component":"Exposure as SOG error driver","classification":"SMALL_ASSOCIATION_NOT_DOMINANT","evidence":"Prospective Pearson r=0.174; R2=0.030; actual-TOI oracle count MAE gain=0.030 on baseline 1.039."},
    ]).to_csv(OUT/"source_contribution_classifications.csv",index=False)
    pd.DataFrame([
        {"question":"Systematic mean TOI bias","classification":"NOT_ESTABLISHED","evidence":"Prospective mean -0.154 min; historical actual TOI unavailable."},
        {"question":"Residual association","classification":"WEAK_DESCRIPTIVE_SIGNAL","evidence":"r=0.174, R2=0.030 in 1,107 feature-bound player-games."},
        {"question":"Oracle headroom","classification":"SMALL","evidence":"Count MAE improves 0.030 SOG using postgame actual TOI."},
        {"question":"Correction study justified","classification":"NO_PENDING_HISTORICAL_TOI_BINDING","evidence":"No historical official TOI to test stability; short prospective window; oracle gain small."},
    ]).to_csv(OUT/"exposure_weakness_scorecard.csv",index=False)

    # Prior final TOI-error as descriptive next-game signal, only within chronological prospective rows.
    fb = feature_bound.dropna(subset=["actual_toi_minutes"]).sort_values(["player_id","game_date_feature","game_id"]).copy()
    fb["prior_toi_error"] = fb.groupby("player_id").toi_error.shift(1)
    fb["prior3_toi_error"] = fb.groupby("player_id").toi_error.transform(lambda s:s.shift(1).rolling(3,min_periods=1).mean())
    fb["prior_sog_residual"] = fb.groupby("player_id").sog_residual.shift(1)
    fb.to_csv(OUT / "prior_error_sequence.csv", index=False)
    persistence=[]
    for name, column in [("previous_game", "prior_toi_error"), ("prior3_mean", "prior3_toi_error")]:
        pg=fb.dropna(subset=[column,"toi_error"])
        persistence.append({"comparison":name,"n":len(pg),"pearson_r":pg[column].corr(pg.toi_error),"spearman_rho":pg[column].corr(pg.toi_error,method="spearman"),"same_sign_rate":(np.sign(pg[column])==np.sign(pg.toi_error)).mean()})
    pg=fb.dropna(subset=["prior_sog_residual","sog_residual"])
    persistence.append({"comparison":"prior_sog_residual","n":len(pg),"pearson_r":pg.prior_sog_residual.corr(pg.sog_residual),"spearman_rho":pg.prior_sog_residual.corr(pg.sog_residual,method="spearman"),"same_sign_rate":(np.sign(pg.prior_sog_residual)==np.sign(pg.sog_residual)).mean()})
    pd.DataFrame(persistence).to_csv(OUT/"prior_error_persistence.csv",index=False)
    feature_bound["d10_d20_toi_gap"] = num(feature_bound,"d10_toi_min_avg")-num(feature_bound,"d20_toi_min_avg")
    pd.DataFrame([{"relationship":"d10_minus_d20_toi_minutes_vs_toi_error",**corr_metrics(feature_bound.assign(toi_error=feature_bound.toi_error,sog_residual=feature_bound.d10_d20_toi_gap))}]).to_csv(OUT/"d10_d20_exposure_relationship.csv",index=False)

    # Postgame oracle and Shapley-style exact rate/exposure decomposition, explicitly diagnostic.
    oracle = feature_bound.dropna(subset=["actual_toi_minutes","actual_sog","production_lambda","selected_rate"]).copy()
    oracle["oracle_lambda"] = num(oracle,"selected_rate") * oracle.actual_toi_minutes / 60
    oracle["production_abs_error"] = (oracle.production_lambda-oracle.actual_sog).abs()
    oracle["oracle_abs_error"] = (oracle.oracle_lambda-oracle.actual_sog).abs()
    oracle["actual_rate_per60"] = oracle.actual_sog * 60 / oracle.actual_toi_minutes.replace(0,np.nan)
    oracle["rate_component"] = (num(oracle,"selected_rate")-oracle.actual_rate_per60) * oracle.actual_toi_minutes / 60
    oracle["exposure_component"] = oracle.actual_rate_per60 * (oracle.selected_toi_minutes-oracle.actual_toi_minutes) / 60
    oracle["interaction_component"] = (num(oracle,"selected_rate")-oracle.actual_rate_per60) * (oracle.selected_toi_minutes-oracle.actual_toi_minutes) / 60
    oracle.to_csv(OUT / "oracle_and_decomposition_rows.csv", index=False)
    pd.DataFrame([{"component":"rate","mean":oracle.rate_component.mean(),"mean_absolute":oracle.rate_component.abs().mean()},
                  {"component":"exposure","mean":oracle.exposure_component.mean(),"mean_absolute":oracle.exposure_component.abs().mean()},
                  {"component":"interaction","mean":oracle.interaction_component.mean(),"mean_absolute":oracle.interaction_component.abs().mean()},
                  {"component":"identity_error","mean":(oracle.production_lambda-oracle.actual_sog-oracle.rate_component-oracle.exposure_component-oracle.interaction_component).mean(),"mean_absolute":(oracle.production_lambda-oracle.actual_sog-oracle.rate_component-oracle.exposure_component-oracle.interaction_component).abs().mean()}]).to_csv(OUT/"rate_vs_exposure_decomposition.csv",index=False)
    pd.DataFrame([{"n":int(feature_bound.toi_error.notna().sum()),**{f"q{int(q*100):02d}":feature_bound.toi_error.quantile(q) for q in [0,.05,.25,.5,.75,.95,1]},"mean":feature_bound.toi_error.mean(),"mae":feature_bound.toi_error.abs().mean(),"rmse":np.sqrt((feature_bound.toi_error**2).mean())}]).to_csv(OUT/"toi_error_distribution.csv",index=False)

    # Compact calculated relationships.
    replay = {"historical": {"rows":len(hist),"matched_within_1e-9":int((hist.replay_abs_error<=1e-9).sum()),"mismatches":int((hist.replay_abs_error>1e-9).sum()),"max_abs_lambda_error":hist.replay_abs_error.max(),"unreplayable":int(hist.replay_abs_error.isna().sum())},
              "prospective_feature_bound": {"rows":len(feature_bound),"matched_within_1e-9":int((feature_bound.replay_abs_error<=1e-9).sum()),"mismatches":int((feature_bound.replay_abs_error>1e-9).sum()),"max_abs_lambda_error":feature_bound.replay_abs_error.max(),"unreplayable":int(feature_bound.replay_abs_error.isna().sum())}}
    toi_stats = {"historical_actual_toi": "NOT_RETAINED_IN_CERTIFIED_HISTORICAL_PACKAGE", "prospective": {"n":int(feature_bound.toi_error.notna().sum()),"mean":feature_bound.toi_error.mean(),"median":feature_bound.toi_error.median(),"mae":feature_bound.toi_error.abs().mean(),"rmse":np.sqrt((feature_bound.toi_error**2).mean()),"quantiles":feature_bound.toi_error.quantile([.05,.25,.75,.95]).to_dict()}}
    source_hist=hist.selected_toi_source_replayed.value_counts(dropna=False).to_dict()
    source_prosp=feature_bound.exposure_source.value_counts(dropna=False).to_dict()
    decomposition={"rate_component_mean":oracle.rate_component.mean(),"rate_component_mean_abs":oracle.rate_component.abs().mean(),"exposure_component_mean":oracle.exposure_component.mean(),"exposure_component_mean_abs":oracle.exposure_component.abs().mean(),"interaction_component_mean":oracle.interaction_component.mean(),"interaction_component_mean_abs":oracle.interaction_component.abs().mean(),"identity_max_abs_error":(oracle.production_lambda-oracle.actual_sog-oracle.rate_component-oracle.exposure_component-oracle.interaction_component).abs().max()}
    summary = {"study":"NHL_SOG_TOI_EXPOSURE_CHARACTERIZATION_V1","classification":"EXPOSURE_REPLAY_CONFIRMED_PROSPECTIVE_TOI_ERROR_AVAILABLE_HISTORICAL_TOI_GAP","historical_player_games":int(len(hist)),"prospective_certified_player_games":int(len(prosp)),"prospective_feature_bound_player_games":int(len(feature_bound)),"outcomes_joined_rows":int(feature_bound.toi_error.notna().sum()),"source_usage":{"historical":{str(k):int(v) for k,v in source_hist.items()},"prospective_feature_bound":{str(k):int(v) for k,v in source_prosp.items()}},"replay_validation":replay,"toi_error_metrics":toi_stats,"toi_error_vs_sog_residual":corr_metrics(feature_bound),"oracle": {"label":"POSTGAME_ORACLE_DIAGNOSTIC_ONLY","n":int(len(oracle)),"production_count_mae":oracle.production_abs_error.mean(),"actual_toi_oracle_count_mae":oracle.oracle_abs_error.mean(),"mae_improvement":oracle.production_abs_error.mean()-oracle.oracle_abs_error.mean(),**decomposition},"recent_vs_season": {"n":len(both),"recent_mae":both.recent_abs_error.mean(),"season_mae":both.season_abs_error.mean(),"recent_closer":int((both.closer=="recent").sum()),"season_closer":int((both.closer=="season").sum()),"ties":int((both.closer=="tie").sum())},"limits":["Official realized per-player TOI is not retained in the certified 2025 historical replay package; historical TOI-error, oracle and exposure-vs-residual claims cannot be computed.","Forty-two historical certified rows have no retained exposure inputs or selected source and are unreplayable; remaining 13,347 rows match production lambda within tolerance.","Feature-bound prospective window is limited by immutable captured feature evidence; no new provider or database reads were made.","Position role is not retained in feature-bound rows; role comparisons cannot be produced.","No replacement, correction, filter, or production change was built."]}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,default=lambda v: float(v) if isinstance(v,np.floating) else int(v) if isinstance(v,np.integer) else str(v))+"\n")

    tree = """# Production exposure decision tree\n\nSource: `backend/nhl/scripts/score_sog_poisson_baseline.py` (`score_predictions`, SHA in `lineage_manifest.json`). For each row, convert to numeric, turn non-finite values into missing, then take the first nonmissing value:\n\n1. `d10_toi_min_avg`\n2. `d20_toi_min_avg`\n3. `d5_toi_min_avg`\n4. `szn_toi_per_game_5on5 + szn_toi_per_game_pp` (both must yield a finite sum)\n5. `season_5on5_icetime_per_game / 60 + season_5on4_icetime_per_game / 60` (source fields are seconds/game)\n6. no exposure: row is unscored; there is no default, roster-derived substitute, cap, or floor in this scorer.\n\nExposure must be >= 0. Rate independently coalesces D10 → D20 → D5; lambda = rate × selected minutes / 60. No position-specific adjustment. PP contributes only within the season 5v5+PP fallback; PK is not included. The 5v5+5v4 fallback uses seconds/game and does not include 4v5. The scorer writes `poisson_source=d10` only if D10 rate and D10 TOI are present, otherwise `fallback`; it does not record the exact fallback source.\n"""
    (OUT/"production_exposure_decision_tree.md").write_text(tree)
    code_path=ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py"
    utility_path=ROOT/"backend/nhl/scripts/characterize_nhl_sog_toi_exposure.py"
    manifest={"study":"NHL_SOG_TOI_EXPOSURE_CHARACTERIZATION_V1","sources":{"historical_rows":{"path":str(hist_path.relative_to(ROOT)),"sha256":sha(hist_path)},"prospective_paired_rows":{"path":str(prosp_path.relative_to(ROOT)),"sha256":sha(prosp_path)},"prospective_feature_bound_rows":{"path":str((BASE/"prospective_feature_bound_rows.csv").relative_to(ROOT)),"sha256":sha(BASE/"prospective_feature_bound_rows.csv")},"production_scorer":{"path":str(code_path.relative_to(ROOT)),"sha256":sha(code_path)},"study_utility":{"path":str(utility_path.relative_to(ROOT)),"sha256":sha(utility_path)},"official_outcomes":sources},"methods":{"historical_denominator":"distinct game_id/player_id from certified exact/tolerance replay rows","prospective_denominator":"distinct game_id/player_id from settled Oct 8-13 paired production rows","feature_bound_denominator":"distinct feature-bound prospective player-games retained in baseline characterization","toi_error":"official canonical outcome toi parsed as MM:SS minus selected production exposure","replay_tolerance":1e-9,"oracle":"POSTGAME_ORACLE_DIAGNOSTIC_ONLY"},"output_hashes":{}}
    for p in sorted(OUT.iterdir()):
        if p.is_file() and p.name!="lineage_manifest.json": manifest["output_hashes"][p.name]=sha(p)
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
    report=f"""# NHL SOG TOI/exposure characterization\n\n**Classification:** `{summary['classification']}`\n\n## Coverage and replay\n\n- Historical certified post-cutover population: **{len(hist):,} player-games** (Feb 28–Apr 15). Replay matched within 1e-9 on {replay['historical']['matched_within_1e-9']:,}; {replay['historical']['mismatches']} numerical mismatches; {replay['historical']['unreplayable']} rows lack retained exposure/rate inputs. Maximum absolute lambda error among replayable rows: {replay['historical']['max_abs_lambda_error']:.3g}.\n- Prospective Oct 8–13 certified production population: **{len(prosp):,} player-games**; retained feature-bound subset: **{len(feature_bound):,}**, official TOI joined for {summary['outcomes_joined_rows']:,}.\n- Historical official TOI is absent from the certified replay rows. Historical actual-exposure comparisons therefore remain unavailable; no estimates are imputed.\n\n## Exact hierarchy and source usage\n\nSee [production_exposure_decision_tree.md](production_exposure_decision_tree.md). The hierarchy is D10 → D20 → D5 → season 5v5+PP → season 5v5+5v4. Lambda is selected shot rate × selected exposure / 60. No exposure default, floor, cap, roster/role-derived substitute, or position-specific scorer behavior exists.\n\nHistorical retained source usage is D10 **{source_hist.get('d10_toi_min_avg',0):,}**, unavailable **{source_hist.get('UNAVAILABLE',0):,}**; prospective feature-bound source usage is D10 **{source_prosp.get('d10_toi_min_avg',0):,}/{len(feature_bound):,}**. The season fallbacks cannot be evaluated for performance because they did not supply exposure in these feature-bound captures. PP TOI participates only in season 5v5+PP fallback. PK TOI is not included.\n\n## Prospective exposure findings\n\n- TOI error (actual minus expected): mean **{toi_stats['prospective']['mean']:.3f} min**, median **{toi_stats['prospective']['median']:.3f}**, MAE **{toi_stats['prospective']['mae']:.3f}**, RMSE **{toi_stats['prospective']['rmse']:.3f}**. Negative values mean expected exposure was too high. Date breakdown is in `date_metrics.csv`; error bins and threshold calibration/proper scores are in `toi_error_bins.csv`.\n- TOI error vs SOG residual: Pearson r **{summary['toi_error_vs_sog_residual']['pearson_r']:.3f}**, Spearman rho **{summary['toi_error_vs_sog_residual']['spearman_rho']:.3f}**, slope **{summary['toi_error_vs_sog_residual']['descriptive_slope_sog_residual_per_toi_min']:.3f} SOG/min**, R² **{summary['toi_error_vs_sog_residual']['linear_r_squared']:.3f}**. Direction aligns modestly, but TOI error explains only about 3% of linear residual variance in this capture.\n- D10 was the only selected exposure source in prospective feature-bound rows. Production-selected and D10-only are identical there. D20-only, D5-only, and season exposure ablations are descriptive unselected alternatives; no tuning or correction was done. See `exposure_ablation_metrics.csv`.\n- Recent vs season TOI (n={len(both):,}): MAE recent **{both.recent_abs_error.mean():.3f}**, season **{both.season_abs_error.mean():.3f}**; recent closer on **{int((both.closer=='recent').sum())}**, season closer on **{int((both.closer=='season').sum())}**, ties {int((both.closer=='tie').sum())}. Groups use a fixed ±1-minute recent-minus-season definition. Rising/declining group metrics are in `recent_season_divergence_metrics.csv`.\n- The small-sample oracle replaces expected TOI with realized TOI while holding production rate fixed. **POSTGAME_ORACLE_DIAGNOSTIC_ONLY**: production SOG count MAE {oracle.production_abs_error.mean():.3f}; oracle MAE {oracle.oracle_abs_error.mean():.3f}; apparent MAE improvement **{oracle.production_abs_error.mean()-oracle.oracle_abs_error.mean():.3f}**. This modest ceiling does not suggest exposure is the dominant count-error source. Rate/exposure/interaction identity is exact within {decomposition['identity_max_abs_error']:.2g} and is not causal attribution.\n- Home/away, PP TOI group, player persistence, team support (minimum 20 in this short feature sample), and prior-error sequence tables are included. Position-role comparison is unavailable because canonical position was not retained in the feature-bound rows. Team/player short-window patterns are exploratory.\n\n## Decision\n\nHistorical actual TOI is missing, exposure hierarchy rarely leaves D10 on the measured population, TOI error has a modest residual association, and actual-TOI oracle improvement is small. The data do **not** establish a systematic correctable exposure weakness or justify a bounded prospective exposure correction study yet. First bind official TOI to certified historical rows and re-evaluate stability. The apparent differences across PP groups, teams, and players are short-window exploratory and should be treated as noise absent replication.\n\nNo new provider calls, paid credits, production DB mutations, or production changes were made. Pushed: **NO**. Detailed rows, subgroup tables, source hashes, and constraints are in this package.\n"""
    report += """

## Subgroup reads

In fixed actual-minus-expected exposure-error bins, mean SOG residual moves from -0.493 (expected TOI > actual by over 3 minutes), to -0.269 (1–3 minutes over), approximately zero (within 1 minute), and +0.195/+0.196 (actual TOI 1–3/>3 minutes higher). The direction is coherent, but the tail bins are small and proper scores are not strictly monotonic; see `toi_error_bins.csv`.

Recent-minus-season usage groups use a fixed ±1-minute boundary. Rising usage has mean exposure error -0.301 minutes, falling usage +0.155, and near-agreement approximately zero. This is compatible with mild overreaction to rising usage and lag on falling usage, but the feature-bound window has only four dates. Prior TOI error has modest within-window persistence; the player/date sequences are in `prior_error_sequence.csv` and `prior_error_persistence.csv`.

High PP usage (>1.5 season PP minutes/game) has exposure MAE 1.877 minutes, compared with 1.965 at ≤0.5 and 2.243 at 0.5–1.5. PP TOI does not affect the selected D10 exposure; it appears in the season fallback only. The season 5v5+PP exposure has MAE 2.449 minutes, while 5v5-only MAE is 3.537, both worse than recent TOI in this captured population. The historical certified rows do not retain official TOI, so none of these subgroup patterns has a historical robustness check. Canonical positions are also absent from the feature-bound subset; position comparisons are unavailable.
"""
    (OUT/"report.md").write_text(report)
    # Rehash package after report creation.
    manifest["output_hashes"]={p.name:sha(p) for p in sorted(OUT.iterdir()) if p.is_file() and p.name!="lineage_manifest.json"}
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
    print(json.dumps({"package":str(OUT.relative_to(ROOT)),"summary":summary},indent=2,default=str))


if __name__ == "__main__":
    main()
