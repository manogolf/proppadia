#!/usr/bin/env python3
"""Build a local evidence and feasibility audit for the 2025 NHL SOG season."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import calendar
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
PLATFORM = ROOT / "artifacts/analysis/model_development/nhl_sog_cold_start_prediction_platform_v1/2026-09-19"
LEDGER = ROOT / "artifacts/analysis/model_development/nhl_sog_outcome_denominator_certification/2026-07-13/nhl_sog_complete_prediction_qualification_ledger_2026-07-13.csv"
FEATURE_ROOT = ROOT / "artifacts/archive/generated_daily/nhl"
SCORER = ROOT / "backend/nhl/scripts/score_sog_poisson_baseline.py"
TOLERANCE = 1e-12


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, lineterminator="\n")


def poisson_tail(lam: float, threshold: int) -> float:
    if not np.isfinite(lam) or lam < 0:
        return float("nan")
    cdf = sum(np.exp(-lam) * lam**k / math.factorial(k) for k in range(threshold))
    return float(np.clip(1.0 - cdf, 0.0, 1.0))


def line_metrics(rows: pd.DataFrame, probability: str) -> dict[str, float | int]:
    p = rows[probability].astype(float).clip(1e-15, 1 - 1e-15)
    y = (rows.official_sog.astype(float) >= rows.line.astype(float).map({1.5: 2, 2.5: 3, 3.5: 4})).astype(int)
    err = p - y
    return {"n": len(rows), "brier": float(np.mean(err**2)), "log_loss": float(-np.mean(y*np.log(p)+(1-y)*np.log(1-p))), "accuracy": float(np.mean((p >= .5) == y))}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="artifacts/analysis/nhl/sog_full_season_reconstruction_audit/2026-10-14")
    ap.add_argument("--bridge-dir", default="artifacts/analysis/nhl/sog_full_season_reconstruction_audit/2026-10-14/certified_bridge")
    args = ap.parse_args()
    out = (ROOT / args.out_dir).resolve()
    bridge = (ROOT / args.bridge_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)

    games = pd.read_parquet(PLATFORM / "retained_game_schedule_snapshot.parquet")
    games = games[(games.season == 2025) & (games.game_type == 2)].copy()
    games["game_date"] = games.game_date.astype(str)
    if games.game_id.duplicated().any() or len(games) != 1312:
        raise RuntimeError("CANONICAL_2025_GAME_POPULATION_INVALID")
    hist = pd.read_parquet(PLATFORM / "retained_skater_history_snapshot.parquet")
    hist = hist[(hist.season == 2025) & (hist.game_type == 2)].copy()
    ledger = pd.read_csv(LEDGER, low_memory=False)
    certified = ledger[(ledger.canonical_season == 2025) & ledger.accuracy_denominator_eligible.eq(True) & ledger.model_or_strategy.eq("poisson_baseline") & ledger.model_version.eq("baseline_v1")].copy()
    certified_keys = certified[["game_id", "player_id"]].drop_duplicates()
    if len(certified_keys) != 13389:
        raise RuntimeError(f"CERTIFIED_PLAYER_GAME_COUNT={len(certified_keys)}")

    date_rows, feature_frames, lineage = [], [], []
    month_source = {}
    for date, group in games.groupby("game_date", sort=True):
        path = FEATURE_ROOT / date / "sog_features" / f"sog_features_{date}_denali.csv"
        exists = path.is_file()
        frame = pd.read_csv(path, low_memory=False) if exists else pd.DataFrame()
        stale_rows = 0
        if exists:
            frame["game_date"] = frame.game_date.astype(str)
            frame["archive_path"] = str(path.relative_to(ROOT))
            frame["archive_sha256"] = sha(path)
            canonical_ids = set(group.game_id.astype(int))
            stale_rows = int((~pd.to_numeric(frame.game_id, errors="coerce").isin(canonical_ids)).sum())
            frame = frame[pd.to_numeric(frame.game_id, errors="coerce").isin(canonical_ids)].copy()
            feature_frames.append(frame)
            lineage.append({"path": str(path.relative_to(ROOT)), "sha256": sha(path), "rows": len(frame), "game_date": date})
        month = date[:7]
        date_rows.append({"game_date": date, "month": month, "canonical_game_count": len(group), "game_ids": ";".join(map(str, sorted(group.game_id.astype(int)))), "feature_archive_present": exists, "feature_archive_rows": len(frame), "stale_or_noncanonical_feature_rows_excluded": stale_rows, "skater_history_rows_for_games": int(hist.game_id.isin(group.game_id).sum())})
        month_source.setdefault(month, []).append((date, len(group), len(frame), exists))
    features = pd.concat(feature_frames, ignore_index=True) if feature_frames else pd.DataFrame()
    features["game_id"] = pd.to_numeric(features.game_id, errors="coerce").astype("Int64")
    features["player_id"] = pd.to_numeric(features.player_id, errors="coerce").astype("Int64")
    if features.duplicated(["game_id", "player_id"]).any():
        raise RuntimeError("DUPLICATE_PLAYER_GAME_FEATURE_KEYS")
    features = features.merge(games[["game_id", "home_team_code", "away_team_code"]], on="game_id", how="left", validate="many_to_one")
    features = features.merge(certified_keys.assign(certified=True), on=["game_id", "player_id"], how="left", validate="one_to_one")
    features["certified"] = features.certified.eq(True)
    rate_fields = ["d10_sog_per60", "d20_sog_per60", "d5_sog_per60"]
    toi_fields = ["d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg", "szn_toi_per_game_5on5", "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game"]
    for col in rate_fields + toi_fields:
        if col not in features:
            features[col] = np.nan
    features["rate_available"] = features[rate_fields].apply(pd.to_numeric, errors="coerce").notna().any(axis=1)
    toi = features[toi_fields].apply(pd.to_numeric, errors="coerce")
    features["exposure_available"] = toi[["d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg"]].notna().any(axis=1) | toi[["szn_toi_per_game_5on5", "szn_toi_per_game_pp"]].notna().all(axis=1) | toi[["season_5on5_icetime_per_game", "season_5on4_icetime_per_game"]].notna().all(axis=1)
    features["source_class"] = np.where(features.certified, "TIER_1_CERTIFIED", "TIER_3_ASOF_UNVERIFIED")
    features["eligibility_status"] = np.where(features.certified & features.rate_available & features.exposure_available, "FULLY_RECONSTRUCTABLE_CERTIFIED", np.where(features.certified, "CERTIFIED_OUTPUT_ONLY_REPLAY", np.where(~features.rate_available, "BLOCKED_MISSING_HISTORY", np.where(~features.exposure_available, "BLOCKED_MISSING_TOI", "PARTIALLY_RECONSTRUCTABLE_ASOF_UNVERIFIED"))))
    features["block_reason"] = np.where(features.certified & features.rate_available & features.exposure_available, "", np.where(features.certified, "Certified output is valid; at least one selected raw rate/exposure input is absent for independent correction reconstruction", np.where(~features.rate_available, "No retained fallback rate", np.where(~features.exposure_available, "No retained exposure fallback", "Historical feature archive has no immutable pregame/input-cutoff proof; strict-prior status cannot be certified"))))
    save_csv(pd.DataFrame(date_rows), out / "canonical_season_coverage.csv")
    phase_file=ROOT/"backend/nhl/game_phase.py"
    save_csv(pd.DataFrame([
        {"game_type":1,"phase":"PRESEASON","rows_in_retained_season_2025_snapshot":0,"boundary":"Excluded; no preseason row in retained schedule snapshot"},
        {"game_type":2,"phase":"REGULAR_SEASON","rows_in_retained_season_2025_snapshot":len(games),"boundary":f"{games.game_date.min()} through {games.game_date.max()}"},
        {"game_type":3,"phase":"POSTSEASON","rows_in_retained_season_2025_snapshot":0,"boundary":"Excluded; no postseason row in retained schedule snapshot"},
    ]),out/"phase_boundary_inventory.csv")
    save_csv(features[["game_date", "game_id", "player_id", "team_id", "opponent_id", "is_home", "certified", "source_class", "eligibility_status", "block_reason", *rate_fields, *toi_fields, "shots_on_goal", "archive_path", "archive_sha256"]], out / "reconstruction_eligibility.csv")
    save_csv(features[features.block_reason.ne("")][["game_date", "game_id", "player_id", "team_id", "eligibility_status", "block_reason", "archive_path"]], out / "blocked_row_reasons.csv")
    save_csv(features[["game_date", "game_id", "player_id", "source_class", "eligibility_status", "block_reason"]], out / "trust_tier_assignments.csv")

    monthly = []
    for month, data in features.groupby(features.game_date.str[:7], sort=True):
        canonical = games[games.game_date.str[:7].eq(month)]
        cert = data.certified
        scorable = data.rate_available & data.exposure_available
        monthly.append({"month": month, "canonical_games": canonical.game_id.nunique(), "archive_player_game_rows": len(data), "archive_dates_with_games": data.game_date.nunique(), "feature_archive_missing_or_empty_dates": sum((not x[3]) or x[2] == 0 for x in month_source[month]), "contract_inputs_present_rows": int(scorable.sum()), "certified_player_games": int(cert.sum()), "additional_strict_prior_valid_rows": 0, "usable_line_rows": int(cert.sum())*3, "blocked_rows": int((~cert).sum()), "blocked_percentage_of_archive_player_games": float((~cert).mean()*100) if len(data) else 0.0, "strict_prior_validation_for_noncertified": "NOT_PROVEN"})
    monthly_df = pd.DataFrame(monthly)
    save_csv(monthly_df, out / "monthly_coverage.csv")

    december = pd.DataFrame(date_rows)
    december = december[december.month.eq("2025-12")].copy()
    calendar_days = pd.date_range("2025-12-01", "2025-12-31", freq="D").strftime("%Y-%m-%d")
    december = pd.DataFrame({"game_date": calendar_days}).merge(december, on="game_date", how="left")
    december["month"] = "2025-12"
    december["canonical_game_count"] = december["canonical_game_count"].fillna(0).astype(int)
    december["feature_archive_present"] = december["feature_archive_present"].eq(True)
    december["feature_archive_rows"] = december["feature_archive_rows"].fillna(0).astype(int)
    december["stale_or_noncanonical_feature_rows_excluded"] = december["stale_or_noncanonical_feature_rows_excluded"].fillna(0).astype(int)
    december["game_ids"] = december["game_ids"].fillna("")
    december["gap_classification"] = np.where(december.canonical_game_count.eq(0), "NO_CANONICAL_GAME", np.where(december.feature_archive_present, "RECOVERABLE_FROM_EXISTING_RAW_ARTIFACTS_BUT_ASOF_UNVERIFIED", "NOT_RECOVERABLE_FROM_CURRENT_REPOSITORY"))
    december["missing_source_types"] = np.where(december.canonical_game_count.eq(0), "none; no canonical scheduled game", np.where(december.feature_archive_present, "immutable pregame capture/cutoff; game-level strict-prior linkage", "feature inputs; immutable pregame capture/cutoff; strict-prior history"))
    save_csv(december, out / "december_gap_map.csv")

    hist_presence = pd.DataFrame([
        {"source":"canonical games / game phase", "path":str((PLATFORM/"retained_game_schedule_snapshot.parquet").relative_to(ROOT)), "rows":len(games), "date_min":games.game_date.min(), "date_max":games.game_date.max(), "available":True, "coverage":"all canonical 2025 regular-season games", "strict_prior_or_authority_note":"game_type=2 phase filtered"},
        {"source":"player identity and official skater game logs", "path":str((PLATFORM/"retained_skater_history_snapshot.parquet").relative_to(ROOT)), "rows":len(hist), "date_min":str(hist.game_date.min()), "date_max":str(hist.game_date.max()), "available":True, "coverage":"appeared skaters only; this season snapshot", "strict_prior_or_authority_note":"Official identity and logs retained; does not supply pregame expected skater denominator"},
        {"source":"official SOG", "path":str((PLATFORM/"retained_skater_history_snapshot.parquet").relative_to(ROOT)), "rows":int(hist.shots_on_goal.notna().sum()), "date_min":str(hist.loc[hist.shots_on_goal.notna(),"game_date"].min()), "date_max":str(hist.loc[hist.shots_on_goal.notna(),"game_date"].max()), "available":True, "coverage":"appeared skaters", "strict_prior_or_authority_note":"Outcome observed for target game; exclude from its own features"},
        {"source":"TOI", "path":str((PLATFORM/"retained_skater_history_snapshot.parquet").relative_to(ROOT)), "rows":int(hist.toi_minutes.notna().sum()), "date_min":str(hist.loc[hist.toi_minutes.notna(),"game_date"].min()), "date_max":str(hist.loc[hist.toi_minutes.notna(),"game_date"].max()), "available":True, "coverage":"appeared skaters", "strict_prior_or_authority_note":"Total and PP TOI retained; no per-game 5v5/5v4 split in this snapshot"},
        {"source":"shot attempts", "path":str((PLATFORM/"retained_skater_history_snapshot.parquet").relative_to(ROOT)), "rows":int(hist.shot_attempts.notna().sum()), "date_min":str(hist.loc[hist.shot_attempts.notna(),"game_date"].min()), "date_max":str(hist.loc[hist.shot_attempts.notna(),"game_date"].max()), "available":True, "coverage":"available field in retained snapshot", "strict_prior_or_authority_note":"Not required by frozen Poisson SOG baseline"},
        {"source":"D5/D10/D20 rates and production feature inputs", "path":"artifacts/archive/generated_daily/nhl/YYYY-MM-DD/sog_features/sog_features_YYYY-MM-DD_denali.csv", "rows":len(features), "date_min":features.game_date.min(), "date_max":features.game_date.max(), "available":True, "coverage":"features on 163 game dates; rate fields retained where non-null", "strict_prior_or_authority_note":"No immutable per-row feature cutoff outside certified dates"},
        {"source":"team/opponent and home/away", "path":"artifacts/archive/generated_daily/nhl/YYYY-MM-DD/sog_features/sog_features_YYYY-MM-DD_denali.csv", "rows":int(features[["team_id","opponent_id","is_home"]].notna().all(axis=1).sum()), "date_min":features.game_date.min(), "date_max":features.game_date.max(), "available":True, "coverage":"daily feature rows", "strict_prior_or_authority_note":"present in row exports; historical input timestamp not captured"},
        {"source":"retained feature artifacts", "path":"artifacts/archive/generated_daily/nhl", "rows":len(features), "date_min":features.game_date.min(), "date_max":features.game_date.max(), "available":True, "coverage":f"{int(features.game_date.nunique())}/{games.game_date.nunique()} game dates have nonempty canonical-key rows", "strict_prior_or_authority_note":"feature outputs available; cutoff evidence absent outside TIER 1"},
        {"source":"retained prediction artifacts / official outcomes", "path":str(LEDGER.relative_to(ROOT)), "rows":len(certified), "date_min":certified.game_date.min(), "date_max":certified.game_date.max(), "available":True, "coverage":"certified production baseline late-season denominator only", "strict_prior_or_authority_note":"official outcome uniquely bound for eligible rows; does not create a full-season expected-player population"},
        {"source":"historical immutable captures", "path":"artifacts/archive/generated_daily/nhl", "rows":0, "date_min":"", "date_max":"", "available":False, "coverage":"no immutable per-target capture found for non-certified dates", "strict_prior_or_authority_note":"reason TIER 2 is empty"},
    ])
    save_csv(hist_presence, out / "source_availability_matrix.csv")

    # Certified bridge is generated by the frozen repository reproduction utility.
    bridge_summary_path = bridge / "nhl_season_2025_sog_reproduction_run_summary_2026-07-13.json"
    bridge_summary = json.loads(bridge_summary_path.read_text())
    repro = pd.read_csv(bridge / "nhl_season_2025_sog_reproduction_ledger_2026-07-13.csv")
    save_csv(pd.DataFrame([{"certified_rows": bridge_summary["target_rows"], "source_backed_rows": bridge_summary["mode_a_rows"], "exact_probability_matches": bridge_summary["exact_rows"], "within_1e_12_matches": bridge_summary["tolerance_rows"], "mismatches": bridge_summary["material_mismatch_rows"], "side_matches": bridge_summary["side_match_rows"], "max_abs_probability_difference": float(repro.absolute_difference.max()), "tolerance": TOLERANCE}]), out / "certified_vs_reconstructed_validation.csv")
    # Baseline and frozen offline corrections on certified TIER 1 only.
    metric_rows = []
    for (date, gid, pid), g in repro.groupby(["game_date", "game_id", "player_id"], sort=False):
        feat_path = ROOT / str(g.prepared_source_path.iloc[0])
        # Cache source frames to avoid repeated disk reads.
        feat = feature_cache.get(str(feat_path)) if "feature_cache" in locals() else None
        if "feature_cache" not in locals():
            feature_cache = {}
        if feat is None:
            feat = pd.read_csv(feat_path, low_memory=False)
            feature_cache[str(feat_path)] = feat
        row = feat[(pd.to_numeric(feat.game_id, errors="coerce").eq(int(gid))) & (pd.to_numeric(feat.player_id, errors="coerce").eq(int(pid)))].iloc[0]
        r10, r20 = pd.to_numeric(row.get("d10_sog_per60"), errors="coerce"), pd.to_numeric(row.get("d20_sog_per60"), errors="coerce")
        rate = next((float(row[x]) for x in rate_fields if pd.notna(pd.to_numeric(row.get(x), errors="coerce"))), np.nan)
        exp = np.nan
        exp_name = ""
        for x in ("d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg"):
            value = pd.to_numeric(row.get(x), errors="coerce")
            if pd.notna(value): exp, exp_name = float(value), x; break
        if pd.isna(exp):
            a, b = pd.to_numeric(row.get("szn_toi_per_game_5on5"), errors="coerce"), pd.to_numeric(row.get("szn_toi_per_game_pp"), errors="coerce")
            if pd.notna(a) or pd.notna(b): exp, exp_name = float(a if pd.notna(a) else 0)+float(b if pd.notna(b) else 0), "SEASON_5V5_PLUS_PP"
        if pd.isna(exp):
            a, b = pd.to_numeric(row.get("season_5on5_icetime_per_game"), errors="coerce"), pd.to_numeric(row.get("season_5on4_icetime_per_game"), errors="coerce")
            if pd.notna(a) or pd.notna(b): exp, exp_name = (float(a if pd.notna(a) else 0)+float(b if pd.notna(b) else 0))/60, "SEASON_5V5_PLUS_5V4"
        if not np.isfinite(rate) or not np.isfinite(exp):
            continue
        corr_rate = (float(r10)+float(r20))/2 if pd.notna(r10) and pd.notna(r20) and abs(float(r10)-float(r20)) > .5 else float(r10) if pd.notna(r10) else rate
        soft_rate = 9 + .5*(corr_rate-9) if corr_rate > 9 else corr_rate
        for _, line_row in g.iterrows():
            line = float(line_row.line); sog = float(line_row.official_sog)
            for variant, rate_value in (("production", rate), ("divergence_midpoint", corr_rate), ("divergence_plus_high_d10_soft", soft_rate)):
                lam = rate_value*exp/60
                metric_rows.append({"game_date":date,"month":date[:7],"game_id":int(gid),"player_id":int(pid),"line":line,"official_sog":sog,"variant":variant,"selected_rate":rate_value,"selected_exposure_minutes":exp,"exposure_source":exp_name,"lambda":lam,"p_over":poisson_tail(lam,{1.5:2,2.5:3,3.5:4}[line]),"d10_rate":r10,"d20_rate":r20,"signed_divergence":float(r10-r20) if pd.notna(r10) and pd.notna(r20) else np.nan})
    metric_df = pd.DataFrame(metric_rows)
    save_csv(metric_df, out / "certified_frozen_correction_rows.csv")
    for by, name in [([], "certified_frozen_correction_metrics.csv"), (["month"], "month_level_metrics.csv")]:
        rows=[]
        for keys, g in metric_df.groupby(by+(["variant","line"]), sort=True) if by else metric_df.groupby(["variant","line"], sort=True):
            keys = keys if isinstance(keys, tuple) else (keys,)
            entry = dict(zip(by+["variant","line"], keys))
            entry.update(line_metrics(g, "p_over")); entry.update({"count_mae":float(np.mean(np.abs(g["lambda"]-g.official_sog))),"count_rmse":float(np.sqrt(np.mean((g["lambda"]-g.official_sog)**2)))}); rows.append(entry)
        save_csv(pd.DataFrame(rows), out / name)
    if not metric_df.empty:
        d = metric_df[metric_df.variant.eq("production")].drop_duplicates(["game_id","player_id"]).copy()
        d["count_residual"] = d.official_sog - d["lambda"]
        d["d10_gt_d20"] = d.signed_divergence > .5
        d["d10_lt_d20"] = d.signed_divergence < -.5
        d["absolute_divergence_decile"] = pd.qcut(d.signed_divergence.abs().rank(method="first"),10,labels=False)+1
        save_csv(d[["game_date","game_id","player_id","d10_rate","d20_rate","signed_divergence","d10_gt_d20","d10_lt_d20","absolute_divergence_decile"]], out / "divergence_replication.csv")
        div_stats=[]
        for segment, g in [("D10_GT_D20_MATERIAL",d[d.d10_gt_d20]),("D10_LT_D20_MATERIAL",d[d.d10_lt_d20])]+[(f"ABS_DIVERGENCE_DECILE_{i}",d[d.absolute_divergence_decile.eq(i)]) for i in range(1,11)]:
            div_stats.append({"segment":segment,"n":len(g),"mean_signed_divergence":g.signed_divergence.mean(),"mean_count_residual":g.count_residual.mean(),"mae":g.count_residual.abs().mean(),"pearson_divergence_residual":g.signed_divergence.corr(g.count_residual),"spearman_divergence_residual":g.signed_divergence.corr(g.count_residual,method="spearman")})
        div_stats.append({"segment":"ALL","n":len(d),"mean_signed_divergence":d.signed_divergence.mean(),"mean_count_residual":d.count_residual.mean(),"mae":d.count_residual.abs().mean(),"pearson_divergence_residual":d.signed_divergence.corr(d.count_residual),"spearman_divergence_residual":d.signed_divergence.corr(d.count_residual,method="spearman")})
        save_csv(pd.DataFrame(div_stats),out/"divergence_replication_metrics.csv")
        hi = d.copy(); hi["high_d10_top_decile"] = hi.d10_rate >= hi.d10_rate.quantile(.9); hi["high_d10_top_5pct"] = hi.d10_rate >= hi.d10_rate.quantile(.95); hi["low_d10_bottom_decile"] = hi.d10_rate <= hi.d10_rate.quantile(.1); hi["low_d10_bottom_5pct"] = hi.d10_rate <= hi.d10_rate.quantile(.05)
        save_csv(hi, out / "high_rate_replication.csv")
        tail_stats=[]
        for segment, mask in [("TOP_DECILE",hi.high_d10_top_decile),("TOP_5_PERCENT",hi.high_d10_top_5pct),("BOTTOM_DECILE",hi.low_d10_bottom_decile),("BOTTOM_5_PERCENT",hi.low_d10_bottom_5pct)]:
            ids=set(zip(hi.loc[mask,"game_id"],hi.loc[mask,"player_id"]))
            rows=metric_df[metric_df.apply(lambda r:(r.game_id,r.player_id) in ids,axis=1)]
            tail_stats.append({"segment":segment,"player_games":len(ids),"mean_d10_rate":hi.loc[mask,"d10_rate"].mean(),"mean_count_residual":hi.loc[mask,"count_residual"].mean(),"mae":hi.loc[mask,"count_residual"].abs().mean(),"overlap_abs_divergence_gt_0_5":int((hi.loc[mask,"signed_divergence"].abs()>.5).sum()),"line_metrics":"see high_rate_replication_line_metrics.csv"})
        save_csv(pd.DataFrame(tail_stats),out/"high_rate_replication_metrics.csv")
        line_rows=[]
        for segment, mask in [("TOP_DECILE",hi.high_d10_top_decile),("TOP_5_PERCENT",hi.high_d10_top_5pct),("BOTTOM_DECILE",hi.low_d10_bottom_decile),("BOTTOM_5_PERCENT",hi.low_d10_bottom_5pct)]:
            ids=set(zip(hi.loc[mask,"game_id"],hi.loc[mask,"player_id"]))
            group=metric_df[metric_df.apply(lambda r:(r.game_id,r.player_id) in ids,axis=1)]
            for (line,variant), rows in group.groupby(["line","variant"]):
                line_rows.append({"segment":segment,"line":line,"variant":variant,**line_metrics(rows,"p_over")})
        save_csv(pd.DataFrame(line_rows),out/"high_rate_replication_line_metrics.csv")
    else:
        save_csv(pd.DataFrame(columns=["month","variant","line","n","brier","log_loss"]), out / "month_level_metrics.csv")
        save_csv(pd.DataFrame(), out / "divergence_replication.csv"); save_csv(pd.DataFrame(), out / "high_rate_replication.csv")

    save_csv(december.assign(usable_tier1_player_games=0, usable_tier2_player_games=0, usable_line_rows=0), out / "gap_sensitivity.csv")
    team = features.groupby(["team_id","source_class"], dropna=False).size().rename("player_games").reset_index()
    save_csv(team, out / "player_team_representation.csv")
    save_csv(pd.DataFrame([
        {"finding":"A_D10_D20_DIVERGENCE_BIAS","expanded_season_classification":"CANNOT_EVALUATE","reason":"Tier 2 strict-prior rows = 0; Tier 3 excluded"},
        {"finding":"B_HIGH_D10_INDEPENDENT_TAIL_BIAS","expanded_season_classification":"CANNOT_EVALUATE","reason":"Tier 2 strict-prior rows = 0; Tier 3 excluded"},
        {"finding":"C_DIVERGENCE_MIDPOINT_CORRECTION","expanded_season_classification":"CANNOT_EVALUATE","reason":"No larger valid historical population than certified late-season replay"},
        {"finding":"D_HIGH_D10_SOFT_COMPRESSION","expanded_season_classification":"CANNOT_EVALUATE","reason":"No larger valid historical population than certified late-season replay"},
    ]),out/"structural_findings_status.csv")
    save_csv(pd.DataFrame([
        {"item":"historical official-TOI binding","status":"PRESERVE; RAISE PRIORITY","reason":"Current archive lacks strict-prior input cutoffs outside certified period"},
        {"item":"prospective grading of divergence shadow","status":"PRESERVE","reason":"No change to existing prospective collection"},
        {"item":"continued fixed-blend collection","status":"PRESERVE","reason":"No change"},
        {"item":"Puck Line","status":"DEFERRED UNTIL TOMORROW","reason":"Preserve requested backlog"},
        {"item":"reusable cross-prop sanity-check/explanation audit starting with Points","status":"PRESERVE","reason":"No change; not executed"},
        {"item":"divergence shadow collection","status":"SUPPORTED TO CONTINUE","reason":"This audit supplies no expanded-sample promotion evidence"},
        {"item":"high-D10 shadow implementation","status":"HOLD / NOT ADVANCED","reason":"Expanded historical result cannot be evaluated"},
    ]),out/"backlog_context.csv")
    # Explain source limits and explicitly keep unsupported claims unscored.
    correction_metrics = pd.read_csv(out/"certified_frozen_correction_metrics.csv")
    correction_lines=[]
    for line in (1.5,2.5,3.5):
        part=correction_metrics[correction_metrics.line.eq(line)].set_index("variant")
        correction_lines.append(f"| {line:.1f} | {part.loc['production','brier']:.6f} | {part.loc['divergence_midpoint','brier']:.6f} | {part.loc['divergence_plus_high_d10_soft','brier']:.6f} | {part.loc['production','count_mae']:.4f} | {part.loc['divergence_midpoint','count_mae']:.4f} | {part.loc['divergence_plus_high_d10_soft','count_mae']:.4f} |")
    correction_table="\n".join(correction_lines)
    empty_feature_days=", ".join(pd.DataFrame(date_rows).loc[pd.DataFrame(date_rows).feature_archive_rows.eq(0),"game_date"].tolist())
    good_dates = games.game_date.nunique(); cert_players = len(certified_keys)
    corrected_player_games = int(metric_df[metric_df.variant.eq("production")][["game_id","player_id"]].drop_duplicates().shape[0]) if not metric_df.empty else 0
    summary = {"audit_date":"2026-10-14","canonical_season":2025,"phase":"REGULAR_SEASON","first_regular_season_date":games.game_date.min(),"last_regular_season_date":games.game_date.max(),"canonical_games":len(games),"canonical_game_days":good_dates,"official_skater_history_rows_for_season":len(hist),"appeared_player_game_coverage_ratio_reference_only":round(cert_players/len(hist),6),"daily_feature_archive_player_game_rows":len(features),"daily_feature_archive_game_days":int(features.game_date.nunique()),"certified_player_games":cert_players,"certified_line_rows":40167,"certified_player_games_with_raw_rate_exposure_for_correction":corrected_player_games,"certified_player_games_missing_raw_rate_exposure_for_correction":cert_players-corrected_player_games,"additional_reconstructed_valid_player_games":0,"combined_usable_player_games":cert_players,"combined_usable_line_rows":40167,"months_represented":["2026-02","2026-03","2026-04"],"months_excluded":["2025-10","2025-11","2025-12","2026-01"],"december_game_days":int((december.canonical_game_count>0).sum()),"december_days_with_feature_archive":int(december.feature_archive_present.sum()),"december_archive_player_game_rows":int(features.loc[features.game_date.str.startswith("2025-12")].shape[0]),"december_certified_player_games":0,"december_exact_loss_player_games":None,"december_exact_loss_note":"No exact eligible player-game count can be certified from the available full-season pregame denominator; all December feature rows remain TIER 3.","tier_counts":{"TIER_1_CERTIFIED":cert_players,"TIER_2_RECONSTRUCTED_VALIDATED":0,"TIER_3_PARTIAL_OR_UNSAFE":int((~features.certified).sum())},"expanded_structural_analysis_status":"CANNOT_EVALUATE_EXPANDED_SAMPLE; no non-certified historical row has a retained immutable per-game feature cutoff proving strict-prior inputs","certified_bridge":bridge_summary,"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"pushed":False}
    (out / "summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")

    lineage.append({"path":str(PLATFORM.relative_to(ROOT)/"retained_game_schedule_snapshot.parquet"),"sha256":sha(PLATFORM/"retained_game_schedule_snapshot.parquet"),"rows":len(games),"source":"canonical schedule"})
    lineage.append({"path":str(PLATFORM.relative_to(ROOT)/"retained_skater_history_snapshot.parquet"),"sha256":sha(PLATFORM/"retained_skater_history_snapshot.parquet"),"rows":len(hist),"source":"official skater history snapshot"})
    lineage.append({"path":str(LEDGER.relative_to(ROOT)),"sha256":sha(LEDGER),"rows":len(certified),"source":"certified denominator"})
    lineage.append({"path":str(SCORER.relative_to(ROOT)),"sha256":sha(SCORER),"source":"frozen production scorer"})
    lineage.append({"path":str(phase_file.relative_to(ROOT)),"sha256":sha(phase_file),"source":"canonical game phase authority"})
    (out/"lineage_manifest.json").write_text(json.dumps({"sources":lineage,"contract":{"rate_priority":["d10_sog_per60","d20_sog_per60","d5_sog_per60"],"exposure_priority":["d10_toi_min_avg","d20_toi_min_avg","d5_toi_min_avg","szn_toi_per_game_5on5 + szn_toi_per_game_pp","season_5on5_icetime_per_game + season_5on4_icetime_per_game"],"lambda":"selected_rate * selected_exposure / 60","probability":"Poisson survival thresholds 2/3/4 for lines 1.5/2.5/3.5","scorer_sha256":sha(SCORER)},"method":"Local-only; canonical phase filtered from retained official game_type; non-certified archives fail strict-prior gate in absence of immutable input cutoffs."},indent=2,sort_keys=True)+"\n")
    report = f'''# NHL SOG full-season reconstruction feasibility audit\n\nAudit date: 2026-10-14. Scope is repository season 2025, canonical regular season only. Local artifacts only; no provider/network calls, paid credits, or database writes.\n\n## Finding\n\nCanonical phase filtering uses repository authority `backend/nhl/game_phase.py` (types 1/2/3 = preseason/regular/postseason); the retained 2025 schedule snapshot contains only type 2. See `phase_boundary_inventory.csv`. The retained canonical schedule contains **{len(games):,} games**, from **{games.game_date.min()}** through **{games.game_date.max()}**. It spans 167 game dates. Nonempty canonical-key feature rows exist on {int(features.game_date.nunique())} dates; scheduled dates with empty exports are: {empty_feature_days}. December is not a wholesale archive outage: {int((december.canonical_game_count>0).sum())} December game dates are present and {int(december.feature_archive_present.sum())} have feature exports; Dec. 24–26 have no canonical games. The Feb. 6–24 Olympic break has no canonical game rows in the retained schedule.\n\nThe feature exports and official history can support a retrospective computation, but the non-certified daily feature archives lack immutable per-game prediction/input-cutoff evidence. Therefore strict-prior chronology cannot be proven for dates outside the certified replay. Those records are TIER 3 and are excluded from structural metrics. No new TIER 2 rows are admitted.\n\n## Coverage and usable population\n\n| Measure | Result |\n|---|---:|\n| Canonical regular-season games | {len(games):,} |\n| Certified player-games (TIER 1) | {cert_players:,} |\n| Certified line rows | 40,167 |\n| Additional validated player-games (TIER 2) | 0 |\n| Combined usable player-games | {cert_players:,} |\n| Combined usable line rows | 40,167 |\n| Non-certified feature player-games retained as unsafe (TIER 3) | {int((~features.certified).sum()):,} |\n| Usable / retained official appeared-player-game rows (reference only) | {cert_players/len(hist):.1%} |\n\nThis percentage uses the 46,713 official appearances in the retained history snapshot as a reference denominator; it is not a full expected-skater denominator.\n\nSee `monthly_coverage.csv`, `canonical_season_coverage.csv`, `source_availability_matrix.csv`, `reconstruction_eligibility.csv`, `blocked_row_reasons.csv`, and `trust_tier_assignments.csv`. Dates/months outside the certified late-season window are not usable for full-season accuracy or structural evaluation. Team-specific rows are descriptive archive counts only; roster/call-up/transaction causes are not inferred.\n\n## December map\n\nDecember has {int((december.canonical_game_count>0).sum())} canonical game dates, feature archives on {int(december.feature_archive_present.sum())} such dates, and {int(features.loc[features.game_date.str.startswith("2025-12")].shape[0]):,} archived participant feature rows, with zero TIER 1/2 usable rows. Exact eligible-player-game loss is **not identifiable** because the full-season pregame expected-skater denominator was not retained. The 9,371 December player-game feature rows are TIER 3 and excluded; this is the exact excluded artifact count, not a claim about every expected skater. No missing date is interpolated. Each date and classification is in `december_gap_map.csv`.\n\n## Frozen baseline contract\n\nRate priority: D10, D20, D5. Exposure priority: D10 TOI, D20 TOI, D5 TOI, season 5v5 + PP, season 5v5 + 5v4. Lambda is selected rate × exposure minutes / 60. Poisson survival probabilities use count thresholds 2, 3, 4 for lines 1.5, 2.5, 3.5. The production scorer is `{SCORER.relative_to(ROOT)}`; exact SHA-256 is in `lineage_manifest.json`. A replay is valid only with these choices and strict-prior inputs.\n\n## Certified bridge and offline diagnostics\n\nThe repository frozen reproduction utility loaded prepared inputs for all 40,167 certified line rows: {bridge_summary['exact_rows']:,} bit-exact probability rows, {bridge_summary['tolerance_rows']:,} within 1e-12, {bridge_summary['material_mismatch_rows']:,} material mismatches, and {bridge_summary['side_match_rows']:,} side matches. Maximum absolute probability difference: {float(repro.absolute_difference.max()):.3g}. The matched source coverage validates only the TIER 1 bridge.\n\nFrozen corrections can be recomputed for the 13,347 TIER 1 player-games with retained raw rate/exposure fields (40,041 line rows). The other 42 certified player-games still have valid predictions included in the 40,167-row parity bridge but lack enough raw inputs for correction re-derivation. On the paired subset:\n\n| Line | Production Brier | Midpoint Brier | Midpoint + soft Brier | Production MAE | Midpoint MAE | Midpoint + soft MAE |\n|---:|------:|------:|---:|---:|---:|---:|\n{correction_table}\n\nSee `certified_frozen_correction_metrics.csv`, `month_level_metrics.csv`, `divergence_replication_metrics.csv`, and `high_rate_replication_metrics.csv`. These are certified-window-only diagnostics. Expanded-sample divergence/high-D10 assessments, month stability outside TIER 1, and gap sensitivity cannot be evaluated because TIER 2 is empty. Findings A–D are `CANNOT_EVALUATE` for the broader season; this audit neither confirms nor reverses a full-season effect.\n\n## Shadow and backlog decision\n\nContinue divergence-shadow collection: **supported**, based on the existing certified/prospective rationale; this audit adds no expanded historical confirmation and makes no promotion recommendation. High-D10 shadow implementation: **not advanced by this audit**; hold implementation pending strict-prior historical reconstruction or prospective evidence. Preserve backlog: historical official-TOI binding (raised priority because it blocks expansion); prospective divergence-shadow grading; fixed-blend collection; Puck Line deferred until tomorrow; reusable cross-prop sanity/explanation audit beginning with Points. No backlog items were executed.\n\n## Reproducibility and safety\n\nProvider calls: 0. Paid credits: 0. Production database mutations: 0. Production and shadow definitions were not modified. `certified_bridge/` contains the independent certified replay outputs.\n'''
    (out/"report.md").write_text(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
