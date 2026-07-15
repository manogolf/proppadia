#!/usr/bin/env python3
"""Characterize NHL season 2025 SOG features on the frozen baseline spine.

No model is fit, no thresholds are optimized, and no source is mutated.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

STAMP = "2026-07-13"
EXPECTED_REPRO_ROWS = 40167
EXPECTED_PLAYER_GAMES = 13389
BASELINE_FIELDS = {
    "d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "d5_toi_min_avg",
    "d10_toi_min_avg", "d20_toi_min_avg", "szn_toi_per_game_5on5",
    "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game",
}
IDENTITY_FIELDS = {"player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date", "season"}
OUTCOME_FIELDS = {"shots_on_goal"}

FAMILY_FIELDS = {
    "PLAYER_SHOOTING_SKILL": ["d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "attempts_d10_per60", "num_sog_last5", "num_sog_last10", "num_sog_szn_to_date", "num_event_last5", "num_event_last10", "num_event_szn_to_date"],
    "PLAYER_OPPORTUNITY": ["d5_toi_min_avg", "d10_toi_min_avg", "d20_toi_min_avg", "szn_toi_per_game_5on5", "szn_toi_per_game_pp", "szn_toi_per_game_pk", "szn_shifts_per_game_5on5", "szn_shifts_per_game_pp", "szn_shifts_per_game_pk", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game", "season_4on5_icetime_per_game"],
    "LINE_POWER_PLAY_ROLE": ["role_pp_share", "d10_top_mate_overlap_share_avg", "d10_top_mate_overlap_share_std", "d10_top3_mates_overlap_share_avg", "d10_top3_mates_overlap_share_std", "d20_top_mate_overlap_share_avg", "d20_top_mate_overlap_share_std", "d20_top3_mates_overlap_share_avg", "d20_top3_mates_overlap_share_std", "d20_top_mate_repeat_rate"],
    "TEAM_OFFENSIVE_ENVIRONMENT": ["team_d10_sf_per_game", "pace_index", "team_num_sog_last10", "team_num_event_last10", "last10_team_sog_share", "team_szn_5on5_top_line_xgf_share", "team_5v5_top_line_icetime_share", "team_5v5_top_line_shotattempts_share"],
    "OPPONENT_DEFENSE_ENVIRONMENT": ["opp_d10_sf_allowed_per_game", "pace_matchup_index", "opp_d10_sf_per60", "team_d10_sa_per60", "opp_d10_sa_per60"],
    "SCHEDULE_REST_TRAVEL": ["rest_days", "b2b_flag"],
    "RECENT_FORM_PERSISTENCE": ["hot_last5_flag", "last10_team_sog_share", "d20_top_mate_repeat_rate"],
    "PAIRING_DATA_QUALITY": ["d10_shiftcharts_games", "d10_shiftcharts_coverage_rate", "d20_shiftcharts_games", "d20_shiftcharts_coverage_rate", "d10_pairings_available", "d20_pairings_available", "d10_pairings_cov_bucket", "d20_pairings_cov_bucket"],
}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""): h.update(chunk)
    return h.hexdigest()


def write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, lineterminator="\n")


def owner(field: str) -> str:
    if field in IDENTITY_FIELDS: return "IDENTITY_METADATA"
    if field in OUTCOME_FIELDS: return "OUTCOME_ONLY"
    if field in FAMILY_FIELDS["PLAYER_SHOOTING_SKILL"]: return "PLAYER_SKILL"
    if field in FAMILY_FIELDS["PLAYER_OPPORTUNITY"]: return "PLAYER_OPPORTUNITY"
    if field in FAMILY_FIELDS["LINE_POWER_PLAY_ROLE"] or "pairing" in field or "mate" in field: return "PLAYER_ROLE"
    if field in FAMILY_FIELDS["TEAM_OFFENSIVE_ENVIRONMENT"]: return "TEAM_OFFENSE_ENVIRONMENT"
    if field in FAMILY_FIELDS["OPPONENT_DEFENSE_ENVIRONMENT"]: return "OPPONENT_DEFENSE_ENVIRONMENT"
    if field in FAMILY_FIELDS["SCHEDULE_REST_TRAVEL"]: return "SCHEDULE_REST_TRAVEL"
    if field in FAMILY_FIELDS["RECENT_FORM_PERSISTENCE"]: return "RECENT_FORM_PERSISTENCE"
    return "UNRESOLVED"


def timing(field: str) -> tuple[str, str, str]:
    if field in IDENTITY_FIELDS: return "RAW_AUTHORITATIVE", "STRICT_PRIOR_VERIFIED", "EXACT_PREPARED_VALUE"
    if field in OUTCOME_FIELDS: return "POSTGAME_ONLY", "POSTGAME", "NOT_REPLAYABLE"
    if field == "role_pp_share": return "DERIVED_TIMING_UNVERIFIED", "DATE_ONLY_TIMING", "EXACT_PREPARED_VALUE"
    if field.startswith("team_szn_5on5") or field.startswith("team_5v5_top_line") or field.startswith("season_5on4_shifts") or field.startswith("season_4on5_shifts"):
        return "UNKNOWN", "BLOCKED_BY_NO_TIMESTAMP", "EXACT_PREPARED_VALUE"
    return "DERIVED_STRICT_PRIOR", "STRICT_PRIOR_VERIFIED", "EXACT_PREPARED_VALUE"


def visibility(field: str) -> str:
    if field in BASELINE_FIELDS: return "BASELINE_INPUT"
    if field in OUTCOME_FIELDS or field in IDENTITY_FIELDS: return "NOT_APPLICABLE"
    if field in {x for xs in FAMILY_FIELDS.values() for x in xs}: return "PRODUCTION_AVAILABLE_NOT_USED"
    return "MODEL_BLIND"


def semantic(field: str) -> str:
    descriptions = {
        "d5_sog_per60": "prior five-game SOG per 60", "d10_sog_per60": "prior ten-game SOG per 60", "d20_sog_per60": "prior twenty-game SOG per 60",
        "attempts_d10_per60": "prior ten-game shot attempts per 60", "role_pp_share": "pregame table power-play role share with date-only timing",
        "team_d10_sf_per_game": "team recent shot-for context", "opp_d10_sf_allowed_per_game": "opponent recent shots-for-allowed compatibility field",
        "pace_matchup_index": "derived team/opponent pace matchup", "rest_days": "days since prior appearance", "b2b_flag": "one-day-rest indicator",
        "shots_on_goal": "realized target-game SOG; outcome only", "hot_last5_flag": "recent outcome-level flag from prior window",
    }
    return descriptions.get(field, field.replace("_", " "))


def family_for(field: str) -> str:
    matches = [fam for fam, fields in FAMILY_FIELDS.items() if field in fields]
    return matches[0] if matches else owner(field)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reproduction-ledger", required=True)
    ap.add_argument("--prepared-root", default="artifacts/archive/generated_daily/nhl")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    repro_path, out_dir = Path(args.reproduction_ledger), Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    r = pd.read_csv(repro_path, low_memory=False)
    if len(r) != EXPECTED_REPRO_ROWS or r.prediction_identity.nunique() != EXPECTED_REPRO_ROWS or set(r.canonical_season) != {2025}:
        raise RuntimeError("Frozen reproduction spine mismatch")
    pg = r.sort_values(["game_date", "game_id", "player_id", "line"]).drop_duplicates(["game_date", "game_id", "player_id"]).copy()
    if len(pg) != EXPECTED_PLAYER_GAMES: raise RuntimeError("Player-game spine mismatch")

    frames, source_audit = [], []
    root = Path(args.prepared_root)
    for ds in sorted(pg.game_date.unique()):
        fp = root / ds / "sog_features" / f"sog_features_{ds}_denali.csv"
        if not fp.exists(): raise RuntimeError(f"Missing prepared source {fp}")
        d = pd.read_csv(fp, low_memory=False); d["game_date"] = d.game_date.astype(str)
        source_audit.append({"game_date": ds, "source_path": str(fp), "rows": len(d), "sha256": sha(fp)})
        frames.append(d)
    prepared = pd.concat(frames, ignore_index=True)
    key = ["game_date", "game_id", "player_id"]
    prepared.game_id = pd.to_numeric(prepared.game_id, errors="raise").astype("int64")
    prepared.player_id = pd.to_numeric(prepared.player_id, errors="raise").astype("int64")
    if prepared.duplicated(key).any(): raise RuntimeError("Prepared natural-grain duplicate")
    joined = pg.merge(prepared, on=key, how="left", validate="one_to_one", suffixes=("_spine", ""), indicator=True)
    if len(joined) != EXPECTED_PLAYER_GAMES or (joined._merge != "both").any(): raise RuntimeError("Fixed-spine join loss")
    joined["baseline_residual"] = joined.official_sog.astype(float) - joined.regenerated_expected_sog.astype(float)
    joined["baseline_correct"] = joined.settlement_status.eq("WIN").astype(int)
    joined["calendar_month"] = joined.game_date.str[:7]

    fields = list(prepared.columns)
    inventory = []
    for f in fields:
        lineage, time_status, replay = timing(f)
        s = joined[f] if f in joined.columns else pd.Series(dtype=float)
        inventory.append({"canonical_concept": semantic(f), "raw_field": f, "source": "saved daily Denali prepared feature archive", "source_authority": "PRODUCTION_PREPARED_VALUE" if f not in OUTCOME_FIELDS else "DERIVED_OUTCOME_COPY", "grain": "player-game", "date_coverage": f"{pg.game_date.min()}..{pg.game_date.max()}", "season_coverage": "2025", "missing_rows": int(s.isna().sum()), "missing_rate": float(s.isna().mean()), "timing_availability": time_status, "strict_prior_status": time_status, "production_usage": visibility(f), "research_usage": "DENALI_AND_POISSON_RESEARCH_PLATFORM" if f not in IDENTITY_FIELDS | OUTCOME_FIELDS else "NOT_FEATURE", "ui_report_usage": "CONTEXT_OR_UNKNOWN", "historical_replayability": replay, "future_daily_collectability": "YES_EXISTING_EXPORT" if f not in OUTCOME_FIELDS else "POSTGAME_ONLY", "owner": owner(f), "duplication_status": "ALIAS_OR_OVERLAP_REVIEW" if f in {"pace_index", "num_sog_last5", "num_sog_last10", "num_sog_szn_to_date", "num_event_last5", "num_event_last10", "num_event_szn_to_date"} else "UNIQUE_FIELD", "notes": lineage})
    # Materially relevant repository concepts absent from the prepared vectors.
    extras = [
        ("expected goalie identity", "projected_goalie_id", "GOALIE_GAME_ENVIRONMENT", "FUTURE_OBSERVATION_ONLY", "BLOCKED_BY_NO_TIMESTAMP"),
        ("goalie start probability", "projected_goalie_start_prob", "GOALIE_GAME_ENVIRONMENT", "FUTURE_OBSERVATION_ONLY", "BLOCKED_BY_NO_TIMESTAMP"),
        ("injury status", "injury_status", "LINEUP_AVAILABILITY", "NOT_REPLAYABLE", "BLOCKED_BY_SOURCE_CONTINUITY"),
        ("scratch certainty", "scratch_status", "LINEUP_AVAILABILITY", "NOT_REPLAYABLE", "BLOCKED_BY_NO_TIMESTAMP"),
        ("travel distance", "travel_distance", "SCHEDULE_REST_TRAVEL", "NOT_REPLAYABLE", "UNKNOWN"),
        ("market price", "price_over_or_under", "MARKET_CONTEXT", "BOUNDED_RECONSTRUCTION", "BLOCKED_BY_NO_TIMESTAMP"),
        ("market line movement", "line_movement", "MARKET_CONTEXT", "NOT_REPLAYABLE", "BLOCKED_BY_NO_TIMESTAMP"),
        ("candidate policy threshold", "policy_threshold", "SELECTION_POLICY", "NOT_REPLAYABLE", "BLOCKED_BY_NO_RUN_BOUND_POLICY"),
        ("defense surprise ratio", "defense_surprise_ratio", "OPPONENT_DEFENSE_ENVIRONMENT", "BOUNDED_RECONSTRUCTION", "DATE_ONLY_TIMING"),
    ]
    for concept, raw, own, replay, time_status in extras:
        inventory.append({"canonical_concept": concept, "raw_field": raw, "source": "repository research/market/current-state paths", "source_authority": "SUPPORTING_OR_UNCERTIFIED", "grain": "varies; not joined", "date_coverage": "partial or unknown", "season_coverage": "2025 partial", "missing_rows": EXPECTED_PLAYER_GAMES, "missing_rate": 1.0, "timing_availability": time_status, "strict_prior_status": time_status, "production_usage": "RESEARCH_ONLY" if own != "SELECTION_POLICY" else "SELECTION_ONLY", "research_usage": "REPOSITORY_CONCEPT", "ui_report_usage": "varies", "historical_replayability": replay, "future_daily_collectability": "REQUIRES_CERTIFICATION", "owner": own, "duplication_status": "CONCEPT_ONLY", "notes": "not present in exact prepared control vectors"})
    inv = pd.DataFrame(inventory)
    write(out_dir / f"nhl_season_2025_sog_feature_inventory_{STAMP}.csv", inv)

    ownership = inv[["canonical_concept", "raw_field", "owner", "grain", "production_usage", "historical_replayability", "duplication_status"]].copy()
    ownership["ownership_decision"] = np.where(ownership.owner == "UNRESOLVED", "REQUIRES_OWNER", "OWNER_ASSIGNED")
    write(out_dir / f"nhl_season_2025_sog_feature_ownership_registry_{STAMP}.csv", ownership)
    lineage = inv[["raw_field", "source", "source_authority", "grain", "strict_prior_status", "production_usage", "research_usage", "historical_replayability", "future_daily_collectability", "notes"]].copy()
    write(out_dir / f"nhl_season_2025_sog_feature_lineage_matrix_{STAMP}.csv", lineage)

    dup_groups = [
        ["RECENT_SOG_RATE", "d5_sog_per60|d10_sog_per60|d20_sog_per60", "parent-child windows", "PLAYER_SKILL", "retain distinct windows; baseline coalesce order is canonical"],
        ["EXPECTED_TOI", "d5_toi_min_avg|d10_toi_min_avg|d20_toi_min_avg|szn_toi_per_game_5on5+szn_toi_per_game_pp|season_5on5_icetime_per_game+season_5on4_icetime_per_game", "multiple fallback definitions and units", "PLAYER_OPPORTUNITY", "retain formula-specific coalesce and unit conversion"],
        ["PACE", "team_d10_sf_per_game|opp_d10_sf_allowed_per_game|pace_matchup_index|pace_index|opp_d10_sf_per60|team_d10_sa_per60|opp_d10_sa_per60", "overlapping team/opponent definitions", "TEAM_OFFENSE_ENVIRONMENT", "separate offense from opponent defense; pace_index aliases pace_matchup_index"],
        ["SOG_COUNTS", "num_sog_last5|num_sog_last10|num_sog_szn_to_date|num_event_last5|num_event_last10|num_event_szn_to_date", "SOG versus event-shot counts and windows", "PLAYER_SKILL", "do not treat event shots as SOG aliases"],
        ["PP_ROLE", "role_pp_share|szn_toi_per_game_pp|season_5on4_icetime_per_game|szn_shifts_per_game_pp", "role share versus realized prior opportunity", "PLAYER_ROLE", "role share timing requires certification"],
        ["LINE_STABILITY", "mate overlap averages|standard deviations|repeat rate|coverage flags", "signal and data-quality concepts mixed", "PLAYER_ROLE", "separate overlap signal from shiftchart coverage"],
        ["REST", "rest_days|b2b_flag", "deterministic parent-child", "SCHEDULE_REST_TRAVEL", "b2b is rest_days=1; retain one canonical owner"],
        ["GOALIE_CONTEXT", "projected goalie|actual goalie|goalie quality", "certainty and quality conflated in research", "GOALIE_GAME_ENVIRONMENT", "do not use actual goalie as pregame substitute"],
    ]
    write(out_dir / f"nhl_season_2025_sog_feature_duplicate_concept_audit_{STAMP}.csv", pd.DataFrame(dup_groups, columns=["canonical_concept", "fields", "conflict_type", "recommended_owner", "resolution"] ))

    join_rows = []
    for fam, fs in FAMILY_FIELDS.items():
        present = [f for f in fs if f in joined.columns]
        any_value = joined[present].notna().any(axis=1) if present else pd.Series(False, index=joined.index)
        all_value = joined[present].notna().all(axis=1) if present else pd.Series(False, index=joined.index)
        strict = [f for f in present if timing(f)[1] == "STRICT_PRIOR_VERIFIED"]
        strict_any = joined[strict].notna().any(axis=1) if strict else pd.Series(False, index=joined.index)
        join_rows.append({"feature_family": fam, "source_rows": len(prepared), "unique_source_player_games": len(prepared), "control_player_games": len(joined), "joinable_player_games": int(any_value.sum()), "joined_prediction_rows": int(any_value.sum()*3), "exact_join_rate": float(any_value.mean()), "all_family_fields_present_rate": float(all_value.mean()), "missing_player_games": int((~any_value).sum()), "duplicate_source_keys": 0, "one_to_many_joins": 0, "many_to_many_joins": 0, "identity_conflicts": 0, "timing_qualified_player_games": int(strict_any.sum()), "strict_prior_qualified_player_games": int(strict_any.sum()), "exact_replay_player_games": int(any_value.sum()), "bounded_reconstruction_player_games": 0, "blocked_player_games": int((~any_value).sum()), "natural_grain": "player-game", "broadcast_rule": "one player-game value to exactly three fixed line rows"})
    join_df = pd.DataFrame(join_rows)
    write(out_dir / f"nhl_season_2025_sog_feature_fixed_spine_join_audit_{STAMP}.csv", join_df)

    timing_rows = []
    for f in fields:
        lin, t, rep = timing(f)
        timing_rows.append({"raw_field": f, "owner": owner(f), "lineage_status": lin, "timing_decision": t, "same_game_leakage_risk": "CONFIRMED_OUTCOME_FIELD" if f in OUTCOME_FIELDS else "NO_SAME_GAME_REFERENCE_IN_PRODUCING_SQL" if t == "STRICT_PRIOR_VERIFIED" else "UNRESOLVED", "mutable_source_risk": "LOW_SAVED_VALUE" if rep == "EXACT_PREPARED_VALUE" else "HIGH", "evidence": "export_sog_denali_pregame.sql uses game_date < slate_date; pairings/season TOI SQL use prior_games; role/current context lacks prediction-time timestamp", "use_decision": "EXCLUDE_FROM_FEATURES" if f in OUTCOME_FIELDS else "HISTORICAL_OK" if t == "STRICT_PRIOR_VERIFIED" else "CHARACTERIZE_ONLY"})
    write(out_dir / f"nhl_season_2025_sog_feature_timing_and_leakage_audit_{STAMP}.csv", pd.DataFrame(timing_rows))

    numeric = [f for f in fields if f not in IDENTITY_FIELDS | OUTCOME_FIELDS and pd.api.types.is_numeric_dtype(joined[f])]
    diag = []
    for f in numeric:
        x = pd.to_numeric(joined[f], errors="coerce")
        valid = x.notna()
        if valid.sum() < 50: continue
        row = {"feature_family": family_for(f), "raw_field": f, "player_games": len(joined), "nonmissing": int(valid.sum()), "missing_rate": float(1-valid.mean()), "mean": x[valid].mean(), "std": x[valid].std(), "min": x[valid].min(), "q25": x[valid].quantile(.25), "median": x[valid].median(), "q75": x[valid].quantile(.75), "max": x[valid].max(), "spearman_official_sog": x.corr(joined.official_sog.astype(float), method="spearman"), "spearman_baseline_residual": x.corr(joined.baseline_residual, method="spearman"), "spearman_baseline_correctness": x.corr(joined.baseline_correct, method="spearman"), "strict_prior_status": timing(f)[1]}
        month_corr = []
        for _, g in joined[valid].groupby("calendar_month"):
            if len(g) >= 100 and pd.to_numeric(g[f], errors="coerce").nunique() > 1:
                month_corr.append(pd.to_numeric(g[f], errors="coerce").corr(g.baseline_residual, method="spearman"))
        nz = [v for v in month_corr if pd.notna(v) and abs(v) > 1e-12]
        row["monthly_segments_evaluated"] = len(nz)
        row["monthly_residual_sign_consistency"] = max(sum(v>0 for v in nz),sum(v<0 for v in nz))/len(nz) if nz else np.nan
        diag.append(row)
    diag_df = pd.DataFrame(diag)
    write(out_dir / f"nhl_season_2025_sog_feature_association_diagnostics_{STAMP}.csv", diag_df)

    family_rows, screen_rows = [], []
    for fam, fs in FAMILY_FIELDS.items():
        jd = join_df[join_df.feature_family == fam].iloc[0]
        fd = diag_df[diag_df.feature_family == fam]
        residual_abs_median = float(fd.spearman_baseline_residual.abs().median()) if len(fd) else np.nan
        stable = int((fd.monthly_residual_sign_consistency >= .667).sum()) if len(fd) else 0
        strict_fields = sum(timing(f)[1] == "STRICT_PRIOR_VERIFIED" for f in fs if f in fields)
        if jd.exact_join_rate < .5: classification = "INSUFFICIENT_COVERAGE"
        elif strict_fields == 0: classification = "TIMING_NOT_CERTIFIED"
        elif fam in {"PLAYER_SHOOTING_SKILL", "PLAYER_OPPORTUNITY"}: classification = "INFORMATION_LIKELY_ALREADY_IN_BASELINE"
        elif residual_abs_median >= .03 and stable >= 1: classification = "POTENTIAL_INCREMENTAL_INFORMATION"
        else: classification = "WEAK_OR_UNSTABLE_ASSOCIATION"
        family_rows.append({"feature_family": fam, "conceptual_domain": fam, "fields_present": "|".join([f for f in fs if f in fields]), "fields_requested": len(fs), "fields_present_count": sum(f in fields for f in fs), "player_game_join_rate": jd.exact_join_rate, "strict_prior_fields": strict_fields, "median_absolute_residual_spearman": residual_abs_median, "temporally_stable_field_count": stable, "baseline_visibility": "MIXED" if any(f in BASELINE_FIELDS for f in fs) else "MODEL_BLIND", "characterization": classification, "notes": "descriptive only; no fitted model or optimized threshold"})
        screen_rows.append({"feature_family": fam, "incremental_information_class": classification, "fixed_spine_coverage": jd.exact_join_rate, "median_absolute_residual_spearman": residual_abs_median, "stable_field_count": stable, "evidence_across_multiple_segments": "YES" if stable else "NO", "historical_experiment_decision": "READY_FOR_LIMITED_HISTORICAL_EXPERIMENT" if classification == "POTENTIAL_INCREMENTAL_INFORMATION" and strict_fields else "DEFER", "caveat": "association is not lift; baseline overlap and multiplicity remain"})
    family_df, screen_df = pd.DataFrame(family_rows), pd.DataFrame(screen_rows)
    write(out_dir / f"nhl_season_2025_sog_feature_family_characterization_{STAMP}.csv", family_df)
    write(out_dir / f"nhl_season_2025_sog_feature_incremental_information_screen_{STAMP}.csv", screen_df)

    hist = family_df[["feature_family", "player_game_join_rate", "strict_prior_fields", "characterization"]].copy()
    hist["historical_readiness"] = np.where((hist.player_game_join_rate >= .9) & (hist.strict_prior_fields > 0), "READY_FOR_LIMITED_HISTORICAL_EXPERIMENT", np.where(hist.player_game_join_rate >= .5, "READY_FOR_CHARACTERIZATION", "BLOCKED_BY_COVERAGE"))
    hist["required_boundary"] = "frozen 40167-row control; candidate policy excluded"
    write(out_dir / f"nhl_season_2025_sog_feature_historical_readiness_{STAMP}.csv", hist)
    future = hist[["feature_family"]].copy()
    future["season_2026_readiness"] = future.feature_family.map({"PLAYER_SHOOTING_SKILL":"READY_FOR_PROSPECTIVE_OBSERVATION", "PLAYER_OPPORTUNITY":"READY_FOR_PROSPECTIVE_OBSERVATION", "LINE_POWER_PLAY_ROLE":"READY_WITH_BOUNDED_LIMITS", "TEAM_OFFENSIVE_ENVIRONMENT":"READY_FOR_PROSPECTIVE_OBSERVATION", "OPPONENT_DEFENSE_ENVIRONMENT":"READY_FOR_PROSPECTIVE_OBSERVATION", "SCHEDULE_REST_TRAVEL":"READY_WITH_BOUNDED_LIMITS", "RECENT_FORM_PERSISTENCE":"READY_FOR_PROSPECTIVE_OBSERVATION", "PAIRING_DATA_QUALITY":"READY_WITH_BOUNDED_LIMITS"}).fillna("DEFER")
    future["operational_gap"] = np.where(future.season_2026_readiness == "READY_FOR_PROSPECTIVE_OBSERVATION", "requires morning timestamp/coverage gate", "timing/source continuity or grain certification incomplete")
    write(out_dir / f"nhl_season_2026_sog_feature_operational_readiness_{STAMP}.csv", future)
    write(out_dir / f"nhl_season_2025_sog_feature_source_audit_{STAMP}.csv", pd.DataFrame(source_audit))
    summary = {"prediction_rows": len(r), "player_games": len(pg), "prepared_columns": len(fields), "inventory_rows": len(inv), "numeric_diagnostic_fields": len(diag_df), "families": len(family_df), "source_dates": len(source_audit), "reproduction_ledger_sha256": sha(repro_path)}
    (out_dir / f"nhl_season_2025_sog_feature_characterization_run_summary_{STAMP}.json").write_text(json.dumps(summary, indent=2, sort_keys=True)+"\n")
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__": raise SystemExit(main())
