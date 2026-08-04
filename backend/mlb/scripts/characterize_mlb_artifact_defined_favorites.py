#!/usr/bin/env python3
"""Characterize immutable deployed MLB favorite artifacts under lineage Path D."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd


PROPS = {"hits", "total_bases", "strikeouts_pitching"}
LABEL = "HISTORICAL_DEPLOYED_ARTIFACT_CHARACTERIZATION_ONLY"
DECISION = "ARTIFACT_DEFINED_CHARACTERIZATION_COMPLETED_TRANSFERABILITY_UNRESOLVED"


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def text_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def git_commit_before(path: str, date: str) -> str:
    cp = subprocess.run(
        ["git", "log", "-1", "--format=%H", f"--before={date} 23:59:59", "--", path],
        check=False,
        text=True,
        capture_output=True,
    )
    return cp.stdout.strip()


def american_implied(price: pd.Series) -> pd.Series:
    p = pd.to_numeric(price, errors="coerce")
    return pd.Series(np.where(p < 0, -p / (-p + 100.0), 100.0 / (p + 100.0)), index=p.index)


def pnl(price: float, result: str) -> float | None:
    if result == "push":
        return 0.0
    if result == "loss":
        return -1.0
    if result != "win" or not math.isfinite(price) or price == 0:
        return None
    return price / 100.0 if price > 0 else 100.0 / abs(price)


def prob_band(value: object) -> str:
    x = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(x): return "missing"
    for lo, hi in [(0.50, .55), (.55, .60), (.60, .65), (.65, .70), (.70, .75), (.75, .80), (.80, .90), (.90, 1.000001)]:
        if lo <= float(x) < hi: return f"{lo:.2f}-{min(hi,1):.2f}"
    return "outside"


def price_band(value: object) -> str:
    x = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(x): return "missing"
    x = float(x)
    if -149 <= x <= -100: return "-100_to_-149"
    if -199 <= x <= -150: return "-150_to_-199"
    if -249 <= x <= -200: return "-200_to_-249"
    if x <= -250: return "-250_or_shorter"
    return "plus_money_or_other"


def summarize(group: pd.DataFrame) -> dict:
    resolved = group[group["selected_outcome"].isin(["win", "loss"])].copy()
    scores = group[group["selected_outcome"].isin(["win", "loss"]) & group["model_probability"].between(0, 1)].copy()
    wins = int((group.selected_outcome == "win").sum())
    losses = int((group.selected_outcome == "loss").sum())
    pushes = int((group.selected_outcome == "push").sum())
    returns = pd.to_numeric(group["pnl_1u"], errors="coerce")
    y = (scores.selected_outcome == "win").astype(float)
    p = scores.model_probability.astype(float).clip(1e-12, 1 - 1e-12)
    return {
        "eligible_rows": int(len(group)),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "dnp_void_unresolved": int(len(group) - wins - losses - pushes),
        "binary_win_rate": wins / len(resolved) if len(resolved) else np.nan,
        "roi_1u": returns.mean() if returns.notna().any() else np.nan,
        "average_price": group.selected_side_price.mean(),
        "aggregate_break_even_rate": group.break_even_probability.mean(),
        "average_model_probability": group.model_probability.mean(),
        "average_no_vig_market_probability": group.selected_side_no_vig_implied.mean(),
        "calibration_gap": p.mean() - y.mean() if len(scores) else np.nan,
        "brier_score": ((p - y) ** 2).mean() if len(scores) else np.nan,
        "log_loss": (-(y * np.log(p) + (1 - y) * np.log(1 - p))).mean() if len(scores) else np.nan,
    }


def grouped_report(df: pd.DataFrame, dimensions: list[str]) -> pd.DataFrame:
    rows = []
    for key, group in df.groupby(dimensions, dropna=False, sort=True):
        key = key if isinstance(key, tuple) else (key,)
        rows.append({LABEL: True, **dict(zip(dimensions, key)), **summarize(group)})
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reconcile-csv", required=True)
    ap.add_argument("--prior-manifest", required=True)
    ap.add_argument("--odds-root", default="backend/mlb/exports/odds_history")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    out = Path(args.out_dir)
    if out.exists() and (out / "SHA256SUMS.csv").exists():
        raise SystemExit(f"immutable completed output exists: {out}")
    out.mkdir(parents=True, exist_ok=True)
    root = Path(args.odds_root)

    manifest = pd.read_csv(args.prior_manifest)
    chosen = manifest[manifest.decision.eq("CHOSEN")].copy()
    chosen_by_date = chosen.set_index("game_date").to_dict("index")

    # Outcome-blind lineage reconstruction and safe-path decision.
    lineage = []
    enrich_frames = []
    for date, meta in chosen_by_date.items():
        tag = str(meta["run_tag"])
        slate = root / date / f"mlb_slate_output__{tag}.csv"
        wide = root / date / f"mlb_predictions_wide_calibrated__{tag}.csv"
        odds = root / date / f"odds_mlb_playerprops__{tag}.json"
        slate_df = pd.read_csv(slate, low_memory=False)
        wide_df = pd.read_csv(wide, low_memory=False)
        slate_cols = list(slate_df.columns)
        wide_cols = list(wide_df.columns)
        schema_fp = text_hash("|".join(slate_cols) + "||" + "|".join(wide_cols))
        cal_methods = sorted(set(slate_df.get("calibration_method", pd.Series(dtype=str)).fillna("").astype(str)))
        prediction_sources = sorted(set(slate_df.get("prediction_source_file", pd.Series(dtype=str)).fillna("").astype(str)))
        code_commit = git_commit_before("backend/mlb/scripts/build_mlb_predictions_wide.py", date)
        pred_commit = git_commit_before("backend/mlb/prediction/make_prediction.py", date)
        config_fp = text_hash(json.dumps({"calibration": cal_methods, "sources": prediction_sources, "wide_columns": wide_cols}, sort_keys=True))
        lineage.append({
            "date": date, "selected_run_tag": tag, "prediction_artifact": str(slate),
            "odds_artifact": str(odds), "prediction_artifact_sha": sha(slate),
            "producing_script_path": "backend/mlb/scripts/build_mlb_predictions_wide.py|backend/mlb/prediction/make_prediction.py|backend/mlb/scripts/build_mlb_slate_output.py",
            "script_git_commit_or_bounded_interval": f"wide={code_commit};prediction={pred_commit}",
            "loaded_model_path": "/var/data/proppadia/models/latest/{prop}.joblib (code default; historical resolved path not logged)",
            "model_artifact_sha": "", "calibration_method": "|".join(cal_methods) or "none_recorded",
            "configuration_fingerprint": config_fp, "feature_schema_fingerprint": schema_fp,
            "known_semantic_changes_in_force": "mutable latest model pointer; code/workflow changes occur within May-June; no per-run model binding",
            "lineage_evidence_sources": f"{slate};{wide};{odds};git history;model-loading code;prior snapshot manifest",
            "lineage_confidence": "ARTIFACT_ONLY",
            "unresolved_contradictions": "archived candidate model artifacts exist but deployment of any artifact to this run is not affirmatively bound",
        })
        keys = ["game_id", "player_id", "prop_type", "line"]
        keep = [c for c in keys + ["calibration_method"] if c in slate_df.columns]
        s = slate_df[keep].copy()
        for c in keys: s[c] = pd.to_numeric(s[c], errors="coerce") if c != "prop_type" else s[c].astype(str).str.lower()
        s["game_date"] = date; s["schema_fingerprint"] = schema_fp; s["configuration_fingerprint"] = config_fp
        game_times = wide_df[["game_id", "game_time"]].drop_duplicates("game_id") if "game_time" in wide_df else pd.DataFrame(columns=["game_id", "game_time"])
        s = s.merge(game_times, on="game_id", how="left")
        enrich_frames.append(s)
    lineage_df = pd.DataFrame(lineage)
    lineage_df.to_csv(out / "historical_model_lineage_evidence.csv", index=False)
    safe_path = {
        "path": "D", "label": LABEL, "lineage_confidence": "ARTIFACT_ONLY",
        "frozen_before_outcome_join": True,
        "reason": "no affirmative per-run binding to exact model artifacts or unchanged semantic eras",
        "permitted_claim": "The archived deployed prediction population behaved as follows.",
    }
    (out / "safe_path_decision.json").write_text(json.dumps(safe_path, indent=2) + "\n")

    rec = pd.read_csv(args.reconcile_csv, low_memory=False)
    rec["game_date"] = rec.game_date.astype(str)
    rec = rec[rec.prop_type.astype(str).str.lower().isin(PROPS)].copy()
    rec["prop_type"] = rec.prop_type.astype(str).str.lower()
    rec["selected_run_tag"] = rec.game_date.map(lambda d: chosen_by_date[d]["run_tag"])
    enrich = pd.concat(enrich_frames, ignore_index=True)
    keys = ["game_date", "game_id", "player_id", "prop_type", "line"]
    for c in ["game_id", "player_id", "line"]:
        rec[c] = pd.to_numeric(rec[c], errors="coerce"); enrich[c] = pd.to_numeric(enrich[c], errors="coerce")
    enrich = enrich.drop_duplicates(keys)
    rec = rec.merge(enrich, on=keys, how="left", validate="many_to_one")

    # Outcomes are joined only after Path D is frozen above. July is never read.
    outcome_frames = []
    for p in sorted(Path("artifacts/analysis/mlb/execution_vs_model").glob("2026-0[56]-*/reconcile_rows.csv")):
        d = pd.read_csv(p, usecols=lambda c: c in {"game_date", "game_id", "player_id", "prop_type", "line", "actual_value", "team", "opponent"}, low_memory=False)
        outcome_frames.append(d)
    outcomes = pd.concat(outcome_frames, ignore_index=True)
    outcomes["game_date"] = outcomes.game_date.astype(str); outcomes["prop_type"] = outcomes.prop_type.astype(str).str.lower()
    for c in ["game_id", "player_id", "line", "actual_value"]: outcomes[c] = pd.to_numeric(outcomes[c], errors="coerce")
    outcomes = outcomes.sort_values(keys).drop_duplicates(keys)
    rec = rec.merge(outcomes, on=keys, how="left", suffixes=("", "_outcome"), validate="many_to_one")
    if "actual_value_outcome" in rec.columns:
        rec["actual_value"] = pd.to_numeric(rec["actual_value"], errors="coerce").where(
            pd.to_numeric(rec["actual_value"], errors="coerce").notna(),
            pd.to_numeric(rec["actual_value_outcome"], errors="coerce"),
        )

    rec["snapshot_time"] = pd.to_datetime(rec.game_date.map(lambda d: chosen_by_date[d]["snapshot_time_utc"]), utc=True)
    game_time_col = "game_time_y" if "game_time_y" in rec.columns else "game_time"
    rec["scheduled_start"] = pd.to_datetime(rec[game_time_col], errors="coerce", utc=True)
    rec["pregame_valid"] = rec.scheduled_start.notna() & rec.snapshot_time.lt(rec.scheduled_start)
    rec["identity_valid"] = rec[["game_id", "player_id", "prop_type", "line"]].notna().all(axis=1)
    for c in ["price_over_american", "price_under_american", "model_prob_over", "model_prob_under"]: rec[c] = pd.to_numeric(rec[c], errors="coerce")
    rec["two_sided_valid"] = rec.price_over_american.notna() & rec.price_under_american.notna() & rec.price_over_american.ne(0) & rec.price_under_american.ne(0)
    rec["selected_side"] = rec.model_pick_side.astype(str).str.lower()
    rec["selected_side_price"] = np.where(rec.selected_side.eq("over"), rec.price_over_american, rec.price_under_american)
    rec["selected_side_no_vig_implied"] = np.where(rec.selected_side.eq("over"), rec.implied_over_novig, rec.implied_under_novig)
    rec["model_probability"] = np.where(rec.selected_side.eq("over"), rec.model_prob_over, rec.model_prob_under)
    rec["probability_valid"] = pd.Series(rec.model_probability).between(0, 1, inclusive="neither") & pd.Series(rec.selected_side_no_vig_implied).between(0, 1, inclusive="neither")
    rec["favorite"] = pd.to_numeric(rec.selected_side_no_vig_implied, errors="coerce").ge(.5)
    control = rec[rec.identity_valid & rec.pregame_valid & rec.two_sided_valid & rec.probability_valid & rec.favorite].copy()
    actual = pd.to_numeric(control.actual_value, errors="coerce")
    control["selected_outcome"] = np.where(actual.isna(), "unresolved", np.where(actual.eq(control.line), "push", np.where((actual.gt(control.line) & control.selected_side.eq("over")) | (actual.lt(control.line) & control.selected_side.eq("under")), "win", "loss")))
    control["pnl_1u"] = [pnl(float(px), str(res)) for px, res in zip(control.selected_side_price, control.selected_outcome)]
    control["break_even_probability"] = american_implied(control.selected_side_price)
    control["month"] = control.game_date.str[:7]
    control["model_probability_band"] = control.model_probability.map(prob_band)
    control["market_probability_band"] = control.selected_side_no_vig_implied.map(prob_band)
    control["american_price_band"] = control.selected_side_price.map(price_band)
    control["calibration_method"] = control.get("calibration_method", "").fillna("none_recorded").replace("", "none_recorded")
    control["operational_era"] = control.schema_fingerprint.str[:12] + ":" + control.configuration_fingerprint.str[:12]
    control["characterization_label"] = LABEL
    control.to_csv(out / "artifact_defined_favorite_control_population.csv", index=False)

    dims = [
        ["month"], ["prop_type"], ["selected_side"], ["line"], ["bookmaker_key"],
        ["model_probability_band"], ["market_probability_band"], ["american_price_band"],
        ["calibration_method"], ["schema_fingerprint"], ["operational_era"],
        ["month", "prop_type", "selected_side"], ["prop_type", "selected_side", "line"],
    ]
    reports = []
    for dimension in dims:
        part = grouped_report(control, dimension)
        part.insert(1, "report_dimension", "|".join(dimension))
        reports.append(part)
    pd.concat(reports, ignore_index=True).to_csv(out / "artifact_defined_characterization.csv", index=False)
    grouped_report(control, ["month", "model_probability_band"]).to_csv(out / "probability_calibration.csv", index=False)
    grouped_report(control, ["month", "prop_type", "selected_side", "american_price_band"]).to_csv(out / "price_burden.csv", index=False)
    grouped_report(control, ["month", "schema_fingerprint", "calibration_method"]).to_csv(out / "schema_calibration_operational_eras.csv", index=False)

    admission = pd.DataFrame([
        {"stage": "scoped_reconstructed_rows", "rows": len(rec)},
        {"stage": "missing_or_nonpregame_scheduled_start", "rows": int((~rec.pregame_valid).sum())},
        {"stage": "invalid_identity", "rows": int((~rec.identity_valid).sum())},
        {"stage": "invalid_two_sided_pair", "rows": int((~rec.two_sided_valid).sum())},
        {"stage": "invalid_probability", "rows": int((~rec.probability_valid).sum())},
        {"stage": "selected_side_not_market_favorite", "rows": int((~rec.favorite).sum())},
        {"stage": "final_artifact_defined_favorite_control", "rows": len(control)},
    ])
    admission.to_csv(out / "population_admission_summary.csv", index=False)

    statuses = {
        "decision": DECISION, "characterization_label": LABEL,
        "historical_prediction_population_recoverability": "RECOVERED",
        "semantic_model_lineage_recoverability": "ARTIFACT_ONLY_NOT_SEMANTICALLY_RECOVERED",
        "current_pipeline_transferability": "UNRESOLVED",
        "residual_phase_readiness": "NOT_READY",
        "production_readiness": "NOT_AUTHORIZED",
        "favorite_control_rows": int(len(control)), "outcomes_joined_after_path_freeze": True,
        "july_outcomes_inspected": False,
    }
    (out / "decision.json").write_text(json.dumps(statuses, indent=2) + "\n")
    overall = summarize(control)
    (out / "executive_decision.md").write_text(f"""# Artifact-defined MLB favorite characterization

Decision: `{DECISION}`  
Label: `{LABEL}`

The 61 selected ordinary-run prediction artifacts are recovered, but semantic model lineage is not.
All selected runs are therefore `ARTIFACT_ONLY`; no exact model or defensible unchanged semantic era
spanning May and June is certified. After freezing Path D and then joining May-June results, the strict
pregame, exact-identity, two-sided, selected-side market-favorite control contains {len(control):,} rows.

Overall descriptive metrics: wins={overall['wins']}, losses={overall['losses']}, pushes={overall['pushes']},
unresolved={overall['dnp_void_unresolved']}, binary win rate={overall['binary_win_rate']:.4f},
one-unit ROI={overall['roi_1u']:.4f}, Brier={overall['brier_score']:.4f}, log loss={overall['log_loss']:.4f}.

These results describe archived deployed predictions only. They do not identify a named model, establish
transferability to the current pipeline, validate reusable rejection conditions, authorize a residual
phase, or support any production, selector, wager, model, or promotion action.
""", encoding="utf-8")

    files = []
    for p in sorted(x for x in out.iterdir() if x.name != "SHA256SUMS.csv"):
        files.append({"file": p.name, "sha256": sha(p), "bytes": p.stat().st_size})
    pd.DataFrame(files).to_csv(out / "SHA256SUMS.csv", index=False)
    print(json.dumps(statuses, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
