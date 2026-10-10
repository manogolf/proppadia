"""Prospective, artifact-only NHL SOG fixed-blend research shadows."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, verify_package
from backend.nhl.sog_feature_input import sha256_file
from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail

CONTRACT = "NHL_SOG_D10_D20_FIXED_BLEND_SHADOW_V1"
MODELS = (
    {"identity": "NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1", "label": "D10_25_D20_75",
     "priority": "PRIMARY_FIXED_BLEND_SHADOW", "d10_weight": 0.25, "d20_weight": 0.75},
    {"identity": "NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1", "label": "D10_50_D20_50",
     "priority": "FIXED_BLEND_COMPARATOR_SHADOW", "d10_weight": 0.50, "d20_weight": 0.50},
)
LINES = (1.5, 2.5, 3.5)


def _manifest(path: Path) -> str:
    sums = path / "SHA256SUMS"
    sums.write_text("".join(
        f"{sha256_file(p)}  {p.name}\n" for p in sorted(path.iterdir())
        if p.is_file() and p.name != "SHA256SUMS"))
    return sha256_file(sums)


def build_predictions(features: pd.DataFrame, production: pd.DataFrame, *, run_id: str,
                      slate_date: str, season: int, feature_sha256: str,
                      cutoff_utc: str, canonical_game_ids: list[int]) -> dict[str, pd.DataFrame]:
    """Score fixed blends using the production scored set and its exact selected TOI."""
    keys = ["game_id", "player_id"]
    if features.duplicated(keys).any() or production.duplicated(keys).any():
        raise ValueError("FIXED_BLEND_DUPLICATE_PLAYER_GAME")
    if not {"d10_sog_per60", "d20_sog_per60"}.issubset(features.columns):
        raise ValueError("FIXED_BLEND_RATE_FIELDS_MISSING")
    production_cols = keys + [c for c in ("expected_sog", "selected_toi_minutes") if c in production]
    merged = features.merge(production[production_cols], on=keys, how="inner", validate="one_to_one")
    if "selected_toi_minutes" not in merged:
        def numeric(name: str) -> pd.Series:
            return pd.to_numeric(merged[name], errors="coerce") if name in merged else pd.Series(float("nan"), index=merged.index)
        toi_candidates = [numeric("d10_toi_min_avg"), numeric("d20_toi_min_avg"), numeric("d5_toi_min_avg"),
                          numeric("szn_toi_per_game_5on5") + numeric("szn_toi_per_game_pp"),
                          numeric("season_5on5_icetime_per_game") / 60.0 + numeric("season_5on4_icetime_per_game") / 60.0]
        selected = toi_candidates[0]
        for candidate in toi_candidates[1:]:
            selected = selected.where(selected.notna(), candidate)
        merged["selected_toi_minutes"] = selected
    toi_source_fields = ("d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg", "szn_toi_per_game_5on5",
                         "season_5on5_icetime_per_game")
    source = pd.Series("", index=merged.index, dtype=object)
    source_for_field = {"szn_toi_per_game_5on5": "szn_toi_per_game_5on5+szn_toi_per_game_pp",
                        "season_5on5_icetime_per_game": "season_5on5_icetime_per_game/60+season_5on4_icetime_per_game/60"}
    for field in toi_source_fields:
        if field not in merged:
            continue
        present = pd.to_numeric(merged[field], errors="coerce").notna() & source.eq("")
        source.loc[present] = source_for_field.get(field, field)
    merged["selected_toi_source"] = source
    # Do not infer a different eligible population. Each fixed blend requires both rates.
    merged = merged[pd.to_numeric(merged.d10_sog_per60, errors="coerce").notna()
                    & pd.to_numeric(merged.d20_sog_per60, errors="coerce").notna()].copy()
    outputs: dict[str, pd.DataFrame] = {}
    for model in MODELS:
        rate = model["d10_weight"] * pd.to_numeric(merged.d10_sog_per60) + model["d20_weight"] * pd.to_numeric(merged.d20_sog_per60)
        toi = pd.to_numeric(merged.selected_toi_minutes, errors="coerce")
        valid = rate.notna() & toi.notna() & rate.ge(0) & toi.ge(0)
        basecols = [c for c in ("game_id", "player_id", "team_id", "opponent_id", "is_home", "game_date", "season") if c in merged]
        base = merged.loc[valid, basecols].copy()
        base["model_identity"] = model["identity"]
        base["research_label"] = model["label"]
        base["research_priority"] = model["priority"]
        base["d10_weight"] = model["d10_weight"]
        base["d20_weight"] = model["d20_weight"]
        base["d10_sog_per60"] = pd.to_numeric(merged.loc[valid, "d10_sog_per60"])
        base["d20_sog_per60"] = pd.to_numeric(merged.loc[valid, "d20_sog_per60"])
        base["d10_prior_game_count"] = pd.to_numeric(merged.loc[valid, "d10_prior_game_count"], errors="coerce") if "d10_prior_game_count" in merged else pd.NA
        base["d20_prior_game_count"] = pd.to_numeric(merged.loc[valid, "d20_prior_game_count"], errors="coerce") if "d20_prior_game_count" in merged else pd.NA
        base["selected_toi_minutes"] = toi.loc[valid]
        base["selected_toi_source"] = merged.loc[valid, "selected_toi_source"]
        base["blended_rate_per60"] = rate.loc[valid]
        base["shadow_lambda"] = rate.loc[valid] * toi.loc[valid] / 60.0
        if "expected_sog" in merged:
            baseline_lam = pd.to_numeric(merged.loc[valid, "expected_sog"], errors="coerce")
            production_rate = pd.to_numeric(merged.loc[valid, "d10_sog_per60"], errors="coerce")
            production_rate = production_rate.where(production_rate.notna(), pd.to_numeric(merged.loc[valid, "d20_sog_per60"], errors="coerce"))
            if "d5_sog_per60" in merged:
                production_rate = production_rate.where(production_rate.notna(), pd.to_numeric(merged.loc[valid, "d5_sog_per60"], errors="coerce"))
            production_lambda = production_rate * toi.loc[valid] / 60.0
            comparable = baseline_lam.notna() & production_lambda.notna()
            if not (baseline_lam[comparable] - production_lambda[comparable]).abs().le(1e-12).all():
                raise ValueError("FIXED_BLEND_PRODUCTION_TOI_REUSE_PARITY_FAILED")
            for line, threshold, pcol in ((1.5, 2, "p_over_1_5"), (2.5, 3, "p_over_2_5"), (3.5, 4, "p_over_3_5")):
                if pcol in merged:
                    expected = baseline_lam.map(lambda value: _poisson_tail(float(value), threshold))
                    observed = pd.to_numeric(merged.loc[valid, pcol], errors="coerce")
                    if not (expected[observed.notna()].sub(observed[observed.notna()]).abs().le(1e-12).all()):
                        raise ValueError(f"FIXED_BLEND_PRODUCTION_PROBABILITY_PARITY_FAILED:{line}")
        for line, threshold in zip(LINES, (2, 3, 4)):
            part = base.copy()
            part["line"] = line
            part["p_over"] = part.shadow_lambda.map(lambda value: _poisson_tail(float(value), threshold))
            part["p_under"] = 1.0 - part.p_over
            part["selected_side"] = part.p_over.ge(0.5).map({True: "OVER", False: "UNDER"})
            outputs.setdefault(model["identity"], []).append(part)
        outputs[model["identity"]] = pd.concat(outputs[model["identity"]], ignore_index=True)
        frame = outputs[model["identity"]]
        if frame.duplicated(keys + ["line", "model_identity"]).any():
            raise ValueError("FIXED_BLEND_DUPLICATE_PREDICTION_IDENTITY")
        if not set(pd.to_numeric(frame.game_id).astype(int)).issubset(set(map(int, canonical_game_ids))):
            raise ValueError("FIXED_BLEND_NONCANONICAL_GAME")
    return outputs


def capture(*, feature_path: Path, production_path: Path, output_root: Path, run_id: str,
            slate_date: str, season: int, feature_sha256: str, cutoff_utc: str,
            canonical_game_ids: list[int], scorer_path: Path) -> list[dict[str, Any]]:
    """Build two create-only packages; exceptions are intended for nonblocking lane handling."""
    if sha256_file(feature_path) != feature_sha256:
        raise ValueError("FIXED_BLEND_FEATURE_INPUT_HASH_MISMATCH")
    features, production = pd.read_csv(feature_path), pd.read_csv(production_path)
    predictions = build_predictions(features, production, run_id=run_id, slate_date=slate_date,
                                    season=season, feature_sha256=feature_sha256,
                                    cutoff_utc=cutoff_utc, canonical_game_ids=canonical_game_ids)
    replay = build_predictions(features, production, run_id=run_id, slate_date=slate_date,
                               season=season, feature_sha256=feature_sha256,
                               cutoff_utc=cutoff_utc, canonical_game_ids=canonical_game_ids)
    for model in MODELS:
        if not predictions[model["identity"]].equals(replay[model["identity"]]):
            raise ValueError("FIXED_BLEND_DETERMINISTIC_REPLAY_FAILED")
        ordered = predictions[model["identity"]].pivot(index=["game_id", "player_id"], columns="line", values="p_over")
        if not ((ordered[1.5] >= ordered[2.5]) & (ordered[2.5] >= ordered[3.5])).all():
            raise ValueError("FIXED_BLEND_POISSON_THRESHOLD_COHERENCE_FAILED")
    cutoff = pd.Timestamp(cutoff_utc)
    if cutoff.tzinfo is None:
        raise ValueError("FIXED_BLEND_PREGAME_CUTOFF_FAILED")
    result = []
    control_parts = []
    control_source = features.merge(production[["game_id", "player_id", "expected_sog"]],
                                    on=["game_id", "player_id"], how="inner", validate="one_to_one")
    for line, threshold in zip(LINES, (2, 3, 4)):
        control = control_source[["game_id", "player_id", "expected_sog", "d10_sog_per60"]].copy()
        control["line"] = line
        control["model_identity"] = "NHL_SOG_PRODUCTION_D10_CONTROL_V1"
        control["shadow_lambda"] = pd.to_numeric(control.expected_sog, errors="coerce")
        control["p_over"] = control.shadow_lambda.map(lambda value: _poisson_tail(float(value), threshold))
        control["p_under"] = 1.0 - control.p_over
        control["selected_side"] = control.p_over.ge(0.5).map({True: "OVER", False: "UNDER"})
        control_parts.append(control)
    control_frame = pd.concat(control_parts, ignore_index=True)
    for model in MODELS:
        parent = Path(output_root) / f"season={season}" / f"slate_date={slate_date}" / f"run_id={run_id}"
        final = parent / f"model={model['identity']}"
        staging = parent / f".{model['identity']}.incomplete"
        parent.mkdir(parents=True, exist_ok=True)
        if final.exists():
            raise FileExistsError(f"FIXED_BLEND_PACKAGE_ALREADY_EXISTS:{final}")
        staging.mkdir(exist_ok=False)
        try:
            pred_path = staging / "predictions.csv"
            predictions[model["identity"]].to_csv(pred_path, index=False, float_format="%.17g")
            replay_bytes = replay[model["identity"]].to_csv(index=False, float_format="%.17g").encode()
            replay_sha = hashlib.sha256(replay_bytes).hexdigest()
            if replay_sha != sha256_file(pred_path):
                raise ValueError("FIXED_BLEND_REPLAY_PREDICTION_HASH_MISMATCH")
            control_path = staging / "production_control.csv"
            control_frame.to_csv(control_path, index=False, float_format="%.17g")
            metadata = {
                "contract": CONTRACT, "season": season, "slate_date": slate_date,
                "parent_daily_run_id": run_id, "canonical_game_ids": sorted(set(canonical_game_ids)),
                "canonical_game_set_hash": canonical_game_set_hash(canonical_game_ids),
                "model_identity": model["identity"], "research_label": model["label"],
                "research_priority": model["priority"], "weights": {"d10": model["d10_weight"], "d20": model["d20_weight"]},
                "history_contract": "LATEST_AVAILABLE_10_OR_20_STRICT_PRIOR_PLAYER_GAMES_SUM_SOG_DIV_SUM_TOI_X60",
                "feature_input_path": str(Path(feature_path).resolve()), "feature_input_sha256": feature_sha256,
                "selected_toi_source": "production scorer selected TOI; same player-game inner join",
                "scorer_path": str(Path(scorer_path).resolve()), "scorer_sha256": sha256_file(scorer_path),
                "prediction_sha256": sha256_file(pred_path), "feature_cutoff_utc": cutoff.isoformat(),
                "pregame_cutoff_status": "PASS_FROM_PRODUCTION_FEATURE_INPUT_CONTRACT",
                "deterministic_replay": "PASS",
                "replay_prediction_sha256": replay_sha,
                "threshold_coherence_crossing_count": 0,
                "production_control_sha256": sha256_file(control_path),
                "row_count": len(predictions[model["identity"]]),
                "player_game_count": predictions[model["identity"]][["game_id", "player_id"]].drop_duplicates().shape[0],
                "line_count": len(LINES), "status": "COMPLETE",
            }
            (staging / "manifest.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
            (staging / "RUN_COMPLETE.json").write_text(json.dumps({"parent_daily_run_id": run_id, "status": "COMPLETE"}) + "\n")
            manifest_sha = _manifest(staging)
            staging.rename(final)
            verify_package(final)
            result.append({**metadata, "path": str(final.resolve()), "manifest_sha256": manifest_sha})
        except Exception:
            # Never rewrite a published package. Incomplete staging remains diagnosable.
            raise
    return result


def grade(predictions: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    """Grade immutable line-grain rows against exact official player-game outcomes."""
    keys = ["game_id", "player_id"]
    if outcomes.duplicated(keys).any():
        raise ValueError("FIXED_BLEND_OUTCOME_DUPLICATE_IDENTITY")
    value_col = "shots_on_goal" if "shots_on_goal" in outcomes else "official_sog"
    if value_col not in outcomes:
        raise ValueError("FIXED_BLEND_OFFICIAL_SOG_OUTCOME_MISSING")
    joined = predictions.merge(outcomes[keys + [value_col]].rename(columns={value_col: "shots_on_goal"}), on=keys, how="left", validate="many_to_one")
    y = pd.to_numeric(joined.shots_on_goal, errors="coerce")
    line = pd.to_numeric(joined.line, errors="coerce")
    eligible = y.notna() & y.ge(0)
    joined["outcome_status"] = "UNRESOLVED_UNGRADED"
    joined.loc[eligible, "outcome_status"] = "SETTLED"
    joined["realized_over"] = (y > line).where(eligible)
    joined["correct_side"] = (((joined.selected_side == "OVER") & (y > line)) | ((joined.selected_side == "UNDER") & (y < line))).where(eligible)
    joined["brier"] = ((joined.p_over - joined.realized_over.astype(float)) ** 2).where(eligible)
    eps = 1e-15
    p = joined.p_over.clip(eps, 1 - eps)
    joined["log_loss"] = (-(joined.realized_over * p.map(math.log) + (1 - joined.realized_over) * (1 - p).map(math.log))).where(eligible)
    return joined


def performance_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    settled = frame[frame.brier.notna()].copy()
    result: dict[str, Any] = {"n": int(len(settled)), "unsettled_n": int(len(frame) - len(settled))}
    if settled.empty:
        return result
    y_count = pd.to_numeric(settled.shots_on_goal)
    lam = pd.to_numeric(settled.shadow_lambda)
    residual = lam - y_count
    result.update({"mean_lambda": float(lam.mean()), "realized_mean_sog": float(y_count.mean()),
                   "mean_residual": float(residual.mean()), "count_mae": float(residual.abs().mean()),
                   "count_rmse": float((residual.pow(2).mean()) ** 0.5)})
    line_metrics = {}
    for line in LINES:
        part = settled[settled.line.eq(line)]
        if part.empty:
            line_metrics[str(line)] = {"settled_n": 0}
            continue
        over = (part.shots_on_goal > line).astype(int)
        line_entry: dict[str, Any] = {
            "settled_n": len(part), "wins": int(part.correct_side.eq(True).sum()),
            "losses": int(part.correct_side.eq(False).sum()), "accuracy": float(part.correct_side.mean()),
            "brier": float(part.brier.mean()), "log_loss": float(part.log_loss.mean()),
            "predicted_over_frequency": float(part.p_over.mean()),
            "realized_over_frequency": float(over.mean()),
        }
        if over.nunique() == 2:
            try:
                from sklearn.metrics import average_precision_score, roc_auc_score
                line_entry["auc"] = float(roc_auc_score(over, part.p_over))
                line_entry["average_precision"] = float(average_precision_score(over, part.p_over))
            except Exception:
                line_entry["auc"] = line_entry["average_precision"] = None
        else:
            line_entry["auc"] = line_entry["average_precision"] = None
        line_metrics[str(line)] = line_entry
    result["by_line"] = line_metrics
    return result


def grade_slate(*, slate_date: str, reconciliation_package: Path,
                daily_run_root: Path, output_root: Path) -> dict[str, Any]:
    """Grade only retained same-slate captures; create immutable grade packages."""
    outcomes = pd.read_csv(Path(reconciliation_package) / "canonical_skater_outcomes.csv")
    outcomes = outcomes[outcomes.slate_date.astype(str).eq(slate_date)].copy()
    if "participation_state" in outcomes:
        outcomes = outcomes[outcomes.participation_state.eq("PARTICIPATED")]
    if "official_final" in outcomes:
        outcomes = outcomes[outcomes.official_final.astype(str).str.lower().eq("true")]
    outcome_path = Path(reconciliation_package) / "canonical_skater_outcomes.csv"
    grades = []
    for package in sorted(Path(daily_run_root).glob(f"season=*/slate_date={slate_date}/run_id=*/model=*")):
        for model in MODELS:
            if package.name != f"model={model['identity']}":
                continue
            if not package.is_dir():
                continue
            verify_package(package)
            manifest = json.loads((package / "manifest.json").read_text())
            pred_path = package / "predictions.csv"
            pred = pd.read_csv(pred_path)
            graded = grade(pred, outcomes)
            control = grade(pd.read_csv(package / "production_control.csv"), outcomes)
            paired = graded.merge(control[["game_id", "player_id", "line", "shots_on_goal", "p_over", "shadow_lambda", "correct_side"]],
                                  on=["game_id", "player_id", "line", "shots_on_goal"], suffixes=("", "_production"),
                                  validate="one_to_one")
            production_over_ids = set(control.loc[control.line.eq(1.5) & control.selected_side.eq("OVER"), "player_id"])
            player_d10 = control[["player_id", "d10_sog_per60"]].drop_duplicates("player_id")
            player_d10["d10_sog_per60"] = pd.to_numeric(player_d10.d10_sog_per60, errors="coerce")
            q80, q90 = player_d10.d10_sog_per60.quantile([0.8, 0.9])
            top20_ids = set(player_d10.loc[player_d10.d10_sog_per60.ge(q80), "player_id"])
            top10_ids = set(player_d10.loc[player_d10.d10_sog_per60.ge(q90), "player_id"])
            groups = {
                "production_1_5_over": graded[graded.player_id.isin(production_over_ids)],
                "production_d10_top_quintile": graded[graded.player_id.isin(top20_ids)],
                "production_d10_top_decile": graded[graded.player_id.isin(top10_ids)],
                "d10_gt_d20": graded[pd.to_numeric(graded.d10_sog_per60, errors="coerce") > pd.to_numeric(graded.d20_sog_per60, errors="coerce")],
                "d10_lt_d20": graded[pd.to_numeric(graded.d10_sog_per60, errors="coerce") < pd.to_numeric(graded.d20_sog_per60, errors="coerce")],
            }
            paired_by_line = {}
            for line in LINES:
                part = paired[paired.line.eq(line)]
                over = (part.shots_on_goal > line).astype(float)
                prod_p = pd.to_numeric(part.p_over_production, errors="coerce").clip(1e-15, 1 - 1e-15)
                cand_p = pd.to_numeric(part.p_over, errors="coerce").clip(1e-15, 1 - 1e-15)
                prod_ll = -(over * prod_p.map(math.log) + (1 - over) * (1 - prod_p).map(math.log))
                cand_ll = -(over * cand_p.map(math.log) + (1 - over) * (1 - cand_p).map(math.log))
                prod_brier = (prod_p - over).pow(2)
                cand_brier = (cand_p - over).pow(2)
                paired_by_line[str(line)] = {
                    "paired_n": len(part),
                    "brier_difference_vs_d10": float(cand_brier.mean() - prod_brier.mean()) if len(part) else None,
                    "log_loss_difference_vs_d10": float(cand_ll.mean() - prod_ll.mean()) if len(part) else None,
                    "side_disagreements": int(part.selected_side.ne(part.selected_side_production).sum()),
                    "wins_gained": int((part.correct_side.astype(bool) & ~part.correct_side_production.astype(bool)).sum()),
                    "wins_lost": int((~part.correct_side.astype(bool) & part.correct_side_production.astype(bool)).sum()),
                }
            out_root = Path(output_root) / f"slate_date={slate_date}" / f"run_id={manifest['parent_daily_run_id']}" / f"model={model['identity']}"
            if out_root.exists():
                verify_package(out_root)
                prior = json.loads((out_root / "grade_summary.json").read_text())
                if prior.get("prediction_sha256") != sha256_file(pred_path) or prior.get("outcome_sha256") != sha256_file(outcome_path):
                    raise ValueError("FIXED_BLEND_EXISTING_GRADE_LINEAGE_MISMATCH")
                grades.append({**prior, "grade_path": str(out_root.resolve()), "reuse_mode": "REUSED_VALID_IMMUTABLE_GRADE"})
                continue
            out_root.parent.mkdir(parents=True, exist_ok=True)
            stage = out_root.with_name(f".{out_root.name}.incomplete")
            stage.mkdir(exist_ok=False)
            graded.to_csv(stage / "graded_predictions.csv", index=False)
            stats = {"model_identity": model["identity"], **performance_metrics(graded),
                     "settled_player_games": int(graded.loc[graded.brier.notna(), ["game_id", "player_id"]].drop_duplicates().shape[0]),
                     "unsettled_player_games": int(graded.loc[graded.brier.isna(), ["game_id", "player_id"]].drop_duplicates().shape[0]),
                     "paired_n": int(paired.brier.notna().sum()),
                     "paired_count_mae_difference_vs_d10": float((paired.shadow_lambda - paired.shots_on_goal).abs().mean() - (paired.shadow_lambda_production - paired.shots_on_goal).abs().mean()) if len(paired) else None,
                     "paired_brier_difference_1_5_vs_d10": float((paired.loc[paired.line.eq(1.5), "brier"].mean() - ((paired.loc[paired.line.eq(1.5), "p_over_production"] - (paired.loc[paired.line.eq(1.5), "shots_on_goal"] > 1.5).astype(float)) ** 2).mean())) if paired.line.eq(1.5).any() else None,
                "paired_side_disagreements": int(paired.selected_side.ne(paired.selected_side_production).sum()),
                "paired_wins_gained": int((paired.correct_side.astype(bool) & ~paired.correct_side_production.astype(bool)).sum()),
                "paired_wins_lost": int((~paired.correct_side.astype(bool) & paired.correct_side_production.astype(bool)).sum())}
            (stage / "grade_summary.json").write_text(json.dumps({**stats,
                "paired_metrics_by_line": paired_by_line,
                "prospective_groups": {name: performance_metrics(group) for name, group in groups.items()},
                "slate_date": slate_date,
                "run_id": manifest["parent_daily_run_id"], "prediction_sha256": sha256_file(pred_path),
                "outcome_sha256": sha256_file(outcome_path), "paired_metrics": stats}, indent=2, sort_keys=True) + "\n")
            (stage / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}) + "\n")
            _manifest(stage)
            stage.rename(out_root)
            grades.append({**stats, "grade_path": str(out_root.resolve())})
    # Regenerate compact cumulative view strictly from immutable grade summaries.
    tracker = Path(output_root).parent / "sog_fixed_blend_prospective"
    tracker.mkdir(parents=True, exist_ok=True)
    summaries = []
    for summary_path in Path(output_root).glob("slate_date=*/run_id=*/model=*/grade_summary.json"):
        verify_package(summary_path.parent)
        summaries.append(json.loads(summary_path.read_text()))
    captures = []
    for manifest_path in Path(daily_run_root).glob("season=*/slate_date=*/run_id=*/model=*/manifest.json"):
        captures.append(json.loads(manifest_path.read_text()))
    capture_slates = sorted({str(item["slate_date"]) for item in captures})
    capture_counts = {model["identity"]: {
        "slates_captured": len({item["slate_date"] for item in captures if item.get("model_identity") == model["identity"]}),
        "player_games_captured": sum(int(item.get("player_game_count", 0)) for item in captures if item.get("model_identity") == model["identity"]),
        "settled_player_games": sum(int(item.get("settled_player_games", 0)) for item in summaries if item.get("model_identity") == model["identity"]),
        "unsettled_player_games": sum(int(item.get("unsettled_player_games", 0)) for item in summaries if item.get("model_identity") == model["identity"]),
        "exact_common_rows_with_production": sum(int(item.get("paired_n", 0)) for item in summaries if item.get("model_identity") == model["identity"]),
    } for model in MODELS}
    capture_counts["exact_common_rows_across_all_three"] = min(
        (capture_counts[model["identity"]]["exact_common_rows_with_production"] for model in MODELS), default=0)
    summary = {"contract": CONTRACT, "first_prospective_capture_date": min(capture_slates) if capture_slates else None,
               "capture_counts": capture_counts,
               "grades": sorted(summaries, key=lambda x: (x["slate_date"], x["model_identity"])),
               "status": "PROSPECTIVE_IMMUTABLE_GRADES_ONLY"}
    tmp = tracker / ".sog_fixed_blend_prospective_summary.json.tmp"
    tmp.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, tracker / "sog_fixed_blend_prospective_summary.json")
    md = "# NHL SOG fixed blend prospective summary\n\nProduction remains d10. Retrospective evidence motivated these research shadows; prospective immutable evidence is required for any authority decision.\n\n"
    md += f"Captured model-slate grades: {len(summaries)}\n\n| Model identity | Settled | Unsettled | Paired rows | Count MAE Δ vs d10 | 1.5 Brier Δ | Side disagreements |\n|---|---:|---:|---:|---:|---:|---:|\n"
    for item in sorted(summaries, key=lambda x: (x["slate_date"], x["model_identity"])):
        md += (f"| {item['model_identity']} | {item.get('n', 0)} | {item.get('unsettled_n', 0)} | "
               f"{item.get('paired_n', 0)} | {item.get('paired_count_mae_difference_vs_d10')} | "
               f"{item.get('paired_brier_difference_1_5_vs_d10')} | {item.get('paired_side_disagreements', 0)} |\n")
    (tracker / "sog_fixed_blend_prospective_summary.md").write_text(md)
    return {"status": "COMPLETE" if grades else "NO_IMMUTABLE_SHADOW_CAPTURE", "grades": grades,
            "summary_json": str((tracker / "sog_fixed_blend_prospective_summary.json").resolve()),
            "summary_md": str((tracker / "sog_fixed_blend_prospective_summary.md").resolve())}

