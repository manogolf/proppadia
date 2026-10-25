"""Immutable SOG shadow: the Poisson formula after the HGB cutover (2026-10-25).

When `NHL_SOG_SCORER=hgb_count`, the formula (`score_sog_poisson_baseline.py`,
`poisson_baseline/baseline_v1`) still scores every slate first; its wide file
is the population and fallback source for the HGB production file. This module
captures that formula output as a shadow paired with production, in the same
package layout as the other count-model SOG shadows, and grades it with their
shared grader. The formula's own published probabilities are kept as-is.
"""
from __future__ import annotations

import json
from functools import partial
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, verify_package
from backend.nhl.sog_feature_input import sha256_file
from backend.nhl.sog_shrunk_rate_shadow import _write_sums
from backend.nhl.sog_shrunk_rate_shadow import grade_slate as _grade_count_shadow

MODEL_IDENTITY = "NHL_SOG_POISSON_FORMULA_V1"
PRIMARY_SOURCE = "FORMULA"
LINES = ((1.5, "p_over_1_5"), (2.5, "p_over_2_5"), (3.5, "p_over_3_5"))

grade_slate = partial(_grade_count_shadow, model_identity=MODEL_IDENTITY, primary_source=PRIMARY_SOURCE)


def build_predictions(formula: pd.DataFrame, production: pd.DataFrame, *, slate_date: str, season: int,
                      run_id: str, feature_cutoff_utc: str, canonical_game_ids: list[int],
                      production_identity: tuple[str, str]) -> pd.DataFrame:
    keys = ["game_id", "player_id"]
    for name, frame in (("FORMULA", formula), ("PRODUCTION", production)):
        if {"game_id", "player_id", "expected_sog", *(c for _, c in LINES)} - set(frame.columns):
            raise ValueError(f"SOG_FORMULA_SHADOW_{name}_SCHEMA_INVALID")
        if frame.duplicated(keys).any():
            raise ValueError(f"SOG_FORMULA_SHADOW_{name}_DUPLICATE_PLAYER_GAME")
    f = formula.copy(); p = production.copy()
    for frame in (f, p):
        frame["game_id"] = pd.to_numeric(frame.game_id).astype("int64")
        frame["player_id"] = pd.to_numeric(frame.player_id).astype("int64")
    if not set(f.game_id).issubset(set(map(int, canonical_game_ids))):
        raise ValueError("SOG_FORMULA_SHADOW_NONCANONICAL_GAME")
    joined = f.merge(p[keys + ["expected_sog"] + [c for _, c in LINES]], on=keys, how="outer",
                     suffixes=("_formula", "_production"), indicator=True, validate="one_to_one")
    if not joined["_merge"].eq("both").all():
        raise ValueError("SOG_FORMULA_SHADOW_POPULATION_MISMATCH")
    out = joined[[c for c in ("game_id", "player_id", "team_id", "opponent_id", "is_home", "game_date") if c in joined]].copy()
    out.insert(0, "slate_date", slate_date)
    out["season"] = season
    out["parent_daily_run_id"] = run_id
    out["feature_cutoff_utc"] = feature_cutoff_utc
    out["production_model_family"], out["production_model_version"] = production_identity
    out["formula_poisson_source"] = joined.get("poisson_source")
    out["shadow_source"] = PRIMARY_SOURCE
    out["production_lambda"] = pd.to_numeric(joined.expected_sog_production)
    out["shadow_lambda"] = pd.to_numeric(joined.expected_sog_formula)
    out["lambda_delta"] = out.shadow_lambda - out.production_lambda
    parts = []
    for line, col in LINES:
        part = out.copy()
        part["line"] = line
        part["production_p_over"] = pd.to_numeric(joined[f"{col}_production"])
        part["shadow_p_over"] = pd.to_numeric(joined[f"{col}_formula"])
        part["probability_delta"] = part.shadow_p_over - part.production_p_over
        part["p_under"] = 1 - part.shadow_p_over
        part["selected_side"] = np.where(part.shadow_p_over.ge(.5), "OVER", "UNDER")
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def capture(*, formula_path: Path, production_path: Path, output_root: Path, run_id: str, slate_date: str,
            season: int, feature_cutoff_utc: str, canonical_game_ids: list[int], scorer_path: Path,
            production_identity: tuple[str, str], feature_binding: dict[str, Any] | None = None,
            formula_fitted_identity_sha256: str | None = None) -> dict[str, Any]:
    formula, production = pd.read_csv(formula_path), pd.read_csv(production_path)
    kwargs = dict(slate_date=slate_date, season=season, run_id=run_id, feature_cutoff_utc=feature_cutoff_utc,
                  canonical_game_ids=canonical_game_ids, production_identity=production_identity)
    frame = build_predictions(formula, production, **kwargs)
    replay = build_predictions(formula, production, **kwargs)
    out_root = Path(output_root) / f"season={season}" / f"slate_date={slate_date}" / f"run_id={run_id}"
    final = out_root / f"model={MODEL_IDENTITY}"
    stage = out_root / f".{MODEL_IDENTITY}.incomplete"
    out_root.mkdir(parents=True, exist_ok=True)
    if final.exists():
        raise FileExistsError(f"SOG_FORMULA_SHADOW_CAPTURE_ALREADY_EXISTS:{final}")
    stage.mkdir(exist_ok=False)
    prediction_path = stage / "predictions.csv"
    frame.to_csv(prediction_path, index=False, float_format="%.17g")
    replay_path = stage / "replay_predictions.csv"
    replay.to_csv(replay_path, index=False, float_format="%.17g")
    if sha256_file(prediction_path) != sha256_file(replay_path):
        raise ValueError("SOG_FORMULA_SHADOW_DETERMINISTIC_REPLAY_FAILED")
    replay_path.unlink()
    manifest = {
        "contract": "NHL_SOG_POISSON_FORMULA_SHADOW_V1", "model_identity": MODEL_IDENTITY,
        "formula_model_identity": {"family": "poisson_baseline", "version": "baseline_v1"},
        "research_role": "PROSPECTIVE_SHADOW", "season": season, "slate_date": slate_date,
        "parent_daily_run_id": run_id,
        "formula_scorer_path": str(Path(scorer_path).resolve()), "formula_scorer_sha256": sha256_file(scorer_path),
        "formula_fitted_model_identity_sha256": formula_fitted_identity_sha256,
        "formula_prediction_path": str(Path(formula_path).resolve()),
        "formula_prediction_sha256": sha256_file(formula_path),
        "formula_feature_binding": feature_binding or {},
        "production_model_identity": {"family": production_identity[0], "version": production_identity[1]},
        "production_prediction_path": str(Path(production_path).resolve()),
        "production_prediction_sha256": sha256_file(production_path),
        "canonical_game_ids": sorted(set(map(int, canonical_game_ids))),
        "canonical_game_set_hash": canonical_game_set_hash(canonical_game_ids),
        "feature_cutoff_utc": feature_cutoff_utc, "prediction_row_count": len(frame),
        "player_game_count": int(len(frame) // len(LINES)), "line_count": len(LINES),
        "deterministic_replay": "PASS", "prediction_sha256": sha256_file(prediction_path), "status": "COMPLETE",
    }
    (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n")
    readback = pd.read_csv(prediction_path)
    if len(readback) != len(frame) or not np.allclose(readback.shadow_p_over, frame.shadow_p_over, rtol=0, atol=1e-15):
        raise ValueError("SOG_FORMULA_SHADOW_CAPTURE_READBACK_FAILED")
    (stage / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE", "parent_daily_run_id": run_id}) + "\n")
    _write_sums(stage)
    stage.rename(final)
    verify_package(final)
    manifest["path"] = str(final.resolve())
    manifest["manifest_sha256"] = sha256_file(final / "manifest.json")
    return manifest
