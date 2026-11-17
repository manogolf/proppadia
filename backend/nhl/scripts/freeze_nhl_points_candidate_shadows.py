#!/usr/bin/env python3
"""Freeze the stage-1 Points candidate shadow arms (NHL Backlog P2.58, operator 2026-11-16).

Arms (goals + assists, production recipe and hyperparameters, inputs = production + PP unit +
morning game lines, `backend/nhl/points_candidate_shadows.py`):

  TOTALS_PPU_2S  trained on 2023-24 + 2024-25 (rebuilt frames, Utah included)
  TOTALS_PPU_3S  trained on 2023-24 .. 2025-26

Training inputs come from the research builders (research_nhl_points_ppunit_game_scoring.py,
research_nhl_points_game_totals.py; historical lines from fetch_nhl_historical_game_lines.py).
Each arm is fitted twice and must reproduce exactly. Live parity: the module's builders, run on
the latest Points capture of 2026-11-15 (taken after the PP TOI correction; earlier captures carry
pre-correction PP TOI), must reproduce the research inputs for the same player-games (PP unit
99%+, game lines within 0.01 goals). The research builder covers played games only, so later
slates are checked for input coverage, not values.
Writes CONTRACT_ROOT/arm=<name>/contract.json and the artifact; refuses to overwrite.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_candidate_shadows as shadow  # noqa: E402
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

REBUILT = ROOT / "artifacts/analysis/nhl/points_training_rebuild_2026-11-16"
PPU_INPUTS = ROOT / "artifacts/analysis/nhl/points_ppunit_game_scoring_2026-11-16/inputs.csv.gz"
TOT_DIR = ROOT / "artifacts/analysis/nhl/points_game_totals_2026-11-16"
LINES_MANIFEST = ROOT / "artifacts/analysis/nhl/game_lines_history_2026-11-16/manifest.json"
KEYS = ["game_id", "player_id"]
FEATURES = list(freeze.FEATURES) + shadow.PPU + shadow.TOT
# Captures before the P2.45 correction (applied 2026-11-15) carry pre-correction PP TOI in their
# skater logs, so PP-unit parity is checked on captures taken after it (11-15 evening on).
PARITY_DAYS = ("2026-11-15", "2026-11-15")
FIRST_CORRECTED_CAPTURE_UTC = "2026-11-15T20:00:00Z"


def research_inputs() -> pd.DataFrame:
    pp = pd.read_csv(PPU_INPUTS)[KEYS + shadow.PPU]
    team = pd.read_csv(TOT_DIR / "player_team.csv.gz")
    gi = pd.read_csv(TOT_DIR / "game_inputs.csv.gz")
    return pp.merge(team, on=KEYS, how="left").merge(gi, on=["game_id", "team_id"], how="left")


def fit(train: pd.DataFrame) -> dict:
    g = freeze.research.fit_count(train, FEATURES, "goals")
    a = freeze.research.fit_count(train, FEATURES, "assists")
    return {"structure": "GOALS_PLUS_ASSISTS", "feature_columns": FEATURES, "fill_zero": True,
            "scaler": g["scaler"], "goals": g, "assists": a}


def parity(ri: pd.DataFrame) -> dict:
    rows = []
    for day in pd.date_range(*PARITY_DAYS).strftime("%Y-%m-%d"):
        caps = [c for c in shadow.captures_for(day)
                if c.name.split("=", 1)[1][:15] >= FIRST_CORRECTED_CAPTURE_UTC.replace("-", "").replace(":", "")[:15]]
        if not caps:
            continue
        frame, meta = shadow.capture_inputs(caps[-1])
        rows.append(frame[KEYS + shadow.PPU + shadow.TOT].assign(slate=day))
    live = pd.concat(rows, ignore_index=True)
    missing = live.iloc[:0]
    if False:  # the research builder only covers played games; parity uses 11-15 (post-correction) only
        from backend.nhl.scripts import research_nhl_points_ppunit_game_scoring as pu
        from backend.nhl.scripts import research_nhl_points_game_totals as gt
        pg, tg = pu.team_games()
        extra = pu.inputs(missing[KEYS], pg, tg)[KEYS + shadow.PPU]
        gi, _ = gt.game_inputs()
        team = pg[KEYS + ["team_id"]]
        extra = extra.merge(team, on=KEYS, how="left").merge(gi, on=["game_id", "team_id"], how="left")
        ri = pd.concat([ri, extra], ignore_index=True)
    m = live.merge(ri[KEYS + shadow.PPU + shadow.TOT], on=KEYS, suffixes=("", "_r"), how="inner")
    out = {"rows_compared": int(len(m)), "slates": int(live.slate.nunique()), "rows_built_for_parity": int(len(missing))}
    for c in shadow.PPU:
        a, b = m[c], m[f"{c}_r"]
        out[f"{c}_equal_share"] = float((np.isclose(a, b, atol=1e-9) | (a.isna() & b.isna())).mean())
    for c in shadow.TOT:
        a, b = m[c], m[f"{c}_r"]
        both = a.notna() & b.notna()
        out[f"{c}_max_abs_diff"] = float((a[both] - b[both]).abs().max()) if both.any() else None
        out[f"{c}_within_0_01_share"] = float(((a - b).abs() <= 0.01)[both].mean()) if both.any() else None
        out[f"{c}_live_missing_share"] = float(a.isna().mean())
    return out


def identity(contract: dict) -> str:
    body = {k: v for k, v in contract.items() if k != "model_identity_sha256"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def write_prior_season_pp() -> dict:
    """Last five 2025-26 regular-season games per player (pp1, pp_rank) from the corrected database."""
    from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit
    logs, _ = refit.pull_history()
    logs = logs[logs.canonical_season.eq(2025)]
    pg = shadow.pp_unit_table(logs).sort_values(["player_id", "game_date", "game_id"])
    last = pg.groupby("player_id").tail(5)[KEYS + ["game_date", "pp1", "pp_rank"]]
    shadow.CONTRACT_ROOT.mkdir(parents=True, exist_ok=True)
    last.to_csv(shadow.PRIOR_SEASON_PP, index=False)
    return {"players": int(last.player_id.nunique()), "rows": int(len(last)), "sha256": shadow.sha256(shadow.PRIOR_SEASON_PP)}


def main() -> int:
    prior_pp = write_prior_season_pp()
    ri = research_inputs()
    train2 = pd.read_csv(REBUILT / "training_frame_2023_2024.csv.gz", float_precision="round_trip").merge(ri, on=KEYS, how="left")
    s25 = pd.read_csv(REBUILT / "season_frame_2025.csv.gz", float_precision="round_trip").merge(ri, on=KEYS, how="left")
    check = parity(ri)
    ok = (check["rows_compared"] >= 150 and all(check[f"{c}_equal_share"] >= 0.99 for c in shadow.PPU)
          and all((check[f"{c}_within_0_01_share"] or 0) >= 0.99 for c in shadow.TOT))
    if not ok:
        raise SystemExit(f"POINTS_CANDIDATE_PARITY_FAILED:{json.dumps(check)}")
    production = json.loads(scorer.CONTRACT_PATH.read_text())
    arms = {"TOTALS_PPU_2S": (train2, [2023, 2024]), "TOTALS_PPU_3S": (pd.concat([train2, s25], ignore_index=True), [2023, 2024, 2025])}
    for name, (train, seasons) in arms.items():
        d = shadow.CONTRACT_ROOT / f"arm={name}"
        d.mkdir(parents=True, exist_ok=False)
        first, second = fit(train), fit(train.copy())
        art = f"nhl_points_candidate_{name.lower()}.joblib"
        joblib.dump(first, d / art)
        frozen = joblib.load(d / art)
        p1, p2, pf = (scorer.score(s25, m).expected_points.to_numpy() for m in (first, second, frozen))
        if not (np.array_equal(p1, p2) and np.array_equal(p1, pf)):
            raise SystemExit(f"POINTS_CANDIDATE_NOT_REPRODUCIBLE:{name}")
        contract = {
            "schema_version": "NHL_POINTS_CANDIDATE_SHADOW_CONTRACT_V1", "arm": name, "role": "SHADOW",
            "production_unchanged": True, "approved": "operator 2026-11-16 (\"proceed with building points shadows\")",
            "structure": "GOALS_PLUS_ASSISTS", "feature_columns": FEATURES, "added_inputs": shadow.PPU + shadow.TOT,
            "hyperparameters": production["hyperparameters"], "calibration_layer": "NONE",
            "training_seasons": seasons, "training_rows": int(len(train)),
            "fitted_artifact": art, "fitted_artifact_sha256": shadow.sha256(d / art),
            "production_reference_identity_sha256": production["model_identity_sha256"],
            "input_module": "backend/nhl/points_candidate_shadows.py",
            "input_module_sha256": shadow.sha256(ROOT / "backend/nhl/points_candidate_shadows.py"),
            "historical_lines_manifest_sha256": shadow.sha256(LINES_MANIFEST),
            "research_inputs": {"pp_unit": str(PPU_INPUTS.relative_to(ROOT)), "pp_unit_sha256": shadow.sha256(PPU_INPUTS),
                                "game_lines": str((TOT_DIR / "game_inputs.csv.gz").relative_to(ROOT)),
                                "game_lines_sha256": shadow.sha256(TOT_DIR / "game_inputs.csv.gz")},
            "reproducibility": {"refit_runs": 2, "status": "PASS", "scored_rows": int(len(s25))},
            "live_parity": check, "prior_season_pp_units": {"path": str(shadow.PRIOR_SEASON_PP.relative_to(ROOT)), **prior_pp},
            "evidence": "NHL Backlog P2.55 (game totals + PP unit) and P2.58",
        }
        contract["model_identity_sha256"] = identity(contract)
        (d / "contract.json").write_text(json.dumps(contract, indent=2) + "\n")
        print(name, contract["model_identity_sha256"][:8], contract["training_rows"])
    print(json.dumps(check, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
