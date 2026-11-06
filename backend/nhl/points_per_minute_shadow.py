"""Points per-minute shadow (NHL Backlog P2.46, operator-approved 2026-11-06).

A shadow beside the production Points model (goals + assists, P2.39): the same
inputs plus per-minute scoring rates, ``own_rate_toi``, ``goal_rate_toi`` and
``assist_rate_toi`` = (last season's + this season's points, goals or assists
+ 30 games of league rate) / (last season's + this season's minutes + 30 games
of league-average minutes) x the player's TOI over his last 10 games. Research
(``scripts/research_nhl_points_role_change_tests.py``, candidate
PER_MINUTE_ADD) found it recovers most of the market's lead on players whose
role changed. Nothing reads these predictions except the grader; production
is unchanged.

Inputs per Points capture (``points_hgb_shadow/.../captured_at=*``):
``features.csv``, this season's official outcomes and skater logs up to the
capture's cutoff; last season's totals and league rates from the production
Points contract; last season's minutes from the input watch's
``prior_season_usage.csv``. Output: ``OUTPUT_ROOT/season=<s>/slate_date=<d>/
run_id=<r>/captured_at=<t>/`` with ``predictions.csv`` and ``summary.json``.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIR = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/per_minute_shadow_2026-11-06"
CONTRACT_PATH = CONTRACT_DIR / "shadow_contract.json"
CAPTURE_ROOT = ROOT / "artifacts/operational/nhl/points_hgb_shadow"
OUTPUT_ROOT = ROOT / "artifacts/operational/nhl/points_per_minute_shadows"
USAGE_PATH = ROOT / "artifacts/operational/nhl/input_watch/prior_season_usage.csv"
SCHEMA = "NHL_POINTS_PER_MINUTE_SHADOW_V1"
MODEL_VERSION = "NHL_POINTS_PER_MINUTE_SHADOW_V1"
K = 30
KEYS = ["game_id", "player_id"]
TOI_FEATURES = ["own_rate_toi", "goal_rate_toi", "assist_rate_toi"]


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def season_of(day: str) -> int:
    return int(day[:4]) if int(day[5:7]) >= 9 else int(day[:4]) - 1


def per_minute_inputs(features: pd.DataFrame, outcomes: pd.DataFrame, logs: pd.DataFrame,
                      prior_totals: pd.DataFrame, league_rates: pd.DataFrame, usage: pd.DataFrame,
                      season: int) -> pd.DataFrame:
    """Per-minute rates for each target row, from strict-prior history only."""
    f = features.copy()
    f["player_id"] = f.player_id.astype("int64")
    day = pd.to_datetime(f.game_date).min()
    o = outcomes[outcomes.canonical_season.eq(season) & (pd.to_datetime(outcomes.slate_date) < day)]
    cur = o.groupby("player_id").agg(cgo=("official_goals", "sum"), ca=("official_assists", "sum"))
    lg = logs[logs.canonical_season.eq(season) & (pd.to_datetime(logs.game_date) < day)]
    cur_min = pd.to_numeric(lg.toi_minutes, errors="coerce").groupby(lg.player_id.astype("int64")).sum()
    prior = prior_totals[prior_totals.season.eq(season - 1)].set_index("player_id")
    use = usage[usage.season.eq(season - 1)].copy()
    use["minutes"] = use.games * use.toi_per_game
    lg_min = float(use.minutes.sum() / use.games.sum())
    mu = league_rates.set_index("season").loc[season - 1]
    p_min = f.player_id.map(use.set_index("player_id").minutes)
    f["cur_min"] = f.player_id.map(cur_min)
    f["cur_min"] = f.cur_min.fillna(f.current_season_games_prior * f.mean_toi_last10)
    cur_toi = (f.cur_min / f.current_season_games_prior.replace(0, np.nan)).fillna(f.mean_toi_last10)
    pg = f.player_id.map(prior.games).fillna(0)
    f["prior_minutes_estimated"] = p_min.isna()
    f["p_min"] = p_min.fillna(pg * cur_toi)
    counts = {
        "own_rate_toi": (f.player_id.map(prior.points).fillna(0), f.current_season_points_prior, mu.points),
        "goal_rate_toi": (f.player_id.map(prior.goals).fillna(0), f.player_id.map(cur.cgo).fillna(0), mu.goals),
        "assist_rate_toi": (f.player_id.map(prior.assists).fillna(0), f.player_id.map(cur.ca).fillna(0), mu.assists),
    }
    for name, (before, now, rate) in counts.items():
        f[name] = (before + now + K * rate) / (f.p_min + f.cur_min + K * lg_min) * f.mean_toi_last10
    return f[KEYS + TOI_FEATURES + ["cur_min", "p_min", "prior_minutes_estimated"]]


def load_contract() -> tuple[dict[str, Any], Any]:
    contract = json.loads(CONTRACT_PATH.read_text())
    artifact = CONTRACT_DIR / contract["fitted_artifact"]
    if sha256(artifact) != contract["fitted_artifact_sha256"]:
        raise RuntimeError("POINTS_PER_MINUTE_ARTIFACT_HASH_DRIFT")
    return contract, joblib.load(artifact)


def production_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
    contract = json.loads(scorer.CONTRACT_PATH.read_text())
    folder = scorer.CONTRACT_PATH.parent
    return pd.read_csv(folder / contract["prior_season_totals"]), pd.read_csv(folder / contract["league_rates"])


def output_dir(capture_dir: Path, output_root: Path = OUTPUT_ROOT) -> Path:
    return Path(output_root) / Path(capture_dir).relative_to(CAPTURE_ROOT)


def score_capture(capture_dir: Path, *, output_root: Path = OUTPUT_ROOT, usage_path: Path = USAGE_PATH) -> dict:
    """Score one Points capture; existing output is left as it is."""
    from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
    capture_dir = Path(capture_dir)
    out = output_dir(capture_dir, output_root)
    if (out / "summary.json").is_file():
        return {"status": "EXISTS", "path": str(out)}
    contract, model = load_contract()
    features = pd.read_csv(capture_dir / "features.csv")
    base_inputs = [c for c in contract["feature_columns"] if c not in TOI_FEATURES]
    missing = sorted(set(base_inputs) - set(features.columns))
    if missing:
        # Captures from before the 2026-11-01 Points cutover lack the component rates.
        return {"status": "SKIPPED_INPUTS_MISSING", "missing": missing, "path": str(capture_dir)}
    outcomes = pd.read_csv(capture_dir / "source_input_01_official_outcomes_00.csv")
    logs = pd.read_csv(capture_dir / "source_input_02_official_skater_logs.csv")
    totals, league = production_tables()
    day = str(features.slate_date.iloc[0])
    inputs = per_minute_inputs(features, outcomes, logs, totals, league, pd.read_csv(usage_path), season_of(day))
    frame = features.merge(inputs, on=KEYS, how="left", validate="one_to_one")
    scored = scorer.score(frame, model)
    predictions = frame[KEYS + ["game_date"] + TOI_FEATURES + ["prior_minutes_estimated"]].merge(
        scored, on=KEYS, how="left", validate="one_to_one")
    out.mkdir(parents=True, exist_ok=False)
    predictions.to_csv(out / "predictions.csv", index=False, float_format="%.10g")
    summary = {"schema_version": SCHEMA, "model_version": MODEL_VERSION, "slate_date": day,
               "capture_dir": str(capture_dir.relative_to(ROOT)), "rows": int(len(predictions)),
               "prior_minutes_estimated": int(predictions.prior_minutes_estimated.sum()),
               "contract_sha256": sha256(CONTRACT_PATH), "fitted_artifact_sha256": contract["fitted_artifact_sha256"],
               "usage_sha256": sha256(usage_path),
               "scored_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
               "production_unchanged": True, "database_writes": 0}
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return {"status": "SCORED", "rows": summary["rows"], "path": str(out)}


def captures_for(slate_date: str, capture_root: Path = CAPTURE_ROOT) -> list[Path]:
    root = Path(capture_root) / f"season={season_of(slate_date)}" / f"slate_date={slate_date}"
    return sorted(p for p in root.glob("run_id=*/phase=*/captured_at=*") if (p / "features.csv").is_file())
