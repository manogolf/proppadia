#!/usr/bin/env python3
"""Reproduce the certified season 2025 NHL SOG Poisson baseline.

Reads the certified denominator ledger and immutable daily prepared feature
archives. Writes analysis artifacts only. It does not train, tune, write a DB,
call a network service, or change production behavior.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail

STAMP = "2026-07-13"
EXPECTED_LEDGER_SHA = "92a9563fe73e3eb5ebff4310056d9e0a7f18f2a6629b5c75a32894dbae9da5aa"
TOLERANCE = 1e-12
TARGET_FAMILY = "poisson_baseline"
TARGET_VERSION = "baseline_v1"
TARGET_START = "2026-02-28"
TARGET_END = "2026-04-15"


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, lineterminator="\n")


def parse_p_over(notes: pd.Series) -> pd.Series:
    return pd.to_numeric(notes.str.extract(r"(?:^|;)p_over=([^;]+)")[0], errors="raise")


def coalesce_numeric(df: pd.DataFrame, names: list[str]) -> tuple[pd.Series, pd.Series]:
    value = pd.Series(np.nan, index=df.index, dtype=float)
    source = pd.Series("", index=df.index, dtype=str)
    for name in names:
        if name.startswith("SUM:"):
            cols = name[4:].split("+")
            vals = sum((pd.to_numeric(df.get(c, np.nan), errors="coerce") for c in cols), start=pd.Series(0.0, index=df.index))
            vals = vals.where(pd.concat([pd.to_numeric(df.get(c, np.nan), errors="coerce") for c in cols], axis=1).notna().any(axis=1))
        elif name.startswith("SEC_SUM:"):
            cols = name[8:].split("+")
            vals = sum((pd.to_numeric(df.get(c, np.nan), errors="coerce") for c in cols), start=pd.Series(0.0, index=df.index)) / 60.0
            vals = vals.where(pd.concat([pd.to_numeric(df.get(c, np.nan), errors="coerce") for c in cols], axis=1).notna().any(axis=1))
        else:
            vals = pd.to_numeric(df.get(name, np.nan), errors="coerce")
        take = value.isna() & vals.notna()
        value.loc[take] = vals.loc[take]
        source.loc[take] = name
    return value, source


def load_prepared(target: pd.DataFrame, root: Path) -> tuple[pd.DataFrame, list[dict]]:
    frames, audit = [], []
    for ds in sorted(target.game_date.unique()):
        fp = root / ds / "sog_features" / f"sog_features_{ds}_denali.csv"
        if not fp.exists():
            audit.append({"game_date": ds, "path": str(fp), "status": "MISSING", "rows": 0, "sha256": ""})
            continue
        f = pd.read_csv(fp, low_memory=False)
        f["game_date"] = f["game_date"].astype(str)
        f["prepared_source_path"] = str(fp)
        f["prepared_source_sha256"] = sha(fp)
        audit.append({"game_date": ds, "path": str(fp), "status": "LOADED", "rows": len(f), "sha256": sha(fp)})
        frames.append(f)
    if not frames:
        return pd.DataFrame(), audit
    return pd.concat(frames, ignore_index=True), audit


def diagnostics(df: pd.DataFrame, probability_col: str, output_kind: str) -> list[dict]:
    rows = []
    groupings = [([], "ALL"), (["line"], "LINE"), (["side"], "SIDE"), (["calendar_month"], "MONTH"), (["model_version"], "MODEL_VERSION")]
    for cols, dimension in groupings:
        groups = [((), df)] if not cols else df.groupby(cols, dropna=False)
        for key, g in groups:
            p = pd.to_numeric(g[probability_col], errors="coerce").clip(1e-15, 1 - 1e-15)
            threshold = g["line"].astype(float).map({1.5: 2, 2.5: 3, 3.5: 4})
            y_over = (g["official_sog"].astype(float) >= threshold).astype(int)
            pred_over = p >= 0.5
            win = np.where(pred_over, y_over == 1, y_over == 0)
            brier = float(np.mean((p - y_over) ** 2))
            logloss = float(-np.mean(y_over * np.log(p) + (1 - y_over) * np.log(1 - p)))
            label = "ALL" if not cols else str(key if not isinstance(key, tuple) else "|".join(map(str, key)))
            rows.append({"output_kind": output_kind, "dimension": dimension, "segment": label, "rows": len(g), "wins": int(win.sum()), "losses": int((~win).sum()), "pushes": 0, "win_rate": float(win.mean()), "brier_score": brier, "log_loss": logloss})
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--certified-ledger", required=True)
    ap.add_argument("--prepared-root", default="artifacts/archive/generated_daily/nhl")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    ledger_path, out_dir = Path(args.certified_ledger), Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if sha(ledger_path) != EXPECTED_LEDGER_SHA:
        raise RuntimeError("Certified ledger hash mismatch")

    ledger = pd.read_csv(ledger_path, low_memory=False)
    if len(ledger) != 341900 or set(ledger.canonical_season.unique()) != {2023, 2024, 2025}:
        raise RuntimeError("Certified parent population mismatch")
    eligible = ledger[(ledger.canonical_season == 2025) & (ledger.accuracy_denominator_eligible == True)].copy()
    if len(eligible) != 91049:
        raise RuntimeError(f"Expected 91049 certified season 2025 rows, got {len(eligible)}")
    eligible["stored_p_over"] = parse_p_over(eligible.notes)
    eligible["calendar_month"] = eligible.game_date.str[:7]
    target = eligible[(eligible.model_or_strategy == TARGET_FAMILY) & (eligible.model_version == TARGET_VERSION) & eligible.game_date.between(TARGET_START, TARGET_END)].copy()
    if len(target) != 40167:
        raise RuntimeError(f"Expected 40167 certified target rows, got {len(target)}")

    prepared, source_audit = load_prepared(target, Path(args.prepared_root))
    key = ["game_date", "game_id", "player_id"]
    duplicate_prepared = prepared.duplicated(key, keep=False) if not prepared.empty else pd.Series(dtype=bool)
    if not prepared.empty and duplicate_prepared.any():
        raise RuntimeError(f"Prepared input has duplicate player-game keys: {int(duplicate_prepared.sum())}")
    target["game_id"] = target.game_id.astype("int64")
    target["player_id"] = target.player_id.astype("int64")
    if not prepared.empty:
        prepared["game_id"] = pd.to_numeric(prepared.game_id, errors="raise").astype("int64")
        prepared["player_id"] = pd.to_numeric(prepared.player_id, errors="raise").astype("int64")
    merged = target.merge(prepared, on=key, how="left", validate="many_to_one", suffixes=("", "_prepared"), indicator=True)
    rate, rate_source = coalesce_numeric(merged, ["d10_sog_per60", "d20_sog_per60", "d5_sog_per60"])
    toi, toi_source = coalesce_numeric(merged, ["d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg", "SUM:szn_toi_per_game_5on5+szn_toi_per_game_pp", "SEC_SUM:season_5on5_icetime_per_game+season_5on4_icetime_per_game"])
    expected = ((rate * toi) / 60.0).fillna(0.0).clip(lower=0.0)
    threshold = merged.line.astype(float).map({1.5: 2, 2.5: 3, 3.5: 4})
    regenerated = pd.Series([_poisson_tail(float(lam), int(t)) for lam, t in zip(expected, threshold)], index=merged.index)
    diff = (regenerated - merged.stored_p_over).abs()
    regen_side = np.where(regenerated >= 0.5, "OVER", "UNDER")
    exact = diff == 0
    within = diff <= TOLERANCE
    prepared_present = merged._merge == "both"

    reproduction_mode = np.where(prepared_present, "MODE_A_EXACT_PREPARED_INPUT_REPLAY", "MODE_D_STORED_OUTPUT_ONLY")
    parity_status = np.where(~prepared_present, "MISSING_PREPARED_INPUT", np.where(exact, "EXACT_MATCH", np.where(within, "TOLERANCE_MATCH", "MATERIAL_MISMATCH")))
    repro = pd.DataFrame({
        "prediction_identity": merged.prediction_identity, "canonical_season": merged.canonical_season,
        "game_date": merged.game_date, "game_id": merged.game_id, "player_id": merged.player_id,
        "line": merged.line, "side": merged.side, "stored_side": merged.side, "regenerated_side": regen_side,
        "model_or_strategy": merged.model_or_strategy, "model_version": merged.model_version,
        "stored_p_over": merged.stored_p_over, "regenerated_p_over": regenerated,
        "absolute_difference": diff, "regenerated_expected_sog": expected,
        "rate_source": rate_source, "toi_source": toi_source,
        "prepared_source_path": merged.get("prepared_source_path", ""),
        "prepared_source_sha256": merged.get("prepared_source_sha256", ""),
        "reproduction_mode": reproduction_mode, "probability_parity_status": parity_status,
        "side_parity": merged.side.astype(str) == regen_side, "official_sog": merged.official_sog,
        "settlement_status": merged.settlement_status,
    })
    write_csv(out_dir / f"nhl_season_2025_sog_reproduction_ledger_{STAMP}.csv", repro)

    parity = []
    for cols, dimension in [([], "ALL"), (["line"], "LINE"), (["game_date"], "DATE"), (["reproduction_mode"], "MODE")]:
        groups = [((), repro)] if not cols else repro.groupby(cols, dropna=False)
        for k, g in groups:
            d = g.absolute_difference
            parity.append({"dimension": dimension, "segment": "ALL" if not cols else str(k), "rows": len(g), "exact_matches": int((d == 0).sum()), "tolerance_matches": int((d <= TOLERANCE).sum()), "material_mismatches": int((d > TOLERANCE).sum()), "missing_regenerated_rows": int((g.probability_parity_status == "MISSING_PREPARED_INPUT").sum()), "mean_absolute_difference": d.mean(), "median_absolute_difference": d.median(), "p95_absolute_difference": d.quantile(.95), "max_absolute_difference": d.max(), "correlation": g.stored_p_over.corr(g.regenerated_p_over), "exact_match_rate": float((d == 0).mean()), "tolerance_match_rate": float((d <= TOLERANCE).mean()), "tolerance": TOLERANCE})
    write_csv(out_dir / f"nhl_season_2025_sog_probability_parity_{STAMP}.csv", pd.DataFrame(parity))

    mismatch = repro[(repro.probability_parity_status != "EXACT_MATCH") | (~repro.side_parity)].copy()
    mismatch["mismatch_cause"] = np.where(mismatch.probability_parity_status == "MISSING_PREPARED_INPUT", "MISSING_PREPARED_FEATURE_ARCHIVE", np.where(mismatch.absolute_difference <= TOLERANCE, "FLOATING_POINT_ROUNDING", "UNKNOWN_OR_INPUT_CODE_DRIFT"))
    write_csv(out_dir / f"nhl_season_2025_sog_reproduction_mismatches_{STAMP}.csv", mismatch)

    candidate = repro[["prediction_identity", "game_date", "game_id", "player_id", "line", "stored_side", "regenerated_side", "side_parity"]].copy()
    candidate["stored_candidate_eligibility"] = "UNKNOWN"
    candidate["regenerated_candidate_eligibility"] = "NOT_ATTEMPTED"
    candidate["candidate_parity_status"] = "BLOCKED_NO_RUN_BOUND_POLICY_AND_ODDS_SNAPSHOT"
    candidate["notes"] = "Model probability/side parity is separate from candidate selection; no nearest policy fallback used."
    write_csv(out_dir / f"nhl_season_2025_sog_candidate_parity_{STAMP}.csv", candidate)

    accuracy = diagnostics(repro.assign(calendar_month=repro.game_date.str[:7]), "stored_p_over", "STORED_PRODUCTION")
    exact_repro = repro[repro.probability_parity_status.isin(["EXACT_MATCH", "TOLERANCE_MATCH"])].copy()
    if not exact_repro.empty:
        exact_repro["calendar_month"] = exact_repro.game_date.str[:7]
        accuracy += diagnostics(exact_repro, "regenerated_p_over", "REGENERATED_EXACT_OR_TOLERANCE")
    write_csv(out_dir / f"nhl_season_2025_sog_accuracy_diagnostics_{STAMP}.csv", pd.DataFrame(accuracy))

    timeline = ledger[ledger.canonical_season == 2025].copy()
    timeline["prediction_timestamp_date"] = timeline.prediction_timestamp_utc.astype(str).str[:10]
    timeline["production_shadow_status"] = np.where(timeline.model_or_strategy.isin(["denali_blend", "poisson_baseline"]), "PRODUCTION_DB_WRITE", "OTHER")
    tl = timeline.groupby(["game_date", "prediction_timestamp_date", "model_or_strategy", "model_version", "prediction_source", "production_shadow_status"], dropna=False).size().reset_index(name="prediction_rows")
    write_csv(out_dir / f"nhl_season_2025_sog_production_timeline_{STAMP}.csv", tl)

    waterfall = pd.DataFrame([
        ["CERTIFIED_SEASON_2025_ACCURACY_DENOMINATOR", 91049, "parent filter"],
        ["TARGET_MODEL_VERSION", len(target), "poisson_baseline/baseline_v1"],
        ["TARGET_OPERATIONAL_DATE_RANGE", len(target), f"{TARGET_START}..{TARGET_END}"],
        ["MODE_A_PREPARED_INPUT_ROWS", int(prepared_present.sum()), "saved daily prepared feature archive"],
        ["EXACT_PROBABILITY_ROWS", int(exact.sum()), "absolute difference 0"],
        ["TOLERANCE_PROBABILITY_ROWS", int(within.sum()), f"absolute difference <= {TOLERANCE}"],
        ["BOUNDED_RECONSTRUCTION_ROWS", 0, "not used"],
        ["STORED_OUTPUT_ONLY_ROWS", int((~prepared_present).sum()), "prepared input archive missing"],
        ["BLOCKED_OR_MATERIAL_MISMATCH_ROWS", int(((~prepared_present) | (~within)).sum()), "missing input or material mismatch"],
        ["ACCURACY_ELIGIBLE_REPLAY_ROWS", int((prepared_present & within).sum()), "Mode A within tolerance"],
    ], columns=["stage", "rows", "definition"])
    write_csv(out_dir / f"nhl_season_2025_sog_population_waterfall_{STAMP}.csv", waterfall)

    write_csv(out_dir / f"nhl_season_2025_sog_prepared_source_audit_{STAMP}.csv", pd.DataFrame(source_audit))
    summary = {"target_rows": len(target), "mode_a_rows": int(prepared_present.sum()), "exact_rows": int(exact.sum()), "tolerance_rows": int(within.sum()), "material_mismatch_rows": int((prepared_present & ~within).sum()), "stored_output_only_rows": int((~prepared_present).sum()), "side_match_rows": int((merged.side.astype(str) == regen_side).sum()), "ledger_sha256": EXPECTED_LEDGER_SHA, "tolerance": TOLERANCE}
    (out_dir / f"nhl_season_2025_sog_reproduction_run_summary_{STAMP}.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
