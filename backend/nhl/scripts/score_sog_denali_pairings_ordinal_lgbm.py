# backend/nhl/scripts/score_sog_denali_pairings_ordinal_lgbm.py
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

DEFAULT_FEATURE_META = (
    "backend/nhl/models/experimental/shots_on_goal/"
    "sog_player_denali_pairings_ordinal_v1/ge_2/metadata.json"
)

DEFAULT_MODEL_ROOT = (
    "backend/nhl/models/experimental/shots_on_goal/"
    "sog_player_denali_pairings_ordinal_v1"
)

DEFAULT_OUT = "backend/nhl/data/processed/sog_predictions_ordinal_wide.csv"

def _fill_missing_pairings_cov_cols(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Back-compat shim:
      Early-season / older Denali feature exports may not include the newer
      pairings + shiftcharts coverage columns, but the deployed ordinal model
      metadata expects them.

    We fill them with safe "no data available" defaults:
      - *_pairings_available: 0
      - *_shiftcharts_coverage_rate: 0.0
      - *_pairings_cov_bucket: 'none'   (verified domain includes 'none')
      - overlap/repeat rates: 0.0
    """
    defaults = {
        "d10_top_mate_overlap_share_avg": 0.0,
        "d10_top3_mates_overlap_share_avg": 0.0,
        "d20_top_mate_overlap_share_avg": 0.0,
        "d20_top3_mates_overlap_share_avg": 0.0,
        "d10_shiftcharts_coverage_rate": 0.0,
        "d20_shiftcharts_coverage_rate": 0.0,
        "d10_pairings_cov_bucket": "none",
        "d20_pairings_cov_bucket": "none",
        "d20_top_mate_repeat_rate": 0.0,
    }

    missing = [c for c in defaults.keys() if c not in df.columns]
    if missing:
        for c in missing:
            df[c] = defaults[c]

        # Buckets are categorical; domain includes '0','3','high','none'
        for c in ("d10_pairings_cov_bucket", "d20_pairings_cov_bucket"):
            if c in df.columns:
                df[c] = df[c].astype(str).fillna("none")

        print(f"[ordinal scorer] filled missing pairings/coverage cols: {missing}")

    return df

def _alias_sog_count_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    The ordinal model expects legacy count names:
      num_sog_*, num_event_*, team_num_sog_*, team_num_event_*
    But the Denali export now provides the canonical names:
      num_shotwasongoal_*, num_event_shot_*, team_num_shotwasongoal_for_last10, team_num_event_shot_for_last10

    This bridges the gap without touching SQL/views.
    """
    pairs = [
        ("num_sog_last5",      "num_shotwasongoal_last5"),
        ("num_sog_last10",     "num_shotwasongoal_last10"),
        ("num_sog_szn_to_date","num_shotwasongoal_season_to_date"),
        ("team_num_sog_last10","team_num_shotwasongoal_for_last10"),

        ("num_event_last5",     "num_event_shot_last5"),
        ("num_event_last10",    "num_event_shot_last10"),
        ("num_event_szn_to_date","num_event_shot_season_to_date"),
        ("team_num_event_last10","team_num_event_shot_for_last10"),
    ]

    for dst, src in pairs:
        if src not in df.columns:
            continue
        if dst not in df.columns:
            df[dst] = df[src]
            continue

        # If dst exists but is entirely null, backfill from src
        if df[dst].isna().all():
            df[dst] = df[src]

    return df

def _quick_feature_sanity_check(df: pd.DataFrame, feats: list[str], *, in_path: Path) -> None:
    # 1) Required cols exist (model contract)
    missing = [c for c in feats if c not in df.columns]
    if missing:
        raise SystemExit(
            f"[sanity] {in_path} missing {len(missing)} required feature cols (sample): {missing[:20]}"
        )

    def _stat(col: str):
        s = pd.to_numeric(df[col], errors="coerce")
        return {
            "nonnull": float(s.notna().mean()),
            "gt0": float((s.fillna(0) > 0).mean()),
            "nunique": int(s.dropna().nunique()),
            "max": float(s.dropna().max()) if s.notna().any() else float("nan"),
        }

    # 2) Report BOTH naming families so we stop guessing
    families = {
        "sog_alias_family": [
            "num_sog_last5",
            "num_sog_last10",
            "num_sog_szn_to_date",
            "team_num_sog_last10",
        ],
        "canonical_denali_family": [
            "num_shotwasongoal_last5",
            "num_shotwasongoal_last10",
            "num_shotwasongoal_season_to_date",
            "team_num_shotwasongoal_for_last10",
        ],
        "attempts_alias_family": [
            "num_event_last5",
            "num_event_last10",
            "num_event_szn_to_date",
            "team_num_event_last10",
        ],
        "canonical_attempts_family": [
            "num_event_shot_last5",
            "num_event_shot_last10",
            "num_event_shot_season_to_date",
            "team_num_event_shot_for_last10",
        ],
    }

    print(f"[sanity] in={in_path} rows={len(df)}")

    for fam, cols in families.items():
        cols = [c for c in cols if c in df.columns]
        if not cols:
            print(f"[sanity] {fam}: (none present)")
            continue
        parts = []
        for c in cols:
            st = _stat(c)
            parts.append(
                f"{c}: nonnull={st['nonnull']:.3f} gt0={st['gt0']:.3f} nunique={st['nunique']} max={st['max']:.1f}"
            )
        print(f"[sanity] {fam}: " + " | ".join(parts))

    # 3) Hard fail if the model-required “dead columns” pattern appears.
    #    This catches the exact thing you just discovered in the 22,658-row training CSV.
    #    If the model expects num_sog_* but those are all-null/all-zero, stop now.
    # Some required features are legitimately constant on a given slate.
    # Example: first slate after a long break can produce b2b_flag=0 for everyone.
    allow_constant_required = {
        "b2b_flag",
        "d10_pairings_available",
        "d20_pairings_available",
    }

    dead = []
    constant_allowed = []
    for c in feats:
        if c not in df.columns:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        nonnull = float(s.notna().mean())
        nunique = int(s.dropna().nunique())
        gt0 = float((s.fillna(0) > 0).mean())
        if nonnull == 0.0:
            dead.append((c, nonnull, gt0, nunique))
            continue
        if nunique <= 1:
            if c in allow_constant_required:
                constant_allowed.append((c, nonnull, gt0, nunique))
            else:
                dead.append((c, nonnull, gt0, nunique))

    if constant_allowed:
        sample_ok = constant_allowed[:10]
        msg_ok = " | ".join([f"{c}:nonnull={nn:.3f} gt0={g0:.3f} nunique={nu}" for c, nn, g0, nu in sample_ok])
        print(
            f"[sanity] allowed constant required feature cols ({len(constant_allowed)}): {msg_ok}",
            file=sys.stderr,
        )

    # Don’t print 200 columns; just the first few to prove the point.
    if dead:
        sample = dead[:15]
        msg = " | ".join([f"{c}:nonnull={nn:.3f} gt0={g0:.3f} nunique={nu}" for c, nn, g0, nu in sample])
        raise SystemExit(
            f"[sanity] detected {len(dead)} dead/constant feature cols required by model. sample: {msg}"
        )

def load_features_list(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"[ordinal scorer] feature metadata not found: {p}")

    meta = json.loads(p.read_text())
    feats = meta.get("features")
    if not feats or not isinstance(feats, list):
        raise SystemExit(f"[ordinal scorer] metadata missing 'features': {p}")

    # Ensure unique, stable order, string-only
    out = []
    seen = set()
    for f in feats:
        if isinstance(f, str) and f and f not in seen:
            out.append(f)
            seen.add(f)

    if not out:
        raise SystemExit(f"[ordinal scorer] metadata 'features' empty after cleaning: {p}")

    return out

def prep_X(df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    miss = [c for c in feats if c not in df.columns]
    if miss:
        raise SystemExit(
            f"Input CSV missing {len(miss)} feature cols (sample): {miss[:25]}"
        )

    X = df[feats].copy()

    # 1) Bool-like columns stored as 't'/'f'
    BOOL_COLS = {
        "is_home",
        "b2b_flag",
        "hot_last5_flag",
    }
    for c in (BOOL_COLS & set(feats)):
        s = X[c].astype(str).str.strip().str.lower()
        X[c] = s.map({"t": 1.0, "true": 1.0, "1": 1.0, "f": 0.0, "false": 0.0, "0": 0.0})
        X[c] = X[c].fillna(0.0)

    # 2) Coverage bucket categorical → ordinal numeric
    # adjust mapping if your buckets differ
    BUCKET_MAP = {
        "none": 0.0,
        "low": 1.0,
        "med": 2.0,
        "medium": 2.0,
        "high": 3.0,
        "unknown": 0.0,
        "nan": 0.0,
        "": 0.0,
    }
    BUCKET_COLS = {"d10_pairings_cov_bucket", "d20_pairings_cov_bucket"}
    for c in (BUCKET_COLS & set(feats)):
        s = X[c].astype(str).str.strip().str.lower()
        X[c] = s.map(BUCKET_MAP).fillna(0.0)

    # 3) Everything else must be numeric
    remaining = [c for c in feats if c not in BOOL_COLS and c not in BUCKET_COLS]
    for c in remaining:
        X[c] = pd.to_numeric(X[c], errors="coerce")

    X = (
        X.replace([np.inf, -np.inf], np.nan)
         .fillna(0.0)
         .astype(float)
    )
    return X

def load_model(model_root: Path, name: str):
    """
    Load the LightGBM model artifact.

    Supports two layouts:
      A) model_root / name / lgbm.joblib
      B) model_root / <something> / metadata.json  (name points at metadata.json)
         and model lives next to it: <that_dir>/lgbm.joblib
    """

    # Case B: caller passed a metadata path (relative or absolute)
    # e.g. name = "sog_player_denali_pairings_ordinal_v1__no_shiftcounts/ge_2/metadata.json"
    candidate = Path(name)
    if candidate.suffix == ".json" and candidate.name == "metadata.json":
        meta_path = candidate if candidate.is_absolute() else (model_root / candidate)
        model_path = meta_path.parent / "lgbm.joblib"
        if not model_path.exists():
            raise SystemExit(f"Missing model file next to metadata.json: {model_path}")
        return joblib.load(model_path)

    # Case A: original behavior
    model_path = model_root / name / "lgbm.joblib"
    if not model_path.exists():
        raise SystemExit(f"Missing model file: {model_path}")
    return joblib.load(model_path)

def predict_prob_1(model, X: pd.DataFrame) -> np.ndarray:
    # sklearn-style
    if hasattr(model, "predict_proba"):
        p = model.predict_proba(X)[:, 1]
        return np.asarray(p, dtype=float)

    # raw Booster style (already prob for binary objective)
    p = model.predict(X)
    p = np.asarray(p, dtype=float)

    # If a model ever outputs logits by mistake, clamp sanity later.
    return p

def _ensure_required_feature_defaults(df: pd.DataFrame, feats: list[str], *, in_path: Path) -> pd.DataFrame:
    """
    Ensure model-required feature columns exist and are non-null when the slate exporter
    doesn't provide them. This is NOT "alias everything"; it's a tight, explicit policy
    for a handful of known-required flags/cols.

    Returns df (mutated) for convenience.
    """
    # Only touch columns the model actually requires.
    required = set(feats)

    # 1) Boolean-ish flags
    #    - Core flags: safe default = 0 (explicit)
    #    - Pairings availability: contract features -> NO defaults (coerce only)

    # 1a) Core flags (allowed to default)
    core_bool_defaults = {
        "is_home": 0,
        "b2b_flag": 0,
        "hot_last5_flag": 0,
    }

    for col, default in core_bool_defaults.items():
        if col in required:
            if col not in df.columns:
                df[col] = default
            else:
                df[col] = (
                    df[col]
                    .replace(
                        {
                            True: 1,
                            False: 0,
                            "t": 1,
                            "f": 0,
                            "true": 1,
                            "false": 0,
                            "True": 1,
                            "False": 0,
                        }
                    )
                    .fillna(default)
                )

    # 1b) Pairings availability (NO defaulting; upstream contract)
    pairings_avail_cols = ["d10_pairings_available", "d20_pairings_available"]
    for col in pairings_avail_cols:
        if col in required:
            if col not in df.columns:
                raise AssertionError(f"[contract] missing required column: {col}")
            df[col] = (
                df[col]
                .replace(
                    {
                        True: 1,
                        False: 0,
                        "t": 1,
                        "f": 0,
                        "true": 1,
                        "false": 0,
                        "True": 1,
                        "False": 0,
                    }
                )
            )
            # coerce to numeric, but DO NOT fill NaNs
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 2) Season manpower TOI features (if model requires them but exporter omitted): safe default = 0.0
    float_zero_defaults = [
        "season_5on4_icetime_per_game",
        "season_4on5_icetime_per_game",
    ]
    for col in float_zero_defaults:
        if col in required:
            if col not in df.columns:
                df[col] = 0.0
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # 3) Coverage rates / buckets: DO NOT default.
    #    These are upstream contract features. If missing/null, sanity should fail.
    cov_rate_cols = [
        "d10_shiftcharts_coverage_rate",
        "d20_shiftcharts_coverage_rate",
    ]
    for col in cov_rate_cols:
        if col in required:
            if col not in df.columns:
                raise AssertionError(f"[contract] missing required column: {col}")
            df[col] = pd.to_numeric(df[col], errors="coerce")  # no fillna

    cov_bucket_cols = [
        "d10_pairings_cov_bucket",
        "d20_pairings_cov_bucket",
    ]
    for col in cov_bucket_cols:
        if col in required:
            if col not in df.columns:
                raise AssertionError(f"[contract] missing required column: {col}")
            # keep as numeric; cast only after coercion; no fillna
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def main() -> None:

    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--feature-meta", dest="feature_meta", default=DEFAULT_FEATURE_META)
    ap.add_argument("--model-root", dest="model_root", default=DEFAULT_MODEL_ROOT)
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    model_root = Path(args.model_root)

    df = pd.read_csv(in_path)
    df = _fill_missing_pairings_cov_cols(df)
    df = _alias_sog_count_cols(df)

    feats = load_features_list(args.feature_meta)

    df = _ensure_required_feature_defaults(df, feats, in_path=in_path)  # <-- add this line
    _quick_feature_sanity_check(df, feats, in_path=in_path)         # <-- then this

    X = prep_X(df, feats)

    # Optional: only if those columns exist (never crash the scorer for logging)
    for c in ["num_sog_last10", "num_event_last10", "team_num_sog_last10", "team_num_event_last10"]:
        if c in df.columns:
            print(f"[alias check] {c} null_frac={df[c].isna().mean():.3f}")

    X = prep_X(df, feats)


    # Load 3 threshold models
    print(f"[ordinal scorer] model_root={model_root}")

    for name in ("ge_2", "ge_3", "ge_4"):
        p = model_root / name / "lgbm.joblib"
        print(f"[ordinal scorer] expecting: {p} exists={p.exists()}")

    m2 = load_model(model_root, "ge_2")
    m3 = load_model(model_root, "ge_3")
    m4 = load_model(model_root, "ge_4")

    # Predict P(SOG >= k)
    p_ge2 = predict_prob_1(m2, X)
    p_ge3 = predict_prob_1(m3, X)
    p_ge4 = predict_prob_1(m4, X)

    # Wide outputs (match your existing naming)
    out = pd.DataFrame()
    # keep IDs if present (your pregame csv usually has these)
    for c in ["player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date", "season"]:
        if c in df.columns:
            out[c] = df[c]

    # --- enforce ordinal monotonicity (separate threshold models can violate it) ---
    p_ge2 = np.clip(p_ge2, 0.0, 1.0)
    p_ge3 = np.clip(p_ge3, 0.0, 1.0)
    p_ge4 = np.clip(p_ge4, 0.0, 1.0)

    # Ensure: P(>=2) >= P(>=3) >= P(>=4)
    p_ge3 = np.minimum(p_ge3, p_ge2)
    p_ge4 = np.minimum(p_ge4, p_ge3)
    # ---------------------------------------------------------------------------
    out["p_over_1_5"] = p_ge2.astype(float)
    out["p_over_2_5"] = p_ge3.astype(float)
    out["p_over_3_5"] = p_ge4.astype(float)

    # Optional: derived bucket probs (useful for debugging / future EV calc)
    # P(0-1) = 1 - P>=2
    # P(2)   = P>=2 - P>=3
    # P(3)   = P>=3 - P>=4
    # P(4+)  = P>=4
    out["p_0_1"] = (1.0 - out["p_over_1_5"]).clip(0, 1)
    out["p_2"]   = (out["p_over_1_5"] - out["p_over_2_5"]).clip(0, 1)
    out["p_3"]   = (out["p_over_2_5"] - out["p_over_3_5"]).clip(0, 1)
    out["p_4p"]  = out["p_over_3_5"].clip(0, 1)

    # Monotone sanity check (should be 0 violations)
    v12 = (out["p_over_1_5"] < out["p_over_2_5"]).sum()
    v23 = (out["p_over_2_5"] < out["p_over_3_5"]).sum()
    print(f"[ordinal scorer] monotone violations: 1.5<2.5={int(v12)}  2.5<3.5={int(v23)}")
    print(f"[ordinal scorer] rows: {len(out)}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"[ordinal scorer] wrote: {out_path}")

if __name__ == "__main__":
    main()
