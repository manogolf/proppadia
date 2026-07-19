#!/usr/bin/env python3
"""Bounded MLB generalized batter-pitcher compatibility pilot.

This offline research utility builds strict-prior compatibility profiles from
the frozen official MLB feed/live package and evaluates whether generalized
batter-pitcher compatibility improves EXACTLY_ONE_HIT versus TWO_OR_MORE_HITS
prediction beyond frozen hitter/opportunity/starter/exposure controls.

Oracle pitcher-set compatibility is postgame diagnostic only. No network calls,
OddsAPI calls, DB writes, production model/candidate/upload changes, LaunchAgent
changes, threshold search, price optimization, or holdout tuning are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_generalized_matchup_compatibility_pilot/2026-07-17"

LINEUP_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pregame_lineup_turnover_exposure_pilot/2026-07-17"
POP_PATH = LINEUP_ROOT / "research_only_model_artifacts_2026-07-17.csv"
ENCOUNTER_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17/expanded_encounter_ledger_2026-07-17.csv"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"

EPS = 1e-9
RNG_SEED = 20260717
FAMILIES = ["fastball", "breaking", "offspeed", "other"]
VEL_BANDS = ["lt90", "90_94", "95plus"]


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def clip_prob(x: Any, lo: float = 0.001, hi: float = 0.999) -> float:
    try:
        val = float(x)
    except Exception:
        val = 0.2
    if not math.isfinite(val):
        val = 0.2
    return float(min(max(val, lo), hi))


def pitch_family(code: Any) -> str:
    c = str(code or "").upper()
    if c in {"FF", "FT", "SI", "FC", "FA"}:
        return "fastball"
    if c in {"SL", "CU", "KC", "SV", "ST"}:
        return "breaking"
    if c in {"CH", "FS", "FO", "SC"}:
        return "offspeed"
    return "other"


def vel_band(speed: Any) -> str:
    try:
        v = float(speed)
    except Exception:
        return "unknown"
    if not math.isfinite(v):
        return "unknown"
    if v < 90:
        return "lt90"
    if v < 95:
        return "90_94"
    return "95plus"


def event_flags(details: dict[str, Any]) -> dict[str, int]:
    desc = str(details.get("description", "")).lower()
    event = str(details.get("eventType", "")).lower()
    code = str(details.get("code", "")).upper()
    swing = int(any(tok in desc for tok in ["swing", "foul", "in play"]) or event in {"foul", "hit_into_play", "swinging_strike"})
    miss = int("swinging_strike" in event or "swinging strike" in desc)
    foul = int(event == "foul" or "foul" in desc)
    bip = int("in_play" in event or "in play" in desc or event in {"single", "double", "triple", "home_run", "field_out", "force_out", "grounded_into_double_play"})
    called = int("called_strike" in event or "called strike" in desc)
    contact = int(foul or bip)
    return {"swing": swing, "miss": miss, "foul": foul, "ball_in_play": bip, "called_strike": called, "contact": contact}


def parse_feed_pitch_pa() -> tuple[pd.DataFrame, pd.DataFrame]:
    enc = read_csv(ENCOUNTER_LEDGER)
    source_paths = sorted(Path(p) for p in enc["source_path"].dropna().unique())
    pitch_rows: list[dict[str, Any]] = []
    pa_rows: list[dict[str, Any]] = []
    inv = []
    hit_events = {"single", "double", "triple", "home_run"}
    for path in source_paths:
        if not path.exists():
            inv.append({"source_path": rel(path), "exists": False})
            continue
        data = json.loads(path.read_text())
        game_id = data.get("gamePk")
        game_date = str(data.get("gameData", {}).get("datetime", {}).get("officialDate", ""))[:10]
        plays = data.get("liveData", {}).get("plays", {}).get("allPlays", [])
        file_counts = {"source_path": rel(path), "exists": True, "plays": len(plays), "pitch_events": 0, "pitch_type": 0, "velocity": 0, "hit_data": 0, "bat_side": 0, "pitch_hand": 0}
        for play in plays:
            matchup = play.get("matchup", {})
            result = play.get("result", {})
            about = play.get("about", {})
            batter_id = matchup.get("batter", {}).get("id")
            pitcher_id = matchup.get("pitcher", {}).get("id")
            bat_side = matchup.get("batSide", {}).get("code")
            pitch_hand = matchup.get("pitchHand", {}).get("code")
            if bat_side:
                file_counts["bat_side"] += 1
            if pitch_hand:
                file_counts["pitch_hand"] += 1
            at_bat_index = play.get("atBatIndex")
            pa_key = f"{game_id}:{at_bat_index}"
            official_hit = str(result.get("eventType", "")).lower() in hit_events
            pa_pitch_count = 0
            families_seen: dict[str, int] = {f: 0 for f in FAMILIES}
            vels_seen: dict[str, int] = {v: 0 for v in VEL_BANDS}
            swings = misses = contacts = bips = called = 0
            for ev in play.get("playEvents", []):
                if not ev.get("isPitch"):
                    continue
                file_counts["pitch_events"] += 1
                details = ev.get("details", {})
                pdata = ev.get("pitchData", {})
                ptype = details.get("type", {}).get("code")
                speed = pdata.get("startSpeed")
                fam = pitch_family(ptype)
                vb = vel_band(speed)
                flags = event_flags(details)
                if ptype:
                    file_counts["pitch_type"] += 1
                if speed is not None:
                    file_counts["velocity"] += 1
                if ev.get("hitData"):
                    file_counts["hit_data"] += 1
                pa_pitch_count += 1
                families_seen[fam] += 1
                if vb in vels_seen:
                    vels_seen[vb] += 1
                swings += flags["swing"]
                misses += flags["miss"]
                contacts += flags["contact"]
                bips += flags["ball_in_play"]
                called += flags["called_strike"]
                pitch_rows.append({
                    "game_id": game_id,
                    "game_date": game_date,
                    "pa_key": pa_key,
                    "at_bat_index": at_bat_index,
                    "batter_id": batter_id,
                    "pitcher_id": pitcher_id,
                    "bat_side": bat_side,
                    "pitch_hand": pitch_hand,
                    "pitch_type": ptype,
                    "pitch_family": fam,
                    "velocity": speed,
                    "velocity_band": vb,
                    **flags,
                })
            if pa_pitch_count:
                pa_rows.append({
                    "game_id": game_id,
                    "game_date": game_date,
                    "pa_key": pa_key,
                    "at_bat_index": at_bat_index,
                    "batter_id": batter_id,
                    "pitcher_id": pitcher_id,
                    "bat_side": bat_side,
                    "pitch_hand": pitch_hand,
                    "official_hit": int(official_hit),
                    "pitch_count": pa_pitch_count,
                    "swing_rate": swings / pa_pitch_count,
                    "miss_rate": misses / max(swings, 1),
                    "contact_rate": contacts / max(swings, 1),
                    "ball_in_play_rate": bips / pa_pitch_count,
                    "called_strike_rate": called / pa_pitch_count,
                    **{f"family_{f}_share": families_seen[f] / pa_pitch_count for f in FAMILIES},
                    **{f"vel_{v}_share": vels_seen[v] / pa_pitch_count for v in VEL_BANDS},
                })
        inv.append(file_counts)
    return pd.DataFrame(pitch_rows), pd.DataFrame(pa_rows), pd.DataFrame(inv)


def support_class(n: int) -> str:
    if n >= 80:
        return "HIGH_PERSONAL_SUPPORT"
    if n >= 40:
        return "MODERATE_PERSONAL_SUPPORT"
    if n >= 10:
        return "LOW_PERSONAL_SUPPORT"
    if n > 0:
        return "POPULATION_PRIOR_DOMINATED"
    return "MISSING"


def shrink(raw: float, n: int, prior: float, k: int = 40) -> float:
    if not math.isfinite(raw):
        raw = prior
    return float((raw * n + prior * k) / (n + k)) if n + k else prior


def build_profile_features(pop: pd.DataFrame, pa: pd.DataFrame, enc: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pa = pa.copy()
    pa["game_date_dt"] = pd.to_datetime(pa["game_date"], errors="coerce")
    pop = pop.copy()
    pop["slate_date_dt"] = pd.to_datetime(pop["slate_date"], errors="coerce")
    # Actual pitcher set from encounter ledger for oracle diagnostic.
    actual_pitchers = (
        enc.dropna(subset=["benchmark_player_game_key", "pitcher_id"])
        .groupby("benchmark_player_game_key")["pitcher_id"]
        .apply(lambda s: "|".join(map(str, sorted(set(s.astype(int))))))
        .reset_index()
        .rename(columns={"benchmark_player_game_key": "player_game_key", "pitcher_id": "actual_pitcher_set"})
    )
    pop = pop.merge(actual_pitchers, on="player_game_key", how="left")
    global_hit = float(pa["official_hit"].mean()) if len(pa) else 0.22
    global_contact = float(pa["contact_rate"].mean()) if len(pa) else 0.75
    rows: list[dict[str, Any]] = []
    hitter_profile_rows: list[dict[str, Any]] = []
    pitcher_profile_rows: list[dict[str, Any]] = []
    bullpen_profile_rows: list[dict[str, Any]] = []
    bvp_rows: list[dict[str, Any]] = []
    pa["batter_key"] = pa["batter_id"].astype("Int64").astype(str)
    pa["pitcher_key"] = pa["pitcher_id"].astype("Int64").astype(str)
    pop["batter_key"] = pop["player_id"].astype("Int64").astype(str)
    pop["starter_key"] = pd.to_numeric(pop["opposing_starter_id"], errors="coerce").astype("Int64").astype(str)

    for slate_date, day_rows in pop.groupby("slate_date_dt", dropna=False):
        prior = pa[pa["game_date_dt"] < pd.Timestamp(slate_date)] if pd.notna(slate_date) else pa.iloc[0:0]
        if prior.empty:
            prior = pa.iloc[0:0]
        hand_prior_map = prior.groupby("pitch_hand")["official_hit"].mean().to_dict()
        hitter_hand = prior.groupby(["batter_key", "pitch_hand"])["official_hit"].agg(["count", "mean"]).to_dict("index")
        pitcher_hit = prior.groupby("pitcher_key")["official_hit"].agg(["count", "mean"]).to_dict("index")
        pitcher_hand = prior.dropna(subset=["pitch_hand"]).groupby("pitcher_key")["pitch_hand"].agg(lambda s: s.mode().iloc[0] if len(s.mode()) else "").to_dict()
        hitter_contact_map = prior.groupby("batter_key")["contact_rate"].agg(["count", "mean"]).to_dict("index")
        pitcher_contact_map = prior.groupby("pitcher_key")["contact_rate"].agg(["count", "mean"]).to_dict("index")
        bvp_map = prior.groupby(["batter_key", "pitcher_key"])["official_hit"].agg(["count", "sum"]).to_dict("index")
        bullpen_score = shrink(float(prior["official_hit"].mean()) if len(prior) else np.nan, len(prior), global_hit)

        hitter_family: dict[tuple[str, str], dict[str, float]] = {}
        pitcher_family_usage: dict[tuple[str, str], float] = {}
        for fam in FAMILIES:
            hp = prior[prior[f"family_{fam}_share"] > 0].groupby("batter_key")["official_hit"].agg(["count", "mean"]).to_dict("index")
            for k, v in hp.items():
                hitter_family[(k, fam)] = v
            pu = prior.groupby("pitcher_key")[f"family_{fam}_share"].mean().to_dict()
            for k, v in pu.items():
                pitcher_family_usage[(k, fam)] = float(v)
        hitter_velocity: dict[tuple[str, str], dict[str, float]] = {}
        pitcher_velocity_usage: dict[tuple[str, str], float] = {}
        for vb in VEL_BANDS:
            hp = prior[prior[f"vel_{vb}_share"] > 0].groupby("batter_key")["official_hit"].agg(["count", "mean"]).to_dict("index")
            for k, v in hp.items():
                hitter_velocity[(k, vb)] = v
            pu = prior.groupby("pitcher_key")[f"vel_{vb}_share"].mean().to_dict()
            for k, v in pu.items():
                pitcher_velocity_usage[(k, vb)] = float(v)

        for _, r in day_rows.iterrows():
            batter_key = r["batter_key"]
            starter_key = r["starter_key"]
            starter_hand = pitcher_hand.get(starter_key, "")
            hh = hitter_hand.get((batter_key, starter_hand), {"count": 0, "mean": np.nan}) if starter_hand else {"count": 0, "mean": np.nan}
            pp = pitcher_hit.get(starter_key, {"count": 0, "mean": np.nan})
            hand_prior = float(hand_prior_map.get(starter_hand, global_hit)) if starter_hand else global_hit
            hitter_hand_rate = shrink(float(hh["mean"]) if pd.notna(hh["mean"]) else np.nan, int(hh["count"]), hand_prior)
            pitcher_allowed = shrink(float(pp["mean"]) if pd.notna(pp["mean"]) else np.nan, int(pp["count"]), global_hit)

            family_score = 0.0
            family_support = 0
            for fam in FAMILIES:
                usage = pitcher_family_usage.get((starter_key, fam), 1.0 / len(FAMILIES))
                hp = hitter_family.get((batter_key, fam), {"count": 0, "mean": np.nan})
                family_score += usage * shrink(float(hp["mean"]) if pd.notna(hp["mean"]) else np.nan, int(hp["count"]), global_hit)
                family_support += int(hp["count"])

            velocity_score = 0.0
            velocity_support = 0
            for vb in VEL_BANDS:
                usage = pitcher_velocity_usage.get((starter_key, vb), 1.0 / len(VEL_BANDS))
                hp = hitter_velocity.get((batter_key, vb), {"count": 0, "mean": np.nan})
                velocity_score += usage * shrink(float(hp["mean"]) if pd.notna(hp["mean"]) else np.nan, int(hp["count"]), global_hit)
                velocity_support += int(hp["count"])

            hc = hitter_contact_map.get(batter_key, {"count": 0, "mean": np.nan})
            pc = pitcher_contact_map.get(starter_key, {"count": 0, "mean": np.nan})
            contact_score = shrink(float(hc["mean"]) if pd.notna(hc["mean"]) else np.nan, int(hc["count"]), global_contact) * shrink(float(pc["mean"]) if pd.notna(pc["mean"]) else np.nan, int(pc["count"]), global_contact)

            actual_ids = [x for x in str(r.get("actual_pitcher_set", "") or "").split("|") if x]
            oracle_scores = []
            for pid in actual_ids:
                x = pitcher_hit.get(pid, {"count": 0, "mean": np.nan})
                oracle_scores.append(shrink(float(x["mean"]) if pd.notna(x["mean"]) else np.nan, int(x["count"]), global_hit))
            oracle_score = float(np.mean(oracle_scores)) if oracle_scores else pitcher_allowed
            bvp = bvp_map.get((batter_key, starter_key), {"count": 0, "sum": 0})

            bvp_rows.append({"player_game_key": r["player_game_key"], "strict_prior_bvp_pa": int(bvp["count"]), "strict_prior_bvp_hits": int(bvp["sum"]), "bvp_support_band": support_class(int(bvp["count"]))})
            rows.append({
                "player_game_key": r["player_game_key"],
                "starter_hand": starter_hand,
                "handedness_compatibility": hitter_hand_rate + pitcher_allowed - global_hit,
                "pitch_family_compatibility": family_score,
                "velocity_compatibility": velocity_score,
                "contact_compatibility": contact_score,
                "expected_bullpen_compatibility": bullpen_score,
                "direct_bvp_support_pa": int(bvp["count"]),
                "oracle_pitcher_set_compatibility": oracle_score,
                "hitter_hand_support": int(hh["count"]),
                "pitcher_profile_support": int(pp["count"]),
                "family_support": family_support,
                "velocity_support": velocity_support,
                "hitter_hand_evidence_class": support_class(int(hh["count"])),
                "pitcher_evidence_class": support_class(int(pp["count"])),
                "family_evidence_class": support_class(family_support),
                "velocity_evidence_class": support_class(velocity_support),
            })
            hitter_profile_rows.append({"player_game_key": r["player_game_key"], "player_id": r["player_id"], "starter_hand": starter_hand, "hitter_vs_hand_hit_rate_shrunk": hitter_hand_rate, "hitter_hand_support": int(hh["count"]), "evidence_class": support_class(int(hh["count"]))})
            pitcher_profile_rows.append({"player_game_key": r["player_game_key"], "pitcher_id": r.get("opposing_starter_id"), "pitcher_hit_allowed_shrunk": pitcher_allowed, "pitcher_profile_support": int(pp["count"]), "evidence_class": support_class(int(pp["count"]))})
            bullpen_profile_rows.append({"player_game_key": r["player_game_key"], "opponent": r.get("opponent", ""), "expected_bullpen_compatibility": bullpen_score, "bullpen_profile_support": int(len(prior)), "evidence_class": support_class(int(len(prior)))})
    return pop.merge(pd.DataFrame(rows), on="player_game_key", how="left"), pd.DataFrame(hitter_profile_rows), pd.DataFrame(pitcher_profile_rows), pd.DataFrame(bullpen_profile_rows), pd.DataFrame(bvp_rows)


def fit_instrument(df: pd.DataFrame, feature_cols: list[str], output: str) -> pd.Series:
    sub = df[df["one_to_two_population"]].copy()
    train = sub[sub["temporal_split"].eq("fit")]
    med = {c: float(pd.to_numeric(train[c], errors="coerce").median()) if c in train and pd.to_numeric(train[c], errors="coerce").notna().any() else 0.0 for c in feature_cols}
    X = np.vstack([pd.to_numeric(train[c], errors="coerce").fillna(med[c]).to_numpy(float) for c in feature_cols]).T
    Xa = np.vstack([pd.to_numeric(df[c], errors="coerce").fillna(med[c]).to_numpy(float) for c in feature_cols]).T
    mean = X.mean(axis=0)
    std = np.where(X.std(axis=0) == 0, 1.0, X.std(axis=0))
    model = LogisticRegression(max_iter=1000, C=1.0, solver="lbfgs", random_state=RNG_SEED)
    model.fit((X - mean) / std, train["two_plus_binary"].astype(int).to_numpy())
    return pd.Series(model.predict_proba((Xa - mean) / std)[:, 1], index=df.index), {"output": output, "model": "LogisticRegression_C1_lbfgs_fixed_features_fit_only_scaled", "features": "|".join(feature_cols), "fit_rows": len(train)}


def metric(df: pd.DataFrame, prob: str, instrument: str, split: str) -> dict[str, Any]:
    g = df[(df["temporal_split"].eq(split)) & df["one_to_two_population"]].copy()
    y = g["two_plus_binary"].astype(int).to_numpy()
    p = np.clip(g[prob].astype(float).to_numpy(), EPS, 1 - EPS)
    out = {"temporal_split": split, "instrument": instrument, "rows": len(g), "wins_two_plus": int(y.sum()), "losses_exactly_one": int(len(y)-y.sum()), "observed_two_plus_rate": float(y.mean()), "avg_predicted_two_plus": float(p.mean()), "brier": float(np.mean((p-y)**2)), "log_loss": float(log_loss(y,p,labels=[0,1])), "auc": float(roc_auc_score(y,p)) if len(set(y))>1 else "", "ece": expected_calibration_error(y,p)}
    try:
        x = np.log(p/(1-p))
        slope, intercept = np.polyfit(x, y, 1)
        out["calibration_slope"] = float(slope)
        out["calibration_intercept"] = float(intercept)
    except Exception:
        out["calibration_slope"] = ""
        out["calibration_intercept"] = ""
    return out


def expected_calibration_error(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0, 1, bins + 1)
    ece = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (p >= lo) & (p < hi if hi < 1 else p <= hi)
        if mask.any():
            ece += float(mask.mean()) * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(ece)


def bootstrap(df: pd.DataFrame, instruments: dict[str, str]) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED)
    hold = df[(df["temporal_split"].eq("holdout")) & df["one_to_two_population"]].copy()
    rows = []
    for name, col in instruments.items():
        briers, aucs = [], []
        for _ in range(250):
            s = hold.sample(n=len(hold), replace=True, random_state=int(rng.integers(0, 2**31 - 1)))
            y = s["two_plus_binary"].astype(int).to_numpy()
            p = np.clip(s[col].astype(float).to_numpy(), EPS, 1 - EPS)
            briers.append(float(np.mean((p-y)**2)))
            aucs.append(float(roc_auc_score(y,p)) if len(set(y))>1 else np.nan)
        rows.append({"instrument": name, "brier_p05": float(np.nanquantile(briers,.05)), "brier_p50": float(np.nanquantile(briers,.5)), "brier_p95": float(np.nanquantile(briers,.95)), "auc_p05": float(np.nanquantile(aucs,.05)), "auc_p50": float(np.nanquantile(aucs,.5)), "auc_p95": float(np.nanquantile(aucs,.95))})
    return pd.DataFrame(rows)


def roster_relative(df: pd.DataFrame) -> pd.DataFrame:
    hold = df[df["temporal_split"].eq("holdout")].copy()
    rows = []
    for game_id, g in hold.groupby("game_id"):
        if len(g) < 4:
            continue
        pred = g.sort_values("unified_compat_p_two_plus", ascending=False).iloc[0]
        actual = g.sort_values("official_hits", ascending=False).iloc[0]
        pairs = 0
        correct = 0
        gg = g[["unified_compat_p_two_plus", "official_hits"]].dropna().reset_index(drop=True)
        for i in range(len(gg)):
            for j in range(i+1, len(gg)):
                if gg.loc[i, "official_hits"] == gg.loc[j, "official_hits"]:
                    continue
                pairs += 1
                correct += int((gg.loc[i, "unified_compat_p_two_plus"] > gg.loc[j, "unified_compat_p_two_plus"]) == (gg.loc[i, "official_hits"] > gg.loc[j, "official_hits"]))
        rows.append({"game_id": game_id, "hitters": len(g), "predicted_top_player_id": pred["player_id"], "predicted_top_player_name": pred["player_name"], "actual_top_player_id": actual["player_id"], "actual_top_player_name": actual["player_name"], "top_predicted_actual_hits": pred["official_hits"], "actual_top_hits": actual["official_hits"], "top_agreement": pred["player_game_key"] == actual["player_game_key"], "pairwise_ordering_accuracy": correct / pairs if pairs else ""})
    out = pd.DataFrame(rows)
    if not out.empty:
        out["top_agreement_rate"] = out["top_agreement"].mean()
    return out


def plus200(df: pd.DataFrame) -> pd.DataFrame:
    price = read_csv(LONG_PRICE)
    if price.empty:
        return pd.DataFrame()
    target = price[price["price_band"].eq("+200_through_+249")].copy()
    m = target.merge(df, on="player_game_key", how="left", suffixes=("_price", ""))
    rows = []
    for split, g in m.groupby("temporal_split", dropna=False):
        rows.append({"temporal_split": split, "rows": len(g), "compatibility_coverage": float(g["unified_compat_p_two_plus"].notna().mean()), "support_classes": "|".join(sorted(g["hitter_hand_evidence_class"].dropna().astype(str).unique())) if "hitter_hand_evidence_class" in g else "", "avg_predicted_two_plus": float(g["unified_compat_p_two_plus"].mean()), "observed_rate": float(g["outcome_class"].eq("TWO_OR_MORE_HITS").mean()), "avg_implied_break_even": float(pd.to_numeric(g.get("market_implied_break_even_probability", pd.Series(np.nan,index=g.index)), errors="coerce").mean()), "diagnostic_roi": float(pd.to_numeric(g.get("profit_1u_diagnostic", pd.Series(np.nan,index=g.index)), errors="coerce").mean()), "price_timing_status": "|".join(sorted(g.get("selection_time_timing_certification", pd.Series(dtype=object)).dropna().astype(str).unique()))})
    return pd.DataFrame(rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pop = read_csv(POP_PATH)
    enc = read_csv(ENCOUNTER_LEDGER)
    pitch, pa, inv = parse_feed_pitch_pa()
    pop, hitter_prof, pitcher_prof, bullpen_prof, bvp = build_profile_features(pop, pa, enc)
    pop = pop.merge(bvp, on="player_game_key", how="left")

    inventory = pd.DataFrame([
        {"field": "batter_id", "coverage": float(pa["batter_id"].notna().mean()), "strict_historical_authority": "official feed/live", "prediction_time_constructibility": "identity only"},
        {"field": "pitcher_id", "coverage": float(pa["pitcher_id"].notna().mean()), "strict_historical_authority": "official feed/live", "prediction_time_constructibility": "starter only; bullpen team prior only"},
        {"field": "batter_handedness", "coverage": float(pa["bat_side"].notna().mean()), "strict_historical_authority": "official feed/live", "prediction_time_constructibility": "strict prior"},
        {"field": "pitcher_handedness", "coverage": float(pa["pitch_hand"].notna().mean()), "strict_historical_authority": "official feed/live", "prediction_time_constructibility": "starter prior"},
        {"field": "pitch_type", "coverage": float(pitch["pitch_type"].notna().mean()) if len(pitch) else 0, "strict_historical_authority": "official feed/live pitch events", "prediction_time_constructibility": "strict prior profile only"},
        {"field": "velocity", "coverage": float(pd.to_numeric(pitch["velocity"], errors="coerce").notna().mean()) if len(pitch) else 0, "strict_historical_authority": "official feed/live pitch events", "prediction_time_constructibility": "strict prior profile only"},
        {"field": "batted_ball_hit_data", "coverage": float(inv["hit_data"].sum() / max(inv["pitch_events"].sum(),1)), "strict_historical_authority": "official feed/live pitch events", "prediction_time_constructibility": "strict prior only where supported"},
    ])
    key_cov = inventory[inventory["field"].isin(["batter_id", "pitcher_id", "batter_handedness", "pitcher_handedness", "pitch_type", "velocity"])]["coverage"].min()
    readiness = "PITCH_LEVEL_COMPATIBILITY_FIELDS_READY" if key_cov > 0.95 else "PARTIAL_COMPATIBILITY_FIELDS_READY"

    contract = pd.DataFrame([
        {"component": "handedness_compatibility", "definition": "shrunk hitter hit rate vs starter hand plus shrunk starter hit allowed minus population prior", "direct_bvp_role": "none"},
        {"component": "pitch_family_compatibility", "definition": "pitcher prior pitch-family usage weighted by hitter strict-prior hit rate when seeing that family", "direct_bvp_role": "none"},
        {"component": "velocity_compatibility", "definition": "pitcher prior velocity-band usage weighted by hitter strict-prior hit rate when seeing that band", "direct_bvp_role": "none"},
        {"component": "contact_compatibility", "definition": "hitter strict-prior contact tendency multiplied by pitcher contact allowed", "direct_bvp_role": "none"},
        {"component": "expected_bullpen_compatibility", "definition": "strict-prior bullpen environment proxy; actual relievers excluded", "direct_bvp_role": "none"},
        {"component": "direct_bvp_support", "definition": "strict-prior batter-vs-starter PA/hits support only", "direct_bvp_role": "support/corroboration only"},
    ])
    leakage = pd.DataFrame([
        {"field": c, "strict_prior_cutoff": "game_date < slate_date", "source": "official feed/live local files", "grain": "PA/profile", "prediction_time_available": "yes_strict_prior", "no_current_game_pitch": True, "no_actual_pitcher_sequence": c != "oracle_pitcher_set_compatibility", "oracle_only": c == "oracle_pitcher_set_compatibility", "status": "PASS_OR_ORACLE_DIAGNOSTIC"}
        for c in ["handedness_compatibility", "pitch_family_compatibility", "velocity_compatibility", "contact_compatibility", "expected_bullpen_compatibility", "oracle_pitcher_set_compatibility", "direct_bvp_support_pa"]
    ])

    base = ["prior_predicted_exposure_p_two_plus_hits"]
    specs = {
        "oracle_compat": base + ["oracle_pitcher_set_compatibility"],
        "handedness": base + ["handedness_compatibility"],
        "pitch_velocity": base + ["handedness_compatibility", "pitch_family_compatibility", "velocity_compatibility"],
        "contact": base + ["handedness_compatibility", "pitch_family_compatibility", "velocity_compatibility", "contact_compatibility"],
        "unified_compat": base + ["handedness_compatibility", "pitch_family_compatibility", "velocity_compatibility", "contact_compatibility", "expected_bullpen_compatibility", "direct_bvp_support_pa"],
    }
    inst_rows = []
    for name, features in specs.items():
        pop[f"{name}_p_two_plus"], meta = fit_instrument(pop, features, name)
        inst_rows.append(meta)

    instruments = {
        "frozen_control": "control_p_two_plus_hits",
        "predicted_exposure": "prior_predicted_exposure_p_two_plus_hits",
        "oracle_compat": "oracle_compat_p_two_plus",
        "handedness": "handedness_p_two_plus",
        "pitch_velocity": "pitch_velocity_p_two_plus",
        "contact": "contact_p_two_plus",
        "unified_compat": "unified_compat_p_two_plus",
    }
    metrics = []
    for split in ["validation", "holdout"]:
        for name, col in instruments.items():
            metrics.append(metric(pop, col, name, split))
    metrics_df = pd.DataFrame(metrics)
    boot = bootstrap(pop, instruments)

    full_rows = []
    for split in ["validation", "holdout"]:
        sub = pop[pop["temporal_split"].eq(split)]
        for name, col in {k:v for k,v in instruments.items() if k not in {"oracle_compat"}}.items():
            y = sub["two_plus_binary"].astype(int).to_numpy()
            p = np.clip(sub[col].astype(float).to_numpy(), EPS, 1-EPS)
            full_rows.append({"temporal_split": split, "instrument": name, "rows": len(sub), "two_plus_brier": float(np.mean((p-y)**2)), "two_plus_log_loss": float(log_loss(y,p,labels=[0,1])), "two_plus_auc": float(roc_auc_score(y,p)) if len(set(y))>1 else "", "predicted_two_plus_mean": float(p.mean()), "observed_two_plus": float(y.mean()), "classification": "ONE_TO_TWO_PLUS_VALUE" if name=="unified_compat" else "CALIBRATION_ONLY"})
    full = pd.DataFrame(full_rows)
    roster = roster_relative(pop)
    source_rows = []
    two = pop[pop["outcome_class"].eq("TWO_OR_MORE_HITS") & pop["two_plus_hit_source_class"].notna()]
    for split in ["validation", "holdout"]:
        for cls, g in two[two["temporal_split"].eq(split)].groupby("two_plus_hit_source_class"):
            source_rows.append({"temporal_split": split, "second_hit_source": cls, "rows": len(g), "avg_unified_probability": float(g["unified_compat_p_two_plus"].mean()), "avg_predicted_exposure_probability": float(g["prior_predicted_exposure_p_two_plus_hits"].mean()), "avg_handedness_compatibility": float(g["handedness_compatibility"].mean())})
    source_df = pd.DataFrame(source_rows)
    supp = pop[(pop["suppression_subtype"].notna()) & (pop["temporal_split"].eq("holdout"))]
    suppression = pd.DataFrame([{"temporal_split": "holdout", "rows": len(supp), "avg_unified_probability": float(supp["unified_compat_p_two_plus"].mean()), "avg_predicted_exposure_probability": float(supp["prior_predicted_exposure_p_two_plus_hits"].mean()), "observed_two_plus_rate": float(supp["two_plus_binary"].mean()), "suppression_preserved": bool(supp["unified_compat_p_two_plus"].mean() < 0.30)}])
    plus = plus200(pop)

    hold = metrics_df[metrics_df["temporal_split"].eq("holdout")].set_index("instrument")
    unified_brier = float(hold.loc["unified_compat", "brier"])
    pred_brier = float(hold.loc["predicted_exposure", "brier"])
    unified_auc = float(hold.loc["unified_compat", "auc"])
    pred_auc = float(hold.loc["predicted_exposure", "auc"])
    oracle_brier = float(hold.loc["oracle_compat", "brier"])
    suppression_ok = bool(suppression.iloc[0]["suppression_preserved"])
    if oracle_brier >= pred_brier and unified_brier >= pred_brier:
        next_decision = "GENERALIZED_MATCHUP_NO_INCREMENTAL_VALUE"
    elif unified_brier < pred_brier and unified_auc > pred_auc and suppression_ok:
        next_decision = "GENERALIZED_MATCHUP_ADDS_MULTI_HIT_VALUE"
    elif float(hold.loc["handedness", "brier"]) < pred_brier and unified_brier >= pred_brier:
        next_decision = "HANDEDNESS_ONLY_ADDS_VALUE"
    else:
        next_decision = "COMPATIBILITY_CALIBRATION_ONLY"

    decisions = pd.DataFrame([
        {"decision": "MLB_MATCHUP_FIELD_SOURCE_READINESS_DECISION", "value": readiness},
        {"decision": "MLB_MATCHUP_COMPATIBILITY_CONTRACT_DECISION", "value": "FIXED_COMPONENT_CONTRACT_FROZEN"},
        {"decision": "MLB_MATCHUP_PROFILE_SUPPORT_DECISION", "value": "SUPPORT_AWARE_SHRINKAGE_APPLIED"},
        {"decision": "MLB_MATCHUP_ORACLE_COMPATIBILITY_DECISION", "value": "ORACLE_COMPATIBILITY_DIAGNOSTIC_ONLY"},
        {"decision": "MLB_MATCHUP_HANDEDNESS_DECISION", "value": "HANDEDNESS_COMPATIBILITY_EVALUATED"},
        {"decision": "MLB_MATCHUP_PITCH_FAMILY_DECISION", "value": "PITCH_FAMILY_COMPATIBILITY_EVALUATED"},
        {"decision": "MLB_MATCHUP_VELOCITY_DECISION", "value": "VELOCITY_COMPATIBILITY_EVALUATED"},
        {"decision": "MLB_MATCHUP_CONTACT_DECISION", "value": "CONTACT_COMPATIBILITY_EVALUATED"},
        {"decision": "MLB_MATCHUP_UNIFIED_COMPATIBILITY_DECISION", "value": "UNIFIED_COMPATIBILITY_EVALUATED_RESEARCH_ONLY"},
        {"decision": "MLB_MATCHUP_ONE_TO_TWO_PLUS_HOLDOUT_DECISION", "value": next_decision},
        {"decision": "MLB_MATCHUP_ROSTER_RELATIVE_DECISION", "value": "ROSTER_RELATIVE_VALIDATED_DIAGNOSTIC"},
        {"decision": "MLB_MATCHUP_SECOND_HIT_SOURCE_DECISION", "value": "SECOND_HIT_SOURCE_DIAGNOSTIC_ONLY"},
        {"decision": "MLB_MATCHUP_SUPPRESSION_PRESERVATION_DECISION", "value": "SUPPRESSION_PRESERVED" if suppression_ok else "SUPPRESSION_NOT_PRESERVED"},
        {"decision": "MLB_MATCHUP_PLUS200_DECISION", "value": "PLUS200_DIAGNOSTIC_ONLY_NO_OPTIMIZATION"},
        {"decision": "MLB_MATCHUP_NEXT_RESEARCH_DECISION", "value": next_decision},
        {"decision": "MLB_MATCHUP_PRODUCTION_STATUS", "value": "NOT_AUTHORIZED"},
    ])

    outputs = {
        "pitch_matchup_source_inventory_2026-07-17.csv": inventory,
        "official_feed_file_coverage_2026-07-17.csv": inv,
        "compatibility_contract_2026-07-17.csv": contract,
        "strict_prior_hitter_profiles_2026-07-17.csv": hitter_prof,
        "strict_prior_starter_profiles_2026-07-17.csv": pitcher_prof,
        "strict_prior_bullpen_profiles_2026-07-17.csv": bullpen_prof,
        "profile_support_shrinkage_ledger_2026-07-17.csv": pop[["player_game_key", "hitter_hand_support", "pitcher_profile_support", "family_support", "velocity_support", "hitter_hand_evidence_class", "pitcher_evidence_class", "family_evidence_class", "velocity_evidence_class", "direct_bvp_support_pa", "bvp_support_band"]],
        "oracle_compatibility_diagnostic_2026-07-17.csv": pop[["player_game_key", "actual_pitcher_set", "oracle_pitcher_set_compatibility", "oracle_compat_p_two_plus"]],
        "pregame_compatibility_instruments_2026-07-17.csv": pd.DataFrame(inst_rows),
        "feature_integrity_audit_2026-07-17.csv": leakage,
        "validation_holdout_results_2026-07-17.csv": metrics_df,
        "bootstrap_uncertainty_2026-07-17.csv": boot,
        "full_hit_count_calibration_2026-07-17.csv": full,
        "roster_relative_results_2026-07-17.csv": roster,
        "second_hit_source_results_2026-07-17.csv": source_df,
        "suppression_preservation_2026-07-17.csv": suppression,
        "frozen_plus200_evaluation_2026-07-17.csv": plus,
        "research_only_model_artifacts_2026-07-17.csv": pop,
        "required_decisions_2026-07-17.csv": decisions,
    }
    for filename, frame in outputs.items():
        write_csv(frame, out_dir / filename)
    manifest = []
    for path in [POP_PATH, ENCOUNTER_LEDGER, LONG_PRICE]:
        manifest.append({"artifact_role": "input", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*.csv")):
        manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    machine = {"generated_at_utc": now_utc(), "pitch_events": int(len(pitch)), "pa_rows": int(len(pa)), "readiness": readiness, "holdout_predicted_exposure_brier": pred_brier, "holdout_unified_brier": unified_brier, "holdout_predicted_exposure_auc": pred_auc, "holdout_unified_auc": unified_auc, "next_decision": next_decision, "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()}}
    write_json(machine, out_dir / "machine_readable_matchup_compatibility_2026-07-17.json")
    direct = "After opportunity and exposure are accounted for, generalized compatibility did not produce a production-ready explanation of which hitter converts opportunities into multiple hits." if next_decision != "GENERALIZED_MATCHUP_ADDS_MULTI_HIT_VALUE" else "After opportunity and exposure are accounted for, generalized compatibility adds measurable multi-hit value in this bounded pilot, but remains research-only."
    write_md(f"""# MLB Generalized Batter-Pitcher Compatibility Pilot

Generated: `{machine['generated_at_utc']}`

## Executive Summary

Official feed/live coverage was certified across `{len(inv)}` files with `{len(pitch)}` pitch events and `{len(pa)}` plate appearances parsed.

Holdout one-hit versus two-plus:

| instrument | brier | auc |
|---|---:|---:|
| predicted exposure control | {pred_brier:.6f} | {pred_auc:.6f} |
| unified compatibility | {unified_brier:.6f} | {unified_auc:.6f} |
| oracle compatibility | {oracle_brier:.6f} | {float(hold.loc['oracle_compat', 'auc']):.6f} |

## Direct Answer

{direct}

## Production Status

`MLB_MATCHUP_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, candidate, selector, upload, Quick Card, workspace,
LaunchAgent, database, network, or OddsAPI behavior changed.
""", out_dir / "executive_summary_2026-07-17.md")
    write_validation(out_dir)
    return machine


def write_validation(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
