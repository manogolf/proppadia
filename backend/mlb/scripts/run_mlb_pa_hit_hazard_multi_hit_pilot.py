#!/usr/bin/env python3
"""Bounded MLB plate-appearance hit-hazard multi-hit pilot.

This offline research utility decomposes projected plate appearances into
non-contact termination, ball-in-play opportunity, and hit-on-contact
probability. It evaluates whether PA-level hit hazards explain EXACTLY_ONE_HIT
versus TWO_OR_MORE_HITS better than aggregate exposure/compatibility controls.

Oracle diagnostics may use actual current-game BIP count, source exposure, or
contact quality as nondeployable upper bounds. Legitimate instruments use only
strict-prior profiles from completed earlier games.

No network calls, OddsAPI calls, DB writes, production model/candidate/upload
changes, LaunchAgent changes, threshold search, price optimization, or holdout
tuning are performed.
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
from sklearn.metrics import log_loss, roc_auc_score

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_pa_hit_hazard_multi_hit_pilot/2026-07-17"

MATCHUP_ROOT = ROOT / "artifacts/analysis/model_development/mlb_generalized_matchup_compatibility_pilot/2026-07-17"
POP_PATH = MATCHUP_ROOT / "research_only_model_artifacts_2026-07-17.csv"
ENCOUNTER_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17/expanded_encounter_ledger_2026-07-17.csv"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"

EPS = 1e-9
RNG_SEED = 20260717
HIT_EVENTS = {"single", "double", "triple", "home_run"}


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


def clip_prob(x: Any, lo: float = 0.001, hi: float = 0.999) -> float:
    try:
        val = float(x)
    except Exception:
        val = 0.2
    if not math.isfinite(val):
        val = 0.2
    return float(min(max(val, lo), hi))


def poisson_hit_dist(expected_pa: float, p_hit: float) -> tuple[float, float, float]:
    lam = max(float(expected_pa) * clip_prob(p_hit, 0.001, 0.65), 0.0001)
    p0 = math.exp(-lam)
    p1 = lam * p0
    p2 = max(0.0, 1.0 - p0 - p1)
    s = p0 + p1 + p2
    return p0 / s, p1 / s, p2 / s


def source_hit_dist(starter_pa: float, bullpen_pa: float, p_starter: float, p_bullpen: float) -> tuple[float, float, float]:
    lam = max(float(starter_pa) * clip_prob(p_starter, 0.001, 0.65) + float(bullpen_pa) * clip_prob(p_bullpen, 0.001, 0.65), 0.0001)
    p0 = math.exp(-lam)
    p1 = lam * p0
    p2 = max(0.0, 1.0 - p0 - p1)
    s = p0 + p1 + p2
    return p0 / s, p1 / s, p2 / s


def event_flags(result_type: str, details: dict[str, Any]) -> dict[str, int]:
    event = str(result_type or "").lower()
    is_hit = int(event in HIT_EVENTS)
    is_k = int("strikeout" in event)
    is_walk_hbp = int(event in {"walk", "hit_by_pitch", "intent_walk"})
    is_bip = int(event in HIT_EVENTS or event in {"field_out", "force_out", "grounded_into_double_play", "double_play", "fielders_choice_out", "field_error", "sac_fly", "sac_bunt", "fielders_choice", "sac_fly_double_play"})
    other_non_bip = int(not is_k and not is_walk_hbp and not is_bip)
    xbh = int(event in {"double", "triple", "home_run"})
    return {"strikeout": is_k, "walk_hbp": is_walk_hbp, "ball_in_play": is_bip, "other_non_bip": other_non_bip, "official_hit": is_hit, "extra_base_hit": xbh, "bip_out": int(is_bip and not is_hit)}


def parse_pa_ledger() -> tuple[pd.DataFrame, pd.DataFrame]:
    enc = read_csv(ENCOUNTER_LEDGER)
    role = enc[["game_id", "source_event_identity", "role_classification"]].drop_duplicates()
    role["pa_key"] = role["source_event_identity"].astype(str)
    role_map = role.set_index("pa_key")["role_classification"].to_dict()
    rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    pitch_event_rows = 0
    pitch_event_type_rows = 0
    pitch_event_velocity_rows = 0
    pitch_event_hit_data_rows = 0
    pitch_event_launch_speed_rows = 0
    pitch_event_launch_angle_rows = 0
    pitch_event_coordinate_rows = 0
    pitch_event_batted_ball_type_rows = 0
    pitch_event_hard_hit_rows = 0
    for source in sorted(enc["source_path"].dropna().unique()):
        path = Path(source)
        if not path.exists():
            continue
        data = json.loads(path.read_text())
        game_id = data.get("gamePk")
        game_date = str(data.get("gameData", {}).get("datetime", {}).get("officialDate", ""))[:10]
        for seq, play in enumerate(data.get("liveData", {}).get("plays", {}).get("allPlays", []), start=1):
            matchup = play.get("matchup", {})
            result = play.get("result", {})
            about = play.get("about", {})
            events = [ev for ev in play.get("playEvents", []) if ev.get("isPitch")]
            if not events:
                continue
            for ev in events:
                pitch_event_rows += 1
                details_ev = ev.get("details", {}) or {}
                pitch_data_ev = ev.get("pitchData", {}) or {}
                hit_data_ev = ev.get("hitData", {}) or {}
                if details_ev.get("type", {}).get("code") is not None:
                    pitch_event_type_rows += 1
                if pitch_data_ev.get("startSpeed") is not None:
                    pitch_event_velocity_rows += 1
                if hit_data_ev:
                    pitch_event_hit_data_rows += 1
                if hit_data_ev.get("launchSpeed") is not None:
                    pitch_event_launch_speed_rows += 1
                if hit_data_ev.get("launchAngle") is not None:
                    pitch_event_launch_angle_rows += 1
                if (hit_data_ev.get("coordinates", {}) or {}).get("coordX") is not None:
                    pitch_event_coordinate_rows += 1
                if hit_data_ev.get("trajectory") is not None:
                    pitch_event_batted_ball_type_rows += 1
                launch_speed_ev = hit_data_ev.get("launchSpeed")
                if launch_speed_ev is not None:
                    pitch_event_hard_hit_rows += 1
            terminal = events[-1]
            details = terminal.get("details", {})
            pdata = terminal.get("pitchData", {})
            hdata = terminal.get("hitData", {}) or {}
            event_type = str(result.get("eventType", "")).lower()
            flags = event_flags(event_type, details)
            pa_key = f"{game_id}:{play.get('atBatIndex')}"
            launch_speed = hdata.get("launchSpeed")
            launch_angle = hdata.get("launchAngle")
            rows.append({
                "game_date": game_date,
                "game_id": game_id,
                "plate_appearance_sequence": seq,
                "pa_key": pa_key,
                "batter_id": matchup.get("batter", {}).get("id"),
                "pitcher_id": matchup.get("pitcher", {}).get("id"),
                "batter_hand": matchup.get("batSide", {}).get("code"),
                "pitcher_hand": matchup.get("pitchHand", {}).get("code"),
                "starter_reliever_role": role_map.get(pa_key, ""),
                "official_pa_result": event_type,
                "pitch_count": len(events),
                "terminal_pitch_type": details.get("type", {}).get("code"),
                "terminal_velocity": pdata.get("startSpeed"),
                "terminal_count_balls": terminal.get("count", {}).get("balls"),
                "terminal_count_strikes": terminal.get("count", {}).get("strikes"),
                "launch_speed": launch_speed,
                "launch_angle": launch_angle,
                "hit_coordinates_x": hdata.get("coordinates", {}).get("coordX"),
                "hit_coordinates_y": hdata.get("coordinates", {}).get("coordY"),
                "batted_ball_type": hdata.get("trajectory"),
                "hard_hit": int(float(launch_speed) >= 95) if launch_speed is not None else "",
                "source_path": rel(path),
                "source_sha256": sha256(path),
                **flags,
            })
    pa = pd.DataFrame(rows)
    if pa.empty:
        return pa, pd.DataFrame()
    terminal = pa.copy()
    pitch_terminal_type = int(pa["terminal_pitch_type"].notna().sum())
    pitch_terminal_vel = int(pd.to_numeric(pa["terminal_velocity"], errors="coerce").notna().sum())
    bip = pa[pa["ball_in_play"].eq(1)]
    hits = pa[pa["official_hit"].eq(1)]
    contact_outs = pa[pa["bip_out"].eq(1)]
    # Pitch-event total is read from the prior inventory if available; otherwise terminal-only.
    coverage_rows = [
        {"scope": "all_pitch_events", "rows": pitch_event_rows, "pitch_type_coverage": pitch_event_type_rows / pitch_event_rows if pitch_event_rows else "", "velocity_coverage": pitch_event_velocity_rows / pitch_event_rows if pitch_event_rows else "", "hit_data_launch_speed_coverage": pitch_event_launch_speed_rows / pitch_event_rows if pitch_event_rows else "", "launch_angle_coverage": pitch_event_launch_angle_rows / pitch_event_rows if pitch_event_rows else "", "coordinates_coverage": pitch_event_coordinate_rows / pitch_event_rows if pitch_event_rows else "", "batted_ball_type_coverage": pitch_event_batted_ball_type_rows / pitch_event_rows if pitch_event_rows else "", "hard_hit_coverage": pitch_event_hard_hit_rows / pitch_event_rows if pitch_event_rows else "", "hit_data_object_coverage": pitch_event_hit_data_rows / pitch_event_rows if pitch_event_rows else ""},
        {"scope": "terminal_pa_pitches", "rows": len(pa), "pitch_type_coverage": pitch_terminal_type / len(pa), "velocity_coverage": pitch_terminal_vel / len(pa), "hit_data_launch_speed_coverage": pd.to_numeric(pa["launch_speed"], errors="coerce").notna().mean(), "launch_angle_coverage": pd.to_numeric(pa["launch_angle"], errors="coerce").notna().mean(), "coordinates_coverage": pa["hit_coordinates_x"].notna().mean(), "batted_ball_type_coverage": pa["batted_ball_type"].notna().mean(), "hard_hit_coverage": pa["hard_hit"].astype(str).ne("").mean()},
        {"scope": "balls_in_play", "rows": len(bip), "pitch_type_coverage": bip["terminal_pitch_type"].notna().mean() if len(bip) else "", "velocity_coverage": pd.to_numeric(bip["terminal_velocity"], errors="coerce").notna().mean() if len(bip) else "", "hit_data_launch_speed_coverage": pd.to_numeric(bip["launch_speed"], errors="coerce").notna().mean() if len(bip) else "", "launch_angle_coverage": pd.to_numeric(bip["launch_angle"], errors="coerce").notna().mean() if len(bip) else "", "coordinates_coverage": bip["hit_coordinates_x"].notna().mean() if len(bip) else "", "batted_ball_type_coverage": bip["batted_ball_type"].notna().mean() if len(bip) else "", "hard_hit_coverage": bip["hard_hit"].astype(str).ne("").mean() if len(bip) else ""},
        {"scope": "official_hits", "rows": len(hits), "hit_data_launch_speed_coverage": pd.to_numeric(hits["launch_speed"], errors="coerce").notna().mean() if len(hits) else "", "launch_angle_coverage": pd.to_numeric(hits["launch_angle"], errors="coerce").notna().mean() if len(hits) else "", "coordinates_coverage": hits["hit_coordinates_x"].notna().mean() if len(hits) else "", "batted_ball_type_coverage": hits["batted_ball_type"].notna().mean() if len(hits) else ""},
        {"scope": "official_outs_on_contact", "rows": len(contact_outs), "hit_data_launch_speed_coverage": pd.to_numeric(contact_outs["launch_speed"], errors="coerce").notna().mean() if len(contact_outs) else "", "launch_angle_coverage": pd.to_numeric(contact_outs["launch_angle"], errors="coerce").notna().mean() if len(contact_outs) else "", "coordinates_coverage": contact_outs["hit_coordinates_x"].notna().mean() if len(contact_outs) else "", "batted_ball_type_coverage": contact_outs["batted_ball_type"].notna().mean() if len(contact_outs) else ""},
    ]
    return pa, pd.DataFrame(coverage_rows)


def build_profiles(pop: pd.DataFrame, pa: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pa = pa.copy()
    pa["game_date_dt"] = pd.to_datetime(pa["game_date"], errors="coerce")
    pa["batter_key"] = pa["batter_id"].astype("Int64").astype(str)
    pa["pitcher_key"] = pa["pitcher_id"].astype("Int64").astype(str)
    pop = pop.copy()
    pop["slate_date_dt"] = pd.to_datetime(pop["slate_date"], errors="coerce")
    pop["batter_key"] = pop["player_id"].astype("Int64").astype(str)
    pop["starter_key"] = pd.to_numeric(pop["opposing_starter_id"], errors="coerce").astype("Int64").astype(str)
    priors = {k: float(pa[k].mean()) for k in ["strikeout", "walk_hbp", "ball_in_play", "other_non_bip", "official_hit", "extra_base_hit"] if k in pa}
    bip = pa[pa["ball_in_play"].eq(1)]
    priors["hit_on_bip"] = float(bip["official_hit"].mean()) if len(bip) else 0.32
    priors["hard_hit"] = float(pd.to_numeric(bip["hard_hit"], errors="coerce").mean()) if len(bip) else 0.30
    rows = []
    hitter_rows = []
    pitcher_rows = []
    for date, day in pop.groupby("slate_date_dt", dropna=False):
        prior = pa[pa["game_date_dt"] < pd.Timestamp(date)] if pd.notna(date) else pa.iloc[0:0]
        if prior.empty:
            prior = pa.iloc[0:0]
        hitter = prior.groupby("batter_key").agg(
            pa_count=("official_hit", "count"),
            k_rate=("strikeout", "mean"),
            walk_hbp_rate=("walk_hbp", "mean"),
            bip_rate=("ball_in_play", "mean"),
            hit_rate=("official_hit", "mean"),
            xbh_rate=("extra_base_hit", "mean"),
        ).to_dict("index")
        pitcher = prior.groupby("pitcher_key").agg(
            pa_count=("official_hit", "count"),
            k_rate=("strikeout", "mean"),
            bip_allowed=("ball_in_play", "mean"),
            hit_allowed=("official_hit", "mean"),
        ).to_dict("index")
        prior_bip = prior[prior["ball_in_play"].eq(1)].copy()
        hitter_bip = prior_bip.groupby("batter_key").agg(bip_count=("official_hit", "count"), hit_on_bip=("official_hit", "mean"), hard_hit=("hard_hit", lambda s: pd.to_numeric(s, errors="coerce").mean())).to_dict("index") if len(prior_bip) else {}
        pitcher_bip = prior_bip.groupby("pitcher_key").agg(bip_count=("official_hit", "count"), hit_on_bip_allowed=("official_hit", "mean"), hard_hit_allowed=("hard_hit", lambda s: pd.to_numeric(s, errors="coerce").mean())).to_dict("index") if len(prior_bip) else {}
        bullpen_prior = prior[prior["starter_reliever_role"].eq("RELIEVER_FACING_PA")]
        bullpen_bip = bullpen_prior[bullpen_prior["ball_in_play"].eq(1)]
        bullpen_bip_rate = shrink(float(bullpen_prior["ball_in_play"].mean()) if len(bullpen_prior) else np.nan, len(bullpen_prior), priors["ball_in_play"])
        bullpen_hit_bip = shrink(float(bullpen_bip["official_hit"].mean()) if len(bullpen_bip) else np.nan, len(bullpen_bip), priors["hit_on_bip"])
        for _, r in day.iterrows():
            hk = r["batter_key"]
            pk = r["starter_key"]
            hp = hitter.get(hk, {})
            pp = pitcher.get(pk, {})
            hb = hitter_bip.get(hk, {})
            pb = pitcher_bip.get(pk, {})
            h_n = int(hp.get("pa_count", 0) or 0)
            p_n = int(pp.get("pa_count", 0) or 0)
            hb_n = int(hb.get("bip_count", 0) or 0)
            pb_n = int(pb.get("bip_count", 0) or 0)
            hitter_hit = shrink(float(hp.get("hit_rate", np.nan)), h_n, priors["official_hit"])
            pitcher_hit = shrink(float(pp.get("hit_allowed", np.nan)), p_n, priors["official_hit"])
            direct_pa_hit = (hitter_hit + pitcher_hit) / 2.0
            h_bip = shrink(float(hp.get("bip_rate", np.nan)), h_n, priors["ball_in_play"])
            p_bip = shrink(float(pp.get("bip_allowed", np.nan)), p_n, priors["ball_in_play"])
            starter_bip = (h_bip + p_bip) / 2.0
            h_hit_bip = shrink(float(hb.get("hit_on_bip", np.nan)), hb_n, priors["hit_on_bip"])
            p_hit_bip = shrink(float(pb.get("hit_on_bip_allowed", np.nan)), pb_n, priors["hit_on_bip"])
            starter_hit_bip = (h_hit_bip + p_hit_bip) / 2.0
            h_hard = shrink(float(hb.get("hard_hit", np.nan)), hb_n, priors["hard_hit"])
            p_hard = shrink(float(pb.get("hard_hit_allowed", np.nan)), pb_n, priors["hard_hit"])
            cq_factor = 1.0 + 0.25 * (((h_hard + p_hard) / 2.0) - priors["hard_hit"])
            cq_hit_bip = clip_prob(starter_hit_bip * cq_factor, 0.05, 0.65)
            rows.append({
                "player_game_key": r["player_game_key"],
                "hitter_pa_support": h_n,
                "pitcher_pa_support": p_n,
                "hitter_bip_support": hb_n,
                "pitcher_bip_support": pb_n,
                "hitter_support_class": support_class(h_n),
                "pitcher_support_class": support_class(p_n),
                "hitter_bip_support_class": support_class(hb_n),
                "pitcher_bip_support_class": support_class(pb_n),
                "pred_strikeout_rate": shrink(float(hp.get("k_rate", np.nan)), h_n, priors["strikeout"]),
                "pred_walk_hbp_rate": shrink(float(hp.get("walk_hbp_rate", np.nan)), h_n, priors["walk_hbp"]),
                "pred_bip_rate": starter_bip,
                "pred_hit_on_bip_rate": starter_hit_bip,
                "pred_contact_quality_hardhit": (h_hard + p_hard) / 2.0,
                "pred_contact_quality_hit_on_bip": cq_hit_bip,
                "direct_pa_hit_rate": direct_pa_hit,
                "source_starter_pa_hit_rate": starter_bip * starter_hit_bip,
                "source_bullpen_pa_hit_rate": ((h_bip + bullpen_bip_rate) / 2.0) * ((h_hit_bip + bullpen_hit_bip) / 2.0),
                "source_starter_cq_hit_rate": starter_bip * cq_hit_bip,
            })
            hitter_rows.append({"player_game_key": r["player_game_key"], "player_id": r["player_id"], "pa_support": h_n, "bip_support": hb_n, "strikeout_rate": shrink(float(hp.get("k_rate", np.nan)), h_n, priors["strikeout"]), "bip_rate": h_bip, "hit_on_bip_rate": h_hit_bip, "evidence_class": support_class(h_n)})
            pitcher_rows.append({"player_game_key": r["player_game_key"], "pitcher_id": r.get("opposing_starter_id"), "pa_support": p_n, "bip_support": pb_n, "bip_allowed_rate": p_bip, "hit_on_bip_allowed_rate": p_hit_bip, "evidence_class": support_class(p_n)})
    prof = pd.DataFrame(rows)
    return pop.merge(prof, on="player_game_key", how="left"), pd.DataFrame(hitter_rows), pd.DataFrame(pitcher_rows)


def apply_instruments(df: pd.DataFrame) -> pd.DataFrame:
    total_pa = pd.to_numeric(df["turnover_total_pa"], errors="coerce").fillna(pd.to_numeric(df["pred_total_pa"], errors="coerce")).fillna(4.0)
    starter_pa = pd.to_numeric(df["turnover_starter_pa"], errors="coerce").fillna(pd.to_numeric(df["prior_pred_starter_pa"], errors="coerce")).fillna(2.4)
    bullpen_pa = pd.to_numeric(df["turnover_bullpen_pa"], errors="coerce").fillna(pd.to_numeric(df["prior_pred_bullpen_pa"], errors="coerce")).fillna(1.6)
    instruments = {
        "pa_hit_rate_baseline": (total_pa, df["direct_pa_hit_rate"]),
        "bip_decomposition": (total_pa, df["pred_bip_rate"] * df["pred_hit_on_bip_rate"]),
        "contact_quality_extension": (total_pa, df["pred_bip_rate"] * df["pred_contact_quality_hit_on_bip"]),
    }
    for prefix, (pa_count, hit_rate) in instruments.items():
        vals = [poisson_hit_dist(n, p) for n, p in zip(pa_count, hit_rate)]
        df[f"{prefix}_p_zero_hits"] = [v[0] for v in vals]
        df[f"{prefix}_p_exactly_one_hit"] = [v[1] for v in vals]
        df[f"{prefix}_p_two_plus_hits"] = [v[2] for v in vals]
    vals = [source_hit_dist(s, b, ps, pb) for s, b, ps, pb in zip(starter_pa, bullpen_pa, df["source_starter_pa_hit_rate"], df["source_bullpen_pa_hit_rate"])]
    df["source_aware_pa_hazard_p_zero_hits"] = [v[0] for v in vals]
    df["source_aware_pa_hazard_p_exactly_one_hit"] = [v[1] for v in vals]
    df["source_aware_pa_hazard_p_two_plus_hits"] = [v[2] for v in vals]
    df["unified_pa_sequence_p_zero_hits"] = df["source_aware_pa_hazard_p_zero_hits"]
    df["unified_pa_sequence_p_exactly_one_hit"] = df["source_aware_pa_hazard_p_exactly_one_hit"]
    df["unified_pa_sequence_p_two_plus_hits"] = df["source_aware_pa_hazard_p_two_plus_hits"]
    # Oracle diagnostics.
    vals = [source_hit_dist(s, b, ps, pb) for s, b, ps, pb in zip(df["actual_starter_pa_target"], df["actual_bullpen_pa_target"], df["source_starter_pa_hit_rate"], df["source_bullpen_pa_hit_rate"])]
    df["oracle_pa_source_p_two_plus_hits"] = [v[2] for v in vals]
    actual_bip = pd.to_numeric(df["actual_bip_count"], errors="coerce").fillna(0)
    vals = [poisson_hit_dist(n, p) for n, p in zip(actual_bip, df["pred_hit_on_bip_rate"])]
    df["oracle_bip_count_p_two_plus_hits"] = [v[2] for v in vals]
    actual_hard = pd.to_numeric(df["actual_hard_hit_count"], errors="coerce").fillna(0)
    actual_bip_nonzero = actual_bip.replace(0, np.nan)
    quality_rate = (df["pred_hit_on_bip_rate"] * (1 + 0.35 * ((actual_hard / actual_bip_nonzero).fillna(df["pred_contact_quality_hardhit"]) - df["pred_contact_quality_hardhit"]))).clip(0.05, 0.75)
    vals = [poisson_hit_dist(n, p) for n, p in zip(actual_bip, quality_rate)]
    df["oracle_contact_quality_p_two_plus_hits"] = [v[2] for v in vals]
    return df


def hitter_game_actuals(pa: pd.DataFrame) -> pd.DataFrame:
    agg = pa.groupby(["game_date", "game_id", "batter_id"]).agg(
        ledger_pa_count=("official_hit", "count"),
        ledger_hits_count=("official_hit", "sum"),
        actual_bip_count=("ball_in_play", "sum"),
        actual_hard_hit_count=("hard_hit", lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
        actual_strikeouts=("strikeout", "sum"),
        actual_walk_hbp=("walk_hbp", "sum"),
    ).reset_index().rename(columns={"batter_id": "player_id", "game_date": "slate_date"})
    return agg


def hitter_game_reconciliation(pop: pd.DataFrame) -> pd.DataFrame:
    out = pop[[
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "official_pa",
        "official_hits",
        "ledger_pa_count",
        "ledger_hits_count",
    ]].copy()
    out["official_pa_numeric"] = pd.to_numeric(out["official_pa"], errors="coerce")
    out["official_hits_numeric"] = pd.to_numeric(out["official_hits"], errors="coerce")
    out["ledger_pa_numeric"] = pd.to_numeric(out["ledger_pa_count"], errors="coerce")
    out["ledger_hits_numeric"] = pd.to_numeric(out["ledger_hits_count"], errors="coerce")
    out["pa_reconciles"] = out["official_pa_numeric"].eq(out["ledger_pa_numeric"])
    out["hits_reconciles"] = out["official_hits_numeric"].eq(out["ledger_hits_numeric"])
    out["reconciliation_status"] = np.select(
        [
            out["pa_reconciles"] & out["hits_reconciles"],
            out["ledger_pa_numeric"].isna(),
            out["pa_reconciles"] & ~out["hits_reconciles"],
            ~out["pa_reconciles"] & out["hits_reconciles"],
        ],
        [
            "PASS",
            "NO_LEDGER_ROW",
            "HITS_MISMATCH",
            "PA_MISMATCH",
        ],
        default="PA_AND_HITS_MISMATCH",
    )
    return out[[
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "official_pa",
        "official_hits",
        "ledger_pa_count",
        "ledger_hits_count",
        "pa_reconciles",
        "hits_reconciles",
        "reconciliation_status",
    ]]


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
            sample = hold.sample(n=len(hold), replace=True, random_state=int(rng.integers(0, 2**31 - 1)))
            y = sample["two_plus_binary"].astype(int).to_numpy()
            p = np.clip(sample[col].astype(float).to_numpy(), EPS, 1 - EPS)
            briers.append(float(np.mean((p - y) ** 2)))
            aucs.append(float(roc_auc_score(y, p)) if len(set(y)) > 1 else np.nan)
        rows.append({"instrument": name, "brier_p05": float(np.nanquantile(briers, .05)), "brier_p50": float(np.nanquantile(briers, .5)), "brier_p95": float(np.nanquantile(briers, .95)), "auc_p05": float(np.nanquantile(aucs, .05)), "auc_p50": float(np.nanquantile(aucs, .5)), "auc_p95": float(np.nanquantile(aucs, .95))})
    return pd.DataFrame(rows)


def roster_relative(df: pd.DataFrame, prob_col: str) -> pd.DataFrame:
    hold = df[df["temporal_split"].eq("holdout")].copy()
    rows = []
    for game_id, g in hold.groupby("game_id"):
        if len(g) < 4:
            continue
        pred = g.sort_values(prob_col, ascending=False).iloc[0]
        actual = g.sort_values("official_hits", ascending=False).iloc[0]
        pairs = correct = 0
        gg = g[[prob_col, "official_hits"]].dropna().reset_index(drop=True)
        for i in range(len(gg)):
            for j in range(i + 1, len(gg)):
                if gg.loc[i, "official_hits"] == gg.loc[j, "official_hits"]:
                    continue
                pairs += 1
                correct += int((gg.loc[i, prob_col] > gg.loc[j, prob_col]) == (gg.loc[i, "official_hits"] > gg.loc[j, "official_hits"]))
        rows.append({"game_id": game_id, "hitters": len(g), "predicted_top_player_id": pred["player_id"], "predicted_top_player_name": pred["player_name"], "actual_top_player_id": actual["player_id"], "actual_top_player_name": actual["player_name"], "top_predicted_actual_hits": pred["official_hits"], "actual_top_hits": actual["official_hits"], "top_agreement": pred["player_game_key"] == actual["player_game_key"], "pairwise_ordering_accuracy": correct / pairs if pairs else ""})
    out = pd.DataFrame(rows)
    if not out.empty:
        out["top_agreement_rate"] = out["top_agreement"].mean()
    return out


def plus200(df: pd.DataFrame) -> pd.DataFrame:
    price = read_csv(LONG_PRICE)
    target = price[price["price_band"].eq("+200_through_+249")].copy() if not price.empty else pd.DataFrame()
    if target.empty:
        return pd.DataFrame()
    m = target.merge(df, on="player_game_key", how="left", suffixes=("_price", ""))
    rows = []
    for split, g in m.groupby("temporal_split", dropna=False):
        rows.append({"temporal_split": split, "rows": len(g), "avg_predicted_bip_rate": float(g["pred_bip_rate"].mean()), "avg_predicted_hit_on_contact": float(g["pred_hit_on_bip_rate"].mean()), "avg_predicted_two_plus": float(g["unified_pa_sequence_p_two_plus_hits"].mean()), "observed_two_plus_rate": float(g["outcome_class"].eq("TWO_OR_MORE_HITS").mean()), "avg_implied_break_even": float(pd.to_numeric(g.get("market_implied_break_even_probability", pd.Series(np.nan,index=g.index)), errors="coerce").mean()), "diagnostic_roi": float(pd.to_numeric(g.get("profit_1u_diagnostic", pd.Series(np.nan,index=g.index)), errors="coerce").mean()), "timing_certification": "|".join(sorted(g.get("selection_time_timing_certification", pd.Series(dtype=object)).dropna().astype(str).unique()))})
    return pd.DataFrame(rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pop = read_csv(POP_PATH)
    pa, coverage = parse_pa_ledger()
    actual = hitter_game_actuals(pa)
    pop = pop.merge(actual, on=["slate_date", "game_id", "player_id"], how="left")
    reconciliation = hitter_game_reconciliation(pop)
    pop, hitter_profiles, pitcher_profiles = build_profiles(pop, pa)
    pop = apply_instruments(pop)
    instruments = {
        "frozen_predicted_exposure_control": "prior_predicted_exposure_p_two_plus_hits",
        "pa_hit_rate_baseline": "pa_hit_rate_baseline_p_two_plus_hits",
        "bip_decomposition": "bip_decomposition_p_two_plus_hits",
        "contact_quality_extension": "contact_quality_extension_p_two_plus_hits",
        "source_aware_pa_hazard": "source_aware_pa_hazard_p_two_plus_hits",
        "unified_pa_sequence": "unified_pa_sequence_p_two_plus_hits",
        "oracle_pa_source": "oracle_pa_source_p_two_plus_hits",
        "oracle_bip_count": "oracle_bip_count_p_two_plus_hits",
        "oracle_contact_quality": "oracle_contact_quality_p_two_plus_hits",
    }
    metrics = []
    for split in ["validation", "holdout"]:
        for name, col in instruments.items():
            metrics.append(metric(pop, col, name, split))
    metrics_df = pd.DataFrame(metrics)
    boot = bootstrap(pop, {k: v for k, v in instruments.items() if not k.startswith("oracle")})
    mechanism = []
    for split in ["validation", "holdout"]:
        sub = pop[(pop["temporal_split"].eq(split)) & pop["one_to_two_population"] & pop["suppression_subtype"].isna()]
        for outcome, g in sub.groupby("outcome_class"):
            mechanism.append({"temporal_split": split, "outcome_class": outcome, "rows": len(g), "avg_projected_pa": float(g["turnover_total_pa"].mean()), "avg_projected_bip": float((g["turnover_total_pa"] * g["pred_bip_rate"]).mean()), "avg_strikeout_probability": float(g["pred_strikeout_rate"].mean()), "avg_bip_probability": float(g["pred_bip_rate"].mean()), "avg_hit_on_contact": float(g["pred_hit_on_bip_rate"].mean()), "avg_contact_quality_hardhit": float(g["pred_contact_quality_hardhit"].mean()), "avg_starter_conversion": float(g["source_starter_pa_hit_rate"].mean()), "avg_bullpen_conversion": float(g["source_bullpen_pa_hit_rate"].mean())})
    mechanism_df = pd.DataFrame(mechanism)
    supp = pop[(pop["temporal_split"].eq("holdout")) & pop["suppression_subtype"].notna()]
    suppression = pd.DataFrame([{"temporal_split": "holdout", "rows": len(supp), "avg_pred_strikeout_probability": float(supp["pred_strikeout_rate"].mean()), "avg_pred_bip_probability": float(supp["pred_bip_rate"].mean()), "avg_pred_hit_on_contact": float(supp["pred_hit_on_bip_rate"].mean()), "avg_pred_two_plus": float(supp["unified_pa_sequence_p_two_plus_hits"].mean()), "observed_two_plus_rate": float(supp["two_plus_binary"].mean()), "suppression_preserved": bool(supp["unified_pa_sequence_p_two_plus_hits"].mean() < 0.30)}])
    roster = roster_relative(pop, "unified_pa_sequence_p_two_plus_hits")
    source_rows = []
    two = pop[pop["outcome_class"].eq("TWO_OR_MORE_HITS") & pop["two_plus_hit_source_class"].notna()]
    for split in ["validation", "holdout"]:
        for cls, g in two[two["temporal_split"].eq(split)].groupby("two_plus_hit_source_class"):
            source_rows.append({"temporal_split": split, "second_hit_source": cls, "rows": len(g), "avg_bip_probability": float(g["pred_bip_rate"].mean()), "avg_hit_on_contact": float(g["pred_hit_on_bip_rate"].mean()), "avg_starter_hit_rate": float(g["source_starter_pa_hit_rate"].mean()), "avg_bullpen_hit_rate": float(g["source_bullpen_pa_hit_rate"].mean()), "avg_unified_two_plus": float(g["unified_pa_sequence_p_two_plus_hits"].mean())})
    source_df = pd.DataFrame(source_rows)
    plus = plus200(pop)
    reconcile_pass = int(reconciliation["reconciliation_status"].eq("PASS").sum()) if not reconciliation.empty else 0
    reconcile_rows = int(len(reconciliation))
    hold = metrics_df[metrics_df["temporal_split"].eq("holdout")].set_index("instrument")
    control_brier = float(hold.loc["frozen_predicted_exposure_control", "brier"])
    unified_brier = float(hold.loc["unified_pa_sequence", "brier"])
    control_auc = float(hold.loc["frozen_predicted_exposure_control", "auc"])
    unified_auc = float(hold.loc["unified_pa_sequence", "auc"])
    oracle_bip_auc = float(hold.loc["oracle_bip_count", "auc"])
    if not bool(suppression.iloc[0]["suppression_preserved"]):
        next_decision = "NO HITTER_OWNED CHALLENGER READY"
    elif unified_brier < control_brier and unified_auc > control_auc:
        next_decision = "PA_HIT_HAZARD_ADDS_ONE_TO_TWO_PLUS_VALUE"
    elif oracle_bip_auc > control_auc + 0.03:
        next_decision = "BALL_IN_PLAY_OPPORTUNITY_IS_PRIMARY_SIGNAL"
    elif float(hold.loc["oracle_contact_quality", "auc"]) > control_auc + 0.03:
        next_decision = "HIT_ON_CONTACT_QUALITY_IS_PRIMARY_SIGNAL"
    elif unified_brier < control_brier:
        next_decision = "PA_HAZARD_CALIBRATION_ONLY"
    else:
        next_decision = "PA_HAZARD_NO_INCREMENTAL_VALUE"
    pa_gap_value = "HIT_TOTALS_RECONCILED_PA_DENOMINATOR_GAP_RETAINED" if reconcile_pass < reconcile_rows else "PA_AND_HIT_TOTALS_RECONCILED"
    ledger_value = "CANONICAL_PA_OUTCOME_LEDGER_CREATED_WITH_PA_RECONCILIATION_GAP" if reconcile_pass < reconcile_rows else "CANONICAL_PA_OUTCOME_LEDGER_CREATED"
    decisions = pd.DataFrame([
        {"decision": "MLB_PA_HAZARD_SOURCE_RECONCILIATION_DECISION", "value": pa_gap_value},
        {"decision": "MLB_PA_HAZARD_OUTCOME_LEDGER_DECISION", "value": ledger_value},
        {"decision": "MLB_PA_HAZARD_PROFILE_SUPPORT_DECISION", "value": "SUPPORT_AWARE_SHRINKAGE_APPLIED"},
        {"decision": "MLB_PA_HAZARD_BIP_MODEL_DECISION", "value": "BIP_DECOMPOSITION_EVALUATED"},
        {"decision": "MLB_PA_HAZARD_HIT_ON_CONTACT_DECISION", "value": "HIT_ON_CONTACT_EVALUATED"},
        {"decision": "MLB_PA_HAZARD_CONTACT_QUALITY_DECISION", "value": "CONTACT_QUALITY_EXTENSION_DIAGNOSTIC_LIMITED"},
        {"decision": "MLB_PA_HAZARD_SOURCE_AWARE_DECISION", "value": "SOURCE_AWARE_PA_HAZARD_EVALUATED"},
        {"decision": "MLB_PA_HAZARD_UNIFIED_COUNT_DECISION", "value": "UNIFIED_PA_SEQUENCE_EVALUATED"},
        {"decision": "MLB_PA_HAZARD_ONE_TO_TWO_PLUS_HOLDOUT_DECISION", "value": next_decision},
        {"decision": "MLB_PA_HAZARD_ORACLE_DIAGNOSTIC_DECISION", "value": "ORACLE_DIAGNOSTICS_RETAINED_NONDEPLOYABLE"},
        {"decision": "MLB_PA_HAZARD_SUPPRESSION_PRESERVATION_DECISION", "value": "SUPPRESSION_PRESERVED" if bool(suppression.iloc[0]["suppression_preserved"]) else "SUPPRESSION_NOT_PRESERVED"},
        {"decision": "MLB_PA_HAZARD_ROSTER_RELATIVE_DECISION", "value": "ROSTER_RELATIVE_VALIDATED_DIAGNOSTIC"},
        {"decision": "MLB_PA_HAZARD_SECOND_HIT_SOURCE_DECISION", "value": "SECOND_HIT_SOURCE_DIAGNOSTIC_ONLY"},
        {"decision": "MLB_PA_HAZARD_PLUS200_DECISION", "value": "PLUS200_DIAGNOSTIC_ONLY_NO_OPTIMIZATION"},
        {"decision": "MLB_PA_HAZARD_NEXT_RESEARCH_DECISION", "value": next_decision},
        {"decision": "MLB_PA_HAZARD_PRODUCTION_STATUS", "value": "NOT_AUTHORIZED"},
    ])
    contract = pd.DataFrame([
        {"instrument": "PA hit-rate baseline", "construction": "projected PA count x shrunk direct per-PA hit rate from hitter and pitcher priors"},
        {"instrument": "BIP decomposition", "construction": "P(hit)=P(BIP) x P(hit|BIP) with support-aware hitter/pitcher priors"},
        {"instrument": "Contact-quality extension", "construction": "BIP decomposition adjusted by strict-prior hard-hit support where available"},
        {"instrument": "Source-aware PA hazard", "construction": "separate starter and bullpen PA hit hazards weighted by predicted exposure"},
        {"instrument": "Unified PA-sequence", "construction": "source-aware PA hazards converted to zero/exactly-one/two-plus probabilities"},
    ])
    outputs = {
        "pitch_bip_coverage_reconciliation_2026-07-17.csv": coverage,
        "canonical_pa_outcome_ledger_2026-07-17.csv": pa,
        "hitter_game_pa_hit_reconciliation_2026-07-17.csv": reconciliation,
        "strict_prior_hitter_profiles_2026-07-17.csv": hitter_profiles,
        "strict_prior_pitcher_profiles_2026-07-17.csv": pitcher_profiles,
        "profile_support_ledger_2026-07-17.csv": pop[["player_game_key", "hitter_pa_support", "pitcher_pa_support", "hitter_bip_support", "pitcher_bip_support", "hitter_support_class", "pitcher_support_class", "hitter_bip_support_class", "pitcher_bip_support_class"]],
        "frozen_pa_hazard_instruments_2026-07-17.csv": contract,
        "oracle_diagnostics_2026-07-17.csv": pop[["player_game_key", "oracle_pa_source_p_two_plus_hits", "oracle_bip_count_p_two_plus_hits", "oracle_contact_quality_p_two_plus_hits", "actual_bip_count", "actual_hard_hit_count"]],
        "validation_holdout_metrics_2026-07-17.csv": metrics_df,
        "bootstrap_uncertainty_2026-07-17.csv": boot,
        "mechanism_attribution_2026-07-17.csv": mechanism_df,
        "suppression_preservation_2026-07-17.csv": suppression,
        "roster_relative_results_2026-07-17.csv": roster,
        "second_hit_source_analysis_2026-07-17.csv": source_df,
        "frozen_plus200_evaluation_2026-07-17.csv": plus,
        "research_only_model_artifacts_2026-07-17.csv": pop,
        "required_decisions_2026-07-17.csv": decisions,
    }
    for filename, df in outputs.items():
        write_csv(df, out_dir / filename)
    manifest = []
    for path in [POP_PATH, ENCOUNTER_LEDGER, LONG_PRICE]:
        manifest.append({"artifact_role": "input", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*.csv")):
        manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    machine = {"generated_at_utc": now_utc(), "pa_rows": int(len(pa)), "reconciliation_rows": reconcile_rows, "reconciliation_pass_rows": reconcile_pass, "holdout_control_brier": control_brier, "holdout_unified_brier": unified_brier, "holdout_control_auc": control_auc, "holdout_unified_auc": unified_auc, "next_decision": next_decision, "suppression_preserved": bool(suppression.iloc[0]["suppression_preserved"]), "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()}}
    write_json(machine, out_dir / "machine_readable_pa_hazard_pilot_2026-07-17.json")
    direct = "No. The PA hazard decomposition did not reveal deployable hitter-owned multi-hit ranking signal after opportunity and exposure; it remains diagnostic/research-only." if next_decision != "PA_HIT_HAZARD_ADDS_ONE_TO_TWO_PLUS_VALUE" else "Yes, bounded evidence shows PA hazard decomposition adds hitter-owned multi-hit value, but it remains research-only."
    write_md(f"""# MLB Plate-Appearance Hit-Hazard Multi-Hit Pilot

Generated: `{machine['generated_at_utc']}`

## Executive Summary

The pilot created a canonical PA outcome ledger with `{len(pa)}` plate appearances and decomposed projected PA hit probability into BIP opportunity and hit-on-contact conversion. Hitter-game PA/hit reconciliation passed `{reconcile_pass}` of `{reconcile_rows}` benchmark rows.

Holdout one-hit versus two-plus:

| instrument | brier | auc |
|---|---:|---:|
| predicted exposure control | {control_brier:.6f} | {control_auc:.6f} |
| unified PA sequence | {unified_brier:.6f} | {unified_auc:.6f} |

## Direct Answer

{direct}

## Production Status

`MLB_PA_HAZARD_PRODUCTION_STATUS = NOT_AUTHORIZED`

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
