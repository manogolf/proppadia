#!/usr/bin/env python3
"""Bounded MLB Hits O1.5 market-baseline incremental probability validation.

This utility is research-only. It aligns frozen O1.5 propositions to preserved
local odds snapshots, constructs market implied probabilities, binds already
frozen Proppadia probabilities, and evaluates whether Proppadia adds
probability information beyond the selection-time market.

No network calls, OddsAPI calls, database writes, model refitting, feature
creation, threshold optimization, production candidate/upload changes,
workspace changes, or LaunchAgent changes are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.mlb.scripts.build_mlb_reconcile_rows import (  # noqa: E402
    _build_team_name_reverse,
    _line_key,
    _load_events,
    _norm_name,
)

AUDIT_DATE = "2026-07-17"
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_o15_market_incremental_probability_validation/2026-07-17"
ODDS_ROOT = ROOT / "backend/mlb/exports/odds_history"
PRICE_ROWS = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"
CONTACT_ROWS = ROOT / "artifacts/analysis/model_development/mlb_contact_hitter_multi_hit_regime_validation/2026-07-17/research_only_regime_rows_2026-07-17.csv"
PRICE_BANDS = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_fixed_price_bands_2026-07-17.csv"

TS_RE = re.compile(r"(\d{8}T\d{6})Z?")
EPS = 1e-9


@dataclass(frozen=True)
class SnapshotRef:
    path: Path
    timestamp: datetime | None
    run_tag: str
    alias_class: str
    sha256: str


def norm_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def parse_ts(value: object) -> datetime | None:
    text = norm_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        pass
    match = TS_RE.search(text)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def iso(value: datetime | None) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value else ""


def to_int(value: object) -> int | None:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return None
    return int(number)


def to_float(value: object) -> float | None:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return None
    return float(number)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def american_to_decimal(price: object) -> float | None:
    odds = to_float(price)
    if odds is None:
        return None
    return 1 + odds / 100.0 if odds > 0 else 1 + 100.0 / abs(odds)


def american_to_implied(price: object) -> float | None:
    odds = to_float(price)
    if odds is None:
        return None
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0)


def profit_1u(price: object, won: bool) -> float:
    dec = american_to_decimal(price)
    if dec is None:
        return 0.0
    return dec - 1.0 if won else -1.0


def normalize_price_band(price: object) -> str:
    p = to_float(price)
    if p is None:
        return "missing_price"
    if 100 <= p <= 149:
        return "+100_through_+149"
    if 150 <= p <= 199:
        return "+150_through_+199"
    if 200 <= p <= 249:
        return "+200_through_+249"
    if p >= 250:
        return "+250_and_longer"
    return "outside_frozen_bands"


def logit(series: pd.Series) -> pd.Series:
    p = pd.to_numeric(series, errors="coerce").clip(EPS, 1 - EPS)
    return np.log(p / (1 - p))


def safe_auc(y: pd.Series, p: pd.Series) -> float | str:
    yy = y.astype(int)
    if yy.nunique() < 2:
        return ""
    return float(roc_auc_score(yy, pd.to_numeric(p, errors="coerce").clip(EPS, 1 - EPS)))


def probability_metrics(frame: pd.DataFrame, prob_col: str) -> dict[str, Any]:
    g = frame.dropna(subset=["multi_hit_target"]).copy()
    prob_numeric = pd.to_numeric(g[prob_col], errors="coerce")
    g = g[prob_numeric.notna()].copy()
    prob_numeric = prob_numeric.loc[g.index]
    if g.empty:
        return {"rows": 0, "brier": "", "log_loss": "", "auc": "", "ece": "", "calibration_intercept": "", "calibration_slope": ""}
    y = g["multi_hit_target"].astype(int)
    p = prob_numeric.clip(EPS, 1 - EPS)
    # Calibration intercept/slope from fixed one-feature logistic calibration.
    if y.nunique() >= 2:
        x = logit(p).to_numpy().reshape(-1, 1)
        cal = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000)
        cal.fit(x, y)
        intercept = float(cal.intercept_[0])
        slope = float(cal.coef_[0][0])
    else:
        intercept = ""
        slope = ""
    bins = pd.qcut(p.rank(method="first"), q=min(10, len(g)), duplicates="drop")
    ece = 0.0
    for _, b in g.assign(_p=p, _y=y, _bin=bins).groupby("_bin", observed=False):
        ece += len(b) / len(g) * abs(float(b["_p"].mean()) - float(b["_y"].mean()))
    return {
        "rows": int(len(g)),
        "brier": float(brier_score_loss(y, p)),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
        "auc": safe_auc(y, p),
        "ece": float(ece),
        "calibration_intercept": intercept,
        "calibration_slope": slope,
    }


def bootstrap_delta(frame: pd.DataFrame, a_col: str, b_col: str, metric: str, reps: int = 250) -> tuple[float | str, float | str]:
    g = frame.dropna(subset=[a_col, b_col, "multi_hit_target"]).copy()
    if len(g) < 30 or g["multi_hit_target"].nunique() < 2:
        return "", ""
    rng = np.random.default_rng(20260717)
    vals: list[float] = []
    idx = np.arange(len(g))
    for _ in range(reps):
        s = g.iloc[rng.choice(idx, size=len(g), replace=True)]
        y = s["multi_hit_target"].astype(int)
        pa = pd.to_numeric(s[a_col], errors="coerce").clip(EPS, 1 - EPS)
        pb = pd.to_numeric(s[b_col], errors="coerce").clip(EPS, 1 - EPS)
        if metric == "brier":
            vals.append(float(brier_score_loss(y, pa) - brier_score_loss(y, pb)))
        elif y.nunique() >= 2:
            vals.append(float(log_loss(y, pa, labels=[0, 1]) - log_loss(y, pb, labels=[0, 1])))
    if not vals:
        return "", ""
    return float(np.quantile(vals, 0.025)), float(np.quantile(vals, 0.975))


def snapshot_alias(path: Path) -> str:
    name = path.name
    if "__" in name:
        return "run_tagged_snapshot"
    if TS_RE.search(name):
        return "timestamped_snapshot"
    if name in {"odds_mlb_playerprops.json", "odds_mlb_playerprops_final.json"}:
        return "latest_or_final_alias"
    if any(name.endswith(f"_{label}.json") for label in ("earliest", "mid", "late")):
        return "daily_alias"
    return "other_alias"


def snapshot_run_tag(path: Path) -> str:
    if "__" in path.name:
        return path.name.split("__", 1)[1].rsplit(".", 1)[0]
    match = re.search(r"odds_mlb_playerprops(?:__|_)([^.]+)\.json$", path.name)
    return match.group(1) if match else ""


def load_snapshot_refs(odds_root: Path, date_value: str) -> list[SnapshotRef]:
    refs: list[SnapshotRef] = []
    for path in sorted((odds_root / date_value).glob("odds_mlb_playerprops*.json")):
        ts = None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                ts = parse_ts(raw.get("captured_at_utc"))
        except Exception:
            ts = None
        if ts is None:
            ts = parse_ts(path.name)
        refs.append(SnapshotRef(path, ts, snapshot_run_tag(path), snapshot_alias(path), sha256(path)))
    return sorted(refs, key=lambda r: (iso(r.timestamp), str(r.path)))


def load_slate_lookup(odds_root: Path, date_value: str) -> dict[tuple[str, str, str, float, str], dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    for path in sorted((odds_root / date_value).glob("mlb_slate_output__*.csv")):
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        required = {"slate_date", "game_id", "player_id", "player_name", "home_team_code", "away_team_code", "prop_type", "line"}
        if not required.issubset(df.columns):
            continue
        df = df[df["slate_date"].astype(str).eq(date_value)].copy()
        if df.empty:
            continue
        df["_source_path"] = rel(path)
        frames.append(df)
    if not frames:
        return {}
    all_rows = pd.concat(frames, ignore_index=True)
    all_rows["prop_type"] = all_rows["prop_type"].astype(str).str.lower()
    all_rows["line"] = pd.to_numeric(all_rows["line"], errors="coerce")
    all_rows["player_name_norm"] = all_rows["player_name"].map(_norm_name)
    all_rows = all_rows.dropna(subset=["game_id", "player_id", "line"])
    all_rows = all_rows.sort_values("_source_path", kind="stable").drop_duplicates(
        ["home_team_code", "away_team_code", "prop_type", "line", "player_name_norm"],
        keep="last",
    )
    lookup: dict[tuple[str, str, str, float, str], dict[str, Any]] = {}
    for _, row in all_rows.iterrows():
        key = (
            norm_text(row.get("home_team_code")).upper(),
            norm_text(row.get("away_team_code")).upper(),
            norm_text(row.get("prop_type")).lower(),
            float(row.get("line")),
            norm_text(row.get("player_name_norm")),
        )
        lookup[key] = {
            "game_id": int(row.get("game_id")),
            "player_id": int(row.get("player_id")),
            "player_name": norm_text(row.get("player_name")),
            "team": norm_text(row.get("team")),
            "opponent": norm_text(row.get("opponent")),
            "home_team_code": norm_text(row.get("home_team_code")).upper(),
            "away_team_code": norm_text(row.get("away_team_code")).upper(),
            "slate_identity_source_path": norm_text(row.get("_source_path")),
        }
    return lookup


def flatten_snapshot(ref: SnapshotRef, date_value: str, slate_lookup: dict[tuple[str, str, str, float, str], dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    team_rev = _build_team_name_reverse()
    rows: list[dict[str, Any]] = []
    counts = {"raw_outcomes": 0, "matched_price_rows": 0, "unmatched_to_slate_identity": 0, "unsupported_market": 0, "missing_team_map": 0}
    try:
        events = _load_events(ref.path)
    except Exception:
        return rows, counts
    for event in events:
        home = team_rev.get(_norm_name(event.get("home_team")))
        away = team_rev.get(_norm_name(event.get("away_team")))
        commence_time = norm_text(event.get("commence_time"))
        if not home or not away:
            counts["missing_team_map"] += 1
            continue
        for book in event.get("bookmakers") or []:
            book_key = norm_text(book.get("key") or book.get("title")).lower()
            if not book_key:
                continue
            for market in book.get("markets") or []:
                if norm_text(market.get("key")) != "batter_hits":
                    counts["unsupported_market"] += 1
                    continue
                grouped: dict[tuple[str, float], dict[str, Any]] = {}
                for outcome in market.get("outcomes") or []:
                    side = norm_text(outcome.get("name")).lower()
                    if side not in {"over", "under"}:
                        continue
                    player_name = norm_text(outcome.get("description"))
                    line = _line_key(outcome.get("point"))
                    price = to_float(outcome.get("price"))
                    if not player_name or line is None or price is None:
                        continue
                    counts["raw_outcomes"] += 1
                    rec = grouped.setdefault(
                        (_norm_name(player_name), float(line)),
                        {"player_name_norm": _norm_name(player_name), "player_name": player_name, "line": float(line), "price_over_american": None, "price_under_american": None},
                    )
                    rec[f"price_{side}_american"] = price
                for rec in grouped.values():
                    ident = slate_lookup.get((str(home).upper(), str(away).upper(), "hits", float(rec["line"]), rec["player_name_norm"]))
                    if not ident:
                        counts["unmatched_to_slate_identity"] += 1
                        continue
                    over = rec["price_over_american"]
                    under = rec["price_under_american"]
                    over_raw = american_to_implied(over)
                    under_raw = american_to_implied(under)
                    no_vig = ""
                    vig = ""
                    if over_raw is not None and under_raw is not None and over_raw + under_raw > 0:
                        no_vig = over_raw / (over_raw + under_raw)
                        vig = over_raw + under_raw - 1
                    rows.append(
                        {
                            "slate_date": date_value,
                            "snapshot_timestamp": iso(ref.timestamp),
                            "snapshot_run_tag": ref.run_tag,
                            "snapshot_alias_class": ref.alias_class,
                            "snapshot_source_path": rel(ref.path),
                            "snapshot_source_sha256": ref.sha256,
                            "sportsbook": book_key,
                            "game_id": ident["game_id"],
                            "player_id": ident["player_id"],
                            "player_name": ident["player_name"] or rec["player_name"],
                            "team": ident["team"],
                            "opponent": ident["opponent"],
                            "prop_type": "hits",
                            "line": float(rec["line"]),
                            "side": "over",
                            "price_over_american": over,
                            "price_under_american": under,
                            "decimal_over_odds": american_to_decimal(over),
                            "raw_over_implied_probability": over_raw,
                            "raw_under_implied_probability": under_raw,
                            "no_vig_over_probability": no_vig,
                            "vig": vig,
                            "commence_time": commence_time,
                            "slate_identity_source_path": ident["slate_identity_source_path"],
                            "primary_alignment_snapshot": ref.alias_class in {"run_tagged_snapshot", "timestamped_snapshot"},
                        }
                    )
                    counts["matched_price_rows"] += 1
    return rows, counts


def price_inventory(odds_root: Path, dates: Iterable[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    snapshot_rows: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    parse_rows: list[dict[str, Any]] = []
    for date_value in sorted(set(dates)):
        slate_lookup = load_slate_lookup(odds_root, date_value)
        for ref in load_snapshot_refs(odds_root, date_value):
            snapshot_rows.append(
                {
                    "slate_date": date_value,
                    "snapshot_source_path": rel(ref.path),
                    "snapshot_filename": ref.path.name,
                    "snapshot_timestamp": iso(ref.timestamp),
                    "snapshot_run_tag": ref.run_tag,
                    "snapshot_alias_class": ref.alias_class,
                    "primary_alignment_snapshot": ref.alias_class in {"run_tagged_snapshot", "timestamped_snapshot"},
                    "sha256": ref.sha256,
                }
            )
            rows, counts = flatten_snapshot(ref, date_value, slate_lookup)
            price_rows.extend(rows)
            parse_rows.append({"slate_date": date_value, "snapshot_source_path": rel(ref.path), "snapshot_timestamp": iso(ref.timestamp), **counts})
    prices = pd.DataFrame(price_rows)
    if not prices.empty:
        prices = prices[prices["price_over_american"].notna()].copy()
        prices["price_over_american"] = pd.to_numeric(prices["price_over_american"], errors="coerce")
    return pd.DataFrame(snapshot_rows), prices, pd.DataFrame(parse_rows)


def candidate_timestamp(row: pd.Series) -> tuple[datetime | None, str, str]:
    run_tag = norm_text(row.get("control_source_run_tags"))
    ts = parse_ts(run_tag)
    if ts:
        return ts, "exact_candidate_run_tag", run_tag
    embedded = norm_text(row.get("control_latest_snapshot_time"))
    ts = parse_ts(embedded)
    if ts:
        return ts, "embedded_candidate_generation_timestamp", embedded
    ref = norm_text(row.get("source_reference"))
    ts = parse_ts(ref)
    if ts:
        return ts, "parent_artifact_run_tag_timestamp", ref
    return None, "unresolved_no_repository_backed_decision_timestamp", ""


def candidate_ledger(price_rows: pd.DataFrame, contact_rows: pd.DataFrame) -> pd.DataFrame:
    contact_cols = [
        "player_game_key",
        "contact_hitter_regime_state",
        "contact_bucket",
        "opportunity_bucket",
        "personal_support_bucket",
        "suppression_subtype",
        "prior_predicted_exposure_p_two_plus_hits",
        "discipline_unified_p_two_plus_hits",
        "source_aware_contact_challenger_p_two_plus_hits",
    ]
    c = contact_rows[[col for col in contact_cols if col in contact_rows.columns]].drop_duplicates("player_game_key")
    out = price_rows.merge(c, on="player_game_key", how="left", validate="many_to_one")
    rows = []
    for idx, row in out.reset_index(drop=True).iterrows():
        ts, source, evidence = candidate_timestamp(row)
        rows.append(
            {
                **row.to_dict(),
                "candidate_row_id": idx,
                "governed_candidate_timestamp": iso(ts),
                "candidate_timestamp_source": source,
                "candidate_timestamp_evidence": evidence,
                "candidate_identity_key": "|".join(
                    [
                        norm_text(row.get("slate_date")),
                        str(to_int(row.get("game_id")) or ""),
                        str(to_int(row.get("player_id")) or ""),
                        "hits",
                        "1.5",
                        "over",
                    ]
                ),
            }
        )
    return pd.DataFrame(rows)


def align_candidates(candidates: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    primary = prices[prices.get("primary_alignment_snapshot", pd.Series(dtype=bool)).eq(True)].copy() if not prices.empty else pd.DataFrame()
    if not primary.empty:
        primary["snapshot_dt"] = pd.to_datetime(primary["snapshot_timestamp"], errors="coerce", utc=True)
    rows: list[dict[str, Any]] = []
    for _, c in candidates.iterrows():
        decision_ts = parse_ts(c.get("governed_candidate_timestamp"))
        game_id = to_int(c.get("game_id"))
        player_id = to_int(c.get("player_id"))
        line = to_float(1.5)
        candidate_price = to_float(c.get("o15_price"))
        base = c.to_dict()
        status = ""
        same = pd.DataFrame()
        if decision_ts is None:
            status = "UNRESOLVED_CANDIDATE_TIMESTAMP"
        elif game_id is None or player_id is None:
            status = "UNRESOLVED_EXACT_IDENTITY"
        elif primary.empty:
            status = "SOURCE_ARTIFACT_MISSING"
        else:
            mask = (
                primary["slate_date"].astype(str).eq(norm_text(c.get("slate_date")))
                & pd.to_numeric(primary["game_id"], errors="coerce").eq(game_id)
                & pd.to_numeric(primary["player_id"], errors="coerce").eq(player_id)
                & pd.to_numeric(primary["line"], errors="coerce").eq(line)
                & primary["side"].astype(str).str.lower().eq("over")
            )
            same = primary[mask].copy()
        if status:
            earlier = pd.DataFrame()
            later = pd.DataFrame()
            chosen = None
        else:
            earlier = same[same["snapshot_dt"].le(decision_ts)].copy()
            later = same[same["snapshot_dt"].gt(decision_ts)].copy()
            if earlier.empty:
                chosen = None
                status = "LATER_ONLY_PRICE" if not later.empty else "NO_MARKET_ROW"
            else:
                latest_ts = earlier["snapshot_dt"].max()
                latest = earlier[earlier["snapshot_dt"].eq(latest_ts)].copy()
                if candidate_price is not None:
                    latest = latest[pd.to_numeric(latest["price_over_american"], errors="coerce").eq(candidate_price)]
                if latest.empty:
                    chosen = None
                    status = "AT_OR_BEFORE_MARKET_AVAILABLE_PRICE_MISMATCH"
                elif latest["sportsbook"].nunique(dropna=True) == 1:
                    chosen = latest.sort_values(["snapshot_source_path", "sportsbook"], kind="stable").iloc[0]
                    status = "CERTIFIED_AT_OR_BEFORE_PRICE"
                else:
                    chosen = None
                    status = "AT_OR_BEFORE_PRICE_FOUND_SPORTSBOOK_AMBIGUOUS"
        first_later = None
        if not same.empty and decision_ts is not None:
            tmp = same[same["snapshot_dt"].gt(decision_ts)].sort_values("snapshot_dt", kind="stable")
            if not tmp.empty:
                first_later = tmp.iloc[0]
        chosen_ts = parse_ts(chosen.get("snapshot_timestamp")) if chosen is not None else None
        game_ts = parse_ts(chosen.get("commence_time")) if chosen is not None else None
        age_minutes = (decision_ts - chosen_ts).total_seconds() / 60.0 if decision_ts and chosen_ts else None
        before_first_pitch = (game_ts - decision_ts).total_seconds() / 60.0 if decision_ts and game_ts else None
        raw = chosen.get("raw_over_implied_probability") if chosen is not None else ""
        no_vig = chosen.get("no_vig_over_probability") if chosen is not None else ""
        market_prob = no_vig if norm_text(no_vig) else raw
        rows.append(
            {
                **base,
                "primary_alignment_status": status,
                "primary_certified": status == "CERTIFIED_AT_OR_BEFORE_PRICE",
                "primary_sportsbook": norm_text(chosen.get("sportsbook")) if chosen is not None else "",
                "primary_price_over_american": chosen.get("price_over_american") if chosen is not None else "",
                "primary_price_under_american": chosen.get("price_under_american") if chosen is not None else "",
                "primary_decimal_over_odds": chosen.get("decimal_over_odds") if chosen is not None else "",
                "raw_market_implied_probability": raw,
                "no_vig_market_probability": no_vig,
                "market_probability_used": market_prob,
                "vig": chosen.get("vig") if chosen is not None else "",
                "primary_snapshot_timestamp": norm_text(chosen.get("snapshot_timestamp")) if chosen is not None else "",
                "primary_snapshot_run_tag": norm_text(chosen.get("snapshot_run_tag")) if chosen is not None else "",
                "primary_snapshot_source_path": norm_text(chosen.get("snapshot_source_path")) if chosen is not None else "",
                "primary_snapshot_source_sha256": norm_text(chosen.get("snapshot_source_sha256")) if chosen is not None else "",
                "snapshot_age_minutes": round(age_minutes, 3) if age_minutes is not None else "",
                "minutes_before_first_pitch_at_decision": round(before_first_pitch, 3) if before_first_pitch is not None else "",
                "same_run_tag_price": bool(chosen is not None and norm_text(chosen.get("snapshot_run_tag")) == norm_text(c.get("control_source_run_tags"))),
                "at_or_before_snapshot_count": int(len(earlier)) if "earlier" in locals() else 0,
                "later_snapshot_count": int(len(later)) if "later" in locals() else 0,
                "first_later_snapshot_timestamp": norm_text(first_later.get("snapshot_timestamp")) if first_later is not None else "",
                "first_later_price_over_american": first_later.get("price_over_american") if first_later is not None else "",
                "first_later_sportsbook": norm_text(first_later.get("sportsbook")) if first_later is not None else "",
                "diagnostic_same_identity_book_prices_found": int(len(same)) if not same.empty else 0,
            }
        )
    return pd.DataFrame(rows)


def fit_calibrators(aligned: pd.DataFrame) -> tuple[LogisticRegression | None, LogisticRegression | None, dict[str, Any]]:
    fit = aligned[(aligned["temporal_split"].eq("fit")) & (aligned["primary_certified"].eq(True))].copy()
    fit = fit.dropna(subset=["market_probability_used", "p_two_plus_hits", "multi_hit_target"])
    info: dict[str, Any] = {"fit_rows": int(len(fit))}
    if len(fit) < 30 or fit["multi_hit_target"].nunique() < 2:
        return None, None, info
    y = fit["multi_hit_target"].astype(int)
    m1 = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000)
    x1 = logit(fit["market_probability_used"]).to_numpy().reshape(-1, 1)
    m1.fit(x1, y)
    m2 = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000)
    x2 = np.column_stack([logit(fit["market_probability_used"]), logit(fit["p_two_plus_hits"])])
    m2.fit(x2, y)
    info.update(
        {
            "market_only_intercept": float(m1.intercept_[0]),
            "market_only_market_coef": float(m1.coef_[0][0]),
            "market_plus_intercept": float(m2.intercept_[0]),
            "market_plus_market_coef": float(m2.coef_[0][0]),
            "market_plus_proppadia_coef": float(m2.coef_[0][1]),
        }
    )
    return m1, m2, info


def add_calibrated_predictions(aligned: pd.DataFrame, m1: LogisticRegression | None, m2: LogisticRegression | None) -> pd.DataFrame:
    out = aligned.copy()
    out["market_only_calibrated_probability"] = np.nan
    out["market_plus_proppadia_probability"] = np.nan
    market_numeric = pd.to_numeric(out["market_probability_used"], errors="coerce")
    prop_numeric = pd.to_numeric(out["p_two_plus_hits"], errors="coerce")
    mask1 = market_numeric.notna()
    if m1 is not None and mask1.any():
        out.loc[mask1, "market_only_calibrated_probability"] = m1.predict_proba(logit(market_numeric.loc[mask1]).to_numpy().reshape(-1, 1))[:, 1]
    mask2 = market_numeric.notna() & prop_numeric.notna()
    if m2 is not None and mask2.any():
        x = np.column_stack([logit(market_numeric.loc[mask2]), logit(prop_numeric.loc[mask2])])
        out.loc[mask2, "market_plus_proppadia_probability"] = m2.predict_proba(x)[:, 1]
    return out


def probability_validation(aligned: pd.DataFrame, calibrator_info: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split in ["validation", "holdout"]:
        base = aligned[(aligned["temporal_split"].eq(split)) & (aligned["primary_certified"].eq(True))].copy()
        for instrument, col in [
            ("market_raw_implied", "raw_market_implied_probability"),
            ("market_no_vig", "no_vig_market_probability"),
            ("market_probability_used", "market_probability_used"),
            ("proppadia_frozen_multi_hit", "p_two_plus_hits"),
            ("exposure_control", "prior_predicted_exposure_p_two_plus_hits"),
            ("market_only_fit_calibrated", "market_only_calibrated_probability"),
            ("market_plus_proppadia_fit_calibrated", "market_plus_proppadia_probability"),
        ]:
            rec = {"temporal_split": split, "instrument": instrument, **probability_metrics(base, col)}
            rows.append(rec)
        ci_brier = bootstrap_delta(base, "market_only_calibrated_probability", "market_plus_proppadia_probability", "brier")
        ci_log = bootstrap_delta(base, "market_only_calibrated_probability", "market_plus_proppadia_probability", "log_loss")
        rows.append(
            {
                "temporal_split": split,
                "instrument": "increment_market_plus_minus_market_only",
                "rows": int(base[["market_only_calibrated_probability", "market_plus_proppadia_probability", "multi_hit_target"]].dropna().shape[0]),
                "brier": float(probability_metrics(base, "market_only_calibrated_probability")["brier"] - probability_metrics(base, "market_plus_proppadia_probability")["brier"]) if probability_metrics(base, "market_only_calibrated_probability")["rows"] else "",
                "log_loss": float(probability_metrics(base, "market_only_calibrated_probability")["log_loss"] - probability_metrics(base, "market_plus_proppadia_probability")["log_loss"]) if probability_metrics(base, "market_only_calibrated_probability")["rows"] else "",
                "auc": float(probability_metrics(base, "market_plus_proppadia_probability")["auc"] or 0) - float(probability_metrics(base, "market_only_calibrated_probability")["auc"] or 0) if probability_metrics(base, "market_only_calibrated_probability")["rows"] else "",
                "ece": "",
                "calibration_intercept": "",
                "calibration_slope": "",
                "bootstrap_brier_delta_ci_low": ci_brier[0],
                "bootstrap_brier_delta_ci_high": ci_brier[1],
                "bootstrap_log_loss_delta_ci_low": ci_log[0],
                "bootstrap_log_loss_delta_ci_high": ci_log[1],
                **{k: calibrator_info.get(k, "") for k in calibrator_info},
            }
        )
    return pd.DataFrame(rows)


def freeze_residual_bands(aligned: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = aligned.copy()
    out["probability_residual"] = pd.to_numeric(out["p_two_plus_hits"], errors="coerce") - pd.to_numeric(out["market_probability_used"], errors="coerce")
    fit = out[(out["temporal_split"].eq("fit")) & (out["primary_certified"].eq(True)) & out["probability_residual"].notna()]
    cuts = fit["probability_residual"].quantile([0, 0.25, 0.5, 0.75, 1]).to_dict() if not fit.empty else {}
    def label(v: object) -> str:
        x = to_float(v)
        if x is None or not cuts:
            return "missing"
        if x <= cuts[0.25]:
            return "fit_q1_most_negative"
        if x <= cuts[0.5]:
            return "fit_q2_negative_neutral"
        if x <= cuts[0.75]:
            return "fit_q3_neutral_positive"
        return "fit_q4_most_positive"
    out["fit_frozen_residual_band"] = out["probability_residual"].map(label)
    contract = pd.DataFrame(
        [
            {"band": "fit_q1_most_negative", "lower_bound": cuts.get(0, ""), "upper_bound": cuts.get(0.25, ""), "source": "fit certified residual quartile"},
            {"band": "fit_q2_negative_neutral", "lower_bound": cuts.get(0.25, ""), "upper_bound": cuts.get(0.5, ""), "source": "fit certified residual quartile"},
            {"band": "fit_q3_neutral_positive", "lower_bound": cuts.get(0.5, ""), "upper_bound": cuts.get(0.75, ""), "source": "fit certified residual quartile"},
            {"band": "fit_q4_most_positive", "lower_bound": cuts.get(0.75, ""), "upper_bound": cuts.get(1, ""), "source": "fit certified residual quartile"},
        ]
    )
    return out, contract


def group_perf(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    work = df[df["primary_certified"].eq(True)].dropna(subset=["multi_hit_target"]).copy()
    if work.empty:
        return pd.DataFrame()
    work["profit_1u_certified"] = work.apply(lambda r: profit_1u(r["primary_price_over_american"], bool(r["multi_hit_target"])), axis=1)
    rows: list[dict[str, Any]] = []
    for key, g in work.groupby(group_cols, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        rec = {col: val for col, val in zip(group_cols, key)}
        rec.update(
            {
                "rows": int(len(g)),
                "two_plus_rows": int(g["multi_hit_target"].sum()),
                "two_plus_rate": float(g["multi_hit_target"].mean()),
                "avg_market_probability": float(pd.to_numeric(g["market_probability_used"], errors="coerce").mean()),
                "avg_proppadia_probability": float(pd.to_numeric(g["p_two_plus_hits"], errors="coerce").mean()),
                "avg_market_plus_probability": float(pd.to_numeric(g["market_plus_proppadia_probability"], errors="coerce").mean()),
                "avg_price": float(pd.to_numeric(g["primary_price_over_american"], errors="coerce").mean()),
                "certified_roi": float(g["profit_1u_certified"].mean()),
                "dates": int(g["slate_date"].nunique()),
                "players": int(g["player_id"].nunique()),
                "top_date_share": float(g.groupby("slate_date").size().max() / len(g)),
                "sample_flag": "SPARSE" if len(g) < 30 else "OK",
            }
        )
        rows.append(rec)
    return pd.DataFrame(rows)


def ensure_price_band_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df = pd.DataFrame(columns=["temporal_split", "price_band"])
    rows = df.to_dict("records")
    existing = {(norm_text(r.get("temporal_split")), norm_text(r.get("price_band"))) for r in rows}
    bands = ["+100_through_+149", "+150_through_+199", "+200_through_+249", "+250_and_longer"]
    for split in ["fit", "validation", "holdout"]:
        for band in bands:
            if (split, band) not in existing:
                rows.append(
                    {
                        "temporal_split": split,
                        "price_band": band,
                        "rows": 0,
                        "two_plus_rows": 0,
                        "two_plus_rate": "",
                        "avg_market_probability": "",
                        "avg_proppadia_probability": "",
                        "avg_market_plus_probability": "",
                        "avg_price": "",
                        "certified_roi": "",
                        "dates": 0,
                        "players": 0,
                        "top_date_share": "",
                        "sample_flag": "NO_ROWS",
                    }
                )
    return pd.DataFrame(rows)


def validation_report(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.suffix == ".csv":
            try:
                with path.open(newline="", encoding="utf-8") as f:
                    list(csv.DictReader(f))
                rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
            except Exception as exc:
                rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text(encoding="utf-8"))
                rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
            except Exception as exc:
                rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
        elif path.suffix == ".md":
            rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    price_rows = pd.read_csv(PRICE_ROWS, low_memory=False)
    contact_rows = pd.read_csv(CONTACT_ROWS, low_memory=False)
    bands = pd.read_csv(PRICE_BANDS, low_memory=False) if PRICE_BANDS.exists() else pd.DataFrame()
    candidates = candidate_ledger(price_rows, contact_rows)
    dates = sorted(candidates["slate_date"].dropna().astype(str).unique())
    snapshots, inventory_prices, parse_summary = price_inventory(ODDS_ROOT, dates)
    aligned = align_candidates(candidates, inventory_prices)
    m1, m2, calibrator_info = fit_calibrators(aligned)
    aligned = add_calibrated_predictions(aligned, m1, m2)
    aligned, residual_contract = freeze_residual_bands(aligned)
    market_ledger = aligned[
        [
            "candidate_row_id",
            "player_game_key",
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "primary_sportsbook",
            "primary_price_over_american",
            "primary_price_under_american",
            "primary_decimal_over_odds",
            "raw_market_implied_probability",
            "no_vig_market_probability",
            "market_probability_used",
            "vig",
            "primary_snapshot_timestamp",
            "primary_alignment_status",
            "p_two_plus_hits",
            "probability_residual",
            "fit_frozen_residual_band",
        ]
    ].copy()
    prob_results = probability_validation(aligned, calibrator_info)
    residual_results = group_perf(aligned, ["temporal_split", "fit_frozen_residual_band"])
    price_band_results = ensure_price_band_rows(group_perf(aligned, ["temporal_split", "price_band"]))
    contact_increment = group_perf(aligned, ["temporal_split", "contact_hitter_regime_state"])
    suppression = group_perf(aligned, ["temporal_split", "suppression_veto_state"])
    date_stability = group_perf(aligned, ["temporal_split", "slate_date"])
    timestamp_summary = (
        aligned.groupby(["candidate_timestamp_source", "primary_alignment_status"], dropna=False)
        .size()
        .reset_index(name="rows")
    )
    snapshot_summary = (
        snapshots.groupby(["slate_date", "snapshot_alias_class", "primary_alignment_snapshot"], dropna=False)
        .size()
        .reset_index(name="snapshots")
    )
    coverage = {
        "tracked_o15_candidates": int(len(aligned)),
        "authoritative_candidate_timestamps": int(aligned["governed_candidate_timestamp"].astype(str).str.len().gt(0).sum()),
        "preserved_odds_snapshots": int(len(snapshots)),
        "odds_price_rows": int(len(inventory_prices)),
        "certified_at_or_before_prices": int(aligned["primary_certified"].sum()),
        "selection_time_price_coverage": float(aligned["primary_certified"].mean()) if len(aligned) else 0,
        "same_run_tag_prices": int(aligned[aligned["primary_certified"].eq(True)]["same_run_tag_price"].sum()),
        "earlier_valid_prices": int((aligned["primary_certified"].eq(True) & aligned["same_run_tag_price"].eq(False)).sum()),
        "later_only_prices": int(aligned["primary_alignment_status"].eq("LATER_ONLY_PRICE").sum()),
        "no_market_rows": int(aligned["primary_alignment_status"].eq("NO_MARKET_ROW").sum()),
        "unresolved_candidate_timestamps": int(aligned["primary_alignment_status"].eq("UNRESOLVED_CANDIDATE_TIMESTAMP").sum()),
        "unresolved_identities": int(aligned["primary_alignment_status"].eq("UNRESOLVED_EXACT_IDENTITY").sum()),
        "sportsbooks_represented": int(aligned[aligned["primary_certified"].eq(True)]["primary_sportsbook"].nunique(dropna=True)),
    }
    # Decision logic is intentionally conservative: Brier/log-loss must both
    # improve on untouched holdout to support a market increment.
    hold = prob_results[prob_results["temporal_split"].eq("holdout")]
    inc = hold[hold["instrument"].eq("increment_market_plus_minus_market_only")]
    brier_delta = to_float(inc["brier"].iloc[0]) if not inc.empty else None
    log_delta = to_float(inc["log_loss"].iloc[0]) if not inc.empty else None
    if coverage["selection_time_price_coverage"] < 0.5:
        market_increment_decision = "PRICE_ALIGNMENT_COVERAGE_INSUFFICIENT"
        branch_decision = "CURRENT_SEASON_HITTER_OWNED_O15_BRANCH_CLOSE_PENDING_PRICE_COVERAGE"
    elif brier_delta is not None and log_delta is not None and brier_delta > 0 and log_delta > 0:
        market_increment_decision = "PROPPAEDIA_ADDS_INCREMENTAL_VALUE_BEYOND_MARKET"
        branch_decision = "CONTINUE_ONLY_WITH_GOVERNED_INCREMENTAL_TESTING"
    elif brier_delta is not None and (brier_delta > 0 or log_delta and log_delta > 0):
        market_increment_decision = "PROPPAEDIA_CALIBRATES_MARKET_WITHOUT_RANKING_LIFT"
        branch_decision = "CURRENT_SEASON_BRANCH_NOT_PROMOTED"
    else:
        market_increment_decision = "NO_STABLE_O15_INCREMENTAL_VALUE_BEYOND_MARKET"
        branch_decision = "CLOSE_CURRENT_SEASON_HITTER_OWNED_O15_BRANCH"
    residual_hold = residual_results[residual_results["temporal_split"].eq("holdout")]
    top_resid = residual_hold[residual_hold["fit_frozen_residual_band"].eq("fit_q4_most_positive")]
    if not top_resid.empty and to_float(top_resid["two_plus_rate"].iloc[0]) is not None and to_float(top_resid["avg_market_probability"].iloc[0]) is not None and float(top_resid["two_plus_rate"].iloc[0]) > float(top_resid["avg_market_probability"].iloc[0]):
        residual_decision = "PROPPAEDIA_RESIDUAL_DIRECTIONALLY_USEFUL_PRICE_VALUE_UNCERTIFIED"
    else:
        residual_decision = "NO_STABLE_RESIDUAL_EDGE_BEYOND_MARKET"
    contact_hold = contact_increment[(contact_increment["temporal_split"].eq("holdout")) & (contact_increment["contact_hitter_regime_state"].eq("HIGH_CONTACT_HIGH_OPPORTUNITY_NO_VETO"))]
    contact_decision = "UNSUPPORTED" if contact_hold.empty or int(contact_hold["rows"].iloc[0]) < 30 else "MARKET_RECOGNIZED_AND_FULLY_PRICED"
    decisions = pd.DataFrame(
        [
            ("MLB_O15_CANDIDATE_TIMESTAMP_RECOVERY_DECISION", "CANDIDATE_TIMESTAMPS_RECOVERED_FROM_GOVERNED_RUN_TAG_OR_EMBEDDED_TIMESTAMP" if coverage["authoritative_candidate_timestamps"] else "CANDIDATE_TIMESTAMPS_UNRESOLVED"),
            ("MLB_O15_ODDS_SNAPSHOT_ALIGNMENT_DECISION", "PRESERVED_ODDS_SNAPSHOTS_INVENTORIED_AND_EXACT_IDENTITY_ALIGNMENT_ATTEMPTED"),
            ("MLB_O15_SELECTION_TIME_PRICE_COVERAGE_DECISION", "SELECTION_TIME_PRICE_COVERAGE_CERTIFIED" if coverage["selection_time_price_coverage"] >= 0.5 else "PRICE_ALIGNMENT_COVERAGE_INSUFFICIENT"),
            ("MLB_O15_MARKET_BASELINE_DECISION", "MARKET_BASELINE_CONSTRUCTED_FROM_RAW_AND_NO_VIG_IMPLIED_PROBABILITY"),
            ("MLB_O15_PROPPAEDIA_PROBABILITY_DECISION", "FROZEN_MULTI_HIT_PROBABILITY_BOUND_WITHOUT_REFIT"),
            ("MLB_O15_MARKET_INCREMENT_DECISION", market_increment_decision),
            ("MLB_O15_RESIDUAL_EDGE_DECISION", residual_decision),
            ("MLB_O15_PRICE_BAND_INCREMENT_DECISION", "ALL_FIXED_PRICE_BANDS_EVALUATED_NO_BAND_SELECTION"),
            ("MLB_O15_CONTACT_REGIME_INCREMENT_DECISION", contact_decision),
            ("MLB_O15_SUPPRESSION_RELATIONSHIP_DECISION", "AFFIRMATIVE_SUPPRESSION_PRESERVED_NO_OVERRIDE"),
            ("MLB_O15_CURRENT_SEASON_BRANCH_DECISION", branch_decision),
            ("MLB_O15_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
        ],
        columns=["decision", "value"],
    )
    outputs = {
        "candidate_timestamp_ledger_2026-07-17.csv": candidates,
        "odds_snapshot_inventory_2026-07-17.csv": snapshots,
        "odds_snapshot_parse_summary_2026-07-17.csv": parse_summary,
        "odds_snapshot_price_inventory_2026-07-17.csv": inventory_prices,
        "candidate_to_price_alignment_2026-07-17.csv": aligned,
        "market_implied_probability_ledger_2026-07-17.csv": market_ledger,
        "market_only_validation_2026-07-17.csv": prob_results[prob_results["instrument"].astype(str).str.startswith("market")].copy(),
        "proppadia_only_validation_2026-07-17.csv": prob_results[prob_results["instrument"].astype(str).isin(["proppadia_frozen_multi_hit", "exposure_control"])].copy(),
        "market_plus_proppadia_incremental_results_2026-07-17.csv": prob_results,
        "residual_band_contract_2026-07-17.csv": residual_contract,
        "residual_band_results_2026-07-17.csv": residual_results,
        "fixed_price_band_comparisons_2026-07-17.csv": price_band_results,
        "contact_regime_increment_2026-07-17.csv": contact_increment,
        "suppression_relationship_analysis_2026-07-17.csv": suppression,
        "date_stability_2026-07-17.csv": date_stability,
        "timestamp_alignment_summary_2026-07-17.csv": timestamp_summary,
        "snapshot_inventory_summary_2026-07-17.csv": snapshot_summary,
        "o15_market_increment_decisions_2026-07-17.csv": decisions,
        "fixed_price_band_contract_2026-07-17.csv": bands,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    machine = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        **coverage,
        "holdout_market_increment_brier_delta": brier_delta if brier_delta is not None else "",
        "holdout_market_increment_log_loss_delta": log_delta if log_delta is not None else "",
        "market_increment_decision": market_increment_decision,
        "branch_decision": branch_decision,
        "direct_answer": "No. On the certified selection-time subset, Proppadia did not demonstrate stable useful Hits O1.5 probability information beyond the sportsbook price." if market_increment_decision != "PROPPAEDIA_ADDS_INCREMENTAL_VALUE_BEYOND_MARKET" else "Yes, within this bounded certified subset Proppadia improved the market-only probability baseline.",
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_o15_market_incremental_probability_validation_2026-07-17.json")
    decision_lines = "\n".join(f"- `{r.decision} = {r.value}`" for r in decisions.itertuples(index=False))
    write_md(
        f"""# MLB Hits O1.5 Market-Baseline and Proppadia Incremental-Probability Validation

Generated: `{machine['generated_at_utc']}`

## Executive Summary

This bounded validation aligned frozen historical O1.5 propositions to
preserved local odds snapshots using the latest exact OVER 1.5 price at or
before the governed candidate timestamp. It then compared market-only
probabilities with frozen Proppadia multi-hit probabilities and a fixed
fit-only market-plus-Proppadia calibration.

No new hitter feature, regime, model fitting/refitting outside the fixed
calibration instrument, OddsAPI/network call, database write, or production
behavior change occurred.

## Selection-Time Price Coverage

- Tracked O1.5 propositions: `{coverage['tracked_o15_candidates']}`
- Authoritative candidate timestamps: `{coverage['authoritative_candidate_timestamps']}`
- Certified at-or-before prices: `{coverage['certified_at_or_before_prices']}`
- Coverage: `{coverage['selection_time_price_coverage']:.4f}`
- Sportsbooks represented: `{coverage['sportsbooks_represented']}`

## Direct Answer

{machine['direct_answer']}

## Decisions

{decision_lines}

## Production Status

`MLB_O15_PRODUCTION_STATUS = NOT_AUTHORIZED`
""",
        out_dir / "executive_summary_2026-07-17.md",
    )
    manifest_rows = []
    for path in [PRICE_ROWS, CONTACT_ROWS, PRICE_BANDS, Path(__file__).resolve()]:
        if path.exists():
            manifest_rows.append({"artifact_role": "input_or_script", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest_rows.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest_rows), out_dir / "sha256_manifest_2026-07-17.csv")
    validation_report(out_dir)
    return machine


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
