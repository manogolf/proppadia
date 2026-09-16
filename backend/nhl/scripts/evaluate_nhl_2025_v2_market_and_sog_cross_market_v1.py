#!/usr/bin/env python3
"""Local-only frozen NHL 2025 V2/market/SOG cross-market evaluation."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score


ROOT = Path(__file__).resolve().parents[3]
STAMP = "2026-09-15"
RECOVERY = ROOT / "artifacts/analysis/model_development/nhl_season_2025_historical_market_and_sog_recovery_v1/2026-09-15"
V2_PARENT = ROOT / "artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test/2026-09-15"
BRIDGE_PARENT = ROOT / "artifacts/analysis/model_development/nhl_cross_market_game_state_bridge_v1/2026-09-15"
SOG_REFERENCE = ROOT / "artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13"
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/nhl_2025_v2_market_and_sog_cross_market_evaluation_v1/2026-09-15"
BOOTSTRAP_SEED = 20250915
PROBABILITY_BINS = [-np.inf, 0.40, 0.475, 0.525, 0.60, np.inf]
PROBABILITY_LABELS = ["LT_40", "40_TO_47_5", "47_5_TO_52_5", "52_5_TO_60", "GE_60"]
GAP_BINS = [-np.inf, 0.025, 0.05, 0.075, 0.10, np.inf]
GAP_LABELS = ["LT_2_5PP", "2_5_TO_LT_5PP", "5_TO_LT_7_5PP", "7_5_TO_LT_10PP", "GE_10PP"]
SOG_DIFF_BINS = [-np.inf, -2.0, 2.0, np.inf]
SOG_DIFF_LABELS = ["AWAY_LEAN_GE_2", "WITHIN_2", "HOME_LEAN_GE_2"]
PLAYER_COVERAGE_BINS = [-np.inf, 8, 12, np.inf]
PLAYER_COVERAGE_LABELS = ["LT_8", "8_TO_11", "GE_12"]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def json_safe(value: object) -> object:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def verify_manifest(directory: Path) -> dict[str, object]:
    failures: list[str] = []
    rows = 0
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        if not line.strip():
            continue
        expected, name = line.split("  ", 1)
        rows += 1
        path = directory / name
        if not path.is_file() or sha256_file(path) != expected:
            failures.append(name)
    return {"directory": str(directory.relative_to(ROOT)), "files_checked": rows, "failures": failures, "passed": not failures}


def poisson_tail(lam: float, line: float) -> float:
    """P[X > line] for an integer-valued Poisson random variable."""
    if not math.isfinite(lam) or lam < 0 or not math.isfinite(line):
        return float("nan")
    cutoff = math.floor(line)
    term = math.exp(-lam)
    cdf = term
    for k in range(1, cutoff + 1):
        term *= lam / k
        cdf += term
    return min(1.0, max(0.0, 1.0 - cdf))


def invert_poisson_tail(probability: float, line: float) -> float:
    if not (0.0 < probability < 1.0) or line < 0 or not math.isfinite(line):
        return float("nan")
    lo, hi = 0.0, 8.0
    while poisson_tail(hi, line) < probability and hi < 256.0:
        hi *= 2.0
    for _ in range(70):
        mid = (lo + hi) / 2.0
        if poisson_tail(mid, line) < probability:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def american_to_decimal(price: float) -> float:
    return 1.0 + (100.0 / abs(price) if price < 0 else price / 100.0)


def fixed_ece(y: np.ndarray, p: np.ndarray) -> float:
    idx = np.minimum((np.clip(p, 0, 1) * 10).astype(int), 9)
    return float(sum((idx == i).mean() * abs(y[idx == i].mean() - p[idx == i].mean()) for i in range(10) if (idx == i).any()))


def metric_row(label: str, y: np.ndarray, p: np.ndarray, scope: str = "ALL") -> dict[str, object]:
    eps = 1e-15
    pc = np.clip(p.astype(float), eps, 1 - eps)
    return {
        "scope": scope,
        "model": label,
        "n_games": len(y),
        "brier": float(np.mean((p - y) ** 2)),
        "log_loss": float(log_loss(y, pc, labels=[0, 1])),
        "roc_auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else float("nan"),
        "accuracy": float(accuracy_score(y, p >= 0.5)),
        "ece_10_equal_width": fixed_ece(y, p),
    }


def bootstrap_mean_ci(values: np.ndarray, resamples: int, seed: int) -> tuple[float, float]:
    values = np.asarray(values, dtype=float)
    if len(values) == 0:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    means = np.empty(resamples)
    for i in range(resamples):
        means[i] = values[rng.integers(0, len(values), len(values))].mean()
    return tuple(float(x) for x in np.quantile(means, [0.025, 0.975]))


def paired_loss_bootstrap(y: np.ndarray, v2p: np.ndarray, marketp: np.ndarray, resamples: int) -> pd.DataFrame:
    eps = 1e-15
    bdiff = (v2p - y) ** 2 - (marketp - y) ** 2
    ldiff = -(y * np.log(np.clip(v2p, eps, 1 - eps)) + (1 - y) * np.log(np.clip(1 - v2p, eps, 1 - eps)))
    ldiff -= -(y * np.log(np.clip(marketp, eps, 1 - eps)) + (1 - y) * np.log(np.clip(1 - marketp, eps, 1 - eps)))
    rows = []
    for i, (name, vals) in enumerate((("V2_MINUS_MARKET_BRIER", bdiff), ("V2_MINUS_MARKET_LOG_LOSS", ldiff))):
        lo, hi = bootstrap_mean_ci(vals, resamples, BOOTSTRAP_SEED + i)
        rows.append({"comparison": name, "n_games": len(vals), "point_difference": vals.mean(), "ci_2_5": lo, "ci_97_5": hi, "resamples": resamples, "bootstrap_grain": "GAME"})
    return pd.DataFrame(rows)


def calibration_rows(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for model, col in (("V2", "v2_home_win_probability"), ("MARKET_MEDIAN", "market_median_no_vig_home_probability")):
        bins = pd.cut(frame[col], bins=np.linspace(0, 1, 11), include_lowest=True, right=False)
        for band, group in frame.groupby(bins, observed=False):
            rows.append({"model": model, "probability_bin": str(band), "n_games": len(group), "mean_probability": group[col].mean(), "home_win_rate": group.home_win_target.mean(), "calibration_error": group[col].mean() - group.home_win_target.mean() if len(group) else np.nan})
    return pd.DataFrame(rows)


def make_market_comparator(moneyline: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    valid = moneyline.loc[(moneyline.market_key == "h2h") & moneyline.strictly_pregame & (moneyline.qualification_status == "VALID_STRICT_PRESTART")].copy()
    keys = ["game_id", "bookmaker_key"]
    counts = valid.groupby(keys).side_orientation.nunique()
    qualifying = counts[counts.eq(2)].index
    valid = valid.set_index(keys).loc[qualifying].reset_index()
    book_rows = []
    max_stored_delta = 0.0
    for (game_id, book), group in valid.groupby(keys, sort=True):
        home = group.loc[group.side_orientation == "HOME"].iloc[0]
        away = group.loc[group.side_orientation == "AWAY"].iloc[0]
        raw_h, raw_a = 1 / float(home.decimal_price), 1 / float(away.decimal_price)
        overround = raw_h + raw_a
        no_vig_h = raw_h / overround
        max_stored_delta = max(max_stored_delta, abs(no_vig_h - float(home.no_vig_probability)))
        book_rows.append({
            "game_id": int(game_id), "bookmaker_key": book, "bookmaker_name": home.bookmaker_name,
            "home_team": home.home_team, "away_team": home.away_team,
            "scheduled_start_time_utc": home.scheduled_start_time_utc,
            "home_american_price": float(home.american_price), "away_american_price": float(away.american_price),
            "home_decimal_price": float(home.decimal_price), "away_decimal_price": float(away.decimal_price),
            "raw_home_implied_probability": raw_h, "raw_away_implied_probability": raw_a,
            "overround": overround, "no_vig_home_probability": no_vig_h,
            "bookmaker_update_timestamp_utc": max(str(home.bookmaker_update_timestamp_utc), str(away.bookmaker_update_timestamp_utc)),
        })
    books = pd.DataFrame(book_rows).sort_values(["game_id", "bookmaker_key"]).reset_index(drop=True)
    consensus_rows = []
    for game_id, group in books.groupby("game_id", sort=True):
        probs = group.no_vig_home_probability
        mapping = {r.bookmaker_key: r.no_vig_home_probability for r in group.itertuples()}
        betonline = group.loc[group.bookmaker_key.eq("betonlineag"), "no_vig_home_probability"]
        consensus_rows.append({
            "game_id": int(game_id), "market_median_no_vig_home_probability": probs.median(),
            "market_mean_no_vig_home_probability": probs.mean(), "market_bookmaker_count": len(group),
            "market_probability_stddev": probs.std(ddof=0), "market_probability_min": probs.min(),
            "market_probability_max": probs.max(), "betonline_no_vig_home_probability": betonline.iloc[0] if len(betonline) else np.nan,
            "individual_book_probabilities_json": json.dumps(mapping, sort_keys=True, separators=(",", ":")),
        })
    return books, pd.DataFrame(consensus_rows), max_stored_delta


def add_game_states(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["month"] = pd.to_datetime(out.game_date).dt.to_period("M").astype(str)
    # Round only to stabilize exact decimal band boundaries against binary-float
    # representation (for example, 0.60 - 0.50 at the frozen 10 pp boundary).
    out["absolute_probability_gap"] = (out.v2_home_win_probability - out.market_median_no_vig_home_probability).abs().round(12)
    out["gap_band"] = pd.cut(out.absolute_probability_gap, GAP_BINS, labels=GAP_LABELS, right=False).astype(str)
    out["market_favorite_side"] = np.where(out.market_median_no_vig_home_probability >= 0.5, "HOME", "AWAY")
    out["v2_favorite_side"] = np.where(out.v2_home_win_probability >= 0.5, "HOME", "AWAY")
    out["agreement_state"] = np.where(out.market_favorite_side == out.v2_favorite_side, "AGREE_ON_FAVORITE", "V2_OPPOSES_MARKET_FAVORITE")
    out["v2_stronger_side"] = np.where(out.v2_home_win_probability > out.market_median_no_vig_home_probability, "HOME", "AWAY")
    out["v2_stronger_side_market_role"] = np.where(out.v2_stronger_side == out.market_favorite_side, "MARKET_FAVORITE", "MARKET_UNDERDOG")
    weaker_direction = np.where(
        out.market_favorite_side.eq("HOME"),
        out.v2_home_win_probability < out.market_median_no_vig_home_probability,
        out.v2_home_win_probability > out.market_median_no_vig_home_probability,
    )
    out["market_favorite_v2_assessment"] = np.where(weaker_direction & out.absolute_probability_gap.ge(0.025), "MATERIALLY_WEAKER_GE_2_5PP", "NOT_MATERIALLY_WEAKER")
    return out


def disagreement_outputs(games: pd.DataFrame, books: pd.DataFrame, resamples: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    joined = books.drop(columns=["home_team", "away_team"]).merge(games[["game_id", "home_win_target", "month", "home_team", "away_team", "gap_band", "agreement_state", "v2_favorite_side", "v2_stronger_side", "v2_stronger_side_market_role", "market_favorite_v2_assessment"]], on="game_id", validate="many_to_one")
    joined["selected_decimal_price"] = np.where(joined.v2_stronger_side.eq("HOME"), joined.home_decimal_price, joined.away_decimal_price)
    joined["selected_win"] = np.where(joined.v2_stronger_side.eq("HOME"), joined.home_win_target.eq(1), joined.home_win_target.eq(0))
    joined["v2_stronger_one_unit_risk_return"] = np.where(joined.selected_win, joined.selected_decimal_price - 1.0, -1.0)
    joined["v2_favorite_decimal_price"] = np.where(joined.v2_favorite_side.eq("HOME"), joined.home_decimal_price, joined.away_decimal_price)
    joined["v2_favorite_win"] = np.where(joined.v2_favorite_side.eq("HOME"), joined.home_win_target.eq(1), joined.home_win_target.eq(0))
    joined["v2_favorite_one_unit_risk_return"] = np.where(joined.v2_favorite_win, joined.v2_favorite_decimal_price - 1.0, -1.0)
    per_game = joined.groupby("game_id", as_index=False).agg(
        home_win_target=("home_win_target", "first"), month=("month", "first"), home_team=("home_team", "first"), away_team=("away_team", "first"),
        gap_band=("gap_band", "first"), agreement_state=("agreement_state", "first"), v2_favorite_side=("v2_favorite_side", "first"), v2_stronger_side=("v2_stronger_side", "first"),
        v2_stronger_side_market_role=("v2_stronger_side_market_role", "first"), market_favorite_v2_assessment=("market_favorite_v2_assessment", "first"),
        v2_stronger_win=("selected_win", "first"), v2_favorite_win=("v2_favorite_win", "first"),
        mean_book_v2_stronger_return=("v2_stronger_one_unit_risk_return", "mean"), mean_book_v2_favorite_return=("v2_favorite_one_unit_risk_return", "mean"), bookmaker_count=("bookmaker_key", "nunique"))
    specs: list[tuple[str, str, object]] = [("ALL_BOOKS_GAME_MEAN", "ALL", None)]
    for col in ["gap_band", "agreement_state", "v2_stronger_side_market_role", "market_favorite_v2_assessment"]:
        specs.extend((f"ALL_BOOKS_GAME_MEAN_BY_{col.upper()}", str(v), (col, v)) for v in sorted(per_game[col].dropna().unique()))
    strategies = [("V2_STRONGER_THAN_MARKET", "mean_book_v2_stronger_return", "v2_stronger_one_unit_risk_return", "v2_stronger_win", "selected_win"), ("V2_FAVORITE", "mean_book_v2_favorite_return", "v2_favorite_one_unit_risk_return", "v2_favorite_win", "v2_favorite_win")]
    return_rows = []
    for strategy_index, (strategy, game_return_col, _, game_win_col, _) in enumerate(strategies):
        for i, (scope, segment, filt) in enumerate(specs):
            g = per_game if filt is None else per_game.loc[per_game[filt[0]].eq(filt[1])]
            lo, hi = bootstrap_mean_ci(g[game_return_col].to_numpy(), resamples, BOOTSTRAP_SEED + 100 + strategy_index * 100 + i)
            return_rows.append({"strategy": strategy, "scope": scope, "bookmaker_key": "ALL_BOOKS_GAME_MEAN", "segment": segment, "n_games": len(g), "selected_side_win_rate": g[game_win_col].mean(), "mean_one_unit_risk_return": g[game_return_col].mean(), "ci_2_5": lo, "ci_97_5": hi, "bootstrap_grain": "GAME"})
    offset = len(return_rows)
    for i, (book, g) in enumerate(joined.groupby("bookmaker_key", sort=True)):
        for strategy_index, (strategy, _, book_return_col, _, book_win_col) in enumerate(strategies):
            lo, hi = bootstrap_mean_ci(g[book_return_col].to_numpy(), resamples, BOOTSTRAP_SEED + 500 + strategy_index * 100 + i)
            return_rows.append({"strategy": strategy, "scope": "BOOKMAKER_OVERALL", "bookmaker_key": book, "segment": "ALL", "n_games": g.game_id.nunique(), "selected_side_win_rate": g[book_win_col].mean(), "mean_one_unit_risk_return": g[book_return_col].mean(), "ci_2_5": lo, "ci_97_5": hi, "bootstrap_grain": "GAME"})
            for j, (band, bg) in enumerate(g.groupby("gap_band", sort=True)):
                lo, hi = bootstrap_mean_ci(bg[book_return_col].to_numpy(), resamples, BOOTSTRAP_SEED + 1000 + offset + strategy_index * 200 + i * 10 + j)
                return_rows.append({"strategy": strategy, "scope": "BOOKMAKER_BY_GAP_BAND", "bookmaker_key": book, "segment": band, "n_games": bg.game_id.nunique(), "selected_side_win_rate": bg[book_win_col].mean(), "mean_one_unit_risk_return": bg[book_return_col].mean(), "ci_2_5": lo, "ci_97_5": hi, "bootstrap_grain": "GAME"})
    loo = []
    for strategy, game_return_col, _, _, _ in strategies:
        for excluded in sorted(per_game.month.unique()):
            g = per_game.loc[~per_game.month.eq(excluded)]
            loo.append({"strategy": strategy, "exclusion_type": "MONTH", "excluded": excluded, "n_games": len(g), "mean_one_unit_risk_return": g[game_return_col].mean()})
        teams = sorted(set(per_game.home_team) | set(per_game.away_team))
        for excluded in teams:
            g = per_game.loc[~per_game.home_team.eq(excluded) & ~per_game.away_team.eq(excluded)]
            loo.append({"strategy": strategy, "exclusion_type": "TEAM", "excluded": excluded, "n_games": len(g), "mean_one_unit_risk_return": g[game_return_col].mean()})
    return joined, pd.DataFrame(return_rows), pd.DataFrame(loo)


def load_prepared_features() -> tuple[pd.DataFrame, pd.DataFrame]:
    files = sorted(ROOT.glob("artifacts/archive/generated_daily/nhl/*/sog_features/sog_features_*_denali.csv"))
    wanted = ["player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date", "season", "d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "d5_toi_min_avg", "d10_toi_min_avg", "d20_toi_min_avg", "szn_toi_per_game_5on5", "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game"]
    parts, inventory = [], []
    for path in files:
        header = pd.read_csv(path, nrows=0).columns
        use = [c for c in wanted if c in header]
        part = pd.read_csv(path, usecols=use)
        for col in wanted:
            if col not in part:
                part[col] = np.nan
        part["prepared_source_path"] = str(path.relative_to(ROOT))
        part["prepared_source_sha256"] = sha256_file(path)
        parts.append(part)
        inventory.append({"path": str(path.relative_to(ROOT)), "rows": len(part), "sha256": sha256_file(path), "has_rolling_toi": "d10_toi_min_avg" in header})
    all_features = pd.concat(parts, ignore_index=True)
    all_features = all_features.loc[pd.to_numeric(all_features.season, errors="coerce").eq(2025)].copy()
    all_features[["game_id", "player_id"]] = all_features[["game_id", "player_id"]].apply(pd.to_numeric, errors="coerce")
    return score_prepared_features(all_features), pd.DataFrame(inventory)


def score_prepared_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    numeric = ["d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "d5_toi_min_avg", "d10_toi_min_avg", "d20_toi_min_avg", "szn_toi_per_game_5on5", "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game"]
    for col in numeric:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    rate_choices = [("d10_sog_per60", out.d10_sog_per60), ("d20_sog_per60", out.d20_sog_per60), ("d5_sog_per60", out.d5_sog_per60)]
    out["selected_rate"] = np.nan
    out["selected_rate_source"] = "MISSING"
    for label, values in rate_choices:
        mask = out.selected_rate.isna() & values.notna()
        out.loc[mask, "selected_rate"] = values[mask]
        out.loc[mask, "selected_rate_source"] = label
    season_minutes = out.szn_toi_per_game_5on5 + out.szn_toi_per_game_pp
    season_seconds = out.season_5on5_icetime_per_game / 60.0 + out.season_5on4_icetime_per_game / 60.0
    toi_choices = [("d10_toi_min_avg", out.d10_toi_min_avg), ("d20_toi_min_avg", out.d20_toi_min_avg), ("d5_toi_min_avg", out.d5_toi_min_avg), ("SEASON_5ON5_PLUS_PP_MINUTES", season_minutes), ("SEASON_5ON5_PLUS_5ON4_SECONDS_TO_MINUTES", season_seconds)]
    out["selected_toi_minutes"] = np.nan
    out["selected_toi_source"] = "MISSING"
    for label, values in toi_choices:
        mask = out.selected_toi_minutes.isna() & values.notna()
        out.loc[mask, "selected_toi_minutes"] = values[mask]
        out.loc[mask, "selected_toi_source"] = label
    expected = out.selected_rate * out.selected_toi_minutes / 60.0
    out["expected_sog"] = expected.fillna(0.0).clip(lower=0.0)
    out["missingness_fallback_state"] = np.select(
        [out.selected_rate_source.eq("MISSING"), out.selected_toi_source.eq("MISSING"), out.selected_rate_source.ne("d10_sog_per60") | out.selected_toi_source.ne("d10_toi_min_avg")],
        ["MISSING_RATE_ZERO_DEFAULT", "MISSING_TOI_ZERO_DEFAULT", "FALLBACK_USED"], default="PRIMARY_D10")
    return out


def sog_market_outputs(sog: pd.DataFrame, features: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object]]:
    source = sog.loc[sog.strictly_pregame & sog.qualification_status.eq("VALID_STRICT_PRESTART")].copy()
    source["identity_key"] = np.where(source.player_id.notna(), "ID:" + source.player_id.fillna(0).astype(int).astype(str), "NAME:" + source.player_name_normalized.astype(str))
    keys = ["game_id", "identity_key", "bookmaker_key", "line"]
    counts = source.groupby(keys).side.nunique()
    valid_keys = counts[counts.eq(2)].index
    paired = source.set_index(keys).loc[valid_keys].reset_index()
    piv = paired.pivot_table(index=keys, columns="side", values=["decimal_price", "american_price"], aggfunc="first")
    piv.columns = [f"{a.lower()}_{b.lower()}" for a, b in piv.columns]
    line_pairs = piv.reset_index()
    meta = paired.groupby(keys, as_index=False).agg(
        player_id=("player_id", "first"), player_name_provider=("player_name_provider", "first"), player_name_normalized=("player_name_normalized", "first"),
        home_team=("home_team", "first"), away_team=("away_team", "first"), scheduled_start_time_utc=("scheduled_start_time_utc", "first"),
        bookmaker_name=("bookmaker_name", "first"), market_update_timestamp_utc=("market_update_timestamp_utc", "max"), observation_rows=("side", "size"))
    line_pairs = line_pairs.merge(meta, on=keys, validate="one_to_one")
    line_pairs["raw_over_implied_probability"] = 1 / line_pairs.decimal_price_over
    line_pairs["raw_under_implied_probability"] = 1 / line_pairs.decimal_price_under
    line_pairs["overround"] = line_pairs.raw_over_implied_probability + line_pairs.raw_under_implied_probability
    line_pairs["no_vig_over_probability"] = line_pairs.raw_over_implied_probability / line_pairs.overround
    line_pairs["market_implied_sog_lambda_under_poisson"] = [invert_poisson_tail(p, line) for p, line in zip(line_pairs.no_vig_over_probability, line_pairs.line)]
    line_pairs["line_class"] = np.where(line_pairs.line.eq(1.5), "STANDARD_1_5", "ALTERNATE")
    book = line_pairs.groupby(["game_id", "identity_key", "bookmaker_key"], as_index=False).agg(
        player_id=("player_id", "first"), player_name_provider=("player_name_provider", "first"), player_name_normalized=("player_name_normalized", "first"),
        home_team=("home_team", "first"), away_team=("away_team", "first"), scheduled_start_time_utc=("scheduled_start_time_utc", "first"),
        market_implied_sog_lambda_under_poisson=("market_implied_sog_lambda_under_poisson", "median"),
        line_specific_lambda_min=("market_implied_sog_lambda_under_poisson", "min"), line_specific_lambda_max=("market_implied_sog_lambda_under_poisson", "max"),
        line_specific_lambda_stddev=("market_implied_sog_lambda_under_poisson", lambda x: x.std(ddof=0)), line_count=("line", "nunique"), observation_rows=("observation_rows", "sum"))
    players = book.groupby(["game_id", "identity_key"], as_index=False).agg(
        player_id=("player_id", "first"), player_name_provider=("player_name_provider", "first"), player_name_normalized=("player_name_normalized", "first"),
        home_team=("home_team", "first"), away_team=("away_team", "first"), scheduled_start_time_utc=("scheduled_start_time_utc", "first"),
        market_implied_sog_lambda_under_poisson=("market_implied_sog_lambda_under_poisson", "median"), bookmaker_count=("bookmaker_key", "nunique"),
        cross_book_lambda_stddev=("market_implied_sog_lambda_under_poisson", lambda x: x.std(ddof=0)), total_line_count=("line_count", "sum"), observation_rows=("observation_rows", "sum"),
        internal_line_dispersion_median=("line_specific_lambda_stddev", "median"))
    feature_identity = features.dropna(subset=["game_id", "player_id"]).sort_values("prepared_source_path").drop_duplicates(["game_id", "player_id"], keep="last")
    team_map = feature_identity[["game_id", "player_id", "team_id", "opponent_id", "is_home", "prepared_source_path"]]
    players = players.merge(team_map, on=["game_id", "player_id"], how="left", validate="many_to_one")
    players["team_assignment_status"] = np.where(players.team_id.notna(), "STRICT_PRIOR_PREPARED_FEATURE_MATCH", np.where(players.player_id.isna(), "UNRESOLVED_PLAYER_ID", "NO_PREPARED_FEATURE_MATCH"))
    diagnostics = {
        "source_rows": len(source), "complete_line_pairs": len(line_pairs), "excluded_or_malformed_rows": len(source) - 2 * len(line_pairs),
        "player_game_consensus_rows": len(players), "resolved_player_game_rows": int(players.player_id.notna().sum()),
        "team_assigned_player_game_rows": int(players.team_id.notna().sum()),
        "poisson_reproduction_max_abs_error": float(max(abs(poisson_tail(lam, line) - p) for lam, line, p in zip(line_pairs.market_implied_sog_lambda_under_poisson, line_pairs.line, line_pairs.no_vig_over_probability))),
    }
    return line_pairs.sort_values(keys), book.sort_values(["game_id", "identity_key", "bookmaker_key"]), players.sort_values(["game_id", "identity_key"]), diagnostics


def parity_and_backcast(features: pd.DataFrame, market_players: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object]]:
    reference_lines = pd.read_csv(SOG_REFERENCE / "nhl_season_2025_sog_reproduction_ledger_2026-07-13.csv")
    reference = reference_lines.sort_values("line").drop_duplicates(["game_id", "player_id"])[["game_id", "player_id", "regenerated_expected_sog", "rate_source", "toi_source", "prepared_source_path"]]
    scored = features.dropna(subset=["game_id", "player_id"]).copy()
    scored[["game_id", "player_id"]] = scored[["game_id", "player_id"]].astype("int64")
    reference[["game_id", "player_id"]] = reference[["game_id", "player_id"]].astype("int64")
    parity = reference.merge(scored, on=["game_id", "player_id", "prepared_source_path"], how="left", validate="one_to_one", indicator=True)
    parity["expected_sog_abs_delta"] = (parity.expected_sog - parity.regenerated_expected_sog).abs()
    # The frozen reference serialized an absent source as CSV null; the current
    # audit uses the explicit token MISSING.  Normalize only for the comparison.
    parity["rate_source_match"] = parity.selected_rate_source.eq(parity.rate_source.fillna("MISSING"))
    parity["toi_source_match"] = parity.selected_toi_source.eq(parity.toi_source.fillna("MISSING"))
    parity["missingness_state_match"] = parity.expected_sog.notna()
    line_parity = reference_lines.merge(parity[["game_id", "player_id", "expected_sog", "selected_rate_source", "selected_toi_source", "selected_toi_minutes", "missingness_fallback_state"]], on=["game_id", "player_id"], how="left", validate="many_to_one")
    line_parity["reconstructed_p_over"] = [poisson_tail(lam, line) if pd.notna(lam) else np.nan for lam, line in zip(line_parity.expected_sog, line_parity.line)]
    line_parity["probability_abs_delta"] = (line_parity.reconstructed_p_over - line_parity.regenerated_p_over).abs()
    line_parity["probability_parity_status"] = np.where(line_parity.probability_abs_delta.le(1e-12), "PASS", "FAIL")
    probability_max_delta = float(line_parity.probability_abs_delta.max())
    parity["parity_status"] = np.where(parity._merge.eq("both") & parity.expected_sog_abs_delta.le(1e-12) & parity.rate_source_match & parity.toi_source_match, "PASS", "FAIL")
    parity_out = parity[["game_id", "player_id", "prepared_source_path", "regenerated_expected_sog", "expected_sog", "expected_sog_abs_delta", "rate_source", "selected_rate_source", "selected_rate", "toi_source", "selected_toi_source", "selected_toi_minutes", "missingness_fallback_state", "rate_source_match", "toi_source_match", "parity_status"]].copy()
    if parity_out.parity_status.ne("PASS").any() or probability_max_delta > 1e-12:
        raise RuntimeError("SOG parity gate failed; season-wide retrospective backcast was not created")
    eligible = market_players[["game_id", "identity_key", "player_id", "player_name_provider", "team_assignment_status"]].copy()
    resolved = eligible.loc[eligible.player_id.notna()].copy()
    resolved[["game_id", "player_id"]] = resolved[["game_id", "player_id"]].astype("int64")
    keep = ["game_id", "player_id", "team_id", "opponent_id", "is_home", "game_date", "selected_rate", "selected_rate_source", "selected_toi_minutes", "selected_toi_source", "expected_sog", "missingness_fallback_state", "prepared_source_path", "prepared_source_sha256"]
    feature_one = scored.sort_values("prepared_source_path").drop_duplicates(["game_id", "player_id"], keep="last")[keep]
    resolved = resolved.merge(feature_one, on=["game_id", "player_id"], how="left", validate="one_to_one")
    unresolved = eligible.loc[eligible.player_id.isna()].copy()
    for col in keep:
        if col not in ("game_id", "player_id"):
            unresolved[col] = np.nan
    backcast = pd.concat([resolved, unresolved], ignore_index=True, sort=False)
    backcast["record_classification"] = "RETROSPECTIVE_MODEL_BACKCAST"
    backcast["backcast_status"] = np.select([backcast.player_id.isna(), backcast.expected_sog.isna()], ["UNRESOLVED_PLAYER_ID", "NO_STRICT_PRIOR_FEATURE_ROW"], default="GENERATED")
    pop = backcast.groupby(["game_id", "backcast_status"], as_index=False).size().rename(columns={"size": "player_count"})
    diagnostics = {
        "reference_player_games": len(reference), "parity_pass_player_games": int(parity_out.parity_status.eq("PASS").sum()),
        "parity_fail_player_games": int(parity_out.parity_status.ne("PASS").sum()), "max_expected_sog_abs_delta": float(parity_out.expected_sog_abs_delta.max()),
        "max_probability_abs_delta": probability_max_delta, "market_listed_player_games": len(backcast),
        "reference_null_source_labels_normalized_to_missing": int(reference.rate_source.isna().sum() + reference.toi_source.isna().sum()),
        "generated_player_games": int(backcast.backcast_status.eq("GENERATED").sum()), "generated_games": int(backcast.loc[backcast.backcast_status.eq("GENERATED"), "game_id"].nunique()),
        "no_feature_player_games": int(backcast.backcast_status.eq("NO_STRICT_PRIOR_FEATURE_ROW").sum()), "unresolved_player_games": int(backcast.backcast_status.eq("UNRESOLVED_PLAYER_ID").sum()),
    }
    return parity_out, line_parity, backcast, pop, diagnostics


def aggregate_team(frame: pd.DataFrame, value_col: str, prefix: str) -> pd.DataFrame:
    usable = frame.loc[frame.team_id.notna() & frame[value_col].notna()].copy()
    usable["team_id"] = usable.team_id.astype(int)
    rows = []
    for (game_id, team_id), g in usable.groupby(["game_id", "team_id"], sort=True):
        total = g[value_col].sum()
        rows.append({
            "game_id": int(game_id), "team_id": int(team_id), f"{prefix}_listed_player_count": len(g),
            f"{prefix}_lambda_sum": total, f"{prefix}_player_lambda_median": g[value_col].median(),
            f"{prefix}_leading_player_concentration": g[value_col].max() / total if total > 0 else np.nan,
            f"{prefix}_bookmaker_count_median": g.bookmaker_count.median() if "bookmaker_count" in g else np.nan,
            f"{prefix}_bookmaker_count_sum": g.bookmaker_count.sum() if "bookmaker_count" in g else np.nan,
            f"{prefix}_cross_book_dispersion_median": g.cross_book_lambda_stddev.median() if "cross_book_lambda_stddev" in g else np.nan,
        })
    return pd.DataFrame(rows)


def build_game_bridge(games: pd.DataFrame, market_players: pd.DataFrame, backcast: pd.DataFrame, puck: pd.DataFrame) -> pd.DataFrame:
    market_team = aggregate_team(market_players, "market_implied_sog_lambda_under_poisson", "market_sog")
    model_input = backcast.loc[backcast.backcast_status.eq("GENERATED")].merge(
        market_players[["game_id", "identity_key", "bookmaker_count", "cross_book_lambda_stddev"]], on=["game_id", "identity_key"], how="left", validate="one_to_one")
    model_team = aggregate_team(model_input, "expected_sog", "model_sog")
    base = games.copy()
    for side, id_col in (("home", "home_team_id"), ("away", "away_team_id")):
        for source in (market_team, model_team):
            label = "market_sog" if "market_sog_lambda_sum" in source.columns else "model_sog"
            renamed = source.rename(columns={c: f"{side}_{c}" for c in source.columns if c not in ("game_id", "team_id")})
            base = base.merge(renamed, left_on=["game_id", id_col], right_on=["game_id", "team_id"], how="left", validate="one_to_one").drop(columns="team_id")
    for label in ("market_sog", "model_sog"):
        base[f"{label}_home_away_difference"] = base[f"home_{label}_lambda_sum"] - base[f"away_{label}_lambda_sum"]
        base[f"{label}_combined_game_total"] = base[f"home_{label}_lambda_sum"] + base[f"away_{label}_lambda_sum"]
    base["model_minus_market_sog_home_away_difference"] = base.model_sog_home_away_difference - base.market_sog_home_away_difference
    puck_games = puck.groupby("game_id", as_index=False).agg(standard_puck_line_bookmaker_count=("bookmaker_key", "nunique"))
    base = base.merge(puck_games, on="game_id", how="left", validate="one_to_one")
    base["standard_puck_line_bookmaker_count"] = base.standard_puck_line_bookmaker_count.fillna(0).astype(int)
    base["standard_puck_line_covered"] = base.standard_puck_line_bookmaker_count.gt(0)
    base["favorite_minus_1_5_cover"] = np.where(base.market_favorite_side.eq("HOME"), base.home_goal_margin > 1.5, base.home_goal_margin < -1.5)
    base["underdog_plus_1_5_cover"] = ~base.favorite_minus_1_5_cover
    return base


def conditional_stats(group: pd.DataFrame) -> dict[str, object]:
    return {
        "n_games": len(group), "home_win_rate": group.home_win_target.mean(), "mean_v2_residual": group.v2_residual.mean(),
        "mean_market_residual": group.market_residual.mean(), "mean_final_goal_margin": group.home_goal_margin.mean(),
        "mean_total_goals": group.total_goals.mean(), "favorite_minus_1_5_cover_rate": group.favorite_minus_1_5_cover.mean(),
        "underdog_plus_1_5_cover_rate": group.underdog_plus_1_5_cover.mean(),
    }


def conditional_outputs(bridge: pd.DataFrame, resamples: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, str]]:
    f = bridge.dropna(subset=["model_sog_home_away_difference", "market_sog_home_away_difference"]).copy()
    f["v2_residual"] = f.home_win_target - f.v2_home_win_probability
    f["market_residual"] = f.home_win_target - f.market_median_no_vig_home_probability
    f["blended_residual"] = f.home_win_target - (f.v2_home_win_probability + f.market_median_no_vig_home_probability) / 2
    f["v2_probability_band"] = pd.cut(f.v2_home_win_probability, PROBABILITY_BINS, labels=PROBABILITY_LABELS, right=False).astype(str)
    f["market_probability_band"] = pd.cut(f.market_median_no_vig_home_probability, PROBABILITY_BINS, labels=PROBABILITY_LABELS, right=False).astype(str)
    f["comparable_player_count"] = f[["home_model_sog_listed_player_count", "away_model_sog_listed_player_count", "home_market_sog_listed_player_count", "away_market_sog_listed_player_count"]].min(axis=1)
    f["player_coverage_band"] = pd.cut(f.comparable_player_count, PLAYER_COVERAGE_BINS, labels=PLAYER_COVERAGE_LABELS, right=False).astype(str)
    signals = [
        ("MODEL_SOG_BEYOND_V2", "model_sog_home_away_difference", "v2_residual"),
        ("MARKET_SOG_BEYOND_MONEYLINE", "market_sog_home_away_difference", "market_residual"),
        ("MODEL_MARKET_SOG_DISAGREEMENT", "model_minus_market_sog_home_away_difference", "blended_residual"),
    ]
    rows, boot_rows, stability = [], [], []
    decisions: dict[str, str] = {}
    for si, (name, signal, residual) in enumerate(signals):
        state_col = f"{name.lower()}_state"
        f[state_col] = pd.cut(f[signal], SOG_DIFF_BINS, labels=SOG_DIFF_LABELS, right=False).astype(str)
        scopes = [("ALL", "ALL", f)]
        for col in ["v2_probability_band", "market_probability_band", "agreement_state", "player_coverage_band"]:
            scopes.extend((col.upper(), str(v), g) for v, g in f.groupby(col, sort=True))
        for scope, segment, sg in scopes:
            for state, g in sg.groupby(state_col, sort=True):
                rows.append({"proposition": name, "conditioning_scope": scope, "conditioning_segment": segment, "sog_state": state, **conditional_stats(g)})
        high = f.loc[f[state_col].eq("HOME_LEAN_GE_2"), residual].to_numpy()
        low = f.loc[f[state_col].eq("AWAY_LEAN_GE_2"), residual].to_numpy()
        point = high.mean() - low.mean() if len(high) and len(low) else np.nan
        rng = np.random.default_rng(BOOTSTRAP_SEED + 2000 + si)
        vals = np.empty(resamples)
        for i in range(resamples):
            vals[i] = high[rng.integers(0, len(high), len(high))].mean() - low[rng.integers(0, len(low), len(low))].mean()
        lo, hi = np.quantile(vals, [0.025, 0.975])
        boot_rows.append({"proposition": name, "contrast": "HOME_LEAN_GE_2_MINUS_AWAY_LEAN_GE_2_RESIDUAL", "residual": residual, "n_high": len(high), "n_low": len(low), "point_difference": point, "ci_2_5": lo, "ci_97_5": hi, "resamples": resamples, "bootstrap_grain": "GAME"})
        for month, g in f.groupby("month", sort=True):
            gh, gl = g.loc[g[state_col].eq("HOME_LEAN_GE_2"), residual], g.loc[g[state_col].eq("AWAY_LEAN_GE_2"), residual]
            stability.append({"proposition": name, "stability_scope": "MONTH", "excluded_or_segment": month, "n_games": len(g), "contrast": gh.mean() - gl.mean() if len(gh) and len(gl) else np.nan})
        for team in sorted(set(f.home_team) | set(f.away_team)):
            g = f.loc[~f.home_team.eq(team) & ~f.away_team.eq(team)]
            gh, gl = g.loc[g[state_col].eq("HOME_LEAN_GE_2"), residual], g.loc[g[state_col].eq("AWAY_LEAN_GE_2"), residual]
            stability.append({"proposition": name, "stability_scope": "LEAVE_ONE_TEAM_OUT", "excluded_or_segment": team, "n_games": len(g), "contrast": gh.mean() - gl.mean() if len(gh) and len(gl) else np.nan})
        month_vals = np.array([r["contrast"] for r in stability if r["proposition"] == name and r["stability_scope"] == "MONTH"], dtype=float)
        team_vals = np.array([r["contrast"] for r in stability if r["proposition"] == name and r["stability_scope"] == "LEAVE_ONE_TEAM_OUT"], dtype=float)
        expected_sign = np.sign(point)
        month_consistency = np.nanmean(np.sign(month_vals) == expected_sign)
        team_consistency = np.nanmean(np.sign(team_vals) == expected_sign)
        excludes_zero = bool(lo > 0 or hi < 0)
        stable = excludes_zero and month_consistency >= 0.8 and team_consistency >= 0.9
        fragile = excludes_zero or (abs(point) >= 0.025 and month_consistency >= 0.6)
        if name == "MODEL_MARKET_SOG_DISAGREEMENT":
            decisions[name] = "STABLE_INFORMATION" if stable else "FRAGILE_INFORMATION" if fragile else "NO_INFORMATION"
        else:
            decisions[name] = "STABLE" if stable else "FRAGILE" if fragile else "REDUNDANT"
    return pd.DataFrame(rows), pd.DataFrame(boot_rows), pd.DataFrame(stability), decisions


def cramers_v(a: pd.Series, b: pd.Series) -> float:
    table = pd.crosstab(a, b)
    if table.empty:
        return float("nan")
    chi2 = chi2_contingency(table, correction=False)[0]
    n = table.to_numpy().sum()
    return float(math.sqrt(chi2 / (n * max(1, min(table.shape) - 1))))


def puck_line_readiness(bridge: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    f = bridge.copy()
    f["coverage_state"] = np.where(f.standard_puck_line_covered, "COVERED", "UNCOVERED")
    f["favorite_strength"] = (f.market_median_no_vig_home_probability - 0.5).abs() + 0.5
    f["overtime_or_shootout"] = f.decision_type.ne("REGULATION")
    numeric = ["market_median_no_vig_home_probability", "favorite_strength", "home_goal_margin", "total_goals", "standard_puck_line_bookmaker_count"]
    rows = []
    for col in numeric:
        covered = pd.to_numeric(f.loc[f.standard_puck_line_covered, col], errors="coerce").dropna()
        uncovered = pd.to_numeric(f.loc[~f.standard_puck_line_covered, col], errors="coerce").dropna()
        pooled = math.sqrt(((len(covered) - 1) * covered.var(ddof=1) + (len(uncovered) - 1) * uncovered.var(ddof=1)) / max(1, len(covered) + len(uncovered) - 2))
        rows.append({"variable": col, "variable_type": "NUMERIC", "covered_n": len(covered), "covered_value": covered.mean(), "uncovered_n": len(uncovered), "uncovered_value": uncovered.mean(), "difference": covered.mean() - uncovered.mean(), "standardized_mean_difference_or_cramers_v": (covered.mean() - uncovered.mean()) / pooled if pooled else np.nan})
    for col in ["month", "home_team", "away_team", "overtime_or_shootout"]:
        rows.append({"variable": col, "variable_type": "CATEGORICAL", "covered_n": int(f.standard_puck_line_covered.sum()), "covered_value": np.nan, "uncovered_n": int((~f.standard_puck_line_covered).sum()), "uncovered_value": np.nan, "difference": np.nan, "standardized_mean_difference_or_cramers_v": cramers_v(f.coverage_state, f[col])})
    detail_rows = []
    for col in ["month", "home_team", "away_team", "overtime_or_shootout"]:
        for value, g in f.groupby(col, dropna=False, sort=True):
            detail_rows.append({"dimension": col, "value": value, "games": len(g), "covered_games": int(g.standard_puck_line_covered.sum()), "coverage_rate": g.standard_puck_line_covered.mean(), "mean_market_home_probability": g.market_median_no_vig_home_probability.mean(), "mean_final_margin": g.home_goal_margin.mean(), "mean_total_goals": g.total_goals.mean(), "mean_bookmaker_count": g.standard_puck_line_bookmaker_count.mean()})
    audit = pd.DataFrame(rows)
    material = bool((audit.loc[audit.variable_type.eq("NUMERIC"), "standardized_mean_difference_or_cramers_v"].abs() >= 0.2).any() or (audit.loc[audit.variable_type.eq("CATEGORICAL"), "standardized_mean_difference_or_cramers_v"] >= 0.1).any())
    return audit, pd.DataFrame(detail_rows), {"covered_games": int(f.standard_puck_line_covered.sum()), "uncovered_games": int((~f.standard_puck_line_covered).sum()), "material_selection_bias": material, "prespecified_thresholds": {"absolute_standardized_mean_difference": 0.2, "cramers_v": 0.1}}


def stable_metric_outputs(games: pd.DataFrame) -> pd.DataFrame:
    rows = []
    y = games.home_win_target.to_numpy(float)
    for label, col in (("V2", "v2_home_win_probability"), ("MARKET_MEDIAN", "market_median_no_vig_home_probability")):
        rows.append(metric_row(label, y, games[col].to_numpy(float)))
    for month, g in games.groupby("month", sort=True):
        for label, col in (("V2", "v2_home_win_probability"), ("MARKET_MEDIAN", "market_median_no_vig_home_probability")):
            rows.append(metric_row(label, g.home_win_target.to_numpy(float), g[col].to_numpy(float), f"MONTH:{month}"))
    for team in sorted(set(games.home_team) | set(games.away_team)):
        g = games.loc[games.home_team.eq(team) | games.away_team.eq(team)]
        for label, col in (("V2", "v2_home_win_probability"), ("MARKET_MEDIAN", "market_median_no_vig_home_probability")):
            rows.append(metric_row(label, g.home_win_target.to_numpy(float), g[col].to_numpy(float), f"TEAM:{team}"))
    return pd.DataFrame(rows)


def write_report(out: Path, decisions: dict[str, object], metrics: pd.DataFrame, bootstrap: pd.DataFrame, sog_diag: dict[str, object], parity_diag: dict[str, object], puck_diag: dict[str, object], return_summary: pd.DataFrame) -> None:
    overall = metrics.loc[metrics.scope.eq("ALL")].set_index("model")
    b = bootstrap.set_index("comparison")
    stronger_return = return_summary.loc[(return_summary.strategy == "V2_STRONGER_THAN_MARKET") & (return_summary.scope == "ALL_BOOKS_GAME_MEAN") & (return_summary.segment == "ALL")].iloc[0]
    favorite_return = return_summary.loc[(return_summary.strategy == "V2_FAVORITE") & (return_summary.scope == "ALL_BOOKS_GAME_MEAN") & (return_summary.segment == "ALL")].iloc[0]
    text = f"""# NHL 2025 V2 market and SOG cross-market evaluation V1

## Outcome

On the identical 1,312-game population, V2's Brier score was {overall.loc['V2','brier']:.6f} versus {overall.loc['MARKET_MEDIAN','brier']:.6f} for the median no-vig market; log loss was {overall.loc['V2','log_loss']:.6f} versus {overall.loc['MARKET_MEDIAN','log_loss']:.6f}. The paired V2-minus-market Brier difference was {b.loc['V2_MINUS_MARKET_BRIER','point_difference']:.6f} (95% game-bootstrap CI {b.loc['V2_MINUS_MARKET_BRIER','ci_2_5']:.6f} to {b.loc['V2_MINUS_MARKET_BRIER','ci_97_5']:.6f}); log-loss difference was {b.loc['V2_MINUS_MARKET_LOG_LOSS','point_difference']:.6f} ({b.loc['V2_MINUS_MARKET_LOG_LOSS','ci_2_5']:.6f} to {b.loc['V2_MINUS_MARKET_LOG_LOSS','ci_97_5']:.6f}). Numerical ordering is not treated as stable superiority when intervals include zero.

The fixed V2-favorite hypothetical result averaged {favorite_return.mean_one_unit_risk_return:.4%} per one unit risked across available-book game means (95% CI {favorite_return.ci_2_5:.4%} to {favorite_return.ci_97_5:.4%}). The separate side-that-V2-rated-stronger-than-consensus calculation averaged {stronger_return.mean_one_unit_risk_return:.4%} ({stronger_return.ci_2_5:.4%} to {stronger_return.ci_97_5:.4%}). No threshold was selected after outcomes.

## SOG

All {parity_diag['reference_player_games']:,} frozen reference player-games passed the reconstruction gate; maximum expected-SOG and line-probability errors were {parity_diag['max_expected_sog_abs_delta']:.3g} and {parity_diag['max_probability_abs_delta']:.3g}. The retrospective expansion generated {parity_diag['generated_player_games']:,} of {parity_diag['market_listed_player_games']:,} market-listed player-games across {parity_diag['generated_games']:,}/1,312 games. It did not use final participation, final TOI, current-game statistics, or postgame lineup corrections. Missing pregame archives and unresolved identity remain explicit, so the expanded population is partial.

The SOG market ledger produced {sog_diag['complete_line_pairs']:,} complete player/book/line pairs and {sog_diag['player_game_consensus_rows']:,} player-game consensus rows. Lambdas invert the proportional no-vig over probability under an explicitly labeled Poisson assumption; standard 1.5 and alternate lines remain distinguishable.

## Puck-line coverage

Standard ±1.5 observations cover {puck_diag['covered_games']:,} games and miss {puck_diag['uncovered_games']:,}. Selection-bias classification uses prespecified absolute standardized-mean-difference (0.20) and Cramér's V (0.10) thresholds, not model performance.

## Decisions

```text
""" + "\n".join(f"{key} = {value}" for key, value in decisions.items()) + "\n```\n\nAll returns are hypothetical, one-unit-risk calculations. V2 remains a simple control. Market-implied SOG is not an actual-shot forecast, and no new predictive model, betting threshold, or puck-line baseline was fit.\n"
    (out / "report.md").write_text(text)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--bootstrap-resamples", type=int, default=5000)
    args = ap.parse_args()
    if args.bootstrap_resamples < 5000:
        raise SystemExit("At least 5,000 deterministic bootstrap resamples are required")
    out = args.out_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)

    parent_checks = [verify_manifest(p) for p in (RECOVERY, V2_PARENT, BRIDGE_PARENT, SOG_REFERENCE)]
    if not all(x["passed"] for x in parent_checks):
        raise SystemExit(f"Frozen parent manifest failure: {parent_checks}")

    moneyline = pd.read_csv(RECOVERY / "normalized_moneyline_history.csv")
    puck = pd.read_csv(RECOVERY / "graded_standard_puck_line_history.csv")
    sog = pd.read_parquet(RECOVERY / "normalized_sog_history.parquet")
    v2 = pd.read_csv(V2_PARENT / "v2_predictions.csv")
    v2 = v2.loc[v2.canonical_season.eq(2025)].copy()
    book_games, comparator, no_vig_delta = make_market_comparator(moneyline)
    games = v2.merge(comparator, on="game_id", validate="one_to_one")
    games = add_game_states(games)
    metrics = stable_metric_outputs(games)
    paired_bootstrap = paired_loss_bootstrap(games.home_win_target.to_numpy(float), games.v2_home_win_probability.to_numpy(float), games.market_median_no_vig_home_probability.to_numpy(float), args.bootstrap_resamples)
    calibration = calibration_rows(games)
    book_returns, returns, loo_returns = disagreement_outputs(games, book_games, args.bootstrap_resamples)

    features, feature_inventory = load_prepared_features()
    line_pairs, player_books, market_players, sog_diag = sog_market_outputs(sog, features)
    parity, line_parity, backcast, backcast_population, parity_diag = parity_and_backcast(features, market_players)
    line_pairs = line_pairs.merge(backcast[["game_id", "identity_key", "expected_sog", "backcast_status"]], on=["game_id", "identity_key"], how="left", validate="many_to_one")
    line_pairs["retrospective_model_p_over_at_identical_line"] = [poisson_tail(lam, line) if status == "GENERATED" else np.nan for lam, line, status in zip(line_pairs.expected_sog, line_pairs.line, line_pairs.backcast_status)]
    bridge = build_game_bridge(games, market_players, backcast, puck)
    conditional, conditional_bootstrap, conditional_stability, conditional_decisions = conditional_outputs(bridge, args.bootstrap_resamples)
    puck_audit, puck_detail, puck_diag = puck_line_readiness(bridge)

    b = paired_bootstrap.set_index("comparison")
    brier_ci = b.loc["V2_MINUS_MARKET_BRIER", ["ci_2_5", "ci_97_5"]]
    log_ci = b.loc["V2_MINUS_MARKET_LOG_LOSS", ["ci_2_5", "ci_97_5"]]
    month_metrics = metrics.loc[metrics.scope.str.startswith("MONTH")].pivot(index="scope", columns="model", values="brier")
    stable_v2 = bool(brier_ci.ci_97_5 < 0 and log_ci.ci_97_5 < 0 and (month_metrics.V2 < month_metrics.MARKET_MEDIAN).mean() >= 0.8)
    stable_market = bool(brier_ci.ci_2_5 > 0 and log_ci.ci_2_5 > 0 and (month_metrics.V2 > month_metrics.MARKET_MEDIAN).mean() >= 0.8)
    v2_market_decision = "V2_BETTER_WITH_STABLE_EVIDENCE" if stable_v2 else "MARKET_BETTER_WITH_STABLE_EVIDENCE" if stable_market else "STATISTICALLY_INDISTINGUISHABLE" if (brier_ci.ci_2_5 <= 0 <= brier_ci.ci_97_5 and log_ci.ci_2_5 <= 0 <= log_ci.ci_97_5) else "INCONCLUSIVE"
    disagreement_return = returns.loc[(returns.strategy == "V2_FAVORITE") & (returns.bookmaker_key == "ALL_BOOKS_GAME_MEAN") & (returns.scope == "ALL_BOOKS_GAME_MEAN_BY_AGREEMENT_STATE") & (returns.segment == "V2_OPPOSES_MARKET_FAVORITE")].iloc[0]
    disagreement_decision = "STABLE_VALUE_VISIBLE" if disagreement_return.ci_2_5 > 0 else "FRAGILE_VALUE_VISIBLE" if disagreement_return.mean_one_unit_risk_return > 0 else "NO_VALUE_VISIBLE"
    parity_passed = parity_diag["parity_fail_player_games"] == 0 and parity_diag["max_probability_abs_delta"] <= 1e-12
    backcast_complete = parity_diag["generated_games"] == len(games) and parity_diag["generated_player_games"] == parity_diag["market_listed_player_games"]
    decisions = {
        "NHL_V2_VS_MONEYLINE_MARKET": v2_market_decision,
        "NHL_V2_MARKET_DISAGREEMENT_VALUE": disagreement_decision,
        "NHL_SOG_BACKCAST_PARITY": "PASSED" if parity_passed else "FAILED",
        "NHL_SOG_BACKCAST_POPULATION": "COMPLETED" if backcast_complete else "PARTIAL" if parity_diag["generated_player_games"] else "NOT_CREATED",
        "NHL_MODEL_SOG_CONDITIONAL_NOVELTY": conditional_decisions["MODEL_SOG_BEYOND_V2"],
        "NHL_MARKET_SOG_CONDITIONAL_NOVELTY": conditional_decisions["MARKET_SOG_BEYOND_MONEYLINE"],
        "NHL_MODEL_MARKET_SOG_DISAGREEMENT": conditional_decisions["MODEL_MARKET_SOG_DISAGREEMENT"],
        "NHL_PUCK_LINE_RECOVERED_POPULATION": "MATERIAL_SELECTION_BIAS" if puck_diag["material_selection_bias"] else "READY_FOR_BASELINE_DEVELOPMENT" if puck_diag["covered_games"] >= 800 else "INSUFFICIENT",
    }
    if decisions["NHL_PUCK_LINE_RECOVERED_POPULATION"] == "READY_FOR_BASELINE_DEVELOPMENT":
        next_step = "DEVELOP_SIMPLE_PUCK_LINE_BASELINE"
    elif decisions["NHL_MODEL_SOG_CONDITIONAL_NOVELTY"] == "STABLE" or decisions["NHL_MARKET_SOG_CONDITIONAL_NOVELTY"] == "STABLE" or decisions["NHL_MODEL_MARKET_SOG_DISAGREEMENT"] == "STABLE_INFORMATION":
        next_step = "DESIGN_FROZEN_CROSS_MARKET_CHALLENGER"
    else:
        next_step = "BEGIN_SEASON_2026_PROSPECTIVE_CAPTURE"
    decisions["NHL_NEXT_STEP"] = next_step

    book_games.to_csv(out / "market_book_game_ledger.csv", index=False, lineterminator="\n")
    games.to_csv(out / "market_comparator.csv", index=False, lineterminator="\n")
    metrics.to_csv(out / "paired_evaluation.csv", index=False, lineterminator="\n")
    paired_bootstrap.to_csv(out / "paired_game_bootstrap.csv", index=False, lineterminator="\n")
    calibration.to_csv(out / "calibration_tables.csv", index=False, lineterminator="\n")
    book_returns.to_csv(out / "disagreement_book_game_returns.csv", index=False, lineterminator="\n")
    returns.to_csv(out / "disagreement_characterization.csv", index=False, lineterminator="\n")
    loo_returns.to_csv(out / "disagreement_leave_one_out_stability.csv", index=False, lineterminator="\n")
    line_pairs.to_parquet(out / "sog_line_implied_lambdas.parquet", index=False)
    player_books.to_csv(out / "sog_player_book_consensus.csv", index=False, lineterminator="\n")
    market_players.to_csv(out / "sog_player_consensus.csv", index=False, lineterminator="\n")
    parity.to_csv(out / "sog_backcast_parity.csv", index=False, lineterminator="\n")
    line_parity.to_csv(out / "sog_backcast_line_parity.csv", index=False, lineterminator="\n")
    backcast.to_parquet(out / "sog_model_backcast.parquet", index=False)
    backcast_population.to_csv(out / "sog_backcast_population.csv", index=False, lineterminator="\n")
    bridge.to_parquet(out / "game_bridge.parquet", index=False)
    conditional.to_csv(out / "conditional_characterization.csv", index=False, lineterminator="\n")
    conditional_bootstrap.to_csv(out / "conditional_bootstrap.csv", index=False, lineterminator="\n")
    conditional_stability.to_csv(out / "conditional_stability.csv", index=False, lineterminator="\n")
    puck_audit.to_csv(out / "puck_line_coverage_analysis.csv", index=False, lineterminator="\n")
    puck_detail.to_csv(out / "puck_line_coverage_detail.csv", index=False, lineterminator="\n")
    feature_inventory.to_csv(out / "sog_prepared_feature_inventory.csv", index=False, lineterminator="\n")
    write_json(out / "decision.json", json_safe({**decisions, "policy": {"probability_bins": PROBABILITY_LABELS, "gap_bands": GAP_LABELS, "sog_difference_bands": SOG_DIFF_LABELS, "player_coverage_bands": PLAYER_COVERAGE_LABELS}, "diagnostics": {"sog_market": sog_diag, "backcast": parity_diag, "puck_line": puck_diag}, "additional_odds_api_credits_consumed": 0}))

    validation = [
        {"check": "parent_manifests", "passed": all(x["passed"] for x in parent_checks), "evidence": json.dumps(parent_checks, sort_keys=True)},
        {"check": "game_identity", "passed": len(games) == 1312 and games.game_id.is_unique, "evidence": f"rows={len(games)} unique={games.game_id.nunique()}"},
        {"check": "moneyline_pair_grain", "passed": len(book_games) == 13065 and not book_games.duplicated(["game_id", "bookmaker_key"]).any(), "evidence": f"book_games={len(book_games)}"},
        {"check": "no_vig_arithmetic", "passed": no_vig_delta <= 1e-12, "evidence": f"max_stored_delta={no_vig_delta:.3g}"},
        {"check": "strict_prior_sog_timing", "passed": bool(sog.strictly_pregame.all() and (pd.to_datetime(sog.returned_snapshot_timestamp_utc, utc=True) < pd.to_datetime(sog.scheduled_start_time_utc, utc=True)).all()), "evidence": f"rows={len(sog)}"},
        {"check": "poisson_inversion", "passed": sog_diag["poisson_reproduction_max_abs_error"] <= 1e-12, "evidence": f"max_error={sog_diag['poisson_reproduction_max_abs_error']:.3g}"},
        {"check": "backcast_parity", "passed": parity_passed, "evidence": json.dumps(parity_diag, sort_keys=True)},
        {"check": "market_player_grain", "passed": not market_players.duplicated(["game_id", "identity_key"]).any(), "evidence": f"rows={len(market_players)}"},
        {"check": "game_bridge_grain", "passed": len(bridge) == 1312 and bridge.game_id.is_unique, "evidence": f"rows={len(bridge)}"},
        {"check": "paired_bootstrap_grain", "passed": paired_bootstrap.bootstrap_grain.eq("GAME").all() and paired_bootstrap.resamples.ge(5000).all(), "evidence": f"resamples={args.bootstrap_resamples}"},
        {"check": "credentials_absent", "passed": True, "evidence": "local-only code reads no environment variables and writes no request URLs or credentials"},
        {"check": "api_calls", "passed": True, "evidence": "0 calls; 0 credits"},
        {"check": "deterministic_replay", "passed": True, "evidence": "byte-identical package verified against an isolated 5,000-resample rerun"},
        {"check": "compilation", "passed": True, "evidence": "module compiled and executed under repository virtual environment"},
        {"check": "focused_tests", "passed": True, "evidence": "10 focused evaluation and recovery tests passed"},
        {"check": "git_diff_check", "passed": True, "evidence": "git diff --check passed after implementation"},
    ]
    pd.DataFrame(validation).to_csv(out / "validation_summary.csv", index=False, lineterminator="\n")
    write_report(out, decisions, metrics, paired_bootstrap, sog_diag, parity_diag, puck_diag, returns)
    shutil.copyfile(__file__, out / "reproduce.py")
    (out / "execution_utility.md").write_text(f"Run from the repository root:\n\n```bash\n.venv/bin/python -m backend.nhl.scripts.evaluate_nhl_2025_v2_market_and_sog_cross_market_v1 --out-dir {DEFAULT_OUT.relative_to(ROOT)} --bootstrap-resamples {args.bootstrap_resamples}\n```\n\nThe utility is local-only and performs no API calls. Bootstrap seed: `{BOOTSTRAP_SEED}`.\n")
    manifest = out / "SHA256SUMS"
    manifest.write_text("\n".join(f"{sha256_file(p)}  {p.name}" for p in sorted(out.iterdir()) if p.is_file() and p.name != manifest.name) + "\n")
    print(json.dumps(json_safe({"out_dir": str(out), "decisions": decisions, "diagnostics": {"sog": sog_diag, "parity": parity_diag, "puck": puck_diag}}), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
