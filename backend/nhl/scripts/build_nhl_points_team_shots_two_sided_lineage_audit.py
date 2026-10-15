#!/usr/bin/env python3
"""Independently reconcile NHL Points team shots-for features to official games.

Historical reference values come from retained NHL Gamecenter boxscores' team
SOG totals. The Oct. 9-14, 2026 operational values are reconstructed from
official postgame NHL Gamecenter per-skater SOG outcomes, aggregated by game
and team. Neither source path calls the production feature exporter.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
from collections import defaultdict
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15"
BOX_ROOT = ROOT / "artifacts/operational/nhl/moneyline_team_history/raw"
RESTORE = ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3"
MODEL_PATH = ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/nhl_points_count_hgb_v1.joblib"
FEATURES = ["is_home", "d10_sog_per60", "attempts_d10_per60", "player_points_last10",
            "current_season_points_prior", "current_season_games_prior", "mean_toi_last10",
            "mean_pp_toi_last10", "team_d10_sf_per_game", "last10_team_sog_share"]
DEPTHS = [(0, 0, "0"), (1, 2, "1-2"), (3, 5, "3-5"), (6, 9, "6-9"), (10, 10_000, "10+")]


def strict_prior_team_history(team_games: pd.DataFrame, target_date, *, days: int = 120,
                              max_games: int = 10) -> pd.DataFrame:
    """Return valid official team games in the strict-prior rolling window."""
    date = pd.Timestamp(target_date).normalize()
    history = team_games.copy()
    history["game_date"] = pd.to_datetime(history.game_date).dt.normalize()
    history["official_team_sog"] = pd.to_numeric(history.official_team_sog, errors="coerce")
    history = history.loc[history.game_date.lt(date) &
                          history.game_date.ge(date - pd.Timedelta(days=days)) &
                          history.official_team_sog.notna()]
    return history.sort_values(["game_date", "game_id"], kind="mergesort").tail(max_games)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json_default(value):
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if pd.isna(value):
        return None
    raise TypeError(f"Not JSON serializable: {type(value).__name__}")


def _boxscore_team_games() -> pd.DataFrame:
    rows = []
    for path in sorted(BOX_ROOT.glob("season=*/game=*/boxscore.json")):
        raw = json.loads(path.read_text())
        if raw.get("gameType") != 2 or raw.get("gameState") not in {"OFF", "FINAL"}:
            continue
        game_id, date = int(raw["id"]), pd.Timestamp(raw["gameDate"])
        for side in ("home", "away"):
            team = raw.get(f"{side}Team", {})
            rows.append({"source_period": "RETAINED_OFFICIAL_BOXSCORE", "game_id": game_id,
                         "game_date": date, "team_id": int(team["id"]),
                         "official_team_sog": pd.to_numeric(team.get("sog"), errors="coerce"),
                         "home_team_id": int(raw["homeTeam"]["id"]), "away_team_id": int(raw["awayTeam"]["id"]),
                         "is_home": int(side == "home"), "raw_path": str(path.relative_to(ROOT)),
                         "raw_sha256": sha(path)})
    out = pd.DataFrame(rows)
    if out.duplicated(["game_id", "team_id"]).any():
        raise ValueError("DUPLICATE_OFFICIAL_BOXSCORE_GAME_TEAM")
    return out


def _operational_team_games() -> tuple[pd.DataFrame, dict]:
    files = sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation").glob(
        "2026-10-*/reconciliation=*/canonical_skater_outcomes.csv"))
    frames = []
    for path in files:
        if path.parent.parent.name not in {f"2026-10-{d:02d}" for d in range(7, 15)}:
            continue
        df = pd.read_csv(path)
        df["source_path"] = str(path.relative_to(ROOT))
        frames.append(df)
    if not frames:
        raise ValueError("MISSING_OPERATIONAL_OFFICIAL_OUTCOMES")
    source = pd.concat(frames, ignore_index=True)
    key = ["game_id", "player_id"]
    if source.duplicated(key).any():
        raise ValueError("DUPLICATE_OFFICIAL_OPERATIONAL_SKATER_IDENTITY")
    source["official_sog"] = pd.to_numeric(source.official_sog, errors="coerce")
    if not source["official_final"].astype(str).str.lower().isin({"true", "1"}).all():
        raise ValueError("NONFINAL_OPERATIONAL_OUTCOME_SOURCE")
    # Canonical player outcomes are a second source path; direct team totals
    # come from the independently retained NHL Gamecenter boxscores.
    # Null player SOG remains null and invalidates its sum comparison.
    player_teams = source.groupby(["game_id", "team_id", "slate_date"], as_index=False).agg(
        player_sog_sum=("official_sog", lambda s: s.sum(min_count=len(s))),
        source_skater_rows=("player_id", "size"),
        source_unique_players=("player_id", "nunique"),
        missing_player_sog=("official_sog", lambda s: int(s.isna().sum())),
        official_sources=("outcome_source", lambda s: "|".join(sorted(set(map(str, s))))))
    manifest_path = OUT / "official_boxscore_acquisition_manifest.json"
    if not manifest_path.exists():
        raise ValueError("MISSING_OFFICIAL_OPERATIONAL_BOXSCORE_ACQUISITION")
    manifest = json.loads(manifest_path.read_text())
    records = []
    for item in manifest["responses"]:
        for side in ("home", "away"):
            team = item[side]
            records.append({"game_id": int(item["game_id"]), "team_id": int(team["team_id"]),
                "game_date": item["game_date"], "official_team_sog": int(team["team_sog"]),
                "home_team_id": int(item["home"]["team_id"]), "away_team_id": int(item["away"]["team_id"]),
                "is_home": int(side == "home"), "source_period": "OFFICIAL_NHL_GAMECENTER_BOXSCORE_2026",
                "raw_path": item["raw_path"], "raw_sha256": item["sha256"]})
    tg = pd.DataFrame(records).merge(player_teams, on=["game_id", "team_id"], how="left", validate="one_to_one")
    tg["game_date"] = pd.to_datetime(tg.game_date)
    tg["player_team_sog_exact_match"] = np.isclose(tg.official_team_sog, tg.player_sog_sum,
        atol=1e-9, rtol=0, equal_nan=False)
    tg["player_team_sog_difference"] = tg.official_team_sog - tg.player_sog_sum
    if tg.duplicated(["game_id", "team_id"]).any():
        raise ValueError("DUPLICATE_OPERATIONAL_OFFICIAL_GAME_TEAM")
    meta = {"input_files": sorted({x for x in source.source_path}),
            "official_boxscore_manifest": str(manifest_path.relative_to(ROOT)),
            "official_boxscore_manifest_sha256": sha(manifest_path),
            "official_boxscore_game_count": int(tg.game_id.nunique()),
            "team_games_exactly_reconciled_to_player_outcome_sum": int(tg.player_team_sog_exact_match.sum()),
            "team_game_mean_abs_boxscore_vs_player_sum": float(tg.player_team_sog_difference.abs().mean()),
            "team_games_missing_player_sog": int(tg.missing_player_sog.fillna(0).gt(0).sum()),
            "source_skater_rows": len(source), "source_game_count": int(source.game_id.nunique()),
            "null_sog_rows": int(source.official_sog.isna().sum()),
            "official_source_values": sorted(source.outcome_source.dropna().astype(str).unique().tolist())}
    return tg, meta


def _source_games() -> tuple[pd.DataFrame, dict]:
    hist = _boxscore_team_games()
    ops, ops_meta = _operational_team_games()
    combined = pd.concat([hist, ops], ignore_index=True, sort=False)
    combined["game_date"] = pd.to_datetime(combined.game_date).dt.normalize()
    combined["official_team_sog"] = pd.to_numeric(combined.official_team_sog, errors="coerce")
    if combined.duplicated(["game_id", "team_id"]).any():
        raise ValueError("SOURCE_GAME_TEAM_ID_COLLISION")
    return combined.sort_values(["team_id", "game_date", "game_id"]).reset_index(drop=True), {
        "boxscore_game_team_rows": len(hist), "boxscore_games": int(hist.game_id.nunique()),
        "boxscore_missing_team_sog": int(hist.official_team_sog.isna().sum()),
        "historical_zero_sog_team_games": int(hist.official_team_sog.eq(0).sum()),
        "boxscore_team_game_duplicates": 0, "operational": ops_meta}


def _target_periods() -> tuple[dict[str, pd.DataFrame], dict]:
    train_path = ROOT / "artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/output/canonical_training_frame.csv.gz"
    hold_path = RESTORE / "validation_input_2023_2025.csv.gz"
    train = pd.read_csv(train_path)
    hold = pd.read_csv(hold_path)
    # This is the exact historical feature arm bound to the frozen estimator.
    train = train.loc[train.history_contract.eq("120_DAY_LEGACY_BOUND") & train.canonical_season.isin([2023, 2024])].copy()
    hold = hold.loc[hold.history_contract.eq("120_DAY_LEGACY_BOUND") & hold.canonical_season.eq(2025)].copy()
    ops_path = ROOT / "artifacts/analysis/nhl/points_market_selection_population/2026-10-15/full_2026_operational_population.csv"
    ops = pd.read_csv(ops_path)
    ops = ops.loc[ops.game_date.astype(str).between("2026-10-09", "2026-10-14")].copy()
    outcome_files = sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation").glob(
        "2026-10-*/reconciliation=*/canonical_skater_outcomes.csv"))
    outcome_parts = []
    for path in outcome_files:
        if path.parent.parent.name in {f"2026-10-{d:02d}" for d in range(9, 15)}:
            part = pd.read_csv(path, usecols=["game_id", "player_id", "team_id", "official_final"])
            outcome_parts.append(part)
    outcomes = pd.concat(outcome_parts, ignore_index=True) if outcome_parts else pd.DataFrame()
    if len(outcomes):
        outcomes = outcomes.drop_duplicates(["game_id", "player_id"])
        binding = ops[["game_id", "player_id", "team_id"]].merge(
            outcomes, on=["game_id", "player_id"], how="left", suffixes=("_target", "_outcome"), validate="one_to_one")
        target_team = pd.to_numeric(binding.team_id_target, errors="coerce")
        outcome_team = pd.to_numeric(binding.team_id_outcome, errors="coerce")
        target_binding = {"target_population_rows": len(binding),
            "matched_official_player_games": int(outcome_team.notna().sum()),
            "unmatched_official_player_games": int(outcome_team.isna().sum()),
            "team_id_mismatches": int((target_team.notna() & outcome_team.notna() & target_team.ne(outcome_team)).sum()),
            "nonfinal_matched_rows": int(binding.official_final.astype(str).str.lower().isin({"false", "0"}).sum())}
    else:
        target_binding = {"status": "NO_OPERATIONAL_CANONICAL_OUTCOME_BINDING_SOURCE"}
    populations = {"TRAIN_2023_2024": train, "HOLDOUT_2025": hold, "OPERATIONS_2026_10_09_TO_14": ops}
    for name, df in populations.items():
        needed = {"game_id", "player_id", "team_id", "game_date", "team_d10_sf_per_game"}
        if not needed.issubset(df.columns):
            raise ValueError(f"MISSING_TARGET_COLUMNS:{name}:{sorted(needed - set(df.columns))}")
        if df.duplicated(["game_id", "player_id"]).any():
            raise ValueError(f"DUPLICATE_TARGET_IDENTITY:{name}")
        df["game_date"] = pd.to_datetime(df.game_date).dt.normalize()
        df["team_id"] = pd.to_numeric(df.team_id, errors="coerce")
        df["team_d10_sf_per_game"] = pd.to_numeric(df.team_d10_sf_per_game, errors="coerce")
        df["game_id"] = pd.to_numeric(df.game_id, errors="raise").astype("int64")
        df["player_id"] = pd.to_numeric(df.player_id, errors="raise").astype("int64")
        if name != "OPERATIONS_2026_10_09_TO_14":
            df["realized_points"] = pd.to_numeric(df.realized_points, errors="coerce")
    meta = {"inputs": {"training_frame": {"path": str(train_path.relative_to(ROOT)), "sha256": sha(train_path)},
                       "holdout_frame": {"path": str(hold_path.relative_to(ROOT)), "sha256": sha(hold_path)},
                       "operational_population": {"path": str(ops_path.relative_to(ROOT)), "sha256": sha(ops_path)}},
            "population_rows": {k: len(v) for k, v in populations.items()},
            "operational_target_player_team_binding": target_binding,
            "canonical_seasons": {"training": sorted(map(int, train.canonical_season.unique())), "holdout": [2025]}}
    return populations, meta


def _source_row_reconciliation(games: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Compare retained player-game SOG source sums to official team SOG."""
    source_specs = [
        ("TRAIN_2023_2024", ROOT / "artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/inputs/canonical_points_sources.csv.gz",
         "season", {2023, 2024}),
        ("HOLDOUT_2025", RESTORE / "skater_log_feature_source.csv.gz", "canonical_season", {2025}),
    ]
    direct = games.loc[games.source_period.eq("RETAINED_OFFICIAL_BOXSCORE"),
                       ["game_id", "team_id", "official_team_sog", "game_date", "raw_path"]].copy()
    direct = direct.rename(columns={"team_id": "official_team_id", "raw_path": "official_boxscore_path"})
    sides = {}
    for row in games.loc[games.source_period.eq("RETAINED_OFFICIAL_BOXSCORE")].itertuples(index=False):
        if pd.notna(row.home_team_id) and pd.notna(row.away_team_id):
            sides[int(row.game_id)] = (int(row.home_team_id), int(row.away_team_id))
    parts = []
    source_meta = {}
    for name, path, season_col, seasons in source_specs:
        df = pd.read_csv(path, low_memory=False)
        if season_col not in df:
            raise ValueError(f"SOURCE_SEASON_COLUMN_MISSING:{path}:{season_col}")
        df = df.loc[pd.to_numeric(df[season_col], errors="coerce").isin(seasons)].copy()
        if df.duplicated(["game_id", "player_id"]).any():
            raise ValueError(f"DUPLICATE_RAW_SOURCE_PLAYER_GAME:{name}")
        df["game_id"] = pd.to_numeric(df.game_id, errors="raise").astype("int64")
        df["is_home"] = pd.to_numeric(df.is_home, errors="coerce")
        df["shots_on_goal"] = pd.to_numeric(df.shots_on_goal, errors="coerce")
        df["team_id"] = pd.to_numeric(df.team_id, errors="coerce")
        df["official_team_id"] = [sides.get(int(gid), (np.nan, np.nan))[0 if h == 1 else 1]
                                   if pd.notna(h) else np.nan
                                   for gid, h in zip(df.game_id, df.is_home)]
        df["target_period"] = name
        per_team = df.groupby(["target_period", "game_id", "official_team_id"], dropna=False, as_index=False).agg(
            source_skater_rows=("player_id", "size"),
            source_unique_players=("player_id", "nunique"),
            source_team_ids=("team_id", lambda s: "|".join(map(str, sorted(s.dropna().unique())))),
            source_missing_team_id_rows=("team_id", lambda s: int(s.isna().sum())),
            source_missing_sog_rows=("shots_on_goal", lambda s: int(s.isna().sum())),
            source_sog_sum_zero_filled=("shots_on_goal", lambda s: float(s.fillna(0).sum())),
            source_sog_sum_valid_only=("shots_on_goal", lambda s: float(s.sum(min_count=1)) if s.notna().any() else np.nan),
            source_home_values=("is_home", lambda s: "|".join(map(str, sorted(s.dropna().unique())))))
        parts.append(per_team)
        source_meta[name] = {"path": str(path.relative_to(ROOT)), "sha256": sha(path),
            "player_game_rows": len(df), "source_games": int(df.game_id.nunique()),
            "source_team_games": len(per_team), "missing_team_id_rows": int(df.team_id.isna().sum()),
            "missing_sog_rows": int(df.shots_on_goal.isna().sum()),
            "target_games_without_official_boxscore": int((~df.game_id.isin(sides)).sum())}
    combined = pd.concat(parts, ignore_index=True)
    joined = combined.merge(direct, left_on=["game_id", "official_team_id"], right_on=["game_id", "official_team_id"],
                            how="left", validate="one_to_one")
    joined["official_sog_minus_source_zero_filled"] = joined.official_team_sog - joined.source_sog_sum_zero_filled
    joined["team_sog_exact_match"] = np.isclose(joined.official_team_sog, joined.source_sog_sum_zero_filled,
                                                atol=1e-9, rtol=0, equal_nan=False)
    joined["official_source_status"] = np.where(joined.official_team_sog.isna(), "MISSING_OFFICIAL_BOXSCORE",
                                                np.where(joined.source_missing_sog_rows.gt(0), "SOURCE_SOG_NULL_ZERO_FILLED",
                                                         "COMPLETE"))
    summary = {}
    for name, g in joined.groupby("target_period"):
        summary[name] = {"source_team_games": len(g), "official_team_games_matched": int(g.official_team_sog.notna().sum()),
            "exact_team_sog_sums": int(g.team_sog_exact_match.sum()),
            "exact_match_rate": float(g.team_sog_exact_match.mean()),
            "missing_source_sog_team_games": int(g.source_missing_sog_rows.gt(0).sum()),
            "missing_official_boxscore_team_games": int(g.official_team_sog.isna().sum()),
            "mean_abs_team_total_difference": float(g.official_sog_minus_source_zero_filled.abs().mean()),
            "max_abs_team_total_difference": float(g.official_sog_minus_source_zero_filled.abs().max())}
    return joined, {"sources": source_meta, "comparison": summary}


def reconstruct(populations: dict[str, pd.DataFrame], games: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    game_index = {int(tid): group.sort_values(["game_date", "game_id"], kind="mergesort")
                  for tid, group in games.loc[games.team_id.notna()].groupby("team_id")}
    all_targets, evidence = [], []
    chronology_bad = 0
    source_keys = set(map(tuple, games.loc[games.team_id.notna(), ["game_id", "team_id"]]
                           .assign(team_id=lambda d: pd.to_numeric(d.team_id, errors="coerce").astype("Int64"))
                           .dropna().astype({"game_id": "int64", "team_id": "int64"}).to_numpy()))
    source_homeaway = {(int(row.game_id), int(row.team_id)): (getattr(row, "is_home", np.nan),
                       getattr(row, "home_team_id", np.nan), getattr(row, "away_team_id", np.nan))
                       for row in games.loc[games.team_id.notna()].itertuples(index=False)}
    source_sides = {}
    for (gid, _), side in source_homeaway.items():
        if pd.notna(side[1]) and pd.notna(side[2]):
            source_sides[gid] = (int(side[1]), int(side[2]))
    alias_observations = defaultdict(list)
    games["history_team_identity_id"] = pd.to_numeric(games.team_id, errors="coerce")
    # The target game's official side is the identity anchor. Historical numeric
    # team IDs are not assumed to be universal: they can be missing or collide
    # across the retained input frame. Operations use canonical official IDs.
    binding = defaultdict(lambda: {"checked": 0, "mismatch": 0, "home_away_checked": 0, "home_away_mismatch": 0})
    for period, target in populations.items():
        result = target.copy().reset_index(drop=True)
        result["target_period"] = period
        result["resolved_official_team_id"] = result.team_id
        if period != "OPERATIONS_2026_10_09_TO_14" and "is_home" in result:
            def resolve_side(row):
                sides = source_sides.get(int(row.game_id))
                if not sides or pd.isna(row.is_home):
                    return np.nan
                return sides[0] if int(row.is_home) == 1 else sides[1]
            result["resolved_official_team_id"] = result.apply(resolve_side, axis=1)
            season_col = result["canonical_season"] if "canonical_season" in result else pd.Series(2026, index=result.index)
            alias_rows = pd.DataFrame({"team_id": result.team_id, "resolved": result.resolved_official_team_id,
                                       "season": season_col}).drop_duplicates()
            for row in alias_rows.itertuples(index=False):
                if pd.notna(row.team_id) and int(row.team_id) >= 0 and pd.notna(row.resolved):
                    alias_observations[(period, int(row.season), int(row.team_id))].append(int(row.resolved))
        result["history_team_games"] = 0
        result["official_prior_shots_sum"] = 0.0
        result["independent_team_d10"] = np.nan
        result["fixed10_diagnostic"] = np.nan
        result["observed_completed_game_count"] = 0
        result["missing_official_history_games"] = 0
        result["prior_game_ids"] = ""
        result["prior_game_dates"] = ""
        result["history_source_status"] = "COMPLETE"
        # Compute one history per unique team/date and bind it back to each player-game.
        cache = {}
        for tid_raw, date in result[["resolved_official_team_id", "game_date"]].drop_duplicates().itertuples(index=False, name=None):
            if pd.isna(tid_raw):
                cache[(tid_raw, date)] = pd.DataFrame()
                continue
            tid = int(tid_raw)
            g = game_index.get(tid, games.iloc[:0])
            hist = strict_prior_team_history(g, date, days=120, max_games=10)
            cache[(tid_raw, date)] = hist
            target_games = result.loc[result.resolved_official_team_id.eq(tid_raw) & result.game_date.eq(date), "game_id"].drop_duplicates()
            for target_game_id in target_games:
                for _, game in hist.iterrows():
                    evidence.append({"target_period": period, "target_game_id": int(target_game_id),
                        "target_game_date": date.date().isoformat(), "target_team_id": tid,
                        "prior_game_id": int(game.game_id), "prior_game_date": game.game_date.date().isoformat(),
                        "prior_history_depth": len(hist), "prior_team_id": int(game.team_id),
                        "official_team_sog": game.official_team_sog, "source_period": game.source_period,
                        "source_skater_rows": game.get("source_skater_rows", np.nan),
                        "missing_player_sog": game.get("missing_player_sog", np.nan),
                        "source_path": game.get("raw_path", "")})
        for i, row in result.iterrows():
            hist = cache.get((row.resolved_official_team_id, row.game_date), pd.DataFrame())
            if len(hist):
                complete = hist.official_team_sog.notna()
                valid = hist.loc[complete]
                # If source statistics are absent, the independent value is unknown;
                # the production's zero-fill is recorded only as a diagnostic.
                result.at[i, "history_team_games"] = len(valid)
                result.at[i, "observed_completed_game_count"] = len(hist)
                result.at[i, "missing_official_history_games"] = int((~complete).sum())
                result.at[i, "official_prior_shots_sum"] = valid.official_team_sog.sum()
                if len(valid) == len(hist) and len(valid):
                    result.at[i, "independent_team_d10"] = float(valid.official_team_sog.mean())
                if len(hist):
                    result.at[i, "fixed10_diagnostic"] = float(valid.official_team_sog.sum() / 10.0)
                if (~complete).any():
                    result.at[i, "history_source_status"] = "MISSING_OFFICIAL_STAT"
                ids = hist.game_id.astype(int).tolist()
                dates = hist.game_date.dt.strftime("%Y-%m-%d").tolist()
                result.at[i, "prior_game_ids"] = "|".join(map(str, ids))
                result.at[i, "prior_game_dates"] = "|".join(dates)
                if any(d >= row.game_date for d in hist.game_date):
                    chronology_bad += 1
            else:
                result.at[i, "history_source_status"] = "NO_PRIOR_TEAM_GAME"
                result.at[i, "independent_team_d10"] = 0.0
                result.at[i, "fixed10_diagnostic"] = 0.0
            if not pd.isna(row.team_id):
                bind = binding[period]
                bind["checked"] += 1
                key = (int(row.game_id), int(row.team_id))
                if period != "OPERATIONS_2026_10_09_TO_14" and pd.notna(row.get("is_home")):
                    # Physical team is resolved by the target game's official side;
                    # separately measure whether the input's numeric team ID agrees.
                    sides = source_sides.get(int(row.game_id))
                    expected = sides[0] if sides and int(row.is_home) == 1 else (sides[1] if sides else None)
                    bind["home_away_checked"] += 1
                    if expected is None or int(row.resolved_official_team_id) != expected:
                        bind["mismatch"] += 1
                    if expected is not None and int(row.team_id) in sides:
                        bind["numeric_id_checked"] = bind.get("numeric_id_checked", 0) + 1
                        if int(row.team_id) != expected:
                            bind["numeric_id_vs_side_mismatch"] = bind.get("numeric_id_vs_side_mismatch", 0) + 1
                elif period != "OPERATIONS_2026_10_09_TO_14" and key not in source_keys:
                    bind["mismatch"] += 1
        result["feature_difference"] = result.independent_team_d10 - result.team_d10_sf_per_game
        result["feature_exact_match"] = np.isclose(result.independent_team_d10, result.team_d10_sf_per_game,
                                                    atol=1e-9, rtol=0, equal_nan=False)
        all_targets.append(result)
    if chronology_bad:
        raise ValueError("FUTURE_GAME_IN_STRICT_PRIOR_HISTORY")
    return pd.concat(all_targets, ignore_index=True), pd.DataFrame(evidence), {
        "future_or_same_day_prior_rows": chronology_bad, "team_binding": dict(binding),
        "team_id_alias_identity_cardinality": {period: {"source_team_id_season_pairs": sum(1 for (p, _, _) in alias_observations if p == period),
                "one_to_one": sum(1 for (p, _, _), values in alias_observations.items() if p == period and len(set(values)) == 1),
                "conflicted": sum(1 for (p, _, _), values in alias_observations.items() if p == period and len(set(values)) > 1)}
                for period in populations},
        "team_id_alias_conflicts": {str(k): {str(team_id): int(n) for team_id, n in pd.Series(v).value_counts().items()}
                                    for k, v in alias_observations.items() if len(set(v)) > 1}}


def summarize(targets: pd.DataFrame, evidence: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    rows, denom = [], []
    for period, group in targets.groupby("target_period", sort=False):
        for lo, hi, label in DEPTHS:
            x = group.loc[group.history_team_games.between(lo, hi)].copy()
            if x.empty:
                continue
            diff = x.feature_difference.dropna()
            e = evidence.loc[evidence.target_period.eq(period) & evidence.prior_history_depth.between(lo, hi)] if len(evidence) else evidence
            unique_histories = e.drop_duplicates(["target_game_id", "target_team_id", "prior_game_id"])
            rows.append({"target_period": period, "prior_valid_team_games": label,
                "player_games": len(x), "contributing_team_games_player_weighted": int(x.history_team_games.sum()),
                "unique_target_team_game_contributions": int(len(unique_histories)),
                "actual_prior_shots_sum_distinct_target_histories": float(unique_histories.official_team_sog.sum()) if len(unique_histories) else 0.0,
                "unique_target_teams": int(x.team_id.nunique()), "actual_prior_shots_sum_player_weighted": float(x.official_prior_shots_sum.sum()),
                "mean_actual_shots_per_team_game": float((x.official_prior_shots_sum / x.history_team_games.replace(0, np.nan)).mean()) if x.history_team_games.gt(0).any() else 0.0,
                "independent_feature_mean": float(x.independent_team_d10.mean()),
                "frozen_feature_mean": float(x.team_d10_sf_per_game.mean()),
                "mean_difference_independent_minus_frozen": float(diff.mean()) if len(diff) else None,
                "max_abs_difference": float(diff.abs().max()) if len(diff) else None,
                "exact_agreement_rate": float(x.feature_exact_match.mean()),
                "missing_source_player_games": int(x.history_source_status.eq("MISSING_OFFICIAL_STAT").sum()),
                "missing_source_rate": float(x.history_source_status.eq("MISSING_OFFICIAL_STAT").mean()),
                "zero_official_team_game_count": int(unique_histories.official_team_sog.eq(0).sum()) if len(unique_histories) else 0})
            x["fixed10_feature"] = x.official_prior_shots_sum / 10.0
            x["observed_completed_divisor_feature"] = np.where(x.observed_completed_game_count.gt(0),
                                                      x.official_prior_shots_sum / x.observed_completed_game_count, 0.0)
            denom.append({"target_period": period, "prior_valid_team_games": label,
                "player_games": len(x), "mean_actual_valid_games": float(x.history_team_games.mean()),
                "mean_observed_completed_games": float(x.observed_completed_game_count.mean()),
                "mean_available_games_divisor": float(x.independent_team_d10.mean()),
                "mean_fixed_ten_divisor": float(x.fixed10_feature.mean()),
                "mean_observed_completed_game_divisor": float(x.observed_completed_divisor_feature.mean()),
                "mean_frozen_feature": float(x.team_d10_sf_per_game.mean()),
                "mean_abs_error_available_games": float((x.independent_team_d10-x.team_d10_sf_per_game).abs().mean()),
                "mean_abs_error_fixed_ten": float((x.fixed10_feature-x.team_d10_sf_per_game).abs().mean()),
                "mean_abs_error_observed_completed_game_divisor": float((x.observed_completed_divisor_feature-x.team_d10_sf_per_game).abs().mean())})
    pop = targets.groupby("target_period", sort=False).agg(
        player_games=("game_id", "size"), target_teams=("team_id", "nunique"),
        frozen_mean=("team_d10_sf_per_game", "mean"), independent_mean=("independent_team_d10", "mean"),
        exact_agreement_rate=("feature_exact_match", "mean"), mean_abs_difference=("feature_difference", lambda s: s.abs().mean()),
        max_abs_difference=("feature_difference", lambda s: s.abs().max()),
        no_prior_history=("history_team_games", lambda s: int(s.eq(0).sum())),
        missing_source_rows=("history_source_status", lambda s: int(s.eq("MISSING_OFFICIAL_STAT").sum()))).reset_index()
    settled = targets.loc[targets.target_period.eq("OPERATIONS_2026_10_09_TO_14") & targets.realized_points.notna()]
    pop = pd.concat([pop, pd.DataFrame([{"target_period": "OPERATIONS_2026_SETTLED",
        "player_games": len(settled), "target_teams": int(settled.team_id.nunique()),
        "frozen_mean": float(settled.team_d10_sf_per_game.mean()),
        "independent_mean": float(settled.independent_team_d10.mean()),
        "exact_agreement_rate": float(settled.feature_exact_match.mean()),
        "mean_abs_difference": float(settled.feature_difference.abs().mean()),
        "max_abs_difference": float(settled.feature_difference.abs().max()),
        "no_prior_history": int(settled.history_team_games.eq(0).sum()),
        "missing_source_rows": int(settled.history_source_status.eq("MISSING_OFFICIAL_STAT").sum())}])], ignore_index=True)
    # The settled 2026 population is primary; retain its equal-depth denominator
    # rows separately from all captured identities (some outcomes are unresolved).
    for lo, hi, label in DEPTHS:
        x = settled.loc[settled.history_team_games.between(lo, hi)].copy()
        if x.empty:
            continue
        keys = x[["game_id", "team_id", "game_date"]].drop_duplicates().rename(
            columns={"game_id": "target_game_id", "team_id": "target_team_id", "game_date": "target_game_date"})
        e = evidence.loc[evidence.target_period.eq("OPERATIONS_2026_10_09_TO_14")].merge(
            keys, on=["target_game_id", "target_team_id"], how="inner")
        e = e.loc[e.prior_history_depth.between(lo, hi)].drop_duplicates(
            ["target_game_id", "target_team_id", "prior_game_id"])
        diff = x.feature_difference.dropna()
        rows.append({"target_period": "OPERATIONS_2026_SETTLED", "prior_valid_team_games": label,
            "player_games": len(x), "contributing_team_games_player_weighted": int(x.history_team_games.sum()),
            "unique_target_team_game_contributions": int(len(e)),
            "actual_prior_shots_sum_distinct_target_histories": float(e.official_team_sog.sum()) if len(e) else 0.0,
            "unique_target_teams": int(x.team_id.nunique()), "actual_prior_shots_sum_player_weighted": float(x.official_prior_shots_sum.sum()),
            "mean_actual_shots_per_team_game": float((x.official_prior_shots_sum / x.history_team_games.replace(0, np.nan)).mean()) if x.history_team_games.gt(0).any() else 0.0,
            "independent_feature_mean": float(x.independent_team_d10.mean()), "frozen_feature_mean": float(x.team_d10_sf_per_game.mean()),
            "mean_difference_independent_minus_frozen": float(diff.mean()) if len(diff) else None,
            "max_abs_difference": float(diff.abs().max()) if len(diff) else None,
            "exact_agreement_rate": float(x.feature_exact_match.mean()),
            "missing_source_player_games": int(x.history_source_status.eq("MISSING_OFFICIAL_STAT").sum()),
            "missing_source_rate": float(x.history_source_status.eq("MISSING_OFFICIAL_STAT").mean()),
            "zero_official_team_game_count": int(e.official_team_sog.eq(0).sum()) if len(e) else 0})
        x["fixed10_feature"] = x.official_prior_shots_sum / 10.0
        x["observed_completed_divisor_feature"] = np.where(x.observed_completed_game_count.gt(0),
                                                  x.official_prior_shots_sum / x.observed_completed_game_count, 0.0)
        denom.append({"target_period": "OPERATIONS_2026_SETTLED", "prior_valid_team_games": label,
            "player_games": len(x), "mean_actual_valid_games": float(x.history_team_games.mean()),
            "mean_observed_completed_games": float(x.observed_completed_game_count.mean()),
            "mean_available_games_divisor": float(x.independent_team_d10.mean()),
            "mean_fixed_ten_divisor": float(x.fixed10_feature.mean()),
            "mean_observed_completed_game_divisor": float(x.observed_completed_divisor_feature.mean()),
            "mean_frozen_feature": float(x.team_d10_sf_per_game.mean()),
            "mean_abs_error_available_games": float((x.independent_team_d10-x.team_d10_sf_per_game).abs().mean()),
            "mean_abs_error_fixed_ten": float((x.fixed10_feature-x.team_d10_sf_per_game).abs().mean()),
            "mean_abs_error_observed_completed_game_divisor": float((x.observed_completed_divisor_feature-x.team_d10_sf_per_game).abs().mean())})
    return pd.DataFrame(rows), pd.DataFrame(denom), {"population": pop.to_dict("records")}


def counterfactual(targets: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    fitted = joblib.load(MODEL_PATH)
    if fitted.get("feature_columns") != FEATURES:
        raise ValueError("FROZEN_MODEL_FEATURE_ORDER_MISMATCH")
    scaler, estimator = fitted["scaler"], fitted["model"]
    report, rows = [], []
    for period in ("TRAIN_2023_2024", "HOLDOUT_2025", "OPERATIONS_2026_10_09_TO_14"):
        x = targets.loc[targets.target_period.eq(period)].copy()
        x = x.loc[x.independent_team_d10.notna() & x.realized_points.notna()].copy()
        original = x[FEATURES].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0).astype(float)
        altered = original.copy()
        altered["team_d10_sf_per_game"] = x.independent_team_d10.to_numpy()
        mu_before = np.maximum(estimator.predict(scaler.transform(original.to_numpy())), 1e-8)
        mu_after = np.maximum(estimator.predict(scaler.transform(altered.to_numpy())), 1e-8)
        y = x.realized_points.to_numpy(dtype=float)
        over = (y >= 1).astype(int)
        p_before, p_after = poisson.sf(0, mu_before), poisson.sf(0, mu_after)
        lb, la = np.log(np.clip(p_before, 1e-15, 1-1e-15)), np.log(np.clip(p_after, 1e-15, 1-1e-15))
        ub, ua = np.log(np.clip(1-p_before, 1e-15, 1-1e-15)), np.log(np.clip(1-p_after, 1e-15, 1-1e-15))
        pred_over_b, pred_over_a = p_before >= 0.5, p_after >= 0.5
        high_under = (p_before <= 0.35) & (y >= 1)
        correct_hu_before = int(((~pred_over_b) & (over == 0) & (p_before <= 0.35)).sum())
        wrong_hu_before = int(high_under.sum())
        correct_hu_after = int(((~pred_over_a) & (over == 0) & (p_after <= 0.35)).sum())
        wrong_hu_after = int(((~pred_over_a) & (over == 1) & (p_after <= 0.35)).sum())
        market_comparison = {"status": "NOT_AVAILABLE"}
        if "p_over_mkt" in x:
            market_p = pd.to_numeric(x.p_over_mkt, errors="coerce").to_numpy(dtype=float)
            valid_market = np.isfinite(market_p)
            market_disagree = valid_market & ((p_before >= 0.5) != (market_p >= 0.5))
            same_side = valid_market & ~market_disagree
            def subset_metrics(mask):
                if not mask.any():
                    return {"n": 0}
                return {"n": int(mask.sum()),
                    "mean_residual_before": float((y[mask]-mu_before[mask]).mean()),
                    "mean_residual_after": float((y[mask]-mu_after[mask]).mean()),
                    "brier_before": float(np.mean((p_before[mask]-over[mask])**2)),
                    "brier_after": float(np.mean((p_after[mask]-over[mask])**2))}
            market_comparison = {"status": "COMPLETE" if valid_market.any() else "NO_MARKET_QUOTES",
                "definition": "model P(Over 0.5) side versus retained market P(Over 0.5) side",
                "same_side": subset_metrics(same_side), "market_opposed": subset_metrics(market_disagree)}
        record = {"period": period, "evaluation_role": "IN_SAMPLE_RESPONSE_ONLY" if period == "TRAIN_2023_2024" else "SETTLED_HOLDOUT_SENSITIVITY",
            "verified_rows": len(x), "rows_with_changed_feature": int((x.feature_difference.abs() > 1e-9).sum()),
            "mean_expected_points_before": float(mu_before.mean()), "mean_expected_points_after": float(mu_after.mean()),
            "mean_expected_points_change": float((mu_after-mu_before).mean()), "mean_realized_points": float(y.mean()),
            "mean_residual_realized_minus_predicted_before": float((y-mu_before).mean()),
            "mean_residual_realized_minus_predicted_after": float((y-mu_after).mean()),
            "over_0_5_brier_before": float(np.mean((p_before-over)**2)), "over_0_5_brier_after": float(np.mean((p_after-over)**2)),
            "over_0_5_log_loss_before": float(-np.mean(over*lb+(1-over)*ub)),
            "over_0_5_log_loss_after": float(-np.mean(over*la+(1-over)*ua)),
            "high_confidence_under_correct_before": correct_hu_before, "high_confidence_under_wrong_before": wrong_hu_before,
            "high_confidence_under_correct_after": correct_hu_after, "high_confidence_under_wrong_after": wrong_hu_after,
            "market_opposition_secondary": market_comparison,
            "only_team_feature_changed": True}
        report.append(record)
        x["expected_points_original"] = mu_before
        x["expected_points_independent_team_shots"] = mu_after
        x["counterfactual_expected_points_change"] = mu_after-mu_before
        x["prob_over_0_5_original"] = p_before
        x["prob_over_0_5_independent_team_shots"] = p_after
        rows.append(x[["target_period", "game_id", "player_id", "game_date", "team_id", "realized_points",
                       "team_d10_sf_per_game", "independent_team_d10", "feature_difference", "expected_points_original",
                       "expected_points_independent_team_shots", "counterfactual_expected_points_change",
                       "prob_over_0_5_original", "prob_over_0_5_independent_team_shots"]])
    return pd.concat(rows, ignore_index=True), {"frozen_model_sha256": sha(MODEL_PATH), "metrics": report,
        "interpretation": "frozen-model sensitivity only; not out-of-time model improvement evidence"}


def run() -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    games, source_meta = _source_games()
    populations, population_meta = _target_periods()
    raw_reconciliation, raw_reconciliation_meta = _source_row_reconciliation(games)
    targets, evidence, checks = reconstruct(populations, games)
    depth, denominators, metrics = summarize(targets, evidence)
    cf_rows, cf = counterfactual(targets)
    targets.to_csv(OUT / "target_feature_reconstruction.csv", index=False, float_format="%.17g")
    evidence.to_csv(OUT / "contributing_team_game_evidence.csv", index=False, float_format="%.17g")
    games.to_csv(OUT / "official_team_game_source.csv", index=False, float_format="%.17g")
    raw_reconciliation.to_csv(OUT / "source_to_official_team_game_reconciliation.csv", index=False, float_format="%.17g")
    depth.to_csv(OUT / "history_depth_comparison.csv", index=False, float_format="%.17g")
    denominators.to_csv(OUT / "denominator_diagnostics.csv", index=False, float_format="%.17g")
    cf_rows.to_csv(OUT / "verified_frozen_model_counterfactual_rows.csv", index=False, float_format="%.17g")
    (OUT / "frozen_model_counterfactual_metrics.json").write_text(json.dumps(cf, indent=2, sort_keys=True, default=_json_default)+"\n")
    acquisition = json.loads((OUT / "official_boxscore_acquisition_manifest.json").read_text())
    source_id_checks = checks["team_binding"].get("TRAIN_2023_2024", {})
    train_targets = targets.loc[targets.target_period.eq("TRAIN_2023_2024")]
    no_history_nonzero = train_targets.loc[train_targets.history_team_games.eq(0) & train_targets.team_d10_sf_per_game.ne(0)]
    train_alias_rows = train_targets.loc[train_targets.canonical_season.eq(2023) & train_targets.team_id.notna() &
        train_targets.resolved_official_team_id.notna()]
    train_numeric_alias_checked = train_alias_rows.loc[train_alias_rows.team_id.isin(
        train_alias_rows.groupby("game_id").resolved_official_team_id.unique().explode().dropna().unique())]
    summary = {"schema_version": "NHL_POINTS_TEAM_SHOTS_TWO_SIDED_LINEAGE_AUDIT_V1",
        "classification": "MIXED_FINDINGS", "created_date": "2026-10-15", "sources": source_meta,
        "source_row_reconciliation": raw_reconciliation_meta,
        "populations": population_meta, "checks": checks, **metrics, "counterfactual": cf,
        "attempts_shared_upstream": {"common_source_table": "nhl.skater_game_logs_raw", "common_player_game_record": True,
            "features": ["shots_on_goal", "shot_attempts"],
            "shared_zero_fill_in_historical_normalizer": True,
            "confirmed_single_provider_or_parser_failure": False,
            "prior_attempts_status": "CONFIRMED_OPERATIONAL_ZEROING_DEFECT; causal responsibility not established"},
        "historical_identity_findings": {"training_missing_team_id_rows": raw_reconciliation_meta["sources"]["TRAIN_2023_2024"]["missing_team_id_rows"],
            "training_rows_with_numeric_team_id_disagreeing_with_official_home_away_when_comparable": source_id_checks.get("numeric_id_vs_side_mismatch", 0),
            "training_numeric_id_side_comparison_n": source_id_checks.get("numeric_id_checked", 0),
            "training_conflicted_team_id_season_pairs": checks["team_id_alias_identity_cardinality"]["TRAIN_2023_2024"]["conflicted"],
            "training_no_prior_physical_team_history_but_frozen_value_nonzero_player_games": int(len(no_history_nonzero)),
            "training_no_prior_physical_team_history_but_frozen_nonzero_examples": no_history_nonzero[["canonical_season", "game_id", "game_date", "team_id", "resolved_official_team_id", "team_d10_sf_per_game"]].drop_duplicates().to_dict("records")},
        "official_operational_acquisition": {"provider": acquisition["provider"], "game_count": acquisition["game_count"],
            "team_game_rows": acquisition["team_game_rows"], "network_attempt_count": sum(int(x.get("attempt", 1)) for x in acquisition["responses"]),
            "paid_credits": acquisition["paid_credits"], "production_database_mutations": acquisition["production_database_mutations"]},
        "production_changes": "NONE", "provider_calls": len(acquisition["responses"]), "paid_credits": 0, "production_database_mutations": 0}
    settled_2026 = targets.loc[targets.target_period.eq("OPERATIONS_2026_10_09_TO_14") & targets.realized_points.notna()]
    summary["settled_primary_2026"] = {"player_games": int(len(settled_2026)),
        "frozen_mean": float(settled_2026.team_d10_sf_per_game.mean()),
        "independent_mean": float(settled_2026.independent_team_d10.mean()),
        "exact_agreement_rate": float(settled_2026.feature_exact_match.mean()),
        "mean_absolute_difference": float(settled_2026.feature_difference.abs().mean()),
        "no_prior_history": int(settled_2026.history_team_games.eq(0).sum())}
    (OUT / "feature_contract.json").write_text(json.dumps({
        "schema_version": "NHL_POINTS_TEAM_D10_SF_CONTRACT_AUDIT_V1",
        "intended_statistical_contract": "mean team shots on goal across the team's latest up to 10 completed, valid regular-season team-games strictly before target game date, within prior 120 calendar days; denominator is number of valid games; no qualifying history maps to neutral zero; real zero SOG is valid",
        "implemented_historical_contract": "historical builder groups player-game shots_on_goal by raw team_id/game_id/date, uses latest up to 10 strict-prior rows within 120 days, averages available rows; source normalization zero-fills missing SOG; no fixed-ten or schedule divisor",
        "implemented_operational_contract": "operational exporter groups player-game shots_on_goal by canonical team_id/game_id/date, uses latest up to 10 strict-prior rows within 120 days, averages available rows; missing SOG is zero-filled; no fixed-ten or schedule divisor",
        "frozen_model_learned_contract": "numeric team_d10_sf_per_game column in NHL_POINTS_LEADER_CORE_FEATURES_V1, fitted on 2023-24 historical feature frame then passed through frozen StandardScaler; missing/nonfinite model inputs are filled with zero",
        "season_boundary": "120-calendar-day window normally prevents summer carryover; actual source rows determine whether boundary is crossed",
        "target_date_cutoff": "strict prior by game_date; excludes all games on target date regardless of start-time ordering",
        "scaling": "no feature scaling in either feature builder; frozen model applies its retained StandardScaler",
        "intended_vs_implemented_status": "same formula described, but source identity and source values fail validation in subsets; exact upstream operational divergence stage unresolved"
    }, indent=2, sort_keys=True)+"\n")
    (OUT / "coverage_exclusion_manifest.json").write_text(json.dumps({
        "schema_version": "NHL_POINTS_TEAM_SHOTS_AUDIT_COVERAGE_V1",
        "historical_training": {"canonical_seasons": [2023, 2024], "player_games": int(len(train_targets)), "official_source_team_games": raw_reconciliation_meta["comparison"]["TRAIN_2023_2024"]["official_team_games_matched"]},
        "historical_holdout": {"canonical_seasons": [2025], "player_games": int(targets.target_period.eq("HOLDOUT_2025").sum()), "official_source_team_games": raw_reconciliation_meta["comparison"]["HOLDOUT_2025"]["official_team_games_matched"]},
        "operational": {"target_dates": ["2026-10-09", "2026-10-14"], "target_player_games": int(targets.target_period.eq("OPERATIONS_2026_10_09_TO_14").sum()), "settled_primary_rows": int(len(settled_2026)), "official_history_games": acquisition["game_count"], "official_history_dates": acquisition["scope"]},
        "excluded": ["Oct. 15, 2026 outcomes", "preseason and postseason boxscores", "future or same-target-date games", "rows whose official prior source stat is missing from valid-history averages"],
        "scheduled_unplayed_games": "not separately reconstructed; the independent history uses completed official game rows, and no direct team-game SOG values were missing in the inspected prior histories",
        "unresolved_rows": {"operational_target_outcomes_not_settled": int(targets.loc[targets.target_period.eq("OPERATIONS_2026_10_09_TO_14"), "realized_points"].isna().sum()),
            "target_rows_without_official_prior_game": int(targets.history_source_status.eq("NO_PRIOR_TEAM_GAME").sum())},
        "production_database_mutations": 0, "paid_odds_calls": 0
    }, indent=2, sort_keys=True)+"\n")
    (OUT / "discrepancy_classification.json").write_text(json.dumps({
        "classification": "MIXED_FINDINGS",
        "findings": [
            {"period": "TRAIN_2023_2024", "classification": "HISTORICAL_FEATURE_DEFECT", "evidence": "2023 numeric team IDs disagree with the target game's official home/away identity in 2,194 of 8,043 comparable rows; three team_id-season pairs are conflicted; 53 opening-week player-games have nonzero frozen features despite no official prior games for their physical teams"},
            {"period": "HOLDOUT_2025", "classification": "LEGITIMATE_SOURCE_VALUE_DIFFERENCE", "evidence": "independent mean 27.599 versus frozen mean 27.428; mean absolute target difference 0.200; no history maps to zero; direct game-team SOG source and skater-row sum differ in 10.3% of team-games"},
            {"period": "OPERATIONS_2026_10_09_TO_14", "classification": "OPERATIONAL_FEATURE_DEFECT", "evidence": "direct NHL team boxscores yield settled-history mean 25.972 versus frozen feature mean 14.907; exact row agreement 0%; discrepancy persists at equal observed history depth and no-history targets contain nonzero frozen features"},
            {"period": "ROOT_STAGE", "classification": "INSUFFICIENT_SOURCE_EVIDENCE", "evidence": "the retained pregame 2026 player-log snapshot and field-level normalizer output are absent, so the first exact stage where direct source truth becomes the captured feature cannot be identified"}
        ],
        "next_action": "recover immutable field-level pregame skater SOG input for affected 2026 dates; reconcile to direct official team-game boxscores before any implementation change or retraining",
        "production_change_authorized_or_performed": False
    }, indent=2, sort_keys=True)+"\n")
    prior_studies = {
        "cross_season_feature_response": ROOT / "artifacts/analysis/nhl/points_cross_season_feature_response/2026-10-15",
        "scoring_history_root_cause": ROOT / "artifacts/analysis/nhl/points_scoring_history_root_cause/2026-10-15",
        "history_volume_sensitivity": ROOT / "artifacts/analysis/nhl/points_history_volume_sensitivity/2026-10-15",
        "attempts_feature_causal_replay": ROOT / "artifacts/analysis/nhl/points_attempts_feature_causal_replay/2026-10-15"}
    prior_manifest = {}
    for name, directory in prior_studies.items():
        summary_file = directory / "summary.json"
        prior_manifest[name] = {"path": str(directory.relative_to(ROOT)),
            "summary_path": str(summary_file.relative_to(ROOT)) if summary_file.exists() else None,
            "summary_sha256": sha(summary_file) if summary_file.exists() else None,
            "backlog_context_path": str((directory / "backlog_context.md").relative_to(ROOT))
                if (directory / "backlog_context.md").exists() else None}
    (OUT / "prior_studies_manifest.json").write_text(json.dumps(prior_manifest, indent=2, sort_keys=True)+"\n")
    backlog = ROOT / "artifacts/analysis/nhl/points_attempts_feature_causal_replay/2026-10-15/backlog_context.md"
    preserved = backlog.read_text() if backlog.exists() else "Existing NHL Points backlog context was unavailable.\n"
    (OUT / "backlog_context.md").write_text(preserved.rstrip()+
        "\n\n## Result of this audit\n\n"
        "The two-sided audit confirms historical opening-week 2023 team identity and operational Oct. 9–14 team-shot feature defects. "
        "Next: preserve the existing shot-attempt repair, Funnel V1 validation, and cross-market review backlog; first recover and reconcile immutable pregame team-shot inputs, then rebuild and validate the feature contract. "
        "No calibration, retraining, production feature, Funnel, or authority change is proposed by this audit.\n")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=_json_default)+"\n")
    _write_report(summary, depth, denominators)
    return summary


def _write_report(summary: dict, depth: pd.DataFrame, denominators: pd.DataFrame) -> None:
    def markdown_table(frame: pd.DataFrame, columns: list[str] | None = None) -> str:
        use = frame.loc[:, columns] if columns else frame
        headers = list(use.columns)
        body = [[("" if pd.isna(v) else (f"{v:.3f}" if isinstance(v, (float, np.floating)) else str(v)))
                 for v in row] for row in use.itertuples(index=False, name=None)]
        return "| " + " | ".join(headers) + " |\n| " + " | ".join("---" for _ in headers) + " |\n" + "\n".join(
            "| " + " | ".join(row) + " |" for row in body)
    pop = pd.DataFrame(summary["population"]).set_index("target_period")
    train, hold, ops_all = (pop.loc[x] for x in ("TRAIN_2023_2024", "HOLDOUT_2025", "OPERATIONS_2026_10_09_TO_14"))
    ops = summary["settled_primary_2026"]
    raw = summary["source_row_reconciliation"]["comparison"]
    training_identity = summary["historical_identity_findings"]
    ops_bind = summary["populations"]["operational_target_player_team_binding"]
    equal_depth = depth.loc[depth.target_period.isin(["TRAIN_2023_2024", "HOLDOUT_2025", "OPERATIONS_2026_SETTLED"]),
        ["target_period", "prior_valid_team_games", "player_games", "contributing_team_games_player_weighted",
         "mean_actual_shots_per_team_game", "independent_feature_mean", "frozen_feature_mean",
         "mean_difference_independent_minus_frozen", "max_abs_difference", "exact_agreement_rate", "missing_source_rate"]]
    lines = ["# NHL Points two-sided team-shot integrity audit", "",
        "## Result", "",
        "**MIXED_FINDINGS.** The 2025 holdout mean of 27.43 is broadly consistent with direct NHL team-game totals, but row values are not exact. The 2023–24 training frame contains a confirmed opening-week team-identity defect. For the primary Oct. 9–14, 2026 settled cohort, direct team boxscores reconstruct mean prior-team SOG of 25.97 against a captured feature mean of 14.91, with no row exact matches. Equal-history-depth checks show this is not explained by the early-season denominator alone. The exact operational transformation where the values diverge is not retained.", "",
        "## Three contracts", "",
        "**Intended statistical contract.** Mean team shots on goal (SOG, not attempts) over the latest up to ten completed, valid regular-season team games strictly before the target date, limited to 120 calendar days. Denominator is actual valid game count. A genuine zero-shot game remains a valid denominator row. If no qualifying game exists, feature value is neutral zero. A missing SOG statistic is unknown and must not be silently treated as a valid zero. Same-date games are excluded wholesale; no start-time ordering within the target date is used. A 120-day window prevents normal summer carryover. Postponed games contribute only when completed; preseason and postseason are excluded by the canonical regular-season source.", "",
        "**Implemented feature contracts.** Historical training/holdout code groups source skater rows by raw `(team_id, game_id, game_date)`, sums `shots_on_goal`, and averages up to ten strict-prior group sums in 120 days. It zero-fills missing skater SOG before aggregation. The operational exporter uses the same nominal window and divisor, but applies current canonical team IDs and its retained player-log inputs; it also zero-fills missing SOG. Neither code uses fixed ten or scheduled games as the denominator. The shared formula does not guarantee shared identity or source coverage.", "",
        "**Frozen-model learned contract.** The frozen HGB was trained on the 2023–24 numeric feature frame. `team_d10_sf_per_game` is one feature in the fixed ten-column CORE contract. Feature construction itself does not scale it; the frozen `StandardScaler` applies training means/scales at scoring. Missing/nonfinite model inputs are filled with zero before scaling. The model therefore learned the historical representation as supplied, including its identified identity anomaly; it has not been shown to learn only correct official team histories.", "",
        "## Independent source reconstruction", "",
        "For 2023–25, each source skater's team was bound to that target game's official home/away side, then its player SOG sum was compared with retained NHL Gamecenter `homeTeam.sog` / `awayTeam.sog` totals. For 2026, 53 free official NHL Gamecenter boxscores (106 team-game totals) were acquired for Oct. 7–13; these directly supply strictly prior histories for Oct. 9–14 targets. Canonical official per-skater postgame SOG was kept as a separate cross-check, not used as the team-total truth. No paid odds service or production database was contacted.", "",
        "| Population | Player-games | Frozen mean | Independent team-history mean | Exact target agreement | Mean absolute difference | No prior team history |",
        "|---|---:|---:|---:|---:|---:|---:|",
        f"| 2023–24 training | {int(train.player_games):,} | {train.frozen_mean:.3f} | {train.independent_mean:.3f} | {train.exact_agreement_rate:.1%} | {train.mean_abs_difference:.3f} | {int(train.no_prior_history):,} |",
        f"| 2025 holdout | {int(hold.player_games):,} | {hold.frozen_mean:.3f} | {hold.independent_mean:.3f} | {hold.exact_agreement_rate:.1%} | {hold.mean_abs_difference:.3f} | {int(hold.no_prior_history):,} |",
        f"| 2026 Oct. 9–14 all captured | {int(ops_all.player_games):,} | {ops_all.frozen_mean:.3f} | {ops_all.independent_mean:.3f} | {ops_all.exact_agreement_rate:.1%} | {ops_all.mean_abs_difference:.3f} | {int(ops_all.no_prior_history):,} |",
        f"| 2026 settled primary cohort | {ops['player_games']:,} | {ops['frozen_mean']:.3f} | {ops['independent_mean']:.3f} | {ops['exact_agreement_rate']:.1%} | {ops['mean_absolute_difference']:.3f} | {ops['no_prior_history']:,} |", "",
        "The 2026 independent and frozen means are evaluated on the same settled identities; 71 settled target player-games had no official prior game, yet the captured feature was nonzero on those rows. The remaining 1,249 settled rows had at least one direct official prior team game, and captured values still materially understated the independent history. The 480 all-population rows without settled outcomes are kept out of the primary performance replay.", "",
        "### Source-to-boxscore validation", "",
        "| Source population | Team-games | Direct official totals matched | Exact skater-sum matches | Exact rate | Mean absolute per-game difference | Missing SOG values |",
        "|---|---:|---:|---:|---:|---:|---:|",
        *[f"| {label} | {r['source_team_games']:,} | {r['official_team_games_matched']:,} | {r['exact_team_sog_sums']:,} | {r['exact_match_rate']:.1%} | {r['mean_abs_team_total_difference']:.3f} | {r['missing_source_sog_team_games']:,} |"
          for label, r in raw.items()],
        "| 2026 canonical outcomes vs direct boxscores | 106 | 106 | {}/106 | {:.1%} | {:.3f} | {} |".format(
            summary["sources"]["operational"]["team_games_exactly_reconciled_to_player_outcome_sum"],
            summary["sources"]["operational"]["team_games_exactly_reconciled_to_player_outcome_sum"]/106,
            summary["sources"]["operational"]["team_game_mean_abs_boxscore_vs_player_sum"],
            summary["sources"]["operational"]["team_games_missing_player_sog"]), "",
        "Direct source comparison shows why the historical feature is not an exact copy of the NHL team totals: skater sums differed from direct team SOG in 205/5,028 training team-games (4.1%) and 268/2,612 holdout team-games (10.3%). Average differences are small, but official boxscore totals define the independent reference here. In 2026, skater outcomes matched direct team totals in 98/106 team-games (92.5%); the direct boxscores were used for the feature reconstruction.", "",
        "## Equal history depth and denominator diagnostics", "",
        markdown_table(equal_depth), "",
        markdown_table(denominators.loc[denominators.target_period.isin(["TRAIN_2023_2024", "HOLDOUT_2025", "OPERATIONS_2026_SETTLED"])]), "",
        "The independent history calculation uses actual valid completed team-games. The fixed-ten column is a diagnostic (prior shots / 10). The observed-completed-game divisor is prior shots divided by the direct source game rows available; because no direct official team-game SOG values are missing in these histories, it equals the valid-game divisor. A separate count of every scheduled but unplayed or postponed game was not reconstructed; those games have no valid completed-game statistic and are excluded from the intended contract. Fixed ten sharply understates shallow history and is not the implemented contract. 2026's captured feature fails even in the 1–2 and 3–5 history groups, where direct independent means remain about 27 SOG per game.", "",
        "## Identity, chronology, missingness, and opening-season cases", "",
        f"{summary['checks']['future_or_same_day_prior_rows']} reconstructed prior rows were same-day or future; duplicate official team-game rows were rejected. Historical target-team physical identity was resolved through the target game's official home/away side. In the 2025 holdout, the raw numeric team ID matched that side on all {summary['checks']['team_binding']['HOLDOUT_2025']['numeric_id_checked']:,} checked rows. In training, numeric IDs could be compared to an official side on {training_identity['training_numeric_id_side_comparison_n']:,} rows and disagreed on {training_identity['training_rows_with_numeric_team_id_disagreeing_with_official_home_away_when_comparable']:,}; three 2023 team-ID/season pairs mapped to multiple official teams, and {training_identity['training_missing_team_id_rows']} source rows had missing team IDs.", "",
        f"The concrete training anomaly is opening week 2023: {training_identity['training_no_prior_physical_team_history_but_frozen_value_nonzero_player_games']} player-games across three team-games have no official prior physical-team games but nonzero frozen features (34, 36, and 41). The target's official team IDs are 26, 8, and 12 while the raw training IDs are 14, 16, and 5. This is evidence of a historical team-identity/grouping defect, not an early-season denominator effect.", "",
        f"Operational target player-team binding was compared with official canonical outcomes: {ops_bind.get('matched_official_player_games', 0):,}/{ops_bind.get('target_population_rows', 0):,} matched; team-ID mismatches: {ops_bind.get('team_id_mismatches', 0):,}; unmatched rows: {ops_bind.get('unmatched_official_player_games', 0):,}. The 2026 official source records have {summary['sources']['operational']['null_sog_rows']} missing player SOG values, but direct NHL team totals remain the reconstruction reference. Historical source normalization zero-fills missing SOG; there were {sum(v['missing_source_sog_team_games'] for v in raw.values())} historical source team-games with missing player SOG. Source zeros are retained as real values, never discarded as missing. Full target and contributing prior-game IDs, dates, source paths, hashes, and values are in the CSV evidence files.", "",
        "The operational pregame player-log snapshot and field-level normalizer intermediate are not retained. The discrepancy is confirmed between the captured feature and direct official prior histories, but whether the first divergence was at source row coverage, team/player binding, null handling, or aggregation cannot be isolated from retained artifacts. The postgame official source verifies target identities and outcomes, not the exact pregame input state.", "",
        "## Shared lineage with attempts defect", "",
        "The existing `attempts_d10_per60` issue and this feature both involve NHL skater game logs and the `shots_on_goal` / `shot_attempts` family. Historical normalization has a missing-to-zero transformation. This is evidence of a shared data family and a possible row-coverage/missingness boundary, not proof that one provider request or parser failure caused both defects. The attempts repair is outside this audit.", "",
        "## Frozen-model sensitivity", "",
        "These are one-feature replays with the frozen HGB and StandardScaler, holding all other inputs constant. Training rows are in-sample response only. 2025 and 2026 values are settled-outcome sensitivity checks, not prospective improvement evidence. Rows without an independently verified value are excluded; none were replaced with a median or hypothetical rate. Market comparisons use 2026 quotes only and are secondary.", "",
        markdown_table(pd.DataFrame(summary["counterfactual"]["metrics"])), "",
        "On 2025, changing to direct history shifts the mean expected count by only +0.0011, while Brier/log loss move slightly worse. On 2026, the verified feature shifts mean expected Points from 0.317 to 0.375; residual falls from +0.149 to +0.091, Brier from 0.2212 to 0.2157, and log loss from 0.6347 to 0.6212. In the 2026 market-opposed group (281 rows), residual falls +0.356 to +0.268 and Brier .2868 to .2746; same-side (550 rows) residual falls +0.137 to +0.073 and Brier .2247 to .2191. These observed-outcome metrics describe this capture only and do not establish the corrected model's future performance.", "",
        "## Classification and next action", "",
        "Classification: `MIXED_FINDINGS`. `HISTORICAL_FEATURE_DEFECT` is confirmed for opening 2023 team identity; 2025 is close in aggregate but not exact; `OPERATIONAL_FEATURE_DEFECT` is confirmed for Oct. 9–14; and `INSUFFICIENT_SOURCE_EVIDENCE` applies to the exact operational root stage. The narrow next action is to recover immutable pregame player-log inputs and reconcile their game/team/player rows to official Gamecenter team totals, then correct the historical identity issue and rebuild the feature contract consistently before considering retraining. Do not substitute operational values or retrain based solely on this frozen replay.", "",
        f"Official NHL network calls: {summary['provider_calls']} games (free). Paid credits: 0. Production database mutations: 0. Production/Funnel changes: NONE. Oct. 15 outcomes: not accessed. Package: `artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/`." ]
    (OUT / "report.md").write_text("\n".join(lines)+"\n")


if __name__ == "__main__":
    run()
