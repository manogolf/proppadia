#!/usr/bin/env python3
"""Rebuild the Points training and holdout frames from the database, Utah 2024-25 included (P2.57 / P2.46).

Research only, read-only (research login). The frames behind production c940ae2f were built
before the Utah restore (P2.57): the 82 Utah games of 2024-25 had no logs, and the restored
official outcomes (`refit.FROZEN_FRAME`) have no Utah rows. This rebuild runs the same path
as `research_nhl_points_hgb_month_end_refit.build_matrices` + `freeze_nhl_points_goals_assists`
(team IDs bound to the game's side by `refit.bind_canonical_team`; PP TOI as stored, corrected
by P2.45), with the Utah games' official goals and assists from their boxscores
(`utah_2024_restore_2026-11-16/boxscore/`) added to the labels.

Checks: rows added; feature parity with the P2.53 corrected frames on the rows both have
(differences expected only where a window or prior-season total includes a Utah game);
labels consistent (goals + assists = points). Writes training 2023-24 + 2024-25, the 2025-26
season, and an audit under artifacts/analysis/nhl/points_training_rebuild_2026-11-16/.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_component_features as pcf  # noqa: E402
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as mer  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import research_nhl_points_pp_corrected_refit as ppfix  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_training_rebuild_2026-11-16"
UTAH = ROOT / "artifacts/analysis/nhl/utah_2024_restore_2026-11-16"
KEYS = ["game_id", "player_id"]


def utah_labels() -> pd.DataFrame:
    games = pd.read_csv(UTAH / "games.csv")
    rows = []
    for g in games.itertuples():
        box = json.loads((UTAH / "boxscore" / f"game={g.game_id}.boxscore.json").read_text())
        for side, is_home in (("homeTeam", 1), ("awayTeam", 0)):
            st = box["playerByGameStats"][side]
            for p in (st.get("forwards") or []) + (st.get("defense") or []):
                goals, assists = int(p.get("goals") or 0), int(p.get("assists") or 0)
                rows.append({"player_id": int(p["playerId"]), "game_id": int(g.game_id), "game_date": g.game_date,
                             "canonical_season": 2024, "goals": goals, "assists": assists,
                             "realized_points": goals + assists, "is_home": is_home})
    return pd.DataFrame(rows)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    audit: dict = {}
    logs, audit["database"] = refit.pull_history()
    logs, audit["pp_zero_rule"] = refit.apply_pp_zero_rule(logs)
    frozen = pd.read_csv(refit.FROZEN_FRAME, low_memory=False)
    frozen["game_id"] = frozen.game_id.astype("int64"); frozen["player_id"] = frozen.player_id.astype("int64")
    utah = utah_labels()
    if utah[KEYS].merge(frozen[KEYS]).shape[0]:
        raise SystemExit("UTAH_ROWS_ALREADY_IN_LABELS")
    frozen = pd.concat([frozen, utah], ignore_index=True)
    audit["utah_label_rows"] = int(len(utah))
    outcomes, audit["labels"] = refit.official_outcomes(frozen)
    history, audit["history"] = refit.history_inputs(logs, outcomes)
    games = logs[["game_id", "game_date", "start_time_utc", "home_team_id", "away_team_id"]].drop_duplicates("game_id")
    frozen = frozen.loc[frozen.game_id.isin(games.game_id)].reset_index(drop=True)
    rebuilt = refit.build_targets(history, games, frozen[KEYS + ["canonical_season", "is_home"]])
    rebuilt = rebuilt.merge(frozen[KEYS + ["canonical_season", "realized_points"]], on=KEYS, validate="one_to_one")
    targets = rebuilt[KEYS + ["game_date", "canonical_season"]]
    rebuilt = rebuilt.merge(mer.extra_features(history, targets), on=KEYS, validate="one_to_one")
    pos = mer.positions()
    rebuilt = rebuilt.drop(columns=["is_defense"], errors="ignore").merge(pos, on="player_id", how="left")
    rebuilt["is_defense"] = rebuilt.is_defense.fillna(0).astype(int)
    # Component rates and prior-season totals as freeze_nhl_points_goals_assists.official(), Utah included.
    o = outcomes.rename(columns={"canonical_season": "season"})
    o["game_id"] = o.game_id.astype("int64"); o["player_id"] = o.player_id.astype("int64")
    api = pd.read_csv(own.SEASON_2022)
    prior = pd.concat([pcf.season_totals(o[o.season.isin([2023, 2024, 2025])]),
                       pd.DataFrame({"player_id": api.player_id, "season": 2022, "games": api.games, "points": api.points,
                                     "goals": api.goals, "assists": api.assists})], ignore_index=True)[pcf.TOTAL_COLUMNS]
    league = pcf.league_rates(prior)
    hist = rebuilt.rename(columns={"canonical_season": "season"})
    hist = pcf.add_features(hist, o, prior, league)
    hist = hist.merge(o[KEYS + ["goals", "assists"]], on=KEYS, how="left", validate="one_to_one")
    if hist.goals.isna().any() or (hist.goals + hist.assists != hist.realized_points).any():
        raise SystemExit("POINTS_COMPONENT_LABELS_INCONSISTENT")
    cols = ["slate_date", *KEYS, "season", *freeze.FEATURES, "goals", "assists", "realized_points"]
    train = hist[hist.season.isin([2023, 2024])][cols].reset_index(drop=True)
    s2025 = hist[hist.season.eq(2025)][cols].reset_index(drop=True)
    train.to_csv(OUT / "training_frame_2023_2024.csv.gz", index=False, float_format="%.17g")
    s2025.to_csv(OUT / "season_frame_2025.csv.gz", index=False, float_format="%.17g")
    prior.sort_values(["season", "player_id"]).to_csv(OUT / "prior_season_totals.csv", index=False)
    league.rename_axis("season").reset_index().to_csv(OUT / "league_rates.csv", index=False, float_format="%.17g")
    # Parity with the P2.53 corrected frames (same recipe; PP from the corrected database).
    old_train = pd.read_csv(ppfix.OUT / "training_frame_2023_2024_pp_corrected.csv.gz", float_precision="round_trip")
    old_hold = pd.read_csv(freeze.OUT / "holdout_frame_2025.csv.gz", float_precision="round_trip")
    utah_ids = set(utah.game_id)
    utah_players = set(utah.player_id)
    parity = {}
    for name, new, old in (("train", train, old_train), ("2025", s2025, old_hold)):
        m = new.merge(old, on=KEYS, suffixes=("", "_old"), how="outer", indicator=True)
        both = m[m._merge == "both"]
        diff = {}
        for f in freeze.FEATURES + ["realized_points"]:
            if f == "mean_pp_toi_last10" and name == "2025":
                continue  # the frozen 2025 frame holds pre-correction PP; the database holds corrected values
            ne = ~np.isclose(pd.to_numeric(both[f], errors="coerce").astype(float),
                             pd.to_numeric(both[f + "_old"], errors="coerce").astype(float), equal_nan=True, atol=1e-9)
            touched = both.player_id.isin(utah_players)
            diff[f] = {"differ": int(ne.sum()), "differ_non_utah_players": int((ne & ~touched).sum())}
        parity[name] = {"new_rows": int(len(new)), "old_rows": int(len(old)), "both": int(len(both)),
                        "only_new": int((m._merge == "left_only").sum()),
                        "only_new_in_utah_games": int(((m._merge == "left_only") & m.game_id.isin(utah_ids)).sum()),
                        "only_old": int((m._merge == "right_only").sum()), "features": diff}
    audit["parity_vs_p2_53_frames"] = parity
    audit["rows"] = {"train": int(len(train)), "season_2025": int(len(s2025)),
                     "train_by_season": train.season.value_counts().sort_index().to_dict()}
    (OUT / "audit.json").write_text(json.dumps(audit, indent=1, default=str) + "\n")
    print(json.dumps({k: audit[k] for k in ("utah_label_rows", "rows", "parity_vs_p2_53_frames")}, indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
