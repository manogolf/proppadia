"""Points candidate shadows (NHL Backlog P2.58; operator 2026-11-16: "proceed with building points shadows").

Shadow arms beside production Points (2f0824cf): the production inputs plus public information
available at the first daily run that the model did not use (P2.55):

- PP unit, from this season's stored per-player PP TOI in the capture's skater logs: in every
  team-game players are ranked by PP minutes and the top five with PP time are PP1. Inputs, from
  the player's games strictly before the target game: ``pp1_last_game``, ``pp1_share_last5``,
  ``pp_rank_last_game`` (10 when he had no PP time).
- The game's expected scoring, from the morning game lines in the daily odds captures (the
  latest observation by 06:30 PT on the slate date, else the latest one so far): per game the
  median total line across books moved by the median no-vig Over lean (0.5 goals per 10 points
  above 50%), and the median no-vig home win probability. ``game_total``, ``team_win_p`` and
  ``team_implied_goals`` = total x (0.5 + 0.6 x (win probability - 0.5)).

Arms are frozen by ``scripts/freeze_nhl_points_candidate_shadows.py`` under ``CONTRACT_ROOT/arm=<name>/``
(contract + goals/assists artifact). Each Points capture is scored once per arm into
``OUTPUT_ROOT/season=<s>/slate_date=<d>/run_id=<r>/phase=<p>/captured_at=<t>/arm=<name>/``.
Nothing reads these predictions except the grader; production is unchanged.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_ROOT = ROOT / "artifacts/analysis/nhl/points_candidate_shadows_2026-11-16"
CAPTURE_ROOT = ROOT / "artifacts/operational/nhl/points_hgb_shadow"
OUTPUT_ROOT = ROOT / "artifacts/operational/nhl/points_candidate_shadows"
ODDS_ROOT = ROOT / "artifacts/operational/nhl/odds_observations"
SCHEMA = "NHL_POINTS_CANDIDATE_SHADOW_V1"
KEYS = ["game_id", "player_id"]
PPU = ["pp1_last_game", "pp1_share_last5", "pp_rank_last_game"]
TOT = ["team_implied_goals", "game_total", "team_win_p"]
K_SHARE = 0.6
OVER_LEAN_GOALS_PER_POINT = 5.0
MORNING_CUTOFF_PT = "06:30"
# Each player's last five regular-season games of the previous season (pp1, pp_rank), frozen with the
# arms, so players with few games this season see the same history as in training.
PRIOR_SEASON_PP = CONTRACT_ROOT / "prior_season_pp_units.csv"
TEAM_IDS = {"ANA": 24, "BOS": 6, "BUF": 7, "CAR": 12, "CBJ": 29, "CGY": 20, "CHI": 16, "COL": 21, "DAL": 25,
            "DET": 17, "EDM": 22, "FLA": 13, "LAK": 26, "MIN": 30, "MTL": 8, "NJD": 1, "NSH": 18, "NYI": 2,
            "NYR": 3, "OTT": 9, "PHI": 4, "PIT": 5, "SEA": 55, "SJS": 28, "STL": 19, "TBL": 14, "TOR": 10,
            "UTA": 68, "VAN": 23, "VGK": 54, "WPG": 52, "WSH": 15, "ARI": 53}
CODES = {"Anaheim Ducks": "ANA", "Arizona Coyotes": "ARI", "Boston Bruins": "BOS", "Buffalo Sabres": "BUF",
         "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR", "Chicago Blackhawks": "CHI", "Colorado Avalanche": "COL",
         "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL", "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM",
         "Florida Panthers": "FLA", "Los Angeles Kings": "LAK", "Minnesota Wild": "MIN", "Montréal Canadiens": "MTL",
         "Montreal Canadiens": "MTL", "Nashville Predators": "NSH", "New Jersey Devils": "NJD",
         "New York Islanders": "NYI", "New York Rangers": "NYR", "Ottawa Senators": "OTT", "Philadelphia Flyers": "PHI",
         "Pittsburgh Penguins": "PIT", "San Jose Sharks": "SJS", "Seattle Kraken": "SEA", "St Louis Blues": "STL",
         "St. Louis Blues": "STL", "Tampa Bay Lightning": "TBL", "Toronto Maple Leafs": "TOR", "Utah Hockey Club": "UTA",
         "Utah Mammoth": "UTA", "Vancouver Canucks": "VAN", "Vegas Golden Knights": "VGK", "Washington Capitals": "WSH",
         "Winnipeg Jets": "WPG"}


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def season_of(day: str) -> int:
    return int(day[:4]) if int(day[5:7]) >= 9 else int(day[:4]) - 1


def _dec(american) -> np.ndarray:
    a = np.asarray(american, float)
    return np.where(a > 0, 1 + a / 100, 1 + 100 / -a)


# ----------------------------------------------------------------------------- PP unit

def pp_unit_table(logs: pd.DataFrame) -> pd.DataFrame:
    """Per player-game: PP1 flag and PP rank within the team-game (top five with PP time = PP1)."""
    lg = logs[KEYS + ["game_date", "team_id", "pp_toi_minutes"]].copy()
    lg["game_date"] = pd.to_datetime(lg.game_date)
    lg["pp"] = pd.to_numeric(lg.pp_toi_minutes, errors="coerce").fillna(0)
    # Deterministic ties: more PP minutes first, then lower player_id (row order differs by source).
    lg = lg.sort_values(["game_id", "team_id", "pp", "player_id"], ascending=[True, True, False, True])
    lg["pp_rank"] = lg.groupby(["game_id", "team_id"]).cumcount() + 1.0
    pp = lg.pp
    lg["pp1"] = ((lg.pp_rank <= 5) & (pp > 0)).astype(float)
    lg.loc[pp.eq(0), "pp_rank"] = 10.0
    return lg


def pp_unit_inputs(targets: pd.DataFrame, logs: pd.DataFrame, prior: pd.DataFrame | None = None) -> pd.DataFrame:
    """Strict-prior PP unit inputs for each target (game_id, player_id, game_date); ``prior`` adds the
    previous season's last games (player_id, game_id, game_date, pp1, pp_rank)."""
    pg = pp_unit_table(logs)[KEYS + ["game_date", "pp1", "pp_rank"]]
    if prior is not None and len(prior):
        pr = prior[KEYS + ["game_date", "pp1", "pp_rank"]].assign(game_date=lambda x: pd.to_datetime(x.game_date))
        pg = pd.concat([pr[~pr.set_index(KEYS).index.isin(pg.set_index(KEYS).index)], pg], ignore_index=True)
    pg = pg.sort_values(["player_id", "game_date", "game_id"])
    out = []
    groups = {int(p): g for p, g in pg.groupby("player_id", sort=False)}
    for t in targets[KEYS + ["game_date"]].itertuples(index=False):
        g = groups.get(int(t.player_id))
        prior = g[g.game_date < pd.Timestamp(t.game_date)] if g is not None else None
        if prior is None or prior.empty:
            out.append((t.game_id, t.player_id, np.nan, np.nan, np.nan))
            continue
        out.append((t.game_id, t.player_id, float(prior.pp1.iloc[-1]), float(prior.pp1.tail(5).mean()),
                    float(prior.pp_rank.iloc[-1])))
    return pd.DataFrame(out, columns=KEYS + PPU)


# -------------------------------------------------------------------------- game lines

def summarize_lines(rows: pd.DataFrame) -> pd.DataFrame:
    """rows: home, away, book, market, outcome, point, price -> one row per game (median across books)."""
    out = []
    for (home, away), g in rows.groupby(["home", "away"]):
        h2h = g[g.market.eq("h2h")].pivot_table(index="book", columns="outcome", values="price", aggfunc="last")
        p_home = np.nan
        if home in h2h and away in h2h:
            h = h2h[[home, away]].dropna()
            ph, pa = 1 / _dec(h[home]), 1 / _dec(h[away])
            p_home = float(np.median(ph / (ph + pa))) if len(h) else np.nan
        tot = g[g.market.eq("totals")]
        line, p_over = np.nan, np.nan
        if len(tot):
            line = float(tot.point.median())
            t = tot[tot.point.eq(line)].pivot_table(index="book", columns="outcome", values="price", aggfunc="last")
            if {"Over", "Under"} <= set(t.columns):
                t = t[["Over", "Under"]].dropna()
                po, pu = 1 / _dec(t.Over), 1 / _dec(t.Under)
                p_over = float(np.median(po / (po + pu))) if len(t) else np.nan
        out.append({"home": home, "away": away, "p_home": p_home, "total_line": line, "p_over": p_over})
    return pd.DataFrame(out, columns=["home", "away", "p_home", "total_line", "p_over"])


def morning_observation(slate_date: str, now: pd.Timestamp | None = None, odds_root: Path = ODDS_ROOT) -> Path | None:
    """Full-slate odds observation for the game lines: the latest non-CLOSING observation by 06:30 PT
    on the slate date, else the day's first non-CLOSING one (CLOSING observations are single games).
    ``now`` (scoring time) excludes observations taken later."""
    d = Path(odds_root) / f"season={season_of(slate_date)}" / f"slate_date={slate_date}"
    obs = []
    for p in d.glob("observation=*/normalized_odds.jsonl"):
        summary = p.parent / "observation_summary.json"
        phase = json.loads(summary.read_text()).get("phase") if summary.is_file() else None
        if phase == "CLOSING":
            continue
        t = pd.Timestamp(re.search(r"observation=(\d{8}T\d{6})", str(p)).group(1), tz="UTC")
        if now is not None and t > now:
            continue
        obs.append((t, p))
    if not obs:
        return None
    obs.sort()
    cutoff = pd.Timestamp(f"{slate_date} {MORNING_CUTOFF_PT}", tz="America/Los_Angeles").tz_convert("UTC")
    morning = [o for o in obs if o[0] <= cutoff]
    return morning[-1][1] if morning else obs[0][1]


def game_line_inputs(slate: pd.DataFrame, observation: Path | None) -> pd.DataFrame:
    """Per (game_id, team_id): team_implied_goals, game_total, team_win_p from one odds observation."""
    cols = ["game_id", "team_id", *TOT]
    if observation is None:
        return pd.DataFrame(columns=cols)
    t = pd.read_json(observation, lines=True)
    if "market" not in t:
        return pd.DataFrame(columns=cols)
    t = t[t.market.isin(["h2h", "totals"])].rename(columns={"home_team": "home", "away_team": "away", "bookmaker": "book"})
    if t.empty:
        return pd.DataFrame(columns=cols)
    g = summarize_lines(t)
    g["home_id"] = g.home.map(CODES).map(TEAM_IDS)
    g["away_id"] = g.away.map(CODES).map(TEAM_IDS)
    games = slate[["game_id", "home_team_id", "away_team_id"]].drop_duplicates()
    m = games.merge(g, left_on=["home_team_id", "away_team_id"], right_on=["home_id", "away_id"], how="inner")
    m["game_total"] = m.total_line + (m.p_over - 0.5).fillna(0) * OVER_LEAN_GOALS_PER_POINT
    rows = []
    for team, pwin in ((m.home_team_id, m.p_home), (m.away_team_id, 1 - m.p_home)):
        rows.append(pd.DataFrame({"game_id": m.game_id, "team_id": team, "game_total": m.game_total, "team_win_p": pwin,
                                  "team_implied_goals": m.game_total * (0.5 + K_SHARE * (pwin - 0.5))}))
    return pd.concat(rows, ignore_index=True)[cols]


# ------------------------------------------------------------------------------ scoring

def arms(contract_root: Path = CONTRACT_ROOT) -> list[Path]:
    return sorted(p for p in Path(contract_root).glob("arm=*") if (p / "contract.json").is_file())


def load_arm(arm_dir: Path) -> tuple[dict[str, Any], Any]:
    contract = json.loads((arm_dir / "contract.json").read_text())
    artifact = arm_dir / contract["fitted_artifact"]
    if sha256(artifact) != contract["fitted_artifact_sha256"]:
        raise RuntimeError(f"POINTS_CANDIDATE_ARTIFACT_HASH_DRIFT:{arm_dir.name}")
    return contract, joblib.load(artifact)


def capture_inputs(capture_dir: Path, now: pd.Timestamp | None = None) -> tuple[pd.DataFrame, dict]:
    features = pd.read_csv(capture_dir / "features.csv")
    slate = pd.read_csv(capture_dir / "source_input_00_canonical_slate.csv")
    logs = pd.read_csv(capture_dir / "source_input_02_official_skater_logs.csv")
    day = str(features.slate_date.iloc[0])
    frame = features.merge(slate[KEYS + ["team_id", "home_team_id", "away_team_id"]], on=KEYS, how="left",
                           validate="one_to_one")
    prior = pd.read_csv(PRIOR_SEASON_PP) if PRIOR_SEASON_PP.is_file() else None
    frame = frame.merge(pp_unit_inputs(frame, logs, prior), on=KEYS, how="left", validate="one_to_one")
    obs = morning_observation(day, now)
    lines = game_line_inputs(slate, obs)
    frame = frame.merge(lines, on=["game_id", "team_id"], how="left", validate="many_to_one")
    meta = {"odds_observation": str(obs.relative_to(ROOT)) if obs else None,
            "games_with_lines": int(lines.game_id.nunique()), "games": int(slate.game_id.nunique()),
            "coverage": {c: float(frame[c].notna().mean()) for c in PPU + TOT}}
    return frame, meta


def output_dir(capture_dir: Path, arm: str, output_root: Path = OUTPUT_ROOT) -> Path:
    return Path(output_root) / Path(capture_dir).relative_to(CAPTURE_ROOT) / f"arm={arm}"


def score_capture(capture_dir: Path, *, output_root: Path = OUTPUT_ROOT, contract_root: Path = CONTRACT_ROOT) -> dict:
    """Score one Points capture with every frozen arm; existing outputs are left as they are."""
    from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
    capture_dir = Path(capture_dir)
    todo = [a for a in arms(contract_root) if not (output_dir(capture_dir, a.name.split("=", 1)[1], output_root)
                                                    / "summary.json").is_file()]
    if not todo:
        return {"status": "EXISTS", "path": str(capture_dir)}
    stamp = pd.Timestamp(re.search(r"captured_at=(\d{8}T\d{6})", str(capture_dir)).group(1), tz="UTC")
    frame, meta = capture_inputs(capture_dir, now=stamp + pd.Timedelta(minutes=10))
    scored = []
    for arm_dir in todo:
        name = arm_dir.name.split("=", 1)[1]
        contract, model = load_arm(arm_dir)
        missing = sorted(set(contract["feature_columns"]) - set(frame.columns))
        if missing:
            return {"status": "SKIPPED_INPUTS_MISSING", "missing": missing, "path": str(capture_dir)}
        out = output_dir(capture_dir, name, output_root)
        preds = frame[KEYS + ["game_date"] + PPU + TOT].merge(scorer.score(frame, model), on=KEYS, how="left",
                                                              validate="one_to_one")
        out.mkdir(parents=True, exist_ok=False)
        preds.to_csv(out / "predictions.csv", index=False, float_format="%.10g")
        summary = {"schema_version": SCHEMA, "arm": name, "model_identity_sha256": contract["model_identity_sha256"],
                   "slate_date": str(frame.slate_date.iloc[0]), "capture_dir": str(capture_dir.relative_to(ROOT)),
                   "rows": int(len(preds)), **meta, "contract_sha256": sha256(arm_dir / "contract.json"),
                   "scored_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                   "production_unchanged": True, "database_writes": 0}
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        scored.append(name)
    return {"status": "SCORED", "arms": scored, "rows": int(len(frame)), "path": str(capture_dir)}


def captures_for(slate_date: str, capture_root: Path = CAPTURE_ROOT) -> list[Path]:
    root = Path(capture_root) / f"season={season_of(slate_date)}" / f"slate_date={slate_date}"
    return sorted(p for p in root.glob("run_id=*/phase=*/captured_at=*") if (p / "features.csv").is_file())
