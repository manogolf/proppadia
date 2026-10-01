"""Thin, catalog-backed NHL adapter for the positional 8rain Station CSV."""

from __future__ import annotations

import json
import math
import re
import unicodedata
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

import pandas as pd

UPLOAD_COLUMNS = [
    "LEAGUE", "DATE", "HOME", "AWAY", "DOUBLEHEADER", "SECTION",
    "MARKET", "SELECTOR", "POINT", "SIDE", "WIN %",
]


def resolve_catalog_dir(path: Path) -> Path:
    path = Path(path)
    if (path / "model_spec.json").is_file():
        return path
    versions = sorted(path.parent.glob("retrieval=*") if path.name == "current" else path.glob("retrieval=*"))
    if versions and (versions[-1] / "model_spec.json").is_file():
        return versions[-1]
    raise FileNotFoundError(f"8rain catalog bundle not found: {path}")


def fair_american(p: float) -> int:
    """Return rounded fair American odds, using the established NHL rounding."""
    p = float(p)
    if not 0 < p < 1:
        raise ValueError("PROBABILITY_OUT_OF_RANGE")
    if p >= 0.5:
        return int(-round(100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))


def fair_american_d(p: float) -> str:
    """Candidate test representation; server acceptance is not implied."""
    value = fair_american(p)
    return f"{value:+d}d" if value > 0 else f"{value}d"


def format_win_probability(p: float, representation: str = "american") -> str:
    p = float(p)
    if not 0 < p < 1:
        raise ValueError("PROBABILITY_OUT_OF_RANGE")
    if representation == "decimal":
        return format(p, ".12g")
    if representation == "american_d":
        return fair_american_d(p)
    if representation == "american":
        value = fair_american(p)
        return f"{value:+d}" if value > 0 else str(value)
    raise ValueError("UNKNOWN_WIN_PERCENT_REPRESENTATION")


def american_probability(value: Any) -> float:
    odds = int(str(value).strip())
    if odds <= -100:
        return abs(odds) / (abs(odds) + 100.0)
    if odds >= 100:
        return 100.0 / (odds + 100.0)
    raise ValueError("WIN_PERCENT_FORMAT_INVALID")


def normalize_player_name(value: Any) -> str:
    raw = unicodedata.normalize("NFKD", str(value or ""))
    return re.sub(r"[^a-z0-9]+", " ", raw.encode("ascii", "ignore").decode().lower()).strip()


def load_catalogs(catalog_dir: Path) -> tuple[dict, dict[str, str], dict[tuple[str, str], str], dict[str, set[str]]]:
    catalog_dir = resolve_catalog_dir(catalog_dir)
    spec = json.loads((catalog_dir / "model_spec.json").read_text())
    teams_json = json.loads((catalog_dir / "teams.json").read_text())
    players_json = json.loads((catalog_dir / "players.json").read_text())
    team_rows = teams_json.get("data", [])
    by_abbr: dict[str, list[str]] = {}
    for row in team_rows:
        abbr, code = str(row.get("abbreviation") or "").upper(), str(row.get("code") or "")
        if abbr and code:
            by_abbr.setdefault(abbr, []).append(code)
    team_map = {abbr: codes[0] for abbr, codes in by_abbr.items() if len(set(codes)) == 1}

    player_rows = players_json.get("data", [])
    # Name + catalog team code is the available canonical binding. Retain only
    # one-to-one rows; blank or duplicate codes must be reported, never guessed.
    player_options: dict[tuple[str, str], set[str]] = {}
    allowed_bets: dict[str, set[str]] = {}
    for row in spec.get("stats", []):
        bets = row.get("bet", [])
        if isinstance(bets, list):
            allowed_bets[str(row.get("code", ""))] = {str(x).lower() for x in bets}
    for row in player_rows:
        key = (normalize_player_name(row.get("name")), str(row.get("team") or ""))
        if key[0] and key[1]:
            player_options.setdefault(key, set()).add(str(row.get("code") or ""))
    player_map = {
        key: next(iter(codes)) for key, codes in player_options.items()
        if len(codes) == 1 and "" not in codes
    }
    return spec, team_map, player_map, allowed_bets


def ambiguous_player_bindings(catalog_dir: Path) -> set[tuple[str, str]]:
    catalog_dir = resolve_catalog_dir(catalog_dir)
    players_json = json.loads((catalog_dir / "players.json").read_text())
    options: dict[tuple[str, str], set[str]] = {}
    for row in players_json.get("data", []):
        name = normalize_player_name(row.get("name"))
        team = str(row.get("team") or "")
        if name and team and row.get("code"):
            options.setdefault((name, team), set()).add(str(row["code"]))
    return {key for key, codes in options.items() if len(codes) > 1}


def _date(value: Any) -> str:
    return pd.to_datetime(value, errors="raise").strftime("%Y-%m-%d")


def _base_row(date: str, home: str, away: str, section: str, market: str,
              selector: str, point: str, side: str, probability: float) -> dict[str, Any]:
    return {
        "LEAGUE": "nhl", "DATE": date, "HOME": home, "AWAY": away,
        "DOUBLEHEADER": "0", "SECTION": section, "MARKET": market,
        "SELECTOR": selector, "POINT": point, "SIDE": side,
        # The last-season exporter used plain integer American fair odds.
        # Keep that representation until decimal is verified by 8rain itself.
        "WIN %": format_win_probability(probability, "american"),
    }


def build_rows(
    *, package_dir: Path, spec: dict, team_map: dict[str, str],
    player_map: dict[tuple[str, str], str], allowed_bets: dict[str, set[str]],
    prop_candidates: pd.DataFrame | None = None,
    ambiguous_player_keys: set[tuple[str, str]] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build only reference rows plus explicitly selected prop candidates.

    Shadow challengers are intentionally excluded. Prop input is an already
    policy-selected population with game/player identity and a selected side.
    The opposing side is added from the complementary model probability.
    """
    ml = pd.read_csv(package_dir / "v2_immutable_predictions.csv")
    pl = pd.read_csv(package_dir / "puck_line_v1_immutable_predictions.csv")
    games = pd.read_csv(package_dir / "schedule_event_identity.csv")
    required = {"game_id", "game_date", "home_team", "away_team"}
    if required - set(games.columns):
        raise ValueError("CANONICAL_GAME_SCHEMA_MISSING")
    if games.empty or games.game_id.duplicated().any():
        raise ValueError("CANONICAL_GAME_GRAIN_INVALID")
    if ml.game_id.duplicated().any() or pl.game_id.duplicated().any():
        raise ValueError("DUPLICATE_REFERENCE_PREDICTION")
    if set(ml.game_id.astype(str)) != set(games.game_id.astype(str)) or set(pl.game_id.astype(str)) != set(games.game_id.astype(str)):
        raise ValueError("REFERENCE_PREDICTION_SLATE_MISMATCH")

    league = str(spec.get("league", {}).get("code", ""))
    if league != "nhl":
        raise ValueError("CATALOG_LEAGUE_MISMATCH")
    market_definitions = spec.get("markets", {})
    if "h2h" not in market_definitions or "spread" not in market_definitions:
        raise ValueError("REQUIRED_MARKET_NOT_IN_CATALOG")

    rows: list[dict[str, Any]] = []
    provenance: list[dict[str, Any]] = []
    team_unmapped: set[str] = set()
    game_identity: dict[str, tuple[str, str, str]] = {}
    for g in games.to_dict("records"):
        home_abbr, away_abbr = str(g["home_team"]).upper(), str(g["away_team"]).upper()
        if home_abbr not in team_map or away_abbr not in team_map:
            team_unmapped.update(x for x in (home_abbr, away_abbr) if x not in team_map)
            continue
        date = _date(g["game_date"])
        game_id = str(g["game_id"])
        game_identity[game_id] = (date, team_map[home_abbr], team_map[away_abbr])
        m = ml[ml.game_id.astype(str).eq(game_id)].iloc[0]
        hp, ap = float(m.v2_home_win_probability), float(m.v2_away_win_probability)
        if not math.isclose(hp + ap, 1.0, abs_tol=1e-6):
            raise ValueError(f"MONEYLINE_PAIR_NOT_NORMALIZED:{game_id}")
        for side, probability in (("home", hp), ("away", ap)):
            rows.append(_base_row(date, team_map[home_abbr], team_map[away_abbr],
                                  "head_to_head", "h2h", "", "", side, probability))
        provenance.extend([
            {"game_id": game_id, "market": "h2h", "model_identity": str(m.get("model_version", "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2")), "side": s, "probability": p}
            for s, p in (("home", hp), ("away", ap))
        ])

        p = pl[pl.game_id.astype(str).eq(game_id)].iloc[0]
        away_by2 = float(p.away_by_2_plus_probability)
        home_by2 = float(p.home_by_2_plus_probability)
        p_sum = away_by2 + float(p.one_goal_game_probability) + home_by2
        if not math.isclose(p_sum, 1.0, abs_tol=1e-6):
            raise ValueError(f"PUCK_LINE_CLASS_NOT_NORMALIZED:{game_id}")
        home_cover, away_cover = home_by2, 1.0 - home_by2
        if not math.isclose(home_cover + away_cover, 1.0, abs_tol=1e-6):
            raise ValueError(f"PUCK_LINE_PAIR_NOT_NORMALIZED:{game_id}")
        for side, point, probability in (("home", "-1.5", home_cover), ("away", "+1.5", away_cover)):
            rows.append(_base_row(date, team_map[home_abbr], team_map[away_abbr],
                                  "spread", "spread", "", point, side, probability))
        provenance.extend([
            {"game_id": game_id, "market": "spread", "model_identity": "NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1", "side": "home", "probability": home_cover},
            {"game_id": game_id, "market": "spread", "model_identity": "NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1", "side": "away", "probability": away_cover},
        ])

    unmapped_players: list[dict[str, Any]] = []
    ambiguous_players: list[dict[str, Any]] = []
    mapped_player_keys: set[tuple[str, str]] = set()
    needed_player_keys: set[tuple[str, str]] = set()
    ambiguous_player_keys = ambiguous_player_keys or set()
    if prop_candidates is not None and not prop_candidates.empty:
        required_prop = {"game_id", "game_date", "player_name", "team", "market", "line", "model_pick", "model_side_prob"}
        if required_prop - set(prop_candidates.columns):
            raise ValueError("PROP_CANDIDATE_SCHEMA_MISSING")
        for r in prop_candidates.to_dict("records"):
            market = str(r["market"])
            if market not in allowed_bets or not {"over", "under"}.issubset(allowed_bets[market]):
                raise ValueError(f"PROP_MARKET_OR_BET_UNSUPPORTED:{market}")
            team_abbr = str(r["team"]).upper()
            team_code = team_map.get(team_abbr)
            if not team_code:
                team_unmapped.add(team_abbr)
                continue
            player_key = (normalize_player_name(r["player_name"]), team_code)
            needed_player_keys.add(player_key)
            selector = player_map.get(player_key)
            if not selector:
                item = {"player_name": str(r["player_name"]), "team": team_abbr, "game_id": str(r["game_id"]), "market": market}
                if player_key in ambiguous_player_keys:
                    ambiguous_players.append(item)
                else:
                    unmapped_players.append(item)
                continue
            mapped_player_keys.add(player_key)
            selected_side = str(r["model_pick"]).lower()
            p = float(r["model_side_prob"])
            if selected_side not in {"over", "under"} or not 0 < p < 1:
                raise ValueError("PROP_SIDE_OR_PROBABILITY_INVALID")
            game_id = str(r["game_id"])
            if game_id not in game_identity:
                raise ValueError(f"PROP_GAME_NOT_IN_CANONICAL_SLATE:{game_id}")
            date, home_code, away_code = game_identity[game_id]
            pair = {selected_side: p, ("under" if selected_side == "over" else "over"): 1.0 - p}
            for side in ("over", "under"):
                rows.append(_base_row(date, home_code, away_code,
                                      "player_prop", market, selector, str(float(r["line"])), side, pair[side]))
            provenance.extend([
                {"game_id": str(r["game_id"]), "market": market, "model_identity": str(r.get("model_identity", "POLICY_SELECTED_CANDIDATE")), "run_id": str(r.get("run_id", "")), "side": s, "probability": pair[s]}
                for s in ("over", "under")
            ])

    out = pd.DataFrame(rows, columns=UPLOAD_COLUMNS)
    if out.duplicated(UPLOAD_COLUMNS[:4] + ["SECTION", "MARKET", "SELECTOR", "POINT", "SIDE"]).any():
        raise ValueError("DUPLICATE_UPLOAD_KEY")
    diagnostics = {
        "mapped_teams": sorted(team_map), "unmapped_teams": sorted(team_unmapped),
        "mapped_prop_rows": max(0, len(provenance) - 2 * len(games) * 2),
        "players_needed": len(needed_player_keys), "players_mapped": len(mapped_player_keys),
        "unmapped_players": unmapped_players, "ambiguous_players": ambiguous_players,
        "provenance": provenance,
        "reference_games": len(games), "challengers_included": False,
    }
    return out, diagnostics


def validate_upload(df: pd.DataFrame, *, spec: dict, team_codes: set[str],
                    player_codes: set[str]) -> dict[str, int]:
    if list(df.columns) != UPLOAD_COLUMNS:
        raise ValueError("UPLOAD_COLUMNS_INVALID")
    if df.empty:
        raise ValueError("UPLOAD_EMPTY")
    if not df.LEAGUE.eq("nhl").all() or not df.DOUBLEHEADER.astype(str).eq("0").all():
        raise ValueError("UPLOAD_LEAGUE_OR_DOUBLEHEADER_INVALID")
    dates = pd.to_datetime(df.DATE, format="%Y-%m-%d", errors="coerce")
    if dates.isna().any():
        raise ValueError("UPLOAD_DATE_INVALID")
    if (df.HOME.eq("") | df.AWAY.eq("") | ~df.HOME.isin(team_codes) | ~df.AWAY.isin(team_codes)).any():
        raise ValueError("UPLOAD_TEAM_INVALID")
    mkts = spec.get("markets", {})
    stat_bets = {str(s.get("code")): set(s.get("bet", [])) for s in spec.get("stats", []) if isinstance(s.get("bet"), list)}
    for row in df.to_dict("records"):
        sec, market, side = row["SECTION"], row["MARKET"], str(row["SIDE"]).lower()
        if sec == "head_to_head":
            if market != "h2h" or side not in {"home", "away"} or row["SELECTOR"] != "" or row["POINT"] != "": raise ValueError("MONEYLINE_ROW_INVALID")
        elif sec == "spread":
            if market != "spread" or side not in {"home", "away"} or row["SELECTOR"] != "": raise ValueError("SPREAD_ROW_INVALID")
            point = float(row["POINT"])
            if not math.isfinite(point) or (side == "home" and point >= 0) or (side == "away" and point <= 0): raise ValueError("SPREAD_POINT_SIGN_INVALID")
        elif sec == "player_prop":
            try:
                valid_point = math.isfinite(float(row["POINT"]))
            except (TypeError, ValueError):
                valid_point = False
            if market not in stat_bets or side not in stat_bets[market] or str(row["SELECTOR"]) not in player_codes or not valid_point: raise ValueError("PROP_ROW_INVALID")
        else:
            raise ValueError("UPLOAD_SECTION_INVALID")
        value = str(row["WIN %"])
        if not re.fullmatch(r"[+-]?\d+", value) or abs(int(value)) < 100:
            raise ValueError("WIN_PERCENT_FORMAT_INVALID")
    keycols = UPLOAD_COLUMNS[:4] + ["SECTION", "MARKET", "SELECTOR", "POINT", "SIDE"]
    if df.duplicated(keycols).any():
        raise ValueError("DUPLICATE_UPLOAD_KEY")
    pair_frame = df.copy()
    pair_frame["_PAIR_POINT"] = pair_frame.apply(
        lambda r: abs(float(r.POINT)) if r.SECTION == "spread" else str(r.POINT), axis=1)
    groups = pair_frame.groupby(UPLOAD_COLUMNS[:4] + ["SECTION", "MARKET", "SELECTOR", "_PAIR_POINT"], dropna=False)
    pair_failures = 0
    probability_pair_failures = 0
    for (_, _, _, _, section, market, _, _), group in groups:
        sides = set(group.SIDE.astype(str).str.lower())
        expected = {"home", "away"} if section in {"head_to_head", "spread"} else {"over", "under"}
        if sides != expected:
            pair_failures += 1
            continue
        pair_probability = sum(american_probability(v) for v in group["WIN %"])
        if not math.isclose(pair_probability, 1.0, abs_tol=0.02):
            probability_pair_failures += 1
    if pair_failures:
        raise ValueError(f"UNPAIRED_MARKET_ROWS:{pair_failures}")
    if probability_pair_failures:
        raise ValueError(f"PROBABILITY_PAIR_NOT_NORMALIZED:{probability_pair_failures}")
    return {"rows": len(df), "market_pairs": len(groups), "duplicate_rows": 0,
            "pair_failures": 0, "probability_pair_failures": 0}
