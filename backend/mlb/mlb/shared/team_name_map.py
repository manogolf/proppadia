# backend/scripts/shared/team_name_map.py

from __future__ import annotations
from typing import Optional, Dict, Any, Union
import json
from urllib.request import urlopen

# -------------------------
# Maps (mirroring your JS)
# -------------------------

teamNameMap: Dict[str, str] = {
    "ATH": "Athletics",
    "ATL": "Atlanta Braves",
    "AZ": "Arizona Diamondbacks",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CWS": "Chicago White Sox",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC": "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD": "San Diego Padres",
    "SEA": "Seattle Mariners",
    "SF": "San Francisco Giants",
    "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals",
}

# teamIdMap keys are ints in Python
teamIdMap: Dict[int, Dict[str, str]] = {
    108: {"abbr": "LAA", "fullName": "Los Angeles Angels"},
    109: {"abbr": "ARI", "fullName": "Arizona Diamondbacks"},
    110: {"abbr": "BAL", "fullName": "Baltimore Orioles"},
    111: {"abbr": "BOS", "fullName": "Boston Red Sox"},
    112: {"abbr": "CHC", "fullName": "Chicago Cubs"},
    113: {"abbr": "CIN", "fullName": "Cincinnati Reds"},
    114: {"abbr": "CLE", "fullName": "Cleveland Guardians"},
    115: {"abbr": "COL", "fullName": "Colorado Rockies"},
    116: {"abbr": "DET", "fullName": "Detroit Tigers"},
    117: {"abbr": "HOU", "fullName": "Houston Astros"},
    118: {"abbr": "KC", "fullName": "Kansas City Royals"},
    119: {"abbr": "LAD", "fullName": "Los Angeles Dodgers"},
    120: {"abbr": "WSH", "fullName": "Washington Nationals"},
    121: {"abbr": "NYM", "fullName": "New York Mets"},
    133: {"abbr": "OAK", "fullName": "Athletics"},  # OAK/LV
    134: {"abbr": "PIT", "fullName": "Pittsburgh Pirates"},
    135: {"abbr": "SD", "fullName": "San Diego Padres"},
    136: {"abbr": "SEA", "fullName": "Seattle Mariners"},
    137: {"abbr": "SF", "fullName": "San Francisco Giants"},
    138: {"abbr": "STL", "fullName": "St. Louis Cardinals"},
    139: {"abbr": "TB", "fullName": "Tampa Bay Rays"},
    140: {"abbr": "TEX", "fullName": "Texas Rangers"},
    141: {"abbr": "TOR", "fullName": "Toronto Blue Jays"},
    142: {"abbr": "MIN", "fullName": "Minnesota Twins"},
    143: {"abbr": "PHI", "fullName": "Philadelphia Phillies"},
    144: {"abbr": "ATL", "fullName": "Atlanta Braves"},
    145: {"abbr": "CWS", "fullName": "Chicago White Sox"},
    146: {"abbr": "MIA", "fullName": "Miami Marlins"},
    147: {"abbr": "NYY", "fullName": "New York Yankees"},
    158: {"abbr": "MIL", "fullName": "Milwaukee Brewers"},
}

# Build once: abbr -> id
abbrToIdMap: Dict[str, int] = {info["abbr"]: tid for tid, info in teamIdMap.items()}

specialValidTeams = ["OAK", "LV", "VIL", "ATH"]


# -------------------------
# Functions (same names as JS)
# -------------------------

def normalizeTeamAbbreviation(abbr: Optional[str]) -> Optional[str]:
    """
    JS: normalizeTeamAbbreviation
    - Uppercase
    - AZ -> ARI
    - ATH/LV/VIL -> OAK
    """
    if not abbr:
        return abbr
    upper = str(abbr).upper()
    if upper == "AZ":
        return "ARI"
    if upper in ("ATH", "LV", "VIL"):
        return "OAK"
    return upper


def getFullTeamName(abbr: Optional[str]) -> Optional[str]:
    """
    JS: getFullTeamName
    """
    normalized = normalizeTeamAbbreviation(abbr)
    if normalized in ("OAK", "LV", "VIL"):
        return "Athletics"
    return teamNameMap.get(normalized, normalized)


def getTeamInfoByID(abbrOrId: Union[int, str]) -> Optional[Dict[str, Any]]:
    """
    JS: getTeamInfoByID
    - If numeric (or numeric string): look up by ID
    - Else: treat as abbr and return matching info
    """
    if isinstance(abbrOrId, int) or (isinstance(abbrOrId, str) and abbrOrId.isdigit()):
        return teamIdMap.get(int(abbrOrId))
    normalized = normalizeTeamAbbreviation(str(abbrOrId))
    for tid, info in teamIdMap.items():
        if info["abbr"] == normalized:
            return info
    return None


def getFullTeamAbbreviationFromID(teamId: Union[int, str, None]) -> Optional[str]:
    """
    JS: getFullTeamAbbreviationFromID
    """
    if not teamId and teamId != 0:
        return None
    info = teamIdMap.get(int(teamId))
    return info["abbr"] if info else None


def getOpponentAbbreviation(teamAbbr: str, gameId: Union[int, str]) -> str:
    """
    JS: getOpponentAbbreviation
    Requests MLB StatsAPI boxscore and returns the opposing team's abbreviation.
    """
    normalized = normalizeTeamAbbreviation(teamAbbr)
    url = f"https://statsapi.mlb.com/api/v1/game/{int(gameId)}/boxscore"
    with urlopen(url) as resp:
        data = json.load(resp)

    home = data["teams"]["home"]["team"]
    away = data["teams"]["away"]["team"]

    if home["abbreviation"] == normalized:
        return away["abbreviation"]
    if away["abbreviation"] == normalized:
        return home["abbreviation"]
    raise ValueError(f"Team {normalized} not found in boxscore for game {gameId}")


def getTeamIdFromAbbr(abbr: Optional[str]) -> Optional[int]:
    """
    JS: getTeamIdFromAbbr
    """
    normalized = normalizeTeamAbbreviation(abbr)
    if not normalized:
        return None
    return abbrToIdMap.get(normalized)


def isValidMLBTeam(abbr: Optional[str]) -> bool:
    """
    JS: isValidMLBTeam
    """
    if not abbr:
        return False
    normalized = str(abbr).upper()
    return normalized in teamNameMap or normalized in ("OAK", "LV", "VIL")


def getTeamInfoByAbbr(abbr: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    JS: getTeamInfoByAbbr
    Returns { id, abbr, fullName } or None
    """
    normalized = normalizeTeamAbbreviation(abbr)
    if not normalized:
        return None
    for tid, info in teamIdMap.items():
        if info["abbr"] == normalized:
            return {"id": int(tid), "abbr": info["abbr"], "fullName": info["fullName"]}
    return None


def getTeamInfoById(teamId: Union[int, str, None]) -> Optional[Dict[str, Any]]:
    """
    JS: getTeamInfoById
    Same as getTeamInfoByID but numeric-only signature.
    """
    if teamId is None:
        return None
    tid = int(teamId)
    info = teamIdMap.get(tid)
    if not info:
        return None
    return {"id": tid, "abbr": info["abbr"], "fullName": info["fullName"]}


# -------------------------
# Backward-compatible aliases (if anything imports the old names)
# -------------------------

# Previously used snake_case names (map them to the JS-style names)
normalize_abbr = normalizeTeamAbbreviation
abbr_to_team_id = getTeamIdFromAbbr
team_id_from_abbr = getTeamIdFromAbbr
# Snake_case aliases for existing code
normalize_team_abbreviation = normalizeTeamAbbreviation
team_id_map = teamIdMap
team_name_map = teamNameMap
abbr_to_id_map = abbrToIdMap
get_team_id_from_abbr = getTeamIdFromAbbr
get_team_abbr_from_team_id = getFullTeamAbbreviationFromID
get_team_info_by_abbr = getTeamInfoByAbbr
get_team_info_by_id = getTeamInfoById
is_valid_mlb_team = isValidMLBTeam

