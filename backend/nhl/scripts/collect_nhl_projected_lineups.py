"""Trial collection of NHL.com projected lineups (NHL Backlog P2.47).

NHL.com writers publish each day's projected forward lines, defense pairs,
goalies (probable starter first), scratches and injuries as one news article.
There is no API: the lineups are article text. This reads the article, saves
the page, parses each team's lineup by its fixed layout and matches names to
our player IDs through the slate's names file (team-scoped). Read-only: no
database access, no model input; only the input watch reads it (goalies and
regulars out, watch-only). Runs as a non-blocking step of each scheduled daily run until
``TRIAL_END``:

    ./bin/python backend/nhl/scripts/collect_nhl_projected_lineups.py [--slate-date YYYY-MM-DD]

Output: ``artifacts/operational/nhl/projected_lineups/season=<s>/slate_date=<d>/
observation=<utc>/`` with ``page.html``, ``lineups.csv`` and
``observation_summary.json``.
"""
from __future__ import annotations

import argparse
import json
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[3]
URL = "https://www.nhl.com/news/nhl-lineup-projections-2026-27-season"
OUT = ROOT / "artifacts/operational/nhl/projected_lineups"
NAMES = ROOT / "backend/nhl/exports/daily/names"
SCHEMA = "NHL_PROJECTED_LINEUPS_TRIAL_V1"
TRIAL_END = "2026-11-30"  # extended from 2026-11-13 (operator, 2026-11-13)
ATTEMPTS = 3
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; proppadia-lineup-trial)"}
# NHL.com headers use upper-case nicknames ("BLUE JACKETS (7-7-0) at SABRES (7-5-3)").
NICKNAMES = {
    "DUCKS": "ANA", "BRUINS": "BOS", "SABRES": "BUF", "FLAMES": "CGY", "HURRICANES": "CAR",
    "BLACKHAWKS": "CHI", "AVALANCHE": "COL", "BLUE JACKETS": "CBJ", "STARS": "DAL", "RED WINGS": "DET",
    "OILERS": "EDM", "PANTHERS": "FLA", "KINGS": "LAK", "WILD": "MIN", "CANADIENS": "MTL",
    "PREDATORS": "NSH", "DEVILS": "NJD", "ISLANDERS": "NYI", "RANGERS": "NYR", "SENATORS": "OTT",
    "FLYERS": "PHI", "PENGUINS": "PIT", "SHARKS": "SJS", "KRAKEN": "SEA", "BLUES": "STL",
    "LIGHTNING": "TBL", "MAPLE LEAFS": "TOR", "MAMMOTH": "UTA", "CANUCKS": "VAN",
    "GOLDEN KNIGHTS": "VGK", "CAPITALS": "WSH", "JETS": "WPG",
}
LABELS = ("scratched:", "injured:", "suspended:")
WEEKDAYS = {"MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"}
# Neutral-site games read "vs." ("HURRICANES (14-3-2) vs. KRAKEN (9-6-1)") and the joiner's case
# varies ("JETS (8-8-3) AT STARS (10-5-2)"), both 2026-11-12.
GAME = re.compile(r"^(?P<away>[A-Z .'\-]+?) \((?:[\d-]+)\) (?i:at|vs\.?) (?P<home>[A-Z .'\-]+?) \((?:[\d-]+)\)$")
# Names on a line or pair are joined by " -- ", but spacing slips ("Jeremy Lauzon--  Rasmus
# Andersson") and an en or em dash sometimes stands in ("Berkly Catton – Mackie Samoskevich"),
# both 2026-11-13. A single hyphen inside a name ("Ukko-Pekka") is not a separator.
SEPARATOR = re.compile(r"\s*--\s*|\s+[–—]\s+")
TEAM = re.compile(r"^(?P<team>.+?) projected lineup$", re.IGNORECASE)


def season_of(day: str) -> int:
    return int(day[:4]) if int(day[5:7]) >= 9 else int(day[:4]) - 1


def norm(name: str) -> str:
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z ]", "", text.lower().replace("-", " ")).strip()


def fetch(url: str = URL) -> str:
    error = None
    for attempt in range(ATTEMPTS):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.content.decode("utf-8")  # the page is UTF-8; requests may guess Latin-1
        except requests.RequestException as exc:  # ordinary retry with backoff
            error = exc
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"PROJECTED_LINEUPS_FETCH_FAILED:{type(error).__name__}")


def article_times(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or "")
        except json.JSONDecodeError:
            continue
        if data.get("@type") == "NewsArticle":
            return {"date_published": data.get("datePublished"), "date_modified": data.get("dateModified")}
    return {"date_published": None, "date_modified": None}


def parse(html: str) -> list[dict]:
    """One row per listed player: game, team, unit (F1-F4, D1-D3, G1-G2, EXTRA, SCRATCHED,
    INJURED, SUSPENDED).

    Single names after the defense pairs are the goalies, starter first. The article sometimes
    lists an extra skater alone there too (a seventh defenseman: Luke Schenn ahead of
    Vancouver's goalies, 2026-11-13), so of those single names the last two are G1 and G2 and
    any before them are EXTRA. A "Label:" line (Scratched, Injured, Suspended, ...) ends the
    goalies, so a suspended player listed after them is not read as one."""
    main = BeautifulSoup(html, "lxml").find("main")
    if main is None:
        return []
    lines = [t.replace("\xa0", " ").strip() for t in main.get_text("\n").split("\n")]
    lines = [t for t in lines if t]
    rows, game, team, day, forwards, pairs, closed = [], None, None, "TODAY", 0, 0, False
    # A bold "Scratched:" label renders as its own text node; join it to the next line.
    joined: list[str] = []
    for text in lines:
        last = joined[-1].lower() if joined else ""
        # An empty label is followed by the status report, never its body (2026-11-13: an empty
        # "Injured:" swallowed "Status report" and the next game's text stayed under the team).
        if text.lower().startswith("status report"):
            joined.append(text)
        elif last in LABELS or (last.startswith(LABELS) and last.endswith(",")):
            joined[-1] = f"{joined[-1]} {text}"
        else:
            joined.append(text)
    for text in joined:
        if text.upper() in WEEKDAYS:
            day, game, team = text.upper(), None, None
            continue
        found = GAME.match(text)
        if found and found["away"] in NICKNAMES and found["home"] in NICKNAMES:
            game = {"away": NICKNAMES[found["away"]], "home": NICKNAMES[found["home"]], "day": day}
            team = None
            continue
        found = TEAM.match(text)
        if found and game and found["team"].upper() in NICKNAMES:
            team, forwards, pairs, closed = NICKNAMES[found["team"].upper()], 0, 0, False
            continue
        if text.lower().startswith("status report"):
            # The status report closes the game, so an unread next header cannot inherit it.
            game = team = None
            continue
        if team is None:
            continue
        base = {"day": game["day"], "away": game["away"], "home": game["home"], "team_code": team}
        if SEPARATOR.search(text):
            names = [n.strip() for n in SEPARATOR.split(text)]
            if len(names) == 3:
                forwards += 1
                rows += [{**base, "unit": f"F{forwards}", "slot": i + 1, "name": n} for i, n in enumerate(names)]
            elif len(names) == 2:
                pairs += 1
                rows += [{**base, "unit": f"D{pairs}", "slot": i + 1, "name": n} for i, n in enumerate(names)]
            continue
        if re.match(r"^[A-Za-z][A-Za-z ]{2,30}:", text):
            closed = True  # any label ends the goalies
        for label in ("Scratched", "Injured", "Suspended"):
            if text.lower().startswith(label.lower() + ":"):
                body = text.split(":", 1)[1].strip()
                if body.lower().rstrip(".") != "none":
                    for item in re.split(r",(?![^(]*\))", body):
                        name, _, note = item.strip().partition(" (")
                        if name:
                            rows.append({**base, "unit": label.upper(), "slot": None, "name": name.strip(),
                                         "note": note.rstrip(")") or None})
                break
        else:
            # Single names after the defense pairs: goalies, or an extra skater (see above).
            if pairs and not closed and re.fullmatch(r"[A-Za-zÀ-ÿ.'\- ]{3,40}", text) and " " in text:
                rows.append({**base, "unit": "SINGLE", "slot": 1, "name": text})
    # An 11-forward lineup lists its last forward line with two names ("Jeffrey Viel --
    # Zemgus Girgensons", Tampa Bay 2026-11-08), read above as a fourth defense pair: with
    # three forward lines and four pairs, the first pair is forward line F4.
    units: dict[tuple, list[dict]] = {}
    for row in rows:
        units.setdefault((row["day"], row["away"], row["home"], row["team_code"]), []).append(row)
    for group in units.values():
        lines = {r["unit"] for r in group if r["unit"][0] in "FD" and r["unit"][1:].isdigit()}
        if lines >= {"F3", "D4"} and "F4" not in lines:
            for r in group:
                if r["unit"][0] == "D" and r["unit"][1:].isdigit():
                    r["unit"] = "F4" if r["unit"] == "D1" else f"D{int(r['unit'][1:]) - 1}"
    singles: dict[tuple, list[dict]] = {}
    for row in rows:
        if row["unit"] == "SINGLE":
            singles.setdefault((row["day"], row["away"], row["home"], row["team_code"]), []).append(row)
    for group in singles.values():
        for i, row in enumerate(group):
            back = len(group) - i  # 2 = second to last, 1 = last
            row["unit"] = "EXTRA" if back > 2 else f"G{len(group) - back + 1 - max(len(group) - 2, 0)}"
    return rows


def match_names(rows: pd.DataFrame, names: pd.DataFrame) -> pd.DataFrame:
    """Team-scoped match: full name, then a unique last name on that team, then a unique
    last name starting with the listed one (4+ letters; catches typos such as "Troub")."""
    names = names.assign(key=names.full_name.map(norm), last=names.last_name.map(norm))
    full = {(t, k): p for t, k, p in zip(names.team_code, names.key, names.player_id)}
    last_counts = names.groupby(["team_code", "last"]).player_id.nunique()
    last = {(t, k): p for t, k, p in zip(names.team_code, names["last"], names.player_id)
            if last_counts.get((t, k), 0) == 1}
    ids, how = [], []
    for team, name in zip(rows.team_code, rows.name):
        key = norm(name)
        if (team, key) in full:
            ids.append(full[(team, key)]); how.append("FULL_NAME")
        elif (team, key.split(" ")[-1]) in last:
            ids.append(last[(team, key.split(" ")[-1])]); how.append("LAST_NAME")
        elif len(stem := key.split(" ")[-1]) >= 4 and len(hits := names[names.team_code.eq(team)
                                                                       & names["last"].str.startswith(stem)]) == 1:
            ids.append(hits.player_id.iloc[0]); how.append("LAST_NAME_PREFIX")
        else:
            ids.append(None); how.append("UNMATCHED")
    return rows.assign(player_id=pd.array(ids, dtype="Int64"), match=how)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--slate-date", default=datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d"))
    parser.add_argument("--html", type=Path, help="parse a saved page instead of fetching")
    parser.add_argument("--output-root", type=Path, default=OUT)
    args = parser.parse_args()
    day = args.slate_date
    if day > TRIAL_END and not args.html:
        print(f"NHL_PROJECTED_LINEUPS=SKIPPED_TRIAL_ENDED end={TRIAL_END}")
        return 0
    fetched = datetime.now(timezone.utc)
    html = args.html.read_text() if args.html else fetch()
    rows = pd.DataFrame(parse(html))
    names_path = NAMES / f"names_{day}.csv"
    slate = pd.read_csv(names_path) if names_path.is_file() else pd.DataFrame(
        columns=["player_id", "full_name", "last_name", "team_code", "game_id"])
    slate_teams = set(slate.team_code)
    if not rows.empty:
        rows["on_slate"] = rows.day.eq("TODAY") & rows.team_code.isin(slate_teams)
        rows = match_names(rows, slate) if len(slate) else rows.assign(player_id=pd.NA, match="NO_NAMES_FILE")
    tonight = rows[rows.on_slate] if not rows.empty else rows
    listed = tonight[tonight.unit.str.match(r"^[FDG]\d$")] if not tonight.empty else tonight
    slate_games = int(slate.game_id.nunique()) if len(slate) else 0
    covered = int(tonight.drop_duplicates(["away", "home"]).shape[0]) if not tonight.empty else 0
    summary = {
        "schema_version": SCHEMA, "slate_date": day, "source_url": URL,
        "fetched_at_utc": fetched.isoformat().replace("+00:00", "Z"), **article_times(html),
        "slate_games": slate_games, "slate_games_with_lineups": covered,
        "teams_with_lineups": int(tonight.team_code.nunique()) if not tonight.empty else 0,
        "lineup_players": int(len(listed)),
        "matched": int(listed.match.isin(["FULL_NAME", "LAST_NAME", "LAST_NAME_PREFIX"]).sum()) if len(listed) else 0,
        "matched_by": listed.match.value_counts().to_dict() if len(listed) else {},
        "unmatched": sorted(f"{t}:{n}" for t, n, m in zip(listed.team_code, listed.name, listed.match)
                            if m == "UNMATCHED") if len(listed) else [],
        "other_day_rows": int((~rows.on_slate).sum()) if not rows.empty else 0,
        "names_file": str(names_path.relative_to(ROOT)) if names_path.is_file() else None,
        "database_writes": 0, "trial_end": TRIAL_END,
    }
    summary["status"] = ("NO_SLATE" if not slate_games else "COMPLETE" if covered == slate_games
                         else "PARTIAL" if covered else "NO_LINEUPS_PARSED")
    stamp = fetched.strftime("%Y%m%dT%H%M%SZ")
    out = args.output_root / f"season={season_of(day)}" / f"slate_date={day}" / f"observation={stamp}"
    out.mkdir(parents=True, exist_ok=False)
    (out / "page.html").write_text(html)
    rows.to_csv(out / "lineups.csv", index=False)
    (out / "observation_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print("NHL_PROJECTED_LINEUPS=" + json.dumps({k: summary[k] for k in (
        "status", "slate_games", "slate_games_with_lineups", "lineup_players", "matched")} | {"path": str(out)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
