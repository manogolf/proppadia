# File: nhl/scripts/tools/check_pbp_aggregates.py
#!/usr/bin/env python3
from __future__ import annotations
import json, sys
from pathlib import Path
from collections import defaultdict

FIXTURE_DIR = Path("nhl/fixtures")

def parse_mmss_to_minutes(mmss: str | None) -> float | None:
    if not mmss or ":" not in str(mmss): return None
    try:
        m, s = str(mmss).strip().split(":")
        return int(m) + int(s)/60.0
    except Exception:
        return None

def load_fixture(game_id: int) -> tuple[dict, dict]:
    box = json.loads((FIXTURE_DIR / f"{game_id}_box.json").read_text())
    pbp = json.loads((FIXTURE_DIR / f"{game_id}_pbp.json").read_text())
    return box, pbp

def name_map_from_box(box: dict) -> dict[int, str]:
    names: dict[int, str] = {}
    for side in ("homeTeam", "awayTeam"):
        roster = (box.get(side) or {}).get("players") or {}
        if isinstance(roster, dict):
            for pid_s, p in roster.items():
                try:
                    pid = int(pid_s)
                except Exception:
                    continue
                fn = (p.get("firstName") or {}).get("default") or p.get("firstName") or ""
                ln = (p.get("lastName") or {}).get("default") or p.get("lastName") or ""
                nm = (f"{fn} {ln}").strip() or f"Player {pid}"
                names[pid] = nm
    return names

def team_sets_from_box(box: dict) -> tuple[set[int], set[int], str, str]:
    home_ids, away_ids = set(), set()
    for arr_key, bucket in (("skaters","homeTeam"), ("forwards","homeTeam"), ("defense","homeTeam"),
                            ("skaters","awayTeam"), ("forwards","awayTeam"), ("defense","awayTeam")):
        team = box.get(bucket) or {}
        for p in team.get(arr_key, []) or []:
            pid = p.get("playerId")
            if isinstance(pid, int):
                (home_ids if bucket=="homeTeam" else away_ids).add(pid)
    home_abbr = (box.get("homeTeam") or {}).get("abbrev") or "HOME"
    away_abbr = (box.get("awayTeam") or {}).get("abbrev") or "AWAY"
    return home_ids, away_ids, home_abbr, away_abbr

def _extract_plays_root(pbp) -> list:
    """Normalize various NHL PBP shapes into a simple list of play dicts."""
    if isinstance(pbp, list):
        return pbp
    if not isinstance(pbp, dict):
        return []
    plays = pbp.get("plays")
    if isinstance(plays, list):
        return plays
    pby = pbp.get("playByPlay") or {}
    if isinstance(pby, dict) and isinstance(pby.get("allPlays"), list):
        return pby["allPlays"]
    return []


def aggregate_attempts_from_pbp(pbp) -> dict[int, dict[str, int]]:
    out: dict[int, dict[str,int]] = defaultdict(lambda: {"sog":0, "missed":0, "blocked":0})
    plays = _extract_plays_root(pbp)
    for p in plays:
        # event type (several possible keys)
        typ = str(
            p.get("typeCode")
            or p.get("typeDescKey")
            or (p.get("result") or {}).get("eventTypeId")
            or ""
        ).upper()

        # shooter id (several possible locations)
        shooter = (p.get("details") or {}).get("playerId")
        if shooter is None:
            for pl in (p.get("players") or []):
                role = (pl.get("playerType") or "").lower()
                if role in ("shooter", "scorer"):
                    shooter = (pl.get("player") or {}).get("id") or pl.get("playerId")
                    break
        try:
            shooter = int(shooter)
        except Exception:
            continue

        if typ in ("SHOT", "SHOT-ON-GOAL"):
            out[shooter]["sog"] += 1
        elif typ in ("MISSED_SHOT", "MISSED-SHOT", "MISS"):
            out[shooter]["missed"] += 1
        elif typ in ("BLOCKED_SHOT", "BLOCKED-SHOT", "BLOCK"):
            out[shooter]["blocked"] += 1

    return out
def main(game_id: int) -> None:
    box, pbp = load_fixture(game_id)
    names = name_map_from_box(box)
    home_ids, away_ids, H, A = team_sets_from_box(box)
    agg = aggregate_attempts_from_pbp(pbp)

    # quick parser checks
    samples = ["15:24", "00:59", "20:00", None, ""]
    parsed = {s: parse_mmss_to_minutes(s) for s in samples}

    # summarize attempts
    rows = []
    for pid, d in agg.items():
        total = d["sog"] + d["missed"] + d["blocked"]
        team = H if pid in home_ids else (A if pid in away_ids else "UNK")
        rows.append((total, d["sog"], d["missed"], d["blocked"], team, pid, names.get(pid, f"Player {pid}")))
    rows.sort(reverse=True)  # highest attempts first

    print(json.dumps({
        "parser_samples": parsed,
        "counts": {
            "players_with_events": len(rows),
            "total_events": sum(t for t, *_ in rows) if rows else 0
        },
        "top10": [
            {
                "player_id": pid,
                "name": nm,
                "team": team,
                "attempts": total,
                "sog": sog,
                "missed": miss,
                "blocked": blk
            }
            for total, sog, miss, blk, team, pid, nm in rows[:10]
        ]
    }, indent=2))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python nhl/scripts/tools/check_pbp_aggregates.py <game_id>", file=sys.stderr)
        sys.exit(2)
    main(int(sys.argv[1]))
