# File: nhl/scripts/tools/inspect_fixture.py
#!/usr/bin/env python3
from __future__ import annotations
import json, sys
from pathlib import Path
from collections import Counter, defaultdict

FIXTURE_DIR = Path("nhl/fixtures")

def load_fixture(game_id: int) -> tuple[dict, object]:
    box = json.loads((FIXTURE_DIR / f"{game_id}_box.json").read_text(encoding="utf-8"))
    pbp_path = FIXTURE_DIR / f"{game_id}_pbp.json"
    pbp = json.loads(pbp_path.read_text(encoding="utf-8")) if pbp_path.exists() else {}
    return box, pbp

def plays_list(pbp_obj) -> list:
    if isinstance(pbp_obj, list):
        return pbp_obj
    if isinstance(pbp_obj, dict):
        # modern api-web sometimes: {"plays": [...]} at root
        if isinstance(pbp_obj.get("plays"), list):
            return pbp_obj["plays"]

        # legacy-ish: {"playByPlay": {"allPlays": [...]}} or {"playByPlay":{"plays":[...]}}
        pby = pbp_obj.get("playByPlay")
        if isinstance(pby, dict):
            if isinstance(pby.get("allPlays"), list):
                return pby["allPlays"]
            if isinstance(pby.get("plays"), list):
                return pby["plays"]

        # NHL statsapi style: {"liveData":{"plays":{"allPlays":[...]}}}
        live = pbp_obj.get("liveData")
        if isinstance(live, dict):
            plays = live.get("plays")
            if isinstance(plays, dict) and isinstance(plays.get("allPlays"), list):
                return plays["allPlays"]

    return []

def event_type(p: dict) -> str:
    """
    Prefer human-readable labels if present; otherwise fall back to numeric codes.
    Supports multiple payload shapes (modern api-web, legacy statsapi).
    """
    # 1) Prefer human-readable strings in common nests
    #    - modern: p["typeDescKey"] or p["details"]["typeDescKey"]
    #    - legacy: p["result"]["eventTypeId"] (e.g., "SHOT", "MISSED_SHOT")
    for k in ("typeDescKey",):
        v = p.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()

    for nest in ("details", "result"):
        d = p.get(nest)
        if isinstance(d, dict):
            for kk in ("typeDescKey", "eventTypeId"):
                vv = d.get(kk)
                if isinstance(vv, str) and vv.strip():
                    return vv.strip().upper()

    # 2) Fall back to numeric-ish codes
    for k in ("typeCode", "eventTypeId", "eventCode"):
        code = p.get(k)
        if isinstance(code, int):
            return f"CODE_{code}"
        if isinstance(code, str) and code.strip().isdigit():
            return f"CODE_{code.strip()}"

    # 3) Final fallback
    return "UNKNOWN"

def _team_players_dict(team_obj: dict) -> dict[int, dict]:
    """
    boxscore often has team_obj["players"] = { "<pid>": {...}, ... }
    Return {pid: player_dict} with int keys. Gracefully handles "ID8480000" keys.
    """
    raw = (team_obj or {}).get("players") or {}
    out: dict[int, dict] = {}
    if isinstance(raw, dict):
        for pid_s, pdata in raw.items():
            try:
                pid = int(pid_s)
            except Exception:
                try:
                    pid = int("".join(ch for ch in str(pid_s) if ch.isdigit()))
                except Exception:
                    continue
            out[pid] = pdata or {}
    return out

def _pos_code(player_dict: dict, default: str = "F") -> str:
    code = (player_dict.get("positionCode") or player_dict.get("position") or "").upper()
    if code in ("G","D","F"): return code
    if code in ("LW","RW","C"): return "F"
    return default

def collect_skaters(box: dict) -> list[dict]:
    """
    Prefer team.players{} dict; fall back to skaters/forwards/defense arrays if present.
    Only returns non-goalies.
    """
    out: list[dict] = []
    for side in ("homeTeam", "awayTeam"):
        team = box.get(side) or {}

        # 1) Preferred: players dict
        players = _team_players_dict(team)
        if players:
            for _, pdata in players.items():
                if _pos_code(pdata, "F") != "G":
                    out.append(pdata)
            continue

        # 2) Fallback: arrays
        for key in ("skaters", "forwards", "defense"):
            arr = team.get(key)
            if isinstance(arr, list):
                out.extend(arr)
    return out

def get_int(d: dict, *keys, default=None):
    for k in keys:
        v = d.get(k)
        if v is None: 
            continue
        try:
            return int(v)
        except Exception:
            try:
                return int(float(v))
            except Exception:
                continue
    return default

def _strval(x) -> str:
    return "" if x is None else str(x)

def name_of(p: dict) -> str:
    """
    Resolve first/last name across common shapes:
    - {"firstName":{"default":"Sidney"}}, {"lastName":{"default":"Crosby"}}
    - {"firstName":"Sidney"}, {"lastName":"Crosby"}
    """
    def pick(d: dict, key: str) -> str:
        v = d.get(key)
        if isinstance(v, dict):
            v = v.get("default") or v.get("en") or v.get("eng") or ""
        return _strval(v).strip()
    fn = pick(p, "firstName")
    ln = pick(p, "lastName")
    nm = (fn + " " + ln).strip()
    if nm:
        return nm
    # fallback: sometimes nested under "player" or "info"
    for node_key in ("player", "info"):
        node = p.get(node_key) or {}
        if isinstance(node, dict):
            fn = pick(node, "firstName")
            ln = pick(node, "lastName")
            nm = (fn + " " + ln).strip()
            if nm:
                return nm
    # final fallback
    pid = p.get("playerId") or p.get("id")
    return f"Player {pid}" if pid else "Unknown"

def shooter_id_from_event(p: dict) -> int | None:
    """
    Try multiple shapes to extract the shooter/scorer id.
    - modern api-web: p["details"]["playerId"]
    - legacy: p["players"][{"playerType":"Shooter"/"Scorer"}]["player"]["id"]
    """
    # modern
    d = p.get("details")
    if isinstance(d, dict):
        sid = d.get("playerId")
        try:
            return int(sid) if sid is not None else None
        except Exception:
            pass

    # legacy-ish list
    for pl in p.get("players", []) or []:
        role = (pl.get("playerType") or "").lower()
        if role in ("shooter", "scorer"):
            pid = (pl.get("player") or {}).get("id") or pl.get("playerId")
            try:
                return int(pid)
            except Exception:
                continue
    return None

def event_bucket(p: dict) -> str | None:
    """
    Map an event to one of: 'sog', 'missed', 'blocked', or None.
    Uses `event_type(p)` you defined earlier.
    """
    et = event_type(p)  # already normalized to UPPER
    if et in ("SHOT", "SHOT-ON-GOAL", "SHOT_ON_GOAL"):
        return "sog"
    if et in ("MISSED_SHOT", "MISSED-SHOT", "MISS"):
        return "missed"
    if et in ("BLOCKED_SHOT", "BLOCKED-SHOT", "BLOCK"):
        return "blocked"
    return None

def aggregate_attempts_from_pbp(pbp_obj) -> dict[int, dict[str, int]]:
    """
    Return { player_id: { 'sog': n, 'missed': n, 'blocked': n } }
    Compatible with both api-web and statsapi-like payloads.
    """
    out: dict[int, dict[str, int]] = defaultdict(lambda: {"sog": 0, "missed": 0, "blocked": 0})
    plays = plays_list(pbp_obj)
    for p in plays:
        if not isinstance(p, dict):
            continue
        bucket = event_bucket(p)
        if not bucket:
            continue
        sid = shooter_id_from_event(p)
        if sid is None:
            continue
        out[sid][bucket] += 1
    return dict(out)

def to_int(x):
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: python nhl/scripts/tools/inspect_fixture.py <game_id>", file=sys.stderr)
        sys.exit(2)

    gid = int(sys.argv[1])
    box, pbp = load_fixture(gid)

    # --- PBP summary ---
    plays = plays_list(pbp)
    counts = Counter(event_type(p) for p in plays if isinstance(p, dict))
    limited = False
    if isinstance(pbp, dict):
        limited = bool(pbp.get("limitedScoring") or (pbp.get("gameState") == "FINAL_LIM"))

    print("=== PBP SUMMARY ===")
    print(f"plays: {len(plays)} | unique event types: {len([k for k in counts if k])} | limitedScoring: {limited}")
    for et, n in counts.most_common(20):
        if et:
            print(f"  {et:18s} {n}")

    # OPTIONAL: see top shooters by attempts
    agg = aggregate_attempts_from_pbp(pbp)
    if agg:
        top = sorted(
            ((pid, d["sog"], d["missed"], d["blocked"], d["sog"] + d["missed"] + d["blocked"])
             for pid, d in agg.items()),
            key=lambda x: x[4], reverse=True
        )[:10]
        print("\nTop attempts by PBP (pid  sog miss blk att):")
        for pid, sog, miss, blk, att in top:
            print(f"  {pid:9d}  {sog:3d} {miss:4d} {blk:3d} {att:4d}")
    

    # --- Boxscore skater fields summary ---
    skaters = collect_skaters(box)
    print("\n=== BOXSCORE SKATERS ===")
    print(f"skaters listed: {len(skaters)}")

    # sample a few fields we care about (works for players-dict or array shapes)
    samples = []
    for p in skaters[:8]:
        nm = name_of(p)
        pid = p.get("playerId") or p.get("id")
        # SOG / misses / blocks may be on top-level or within a nested "stats"
        stats = p.get("stats") if isinstance(p.get("stats"), dict) else {}
        sog  = get_int(p, "sog", "shotsOnGoal", "shots") or get_int(stats or {}, "sog", "shotsOnGoal", "shots")
        miss = get_int(p, "missedShots", "missed") or get_int(stats or {}, "missedShots", "missed")
        blk  = get_int(p, "blockedShotsTaken", "blocked") or get_int(stats or {}, "blockedShotsTaken", "blocked")
        toi  = (p.get("toi") or p.get("timeOnIce")
                or (stats or {}).get("toi") or (stats or {}).get("timeOnIce"))
        samples.append({"name": nm, "player_id": pid, "sog": sog, "miss": miss, "blk": blk, "toi": toi})
    for row in samples:
        print(f"  {row}")

    # show which per-player numeric keys exist across the roster (top-level + stats.*)
    numeric_keys = Counter()
    for p in skaters:
        for k, v in p.items():
            if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
                numeric_keys[k] += 1
        stats = p.get("stats") if isinstance(p.get("stats"), dict) else {}
        for k, v in (stats or {}).items():
            if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
                numeric_keys[f"stats.{k}"] += 1

    common_keys = [k for k, n in numeric_keys.most_common() if n >= max(3, len(skaters)//10)]
    print("\ncommon numeric-ish keys (appear often):")
    for k in common_keys[:30]:
        print(" ", k)

if __name__ == "__main__":
    main()
