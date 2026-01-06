#!/usr/bin/env python3
"""
pbp_situation_breakdown.py

Fetches api-web.nhle.com play-by-play for one or more game_ids and prints:
  1) situationCode_top=[...]
  2) totals by situationCode (non-zero) with goalie/skater context
  3) inferred advantage segments (HOME/ AWAY / EVEN) based on skater counts (both goalies in net)

Usage:
  python backend/nhl/scripts/pbp_situation_breakdown.py 2025020305 2025020565
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

API = "https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"


def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def mmss_to_sec(mmss: str) -> Optional[int]:
    try:
        m, s = mmss.strip().split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return None


def abs_sec(period: int, mmss: str) -> Optional[int]:
    t = mmss_to_sec(mmss)
    if t is None:
        return None
    # periods treated as 20:00 blocks; OT handled as period 4+ same 20-min blocks (good enough for diagnostics)
    return (int(period) - 1) * 20 * 60 + t


def pbp_event_abs_sec(ev: Dict[str, Any]) -> Optional[int]:
    pd = ev.get("periodDescriptor") or {}
    per = pd.get("number")
    tip = ev.get("timeInPeriod")
    if per is None or not tip:
        return None
    return abs_sec(int(per), str(tip))


@dataclass(frozen=True)
class SitCtx:
    away_skaters: int
    home_skaters: int
    away_goalie: int
    home_goalie: int


def parse_situation_code(sc: str) -> Optional[SitCtx]:
    """
    api-web situationCode is 4 digits in order:

      AG AS HS HG
      - AG: away goalie present (1) / pulled (0)
      - AS: away skaters (3-6)
      - HS: home skaters (3-6)
      - HG: home goalie present (1) / pulled (0)

    Examples:
      1551 => 5v5, both goalies
      1451 => 4v5, home advantage
      1541 => 5v4, away advantage
      1560 => away goalie in, home goalie pulled (6 skaters)
      0651 => away goalie pulled (6 skaters), home goalie in
    """
    if not sc or len(sc) != 4 or not sc.isdigit():
        return None
    ag = int(sc[0])
    a_sk = int(sc[1])
    h_sk = int(sc[2])
    hg = int(sc[3])
    return SitCtx(away_skaters=a_sk, away_goalie=ag, home_skaters=h_sk, home_goalie=hg)


def advantage_label(ctx: SitCtx) -> str:
    # only treat "manpower advantage" when both goalies present (avoid empty net / weirdness)
    if ctx.away_goalie != 1 or ctx.home_goalie != 1:
        return "NON_STD"
    if ctx.home_skaters > ctx.away_skaters:
        return "HOME_ADV"
    if ctx.away_skaters > ctx.home_skaters:
        return "AWAY_ADV"
    return "EVEN"


def fetch_pbp(game_id: int) -> Dict[str, Any]:
    url = API.format(game_id=game_id)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    pbp = r.json()
    return pbp


def get_team_ids(pbp: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    home = (pbp.get("homeTeam") or {}).get("id")
    away = (pbp.get("awayTeam") or {}).get("id")
    return home, away


def iter_plays(pbp: Dict[str, Any]) -> List[Dict[str, Any]]:
    plays = pbp.get("plays")
    if not isinstance(plays, list):
        return []
    return plays


def code_summary(plays: List[Dict[str, Any]]) -> List[Tuple[str, int]]:
    c = Counter()
    for ev in plays:
        sc = ev.get("situationCode")
        if sc:
            c[str(sc)] += 1
    return c.most_common()


def compute_intervals_by_situation(plays: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Approximate time spent in each situationCode by diffing consecutive events,
    using timeInPeriod. This is a diagnostic; good enough to show "where minutes are".
    """
    # gather (abs_t, situationCode, ev) tuples
    rows: List[Tuple[int, str, Dict[str, Any]]] = []
    for ev in plays:
        t = pbp_event_abs_sec(ev)
        if t is None:
            continue
        sc = ev.get("situationCode")
        if not sc:
            continue
        rows.append((t, str(sc), ev))

    rows.sort(key=lambda x: x[0])

    totals: Dict[str, int] = defaultdict(int)
    for i in range(len(rows) - 1):
        t0, sc0, _ = rows[i]
        t1, _, _ = rows[i + 1]
        dt = max(0, t1 - t0)
        if dt:
            totals[sc0] += dt
    return dict(totals)


def segments_for_team_advantage(plays: List[Dict[str, Any]], which: str) -> List[Tuple[int, int]]:
    """
    which in {"HOME_ADV", "AWAY_ADV"}
    returns list of [start_abs_sec, end_abs_sec) segments where advantage holds,
    restricted to both-goalies-present states.
    """
    rows: List[Tuple[int, str, Dict[str, Any]]] = []
    for ev in plays:
        t = pbp_event_abs_sec(ev)
        if t is None:
            continue
        sc = ev.get("situationCode")
        if not sc:
            continue
        rows.append((t, str(sc), ev))

    rows.sort(key=lambda x: x[0])
    segs: List[Tuple[int, int]] = []

    cur_start: Optional[int] = None
    cur_on = False

    for i in range(len(rows) - 1):
        t0, sc0, _ = rows[i]
        t1, _, _ = rows[i + 1]
        dt = max(0, t1 - t0)
        if dt <= 0:
            continue

        ctx = parse_situation_code(sc0)
        if ctx is None:
            continue

        lab = advantage_label(ctx)
        on = (lab == which)

        if on and not cur_on:
            cur_start = t0
            cur_on = True
        if (not on) and cur_on:
            segs.append((cur_start if cur_start is not None else t0, t0))
            cur_start = None
            cur_on = False

        # if advantage continues, we extend implicitly until switch.

    # close open segment at last event time (best-effort)
    if cur_on and cur_start is not None and rows:
        last_t = rows[-1][0]
        segs.append((cur_start, last_t))

    # merge overlaps/adjacent
    segs.sort()
    merged: List[Tuple[int, int]] = []
    for s, e in segs:
        if not merged:
            merged.append((s, e))
            continue
        ps, pe = merged[-1]
        if s <= pe:
            merged[-1] = (ps, max(pe, e))
        elif s - pe <= 1:
            merged[-1] = (ps, e)
        else:
            merged.append((s, e))
    return merged


def sum_segments(segs: List[Tuple[int, int]]) -> int:
    return sum(max(0, e - s) for s, e in segs)


def fmt_abs_sec_to_p_mmss(t: int) -> str:
    per = (t // (20 * 60)) + 1
    within = t % (20 * 60)
    mm = within // 60
    ss = within % 60
    return f"P{per} {mm:02d}:{ss:02d}"


def print_game(game_id: int) -> None:
    pbp = fetch_pbp(game_id)
    url = API.format(game_id=game_id)
    plays = iter_plays(pbp)
    home_id, away_id = get_team_ids(pbp)

    print(f"\ngame_id={game_id} plays={len(plays)} url={url}")
    print(f"homeTeamId={home_id} awayTeamId={away_id}")

    top = code_summary(plays)
    print("\nsituationCode_top=", top[:10])

    totals = compute_intervals_by_situation(plays)
    # show only non-zero, sorted by seconds desc
    items = sorted(((k, v) for k, v in totals.items() if v > 0), key=lambda x: -x[1])

    print("\n--- totals by situationCode (non-zero) ---")
    for sc, secs in items:
        ctx = parse_situation_code(sc)
        if ctx is None:
            continue
        lab = advantage_label(ctx)
        print(
            f"{sc}: {secs:4d}s  "
            f"(away={ctx.away_skaters} home={ctx.home_skaters}  ag={ctx.away_goalie} hg={ctx.home_goalie}) "
            f"{lab}"
        )

    # segments (both-goalies advantage only)
    home_segs = segments_for_team_advantage(plays, "HOME_ADV")
    away_segs = segments_for_team_advantage(plays, "AWAY_ADV")

    print("\n--- inferred advantage segments (both goalies present) ---")
    print(f"home_pp_segs={len(home_segs)} home_pp_sec={sum_segments(home_segs)}")
    for s, e in home_segs[:20]:
        print(f"  H {s:4d}->{e:4d} ({e-s:3d}s)  {fmt_abs_sec_to_p_mmss(s)} -> {fmt_abs_sec_to_p_mmss(e)}")

    print(f"away_pp_segs={len(away_segs)} away_pp_sec={sum_segments(away_segs)}")
    for s, e in away_segs[:20]:
        print(f"  A {s:4d}->{e:4d} ({e-s:3d}s)  {fmt_abs_sec_to_p_mmss(s)} -> {fmt_abs_sec_to_p_mmss(e)}")


def main(argv: List[str]) -> None:
    if len(argv) < 2:
        die("Usage: python pbp_situation_breakdown.py <game_id> [game_id2 ...]")

    for raw in argv[1:]:
        try:
            gid = int(raw)
        except ValueError:
            die(f"Bad game_id: {raw}")
        print_game(gid)


if __name__ == "__main__":
    main(sys.argv)
