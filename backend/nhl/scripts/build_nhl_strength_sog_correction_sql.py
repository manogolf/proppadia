#!/usr/bin/env python3
"""Generate the guarded ev/pp/sh_sog correction SQL for one season (P2.31).

Input: a correction proposal CSV (game_id, player_id, team_id, ev, pp, sh,
pbp_ev, pbp_pp, pbp_sh) built from official play-by-play. Output: repair,
rollback and read-only verify SQL next to the proposal. Each statement is
all-or-nothing: every row must hold its exact stored (ev, pp, sh) values under
its pinned team or the guard raises. Run the repair as the ingestion login.
The generated files are large and stay in the git-ignored evidence folder.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def values(frame: pd.DataFrame) -> str:
    out = []
    for i, r in enumerate(frame.itertuples()):
        t = (int(r.game_id), int(r.player_id), int(r.team_id), int(r.ev), int(r.pp), int(r.sh),
             int(r.pbp_ev), int(r.pbp_pp), int(r.pbp_sh))
        fmt = ("    ({}::bigint, {}::bigint, {}::bigint, {}::int, {}::int, {}::int, {}::int, {}::int, {}::int)"
               if i == 0 else "    ({}, {}, {}, {}, {}, {}, {}, {}, {})")
        out.append(fmt.format(*t))
    return ",\n".join(out)


def statement(v: str, n: int, label: str, rollback: bool) -> str:
    f, t = ("to", "from") if rollback else ("from", "to")
    return f"""WITH c(game_id, player_id, raw_team_id, from_ev, from_pp, from_sh, to_ev, to_pp, to_sh) AS (VALUES
{v}
)
UPDATE nhl.skater_game_logs_raw AS raw
   SET ev_sog = c.{t}_ev, pp_sog = c.{t}_pp, sh_sog = c.{t}_sh
  FROM c
 WHERE raw.game_id = c.game_id AND raw.player_id = c.player_id AND raw.team_id = c.raw_team_id
   AND raw.ev_sog = c.{f}_ev AND raw.pp_sog = c.{f}_pp AND raw.sh_sog = c.{f}_sh
   AND (SELECT CASE WHEN count(*) = {n} THEN TRUE
                ELSE ('{label}_GUARD_FAILED:matching_rows=' || count(*))::boolean END
          FROM nhl.skater_game_logs_raw g JOIN c k
            ON g.game_id = k.game_id AND g.player_id = k.player_id AND g.team_id = k.raw_team_id
           AND g.ev_sog = k.{f}_ev AND g.pp_sog = k.{f}_pp AND g.sh_sog = k.{f}_sh);
"""


def verify(v: str, n: int, sha: str) -> str:
    return f"""-- Read-only receipt. Expected before: original={n}, corrected=0; after: corrected={n}.
-- Proposal SHA-256: {sha}
BEGIN READ ONLY;
WITH c(game_id, player_id, raw_team_id, from_ev, from_pp, from_sh, to_ev, to_pp, to_sh) AS (VALUES
{v}
)
SELECT now() AS checked_at_utc, current_user AS checked_as,
       count(*) FILTER (WHERE r.ev_sog = c.from_ev AND r.pp_sog = c.from_pp AND r.sh_sog = c.from_sh) AS original,
       count(*) FILTER (WHERE r.ev_sog = c.to_ev AND r.pp_sog = c.to_pp AND r.sh_sog = c.to_sh) AS corrected,
       count(*) FILTER (WHERE r.player_id IS NULL) AS missing,
       count(*) FILTER (WHERE r.player_id IS NOT NULL
                          AND NOT (r.ev_sog = c.from_ev AND r.pp_sog = c.from_pp AND r.sh_sog = c.from_sh)
                          AND NOT (r.ev_sog = c.to_ev AND r.pp_sog = c.to_pp AND r.sh_sog = c.to_sh)) AS other
FROM c LEFT JOIN nhl.skater_game_logs_raw r USING (game_id, player_id);
COMMIT;
"""


def build(proposal: Path, season: str, out_dir: Path) -> dict:
    frame = pd.read_csv(proposal)
    frame = frame[frame.game_id.astype(str).str[:4].eq(season)].sort_values(["game_id", "player_id"])
    n = len(frame)
    sha = hashlib.sha256(proposal.read_bytes()).hexdigest()
    v = values(frame)
    label = f"STRENGTH_SOG_{season}"
    head = (f"-- PREPARED ONLY. Run as the ingestion login in one transaction; commit only if {n} rows change.\n"
            f"-- ev/pp/sh_sog correction for season {season} from official play-by-play (P2.31).\n"
            f"-- Proposal: {proposal.name} SHA-256 {sha}\n")
    files = {
        f"repair_nhl_strength_sog_{season}.sql": head + statement(v, n, label, False),
        f"rollback_nhl_strength_sog_{season}.sql": head.replace("PREPARED ONLY", "ROLLBACK ONLY") + statement(v, n, label + "_ROLLBACK", True),
        f"verify_nhl_strength_sog_{season}.sql": verify(v, n, sha),
    }
    out = {"season": season, "rows": n, "proposal_sha256": sha, "files": {}}
    for name, text in files.items():
        (out_dir / name).write_text(text)
        out["files"][name] = hashlib.sha256(text.encode()).hexdigest()
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--proposal", type=Path, required=True)
    ap.add_argument("--season", action="append", required=True)
    args = ap.parse_args()
    print(json.dumps([build(args.proposal, s, args.proposal.parent) for s in args.season], indent=2))


if __name__ == "__main__":
    main()
