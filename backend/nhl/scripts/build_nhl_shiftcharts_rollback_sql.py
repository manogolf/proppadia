#!/usr/bin/env python3
"""Generate the guarded rollback for the season-2025 shift-chart payload correction (P2.31).

Input: the preview snapshot (shift_id, original start/end/duration/team and the
new values). Output: one all-or-nothing UPDATE that restores the originals only
if every row still holds the values the correction wrote. Large generated
output; keep it in the git-ignored evidence folder.
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pandas as pd


def sql_value(v) -> str:
    return "NULL" if pd.isna(v) else str(int(v))


def build(snapshot: Path, out: Path) -> str:
    frame = pd.read_csv(snapshot).sort_values("shift_id")
    n = len(frame)
    rows = []
    for i, r in enumerate(frame.itertuples()):
        vals = [r.shift_id, r.start_sec, r.end_sec, r.duration_sec, r.team_id, r.new_start, r.new_end, r.new_duration, r.new_team]
        cells = [sql_value(v) for v in vals]
        if i == 0:
            cells = [f"{c}::bigint" if j in (0, 4, 8) else f"{c}::int" for j, c in enumerate(cells)]
        rows.append("    (" + ", ".join(cells) + ")")
    text = f"""-- ROLLBACK ONLY with separate approval. Run as the ingestion login in one
-- transaction. Restores the {n} original values only if every row still holds
-- the corrected values. Snapshot SHA-256: {hashlib.sha256(snapshot.read_bytes()).hexdigest()}
WITH c(shift_id, o_start, o_end, o_duration, o_team, n_start, n_end, n_duration, n_team) AS (VALUES
{chr(10).join(r + ',' for r in rows[:-1])}
{rows[-1]}
)
UPDATE nhl.shiftcharts_raw AS raw
   SET start_sec = c.o_start, end_sec = c.o_end, duration_sec = c.o_duration, team_id = c.o_team
  FROM c
 WHERE raw.shift_id = c.shift_id
   AND raw.start_sec = c.n_start AND raw.end_sec = c.n_end
   AND raw.duration_sec IS NOT DISTINCT FROM c.n_duration AND raw.team_id IS NOT DISTINCT FROM c.n_team
   AND (SELECT CASE WHEN count(*) = {n} THEN TRUE
                ELSE ('SHIFTCHARTS_2025_ROLLBACK_GUARD_FAILED:matching_rows=' || count(*))::boolean END
          FROM nhl.shiftcharts_raw g JOIN c k ON g.shift_id = k.shift_id
           AND g.start_sec = k.n_start AND g.end_sec = k.n_end
           AND g.duration_sec IS NOT DISTINCT FROM k.n_duration AND g.team_id IS NOT DISTINCT FROM k.n_team);
"""
    out.write_text(text)
    return hashlib.sha256(text.encode()).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    print(build(a.snapshot, a.out))


if __name__ == "__main__":
    main()
