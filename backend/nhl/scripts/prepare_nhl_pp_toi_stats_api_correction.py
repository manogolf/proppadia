#!/usr/bin/env python3
"""Prepare the PP TOI correction from the NHL stats API (NHL Backlog P2.45).

Source of record (operator decision 2026-11-15): the stats REST endpoint
`skater/timeonice` (`ppTimeOnIce`, seconds, per player-game). This script never
writes to the database. It:

1. `--fetch`: captures one official response per regular-season date (raw
   bytes, SHA-256 manifest) under the evidence directory; resumable.
2. `--build`: reads the raw table read-only (research identity, repeatable
   read), compares each stored `pp_toi_minutes` with the official value
   rounded to 2 decimals, and writes the proposal, per-month guarded UPDATE and
   rollback statements (each one statement for `execute_nhl_source_correction.py`,
   validated by `validate_correction_sql`), and a summary with checksums.
3. `--verify`: read-only counts per batch: original, corrected, missing, other.

Rows are excluded (never changed) when the official and stored TOI differ by
more than 0.05 minutes, when the stored team does not match the official team
under that season's team mapping, or when the official PP time exceeds TOI.
Official player-games with no stored row are reported, not inserted.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
EVIDENCE = ROOT / "artifacts/analysis/nhl/pp_toi_stats_api_correction_2026-11-15"
OFFICIAL_URL = "https://api.nhle.com/stats/rest/en/skater/timeonice"
FIRST_DATE = "2023-10-10"
TOI_TOLERANCE = Decimal("0.05")
MAX_SQL_BYTES = 950_000   # executor limit is 1,000,000 bytes per statement
GUARD_ERROR = "PP_TOI_STATS_API_GUARD_FAILED"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def official_params(day: str) -> dict:
    return {"isAggregate": "false", "isGame": "true", "limit": "-1",
            "cayenneExp": f'gameTypeId=2 and gameDate>="{day}" and gameDate<="{day} 23:59:59"'}


def minutes(seconds) -> Decimal:
    """Official seconds as the table's numeric(5,2) minutes."""
    return (Decimal(int(seconds)) / Decimal(60)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _dec(value) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------- database read

def read_raw(first_date: str, last_date: str) -> list[dict]:
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as conn, conn.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT l.game_id::bigint, l.player_id::bigint, l.team_id::bigint, g.game_date::text,
                              l.pp_toi_minutes, l.toi_minutes
                       FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING (game_id)
                       WHERE substring(g.game_id::text, 5, 2) = '02'
                         AND g.game_date BETWEEN %s AND %s""", (first_date, last_date))
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def regular_season_dates(rows: list[dict]) -> list[str]:
    return sorted({r["game_date"] for r in rows})


# ------------------------------------------------------------------- official

def fetch(dates: list[str], evidence: Path = EVIDENCE) -> dict:
    import requests
    out = evidence / "official"
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = evidence / "official_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.is_file() else {}
    session = requests.Session()
    session.headers["User-Agent"] = "research-client/1.0"
    for day in dates:
        path = out / f"{day}.json"
        if day in manifest and path.is_file() and sha256_bytes(path.read_bytes()) == manifest[day]["sha256"]:
            continue
        for attempt in range(5):
            try:
                resp = session.get(OFFICIAL_URL, params=official_params(day), timeout=60)
                resp.raise_for_status()
                body = resp.content
                payload = json.loads(body)
                break
            except Exception:
                if attempt == 4:
                    raise
                time.sleep(2 * (attempt + 1))
        if len(payload["data"]) != payload["total"]:
            raise RuntimeError(f"OFFICIAL_RESPONSE_TRUNCATED:{day}")
        path.write_bytes(body)
        manifest[day] = {"url": OFFICIAL_URL, "params": official_params(day), "sha256": sha256_bytes(body),
                         "rows": len(payload["data"]), "fetched_at_utc": datetime.now(timezone.utc).isoformat()}
        manifest_path.write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
        time.sleep(0.25)
    return manifest


def load_official(evidence: Path = EVIDENCE) -> dict[tuple[int, int], dict]:
    manifest = json.loads((evidence / "official_manifest.json").read_text())
    rows = {}
    for day, entry in sorted(manifest.items()):
        body = (evidence / "official" / f"{day}.json").read_bytes()
        if sha256_bytes(body) != entry["sha256"]:
            raise RuntimeError(f"OFFICIAL_EVIDENCE_HASH_MISMATCH:{day}")
        for r in json.loads(body)["data"]:
            rows[(int(r["gameId"]), int(r["playerId"]))] = {
                "team": r["teamAbbrev"], "pp_seconds": int(r["ppTimeOnIce"]), "toi_seconds": int(r["timeOnIce"]),
                "game_date": r["gameDate"], "source_sha256": entry["sha256"]}
    return rows


# --------------------------------------------------------------------- compare

def team_mapping(raw: list[dict], official: dict) -> dict[tuple[int, int], str]:
    """(season prefix, stored team_id) -> the official team it carries most often."""
    votes: dict[tuple[int, int], Counter] = defaultdict(Counter)
    for r in raw:
        o = official.get((int(r["game_id"]), int(r["player_id"])))
        if o:
            votes[(int(r["game_id"]) // 1_000_000, int(r["team_id"]))][o["team"]] += 1
    return {key: c.most_common(1)[0][0] for key, c in votes.items()}


def build_proposal(raw: list[dict], official: dict) -> tuple[list[dict], Counter, list[dict]]:
    """Rows to change, outcome counts, and official player-games with no stored row."""
    mapping = team_mapping(raw, official)
    stored_keys = set()
    proposal, counts = [], Counter()
    for r in sorted(raw, key=lambda x: (int(x["game_id"]), int(x["player_id"]))):
        key = (int(r["game_id"]), int(r["player_id"]))
        stored_keys.add(key)
        o = official.get(key)
        if o is None:
            counts["no_official_row"] += 1
            continue
        toi_official = minutes(o["toi_seconds"])
        toi_stored = _dec(r["toi_minutes"])
        if toi_stored is None or abs(toi_stored - toi_official) > TOI_TOLERANCE:
            counts["excluded_toi_mismatch"] += 1
            continue
        if mapping.get((key[0] // 1_000_000, int(r["team_id"]))) != o["team"]:
            counts["excluded_team_mismatch"] += 1
            continue
        if o["pp_seconds"] > o["toi_seconds"]:
            counts["excluded_pp_above_toi"] += 1
            continue
        to_value = minutes(o["pp_seconds"])
        from_value = _dec(r["pp_toi_minutes"])
        if from_value == to_value:
            counts["already_correct"] += 1
            continue
        counts["change_from_null" if from_value is None else "change"] += 1
        proposal.append({"game_id": key[0], "player_id": key[1], "team_id": int(r["team_id"]),
                         "game_date": str(r["game_date"]), "official_team": o["team"],
                         "from_value": "" if from_value is None else str(from_value), "to_value": str(to_value),
                         "source_sha256": o["source_sha256"]})
    missing = [{"game_id": g, "player_id": p, **o} for (g, p), o in sorted(official.items()) if (g, p) not in stored_keys]
    counts["official_without_stored_row"] = len(missing)
    return proposal, counts, missing


# ------------------------------------------------------------------------- SQL

def _values(rows: list[dict], *, reverse: bool) -> str:
    lines = []
    for i, r in enumerate(rows):
        frm, to = (r["to_value"], r["from_value"]) if reverse else (r["from_value"], r["to_value"])
        frm = "NULL" if frm == "" else frm
        to = "NULL" if to == "" else to
        if i == 0:
            lines.append(f"    ({r['game_id']}::bigint, {r['player_id']}::bigint, {r['team_id']}::bigint, "
                         f"{frm}::numeric, {to}::numeric)")
        else:
            lines.append(f"    ({r['game_id']}, {r['player_id']}, {r['team_id']}, {frm}, {to})")
    return ",\n".join(lines)


def update_sql(rows: list[dict], batch: str, *, reverse: bool = False) -> str:
    """One guarded, all-or-nothing UPDATE: every key must hold its pinned value under its pinned team."""
    kind = "Rollback" if reverse else "Correction"
    n = len(rows)
    return f"""-- PREPARED ONLY. {kind} batch {batch}: {n} rows of nhl.skater_game_logs_raw.pp_toi_minutes
-- set to the NHL stats API ppTimeOnIce (NHL Backlog P2.45). Source-correction role required;
-- run through backend/nhl/scripts/execute_nhl_source_correction.py with this file's SHA-256.
-- All-or-nothing: unless all {n} keys currently hold their expected value under their pinned
-- team, the guard's cast raises and nothing changes.
UPDATE nhl.skater_game_logs_raw
SET pp_toi_minutes = c.to_value
FROM (
  SELECT k.*,
         count(r.player_id) OVER () AS matching_rows
  FROM (VALUES
{_values(rows, reverse=reverse)}
  ) AS k(game_id, player_id, raw_team_id, from_value, to_value)
  LEFT JOIN nhl.skater_game_logs_raw r
    ON r.game_id = k.game_id AND r.player_id = k.player_id
   AND r.team_id = k.raw_team_id AND r.pp_toi_minutes IS NOT DISTINCT FROM k.from_value
) AS c
WHERE skater_game_logs_raw.game_id = c.game_id
  AND skater_game_logs_raw.player_id = c.player_id
  AND skater_game_logs_raw.team_id = c.raw_team_id
  AND skater_game_logs_raw.pp_toi_minutes IS NOT DISTINCT FROM c.from_value
  AND (CASE WHEN c.matching_rows = {n} THEN TRUE
       ELSE ('{GUARD_ERROR}:{batch}:matching_rows=' || c.matching_rows)::boolean END);
"""


def editor_block(update: str, batch: str, rows: int) -> str:
    """Paste-ready Supabase SQL editor block: the reviewed UPDATE inside a temporary role grant.

    Same shape as the earlier operator PP-TOI blocks: one transaction grants the
    correction role to postgres, switches to it, runs the UPDATE unchanged,
    switches back, revokes and checks the revoke. Any failure rolls everything
    back, the grant included. Re-running after success raises the guard
    (`matching_rows=0`) and rolls back: no change, but an error, not a silent pass.
    """
    return f"""-- PREPARED ONLY. Owner authorization required. Paste this whole block into the Supabase SQL
-- editor for the production project and run it once. Batch {batch}: {rows} rows of
-- nhl.skater_game_logs_raw.pp_toi_minutes set to the NHL stats API ppTimeOnIce (NHL Backlog P2.45).
-- One transaction; any failure aborts it and nothing is committed, including the grant.
-- Afterwards: prepare_nhl_pp_toi_stats_api_correction.py --verify (expect this batch corrected={rows}).

BEGIN;

DO $$
BEGIN
  IF session_user <> 'postgres' THEN
    RAISE EXCEPTION 'RUN_AS_POSTGRES_IN_SQL_EDITOR:session_user=%', session_user;
  END IF;
  IF pg_has_role(session_user, 'proppadia_nhl_source_correction', 'MEMBER') THEN
    RAISE EXCEPTION 'CORRECTION_ROLE_ALREADY_GRANTED_REFUSING_TO_PROCEED';
  END IF;
END $$;

GRANT proppadia_nhl_source_correction TO postgres;
SET LOCAL ROLE proppadia_nhl_source_correction;

DO $$
BEGIN
  IF current_user <> 'proppadia_nhl_source_correction' THEN
    RAISE EXCEPTION 'CORRECTION_SET_ROLE_VERIFICATION_FAILED:current_user=%', current_user;
  END IF;
END $$;

{update.strip()}

RESET ROLE;
REVOKE proppadia_nhl_source_correction FROM postgres;

DO $$
BEGIN
  IF current_user <> 'postgres'
     OR pg_has_role('postgres', 'proppadia_nhl_source_correction', 'MEMBER') THEN
    RAISE EXCEPTION 'CORRECTION_ROLE_RESET_OR_REVOKE_FAILED';
  END IF;
END $$;

COMMIT;

SELECT now() AS checked_at_utc, current_user AS checked_as, '{batch}' AS batch, {rows} AS batch_rows,
       pg_has_role('postgres', 'proppadia_nhl_source_correction', 'MEMBER') AS correction_role_still_granted;
"""


def batches(proposal: list[dict]) -> dict[str, list[dict]]:
    """One batch per calendar month; split further only if a statement would exceed the size limit."""
    by_month: dict[str, list[dict]] = defaultdict(list)
    for r in proposal:
        by_month[r["game_date"][:7]].append(r)
    out = {}
    for month, rows in sorted(by_month.items()):
        parts = 1
        while len(update_sql(rows[: -(-len(rows) // parts)], month).encode()) > MAX_SQL_BYTES:
            parts += 1
        size = -(-len(rows) // parts)
        for i in range(parts):
            out[month if parts == 1 else f"{month}-part{i + 1}"] = rows[i * size:(i + 1) * size]
    return out


def write_build(proposal, counts, missing, evidence: Path = EVIDENCE) -> dict:
    from backend.nhl.source_correction_access import validate_correction_sql
    sql_dir = evidence / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)
    fields = ["game_id", "player_id", "team_id", "game_date", "official_team", "from_value", "to_value", "source_sha256"]
    with open(evidence / "proposal.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(proposal)
    with open(evidence / "official_without_stored_row.csv", "w", newline="") as fh:
        cols = ["game_id", "player_id", "team", "game_date", "pp_seconds", "toi_seconds", "source_sha256"]
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(missing)
    summary = {"generated_at_utc": datetime.now(timezone.utc).isoformat(), "source": OFFICIAL_URL,
               "counts": dict(counts), "proposal_rows": len(proposal),
               "proposal_sha256": sha256_bytes((evidence / "proposal.csv").read_bytes()), "batches": []}
    for name, rows in batches(proposal).items():
        entry = {"batch": name, "rows": len(rows)}
        for kind, reverse in (("repair", False), ("rollback", True)):
            sql = update_sql(rows, name, reverse=reverse)
            validate_correction_sql(sql)
            path = sql_dir / f"{kind}_{name}.sql"
            path.write_text(sql)
            entry[f"{kind}_sql"] = str(path.relative_to(ROOT))
            entry[f"{kind}_sha256"] = sha256_bytes(sql.encode())
            if kind == "repair":
                block = editor_block(sql, name, len(rows))
                block_path = sql_dir / f"editor_{name}.sql"
                block_path.write_text(block)
                entry["editor_sql"] = str(block_path.relative_to(ROOT))
                entry["editor_sha256"] = sha256_bytes(block.encode())
        summary["batches"].append(entry)
    (evidence / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    return summary


def verify(evidence: Path = EVIDENCE) -> list[dict]:
    """Read-only state of every proposal row: original, corrected, missing or other, per batch."""
    with open(evidence / "proposal.csv") as fh:
        proposal = list(csv.DictReader(fh))
    first, last = min(r["game_date"] for r in proposal), max(r["game_date"] for r in proposal)
    current = {(int(r["game_id"]), int(r["player_id"]), int(r["team_id"])): _dec(r["pp_toi_minutes"])
               for r in read_raw(first, last)}
    out = []
    for name, rows in batches(proposal).items():
        c = Counter()
        for r in rows:
            key = (int(r["game_id"]), int(r["player_id"]), int(r["team_id"]))
            if key not in current:
                c["missing"] += 1
                continue
            value = current[key]
            if value == _dec(r["to_value"]):
                c["corrected"] += 1
            elif value == _dec(r["from_value"]):
                c["original"] += 1
            else:
                c["other"] += 1
        out.append({"batch": name, "rows": len(rows), **{k: c[k] for k in ("original", "corrected", "missing", "other")}})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--last-date", required=True, help="last regular-season date to include, YYYY-MM-DD")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fetch", action="store_true")
    mode.add_argument("--build", action="store_true")
    mode.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    if args.verify:
        for row in verify():
            print(json.dumps(row))
        return 0
    raw = read_raw(FIRST_DATE, args.last_date)
    if args.fetch:
        manifest = fetch(regular_season_dates(raw))
        print(f"PP TOI STATS API EVIDENCE: {len(manifest)} dates, {sum(e['rows'] for e in manifest.values())} rows")
        return 0
    proposal, counts, missing = build_proposal(raw, load_official())
    summary = write_build(proposal, counts, missing)
    print(f"PP TOI STATS API PROPOSAL: {summary['proposal_rows']} rows in {len(summary['batches'])} batches; "
          f"counts {json.dumps(dict(counts), sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
