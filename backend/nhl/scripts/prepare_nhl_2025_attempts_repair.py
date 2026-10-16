"""Build guarded, review-only SQL from the retained official-PBP candidate."""

from __future__ import annotations

import argparse
import csv
import hashlib
from pathlib import Path
from typing import Any


MANIFEST_SHA256 = "354441185ee1fc981a81d0cd7730062113c5e19ecf8eca41a74160d6a9fdddc4"
EXPECTED_TARGETS = 34_108
EXPECTED_UNRESOLVED = 6_368
EXPECTED_VERIFIED_CORRECT = 6_237
CURRENT_SNAPSHOT_SHA256 = "4d5745c4818d871ebb71d18ec1fc14157c753be7affe7f68a9301403249778bf"
TARGET_STATUSES = {"VERIFIED_INCORRECT", "RECOVERABLE_NULL"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def sql_int(value: str) -> str:
    return "NULL" if value in ("", "NULL") else str(int(value))


def sql_text(value: str) -> str:
    return "NULL" if value in ("", "NULL") else "'" + value.replace("'", "''") + "'"


def validate(candidate: list[dict[str, str]], snapshot: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    current = {(r["game_id"], r["player_id"]): r for r in snapshot}
    if len(current) != len(snapshot):
        raise ValueError("CURRENT_SNAPSHOT_DUPLICATE_IDENTITY")
    if len({(r["game_id"], r["player_id"]) for r in candidate}) != len(candidate):
        raise ValueError("CANDIDATE_DUPLICATE_IDENTITY")
    targets = [r for r in candidate if r["candidate_status"] in TARGET_STATUSES]
    unresolved = [r for r in candidate if r["candidate_status"].startswith("UNRESOLVED")]
    if len(targets) != EXPECTED_TARGETS or len(unresolved) != EXPECTED_UNRESOLVED:
        raise ValueError(f"CANDIDATE_COUNTS_MISMATCH:{len(targets)}:{len(unresolved)}")
    if len(candidate) != EXPECTED_TARGETS + EXPECTED_UNRESOLVED + EXPECTED_VERIFIED_CORRECT:
        raise ValueError(f"CANDIDATE_POPULATION_MISMATCH:{len(candidate)}")
    seen: set[tuple[str, str]] = set()
    for row in targets + unresolved:
        key = (row["game_id"], row["player_id"])
        dbrow = current.get(key)
        if dbrow is None:
            raise ValueError(f"CURRENT_KEY_MISSING:{key}")
        if row["game_id"][:6] != "202502":
            raise ValueError(f"OUT_OF_SCOPE_GAME:{row['game_id']}")
        if row["shot_attempts"] != dbrow["shot_attempts"]:
            raise ValueError(f"CURRENT_ATTEMPTS_CHANGED:{key}:{row['shot_attempts']}:{dbrow['shot_attempts']}")
        # Compare all retained raw fields which are present in both snapshots.
        for col in ("team_id", "opponent_id", "game_date", "shots_on_goal", "toi_minutes", "pp_toi_minutes", "blocks", "created_at"):
            if col in dbrow and row.get(col, "") != dbrow.get(col, ""):
                raise ValueError(f"CURRENT_SOURCE_FIELD_CHANGED:{key}:{col}")
        if row["candidate_status"] in TARGET_STATUSES:
            original, official = row["shot_attempts"], row["official_attempts"]
            if official == "" or int(official) < 0 or original == official:
                raise ValueError(f"INVALID_TARGET_VALUES:{key}")
            if row["candidate_status"] == "RECOVERABLE_NULL" and original != "":
                raise ValueError(f"RECOVERABLE_NULL_NOT_NULL:{key}")
            if row["candidate_status"] == "VERIFIED_INCORRECT" and original == "":
                raise ValueError(f"INCORRECT_VALUE_IS_NULL:{key}")
        seen.add(key)
    return targets, unresolved


def values(rows: list[dict[str, str]], unresolved: bool = False) -> str:
    lines = []
    for row in rows:
        fields = [row["game_id"], row["player_id"], row["shot_attempts"]]
        if not unresolved:
            fields += [row["official_attempts"], row["pbp_sha256"], row["boxscore_sha256"]]
        vals = [sql_int(x) for x in fields[:3]]
        if not unresolved:
            vals += [sql_int(fields[3]), sql_text(fields[4]), sql_text(fields[5])]
        lines.append("(" + ", ".join(vals) + ")")
    return ",\n".join(lines)


def sql_scripts(targets: list[dict[str, str]], unresolved: list[dict[str, str]], candidate_hash: str) -> tuple[str, str]:
    common = f"""-- PREPARED ONLY. Requires separate authorization before execution.
-- Recovery manifest SHA-256: {MANIFEST_SHA256}
-- Candidate CSV SHA-256: {candidate_hash}
-- Current read-only DB snapshot SHA-256: {CURRENT_SNAPSHOT_SHA256}
-- Scope: 2025 game-id prefix 202502; exact official-PBP-reconciled identities only.
-- Targets: {len(targets)}; unresolved guard rows: {len(unresolved)}.
-- This script changes shot_attempts only. No 2026-prefix key is in scope.
BEGIN;
SET LOCAL lock_timeout = '10s';
CREATE TEMP TABLE attempt_2025_candidates (
  game_id bigint NOT NULL CHECK (game_id >= 2025020000 AND game_id < 2025030000),
  player_id bigint NOT NULL,
  original_attempts integer,
  official_attempts integer NOT NULL CHECK (official_attempts >= 0),
  pbp_sha256 text NOT NULL,
  boxscore_sha256 text NOT NULL,
  PRIMARY KEY (game_id, player_id),
  CHECK (original_attempts IS DISTINCT FROM official_attempts)
) ON COMMIT DROP;
INSERT INTO attempt_2025_candidates VALUES
{values(targets)};
CREATE TEMP TABLE attempt_2025_unresolved_guard (
  game_id bigint NOT NULL CHECK (game_id >= 2025020000 AND game_id < 2025030000),
  player_id bigint NOT NULL,
  expected_attempts integer,
  PRIMARY KEY (game_id, player_id)
) ON COMMIT DROP;
INSERT INTO attempt_2025_unresolved_guard VALUES
{values(unresolved, unresolved=True)};
CREATE TEMP TABLE attempt_2025_changed_evidence (
  game_id bigint NOT NULL, player_id bigint NOT NULL,
  before_attempts integer, after_attempts integer NOT NULL,
  PRIMARY KEY (game_id, player_id)
) ON COMMIT PRESERVE ROWS;
"""
    checks = f"""DO $$
DECLARE target_n bigint; unresolved_n bigint; missing_n bigint; mismatch_n bigint;
BEGIN
  SELECT count(*) INTO target_n FROM attempt_2025_candidates;
  SELECT count(*) INTO unresolved_n FROM attempt_2025_unresolved_guard;
  IF target_n <> {EXPECTED_TARGETS} THEN RAISE EXCEPTION 'TARGET_COUNT_MISMATCH:%', target_n; END IF;
  IF unresolved_n <> {EXPECTED_UNRESOLVED} THEN RAISE EXCEPTION 'UNRESOLVED_GUARD_COUNT_MISMATCH:%', unresolved_n; END IF;
  SELECT count(*) INTO missing_n FROM attempt_2025_candidates c
    LEFT JOIN nhl.skater_game_logs_raw t USING (game_id, player_id) WHERE t.player_id IS NULL;
  IF missing_n <> 0 THEN RAISE EXCEPTION 'TARGET_ROWS_MISSING:%', missing_n; END IF;
  SELECT count(*) INTO missing_n FROM attempt_2025_unresolved_guard c
    LEFT JOIN nhl.skater_game_logs_raw t USING (game_id, player_id) WHERE t.player_id IS NULL;
  IF missing_n <> 0 THEN RAISE EXCEPTION 'UNRESOLVED_ROWS_MISSING:%', missing_n; END IF;
  SELECT count(*) INTO mismatch_n FROM attempt_2025_unresolved_guard c
    JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
    WHERE t.shot_attempts IS DISTINCT FROM c.expected_attempts;
  IF mismatch_n <> 0 THEN RAISE EXCEPTION 'UNRESOLVED_VALUES_CHANGED:%', mismatch_n; END IF;
END $$;
"""
    forward = common + checks + f"""DO $$
DECLARE original_n bigint; corrected_n bigint; unexpected_n bigint; changed_n bigint;
BEGIN
  SELECT count(*) FILTER (WHERE t.shot_attempts IS NOT DISTINCT FROM c.original_attempts),
         count(*) FILTER (WHERE t.shot_attempts = c.official_attempts),
         count(*) FILTER (WHERE t.shot_attempts IS DISTINCT FROM c.original_attempts AND t.shot_attempts IS DISTINCT FROM c.official_attempts)
    INTO original_n, corrected_n, unexpected_n
    FROM attempt_2025_candidates c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id);
  IF unexpected_n <> 0 OR NOT ((original_n = {EXPECTED_TARGETS} AND corrected_n = 0) OR (original_n = 0 AND corrected_n = {EXPECTED_TARGETS})) THEN
    RAISE EXCEPTION 'TARGET_SET_NOT_ALL_ORIGINAL_OR_CORRECTED:original=% corrected=% unexpected=%', original_n, corrected_n, unexpected_n;
  END IF;
  WITH changed AS (
    UPDATE nhl.skater_game_logs_raw t SET shot_attempts = c.official_attempts
      FROM attempt_2025_candidates c
      WHERE t.game_id = c.game_id AND t.player_id = c.player_id
        AND t.shot_attempts IS NOT DISTINCT FROM c.original_attempts
      RETURNING t.game_id, t.player_id
  )
  INSERT INTO attempt_2025_changed_evidence
    SELECT c.game_id, c.player_id, c.original_attempts, c.official_attempts
    FROM changed x JOIN attempt_2025_candidates c USING (game_id, player_id);
  GET DIAGNOSTICS changed_n = ROW_COUNT;
  IF changed_n NOT IN (0, {EXPECTED_TARGETS}) THEN RAISE EXCEPTION 'UNEXPECTED_UPDATED_ROW_COUNT:%', changed_n; END IF;
  IF (original_n = {EXPECTED_TARGETS} AND changed_n <> {EXPECTED_TARGETS}) OR (corrected_n = {EXPECTED_TARGETS} AND changed_n <> 0) THEN
    RAISE EXCEPTION 'IDEMPOTENCY_STATE_CHANGED_DURING_UPDATE:%', changed_n;
  END IF;
  RAISE NOTICE 'NHL_2025_ATTEMPTS_UPDATED_ROWS:%', changed_n;
END $$;
DO $$
DECLARE bad_targets bigint; bad_unresolved bigint;
BEGIN
  SELECT count(*) INTO bad_targets FROM attempt_2025_candidates c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
    WHERE t.shot_attempts IS DISTINCT FROM c.official_attempts;
  IF bad_targets <> 0 THEN RAISE EXCEPTION 'POST_UPDATE_TARGET_MISMATCH:%', bad_targets; END IF;
  SELECT count(*) INTO bad_unresolved FROM attempt_2025_unresolved_guard c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
    WHERE t.shot_attempts IS DISTINCT FROM c.expected_attempts;
  IF bad_unresolved <> 0 THEN RAISE EXCEPTION 'POST_UPDATE_UNRESOLVED_CHANGED:%', bad_unresolved; END IF;
END $$;
COMMIT;
SELECT game_id, player_id, before_attempts, after_attempts FROM attempt_2025_changed_evidence ORDER BY game_id, player_id;
"""
    rollback = common + checks + f"""DO $$
DECLARE original_n bigint; corrected_n bigint; unexpected_n bigint; changed_n bigint;
BEGIN
  SELECT count(*) FILTER (WHERE t.shot_attempts IS NOT DISTINCT FROM c.original_attempts),
         count(*) FILTER (WHERE t.shot_attempts = c.official_attempts),
         count(*) FILTER (WHERE t.shot_attempts IS DISTINCT FROM c.original_attempts AND t.shot_attempts IS DISTINCT FROM c.official_attempts)
    INTO original_n, corrected_n, unexpected_n
    FROM attempt_2025_candidates c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id);
  IF unexpected_n <> 0 OR NOT ((corrected_n = {EXPECTED_TARGETS} AND original_n = 0) OR (original_n = {EXPECTED_TARGETS} AND corrected_n = 0)) THEN
    RAISE EXCEPTION 'ROLLBACK_SET_NOT_ALL_CORRECTED_OR_ORIGINAL:original=% corrected=% unexpected=%', original_n, corrected_n, unexpected_n;
  END IF;
  WITH changed AS (
    UPDATE nhl.skater_game_logs_raw t SET shot_attempts = c.original_attempts
      FROM attempt_2025_candidates c
      WHERE t.game_id = c.game_id AND t.player_id = c.player_id
        AND t.shot_attempts = c.official_attempts
      RETURNING t.game_id, t.player_id
  )
  INSERT INTO attempt_2025_changed_evidence
    SELECT c.game_id, c.player_id, c.official_attempts, c.original_attempts
    FROM changed x JOIN attempt_2025_candidates c USING (game_id, player_id);
  GET DIAGNOSTICS changed_n = ROW_COUNT;
  IF changed_n NOT IN (0, {EXPECTED_TARGETS}) THEN RAISE EXCEPTION 'UNEXPECTED_ROLLBACK_ROW_COUNT:%', changed_n; END IF;
  IF (corrected_n = {EXPECTED_TARGETS} AND changed_n <> {EXPECTED_TARGETS}) OR (original_n = {EXPECTED_TARGETS} AND changed_n <> 0) THEN
    RAISE EXCEPTION 'ROLLBACK_STATE_CHANGED_DURING_UPDATE:%', changed_n;
  END IF;
  RAISE NOTICE 'NHL_2025_ATTEMPTS_ROLLED_BACK_ROWS:%', changed_n;
END $$;
DO $$
DECLARE bad_targets bigint; bad_unresolved bigint;
BEGIN
  SELECT count(*) INTO bad_targets FROM attempt_2025_candidates c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
    WHERE t.shot_attempts IS DISTINCT FROM c.original_attempts;
  IF bad_targets <> 0 THEN RAISE EXCEPTION 'POST_ROLLBACK_TARGET_MISMATCH:%', bad_targets; END IF;
  SELECT count(*) INTO bad_unresolved FROM attempt_2025_unresolved_guard c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
    WHERE t.shot_attempts IS DISTINCT FROM c.expected_attempts;
  IF bad_unresolved <> 0 THEN RAISE EXCEPTION 'POST_ROLLBACK_UNRESOLVED_CHANGED:%', bad_unresolved; END IF;
END $$;
COMMIT;
SELECT game_id, player_id, before_attempts, after_attempts FROM attempt_2025_changed_evidence ORDER BY game_id, player_id;
"""
    return forward, rollback


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recovery-dir", type=Path, default=Path("artifacts/analysis/nhl/points_attempts_2025_full_recovery_2026-10-16"))
    parser.add_argument("--sql-dir", type=Path, default=Path("backend/nhl/sql/repairs"))
    args = parser.parse_args()
    candidate_path = args.recovery_dir / "recovery_candidate.csv"
    snapshot_path = args.recovery_dir / "current_database_snapshot.csv"
    candidate_hash = sha256(candidate_path)
    if sha256(snapshot_path) != CURRENT_SNAPSHOT_SHA256:
        raise SystemExit("CURRENT_SNAPSHOT_HASH_MISMATCH")
    if candidate_hash != "66289cd37dae0b629b18db9c297177a2edd07f87f312d26ad83595fa438dedb7":
        raise SystemExit("CANDIDATE_HASH_MISMATCH")
    targets, unresolved = validate(read_csv(candidate_path), read_csv(snapshot_path))
    forward, rollback = sql_scripts(targets, unresolved, candidate_hash)
    args.sql_dir.mkdir(parents=True, exist_ok=True)
    (args.sql_dir / "repair_nhl_2025_shot_attempts_from_official_pbp.sql").write_text(forward)
    (args.sql_dir / "rollback_nhl_2025_shot_attempts_from_official_pbp.sql").write_text(rollback)
    print(f"targets={len(targets)} unresolved_guard={len(unresolved)} candidate_sha256={candidate_hash}")


if __name__ == "__main__":
    main()
