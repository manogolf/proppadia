"""BvP identity boundary; no feature calculations, HTTP calls or database writes."""
from __future__ import annotations

import gzip
import hashlib
import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

CONTRACT = "BVP_CANONICAL_SLATE_IDENTITY_V1"
PT = ZoneInfo("America/Los_Angeles")
ROOT = Path(__file__).resolve().parents[3]
FORWARD_IDENTITY_EFFECTIVE_UTC = datetime(2026, 9, 18, 17, 41, 34, tzinfo=timezone.utc)
EXCLUSION_PATH = ROOT / "artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_identity_exclusions_v1.json"
EXCLUSION_RECEIPT_SHA256 = "cb5d014c8ce9b82031fed3dce297d3b65d45f233a1d8fec05e56dd7dd1b69622"


class CanonicalSlateIdentityError(RuntimeError):
    pass


def stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def utc_time(value: Any) -> datetime:
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_NAIVE_START")
    return stamp.astimezone(timezone.utc)


def game_rejection(game: Any, requested_date: str) -> str | None:
    """Validate independent source fields, never a date assigned by this collector."""
    if (game.game_date != requested_date or not game.official_date
            or not game.scheduled_start_utc or not game.schedule_date):
        raise CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_SOURCE_AUTHORITY")
    try:
        start = utc_time(game.scheduled_start_utc)
        datetime.strptime(game.official_date, "%Y-%m-%d")
        datetime.strptime(game.schedule_date, "%Y-%m-%d")
    except (ValueError, TypeError) as exc:
        raise CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_SOURCE_AUTHORITY") from exc
    if (game.official_date != requested_date or game.schedule_date != requested_date
            or start.astimezone(PT).date().isoformat() != requested_date):
        return "OFF_DATE_GAME_REJECTED"
    if (game.game_id != game.official_game_id
            or any(isinstance(v, bool) or not isinstance(v, int) or v <= 0
                   for v in (game.game_id, game.home_team_id, game.away_team_id))):
        raise CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_OFFICIAL_ID")
    if game.home_team_id == game.away_team_id:
        raise CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_TEAM_PAIR")
    if any(token in game.game_state.lower() for token in ("postpon", "cancel", "suspend")):
        return "CANONICAL_IDENTITY_UNRESOLVED"
    return None


def validate_slate(games: Sequence[Any], requested_date: str) -> tuple[list[Any], list[tuple[Any, str]]]:
    valid, rejected = [], []
    seen: set[int] = set()
    for game in games:
        if game.official_game_id in seen:
            raise CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_DUPLICATE_GAME")
        seen.add(game.official_game_id)
        reason = game_rejection(game, requested_date)
        (rejected if reason else valid).append((game, reason) if reason else game)
    return valid, rejected


def filter_prepared_rows(rows: Sequence[tuple], games: Sequence[Any], requested_date: str) -> tuple[list[tuple], list[dict]]:
    """Second boundary immediately before admission; foreign/changed IDs never pass."""
    authority = {g.official_game_id: g for g in games}
    valid, rejected, seen = [], [], set()
    for row in rows:
        prop, batter, game_id, slate, features, feature_tag, model_tag = row
        game = authority.get(game_id)
        reason = "OFF_DATE_GAME_REJECTED" if slate != requested_date else (
            "CANONICAL_IDENTITY_UNRESOLVED" if game is None else game_rejection(game, requested_date))
        if reason:
            rejected.append({"game_id": game_id, "batter_id": batter, "prop_type": prop, "reason_code": reason})
            continue
        if not prop or not feature_tag or not model_tag or isinstance(batter, bool) or int(batter) <= 0:
            raise CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_ROW_KEY")
        key = (prop, batter, game_id, feature_tag)
        if key in seen:
            raise CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_DUPLICATE_ROW")
        seen.add(key)
        valid.append(row)
    return valid, rejected


@lru_cache(maxsize=1)
def archived_game_dates() -> dict[int, set[str]]:
    """Retained official archives, not unsafe game_info.game_date enrichment."""
    dates: dict[int, set[str]] = {}
    for path in sorted((ROOT / "artifacts/raw/mlb/totals_feature_spine_v1/schedule").glob("*.json.gz")):
        payload = json.loads(gzip.decompress(path.read_bytes()))
        for day in payload.get("dates", []):
            for game in day.get("games", []):
                if game.get("gamePk") and game.get("officialDate"):
                    dates.setdefault(int(game["gamePk"]), set()).add(game["officialDate"])
    return dates


def retained_authority(rows: Sequence[Mapping]) -> dict[int, set[str]]:
    from backend.shared.db.pg import pg_fetchall
    dates = {k: set(v) for k, v in archived_game_dates().items()}
    ids = sorted({int(r["game_id"]) for r in rows})
    if ids:
        for row in pg_fetchall("""
            SELECT DISTINCT game_id, game_date::text AS game_date
            FROM mlb.public_game_moneyline_predictions
            WHERE game_id = ANY(%s)
              AND model_version = 'MLB_GAME_PYTHAGOREAN_LOG5_V1'
              AND prediction_snapshot_class = 'DESIGNATED_DAILY_PUBLIC_SNAPSHOT'
              AND admission_status = 'ADMITTED_SHADOW'
        """, (ids,)):
            dates.setdefault(int(row["game_id"]), set()).add(row["game_date"])
    for row in rows:
        if row.get("computed_at") and utc_time(row["computed_at"]) >= FORWARD_IDENTITY_EFFECTIVE_UTC and forward_source_identity_valid(row):
            dates.setdefault(int(row["game_id"]),set()).add(str(row["game_date"])[:10])
    return dates


@lru_cache(maxsize=1)
def exclusion_receipt() -> dict:
    content = EXCLUSION_PATH.read_bytes()
    if hashlib.sha256(content).hexdigest() != EXCLUSION_RECEIPT_SHA256:
        raise CanonicalSlateIdentityError("BVP_EXCLUSION_RECEIPT_HASH_MISMATCH")
    receipt = json.loads(content)
    if receipt["contract"] != CONTRACT or receipt["excluded_row_count"] != 91 or len(receipt["excluded_rows"]) != 91:
        raise CanonicalSlateIdentityError("INVALID_BVP_EXCLUSION_RECEIPT")
    return receipt


@lru_cache(maxsize=32)
def _read_journals(signature: tuple[tuple[str, int], ...]) -> list[list[dict]]:
    # Size is a cache invalidation key, never an authoritative observation time.
    return [[json.loads(line) for line in Path(path).read_text().splitlines()]
            for path, _ in signature]


def forward_source_identity_valid(row: Mapping, journals: Sequence[Sequence[Mapping]] | None = None) -> bool:
    """New captures need a committed request/pitcher receipt, not date alone."""
    if not row.get("computed_at"):
        return False
    computed = utc_time(row["computed_at"])
    if computed < FORWARD_IDENTITY_EFFECTIVE_UTC:
        return True  # Legacy game-identity eligibility only, NOT pitcher proof.
    if journals is None:
        folder = ROOT / "artifacts/ops/bvp_identity_v1" / str(row["game_date"])[:10]
        signature = tuple((str(p), p.stat().st_size) for p in sorted(folder.glob("*.jsonl")))
        journals = _read_journals(signature)
    features_hash = stable_hash({k:v for k,v in (row.get("features") or {}).items() if str(k).startswith("bvp_")})
    for events in journals:
        commits = [e for e in events if e.get("reason_code") == "DATABASE_WRITE_COMMITTED"]
        if not commits:
            continue
        for event in events:
            if (event.get("contract") != CONTRACT or event.get("reason_code") != "BVP_RESPONSE_NONEMPTY"
                    or event.get("game_id") != row["game_id"] or event.get("official_game_id") != row["game_id"]
                    or event.get("batter_id") != row["player_id"]
                    or event.get("feature_set_tag") != row["feature_set_tag"]
                    or event.get("model_tag") != row.get("model_tag")
                    or event.get("feature_payload_sha256") != features_hash
                    or not event.get("successful_response")
                    or event.get("pitcher_id") is None or event["pitcher_id"] <= 0):
                continue
            start = utc_time(event["scheduled_start_utc"])
            observed = utc_time(event["response_observed_at_utc"])
            day = str(row["game_date"])[:10]
            if (event.get("official_date") != day or event.get("schedule_date") != day
                    or event.get("slate_date") != day or start.astimezone(PT).date().isoformat() != day
                    or event.get("team_id") not in (event.get("home_team_id"),event.get("away_team_id"))
                    or event.get("opponent_team_id") not in (event.get("home_team_id"),event.get("away_team_id"))
                    or event.get("team_id") == event.get("opponent_team_id")):
                continue
            if observed <= computed < start and any(computed <= utc_time(c["acquisition_timestamp_utc"]) for c in commits):
                return True
    return False


def certified_rows(rows: Sequence[Mapping], *, authority: Mapping[int, set[str]] | None = None,
                   exclusions: Sequence[Mapping] | None = None) -> list[Mapping]:
    """Legacy identity certification, not proof of original pitcher/as-of lineage.

    Unknown/ambiguous source identity fails closed. Never bless raw game_date.
    Only BvP-bearing rows are gated; unrelated rolling payloads remain unchanged.
    """
    bvp_rows = [r for r in rows if any(str(k).startswith("bvp_") for k in (r.get("features") or {}))]
    if not bvp_rows:
        return list(rows)
    dates = retained_authority(bvp_rows) if authority is None else authority
    excluded = exclusion_receipt()["excluded_rows"] if exclusions is None else exclusions
    banned = {(r["key"]["prop_type"], r["key"]["player_id"], r["key"]["game_id"], r["key"]["feature_set_tag"]): r for r in excluded}
    output = []
    for row in rows:
        if not any(str(k).startswith("bvp_") for k in (row.get("features") or {})):
            output.append(row)
            continue
        key = (row.get("prop_type"), row.get("player_id"), row.get("game_id"), row.get("feature_set_tag"))
        old = banned.get(key)
        if old and (not row.get("computed_at") or utc_time(row["computed_at"]) == utc_time(old["computed_at"])):
            continue
        if (dates.get(int(row["game_id"]), set()) == {str(row.get("game_date"))[:10]}
                and forward_source_identity_valid(row)):
            output.append(row)
    return output


def validate_exclusion_set(rows: Sequence[Mapping], receipt: Mapping) -> dict:
    """No-write proof of retained bytes and exact exclusion-set membership."""
    if len(rows) != 1937 or stable_hash(list(rows)) != receipt["all_1937_row_stream_sha256"]:
        raise CanonicalSlateIdentityError("RETAINED_BVP_ROW_STREAM_CHANGED")
    mismatches = [r for r in rows if r["game_id"] == receipt["retained_game_id"]]
    actual = {stable_hash(r) for r in mismatches}
    expected = {r["row_sha256"] for r in receipt["excluded_rows"]}
    if len(mismatches) != 91 or len(expected) != 91 or actual != expected:
        raise CanonicalSlateIdentityError("BVP_EXCLUSION_SET_MISMATCH")
    return {"status": "PASS", "retained_rows": 1937, "excluded_rows": 91, "remaining_identity_eligible_rows": 1846}
