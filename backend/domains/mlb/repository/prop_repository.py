"""MLB prop persistence queries."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from backend.mlb.shared.team_name_map import getFullTeamAbbreviationFromID, normalizeTeamAbbreviation
from backend.shared.db import pg_execute, pg_fetchall, pg_fetchone


ACTIVE_DASHBOARD_PROP_TYPES: Tuple[str, ...] = (
    "hits",
    "total_bases",
    "strikeouts_batting",
    "walks",
    "hits_runs_rbis",
    "runs_scored",
    "rbis",
)

DASHBOARD_POSITIVE_OVER_PROPS: Set[str] = {
    "hits",
    "total_bases",
    "hits_runs_rbis",
    "rbis",
    "runs_scored",
    "walks",
}

DASHBOARD_NEGATIVE_OVER_PROPS: Set[str] = {
    "strikeouts_batting",
}

DASHBOARD_PROP_FAMILIES: Dict[str, str] = {
    "hits": "Contact",
    "total_bases": "Total-base",
    "hits_runs_rbis": "Run-production",
    "rbis": "Run-production",
    "runs_scored": "Run-production",
    "walks": "Plate-discipline",
    "strikeouts_batting": "Contact",
}


def _display_team(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        return normalizeTeamAbbreviation(getFullTeamAbbreviationFromID(int(raw)))
    return normalizeTeamAbbreviation(raw)


def _temperature_flag(prop_type: str, over_hit_flag: int) -> Optional[int]:
    prop = str(prop_type or "").strip()
    if prop in DASHBOARD_POSITIVE_OVER_PROPS:
        return int(over_hit_flag)
    if prop in DASHBOARD_NEGATIVE_OVER_PROPS:
        return 1 - int(over_hit_flag)
    return None


def _format_prop_label(prop_type: str) -> str:
    labels = {
        "hits": "Hits",
        "total_bases": "Total Bases",
        "hits_runs_rbis": "Hits + Runs + RBIs",
        "rbis": "RBIs",
        "runs_scored": "Runs",
        "walks": "Walks",
        "strikeouts_batting": "Batter Ks",
    }
    return labels.get(str(prop_type or "").strip(), str(prop_type or "").replace("_", " ").title())


def _temperature_reason(row: Dict[str, Any]) -> str:
    prop_key = str(row.get("primary_driver_prop") or "").strip()
    prop = _format_prop_label(prop_key)
    count = int(row.get("primary_streak_count") or 0)
    side = str(row.get("streak_side") or "")
    games = "game" if count == 1 else "games"
    if count <= 0:
        return f"{prop} trend"
    if prop_key == "strikeouts_batting":
        if side == "HOT":
            return f"{prop} under in {count} straight {games}"
        return f"{prop} over in {count} straight {games}"
    if prop_key in {"total_bases", "hits_runs_rbis", "walks"}:
        if side == "HOT":
            return f"{prop} over in {count} straight {games}"
        return f"{prop} cold for {count} straight {games}"
    if side == "HOT":
        return f"{prop} in {count} straight {games}"
    return f"{prop} cold for {count} straight {games}"


class DuplicatePropError(Exception):
    """Raised when DB unique constraints indicate a duplicate prop insert."""


_has_user_id_column_cache: Optional[bool] = None
_player_props_columns_cache: Optional[Set[str]] = None


def _player_props_columns() -> Set[str]:
    global _player_props_columns_cache
    if _player_props_columns_cache is not None:
        return _player_props_columns_cache
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='mlb'
          AND table_name='player_props'
    """
    rows = pg_fetchall(sql)
    _player_props_columns_cache = {str(r.get("column_name") or "").strip() for r in rows}
    return _player_props_columns_cache


def _has_user_id_column() -> bool:
    global _has_user_id_column_cache
    if _has_user_id_column_cache is not None:
        return _has_user_id_column_cache
    _has_user_id_column_cache = "user_id" in _player_props_columns()
    return _has_user_id_column_cache


def find_duplicate_prop_id(
    *,
    player_id: int,
    game_id: int,
    prop_type: str,
    over_under: str,
    prop_value: float,
    prop_source: str,
) -> Optional[str]:
    sql = """
        SELECT id
        FROM mlb.player_props
        WHERE CAST(player_id AS TEXT) = %s
          AND CAST(game_id AS TEXT) = %s
          AND prop_type = %s
          AND over_under = %s
          AND prop_value = %s
          AND prop_source = %s
        LIMIT 1
    """
    row = pg_fetchone(
        sql,
        (
            str(player_id),
            str(game_id),
            prop_type,
            over_under,
            prop_value,
            prop_source,
        ),
    )
    if not row:
        return None
    value = row.get("id")
    return str(value) if value is not None else None


def insert_prop_row(
    *,
    player_id: int,
    player_name: Optional[str],
    team: Optional[str],
    team_id: Optional[int],
    game_id: int,
    game_date: str,
    prop_type: str,
    prop_value: float,
    over_under: str,
    prop_source: str,
    recommendation: str,
    probability: float,
    game_type: Optional[str] = None,
    user_id: Optional[str] = None,
) -> None:
    normalized_game_type = str(game_type or "").strip().upper() or None
    columns = [
        "player_id",
        "player_name",
        "team",
        "team_id",
        "game_id",
        "game_date",
        "prop_type",
        "prop_value",
        "over_under",
        "status",
        "prop_source",
        "predicted_outcome",
        "confidence_score",
    ]
    values = [
        str(player_id),
        player_name,
        team,
        int(team_id) if team_id is not None else None,
        str(game_id),
        game_date,
        prop_type,
        prop_value,
        over_under,
        "pending",
        prop_source,
        recommendation,
        probability,
    ]
    placeholders = ["%s"] * len(values)
    columns.extend(["created_at", "prediction_timestamp"])
    placeholders.extend(["NOW()", "NOW()"])

    if "game_type" in _player_props_columns():
        columns.append("game_type")
        placeholders.append("%s")
        values.append(normalized_game_type)

    if user_id and _has_user_id_column():
        columns.append("user_id")
        placeholders.append("%s")
        values.append(str(user_id))

    sql = f"""
        INSERT INTO mlb.player_props ({", ".join(columns)})
        VALUES ({", ".join(placeholders)})
    """
    try:
        pg_execute(
            sql,
            tuple(values),
        )
    except Exception as e:
        # SQLSTATE 23505 = unique_violation (race-safe duplicate handling).
        if getattr(e, "sqlstate", None) == "23505":
            raise DuplicatePropError(str(e)) from e
        raise


def fetch_prop_history_rows(
    *,
    limit: int = 50,
    offset: int = 0,
    user_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = None,
    prop_source_prefix: Optional[str] = None,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    where_sql, params = _build_prop_history_where(
        user_id=user_id,
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        prop_source_prefix=prop_source_prefix,
        status=status,
    )

    columns = [
        "id",
        "player_id",
        "player_name",
        "team",
        "team_id",
        "game_id",
        "game_date",
        "prop_type",
        "prop_value",
        "over_under",
        "status",
        "outcome",
        "prop_source",
        "confidence_score",
        "predicted_outcome",
        "prediction_timestamp",
        "created_at",
    ]
    available = _player_props_columns()
    if "updated_at" in available:
        columns.append("updated_at")
    if "user_id" in available:
        columns.append("user_id")

    sql = f"""
        SELECT
          {", ".join(columns)}
        FROM mlb.player_props
        WHERE {where_sql}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([lim, off])
    return pg_fetchall(sql, tuple(params))


def count_prop_history_rows(
    *,
    user_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = None,
    prop_source_prefix: Optional[str] = None,
    status: Optional[str] = None,
) -> int:
    where_sql, params = _build_prop_history_where(
        user_id=user_id,
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        prop_source_prefix=prop_source_prefix,
        status=status,
    )
    sql = f"""
        SELECT COUNT(*) AS total
        FROM mlb.player_props
        WHERE {where_sql}
    """
    row = pg_fetchone(sql, tuple(params))
    total = (row or {}).get("total", 0)
    try:
        return int(total)
    except Exception:
        return 0


def fetch_model_training_prop_history_rows(
    *,
    limit: int = 50,
    offset: int = 0,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = "mlb_api",
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Read current model-backed MLB prop history for dashboard context.

    This intentionally does not read mlb.player_props, which is the legacy
    user-added/current-prop table and is no longer refreshed for broad MLB data.
    """
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    where_sql, params = _build_model_training_history_where(
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        status=status,
    )
    sql = f"""
        SELECT
          CAST(id AS TEXT) AS id,
          player_id,
          player_name,
          team,
          team_id,
          game_id,
          game_date,
          prop_type,
          COALESCE(prop_value, line) AS prop_value,
          over_under,
          status,
          outcome,
          prop_source,
          confidence_score,
          predicted_outcome,
          prediction_timestamp,
          created_at,
          updated_at
        FROM mlb.model_training_props
        WHERE {where_sql}
        ORDER BY game_date DESC NULLS LAST, created_at DESC NULLS LAST
        LIMIT %s OFFSET %s
    """
    params.extend([lim, off])
    return pg_fetchall(sql, tuple(params))


def count_model_training_prop_history_rows(
    *,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = "mlb_api",
    status: Optional[str] = None,
) -> int:
    where_sql, params = _build_model_training_history_where(
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        status=status,
    )
    row = pg_fetchone(
        f"""
        SELECT COUNT(*) AS total
        FROM mlb.model_training_props
        WHERE {where_sql}
        """,
        tuple(params),
    )
    total = (row or {}).get("total", 0)
    try:
        return int(total)
    except Exception:
        return 0


def fetch_streak_dashboard_rows(
    *,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = "mlb_api",
    limit_per_side: int = 5,
) -> List[Dict[str, Any]]:
    """Return dashboard-ready player-level streak signals from current MLB data."""
    limit = max(1, min(int(limit_per_side), 20))
    source = str(prop_source or "mlb_api").strip() or "mlb_api"
    where = [
        "m.player_id IS NOT NULL",
        "m.prop_type IS NOT NULL",
        "m.game_id IS NOT NULL",
        "m.game_date IS NOT NULL",
        "m.prop_source = %s",
        "m.prop_type = ANY(%s::text[])",
        "lower(trim(coalesce(m.outcome, ''))) IN ('win', 'loss')",
        "lower(trim(coalesce(m.over_under, ''))) IN ('over', 'under')",
    ]
    params: List[Any] = [source, list(ACTIVE_DASHBOARD_PROP_TYPES)]
    if from_date:
        where.append("m.game_date >= %s::date")
        params.append(from_date)
    else:
        where.append("m.game_date >= current_date - interval '14 days'")
    if to_date:
        where.append("m.game_date < (%s::date + interval '1 day')")
        params.append(to_date)

    where_sql = " AND ".join(where)
    sql = f"""
        SELECT
          m.player_id::bigint AS player_id,
          nullif(trim(m.player_name), '') AS player_name,
          nullif(trim(m.team), '') AS team,
          m.prop_type,
          m.game_id::bigint AS game_id,
          m.game_date::date AS game_date,
          CASE
            WHEN m.over_under = 'over' AND m.outcome = 'win' THEN 1
            WHEN m.over_under = 'over' AND m.outcome = 'loss' THEN 0
            WHEN m.over_under = 'under' AND m.outcome = 'win' THEN 0
            WHEN m.over_under = 'under' AND m.outcome = 'loss' THEN 1
            ELSE NULL
          END AS over_hit_flag
        FROM mlb.model_training_props m
        WHERE {where_sql}
        ORDER BY m.player_id, m.prop_type, m.game_date DESC, m.game_id DESC
    """
    source_rows = pg_fetchall(sql, tuple(params))

    prop_flags: Dict[str, List[int]] = {}
    player_prop: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in source_rows:
        flag = row.get("over_hit_flag")
        if flag not in (0, 1):
            continue
        prop_type = str(row.get("prop_type") or "").strip()
        temp_flag = _temperature_flag(prop_type, int(flag))
        if temp_flag is None:
            continue
        game_date = row.get("game_date")
        player_id = str(row.get("player_id") or "").strip()
        if not player_id or not prop_type:
            continue
        key = (player_id, prop_type)
        prop_flags.setdefault(prop_type, []).append(temp_flag)
        current = player_prop.get(key)
        if current is None:
            current = {
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name"),
                "team": _display_team(row.get("team")),
                "prop_type": prop_type,
                "family": DASHBOARD_PROP_FAMILIES.get(prop_type, "Player"),
                "sample": 0,
                "positive_count": 0,
                "latest_temp_flag": temp_flag,
                "current_streak_count": 0,
                "streak_broken": False,
                "latest_game_date": None,
            }
            player_prop[key] = current
        current["sample"] += 1
        current["positive_count"] += temp_flag
        if not current["streak_broken"]:
            if int(temp_flag) == int(current["latest_temp_flag"]):
                current["current_streak_count"] += 1
            else:
                current["streak_broken"] = True
        if game_date is not None and (current["latest_game_date"] is None or game_date > current["latest_game_date"]):
            current["latest_game_date"] = game_date

    prop_baselines: Dict[str, float] = {}
    for prop_type, flags in prop_flags.items():
        if flags:
            prop_baselines[prop_type] = sum(flags) / len(flags)

    per_player: Dict[str, Dict[str, Any]] = {}
    for item in player_prop.values():
        sample = int(item.get("sample") or 0)
        if sample < 2:
            continue
        prop_type = str(item.get("prop_type") or "")
        baseline = prop_baselines.get(prop_type)
        if baseline is None:
            continue
        rate = float(item.get("positive_count") or 0) / sample
        delta = rate - baseline
        weighted_delta = delta * min(2.0, sample ** 0.5)
        player_id = str(item.get("player_id") or "").strip()
        bucket = per_player.setdefault(
            player_id,
            {
                "player_id": item.get("player_id"),
                "player_name": item.get("player_name"),
                "team": item.get("team"),
                "latest_game_date": item.get("latest_game_date"),
                "prop_scores": [],
            },
        )
        if item.get("latest_game_date") is not None and (
            bucket.get("latest_game_date") is None or item.get("latest_game_date") > bucket.get("latest_game_date")
        ):
            bucket["latest_game_date"] = item.get("latest_game_date")
        bucket["prop_scores"].append(
            {
                "prop_type": prop_type,
                "family": item.get("family"),
                "sample": sample,
                "rate": rate,
                "baseline": baseline,
                "delta": delta,
                "weighted_delta": weighted_delta,
                "current_streak_count": int(item.get("current_streak_count") or 0),
            }
        )

    scored: List[Dict[str, Any]] = []
    for bucket in per_player.values():
        prop_scores = bucket.get("prop_scores") or []
        if not prop_scores:
            continue
        total_delta = sum(float(p.get("weighted_delta") or 0) for p in prop_scores)
        average_delta = total_delta / max(1, len(prop_scores))
        aligned_positive = [p for p in prop_scores if float(p.get("delta") or 0) >= 0.10]
        aligned_negative = [p for p in prop_scores if float(p.get("delta") or 0) <= -0.10]
        if abs(average_delta) < 0.08:
            continue
        side = "HOT" if average_delta > 0 else "COLD"
        aligned = aligned_positive if side == "HOT" else aligned_negative
        conflicts = aligned_negative if side == "HOT" else aligned_positive
        if not aligned:
            continue
        primary = max(aligned, key=lambda p: abs(float(p.get("weighted_delta") or 0)))
        supporting = len(aligned)
        conflict_count = len(conflicts)
        score = round(max(0, min(100, 50 + abs(average_delta) * 45 + supporting * 4 - conflict_count * 6)), 1)
        sample_rows = sum(int(p.get("sample") or 0) for p in prop_scores)
        temperature_row = {
            **bucket,
            "streak_side": side,
            "player_temperature_score": score,
            "streak_quality_score": score,
            "primary_driver_prop": primary.get("prop_type"),
            "primary_prop": primary.get("prop_type"),
            "primary_family": primary.get("family"),
                "primary_streak_count": int(primary.get("current_streak_count") or 0),
            "supporting_prop_count": supporting,
            "conflict_count": conflict_count,
            "sample_rows": sample_rows,
            "primary_delta": round(float(primary.get("delta") or 0), 3),
        }
        temperature_row["reason"] = _temperature_reason(temperature_row)
        scored.append(
            temperature_row
        )

    best_by_player: Dict[str, Dict[str, Any]] = {}
    for row in scored:
        player_id = str(row.get("player_id") or "")
        existing = best_by_player.get(player_id)
        if existing is None or (
            float(row.get("player_temperature_score") or 0),
            int(row.get("sample_rows") or 0),
            row.get("latest_game_date"),
        ) > (
            float(existing.get("player_temperature_score") or 0),
            int(existing.get("sample_rows") or 0),
            existing.get("latest_game_date"),
        ):
            best_by_player[player_id] = row

    def sort_key(row: Dict[str, Any]) -> Tuple[float, int, Any, str]:
        return (
            float(row.get("player_temperature_score") or 0),
            int(row.get("supporting_prop_count") or 0),
            row.get("latest_game_date"),
            str(row.get("player_name") or ""),
        )

    hot = sorted(
        [r for r in best_by_player.values() if r.get("streak_side") == "HOT"],
        key=sort_key,
        reverse=True,
    )[:limit]
    cold = sorted(
        [r for r in best_by_player.values() if r.get("streak_side") == "COLD"],
        key=sort_key,
        reverse=True,
    )[:limit]
    return hot + cold


def _build_prop_history_where(
    *,
    user_id: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    prop_source: Optional[str],
    prop_source_prefix: Optional[str],
    status: Optional[str],
) -> Tuple[str, List[Any]]:
    where = ["1=1"]
    params: List[Any] = []

    if from_date:
        where.append("game_date >= %s")
        params.append(from_date)
    if to_date:
        where.append("game_date <= %s")
        params.append(to_date)
    if prop_source:
        where.append("prop_source = %s")
        params.append(prop_source)
    elif prop_source_prefix:
        where.append("prop_source LIKE %s")
        params.append(f"{str(prop_source_prefix).strip()}%")
    if status:
        where.append(
            """
            (
              CASE
                WHEN LOWER(COALESCE(outcome, '')) IN ('win', 'loss', 'push')
                  THEN LOWER(outcome)
                ELSE LOWER(COALESCE(status, 'pending'))
              END
            ) = %s
            """
        )
        params.append(str(status).strip().lower())
    if user_id and _has_user_id_column():
        where.append("CAST(user_id AS TEXT) = %s")
        params.append(str(user_id))
    return " AND ".join(where), params


def _build_model_training_history_where(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    prop_source: Optional[str],
    status: Optional[str],
) -> Tuple[str, List[Any]]:
    where = ["1=1"]
    params: List[Any] = []

    if from_date:
        where.append("game_date >= %s")
        params.append(from_date)
    if to_date:
        where.append("game_date <= %s")
        params.append(to_date)
    if prop_source:
        where.append("prop_source = %s")
        params.append(prop_source)
    if status:
        where.append(
            """
            (
              CASE
                WHEN LOWER(COALESCE(outcome, '')) IN ('win', 'loss', 'push')
                  THEN LOWER(outcome)
                ELSE LOWER(COALESCE(status, 'pending'))
              END
            ) = %s
            """
        )
        params.append(str(status).strip().lower())
    return " AND ".join(where), params
