#!/usr/bin/env python3
"""Build a read-only, season-partitioned NHL SOG denominator certification package.

The utility performs SELECTs only. It writes exclusively beneath --out-dir and
preserves every prediction row, including unresolved and duplicate groups.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row


LEDGER_COLUMNS = [
    "canonical_season", "game_date", "game_id", "home_team", "away_team",
    "player_id", "player_name", "team", "opponent", "market", "line", "side",
    "model_or_strategy", "model_version", "prediction_source", "prediction_source_path",
    "prediction_identity", "prediction_timestamp_utc", "prediction_timestamp_basis",
    "game_start_time_utc", "temporal_status", "participation_status", "official_sog",
    "outcome_source", "outcome_binding_status", "settlement_status",
    "accuracy_denominator_eligible", "accuracy_exclusion_reason", "price", "price_format",
    "sportsbook", "odds_timestamp_utc", "odds_match_policy",
    "market_return_denominator_eligible", "market_return_exclusion_reason",
    "execution_evidence", "duplicate_group_id", "identity_status", "certification_status", "notes",
]


PREDICTION_SQL = """
SELECT
  p.prediction_id, p.player_id, p.game_id, p.prop, p.line, p.p_over,
  p.model_family, COALESCE(p.model_version, '') AS model_version,
  p.feature_hash, p.created_at AS prediction_created_at, p.updated_at AS prediction_updated_at,
  g.season AS canonical_season, g.game_date, g.start_time_utc, g.status AS game_status,
  g.home_team_code, g.away_team_code, g.home_team_id, g.away_team_id,
  pl.full_name AS player_name,
  l.team_id AS outcome_team_id, l.opponent_id AS outcome_opponent_id,
  l.toi_minutes, l.shots_on_goal,
  ht.team AS home_team_name, at.team AS away_team_name,
  ot.team AS outcome_team_name, oo.team AS outcome_opponent_name,
  rs.active_flag AS roster_active_flag, rs.asof_ts AS roster_asof_ts,
  COALESCE(log_counts.n, 0) AS outcome_log_rows
FROM nhl.predictions p
JOIN nhl.games g ON g.game_id = p.game_id
LEFT JOIN nhl.players pl ON pl.player_id = p.player_id
LEFT JOIN (
  SELECT game_id, player_id, MIN(team_id) AS team_id, MIN(opponent_id) AS opponent_id,
         MAX(toi_minutes) AS toi_minutes, MAX(shots_on_goal) AS shots_on_goal
  FROM nhl.skater_game_logs_raw GROUP BY game_id, player_id
) l ON l.game_id = p.game_id AND l.player_id = p.player_id
LEFT JOIN (
  SELECT game_id, player_id, COUNT(*) AS n
  FROM nhl.skater_game_logs_raw GROUP BY game_id, player_id
) log_counts ON log_counts.game_id = p.game_id AND log_counts.player_id = p.player_id
LEFT JOIN nhl.teams ht ON ht.team_id = g.home_team_id
LEFT JOIN nhl.teams at ON at.team_id = g.away_team_id
LEFT JOIN nhl.teams ot ON ot.team_id = l.team_id
LEFT JOIN nhl.teams oo ON oo.team_id = l.opponent_id
LEFT JOIN LATERAL (
  SELECT r.active_flag, r.asof_ts FROM nhl.roster_status r
  WHERE r.game_id = p.game_id AND r.player_id = p.player_id
  ORDER BY r.asof_ts DESC NULLS LAST LIMIT 1
) rs ON TRUE
WHERE p.prop = 'shots_on_goal'
  AND g.game_date BETWEEN %s::date AND %s::date
ORDER BY g.season, g.game_date, p.game_id, p.player_id, p.line,
         p.model_family, COALESCE(p.model_version, ''), p.created_at, p.prediction_id
"""


def _iso(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _bool(value: bool) -> str:
    return "true" if bool(value) else "false"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _fetch_predictions(db_url: str, start_date: str, end_date: str) -> pd.DataFrame:
    with psycopg.connect(db_url, row_factory=dict_row, prepare_threshold=None) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        with conn.cursor() as cur:
            cur.execute(PREDICTION_SQL, (start_date, end_date))
            rows = cur.fetchall()
    if not rows:
        raise RuntimeError("No SOG predictions found in requested bounds")
    return pd.DataFrame(rows)


def _fetch_event_agreement(db_url: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = """
    WITH ev AS (
      SELECT game_id, shooting_player_id AS player_id, COUNT(*)::int AS event_sog
      FROM nhl.shot_on_goal_events GROUP BY game_id, shooting_player_id
    )
    SELECT g.season AS canonical_season, l.game_id, l.player_id,
           l.shots_on_goal AS official_log_sog, ev.event_sog,
           CASE WHEN l.shots_on_goal = ev.event_sog THEN 'AGREE'
                ELSE 'DISAGREE' END AS agreement_status,
           'OFFICIAL_LOG_RETAINED_EVENT_DERIVATION_SUPPORTING_ONLY' AS authority_decision
    FROM nhl.skater_game_logs_raw l
    JOIN nhl.games g ON g.game_id = l.game_id
    JOIN ev ON ev.game_id = l.game_id AND ev.player_id = l.player_id
    WHERE g.game_date BETWEEN %s::date AND %s::date
    ORDER BY g.season, l.game_id, l.player_id
    """
    with psycopg.connect(db_url, row_factory=dict_row, prepare_threshold=None) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        with conn.cursor() as cur:
            cur.execute(sql, (start_date, end_date))
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["canonical_season", "game_id", "player_id", "official_log_sog", "event_sog", "agreement_status", "authority_decision"])


def _load_price_map(odds_root: Path, start_date: str, end_date: str) -> tuple[dict, list[dict]]:
    prices: dict[tuple[int, int, float], dict] = {}
    sources: list[dict] = []
    for fp in sorted(odds_root.glob("*/sog_with_market.csv")):
        slate = fp.parent.name
        if not (start_date <= slate <= end_date):
            continue
        try:
            df = pd.read_csv(fp, low_memory=False)
        except Exception as exc:
            sources.append({"source": str(fp), "rows": 0, "status": f"PARSE_ERROR:{type(exc).__name__}"})
            continue
        needed = {"game_id", "player_id", "line"}
        if not needed.issubset(df.columns):
            sources.append({"source": str(fp), "rows": len(df), "status": "SCHEMA_MISMATCH"})
            continue
        sources.append({"source": str(fp), "rows": len(df), "status": "LOADED"})
        for row in df.to_dict("records"):
            try:
                key = (int(row["game_id"]), int(row["player_id"]), float(row["line"]))
            except Exception:
                continue
            # One file per slate is retained; it has no certified quote timestamp.
            prices[key] = {
                "price_over": row.get("price_over"), "price_under": row.get("price_under"),
                "source": str(fp), "slate": slate,
            }
    return prices, sources


def _settlement(side: str, sog: Any, line: Any) -> str:
    if sog is None or pd.isna(sog):
        return "UNSETTLED_MISSING_OUTCOME"
    s, l = float(sog), float(line)
    if s == l:
        return "PUSH"
    if side == "OVER":
        return "WIN" if s > l else "LOSS"
    return "WIN" if s < l else "LOSS"


def _build_ledger(raw: pd.DataFrame, price_map: dict) -> pd.DataFrame:
    base_keys = []
    exact_keys = []
    for r in raw.to_dict("records"):
        side = "OVER" if float(r["p_over"]) >= 0.5 else "UNDER"
        base = "|".join(map(str, [r["canonical_season"], r["game_id"], r["player_id"],
                                  "shots_on_goal", float(r["line"]), side,
                                  r["model_family"], r["model_version"]]))
        exact = base + "|" + _iso(r["prediction_created_at"]) + "|" + str(r["p_over"])
        base_keys.append(base)
        exact_keys.append(exact)
    base_counts, exact_counts = Counter(base_keys), Counter(exact_keys)

    out = []
    for r, base, exact in zip(raw.to_dict("records"), base_keys, exact_keys):
        side = "OVER" if float(r["p_over"]) >= 0.5 else "UNDER"
        prediction_ts, start_ts = r["prediction_created_at"], r["start_time_utc"]
        if prediction_ts is None or pd.isna(prediction_ts):
            temporal = "PREDICTION_TIMESTAMP_UNKNOWN"
        elif start_ts is None or pd.isna(start_ts):
            temporal = "GAME_START_TIMESTAMP_UNKNOWN"
        elif prediction_ts < start_ts:
            temporal = "PREGAME_BY_DATABASE_CREATED_AT"
        else:
            temporal = "POST_START_OR_BACKFILLED"

        logs = int(r["outcome_log_rows"] or 0)
        toi = r["toi_minutes"]
        if logs > 1:
            participation = "INCOMPLETE_OFFICIAL_RECORD_DUPLICATE"
        elif logs == 0:
            participation = "UNKNOWN"
        elif toi is None or pd.isna(toi):
            participation = "INCOMPLETE_OFFICIAL_RECORD"
        elif float(toi) > 0:
            participation = "PLAYED"
        else:
            participation = "DRESSED_ZERO_RECORDED_ICE_TIME"

        if logs == 1 and r["shots_on_goal"] is not None and not pd.isna(r["shots_on_goal"]):
            outcome_status, outcome_source, official_sog = "BOUND_UNIQUE_OFFICIAL_LOG", "nhl.skater_game_logs_raw", int(r["shots_on_goal"])
        elif logs > 1:
            outcome_status, outcome_source, official_sog = "CONFLICTING_DUPLICATE_OFFICIAL_LOG", "nhl.skater_game_logs_raw", ""
        else:
            outcome_status, outcome_source, official_sog = "NO_OFFICIAL_PLAYER_LOG", "", ""

        identity_issues = []
        for col in ("canonical_season", "game_id", "player_id", "line", "model_family"):
            if r.get(col) is None or pd.isna(r.get(col)) or str(r.get(col)) == "":
                identity_issues.append(f"MISSING_{col.upper()}")
        if not r.get("player_name"):
            identity_issues.append("MISSING_PLAYER_NAME")
        identity_status = "BOUND_AUTHORITATIVE_IDS" if not identity_issues else ";".join(identity_issues)

        duplicate_group = _sha(base)[:20] if base_counts[base] > 1 else ""
        if exact_counts[exact] > 1:
            duplicate_class = "EXACT_DUPLICATE"
        elif base_counts[base] > 1:
            duplicate_class = "DISTINCT_RUN"
        else:
            duplicate_class = "UNIQUE"

        accuracy_reasons = []
        if identity_status != "BOUND_AUTHORITATIVE_IDS": accuracy_reasons.append("IDENTITY_NOT_QUALIFIED")
        if temporal != "PREGAME_BY_DATABASE_CREATED_AT": accuracy_reasons.append("TEMPORAL_NOT_QUALIFIED")
        if participation != "PLAYED": accuracy_reasons.append("PARTICIPATION_NOT_QUALIFIED")
        if outcome_status != "BOUND_UNIQUE_OFFICIAL_LOG": accuracy_reasons.append("OUTCOME_NOT_QUALIFIED")
        if duplicate_class == "EXACT_DUPLICATE": accuracy_reasons.append("EXACT_DUPLICATE")
        accuracy_ok = not accuracy_reasons
        settlement = _settlement(side, official_sog if official_sog != "" else None, r["line"])

        pk = (int(r["game_id"]), int(r["player_id"]), float(r["line"]))
        price_rec = price_map.get(pk)
        price = ""
        if price_rec:
            price = price_rec["price_over"] if side == "OVER" else price_rec["price_under"]
        price_attached = price is not None and not pd.isna(price) and str(price) != ""
        odds_policy = "TIMESTAMP_UNKNOWN" if price_attached else "NO_PRICE"
        market_reasons = list(accuracy_reasons)
        if not price_attached: market_reasons.append("NO_PRICE")
        else: market_reasons.append("ODDS_TIMESTAMP_UNKNOWN")

        cert = "ACCURACY_QUALIFIED" if accuracy_ok else "UNRESOLVED_OR_EXCLUDED"
        notes = f"duplicate_class={duplicate_class};p_over={r['p_over']};game_status={r.get('game_status') or ''}"
        out.append({
            "canonical_season": int(r["canonical_season"]), "game_date": _iso(r["game_date"]),
            "game_id": int(r["game_id"]), "home_team": r.get("home_team_name") or r.get("home_team_code") or "",
            "away_team": r.get("away_team_name") or r.get("away_team_code") or "",
            "player_id": int(r["player_id"]), "player_name": r.get("player_name") or "",
            "team": r.get("outcome_team_name") or "", "opponent": r.get("outcome_opponent_name") or "",
            "market": "shots_on_goal", "line": float(r["line"]), "side": side,
            "model_or_strategy": r["model_family"], "model_version": r["model_version"],
            "prediction_source": "nhl.predictions", "prediction_source_path": "database:nhl.predictions",
            "prediction_identity": _sha(exact), "prediction_timestamp_utc": _iso(prediction_ts),
            "prediction_timestamp_basis": "DATABASE_CREATED_AT_UNCERTIFIED_RUN_TIME",
            "game_start_time_utc": _iso(start_ts), "temporal_status": temporal,
            "participation_status": participation, "official_sog": official_sog,
            "outcome_source": outcome_source, "outcome_binding_status": outcome_status,
            "settlement_status": settlement, "accuracy_denominator_eligible": _bool(accuracy_ok),
            "accuracy_exclusion_reason": ";".join(accuracy_reasons), "price": "" if not price_attached else price,
            "price_format": "AMERICAN" if price_attached else "", "sportsbook": "BETONLINEAG_DERIVED_RECONCILE" if price_attached else "",
            "odds_timestamp_utc": "", "odds_match_policy": odds_policy,
            "market_return_denominator_eligible": "false",
            "market_return_exclusion_reason": ";".join(dict.fromkeys(market_reasons)),
            "execution_evidence": "NONE", "duplicate_group_id": duplicate_group,
            "identity_status": identity_status, "certification_status": cert, "notes": notes,
        })
    return pd.DataFrame(out, columns=LEDGER_COLUMNS)


def _gate_counts(ledger: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for season_label, d in [(str(s), ledger[ledger.canonical_season == s]) for s in sorted(ledger.canonical_season.unique())] + [("ALL", ledger)]:
        exact_dup = d.notes.str.contains("duplicate_class=EXACT_DUPLICATE", regex=False)
        unresolved_dup = d.notes.str.contains("duplicate_class=UNRESOLVED_DUPLICATE", regex=False)
        metrics = {
            "raw_prediction_rows": len(d), "unique_prediction_identities": d.prediction_identity.nunique(),
            "exact_duplicates": int(exact_dup.sum()), "unresolved_duplicates": int(unresolved_dup.sum()),
            "authoritative_game_binding": int(d.game_id.notna().sum()),
            "authoritative_player_binding": int((d.identity_status == "BOUND_AUTHORITATIVE_IDS").sum()),
            "canonical_season_binding": int(d.canonical_season.notna().sum()),
            "temporally_qualified": int((d.temporal_status == "PREGAME_BY_DATABASE_CREATED_AT").sum()),
            "confirmed_participants": int((d.participation_status == "PLAYED").sum()),
            "non_participants": int((d.participation_status == "DID_NOT_PLAY").sum()),
            "unknown_participation": int((d.participation_status == "UNKNOWN").sum()),
            "rows_with_official_sog": int((d.outcome_binding_status == "BOUND_UNIQUE_OFFICIAL_LOG").sum()),
            "pushes": int((d.settlement_status == "PUSH").sum()),
            "accuracy_denominator_rows": int((d.accuracy_denominator_eligible == "true").sum()),
            "price_attached_rows": int(d.price.astype(str).ne("").sum()),
            "pregame_price_certified_rows": int(d.odds_match_policy.str.startswith("EXACT_PREGAME").sum()),
            "market_return_denominator_rows": int((d.market_return_denominator_eligible == "true").sum()),
            "execution_evidenced_rows": int(d.execution_evidence.ne("NONE").sum()),
            "unresolved_rows": int((d.certification_status == "UNRESOLVED_OR_EXCLUDED").sum()),
        }
        rows.extend({"canonical_season": season_label, "metric": k, "count": v} for k, v in metrics.items())
    return pd.DataFrame(rows)


def _write_audits(ledger: pd.DataFrame, event_agreement: pd.DataFrame, out_dir: Path, stamp: str, price_sources: list[dict]) -> None:
    def write(name: str, df: pd.DataFrame) -> None:
        df.to_csv(out_dir / f"{name}_{stamp}.csv", index=False, lineterminator="\n")

    gates = _gate_counts(ledger)
    write("nhl_sog_denominator_gate_counts_by_season", gates)

    dup = ledger[ledger.duplicate_group_id.ne("")][["canonical_season", "game_date", "game_id", "player_id", "line", "side", "model_or_strategy", "model_version", "prediction_timestamp_utc", "prediction_identity", "duplicate_group_id", "notes"]]
    if dup.empty:
        dup = pd.DataFrame(columns=["canonical_season", "game_date", "game_id", "player_id", "line", "side", "model_or_strategy", "model_version", "prediction_timestamp_utc", "prediction_identity", "duplicate_group_id", "notes"])
    write("nhl_sog_identity_and_duplicate_audit", dup)

    write("nhl_sog_outcome_binding_audit", ledger.groupby(["canonical_season", "outcome_binding_status", "outcome_source"], dropna=False).size().reset_index(name="rows"))
    write("nhl_sog_outcome_source_agreement_audit", event_agreement)
    write("nhl_sog_participation_and_settlement_audit", ledger.groupby(["canonical_season", "participation_status", "settlement_status"], dropna=False).size().reset_index(name="rows"))
    write("nhl_sog_price_timestamp_certification", ledger.groupby(["canonical_season", "odds_match_policy", "sportsbook", "market_return_denominator_eligible"], dropna=False).size().reset_index(name="rows"))

    unresolved = ledger[ledger.certification_status != "ACCURACY_QUALIFIED"]
    write("nhl_sog_unresolved_rows", unresolved)

    source_rows = [
        ["database:nhl.games", "game", "2024-01-02..2026-04-16", "2023;2024;2025", "game_id;season", "start_time_utc", "", "", "AUTHORITATIVE_IDENTITY", "production", "season/start missing in historical rows"],
        ["database:nhl.predictions", "model output", "2024-01-02..2026-04-16", "2023;2024;2025", "prediction_id;game_id;player_id;line", "created_at;updated_at", "", "", "AUTHORITATIVE_PREDICTION_POPULATION", "production", "side is derived from p_over>=0.5; created_at may be backfill time"],
        ["database:nhl.skater_game_logs_raw", "player-game", "bounded join", "2023;2024;2025", "game_id;player_id", "created_at", "shots_on_goal;toi_minutes", "", "AUTHORITATIVE_OUTCOME_CANDIDATE", "production", "season 2025 incomplete"],
        ["database:nhl.shot_on_goal_events", "SOG event", "2025-10-07..2026-03-01", "2025", "game_id;shooting_player_id;event_id", "created_at", "derived event count", "", "SUPPORTING_OUTCOME_CROSSCHECK", "derived", "partial reconstruction; disagreements retained; not silent replacement"],
        ["database:nhl.roster_status", "game-player snapshot", "bounded join", "2023;2024;2025", "game_id;player_id", "asof_ts", "active_flag", "", "SUPPORTING_PARTICIPATION", "production", "absence not proof of DNP"],
        ["backend/nhl/exports/odds_history/*/sog_with_market.csv", "player-game-line market join", "season 2025", "2025", "game_id;player_id;line", "directory date only", "", "price_over;price_under", "DERIVED_PRICE_SUPPORT", "derived", "quote timestamp and book provenance uncertified"],
        ["tmp/graded/nhl_sog_graded_*.csv", "graded upload row", "season 2025 subset", "2025", "name;date;line;side", "file/date", "grade", "price;pnl", "REPORTING_ONLY", "reporting", "manual external grader; not official outcome or execution"],
    ]
    pd.DataFrame(source_rows, columns=["source_path_or_table", "grain", "date_coverage", "season_coverage", "identity_fields", "time_fields", "outcome_fields", "price_fields", "authority_level", "usage", "known_limitations"]).to_csv(out_dir / f"nhl_sog_source_authority_inventory_{stamp}.csv", index=False, lineterminator="\n")

    coverage_frames = []
    for dims, label in [(["canonical_season", "model_or_strategy", "model_version"], "model_version"), (["canonical_season", "line", "side"], "line_side"), (["canonical_season", "prediction_source"], "source_family")]:
        x = ledger.groupby(dims, dropna=False).agg(raw_rows=("prediction_identity", "size"), accuracy_rows=("accuracy_denominator_eligible", lambda s: int((s == "true").sum())), official_sog_rows=("outcome_binding_status", lambda s: int((s == "BOUND_UNIQUE_OFFICIAL_LOG").sum()))).reset_index()
        x.insert(0, "coverage_dimension", label)
        coverage_frames.append(x.astype(str))
    monthly = ledger.assign(calendar_month=ledger.game_date.str[:7]).groupby(["canonical_season", "calendar_month"]).agg(raw_rows=("prediction_identity", "size"), accuracy_rows=("accuracy_denominator_eligible", lambda s: int((s == "true").sum())), official_sog_rows=("outcome_binding_status", lambda s: int((s == "BOUND_UNIQUE_OFFICIAL_LOG").sum()))).reset_index().astype(str)
    monthly.insert(0, "coverage_dimension", "calendar_month")
    coverage_frames.append(monthly)
    # Heterogeneous dimension values are serialized in a stable detail JSON column.
    normalized = []
    for frame in coverage_frames:
        for rec in frame.to_dict("records"):
            normalized.append({"coverage_dimension": rec.pop("coverage_dimension"), "detail_json": json.dumps(rec, sort_keys=True)})
    pd.DataFrame(normalized).to_csv(out_dir / f"nhl_sog_coverage_slices_{stamp}.csv", index=False, lineterminator="\n")

    pd.DataFrame(price_sources, columns=["source", "rows", "status"]).to_csv(out_dir / f"nhl_sog_price_source_load_audit_{stamp}.csv", index=False, lineterminator="\n")


def _helper_parity(raw: pd.DataFrame, stamp: str, out_dir: Path) -> None:
    dates = sorted(set(raw.game_date.astype(str)))
    probes = ["2025-06-30", "2025-07-01", "2025-08-15", "2025-09-01", "2025-09-15", "2025-10-07", "2026-01-15", "2026-04-16", "2026-06-30"]
    authority = {str(r.game_date): int(r.canonical_season) for r in raw[["game_date", "canonical_season"]].drop_duplicates().itertuples()}
    rows = []
    for ds in sorted(set(probes + dates)):
        y, m, _ = map(int, ds.split("-"))
        auth = authority.get(ds)
        helpers = {
            "cli_and_shadow_sep_cutoff": y if m >= 9 else y - 1,
            "roster_and_schedule_fallback_jul_cutoff": y if m >= 7 else y - 1,
            "saves_sql_ending_year_expression": y + 1 if m >= 7 else y,
        }
        for helper, inferred in helpers.items():
            status = "NO_AUTHORITATIVE_GAME_FOR_PROBE" if auth is None else ("MATCH" if inferred == auth else "MISMATCH")
            rows.append({"game_date": ds, "authoritative_season": "" if auth is None else auth, "helper": helper, "helper_value": inferred, "status": status, "notes": "authority=nhl.games.season" if auth is not None else "boundary probe only"})
    pd.DataFrame(rows).to_csv(out_dir / f"nhl_season_helper_parity_audit_{stamp}.csv", index=False, lineterminator="\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--odds-root", default="backend/nhl/exports/odds_history")
    ap.add_argument("--stamp", default="2026-07-13")
    args = ap.parse_args()
    if args.start_date > args.end_date:
        raise SystemExit("start date is after end date")
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise SystemExit("SUPABASE_DB_URL is required")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw = _fetch_predictions(db_url, args.start_date, args.end_date)
    event_agreement = _fetch_event_agreement(db_url, args.start_date, args.end_date)
    seasons = set(pd.to_numeric(raw.canonical_season, errors="raise").astype(int))
    if not seasons.issubset({2023, 2024, 2025}):
        raise RuntimeError(f"Unexpected canonical seasons in bounded population: {sorted(seasons)}")
    if raw.game_date.astype(str).min() < args.start_date or raw.game_date.astype(str).max() > args.end_date:
        raise RuntimeError("Date-bound violation")

    price_map, price_sources = _load_price_map(Path(args.odds_root), args.start_date, args.end_date)
    ledger = _build_ledger(raw, price_map)
    ledger_path = out_dir / f"nhl_sog_complete_prediction_qualification_ledger_{args.stamp}.csv"
    ledger.to_csv(ledger_path, index=False, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    _write_audits(ledger, event_agreement, out_dir, args.stamp, price_sources)
    _helper_parity(raw, args.stamp, out_dir)

    metadata = {
        "start_date": args.start_date, "end_date": args.end_date,
        "canonical_seasons": sorted(map(int, seasons)), "raw_rows": len(raw), "ledger_rows": len(ledger),
        "event_crosscheck_rows": len(event_agreement),
        "event_crosscheck_disagreements": int((event_agreement.agreement_status == "DISAGREE").sum()) if not event_agreement.empty else 0,
        "ledger_sha256": hashlib.sha256(ledger_path.read_bytes()).hexdigest(),
        "authority": {"season": "nhl.games.season", "prediction": "nhl.predictions", "outcome": "nhl.skater_game_logs_raw"},
        "fallbacks": {"price": "derived sog_with_market files; timestamp unknown", "participation": "official log presence; roster is supporting only"},
    }
    (out_dir / f"nhl_sog_certification_run_metadata_{args.stamp}.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(metadata, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
