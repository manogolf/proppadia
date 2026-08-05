"""Postgres durability boundary for public MLB moneyline predictions."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any, Iterable

from backend.app.deps import pg_connect

from .pythagorean_log5_v1 import MODEL_VERSION, PublicGamePredictionError
from .state_v1 import OfficialFinalGame


def _value(row: Any, key: str, index: int) -> Any:
    return row.get(key) if isinstance(row, dict) else row[index]


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def canonical_payload_hash(row: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":"),
                                     default=_json_default).encode()).hexdigest()


def load_official_finals_before(cutoff_utc: str) -> list[OfficialFinalGame]:
    sql = """
      SELECT f.game_pk, f.game_date::text, f.scheduled_start_utc, f.game_number,
             f.home_team_id, f.away_team_id, COALESCE(c.corrected_home_runs,f.home_runs) AS home_runs,
             COALESCE(c.corrected_away_runs,f.away_runs) AS away_runs, f.official_status,
             f.official_final_effective_utc, f.observed_final_at_utc,
             COALESCE(c.source_identity,f.source_identity) AS source_identity,
             COALESCE(c.source_sha256,f.source_sha256) AS source_sha256
      FROM mlb.public_game_official_finals f
      LEFT JOIN LATERAL (
        SELECT * FROM mlb.public_game_official_final_corrections x
        WHERE x.game_pk=f.game_pk AND x.observed_at_utc < %s::timestamptz
        ORDER BY x.observed_at_utc DESC,x.correction_id DESC LIMIT 1
      ) c ON true
      WHERE f.official_final_effective_utc <= %s::timestamptz
      ORDER BY f.game_date, f.scheduled_start_utc, f.game_number, f.game_pk
    """
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(sql, (cutoff_utc,cutoff_utc))
        return [OfficialFinalGame(
            game_pk=int(_value(r,'game_pk',0)), game_date=str(_value(r,'game_date',1)),
            scheduled_start_utc=_value(r,'scheduled_start_utc',2).isoformat(),
            game_number=int(_value(r,'game_number',3)), home_team_id=int(_value(r,'home_team_id',4)),
            away_team_id=int(_value(r,'away_team_id',5)), home_runs=int(_value(r,'home_runs',6)),
            away_runs=int(_value(r,'away_runs',7)), official_status=str(_value(r,'official_status',8)),
            official_final_effective_utc=_value(r,'official_final_effective_utc',9).isoformat(),
            observed_final_at_utc=_value(r,'observed_final_at_utc',10).isoformat(),
            source_identity=str(_value(r,'source_identity',11)), source_sha256=str(_value(r,'source_sha256',12)),
        ) for r in cur.fetchall()]


def append_official_finals(rows: Iterable[OfficialFinalGame]) -> int:
    inserted = 0
    with pg_connect() as conn, conn.cursor() as cur:
        for row in rows:
            cur.execute("""
              INSERT INTO mlb.public_game_official_finals
              (game_pk,game_date,scheduled_start_utc,game_number,home_team_id,away_team_id,
               home_runs,away_runs,official_status,official_final_effective_utc,observed_final_at_utc,
               source_identity,source_sha256,content_sha256)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
              ON CONFLICT (game_pk) DO NOTHING
              RETURNING game_pk
            """, (row.game_pk,row.game_date,row.scheduled_start_utc,row.game_number,row.home_team_id,
                  row.away_team_id,row.home_runs,row.away_runs,row.official_status,row.official_final_effective_utc,row.observed_final_at_utc,
                  row.source_identity,row.source_sha256,row.content_hash))
            if cur.fetchone():
                inserted += 1
            else:
                cur.execute("SELECT content_sha256 FROM mlb.public_game_official_finals WHERE game_pk=%s",(row.game_pk,))
                existing=cur.fetchone()
                if not existing or _value(existing,'content_sha256',0) != row.content_hash:
                    raise PublicGamePredictionError(f"OFFICIAL_FINAL_CORRECTION_REQUIRES_REPLAY:{row.game_pk}")
        conn.commit()
    return inserted


def append_state_snapshot(snapshot: dict[str, Any]) -> bool:
    payload_hash = canonical_payload_hash(snapshot)
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("""
          INSERT INTO mlb.public_game_team_state_snapshots
          (model_version,prediction_cutoff_utc,state_through_game_date,state_hash,state_generated_at_utc,
           state_payload,payload_sha256)
          VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s)
          ON CONFLICT (model_version,prediction_cutoff_utc) DO NOTHING RETURNING state_hash
        """, (MODEL_VERSION,snapshot['prediction_cutoff_utc'],snapshot['state_through_game_date'],
              snapshot['state_hash'],snapshot['state_generated_at_utc'],json.dumps(snapshot),payload_hash))
        inserted=cur.fetchone() is not None
        if not inserted:
            cur.execute("SELECT state_hash FROM mlb.public_game_team_state_snapshots WHERE model_version=%s AND prediction_cutoff_utc=%s",(MODEL_VERSION,snapshot['prediction_cutoff_utc']))
            existing=cur.fetchone()
            if not existing or _value(existing,'state_hash',0)!=snapshot['state_hash']:
                raise PublicGamePredictionError("IMMUTABLE_STATE_SNAPSHOT_CONFLICT")
        conn.commit()
    return inserted


def append_prediction_rows(rows: Iterable[dict[str, Any]]) -> int:
    inserted=0
    with pg_connect() as conn, conn.cursor() as cur:
        for row in rows:
            if row.get('admission_status') != 'ADMITTED_SHADOW':
                continue
            payload_hash=canonical_payload_hash(row)
            cur.execute("""
              INSERT INTO mlb.public_game_moneyline_predictions
              (game_date,game_id,model_version,prediction_snapshot_class,scheduled_start_utc,
               prediction_timestamp_utc,prediction_cutoff_utc,home_team,away_team,home_win_probability,
               away_win_probability,predicted_winner,confidence_band,data_quality_status,model_hash,
               scorer_hash,source_schedule_hash,team_state_hash,admission_status,failure_reason,
               prediction_payload,payload_sha256)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
              ON CONFLICT (game_date,game_id,model_version,prediction_snapshot_class) DO NOTHING
              RETURNING game_id
            """, (row['game_date'],row['game_id'],row['winner_model_version'],row['prediction_snapshot_class'],
                  row['scheduled_start_utc'],row['prediction_timestamp_utc'],row['prediction_cutoff_utc'],
                  row['home_team'],row['away_team'],row['home_win_probability'],row['away_win_probability'],
                  row['predicted_winner'],row['confidence_band'],row['data_quality_status'],row['winner_model_hash'],
                  row['scorer_hash'],row['source_schedule_hash'],row['team_state_hash'],row['admission_status'],
                  row.get('failure_reason'),json.dumps(row,default=_json_default),payload_hash))
            if cur.fetchone(): inserted+=1
            else:
                cur.execute("SELECT payload_sha256 FROM mlb.public_game_moneyline_predictions WHERE game_date=%s AND game_id=%s AND model_version=%s AND prediction_snapshot_class=%s",(row['game_date'],row['game_id'],row['winner_model_version'],row['prediction_snapshot_class']))
                existing=cur.fetchone()
                if not existing or _value(existing,'payload_sha256',0)!=payload_hash:
                    raise PublicGamePredictionError(f"IMMUTABLE_PREDICTION_CONFLICT:{row['game_id']}")
        conn.commit()
    return inserted


def fetch_prediction_rows(game_date: str) -> list[dict[str, Any]]:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT prediction_payload FROM mlb.public_game_moneyline_predictions
                       WHERE game_date=%s AND model_version=%s AND admission_status='ADMITTED_SHADOW'
                       ORDER BY scheduled_start_utc,game_id""",(game_date,MODEL_VERSION))
        return [_value(r,'prediction_payload',0) for r in cur.fetchall()]


def append_outcome_grade(grade: dict[str, Any]) -> bool:
    if grade.get('official_status')!='Final':
        raise PublicGamePredictionError('GRADING_REQUIRES_OFFICIAL_FINAL')
    payload_hash=canonical_payload_hash(grade)
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("""
          INSERT INTO mlb.public_game_moneyline_outcomes
          (game_date,game_id,model_version,prediction_snapshot_class,official_home_runs,
           official_away_runs,official_winner,prediction_correct,observed_outcome_probability,
           brier_contribution,log_loss_contribution,confidence_band,official_source_identity,
           official_source_sha256,grading_timestamp_utc,outcome_payload,payload_sha256)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
          ON CONFLICT (game_date,game_id,model_version,prediction_snapshot_class) DO NOTHING
          RETURNING game_id
        """,(grade['game_date'],grade['game_id'],grade['winner_model_version'],grade['prediction_snapshot_class'],
             grade['official_home_runs'],grade['official_away_runs'],grade['official_winner'],grade['prediction_correct'],
             grade['observed_outcome_probability'],grade['brier_contribution'],grade['log_loss_contribution'],
             grade['confidence_band'],grade['official_source_path'],grade['official_source_sha256'],
             grade['grading_timestamp_utc'],json.dumps(grade,default=_json_default),payload_hash))
        inserted=cur.fetchone() is not None
        if not inserted:
            cur.execute("SELECT payload_sha256 FROM mlb.public_game_moneyline_outcomes WHERE game_date=%s AND game_id=%s AND model_version=%s AND prediction_snapshot_class=%s",(grade['game_date'],grade['game_id'],grade['winner_model_version'],grade['prediction_snapshot_class']))
            existing=cur.fetchone()
            if not existing or _value(existing,'payload_sha256',0)!=payload_hash:
                raise PublicGamePredictionError(f"OUTCOME_CORRECTION_REQUIRES_HISTORY:{grade['game_id']}")
        conn.commit()
    return inserted
