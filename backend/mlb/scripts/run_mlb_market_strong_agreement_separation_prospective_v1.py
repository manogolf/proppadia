#!/usr/bin/env python3
"""Locked prospective MLB market-strong/model-agreement separation study v1."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from backend.app.deps import pg_connect
from backend.mlb.scripts import acquire_and_audit_mlb_oddsapi_historical_joint_strength_transfer_v1 as hardened


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_market_strong_agreement_separation_prospective_v1/2026-09-09"
LEDGER = ROOT / "backend/mlb/exports/model_v2/mlb_market_strong_agreement_separation_prospective_v1.sqlite3"
MODEL_CONFIG = ROOT / "backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json"
MODEL = "MLB_GAME_PYTHAGOREAN_LOG5_V1"
MODEL_HASH = "804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6"
SNAPSHOT = "DESIGNATED_DAILY_PUBLIC_SNAPSHOT"
ADMISSION = "ADMITTED_SHADOW"
PROSPECTIVE_START = "2026-09-10"
PROSPECTIVE_END = "2026-12-31"
BOUNDARY = 0.60
REFERENCE_BOOK = "pinnacle"
BOOKS = tuple(x[1] for x in hardened.BOOKS)
HISTORICAL_URL = hardened.URL
EXPECTED_COST_PER_DATE = 10
BOOT_REPS = 4000
SEED = 20260910
MINIMUM_EVIDENCE = {
    "eligible_resolved_games": 732,
    "agreement_games": 438,
    "no_agreement_games": 294,
    "resolved_dates": 20,
    "out_of_time_scored_games": 100,
    "basis": ("Two-sided alpha 0.05, 80% power for a declared 10 percentage-point lift from "
              "a 60.8% market-strong/non-agreement baseline, using the prior 76:51 allocation only "
              "for planning; 732 is a lower bound before date-cluster inflation."),
}
FINAL_CATEGORIES = {
    "MODEL_AGREEMENT_INCREMENTAL_VALUE_SUPPORTED",
    "MARKET_STRENGTH_EXPLAINS_COHORT",
    "MODEL_AGREEMENT_INCREMENTAL_VALUE_NOT_SUPPORTED",
    "SEPARATION_EVIDENCE_INSUFFICIENT",
}
RISK_STATES = {
    "MARKET_STRONG_MODEL_AGREES",
    "MARKET_STRONG_MODEL_NOT_STRONG",
    "MARKET_STRONG_MODEL_DISAGREES",
    "MARKET_STRONG_MISSING_MODEL",
    "REFERENCE_MARKET_NOT_STRONG",
    "REFERENCE_PRICE_UNAVAILABLE",
    "REFERENCE_BOOKMAKER_ABSENT",
    "REFERENCE_ONE_SIDED_MARKET",
    "REFERENCE_STALE_OR_POST_START",
    "IDENTITY_MISMATCH",
}
REQUEST_FIELDS = (
    "request_id", "recorded_at_utc", "game_date", "requested_timestamp_utc", "attempt",
    "status", "http_status", "x_requests_last", "x_requests_used", "x_requests_remaining",
    "authorized_credit_ceiling", "raw_response_path", "request_parameters_path", "raw_sha256", "error",
)


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS study_metadata (
  study_id TEXT PRIMARY KEY, freeze_sha256 TEXT NOT NULL, prospective_start TEXT NOT NULL,
  prospective_end TEXT NOT NULL, created_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS predictions (
  game_key TEXT PRIMARY KEY, game_date TEXT NOT NULL, game_id INTEGER NOT NULL,
  scheduled_start_utc TEXT NOT NULL, prediction_timestamp_utc TEXT NOT NULL,
  prediction_cutoff_utc TEXT NOT NULL, home_team TEXT NOT NULL, away_team TEXT NOT NULL,
  home_model_probability REAL NOT NULL, away_model_probability REAL NOT NULL,
  model_strong_side TEXT NOT NULL, prediction_payload_sha256 TEXT NOT NULL,
  row_sha256 TEXT NOT NULL, UNIQUE(game_date,game_id)
);
CREATE TABLE IF NOT EXISTS risk_set (
  game_key TEXT PRIMARY KEY, game_date TEXT NOT NULL, game_id INTEGER,
  provider_event_id TEXT, scheduled_start_utc TEXT NOT NULL, home_team TEXT NOT NULL, away_team TEXT NOT NULL,
  requested_timestamp_utc TEXT NOT NULL, returned_snapshot_timestamp_utc TEXT NOT NULL,
  reference_book_last_update_utc TEXT, reference_market_last_update_utc TEXT, market_strong_side TEXT,
  selected_market_probability REAL, selected_model_probability REAL, model_strong_side TEXT,
  agreement_indicator INTEGER, risk_state TEXT NOT NULL, risk_set_eligible INTEGER NOT NULL,
  late_season_regime TEXT NOT NULL, prediction_payload_sha256 TEXT, raw_response_sha256 TEXT NOT NULL,
  row_sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bookmaker_prices (
  game_key TEXT NOT NULL REFERENCES risk_set(game_key), bookmaker_key TEXT NOT NULL,
  bookmaker_last_update_utc TEXT, market_last_update_utc TEXT,
  home_american_price REAL, away_american_price REAL,
  selected_american_price REAL, selected_decimal_price REAL, selected_paid_break_even REAL,
  price_state TEXT NOT NULL, raw_response_sha256 TEXT NOT NULL, row_sha256 TEXT NOT NULL,
  PRIMARY KEY(game_key,bookmaker_key)
);
CREATE TABLE IF NOT EXISTS outcomes (
  game_key TEXT PRIMARY KEY REFERENCES risk_set(game_key), official_winner TEXT NOT NULL,
  selected_side_win INTEGER NOT NULL, outcome_payload_sha256 TEXT NOT NULL,
  grading_timestamp_utc TEXT NOT NULL, row_sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS acquisition_authorization (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1), authorized_credit_ceiling INTEGER NOT NULL,
  recorded_at_utc TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS predictions_no_update BEFORE UPDATE ON predictions BEGIN SELECT RAISE(ABORT,'append-only predictions'); END;
CREATE TRIGGER IF NOT EXISTS predictions_no_delete BEFORE DELETE ON predictions BEGIN SELECT RAISE(ABORT,'append-only predictions'); END;
CREATE TRIGGER IF NOT EXISTS risk_set_no_update BEFORE UPDATE ON risk_set BEGIN SELECT RAISE(ABORT,'append-only risk_set'); END;
CREATE TRIGGER IF NOT EXISTS risk_set_no_delete BEFORE DELETE ON risk_set BEGIN SELECT RAISE(ABORT,'append-only risk_set'); END;
CREATE TRIGGER IF NOT EXISTS bookmaker_prices_no_update BEFORE UPDATE ON bookmaker_prices BEGIN SELECT RAISE(ABORT,'append-only bookmaker_prices'); END;
CREATE TRIGGER IF NOT EXISTS bookmaker_prices_no_delete BEFORE DELETE ON bookmaker_prices BEGIN SELECT RAISE(ABORT,'append-only bookmaker_prices'); END;
CREATE TRIGGER IF NOT EXISTS outcomes_no_update BEFORE UPDATE ON outcomes BEGIN SELECT RAISE(ABORT,'append-only outcomes'); END;
CREATE TRIGGER IF NOT EXISTS outcomes_no_delete BEFORE DELETE ON outcomes BEGIN SELECT RAISE(ABORT,'append-only outcomes'); END;
CREATE TRIGGER IF NOT EXISTS acquisition_authorization_no_update BEFORE UPDATE ON acquisition_authorization BEGIN SELECT RAISE(ABORT,'immutable authorization'); END;
CREATE TRIGGER IF NOT EXISTS acquisition_authorization_no_delete BEFORE DELETE ON acquisition_authorization BEGIN SELECT RAISE(ABORT,'immutable authorization'); END;
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def json_clean(value: Any) -> Any:
    if isinstance(value, dict): return {key: json_clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)): return [json_clean(item) for item in value]
    if isinstance(value, (np.integer,)): return int(value)
    if isinstance(value, (np.floating, float)): return None if not np.isfinite(value) else float(value)
    return value


def digest(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode()).hexdigest()


def file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalize_team(value: Any) -> str:
    return hardened.norm_team(value)


def iso(value: Any) -> str:
    return pd.to_datetime(value, utc=True).isoformat().replace("+00:00", "Z")


def logit(value: Iterable[float]) -> np.ndarray:
    p = np.clip(np.asarray(value, float), 1e-6, 1-1e-6)
    return np.log(p/(1-p))


def implied_probability(american: float) -> float:
    value = float(american)
    return 100/(100+value) if value > 0 else -value/(-value+100)


def decimal_price(american: float) -> float:
    value = float(american)
    return 1 + (value/100 if value > 0 else 100/-value)


def no_vig(home: float, away: float) -> tuple[float, float]:
    h, a = implied_probability(home), implied_probability(away)
    return h/(h+a), a/(h+a)


def late_season_regime(game_date: str) -> str:
    month = int(str(game_date)[5:7])
    if month == 9: return "LATE_SEASON_SEPTEMBER"
    if month in (10, 11): return "POSTSEASON_CALENDAR_WINDOW"
    return "OFFSEASON_CALENDAR_WINDOW"


def freeze_payload(frozen_at: str) -> dict[str, Any]:
    return {
        "study_id": "MLB_MARKET_STRONG_AGREEMENT_SEPARATION_PROSPECTIVE_V1",
        "frozen_at_utc": frozen_at,
        "prospective_start_game_date": PROSPECTIVE_START,
        "prospective_end_game_date": PROSPECTIVE_END,
        "prior_56_20_rows_admitted": False,
        "model": {"version": MODEL, "hash": MODEL_HASH, "snapshot": SNAPSHOT,
                  "admission": ADMISSION, "strong_definition": "selected probability > 0.60 strict"},
        "market": {"reference_book": REFERENCE_BOOK, "market": "h2h", "odds_format": "american",
                   "strong_definition": "reference-book no-vig selected-side probability > 0.60 strict"},
        "agreement_indicator": ("1 only when the reference market-strong side is also the frozen model's "
                                "strictly-over-0.60 side; 0 combines model-not-strong and model-strong-opposite, "
                                "which remain separately reported; missing model is never imputed"),
        "designated_prediction_timestamp": "immutable prediction_timestamp_utc on DESIGNATED_DAILY_PUBLIC_SNAPSHOT",
        "designated_price_timestamp": ("one requested timestamp per game date: immutable daily prediction timestamp "
                                       "floored to whole-second precision; nearest provider historical "
                                       "snapshot at or before it"),
        "bookmakers": list(BOOKS), "bookmaker_selection_used_outcomes": False,
        "bookmaker_selection_rationale": {
            key: reason for _, key, _, _, reason in hardened.BOOKS
        },
        "reference_book_selected_before_prospective_outcomes": True,
        "admission": {"all_provider events and all immutable predictions on an acquired date": True,
                      "risk_states": sorted(RISK_STATES), "two_sided_prices_required": True,
                      "returned_snapshot_must_not_exceed_request": True,
                      "book_and_market_update_must_be_strictly_pregame": True,
                      "identity": "exact normalized home/away plus unique closest start within three hours",
                      "missing_model_or_price_is_classified_not_dropped": True},
        "grading": {"authority": "mlb.public_game_moneyline_outcomes", "one_outcome_per_game": True,
                    "bookmaker_rows_do_not_increase_effective_sample_size": True,
                    "flat_risk": "one unit risked at each bookmaker's fixed selected-side price",
                    "paid_break_even_excess": "selected-side win rate minus mean raw implied probability"},
        "uncertainty": {"unit": "game date cluster", "bootstrap_repetitions": BOOT_REPS, "seed": SEED,
                        "interval": "two-sided percentile 95%"},
        "blocked_date_fit": ("expanding-origin unpenalized-equivalent logistic fit: outcome ~ logit(reference "
                             "market probability) + fixed agreement indicator; test dates never enter training"),
        "evidence_requirement": MINIMUM_EVIDENCE,
        "decision_rule": {
            "MODEL_AGREEMENT_INCREMENTAL_VALUE_SUPPORTED": ("evidence gate met with predictive support "
                "(positive clustered agreement coefficient and both out-of-time score-difference upper bounds below 0) "
                "and economic support (Pinnacle ROI- and paid-break-even-excess-difference lower bounds above 0 and "
                "positive ROI-difference direction at >=8 books)"),
            "MARKET_STRENGTH_EXPLAINS_COHORT": ("evidence gate met, overall market-strong paid-break-even excess is "
                "positive, and agreement effects satisfy frozen equivalence bounds: win-rate interval within +/-0.03 "
                "and Brier difference interval within +/-0.002"),
            "MODEL_AGREEMENT_INCREMENTAL_VALUE_NOT_SUPPORTED": ("evidence gate met and agreement is demonstrably "
                "harmful: win-rate upper bound below 0 or both out-of-time score-difference lower bounds above 0"),
            "SEPARATION_EVIDENCE_INSUFFICIENT": "evidence gate unmet or powered results remain mixed/inconclusive",
        },
        "late_season_regime": ("Remainder-of-2026 fixed horizon; September, postseason-calendar, and offseason-calendar "
                               "windows reported separately; no pooling with the prior 56-20 cohort"),
        "early_stopping": False,
        "decision_not_before_utc_date": "2027-01-01",
        "constraints": {"threshold_optimization": False, "best_price": False, "book_selection_by_roi": False,
                        "outcome_based_subclass_exclusion": False, "production": False, "scheduler": False,
                        "public_prediction": False, "wagering": False},
    }


def schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def immutable_insert(conn: sqlite3.Connection, table: str, key: str | tuple[str, ...], row: dict[str, Any]) -> bool:
    columns = list(row)
    before = conn.total_changes
    conn.execute(f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                 [row[column] for column in columns])
    if conn.total_changes > before: return True
    keys = (key,) if isinstance(key, str) else key
    where = " AND ".join(f"{column}=?" for column in keys)
    existing = conn.execute(f"SELECT row_sha256 FROM {table} WHERE {where}", tuple(row[column] for column in keys)).fetchone()
    if not existing or existing[0] != row["row_sha256"]:
        raise RuntimeError(f"immutable conflict in {table}: {tuple(row[column] for column in keys)}")
    return False


def initialize(output: Path, ledger: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True); ledger.parent.mkdir(parents=True, exist_ok=True)
    freeze_path = output / "pre_outcome_freeze.json"
    if freeze_path.exists():
        freeze = json.loads(freeze_path.read_text())
    else:
        config = json.loads(MODEL_CONFIG.read_text())
        if config["model_hash"] != MODEL_HASH: raise RuntimeError("Frozen model hash does not match repository authority")
        freeze = freeze_payload(utc_now())
        freeze_path.write_text(json.dumps(freeze, indent=2, sort_keys=True) + "\n")
    with sqlite3.connect(ledger) as conn:
        schema(conn)
        row = (freeze["study_id"], file_sha(freeze_path), PROSPECTIVE_START, PROSPECTIVE_END, freeze["frozen_at_utc"])
        conn.execute("INSERT OR IGNORE INTO study_metadata VALUES (?,?,?,?,?)", row)
        existing = conn.execute("SELECT freeze_sha256 FROM study_metadata WHERE study_id=?", (freeze["study_id"],)).fetchone()
        if not existing or existing[0] != file_sha(freeze_path): raise RuntimeError("Freeze/ledger identity mismatch")
        conn.commit()
    (output / "schema.sql").write_text(SCHEMA.strip() + "\n")
    request_ledger = output / "append_only_request_ledger.csv"
    if not request_ledger.exists(): request_ledger.write_text(",".join(REQUEST_FIELDS) + "\n")
    retry_ledger = output / "append_only_retry_decision_ledger.csv"
    if not retry_ledger.exists(): retry_ledger.write_text(",".join(hardened.RETRY_FIELDS) + "\n")
    return freeze


def verify_freeze(output: Path, ledger: Path) -> dict[str, Any]:
    freeze = json.loads((output / "pre_outcome_freeze.json").read_text())
    if freeze != freeze_payload(freeze["frozen_at_utc"]): raise RuntimeError("Pre-outcome freeze changed")
    with sqlite3.connect(ledger) as conn:
        schema(conn)
        row = conn.execute("SELECT freeze_sha256 FROM study_metadata WHERE study_id=?", (freeze["study_id"],)).fetchone()
    if not row or row[0] != file_sha(output / "pre_outcome_freeze.json"):
        raise RuntimeError("Ledger is not bound to the frozen contract")
    return freeze


def ingest_predictions(output: Path, ledger: Path, through_date: str) -> dict[str, int]:
    verify_freeze(output, ledger)
    if not (PROSPECTIVE_START <= through_date <= PROSPECTIVE_END): raise ValueError("through-date outside frozen horizon")
    sql = """
      SELECT game_date::text,game_id,scheduled_start_utc,prediction_timestamp_utc,prediction_cutoff_utc,
             home_team,away_team,home_win_probability,away_win_probability,payload_sha256,model_hash
      FROM mlb.public_game_moneyline_predictions
      WHERE model_version=%s AND prediction_snapshot_class=%s AND admission_status=%s
        AND game_date BETWEEN %s AND %s ORDER BY game_date,game_id
    """
    with pg_connect() as source, source.cursor() as cursor:
        cursor.execute(sql, (MODEL, SNAPSHOT, ADMISSION, PROSPECTIVE_START, through_date)); rows = cursor.fetchall()
    inserted = 0
    with sqlite3.connect(ledger) as conn:
        schema(conn)
        for r in rows:
            if r[10] != MODEL_HASH: raise RuntimeError("Prospective prediction model hash changed")
            home, away = float(r[7]), float(r[8])
            strong = "HOME" if home > BOUNDARY else "AWAY" if away > BOUNDARY else "NONE"
            values = {"game_key": f"MLB|{r[0]}|{int(r[1])}", "game_date": r[0], "game_id": int(r[1]),
                      "scheduled_start_utc": iso(r[2]), "prediction_timestamp_utc": iso(r[3]),
                      "prediction_cutoff_utc": iso(r[4]), "home_team": r[5], "away_team": r[6],
                      "home_model_probability": home, "away_model_probability": away,
                      "model_strong_side": strong, "prediction_payload_sha256": r[9]}
            values["row_sha256"] = digest(values)
            inserted += immutable_insert(conn, "predictions", "game_key", values)
        conn.commit()
    return {"source_rows": len(rows), "inserted_rows": inserted}


def append_request_ledger(output: Path, row: dict[str, Any]) -> None:
    path = output / "append_only_request_ledger.csv"; new = not path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REQUEST_FIELDS, extrasaction="ignore", lineterminator="\n")
        if new: writer.writeheader()
        writer.writerow({key: hardened.redact_sensitive(row.get(key, "")) for key in REQUEST_FIELDS})
        handle.flush(); os.fsync(handle.fileno())


def request_terminal_rows(output: Path) -> pd.DataFrame:
    path = output / "append_only_request_ledger.csv"
    if not path.exists(): return pd.DataFrame(columns=REQUEST_FIELDS)
    return pd.read_csv(path, dtype=str).fillna("")


def requested_timestamp(conn: sqlite3.Connection, game_date: str) -> str:
    values = [row[0] for row in conn.execute(
        "SELECT DISTINCT prediction_timestamp_utc FROM predictions WHERE game_date=?", (game_date,)).fetchall()]
    if len(values) != 1: raise RuntimeError("Acquisition requires exactly one immutable daily prediction timestamp")
    return pd.to_datetime(values[0], utc=True).floor("s").isoformat().replace("+00:00", "Z")


def establish_authorization(conn: sqlite3.Connection, ceiling: int) -> None:
    if ceiling <= 0: raise ValueError("A positive separately authorized credit ceiling is required")
    existing = conn.execute("SELECT authorized_credit_ceiling FROM acquisition_authorization WHERE singleton=1").fetchone()
    if existing and existing[0] != ceiling: raise RuntimeError("Acquisition credit ceiling is already frozen and cannot change")
    conn.execute("INSERT OR IGNORE INTO acquisition_authorization VALUES (1,?,?)", (ceiling, utc_now()))


def select_event(events: list[dict[str, Any]], prediction: pd.Series) -> dict[str, Any] | None:
    candidates = []
    start = pd.to_datetime(prediction.scheduled_start_utc, utc=True)
    for event in events:
        if normalize_team(event.get("home_team")) != normalize_team(prediction.home_team): continue
        if normalize_team(event.get("away_team")) != normalize_team(prediction.away_team): continue
        event_start = pd.to_datetime(event.get("commence_time"), utc=True, errors="coerce")
        if pd.isna(event_start): continue
        candidates.append((abs((event_start-start).total_seconds()), str(event.get("id")), event))
    candidates.sort(key=lambda item: (item[0], item[1]))
    if not candidates or candidates[0][0] > 3*3600: return None
    if len(candidates) > 1 and candidates[0][0] == candidates[1][0]: return None
    return candidates[0][2]


def market_prices(
    event: dict[str, Any], bookmaker_key: str,
) -> tuple[str, str | None, str | None, float | None, float | None]:
    books = [book for book in event.get("bookmakers", []) if book.get("key") == bookmaker_key]
    if not books: return "BOOKMAKER_ABSENT", None, None, None, None
    if len(books) != 1: return "PRICE_UNAVAILABLE", None, None, None, None
    book = books[0]; markets = [market for market in book.get("markets", []) if market.get("key") == "h2h"]
    if len(markets) != 1: return "PRICE_UNAVAILABLE", book.get("last_update"), None, None, None
    market = markets[0]
    outcomes = {normalize_team(x.get("name")): x.get("price") for x in market.get("outcomes", [])}
    home, away = outcomes.get(normalize_team(event.get("home_team"))), outcomes.get(normalize_team(event.get("away_team")))
    timestamps = (book.get("last_update"), market.get("last_update"))
    if (home is None) != (away is None): return "ONE_SIDED_MARKET", *timestamps, home, away
    if home is None: return "PRICE_UNAVAILABLE", *timestamps, None, None
    return "TWO_SIDED", *timestamps, float(home), float(away)


def classify_risk(prediction: pd.Series | None, event: dict[str, Any] | None, requested: str,
                  returned: str, raw_sha: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if prediction is not None:
        game_key = prediction.game_key; game_date = prediction.game_date; game_id = int(prediction.game_id)
        start = prediction.scheduled_start_utc; home = prediction.home_team; away = prediction.away_team
    else:
        assert event is not None
        start = iso(event["commence_time"]); local = pd.to_datetime(start, utc=True).tz_convert(ZoneInfo("America/Los_Angeles"))
        game_date = local.date().isoformat(); game_key = f"MLB_PROVIDER|{game_date}|{event.get('id')}"; game_id = None
        home, away = event.get("home_team"), event.get("away_team")
    base = {"game_key": game_key, "game_date": game_date, "game_id": game_id,
            "provider_event_id": event.get("id") if event else None, "scheduled_start_utc": start,
            "home_team": home, "away_team": away, "requested_timestamp_utc": requested,
            "returned_snapshot_timestamp_utc": returned, "reference_book_last_update_utc": None,
            "reference_market_last_update_utc": None,
            "market_strong_side": None, "selected_market_probability": None,
            "selected_model_probability": None, "model_strong_side": prediction.model_strong_side if prediction is not None else None,
            "agreement_indicator": None, "risk_state": "IDENTITY_MISMATCH", "risk_set_eligible": 0,
            "late_season_regime": late_season_regime(game_date),
            "prediction_payload_sha256": prediction.prediction_payload_sha256 if prediction is not None else None,
            "raw_response_sha256": raw_sha}
    price_rows: list[dict[str, Any]] = []
    if event is None:
        base["risk_state"] = "IDENTITY_MISMATCH"
    else:
        ref_state, ref_updated, ref_market_updated, ref_home, ref_away = market_prices(event, REFERENCE_BOOK)
        base["reference_book_last_update_utc"] = ref_updated
        base["reference_market_last_update_utc"] = ref_market_updated
        start_dt, request_dt, returned_dt = (pd.to_datetime(x, utc=True) for x in (start, requested, returned))
        if ref_state == "BOOKMAKER_ABSENT": base["risk_state"] = "REFERENCE_BOOKMAKER_ABSENT"
        elif ref_state == "ONE_SIDED_MARKET": base["risk_state"] = "REFERENCE_ONE_SIDED_MARKET"
        elif ref_state != "TWO_SIDED": base["risk_state"] = "REFERENCE_PRICE_UNAVAILABLE"
        elif (returned_dt > request_dt
              or any(pd.isna(pd.to_datetime(value, utc=True, errors="coerce"))
                     for value in (ref_updated, ref_market_updated))
              or any(pd.to_datetime(value, utc=True) > returned_dt for value in (ref_updated, ref_market_updated))
              or any(pd.to_datetime(value, utc=True) >= start_dt for value in (ref_updated, ref_market_updated))):
            base["risk_state"] = "REFERENCE_STALE_OR_POST_START"
        else:
            hp, ap = no_vig(ref_home, ref_away)
            market_side = "HOME" if hp > BOUNDARY else "AWAY" if ap > BOUNDARY else "NONE"
            base["market_strong_side"] = market_side
            base["selected_market_probability"] = hp if market_side == "HOME" else ap if market_side == "AWAY" else None
            if market_side == "NONE": base["risk_state"] = "REFERENCE_MARKET_NOT_STRONG"
            elif prediction is None:
                base["risk_state"] = "MARKET_STRONG_MISSING_MODEL"
            else:
                base["selected_model_probability"] = (float(prediction.home_model_probability) if market_side == "HOME"
                                                      else float(prediction.away_model_probability))
                if prediction.model_strong_side == market_side:
                    base.update({"agreement_indicator": 1, "risk_state": "MARKET_STRONG_MODEL_AGREES", "risk_set_eligible": 1})
                elif prediction.model_strong_side == "NONE":
                    base.update({"agreement_indicator": 0, "risk_state": "MARKET_STRONG_MODEL_NOT_STRONG", "risk_set_eligible": 1})
                else:
                    base.update({"agreement_indicator": 0, "risk_state": "MARKET_STRONG_MODEL_DISAGREES", "risk_set_eligible": 1})
        for book in BOOKS:
            state, updated, market_updated, hp, ap = market_prices(event, book)
            if state == "TWO_SIDED":
                updates = [pd.to_datetime(value, utc=True, errors="coerce")
                           for value in (updated, market_updated)]
                if any(pd.isna(value) or value > returned_dt or value >= start_dt for value in updates):
                    state = "STALE_OR_POST_START"
            selected = hp if base["market_strong_side"] == "HOME" else ap if base["market_strong_side"] == "AWAY" else None
            values = {"game_key": game_key, "bookmaker_key": book, "bookmaker_last_update_utc": updated,
                      "market_last_update_utc": market_updated,
                      "home_american_price": hp, "away_american_price": ap,
                      "selected_american_price": selected if state == "TWO_SIDED" else None,
                      "selected_decimal_price": decimal_price(selected) if state == "TWO_SIDED" and selected is not None else None,
                      "selected_paid_break_even": implied_probability(selected) if state == "TWO_SIDED" and selected is not None else None,
                      "price_state": state, "raw_response_sha256": raw_sha}
            values["row_sha256"] = digest(values); price_rows.append(values)
    if base["risk_state"] not in RISK_STATES: raise RuntimeError("Unknown risk-set state")
    base["row_sha256"] = digest(base)
    return base, price_rows


def ingest_response(output: Path, ledger: Path, game_date: str, requested: str, raw_path: Path) -> dict[str, int]:
    payload = json.loads(raw_path.read_text()); returned = iso(payload["timestamp"])
    if pd.to_datetime(returned, utc=True) > pd.to_datetime(requested, utc=True):
        raise RuntimeError("Provider returned a snapshot after the requested timestamp")
    events = payload.get("data", []); raw_sha = file_sha(raw_path)
    def belongs_to_date(event: dict[str, Any]) -> bool:
        start = pd.to_datetime(event.get("commence_time"), utc=True, errors="coerce")
        return not pd.isna(start) and start.tz_convert(ZoneInfo("America/Los_Angeles")).date().isoformat() == game_date
    events = [event for event in events if belongs_to_date(event)]
    with sqlite3.connect(ledger) as conn:
        predictions = pd.read_sql_query("SELECT * FROM predictions WHERE game_date=? ORDER BY game_id", conn, params=(game_date,))
        matched_event_ids: set[str] = set(); risk_inserted = prices_inserted = 0
        for _, prediction in predictions.iterrows():
            event = select_event(events, prediction)
            if event: matched_event_ids.add(str(event.get("id")))
            risk, prices = classify_risk(prediction, event, requested, returned, raw_sha)
            risk_inserted += immutable_insert(conn, "risk_set", "game_key", risk)
            for values in prices: prices_inserted += immutable_insert(conn, "bookmaker_prices", ("game_key", "bookmaker_key"), values)
        for event in events:
            if str(event.get("id")) in matched_event_ids: continue
            risk, prices = classify_risk(None, event, requested, returned, raw_sha)
            risk_inserted += immutable_insert(conn, "risk_set", "game_key", risk)
            for values in prices: prices_inserted += immutable_insert(conn, "bookmaker_prices", ("game_key", "bookmaker_key"), values)
        conn.commit()
    return {"risk_rows_inserted": risk_inserted, "price_rows_inserted": prices_inserted}


def reconcile_successful_response(output: Path, ledger: Path, game_date: str) -> dict[str, int]:
    """Parse an already-preserved successful response without issuing another request."""
    verify_freeze(output, ledger)
    request_id = f"PROSPECTIVE_H2H_{game_date}"
    prior = request_terminal_rows(output)
    success = prior[(prior.request_id.eq(request_id)) & prior.status.eq("SUCCESS")]
    if success.empty:
        raise RuntimeError("No preserved successful response is available for reconciliation")
    row = success.iloc[-1]
    raw_path = ROOT / row.raw_response_path
    if not raw_path.is_file():
        raise RuntimeError("Preserved successful raw response is missing")
    if row.raw_sha256 and file_sha(raw_path) != row.raw_sha256:
        raise RuntimeError("Preserved successful raw response hash mismatch")
    return ingest_response(output, ledger, game_date, row.requested_timestamp_utc, raw_path)


def acquire_date(output: Path, ledger: Path, game_date: str, ceiling: int, timeout: int) -> dict[str, Any]:
    verify_freeze(output, ledger)
    if not (PROSPECTIVE_START <= game_date <= PROSPECTIVE_END): raise ValueError("game date outside frozen horizon")
    with sqlite3.connect(ledger) as conn:
        schema(conn); establish_authorization(conn, ceiling); requested = requested_timestamp(conn, game_date); conn.commit()
    prior = request_terminal_rows(output); request_id = f"PROSPECTIVE_H2H_{game_date}"
    request_rows = prior[prior.request_id.eq(request_id)]
    if len(request_rows[request_rows.status.eq("SUCCESS")]):
        return {"request_id": request_id, "status": "SKIPPED_PRIOR_SUCCESS"}
    for attempt_value, attempt_rows in request_rows.groupby("attempt", dropna=False):
        if len(attempt_rows[attempt_rows.status.eq("REQUEST_STARTED")]) and not len(
                attempt_rows[attempt_rows.status.isin(["SUCCESS", "HTTP_ERROR", "TRANSPORT_ERROR"])]):
            raise RuntimeError(f"Attempt {attempt_value} has unknown charge state; retry refused")
    failures = request_rows[request_rows.status.isin(["HTTP_ERROR", "TRANSPORT_ERROR"])]
    if len(failures):
        allowed, reason = hardened.retry_decision(failures.iloc[-1].status, failures.iloc[-1].x_requests_last)
        hardened.append_retry_ledger(output, {"request_id": request_id, "recorded_at_utc": utc_now(),
            "prior_status": failures.iloc[-1].status, "x_requests_last": failures.iloc[-1].x_requests_last,
            "retry_allowed": allowed, "decision_reason": reason})
        if not allowed: raise RuntimeError("Prior failure was charged or its charge is unknown; retry refused")
    charged = int(pd.to_numeric(prior.x_requests_last, errors="coerce").fillna(0).sum()) if len(prior) else 0
    if charged + EXPECTED_COST_PER_DATE > ceiling: raise RuntimeError("Next request would exceed frozen authorization")
    attempt = len(failures) + 1; raw_dir = output / "raw" / game_date; raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"historical_h2h_attempt_{attempt}.json"
    params_path = raw_dir / f"request_parameters_attempt_{attempt}.json"
    if raw_path.exists(): raise RuntimeError("Raw response path exists; overwrite refused")
    params = {"apiKey": os.getenv("ODDS_API_KEY", "").strip(), "date": requested, "markets": "h2h",
              "bookmakers": ",".join(BOOKS), "oddsFormat": "american", "dateFormat": "iso"}
    if not params["apiKey"]: raise RuntimeError("ODDS_API_KEY missing")
    params_path.write_text(json.dumps({"url": HISTORICAL_URL, "parameters_excluding_secret": hardened.safe_params(params),
                                      "expected_cost": EXPECTED_COST_PER_DATE, "authorized_credit_ceiling": ceiling},
                                     indent=2, sort_keys=True) + "\n")
    common = {"request_id": request_id, "game_date": game_date, "requested_timestamp_utc": requested,
              "attempt": attempt, "authorized_credit_ceiling": ceiling,
              "raw_response_path": str(raw_path.relative_to(ROOT)),
              "request_parameters_path": str(params_path.relative_to(ROOT))}
    append_request_ledger(output, {**common, "recorded_at_utc": utc_now(), "status": "REQUEST_STARTED"})
    try:
        response = hardened.safe_get(HISTORICAL_URL, params, timeout)
    except BaseException as exc:
        error = hardened.render_exception(exc)
        append_request_ledger(output, {**common, "recorded_at_utc": utc_now(), "status": "TRANSPORT_ERROR", "error": error})
        raise RuntimeError(error) from None
    raw_path.write_bytes(hardened.redact_sensitive_bytes(response.content))
    headers = hardened.header_dict(response); last = hardened.header_int(headers, "x-requests-last")
    error = "" if response.ok else hardened.http_error_message("prospective historical odds", response.status_code, response.content)
    status = "SUCCESS" if response.ok else "HTTP_ERROR"
    append_request_ledger(output, {**common, "recorded_at_utc": utc_now(), "status": status,
        "http_status": response.status_code, "x_requests_last": headers["x-requests-last"],
        "x_requests_used": headers["x-requests-used"], "x_requests_remaining": headers["x-requests-remaining"],
        "raw_sha256": file_sha(raw_path), "error": error})
    if last is None: raise RuntimeError("Quota cost header missing; further acquisition refused")
    if charged + last > ceiling: raise RuntimeError("Observed charge exceeded frozen authorization")
    if not response.ok: raise RuntimeError(error)
    if last > EXPECTED_COST_PER_DATE: raise RuntimeError("Observed request cost exceeded expected cost")
    return {"request_id": request_id, "status": "SUCCESS", "x_requests_last": last,
            **ingest_response(output, ledger, game_date, requested, raw_path)}


def grade(output: Path, ledger: Path, through_date: str) -> dict[str, int]:
    verify_freeze(output, ledger)
    with sqlite3.connect(ledger) as conn:
        candidates = pd.read_sql_query("SELECT game_key,game_date,game_id,market_strong_side FROM risk_set WHERE game_id IS NOT NULL AND game_date<=?",
                                       conn, params=(through_date,))
    if candidates.empty: return {"source_rows": 0, "inserted_rows": 0}
    sql = """
      SELECT game_date::text,game_id,official_winner,payload_sha256,grading_timestamp_utc
      FROM mlb.public_game_moneyline_outcomes WHERE model_version=%s AND prediction_snapshot_class=%s
        AND game_date BETWEEN %s AND %s ORDER BY game_date,game_id
    """
    with pg_connect() as source, source.cursor() as cursor:
        cursor.execute(sql, (MODEL, SNAPSHOT, PROSPECTIVE_START, through_date)); source_rows = cursor.fetchall()
    # pg_connect() has a repository-wide dict_row contract.  Keep grading
    # keyed to named source fields so outcome-only runs do not depend on a
    # positional cursor representation.
    authority = {(str(r["game_date"]), int(r["game_id"])): r for r in source_rows}; inserted = 0
    with sqlite3.connect(ledger) as conn:
        schema(conn)
        for r in candidates.itertuples(index=False):
            outcome = authority.get((r.game_date, int(r.game_id)))
            if not outcome or r.market_strong_side not in ("HOME", "AWAY"): continue
            home_won = normalize_team(outcome["official_winner"]) == normalize_team(conn.execute(
                "SELECT home_team FROM risk_set WHERE game_key=?", (r.game_key,)).fetchone()[0])
            values = {"game_key": r.game_key, "official_winner": outcome["official_winner"],
                      "selected_side_win": int(home_won if r.market_strong_side == "HOME" else not home_won),
                      "outcome_payload_sha256": outcome["payload_sha256"],
                      "grading_timestamp_utc": iso(outcome["grading_timestamp_utc"])}
            values["row_sha256"] = digest(values)
            inserted += immutable_insert(conn, "outcomes", "game_key", values)
        conn.commit()
    return {"source_rows": len(source_rows), "inserted_rows": inserted}


def cluster_interval(dates: Iterable[Any], values: Iterable[float]) -> tuple[float, float]:
    d = pd.DataFrame({"date": list(dates), "value": list(values)}).dropna()
    grouped = d.groupby("date", sort=True).value.agg(["sum", "count"])
    if len(grouped) < 2: return math.nan, math.nan
    rng = np.random.default_rng(SEED + len(d)); pick = rng.integers(0, len(grouped), (BOOT_REPS, len(grouped)))
    means = grouped["sum"].to_numpy()[pick].sum(1)/grouped["count"].to_numpy()[pick].sum(1)
    return float(np.quantile(means, .025)), float(np.quantile(means, .975))


def score(y: np.ndarray, p: np.ndarray) -> tuple[float, float]:
    p = np.clip(np.asarray(p, float), 1e-9, 1-1e-9); y = np.asarray(y, int)
    return float(np.mean((p-y)**2)), float(np.mean(-y*np.log(p)-(1-y)*np.log(1-p)))


def blocked_analysis(resolved: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if resolved.empty:
        return (pd.DataFrame(columns=["fold", "train_start", "train_end", "test_start", "test_end",
                                      "train_games", "test_games", "agreement_coefficient"]),
                pd.DataFrame(columns=["score", "difference_combo_minus_market", "ci_2_5", "ci_97_5",
                                      "out_of_time_games", "date_clusters"]),
                {"out_of_time_games": 0})
    d = resolved.sort_values(["game_date", "game_key"]).copy(); dates = sorted(d.game_date.unique())
    if len(dates) < 8 or d.agreement_indicator.nunique() < 2:
        return (pd.DataFrame(columns=["fold", "train_start", "train_end", "test_start", "test_end",
                                      "train_games", "test_games", "agreement_coefficient"]),
                pd.DataFrame(columns=["score", "difference_combo_minus_market", "ci_2_5", "ci_97_5",
                                      "out_of_time_games", "date_clusters"]),
                {"out_of_time_games": 0})
    first_test = max(4, len(dates)//3); blocks = [list(x) for x in np.array_split(dates[first_test:], min(4, len(dates)-first_test)) if len(x)]
    predictions, folds = [], []
    for fold, test_dates in enumerate(blocks, 1):
        train = d[d.game_date.lt(test_dates[0])]; test = d[d.game_date.isin(test_dates)]
        if train.selected_side_win.nunique() < 2 or train.agreement_indicator.nunique() < 2: continue
        x_market = logit(train.selected_market_probability).reshape(-1, 1)
        x_combo = np.c_[logit(train.selected_market_probability), train.agreement_indicator]
        base = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000).fit(x_market, train.selected_side_win)
        combo = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000).fit(x_combo, train.selected_side_win)
        base_p = base.predict_proba(logit(test.selected_market_probability).reshape(-1, 1))[:, 1]
        combo_p = combo.predict_proba(np.c_[logit(test.selected_market_probability), test.agreement_indicator])[:, 1]
        folds.append({"fold": fold, "train_start": train.game_date.min(), "train_end": train.game_date.max(),
                      "test_start": test.game_date.min(), "test_end": test.game_date.max(),
                      "train_games": len(train), "test_games": len(test),
                      "agreement_coefficient": float(combo.coef_[0, 1])})
        for (_, row), bp, cp in zip(test.iterrows(), base_p, combo_p):
            predictions.append({"game_key": row.game_key, "game_date": row.game_date,
                                "selected_side_win": int(row.selected_side_win), "fold": fold,
                                "market_only_probability": bp, "market_plus_agreement_probability": cp})
    pred = pd.DataFrame(predictions); diffs = []
    if len(pred):
        y = pred.selected_side_win.to_numpy(); base = pred.market_only_probability.to_numpy(); alt = pred.market_plus_agreement_probability.to_numpy()
        for label, values in (("BRIER", (alt-y)**2-(base-y)**2),
                              ("LOG_LOSS", -y*np.log(alt)-(1-y)*np.log(1-alt)+y*np.log(base)+(1-y)*np.log(1-base))):
            lo, hi = cluster_interval(pred.game_date, values)
            diffs.append({"score": label, "difference_combo_minus_market": float(np.mean(values)), "ci_2_5": lo, "ci_97_5": hi,
                          "out_of_time_games": len(pred), "date_clusters": pred.game_date.nunique()})
    return (pd.DataFrame(folds, columns=["fold", "train_start", "train_end", "test_start", "test_end",
                                               "train_games", "test_games", "agreement_coefficient"]),
            pd.DataFrame(diffs, columns=["score", "difference_combo_minus_market", "ci_2_5", "ci_97_5",
                                                "out_of_time_games", "date_clusters"]),
            {"out_of_time_games": len(pred)})


def agreement_coefficient_interval(resolved: pd.DataFrame) -> dict[str, float]:
    unavailable = {"coefficient": math.nan, "ci_2_5": math.nan, "ci_97_5": math.nan}
    if len(resolved) < 10 or resolved.game_date.nunique() < 2 or resolved.agreement_indicator.nunique() < 2:
        return unavailable
    def fit(frame: pd.DataFrame) -> float:
        if frame.selected_side_win.nunique() < 2 or frame.agreement_indicator.nunique() < 2: return math.nan
        x = np.c_[logit(frame.selected_market_probability), frame.agreement_indicator]
        model = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000).fit(x, frame.selected_side_win)
        return float(model.coef_[0, 1])
    point = fit(resolved); dates = sorted(resolved.game_date.unique()); rng = np.random.default_rng(SEED + 2*len(resolved))
    draws = []
    for _ in range(BOOT_REPS):
        selected = rng.choice(dates, len(dates), replace=True)
        sample = pd.concat([resolved[resolved.game_date.eq(day)] for day in selected], ignore_index=True)
        value = fit(sample)
        if np.isfinite(value): draws.append(value)
    if not draws or not np.isfinite(point): return unavailable
    return {"coefficient": point, "ci_2_5": float(np.quantile(draws, .025)),
            "ci_97_5": float(np.quantile(draws, .975))}


def clustered_group_difference(frame: pd.DataFrame, value_column: str) -> dict[str, float]:
    """Agreement-minus-no-agreement difference with date-cluster resampling."""
    unavailable = {"difference": math.nan, "ci_2_5": math.nan, "ci_97_5": math.nan}
    if frame.empty or frame.agreement_indicator.nunique() != 2:
        return unavailable
    point = (frame.loc[frame.agreement_indicator.eq(1), value_column].mean()
             - frame.loc[frame.agreement_indicator.eq(0), value_column].mean())
    dates = sorted(frame.game_date.unique())
    if len(dates) < 2:
        return {"difference": float(point), "ci_2_5": math.nan, "ci_97_5": math.nan}
    rng = np.random.default_rng(SEED + len(frame) + len(value_column)); draws = []
    for _ in range(BOOT_REPS):
        selected = rng.choice(dates, len(dates), replace=True)
        sample = pd.concat([frame[frame.game_date.eq(day)] for day in selected], ignore_index=True)
        if sample.agreement_indicator.nunique() == 2:
            draws.append(sample.loc[sample.agreement_indicator.eq(1), value_column].mean()
                         - sample.loc[sample.agreement_indicator.eq(0), value_column].mean())
    return {"difference": float(point),
            "ci_2_5": float(np.quantile(draws, .025)) if draws else math.nan,
            "ci_97_5": float(np.quantile(draws, .975)) if draws else math.nan}


def report(output: Path, ledger: Path) -> dict[str, Any]:
    freeze = verify_freeze(output, ledger)
    with sqlite3.connect(ledger) as conn:
        predictions = pd.read_sql_query("SELECT * FROM predictions ORDER BY game_date,game_key", conn)
        risk = pd.read_sql_query("SELECT * FROM risk_set ORDER BY game_date,game_key", conn)
        prices = pd.read_sql_query("SELECT * FROM bookmaker_prices ORDER BY bookmaker_key,game_key", conn)
        outcomes = pd.read_sql_query("SELECT * FROM outcomes ORDER BY game_key", conn)
    plan_rows = []
    requests = request_terminal_rows(output)
    if len(predictions):
        for game_date, games in predictions.groupby("game_date", sort=True):
            timestamps = sorted(games.prediction_timestamp_utc.unique())
            request_id = f"PROSPECTIVE_H2H_{game_date}"
            prior = requests[requests.request_id.eq(request_id)] if len(requests) else requests
            successful = bool(len(prior[prior.status.eq("SUCCESS")])) if len(prior) else False
            plan_rows.append({"game_date": game_date, "immutable_prediction_rows": len(games),
                              "immutable_prediction_timestamp_count": len(timestamps),
                              "requested_timestamp_utc": (pd.to_datetime(timestamps[0], utc=True).floor("s").isoformat().replace("+00:00", "Z")
                                                          if len(timestamps) == 1 else None),
                              "request_id": request_id, "expected_credit_cost": EXPECTED_COST_PER_DATE,
                              "request_status": "SUCCESS" if successful else
                                                "READY" if len(timestamps) == 1 else "TIMESTAMP_CONFLICT"})
    acquisition_plan = pd.DataFrame(plan_rows, columns=["game_date", "immutable_prediction_rows",
        "immutable_prediction_timestamp_count", "requested_timestamp_utc", "request_id",
        "expected_credit_cost", "request_status"])
    resolved = risk[risk.risk_set_eligible.eq(1)].merge(outcomes, on="game_key", how="inner", validate="one_to_one") if len(risk) else pd.DataFrame()
    group_rows = []
    if len(resolved):
        for label, g in [("MARKET_STRONG_MODEL_AGREEMENT", resolved[resolved.agreement_indicator.eq(1)]),
                         ("MARKET_STRONG_WITHOUT_MODEL_AGREEMENT", resolved[resolved.agreement_indicator.eq(0)]),
                         *[(state, resolved[resolved.risk_state.eq(state)]) for state in sorted(resolved.risk_state.unique())]]:
            y = g.selected_side_win.to_numpy(); market = g.selected_market_probability.to_numpy()
            mb, ml = score(y, market) if len(g) else (math.nan, math.nan)
            market_brier_values = (market-y)**2 if len(g) else []
            clipped_market = np.clip(market, 1e-9, 1-1e-9) if len(g) else np.array([])
            market_log_values = (-y*np.log(clipped_market)-(1-y)*np.log(1-clipped_market)) if len(g) else []
            mb_lo, mb_hi = cluster_interval(g.game_date, market_brier_values) if len(g) else (math.nan, math.nan)
            ml_lo, ml_hi = cluster_interval(g.game_date, market_log_values) if len(g) else (math.nan, math.nan)
            model = g.selected_model_probability.dropna()
            aligned = g.loc[model.index] if len(model) else g.iloc[0:0]
            xb, xl = score(aligned.selected_side_win, model) if len(model) else (math.nan, math.nan)
            model_y = aligned.selected_side_win.to_numpy() if len(model) else np.array([])
            model_p = np.clip(model.to_numpy(), 1e-9, 1-1e-9) if len(model) else np.array([])
            xb_lo, xb_hi = cluster_interval(aligned.game_date, (model_p-model_y)**2) if len(model) else (math.nan, math.nan)
            xl_values = (-model_y*np.log(model_p)-(1-model_y)*np.log(1-model_p)) if len(model) else []
            xl_lo, xl_hi = cluster_interval(aligned.game_date, xl_values) if len(model) else (math.nan, math.nan)
            lo, hi = cluster_interval(g.game_date, y) if len(g) else (math.nan, math.nan)
            group_rows.append({"group": label, "unique_games": len(g), "wins": int(y.sum()) if len(g) else 0,
                               "losses": int(len(g)-y.sum()) if len(g) else 0, "win_rate": float(y.mean()) if len(g) else math.nan,
                               "win_rate_ci_2_5": lo, "win_rate_ci_97_5": hi, "market_brier": mb,
                               "market_brier_ci_2_5": mb_lo, "market_brier_ci_97_5": mb_hi,
                               "market_log_loss": ml, "market_log_loss_ci_2_5": ml_lo,
                               "market_log_loss_ci_97_5": ml_hi, "model_brier": xb,
                               "model_brier_ci_2_5": xb_lo, "model_brier_ci_97_5": xb_hi,
                               "model_log_loss": xl, "model_log_loss_ci_2_5": xl_lo,
                               "model_log_loss_ci_97_5": xl_hi})
    groups = pd.DataFrame(group_rows, columns=["group", "unique_games", "wins", "losses", "win_rate",
                                               "win_rate_ci_2_5", "win_rate_ci_97_5", "market_brier",
                                               "market_brier_ci_2_5", "market_brier_ci_97_5",
                                               "market_log_loss", "market_log_loss_ci_2_5",
                                               "market_log_loss_ci_97_5", "model_brier",
                                               "model_brier_ci_2_5", "model_brier_ci_97_5",
                                               "model_log_loss", "model_log_loss_ci_2_5",
                                               "model_log_loss_ci_97_5"])
    book_rows = []
    if len(resolved) and len(prices):
        priced = resolved.merge(prices[prices.price_state.eq("TWO_SIDED")], on="game_key", how="inner", validate="one_to_many")
        priced["flat_risk_return"] = np.where(priced.selected_side_win.eq(1), priced.selected_decimal_price-1, -1.0)
        priced["paid_break_even_excess"] = priced.selected_side_win-priced.selected_paid_break_even
        for book in BOOKS:
            b = priced[priced.bookmaker_key.eq(book)]
            for label, g in (("ALL_MARKET_STRONG", b), ("MODEL_AGREEMENT", b[b.agreement_indicator.eq(1)]),
                             ("WITHOUT_MODEL_AGREEMENT", b[b.agreement_indicator.eq(0)])):
                roi_lo, roi_hi = cluster_interval(g.game_date, g.flat_risk_return) if len(g) else (math.nan, math.nan)
                pbe_lo, pbe_hi = cluster_interval(g.game_date, g.paid_break_even_excess) if len(g) else (math.nan, math.nan)
                book_rows.append({"bookmaker_key": book, "group": label, "unique_games": g.game_key.nunique(),
                                  "wins": int(g.selected_side_win.sum()) if len(g) else 0,
                                  "flat_risk_roi": g.flat_risk_return.mean() if len(g) else math.nan,
                                  "roi_ci_2_5": roi_lo, "roi_ci_97_5": roi_hi,
                                  "paid_break_even_excess": g.paid_break_even_excess.mean() if len(g) else math.nan,
                                  "paid_break_even_excess_ci_2_5": pbe_lo, "paid_break_even_excess_ci_97_5": pbe_hi,
                                  "effective_outcomes": g.game_key.nunique()})
    books = pd.DataFrame(book_rows, columns=["bookmaker_key", "group", "unique_games", "wins", "flat_risk_roi",
                                              "roi_ci_2_5", "roi_ci_97_5", "paid_break_even_excess",
                                              "paid_break_even_excess_ci_2_5", "paid_break_even_excess_ci_97_5",
                                              "effective_outcomes"])
    incremental_rows = []
    if len(resolved) and len(prices):
        for book in BOOKS:
            b = priced[priced.bookmaker_key.eq(book)]
            roi = clustered_group_difference(b, "flat_risk_return")
            pbe = clustered_group_difference(b, "paid_break_even_excess")
            incremental_rows.append({"bookmaker_key": book,
                "agreement_games": int(b.agreement_indicator.eq(1).sum()),
                "without_agreement_games": int(b.agreement_indicator.eq(0).sum()),
                "roi_difference_agreement_minus_without": roi["difference"],
                "roi_difference_ci_2_5": roi["ci_2_5"], "roi_difference_ci_97_5": roi["ci_97_5"],
                "paid_break_even_excess_difference": pbe["difference"],
                "paid_break_even_excess_difference_ci_2_5": pbe["ci_2_5"],
                "paid_break_even_excess_difference_ci_97_5": pbe["ci_97_5"],
                "effective_unique_outcomes": int(b.game_key.nunique())})
    incremental_books = pd.DataFrame(incremental_rows, columns=["bookmaker_key", "agreement_games",
        "without_agreement_games", "roi_difference_agreement_minus_without", "roi_difference_ci_2_5",
        "roi_difference_ci_97_5", "paid_break_even_excess_difference",
        "paid_break_even_excess_difference_ci_2_5", "paid_break_even_excess_difference_ci_97_5",
        "effective_unique_outcomes"])
    coverage_rows = []
    for book in BOOKS:
        b = prices[prices.bookmaker_key.eq(book)] if len(prices) else prices
        for state in ("TWO_SIDED", "BOOKMAKER_ABSENT", "PRICE_UNAVAILABLE", "ONE_SIDED_MARKET",
                      "STALE_OR_POST_START"):
            coverage_rows.append({"bookmaker_key": book, "price_state": state,
                                  "cells": int(b.price_state.eq(state).sum()) if len(b) else 0,
                                  "risk_set_rows": len(risk),
                                  "coverage_rate": (float(b.price_state.eq(state).sum()/len(risk))
                                                    if len(risk) else math.nan)})
    coverage = pd.DataFrame(coverage_rows)
    regime_rows = []
    if len(resolved):
        for (regime_name, indicator), g in resolved.groupby(["late_season_regime", "agreement_indicator"], sort=True):
            lo, hi = cluster_interval(g.game_date, g.selected_side_win)
            regime_rows.append({"late_season_regime": regime_name, "agreement_indicator": int(indicator),
                                "unique_games": g.game_key.nunique(), "wins": int(g.selected_side_win.sum()),
                                "win_rate": float(g.selected_side_win.mean()),
                                "win_rate_ci_2_5": lo, "win_rate_ci_97_5": hi})
    regime_metrics = pd.DataFrame(regime_rows, columns=["late_season_regime", "agreement_indicator",
        "unique_games", "wins", "win_rate", "win_rate_ci_2_5", "win_rate_ci_97_5"])
    contrast = {"win_rate_difference": math.nan, "ci_2_5": math.nan, "ci_97_5": math.nan}
    if len(resolved) and resolved.agreement_indicator.nunique() == 2:
        by_date = resolved.groupby(["game_date", "agreement_indicator"]).selected_side_win.agg(["sum", "count"]).reset_index()
        dates = sorted(resolved.game_date.unique()); rng = np.random.default_rng(SEED + len(resolved)); values = []
        for _ in range(BOOT_REPS):
            sample_dates = rng.choice(dates, len(dates), replace=True); sample = pd.concat([resolved[resolved.game_date.eq(x)] for x in sample_dates])
            if sample.agreement_indicator.nunique() == 2:
                values.append(sample.loc[sample.agreement_indicator.eq(1), "selected_side_win"].mean()-sample.loc[sample.agreement_indicator.eq(0), "selected_side_win"].mean())
        point = (resolved.loc[resolved.agreement_indicator.eq(1), "selected_side_win"].mean()
                 - resolved.loc[resolved.agreement_indicator.eq(0), "selected_side_win"].mean())
        contrast = {"win_rate_difference": float(point), "ci_2_5": float(np.quantile(values, .025)) if values else math.nan,
                    "ci_97_5": float(np.quantile(values, .975)) if values else math.nan}
    folds, score_diffs, blocked = blocked_analysis(resolved)
    coefficient = agreement_coefficient_interval(resolved)
    agreement_n = int(resolved.agreement_indicator.eq(1).sum()) if len(resolved) else 0
    no_agreement_n = int(resolved.agreement_indicator.eq(0).sum()) if len(resolved) else 0
    horizon_complete = date.today() >= date(2027, 1, 1)
    gate = {"prospective_horizon_complete": horizon_complete,
            "eligible_resolved_games": len(resolved) >= MINIMUM_EVIDENCE["eligible_resolved_games"],
            "agreement_games": agreement_n >= MINIMUM_EVIDENCE["agreement_games"],
            "no_agreement_games": no_agreement_n >= MINIMUM_EVIDENCE["no_agreement_games"],
            "resolved_dates": (resolved.game_date.nunique() if len(resolved) else 0) >= MINIMUM_EVIDENCE["resolved_dates"],
            "out_of_time_scored_games": blocked["out_of_time_games"] >= MINIMUM_EVIDENCE["out_of_time_scored_games"]}
    evidence_met = all(gate.values()); classification = "SEPARATION_EVIDENCE_INSUFFICIENT"; decision_made = False
    # Frozen terminal rules are evaluated only after every evidence gate is met.
    if evidence_met and len(score_diffs) == 2 and len(folds):
        predictive = coefficient["ci_2_5"] > 0 and score_diffs.ci_97_5.lt(0).all() and folds.agreement_coefficient.mean() > 0
        pin = incremental_books[incremental_books.bookmaker_key.eq(REFERENCE_BOOK)] if len(incremental_books) else pd.DataFrame()
        economic = False
        if len(pin) == 1:
            directions = incremental_books.roi_difference_agreement_minus_without.gt(0).fillna(False)
            economic = bool(pin.iloc[0].roi_difference_ci_2_5 > 0
                            and pin.iloc[0].paid_break_even_excess_difference_ci_2_5 > 0
                            and int(directions.sum()) >= 8)
        harmful = contrast["ci_97_5"] < 0 or score_diffs.ci_2_5.gt(0).all()
        equivalent = contrast["ci_2_5"] >= -.03 and contrast["ci_97_5"] <= .03 and score_diffs.loc[
            score_diffs.score.eq("BRIER"), "ci_2_5"].ge(-.002).all() and score_diffs.loc[
            score_diffs.score.eq("BRIER"), "ci_97_5"].le(.002).all()
        overall_excess = books.loc[books.group.eq("ALL_MARKET_STRONG"), "paid_break_even_excess"].mean() if len(books) else math.nan
        if predictive and economic: classification = "MODEL_AGREEMENT_INCREMENTAL_VALUE_SUPPORTED"; decision_made = True
        elif harmful: classification = "MODEL_AGREEMENT_INCREMENTAL_VALUE_NOT_SUPPORTED"; decision_made = True
        elif equivalent and overall_excess > 0: classification = "MARKET_STRENGTH_EXPLAINS_COHORT"; decision_made = True
    if classification not in FINAL_CATEGORIES: raise RuntimeError("Invalid decision category")
    regime = risk.groupby("late_season_regime").size().to_dict() if len(risk) else {}
    today = date.today().isoformat()
    horizon_status = ("NOT_STARTED" if today < PROSPECTIVE_START else
                      "CLOSED" if horizon_complete else "ACTIVE_REMAINDER_OF_2026")
    summary = {"study_id": freeze["study_id"], "report_status": "INTERIM_DESCRIPTIVE" if not decision_made else "FINAL_DECISION",
               "classification": classification, "decision_made": decision_made, "evidence_requirement_met": evidence_met,
               "evidence_gate": gate, "risk_rows": len(risk), "eligible_resolved_unique_games": len(resolved),
               "agreement_resolved_games": agreement_n, "without_agreement_resolved_games": no_agreement_n,
               "bookmaker_price_cells": len(prices), "effective_outcome_count": len(resolved),
               "planned_request_dates": len(acquisition_plan),
               "planned_expected_credit_cost": int(len(acquisition_plan) * EXPECTED_COST_PER_DATE),
               "bookmaker_cells_do_not_inflate_effective_sample_size": True, "primary_contrast": contrast,
               "blocked_out_of_time_games": blocked["out_of_time_games"],
               "agreement_coefficient_clustered": coefficient, "late_season_regime_counts": regime,
               "prospective_horizon_status": horizon_status,
               "prior_56_20_rows_included": False, "book_selected_by_roi": False, "best_price_composite": False,
               "user_access_or_fillability": "UNKNOWN_NOT_ESTABLISHED", "wagering": False, "production_changes": False,
               "scheduler_changes": False, "public_prediction_changes": False}
    output.mkdir(parents=True, exist_ok=True)
    risk.to_csv(output/"prospective_risk_set_ledger.csv", index=False, lineterminator="\n")
    acquisition_plan.to_csv(output/"prospective_date_request_plan.csv", index=False, lineterminator="\n")
    prices.to_csv(output/"bookmaker_price_ledger.csv", index=False, lineterminator="\n")
    outcomes.to_csv(output/"outcome_ledger.csv", index=False, lineterminator="\n")
    groups.to_csv(output/"outcome_and_probability_metrics.csv", index=False, lineterminator="\n")
    books.to_csv(output/"bookmaker_economics.csv", index=False, lineterminator="\n")
    incremental_books.to_csv(output/"bookmaker_incremental_economics.csv", index=False, lineterminator="\n")
    coverage.to_csv(output/"bookmaker_price_state_coverage.csv", index=False, lineterminator="\n")
    regime_metrics.to_csv(output/"late_season_regime_metrics.csv", index=False, lineterminator="\n")
    folds.to_csv(output/"blocked_date_agreement_coefficients.csv", index=False, lineterminator="\n")
    score_diffs.to_csv(output/"blocked_date_score_differences.csv", index=False, lineterminator="\n")
    summary = json_clean(summary)
    (output/"summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n")
    lines = ["# MLB market-strong agreement separation — prospective v1", "", f"**{classification}**", "",
             "This is an interim descriptive report. No post-freeze game has been backfilled, and no decision is made until every frozen evidence gate is met." if not decision_made else "Every frozen evidence gate was met and the terminal rule was applied.", "",
             f"Risk-set rows: {len(risk)}; eligible resolved unique games: {len(resolved)}; agreement/non-agreement: {agreement_n}/{no_agreement_n}. Bookmaker cells never change the effective outcome count.", "",
             f"Prospective horizon status: {horizon_status}. Late-season evidence is reported separately by frozen calendar regime and is not pooled with the prior 56–20 cohort.", "",
             "The observer is manual. Acquisition requires a separately authorized immutable credit ceiling; initialization, prediction ingestion, grading, and reporting do not request odds.", "",
             "No wagering, production, scheduler, public-prediction, threshold, model, or bookmaker-selection change was made."]
    (output/"interim_report.md").write_text("\n".join(lines)+"\n")
    return summary


def write_manifest(output: Path) -> None:
    files = sorted(path for path in output.rglob("*") if path.is_file() and path.name != "sha256_manifest.txt")
    (output/"sha256_manifest.txt").write_text("".join(f"{file_sha(path)}  {path.relative_to(output)}\n" for path in files))


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--ledger", type=Path, default=LEDGER); parser.add_argument("--initialize", action="store_true")
    parser.add_argument("--ingest-predictions", action="store_true"); parser.add_argument("--acquire-date")
    parser.add_argument("--reconcile-date")
    parser.add_argument("--grade", action="store_true"); parser.add_argument("--report", action="store_true")
    parser.add_argument("--through-date", default=date.today().isoformat()); parser.add_argument("--authorized-credit-ceiling", type=int)
    parser.add_argument("--timeout", type=int, default=60); args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT/args.output
    ledger = args.ledger if args.ledger.is_absolute() else ROOT/args.ledger
    result: dict[str, Any] = {}
    if args.initialize: result["freeze"] = initialize(output, ledger)
    if args.ingest_predictions: result["predictions"] = ingest_predictions(output, ledger, args.through_date)
    if args.acquire_date:
        if args.authorized_credit_ceiling is None: raise RuntimeError("--authorized-credit-ceiling is required for acquisition")
        result["acquisition"] = acquire_date(output, ledger, args.acquire_date, args.authorized_credit_ceiling, args.timeout)
    if args.reconcile_date:
        result["reconciliation"] = reconcile_successful_response(output, ledger, args.reconcile_date)
    if args.grade: result["grading"] = grade(output, ledger, args.through_date)
    if args.report or args.initialize: result["report"] = report(output, ledger)
    if not any((args.initialize, args.ingest_predictions, args.acquire_date, args.reconcile_date,
                args.grade, args.report)):
        parser.error("choose at least one action")
    write_manifest(output); print(json.dumps(json_clean(result), indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    try: main()
    except (KeyboardInterrupt, SystemExit): raise
    except BaseException as exc: raise SystemExit(hardened.render_exception(exc)) from None
