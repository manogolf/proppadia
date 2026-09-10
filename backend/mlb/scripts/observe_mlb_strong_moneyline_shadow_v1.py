#!/usr/bin/env python3
"""Manual append-only observer for the unchanged MLB moneyline STRONG cohort.

This utility reads existing canonical predictions, outcomes, and captured market
history.  It does not fetch data, refit the model, publish picks, place wagers,
or install a scheduler.  Eligible prices must be captured after the immutable
prediction timestamp and strictly before first pitch.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import audit_mlb_moneyline_probability_region_premise_v1 as prior


def repository_root() -> Path:
    marker = Path("backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json")
    for candidate in (Path.cwd().resolve(), *Path(__file__).resolve().parents):
        if (candidate / marker).exists():
            return candidate
    raise RuntimeError("run the observer from the repository or keep it within the repository tree")


ROOT = repository_root()
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/strong_moneyline_shadow_v1/strong_moneyline_shadow_v1.sqlite3"
DEFAULT_REPORT = ROOT / "artifacts/analysis/model_development/mlb_strong_moneyline_shadow_v1/latest_observer_report.json"
BASELINE_CUTOFF = "2026-09-08"
STRONG_BOUNDARY = .60
CHECKPOINTS = {"ORDINARY", "END_REGULAR_SEASON", "FINAL_2026"}


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode()).hexdigest()


def american_decimal(value: float) -> float:
    return 1 + (float(value) / 100 if float(value) > 0 else 100 / -float(value))


def schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
      PRAGMA foreign_keys=ON;
      CREATE TABLE IF NOT EXISTS observer_metadata (
        observer_id TEXT PRIMARY KEY, model_version TEXT NOT NULL, model_hash TEXT NOT NULL,
        snapshot_class TEXT NOT NULL, admission_status TEXT NOT NULL,
        strong_boundary REAL NOT NULL, baseline_cutoff TEXT NOT NULL,
        created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TABLE IF NOT EXISTS strong_predictions (
        canonical_identity TEXT PRIMARY KEY, game_date TEXT NOT NULL, game_id INTEGER NOT NULL,
        scheduled_start_utc TEXT NOT NULL, prediction_timestamp_utc TEXT NOT NULL,
        prediction_cutoff_utc TEXT NOT NULL, home_team TEXT NOT NULL, away_team TEXT NOT NULL,
        selected_team TEXT NOT NULL, selected_side TEXT NOT NULL,
        selected_model_probability REAL NOT NULL, prediction_payload_sha256 TEXT NOT NULL,
        observer_row_sha256 TEXT NOT NULL,
        UNIQUE(game_date,game_id)
      );
      CREATE TABLE IF NOT EXISTS strong_outcomes (
        canonical_identity TEXT PRIMARY KEY, game_date TEXT NOT NULL, game_id INTEGER NOT NULL,
        official_winner TEXT NOT NULL, selected_win INTEGER NOT NULL,
        outcome_payload_sha256 TEXT NOT NULL, observer_row_sha256 TEXT NOT NULL,
        FOREIGN KEY(canonical_identity) REFERENCES strong_predictions(canonical_identity)
      );
      CREATE TABLE IF NOT EXISTS postfreeze_prices (
        canonical_market_identity TEXT PRIMARY KEY, canonical_prediction_identity TEXT NOT NULL,
        game_date TEXT NOT NULL, game_id INTEGER NOT NULL, provider TEXT NOT NULL,
        bookmaker_key TEXT NOT NULL, captured_at_utc TEXT NOT NULL,
        provider_market_updated_at_utc TEXT NOT NULL, scheduled_start_utc TEXT NOT NULL,
        selected_american_price REAL NOT NULL, selected_decimal_price REAL NOT NULL,
        selected_implied_probability REAL NOT NULL, selected_no_vig_probability REAL NOT NULL,
        selected_paid_break_even_probability REAL NOT NULL, model_implied_expected_return REAL NOT NULL,
        market_side_status TEXT NOT NULL, model_market_state TEXT NOT NULL,
        market_payload_sha256 TEXT NOT NULL, raw_source_sha256 TEXT,
        observer_row_sha256 TEXT NOT NULL,
        FOREIGN KEY(canonical_prediction_identity) REFERENCES strong_predictions(canonical_identity)
      );
      CREATE TABLE IF NOT EXISTS observer_runs (
        run_identity TEXT PRIMARY KEY, observed_through_date TEXT NOT NULL,
        checkpoint TEXT NOT NULL, strong_prediction_rows INTEGER NOT NULL,
        resolved_rows INTEGER NOT NULL, postfreeze_price_rows INTEGER NOT NULL,
        report_sha256 TEXT NOT NULL, recorded_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TRIGGER IF NOT EXISTS strong_predictions_no_update BEFORE UPDATE ON strong_predictions
        BEGIN SELECT RAISE(ABORT,'append-only: strong_predictions update'); END;
      CREATE TRIGGER IF NOT EXISTS strong_predictions_no_delete BEFORE DELETE ON strong_predictions
        BEGIN SELECT RAISE(ABORT,'append-only: strong_predictions delete'); END;
      CREATE TRIGGER IF NOT EXISTS strong_outcomes_no_update BEFORE UPDATE ON strong_outcomes
        BEGIN SELECT RAISE(ABORT,'append-only: strong_outcomes update'); END;
      CREATE TRIGGER IF NOT EXISTS strong_outcomes_no_delete BEFORE DELETE ON strong_outcomes
        BEGIN SELECT RAISE(ABORT,'append-only: strong_outcomes delete'); END;
      CREATE TRIGGER IF NOT EXISTS postfreeze_prices_no_update BEFORE UPDATE ON postfreeze_prices
        BEGIN SELECT RAISE(ABORT,'append-only: postfreeze_prices update'); END;
      CREATE TRIGGER IF NOT EXISTS postfreeze_prices_no_delete BEFORE DELETE ON postfreeze_prices
        BEGIN SELECT RAISE(ABORT,'append-only: postfreeze_prices delete'); END;
      CREATE TRIGGER IF NOT EXISTS observer_metadata_no_update BEFORE UPDATE ON observer_metadata
        BEGIN SELECT RAISE(ABORT,'append-only: observer_metadata update'); END;
      CREATE TRIGGER IF NOT EXISTS observer_metadata_no_delete BEFORE DELETE ON observer_metadata
        BEGIN SELECT RAISE(ABORT,'append-only: observer_metadata delete'); END;
      CREATE TRIGGER IF NOT EXISTS observer_runs_no_update BEFORE UPDATE ON observer_runs
        BEGIN SELECT RAISE(ABORT,'append-only: observer_runs update'); END;
      CREATE TRIGGER IF NOT EXISTS observer_runs_no_delete BEFORE DELETE ON observer_runs
        BEGIN SELECT RAISE(ABORT,'append-only: observer_runs delete'); END;
    """)


def immutable_insert(conn: sqlite3.Connection, table: str, key: str, row: dict[str, Any]) -> bool:
    columns = list(row)
    sql = f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})"
    before = conn.total_changes
    conn.execute(sql, [row[c] for c in columns])
    if conn.total_changes > before:
        return True
    existing = conn.execute(f"SELECT observer_row_sha256 FROM {table} WHERE {key}=?", (row[key],)).fetchone()
    if existing is None or existing[0] != row["observer_row_sha256"]:
        raise RuntimeError(f"immutable conflict in {table}: {row[key]}")
    return False


def canonical_predictions(through_date: str) -> pd.DataFrame:
    d = prior.load_predictions(through_date)
    d["selected_model_probability"] = d[["home_win_probability", "away_win_probability"]].max(axis=1)
    d["selected_side"] = np.where(d.home_win_probability.ge(.5), "HOME", "AWAY")
    d["selected_team"] = np.where(d.selected_side.eq("HOME"), d.home_team, d.away_team)
    d = d[d.selected_model_probability.gt(STRONG_BOUNDARY)].copy()
    d["canonical_identity"] = d.apply(
        lambda r: f"{r.game_date}|{int(r.game_id)}|{prior.MODEL}|{prior.SNAPSHOT}", axis=1
    )
    return d.sort_values(["game_date", "game_id"])


def eligible_prices(predictions: pd.DataFrame) -> list[dict[str, Any]]:
    if predictions.empty:
        return []
    conn = sqlite3.connect(prior.MARKET_DB)
    raw = pd.read_sql_query("""
      SELECT canonical_market_identity,provider,bookmaker_key,game_date,game_id,
             captured_at_utc,scheduled_start_utc,timing_status,market_payload_json,
             market_payload_sha256,raw_source_sha256
      FROM supplemental_main_market_snapshots
      WHERE market_type='MONEYLINE' AND game_date>=? AND game_date<=?
      ORDER BY game_date,game_id,bookmaker_key,captured_at_utc,canonical_market_identity
    """, conn, params=(predictions.game_date.min(), predictions.game_date.max()))
    conn.close()
    if raw.empty:
        return []
    pred = predictions.set_index("game_id")
    output: list[dict[str, Any]] = []
    for r in raw.itertuples(index=False):
        if int(r.game_id) not in pred.index or r.timing_status != "PREGAME_CERTIFIED":
            continue
        p = pred.loc[int(r.game_id)]
        captured = pd.to_datetime(r.captured_at_utc, utc=True, errors="coerce")
        start = pd.to_datetime(r.scheduled_start_utc, utc=True, errors="coerce")
        frozen = pd.to_datetime(p.prediction_timestamp_utc, utc=True, errors="coerce")
        payload = json.loads(r.market_payload_json)
        updated = pd.to_datetime(payload.get("provider_market_updated_at_utc"), utc=True, errors="coerce")
        if any(pd.isna(x) for x in (captured, start, frozen, updated)):
            continue
        if not (captured >= frozen and captured < start and updated < start):
            continue
        side = p.selected_side.lower()
        price = payload.get(f"{side}_american_price")
        implied = payload.get(f"{side}_implied_probability")
        no_vig = payload.get(f"no_vig_{side}_probability")
        if price is None or implied is None or no_vig is None:
            continue
        decimal = american_decimal(float(price))
        values = {
            "canonical_market_identity": r.canonical_market_identity,
            "canonical_prediction_identity": p.canonical_identity,
            "game_date": r.game_date, "game_id": int(r.game_id), "provider": r.provider,
            "bookmaker_key": r.bookmaker_key, "captured_at_utc": str(r.captured_at_utc),
            "provider_market_updated_at_utc": str(payload["provider_market_updated_at_utc"]),
            "scheduled_start_utc": str(r.scheduled_start_utc),
            "selected_american_price": float(price), "selected_decimal_price": decimal,
            "selected_implied_probability": float(implied), "selected_no_vig_probability": float(no_vig),
            "selected_paid_break_even_probability": 1 / decimal,
            "model_implied_expected_return": float(p.selected_model_probability) * decimal - 1,
            "market_side_status": "MARKET_FAVORITE" if float(no_vig) > .5 else
                                  "MARKET_UNDERDOG" if float(no_vig) < .5 else "MARKET_PICKEM",
            "model_market_state": "MODEL_MARKET_AGREE_FAVORITE" if float(no_vig) > .5 else
                                  "MODEL_OPPOSES_MARKET_FAVORITE" if float(no_vig) < .5 else "MARKET_PICKEM",
            "market_payload_sha256": r.market_payload_sha256,
            "raw_source_sha256": r.raw_source_sha256,
        }
        values["observer_row_sha256"] = digest(values)
        output.append(values)
    return output


def report_payload(conn: sqlite3.Connection, baseline: pd.DataFrame, through_date: str,
                   checkpoint: str, inserted: dict[str, int]) -> dict[str, Any]:
    pred = pd.read_sql_query("SELECT * FROM strong_predictions ORDER BY game_date,game_id", conn)
    outcomes = pd.read_sql_query("SELECT * FROM strong_outcomes ORDER BY game_date,game_id", conn)
    prices = pd.read_sql_query("SELECT * FROM postfreeze_prices ORDER BY game_date,game_id,bookmaker_key,captured_at_utc", conn)
    extension = pred[pred.game_date.gt(BASELINE_CUTOFF)]
    ext_outcomes = outcomes[outcomes.game_date.gt(BASELINE_CUTOFF)]
    designated = prices.sort_values(["bookmaker_key", "game_id", "captured_at_utc", "canonical_market_identity"]).groupby(
        ["bookmaker_key", "game_id"], sort=True
    ).head(1) if len(prices) else prices
    book_rows = []
    if len(designated) and len(outcomes):
        joined = designated.merge(outcomes[["game_id", "selected_win"]], on="game_id", how="inner")
        joined["flat_stake_return"] = np.where(joined.selected_win.eq(1), joined.selected_decimal_price - 1, -1)
        for book, g in joined.groupby("bookmaker_key", sort=True):
            book_rows.append({"bookmaker_key": book, "resolved_priced_games": len(g),
                              "wins": int(g.selected_win.sum()), "roi": float(g.flat_stake_return.mean()),
                              "selection_rule": "EARLIEST_ELIGIBLE_POSTFREEZE_PREGAME_QUOTE"})
    baseline_resolved = baseline[baseline.resolved].copy()
    baseline_wins = int(baseline_resolved.prediction_correct.astype(bool).sum())
    extension_count = len(extension)
    combined_resolved = len(baseline_resolved) + len(ext_outcomes)
    combined_wins = baseline_wins + (int(ext_outcomes.selected_win.sum()) if len(ext_outcomes) else 0)
    return {
        "observer_status": "MANUAL_APPEND_ONLY_SHADOW_OBSERVER",
        "observed_through_date": through_date, "checkpoint": checkpoint,
        "strong_definition": "selected probability > 0.60 strict",
        "model_version": prior.MODEL, "snapshot_class": prior.SNAPSHOT,
        "baseline": {"cutoff": BASELINE_CUTOFF, "resolved": len(baseline_resolved),
                     "wins": baseline_wins, "losses": len(baseline_resolved) - baseline_wins},
        "extension": {"predictions": extension_count, "resolved": len(ext_outcomes),
                      "wins": int(ext_outcomes.selected_win.sum()) if len(ext_outcomes) else 0,
                      "losses": int(len(ext_outcomes) - ext_outcomes.selected_win.sum()) if len(ext_outcomes) else 0,
                      "next_20_selection_checkpoint": ((extension_count // 20) + 1) * 20},
        "combined_baseline_plus_extension": {"resolved": combined_resolved, "wins": combined_wins,
                                               "losses": combined_resolved - combined_wins},
        "ledger_totals": {"strong_predictions": len(pred), "resolved_outcomes": len(outcomes),
                          "all_eligible_postfreeze_quotes": len(prices)},
        "inserted_this_run": inserted, "book_economics": book_rows,
        "constraints": {"network_capture": False, "refit": False, "wagering": False,
                        "publishing": False, "scheduler": False,
                        "price_timing": "captured_at >= immutable prediction timestamp and strictly pregame"},
    }


def run(ledger: Path, report: Path, baseline_cutoff: str, through_date: str, checkpoint: str) -> dict[str, Any]:
    if baseline_cutoff != BASELINE_CUTOFF:
        raise ValueError(f"v1 baseline is immutable at {BASELINE_CUTOFF}")
    config = json.loads(prior.CONFIG.read_text())
    all_strong = canonical_predictions(through_date)
    if set(all_strong.model_hash.dropna()) != {config["model_hash"]}:
        raise RuntimeError("canonical prediction model hash does not match the frozen observer contract")
    baseline = all_strong[all_strong.game_date.le(baseline_cutoff)].copy()
    extension = all_strong[all_strong.game_date.gt(baseline_cutoff)].copy()
    ledger.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    inserted = {"predictions": 0, "outcomes": 0, "prices": 0}
    with sqlite3.connect(ledger) as conn:
        schema(conn)
        meta = {"observer_id": "MLB_STRONG_MONEYLINE_SHADOW_V1", "model_version": prior.MODEL,
                "model_hash": config["model_hash"], "snapshot_class": prior.SNAPSHOT,
                "admission_status": prior.ADMISSION, "strong_boundary": STRONG_BOUNDARY,
                "baseline_cutoff": baseline_cutoff}
        columns = list(meta)
        conn.execute(f"INSERT OR IGNORE INTO observer_metadata ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", [meta[c] for c in columns])
        existing_meta = conn.execute("SELECT model_hash,baseline_cutoff,strong_boundary FROM observer_metadata WHERE observer_id=?", (meta["observer_id"],)).fetchone()
        if existing_meta != (meta["model_hash"], baseline_cutoff, STRONG_BOUNDARY):
            raise RuntimeError("observer identity conflict")
        for r in extension.itertuples(index=False):
            values = {"canonical_identity": r.canonical_identity, "game_date": r.game_date,
                      "game_id": int(r.game_id), "scheduled_start_utc": r.scheduled_start_utc.isoformat(),
                      "prediction_timestamp_utc": r.prediction_timestamp_utc.isoformat(),
                      "prediction_cutoff_utc": r.prediction_cutoff_utc.isoformat(),
                      "home_team": r.home_team, "away_team": r.away_team,
                      "selected_team": r.selected_team, "selected_side": r.selected_side,
                      "selected_model_probability": float(r.selected_model_probability),
                      "prediction_payload_sha256": r.prediction_payload_sha256}
            values["observer_row_sha256"] = digest(values)
            inserted["predictions"] += int(immutable_insert(conn, "strong_predictions", "canonical_identity", values))
            if r.resolved:
                outcome = {"canonical_identity": r.canonical_identity, "game_date": r.game_date,
                           "game_id": int(r.game_id), "official_winner": r.official_winner,
                           "selected_win": int(bool(r.prediction_correct)),
                           "outcome_payload_sha256": r.outcome_payload_sha256}
                outcome["observer_row_sha256"] = digest(outcome)
                inserted["outcomes"] += int(immutable_insert(conn, "strong_outcomes", "canonical_identity", outcome))
        for values in eligible_prices(extension):
            inserted["prices"] += int(immutable_insert(conn, "postfreeze_prices", "canonical_market_identity", values))
        payload = report_payload(conn, baseline, through_date, checkpoint, inserted)
        report_text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        report.write_text(report_text)
        run_identity = digest({"through_date": through_date, "checkpoint": checkpoint,
                               "report_sha256": hashlib.sha256(report_text.encode()).hexdigest()})
        conn.execute("INSERT OR IGNORE INTO observer_runs (run_identity,observed_through_date,checkpoint,strong_prediction_rows,resolved_rows,postfreeze_price_rows,report_sha256) VALUES (?,?,?,?,?,?,?)",
                     (run_identity, through_date, checkpoint, payload["ledger_totals"]["strong_predictions"],
                      payload["ledger_totals"]["resolved_outcomes"], payload["ledger_totals"]["all_eligible_postfreeze_quotes"],
                      hashlib.sha256(report_text.encode()).hexdigest()))
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--baseline-cutoff", default=BASELINE_CUTOFF)
    parser.add_argument("--through-date", default=date.today().isoformat())
    parser.add_argument("--checkpoint", choices=sorted(CHECKPOINTS), default="ORDINARY")
    args = parser.parse_args()
    ledger = args.ledger if args.ledger.is_absolute() else ROOT / args.ledger
    report = args.report if args.report.is_absolute() else ROOT / args.report
    print(json.dumps(run(ledger, report, args.baseline_cutoff, args.through_date, args.checkpoint), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
