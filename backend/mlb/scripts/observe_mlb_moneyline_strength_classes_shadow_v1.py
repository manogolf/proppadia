#!/usr/bin/env python3
"""Manual append-only observer for every frozen MLB moneyline strength class."""
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
        if (candidate / marker).exists(): return candidate
    raise RuntimeError("run from the repository or preserve this script within its tree")


ROOT = repository_root()
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/moneyline_strength_classes_shadow_v1/moneyline_strength_classes_shadow_v1.sqlite3"
DEFAULT_REPORT = ROOT / "artifacts/analysis/model_development/mlb_moneyline_strength_classes_shadow_v1/latest_observer_report.json"
BASELINE_CUTOFF = "2026-09-08"
BOUNDARY = .60
CHECKPOINTS = {"ORDINARY", "END_REGULAR_SEASON", "FINAL_2026"}
CLASSES = {"JOINT_STRONG_SAME_SIDE", "MODEL_STRONG_ONLY", "MARKET_STRONG_ONLY", "STRONG_CONFLICT", "NEITHER_STRONG"}


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode()).hexdigest()


def decimal_price(american: float) -> float:
    return 1 + (float(american)/100 if float(american) > 0 else 100/-float(american))


def schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
      PRAGMA foreign_keys=ON;
      CREATE TABLE IF NOT EXISTS observer_metadata (
        observer_id TEXT PRIMARY KEY, model_version TEXT NOT NULL, model_hash TEXT NOT NULL,
        snapshot_class TEXT NOT NULL, admission_status TEXT NOT NULL, strong_boundary REAL NOT NULL,
        baseline_cutoff TEXT NOT NULL, created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TABLE IF NOT EXISTS game_predictions (
        canonical_identity TEXT PRIMARY KEY, game_date TEXT NOT NULL, game_id INTEGER NOT NULL,
        scheduled_start_utc TEXT NOT NULL, prediction_timestamp_utc TEXT NOT NULL,
        prediction_cutoff_utc TEXT NOT NULL, home_team TEXT NOT NULL, away_team TEXT NOT NULL,
        home_win_probability REAL NOT NULL, away_win_probability REAL NOT NULL,
        model_strong_side TEXT NOT NULL, prediction_payload_sha256 TEXT NOT NULL,
        observer_row_sha256 TEXT NOT NULL, UNIQUE(game_date,game_id)
      );
      CREATE TABLE IF NOT EXISTS game_outcomes (
        canonical_identity TEXT PRIMARY KEY, game_date TEXT NOT NULL, game_id INTEGER NOT NULL,
        official_winner TEXT NOT NULL, home_win INTEGER NOT NULL, outcome_payload_sha256 TEXT NOT NULL,
        observer_row_sha256 TEXT NOT NULL,
        FOREIGN KEY(canonical_identity) REFERENCES game_predictions(canonical_identity)
      );
      CREATE TABLE IF NOT EXISTS sportsbook_quotes (
        canonical_market_identity TEXT PRIMARY KEY, canonical_prediction_identity TEXT NOT NULL,
        game_date TEXT NOT NULL, game_id INTEGER NOT NULL, provider TEXT NOT NULL,
        bookmaker_key TEXT NOT NULL, captured_at_utc TEXT NOT NULL,
        provider_market_updated_at_utc TEXT NOT NULL, scheduled_start_utc TEXT NOT NULL,
        home_american_price REAL NOT NULL, away_american_price REAL NOT NULL,
        home_decimal_price REAL NOT NULL, away_decimal_price REAL NOT NULL,
        no_vig_home_probability REAL NOT NULL, no_vig_away_probability REAL NOT NULL,
        model_strong_side TEXT NOT NULL, market_strong_side TEXT NOT NULL,
        strength_class TEXT NOT NULL, market_payload_sha256 TEXT NOT NULL,
        raw_source_sha256 TEXT, observer_row_sha256 TEXT NOT NULL,
        FOREIGN KEY(canonical_prediction_identity) REFERENCES game_predictions(canonical_identity)
      );
      CREATE TABLE IF NOT EXISTS observer_runs (
        run_identity TEXT PRIMARY KEY, observed_through_date TEXT NOT NULL, checkpoint TEXT NOT NULL,
        prediction_rows INTEGER NOT NULL, outcome_rows INTEGER NOT NULL, quote_rows INTEGER NOT NULL,
        report_sha256 TEXT NOT NULL, recorded_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TRIGGER IF NOT EXISTS game_predictions_no_update BEFORE UPDATE ON game_predictions BEGIN SELECT RAISE(ABORT,'append-only: game_predictions update'); END;
      CREATE TRIGGER IF NOT EXISTS game_predictions_no_delete BEFORE DELETE ON game_predictions BEGIN SELECT RAISE(ABORT,'append-only: game_predictions delete'); END;
      CREATE TRIGGER IF NOT EXISTS game_outcomes_no_update BEFORE UPDATE ON game_outcomes BEGIN SELECT RAISE(ABORT,'append-only: game_outcomes update'); END;
      CREATE TRIGGER IF NOT EXISTS game_outcomes_no_delete BEFORE DELETE ON game_outcomes BEGIN SELECT RAISE(ABORT,'append-only: game_outcomes delete'); END;
      CREATE TRIGGER IF NOT EXISTS sportsbook_quotes_no_update BEFORE UPDATE ON sportsbook_quotes BEGIN SELECT RAISE(ABORT,'append-only: sportsbook_quotes update'); END;
      CREATE TRIGGER IF NOT EXISTS sportsbook_quotes_no_delete BEFORE DELETE ON sportsbook_quotes BEGIN SELECT RAISE(ABORT,'append-only: sportsbook_quotes delete'); END;
      CREATE TRIGGER IF NOT EXISTS observer_metadata_no_update BEFORE UPDATE ON observer_metadata BEGIN SELECT RAISE(ABORT,'append-only: observer_metadata update'); END;
      CREATE TRIGGER IF NOT EXISTS observer_metadata_no_delete BEFORE DELETE ON observer_metadata BEGIN SELECT RAISE(ABORT,'append-only: observer_metadata delete'); END;
      CREATE TRIGGER IF NOT EXISTS observer_runs_no_update BEFORE UPDATE ON observer_runs BEGIN SELECT RAISE(ABORT,'append-only: observer_runs update'); END;
      CREATE TRIGGER IF NOT EXISTS observer_runs_no_delete BEFORE DELETE ON observer_runs BEGIN SELECT RAISE(ABORT,'append-only: observer_runs delete'); END;
    """)


def immutable_insert(conn: sqlite3.Connection, table: str, key: str, row: dict[str, Any]) -> bool:
    columns = list(row)
    before = conn.total_changes
    conn.execute(f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", [row[c] for c in columns])
    if conn.total_changes > before: return True
    existing = conn.execute(f"SELECT observer_row_sha256 FROM {table} WHERE {key}=?", (row[key],)).fetchone()
    if existing is None or existing[0] != row["observer_row_sha256"]: raise RuntimeError(f"immutable conflict in {table}: {row[key]}")
    return False


def predictions(through: str) -> pd.DataFrame:
    d = prior.load_predictions(through)
    d["model_strong_side"] = np.select([d.home_win_probability.gt(BOUNDARY), d.away_win_probability.gt(BOUNDARY)], ["HOME", "AWAY"], default="NONE")
    d["canonical_identity"] = d.apply(lambda r: f"{r.game_date}|{int(r.game_id)}|{prior.MODEL}|{prior.SNAPSHOT}", axis=1)
    return d.sort_values(["game_date", "game_id"])


def quote_rows(pred: pd.DataFrame) -> list[dict[str, Any]]:
    if pred.empty: return []
    conn = sqlite3.connect(f"file:{prior.MARKET_DB}?mode=ro", uri=True)
    raw = pd.read_sql_query("""
      SELECT canonical_market_identity,provider,bookmaker_key,game_date,game_id,captured_at_utc,
             scheduled_start_utc,timing_status,market_payload_json,market_payload_sha256,raw_source_sha256
      FROM supplemental_main_market_snapshots
      WHERE market_type='MONEYLINE' AND game_date>=? AND game_date<=?
      ORDER BY game_date,game_id,bookmaker_key,captured_at_utc,canonical_market_identity
    """, conn, params=(pred.game_date.min(), pred.game_date.max()))
    conn.close(); by_id = pred.set_index("game_id"); out = []
    for r in raw.itertuples(index=False):
        if int(r.game_id) not in by_id.index or r.timing_status != "PREGAME_CERTIFIED": continue
        p = by_id.loc[int(r.game_id)]; payload = json.loads(r.market_payload_json)
        captured = pd.to_datetime(r.captured_at_utc, utc=True, errors="coerce")
        start = pd.to_datetime(r.scheduled_start_utc, utc=True, errors="coerce")
        frozen = pd.to_datetime(p.prediction_timestamp_utc, utc=True, errors="coerce")
        updated = pd.to_datetime(payload.get("provider_market_updated_at_utc"), utc=True, errors="coerce")
        if any(pd.isna(x) for x in (captured, start, frozen, updated)) or not (captured >= frozen and captured < start and updated < start): continue
        needed = ["home_american_price", "away_american_price", "no_vig_home_probability", "no_vig_away_probability"]
        if any(payload.get(k) is None for k in needed): continue
        hn, an = float(payload["no_vig_home_probability"]), float(payload["no_vig_away_probability"])
        market_side = "HOME" if hn > BOUNDARY else "AWAY" if an > BOUNDARY else "NONE"
        model_side = str(p.model_strong_side); ms, ks = model_side != "NONE", market_side != "NONE"
        if ms and ks and model_side == market_side: strength_class = "JOINT_STRONG_SAME_SIDE"
        elif ms and ks: strength_class = "STRONG_CONFLICT"
        elif ms: strength_class = "MODEL_STRONG_ONLY"
        elif ks: strength_class = "MARKET_STRONG_ONLY"
        else: strength_class = "NEITHER_STRONG"
        values = {"canonical_market_identity": r.canonical_market_identity, "canonical_prediction_identity": p.canonical_identity,
                  "game_date": r.game_date, "game_id": int(r.game_id), "provider": r.provider, "bookmaker_key": r.bookmaker_key,
                  "captured_at_utc": str(r.captured_at_utc), "provider_market_updated_at_utc": str(payload["provider_market_updated_at_utc"]),
                  "scheduled_start_utc": str(r.scheduled_start_utc), "home_american_price": float(payload["home_american_price"]),
                  "away_american_price": float(payload["away_american_price"]), "home_decimal_price": decimal_price(payload["home_american_price"]),
                  "away_decimal_price": decimal_price(payload["away_american_price"]), "no_vig_home_probability": hn,
                  "no_vig_away_probability": an, "model_strong_side": model_side, "market_strong_side": market_side,
                  "strength_class": strength_class, "market_payload_sha256": r.market_payload_sha256, "raw_source_sha256": r.raw_source_sha256}
        if strength_class not in CLASSES: raise RuntimeError("invalid strength class")
        values["observer_row_sha256"] = digest(values); out.append(values)
    return out


def report_payload(conn: sqlite3.Connection, through: str, checkpoint: str, inserted: dict[str, int]) -> dict[str, Any]:
    pred = pd.read_sql_query("SELECT * FROM game_predictions ORDER BY game_date,game_id", conn)
    outcome = pd.read_sql_query("SELECT * FROM game_outcomes ORDER BY game_date,game_id", conn)
    quotes = pd.read_sql_query("SELECT * FROM sportsbook_quotes ORDER BY game_date,game_id,bookmaker_key,captured_at_utc", conn)
    first = quotes.groupby(["bookmaker_key", "game_id"], sort=True).head(1) if len(quotes) else quotes
    resolved = first.merge(outcome[["game_id", "home_win"]], on="game_id", how="inner") if len(first) and len(outcome) else pd.DataFrame()
    rows = []
    if len(resolved):
        for (book, cls), g in resolved.groupby(["bookmaker_key", "strength_class"], sort=True):
            side = np.where(g.strength_class.eq("MARKET_STRONG_ONLY"), g.market_strong_side,
                            np.where(g.model_strong_side.ne("NONE"), g.model_strong_side, np.where(g.no_vig_home_probability.ge(.5), "HOME", "AWAY")))
            win = np.where(side == "HOME", g.home_win, 1-g.home_win); american = np.where(side == "HOME", g.home_american_price, g.away_american_price)
            dec = np.array([decimal_price(x) for x in american]); ret = np.where(win == 1, dec-1, -1)
            rows.append({"bookmaker_key": book, "strength_class": cls, "resolved_games": len(g), "wins": int(win.sum()), "roi": float(ret.mean()),
                         "price_rule": "EARLIEST_ELIGIBLE_POST_PREDICTION_PREGAME_QUOTE"})
    return {"observer_status": "MANUAL_APPEND_ONLY_ALL_CLASS_SHADOW", "baseline_cutoff": BASELINE_CUTOFF,
            "observed_through_date": through, "checkpoint": checkpoint, "strong_definition": "probability > 0.60 strict",
            "extension_only": True, "ledger_totals": {"predictions": len(pred), "outcomes": len(outcome), "quotes": len(quotes)},
            "class_counts_by_quote": quotes.strength_class.value_counts().sort_index().to_dict() if len(quotes) else {},
            "book_class_economics": rows, "inserted_this_run": inserted,
            "constraints": {"network_capture": False, "refit": False, "wagering": False, "publishing": False, "scheduler": False,
                            "price_timing": "captured_at >= immutable prediction timestamp and strictly pregame", "best_quadrant_selection": False}}


def run(ledger: Path, report: Path, baseline_cutoff: str, through: str, checkpoint: str) -> dict[str, Any]:
    if baseline_cutoff != BASELINE_CUTOFF: raise ValueError(f"v1 baseline is immutable at {BASELINE_CUTOFF}")
    config = json.loads(prior.CONFIG.read_text()); all_pred = predictions(through)
    if set(all_pred.model_hash.dropna()) != {config["model_hash"]}: raise RuntimeError("frozen model hash mismatch")
    extension = all_pred[all_pred.game_date.gt(BASELINE_CUTOFF)].copy()
    ledger.parent.mkdir(parents=True, exist_ok=True); report.parent.mkdir(parents=True, exist_ok=True)
    inserted = {"predictions": 0, "outcomes": 0, "quotes": 0}
    with sqlite3.connect(ledger) as conn:
        schema(conn)
        meta = {"observer_id": "MLB_MONEYLINE_STRENGTH_CLASSES_SHADOW_V1", "model_version": prior.MODEL, "model_hash": config["model_hash"],
                "snapshot_class": prior.SNAPSHOT, "admission_status": prior.ADMISSION, "strong_boundary": BOUNDARY, "baseline_cutoff": BASELINE_CUTOFF}
        cols = list(meta); conn.execute(f"INSERT OR IGNORE INTO observer_metadata ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})", [meta[c] for c in cols])
        for r in extension.itertuples(index=False):
            values = {"canonical_identity": r.canonical_identity, "game_date": r.game_date, "game_id": int(r.game_id),
                      "scheduled_start_utc": str(r.scheduled_start_utc), "prediction_timestamp_utc": str(r.prediction_timestamp_utc),
                      "prediction_cutoff_utc": str(r.prediction_cutoff_utc), "home_team": r.home_team, "away_team": r.away_team,
                      "home_win_probability": float(r.home_win_probability), "away_win_probability": float(r.away_win_probability),
                      "model_strong_side": r.model_strong_side, "prediction_payload_sha256": r.prediction_payload_sha256}
            values["observer_row_sha256"] = digest(values); inserted["predictions"] += immutable_insert(conn, "game_predictions", "canonical_identity", values)
        for values in quote_rows(extension): inserted["quotes"] += immutable_insert(conn, "sportsbook_quotes", "canonical_market_identity", values)
        for r in extension[extension.resolved].itertuples(index=False):
            home_win = int(bool(r.prediction_correct)) if r.home_win_probability >= .5 else 1-int(bool(r.prediction_correct))
            values = {"canonical_identity": r.canonical_identity, "game_date": r.game_date, "game_id": int(r.game_id),
                      "official_winner": r.home_team if home_win else r.away_team, "home_win": home_win,
                      "outcome_payload_sha256": r.outcome_payload_sha256}
            values["observer_row_sha256"] = digest(values); inserted["outcomes"] += immutable_insert(conn, "game_outcomes", "canonical_identity", values)
        payload = report_payload(conn, through, checkpoint, inserted)
        report_text = json.dumps(payload, indent=2, sort_keys=True)+"\n"; report.write_text(report_text)
        run_values = {"run_identity": digest({"through": through, "checkpoint": checkpoint, "report": hashlib.sha256(report_text.encode()).hexdigest()}),
                      "observed_through_date": through, "checkpoint": checkpoint, "prediction_rows": payload["ledger_totals"]["predictions"],
                      "outcome_rows": payload["ledger_totals"]["outcomes"], "quote_rows": payload["ledger_totals"]["quotes"],
                      "report_sha256": hashlib.sha256(report_text.encode()).hexdigest()}
        # observer_runs has no observer_row_sha256 by design; exact duplicate run identity is idempotent.
        cols = list(run_values); conn.execute(f"INSERT OR IGNORE INTO observer_runs ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})", [run_values[c] for c in cols])
        conn.commit()
    return payload


def main() -> None:
    p = argparse.ArgumentParser(); p.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER); p.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    p.add_argument("--baseline-cutoff", default=BASELINE_CUTOFF); p.add_argument("--through-date", default=date.today().isoformat())
    p.add_argument("--checkpoint", choices=sorted(CHECKPOINTS), default="ORDINARY")
    args = p.parse_args(); ledger = args.ledger if args.ledger.is_absolute() else ROOT/args.ledger; report = args.report if args.report.is_absolute() else ROOT/args.report
    print(json.dumps(run(ledger, report, args.baseline_cutoff, args.through_date, args.checkpoint), indent=2, sort_keys=True))


if __name__ == "__main__": main()
