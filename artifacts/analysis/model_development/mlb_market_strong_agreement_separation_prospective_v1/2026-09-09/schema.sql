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
