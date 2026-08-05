# Durable prediction ledger design

Postgres is the sole production authority. Migration `20260805_create_public_game_moneyline_lifecycle.sql` creates immutable official-final observations, correction history, team-state snapshots, moneyline predictions, outcomes, and outcome-correction history. Canonical prediction identity is `(game_date, game_id, model_version, prediction_snapshot_class)` with a database primary key. Insert-on-conflict is accepted only when the canonical payload hash is identical; differing content fails closed. Database triggers reject UPDATE and DELETE.

Prediction rows preserve exact schedule, cutoff, teams, strict-prior values and strengths, probabilities, identity/hashes, admission status, and reason. Outcome fields are structurally absent. API requests read durable admitted rows only and never synthesize retrospective predictions. Render's filesystem is not authoritative.
