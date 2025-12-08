-- backend/nhl/sql/export_train_sog_denali.sql
-- Exports SOG Denali *training* data:
--   - Source: nhl.training_features_sog_denali
--   - Seasons: 2023, 2024
--   - Only rows with non-null shots_on_goal (label)

COPY (
  SELECT
    *
  FROM nhl.training_features_sog_denali
  WHERE season IN (2023, 2024)
    AND shots_on_goal IS NOT NULL
) TO STDOUT WITH CSV HEADER;
