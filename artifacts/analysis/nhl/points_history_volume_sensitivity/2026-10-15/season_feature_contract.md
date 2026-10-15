# Current-season feature contract

- **schema_version:** NHL_POINTS_CURRENT_SEASON_VOLUME_CONTRACT_V1
- **season_ppg_prior:** diagnostic only: points_prior/games_prior when games_prior > 0; missing at zero games; never passed to HGB
- **cutoff:** strict game_date < target game_date; same-date outcomes excluded
- **calibration:** NONE

Full definitions and lineage are in `season_feature_contract.json`.
