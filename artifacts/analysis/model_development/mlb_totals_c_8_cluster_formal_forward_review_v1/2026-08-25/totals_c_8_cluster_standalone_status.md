# Standalone status frozen before market comparison

- C median MAE: `3.605769`; C mean MAE: `3.673620`; RAW mean MAE: `3.545342`.
- RAW/C actual-minus-forecast bias: `0.529497` / `-0.168272`.
- RAW/C CRPS: `2.478074` / `2.501615`.
- RAW/C Brier: `0.247355` / `0.252493`.
- RAW/C log loss: `0.687761` / `0.698609`.
- Baselines are frozen leakage-safe comparators; no review-window tuning occurred.
- `C_OUTPERFORMS_BOTH_GOVERNED_SIMPLE_BASELINES_ON_POINT_AND_CRPS`.

`C_STANDALONE_FORWARD_EVIDENCE = MIXED`
