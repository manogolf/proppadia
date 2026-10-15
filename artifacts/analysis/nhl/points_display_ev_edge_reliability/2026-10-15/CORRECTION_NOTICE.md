# Correction notice

Status: `SUPERSEDED_DISPLAY_ORIENTATION_ERROR`. This package supersedes the result committed as `90bf6a3b49ce28057f57d217444cd480951e859a`. The earlier analysis joined selected-side outcomes with Over-only prices and probabilities. The implementation mistake was using `p_over` and `price_over` unconditionally after labeling the row with the HGB-selected side.

The corrected research calculations apply side selection consistently to probability, median American price, fair price, Edge, and EV. The six supplied display fixtures pass. The corrected report also records a repository source discrepancy: checked-in React edge cards currently calculate Over probability minus market Over probability, while Funnel V1 calculates selected-side production Edge. The six fixtures establish the acceptance contract for this correction, but the checked-in React source alone does not establish that selected-side behavior is implemented there.

Funnel V1 source and artifacts were read only. Its frozen Stage 1 classification was compared with corrected selected-side Edge; see `funnel_stage1_reconciliation.csv` and JSON for exact coverage and mismatches.
