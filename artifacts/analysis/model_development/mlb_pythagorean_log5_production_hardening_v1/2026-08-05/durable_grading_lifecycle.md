# Durable grading lifecycle

The existing official-final grade builder preserves frozen prediction values and derives official winner, correctness, observed-outcome probability, Brier contribution, and log-loss contribution. Durable insertion requires `Final`, the exact prediction identity, retained official-source identity/hash, confidence band, and grading timestamp. Duplicate identical grades are idempotent; differing grades require append-only correction history. Predictions are never mutated. ROI, EV, profit, odds, and wagering fields are absent.
