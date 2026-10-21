# NHL SOG and Points HGB price view (2026-10-21)

Companion to the W/L baseline
(`docs/nhl/2026_sog_points_hgb_wl_baseline_2026-10-21.md`). The baseline
measures how often the model's pick wins. This view prices **both sides** of
each baseline line, so it includes the side the model did not pick: the
underdogs a win-rate table cannot show.

## Method

- Rows are the frozen baseline's governed rows (same P(over), pick and
  result). For each row and side, the price is the best decimal price across
  books (also reported for BetOnline alone) among each book's latest quote
  that preceded the prediction. Each bet is one unit; model EV uses the model's
  probability for that side.
- Prices come from the all-candidate evaluation export run with the new
  `--price-both-sides` flag. Before this change the export priced only the
  model's selected side for player props, so an EV or edge filter on it could
  never surface an against-the-model underdog.
- Exploratory, not executed wagers; best-price shopping flatters the results.

## Coverage

- **SOG:** 10-10 → 10-20, about 31% of baseline line rows have a price. It
  depends on which lines books listed and which candidates the export binds.
- **Points HGB:** 10-10 → 10-20 (3,331 line rows priced on the against side,
  2,621 on the model side). Earlier exports covered only three days because
  they could not read grades built from several captures; fixed in `bddb1aa8`.
- **No 2026-10-18 prices:** that day's only odds snapshot (18:40 UTC) came after
  its prediction (18:36 UTC), so no quote qualifies as pregame-priced.

## Findings (best price)

| Market | Side | Bets | W-L | ROI | Mean model EV |
|---|---|---|---|---|---|
| SOG | Model pick | 2,068 | 1,328-740 | −0.1% | +8.7% |
| SOG | Against the model | 2,506 | 795-1,711 | −10.5% | −18.3% |
| Points HGB | Model pick | 2,621 | 1,938-683 | −2.0% | +9.4% |
| Points HGB | Against the model | 3,331 | 695-2,636 | −25.8% | −42.0% |

- **Underdog value set** (plus-money sides the model rates positive EV, either
  relation): SOG 950 bets, 317-633, ROI −1.6%; Points HGB 641 bets, 184-457,
  ROI −17.4%. The model expected roughly +20% or more on these.
- **The model overstates its edge.** Its picks average about +9% model EV but
  return −0.1% (SOG) and −2.0% (Points). Its tail probabilities are too
  generous, most visibly on long-priced OVERs (SOG 3.5 OVER plus-money:
  51-218).
- **Only bright spot, small sample:** model picks priced −149 to +149 —
  SOG 418-365 (783 bets, ROI +3.9%) and Points HGB 248-204 (452 bets, ROI
  +6.6%). Eleven to thirteen days is not enough to call this an edge.

Full tables: `by_relation.csv`, `by_relation_price.csv`,
`positive_ev_by_relation_price.csv`, `plus_money_positive_ev.csv`.

## Regeneration

From the main checkout (read-only; each writes a new folder):

```bash
./bin/python -m backend.nhl.scripts.export_nhl_all_candidate_evaluation \
  --root artifacts/operational/nhl --price-both-sides \
  --out artifacts/analysis/nhl/all_candidate_evaluation_export/full_window_2026-10-21_both_sides
./bin/python backend/nhl/scripts/report_nhl_sog_points_price_view.py \
  --baseline artifacts/analysis/nhl/sog_points_wl_baseline/2026-10-21 \
  --export artifacts/analysis/nhl/all_candidate_evaluation_export/full_window_2026-10-21_both_sides \
  --out artifacts/analysis/nhl/sog_points_price_view/2026-10-21
```

The full-window export runs in one pass again: the open, not-yet-reconciled
slate is now derived instead of fixed at 2026-10-17 (`bddb1aa8`), so the
duplicate Phoenix-shadow Points candidate no longer occurs. Package checksum
(sha256 of `SHA256SUMS`):
`421c563403a7bea7b81c5a01b341ab95ce11f649751cb857e20fad72a8e75283`.
