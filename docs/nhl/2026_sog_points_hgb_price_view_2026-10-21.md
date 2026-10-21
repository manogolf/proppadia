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

## Coverage (main caveat)

- **SOG:** 10-10 → 10-20, about 31% of baseline line rows have a price (none on
  10-18). It depends on which lines books listed and which candidates the
  export binds.
- **Points HGB:** only 10-11, 10-13 and 10-14 (1,319 line rows). The export
  takes Points candidates from the reconciliation packages, which carry HGB
  production rows only on those days and Phoenix elsewhere.

## Findings (best price)

| Market | Side | Bets | W-L | ROI | Mean model EV |
|---|---|---|---|---|---|
| SOG | Model pick | 2,068 | 1,328-740 | −0.1% | +8.7% |
| SOG | Against the model | 2,506 | 795-1,711 | −10.5% | −18.3% |
| Points HGB | Model pick | 1,013 | 716-297 | −5.2% | +13.3% |
| Points HGB | Against the model | 1,319 | 304-1,015 | −13.7% | −54.6% |

- **Underdog value set** (plus-money sides the model rates positive EV, either
  relation): SOG 950 bets, 317-633, ROI −1.6%, while the model expected
  roughly +20% to +38%. Points HGB 156 bets, 58-98, ROI −1.4%.
- **The model overstates its edge.** Its picks average +9% to +13% model EV
  but return −0.1% (SOG) and −5.2% (Points). Its tail probabilities are too
  generous, most visibly on long-priced OVERs (SOG 3.5 OVER plus-money:
  51-218).
- **Only bright spot, small sample:** SOG model picks priced −149 to +149 went
  418-365 (783 bets), ROI +3.4% to +4.6% by bucket. Thirteen days is not
  enough to call this an edge.

Full tables: `by_relation.csv`, `by_relation_price.csv`,
`positive_ev_by_relation_price.csv`, `plus_money_positive_ev.csv`.

## Regeneration

From the main checkout (read-only; each writes a new folder):

```bash
for d in $(seq -f "2026-10-%02g" 1 20); do
  ./bin/python -m backend.nhl.scripts.export_nhl_all_candidate_evaluation \
    --root artifacts/operational/nhl --price-both-sides --slate-date "$d" \
    --out "artifacts/analysis/nhl/all_candidate_evaluation_export/both_sides_2026-10-21/slate_date=$d"
done
./bin/python backend/nhl/scripts/report_nhl_sog_points_price_view.py \
  --baseline artifacts/analysis/nhl/sog_points_wl_baseline/2026-10-21 \
  --export artifacts/analysis/nhl/all_candidate_evaluation_export/both_sides_2026-10-21 \
  --out artifacts/analysis/nhl/sog_points_price_view/2026-10-21
```

The export runs per slate date because the combined full-window run currently
stops on a conflicting duplicate Phoenix-shadow Points candidate for 10-17
(each date alone succeeds). Package checksum (sha256 of `SHA256SUMS`):
`d1ee6df3bd187bb96df1fe3ec80dd915a3ea6ee4664304a0b3128f54d4553010`.
