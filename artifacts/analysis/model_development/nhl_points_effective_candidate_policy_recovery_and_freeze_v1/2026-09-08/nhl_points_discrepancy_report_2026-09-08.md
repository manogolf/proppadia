# NHL Points policy recovery discrepancy report

## Replay result

The mandatory 1 pp ladder gate excluded 13,836 of 19,711 player-game ladders (41,508 of 59,133 prediction-line rows), leaving 5,875 ladders / 17,625 line rows. Only 642 gated line rows across 343 player-games have a legacy median Over market attachment; 383 rows pass the current React price bounds.

Counterfactually applying the React research display after the gate yields 294 top-ten rows over all 48 archived dates, or 260 rows over its 41 historically applicable archive dates. Without the gate those counts are 480 and 410. The gated top-ten set still contains 43 negative-gap rows because that surface has no gap floor.

Counterfactually applying the legacy static defaults after the gate yields 955 table-export rows versus 960 without the gate. Of the gated rows, 721 are unpriced because the static edge filter deliberately preserves non-finite edges.

## Parity limits

Exact candidate or upload parity cannot be tested: zero archived Points candidate ledgers, zero Points upload ledgers, and zero `nhl_points_top_*.csv` downloads survive. There is consequently no target for line, side, price, book, ordering, cap, upload, or execution parity. The legacy CSV price is a cross-book median and does not preserve which book supplied the price; with an even count it need not equal any offered quote.

The two replayable surfaces disagree on line choice, required market presence, price bounds, edge/EV logic, ranking, cap, and output schema. Neither is governed as candidate policy. Choosing either would invent authority rather than recover it.
