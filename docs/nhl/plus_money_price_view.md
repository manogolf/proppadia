# NHL plus-money price view (through 2026-11-07)

Rebuilt weekly with the model evaluation. The subject is the model's underdog side: every side of a graded production pick whose model fair odds are plus money (the model gives it under 50%), banded by those fair odds, as the 8rain fair-odds filter would show it at +100 or longer. Each side is settled at one unit at the best quoted price before the prediction, at the operator's books (BetOnline, Novig, Kalshi, Pinnacle) and across all books (members' view). Positive EV means the book's price is longer than the model's fair price. Every side and price is in `priced_rows.csv` under `artifacts/analysis/nhl/price_view/weekly_2026-11-07/`. Collection only: no filter is chosen from these tables until the readiness counts are met.

- Points: goals + assists model (`8cda6061…`), 7 slates (2026-11-01 to 2026-11-07).
- SOG: `NHL_SOG_COUNT_HGB_V1`, 14 slates from 2026-10-25.

## Readiness

Positive-EV underdog sides at the operator's books; a market is ready for evaluation when every fair-odds band has 100.

| Market | Fair odds | Picks | Needed | Ready |
|---|---|---|---|---|
| sog | +100 to +129 | 187 | 100 | yes |
| sog | +130 to +169 | 110 | 100 | yes |
| sog | +170 to +249 | 107 | 100 | yes |
| sog | +250 or longer | 111 | 100 | yes |
| points | +100 to +129 | 110 | 100 | yes |
| points | +130 to +169 | 80 | 100 | no |
| points | +170 to +249 | 41 | 100 | no |
| points | +250 or longer | 116 | 100 | yes |
| sog over 1.5 | +100 to +109 | 26 | 100 | no |
| sog over 1.5 | +110 to +119 | 12 | 100 | no |
| sog over 1.5 | +120 to +129 | 7 | 100 | no |
| sog over 1.5 | +130 to +169 | 8 | 100 | no |
| sog over 1.5 | +170 to +249 | 0 | 100 | no |
| sog over 1.5 | +250 or longer | 0 | 100 | no |

## Underdog sides with positive EV — operator's books

By fair odds:

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 43 | 19-24 | 44.2 | 46.3 | +4.7% |
| points | OVER | +130 to +169 | 40 | 17-23 | 42.5 | 40.8 | +12.4% |
| points | OVER | +170 to +249 | 28 | 8-20 | 28.6 | 33.4 | -9.2% |
| points | OVER | +250 or longer | 114 | 15-99 | 13.2 | 16.1 | -13.9% |
| points | UNDER | +100 to +129 | 67 | 20-47 | 29.8 | 46.8 | -33.2% |
| points | UNDER | +130 to +169 | 40 | 7-33 | 17.5 | 40.6 | -55.5% |
| points | UNDER | +170 to +249 | 13 | 4-9 | 30.8 | 34.6 | -10.1% |
| points | UNDER | +250 or longer | 2 | 0-2 | 0.0 | 25.1 | -100.0% |
| sog | OVER | +100 to +129 | 118 | 51-67 | 43.2 | 47.4 | -1.1% |
| sog | OVER | +130 to +169 | 98 | 41-57 | 41.8 | 40.4 | +11.8% |
| sog | OVER | +170 to +249 | 104 | 20-84 | 19.2 | 32.9 | -36.5% |
| sog | OVER | +250 or longer | 110 | 22-88 | 20.0 | 20.5 | +10.2% |
| sog | UNDER | +100 to +129 | 69 | 28-41 | 40.6 | 47.4 | -10.0% |
| sog | UNDER | +130 to +169 | 12 | 4-8 | 33.3 | 41.0 | -18.2% |
| sog | UNDER | +170 to +249 | 3 | 1-2 | 33.3 | 35.1 | +2.7% |
| sog | UNDER | +250 or longer | 1 | 0-1 | 0.0 | 19.3 | -100.0% |

By line, side and fair odds:

| market | line | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | +100 to +129 | 40 | 18-22 | 45.0 | 46.2 | +6.0% |
| points | 0.5 | OVER | +130 to +169 | 37 | 17-20 | 46.0 | 40.6 | +21.5% |
| points | 0.5 | OVER | +170 to +249 | 15 | 4-11 | 26.7 | 34.3 | -18.2% |
| points | 0.5 | OVER | +250 or longer | 3 | 0-3 | 0.0 | 25.7 | -100.0% |
| points | 0.5 | UNDER | +100 to +129 | 67 | 20-47 | 29.8 | 46.8 | -33.2% |
| points | 0.5 | UNDER | +130 to +169 | 40 | 7-33 | 17.5 | 40.6 | -55.5% |
| points | 0.5 | UNDER | +170 to +249 | 13 | 4-9 | 30.8 | 34.6 | -10.1% |
| points | 0.5 | UNDER | +250 or longer | 2 | 0-2 | 0.0 | 25.1 | -100.0% |
| points | 1.5 | OVER | +100 to +129 | 3 | 1-2 | 33.3 | 46.6 | -12.3% |
| points | 1.5 | OVER | +130 to +169 | 3 | 0-3 | 0.0 | 42.6 | -100.0% |
| points | 1.5 | OVER | +170 to +249 | 13 | 4-9 | 30.8 | 32.3 | +1.2% |
| points | 1.5 | OVER | +250 or longer | 91 | 14-77 | 15.4 | 16.6 | -2.1% |
| points | 2.5 | OVER | +250 or longer | 20 | 1-19 | 5.0 | 12.0 | -54.5% |
| sog | 1.5 | OVER | +100 to +129 | 45 | 15-30 | 33.3 | 47.8 | -27.5% |
| sog | 1.5 | OVER | +130 to +169 | 8 | 2-6 | 25.0 | 41.6 | -35.8% |
| sog | 1.5 | UNDER | +100 to +129 | 49 | 21-28 | 42.9 | 47.1 | -4.4% |
| sog | 1.5 | UNDER | +130 to +169 | 10 | 2-8 | 20.0 | 40.7 | -49.2% |
| sog | 1.5 | UNDER | +170 to +249 | 2 | 1-1 | 50.0 | 35.2 | +54.0% |
| sog | 1.5 | UNDER | +250 or longer | 1 | 0-1 | 0.0 | 19.3 | -100.0% |
| sog | 2.5 | OVER | +100 to +129 | 54 | 25-29 | 46.3 | 47.2 | +8.2% |
| sog | 2.5 | OVER | +130 to +169 | 69 | 33-36 | 47.8 | 40.6 | +28.1% |
| sog | 2.5 | OVER | +170 to +249 | 73 | 15-58 | 20.5 | 33.1 | -31.9% |
| sog | 2.5 | OVER | +250 or longer | 24 | 7-17 | 29.2 | 26.1 | +21.5% |
| sog | 2.5 | UNDER | +100 to +129 | 14 | 6-8 | 42.9 | 48.1 | -5.9% |
| sog | 2.5 | UNDER | +130 to +169 | 2 | 2-0 | 100.0 | 42.8 | +137.0% |
| sog | 2.5 | UNDER | +170 to +249 | 1 | 0-1 | 0.0 | 35.0 | -100.0% |
| sog | 3.5 | OVER | +100 to +129 | 19 | 11-8 | 57.9 | 46.9 | +35.0% |
| sog | 3.5 | OVER | +130 to +169 | 21 | 6-15 | 28.6 | 39.5 | -23.5% |
| sog | 3.5 | OVER | +170 to +249 | 31 | 5-26 | 16.1 | 32.4 | -47.3% |
| sog | 3.5 | OVER | +250 or longer | 86 | 15-71 | 17.4 | 18.9 | +7.1% |
| sog | 3.5 | UNDER | +100 to +129 | 6 | 1-5 | 16.7 | 48.2 | -65.7% |

## Underdog sides with positive EV — all books

By fair odds:

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 49 | 21-28 | 42.9 | 46.2 | +2.9% |
| points | OVER | +130 to +169 | 64 | 29-35 | 45.3 | 40.5 | +21.1% |
| points | OVER | +170 to +249 | 71 | 17-54 | 23.9 | 33.3 | -22.7% |
| points | OVER | +250 or longer | 171 | 23-148 | 13.5 | 15.0 | +8.8% |
| points | UNDER | +100 to +129 | 73 | 22-51 | 30.1 | 46.7 | -31.4% |
| points | UNDER | +130 to +169 | 46 | 8-38 | 17.4 | 40.6 | -55.8% |
| points | UNDER | +170 to +249 | 22 | 5-17 | 22.7 | 34.2 | -32.5% |
| points | UNDER | +250 or longer | 5 | 0-5 | 0.0 | 23.0 | -100.0% |
| sog | OVER | +100 to +129 | 148 | 64-84 | 43.2 | 47.2 | -0.7% |
| sog | OVER | +130 to +169 | 113 | 47-66 | 41.6 | 40.4 | +12.6% |
| sog | OVER | +170 to +249 | 148 | 32-116 | 21.6 | 32.7 | -28.0% |
| sog | OVER | +250 or longer | 209 | 39-170 | 18.7 | 20.1 | +8.1% |
| sog | UNDER | +100 to +129 | 115 | 55-60 | 47.8 | 47.3 | +6.3% |
| sog | UNDER | +130 to +169 | 37 | 14-23 | 37.8 | 41.4 | -5.4% |
| sog | UNDER | +170 to +249 | 22 | 6-16 | 27.3 | 33.3 | -4.3% |
| sog | UNDER | +250 or longer | 4 | 2-2 | 50.0 | 25.5 | +82.5% |

By line, side and fair odds:

| market | line | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | +100 to +129 | 46 | 20-26 | 43.5 | 46.2 | +3.8% |
| points | 0.5 | OVER | +130 to +169 | 61 | 29-32 | 47.5 | 40.4 | +27.1% |
| points | 0.5 | OVER | +170 to +249 | 57 | 12-45 | 21.1 | 33.4 | -32.1% |
| points | 0.5 | OVER | +250 or longer | 9 | 1-8 | 11.1 | 26.7 | -57.8% |
| points | 0.5 | UNDER | +100 to +129 | 73 | 22-51 | 30.1 | 46.7 | -31.4% |
| points | 0.5 | UNDER | +130 to +169 | 46 | 8-38 | 17.4 | 40.6 | -55.8% |
| points | 0.5 | UNDER | +170 to +249 | 22 | 5-17 | 22.7 | 34.2 | -32.5% |
| points | 0.5 | UNDER | +250 or longer | 5 | 0-5 | 0.0 | 23.0 | -100.0% |
| points | 1.5 | OVER | +100 to +129 | 3 | 1-2 | 33.3 | 46.6 | -10.0% |
| points | 1.5 | OVER | +130 to +169 | 3 | 0-3 | 0.0 | 42.6 | -100.0% |
| points | 1.5 | OVER | +170 to +249 | 14 | 5-9 | 35.7 | 32.6 | +15.6% |
| points | 1.5 | OVER | +250 or longer | 120 | 19-101 | 15.8 | 16.3 | +2.7% |
| points | 2.5 | OVER | +250 or longer | 42 | 3-39 | 7.1 | 8.9 | +40.7% |
| sog | 1.5 | OVER | +100 to +129 | 70 | 26-44 | 37.1 | 47.4 | -18.3% |
| sog | 1.5 | OVER | +130 to +169 | 10 | 2-8 | 20.0 | 41.6 | -48.6% |
| sog | 1.5 | UNDER | +100 to +129 | 79 | 41-38 | 51.9 | 47.2 | +16.3% |
| sog | 1.5 | UNDER | +130 to +169 | 28 | 9-19 | 32.1 | 41.2 | -19.7% |
| sog | 1.5 | UNDER | +170 to +249 | 18 | 5-13 | 27.8 | 33.6 | -2.5% |
| sog | 1.5 | UNDER | +250 or longer | 3 | 1-2 | 33.3 | 24.5 | +23.3% |
| sog | 2.5 | OVER | +100 to +129 | 59 | 27-32 | 45.8 | 47.1 | +8.2% |
| sog | 2.5 | OVER | +130 to +169 | 78 | 37-41 | 47.4 | 40.7 | +28.5% |
| sog | 2.5 | OVER | +170 to +249 | 100 | 20-80 | 20.0 | 33.0 | -34.1% |
| sog | 2.5 | OVER | +250 or longer | 56 | 12-44 | 21.4 | 25.4 | -6.8% |
| sog | 2.5 | UNDER | +100 to +129 | 28 | 12-16 | 42.9 | 47.4 | -6.3% |
| sog | 2.5 | UNDER | +130 to +169 | 9 | 5-4 | 55.6 | 41.8 | +38.9% |
| sog | 2.5 | UNDER | +170 to +249 | 4 | 1-3 | 25.0 | 32.2 | -12.5% |
| sog | 2.5 | UNDER | +250 or longer | 1 | 1-0 | 100.0 | 28.3 | +260.0% |
| sog | 3.5 | OVER | +100 to +129 | 19 | 11-8 | 57.9 | 46.9 | +36.7% |
| sog | 3.5 | OVER | +130 to +169 | 25 | 8-17 | 32.0 | 39.3 | -12.6% |
| sog | 3.5 | OVER | +170 to +249 | 48 | 12-36 | 25.0 | 32.0 | -15.2% |
| sog | 3.5 | OVER | +250 or longer | 153 | 27-126 | 17.6 | 18.1 | +13.6% |
| sog | 3.5 | UNDER | +100 to +129 | 8 | 2-6 | 25.0 | 48.2 | -47.8% |

## SOG Over 1.5: model-underdog sides by fair odds

The same sides for this one line, tracked on their own (readiness above).

Positive EV, operator's books:

| fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|
| +100 to +109 | 26 | 10-16 | 38.5 | 49.1 | -16.9% |
| +110 to +119 | 12 | 3-9 | 25.0 | 46.7 | -45.2% |
| +120 to +129 | 7 | 2-5 | 28.6 | 44.7 | -36.6% |
| +130 to +169 | 8 | 2-6 | 25.0 | 41.6 | -35.8% |

Positive EV, all books:

| fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|
| +100 to +109 | 35 | 15-20 | 42.9 | 49.0 | -7.0% |
| +110 to +119 | 22 | 7-15 | 31.8 | 46.6 | -29.1% |
| +120 to +129 | 13 | 4-9 | 30.8 | 44.5 | -30.5% |
| +130 to +169 | 10 | 2-8 | 20.0 | 41.6 | -48.6% |

Any EV, operator's books:

| fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|
| +100 to +109 | 80 | 38-42 | 47.5 | 49.0 | -6.3% |
| +110 to +119 | 66 | 28-38 | 42.4 | 46.6 | -14.8% |
| +120 to +129 | 47 | 28-19 | 59.6 | 44.5 | +22.3% |
| +130 to +169 | 112 | 50-62 | 44.6 | 40.7 | -3.8% |
| +170 to +249 | 52 | 16-36 | 30.8 | 33.6 | -28.8% |
| +250 or longer | 10 | 6-4 | 60.0 | 25.5 | +58.0% |

## Every underdog side, any EV — operator's books

Context: the same sides without the positive-EV condition.

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 121 | 65-56 | 53.7 | 46.4 | +10.5% |
| points | OVER | +130 to +169 | 169 | 78-91 | 46.2 | 40.2 | +5.7% |
| points | OVER | +170 to +249 | 153 | 63-90 | 41.2 | 33.4 | +8.3% |
| points | OVER | +250 or longer | 1431 | 159-1272 | 11.1 | 9.0 | -30.5% |
| points | UNDER | +100 to +129 | 140 | 61-79 | 43.6 | 46.7 | -10.5% |
| points | UNDER | +130 to +169 | 98 | 34-64 | 34.7 | 40.5 | -20.9% |
| points | UNDER | +170 to +249 | 60 | 15-45 | 25.0 | 33.8 | -34.2% |
| points | UNDER | +250 or longer | 21 | 4-17 | 19.1 | 23.4 | -33.7% |
| sog | OVER | +100 to +129 | 333 | 157-176 | 47.1 | 47.0 | -1.8% |
| sog | OVER | +130 to +169 | 329 | 142-187 | 43.2 | 40.4 | +0.9% |
| sog | OVER | +170 to +249 | 428 | 134-294 | 31.3 | 32.8 | -13.4% |
| sog | OVER | +250 or longer | 1579 | 270-1309 | 17.1 | 15.7 | -20.4% |
| sog | UNDER | +100 to +129 | 262 | 137-125 | 52.3 | 47.0 | +3.8% |
| sog | UNDER | +130 to +169 | 232 | 108-124 | 46.6 | 40.6 | -0.8% |
| sog | UNDER | +170 to +249 | 146 | 54-92 | 37.0 | 33.6 | -21.4% |
| sog | UNDER | +250 or longer | 104 | 12-92 | 11.5 | 20.0 | -69.7% |

Return is profit per unit at the stated price; Model % is the model's mean probability for the side. Small cells move by chance: about +/-10 points of return at 100 picks.
