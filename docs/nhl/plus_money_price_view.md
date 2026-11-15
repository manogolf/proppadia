# NHL plus-money price view (through 2026-11-14)

Rebuilt weekly with the model evaluation. The subject is the model's underdog side: every side of a graded production pick whose model fair odds are plus money (the model gives it under 50%), banded by those fair odds, as the 8rain fair-odds filter would show it at +100 or longer. Each side is settled at one unit at the best quoted price before the prediction, at the operator's books (BetOnline, Novig, Kalshi, Pinnacle) and across all books (members' view). Positive EV means the book's price is longer than the model's fair price. Every side and price is in `priced_rows.csv` under `artifacts/analysis/nhl/price_view/weekly_2026-11-14/`. Collection only: no filter is chosen from these tables until the readiness counts are met.

- Points: goals + assists model (`8cda6061…`), 14 slates (2026-11-01 to 2026-11-14).
- SOG: `NHL_SOG_COUNT_HGB_V1`, 21 slates from 2026-10-25.

## Readiness

Positive-EV underdog sides at the operator's books; a market is ready for evaluation when every fair-odds band has 100.

| Market | Fair odds | Picks | Needed | Ready |
|---|---|---|---|---|
| sog | +100 to +129 | 309 | 100 | yes |
| sog | +130 to +169 | 199 | 100 | yes |
| sog | +170 to +249 | 190 | 100 | yes |
| sog | +250 or longer | 221 | 100 | yes |
| points | +100 to +129 | 234 | 100 | yes |
| points | +130 to +169 | 178 | 100 | yes |
| points | +170 to +249 | 88 | 100 | no |
| points | +250 or longer | 271 | 100 | yes |
| sog over 1.5 | +100 to +109 | 39 | 100 | no |
| sog over 1.5 | +110 to +119 | 20 | 100 | no |
| sog over 1.5 | +120 to +129 | 14 | 100 | no |
| sog over 1.5 | +130 to +169 | 12 | 100 | no |
| sog over 1.5 | +170 to +249 | 1 | 100 | no |
| sog over 1.5 | +250 or longer | 0 | 100 | no |

## Underdog sides with positive EV — operator's books

By fair odds:

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 97 | 48-49 | 49.5 | 46.4 | +15.0% |
| points | OVER | +130 to +169 | 98 | 54-44 | 55.1 | 40.8 | +45.8% |
| points | OVER | +170 to +249 | 64 | 16-48 | 25.0 | 33.5 | -20.6% |
| points | OVER | +250 or longer | 264 | 36-228 | 13.6 | 15.8 | -0.4% |
| points | UNDER | +100 to +129 | 137 | 53-84 | 38.7 | 46.7 | -12.4% |
| points | UNDER | +130 to +169 | 80 | 16-64 | 20.0 | 40.7 | -48.8% |
| points | UNDER | +170 to +249 | 24 | 5-19 | 20.8 | 34.5 | -39.7% |
| points | UNDER | +250 or longer | 7 | 0-7 | 0.0 | 24.4 | -100.0% |
| sog | OVER | +100 to +129 | 196 | 82-114 | 41.8 | 47.2 | -4.0% |
| sog | OVER | +130 to +169 | 170 | 65-105 | 38.2 | 40.4 | +2.4% |
| sog | OVER | +170 to +249 | 177 | 40-137 | 22.6 | 33.0 | -25.4% |
| sog | OVER | +250 or longer | 217 | 39-178 | 18.0 | 20.3 | +3.2% |
| sog | UNDER | +100 to +129 | 113 | 51-62 | 45.1 | 47.3 | +0.8% |
| sog | UNDER | +130 to +169 | 29 | 11-18 | 37.9 | 40.7 | -2.0% |
| sog | UNDER | +170 to +249 | 13 | 3-10 | 23.1 | 34.7 | -27.8% |
| sog | UNDER | +250 or longer | 4 | 0-4 | 0.0 | 20.6 | -100.0% |

By line, side and fair odds:

| market | line | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | +100 to +129 | 91 | 46-45 | 50.5 | 46.4 | +17.1% |
| points | 0.5 | OVER | +130 to +169 | 87 | 50-37 | 57.5 | 40.8 | +50.7% |
| points | 0.5 | OVER | +170 to +249 | 36 | 8-28 | 22.2 | 34.0 | -30.5% |
| points | 0.5 | OVER | +250 or longer | 3 | 0-3 | 0.0 | 25.7 | -100.0% |
| points | 0.5 | UNDER | +100 to +129 | 137 | 53-84 | 38.7 | 46.7 | -12.4% |
| points | 0.5 | UNDER | +130 to +169 | 80 | 16-64 | 20.0 | 40.7 | -48.8% |
| points | 0.5 | UNDER | +170 to +249 | 24 | 5-19 | 20.8 | 34.5 | -39.7% |
| points | 0.5 | UNDER | +250 or longer | 7 | 0-7 | 0.0 | 24.4 | -100.0% |
| points | 1.5 | OVER | +100 to +129 | 6 | 2-4 | 33.3 | 46.2 | -17.0% |
| points | 1.5 | OVER | +130 to +169 | 11 | 4-7 | 36.4 | 40.8 | +7.1% |
| points | 1.5 | OVER | +170 to +249 | 28 | 8-20 | 28.6 | 32.9 | -7.8% |
| points | 1.5 | OVER | +250 or longer | 209 | 33-176 | 15.8 | 16.5 | +13.6% |
| points | 2.5 | OVER | +250 or longer | 52 | 3-49 | 5.8 | 12.3 | -51.0% |
| sog | 1.5 | OVER | +100 to +129 | 73 | 27-46 | 37.0 | 47.6 | -17.9% |
| sog | 1.5 | OVER | +130 to +169 | 12 | 4-8 | 33.3 | 41.9 | -14.6% |
| sog | 1.5 | OVER | +170 to +249 | 1 | 0-1 | 0.0 | 34.5 | -100.0% |
| sog | 1.5 | UNDER | +100 to +129 | 84 | 37-47 | 44.0 | 47.3 | -1.7% |
| sog | 1.5 | UNDER | +130 to +169 | 25 | 9-16 | 36.0 | 40.6 | -5.2% |
| sog | 1.5 | UNDER | +170 to +249 | 10 | 3-7 | 30.0 | 35.2 | -6.1% |
| sog | 1.5 | UNDER | +250 or longer | 4 | 0-4 | 0.0 | 20.6 | -100.0% |
| sog | 2.5 | OVER | +100 to +129 | 98 | 40-58 | 40.8 | 47.0 | -5.2% |
| sog | 2.5 | OVER | +130 to +169 | 115 | 47-68 | 40.9 | 40.5 | +10.4% |
| sog | 2.5 | OVER | +170 to +249 | 133 | 33-100 | 24.8 | 33.2 | -18.2% |
| sog | 2.5 | OVER | +250 or longer | 52 | 12-40 | 23.1 | 25.7 | -4.5% |
| sog | 2.5 | UNDER | +100 to +129 | 23 | 13-10 | 56.5 | 47.1 | +27.2% |
| sog | 2.5 | UNDER | +130 to +169 | 4 | 2-2 | 50.0 | 41.7 | +18.5% |
| sog | 2.5 | UNDER | +170 to +249 | 3 | 0-3 | 0.0 | 33.1 | -100.0% |
| sog | 3.5 | OVER | +100 to +129 | 25 | 15-10 | 60.0 | 47.0 | +41.3% |
| sog | 3.5 | OVER | +130 to +169 | 43 | 14-29 | 32.6 | 39.9 | -14.3% |
| sog | 3.5 | OVER | +170 to +249 | 43 | 7-36 | 16.3 | 32.4 | -46.0% |
| sog | 3.5 | OVER | +250 or longer | 165 | 27-138 | 16.4 | 18.6 | +5.6% |
| sog | 3.5 | UNDER | +100 to +129 | 6 | 1-5 | 16.7 | 48.2 | -65.7% |

## Underdog sides with positive EV — all books

By fair odds:

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 109 | 52-57 | 47.7 | 46.4 | +12.2% |
| points | OVER | +130 to +169 | 157 | 75-82 | 47.8 | 40.4 | +28.2% |
| points | OVER | +170 to +249 | 170 | 37-133 | 21.8 | 33.6 | -29.2% |
| points | OVER | +250 or longer | 376 | 50-326 | 13.3 | 14.7 | +18.6% |
| points | UNDER | +100 to +129 | 148 | 57-91 | 38.5 | 46.7 | -11.4% |
| points | UNDER | +130 to +169 | 89 | 19-70 | 21.3 | 40.6 | -44.2% |
| points | UNDER | +170 to +249 | 41 | 9-32 | 21.9 | 34.4 | -35.1% |
| points | UNDER | +250 or longer | 11 | 0-11 | 0.0 | 23.2 | -100.0% |
| sog | OVER | +100 to +129 | 239 | 100-139 | 41.8 | 47.2 | -3.7% |
| sog | OVER | +130 to +169 | 191 | 75-116 | 39.3 | 40.4 | +6.0% |
| sog | OVER | +170 to +249 | 233 | 57-176 | 24.5 | 32.8 | -18.8% |
| sog | OVER | +250 or longer | 379 | 65-314 | 17.2 | 19.9 | +1.4% |
| sog | UNDER | +100 to +129 | 186 | 91-95 | 48.9 | 47.2 | +10.2% |
| sog | UNDER | +130 to +169 | 74 | 28-46 | 37.8 | 40.8 | -1.9% |
| sog | UNDER | +170 to +249 | 50 | 15-35 | 30.0 | 33.7 | +0.7% |
| sog | UNDER | +250 or longer | 11 | 3-8 | 27.3 | 24.9 | +2.7% |

By line, side and fair odds:

| market | line | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | +100 to +129 | 103 | 50-53 | 48.5 | 46.4 | +13.8% |
| points | 0.5 | OVER | +130 to +169 | 145 | 70-75 | 48.3 | 40.4 | +28.8% |
| points | 0.5 | OVER | +170 to +249 | 139 | 27-112 | 19.4 | 33.7 | -36.9% |
| points | 0.5 | OVER | +250 or longer | 11 | 1-10 | 9.1 | 26.9 | -65.5% |
| points | 0.5 | UNDER | +100 to +129 | 148 | 57-91 | 38.5 | 46.7 | -11.4% |
| points | 0.5 | UNDER | +130 to +169 | 89 | 19-70 | 21.3 | 40.6 | -44.2% |
| points | 0.5 | UNDER | +170 to +249 | 41 | 9-32 | 21.9 | 34.4 | -35.1% |
| points | 0.5 | UNDER | +250 or longer | 11 | 0-11 | 0.0 | 23.2 | -100.0% |
| points | 1.5 | OVER | +100 to +129 | 6 | 2-4 | 33.3 | 46.2 | -15.8% |
| points | 1.5 | OVER | +130 to +169 | 12 | 5-7 | 41.7 | 40.7 | +21.1% |
| points | 1.5 | OVER | +170 to +249 | 31 | 10-21 | 32.3 | 33.0 | +5.3% |
| points | 1.5 | OVER | +250 or longer | 278 | 43-235 | 15.5 | 15.8 | +15.3% |
| points | 2.5 | OVER | +250 or longer | 87 | 6-81 | 6.9 | 9.7 | +39.7% |
| sog | 1.5 | OVER | +100 to +129 | 108 | 41-67 | 38.0 | 47.4 | -15.6% |
| sog | 1.5 | OVER | +130 to +169 | 14 | 4-10 | 28.6 | 41.8 | -26.8% |
| sog | 1.5 | OVER | +170 to +249 | 2 | 0-2 | 0.0 | 32.7 | -100.0% |
| sog | 1.5 | UNDER | +100 to +129 | 135 | 68-67 | 50.4 | 47.2 | +13.9% |
| sog | 1.5 | UNDER | +130 to +169 | 59 | 21-38 | 35.6 | 40.8 | -8.3% |
| sog | 1.5 | UNDER | +170 to +249 | 39 | 14-25 | 35.9 | 33.8 | +20.1% |
| sog | 1.5 | UNDER | +250 or longer | 10 | 2-8 | 20.0 | 24.6 | -23.0% |
| sog | 2.5 | OVER | +100 to +129 | 106 | 44-62 | 41.5 | 47.0 | -2.6% |
| sog | 2.5 | OVER | +130 to +169 | 128 | 53-75 | 41.4 | 40.5 | +12.8% |
| sog | 2.5 | OVER | +170 to +249 | 165 | 40-125 | 24.2 | 33.1 | -19.9% |
| sog | 2.5 | OVER | +250 or longer | 98 | 19-79 | 19.4 | 25.1 | -15.3% |
| sog | 2.5 | UNDER | +100 to +129 | 41 | 20-21 | 48.8 | 47.0 | +9.2% |
| sog | 2.5 | UNDER | +130 to +169 | 15 | 7-8 | 46.7 | 40.8 | +23.3% |
| sog | 2.5 | UNDER | +170 to +249 | 11 | 1-10 | 9.1 | 33.4 | -68.2% |
| sog | 2.5 | UNDER | +250 or longer | 1 | 1-0 | 100.0 | 28.3 | +260.0% |
| sog | 3.5 | OVER | +100 to +129 | 25 | 15-10 | 60.0 | 47.0 | +42.5% |
| sog | 3.5 | OVER | +130 to +169 | 49 | 18-31 | 36.7 | 39.7 | -2.3% |
| sog | 3.5 | OVER | +170 to +249 | 66 | 17-49 | 25.8 | 32.1 | -13.5% |
| sog | 3.5 | OVER | +250 or longer | 281 | 46-235 | 16.4 | 18.1 | +7.3% |
| sog | 3.5 | UNDER | +100 to +129 | 10 | 3-7 | 30.0 | 47.7 | -35.2% |

## SOG Over 1.5: model-underdog sides by fair odds

The same sides for this one line, tracked on their own (readiness above).

Positive EV, operator's books:

| fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|
| +100 to +109 | 39 | 16-23 | 41.0 | 49.1 | -10.5% |
| +110 to +119 | 20 | 4-16 | 20.0 | 46.8 | -56.0% |
| +120 to +129 | 14 | 7-7 | 50.0 | 44.6 | +16.3% |
| +130 to +169 | 12 | 4-8 | 33.3 | 41.9 | -14.6% |
| +170 to +249 | 1 | 0-1 | 0.0 | 34.5 | -100.0% |

Positive EV, all books:

| fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|
| +100 to +109 | 52 | 23-29 | 44.2 | 49.0 | -3.3% |
| +110 to +119 | 35 | 9-26 | 25.7 | 46.7 | -42.8% |
| +120 to +129 | 21 | 9-12 | 42.9 | 44.6 | -0.5% |
| +130 to +169 | 14 | 4-10 | 28.6 | 41.8 | -26.8% |
| +170 to +249 | 2 | 0-2 | 0.0 | 32.7 | -100.0% |

Any EV, operator's books:

| fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|
| +100 to +109 | 114 | 57-57 | 50.0 | 49.0 | -0.7% |
| +110 to +119 | 105 | 44-61 | 41.9 | 46.6 | -16.4% |
| +120 to +129 | 73 | 43-30 | 58.9 | 44.5 | +22.9% |
| +130 to +169 | 175 | 81-94 | 46.3 | 40.7 | +0.7% |
| +170 to +249 | 75 | 27-48 | 36.0 | 33.7 | -15.3% |
| +250 or longer | 14 | 6-8 | 42.9 | 25.7 | +12.9% |

## Every underdog side, any EV — operator's books

Context: the same sides without the positive-EV condition.

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 254 | 128-126 | 50.4 | 46.4 | +4.9% |
| points | OVER | +130 to +169 | 334 | 154-180 | 46.1 | 40.4 | +8.1% |
| points | OVER | +170 to +249 | 316 | 114-202 | 36.1 | 33.4 | -4.0% |
| points | OVER | +250 or longer | 2909 | 297-2612 | 10.2 | 9.0 | -32.7% |
| points | UNDER | +100 to +129 | 296 | 141-155 | 47.6 | 46.7 | -0.3% |
| points | UNDER | +130 to +169 | 200 | 78-122 | 39.0 | 40.6 | -11.1% |
| points | UNDER | +170 to +249 | 119 | 39-80 | 32.8 | 33.8 | -12.2% |
| points | UNDER | +250 or longer | 47 | 11-36 | 23.4 | 24.2 | -23.1% |
| sog | OVER | +100 to +129 | 512 | 238-274 | 46.5 | 46.9 | -2.8% |
| sog | OVER | +130 to +169 | 540 | 237-303 | 43.9 | 40.3 | +3.4% |
| sog | OVER | +170 to +249 | 662 | 213-449 | 32.2 | 32.8 | -10.2% |
| sog | OVER | +250 or longer | 2486 | 434-2052 | 17.5 | 15.8 | -14.8% |
| sog | UNDER | +100 to +129 | 410 | 214-196 | 52.2 | 46.9 | +4.4% |
| sog | UNDER | +130 to +169 | 384 | 170-214 | 44.3 | 40.5 | -3.9% |
| sog | UNDER | +170 to +249 | 268 | 90-178 | 33.6 | 33.6 | -23.7% |
| sog | UNDER | +250 or longer | 215 | 26-189 | 12.1 | 20.1 | -64.2% |

Return is profit per unit at the stated price; Model % is the model's mean probability for the side. Small cells move by chance: about +/-10 points of return at 100 picks.
