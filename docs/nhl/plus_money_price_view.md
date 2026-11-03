# NHL plus-money price view (through 2026-11-02)

Rebuilt weekly with the model evaluation. The subject is the model's underdog side: every side of a graded production pick whose model fair odds are plus money (the model gives it under 50%), banded by those fair odds, as the 8rain fair-odds filter would show it at +100 or longer. Each side is settled at one unit at the best quoted price before the prediction, at the operator's books (BetOnline, Novig, Kalshi, Pinnacle) and across all books (members' view). Positive EV means the book's price is longer than the model's fair price. Every side and price is in `priced_rows.csv` under `artifacts/analysis/nhl/price_view/weekly_2026-11-02/`. Collection only: no filter is chosen from these tables until the readiness counts are met.

- Points: goals + assists model (`8cda6061…`), 2 slates (2026-11-01 to 2026-11-02).
- SOG: `NHL_SOG_COUNT_HGB_V1`, 9 slates from 2026-10-25.

## Readiness

Positive-EV underdog sides at the operator's books; a market is ready for evaluation when every fair-odds band has 100.

| Market | Fair odds | Picks | Needed | Ready |
|---|---|---|---|---|
| sog | +100 to +129 | 91 | 100 | no |
| sog | +130 to +169 | 65 | 100 | no |
| sog | +170 to +249 | 61 | 100 | no |
| sog | +250 or longer | 62 | 100 | no |
| points | +100 to +129 | 9 | 100 | no |
| points | +130 to +169 | 12 | 100 | no |
| points | +170 to +249 | 5 | 100 | no |
| points | +250 or longer | 15 | 100 | no |
| sog over 1.5 | +100 to +109 | 13 | 100 | no |
| sog over 1.5 | +110 to +119 | 6 | 100 | no |
| sog over 1.5 | +120 to +129 | 4 | 100 | no |
| sog over 1.5 | +130 to +169 | 3 | 100 | no |
| sog over 1.5 | +170 to +249 | 0 | 100 | no |
| sog over 1.5 | +250 or longer | 0 | 100 | no |

## Underdog sides with positive EV — operator's books

By fair odds:

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 3 | 0-3 | 0.0 | 46.3 | -100.0% |
| points | OVER | +130 to +169 | 6 | 5-1 | 83.3 | 40.5 | +120.0% |
| points | OVER | +170 to +249 | 1 | 0-1 | 0.0 | 29.1 | -100.0% |
| points | OVER | +250 or longer | 15 | 1-14 | 6.7 | 17.0 | -16.7% |
| points | UNDER | +100 to +129 | 6 | 2-4 | 33.3 | 46.7 | -27.5% |
| points | UNDER | +130 to +169 | 6 | 2-4 | 33.3 | 40.7 | -19.5% |
| points | UNDER | +170 to +249 | 4 | 2-2 | 50.0 | 34.4 | +47.2% |
| sog | OVER | +100 to +129 | 55 | 22-33 | 40.0 | 47.5 | -7.5% |
| sog | OVER | +130 to +169 | 58 | 27-31 | 46.6 | 40.4 | +24.1% |
| sog | OVER | +170 to +249 | 60 | 14-46 | 23.3 | 32.4 | -21.6% |
| sog | OVER | +250 or longer | 62 | 14-48 | 22.6 | 20.9 | +33.5% |
| sog | UNDER | +100 to +129 | 36 | 17-19 | 47.2 | 47.6 | +4.3% |
| sog | UNDER | +130 to +169 | 7 | 3-4 | 42.9 | 41.9 | +2.1% |
| sog | UNDER | +170 to +249 | 1 | 0-1 | 0.0 | 35.0 | -100.0% |

By line, side and fair odds:

| market | line | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | +100 to +129 | 1 | 0-1 | 0.0 | 43.7 | -100.0% |
| points | 0.5 | OVER | +130 to +169 | 6 | 5-1 | 83.3 | 40.5 | +120.0% |
| points | 0.5 | OVER | +250 or longer | 2 | 0-2 | 0.0 | 25.3 | -100.0% |
| points | 0.5 | UNDER | +100 to +129 | 6 | 2-4 | 33.3 | 46.7 | -27.5% |
| points | 0.5 | UNDER | +130 to +169 | 6 | 2-4 | 33.3 | 40.7 | -19.5% |
| points | 0.5 | UNDER | +170 to +249 | 4 | 2-2 | 50.0 | 34.4 | +47.2% |
| points | 1.5 | OVER | +100 to +129 | 2 | 0-2 | 0.0 | 47.6 | -100.0% |
| points | 1.5 | OVER | +170 to +249 | 1 | 0-1 | 0.0 | 29.1 | -100.0% |
| points | 1.5 | OVER | +250 or longer | 11 | 1-10 | 9.1 | 14.6 | +13.6% |
| points | 2.5 | OVER | +250 or longer | 2 | 0-2 | 0.0 | 21.8 | -100.0% |
| sog | 1.5 | OVER | +100 to +129 | 23 | 8-15 | 34.8 | 47.9 | -24.5% |
| sog | 1.5 | OVER | +130 to +169 | 3 | 1-2 | 33.3 | 41.9 | -18.7% |
| sog | 1.5 | UNDER | +100 to +129 | 27 | 14-13 | 51.8 | 47.5 | +15.3% |
| sog | 1.5 | UNDER | +130 to +169 | 5 | 1-4 | 20.0 | 41.5 | -51.8% |
| sog | 2.5 | OVER | +100 to +129 | 25 | 10-15 | 40.0 | 47.2 | -3.0% |
| sog | 2.5 | OVER | +130 to +169 | 42 | 23-19 | 54.8 | 40.8 | +46.1% |
| sog | 2.5 | OVER | +170 to +249 | 44 | 11-33 | 25.0 | 32.9 | -16.2% |
| sog | 2.5 | OVER | +250 or longer | 13 | 3-10 | 23.1 | 26.0 | -7.9% |
| sog | 2.5 | UNDER | +100 to +129 | 5 | 2-3 | 40.0 | 47.6 | -12.6% |
| sog | 2.5 | UNDER | +130 to +169 | 2 | 2-0 | 100.0 | 42.8 | +137.0% |
| sog | 2.5 | UNDER | +170 to +249 | 1 | 0-1 | 0.0 | 35.0 | -100.0% |
| sog | 3.5 | OVER | +100 to +129 | 7 | 4-3 | 57.1 | 46.9 | +32.0% |
| sog | 3.5 | OVER | +130 to +169 | 13 | 3-10 | 23.1 | 39.0 | -36.9% |
| sog | 3.5 | OVER | +170 to +249 | 16 | 3-13 | 18.8 | 31.2 | -36.7% |
| sog | 3.5 | OVER | +250 or longer | 49 | 11-38 | 22.4 | 19.6 | +44.5% |
| sog | 3.5 | UNDER | +100 to +129 | 4 | 1-3 | 25.0 | 48.0 | -48.5% |

## Underdog sides with positive EV — all books

By fair odds:

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 3 | 0-3 | 0.0 | 46.3 | -100.0% |
| points | OVER | +130 to +169 | 9 | 6-3 | 66.7 | 40.4 | +80.2% |
| points | OVER | +170 to +249 | 2 | 1-1 | 50.0 | 30.7 | +55.0% |
| points | OVER | +250 or longer | 20 | 1-19 | 5.0 | 15.7 | -37.5% |
| points | UNDER | +100 to +129 | 7 | 2-5 | 28.6 | 46.8 | -37.6% |
| points | UNDER | +130 to +169 | 6 | 2-4 | 33.3 | 40.7 | -15.8% |
| points | UNDER | +170 to +249 | 5 | 3-2 | 60.0 | 34.7 | +80.6% |
| sog | OVER | +100 to +129 | 70 | 29-41 | 41.4 | 47.4 | -4.5% |
| sog | OVER | +130 to +169 | 63 | 29-34 | 46.0 | 40.4 | +24.1% |
| sog | OVER | +170 to +249 | 81 | 21-60 | 25.9 | 32.4 | -12.4% |
| sog | OVER | +250 or longer | 112 | 24-88 | 21.4 | 20.3 | +27.1% |
| sog | UNDER | +100 to +129 | 61 | 32-29 | 52.5 | 47.6 | +15.8% |
| sog | UNDER | +130 to +169 | 17 | 7-10 | 41.2 | 41.4 | +4.8% |
| sog | UNDER | +170 to +249 | 13 | 4-9 | 30.8 | 33.5 | +6.5% |
| sog | UNDER | +250 or longer | 2 | 2-0 | 100.0 | 28.3 | +265.0% |

By line, side and fair odds:

| market | line | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | +100 to +129 | 1 | 0-1 | 0.0 | 43.7 | -100.0% |
| points | 0.5 | OVER | +130 to +169 | 9 | 6-3 | 66.7 | 40.4 | +80.2% |
| points | 0.5 | OVER | +170 to +249 | 1 | 1-0 | 100.0 | 32.4 | +210.0% |
| points | 0.5 | OVER | +250 or longer | 3 | 0-3 | 0.0 | 25.9 | -100.0% |
| points | 0.5 | UNDER | +100 to +129 | 7 | 2-5 | 28.6 | 46.8 | -37.6% |
| points | 0.5 | UNDER | +130 to +169 | 6 | 2-4 | 33.3 | 40.7 | -15.8% |
| points | 0.5 | UNDER | +170 to +249 | 5 | 3-2 | 60.0 | 34.7 | +80.6% |
| points | 1.5 | OVER | +100 to +129 | 2 | 0-2 | 0.0 | 47.6 | -100.0% |
| points | 1.5 | OVER | +170 to +249 | 1 | 0-1 | 0.0 | 29.1 | -100.0% |
| points | 1.5 | OVER | +250 or longer | 12 | 1-11 | 8.3 | 15.0 | +4.2% |
| points | 2.5 | OVER | +250 or longer | 5 | 0-5 | 0.0 | 11.3 | -100.0% |
| sog | 1.5 | OVER | +100 to +129 | 36 | 14-22 | 38.9 | 47.5 | -14.6% |
| sog | 1.5 | OVER | +130 to +169 | 4 | 1-3 | 25.0 | 41.3 | -39.0% |
| sog | 1.5 | UNDER | +100 to +129 | 44 | 26-18 | 59.1 | 47.5 | +31.8% |
| sog | 1.5 | UNDER | +130 to +169 | 12 | 3-9 | 25.0 | 41.3 | -36.1% |
| sog | 1.5 | UNDER | +170 to +249 | 11 | 3-8 | 27.3 | 33.7 | -5.9% |
| sog | 1.5 | UNDER | +250 or longer | 1 | 1-0 | 100.0 | 28.3 | +270.0% |
| sog | 2.5 | OVER | +100 to +129 | 27 | 11-16 | 40.7 | 47.3 | -0.5% |
| sog | 2.5 | OVER | +130 to +169 | 45 | 24-21 | 53.3 | 40.8 | +43.4% |
| sog | 2.5 | OVER | +170 to +249 | 56 | 14-42 | 25.0 | 32.9 | -16.2% |
| sog | 2.5 | OVER | +250 or longer | 29 | 6-23 | 20.7 | 25.7 | -15.2% |
| sog | 2.5 | UNDER | +100 to +129 | 13 | 5-8 | 38.5 | 47.7 | -18.9% |
| sog | 2.5 | UNDER | +130 to +169 | 5 | 4-1 | 80.0 | 41.5 | +103.0% |
| sog | 2.5 | UNDER | +170 to +249 | 2 | 1-1 | 50.0 | 32.4 | +75.0% |
| sog | 2.5 | UNDER | +250 or longer | 1 | 1-0 | 100.0 | 28.3 | +260.0% |
| sog | 3.5 | OVER | +100 to +129 | 7 | 4-3 | 57.1 | 46.9 | +32.0% |
| sog | 3.5 | OVER | +130 to +169 | 14 | 4-10 | 28.6 | 39.0 | -19.7% |
| sog | 3.5 | OVER | +170 to +249 | 25 | 7-18 | 28.0 | 31.3 | -3.9% |
| sog | 3.5 | OVER | +250 or longer | 83 | 18-65 | 21.7 | 18.5 | +41.8% |
| sog | 3.5 | UNDER | +100 to +129 | 4 | 1-3 | 25.0 | 48.0 | -47.5% |

## SOG Over 1.5: model-underdog sides by fair odds

The same sides for this one line, tracked on their own (readiness above).

Positive EV, operator's books:

| fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|
| +100 to +109 | 13 | 4-9 | 30.8 | 49.3 | -34.2% |
| +110 to +119 | 6 | 2-4 | 33.3 | 46.7 | -27.2% |
| +120 to +129 | 4 | 2-2 | 50.0 | 45.0 | +11.0% |
| +130 to +169 | 3 | 1-2 | 33.3 | 41.9 | -18.7% |

Positive EV, all books:

| fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|
| +100 to +109 | 18 | 7-11 | 38.9 | 49.2 | -16.3% |
| +110 to +119 | 11 | 4-7 | 36.4 | 46.5 | -18.6% |
| +120 to +129 | 7 | 3-4 | 42.9 | 44.8 | -3.7% |
| +130 to +169 | 4 | 1-3 | 25.0 | 41.3 | -39.0% |

Any EV, operator's books:

| fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|
| +100 to +109 | 44 | 22-22 | 50.0 | 49.1 | -2.1% |
| +110 to +119 | 31 | 10-21 | 32.3 | 46.6 | -35.9% |
| +120 to +129 | 30 | 19-11 | 63.3 | 44.5 | +30.4% |
| +130 to +169 | 72 | 32-40 | 44.4 | 40.5 | -3.8% |
| +170 to +249 | 36 | 10-26 | 27.8 | 33.3 | -35.1% |
| +250 or longer | 8 | 5-3 | 62.5 | 25.6 | +60.1% |

## Every underdog side, any EV — operator's books

Context: the same sides without the positive-EV condition.

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 13 | 4-9 | 30.8 | 46.3 | -39.7% |
| points | OVER | +130 to +169 | 23 | 14-9 | 60.9 | 39.5 | +45.2% |
| points | OVER | +170 to +249 | 15 | 4-11 | 26.7 | 33.6 | -30.8% |
| points | OVER | +250 or longer | 172 | 17-155 | 9.9 | 8.9 | -19.2% |
| points | UNDER | +100 to +129 | 17 | 8-9 | 47.1 | 47.4 | -9.6% |
| points | UNDER | +130 to +169 | 12 | 3-9 | 25.0 | 40.0 | -42.8% |
| points | UNDER | +170 to +249 | 6 | 3-3 | 50.0 | 34.6 | +40.8% |
| points | UNDER | +250 or longer | 2 | 1-1 | 50.0 | 20.1 | +51.5% |
| sog | OVER | +100 to +129 | 176 | 81-95 | 46.0 | 46.9 | -4.8% |
| sog | OVER | +130 to +169 | 178 | 80-98 | 44.9 | 40.4 | +6.2% |
| sog | OVER | +170 to +249 | 236 | 75-161 | 31.8 | 32.6 | -9.9% |
| sog | OVER | +250 or longer | 895 | 155-740 | 17.3 | 15.4 | -18.6% |
| sog | UNDER | +100 to +129 | 149 | 82-67 | 55.0 | 47.3 | +8.5% |
| sog | UNDER | +130 to +169 | 130 | 59-71 | 45.4 | 40.7 | -4.6% |
| sog | UNDER | +170 to +249 | 73 | 26-47 | 35.6 | 33.5 | -28.1% |
| sog | UNDER | +250 or longer | 70 | 10-60 | 14.3 | 20.5 | -62.7% |

Return is profit per unit at the stated price; Model % is the model's mean probability for the side. Small cells move by chance: about +/-10 points of return at 100 picks.
