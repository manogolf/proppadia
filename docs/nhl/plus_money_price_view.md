# NHL plus-money price view (through 2026-11-01)

Rebuilt weekly with the model evaluation. The subject is the model's underdog side: every side of a graded production pick whose model fair odds are plus money (the model gives it under 50%), banded by those fair odds, as the 8rain fair-odds filter would show it at +100 or longer. Each side is settled at one unit at the best quoted price before the prediction, at the operator's books (BetOnline, Novig, Kalshi, Pinnacle) and across all books (members' view). Positive EV means the book's price is longer than the model's fair price. Every side and price is in `priced_rows.csv` under `artifacts/analysis/nhl/price_view/weekly_2026-11-01/`. Collection only: no filter is chosen from these tables until the readiness counts are met.

- Points: goals + assists model (`8cda6061…`), 1 slates (2026-11-01 to 2026-11-01).
- SOG: `NHL_SOG_COUNT_HGB_V1`, 8 slates from 2026-10-25.

## Readiness

Positive-EV underdog sides at the operator's books; a market is ready for evaluation when every fair-odds band has 100.

| Market | Fair odds | Picks | Needed | Ready |
|---|---|---|---|---|
| sog | +100 to +129 | 84 | 100 | no |
| sog | +130 to +169 | 61 | 100 | no |
| sog | +170 to +249 | 57 | 100 | no |
| sog | +250 or longer | 58 | 100 | no |
| points | +100 to +129 | 3 | 100 | no |
| points | +130 to +169 | 3 | 100 | no |
| points | +170 to +249 | 0 | 100 | no |
| points | +250 or longer | 1 | 100 | no |

## Underdog sides with positive EV — operator's books

By fair odds:

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +130 to +169 | 2 | 2-0 | 100.0 | 39.7 | +154.0% |
| points | OVER | +250 or longer | 1 | 0-1 | 0.0 | 16.5 | -100.0% |
| points | UNDER | +100 to +129 | 3 | 1-2 | 33.3 | 47.1 | -29.0% |
| points | UNDER | +130 to +169 | 1 | 1-0 | 100.0 | 41.5 | +145.0% |
| sog | OVER | +100 to +129 | 52 | 21-31 | 40.4 | 47.4 | -6.4% |
| sog | OVER | +130 to +169 | 54 | 25-29 | 46.3 | 40.3 | +23.8% |
| sog | OVER | +170 to +249 | 56 | 13-43 | 23.2 | 32.6 | -23.1% |
| sog | OVER | +250 or longer | 58 | 13-45 | 22.4 | 20.8 | +36.5% |
| sog | UNDER | +100 to +129 | 32 | 14-18 | 43.8 | 47.5 | -2.8% |
| sog | UNDER | +130 to +169 | 7 | 3-4 | 42.9 | 41.9 | +2.1% |
| sog | UNDER | +170 to +249 | 1 | 0-1 | 0.0 | 35.0 | -100.0% |

By line, side and fair odds:

| market | line | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | +130 to +169 | 2 | 2-0 | 100.0 | 39.7 | +154.0% |
| points | 0.5 | UNDER | +100 to +129 | 3 | 1-2 | 33.3 | 47.1 | -29.0% |
| points | 0.5 | UNDER | +130 to +169 | 1 | 1-0 | 100.0 | 41.5 | +145.0% |
| points | 1.5 | OVER | +250 or longer | 1 | 0-1 | 0.0 | 16.5 | -100.0% |
| sog | 1.5 | OVER | +100 to +129 | 21 | 7-14 | 33.3 | 47.7 | -27.8% |
| sog | 1.5 | OVER | +130 to +169 | 3 | 1-2 | 33.3 | 41.9 | -18.7% |
| sog | 1.5 | UNDER | +100 to +129 | 24 | 12-12 | 50.0 | 47.4 | +11.4% |
| sog | 1.5 | UNDER | +130 to +169 | 5 | 1-4 | 20.0 | 41.5 | -51.8% |
| sog | 2.5 | OVER | +100 to +129 | 24 | 10-14 | 41.7 | 47.2 | +1.0% |
| sog | 2.5 | OVER | +130 to +169 | 39 | 22-17 | 56.4 | 40.6 | +50.9% |
| sog | 2.5 | OVER | +170 to +249 | 41 | 10-31 | 24.4 | 33.1 | -19.6% |
| sog | 2.5 | OVER | +250 or longer | 13 | 3-10 | 23.1 | 26.0 | -7.9% |
| sog | 2.5 | UNDER | +100 to +129 | 5 | 2-3 | 40.0 | 47.6 | -12.6% |
| sog | 2.5 | UNDER | +130 to +169 | 2 | 2-0 | 100.0 | 42.8 | +137.0% |
| sog | 2.5 | UNDER | +170 to +249 | 1 | 0-1 | 0.0 | 35.0 | -100.0% |
| sog | 3.5 | OVER | +100 to +129 | 7 | 4-3 | 57.1 | 46.9 | +32.0% |
| sog | 3.5 | OVER | +130 to +169 | 12 | 2-10 | 16.7 | 38.7 | -53.8% |
| sog | 3.5 | OVER | +170 to +249 | 15 | 3-12 | 20.0 | 31.4 | -32.5% |
| sog | 3.5 | OVER | +250 or longer | 45 | 10-35 | 22.2 | 19.3 | +49.4% |
| sog | 3.5 | UNDER | +100 to +129 | 3 | 0-3 | 0.0 | 47.7 | -100.0% |

## Underdog sides with positive EV — all books

By fair odds:

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +130 to +169 | 2 | 2-0 | 100.0 | 39.7 | +154.0% |
| points | OVER | +250 or longer | 1 | 0-1 | 0.0 | 16.5 | -100.0% |
| points | UNDER | +100 to +129 | 4 | 1-3 | 25.0 | 47.3 | -46.2% |
| points | UNDER | +130 to +169 | 1 | 1-0 | 100.0 | 41.5 | +145.0% |
| points | UNDER | +170 to +249 | 1 | 1-0 | 100.0 | 36.0 | +210.0% |
| sog | OVER | +100 to +129 | 66 | 27-39 | 40.9 | 47.3 | -5.2% |
| sog | OVER | +130 to +169 | 59 | 27-32 | 45.8 | 40.3 | +23.8% |
| sog | OVER | +170 to +249 | 77 | 20-57 | 26.0 | 32.6 | -13.2% |
| sog | OVER | +250 or longer | 102 | 22-80 | 21.6 | 20.1 | +30.9% |
| sog | UNDER | +100 to +129 | 55 | 27-28 | 49.1 | 47.5 | +8.4% |
| sog | UNDER | +130 to +169 | 17 | 7-10 | 41.2 | 41.4 | +4.8% |
| sog | UNDER | +170 to +249 | 11 | 2-9 | 18.2 | 34.2 | -37.7% |
| sog | UNDER | +250 or longer | 1 | 1-0 | 100.0 | 28.3 | +270.0% |

By line, side and fair odds:

| market | line | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | +130 to +169 | 2 | 2-0 | 100.0 | 39.7 | +154.0% |
| points | 0.5 | UNDER | +100 to +129 | 4 | 1-3 | 25.0 | 47.3 | -46.2% |
| points | 0.5 | UNDER | +130 to +169 | 1 | 1-0 | 100.0 | 41.5 | +145.0% |
| points | 0.5 | UNDER | +170 to +249 | 1 | 1-0 | 100.0 | 36.0 | +210.0% |
| points | 1.5 | OVER | +250 or longer | 1 | 0-1 | 0.0 | 16.5 | -100.0% |
| sog | 1.5 | OVER | +100 to +129 | 33 | 12-21 | 36.4 | 47.4 | -19.9% |
| sog | 1.5 | OVER | +130 to +169 | 4 | 1-3 | 25.0 | 41.3 | -39.0% |
| sog | 1.5 | UNDER | +100 to +129 | 40 | 23-17 | 57.5 | 47.4 | +27.8% |
| sog | 1.5 | UNDER | +130 to +169 | 12 | 3-9 | 25.0 | 41.3 | -36.1% |
| sog | 1.5 | UNDER | +170 to +249 | 10 | 2-8 | 20.0 | 34.1 | -31.5% |
| sog | 1.5 | UNDER | +250 or longer | 1 | 1-0 | 100.0 | 28.3 | +270.0% |
| sog | 2.5 | OVER | +100 to +129 | 26 | 11-15 | 42.3 | 47.3 | +3.3% |
| sog | 2.5 | OVER | +130 to +169 | 42 | 23-19 | 54.8 | 40.6 | +47.6% |
| sog | 2.5 | OVER | +170 to +249 | 53 | 13-40 | 24.5 | 33.1 | -19.2% |
| sog | 2.5 | OVER | +250 or longer | 26 | 6-20 | 23.1 | 25.6 | -5.4% |
| sog | 2.5 | UNDER | +100 to +129 | 12 | 4-8 | 33.3 | 47.5 | -29.2% |
| sog | 2.5 | UNDER | +130 to +169 | 5 | 4-1 | 80.0 | 41.5 | +103.0% |
| sog | 2.5 | UNDER | +170 to +249 | 1 | 0-1 | 0.0 | 35.0 | -100.0% |
| sog | 3.5 | OVER | +100 to +129 | 7 | 4-3 | 57.1 | 46.9 | +32.0% |
| sog | 3.5 | OVER | +130 to +169 | 13 | 3-10 | 23.1 | 38.8 | -34.1% |
| sog | 3.5 | OVER | +170 to +249 | 24 | 7-17 | 29.2 | 31.4 | +0.1% |
| sog | 3.5 | OVER | +250 or longer | 76 | 16-60 | 21.1 | 18.2 | +43.3% |
| sog | 3.5 | UNDER | +100 to +129 | 3 | 0-3 | 0.0 | 47.7 | -100.0% |

## Every underdog side, any EV — operator's books

Context: the same sides without the positive-EV condition.

| market | side | fair_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 1 | 1-0 | 100.0 | 44.5 | +75.2% |
| points | OVER | +130 to +169 | 4 | 3-1 | 75.0 | 39.3 | +80.2% |
| points | OVER | +170 to +249 | 3 | 1-2 | 33.3 | 34.5 | -12.3% |
| points | OVER | +250 or longer | 34 | 9-25 | 26.5 | 9.1 | +127.2% |
| points | UNDER | +100 to +129 | 5 | 2-3 | 40.0 | 47.5 | -21.0% |
| points | UNDER | +130 to +169 | 3 | 1-2 | 33.3 | 40.5 | -18.3% |
| points | UNDER | +170 to +249 | 1 | 1-0 | 100.0 | 36.0 | +156.0% |
| sog | OVER | +100 to +129 | 165 | 77-88 | 46.7 | 46.9 | -3.4% |
| sog | OVER | +130 to +169 | 170 | 77-93 | 45.3 | 40.3 | +7.1% |
| sog | OVER | +170 to +249 | 227 | 73-154 | 32.2 | 32.7 | -9.4% |
| sog | OVER | +250 or longer | 845 | 147-698 | 17.4 | 15.4 | -18.2% |
| sog | UNDER | +100 to +129 | 137 | 74-63 | 54.0 | 47.2 | +6.2% |
| sog | UNDER | +130 to +169 | 124 | 57-67 | 46.0 | 40.6 | -3.5% |
| sog | UNDER | +170 to +249 | 67 | 22-45 | 32.8 | 33.7 | -33.4% |
| sog | UNDER | +250 or longer | 66 | 9-57 | 13.6 | 20.2 | -64.2% |

Return is profit per unit at the stated price; Model % is the model's mean probability for the side. Small cells move by chance: about +/-10 points of return at 100 picks.
