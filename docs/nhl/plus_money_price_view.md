# NHL plus-money price view (through 2026-11-01)

Rebuilt weekly with the model evaluation. Every graded production pick of the current SOG and Points models, settled at one unit on both sides at the best quoted price before the prediction: across all books (members' view) and at the operator's books (BetOnline, Novig, Kalshi, Pinnacle). Only plus-money prices are shown here; every price is in `priced_rows.csv` under `artifacts/analysis/nhl/price_view/weekly_2026-11-01/`. Collection only: no filter or threshold is chosen from these tables until the readiness counts are met.

- Points: goals + assists model (`8cda6061…`), 1 slates (2026-11-01 to 2026-11-01).
- SOG: `NHL_SOG_COUNT_HGB_V1`, 8 slates from 2026-10-25.

## Readiness

Plus-money model picks at fair -133 or shorter (8rain's fair-odds floor) at the operator's books; an evaluation is due once a market has 200.

| Market | Picks | Needed | Ready |
|---|---|---|---|
| sog | 10 | 200 | no |
| points | 0 | 200 | no |

## Model picks at plus money — Operator's books

By model fair odds and price:

| market | side | fair_band | plus_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | OVER | longer than -133 | +100 to +129 | 1 | 0-1 | 0.0 | 51.6 | -100.0% |
| points | UNDER | longer than -133 | +100 to +129 | 1 | 0-1 | 0.0 | 55.5 | -100.0% |
| sog | OVER | -133 to -200 | +100 to +129 | 5 | 4-1 | 80.0 | 59.1 | +64.2% |
| sog | OVER | longer than -133 | +100 to +129 | 39 | 13-26 | 33.3 | 52.0 | -30.2% |
| sog | OVER | longer than -133 | +130 to +169 | 3 | 1-2 | 33.3 | 51.6 | -16.7% |
| sog | OVER | longer than -133 | +170 to +249 | 1 | 0-1 | 0.0 | 50.9 | -100.0% |
| sog | UNDER | -133 to -200 | +100 to +129 | 5 | 3-2 | 60.0 | 59.9 | +22.8% |
| sog | UNDER | longer than -133 | +100 to +129 | 35 | 24-11 | 68.6 | 52.6 | +43.2% |
| sog | UNDER | longer than -133 | +130 to +169 | 4 | 2-2 | 50.0 | 51.2 | +15.0% |

By line:

| market | line | side | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | 1 | 0-1 | 0.0 | 51.6 | -100.0% |
| points | 0.5 | UNDER | 1 | 0-1 | 0.0 | 55.5 | -100.0% |
| sog | 1.5 | OVER | 19 | 6-13 | 31.6 | 52.5 | -34.7% |
| sog | 1.5 | UNDER | 32 | 21-11 | 65.6 | 53.4 | +38.4% |
| sog | 2.5 | OVER | 26 | 10-16 | 38.5 | 52.9 | -18.4% |
| sog | 2.5 | UNDER | 8 | 5-3 | 62.5 | 52.6 | +28.0% |
| sog | 3.5 | OVER | 3 | 2-1 | 66.7 | 52.3 | +43.3% |
| sog | 3.5 | UNDER | 4 | 3-1 | 75.0 | 53.9 | +58.5% |

## Model picks at plus money — All books

By model fair odds and price:

| market | side | fair_band | plus_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|---|
| points | OVER | longer than -133 | +100 to +129 | 1 | 0-1 | 0.0 | 51.6 | -100.0% |
| points | UNDER | longer than -133 | +100 to +129 | 1 | 0-1 | 0.0 | 55.5 | -100.0% |
| sog | OVER | -133 to -200 | +100 to +129 | 8 | 5-3 | 62.5 | 59.8 | +27.6% |
| sog | OVER | longer than -133 | +100 to +129 | 50 | 15-35 | 30.0 | 52.2 | -37.1% |
| sog | OVER | longer than -133 | +130 to +169 | 5 | 2-3 | 40.0 | 52.2 | -4.0% |
| sog | OVER | longer than -133 | +170 to +249 | 1 | 0-1 | 0.0 | 50.9 | -100.0% |
| sog | UNDER | -133 to -200 | +100 to +129 | 13 | 9-4 | 69.2 | 60.2 | +42.6% |
| sog | UNDER | -200 or shorter | +100 to +129 | 1 | 1-0 | 100.0 | 67.1 | +100.0% |
| sog | UNDER | longer than -133 | +100 to +129 | 58 | 35-23 | 60.3 | 52.8 | +26.5% |
| sog | UNDER | longer than -133 | +130 to +169 | 9 | 5-4 | 55.6 | 52.1 | +31.7% |

By line:

| market | line | side | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | 0.5 | OVER | 1 | 0-1 | 0.0 | 51.6 | -100.0% |
| points | 0.5 | UNDER | 1 | 0-1 | 0.0 | 55.5 | -100.0% |
| sog | 1.5 | OVER | 30 | 7-23 | 23.3 | 53.3 | -51.8% |
| sog | 1.5 | UNDER | 63 | 39-24 | 61.9 | 54.3 | +31.2% |
| sog | 2.5 | OVER | 31 | 13-18 | 41.9 | 52.9 | -11.1% |
| sog | 2.5 | UNDER | 14 | 8-6 | 57.1 | 53.3 | +18.0% |
| sog | 3.5 | OVER | 3 | 2-1 | 66.7 | 52.3 | +48.3% |
| sog | 3.5 | UNDER | 4 | 3-1 | 75.0 | 53.9 | +63.5% |

## Against the model at plus money — operator's books


| market | side | plus_band | Picks | W-L | Win % | Model % | Return |
|---|---|---|---|---|---|---|---|
| points | OVER | +100 to +129 | 1 | 1-0 | 100.0 | 40.6 | +113.0% |
| points | OVER | +130 to +169 | 4 | 2-2 | 50.0 | 36.9 | +25.2% |
| points | OVER | +170 to +249 | 4 | 2-2 | 50.0 | 31.5 | +53.8% |
| points | OVER | +250 or longer | 32 | 8-24 | 25.0 | 8.0 | +130.6% |
| points | UNDER | +100 to +129 | 3 | 1-2 | 33.3 | 46.7 | -29.0% |
| points | UNDER | +130 to +169 | 5 | 2-3 | 40.0 | 41.3 | +0.2% |
| sog | OVER | +100 to +129 | 150 | 72-78 | 48.0 | 43.3 | +2.2% |
| sog | OVER | +130 to +169 | 152 | 60-92 | 39.5 | 37.7 | -3.3% |
| sog | OVER | +170 to +249 | 293 | 93-200 | 31.7 | 29.5 | -3.8% |
| sog | OVER | +250 or longer | 724 | 111-613 | 15.3 | 14.1 | -19.1% |
| sog | UNDER | +100 to +129 | 139 | 63-76 | 45.3 | 42.5 | -3.9% |
| sog | UNDER | +130 to +169 | 78 | 29-49 | 37.2 | 38.4 | -10.6% |
| sog | UNDER | +170 to +249 | 19 | 4-15 | 21.1 | 30.4 | -39.0% |
| sog | UNDER | +250 or longer | 25 | 2-23 | 8.0 | 16.7 | -63.0% |

Return is profit per unit staked at the stated price; Model % is the model's mean probability for the side. Small cells move by chance: about +/-10 points of return at 100 picks.
