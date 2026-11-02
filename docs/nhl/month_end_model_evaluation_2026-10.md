# NHL month-end evaluation — October 2026, published vs updated models

Same graded picks, two probabilities: what was published in October, and what the models cut over on 2026-11-01 give for those player-games (Points goals + assists `8cda6061…`; Saves `NHL_SAVES_SHOTS_X_SAVE_RATE_V1`). Retrospective: both models were chosen on 2025 holdouts, and their research also looked at these October lines, so this shows where the bands now sit; it is not a live result. Points rows are the weekly evaluation's governed rows from 2026-10-10; two-sided lines as in the weekly report. Saves are the settled starters; "two-sided" for Saves means a market no-vig price on that line.

Points coverage: 5,511 of 5,511 published player-games traced to their capture inputs (0 untraced, left out of both columns). Saves: 5,252 starter-lines (404 starters).

## Probability quality

| Lane | Level | Model | n | W-L | Win % | Mean pick prob | Brier | Log loss |
|---|---|---|---:|---:|---:|---:|---:|---:|
| points | all | published | 16,533 | 14147-2386 | 85.6% | 86.9% | 0.1020 | 0.3239 |
| points | all | goals + assists | 16,533 | 14212-2321 | 86.0% | 86.4% | 0.0989 | 0.3129 |
| points | two_sided | published | 5,859 | 4355-1504 | 74.3% | 78.1% | 0.1754 | 0.5322 |
| points | two_sided | goals + assists | 5,859 | 4393-1466 | 75.0% | 76.5% | 0.1698 | 0.5127 |
| saves | all | published (Phoenix V2) | 5,252 | 3585-1667 | 68.3% | 77.5% | 0.2223 | 1.1154 |
| saves | all | refit | 5,252 | 3677-1575 | 70.0% | 69.4% | 0.1934 | 0.5685 |
| saves | two_sided | published (Phoenix V2) | 433 | 238-195 | 55.0% | 68.5% | 0.2818 | 1.4572 |
| saves | two_sided | refit | 433 | 241-192 | 55.7% | 60.0% | 0.2491 | 0.6918 |

## By line and side

Win % and the model's mean confidence on its pick; two-sided lines in the last columns.

| Lane | Model | Line | Side | W-L | Win % | Predicted | Two-sided W-L | Two-sided win % | Two-sided predicted |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|
| points | goals + assists | 0.5 | OVER | 607-355 | 63.1% | 59.0% | 471-267 | 63.8% | 58.9% |
| points | published | 0.5 | OVER | 483-299 | 61.8% | 58.2% | 368-204 | 64.3% | 58.4% |
| points | goals + assists | 0.5 | UNDER | 3202-1347 | 70.4% | 71.6% | 1415-783 | 64.4% | 66.7% |
| points | published | 0.5 | UNDER | 3258-1471 | 68.9% | 71.7% | 1478-886 | 62.5% | 67.9% |
| points | goals + assists | 1.5 | OVER | 1-4 | 20.0% | 52.2% | 1-3 | 25.0% | 52.6% |
| points | goals + assists | 1.5 | UNDER | 5002-504 | 90.8% | 91.7% | 1902-350 | 84.5% | 87.0% |
| points | published | 1.5 | UNDER | 5006-505 | 90.8% | 92.5% | 1905-351 | 84.4% | 88.7% |
| points | goals + assists | 2.5 | UNDER | 5400-111 | 98.0% | 98.2% | 604-63 | 90.6% | 93.2% |
| points | published | 2.5 | UNDER | 5400-111 | 98.0% | 98.5% | 604-63 | 90.6% | 94.8% |
| saves | published (Phoenix V2) | 18.5 | OVER | 298-87 | 77.4% | 86.0% |  |  |  |
| saves | refit | 18.5 | OVER | 312-90 | 77.6% | 74.2% |  |  |  |
| saves | published (Phoenix V2) | 18.5 | UNDER | 4-15 | 21.1% | 99.9% |  |  |  |
| saves | refit | 18.5 | UNDER | 1-1 | 50.0% | 51.2% |  |  |  |
| saves | published (Phoenix V2) | 19.5 | OVER | 283-102 | 73.5% | 80.6% | 1-3 | 25.0% | 71.1% |
| saves | refit | 19.5 | OVER | 290-100 | 74.4% | 69.6% |  |  |  |
| saves | published (Phoenix V2) | 19.5 | UNDER | 6-13 | 31.6% | 100.0% |  |  |  |
| saves | refit | 19.5 | UNDER | 8-6 | 57.1% | 53.0% | 3-1 | 75.0% | 54.3% |
| saves | published (Phoenix V2) | 20.5 | OVER | 261-124 | 67.8% | 74.3% | 6-6 | 50.0% | 69.9% |
| saves | refit | 20.5 | OVER | 253-105 | 70.7% | 65.5% | 3-2 | 60.0% | 55.5% |
| saves | published (Phoenix V2) | 20.5 | UNDER | 8-11 | 42.1% | 100.0% | 1-0 | 100.0% | 100.0% |
| saves | refit | 20.5 | UNDER | 27-19 | 58.7% | 54.6% | 5-3 | 62.5% | 55.4% |
| saves | published (Phoenix V2) | 21.5 | OVER | 241-136 | 63.9% | 67.6% | 14-18 | 43.8% | 63.8% |
| saves | refit | 21.5 | OVER | 210-93 | 69.3% | 62.2% | 5-5 | 50.0% | 53.7% |
| saves | published (Phoenix V2) | 21.5 | UNDER | 10-17 | 37.0% | 86.2% | 1-0 | 100.0% | 100.0% |
| saves | refit | 21.5 | UNDER | 53-48 | 52.5% | 56.6% | 14-9 | 60.9% | 58.0% |
| saves | published (Phoenix V2) | 22.5 | OVER | 196-133 | 59.6% | 62.0% | 23-21 | 52.3% | 60.6% |
| saves | refit | 22.5 | OVER | 146-77 | 65.5% | 59.8% | 6-6 | 50.0% | 52.7% |
| saves | published (Phoenix V2) | 22.5 | UNDER | 37-38 | 49.3% | 66.4% | 11-3 | 78.6% | 67.5% |
| saves | refit | 22.5 | UNDER | 93-88 | 51.4% | 58.2% | 26-20 | 56.5% | 59.0% |
| saves | published (Phoenix V2) | 23.5 | OVER | 118-91 | 56.5% | 58.4% | 15-17 | 46.9% | 57.8% |
| saves | refit | 23.5 | OVER | 89-45 | 66.4% | 58.9% | 4-5 | 44.4% | 53.8% |
| saves | published (Phoenix V2) | 23.5 | UNDER | 98-97 | 50.3% | 60.8% | 24-21 | 53.3% | 61.3% |
| saves | refit | 23.5 | UNDER | 144-126 | 53.3% | 60.2% | 36-32 | 52.9% | 59.7% |
| saves | refit | 24.5 | OVER | 49-33 | 59.8% | 57.2% | 4-4 | 50.0% | 53.9% |
| saves | published (Phoenix V2) | 24.5 | UNDER | 225-179 | 55.7% | 71.9% | 39-38 | 50.6% | 71.7% |
| saves | refit | 24.5 | UNDER | 192-130 | 59.6% | 63.6% | 35-34 | 50.7% | 61.1% |
| saves | refit | 25.5 | OVER | 26-20 | 56.5% | 55.4% | 2-6 | 25.0% | 55.6% |
| saves | published (Phoenix V2) | 25.5 | UNDER | 250-154 | 61.9% | 74.2% | 53-27 | 66.2% | 69.2% |
| saves | refit | 25.5 | UNDER | 230-128 | 64.2% | 67.1% | 47-25 | 65.3% | 62.1% |
| saves | refit | 26.5 | OVER | 10-8 | 55.6% | 54.3% | 3-2 | 60.0% | 53.1% |
| saves | published (Phoenix V2) | 26.5 | UNDER | 277-127 | 68.6% | 77.8% | 24-26 | 48.0% | 73.8% |
| saves | refit | 26.5 | UNDER | 269-117 | 69.7% | 70.5% | 22-23 | 48.9% | 63.6% |
| saves | refit | 27.5 | OVER | 4-2 | 66.7% | 53.1% |  |  |  |
| saves | published (Phoenix V2) | 27.5 | UNDER | 297-107 | 73.5% | 82.0% | 17-11 | 60.7% | 79.1% |
| saves | refit | 27.5 | UNDER | 295-103 | 74.1% | 74.2% | 17-11 | 60.7% | 62.8% |
| saves | published (Phoenix V2) | 28.5 | UNDER | 309-95 | 76.5% | 85.9% | 6-3 | 66.7% | 82.7% |
| saves | refit | 28.5 | UNDER | 309-95 | 76.5% | 77.8% | 6-3 | 66.7% | 61.3% |
| saves | published (Phoenix V2) | 29.5 | UNDER | 324-80 | 80.2% | 89.2% | 2-1 | 66.7% | 90.4% |
| saves | refit | 29.5 | UNDER | 324-80 | 80.2% | 81.4% | 2-1 | 66.7% | 64.5% |
| saves | published (Phoenix V2) | 30.5 | UNDER | 343-61 | 84.9% | 92.0% | 1-0 | 100.0% | 100.0% |
| saves | refit | 30.5 | UNDER | 343-61 | 84.9% | 84.5% | 1-0 | 100.0% | 74.2% |

## Model vs market by gap (two-sided lines)

Gap = model probability on its side minus the market's no-vig probability on that side. The closer log loss forecast better.

| Lane | Model | Gap | Side | W-L | Win % | Model | Market | Log loss model | Log loss market |
|---|---|---|---|---:|---:|---:|---:|---:|---:|
| points | published | model 10+ below | OVER | 25-6 | 80.6% | 55.8% | 70.3% | 0.6139 | 0.5077 |
| points | published | model 10+ below | UNDER | 43-16 | 72.9% | 58.8% | 71.6% | 0.6403 | 0.6059 |
| points | published | within 10 | OVER | 317-169 | 65.2% | 58.6% | 58.6% | 0.6530 | 0.6575 |
| points | published | within 10 | UNDER | 3169-840 | 79.0% | 80.8% | 77.6% | 0.4660 | 0.4571 |
| points | published | 10-15 above | OVER | 20-16 | 55.6% | 59.5% | 47.5% | 0.6652 | 0.6739 |
| points | published | 10-15 above | UNDER | 468-212 | 68.8% | 81.1% | 68.9% | 0.6229 | 0.5652 |
| points | published | over 15 above | OVER | 6-13 | 31.6% | 56.3% | 37.3% | 0.6961 | 0.5477 |
| points | published | over 15 above | UNDER | 307-232 | 57.0% | 76.7% | 56.3% | 0.7700 | 0.6422 |
| points | goals + assists | model 10+ below | OVER | 9-0 | 100.0% | 54.1% | 65.8% | 0.6154 | 0.4204 |
| points | goals + assists | model 10+ below | UNDER | 10-3 | 76.9% | 57.9% | 69.1% | 0.6320 | 0.5696 |
| points | goals + assists | within 10 | OVER | 452-266 | 63.0% | 58.8% | 58.7% | 0.6591 | 0.6528 |
| points | goals + assists | within 10 | UNDER | 3667-1076 | 77.3% | 79.3% | 76.3% | 0.4800 | 0.4762 |
| points | goals + assists | 10-15 above | OVER | 10-3 | 76.9% | 68.2% | 56.5% | 0.4446 | 0.5313 |
| points | goals + assists | 10-15 above | UNDER | 205-94 | 68.6% | 77.2% | 65.4% | 0.6303 | 0.6035 |
| points | goals + assists | over 15 above | OVER | 1-1 | 50.0% | 61.3% | 45.6% | 0.7372 | 0.7171 |
| points | goals + assists | over 15 above | UNDER | 39-23 | 62.9% | 76.1% | 58.0% | 0.7226 | 0.6647 |
| saves | published (Phoenix V2) | model 10+ below | UNDER | 1-1 | 50.0% | 52.0% | 62.7% | 0.7036 | 0.7306 |
| saves | published (Phoenix V2) | within 10 | OVER | 24-35 | 40.7% | 55.8% | 50.5% | 0.7218 | 0.7015 |
| saves | published (Phoenix V2) | within 10 | UNDER | 63-52 | 54.8% | 57.2% | 50.8% | 0.6921 | 0.6873 |
| saves | published (Phoenix V2) | 10-15 above | OVER | 15-6 | 71.4% | 61.5% | 49.5% | 0.6106 | 0.6915 |
| saves | published (Phoenix V2) | 10-15 above | UNDER | 31-19 | 62.0% | 61.7% | 49.7% | 0.6542 | 0.6908 |
| saves | published (Phoenix V2) | over 15 above | OVER | 20-24 | 45.5% | 70.3% | 49.9% | 0.8266 | 0.6784 |
| saves | published (Phoenix V2) | over 15 above | UNDER | 84-58 | 59.2% | 86.0% | 50.6% | 2.9964 | 0.6894 |
| saves | refit | within 10 | OVER | 25-28 | 47.2% | 53.2% | 51.4% | 0.7096 | 0.7024 |
| saves | refit | within 10 | UNDER | 108-81 | 57.1% | 56.3% | 50.8% | 0.6793 | 0.6891 |
| saves | refit | 10-15 above | OVER | 2-2 | 50.0% | 63.0% | 50.8% | 0.6982 | 0.6872 |
| saves | refit | 10-15 above | UNDER | 63-45 | 58.3% | 63.0% | 50.7% | 0.6798 | 0.6845 |
| saves | refit | over 15 above | UNDER | 43-36 | 54.4% | 68.9% | 50.6% | 0.7256 | 0.6905 |

## Confidence bands (segments with at least 40 picks)

Gap = actual win rate minus mean confidence in the pick; negative is overconfident. Flagged at 5+ points either way.

All lines:

| Lane | Model | Line | Side | Band | n | Win % | Predicted | Gap |
|---|---|---:|---|---|---:|---:|---:|---:|
| points | goals + assists | 0.5 | OVER | .50-.55 | 353 | 58.1% | 52.6% | +5.5 pts ⚑ |
| points | published | 0.5 | OVER | .50-.55 | 310 | 55.5% | 52.4% | +3.1 pts |
| points | goals + assists | 0.5 | OVER | .55-.60 | 254 | 62.2% | 57.2% | +5.0 pts |
| points | published | 0.5 | OVER | .55-.60 | 202 | 61.9% | 57.5% | +4.4 pts |
| points | goals + assists | 0.5 | OVER | .60-.70 | 276 | 68.5% | 64.0% | +4.4 pts |
| points | published | 0.5 | OVER | .60-.70 | 222 | 69.8% | 63.8% | +6.0 pts ⚑ |
| points | goals + assists | 0.5 | OVER | .70-1.00 | 79 | 69.6% | 75.4% | -5.8 pts ⚑ |
| points | published | 0.5 | OVER | .70-1.00 | 48 | 64.6% | 72.8% | -8.2 pts ⚑ |
| points | goals + assists | 0.5 | UNDER | .00-.30 | 2,616 | 78.4% | 79.1% | -0.8 pts |
| points | published | 0.5 | UNDER | .00-.30 | 2,871 | 76.1% | 78.2% | -2.1 pts |
| points | goals + assists | 0.5 | UNDER | .30-.40 | 1,200 | 63.7% | 65.4% | -1.6 pts |
| points | published | 0.5 | UNDER | .30-.40 | 1,157 | 60.0% | 65.4% | -5.4 pts ⚑ |
| points | goals + assists | 0.5 | UNDER | .40-.45 | 400 | 57.2% | 57.6% | -0.3 pts |
| points | published | 0.5 | UNDER | .40-.45 | 363 | 55.9% | 57.6% | -1.7 pts |
| points | goals + assists | 0.5 | UNDER | .45-.50 | 333 | 47.4% | 52.5% | -5.1 pts ⚑ |
| points | published | 0.5 | UNDER | .45-.50 | 338 | 51.8% | 52.6% | -0.8 pts |
| points | goals + assists | 1.5 | UNDER | .00-.30 | 5,368 | 91.6% | 92.4% | -0.9 pts |
| points | published | 1.5 | UNDER | .00-.30 | 5,418 | 91.3% | 92.9% | -1.7 pts |
| points | goals + assists | 1.5 | UNDER | .30-.40 | 103 | 69.9% | 66.6% | +3.3 pts |
| points | published | 1.5 | UNDER | .30-.40 | 84 | 67.9% | 66.0% | +1.9 pts |
| points | goals + assists | 2.5 | UNDER | .00-.30 | 5,511 | 98.0% | 98.2% | -0.2 pts |
| points | published | 2.5 | UNDER | .00-.30 | 5,511 | 98.0% | 98.5% | -0.5 pts |
| saves | refit | 18.5 | OVER | .60-.70 | 93 | 69.9% | 65.3% | +4.6 pts |
| saves | published (Phoenix V2) | 18.5 | OVER | .70-1.00 | 383 | 77.5% | 86.1% | -8.5 pts ⚑ |
| saves | refit | 18.5 | OVER | .70-1.00 | 284 | 82.7% | 78.7% | +4.0 pts |
| saves | refit | 19.5 | OVER | .55-.60 | 44 | 61.4% | 57.7% | +3.6 pts |
| saves | refit | 19.5 | OVER | .60-.70 | 135 | 76.3% | 65.7% | +10.6 pts ⚑ |
| saves | published (Phoenix V2) | 19.5 | OVER | .70-1.00 | 362 | 74.6% | 81.5% | -6.9 pts ⚑ |
| saves | refit | 19.5 | OVER | .70-1.00 | 187 | 80.7% | 77.3% | +3.4 pts |
| saves | refit | 20.5 | OVER | .50-.55 | 47 | 53.2% | 52.5% | +0.6 pts |
| saves | refit | 20.5 | OVER | .55-.60 | 56 | 64.3% | 57.8% | +6.5 pts ⚑ |
| saves | published (Phoenix V2) | 20.5 | OVER | .60-.70 | 88 | 59.1% | 66.0% | -6.9 pts ⚑ |
| saves | refit | 20.5 | OVER | .60-.70 | 146 | 69.9% | 64.6% | +5.3 pts ⚑ |
| saves | published (Phoenix V2) | 20.5 | OVER | .70-1.00 | 284 | 70.1% | 77.7% | -7.6 pts ⚑ |
| saves | refit | 20.5 | OVER | .70-1.00 | 109 | 82.6% | 76.4% | +6.2 pts ⚑ |
| saves | refit | 21.5 | OVER | .50-.55 | 65 | 61.5% | 52.7% | +8.8 pts ⚑ |
| saves | refit | 21.5 | OVER | .55-.60 | 78 | 60.3% | 57.5% | +2.8 pts |
| saves | published (Phoenix V2) | 21.5 | OVER | .60-.70 | 165 | 60.0% | 65.0% | -5.0 pts |
| saves | refit | 21.5 | OVER | .60-.70 | 101 | 74.3% | 64.4% | +9.9 pts ⚑ |
| saves | published (Phoenix V2) | 21.5 | OVER | .70-1.00 | 150 | 72.0% | 75.5% | -3.5 pts |
| saves | refit | 21.5 | OVER | .70-1.00 | 59 | 81.4% | 75.3% | +6.1 pts ⚑ |
| saves | refit | 21.5 | UNDER | .45-.50 | 45 | 40.0% | 52.6% | -12.6 pts ⚑ |
| saves | published (Phoenix V2) | 22.5 | OVER | .50-.55 | 61 | 65.6% | 52.7% | +12.8 pts ⚑ |
| saves | refit | 22.5 | OVER | .50-.55 | 78 | 55.1% | 52.4% | +2.7 pts |
| saves | published (Phoenix V2) | 22.5 | OVER | .55-.60 | 91 | 52.7% | 57.5% | -4.8 pts |
| saves | refit | 22.5 | OVER | .55-.60 | 50 | 64.0% | 57.4% | +6.6 pts ⚑ |
| saves | published (Phoenix V2) | 22.5 | OVER | .60-.70 | 126 | 54.8% | 64.7% | -9.9 pts ⚑ |
| saves | refit | 22.5 | OVER | .60-.70 | 67 | 68.7% | 64.4% | +4.2 pts |
| saves | published (Phoenix V2) | 22.5 | OVER | .70-1.00 | 51 | 76.5% | 74.2% | +2.3 pts |
| saves | refit | 22.5 | UNDER | .30-.40 | 58 | 69.0% | 63.8% | +5.2 pts ⚑ |
| saves | refit | 22.5 | UNDER | .40-.45 | 46 | 43.5% | 57.6% | -14.1 pts ⚑ |
| saves | refit | 22.5 | UNDER | .45-.50 | 70 | 40.0% | 52.4% | -12.4 pts ⚑ |
| saves | published (Phoenix V2) | 23.5 | OVER | .50-.55 | 72 | 45.8% | 52.4% | -6.6 pts ⚑ |
| saves | refit | 23.5 | OVER | .50-.55 | 48 | 58.3% | 52.6% | +5.7 pts ⚑ |
| saves | published (Phoenix V2) | 23.5 | OVER | .55-.60 | 62 | 54.8% | 57.1% | -2.3 pts |
| saves | published (Phoenix V2) | 23.5 | OVER | .60-.70 | 65 | 69.2% | 63.9% | +5.3 pts ⚑ |
| saves | refit | 23.5 | OVER | .60-.70 | 47 | 63.8% | 63.9% | -0.0 pts |
| saves | published (Phoenix V2) | 23.5 | UNDER | .30-.40 | 41 | 63.4% | 63.8% | -0.4 pts |
| saves | refit | 23.5 | UNDER | .30-.40 | 94 | 58.5% | 64.9% | -6.4 pts ⚑ |
| saves | published (Phoenix V2) | 23.5 | UNDER | .40-.45 | 52 | 40.4% | 57.1% | -16.7 pts ⚑ |
| saves | refit | 23.5 | UNDER | .40-.45 | 75 | 48.0% | 57.5% | -9.5 pts ⚑ |
| saves | published (Phoenix V2) | 23.5 | UNDER | .45-.50 | 81 | 48.1% | 52.2% | -4.1 pts |
| saves | refit | 23.5 | UNDER | .45-.50 | 75 | 45.3% | 52.6% | -7.3 pts ⚑ |
| saves | published (Phoenix V2) | 24.5 | UNDER | .00-.30 | 136 | 61.8% | 98.3% | -36.6 pts ⚑ |
| saves | refit | 24.5 | UNDER | .00-.30 | 72 | 83.3% | 74.4% | +8.9 pts ⚑ |
| saves | refit | 24.5 | UNDER | .30-.40 | 131 | 55.7% | 64.6% | -8.9 pts ⚑ |
| saves | published (Phoenix V2) | 24.5 | UNDER | .40-.45 | 265 | 52.8% | 58.3% | -5.5 pts ⚑ |
| saves | refit | 24.5 | UNDER | .40-.45 | 77 | 50.6% | 57.8% | -7.1 pts ⚑ |
| saves | refit | 24.5 | UNDER | .45-.50 | 42 | 47.6% | 52.4% | -4.8 pts |
| saves | published (Phoenix V2) | 25.5 | UNDER | .00-.30 | 136 | 68.4% | 98.3% | -29.9 pts ⚑ |
| saves | refit | 25.5 | UNDER | .00-.30 | 129 | 79.8% | 75.9% | +3.9 pts |
| saves | published (Phoenix V2) | 25.5 | UNDER | .30-.40 | 148 | 63.5% | 64.6% | -1.1 pts |
| saves | refit | 25.5 | UNDER | .30-.40 | 154 | 59.1% | 65.3% | -6.2 pts ⚑ |
| saves | published (Phoenix V2) | 25.5 | UNDER | .40-.45 | 120 | 52.5% | 58.5% | -6.0 pts ⚑ |
| saves | refit | 25.5 | UNDER | .40-.45 | 46 | 47.8% | 57.5% | -9.7 pts ⚑ |
| saves | published (Phoenix V2) | 26.5 | UNDER | .00-.30 | 238 | 74.8% | 87.5% | -12.7 pts ⚑ |
| saves | refit | 26.5 | UNDER | .00-.30 | 210 | 78.6% | 77.1% | +1.5 pts |
| saves | published (Phoenix V2) | 26.5 | UNDER | .30-.40 | 123 | 61.8% | 65.8% | -4.1 pts |
| saves | refit | 26.5 | UNDER | .30-.40 | 121 | 62.8% | 66.0% | -3.1 pts |
| saves | published (Phoenix V2) | 26.5 | UNDER | .40-.45 | 43 | 53.5% | 58.5% | -5.0 pts ⚑ |
| saves | published (Phoenix V2) | 27.5 | UNDER | .00-.30 | 335 | 76.4% | 85.5% | -9.0 pts ⚑ |
| saves | refit | 27.5 | UNDER | .00-.30 | 285 | 78.2% | 78.8% | -0.5 pts |
| saves | published (Phoenix V2) | 27.5 | UNDER | .30-.40 | 60 | 58.3% | 66.3% | -8.0 pts ⚑ |
| saves | refit | 27.5 | UNDER | .30-.40 | 79 | 65.8% | 65.6% | +0.2 pts |
| saves | published (Phoenix V2) | 28.5 | UNDER | .00-.30 | 387 | 77.0% | 86.8% | -9.8 pts ⚑ |
| saves | refit | 28.5 | UNDER | .00-.30 | 331 | 79.5% | 81.0% | -1.5 pts |
| saves | refit | 28.5 | UNDER | .30-.40 | 58 | 63.8% | 65.5% | -1.7 pts |
| saves | published (Phoenix V2) | 29.5 | UNDER | .00-.30 | 398 | 79.9% | 89.6% | -9.7 pts ⚑ |
| saves | refit | 29.5 | UNDER | .00-.30 | 360 | 82.8% | 83.4% | -0.6 pts |
| saves | published (Phoenix V2) | 30.5 | UNDER | .00-.30 | 404 | 84.9% | 92.0% | -7.1 pts ⚑ |
| saves | refit | 30.5 | UNDER | .00-.30 | 388 | 85.6% | 85.3% | +0.3 pts |

Two-sided lines only:

| Lane | Model | Line | Side | Band | n | Win % | Predicted | Gap |
|---|---|---:|---|---|---:|---:|---:|---:|
| points | goals + assists | 0.5 | OVER | .50-.55 | 282 | 59.2% | 52.5% | +6.7 pts ⚑ |
| points | published | 0.5 | OVER | .50-.55 | 221 | 58.8% | 52.4% | +6.4 pts ⚑ |
| points | goals + assists | 0.5 | OVER | .55-.60 | 186 | 62.9% | 57.3% | +5.7 pts ⚑ |
| points | published | 0.5 | OVER | .55-.60 | 146 | 65.8% | 57.5% | +8.2 pts ⚑ |
| points | goals + assists | 0.5 | OVER | .60-.70 | 208 | 68.8% | 64.1% | +4.6 pts |
| points | published | 0.5 | OVER | .60-.70 | 165 | 69.1% | 63.7% | +5.4 pts ⚑ |
| points | goals + assists | 0.5 | OVER | .70-1.00 | 62 | 71.0% | 75.5% | -4.6 pts |
| points | published | 0.5 | OVER | .70-1.00 | 40 | 70.0% | 73.1% | -3.1 pts |
| points | goals + assists | 0.5 | UNDER | .00-.30 | 796 | 72.7% | 76.1% | -3.4 pts |
| points | published | 0.5 | UNDER | .00-.30 | 1,038 | 69.1% | 76.4% | -7.3 pts ⚑ |
| points | goals + assists | 0.5 | UNDER | .30-.40 | 852 | 63.7% | 65.2% | -1.5 pts |
| points | published | 0.5 | UNDER | .30-.40 | 794 | 60.3% | 65.2% | -4.9 pts |
| points | goals + assists | 0.5 | UNDER | .40-.45 | 304 | 57.2% | 57.6% | -0.4 pts |
| points | published | 0.5 | UNDER | .40-.45 | 282 | 55.7% | 57.6% | -1.9 pts |
| points | goals + assists | 0.5 | UNDER | .45-.50 | 246 | 48.4% | 52.5% | -4.1 pts |
| points | published | 0.5 | UNDER | .45-.50 | 250 | 50.0% | 52.5% | -2.5 pts |
| points | goals + assists | 1.5 | UNDER | .00-.30 | 2,141 | 85.6% | 88.2% | -2.6 pts |
| points | published | 1.5 | UNDER | .00-.30 | 2,184 | 85.1% | 89.5% | -4.4 pts |
| points | goals + assists | 1.5 | UNDER | .30-.40 | 82 | 68.3% | 66.9% | +1.4 pts |
| points | published | 1.5 | UNDER | .30-.40 | 63 | 65.1% | 65.9% | -0.8 pts |
| points | goals + assists | 2.5 | UNDER | .00-.30 | 667 | 90.6% | 93.2% | -2.6 pts |
| points | published | 2.5 | UNDER | .00-.30 | 667 | 90.6% | 94.8% | -4.3 pts |
| saves | published (Phoenix V2) | 24.5 | UNDER | .40-.45 | 51 | 49.0% | 58.4% | -9.3 pts ⚑ |
| saves | refit | 25.5 | UNDER | .30-.40 | 42 | 64.3% | 64.7% | -0.4 pts |

## Flagged bands: published vs updated

- points, all, goals + assists: 3 of 11 bands flagged; largest miss 5.8 pts.
- points, all, published: 3 of 11 bands flagged; largest miss 8.2 pts.
- points, two-sided, goals + assists: 2 of 11 bands flagged; largest miss 6.7 pts.
- points, two-sided, published: 4 of 11 bands flagged; largest miss 8.2 pts.
- saves, all, published (Phoenix V2): 20 of 29 bands flagged; largest miss 36.6 pts.
- saves, all, refit: 21 of 40 bands flagged; largest miss 14.1 pts.
- saves, two-sided, published (Phoenix V2): 1 of 1 bands flagged; largest miss 9.3 pts.
- saves, two-sided, refit: 0 of 1 bands flagged; largest miss 0.4 pts.

Tables: `artifacts/analysis/nhl/model_evaluation/month_end_2026-10/`. Script: `backend/nhl/scripts/evaluate_nhl_month_end_updated_models.py`.
