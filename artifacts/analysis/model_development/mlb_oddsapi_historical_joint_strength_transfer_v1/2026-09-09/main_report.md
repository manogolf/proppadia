# MLB Odds API historical joint-strength transfer audit v1

## Result

The frozen cohort remains 76 games and 56-20. The bounded acquisition made 29 one-snapshot historical `h2h` requests across 29 exact cohort dates and consumed 290 credits, within the 400-credit ceiling.

Final classification: **`ALTERNATIVE_BOOK_TRANSFER_SUPPORTED`**.

All 9 alternative bookmakers reached the predeclared 38-game robustness floor and had positive complete-coverage hypothetical ROI. The return therefore transfers economically at these independently captured prices under the predeclared rule. Clustered intervals still include zero, so uncertainty remains substantial. User access and historical fillability are unknown; none of these returns is executable evidence.

The zero-cost quota check recorded last/used/remaining = 0/9559/90441; after acquisition the values were 10/9849/90151. Provider snapshots were 7 to 298 seconds before requested timestamps, with zero returned after the request.

## Frozen bookmaker results

| Book | Valid | Record | ROI | 95% clustered interval | Average American |
|---|---:|---:|---:|---:|---:|
| Pinnacle | 74 | 54-20 | +9.5% | [-3.5%, +22.1%] | -203.9 |
| BetOnline | 73 | 54-19 | +10.7% | [-2.8%, +22.8%] | -205.2 |
| DraftKings | 76 | 56-20 | +10.4% | [-2.0%, +21.8%] | -207.5 |
| FanDuel | 76 | 56-20 | +10.6% | [-1.7%, +22.1%] | -205.8 |
| BetMGM | 76 | 56-20 | +9.0% | [-3.2%, +20.2%] | -214.7 |
| BetRivers | 76 | 56-20 | +9.3% | [-2.9%, +20.7%] | -213.4 |
| Fanatics | 76 | 56-20 | +9.1% | [-3.1%, +20.4%] | -213.6 |
| Bovada | 76 | 56-20 | +8.9% | [-3.3%, +20.2%] | -215.2 |
| MyBookie.ag | 76 | 56-20 | +10.0% | [-2.4%, +21.3%] | -208.7 |
| William Hill US | 76 | 56-20 | +10.7% | [-1.7%, +22.2%] | -206.1 |

## Identical common games versus Pinnacle

| Alternative | Common | Record | Alternative ROI | Pinnacle ROI | Difference |
|---|---:|---:|---:|---:|---:|
| betonlineag | 72 | 53-19 | +9.9% | +10.2% | -0.3% |
| draftkings | 74 | 54-20 | +8.8% | +9.5% | -0.7% |
| fanduel | 74 | 54-20 | +9.1% | +9.5% | -0.5% |
| betmgm | 74 | 54-20 | +7.5% | +9.5% | -2.0% |
| betrivers | 74 | 54-20 | +7.7% | +9.5% | -1.8% |
| fanatics | 74 | 54-20 | +7.6% | +9.5% | -1.9% |
| bovada | 74 | 54-20 | +7.4% | +9.5% | -2.1% |
| mybookieag | 74 | 54-20 | +8.4% | +9.5% | -1.1% |
| williamhill_us | 74 | 54-20 | +9.1% | +9.5% | -0.4% |

The bookmaker list and timestamps were frozen before acquisition without consulting outcomes. Fliff was not requested because preserved Odds API historical evidence did not show it as available. No best-price composite or retrospective book selection was used.

Each raw response, secret-free request parameters, returned snapshot timestamp, bookmaker and market update time, quota headers, and response hash is preserved. Admission is fail-closed into the six requested availability/timing/identity classes.

No SportsGameOdds request, model/cohort/threshold/outcome change, production/scheduler/publication change, or wager occurred.
