# NHL SOG fixed blend prospective shadows

## Readiness

- Production: `d10` (`poisson_baseline / baseline_v1`)
- Prospective primary: `25/75` (`NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1`)
- Prospective comparator: `50/50` (`NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1`)
- Status: `READY_FOR_FIRST_PROSPECTIVE_CAPTURE`
- First capture: `PENDING_NEXT_NORMAL_DAILY_RUN`

The next capture should come from the next legitimate pregame run of
`python -m backend.nhl.cli daily --with-odds`. No slate, player-game, or calendar
day sample threshold is set; evaluate paired evidence as it accumulates.

After that run, verify: (A) production remains authoritative; (B) both shadow
identities appear; (C) they cover the same eligible player-game population;
(D) lines 1.5, 2.5, and 3.5 are present; (E) canonical rows have no duplicates;
(F) d10 equals production d10; (G) d20 uses strict-prior history; (H) actual
d10/d20 history counts are retained; (I) selected TOI matches production;
(J–K) both blend calculations are correct; (L) Poisson probabilities are
coherent; (M) the pregame cutoff passes; (N) production feature-input SHA is
bound; (O) shadow prediction SHA is retained; (P) deterministic replay passes;
(Q) the receipt exposes both identities; (R) shadow failure remains
nonblocking; (S) production prediction semantics are unchanged; and (T) 8rain
authority is unchanged.

Oct. 9 retained production replay evidence remains 178 scored rows and 3
unscored rows, with output SHA-256
`9c42bc1f24842550e2a576b79a37af0f7872269edf62003c3331d462d84e2f41`, matching
the retained daily-run output. No new capture is implied by this record.

Production remains `poisson_baseline / baseline_v1` using `d10_sog_per60` and the production scorer's selected TOI. The research primary `NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1` uses `0.25*d10 + 0.75*d20`; the comparator `NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1` uses `0.50*d10 + 0.50*d20`.

Both shadows are scored from the same retained pregame SOG feature input and reuse the production scorer's player-game population and selected TOI. Their immutable artifacts live under `artifacts/operational/nhl/sog_fixed_blend_shadows/`; later immutable grades and the cumulative summary live under `artifacts/operational/nhl/sog_fixed_blend_grades/` and `artifacts/analysis/nhl/sog_fixed_blend_prospective/`.

The Oct. 3–8 retrospective evidence motivated observation only. It did not promote a model. Any production authority decision requires accumulated immutable prospective evidence.
