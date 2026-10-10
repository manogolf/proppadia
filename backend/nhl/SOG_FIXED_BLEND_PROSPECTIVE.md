# NHL SOG fixed blend prospective shadows

Production remains `poisson_baseline / baseline_v1` using `d10_sog_per60` and the production scorer's selected TOI. The research primary `NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1` uses `0.25*d10 + 0.75*d20`; the comparator `NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1` uses `0.50*d10 + 0.50*d20`.

Both shadows are scored from the same retained pregame SOG feature input and reuse the production scorer's player-game population and selected TOI. Their immutable artifacts live under `artifacts/operational/nhl/sog_fixed_blend_shadows/`; later immutable grades and the cumulative summary live under `artifacts/operational/nhl/sog_fixed_blend_grades/` and `artifacts/analysis/nhl/sog_fixed_blend_prospective/`.

The Oct. 3–8 retrospective evidence motivated observation only. It did not promote a model. Any production authority decision requires accumulated immutable prospective evidence.
