# Manual Pattern Review

Every normalization pattern and rejected class was reviewed. No row was manually
overridden.

## Patterns

- `casefold`: 306 outcome rows, 51 distinct provider names, all admitted uniquely.
- `accent_insensitive|casefold`: 24 outcome rows, four distinct names, all admitted:
  - Angel Martinez → Angel Martínez (`682657`)
  - Eugenio Suarez → Eugenio Suárez (`553993`)
  - Javier Baez → Javier Báez (`595879`)
  - Jose Ramirez → José Ramírez (`608070`)

These are expressly approved deterministic transformations. No nickname, partial
name, initials-only, edit-distance, phonetic, remembered, or manual substitution was
used.

## Rejected and ambiguous classes

- No official roster match: 0
- Multiple official roster matches: 0
- Event identity ambiguous: 0
- Official roster coverage missing: 0
- Normalization not allowed: 0

No doubleheader occurred in the three-game pilot population. Exact home/away teams
plus scheduled time are required; zero or multiple official candidates fail closed.
