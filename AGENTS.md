# Python environment

- Always run repository Python commands with `./bin/python`.
- Prefer `./bin/python -m pytest` when pytest is installed; for unittest-compatible tests, use `./bin/python -m unittest` if pytest is unavailable.
- Do not use ambient `python`, `python3`, or `pytest` for repository work.
- Pytest being unavailable is not a reason to change interpreters or troubleshoot NumPy/SciPy. Do not install packages without explicit authorization.
- For Python import failures, first run `./bin/check-python-env`.
