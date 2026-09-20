#!/usr/bin/env python3
"""Governed general NHL Saves conditional prediction-only entry point."""
from backend.nhl.scripts.nhl_prediction_only_common import cli


if __name__ == "__main__":
    raise SystemExit(cli("SAVES"))
