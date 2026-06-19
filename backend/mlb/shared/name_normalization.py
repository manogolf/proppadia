"""Shared MLB player-name normalization helpers."""

from __future__ import annotations

import unicodedata
from typing import Any


def normalize_player_name_key(value: Any) -> str:
    """Return a stable ASCII-ish key for player-name joins.

    Odds feeds commonly omit accents while `mlb.player_ids` may retain them.
    Normalize accents before stripping punctuation so names like `Ureña` and
    `Urena` resolve to the same key.
    """
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    keep = [ch for ch in text if ch.isalnum() or ch.isspace()]
    return " ".join("".join(keep).split())
