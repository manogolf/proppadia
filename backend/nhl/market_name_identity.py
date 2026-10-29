"""Which slate player a sportsbook name belongs to.

Books identify players by name only. The SOG and Points attachments match a
quote by full name or by first-initial + last name ("e pettersson"). A key is
usable only by the one slate player who owns it. When two slate players share a
key (two Elias Petterssons; Anders and Andre Lee on "a lee"), the key prices
nobody unless a reviewed binding in ``config/market_name_bindings.json`` gives
the full name to one player. Shared keys are reported, never guessed.
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Iterable

BINDINGS_PATH = Path(__file__).resolve().parent / "config/market_name_bindings.json"


def normalize(name) -> str:
    """Lowercase ASCII words; punctuation and accents dropped."""
    if name is None or (isinstance(name, float) and name != name):
        return ""
    text = unicodedata.normalize("NFKD", str(name))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())


def initial_last(normalized: str) -> str:
    parts = normalized.split()
    return f"{parts[0][0]} {parts[-1]}" if len(parts) >= 2 else ""


def load_bindings(path: Path = BINDINGS_PATH) -> dict[str, int]:
    """Reviewed bindings: normalized full name -> player id."""
    data = json.loads(path.read_text())
    return {normalize(b["market_name"]): int(b["player_id"]) for b in data["bindings"]}


class SlateNames:
    """Name-key ownership for one slate's players."""

    def __init__(self, players: Iterable[tuple[object, object]], bindings: dict[str, int] | None = None):
        bindings = load_bindings() if bindings is None else bindings
        owners: dict[str, set[int]] = defaultdict(set)
        self.player_ids: set[int] = set()
        for player_id, name in players:
            try:
                pid = int(player_id)
            except (TypeError, ValueError):
                continue
            full = normalize(name)
            if not full:
                continue
            self.player_ids.add(pid)
            owners[full].add(pid)
            short = initial_last(full)
            if short:
                owners[short].add(pid)
        self.owner: dict[str, int] = {}
        self.shared: dict[str, list[int]] = {}
        self.bound: dict[str, int] = {}
        for key, ids in owners.items():
            if len(ids) == 1:
                self.owner[key] = next(iter(ids))
                continue
            bound = bindings.get(key)
            if bound in ids:
                self.owner[key] = bound
                self.bound[key] = bound
            else:
                self.shared[key] = sorted(ids)

    def keys_for(self, player_id, name) -> list[str]:
        """Name keys this player may be priced by (full name first)."""
        try:
            pid = int(player_id)
        except (TypeError, ValueError):
            return []
        full = normalize(name)
        return [k for k in (full, initial_last(full)) if k and self.owner.get(k) == pid]

    def player_for(self, name) -> int | None:
        """Owner of a book's player name, or None when unknown or shared."""
        full = normalize(name)
        if full in self.owner:
            return self.owner[full]
        if full in self.shared:
            return None
        return self.owner.get(initial_last(full))

    def report(self) -> dict:
        return {"shared_keys": dict(sorted(self.shared.items())),
                "reviewed_bindings_used": dict(sorted(self.bound.items()))}
