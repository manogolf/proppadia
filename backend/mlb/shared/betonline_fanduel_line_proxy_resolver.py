"""FanDuel-to-BetOnline line-only fallback resolver for MLB player props.

The resolver is deliberately conservative:
- direct BetOnline rows always win;
- FanDuel can proxy availability/line only for certified markets;
- FanDuel prices are retained only as context fields;
- unresolved proxy rows are not executable BetOnline wager rows.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Iterable

from backend.mlb.shared.betonline_market_registry import active_market_to_prop_map


TARGET_BOOKMAKER = "betonlineag"
PROXY_BOOKMAKER = "fanduel"
CERTIFIED_LINE_PROXY_MARKETS = {
    "hits": "batter_hits",
    "strikeouts_pitching": "pitcher_strikeouts",
}
CERTIFICATION_LABEL = "FANDUEL_LINE_PROXY_CERTIFIED_EXACT_LINES_ONLY"
UNSUPPORTED_PROXY_MARKETS = {
    "total_bases",
    "hits_runs_rbis",
    "home_runs",
    "stolen_bases",
    "outs_recorded",
    "earned_runs",
    "hits_allowed",
}


BOOK_ALIASES = {
    "betonline": "betonlineag",
    "betonline.ag": "betonlineag",
    "betonline_ag": "betonlineag",
    "fanduel sportsbook": "fanduel",
}


def normalize_bookmaker(value: Any) -> str:
    key = str(value or "").strip().lower()
    return BOOK_ALIASES.get(key, key)


def normalize_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Za-z0-9 ]+", " ", text).lower()
    return re.sub(r"\s+", " ", text).strip()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run_tag_from_path(path: Path) -> str:
    stem = path.stem
    m = re.search(r"(local_[A-Za-z0-9_]+|20\d{6}T\d{6}|oddsapi_[A-Za-z0-9_]+_\d{8}T\d{6}Z)", stem)
    return m.group(1) if m else stem


def slate_date_from_path(path: Path) -> str:
    for part in reversed(path.parts):
        if re.fullmatch(r"20\d\d-\d\d-\d\d", part):
            return part
    return ""


def payload_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("events"), list):
            return [x for x in payload["events"] if isinstance(x, dict)]
        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            return [data]
        if payload.get("bookmakers") is not None:
            return [payload]
    return []


def captured_at(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("captured_at_utc", "capture_timestamp_utc", "timestamp"):
            if payload.get(key):
                return str(payload[key])
    return ""


def fnum(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def american_to_prob(price: float | None) -> float | None:
    if price is None or price == 0:
        return None
    if price > 0:
        return 100.0 / (price + 100.0)
    return abs(price) / (abs(price) + 100.0)


@dataclass(frozen=True)
class MarketObservation:
    slate_date: str
    event_id: str
    home_team: str
    away_team: str
    commence_time: str
    prop_type: str
    raw_market_key: str
    bookmaker: str
    side: str
    line: float
    price: float | None
    player_name: str
    normalized_player_name: str
    player_id: str
    team: str
    opponent: str
    source_capture_timestamp: str
    source_run_tag: str
    source_raw_path: str
    source_raw_sha256: str

    def identity(self) -> tuple[Any, ...]:
        player_identity = self.player_id or self.normalized_player_name
        return (
            self.slate_date,
            self.event_id,
            player_identity,
            self.prop_type,
            float(self.line),
            self.side,
        )


def read_oddsapi_observations(path: Path, *, repo_root: Path | None = None) -> list[MarketObservation]:
    """Parse governed BetOnline/FanDuel player-prop observations from a retained payload."""
    market_to_prop = active_market_to_prop_map()
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except Exception:
        return []
    sha = hashlib.sha256(raw).hexdigest()
    try:
        raw_path = str(path.relative_to(repo_root)) if repo_root else str(path)
    except Exception:
        raw_path = str(path)
    out: list[MarketObservation] = []
    capture_ts = captured_at(payload)
    sdate = slate_date_from_path(path)
    run_tag = run_tag_from_path(path)
    for ev in payload_items(payload):
        event_id = str(ev.get("id") or "")
        home = str(ev.get("home_team") or "")
        away = str(ev.get("away_team") or "")
        commence = str(ev.get("commence_time") or "")
        for book in ev.get("bookmakers", []) or []:
            if not isinstance(book, dict):
                continue
            bookmaker = normalize_bookmaker(book.get("key") or book.get("title"))
            if bookmaker not in {TARGET_BOOKMAKER, PROXY_BOOKMAKER}:
                continue
            for market in book.get("markets", []) or []:
                if not isinstance(market, dict):
                    continue
                market_key = str(market.get("key") or "")
                prop = market_to_prop.get(market_key)
                if not prop:
                    continue
                for outcome in market.get("outcomes", []) or []:
                    if not isinstance(outcome, dict):
                        continue
                    side = str(outcome.get("name") or "").strip().lower()
                    if side not in {"over", "under"}:
                        continue
                    player_name = str(outcome.get("description") or "").strip()
                    line = fnum(outcome.get("point"))
                    if not player_name or line is None:
                        continue
                    out.append(
                        MarketObservation(
                            slate_date=sdate,
                            event_id=event_id,
                            home_team=home,
                            away_team=away,
                            commence_time=commence,
                            prop_type=prop,
                            raw_market_key=market_key,
                            bookmaker=bookmaker,
                            side=side,
                            line=float(line),
                            price=fnum(outcome.get("price")),
                            player_name=player_name,
                            normalized_player_name=normalize_name(player_name),
                            player_id=str(outcome.get("player_id") or outcome.get("id") or "").strip(),
                            team="",
                            opponent="",
                            source_capture_timestamp=capture_ts,
                            source_run_tag=run_tag,
                            source_raw_path=raw_path,
                            source_raw_sha256=sha,
                        )
                    )
    return out


def resolve_line_only_fallback(observations: Iterable[MarketObservation]) -> list[dict[str, Any]]:
    """Resolve direct and certified proxy rows at exact proposition-line-side grain."""
    obs = list(observations)
    direct_by_identity = {o.identity(): o for o in obs if o.bookmaker == TARGET_BOOKMAKER}
    rows: list[dict[str, Any]] = []

    for identity, o in sorted(direct_by_identity.items()):
        rows.append(
            {
                **asdict(o),
                "target_bookmaker": TARGET_BOOKMAKER,
                "market_source_bookmaker": TARGET_BOOKMAKER,
                "line_source_bookmaker": TARGET_BOOKMAKER,
                "price_source_bookmaker": TARGET_BOOKMAKER,
                "line_proxy_status": "DIRECT_BETONLINE",
                "line_proxy_certification": "DIRECT_OBSERVATION",
                "line_proxy_reason": "Direct BetOnline row present; proxy not used.",
                "direct_betonline_market_available": True,
                "direct_betonline_price_available": o.price is not None,
                "betonline_price_status": "DIRECT_BETONLINE_PRICE_AVAILABLE" if o.price is not None else "DIRECT_BETONLINE_PRICE_MISSING",
                "betonline_american_odds": o.price,
                "betonline_decimal_odds": _decimal_odds(o.price),
                "betonline_implied_probability": american_to_prob(o.price),
                "betonline_no_vig_probability": None,
                "betonline_ev": None,
                "betonline_units_projection": None,
                "source_fanduel_odds": None,
                "source_fanduel_implied_probability": None,
                "execution_status": "EXECUTABLE_DIRECT_BETONLINE_PRICE" if o.price is not None else "NOT_EXECUTABLE_MISSING_DIRECT_BETONLINE_PRICE",
                "identity_status": "EXACT_EVENT_PLAYER_PROP_LINE_SIDE",
            }
        )

    for o in sorted((x for x in obs if x.bookmaker == PROXY_BOOKMAKER), key=lambda x: x.identity()):
        if o.prop_type not in CERTIFIED_LINE_PROXY_MARKETS:
            continue
        if o.identity() in direct_by_identity:
            continue
        rows.append(
            {
                **asdict(o),
                "target_bookmaker": TARGET_BOOKMAKER,
                "market_source_bookmaker": PROXY_BOOKMAKER,
                "line_source_bookmaker": PROXY_BOOKMAKER,
                "price_source_bookmaker": None,
                "line_proxy_status": "CERTIFIED_EXACT_LINE_PROXY",
                "line_proxy_certification": CERTIFICATION_LABEL,
                "line_proxy_reason": "Direct BetOnline row absent; FanDuel line-only proxy certified for this market.",
                "direct_betonline_market_available": False,
                "direct_betonline_price_available": False,
                "betonline_price_status": "MISSING_DIRECT_BETONLINE_PRICE",
                "betonline_american_odds": None,
                "betonline_decimal_odds": None,
                "betonline_implied_probability": None,
                "betonline_no_vig_probability": None,
                "betonline_ev": None,
                "betonline_units_projection": None,
                "source_fanduel_odds": o.price,
                "source_fanduel_implied_probability": american_to_prob(o.price),
                "execution_status": "NOT_EXECUTABLE_MISSING_DIRECT_BETONLINE_PRICE",
                "identity_status": "EXACT_EVENT_PLAYER_PROP_LINE_SIDE",
            }
        )
    return rows


def bind_later_betonline_prices(proxy_rows: Iterable[dict[str, Any]], direct_observations: Iterable[MarketObservation]) -> list[dict[str, Any]]:
    """Bind later actual BetOnline prices to existing proxy rows without replacing provenance."""
    direct = {o.identity(): o for o in direct_observations if o.bookmaker == TARGET_BOOKMAKER and o.price is not None}
    out: list[dict[str, Any]] = []
    for row in proxy_rows:
        cp = dict(row)
        key = (
            cp.get("slate_date", ""),
            cp.get("event_id", ""),
            cp.get("player_id") or cp.get("normalized_player_name", ""),
            cp.get("prop_type", ""),
            float(cp.get("line")),
            cp.get("side", ""),
        )
        found = direct.get(key)
        if found and cp.get("line_proxy_status") == "CERTIFIED_EXACT_LINE_PROXY":
            cp["price_source_bookmaker"] = TARGET_BOOKMAKER
            cp["direct_betonline_price_available"] = True
            cp["betonline_price_status"] = "LATER_BOUND_DIRECT_BETONLINE_PRICE"
            cp["betonline_american_odds"] = found.price
            cp["betonline_decimal_odds"] = _decimal_odds(found.price)
            cp["betonline_implied_probability"] = american_to_prob(found.price)
            cp["execution_status"] = "EXECUTABLE_LATER_BOUND_DIRECT_BETONLINE_PRICE"
            cp["later_betonline_capture_timestamp"] = found.source_capture_timestamp
            cp["later_betonline_source_raw_path"] = found.source_raw_path
            cp["later_betonline_source_raw_sha256"] = found.source_raw_sha256
        out.append(cp)
    return out


def _decimal_odds(price: float | None) -> float | None:
    if price is None:
        return None
    if price > 0:
        return 1.0 + price / 100.0
    return 1.0 + 100.0 / abs(price)


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
