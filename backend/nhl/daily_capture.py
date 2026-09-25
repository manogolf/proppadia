"""Governed, append-only evidence for the comprehensive NHL daily runner.

This module deliberately contains no scheduler integration.  Network access is
limited to :class:`RequestsOddsProvider`, which is called only after a phase
claim has been created by :func:`capture_odds_observation`.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
import base64
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter


PACIFIC = ZoneInfo("America/Los_Angeles")
UTC = timezone.utc
ODDS_CONTRACT = "NHL_ODDS_RESEARCH_OBSERVATION_V1"
ROSTER_CONTRACT = "NHL_ROSTER_RESEARCH_OBSERVATION_V1"
PLANNER_CONTRACT = "NHL_FIRST_PUCK_PHASE_PLANNER_V1"
ODDS_REQUEST_PLANNER_CONTRACT = "NHL_CANONICAL_EVENT_ODDS_REQUEST_PLAN_V2"
# The NHL has 32 clubs, so a normal full-league slate can contain no more than
# 16 games.  This is an absolute corruption guard, not a target request count;
# the per-capture limit is always the smaller canonical/selected count.
NHL_ABSOLUTE_GAME_REQUEST_CEILING = 16
ODDS_BINDING_TOLERANCE_MINUTES = 15
DEFAULT_ODDS_CREDIT_RULES = {
    "events_discovery": 0,
    "event_odds": "MARKETS_X_REGIONS",
}
ODDS_CLASSIFICATIONS = {
    "CAPTURED_NONEMPTY",
    "CAPTURED_VALID_EMPTY",
    "CAPTURED_UNMATCHED",
    "FAILED_PROVIDER",
    "FAILED_MALFORMED_RESPONSE",
    "FAILED_BUDGET_GUARD",
    "SKIPPED_NO_AUTHORIZATION",
    "SKIPPED_BUDGET_GUARD",
}
PHASES = ("EARLY", "REFRESH", "FINAL_PREGAME")
SENSITIVE_KEY = re.compile(
    r"(?:^|_)(?:api_?key|authorization|password|secret|token|signature|credential)(?:$|_)",
    re.IGNORECASE,
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def iso_pt(value: datetime) -> str:
    return value.astimezone(PACIFIC).isoformat()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_game_set_hash(game_ids: Iterable[int]) -> str:
    payload = json.dumps(sorted({int(value) for value in game_ids}), separators=(",", ":"))
    return sha256_bytes(payload.encode())


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()


def _jsonl_bytes(rows: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join(
        (json.dumps(dict(row), sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
        for row in rows
    )


def _write_create_only(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise


def _atomic_replace(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_bytes(body)
    temporary.replace(path)


def _manifest(directory: Path) -> Path:
    files = sorted(
        item for item in directory.iterdir()
        if item.is_file() and item.name != "SHA256SUMS"
    )
    manifest = directory / "SHA256SUMS"
    manifest.write_text("".join(f"{sha256_file(item)}  {item.name}\n" for item in files))
    return manifest


def verify_package(directory: Path) -> str:
    manifest = directory / "SHA256SUMS"
    if not manifest.is_file():
        raise RuntimeError("OBSERVATION_MANIFEST_MISSING")
    manifest_names: set[str] = set()
    for line in manifest.read_text().splitlines():
        digest, name = line.split("  ", 1)
        if name in manifest_names:
            raise RuntimeError(f"OBSERVATION_MANIFEST_DUPLICATE_ENTRY:{name}")
        manifest_names.add(name)
        target = directory / name
        if not target.is_file() or sha256_file(target) != digest:
            raise RuntimeError(f"OBSERVATION_MANIFEST_MISMATCH:{name}")
    actual_names = {
        item.name for item in directory.iterdir()
        if item.is_file() and item.name != "SHA256SUMS"
    }
    if actual_names != manifest_names:
        raise RuntimeError("OBSERVATION_MANIFEST_FILE_SET_MISMATCH")
    return sha256_file(manifest)


def _safe_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.username is not None or parsed.password is not None:
        raise RuntimeError("REQUEST_URL_CONTAINS_CREDENTIALS")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def sanitize_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, item in value.items():
        if SENSITIVE_KEY.search(str(key)):
            continue
        if isinstance(item, Mapping):
            out[str(key)] = sanitize_mapping(item)
        elif isinstance(item, list):
            out[str(key)] = [sanitize_mapping(x) if isinstance(x, Mapping) else x for x in item]
        else:
            out[str(key)] = item
    return out


def _localized(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        return str(value.get("default") or value.get("en") or "").strip()
    return ""


def _team_aliases(team: Mapping[str, Any]) -> tuple[str, ...]:
    values = {
        str(team.get("abbrev") or "").strip(),
        str(team.get("triCode") or "").strip(),
        _localized(team.get("name")),
        _localized(team.get("commonName")),
    }
    place, common = _localized(team.get("placeName")), _localized(team.get("commonName"))
    if place and common:
        values.add(f"{place} {common}")
    return tuple(sorted(value for value in values if value))


def _norm_team(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


@dataclass(frozen=True)
class CanonicalGame:
    game_id: int
    start_time_utc: str
    home_team: str
    away_team: str
    home_aliases: tuple[str, ...] = ()
    away_aliases: tuple[str, ...] = ()
    home_team_id: int | None = None
    away_team_id: int | None = None


@dataclass
class HttpExchange:
    method: str
    sanitized_url: str
    sanitized_parameters: dict[str, Any]
    started_at_utc: str
    ended_at_utc: str
    duration_ms: int
    status: int | None
    response_bytes: int
    response_sha256: str
    headers: dict[str, str] = field(default_factory=dict)
    error_type: str | None = None
    error_message: str | None = None


@dataclass
class ProviderCapture:
    classification: str | None
    events: list[dict[str, Any]]
    odds_payload: list[dict[str, Any]]
    exchanges: list[HttpExchange]
    raw_response_bytes: bytes
    empty_reason: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    retry_count: int = 0
    fallback_count: int = 0
    credits_consumed: int | None = None
    credits_remaining: int | None = None
    transport_response_bodies: list[bytes] = field(default_factory=list)
    bindings: list[dict[str, Any]] = field(default_factory=list)
    request_plan: dict[str, Any] | None = None
    credit_accounting: list[dict[str, Any]] = field(default_factory=list)


class BudgetGuardError(RuntimeError):
    """A deterministic request or credit bound could not be proven locally."""


@dataclass(frozen=True)
class OddsRequestPlan:
    planner_contract_version: str
    canonical_game_set_hash: str
    canonical_game_count: int
    canonical_game_ids: tuple[int, ...]
    selected_canonical_game_ids: tuple[int, ...]
    selected_provider_event_ids: tuple[str, ...]
    request_pairs: tuple[tuple[int, str], ...]
    markets: tuple[str, ...]
    regions: tuple[str, ...]
    maximum_discovery_requests: int
    maximum_paid_requests: int
    maximum_total_attempts: int
    credits_per_paid_request: int
    discovery_credit_cost: int
    maximum_credits: int
    absolute_paid_request_ceiling: int
    missing_canonical_game_ids: tuple[int, ...]

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["canonical_game_ids"] = list(self.canonical_game_ids)
        value["selected_canonical_game_ids"] = list(self.selected_canonical_game_ids)
        value["selected_provider_event_ids"] = list(self.selected_provider_event_ids)
        value["request_pairs"] = [
            {"canonical_game_id": game_id, "provider_event_id": event_id}
            for game_id, event_id in self.request_pairs
        ]
        value["markets"] = list(self.markets)
        value["regions"] = list(self.regions)
        value["missing_canonical_game_ids"] = list(self.missing_canonical_game_ids)
        return value


@dataclass(frozen=True)
class OddsObservationResult:
    classification: str
    observation_dir: Path
    summary: dict[str, Any]
    manifest_sha256: str
    replayed: bool = False


class ObservationAlreadyClaimed(RuntimeError):
    pass


class RequestsOddsProvider:
    """The legacy Odds API acquisition, without retries or secret-bearing logs."""

    BASE = "https://api.the-odds-api.com/v4/sports/icehockey_nhl"

    def __init__(self, api_key: str, *, session: requests.Session | None = None,
                 timeout: tuple[float, float] = (5.0, 30.0)) -> None:
        self.api_key = str(api_key).strip()
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "proppadia-nhl-daily-odds-research/1.0"})
        self.session.mount("https://", HTTPAdapter(max_retries=0))
        self.session.mount("http://", HTTPAdapter(max_retries=0))
        self.timeout = timeout

    def _get_json(self, url: str, params: Mapping[str, Any]) -> tuple[Any, HttpExchange, bytes]:
        started = utc_now()
        started_clock = time.monotonic()
        safe_params = sanitize_mapping(params)
        try:
            response = self.session.get(
                url, params=dict(params), timeout=self.timeout, allow_redirects=False)
            body = bytes(response.content)
            ended = utc_now()
            exchange = HttpExchange(
                method="GET", sanitized_url=_safe_url(url), sanitized_parameters=safe_params,
                started_at_utc=iso_utc(started), ended_at_utc=iso_utc(ended),
                duration_ms=max(0, round((time.monotonic() - started_clock) * 1000)),
                status=int(response.status_code), response_bytes=len(body),
                response_sha256=sha256_bytes(body),
                headers={key.lower(): value for key, value in response.headers.items()
                         if key.lower() in {"content-type", "x-requests-used", "x-requests-remaining", "x-requests-last"}},
            )
            if not response.ok:
                exchange.error_type = "HTTP_ERROR"
                exchange.error_message = f"HTTP {response.status_code}"
                return None, exchange, body
            try:
                return response.json(), exchange, body
            except Exception as error:
                exchange.error_type = type(error).__name__
                exchange.error_message = "response JSON validation failed"
                return None, exchange, body
        except Exception as error:
            ended = utc_now()
            exchange = HttpExchange(
                method="GET", sanitized_url=_safe_url(url), sanitized_parameters=safe_params,
                started_at_utc=iso_utc(started), ended_at_utc=iso_utc(ended),
                duration_ms=max(0, round((time.monotonic() - started_clock) * 1000)),
                status=None, response_bytes=0, response_sha256=sha256_bytes(b""),
                error_type=type(error).__name__, error_message="provider transport failed",
            )
            return None, exchange, b""

    def discover_events(self, *, days_from: int) -> ProviderCapture:
        """Make the one allowed free discovery attempt and nothing else."""
        event_params = {"dateFormat": "iso", "daysFrom": int(days_from), "apiKey": self.api_key}
        events, event_exchange, event_body = self._get_json(f"{self.BASE}/events", event_params)
        exchanges = [event_exchange]
        if event_exchange.status is None or not (200 <= event_exchange.status < 300):
            return ProviderCapture(
                "FAILED_PROVIDER", [], [], exchanges, event_body,
                error_type=event_exchange.error_type, error_message=event_exchange.error_message,
                transport_response_bodies=[event_body],
            )
        if events is None or not isinstance(events, list) or any(not isinstance(x, dict) for x in events):
            return ProviderCapture(
                "FAILED_MALFORMED_RESPONSE", [], [], exchanges, event_body,
                error_type=event_exchange.error_type or "INVALID_EVENTS_PAYLOAD",
                error_message=event_exchange.error_message or "events response must be a JSON list of objects",
                transport_response_bodies=[event_body],
            )
        if not events:
            return ProviderCapture(
                None, [], [], exchanges, event_body, empty_reason="NO_EVENTS",
                transport_response_bodies=[event_body],
            )
        return ProviderCapture(
            None, events, [], exchanges, b"[]\n",
            transport_response_bodies=[event_body],
        )

    @staticmethod
    def _quota_header(exchange: HttpExchange, name: str) -> tuple[str, int | None]:
        raw = exchange.headers.get(name)
        if raw is None:
            return "MISSING", None
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            return "MALFORMED", None
        return ("VALID", value) if value >= 0 else ("MALFORMED", None)

    def capture_plan(
        self, *, plan: OddsRequestPlan, discovery: ProviderCapture, odds_format: str,
    ) -> ProviderCapture:
        """Execute only the immutable paid requests enumerated by ``plan``."""
        plan_dict = plan.as_dict()
        pairs = tuple(plan.request_pairs)
        if (
            len(pairs) != plan.maximum_paid_requests
            or len({game_id for game_id, _ in pairs}) != len(pairs)
            or len({event_id for _, event_id in pairs}) != len(pairs)
            or tuple(game_id for game_id, _ in pairs) != plan.selected_canonical_game_ids
            or tuple(event_id for _, event_id in pairs) != plan.selected_provider_event_ids
            or plan.maximum_paid_requests > min(
                plan.canonical_game_count, plan.absolute_paid_request_ceiling)
            or plan.maximum_total_attempts != plan.maximum_discovery_requests + len(pairs)
            or plan.maximum_credits != (
                plan.discovery_credit_cost + len(pairs) * plan.credits_per_paid_request)
        ):
            raise BudgetGuardError("ODDS_REQUEST_PLAN_CONTRACT_INVALID")

        exchanges = list(discovery.exchanges)
        bodies = list(discovery.transport_response_bodies)
        odds_payload: list[dict[str, Any]] = []
        accounting: list[dict[str, Any]] = []
        consumed = 0
        remaining: int | None = None

        if len(exchanges) != 1 or len(bodies) != 1:
            raise BudgetGuardError("ODDS_DISCOVERY_ATTEMPT_CARDINALITY_INVALID")
        discovery_status, discovery_actual = self._quota_header(exchanges[0], "x-requests-last")
        if discovery_status == "MALFORMED" or (
            discovery_actual is not None and discovery_actual > plan.discovery_credit_cost
        ):
            return ProviderCapture(
                "FAILED_BUDGET_GUARD", discovery.events, [], exchanges, b"[]\n",
                error_type="DISCOVERY_CREDIT_ACCOUNTING_CONTRADICTION",
                error_message="discovery credit header exceeds or contradicts the immutable plan",
                credits_consumed=discovery_actual, transport_response_bodies=bodies,
                request_plan=plan_dict,
                credit_accounting=[{
                    "request_kind": "DISCOVERY", "header_status": discovery_status,
                    "planned_credits": plan.discovery_credit_cost,
                    "actual_credits": discovery_actual,
                }],
            )
        consumed += discovery_actual if discovery_actual is not None else plan.discovery_credit_cost
        accounting.append({
            "request_kind": "DISCOVERY", "header_status": discovery_status,
            "planned_credits": plan.discovery_credit_cost,
            "actual_credits": discovery_actual,
            "accounted_credits": discovery_actual if discovery_actual is not None else plan.discovery_credit_cost,
        })

        planned_ids = set(plan.selected_provider_event_ids)
        requested_ids: set[str] = set()
        for request_index, (game_id, event_id) in enumerate(pairs, start=1):
            # These checks immediately precede transport.  A mutated, duplicate,
            # or out-of-plan request cannot reach the network.
            if event_id not in planned_ids or event_id in requested_ids:
                raise BudgetGuardError("ODDS_REQUEST_OUTSIDE_IMMUTABLE_PLAN")
            if request_index > plan.maximum_paid_requests:
                raise BudgetGuardError("ODDS_PAID_REQUEST_COUNT_CEILING_REACHED")
            planned_cumulative = (
                plan.discovery_credit_cost + request_index * plan.credits_per_paid_request)
            if planned_cumulative > plan.maximum_credits:
                raise BudgetGuardError("ODDS_PLANNED_CREDIT_CEILING_REACHED")
            if not re.fullmatch(r"[A-Za-z0-9_-]+", event_id):
                raise BudgetGuardError("ODDS_PROVIDER_EVENT_ID_UNSAFE")

            params = {
                "regions": ",".join(plan.regions), "markets": ",".join(plan.markets),
                "oddsFormat": odds_format, "apiKey": self.api_key,
            }
            payload, exchange, body = self._get_json(
                f"{self.BASE}/events/{event_id}/odds", params)
            exchanges.append(exchange)
            bodies.append(body)
            requested_ids.add(event_id)

            header_status, actual = self._quota_header(exchange, "x-requests-last")
            accounted = actual if actual is not None else plan.credits_per_paid_request
            consumed += accounted
            remaining_status, parsed_remaining = self._quota_header(exchange, "x-requests-remaining")
            if remaining_status == "VALID":
                remaining = parsed_remaining
            accounting.append({
                "request_kind": "EVENT_ODDS", "request_index": request_index,
                "canonical_game_id": game_id, "provider_event_id": event_id,
                "header_status": header_status,
                "planned_credits": plan.credits_per_paid_request,
                "actual_credits": actual, "accounted_credits": accounted,
                "remaining_header_status": remaining_status,
                "credits_remaining": parsed_remaining,
            })
            contradiction = (
                header_status == "MALFORMED"
                or (actual is not None and actual > plan.credits_per_paid_request)
                or consumed > plan.maximum_credits
            )
            if contradiction:
                return ProviderCapture(
                    "FAILED_BUDGET_GUARD", discovery.events, odds_payload, exchanges,
                    _json_bytes(odds_payload),
                    error_type="PAID_CREDIT_ACCOUNTING_CONTRADICTION",
                    error_message="paid credit header exceeds or contradicts the immutable plan",
                    credits_consumed=consumed, credits_remaining=remaining,
                    transport_response_bodies=bodies, request_plan=plan_dict,
                    credit_accounting=accounting,
                )
            if exchange.status is None or not (200 <= exchange.status < 300):
                return ProviderCapture(
                    "FAILED_PROVIDER", discovery.events, odds_payload, exchanges,
                    _json_bytes(odds_payload), error_type=exchange.error_type,
                    error_message=exchange.error_message, credits_consumed=consumed,
                    credits_remaining=remaining, transport_response_bodies=bodies,
                    request_plan=plan_dict, credit_accounting=accounting,
                )
            if not isinstance(payload, dict):
                return ProviderCapture(
                    "FAILED_MALFORMED_RESPONSE", discovery.events, odds_payload, exchanges,
                    _json_bytes(odds_payload),
                    error_type=exchange.error_type or "INVALID_ODDS_PAYLOAD",
                    error_message=exchange.error_message or "event odds response must be a JSON object",
                    credits_consumed=consumed, credits_remaining=remaining,
                    transport_response_bodies=bodies, request_plan=plan_dict,
                    credit_accounting=accounting,
                )
            response_event_id = str(payload.get("id") or event_id).strip()
            if response_event_id != event_id:
                return ProviderCapture(
                    "FAILED_MALFORMED_RESPONSE", discovery.events, odds_payload, exchanges,
                    _json_bytes(odds_payload), error_type="ODDS_EVENT_ID_MISMATCH",
                    error_message="event odds response identity differs from the immutable plan",
                    credits_consumed=consumed, credits_remaining=remaining,
                    transport_response_bodies=bodies, request_plan=plan_dict,
                    credit_accounting=accounting,
                )
            odds_payload.append(payload)

        raw_response = (
            discovery.raw_response_bytes
            if not discovery.events and not pairs else _json_bytes(odds_payload)
        )
        return ProviderCapture(
            None, discovery.events, odds_payload, exchanges, raw_response,
            credits_consumed=consumed, credits_remaining=remaining,
            transport_response_bodies=bodies, request_plan=plan_dict,
            credit_accounting=accounting,
        )


def load_canonical_slate(*, slate_date: str, raw_schedule_path: Path,
                         slate_health_path: Path) -> list[CanonicalGame]:
    health = json.loads(slate_health_path.read_text())
    raw_bytes = raw_schedule_path.read_bytes()
    if health.get("slate_date") != slate_date or not health.get("downstream_ready"):
        raise RuntimeError("CANONICAL_SLATE_HEALTH_INVALID")
    if health.get("raw_source_hash") != sha256_bytes(raw_bytes):
        # The collector hashes canonical JSON with a newline.  Accept that exact
        # canonical representation while still detecting any content change.
        raw_value = json.loads(raw_bytes)
        canonical = _json_bytes_compact(raw_value)
        if health.get("raw_source_hash") != sha256_bytes(canonical):
            raise RuntimeError("CANONICAL_SLATE_RAW_HASH_MISMATCH")
    payload = json.loads(raw_bytes)
    games: list[Mapping[str, Any]] = []
    if isinstance(payload, Mapping) and isinstance(payload.get("gameWeek"), list):
        for day in payload["gameWeek"]:
            if isinstance(day, Mapping):
                games.extend(x for x in day.get("games", []) if isinstance(x, Mapping))
    elif isinstance(payload, Mapping) and isinstance(payload.get("games"), list):
        games = [x for x in payload["games"] if isinstance(x, Mapping)]
    out: list[CanonicalGame] = []
    for game in games:
        start_raw = game.get("startTimeUTC") or game.get("gameDate")
        if not start_raw:
            continue
        start = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
        if start.astimezone(PACIFIC).date().isoformat() != slate_date:
            continue
        game_id = game.get("id") or game.get("gamePk") or game.get("gameId")
        home = game.get("homeTeam") or {}
        away = game.get("awayTeam") or {}
        out.append(CanonicalGame(
            game_id=int(game_id), start_time_utc=iso_utc(start),
            home_team=str(home.get("abbrev") or home.get("triCode") or ""),
            away_team=str(away.get("abbrev") or away.get("triCode") or ""),
            home_aliases=_team_aliases(home), away_aliases=_team_aliases(away),
            home_team_id=int(home["id"]) if home.get("id") is not None else None,
            away_team_id=int(away["id"]) if away.get("id") is not None else None,
        ))
    out.sort(key=lambda game: (game.start_time_utc, game.game_id))
    if len(out) != int(health.get("normalized_game_count") or 0):
        raise RuntimeError("CANONICAL_SLATE_GAME_COUNT_MISMATCH")
    if len({game.game_id for game in out}) != len(out):
        raise RuntimeError("CANONICAL_SLATE_DUPLICATE_GAME_ID")
    return out


def _json_bytes_compact(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _market_rows(payload: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in payload:
        event_id = str(event.get("id") or "")
        for bookmaker in event.get("bookmakers", []) if isinstance(event.get("bookmakers"), list) else []:
            if not isinstance(bookmaker, Mapping):
                continue
            for market in bookmaker.get("markets", []) if isinstance(bookmaker.get("markets"), list) else []:
                if not isinstance(market, Mapping):
                    continue
                for outcome in market.get("outcomes", []) if isinstance(market.get("outcomes"), list) else []:
                    if not isinstance(outcome, Mapping):
                        continue
                    rows.append({
                        "provider_event_id": event_id,
                        "home_team": event.get("home_team") or event.get("homeTeam"),
                        "away_team": event.get("away_team") or event.get("awayTeam"),
                        "commence_time": event.get("commence_time"),
                        "bookmaker": bookmaker.get("key"),
                        "bookmaker_title": bookmaker.get("title"),
                        "market": market.get("key"),
                        "market_last_update": market.get("last_update"),
                        "outcome": outcome.get("name"),
                        "description": outcome.get("description") or outcome.get("player"),
                        "price": outcome.get("price"),
                        "point": outcome.get("point"),
                    })
    return rows


def _bind_events(
    events: Sequence[Mapping[str, Any]], games: Sequence[CanonicalGame], *, slate_date: str,
) -> list[dict[str, Any]]:
    """Return a complete inventory and select only unique one-to-one identities."""
    bindings: list[dict[str, Any]] = []
    event_id_counts: dict[str, int] = {}
    for event in events:
        event_id = str(event.get("id") or "").strip()
        if event_id:
            event_id_counts[event_id] = event_id_counts.get(event_id, 0) + 1

    for source_index, event in enumerate(events):
        event_id = str(event.get("id") or "").strip()
        home = _norm_team(str(event.get("home_team") or event.get("homeTeam") or ""))
        away = _norm_team(str(event.get("away_team") or event.get("awayTeam") or ""))
        commence_raw = event.get("commence_time")
        try:
            commence = datetime.fromisoformat(str(commence_raw).replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            commence = None
        matches = []
        for game in games:
            home_aliases = {_norm_team(x) for x in (game.home_aliases or (game.home_team,))}
            away_aliases = {_norm_team(x) for x in (game.away_aliases or (game.away_team,))}
            scheduled = datetime.fromisoformat(game.start_time_utc.replace("Z", "+00:00")).astimezone(UTC)
            time_matches = commence is not None and abs((commence - scheduled).total_seconds()) <= ODDS_BINDING_TOLERANCE_MINUTES * 60
            if home in home_aliases and away in away_aliases and time_matches:
                matches.append(game.game_id)
        if not event_id:
            status = "UNMATCHED_MISSING_PROVIDER_EVENT_ID"
        elif event_id_counts[event_id] > 1:
            status = "DUPLICATE_PROVIDER_EVENT_ID"
        elif commence is None:
            status = "UNMATCHED_INVALID_START_TIME"
        elif commence.astimezone(PACIFIC).date().isoformat() != slate_date:
            status = "UNMATCHED_OUTSIDE_PACIFIC_SLATE"
            matches = []
        elif len(matches) > 1:
            status = "AMBIGUOUS_MULTIPLE_CANONICAL_GAMES"
        elif not matches:
            status = "UNMATCHED"
        else:
            status = "CANDIDATE"
        bindings.append({
            "source_event_index": source_index,
            "provider_event_id": event_id,
            "provider_home_team": event.get("home_team") or event.get("homeTeam"),
            "provider_away_team": event.get("away_team") or event.get("awayTeam"),
            "provider_commence_time": commence_raw,
            "canonical_game_ids": matches,
            "binding_status": status,
        })

    contenders: dict[int, list[int]] = {}
    for index, row in enumerate(bindings):
        if row["binding_status"] == "CANDIDATE":
            contenders.setdefault(int(row["canonical_game_ids"][0]), []).append(index)
    for game_id, indexes in contenders.items():
        if len(indexes) == 1:
            bindings[indexes[0]]["binding_status"] = "MATCHED"
            bindings[indexes[0]]["canonical_game_id"] = game_id
        else:
            for index in indexes:
                bindings[index]["binding_status"] = "AMBIGUOUS_CANONICAL_COMPETITION"
    return bindings


def _request_values(raw: str, *, label: str) -> tuple[str, ...]:
    values = tuple(value.strip() for value in str(raw).split(",") if value.strip())
    if not values or len(values) != len(set(values)):
        raise BudgetGuardError(f"ODDS_{label}_CONTRACT_INVALID")
    return values


def derive_odds_credit_bound(
    *, paid_request_count: int, markets: Sequence[str], regions: Sequence[str],
    credit_rules: Mapping[str, Any] | None = None,
) -> tuple[int, int, int]:
    """Return discovery, per-event, and total deterministic credit ceilings."""
    rules = dict(DEFAULT_ODDS_CREDIT_RULES if credit_rules is None else credit_rules)
    discovery = rules.get("events_discovery")
    event_rule = rules.get("event_odds")
    if not isinstance(discovery, int) or discovery < 0:
        raise BudgetGuardError("ODDS_DISCOVERY_CREDIT_FORMULA_UNKNOWN")
    if event_rule != "MARKETS_X_REGIONS":
        raise BudgetGuardError("ODDS_EVENT_CREDIT_FORMULA_UNKNOWN")
    per_paid = len(tuple(markets)) * len(tuple(regions))
    if per_paid <= 0 or paid_request_count < 0:
        raise BudgetGuardError("ODDS_CREDIT_INPUT_INVALID")
    return discovery, per_paid, discovery + int(paid_request_count) * per_paid


def preflight_odds_request_budget(
    *, canonical_games: Sequence[CanonicalGame], markets: str, regions: str,
    credit_rules: Mapping[str, Any] | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Prove the worst-case bound before even the discovery request is allowed."""
    game_ids = [int(game.game_id) for game in canonical_games]
    if len(game_ids) != len(set(game_ids)):
        raise BudgetGuardError("ODDS_CANONICAL_SLATE_DUPLICATE_GAME_ID")
    if len(game_ids) > NHL_ABSOLUTE_GAME_REQUEST_CEILING:
        raise BudgetGuardError("ODDS_CANONICAL_SLATE_EXCEEDS_ABSOLUTE_SAFETY_CEILING")
    market_values = _request_values(markets, label="MARKETS")
    region_values = _request_values(regions, label="REGIONS")
    derive_odds_credit_bound(
        paid_request_count=len(game_ids), markets=market_values,
        regions=region_values, credit_rules=credit_rules)
    return market_values, region_values


def build_odds_request_plan(
    *, events: Sequence[Mapping[str, Any]], canonical_games: Sequence[CanonicalGame],
    slate_date: str, markets: str, regions: str,
    credit_rules: Mapping[str, Any] | None = None,
) -> tuple[OddsRequestPlan, list[dict[str, Any]]]:
    market_values, region_values = preflight_odds_request_budget(
        canonical_games=canonical_games, markets=markets, regions=regions,
        credit_rules=credit_rules)
    game_ids = tuple(sorted(int(game.game_id) for game in canonical_games))
    bindings = _bind_events(events, canonical_games, slate_date=slate_date)
    selected = sorted(
        (
            (int(row["canonical_game_id"]), str(row["provider_event_id"]))
            for row in bindings if row["binding_status"] == "MATCHED"
        ),
        key=lambda value: (value[0], value[1]),
    )
    selected_games = tuple(game_id for game_id, _ in selected)
    selected_events = tuple(event_id for _, event_id in selected)
    if len(selected) > min(len(game_ids), NHL_ABSOLUTE_GAME_REQUEST_CEILING):
        raise BudgetGuardError("ODDS_SELECTED_EVENTS_EXCEED_REQUEST_CEILING")
    if len(selected_games) != len(set(selected_games)) or len(selected_events) != len(set(selected_events)):
        raise BudgetGuardError("ODDS_REQUEST_PLAN_NOT_ONE_TO_ONE")
    discovery_cost, per_paid, maximum_credits = derive_odds_credit_bound(
        paid_request_count=len(selected), markets=market_values,
        regions=region_values, credit_rules=credit_rules)
    missing = tuple(sorted(set(game_ids) - set(selected_games)))
    plan = OddsRequestPlan(
        planner_contract_version=ODDS_REQUEST_PLANNER_CONTRACT,
        canonical_game_set_hash=canonical_game_set_hash(game_ids),
        canonical_game_count=len(game_ids), canonical_game_ids=game_ids,
        selected_canonical_game_ids=selected_games,
        selected_provider_event_ids=selected_events, request_pairs=tuple(selected),
        markets=market_values, regions=region_values,
        maximum_discovery_requests=1, maximum_paid_requests=len(selected),
        maximum_total_attempts=1 + len(selected),
        credits_per_paid_request=per_paid, discovery_credit_cost=discovery_cost,
        maximum_credits=maximum_credits,
        absolute_paid_request_ceiling=NHL_ABSOLUTE_GAME_REQUEST_CEILING,
        missing_canonical_game_ids=missing,
    )
    return plan, bindings


def _existing_observation(day_root: Path, phase: str) -> OddsObservationResult | None:
    if not day_root.is_dir():
        return None
    for directory in sorted(day_root.glob("observation=*")):
        summary_path = directory / "observation_summary.json"
        complete = (directory / "RUN_COMPLETE.json").is_file()
        attempted = (directory / "ATTEMPT_COMPLETE.json").is_file()
        if not summary_path.is_file() or not (complete or attempted):
            continue
        summary = json.loads(summary_path.read_text())
        if summary.get("phase") == phase:
            return OddsObservationResult(
                classification=str(summary["classification"]), observation_dir=directory,
                summary=summary, manifest_sha256=verify_package(directory), replayed=True,
            )
    return None


def capture_odds_observation(
    *, root: Path, season: int, slate_date: str, phase: str,
    parent_daily_run_id: str, canonical_games: Sequence[CanonicalGame],
    provider: RequestsOddsProvider | Callable[..., ProviderCapture] | None,
    authorized: bool, compatibility_dir: Path | None = None,
    latest_pointer_path: Path | None = None,
    invocation_id: str | None = None, now: datetime | None = None,
    days_from: int = 1,
    markets: str = "player_shots_on_goal,player_shots_on_goal_alternate,player_total_saves,player_points",
    regions: str = "us,us2", odds_format: str = "american",
    credit_rules: Mapping[str, Any] | None = None,
) -> OddsObservationResult:
    phase = str(phase).upper()
    if phase not in PHASES:
        raise ValueError(f"unsupported odds phase: {phase}")
    observed = (now or utc_now()).astimezone(UTC)
    day_root = Path(root) / f"season={int(season)}" / f"slate_date={slate_date}"
    existing = _existing_observation(day_root, phase)
    if existing is not None:
        return existing

    game_ids = [game.game_id for game in canonical_games]
    game_hash = canonical_game_set_hash(game_ids)
    first_puck = min((game.start_time_utc for game in canonical_games), default=None)
    invocation_id = invocation_id or f"nhldailyodds_{observed.strftime('%Y%m%dT%H%M%S%fZ')}_{uuid.uuid4().hex[:8]}"
    stable_id = sha256_bytes(
        f"{season}|{slate_date}|{phase}|{parent_daily_run_id}|{invocation_id}|{game_hash}".encode()
    )[:16]
    observation_name = f"observation={observed.strftime('%Y%m%dT%H%M%S.%fZ')}_{stable_id}"
    final = day_root / observation_name
    staging = day_root / f".{observation_name}.incomplete"
    claim_root = Path(root) / ".claims" / f"season={int(season)}" / f"slate_date={slate_date}"
    claim = claim_root / f"phase={phase}.claim.json"
    lease = claim_root / f"phase={phase}.lease.json"
    # The lease is acquired before discovery, while the immutable acquisition
    # claim is created only after discovery has fixed the exact paid plan.
    if claim.exists():
        existing = _existing_observation(day_root, phase)
        if existing is not None:
            return existing
        raise ObservationAlreadyClaimed(f"ODDS_PHASE_ALREADY_CLAIMED:{slate_date}:{phase}")
    lease_payload = {
        "schema_version": ODDS_REQUEST_PLANNER_CONTRACT, "season": int(season),
        "slate_date": slate_date, "phase": phase, "invocation_id": invocation_id,
        "lease_timestamp_utc": iso_utc(observed),
        "authorization": "AT_MOST_ONE_DISCOVERY_THEN_IMMUTABLE_PLAN",
        "stale_or_failed_claim_policy": "FAIL_CLOSED_NO_AUTOMATIC_RETRY",
    }
    try:
        _write_create_only(lease, _json_bytes(lease_payload))
    except FileExistsError as error:
        existing = _existing_observation(day_root, phase)
        if existing is not None:
            return existing
        raise ObservationAlreadyClaimed(f"ODDS_PHASE_ALREADY_CLAIMED:{slate_date}:{phase}") from error

    claim_base = {
        "schema_version": ODDS_CONTRACT, "planner_contract_version": ODDS_REQUEST_PLANNER_CONTRACT,
        "season": int(season), "slate_date": slate_date,
        "phase": phase, "invocation_id": invocation_id,
        "parent_daily_run_id": parent_daily_run_id,
        "observation_name": observation_name, "claim_timestamp_utc": iso_utc(observed),
        "canonical_game_set_hash": game_hash, "canonical_game_count": len(game_ids),
        "canonical_game_ids": sorted(game_ids),
        "stale_or_failed_claim_policy": "FAIL_CLOSED_NO_AUTOMATIC_RETRY",
    }

    staging.mkdir(parents=True, exist_ok=False)
    request_started = utc_now()
    plan: OddsRequestPlan | None = None
    bindings: list[dict[str, Any]] = []
    claim_payload: dict[str, Any]
    if not authorized or provider is None:
        try:
            plan, bindings = build_odds_request_plan(
                events=[], canonical_games=canonical_games, slate_date=slate_date,
                markets=markets, regions=regions, credit_rules=credit_rules)
            plan_payload = plan.as_dict()
            # No transport is authorized, so the executable ceilings are zero.
            plan_payload.update({
                "maximum_discovery_requests": 0, "maximum_paid_requests": 0,
                "maximum_total_attempts": 0, "maximum_credits": 0,
                "selected_canonical_game_ids": [], "selected_provider_event_ids": [],
                "request_pairs": [],
            })
        except BudgetGuardError as error:
            plan_payload = {
                "planner_contract_version": ODDS_REQUEST_PLANNER_CONTRACT,
                "canonical_game_set_hash": game_hash, "canonical_game_count": len(game_ids),
                "canonical_game_ids": sorted(game_ids), "selected_canonical_game_ids": [],
                "selected_provider_event_ids": [], "markets": [x.strip() for x in markets.split(",") if x.strip()],
                "regions": [x.strip() for x in regions.split(",") if x.strip()],
                "maximum_discovery_requests": 0, "maximum_paid_requests": 0,
                "maximum_total_attempts": 0, "maximum_credits": 0,
                "budget_guard_error": str(error),
            }
        claim_payload = {**claim_base, **plan_payload}
        _write_create_only(claim, _json_bytes(claim_payload))
        captured = ProviderCapture(
            "SKIPPED_NO_AUTHORIZATION", [], [], [], b"null\n",
            empty_reason="ODDS_NOT_EXPLICITLY_AUTHORIZED_OR_CREDENTIALS_UNAVAILABLE",
            request_plan=plan_payload,
        )
    else:
        try:
            preflight_odds_request_budget(
                canonical_games=canonical_games, markets=markets, regions=regions,
                credit_rules=credit_rules)
            if not hasattr(provider, "discover_events") or not hasattr(provider, "capture_plan"):
                raise BudgetGuardError("ODDS_PROVIDER_LACKS_GOVERNED_PLAN_INTERFACE")
            discovery = provider.discover_events(days_from=days_from)  # type: ignore[union-attr]
            plan, bindings = build_odds_request_plan(
                events=discovery.events, canonical_games=canonical_games,
                slate_date=slate_date, markets=markets, regions=regions,
                credit_rules=credit_rules)
            claim_payload = {**claim_base, **plan.as_dict()}
            _write_create_only(claim, _json_bytes(claim_payload))
            if discovery.classification is not None:
                captured = discovery
                captured.request_plan = plan.as_dict()
            else:
                captured = provider.capture_plan(  # type: ignore[union-attr]
                    plan=plan, discovery=discovery, odds_format=odds_format)
                if captured.classification is None and discovery.events and not plan.request_pairs:
                    captured.classification = "CAPTURED_UNMATCHED"
                    captured.empty_reason = "NO_UNIQUE_CANONICAL_EVENT_MATCHES"
            captured.bindings = bindings
        except BudgetGuardError as error:
            # Preflight failures make zero provider requests.  Failures after
            # discovery preserve that single attempt and prohibit paid calls.
            discovery_capture = locals().get("discovery")
            attempted = discovery_capture if isinstance(discovery_capture, ProviderCapture) else None
            plan_payload = plan.as_dict() if plan is not None else {
                "planner_contract_version": ODDS_REQUEST_PLANNER_CONTRACT,
                "canonical_game_set_hash": game_hash, "canonical_game_count": len(game_ids),
                "canonical_game_ids": sorted(game_ids), "selected_canonical_game_ids": [],
                "selected_provider_event_ids": [], "request_pairs": [],
                "markets": [x.strip() for x in markets.split(",") if x.strip()],
                "regions": [x.strip() for x in regions.split(",") if x.strip()],
                "maximum_discovery_requests": 0 if attempted is None else 1,
                "maximum_paid_requests": 0,
                "maximum_total_attempts": 0 if attempted is None else 1,
                "maximum_credits": None,
                "authorized_credit_ceiling": 0,
                "absolute_paid_request_ceiling": NHL_ABSOLUTE_GAME_REQUEST_CEILING,
                "budget_guard_error": str(error),
            }
            claim_payload = {**claim_base, **plan_payload}
            if not claim.exists():
                _write_create_only(claim, _json_bytes(claim_payload))
            captured = ProviderCapture(
                "SKIPPED_BUDGET_GUARD" if attempted is None else "FAILED_BUDGET_GUARD",
                list(attempted.events) if attempted else [], [],
                list(attempted.exchanges) if attempted else [], b"[]\n",
                error_type=type(error).__name__, error_message=str(error),
                transport_response_bodies=(list(attempted.transport_response_bodies) if attempted else []),
                bindings=bindings, request_plan=plan_payload,
            )
        except Exception as error:
            # Unexpected provider failures remain nonblocking, but the exact
            # zero-paid plan/claim is still durable before finalization.
            discovery_capture = locals().get("discovery")
            attempted = discovery_capture if isinstance(discovery_capture, ProviderCapture) else None
            plan_payload = plan.as_dict() if plan is not None else {
                "planner_contract_version": ODDS_REQUEST_PLANNER_CONTRACT,
                "canonical_game_set_hash": game_hash, "canonical_game_count": len(game_ids),
                "canonical_game_ids": sorted(game_ids), "selected_canonical_game_ids": [],
                "selected_provider_event_ids": [], "request_pairs": [],
                "markets": [x.strip() for x in markets.split(",") if x.strip()],
                "regions": [x.strip() for x in regions.split(",") if x.strip()],
                "maximum_discovery_requests": 0 if attempted is None else 1,
                "maximum_paid_requests": 0, "maximum_total_attempts": 0 if attempted is None else 1,
                "maximum_credits": 0,
            }
            if not claim.exists():
                _write_create_only(claim, _json_bytes({**claim_base, **plan_payload}))
            captured = ProviderCapture(
                "FAILED_PROVIDER", list(attempted.events) if attempted else [], [],
                list(attempted.exchanges) if attempted else [], b"null\n",
                error_type=type(error).__name__, error_message="provider acquisition failed",
                transport_response_bodies=(list(attempted.transport_response_bodies) if attempted else []),
                request_plan=plan_payload,
            )
    request_ended = utc_now()

    rows = _market_rows(captured.odds_payload)
    bindings = captured.bindings or bindings or _bind_events(
        captured.events, canonical_games, slate_date=slate_date)
    matched = sum(row["binding_status"] == "MATCHED" for row in bindings)
    unmatched = sum(str(row["binding_status"]).startswith("UNMATCHED") for row in bindings)
    ambiguous = sum(str(row["binding_status"]).startswith("AMBIGUOUS") for row in bindings)
    raw_market_count = sum(
        len(book.get("markets", []))
        for event in captured.odds_payload
        for book in (event.get("bookmakers", []) if isinstance(event.get("bookmakers"), list) else [])
        if isinstance(book, Mapping)
    )
    classification = captured.classification
    empty_reason = captured.empty_reason
    request_plan = dict(captured.request_plan or {})
    maximum_attempts = int(request_plan.get("maximum_total_attempts") or 0)
    if len(captured.exchanges) > maximum_attempts:
        classification = "FAILED_BUDGET_GUARD"
        captured.error_type = "REQUEST_COUNT_ACCOUNTING_CONTRADICTION"
        captured.error_message = "actual provider attempts exceed the immutable request plan"
    if classification is None:
        if not rows:
            classification = "CAPTURED_VALID_EMPTY"
            empty_reason = empty_reason or (
                "NO_EVENTS" if not captured.events else
                ("NO_REQUESTED_MARKETS" if raw_market_count == 0 else "NO_USABLE_ODDS")
            )
        elif matched == 0:
            classification = "CAPTURED_UNMATCHED"
        else:
            classification = "CAPTURED_NONEMPTY"
    if classification not in ODDS_CLASSIFICATIONS:
        raise RuntimeError(f"ODDS_CLASSIFICATION_INVALID:{classification}")

    exchange_rows = [asdict(exchange) for exchange in captured.exchanges]
    request_metadata = {
        "schema_version": ODDS_CONTRACT, "season": int(season), "slate_date": slate_date,
        "phase": phase, "invocation_id": invocation_id, "parent_daily_run_id": parent_daily_run_id,
        "endpoint_family": "THE_ODDS_API_NHL_PLAYER_PROPS",
        "sanitized_request_parameters": {
            "days_from": int(days_from), "markets": markets,
            "regions": regions, "odds_format": odds_format,
        },
        "request_started_at_utc": iso_utc(request_started),
        "request_ended_at_utc": iso_utc(request_ended),
        "duration_ms": max(0, round((request_ended - request_started).total_seconds() * 1000)),
        "canonical_game_ids": sorted(game_ids), "canonical_game_set_hash": game_hash,
        "first_puck_utc": first_puck,
        "request_plan": request_plan,
    }
    provider_asof_values = sorted({
        str(market.get("last_update"))
        for event in captured.odds_payload
        for book in (event.get("bookmakers", []) if isinstance(event.get("bookmakers"), list) else [])
        if isinstance(book, Mapping)
        for market in (book.get("markets", []) if isinstance(book.get("markets"), list) else [])
        if isinstance(market, Mapping) and market.get("last_update")
    })
    response_envelope = {
        "schema_version": ODDS_CONTRACT, "transport_attempt_count": len(exchange_rows),
        "retry_count": int(captured.retry_count), "fallback_count": int(captured.fallback_count),
        "http_statuses": [row["status"] for row in exchange_rows], "exchanges": exchange_rows,
        "provider_timestamp_or_asof": provider_asof_values,
        "credits_consumed": captured.credits_consumed,
        "credits_remaining": captured.credits_remaining,
        "credit_accounting": captured.credit_accounting,
        "request_plan": request_plan,
        "error_type": captured.error_type, "error_message": captured.error_message,
    }
    raw_body = captured.raw_response_bytes or b"null\n"
    if classification not in {"FAILED_PROVIDER", "FAILED_MALFORMED_RESPONSE", "FAILED_BUDGET_GUARD"}:
        try:
            json.loads(raw_body)
        except Exception as error:
            raise RuntimeError("RAW_ODDS_RESPONSE_NOT_JSON") from error
    first_puck_time = (
        datetime.fromisoformat(first_puck.replace("Z", "+00:00")).astimezone(UTC)
        if first_puck else None
    )
    transport_bodies = captured.transport_response_bodies or (
        [captured.raw_response_bytes] if captured.exchanges else []
    )
    if len(transport_bodies) != len(captured.exchanges):
        raise RuntimeError("TRANSPORT_RESPONSE_BODY_CARDINALITY_MISMATCH")
    transport_body_rows = [{
        "transport_attempt": index,
        "response_byte_length": len(body),
        "response_sha256": sha256_bytes(body),
        "body_base64": base64.b64encode(body).decode("ascii"),
    } for index, body in enumerate(transport_bodies, start=1)]
    for exchange, body_row in zip(captured.exchanges, transport_body_rows):
        if (exchange.response_bytes != body_row["response_byte_length"]
                or exchange.response_sha256 != body_row["response_sha256"]):
            raise RuntimeError("TRANSPORT_RESPONSE_BODY_IDENTITY_MISMATCH")

    summary = {
        "schema_version": ODDS_CONTRACT, "season": int(season), "slate_date": slate_date,
        "observation_timestamp_utc": iso_utc(observed), "observation_timestamp_pt": iso_pt(observed),
        "phase": phase, "invocation_id": invocation_id, "parent_daily_run_id": parent_daily_run_id,
        "classification": classification, "valid_empty_reason": empty_reason,
        "canonical_game_ids": sorted(game_ids), "canonical_game_count": len(game_ids),
        "canonical_game_set_hash": game_hash, "first_puck_utc": first_puck,
        "strictly_prestart": bool(first_puck_time and observed < first_puck_time),
        "raw_event_count": len(captured.events),
        "raw_market_count": raw_market_count,
        "canonical_matched_event_count": matched,
        "unmatched_event_count": unmatched, "ambiguous_event_count": ambiguous,
        "duplicate_provider_event_count": sum(
            row["binding_status"] == "DUPLICATE_PROVIDER_EVENT_ID" for row in bindings),
        "selected_canonical_game_ids": request_plan.get("selected_canonical_game_ids", []),
        "selected_provider_event_ids": request_plan.get("selected_provider_event_ids", []),
        "missing_canonical_game_ids": request_plan.get("missing_canonical_game_ids", sorted(game_ids)),
        "normalized_price_row_count": len(rows),
        "book_count": len({row.get("bookmaker") for row in rows if row.get("bookmaker")}),
        "response_byte_length": len(raw_body), "response_sha256": sha256_bytes(raw_body),
        "logical_request_count": len(captured.exchanges),
        "network_attempt_count": len(captured.exchanges),
        "discovery_request_count": min(1, len(captured.exchanges)),
        "paid_event_odds_request_count": max(0, len(captured.exchanges) - 1),
        "maximum_discovery_requests": request_plan.get("maximum_discovery_requests", 0),
        "maximum_paid_requests": request_plan.get("maximum_paid_requests", 0),
        "maximum_total_attempts": request_plan.get("maximum_total_attempts", 0),
        "maximum_credits": request_plan.get("maximum_credits", 0),
        "network_success_count": sum(
            exchange.status is not None and 200 <= exchange.status < 300
            for exchange in captured.exchanges
        ),
        "network_failure_count": sum(
            exchange.status is None or not (200 <= exchange.status < 300)
            for exchange in captured.exchanges
        ),
        "retry_count": int(captured.retry_count), "fallback_count": int(captured.fallback_count),
        "credits_consumed": captured.credits_consumed, "credits_remaining": captured.credits_remaining,
        "production_writes": 0, "publication_count": 0, "wager_count": 0,
        "model_promotion_count": 0, "claim_sha256": sha256_file(claim),
    }
    artifacts = {
        "request_metadata.json": _json_bytes(request_metadata),
        "response_envelope.json": _json_bytes(response_envelope),
        "request_plan.json": _json_bytes(request_plan),
        "events_response.json": _json_bytes(captured.events),
        "raw_response.json": raw_body,
        "transport_response_bodies.jsonl": _jsonl_bytes(transport_body_rows),
        "normalized_odds.jsonl": _jsonl_bytes(rows),
        "game_binding.jsonl": _jsonl_bytes(bindings),
        "credit_accounting.jsonl": _jsonl_bytes(captured.credit_accounting),
        "observation_summary.json": _json_bytes(summary),
    }
    for name, body in artifacts.items():
        _write_create_only(staging / name, body)
    marker_name = "RUN_COMPLETE.json" if classification.startswith("CAPTURED_") else "ATTEMPT_COMPLETE.json"
    _write_create_only(staging / marker_name, _json_bytes({
        "schema_version": ODDS_CONTRACT, "classification": classification,
        "invocation_id": invocation_id, "completed_at_utc": iso_utc(utc_now()),
    }))
    manifest = _manifest(staging)
    verify_package(staging)
    day_root.mkdir(parents=True, exist_ok=True)
    staging.replace(final)
    manifest_sha = sha256_file(final / manifest.name)

    if compatibility_dir is not None and classification.startswith("CAPTURED_"):
        compatibility_dir = Path(compatibility_dir)
        _atomic_replace(compatibility_dir / "odds_nhl_playerprops_today.json", raw_body)
        _atomic_replace(compatibility_dir / "events_today.json", _json_bytes(captured.events))
        if classification == "CAPTURED_NONEMPTY":
            _atomic_replace(compatibility_dir / "odds_latest.json", raw_body)
        pointer_path = (Path(latest_pointer_path) if latest_pointer_path is not None else
                        compatibility_dir / "odds_observation_latest.json")
        _atomic_replace(pointer_path, _json_bytes({
            "schema_version": ODDS_CONTRACT,
            "semantics": "LATEST_ATTEMPT_POINTER; odds_latest.json remains latest nonempty",
            "observation_dir": str(final), "classification": classification,
            "manifest_sha256": manifest_sha,
        }))
    return OddsObservationResult(classification, final, summary, manifest_sha)


def plan_first_puck_phases(
    *, slate_date: str, first_puck_utc: str | None,
    now_utc: datetime, completed: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return a pure provisional phase plan; this function performs no I/O."""
    if first_puck_utc is None:
        return []
    completed = completed or {}
    first = datetime.fromisoformat(first_puck_utc.replace("Z", "+00:00")).astimezone(UTC)
    day = date.fromisoformat(slate_date)
    early_anchor = datetime.combine(day, wall_time(6, 30), tzinfo=PACIFIC).astimezone(UTC)
    refresh_anchor = datetime.combine(day, wall_time(13, 30), tzinfo=PACIFIC).astimezone(UTC)
    targets = {
        "EARLY": min(early_anchor, first - timedelta(hours=4, minutes=30)),
        "REFRESH": min(refresh_anchor, first - timedelta(hours=2, minutes=30)),
        "FINAL_PREGAME": first - timedelta(minutes=75),
    }
    out = []
    now = now_utc.astimezone(UTC)
    for phase in PHASES:
        target = targets[phase]
        if target >= first:
            raise RuntimeError(f"PLANNER_TARGET_NOT_PRESTART:{phase}")
        prior = completed.get(phase)
        if prior:
            prior_target = str(prior.get("target_utc") or "")
            state = "COMPLETED" if prior_target == iso_utc(target) else "SUPERSEDED"
        elif now >= first:
            state = "MISSED"
        elif now >= target:
            state = "DUE"
        else:
            state = "PLANNED"
        out.append({
            "schema_version": PLANNER_CONTRACT, "phase": phase, "state": state,
            "slate_date": slate_date, "target_utc": iso_utc(target), "target_pt": iso_pt(target),
            "first_puck_utc": iso_utc(first), "first_puck_pt": iso_pt(first),
            "prior_calendar_date": target.astimezone(PACIFIC).date().isoformat() < slate_date,
            "requires_odds_availability": False,
        })
    return out


def write_roster_observation(
    *, root: Path, season: int, slate_date: str, phase: str,
    parent_daily_run_id: str, canonical_games: Sequence[CanonicalGame],
    source_responses: Sequence[Mapping[str, Any]], now: datetime | None = None,
) -> Path:
    """Create an immutable roster/current research observation.

    ``source_responses`` contains one row per official response with ``team``,
    ``source_url``, ``observed_at_utc`` and the parsed ``payload``.  Repeated
    team responses are retained, while the normalized snapshot is deterministic.
    """
    observed = (now or utc_now()).astimezone(UTC)
    phase = str(phase).upper()
    if phase not in PHASES:
        raise ValueError(f"unsupported roster phase: {phase}")
    game_hash = canonical_game_set_hash(game.game_id for game in canonical_games)
    stable = sha256_bytes(f"{season}|{slate_date}|{phase}|{parent_daily_run_id}|{game_hash}".encode())[:16]
    name = f"observation={observed.strftime('%Y%m%dT%H%M%S.%fZ')}_{stable}"
    day_root = Path(root) / f"season={int(season)}" / f"slate_date={slate_date}"
    final, staging = day_root / name, day_root / f".{name}.incomplete"
    if final.exists() or staging.exists():
        raise RuntimeError("ROSTER_OBSERVATION_IDENTITY_EXISTS")
    staging.mkdir(parents=True, exist_ok=False)

    games_by_team: dict[str, list[int]] = {}
    expected_teams: set[str] = set()
    for game in canonical_games:
        for team in (game.home_team.upper(), game.away_team.upper()):
            expected_teams.add(team)
            games_by_team.setdefault(team, []).append(game.game_id)
    raw_rows, expanded = [], []
    excluded = 0
    team_payload_hashes: dict[str, set[str]] = {}
    for index, response in enumerate(source_responses):
        team = str(response.get("team") or "").upper()
        payload = response.get("payload")
        if not isinstance(payload, Mapping):
            raise RuntimeError("ROSTER_RESPONSE_MALFORMED")
        payload_bytes = _json_bytes_compact(payload)
        payload_hash = sha256_bytes(payload_bytes)
        team_payload_hashes.setdefault(team, set()).add(payload_hash)
        requested_url = str(
            response.get("requested_source_url") or response.get("source_url") or "")
        resolved_url = str(
            response.get("resolved_source_url") or response.get("source_url") or requested_url)
        requested_path = urlsplit(requested_url).path
        resolved_path = urlsplit(resolved_url).path
        raw_rows.append({
            "response_index": index, "team": team,
            "official_source_identity": "NHL_API_ROSTER",
            "requested_endpoint_family": (
                "ROSTER_CURRENT" if requested_path.endswith("/current") else "ROSTER_SEASON_SPECIFIC"),
            "resolved_endpoint_family": (
                "ROSTER_CURRENT" if resolved_path.endswith("/current") else "ROSTER_SEASON_SPECIFIC"),
            "sanitized_requested_url": _safe_url(requested_url),
            "sanitized_resolved_url": _safe_url(resolved_url),
            "observed_at_utc": response.get("observed_at_utc") or iso_utc(observed),
            "response_sha256": payload_hash, "response_bytes": len(payload_bytes),
            "payload": payload,
        })
        players = []
        for section, default_pos in (("forwards", "F"), ("defensemen", "D"), ("defense", "D"), ("goalies", "G")):
            values = payload.get(section) or []
            if not isinstance(values, list):
                raise RuntimeError("ROSTER_SECTION_MALFORMED")
            for player in values:
                if not isinstance(player, Mapping):
                    excluded += 1
                    continue
                player_id = player.get("id") or player.get("playerId") or (player.get("player") or {}).get("id")
                if player_id is None:
                    excluded += 1
                    continue
                players.append({
                    "player_id": int(player_id),
                    "position": str(player.get("positionCode") or player.get("position") or default_pos),
                    "first_name": _localized(player.get("firstName")) or None,
                    "last_name": _localized(player.get("lastName")) or None,
                })
        for game_id in games_by_team.get(team, []):
            for player in players:
                expanded.append({
                    "game_id": game_id, "team": team, **player,
                    "official_source_identity": "NHL_API_ROSTER",
                    "response_sha256": payload_hash,
                })

    groups: dict[tuple[int, str, int], list[dict[str, Any]]] = {}
    for row in expanded:
        key = (int(row["game_id"]), str(row["team"]), int(row["player_id"]))
        groups.setdefault(key, []).append(row)
    normalized, conflicts, duplicate_count = [], [], 0
    for key in sorted(groups):
        group = groups[key]
        duplicate_count += len(group) - 1
        semantic = {
            (row.get("position"), row.get("first_name"), row.get("last_name"))
            for row in group
        }
        if len(semantic) > 1:
            conflicts.append({"natural_key": list(key), "values": sorted(map(list, semantic), key=str)})
            continue
        row = dict(sorted(group, key=lambda value: value["response_sha256"])[0])
        row["supporting_response_hashes"] = sorted({value["response_sha256"] for value in group})
        row.pop("response_sha256", None)
        normalized.append(row)

    covered_teams = {str(row["team"]).upper() for row in raw_rows}
    missing_teams = sorted(expected_teams - covered_teams)
    unexpected_teams = sorted(covered_teams - expected_teams)
    divergent_team_snapshots = sorted(
        team for team, hashes in team_payload_hashes.items() if len(hashes) > 1
    )
    per_game_teams = {
        game.game_id: sorted({row["team"] for row in normalized if row["game_id"] == game.game_id})
        for game in canonical_games
    }
    complete = (not missing_teams and not unexpected_teams and not conflicts and not divergent_team_snapshots
                and all(len(teams) == 2 for teams in per_game_teams.values()))
    first_puck = min(
        (datetime.fromisoformat(game.start_time_utc.replace("Z", "+00:00")).astimezone(UTC)
         for game in canonical_games),
        default=None,
    )
    summary = {
        "schema_version": ROSTER_CONTRACT, "season": int(season), "slate_date": slate_date,
        "phase": phase, "parent_daily_run_id": parent_daily_run_id,
        "observation_timestamp_utc": iso_utc(observed), "observation_timestamp_pt": iso_pt(observed),
        "canonical_game_ids": sorted(game.game_id for game in canonical_games),
        "canonical_game_set_hash": game_hash, "source_family": "NHL_API_ROSTER",
        "first_puck_utc": iso_utc(first_puck) if first_puck else None,
        "strictly_prestart": bool(first_puck and observed < first_puck),
        "source_response_count": len(raw_rows), "expected_team_count": len(expected_teams),
        "covered_team_count": len(covered_teams & expected_teams), "missing_teams": missing_teams,
        "unexpected_teams": unexpected_teams,
        "snapshot_row_count": len(normalized), "duplicate_source_row_count": duplicate_count,
        "conflict_count": len(conflicts), "exclusion_count": excluded,
        "divergent_team_snapshots": divergent_team_snapshots,
        "per_game_team_coverage": per_game_teams,
        "complete_per_game_coverage": complete,
        "provenance_warning": "roster/current membership is not dressed-game participation evidence",
    }
    if conflicts or not complete:
        raise RuntimeError(f"ROSTER_OBSERVATION_INCOMPLETE_OR_CONFLICTING:{summary}")
    _write_create_only(staging / "raw_roster_responses.jsonl", _jsonl_bytes(raw_rows))
    _write_create_only(staging / "roster_snapshot.jsonl", _jsonl_bytes(normalized))
    _write_create_only(staging / "roster_conflicts.json", _json_bytes(conflicts))
    _write_create_only(staging / "observation_summary.json", _json_bytes(summary))
    _write_create_only(staging / "RUN_COMPLETE.json", _json_bytes({
        "schema_version": ROSTER_CONTRACT, "status": "COMPLETE",
        "completed_at_utc": iso_utc(utc_now()),
    }))
    _manifest(staging)
    verify_package(staging)
    day_root.mkdir(parents=True, exist_ok=True)
    staging.replace(final)
    return final
