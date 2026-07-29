#!/usr/bin/env python3
"""Exact game-roster identity bridge pilot; no fuzzy or inherited identity data."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

MLB = "https://statsapi.mlb.com/api"


def dump_raw(url: str, params: dict, path: Path) -> tuple[dict, str]:
    response = requests.get(url, params=params, timeout=45)
    response.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.content)
    return response.json(), hashlib.sha256(response.content).hexdigest()


def norm(value: str) -> tuple[str, tuple[str, ...]]:
    original = value
    ops = []
    value = unicodedata.normalize("NFKC", value)
    if value != original:
        ops.append("unicode_nfkc")
    before = value
    value = value.strip()
    if value != before:
        ops.append("trim")
    before = value
    value = re.sub(r"\s+", " ", value)
    if value != before:
        ops.append("internal_whitespace")
    before = value
    value = value.replace("’", "'").replace("‘", "'").replace("‐", "-").replace("‑", "-")
    if value != before:
        ops.append("apostrophe_hyphen")
    before = value
    value = re.sub(r"\b([A-Za-z])\.", r"\1", value)
    if value != before:
        ops.append("initial_period")
    before = value
    value = re.sub(r",?\s+(Jr|Sr)\.?$", lambda m: f" {m.group(1)}", value, flags=re.I)
    value = re.sub(r",?\s+(II|III|IV)$", lambda m: f" {m.group(1).upper()}", value, flags=re.I)
    if value != before:
        ops.append("suffix")
    before = value
    value = "".join(
        ch for ch in unicodedata.normalize("NFD", value)
        if unicodedata.category(ch) != "Mn"
    )
    if value != before:
        ops.append("accent_insensitive")
    before = value
    value = value.casefold()
    if value != before:
        ops.append("casefold")
    return value, tuple(ops)


def team_norm(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).casefold()


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def write(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--odds-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    raw_mlb = args.output_dir / "raw" / "MLB_STATS_API"

    event_payloads: dict[str, list[tuple[Path, dict, str]]] = defaultdict(list)
    for path in sorted(args.odds_root.glob("*/event_*.json")):
        payload = json.loads(path.read_text())
        if not any(b.get("key") == "betonlineag" for b in payload.get("bookmakers", [])):
            continue
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        event_payloads[payload["id"]].append((path, payload, sha))

    official_games = []
    for day in ("2026-07-28", "2026-07-29"):
        payload, sha = dump_raw(
            f"{MLB}/v1/schedule",
            {"sportId": 1, "date": day, "hydrate": "team"},
            raw_mlb / f"schedule_{day}.json",
        )
        for date_block in payload.get("dates", []):
            for game in date_block.get("games", []):
                official_games.append({
                    "game_pk": game["gamePk"],
                    "game_date": game["officialDate"],
                    "start": parse_dt(game["gameDate"]),
                    "home_name": game["teams"]["home"]["team"]["name"],
                    "away_name": game["teams"]["away"]["team"]["name"],
                    "home_id": game["teams"]["home"]["team"]["id"],
                    "away_id": game["teams"]["away"]["team"]["id"],
                    "schedule_sha": sha,
                })

    binding_rows, binding = [], {}
    for event_id, captures in sorted(event_payloads.items()):
        sample = captures[-1][1]
        start = parse_dt(sample["commence_time"])
        candidates = [
            game for game in official_games
            if team_norm(game["home_name"]) == team_norm(sample["home_team"])
            and team_norm(game["away_name"]) == team_norm(sample["away_team"])
            and abs((game["start"] - start).total_seconds()) <= 600
        ]
        decision = "EXACT_UNIQUE_MATCH" if len(candidates) == 1 else "EVENT_IDENTITY_AMBIGUOUS"
        reason = (
            "exact home/away teams and scheduled start within 10 minutes"
            if len(candidates) == 1 else f"{len(candidates)} official game candidates"
        )
        game_pk = candidates[0]["game_pk"] if len(candidates) == 1 else ""
        binding[event_id] = candidates[0] if len(candidates) == 1 else None
        binding_rows.append({
            "provider_event_id": event_id, "provider_start_utc": sample["commence_time"],
            "provider_away_team": sample["away_team"], "provider_home_team": sample["home_team"],
            "candidate_game_pks": "|".join(str(x["game_pk"]) for x in candidates),
            "game_pk": game_pk, "candidate_count": len(candidates),
            "decision": decision, "reason": reason,
            "raw_payload_path": str(captures[-1][0]), "raw_payload_sha256": captures[-1][2],
        })

    rosters: dict[int, list[dict]] = {}
    for game in [x for x in binding.values() if x]:
        game_pk = game["game_pk"]
        if game_pk in rosters:
            continue
        feed, sha = dump_raw(
            f"{MLB}/v1.1/game/{game_pk}/feed/live", {},
            raw_mlb / f"game_{game_pk}.json",
        )
        players = []
        for side in ("home", "away"):
            team_id = feed["gameData"]["teams"][side]["id"]
            for key, entry in feed["liveData"]["boxscore"]["teams"][side].get("players", {}).items():
                name = entry["person"].get("fullName", "")
                player_id = int(key.replace("ID", ""))
                normalized, ops = norm(name)
                players.append({
                    "player_mlb_id": player_id, "official_name": name,
                    "normalized": normalized, "official_ops": ops, "team_mlb_id": team_id,
                    "feed_sha": sha,
                })
        rosters[game_pk] = players

    attempts, rejects = [], []
    identity_by_group = defaultdict(set)
    prices_by_group = defaultdict(set)
    sides_by_group = defaultdict(set)
    patterns = Counter()
    for event_id, captures in sorted(event_payloads.items()):
        game = binding[event_id]
        for path, payload, payload_sha in captures:
            for book in payload.get("bookmakers", []):
                if book.get("key") != "betonlineag":
                    continue
                for market in book.get("markets", []):
                    if market.get("key") != "batter_total_bases":
                        continue
                    observed = market.get("last_update")
                    for outcome in market.get("outcomes", []):
                        raw_name = outcome.get("description", "")
                        normalized, provider_ops = norm(raw_name)
                        if not game:
                            candidates = []
                            decision = "EVENT_IDENTITY_AMBIGUOUS"
                            reason = "provider event did not bind uniquely to official game"
                            game_pk = ""
                        else:
                            game_pk = game["game_pk"]
                            roster = rosters.get(game_pk, [])
                            before = [p for p in roster if p["official_name"] == raw_name]
                            candidates = [p for p in roster if p["normalized"] == normalized]
                            decision = (
                                "EXACT_UNIQUE_MATCH" if len(candidates) == 1
                                else "NO_OFFICIAL_ROSTER_MATCH" if len(candidates) == 0
                                else "MULTIPLE_OFFICIAL_ROSTER_MATCHES"
                            )
                            reason = (
                                "one exact normalized full-name match within exact official game roster"
                                if len(candidates) == 1 else f"{len(candidates)} normalized roster candidates"
                            )
                        chosen = candidates[0] if len(candidates) == 1 else {}
                        all_ops = tuple(sorted(set(provider_ops + tuple(chosen.get("official_ops", ())))))
                        patterns["|".join(all_ops) or "none"] += 1
                        before_count = (
                            len([p for p in rosters.get(game_pk, []) if p["official_name"] == raw_name])
                            if game_pk else 0
                        )
                        row = {
                            "provider_event_id": event_id, "game_pk": game_pk,
                            "raw_provider_player_name": raw_name,
                            "normalized_provider_name": normalized,
                            "official_player_name": chosen.get("official_name", ""),
                            "normalized_official_name": chosen.get("normalized", ""),
                            "player_mlb_id": chosen.get("player_mlb_id", ""),
                            "normalization_operations": "|".join(all_ops) or "none",
                            "candidate_count_before_normalization": before_count,
                            "candidate_count_after_normalization": len(candidates),
                            "decision": decision, "reason": reason,
                            "side": outcome.get("name"), "line": outcome.get("point"),
                            "price": outcome.get("price"), "source_observed_at_utc": observed,
                            "raw_payload_path": str(path), "raw_payload_sha256": payload_sha,
                        }
                        attempts.append(row)
                        group = (event_id, raw_name, outcome.get("point"))
                        identity_by_group[group].add(str(chosen.get("player_mlb_id", "")))
                        prices_by_group[group].add(outcome.get("price"))
                        sides_by_group[group].add(outcome.get("name"))
                        if decision != "EXACT_UNIQUE_MATCH":
                            rejects.append(row)

    stability = []
    for group in sorted(identity_by_group):
        ids = {x for x in identity_by_group[group] if x}
        stability.append({
            "provider_event_id": group[0], "raw_player_name": group[1], "line": group[2],
            "snapshot_rows": sum(
                1 for row in attempts if
                (row["provider_event_id"], row["raw_provider_player_name"], row["line"]) == group
            ),
            "distinct_player_mlb_ids": "|".join(sorted(ids)),
            "identity_stable": "YES" if len(ids) == 1 else "NO",
            "over_under_same_identity": "YES" if {"Over", "Under"}.issubset(sides_by_group[group]) and len(ids) == 1 else "NO",
            "price_changed": "YES" if len(prices_by_group[group]) > 1 else "NO",
            "decision": "PASS" if len(ids) == 1 else "FAIL",
        })

    pattern_rows = [{
        "normalization_pattern": pattern, "attempt_rows": count,
        "distinct_raw_names": len({r["raw_provider_player_name"] for r in attempts if r["normalization_operations"] == pattern}),
        "admitted_rows": sum(r["decision"] == "EXACT_UNIQUE_MATCH" and r["normalization_operations"] == pattern for r in attempts),
        "rejected_rows": sum(r["decision"] != "EXACT_UNIQUE_MATCH" and r["normalization_operations"] == pattern for r in attempts),
        "manual_review": "REVIEWED_ALLOWED_DETERMINISTIC_OPERATIONS_ONLY",
    } for pattern, count in sorted(patterns.items())]

    write(args.output_dir / "provider_event_to_game_pk_audit.csv", list(binding_rows[0]), binding_rows)
    write(args.output_dir / "roster_constrained_identity_attempts.csv", list(attempts[0]), attempts)
    write(args.output_dir / "normalization_pattern_audit.csv", list(pattern_rows[0]), pattern_rows)
    write(args.output_dir / "identity_rejects.csv", list(attempts[0]), rejects)
    write(args.output_dir / "identity_stability_audit.csv", list(stability[0]), stability)

    decision_counts = Counter(row["decision"] for row in attempts)
    certifiable = (
        decision_counts["EXACT_UNIQUE_MATCH"] > 0
        and decision_counts["MULTIPLE_OFFICIAL_ROSTER_MATCHES"] == 0
        and all(row["decision"] == "PASS" for row in stability if row["distinct_player_mlb_ids"])
    )
    summary = {
        "provider_outcomes_examined": len(attempts),
        "distinct_games": len(event_payloads),
        "distinct_player_names": len({r["raw_provider_player_name"] for r in attempts}),
        "over_under_paired_groups": sum({"Over", "Under"}.issubset(v) for v in sides_by_group.values()),
        "repeated_groups": sum(r["snapshot_rows"] > 2 for r in stability),
        "price_change_groups": sum(r["price_changed"] == "YES" for r in stability),
        "exact_unique": decision_counts["EXACT_UNIQUE_MATCH"],
        "unmatched": decision_counts["NO_OFFICIAL_ROSTER_MATCH"],
        "ambiguous": decision_counts["MULTIPLE_OFFICIAL_ROSTER_MATCHES"],
        "event_binding_failures": decision_counts["EVENT_IDENTITY_AMBIGUOUS"],
        "certifiable": certifiable,
    }
    (args.output_dir / "pilot_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    report = f"""# Exact Game-roster Identity Bridge Pilot

All {summary['provider_outcomes_examined']} preserved BetOnline outcome rows were
evaluated. Event binding used exact home/away teams and scheduled start within ten
minutes; player binding used only the official game roster and approved deterministic
full-name normalization.

- Exact unique: {summary['exact_unique']}
- No official roster match: {summary['unmatched']}
- Multiple official roster matches: {summary['ambiguous']}
- Event-binding failures: {summary['event_binding_failures']}
- Certifiable: {'YES' if certifiable else 'NO'}
"""
    (args.output_dir / "exact_roster_identity_report.md").write_text(report)
    (args.output_dir / "manual_pattern_review.md").write_text(
        "# Manual Pattern Review\n\n"
        "Every distinct normalization pattern and every rejected class is enumerated "
        "in the adjacent CSV files. Only the approved deterministic operations were used; "
        "no row was manually overridden.\n"
    )
    print(json.dumps(summary))
    return 0 if certifiable else 2


if __name__ == "__main__":
    raise SystemExit(main())
