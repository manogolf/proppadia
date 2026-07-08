#!/usr/bin/env python3
"""Dry-run postgame lineup-slot backfill prepass for Hits 1.5 research.

This utility reconstructs postgame actual lineup slots from MLB StatsAPI
boxscore ``battingOrder`` fields for existing outcome-backed Hits 1.5 tier
research rows. It writes manifests only. It does not write to the database,
change production features, or treat postgame lineup slot as a pregame signal.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"
DEFAULT_O15_ROWS = (
    ROOT
    / "artifacts/analysis/mlb/starter_expected_hits_allowed/"
    "offense_factor_lineage_phase5_2026-07-05/validation_tier_backtest/"
    "hits_o15_tier_backtest_rows.csv"
)
DEFAULT_U15_ROWS = (
    ROOT
    / "artifacts/analysis/mlb/starter_expected_hits_allowed/"
    "offense_factor_lineage_phase5_2026-07-05/validation_tier_backtest/"
    "hits_u15_tier_backtest_rows.csv"
)
DEFAULT_OUT_DIR = (
    ROOT
    / "artifacts/analysis/mlb/starter_expected_hits_allowed/"
    "lineup_slot_backfill_prepass_2026-07-05"
)
SOURCE = "statsapi_boxscore_postgame"
SEMANTICS = "postgame_actual_lineup_slot"


@dataclass
class BoxscoreLookup:
    player_rows: dict[int, dict[str, Any]]
    fetch_status: str
    fetch_error: str
    source_url: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run postgame lineup-slot backfill prepass for Hits 1.5 tier rows."
    )
    parser.add_argument("--o15-rows", default=str(DEFAULT_O15_ROWS))
    parser.add_argument("--u15-rows", default=str(DEFAULT_U15_ROWS))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--statsapi-timeout-seconds", type=int, default=30)
    parser.add_argument(
        "--max-games",
        type=int,
        default=0,
        help="Optional debug limit. 0 means all games in the source spine.",
    )
    return parser.parse_args()


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        try:
            return int(float(text))
        except Exception:
            return None


def _fetch_json(url: str, timeout_seconds: int) -> tuple[dict[str, Any], str]:
    try:
        with urlopen(url, timeout=timeout_seconds) as resp:  # nosec B310 - public MLB StatsAPI.
            return json.loads(resp.read().decode("utf-8")), ""
    except HTTPError as exc:
        return {}, f"http_{exc.code}"
    except URLError as exc:
        return {}, f"url_error:{exc.reason}"
    except Exception as exc:
        return {}, f"{type(exc).__name__}:{exc}"


def _team_abbrev(team_payload: dict[str, Any]) -> str:
    team = team_payload.get("team") or {}
    return _clean(
        team.get("abbreviation")
        or team.get("abbrev")
        or team.get("teamCode")
        or team.get("fileCode")
    ).upper()


def _lineup_bucket(slot: int | None) -> str:
    if slot is None:
        return "unknown"
    if 1 <= slot <= 3:
        return "top_order"
    if 4 <= slot <= 6:
        return "middle_order"
    if 7 <= slot <= 9:
        return "bottom_order"
    return "unknown"


def _parse_batting_order(raw_value: Any) -> tuple[int | None, str]:
    raw = _clean(raw_value)
    if not raw:
        return None, ""
    parsed = _int_or_none(raw)
    if parsed is None:
        return None, f"invalid_battingOrder:{raw}"
    slot = parsed // 100
    if 1 <= slot <= 9:
        return slot, ""
    return None, f"battingOrder_outside_1_9:{raw}"


def _has_batting_stats(player_payload: dict[str, Any]) -> bool:
    batting = ((player_payload.get("stats") or {}).get("batting") or {})
    if not batting:
        return False
    for key in ("plateAppearances", "atBats", "hits", "baseOnBalls", "strikeOuts"):
        value = _int_or_none(batting.get(key))
        if value is not None and value > 0:
            return True
    return bool(batting)


def _players_from_boxscore(boxscore: dict[str, Any]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    teams = boxscore.get("teams") or {}
    for side in ("away", "home"):
        team_payload = teams.get(side) or {}
        opponent_payload = teams.get("home" if side == "away" else "away") or {}
        team = _team_abbrev(team_payload)
        opponent = _team_abbrev(opponent_payload)
        for player_payload in (team_payload.get("players") or {}).values():
            person = player_payload.get("person") or {}
            player_id = _int_or_none(person.get("id"))
            if player_id is None:
                continue
            batting_order_raw = _clean(player_payload.get("battingOrder"))
            slot, slot_error = _parse_batting_order(batting_order_raw)
            batting = ((player_payload.get("stats") or {}).get("batting") or {})
            out[player_id] = {
                "statsapi_player_id": player_id,
                "statsapi_player_name": _clean(person.get("fullName")),
                "statsapi_team": team,
                "statsapi_opponent": opponent,
                "batting_order_raw": batting_order_raw,
                "lineup_slot": slot,
                "lineup_parse_error": slot_error,
                "has_batting_stats": _has_batting_stats(player_payload),
                "actual_plate_appearances": _int_or_none(batting.get("plateAppearances")),
                "actual_at_bats": _int_or_none(batting.get("atBats")),
            }
    return out


def _fetch_boxscore_lookup(game_id: int, timeout_seconds: int) -> BoxscoreLookup:
    url = f"{STATSAPI_BASE}/game/{game_id}/boxscore"
    payload, error = _fetch_json(url, timeout_seconds)
    if error or not payload:
        return BoxscoreLookup({}, "fetch_failed", error or "empty_boxscore", url)
    return BoxscoreLookup(_players_from_boxscore(payload), "ok", "", url)


def _load_source_rows(o15_path: Path, u15_path: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path, prop_side in ((o15_path, "o15"), (u15_path, "u15")):
        df = pd.read_csv(path, low_memory=False)
        df = df.copy()
        df["prop_side"] = prop_side
        df["source_artifact"] = str(path.relative_to(ROOT) if path.is_absolute() else path)
        frames.append(df)
    rows = pd.concat(frames, ignore_index=True, sort=False)
    rows["game_date"] = pd.to_datetime(rows.get("date"), errors="coerce").dt.strftime("%Y-%m-%d")
    rows["game_id"] = pd.to_numeric(rows.get("game_id"), errors="coerce").astype("Int64")
    rows["player_id"] = pd.to_numeric(rows.get("player_id"), errors="coerce").astype("Int64")
    if "team" in rows.columns:
        team = rows["team"].map(_clean)
    else:
        team = pd.Series("", index=rows.index)
    player_team = rows.get("player_team", pd.Series("", index=rows.index)).map(_clean)
    rows["team"] = team.where(team.ne(""), player_team)
    return rows


def _base_output_row(row: pd.Series) -> dict[str, Any]:
    return {
        "game_id": "" if pd.isna(row.get("game_id")) else int(row.get("game_id")),
        "game_date": _clean(row.get("game_date")),
        "player_id": "" if pd.isna(row.get("player_id")) else int(row.get("player_id")),
        "player_name": _clean(row.get("player_name")),
        "team": _clean(row.get("team")),
        "prop_side": _clean(row.get("prop_side")),
        "source_artifact": _clean(row.get("source_artifact")),
        "lineup_slot": "",
        "lineup_bucket": "unknown",
        "batting_order_raw": "",
        "source": SOURCE,
        "lineup_slot_semantics": SEMANTICS,
        "validation_status": "",
        "reject_reason": "",
        "notes": "",
    }


def classify_rows(rows: pd.DataFrame, timeout_seconds: int, max_games: int = 0) -> pd.DataFrame:
    game_ids = [int(gid) for gid in sorted(rows["game_id"].dropna().unique().tolist())]
    if max_games and max_games > 0:
        game_ids = game_ids[:max_games]
        rows = rows[rows["game_id"].isin(game_ids)].copy()

    lookups: dict[int, BoxscoreLookup] = {}
    for game_id in game_ids:
        lookups[game_id] = _fetch_boxscore_lookup(game_id, timeout_seconds)

    output: list[dict[str, Any]] = []
    duplicate_counts = (
        rows.groupby(["game_id", "player_id", "prop_side"], dropna=False)
        .size()
        .rename("source_duplicate_count")
        .reset_index()
    )
    rows = rows.merge(duplicate_counts, on=["game_id", "player_id", "prop_side"], how="left")

    for _, src in rows.iterrows():
        out = _base_output_row(src)
        game_id = _int_or_none(src.get("game_id"))
        player_id = _int_or_none(src.get("player_id"))
        if game_id is None:
            out["validation_status"] = "rejected"
            out["reject_reason"] = "missing_game_id"
            output.append(out)
            continue
        if player_id is None:
            out["validation_status"] = "rejected"
            out["reject_reason"] = "missing_player_id"
            output.append(out)
            continue
        if int(src.get("source_duplicate_count") or 0) > 1:
            out["validation_status"] = "exception"
            out["reject_reason"] = "duplicate_source_row"
            out["notes"] = f"duplicate_count={int(src.get('source_duplicate_count'))}"
            output.append(out)
            continue

        lookup = lookups.get(game_id)
        if lookup is None:
            out["validation_status"] = "exception"
            out["reject_reason"] = "game_not_fetched"
            output.append(out)
            continue
        if lookup.fetch_status != "ok":
            out["validation_status"] = "exception"
            out["reject_reason"] = "statsapi_unavailable"
            out["notes"] = f"{lookup.fetch_error}; url={lookup.source_url}"
            output.append(out)
            continue

        player = lookup.player_rows.get(player_id)
        if not player:
            out["validation_status"] = "rejected"
            out["reject_reason"] = "player_not_in_statsapi_boxscore"
            out["notes"] = f"url={lookup.source_url}"
            output.append(out)
            continue

        out["batting_order_raw"] = player.get("batting_order_raw") or ""
        slot = player.get("lineup_slot")
        parse_error = player.get("lineup_parse_error") or ""
        if slot is not None:
            out["lineup_slot"] = int(slot)
            out["lineup_bucket"] = _lineup_bucket(int(slot))
            out["validation_status"] = "accepted"
            out["notes"] = (
                f"statsapi_player_name={player.get('statsapi_player_name')}; "
                f"statsapi_team={player.get('statsapi_team')}; "
                f"actual_pa={player.get('actual_plate_appearances')}; "
                f"actual_ab={player.get('actual_at_bats')}; "
                f"url={lookup.source_url}"
            )
            output.append(out)
            continue

        if parse_error:
            out["validation_status"] = "rejected"
            out["reject_reason"] = "battingOrder_cannot_map_to_lineup_slot"
            out["notes"] = f"{parse_error}; url={lookup.source_url}"
            output.append(out)
            continue

        if player.get("has_batting_stats"):
            out["validation_status"] = "exception"
            out["reject_reason"] = "pinch_hitter_or_substitute_ambiguity"
            out["notes"] = (
                f"player has batting stats but no battingOrder; "
                f"actual_pa={player.get('actual_plate_appearances')}; "
                f"actual_ab={player.get('actual_at_bats')}; "
                f"url={lookup.source_url}"
            )
            output.append(out)
            continue

        out["validation_status"] = "rejected"
        out["reject_reason"] = "player_not_in_statsapi_batting_order"
        out["notes"] = f"player found in boxscore without battingOrder or batting stats; url={lookup.source_url}"
        output.append(out)

    return pd.DataFrame(output)


def _summary_rows(all_rows: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    def add(scope: str, frame: pd.DataFrame) -> None:
        total = len(frame)
        accepted = int(frame["validation_status"].eq("accepted").sum()) if total else 0
        rejected = int(frame["validation_status"].eq("rejected").sum()) if total else 0
        exception = int(frame["validation_status"].eq("exception").sum()) if total else 0
        rows.append(
            {
                "scope": scope,
                "rows_checked": total,
                "accepted_rows": accepted,
                "rejected_rows": rejected,
                "exception_rows": exception,
                "coverage_pct": accepted / total if total else 0.0,
                "top_order_rows": int(frame["lineup_bucket"].eq("top_order").sum()) if total else 0,
                "middle_order_rows": int(frame["lineup_bucket"].eq("middle_order").sum()) if total else 0,
                "bottom_order_rows": int(frame["lineup_bucket"].eq("bottom_order").sum()) if total else 0,
                "unknown_rows": int(frame["lineup_bucket"].eq("unknown").sum()) if total else 0,
                "notes": "postgame_actual_lineup_slot; dry_run_only",
            }
        )

    add("all_rows", all_rows)
    for prop_side, frame in all_rows.groupby("prop_side", dropna=False):
        add(f"prop_side={prop_side}", frame)
    for reason, frame in all_rows[all_rows["validation_status"].ne("accepted")].groupby("reject_reason", dropna=False):
        add(f"reject_or_exception={reason}", frame)
    return pd.DataFrame(rows)


def _write_report(out_dir: Path, rows: pd.DataFrame, summary: pd.DataFrame, generated_at: str) -> None:
    total = len(rows)
    accepted = int(rows["validation_status"].eq("accepted").sum()) if total else 0
    rejected = int(rows["validation_status"].eq("rejected").sum()) if total else 0
    exception = int(rows["validation_status"].eq("exception").sum()) if total else 0
    coverage = accepted / total if total else 0.0
    bucket_counts = rows["lineup_bucket"].value_counts(dropna=False).to_dict() if total else {}
    reason_counts = rows.loc[rows["validation_status"].ne("accepted"), "reject_reason"].value_counts(dropna=False).to_dict()

    lines = [
        "# Postgame Lineup Slot Backfill Prepass",
        "",
        f"- Generated: `{generated_at}`",
        "- Mode: `dry_run_only`",
        "- Source: `statsapi_boxscore_postgame`",
        "- Semantics: `postgame_actual_lineup_slot`",
        "",
        "## Summary",
        "",
        f"- Rows checked: `{total}`",
        f"- Accepted rows: `{accepted}`",
        f"- Rejected rows: `{rejected}`",
        f"- Exception rows: `{exception}`",
        f"- Accepted coverage: `{coverage:.2%}`",
        "",
        "## Lineup Bucket Counts",
        "",
    ]
    if bucket_counts:
        for key, value in bucket_counts.items():
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- No rows.")
    lines.extend(["", "## Reject / Exception Reasons", ""])
    if reason_counts:
        for key, value in reason_counts.items():
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- None.")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "Accepted rows mean StatsAPI boxscore contained a player `battingOrder` that mapped clearly to lineup slot 1-9 by exact `game_id + player_id` match.",
            "",
            "This is postgame actual lineup context. It is suitable for historical baseball behavior research after validation, but it is not a pregame confirmed-lineup signal and should not be represented that way.",
            "",
            "## Outputs",
            "",
            "- `lineup_slot_candidate_rows.csv`",
            "- `lineup_slot_accepted_rows.csv`",
            "- `lineup_slot_rejected_rows.csv`",
            "- `lineup_slot_exception_rows.csv`",
            "- `lineup_slot_coverage_summary.csv`",
            "",
            "## No Behavior Changed",
            "",
            "This utility writes local analysis artifacts only. It performs no DB writes, schema changes, formula changes, tier changes, upload changes, selector/model changes, or production behavior changes.",
        ]
    )
    (out_dir / "lineup_slot_backfill_prepass_2026-07-05.md").write_text("\n".join(lines) + "\n")


def main() -> int:
    args = _parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    source_rows = _load_source_rows(Path(args.o15_rows), Path(args.u15_rows))
    classified = classify_rows(source_rows, args.statsapi_timeout_seconds, args.max_games)
    # Stable column order required by the audit, with semantics retained.
    columns = [
        "game_id",
        "game_date",
        "player_id",
        "player_name",
        "team",
        "prop_side",
        "source_artifact",
        "lineup_slot",
        "lineup_bucket",
        "batting_order_raw",
        "source",
        "lineup_slot_semantics",
        "validation_status",
        "reject_reason",
        "notes",
    ]
    classified = classified.reindex(columns=columns)
    accepted = classified[classified["validation_status"].eq("accepted")].copy()
    rejected = classified[classified["validation_status"].eq("rejected")].copy()
    exceptions = classified[classified["validation_status"].eq("exception")].copy()
    summary = _summary_rows(classified)

    classified.to_csv(out_dir / "lineup_slot_candidate_rows.csv", index=False)
    accepted.to_csv(out_dir / "lineup_slot_accepted_rows.csv", index=False)
    rejected.to_csv(out_dir / "lineup_slot_rejected_rows.csv", index=False)
    exceptions.to_csv(out_dir / "lineup_slot_exception_rows.csv", index=False)
    summary.to_csv(out_dir / "lineup_slot_coverage_summary.csv", index=False)
    _write_report(out_dir, classified, summary, generated_at)

    print(f"Wrote {out_dir / 'lineup_slot_candidate_rows.csv'}")
    print(f"Wrote {out_dir / 'lineup_slot_accepted_rows.csv'}")
    print(f"Wrote {out_dir / 'lineup_slot_rejected_rows.csv'}")
    print(f"Wrote {out_dir / 'lineup_slot_exception_rows.csv'}")
    print(f"Wrote {out_dir / 'lineup_slot_coverage_summary.csv'}")
    print(f"Wrote {out_dir / 'lineup_slot_backfill_prepass_2026-07-05.md'}")
    print(
        "counts",
        {
            "rows": len(classified),
            "accepted": len(accepted),
            "rejected": len(rejected),
            "exception": len(exceptions),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
