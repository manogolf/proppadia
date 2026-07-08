#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_REVIEW_AIDS_DIR = Path("artifacts/analysis/mlb/review_aids")
DEFAULT_OUT_ROOT = Path("artifacts/analysis/mlb/environment_v2/daily")
DEFAULT_HITS_ENVIRONMENT_LATEST = Path("artifacts/analysis/mlb/mlb_hits_environment_latest.json")
DEFAULT_HITS_ENVIRONMENT_HISTORY = Path("artifacts/analysis/mlb/mlb_hits_environment_history.jsonl")

SOURCE_BOARDS = [
    ("main", "simple_filter", "hits_o15_simple_filter"),
    ("main", "watch", "hits_o15_watch_candidates"),
    ("main", "expanded_review", "hits_o15_layered_candidates"),
    ("alternate", "alternate_discovery", "hits_o15_alternate_discovery"),
]

COMPONENT_FIELDS = [
    "offense_factor_vs_league_clamped",
    "offense_hits_form_blended",
    "pitcher_expected_hits_allowed_weighted",
    "pitcher_base",
    "starter_expected_hits_allowed",
    "bullpen_hits_allowed_form_blended",
    "team_expected_hits_allowed",
]

PROFILE_MAP = {
    "offense_high_starter_high_bullpen_high": ("aligned_high_environment", "Aligned High Environment", "continue"),
    "offense_high_starter_high_bullpen_low": ("starter_led_with_bullpen_drag", "Starter-Led With Bullpen Drag", "continue"),
    "team_expected_high_starter_mediocre": ("team_high_starter_mediocre", "Team High / Starter Mediocre", "continue"),
    "starter_high_team_expected_mediocre": ("starter_high_team_mediocre", "Starter High / Team Mediocre", "continue"),
    "offense_high_starter_low_bullpen_high": ("bullpen_rescue_starter_suppressed", "Bullpen Rescue / Starter Suppressed", "parking_lot"),
    "offense_low_starter_high_bullpen_high": ("starter_only_offense_suppressed", "Starter Only / Offense Suppressed", "parking_lot"),
}

OUTPUT_FIELDS = [
    "date",
    "player_name",
    "player_id",
    "team",
    "opponent",
    "game_id",
    "canonical_player_id",
    "canonical_game_id",
    "canonical_team",
    "canonical_opponent",
    "prop_type",
    "side",
    "line",
    "market_price",
    "best_over_price",
    "current_hitter_tier",
    "current_pitcher_tier",
    "current_combined_tier",
    "offense_factor_vs_league_clamped",
    "offense_hits_form_blended",
    "pitcher_expected_hits_allowed_weighted",
    "pitcher_base",
    "starter_expected_hits_allowed",
    "bullpen_hits_allowed_form_blended",
    "team_expected_hits_allowed",
    "offense_context_as_of_date",
    "offense_window_excludes_eval_date",
    "offense_window_max_source_game_date",
    "local_team_hits_parity_status",
    "team_hits_mismatch_count",
    "team_hits_rescheduled_outside_window_count",
    "offense_factor_lineage_health_generated_at",
    "env_v2_beta_profile_family",
    "env_v2_beta_profile_label",
    "env_v2_beta_research_status",
    "env_v2_beta_alpha_profile_key",
    "env_v2_beta_offense_bucket",
    "env_v2_beta_starter_bucket",
    "env_v2_beta_bullpen_bucket",
    "env_v2_beta_team_expected_bucket",
    "env_v2_beta_flag_aligned_high_environment",
    "env_v2_beta_flag_starter_led_with_bullpen_drag",
    "env_v2_beta_flag_team_high_starter_mediocre",
    "env_v2_beta_flag_starter_high_team_mediocre",
    "env_v2_beta_flag_bullpen_rescue_starter_suppressed",
    "env_v2_beta_flag_starter_only_offense_suppressed",
    "source_universe",
    "source_population",
    "source_board",
    "source_artifact_path",
    "source_run_timestamp",
    "source_list",
    "source_count",
    "outcome_status",
    "resolved_status",
    "outcome",
    "win_loss",
    "odds_used",
    "units",
]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    import io

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=OUTPUT_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_write_csv_bytes(rows))


def _f(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "null"}:
            return None
        return float(text)
    except Exception:
        return None


def _s(value: Any) -> str:
    return str(value or "").strip()


def _cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _bucket(value: float | None, low_max: float, high_min: float) -> str:
    if value is None:
        return "missing"
    if value < low_max:
        return "low"
    if value >= high_min:
        return "high"
    return "mid"


def _market_key(row: dict[str, Any]) -> str:
    canonical = _s(row.get("canonical_market_key"))
    if canonical:
        return canonical
    return "|".join(
        [
            _s(row.get("date")),
            _s(row.get("canonical_game_id") or row.get("game_id")),
            _s(row.get("canonical_player_id") or row.get("player_id")),
            "hits",
            _s(row.get("side") or "over"),
            _s(row.get("line")),
        ]
    )


def _source_timestamp(row: dict[str, Any], path: Path) -> str:
    for field in ("environment_artifact_timestamp", "starter_context_updated_at"):
        value = _s(row.get(field))
        if value:
            return value
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


def _extract_offense_factor_lineage(payload: dict[str, Any], date_text: str, source_path: Path) -> dict[str, Any] | None:
    if _s(payload.get("evaluation_date"))[:10] != date_text:
        return None
    lineage = payload.get("offense_factor_lineage")
    if not isinstance(lineage, dict):
        return None
    return {
        "offense_context_as_of_date": _cell(lineage.get("offense_context_as_of_date")),
        "offense_window_excludes_eval_date": _cell(lineage.get("offense_window_excludes_eval_date")),
        "offense_window_max_source_game_date": _cell(lineage.get("offense_window_max_source_game_date")),
        "local_team_hits_parity_status": _cell(lineage.get("local_team_hits_parity_status")) or "unknown",
        "team_hits_mismatch_count": _cell(lineage.get("team_hits_mismatch_count")),
        "team_hits_rescheduled_outside_window_count": _cell(lineage.get("team_hits_rescheduled_outside_window_count")),
        "offense_factor_lineage_health_generated_at": _cell(lineage.get("offense_factor_lineage_health_generated_at")),
        "_offense_factor_lineage_source": str(source_path),
        "_offense_factor_lineage_status": "retained",
    }


def _load_offense_factor_lineage(date_text: str) -> dict[str, Any]:
    for path in (DEFAULT_HITS_ENVIRONMENT_LATEST,):
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        lineage = _extract_offense_factor_lineage(payload, date_text, path)
        if lineage is not None:
            return lineage

    history_path = DEFAULT_HITS_ENVIRONMENT_HISTORY
    if history_path.exists():
        try:
            lines = history_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            lines = []
        for line in reversed(lines):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            lineage = _extract_offense_factor_lineage(payload, date_text, history_path)
            if lineage is not None:
                return lineage

    return {
        "offense_context_as_of_date": "",
        "offense_window_excludes_eval_date": "",
        "offense_window_max_source_game_date": "",
        "local_team_hits_parity_status": "unknown",
        "team_hits_mismatch_count": "",
        "team_hits_rescheduled_outside_window_count": "",
        "offense_factor_lineage_health_generated_at": "",
        "_offense_factor_lineage_source": "",
        "_offense_factor_lineage_status": "unknown_missing_hits_environment_lineage",
    }


def _profile(row: dict[str, Any]) -> dict[str, str]:
    offense = _bucket(_f(row.get("offense_factor_vs_league_clamped")), 0.95, 1.05)
    starter = _bucket(_f(row.get("starter_expected_hits_allowed")), 4.5, 5.5)
    bullpen = _bucket(_f(row.get("bullpen_hits_allowed_form_blended")), 3.5, 4.5)
    team = _bucket(_f(row.get("team_expected_hits_allowed")), 8.0, 9.0)
    if offense == "high" and starter == "high" and bullpen == "high":
        key = "offense_high_starter_high_bullpen_high"
    elif offense == "high" and starter == "high" and bullpen == "low":
        key = "offense_high_starter_high_bullpen_low"
    elif team == "high" and starter in {"low", "mid"}:
        key = "team_expected_high_starter_mediocre"
    elif starter == "high" and team in {"low", "mid"}:
        key = "starter_high_team_expected_mediocre"
    elif offense == "high" and starter == "low" and bullpen == "high":
        key = "offense_high_starter_low_bullpen_high"
    elif offense == "low" and starter == "high" and bullpen == "high":
        key = "offense_low_starter_high_bullpen_high"
    elif "missing" in {offense, starter, bullpen, team}:
        key = "component_missing"
    else:
        key = f"offense_{offense}_starter_{starter}_bullpen_{bullpen}"
    family, label, status = PROFILE_MAP.get(key, ("other", "Other / None", "other"))
    return {
        "key": key,
        "family": family,
        "label": label,
        "status": f"research_only_{status}",
        "offense": offense,
        "starter": starter,
        "bullpen": bullpen,
        "team": team,
    }


def _load_sources(date_text: str, review_aids_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_universe, source_population, board in SOURCE_BOARDS:
        path = review_aids_dir / f"{board}_{date_text}.csv"
        for row in _read_csv(path):
            if _s(row.get("side")).lower() not in {"", "over"}:
                continue
            row["_source_universe"] = source_universe
            row["_source_population"] = source_population
            row["_source_board"] = board
            row["_source_path"] = str(path)
            row["_source_timestamp"] = _source_timestamp(row, path)
            rows.append(row)
    return rows


def _combine_rows(source_rows: list[dict[str, Any]], offense_factor_lineage: dict[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in source_rows:
        grouped.setdefault(_market_key(row), []).append(row)
    out: list[dict[str, Any]] = []
    for _key, group in sorted(grouped.items()):
        # Prefer richer operational rows, then alternate discovery.
        group.sort(
            key=lambda r: (
                {"hits_o15_layered_candidates": 0, "hits_o15_watch_candidates": 1, "hits_o15_simple_filter": 2, "hits_o15_alternate_discovery": 3}.get(
                    _s(r.get("_source_board")), 9
                ),
                _s(r.get("player_name") or r.get("player")),
            )
        )
        row = group[0]
        profile = _profile(row)
        sources = sorted({_s(r.get("_source_board")) for r in group if _s(r.get("_source_board"))})
        out.append(
            {
                "date": _s(row.get("date")),
                "player_name": _s(row.get("player_name") or row.get("player")),
                "player_id": _s(row.get("player_id")),
                "team": _s(row.get("team")),
                "opponent": _s(row.get("opponent")),
                "game_id": _s(row.get("game_id")),
                "canonical_player_id": _s(row.get("canonical_player_id")),
                "canonical_game_id": _s(row.get("canonical_game_id")),
                "canonical_team": _s(row.get("canonical_team")),
                "canonical_opponent": _s(row.get("canonical_opponent")),
                "prop_type": "hits",
                "side": _s(row.get("side") or "over"),
                "line": _s(row.get("line")),
                "market_price": _s(row.get("market_price") or row.get("best_over_price") or row.get("price_over")),
                "best_over_price": _s(row.get("best_over_price") or row.get("market_price") or row.get("price_over")),
                "current_hitter_tier": _s(row.get("hitter_tier")),
                "current_pitcher_tier": _s(row.get("pitcher_tier")),
                "current_combined_tier": _s(row.get("combined_tier")),
                "offense_factor_vs_league_clamped": _s(row.get("offense_factor_vs_league_clamped")),
                "offense_hits_form_blended": _s(row.get("offense_hits_form_blended")),
                "pitcher_expected_hits_allowed_weighted": _s(row.get("pitcher_expected_hits_allowed_weighted")),
                "pitcher_base": _s(row.get("pitcher_base")),
                "starter_expected_hits_allowed": _s(row.get("starter_expected_hits_allowed")),
                "bullpen_hits_allowed_form_blended": _s(row.get("bullpen_hits_allowed_form_blended")),
                "team_expected_hits_allowed": _s(row.get("team_expected_hits_allowed")),
                "offense_context_as_of_date": _cell(offense_factor_lineage.get("offense_context_as_of_date")),
                "offense_window_excludes_eval_date": _cell(offense_factor_lineage.get("offense_window_excludes_eval_date")),
                "offense_window_max_source_game_date": _cell(offense_factor_lineage.get("offense_window_max_source_game_date")),
                "local_team_hits_parity_status": _cell(offense_factor_lineage.get("local_team_hits_parity_status")) or "unknown",
                "team_hits_mismatch_count": _cell(offense_factor_lineage.get("team_hits_mismatch_count")),
                "team_hits_rescheduled_outside_window_count": _cell(
                    offense_factor_lineage.get("team_hits_rescheduled_outside_window_count")
                ),
                "offense_factor_lineage_health_generated_at": _cell(
                    offense_factor_lineage.get("offense_factor_lineage_health_generated_at")
                ),
                "env_v2_beta_profile_family": profile["family"],
                "env_v2_beta_profile_label": profile["label"],
                "env_v2_beta_research_status": profile["status"],
                "env_v2_beta_alpha_profile_key": profile["key"],
                "env_v2_beta_offense_bucket": profile["offense"],
                "env_v2_beta_starter_bucket": profile["starter"],
                "env_v2_beta_bullpen_bucket": profile["bullpen"],
                "env_v2_beta_team_expected_bucket": profile["team"],
                "env_v2_beta_flag_aligned_high_environment": "yes" if profile["family"] == "aligned_high_environment" else "no",
                "env_v2_beta_flag_starter_led_with_bullpen_drag": "yes" if profile["family"] == "starter_led_with_bullpen_drag" else "no",
                "env_v2_beta_flag_team_high_starter_mediocre": "yes" if profile["family"] == "team_high_starter_mediocre" else "no",
                "env_v2_beta_flag_starter_high_team_mediocre": "yes" if profile["family"] == "starter_high_team_mediocre" else "no",
                "env_v2_beta_flag_bullpen_rescue_starter_suppressed": "yes" if profile["family"] == "bullpen_rescue_starter_suppressed" else "no",
                "env_v2_beta_flag_starter_only_offense_suppressed": "yes" if profile["family"] == "starter_only_offense_suppressed" else "no",
                "source_universe": _s(row.get("_source_universe")),
                "source_population": _s(row.get("_source_population")),
                "source_board": _s(row.get("_source_board")),
                "source_artifact_path": _s(row.get("_source_path")),
                "source_run_timestamp": _s(row.get("_source_timestamp")),
                "source_list": ",".join(sources),
                "source_count": str(len(sources)),
                "outcome_status": "pending",
                "resolved_status": "unresolved",
                "outcome": "",
                "win_loss": "",
                "odds_used": "",
                "units": "",
            }
        )
    return out


def _summary_payload(
    date_text: str,
    rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    profiles_path: Path,
    offense_factor_lineage: dict[str, Any],
) -> dict[str, Any]:
    family_counts = Counter(row["env_v2_beta_profile_label"] for row in rows)
    status_counts = Counter(row["env_v2_beta_research_status"] for row in rows)
    source_counts = Counter()
    for row in rows:
        for source in filter(None, row.get("source_list", "").split(",")):
            source_counts[source] += 1
    return {
        "date": date_text,
        "generated_at": _now(),
        "research_only": True,
        "production_behavior_changed": False,
        "outcomes_pending": True,
        "source_rows_loaded": len(source_rows),
        "daily_rows_captured": len(rows),
        "profile_family_counts": dict(family_counts),
        "research_status_counts": dict(status_counts),
        "source_board_counts": dict(source_counts),
        "offense_factor_lineage": {
            "status": _s(offense_factor_lineage.get("_offense_factor_lineage_status")),
            "source": _s(offense_factor_lineage.get("_offense_factor_lineage_source")),
            "offense_context_as_of_date": _cell(offense_factor_lineage.get("offense_context_as_of_date")),
            "offense_window_excludes_eval_date": _cell(offense_factor_lineage.get("offense_window_excludes_eval_date")),
            "offense_window_max_source_game_date": _cell(offense_factor_lineage.get("offense_window_max_source_game_date")),
            "local_team_hits_parity_status": _cell(offense_factor_lineage.get("local_team_hits_parity_status")) or "unknown",
            "team_hits_mismatch_count": _cell(offense_factor_lineage.get("team_hits_mismatch_count")),
            "team_hits_rescheduled_outside_window_count": _cell(
                offense_factor_lineage.get("team_hits_rescheduled_outside_window_count")
            ),
            "offense_factor_lineage_health_generated_at": _cell(
                offense_factor_lineage.get("offense_factor_lineage_health_generated_at")
            ),
        },
        "profiles_csv": str(profiles_path),
        "profiles_sha256": hashlib.sha256(profiles_path.read_bytes()).hexdigest() if profiles_path.exists() else "",
    }


def _write_summary_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Environment v2-beta Daily Research Summary",
        "",
        f"- Date: `{payload['date']}`",
        f"- Generated at: `{payload['generated_at']}`",
        "- Scope: research-only observational capture.",
        "- Production behavior changed: `no`",
        "- Outcomes: `pending`",
        "",
        "## Capture",
        "",
        f"- Source rows loaded: `{payload['source_rows_loaded']}`",
        f"- Daily rows captured: `{payload['daily_rows_captured']}`",
        "",
        "## Profile Family Counts",
        "",
        "| profile family | rows |",
        "|---|---:|",
    ]
    for family, count in sorted(payload["profile_family_counts"].items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"| `{family}` | `{count}` |")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This is observational capture, not recommendation logic.",
            "- Rows are designed for future postgame reconciliation by adding outcome, win/loss, odds used, resolved status, and units.",
            "- Missing this research artifact should be a research WARN, not a production blocker.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_health(
    path: Path,
    date_text: str,
    rows: list[dict[str, Any]],
    profiles_path: Path,
    offense_factor_lineage: dict[str, Any],
) -> None:
    missing_component_rows = sum(1 for row in rows if any(not _s(row.get(field)) for field in COMPONENT_FIELDS))
    missing_profile_rows = sum(1 for row in rows if not _s(row.get("env_v2_beta_profile_family")))
    status = "PASS" if profiles_path.exists() and rows and missing_profile_rows == 0 else "WARN"
    lineage_status = _s(offense_factor_lineage.get("_offense_factor_lineage_status"))
    lineage_check = "PASS" if lineage_status == "retained" else "WARN"
    lines = [
        "# Environment v2-beta Daily Research Health",
        "",
        f"- Date: `{date_text}`",
        f"- Generated at: `{_now()}`",
        f"- Status: `{status}`",
        "",
        "| check | value | status |",
        "|---|---:|---|",
        f"| profiles CSV exists | `{profiles_path.exists()}` | `{'PASS' if profiles_path.exists() else 'WARN'}` |",
        f"| rows captured | `{len(rows)}` | `{'PASS' if rows else 'WARN'}` |",
        f"| rows missing profile family | `{missing_profile_rows}` | `{'PASS' if missing_profile_rows == 0 else 'WARN'}` |",
        f"| rows missing one or more component fields | `{missing_component_rows}` | `INFO` |",
        f"| offense factor lineage retained | `{lineage_status}` | `{lineage_check}` |",
        "",
        "Missing component values are tracked for research coverage. They do not alter production behavior.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_outputs(
    date_text: str,
    rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    out_root: Path,
    offense_factor_lineage: dict[str, Any],
) -> dict[str, Path]:
    out_dir = out_root / date_text
    out_dir.mkdir(parents=True, exist_ok=True)
    profiles = out_dir / f"environment_v2_beta_daily_profiles_{date_text}.csv"
    summary_md = out_dir / f"environment_v2_beta_daily_summary_{date_text}.md"
    summary_json = out_dir / f"environment_v2_beta_daily_summary_{date_text}.json"
    health_md = out_dir / f"environment_v2_beta_daily_health_{date_text}.md"

    content = _write_csv_bytes(rows)
    target_profiles = profiles
    if profiles.exists() and profiles.read_bytes() != content:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        rerun_dir = out_dir / f"rerun_{stamp}"
        rerun_dir.mkdir(parents=True, exist_ok=True)
        target_profiles = rerun_dir / profiles.name
        summary_md = rerun_dir / summary_md.name
        summary_json = rerun_dir / summary_json.name
        health_md = rerun_dir / health_md.name
    target_profiles.write_bytes(content)
    payload = _summary_payload(date_text, rows, source_rows, target_profiles, offense_factor_lineage)
    _write_summary_md(summary_md, payload)
    summary_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_health(health_md, date_text, rows, target_profiles, offense_factor_lineage)
    return {"profiles": target_profiles, "summary_md": summary_md, "summary_json": summary_json, "health_md": health_md}


def _write_report(path: Path, date_text: str, rows: list[dict[str, Any]], outputs: dict[str, Path], wrapper_mode: str) -> None:
    counts = Counter(row["env_v2_beta_profile_label"] for row in rows)
    lines = [
        "# Environment v2-beta Daily Research Lane",
        "",
        f"- Date tested: `{date_text}`",
        f"- Generated at: `{_now()}`",
        "- Status: implemented as research-only observational capture.",
        f"- Daily automation integration: `{wrapper_mode}`",
        "- Production behavior changed: `no`",
        "- Selectors/uploads/grading/models changed: `no`",
        "- Morning Workbench/Ops Brief content changed: `no`",
        "",
        "## Outputs",
        "",
        f"- Profiles CSV: `{outputs['profiles']}`",
        f"- Summary MD: `{outputs['summary_md']}`",
        f"- Summary JSON: `{outputs['summary_json']}`",
        f"- Health MD: `{outputs['health_md']}`",
        "",
        "## Capture Summary",
        "",
        f"- Daily rows captured: `{len(rows)}`",
        "- Outcome status: `pending`",
        "",
        "## Profile Family Counts",
        "",
        "| profile family | rows |",
        "|---|---:|",
    ]
    for family, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"| `{family}` | `{count}` |")
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- This lane observes today's profile context only.",
            "- It does not recommend candidates.",
            "- It does not change tiers or production uploads.",
            "- Rows include pending outcome columns for future reconciliation.",
            "- The Make target is lightweight and safe to run after review boards exist.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Capture daily Environment v2-beta research profiles from current O1.5 review artifacts.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--review-aids-dir", type=Path, default=DEFAULT_REVIEW_AIDS_DIR)
    ap.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    ap.add_argument("--implementation-report", type=Path, default=DEFAULT_REVIEW_AIDS_DIR / "environment_v2_beta_daily_research_lane_2026-06-29.md")
    ap.add_argument("--wrapper-mode", default="manual_target_only")
    args = ap.parse_args()

    date_text = str(args.date)[:10]
    source_rows = _load_sources(date_text, args.review_aids_dir)
    offense_factor_lineage = _load_offense_factor_lineage(date_text)
    rows = _combine_rows(source_rows, offense_factor_lineage)
    outputs = _write_outputs(date_text, rows, source_rows, args.out_root, offense_factor_lineage)
    _write_report(args.implementation_report, date_text, rows, outputs, args.wrapper_mode)
    print(
        json.dumps(
            {
                "date": date_text,
                "daily_rows_captured": len(rows),
                "source_rows_loaded": len(source_rows),
                "outputs": {k: str(v) for k, v in outputs.items()},
                "implementation_report": str(args.implementation_report),
                "wrapper_mode": args.wrapper_mode,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
