#!/usr/bin/env python3
"""Audit MLB canonical identity coverage and join-risk patterns.

This is intentionally read-only. It documents where MLB artifacts carry
canonical IDs, provider IDs, labels, aliases, and fallback identity signals.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/identity")

PLAYER_ID_COLS = ("player_id", "mlb_player_id", "pitcher_id", "opposing_starter_id", "starter_id")
GAME_ID_COLS = ("game_id", "mlb_game_id", "game_pk")
TEAM_COLS = ("team", "team_code", "home_team_code", "away_team_code", "pitcher_team", "offense_team")
OPP_COLS = ("opponent", "opp", "opponent_code", "away_team_code", "home_team_code")
EVENT_ID_COLS = ("event_id", "provider_event_id", "oddsapi_event_id")
PLAYER_NAME_COLS = ("player_name", "player", "description", "name", "pitcher_name", "opposing_starter")
NORM_NAME_COLS = ("normalized_player_name", "player_name_norm", "player_name_key", "normalized_name")
PROVIDER_COLS = ("source_provider", "provider", "bookmaker", "bookmaker_key", "source", "market_key")
PROVENANCE_COLS = (
    "identity_provenance",
    "identity_confidence",
    "context_source",
    "starter_context_source",
    "join_method",
    "match_method",
    "source_context_path",
)
MARKET_KEY_COLS = ("canonical_market_key", "market_key", "fallback_market_key")
FALLBACK_COLS = ("fallback", "fallback_key", "identity_fallback_used", "join_method", "match_method")
AMBIGUOUS_COLS = ("ambiguous", "ambiguous_identity", "ambiguous_reason", "unavailable_reason", "reconcile_exclusion_reason")


@dataclass
class ArtifactSpec:
    name: str
    category: str
    path_globs: tuple[str, ...]
    expected_identity: str
    notes: str = ""


ARTIFACT_SPECS = [
    ArtifactSpec(
        "mlb_slate_output",
        "D. derived artifact that should preserve canonical IDs",
        ("backend/mlb/exports/odds_history/*/mlb_slate_output*.csv",),
        "date + MLB game_id + MLB player_id + prop_type + side + line",
        "Primary local slate/scored universe.",
    ),
    ArtifactSpec(
        "OddsAPI raw/book-level artifacts",
        "C. alias/name-only source",
        (
            "artifacts/analysis/mlb/review_aids/oddsapi_batter_hits_alternate_live_discovery/*/live_alternate_book_level_rows.csv",
            "artifacts/analysis/mlb/review_aids/alternate_history/backfill/*/live_alternate_book_level_rows.csv",
        ),
        "provider event_id + player label; canonical IDs attached later when possible",
        "OddsAPI player props commonly lack MLB player_id.",
    ),
    ArtifactSpec(
        "upload prep artifacts",
        "D. derived artifact that should preserve canonical IDs",
        ("backend/mlb/data/processed/mlb_uploads/*/*.csv",),
        "date + MLB game_id + MLB player_id + market key",
        "Upload diagnostics should retain lineage columns.",
    ),
    ArtifactSpec(
        "lane selector / quick card",
        "D. derived artifact that should preserve canonical IDs",
        ("backend/mlb/exports/model_v2/lanes/today/*/*.csv",),
        "date + MLB game_id + MLB player_id + prop_type + side + line",
        "Daily ranking/QC artifacts.",
    ),
    ArtifactSpec(
        "reconcile_rows",
        "D. derived artifact that should preserve canonical IDs",
        ("artifacts/analysis/mlb/execution_vs_model/*/reconcile_rows.csv",),
        "date + MLB game_id + MLB player_id + prop_type + side + line",
        "Completed-slate accounting should preserve canonical row identity.",
    ),
    ArtifactSpec(
        "review boards",
        "D. derived artifact that should preserve canonical IDs",
        ("artifacts/analysis/mlb/review_aids/hits_*_*.csv",),
        "date + MLB player_id/game_id where source supports it",
        "Review aid boards should show fallback/provenance if ID is missing.",
    ),
    ArtifactSpec(
        "expanded_o15_universe_rows",
        "D. derived artifact that should preserve canonical IDs",
        ("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv",),
        "date + MLB player_id + line + side; fallback key only when ID unavailable",
        "Canonical O1.5 research universe.",
    ),
    ArtifactSpec(
        "hits-environment artifacts",
        "D. derived artifact that should preserve canonical IDs",
        ("artifacts/analysis/mlb/mlb_hits_environment_latest.json", "artifacts/analysis/mlb/hits_environment_snapshots/*.json"),
        "pitcher MLB player_id + game_id + pitcher/offense team",
        "Expected hits allowed context and starter diagnostics.",
    ),
    ArtifactSpec(
        "BvP artifacts",
        "D. derived artifact that should preserve canonical IDs",
        ("artifacts/analysis/mlb/**/*bvp*.csv", "artifacts/analysis/mlb/**/*BvP*.csv"),
        "batter MLB player_id + pitcher MLB player_id + date/game_id if available",
        "BvP/PvB must distinguish IDs from labels.",
    ),
    ArtifactSpec(
        "model prediction outputs",
        "D. derived artifact that should preserve canonical IDs",
        ("backend/mlb/exports/*pred*.csv", "backend/mlb/exports/odds_history/*/*pred*.csv"),
        "MLB game_id + player_id + prop_type + line",
        "Prediction rows should be ID-first.",
    ),
]

TABLE_INVENTORY = [
    {
        "source": "mlb.player_ids",
        "classification": "A. official-ID source",
        "canonical_identity": "MLB player_id",
        "identity_fields": "player_id, player_name/name aliases when present",
        "notes": "Official/local mapping table; duplicate names are expected and must be resolved by ID/context.",
    },
    {
        "source": "mlb.game_info",
        "classification": "A. official-ID source",
        "canonical_identity": "MLB game_id",
        "identity_fields": "game_id, game_date, home/away team codes, probable starters when present",
        "notes": "Game identity backbone.",
    },
    {
        "source": "mlb.player_team_by_game",
        "classification": "A. official-ID source",
        "canonical_identity": "game_id + player_id",
        "identity_fields": "game_id, player_id, team, opponent",
        "notes": "Best bridge from player to team/opponent context on a game date.",
    },
    {
        "source": "mlb.player_stats",
        "classification": "A. official-ID source",
        "canonical_identity": "game_id + player_id",
        "identity_fields": "game_id, player_id, team/opponent when present",
        "notes": "Outcome/stat source; names are display labels.",
    },
    {
        "source": "mlb.player_derived_stats",
        "classification": "A. official-ID source",
        "canonical_identity": "game_id + player_id",
        "identity_fields": "game_id, player_id",
        "notes": "Pregame/rolling feature source; must not be joined by name when IDs exist.",
    },
]


JOIN_PATTERNS = [
    (re.compile(r"merge\([^)]*on\s*=\s*\[[^\]]*player_name", re.I | re.S), "merge on player_name"),
    (re.compile(r"merge\([^)]*on\s*=\s*\[[^\]]*player_name_norm", re.I | re.S), "merge on normalized player name"),
    (re.compile(r"merge\([^)]*on\s*=\s*\[[^\]]*team[^\]]*opponent", re.I | re.S), "merge on team/opponent"),
    (re.compile(r"left_on\s*=\s*\[[^\]]*player_name", re.I | re.S), "left_on player_name"),
    (re.compile(r"right_on\s*=\s*\[[^\]]*player_name", re.I | re.S), "right_on player_name"),
    (re.compile(r"drop_duplicates\([^)]*\[[^\]]*player_name", re.I | re.S), "dedupe by player_name"),
    (re.compile(r"normalized_player|player_name_norm|player_name_key|norm_name", re.I), "normalized-name identity helper"),
]


def _read_json_rows(path: Path) -> pd.DataFrame:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return pd.DataFrame()
    if isinstance(data, list):
        return pd.json_normalize(data)
    if isinstance(data, dict):
        for key in ("rows", "records", "items", "pitchers", "starters", "checks"):
            value = data.get(key)
            if isinstance(value, list):
                return pd.json_normalize(value)
        return pd.json_normalize([data])
    return pd.DataFrame()


def _read_artifact(path: Path, max_rows: int | None) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        try:
            return pd.read_csv(path, nrows=max_rows, low_memory=False)
        except Exception:
            return pd.DataFrame()
    if path.suffix.lower() == ".json":
        df = _read_json_rows(path)
        return df.head(max_rows) if max_rows else df
    return pd.DataFrame()


def _latest_paths(patterns: tuple[str, ...], limit: int) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(Path().glob(pattern))
    paths = [p for p in paths if p.is_file()]
    paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return paths[:limit]


def _has_any(columns: set[str], options: tuple[str, ...]) -> bool:
    lower = {c.lower() for c in columns}
    return any(opt.lower() in lower for opt in options)


def _matching_cols(columns: list[str], options: tuple[str, ...]) -> list[str]:
    lower_map = {c.lower(): c for c in columns}
    return [lower_map[opt.lower()] for opt in options if opt.lower() in lower_map]


def _coverage_for_cols(df: pd.DataFrame, cols: tuple[str, ...]) -> tuple[str, int, float]:
    matches = _matching_cols(list(df.columns), cols)
    if not matches or df.empty:
        return "", 0, 0.0
    present = df[matches].notna().any(axis=1)
    for col in matches:
        present = present | (df[col].astype(str).str.strip() != "")
    count = int(present.sum())
    return ",".join(matches), count, count / len(df) if len(df) else 0.0


def _fallback_count(df: pd.DataFrame) -> tuple[str, int, float]:
    matches = _matching_cols(list(df.columns), FALLBACK_COLS)
    if not matches or df.empty:
        return "", 0, 0.0
    flags = pd.Series(False, index=df.index)
    for col in matches:
        vals = df[col].astype(str).str.lower().str.strip()
        flags = flags | vals.str.contains("fallback|name|alias|normalized", regex=True, na=False)
    count = int(flags.sum())
    return ",".join(matches), count, count / len(df) if len(df) else 0.0


def _ambiguous_count(df: pd.DataFrame) -> tuple[str, int, float]:
    matches = _matching_cols(list(df.columns), AMBIGUOUS_COLS)
    if not matches or df.empty:
        return "", 0, 0.0
    flags = pd.Series(False, index=df.index)
    for col in matches:
        vals = df[col].astype(str).str.lower().str.strip()
        flags = flags | vals.str.contains("ambiguous|unresolved|missing|not_found|mismatch", regex=True, na=False)
    count = int(flags.sum())
    return ",".join(matches), count, count / len(df) if len(df) else 0.0


def _classify_artifact(spec: ArtifactSpec, df: pd.DataFrame) -> str:
    columns = set(df.columns)
    if _has_any(columns, PLAYER_ID_COLS) and _has_any(columns, GAME_ID_COLS):
        return "D. derived artifact that should preserve canonical IDs"
    if _has_any(columns, EVENT_ID_COLS) and not _has_any(columns, PLAYER_ID_COLS):
        return "C. alias/name-only source"
    if _has_any(columns, EVENT_ID_COLS):
        return "B. provider-ID source"
    if spec.category.startswith("D.") and not _has_any(columns, PLAYER_ID_COLS) and not _has_any(columns, GAME_ID_COLS):
        return "E. unsafe/mixed identity source"
    return spec.category


def build_coverage(out_dir: Path, max_files_per_spec: int, max_rows: int | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in TABLE_INVENTORY:
        rows.append(
            {
                "artifact": table["source"],
                "path": "database table",
                "classification": table["classification"],
                "expected_identity": table["canonical_identity"],
                "rows_sampled": "",
                "player_id_cols": "player_id" if "player_id" in table["identity_fields"] else "",
                "player_id_coverage_pct": "",
                "game_id_cols": "game_id" if "game_id" in table["identity_fields"] else "",
                "game_id_coverage_pct": "",
                "canonical_team_cols": "",
                "canonical_team_coverage_pct": "",
                "opponent_cols": "",
                "opponent_coverage_pct": "",
                "provider_event_id_cols": "",
                "provider_event_id_coverage_pct": "",
                "canonical_market_key_cols": "",
                "canonical_market_key_coverage_pct": "",
                "player_name_cols": "name/aliases",
                "normalized_player_name_cols": "",
                "source_provider_cols": "",
                "identity_provenance_cols": "",
                "fallback_identity_rows": "",
                "ambiguous_identity_rows": "",
                "notes": table["notes"],
            }
        )
    for spec in ARTIFACT_SPECS:
        paths = _latest_paths(spec.path_globs, max_files_per_spec)
        if not paths:
            rows.append(
                {
                    "artifact": spec.name,
                    "path": "missing",
                    "classification": spec.category,
                    "expected_identity": spec.expected_identity,
                    "rows_sampled": 0,
                    "player_id_cols": "",
                    "player_id_coverage_pct": 0,
                    "game_id_cols": "",
                    "game_id_coverage_pct": 0,
                    "canonical_team_cols": "",
                    "canonical_team_coverage_pct": 0,
                    "opponent_cols": "",
                    "opponent_coverage_pct": 0,
                    "provider_event_id_cols": "",
                    "provider_event_id_coverage_pct": 0,
                    "canonical_market_key_cols": "",
                    "canonical_market_key_coverage_pct": 0,
                    "player_name_cols": "",
                    "normalized_player_name_cols": "",
                    "source_provider_cols": "",
                    "identity_provenance_cols": "",
                    "fallback_identity_rows": 0,
                    "ambiguous_identity_rows": 0,
                    "notes": f"No local artifact matched: {', '.join(spec.path_globs)}",
                }
            )
            continue
        for path in paths:
            df = _read_artifact(path, max_rows)
            p_cols, p_count, p_cov = _coverage_for_cols(df, PLAYER_ID_COLS)
            g_cols, g_count, g_cov = _coverage_for_cols(df, GAME_ID_COLS)
            t_cols, t_count, t_cov = _coverage_for_cols(df, TEAM_COLS)
            o_cols, o_count, o_cov = _coverage_for_cols(df, OPP_COLS)
            e_cols, e_count, e_cov = _coverage_for_cols(df, EVENT_ID_COLS)
            m_cols, m_count, m_cov = _coverage_for_cols(df, MARKET_KEY_COLS)
            f_cols, f_count, _f_cov = _fallback_count(df)
            a_cols, a_count, _a_cov = _ambiguous_count(df)
            rows.append(
                {
                    "artifact": spec.name,
                    "path": path.as_posix(),
                    "classification": _classify_artifact(spec, df),
                    "expected_identity": spec.expected_identity,
                    "rows_sampled": len(df),
                    "player_id_cols": p_cols,
                    "player_id_coverage_pct": round(p_cov * 100, 2),
                    "game_id_cols": g_cols,
                    "game_id_coverage_pct": round(g_cov * 100, 2),
                    "canonical_team_cols": t_cols,
                    "canonical_team_coverage_pct": round(t_cov * 100, 2),
                    "opponent_cols": o_cols,
                    "opponent_coverage_pct": round(o_cov * 100, 2),
                    "provider_event_id_cols": e_cols,
                    "provider_event_id_coverage_pct": round(e_cov * 100, 2),
                    "canonical_market_key_cols": m_cols,
                    "canonical_market_key_coverage_pct": round(m_cov * 100, 2),
                    "player_name_cols": ",".join(_matching_cols(list(df.columns), PLAYER_NAME_COLS)),
                    "normalized_player_name_cols": ",".join(_matching_cols(list(df.columns), NORM_NAME_COLS)),
                    "source_provider_cols": ",".join(_matching_cols(list(df.columns), PROVIDER_COLS)),
                    "identity_provenance_cols": ",".join(_matching_cols(list(df.columns), PROVENANCE_COLS)),
                    "fallback_identity_rows": f_count,
                    "ambiguous_identity_rows": a_count,
                    "notes": spec.notes,
                }
            )
    return rows


def _risk_for_match(label: str, line: str) -> tuple[str, str]:
    text = line.lower()
    if "player_id" in text or "game_id" in text:
        return "safe fallback", "Name/team helper appears near canonical ID usage; inspect but likely bridged."
    if "display" in text or "label" in text:
        return "acceptable display-only use", "Likely label/display handling."
    if "normalized" in label.lower() or "player_name" in label.lower():
        return "should be ID-first", "Name-based identity should be fallback/provenance only when player_id is unavailable."
    if "team/opponent" in label.lower():
        return "safe fallback", "Team-game fallback can be acceptable for game-level context when game_id unavailable."
    return "unsafe and should be refactored", "Identity risk requires manual review."


def build_join_risk_report() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    roots = [Path("backend/mlb/scripts"), Path("backend/mlb/shared")]
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            if path.name == "audit_mlb_canonical_identity.py":
                continue
            try:
                lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            except Exception:
                continue
            for idx, line in enumerate(lines, start=1):
                if not any(token in line for token in ("player_name", "player_name_norm", "player_name_key", "normalized_player", "team", "opponent", "merge", "join")):
                    continue
                window = "\n".join(lines[max(0, idx - 3) : min(len(lines), idx + 3)])
                for pattern, label in JOIN_PATTERNS:
                    if pattern.search(window):
                        classification, recommendation = _risk_for_match(label, window)
                        rows.append(
                            {
                                "path": path.as_posix(),
                                "line": idx,
                                "pattern": label,
                                "risk_classification": classification,
                                "snippet": line.strip()[:240],
                                "recommendation": recommendation,
                            }
                        )
                        break
    seen: set[tuple[str, int, str]] = set()
    deduped = []
    for row in rows:
        key = (row["path"], row["line"], row["pattern"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _md_table(rows: list[dict[str, Any]], columns: list[str], limit: int | None = None) -> list[str]:
    selected = rows[:limit] if limit else rows
    out = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    for row in selected:
        vals = [str(row.get(col, "")).replace("\n", " ") for col in columns]
        out.append("| " + " | ".join(vals) + " |")
    return out


def write_reports(out_dir: Path, coverage: list[dict[str, Any]], risks: list[dict[str, Any]]) -> None:
    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "mlb_identity_coverage_by_artifact.csv", coverage)
    _write_csv(out_dir / "mlb_identity_join_risk_report.csv", risks)

    total_artifacts = len([r for r in coverage if r.get("path") != "database table"])
    id_ready = sum(
        1
        for r in coverage
        if r.get("path") != "database table"
        and float(r.get("player_id_coverage_pct") or 0) > 0
        and float(r.get("game_id_coverage_pct") or 0) > 0
    )
    name_only = sum(1 for r in coverage if str(r.get("classification", "")).startswith("C."))
    high_risk = sum(1 for r in risks if r.get("risk_classification") in {"should be ID-first", "unsafe and should be refactored"})

    audit_lines = [
        "# MLB Canonical Identity Audit",
        "",
        f"- Generated UTC: `{generated}`",
        "- Scope: audit/doctrine only; no production joins changed.",
        "",
        "## Doctrine",
        "",
        "- IDs are identity.",
        "- Names are labels.",
        "- Aliases are bridges.",
        "- Fallback joins are diagnostics, not foundations.",
        "- Stored analytical rows should carry canonical IDs whenever available.",
        "",
        "## Summary",
        "",
        f"- Artifact/table rows inventoried: `{len(coverage)}`",
        f"- Local artifact samples with both player_id and game_id coverage: `{id_ready}` / `{total_artifacts}`",
        f"- Alias/name-only source samples: `{name_only}`",
        f"- Join-risk findings needing ID-first review: `{high_risk}`",
        "",
        "## Canonical Entity Map",
        "",
        "| entity | canonical identity | labels / aliases | fallback policy |",
        "|---|---|---|---|",
        "| Player | MLB `player_id` | name, normalized name, provider name | only when provider source lacks IDs; must carry provenance |",
        "| Game | MLB `game_id` | OddsAPI `event_id`, book event labels | bridge provider event to game by date + teams, then persist game_id |",
        "| Team | canonical team code, later official team_id where available | book/team abbreviations | normalize abbreviations; report unresolved mappings |",
        "| Market | `date + game_id + player_id + prop_type + side + line` | book/provider market labels | fallback `date + normalized_player + canonical team/opponent + prop_type + side + line` only with diagnostic flag |",
        "",
        "## Inventory Highlights",
        "",
    ]
    audit_lines.extend(
        _md_table(
            coverage,
            [
                "artifact",
                "classification",
                "rows_sampled",
                "player_id_coverage_pct",
                "game_id_coverage_pct",
                "canonical_team_coverage_pct",
                "provider_event_id_coverage_pct",
                "canonical_market_key_coverage_pct",
            ],
            limit=80,
        )
    )
    audit_lines.extend(
        [
            "",
            "## Where Numeric IDs Do Not Work Directly",
            "",
            "| case | why ID is unavailable | best bridge | when canonical ID can be attached | required diagnostic |",
            "|---|---|---|---|---|",
            "| OddsAPI batter/player prop markets | provider generally supplies player names, teams, event IDs, bookmaker labels, not MLB player_id | event_id/date/team + normalized player alias, then player_ids/player_team_by_game | after slate/game and player mapping are available | `identity_provenance`, `provider_event_id`, fallback flag |",
            "| Historical alternate market rows | historical event odds are provider-first and over-only in this research path | provider event_id + team/game bridge + alias resolver | during expanded universe build/hydration | source path, source provider, unresolved/ambiguous counts |",
            "| Public catalog / 8rain rows | external catalog may use names and display labels only | normalized name + team/opponent + market | once matched to local slate/player_id | catalog source, match method, confidence |",
            "| Call-ups/prospects absent from local player_ids | player is real but local mapping may lag | MLB StatsAPI probable roster/game context | after roster/player_ids refresh | unresolved prospect warning |",
            "| Provider event before MLB game_id known | books list event IDs before local schedule bridge exists | date + home/away/team labels | after schedule/slate refresh | provider_event_id retained until game_id bridge |",
            "| Future games before probable starter context | pitcher identity may be unnamed or change after morning run | game-level placeholder with source-not-ready status | after probable starter refresh | projected/untrusted reason, no blank team/game labels |",
            "",
            "## Join Risk Snapshot",
            "",
        ]
    )
    audit_lines.extend(_md_table(risks, ["path", "line", "pattern", "risk_classification", "recommendation"], limit=120))
    audit_lines.extend(
        [
            "",
            "## Immediate Recommendations",
            "",
            "1. Keep external-provider ingestion alias-tolerant, but persist provider IDs and match provenance.",
            "2. Require derived analytical artifacts to retain `player_id` and `game_id` whenever source data has them.",
            "3. Treat name/team joins as fallback diagnostics, not canonical identity, and make fallback use visible.",
            "4. Prioritize refactors where name-normalized joins feed performance, reconcile, expanded universe, or board eligibility.",
            "5. Add future health checks for ID coverage and fallback/ambiguous row counts before enforcing fail gates.",
            "",
        ]
    )
    (out_dir / "mlb_canonical_identity_audit.md").write_text("\n".join(audit_lines), encoding="utf-8")

    migration_lines = [
        "# MLB Identity Migration Plan",
        "",
        "## Phase 1 - Audit And Doctrine Only",
        "",
        "- Produce canonical identity doctrine.",
        "- Inventory identity fields and coverage by artifact.",
        "- Report risky name/team joins.",
        "- No join behavior changes.",
        "",
        "## Phase 2 - Canonical Identity Layer And Provenance Columns",
        "",
        "Foundation status: `started`.",
        "",
        "- Shared resolver package: `backend/mlb/identity/`.",
        "- Health command: `make mlb-identity-health`.",
        "- Production caller migration status: baseline only until a priority caller is migrated one at a time.",
        "- Add `canonical_player_id`, `canonical_game_id`, `canonical_team`, `provider_event_id`, `identity_provenance`, and `identity_confidence` where missing.",
        "- Preserve existing columns for compatibility.",
        "- Add explicit fallback/ambiguous flags.",
        "- Measure before/after coverage, fallback usage, ambiguous rows, unresolved rows, and bugs eliminated after each caller migration.",
        "",
        "## Phase 3 - Refactor Highest-Risk Joins To ID-First",
        "",
        "- Start with reconcile, expanded universe, review-board performance, BvP joins, and starter context.",
        "- Use name/team fallback only when IDs are unavailable and record the fallback.",
        "- Add row-level diagnostics before changing downstream semantics.",
        "",
        "## Phase 4 - Enforce Health Checks / Fail Gates",
        "",
        "- Gate durable artifacts on player_id/game_id coverage thresholds appropriate to source type.",
        "- Warn on provider-only rows that fail to graduate to canonical IDs after slate context is available.",
        "- Fail when derived artifacts drop canonical IDs that existed upstream.",
        "",
        "## First Refactor Candidates",
        "",
    ]
    grouped: dict[str, int] = {}
    for row in risks:
        grouped[row["risk_classification"]] = grouped.get(row["risk_classification"], 0) + 1
    for key, value in sorted(grouped.items()):
        migration_lines.append(f"- `{key}`: `{value}` findings")
    migration_lines.append("")
    (out_dir / "mlb_identity_migration_plan.md").write_text("\n".join(migration_lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit MLB canonical identity coverage and join risks.")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--max-files-per-spec", type=int, default=8)
    ap.add_argument("--max-rows", type=int, default=0, help="Optional row sample cap per artifact; 0 means full file.")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    max_rows = args.max_rows if args.max_rows > 0 else None
    coverage = build_coverage(out_dir, args.max_files_per_spec, max_rows)
    risks = build_join_risk_report()
    write_reports(out_dir, coverage, risks)
    print(
        json.dumps(
            {
                "out_dir": out_dir.as_posix(),
                "coverage_rows": len(coverage),
                "join_risk_rows": len(risks),
                "coverage_csv": (out_dir / "mlb_identity_coverage_by_artifact.csv").as_posix(),
                "risk_csv": (out_dir / "mlb_identity_join_risk_report.csv").as_posix(),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
