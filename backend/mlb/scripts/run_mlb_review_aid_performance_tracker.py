#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
WINDOWS = ("full_history", "last_30", "last_14", "last_7", "latest_completed_slate")

BOARD_CONFIGS = {
    "o15_layered": {
        "label": "Hits o1.5 Layered Board",
        "pattern": "hits_o15_layered_candidates_*.csv",
        "side": "over",
        "price_col": "market_price",
        "layer_col": "layer_label",
        "discovery_only": False,
    },
    "o15_watch": {
        "label": "Hits o1.5 Watch Candidates",
        "pattern": "hits_o15_watch_candidates_*.csv",
        "side": "over",
        "price_col": "market_price",
        "layer_col": "",
        "discovery_only": False,
    },
    "o15_alternate_discovery": {
        "label": "Hits o1.5 Alternate Discovery",
        "pattern": "hits_o15_alternate_discovery_*.csv",
        "side": "over",
        "price_col": "best_over_price",
        "layer_col": "alternate_layer",
        "discovery_only": True,
    },
    "u15_favorite_audit": {
        "label": "Hits u1.5 Favorite Audit",
        "pattern": "hits_u15_favorite_audit_*.csv",
        "side": "under",
        "price_col": "market_price",
        "layer_col": "layer_label",
        "discovery_only": False,
    },
}

LAYER_LABELS = {
    "layer_4_qc_d7_d15_starter": "Layer 4 QC+d7+d15+starter",
    "layer_3_d7_d15_starter_non_qc": "Layer 3 d7+d15+starter non-QC",
    "layer_2_d7_d15_no_favorable_starter": "Layer 2 d7+d15 no favorable starter",
    "layer_1_d7_hot_not_d15_consistent": "Layer 1 d7-only",
    "all_o15_other": "All other o1.5",
    "alternate_layer_a_d7_d15_starter": "Alternate Layer A d7+d15+starter",
    "alternate_layer_b_d7_d15": "Alternate Layer B d7+d15",
    "alternate_layer_c_d7_hot": "Alternate Layer C d7-only",
    "alternate_other": "Alternate other",
    "layer_4_qc_d7_d15_tough_starter": "Layer 4 QC+d7+d15+tough starter",
    "layer_3_d7_d15_tough_starter_non_qc": "Layer 3 d7+d15+tough starter non-QC",
    "layer_2_d7_d15_no_tough_starter": "Layer 2 d7+d15 no tough starter",
    "layer_1_d7_cold_not_d15_consistent": "Layer 1 d7 cold not d15 consistent",
    "all_u15_other": "All other u1.5",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    except Exception:
        return None


def _i(value: Any) -> int | None:
    number = _f(value)
    if number is None:
        return None
    return int(number)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _team(value: Any) -> str:
    text = _clean(value).upper()
    if text == "AZ":
        return "ARI"
    if text in {"ATH", "LV", "VIL"}:
        return "OAK"
    return text


def _norm_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return " ".join(text.split())


def _date_from_filename(path: Path) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    return match.group(1) if match else ""


def _line_key(value: Any) -> str:
    line = _f(value)
    return f"{line:.1f}" if line is not None else ""


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_csv_with_fields(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _parse_bool(value: Any) -> bool | None:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "t", "yes", "y", "win", "won"}:
        return True
    if text in {"0", "false", "f", "no", "n", "loss", "lost"}:
        return False
    return None


def _american_units(price: float | None, won: bool | None, push: bool) -> float | None:
    if won is None:
        return None
    if push:
        return 0.0
    if price is None:
        return 1.0 if won else -1.0
    if won:
        return price / 100.0 if price > 0 else 100.0 / abs(price)
    return -1.0


def _window_labels(date_text: str, latest: str) -> list[str]:
    labels = ["full_history"]
    try:
        d = datetime.strptime(date_text, "%Y-%m-%d").date()
        latest_d = datetime.strptime(latest, "%Y-%m-%d").date()
    except Exception:
        return labels
    delta = (latest_d - d).days
    if delta < 0:
        return labels
    if delta <= 29:
        labels.append("last_30")
    if delta <= 13:
        labels.append("last_14")
    if delta <= 6:
        labels.append("last_7")
    if delta == 0:
        labels.append("latest_completed_slate")
    return labels


def _load_reconcile_rows(reconcile_root: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    dates: list[str] = []
    for path in sorted(reconcile_root.glob("20??-??-??/reconcile_rows.csv")):
        date_text = _date_from_filename(path.parent)
        if not date_text:
            continue
        loaded = _read_csv(path)
        if loaded:
            dates.append(date_text)
            rows.extend(loaded)
    return rows, sorted(set(dates))


def _load_qc_flags(lanes_root: Path) -> tuple[set[tuple[str, str, str, str]], set[tuple[str, str, str, str]]]:
    by_id: set[tuple[str, str, str, str]] = set()
    by_name: set[tuple[str, str, str, str]] = set()
    for path in sorted((lanes_root / "today").glob("20??-??-??/quick_card_hits_*.csv")):
        date_text = _date_from_filename(path.parent)
        if not date_text:
            continue
        for row in _read_csv(path):
            if _clean(row.get("prop_type")).lower() != "hits":
                continue
            line = _line_key(row.get("line"))
            side = _clean(row.get("side")).lower()
            if line != "1.5" or side not in {"over", "under"}:
                continue
            player_id = _i(row.get("player_id"))
            if player_id is not None:
                by_id.add((date_text, str(player_id), line, side))
            name = _norm_name(row.get("player_name") or row.get("player"))
            if name:
                by_name.add((date_text, name, line, side))
    return by_id, by_name


def _is_qc_candidate(row: dict[str, Any], qc_by_id: set[tuple[str, str, str, str]], qc_by_name: set[tuple[str, str, str, str]]) -> bool:
    date_text = str(row.get("board_date") or row.get("date") or "")[:10]
    line = _line_key(row.get("line") or 1.5)
    side = _clean(row.get("side")).lower()
    player_id = _i(row.get("player_id"))
    if player_id is not None and (date_text, str(player_id), line, side) in qc_by_id:
        return True
    name = _norm_name(row.get("player_name") or row.get("player"))
    return bool(name and (date_text, name, line, side) in qc_by_name)


def _derive_u15_layer(row: dict[str, Any], qc_by_id: set[tuple[str, str, str, str]], qc_by_name: set[tuple[str, str, str, str]]) -> tuple[str, bool]:
    d7 = _f(row.get("d7_hits_rate"))
    d15 = _f(row.get("d15_hits_rate"))
    starter = _f(row.get("starter_expected_hits_allowed"))
    d7_cold = d7 is not None and d7 < 1.0
    d15_cold = d15 is not None and d15 < 1.0
    tough_starter = starter is not None and starter < 4.5
    qc_candidate = _is_qc_candidate(row, qc_by_id, qc_by_name)
    if qc_candidate and d7_cold and d15_cold and tough_starter:
        return "layer_4_qc_d7_d15_tough_starter", True
    if d7_cold and d15_cold and tough_starter:
        return "layer_3_d7_d15_tough_starter_non_qc", False
    if d7_cold and d15_cold:
        return "layer_2_d7_d15_no_tough_starter", False
    if d7_cold and not d15_cold:
        return "layer_1_d7_cold_not_d15_consistent", False
    return "all_u15_other", False


def _build_reconcile_indexes(rows: list[dict[str, Any]]) -> dict[str, dict[tuple[Any, ...], dict[str, Any]]]:
    indexes: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {
        "player_id": {},
        "name_team": {},
        "name": {},
        "date": {},
        "player_id_any_line": {},
        "name_team_any_line": {},
        "name_any_line": {},
    }
    name_counts: Counter[tuple[str, str, str, str]] = Counter()
    for row in rows:
        if _clean(row.get("prop_type")).lower() != "hits":
            continue
        date_text = str(row.get("game_date") or row.get("slate_date") or "")[:10]
        line = _line_key(row.get("line"))
        if not date_text or line != "1.5":
            continue
        pid = _i(row.get("player_id"))
        team = _team(row.get("team"))
        opp = _team(row.get("opponent"))
        name = _norm_name(row.get("player_name") or row.get("market_player_name"))
        if pid is not None:
            indexes["player_id"][(date_text, str(pid), line)] = row
            indexes["player_id_any_line"].setdefault((date_text, str(pid)), row)
        if name and team and opp:
            indexes["name_team"][(date_text, name, line, team, opp)] = row
            indexes["name_team_any_line"].setdefault((date_text, name, team, opp), row)
        if name:
            name_counts[(date_text, name, line, "")] += 1
            indexes["name_any_line"].setdefault((date_text, name), row)
        indexes["date"].setdefault((date_text,), row)
    for row in rows:
        if _clean(row.get("prop_type")).lower() != "hits":
            continue
        date_text = str(row.get("game_date") or row.get("slate_date") or "")[:10]
        line = _line_key(row.get("line"))
        name = _norm_name(row.get("player_name") or row.get("market_player_name"))
        if date_text and line == "1.5" and name and name_counts[(date_text, name, line, "")] == 1:
            indexes["name"][(date_text, name, line)] = row
    return indexes


def _unmatched_reason(
    *,
    date_text: str,
    line: str,
    player_id: int | None,
    name: str,
    team: str,
    opp: str,
    indexes: dict[str, dict[tuple[Any, ...], dict[str, Any]]],
) -> str:
    if (date_text,) not in indexes.get("date", {}):
        return "no_reconcile_for_board_date"
    if player_id is not None and (date_text, str(player_id)) in indexes.get("player_id_any_line", {}):
        return "player_found_different_line_or_missing_1_5"
    if name and team and opp and (date_text, name, team, opp) in indexes.get("name_team_any_line", {}):
        return "name_team_found_different_line_or_missing_1_5"
    if name and (date_text, name) in indexes.get("name_any_line", {}):
        return "name_found_team_or_line_mismatch"
    if name:
        return "player_not_found_in_reconcile"
    return "missing_player_identity"


def _load_board_rows(review_aids_dir: Path, lanes_root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    qc_by_id, qc_by_name = _load_qc_flags(lanes_root)
    for board_key, config in BOARD_CONFIGS.items():
        for path in sorted(review_aids_dir.glob(str(config["pattern"]))):
            date_text = _date_from_filename(path)
            if not date_text:
                continue
            for raw in _read_csv(path):
                row = dict(raw)
                row_date = str(row.get("date") or date_text)[:10]
                player_name = row.get("player_name") or row.get("player") or ""
                side = str(row.get("side") or config["side"]).strip().lower()
                price_col = str(config["price_col"])
                layer_value = str(row.get(str(config["layer_col"])) or "all").strip() if config["layer_col"] else "all"
                if board_key == "u15_favorite_audit" and layer_value in {"", "all"}:
                    row["board_date"] = row_date
                    row["side"] = side
                    derived_layer, derived_watch = _derive_u15_layer(row, qc_by_id, qc_by_name)
                    layer_value = derived_layer
                    row["watch_candidate"] = derived_watch
                    row["qc_candidate"] = _is_qc_candidate(row, qc_by_id, qc_by_name)
                row.update(
                    {
                        "board": board_key,
                        "board_label": config["label"],
                        "board_source_file": _rel(path),
                        "board_date": row_date,
                        "player_name": player_name,
                        "prop_type": "hits",
                        "side": side,
                        "line": _line_key(row.get("line") or 1.5),
                        "board_price": _f(row.get(price_col)),
                        "layer_value": layer_value,
                        "layer_label_display": LAYER_LABELS.get(
                            layer_value,
                            layer_value,
                        ),
                        "discovery_only": bool(config["discovery_only"]),
                    }
                )
                out.append(row)
    return out


def _join_board_rows(
    board_rows: list[dict[str, Any]],
    indexes: dict[str, dict[tuple[Any, ...], dict[str, Any]]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in board_rows:
        date_text = str(row.get("board_date") or "")[:10]
        line = _line_key(row.get("line"))
        side = str(row.get("side") or "").strip().lower()
        player_id = _i(row.get("player_id"))
        name = _norm_name(row.get("player_name") or row.get("player"))
        team = _team(row.get("team"))
        opp = _team(row.get("opponent"))
        match: dict[str, Any] | None = None
        join_key = ""
        if player_id is not None:
            match = indexes["player_id"].get((date_text, str(player_id), line))
            if match:
                join_key = "date+player_id+line"
        if match is None and name and team and opp:
            match = indexes["name_team"].get((date_text, name, line, team, opp))
            if match:
                join_key = "date+player_name+team+opponent+line"
        if match is None and name:
            match = indexes["name"].get((date_text, name, line))
            if match:
                join_key = "date+unique_player_name+line"

        result = dict(row)
        result["join_key_used"] = join_key
        result["join_status"] = "matched" if match is not None else "unmatched"
        if match is None:
            reason = _unmatched_reason(
                date_text=date_text,
                line=line,
                player_id=player_id,
                name=name,
                team=team,
                opp=opp,
                indexes=indexes,
            )
            result.update(
                {
                    "resolved": False,
                    "win": "",
                    "loss": "",
                    "push": "",
                    "units": "",
                    "actual_value": "",
                    "reconcile_player_id": "",
                    "reconcile_player_name": "",
                    "reconcile_source_file": "",
                    "unmatched_reason": reason,
                }
            )
            out.append(result)
            continue

        actual_value = _f(match.get("actual_value"))
        line_v = _f(row.get("line"))
        push = bool(actual_value is not None and line_v is not None and abs(actual_value - line_v) < 1e-9)
        outcome_col = "actual_over_outcome" if side == "over" else "actual_under_outcome"
        won = _parse_bool(match.get(outcome_col))
        if push:
            won = False
        price = _f(row.get("board_price"))
        if price is None:
            price = _f(match.get("price_over_american" if side == "over" else "price_under_american"))
        units = _american_units(price, won, push)
        result.update(
            {
                "resolved": won is not None or push,
                "win": bool(won) if won is not None and not push else False,
                "loss": bool(won is False and not push),
                "push": push,
                "units": units if units is not None else "",
                "actual_value": actual_value if actual_value is not None else "",
                "reconcile_player_id": match.get("player_id") or "",
                "reconcile_player_name": match.get("player_name") or "",
                "reconcile_source_file": _rel(ROOT / "artifacts/analysis/mlb/execution_vs_model" / date_text / "reconcile_rows.csv"),
            }
        )
        out.append(result)
    return out


def _qc_score_bucket(value: Any) -> str:
    score = _f(value)
    if score is None:
        return "missing"
    if score < 0.50:
        return "<0.50"
    if score < 0.55:
        return "0.50-0.55"
    if score < 0.60:
        return "0.55-0.60"
    if score < 0.65:
        return "0.60-0.65"
    return "0.65+"


def _aggregate(rows: list[dict[str, Any]], *, group: dict[str, Any], latest: str) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved") is True]
    wins = sum(1 for row in resolved if row.get("win") is True)
    losses = sum(1 for row in resolved if row.get("loss") is True)
    pushes = sum(1 for row in resolved if row.get("push") is True)
    graded = wins + losses + pushes
    units = sum((_f(row.get("units")) or 0.0) for row in resolved)
    prices = [_f(row.get("board_price")) for row in rows if _f(row.get("board_price")) is not None]
    d7 = [_f(row.get("d7_hits_rate")) for row in rows if _f(row.get("d7_hits_rate")) is not None]
    d15 = [_f(row.get("d15_hits_rate")) for row in rows if _f(row.get("d15_hits_rate")) is not None]
    starter = [
        _f(row.get("starter_expected_hits_allowed"))
        for row in rows
        if _f(row.get("starter_expected_hits_allowed")) is not None
    ]

    def avg(values: list[float | None]) -> float | None:
        nums = [float(v) for v in values if v is not None]
        return sum(nums) / len(nums) if nums else None

    return {
        **group,
        "rows": len(rows),
        "resolved": graded,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / graded if graded else None,
        "units": units,
        "avg_odds": avg(prices),
        "avg_d7": avg(d7),
        "avg_d15": avg(d15),
        "avg_starter_expected_hits_allowed": avg(starter),
        "latest_completed_slate": latest,
    }


def _build_aggregates(joined: list[dict[str, Any]], latest: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_board: list[dict[str, Any]] = []
    by_layer: list[dict[str, Any]] = []
    by_tier: list[dict[str, Any]] = []

    for window in WINDOWS:
        window_rows = [
            row
            for row in joined
            if window in _window_labels(str(row.get("board_date") or "")[:10], latest)
            and str(row.get("board_date") or "")[:10] <= latest
        ]
        for board in sorted({str(row.get("board") or "") for row in window_rows}):
            rows = [row for row in window_rows if str(row.get("board") or "") == board]
            if rows:
                by_board.append(
                    _aggregate(
                        rows,
                        group={
                            "window": window,
                            "board": board,
                            "board_label": rows[0].get("board_label") or board,
                            "discovery_only": rows[0].get("discovery_only"),
                        },
                        latest=latest,
                    )
                )
            for layer in sorted({str(row.get("layer_value") or "all") for row in rows}):
                layer_rows = [row for row in rows if str(row.get("layer_value") or "all") == layer]
                if layer_rows:
                    by_layer.append(
                        _aggregate(
                            layer_rows,
                            group={
                                "window": window,
                                "board": board,
                                "layer": layer,
                                "layer_label": LAYER_LABELS.get(layer, layer),
                                "discovery_only": layer_rows[0].get("discovery_only"),
                            },
                            latest=latest,
                        )
                    )
            for tier_col in ("combined_tier", "hitter_tier", "pitcher_tier"):
                for tier in sorted({str(row.get(tier_col) or "missing") for row in rows}):
                    tier_rows = [row for row in rows if str(row.get(tier_col) or "missing") == tier]
                    if tier_rows:
                        by_tier.append(
                            _aggregate(
                                tier_rows,
                                group={
                                    "window": window,
                                    "board": board,
                                    "tier_type": tier_col,
                                    "tier": tier,
                                    "discovery_only": tier_rows[0].get("discovery_only"),
                                },
                                latest=latest,
                            )
                        )
            for bucket in sorted({_qc_score_bucket(row.get("qc_score")) for row in rows}):
                bucket_rows = [row for row in rows if _qc_score_bucket(row.get("qc_score")) == bucket]
                if bucket_rows and bucket != "missing":
                    by_tier.append(
                        _aggregate(
                            bucket_rows,
                            group={
                                "window": window,
                                "board": board,
                                "tier_type": "qc_score_bucket",
                                "tier": bucket,
                                "discovery_only": bucket_rows[0].get("discovery_only"),
                            },
                            latest=latest,
                        )
                    )
            for watch_value in sorted({str(row.get("watch_candidate") or "False") for row in rows}):
                watch_rows = [row for row in rows if str(row.get("watch_candidate") or "False") == watch_value]
                if watch_rows and any("watch_candidate" in row for row in rows):
                    by_tier.append(
                        _aggregate(
                            watch_rows,
                            group={
                                "window": window,
                                "board": board,
                                "tier_type": "watch_candidate",
                                "tier": watch_value,
                                "discovery_only": watch_rows[0].get("discovery_only"),
                            },
                            latest=latest,
                        )
                    )
    return by_board, by_layer, by_tier


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100.0:.2f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.{digits}f}"


def _row_for(rows: list[dict[str, Any]], **conds: str) -> dict[str, Any]:
    for row in rows:
        if all(str(row.get(key) or "") == str(value) for key, value in conds.items()):
            return row
    return {}


def _write_report(path: Path, summary: dict[str, Any], by_board: list[dict[str, Any]], by_layer: list[dict[str, Any]], by_tier: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# MLB Review Aid Performance")
    lines.append("")
    lines.append(f"- Generated at: `{summary.get('generated_at')}`")
    lines.append(f"- Status: `{summary.get('status')}`")
    lines.append(f"- Latest completed slate: `{summary.get('latest_completed_slate') or 'n/a'}`")
    lines.append(f"- Board rows loaded: `{summary.get('board_rows_loaded')}`")
    lines.append(f"- Rows matched to reconcile: `{summary.get('matched_rows')}`")
    lines.append(f"- Join policy: `{summary.get('join_policy')}`")
    if summary.get("status") != "ok":
        lines.append(f"- Detail: {summary.get('status_detail') or 'n/a'}")
    lines.append("")
    lines.append("## Board Summary")
    lines.append("")
    lines.append("| board | window | rows | resolved | W-L-P | WR | ROI | units | avg odds |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in by_board:
        if row.get("window") not in {"full_history", "last_30", "last_14", "last_7", "latest_completed_slate"}:
            continue
        lines.append(
            f"| {row.get('board_label') or row.get('board')} | {row.get('window')} | `{row.get('rows')}` | "
            f"`{row.get('resolved')}` | `{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | "
            f"`{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt_num(row.get('units'))}` | "
            f"`{_fmt_num(row.get('avg_odds'))}` |"
        )
    lines.append("")
    lines.append("## Requested Daily Callouts")
    lines.append("")
    callouts = [
        ("o1.5 Layer 4", _row_for(by_layer, window="latest_completed_slate", board="o15_layered", layer="layer_4_qc_d7_d15_starter")),
        ("o1.5 Layer 3", _row_for(by_layer, window="latest_completed_slate", board="o15_layered", layer="layer_3_d7_d15_starter_non_qc")),
        ("o1.5 alternate Layer A", _row_for(by_layer, window="latest_completed_slate", board="o15_alternate_discovery", layer="alternate_layer_a_d7_d15_starter")),
        ("u1.5 Layer 4", _row_for(by_layer, window="latest_completed_slate", board="u15_favorite_audit", layer="layer_4_qc_d7_d15_tough_starter")),
        ("u1.5 Layer 3", _row_for(by_layer, window="latest_completed_slate", board="u15_favorite_audit", layer="layer_3_d7_d15_tough_starter_non_qc")),
        ("u1.5 Layer 2", _row_for(by_layer, window="latest_completed_slate", board="u15_favorite_audit", layer="layer_2_d7_d15_no_tough_starter")),
        ("u1.5 A/A", _row_for(by_tier, window="latest_completed_slate", board="u15_favorite_audit", tier_type="combined_tier", tier="A/A")),
    ]
    for label, row in callouts:
        if row:
            lines.append(
                f"- {label}: `{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` "
                f"ROI `{_fmt_pct(row.get('roi'))}` over `{row.get('resolved')}` resolved rows."
            )
        else:
            lines.append(f"- {label}: no rows for latest completed slate.")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Alternate discovery is discovery-only / Over-only / not production-safe.")
    lines.append("- Rows are joined to execution reconcile by date+player_id+line when possible, then date+player name+team+opponent+line, then unique date+player name+line.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_u15_parity_report(
    path: Path,
    *,
    summary: dict[str, Any],
    joined: list[dict[str, Any]],
    by_board: list[dict[str, Any]],
    by_layer: list[dict[str, Any]],
    by_tier: list[dict[str, Any]],
    latest: str,
    unmatched_path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    u15_rows = [row for row in joined if row.get("board") == "u15_favorite_audit"]
    u15_latest = [row for row in u15_rows if str(row.get("board_date") or "")[:10] == latest]
    matched = [row for row in u15_rows if row.get("join_status") == "matched"]
    resolved = [row for row in u15_rows if row.get("resolved") is True]
    layer_counts = Counter(str(row.get("layer_value") or "all") for row in u15_rows)
    latest_layer_counts = Counter(str(row.get("layer_value") or "all") for row in u15_latest)
    unmatched = [row for row in u15_rows if row.get("join_status") != "matched"]

    def perf(label: str, row: dict[str, Any]) -> str:
        if not row:
            return f"- {label}: no rows."
        return (
            f"- {label}: rows `{row.get('rows')}`, resolved `{row.get('resolved')}`, "
            f"W-L-P `{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}`, "
            f"WR `{_fmt_pct(row.get('wr'))}`, ROI `{_fmt_pct(row.get('roi'))}`."
        )

    lines = [
        "# u1.5 Review Aid History Parity Report",
        "",
        f"- Generated at: `{summary.get('generated_at')}`",
        "- Scope: reporting/history/review-aid tracking only; no selector, upload, threshold, grading, or matching changes.",
        "",
        "## Daily Generation",
        "",
        "- Current-slate producer: `make mlb-hits-u15-favorite-audit DATE=<DATE>`.",
        "- Daily upload-prep path: `mlb-daily-upload-prep` runs `mlb-hits-u15-favorite-audit` for `MLB_UPLOAD_PREP_DATE`.",
        "- Daily performance path: `mlb-daily-reconcile` runs `mlb-review-aid-performance` after completed-slate reconcile.",
        "",
        "## Artifact Discovery",
        "",
        "- History pattern: `artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_*.csv`.",
        "- Tracker uses actual daily board CSV artifacts, not reconstructed u1.5 logic.",
        "- Join policy: date + player_id + line when available; fallback date + normalized player name + team + opponent + line; fallback unique date + normalized player name + line. The board side is preserved as `under` when selecting the reconcile outcome.",
        "",
        "## Latest Run Counts",
        "",
        f"- Latest completed slate: `{latest or 'n/a'}`",
        f"- u1.5 board rows loaded: `{len(u15_rows)}`",
        f"- u1.5 matched rows: `{len(matched)}`",
        f"- u1.5 resolved rows: `{len(resolved)}`",
        f"- u1.5 unmatched rows: `{len(unmatched)}`",
        f"- Latest completed-slate u1.5 rows: `{len(u15_latest)}`",
        f"- Unmatched output: `{_rel(unmatched_path)}`",
        "",
        "## Layer Counts",
        "",
    ]
    for layer in [
        "layer_4_qc_d7_d15_tough_starter",
        "layer_3_d7_d15_tough_starter_non_qc",
        "layer_2_d7_d15_no_tough_starter",
        "layer_1_d7_cold_not_d15_consistent",
        "all_u15_other",
    ]:
        lines.append(
            f"- `{layer}`: full history `{layer_counts.get(layer, 0)}`, latest slate `{latest_layer_counts.get(layer, 0)}`"
        )
    lines.extend(["", "## Performance Callouts", ""])
    for label, row in [
        ("Layer 4 latest slate", _row_for(by_layer, window="latest_completed_slate", board="u15_favorite_audit", layer="layer_4_qc_d7_d15_tough_starter")),
        ("Layer 3 latest slate", _row_for(by_layer, window="latest_completed_slate", board="u15_favorite_audit", layer="layer_3_d7_d15_tough_starter_non_qc")),
        ("Layer 2 latest slate", _row_for(by_layer, window="latest_completed_slate", board="u15_favorite_audit", layer="layer_2_d7_d15_no_tough_starter")),
        ("A/A latest slate", _row_for(by_tier, window="latest_completed_slate", board="u15_favorite_audit", tier_type="combined_tier", tier="A/A")),
        ("Layer 4 full history", _row_for(by_layer, window="full_history", board="u15_favorite_audit", layer="layer_4_qc_d7_d15_tough_starter")),
        ("Layer 3 full history", _row_for(by_layer, window="full_history", board="u15_favorite_audit", layer="layer_3_d7_d15_tough_starter_non_qc")),
        ("Layer 2 full history", _row_for(by_layer, window="full_history", board="u15_favorite_audit", layer="layer_2_d7_d15_no_tough_starter")),
        ("A/A full history", _row_for(by_tier, window="full_history", board="u15_favorite_audit", tier_type="combined_tier", tier="A/A")),
    ]:
        lines.append(perf(label, row))
    lines.extend(["", "## Join Failures", ""])
    if unmatched:
        by_reason = Counter(str(row.get("unmatched_reason") or row.get("join_status") or "unmatched") for row in unmatched)
        for reason, count in sorted(by_reason.items()):
            lines.append(f"- `{reason}`: `{count}`")
    else:
        lines.append("- None.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Track outcome-backed performance for MLB review-aid board artifacts.")
    ap.add_argument("--review-aids-dir", default="artifacts/analysis/mlb/review_aids")
    ap.add_argument("--reconcile-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--lanes-root", default="backend/mlb/exports/model_v2/lanes")
    ap.add_argument("--completed-slate-date", default="")
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/review_aids/performance")
    args = ap.parse_args()

    review_dir = ROOT / args.review_aids_dir
    reconcile_root = ROOT / args.reconcile_root
    out_dir = ROOT / args.out_dir
    summary_path = out_dir / "review_aid_performance_summary.json"
    by_board_path = out_dir / "review_aid_performance_by_board.csv"
    by_layer_path = out_dir / "review_aid_performance_by_layer.csv"
    by_tier_path = out_dir / "review_aid_performance_by_tier.csv"
    latest_path = out_dir / "review_aid_performance_latest_slate.csv"
    report_path = out_dir / "review_aid_performance_report.md"
    u15_parity_path = out_dir / "u15_review_aid_history_parity_report.md"
    u15_unmatched_path = out_dir / "u15_review_aid_unmatched_rows.csv"

    reconcile_rows, reconcile_dates = _load_reconcile_rows(reconcile_root)
    target_completed = str(args.completed_slate_date or "").strip()[:10]
    latest = target_completed or (reconcile_dates[-1] if reconcile_dates else "")
    target_reconcile = reconcile_root / latest / "reconcile_rows.csv" if latest else Path("")

    board_rows = _load_board_rows(review_dir, ROOT / args.lanes_root)
    status = "ok"
    detail = ""
    if not reconcile_dates:
        status = "source_not_ready"
        detail = f"no reconcile_rows.csv files found under {_rel(reconcile_root)}"
    elif target_completed and target_completed not in reconcile_dates:
        status = "source_not_ready"
        detail = f"missing completed-slate reconcile: {_rel(target_reconcile)}"

    indexes = _build_reconcile_indexes(reconcile_rows)
    joined = _join_board_rows(board_rows, indexes)
    latest_rows = [row for row in joined if str(row.get("board_date") or "")[:10] == latest]
    by_board, by_layer, by_tier = _build_aggregates(joined, latest) if latest else ([], [], [])

    summary = {
        "generated_at": _now(),
        "status": status,
        "status_detail": detail,
        "latest_completed_slate": latest,
        "reconcile_root": _rel(reconcile_root),
        "target_reconcile_rows_csv": _rel(target_reconcile) if latest else "",
        "target_reconcile_exists": bool(target_reconcile.exists()) if latest else False,
        "review_aids_dir": _rel(review_dir),
        "board_rows_loaded": len(board_rows),
        "matched_rows": sum(1 for row in joined if row.get("join_status") == "matched"),
        "resolved_rows": sum(1 for row in joined if row.get("resolved") is True),
        "join_policy": "date+player_id+line, then date+normalized_player_name+team+opponent+line, then unique date+normalized_player_name+line",
        "boards_tracked": list(BOARD_CONFIGS),
        "outputs": {
            "by_board_csv": _rel(by_board_path),
            "by_layer_csv": _rel(by_layer_path),
            "by_tier_csv": _rel(by_tier_path),
            "latest_slate_csv": _rel(latest_path),
            "report_md": _rel(report_path),
            "u15_parity_report_md": _rel(u15_parity_path),
            "u15_unmatched_rows_csv": _rel(u15_unmatched_path),
        },
        "callouts": {
            "o15_layer_4_latest": _row_for(by_layer, window="latest_completed_slate", board="o15_layered", layer="layer_4_qc_d7_d15_starter"),
            "o15_layer_3_latest": _row_for(by_layer, window="latest_completed_slate", board="o15_layered", layer="layer_3_d7_d15_starter_non_qc"),
            "o15_alternate_layer_a_latest": _row_for(by_layer, window="latest_completed_slate", board="o15_alternate_discovery", layer="alternate_layer_a_d7_d15_starter"),
            "u15_layer_4_latest": _row_for(by_layer, window="latest_completed_slate", board="u15_favorite_audit", layer="layer_4_qc_d7_d15_tough_starter"),
            "u15_layer_3_latest": _row_for(by_layer, window="latest_completed_slate", board="u15_favorite_audit", layer="layer_3_d7_d15_tough_starter_non_qc"),
            "u15_layer_2_latest": _row_for(by_layer, window="latest_completed_slate", board="u15_favorite_audit", layer="layer_2_d7_d15_no_tough_starter"),
            "u15_aa_latest": _row_for(by_tier, window="latest_completed_slate", board="u15_favorite_audit", tier_type="combined_tier", tier="A/A"),
        },
    }
    u15_unmatched = [row for row in joined if row.get("board") == "u15_favorite_audit" and row.get("join_status") != "matched"]

    _write_json(summary_path, summary)
    _write_csv(by_board_path, by_board)
    _write_csv(by_layer_path, by_layer)
    _write_csv(by_tier_path, by_tier)
    _write_csv(latest_path, latest_rows)
    _write_csv_with_fields(
        u15_unmatched_path,
        u15_unmatched,
        [
            "board_date",
            "player_name",
            "player_id",
            "team",
            "opponent",
            "side",
            "line",
            "layer_value",
            "combined_tier",
            "board_source_file",
            "join_status",
            "unmatched_reason",
            "join_key_used",
        ],
    )
    _write_report(report_path, summary, by_board, by_layer, by_tier)
    _write_u15_parity_report(
        u15_parity_path,
        summary=summary,
        joined=joined,
        by_board=by_board,
        by_layer=by_layer,
        by_tier=by_tier,
        latest=latest,
        unmatched_path=u15_unmatched_path,
    )

    print(f"review_aid_performance_status={status}")
    print(f"latest_completed_slate={latest or 'n/a'}")
    print(f"board_rows_loaded={len(board_rows)}")
    print(f"matched_rows={summary['matched_rows']}")
    print(f"resolved_rows={summary['resolved_rows']}")
    print(f"summary_json={_rel(summary_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
