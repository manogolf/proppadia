#!/usr/bin/env python3
"""Bounded official feed/live acquisition and encounter ledger expansion.

This utility is scoped to the frozen MLB Hits 1.5 multi-hit benchmark only. It
freezes the exact missing-game request manifest, acquires only those official
MLB StatsAPI feed/live responses when explicitly run with acquisition enabled,
and reuses the proven canonical encounter parser from the foundation pilot.

No OddsAPI calls, DB writes, model fitting, threshold optimization, production
mutations, uploads, selectors, workspace, Quick Card, or LaunchAgent changes are
performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.mlb.scripts import run_mlb_batter_pitcher_encounter_ledger_pilot as pilot

AUDIT_DATE = "2026-07-17"
ROOT = pilot.ROOT
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17"
RAW_DIR_NAME = "raw_official_mlb"
BENCH = pilot.BENCH
HITTER_BASE = ROOT / "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
PRIOR_RAW = pilot.RAW_FEEDS
ENDPOINT = "https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def norm_id(value: Any) -> str:
    return pilot.norm_id(value)


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def feed_identity(path: Path) -> dict[str, Any]:
    data = load_json(path)
    if not data:
        return {"game_id": "", "game_date": "", "game_status": "MALFORMED", "path": path}
    ident = pilot.feed_game_identity(path, data)
    ident["path"] = path
    return ident


def local_feed_map(*roots: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.glob("mlb_statsapi_feed_live_game_*.json")):
            ident = feed_identity(path)
            gid = norm_id(ident.get("game_id"))
            if not gid:
                continue
            if gid not in out:
                out[gid] = path
    return out


def team_summary(hitter_base: pd.DataFrame) -> pd.DataFrame:
    if hitter_base.empty:
        return pd.DataFrame(columns=["game_id", "teams"])
    h = hitter_base.copy()
    h["game_id"] = h["game_id"].map(norm_id)
    rows = []
    for gid, g in h.groupby("game_id", dropna=False):
        teams = sorted(set(str(x) for x in g["team"].dropna().unique()) | set(str(x) for x in g["opponent"].dropna().unique()))
        teams = [x for x in teams if x and x.lower() != "nan"]
        rows.append({"game_id": gid, "teams": "/".join(teams[:2])})
    return pd.DataFrame(rows)


def create_or_load_manifest(out_dir: Path, raw_dir: Path) -> pd.DataFrame:
    manifest_path = out_dir / "frozen_missing_game_manifest_2026-07-17.csv"
    if manifest_path.exists():
        return read_csv(manifest_path)

    bench = read_csv(BENCH)
    hitter = read_csv(HITTER_BASE)
    bench["game_id"] = bench["game_id"].map(norm_id)
    bench["slate_date"] = bench["slate_date"].astype(str)
    teams = team_summary(hitter)
    local = local_feed_map(PRIOR_RAW)
    local_final = set()
    for gid, path in local.items():
        ident = feed_identity(path)
        if ident.get("game_status") == "Final":
            local_final.add(gid)

    rows = []
    grouped = bench.groupby(["slate_date", "game_id"], dropna=False).size().reset_index(name="benchmark_row_count")
    grouped = grouped.merge(teams, on="game_id", how="left")
    for _, r in grouped.sort_values(["slate_date", "game_id"]).iterrows():
        gid = norm_id(r["game_id"])
        existing = local.get(gid)
        acquisition_required = gid not in local_final
        rows.append({
            "game_date": r["slate_date"],
            "game_id": gid,
            "teams": r.get("teams", ""),
            "benchmark_row_count": int(r["benchmark_row_count"]),
            "existing_local_feed_status": "FINAL_CERTIFIED_LOCAL" if gid in local_final else ("LOCAL_NONFINAL_OR_UNUSABLE" if existing else "MISSING"),
            "existing_local_feed_path": pilot.rel(existing) if existing else "",
            "acquisition_required": acquisition_required,
            "endpoint": ENDPOINT.format(game_id=gid),
            "expected_raw_output_path": pilot.rel(raw_dir / f"mlb_statsapi_feed_live_game_{gid}.json"),
        })
    manifest = pd.DataFrame(rows)
    if int(manifest["acquisition_required"].sum()) > 568:
        raise RuntimeError("request population exceeds expected frozen missing-game count")
    write_csv(manifest, manifest_path)
    manifest_hash = sha256(manifest_path)
    write_json({
        "created_at_utc": now_utc(),
        "manifest_path": pilot.rel(manifest_path),
        "manifest_sha256": manifest_hash,
        "benchmark_games": int(manifest["game_id"].nunique()),
        "acquisition_required_games": int(manifest["acquisition_required"].sum()),
        "policy": "frozen_before_acquisition",
    }, out_dir / "frozen_missing_game_manifest_sha256_2026-07-17.json")
    return manifest


def copy_existing_benchmark_feeds(manifest: pd.DataFrame, raw_dir: Path) -> pd.DataFrame:
    raw_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    local = local_feed_map(PRIOR_RAW)
    for _, r in manifest.iterrows():
        gid = norm_id(r["game_id"])
        if bool(r["acquisition_required"]):
            continue
        src = local.get(gid)
        if not src or not src.exists():
            continue
        dst = raw_dir / f"mlb_statsapi_feed_live_game_{gid}.json"
        if not dst.exists():
            shutil.copy2(src, dst)
        rows.append({
            "game_id": gid,
            "source": pilot.rel(src),
            "destination": pilot.rel(dst),
            "source_sha256": sha256(src),
            "destination_sha256": sha256(dst),
            "copy_status": "COPIED_OR_ALREADY_PRESENT",
        })
    return pd.DataFrame(rows)


def fetch_one(game_id: str, out_path: Path, retry: bool) -> dict[str, Any]:
    url = ENDPOINT.format(game_id=game_id)
    attempts = 2 if retry else 1
    last: dict[str, Any] = {}
    for attempt in range(1, attempts + 1):
        ts = now_utc()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "proppadia-encounter-ledger-research/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read()
                status = int(resp.status)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(body)
            parsed = load_json(out_path)
            identity = feed_identity(out_path) if parsed else {}
            return {
                "game_id": game_id,
                "request_url": url,
                "retrieval_timestamp_utc": ts,
                "http_status": status,
                "response_size_bytes": len(body),
                "raw_output_path": pilot.rel(out_path),
                "sha256": sha256(out_path),
                "attempt": attempt,
                "retry_status": "RETRY_SUCCESS" if attempt > 1 else "NO_RETRY",
                "request_status": "SUCCESS",
                "parse_readiness_status": "PARSE_READY" if parsed else "MALFORMED_JSON",
                "official_game_id": identity.get("game_id", ""),
                "official_game_date": identity.get("game_date", ""),
                "official_game_status": identity.get("game_status", ""),
                "error": "",
            }
        except urllib.error.HTTPError as exc:
            last = {
                "game_id": game_id, "request_url": url, "retrieval_timestamp_utc": ts,
                "http_status": exc.code, "response_size_bytes": 0, "raw_output_path": pilot.rel(out_path),
                "sha256": "", "attempt": attempt, "retry_status": "RETRY_PENDING" if attempt < attempts else "RETRY_FAILED",
                "request_status": "HTTP_ERROR", "parse_readiness_status": "NOT_READY", "official_game_id": "",
                "official_game_date": "", "official_game_status": "", "error": str(exc),
            }
        except Exception as exc:
            last = {
                "game_id": game_id, "request_url": url, "retrieval_timestamp_utc": ts,
                "http_status": "", "response_size_bytes": 0, "raw_output_path": pilot.rel(out_path),
                "sha256": "", "attempt": attempt, "retry_status": "RETRY_PENDING" if attempt < attempts else "RETRY_FAILED",
                "request_status": "REQUEST_FAILURE", "parse_readiness_status": "NOT_READY", "official_game_id": "",
                "official_game_date": "", "official_game_status": "", "error": str(exc),
            }
        if attempt < attempts:
            time.sleep(0.25)
    return last


def file_mtime_utc(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat()


def acquire_missing(manifest: pd.DataFrame, raw_dir: Path, retry: bool) -> pd.DataFrame:
    requests = manifest[manifest["acquisition_required"].astype(str).str.lower().isin(["true", "1"])].copy()
    if len(requests) > 568:
        raise RuntimeError("request population exceeds frozen expected maximum")
    rows = []
    for _, r in requests.iterrows():
        gid = norm_id(r["game_id"])
        out_path = raw_dir / f"mlb_statsapi_feed_live_game_{gid}.json"
        if out_path.exists() and load_json(out_path):
            ident = feed_identity(out_path)
            rows.append({
                "game_id": gid,
                "request_url": ENDPOINT.format(game_id=gid),
                "retrieval_timestamp_utc": file_mtime_utc(out_path),
                "http_status": "200",
                "response_size_bytes": out_path.stat().st_size,
                "raw_output_path": pilot.rel(out_path),
                "sha256": sha256(out_path),
                "attempt": 0,
                "retry_status": "ALREADY_PRESENT_FROM_PRIOR_BOUNDED_RUN",
                "request_status": "SUCCESS_PREVIOUSLY_ACQUIRED_RAW_PRESENT",
                "parse_readiness_status": "PARSE_READY",
                "official_game_id": ident.get("game_id", ""),
                "official_game_date": ident.get("game_date", ""),
                "official_game_status": ident.get("game_status", ""),
                "error": "",
            })
            continue
        rows.append(fetch_one(gid, out_path, retry=retry))
    return pd.DataFrame(rows)


def certify_raw_sources(manifest: pd.DataFrame, raw_dir: Path) -> pd.DataFrame:
    rows = []
    for _, r in manifest.iterrows():
        gid = norm_id(r["game_id"])
        path = raw_dir / f"mlb_statsapi_feed_live_game_{gid}.json"
        parsed = load_json(path) if path.exists() else None
        failure = ""
        game_id_ok = False
        final_or_usable = False
        all_plays_present = False
        batter_pitcher_present = False
        starter_available = False
        pitcher_subs_available = False
        event_outcomes_present = False
        pa_constructible = False
        if not parsed:
            failure = "source_artifact_missing_or_malformed"
        else:
            ident = pilot.feed_game_identity(path, parsed)
            game_id_ok = norm_id(ident.get("game_id")) == gid
            final_or_usable = ident.get("game_status") == "Final"
            plays = parsed.get("liveData", {}).get("plays", {}).get("allPlays", []) or []
            all_plays_present = len(plays) > 0
            pa_plays = [p for p in plays if pilot.is_pa(p)]
            batter_pitcher_present = all(
                (p.get("matchup", {}) or {}).get("batter", {}).get("id")
                and (p.get("matchup", {}) or {}).get("pitcher", {}).get("id")
                for p in pa_plays
            ) and bool(pa_plays)
            starters = pilot.starters_for_feed(parsed)
            starter_available = all(v.get("starter_identity_status") == "OFFICIAL_BOXSCORE_GAMES_STARTED" for v in starters.values())
            pitcher_count = sum(len(pilot.box_team(parsed, side).get("pitchers", []) or []) for side in ("away", "home"))
            pitcher_subs_available = pitcher_count >= 2
            event_outcomes_present = all((p.get("result", {}) or {}).get("eventType") for p in pa_plays) and bool(pa_plays)
            pa_constructible = all_plays_present and batter_pitcher_present and event_outcomes_present
            if not game_id_ok:
                failure = "official_game_identity_mismatch"
            elif not final_or_usable:
                failure = "nonfinal_or_unusable_status"
            elif not all_plays_present:
                failure = "missing_play_sequence"
            elif not batter_pitcher_present:
                failure = "identity_incomplete"
            elif not pa_constructible:
                failure = "pa_boundaries_not_constructible"
            elif not starter_available:
                failure = "official_starter_identity_unresolved"
        rows.append({
            "game_date": r["game_date"],
            "game_id": gid,
            "source_path": pilot.rel(path),
            "source_sha256": sha256(path) if path.exists() else "",
            "official_game_identity_ok": game_id_ok,
            "final_or_usable_game_status": final_or_usable,
            "play_event_sequence_present": all_plays_present,
            "batter_pitcher_ids_present": batter_pitcher_present,
            "pa_boundaries_constructible": pa_constructible,
            "official_starter_identity_available": starter_available,
            "pitcher_substitutions_available": pitcher_subs_available,
            "event_outcomes_present": event_outcomes_present,
            "certification_status": "CERTIFIED" if (game_id_ok and final_or_usable and pa_constructible and starter_available) else "REJECTED",
            "failure_classification": failure,
        })
    return pd.DataFrame(rows)


def load_package_feeds(raw_dir: Path, certification: pd.DataFrame) -> list[tuple[Path, dict[str, Any]]]:
    certified = set(certification.loc[certification["certification_status"].eq("CERTIFIED"), "game_id"].map(norm_id))
    feeds = []
    for gid in sorted(certified):
        path = raw_dir / f"mlb_statsapi_feed_live_game_{gid}.json"
        parsed = load_json(path)
        if parsed:
            feeds.append((path, parsed))
    return feeds


def temporal_coverage(summary: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split, g in benchmark.groupby("temporal_split", dropna=False):
        ids = set(g["player_game_key"].astype(str))
        covered = summary[summary["player_game_key"].astype(str).isin(ids)] if not summary.empty and "player_game_key" in summary else pd.DataFrame()
        rows.append({
            "temporal_split": split,
            "benchmark_rows": len(g),
            "encounter_covered_rows": len(covered),
            "coverage_pct": len(covered) / len(g) if len(g) else 0,
            "two_plus_rows": int((covered["reconstructed_hits"] >= 2).sum()) if not covered.empty else 0,
            "exactly_one_hit_rows": int((covered["reconstructed_hits"] == 1).sum()) if not covered.empty else 0,
            "adequate_for_later_experiment": len(covered) / len(g) >= 0.95 if len(g) else False,
        })
    return pd.DataFrame(rows)


def next_experiment_population(summary: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame()
    cols = [
        "player_game_key", "game_date", "game_id", "player_id", "player_name", "outcome_class",
        "temporal_split", "official_hits_benchmark", "official_pa_benchmark",
        "reconstructed_total_pa", "actual_starter_facing_pa", "actual_bullpen_facing_pa",
        "hits_against_starter", "hits_against_bullpen", "pitchers_faced",
        "two_plus_hit_source_class", "pa_reconciles_benchmark", "hits_reconciles_benchmark",
        "source_path",
    ]
    pop = summary[summary["player_game_key"].notna()].copy()
    pop["role_integrity_state"] = pop["starter_plus_bullpen_pa_equals_total"].map(lambda x: "ROLE_PA_SUM_PASS" if bool(x) else "ROLE_PA_SUM_FAIL")
    pop["source_provenance"] = "official_statsapi_feed_live_local_package"
    cols.extend(["role_integrity_state", "source_provenance"])
    return pop[[c for c in cols if c in pop.columns]]


def experiment_population_summary(pop: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    rows = []
    rows.append({"scope": "full_population", "rows": len(pop)})
    primary = pop[pop["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])] if not pop.empty else pop
    rows.append({"scope": "one_hit_vs_two_plus_primary_population", "rows": len(primary)})
    if "suppression_subtype" in benchmark.columns:
        supp_keys = set(benchmark[benchmark["suppression_subtype"].notna()]["player_game_key"].astype(str))
        rows.append({"scope": "affirmative_suppression_rows", "rows": int(pop["player_game_key"].astype(str).isin(supp_keys).sum()) if not pop.empty else 0})
    long_price_path = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"
    lp = read_csv(long_price_path)
    if not lp.empty:
        key_col = "canonical_proposition_key" if "canonical_proposition_key" in lp.columns else "player_game_key" if "player_game_key" in lp.columns else ""
        if key_col:
            lp_target = lp.copy()
            if "primary_long_price_target" in lp_target.columns:
                lp_target = lp_target[lp_target["primary_long_price_target"].astype(str).str.lower().isin(["true", "1"])]
            lp_keys = set(lp_target[key_col].astype(str))
            rows.append({"scope": "+200_o15_rows_exact_price_artifact", "rows": int(pop["player_game_key"].astype(str).isin(lp_keys).sum()) if not pop.empty else 0})
    july_path = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/july12_probability_reconstruction_2026-07-17.csv"
    july = read_csv(july_path)
    if not july.empty and "canonical_proposition_key" in july.columns:
        july_keys = set(july["canonical_proposition_key"].astype(str))
        rows.append({"scope": "july12_sentinel_rows_available", "rows": int(pop["player_game_key"].astype(str).isin(july_keys).sum()) if not pop.empty else 0})
    return pd.DataFrame(rows)


def second_hit_expanded_summary(second: pd.DataFrame, population: pd.DataFrame) -> pd.DataFrame:
    if second.empty:
        return pd.DataFrame()
    s = second.merge(population[["player_game_key", "game_id", "player_id", "temporal_split"]], on=["game_id", "player_id"], how="left", suffixes=("", "_pop"))
    rows = []
    two = s[s["hit_count_class"].eq("TWO_PLUS_HITS")].copy()
    for split, g in two.groupby("temporal_split", dropna=False):
        counts = g["two_plus_hit_source_class"].value_counts()
        for cls, n in counts.items():
            rows.append({"temporal_split": split, "second_hit_source_class": cls, "rows": int(n)})
    for cls, g in two.groupby("two_plus_hit_source_class", dropna=False):
        rows.append({
            "temporal_split": "all",
            "second_hit_source_class": cls,
            "rows": len(g),
            "avg_total_pa": float(pd.to_numeric(g["reconstructed_total_pa"], errors="coerce").mean()),
            "avg_pitchers_faced": float(pd.to_numeric(g["pitchers_faced"], errors="coerce").mean()),
            "lineup_slot_available": int(pd.to_numeric(g["lineup_slot"], errors="coerce").notna().sum()),
        })
    return pd.DataFrame(rows)


def write_validation(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            rows.append({"artifact": pilot.rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": pilot.rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        if path.parent.name == RAW_DIR_NAME:
            continue
        try:
            json.loads(path.read_text(encoding="utf-8"))
            rows.append({"artifact": pilot.rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": pilot.rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        rows.append({"artifact": pilot.rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def write_manifest(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.rglob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            rows.append({"path": pilot.rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(pd.DataFrame(rows), out_dir / "sha256_manifest_2026-07-17.csv")


def build(out_dir: Path, acquire: bool, retry: bool) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = out_dir / RAW_DIR_NAME
    manifest = create_or_load_manifest(out_dir, raw_dir)
    copy_ledger = copy_existing_benchmark_feeds(manifest, raw_dir)
    write_csv(copy_ledger, out_dir / "existing_local_feed_copy_ledger_2026-07-17.csv")

    if acquire:
        request_ledger = acquire_missing(manifest, raw_dir, retry=retry)
    else:
        request_ledger = pd.DataFrame([{
            "game_id": norm_id(r["game_id"]),
            "request_url": r["endpoint"],
            "retrieval_timestamp_utc": "",
            "http_status": "",
            "response_size_bytes": "",
            "raw_output_path": r["expected_raw_output_path"],
            "sha256": "",
            "attempt": "",
            "retry_status": "",
            "request_status": "NOT_REQUESTED_ACQUIRE_DISABLED",
            "parse_readiness_status": "NOT_READY",
            "official_game_id": "",
            "official_game_date": "",
            "official_game_status": "",
            "error": "",
        } for _, r in manifest[manifest["acquisition_required"].astype(str).str.lower().isin(["true", "1"])].iterrows()])
    write_csv(request_ledger, out_dir / "request_ledger_2026-07-17.csv")

    certification = certify_raw_sources(manifest, raw_dir)
    write_csv(certification, out_dir / "raw_source_certification_2026-07-17.csv")

    benchmark = read_csv(BENCH)
    benchmark["game_id"] = benchmark["game_id"].map(norm_id)
    benchmark["player_id"] = benchmark["player_id"].map(norm_id)
    feeds = load_package_feeds(raw_dir, certification)
    ledgers = pilot.build_ledgers(feeds, benchmark)
    official = pilot.official_batter_totals(feeds, set(benchmark["game_id"]))
    summary = pilot.hitter_game_summary(ledgers["encounters"], official, benchmark)
    second = pilot.second_hit_source(summary)
    recon = pilot.reconciliation(summary, ledgers["source_manifest"])
    discrepancies = pilot.reconciliation_discrepancies(summary)
    roster = pilot.roster_relative(ledgers["encounters"], summary, benchmark)
    temporal = temporal_coverage(summary, benchmark)
    population = next_experiment_population(summary, benchmark)
    pop_summary = experiment_population_summary(population, benchmark)
    second_summary = second_hit_expanded_summary(second, population)
    designs = pilot.design_tables()

    output_map = {
        "expanded_encounter_ledger_2026-07-17.csv": ledgers["encounters"],
        "pitcher_entry_ledger_2026-07-17.csv": ledgers["pitcher_entry"],
        "hitter_game_exposure_summary_2026-07-17.csv": summary,
        "second_hit_source_ledger_2026-07-17.csv": second,
        "second_hit_source_expanded_summary_2026-07-17.csv": second_summary,
        "official_total_reconciliation_2026-07-17.csv": recon,
        "discrepancy_ledger_2026-07-17.csv": discrepancies,
        "temporal_split_coverage_2026-07-17.csv": temporal,
        "roster_relative_readiness_2026-07-17.csv": roster,
        "frozen_next_experiment_population_2026-07-17.csv": population,
        "frozen_next_experiment_population_summary_2026-07-17.csv": pop_summary,
        "rejection_missingness_ledger_2026-07-17.csv": ledgers["rejected"],
        "canonical_encounter_contract_2026-07-17.csv": designs["contract"],
        "bullpen_exposure_platform_design_2026-07-17.csv": designs["bullpen"],
        "next_probability_experiment_design_2026-07-17.csv": designs["experiment"],
    }
    for name, df in output_map.items():
        write_csv(df, out_dir / name)

    cert_count = int(certification["certification_status"].eq("CERTIFIED").sum()) if not certification.empty else 0
    request_success = int(request_ledger["request_status"].astype(str).str.startswith("SUCCESS").sum()) if not request_ledger.empty else 0
    request_fail = int(len(request_ledger) - request_success)
    pa_rate = float(summary["pa_reconciles_boxscore"].mean()) if not summary.empty else 0
    hit_rate = float(summary["hits_reconciles_boxscore"].mean()) if not summary.empty else 0
    split_ok = bool(not temporal.empty and temporal["adequate_for_later_experiment"].all())
    second_counts = second[second["hit_count_class"].eq("TWO_PLUS_HITS")]["two_plus_hit_source_class"].value_counts().to_dict() if not second.empty else {}

    decisions = {
        "MLB_ENCOUNTER_ACQUISITION_MANIFEST_DECISION": "FROZEN_568_GAME_MANIFEST_BOUND_AND_HASHED" if int(manifest["acquisition_required"].astype(str).str.lower().isin(["true", "1"]).sum()) == 568 else "FROZEN_MANIFEST_COUNT_DIFFERS_FROM_EXPECTED",
        "MLB_ENCOUNTER_OFFICIAL_FEED_ACQUISITION_DECISION": "OFFICIAL_FEED_ACQUISITION_COMPLETED_FOR_FROZEN_MANIFEST" if acquire and request_fail == 0 else ("OFFICIAL_FEED_ACQUISITION_PARTIAL_OR_FAILED" if acquire else "OFFICIAL_FEED_ACQUISITION_NOT_EXECUTED"),
        "MLB_ENCOUNTER_RAW_SOURCE_CERTIFICATION_DECISION": "RAW_SOURCES_CERTIFIED_FOR_FULL_BENCHMARK" if cert_count == 618 else "RAW_SOURCE_CERTIFICATION_PARTIAL",
        "MLB_ENCOUNTER_FULL_LEDGER_EXPANSION_DECISION": "FULL_BENCHMARK_LEDGER_EXPANDED" if cert_count == 618 else "PARTIAL_LEDGER_EXPANSION_ONLY",
        "MLB_ENCOUNTER_HITTER_GAME_EXPOSURE_DECISION": "HITTER_GAME_EXPOSURE_BUILT_FOR_CERTIFIED_FEEDS" if not summary.empty else "HITTER_GAME_EXPOSURE_NOT_BUILT",
        "MLB_ENCOUNTER_EXPANDED_RECONCILIATION_DECISION": "EXPANDED_RECONCILIATION_PASS" if pa_rate >= 0.98 and hit_rate >= 0.99 else "EXPANDED_RECONCILIATION_REVIEW_REQUIRED",
        "MLB_ENCOUNTER_BENCHMARK_COVERAGE_DECISION": "FULL_BENCHMARK_COVERAGE_CERTIFIED" if len(population) == len(benchmark) else "BENCHMARK_COVERAGE_PARTIAL",
        "MLB_ENCOUNTER_SECOND_HIT_SOURCE_EXPANSION_DECISION": "STARTER_TO_BULLPEN_SECOND_HIT_PATH_PERSISTS_DESCRIPTIVE" if second_counts.get("FIRST_STARTER_SECOND_BULLPEN", 0) > 0 else "SECOND_HIT_SOURCE_INSUFFICIENT",
        "MLB_ENCOUNTER_ROSTER_RELATIVE_READINESS_DECISION": "ROSTER_RELATIVE_READY_WITH_FIELD_GAPS" if not roster.empty else "ROSTER_RELATIVE_BLOCKED_BY_IDENTITY_OR_COVERAGE",
        "MLB_ENCOUNTER_PROBABILITY_EXPERIMENT_POPULATION_DECISION": "FROZEN_POPULATION_READY_FOR_BOUNDED_EXPERIMENT" if split_ok and len(population) == len(benchmark) else "FROZEN_POPULATION_NOT_FULLY_READY",
        "MLB_ENCOUNTER_NEXT_EXPERIMENT_STATUS": "DESIGNED_NOT_EXECUTED",
        "MLB_ENCOUNTER_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }
    write_csv(pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()]), out_dir / "required_decisions_2026-07-17.csv")

    metrics = {
        "generated_at_utc": now_utc(),
        "manifest_rows": len(manifest),
        "request_count": int(manifest["acquisition_required"].astype(str).str.lower().isin(["true", "1"]).sum()),
        "request_success": request_success,
        "request_failure": request_fail,
        "certified_games": cert_count,
        "benchmark_games": int(benchmark["game_id"].nunique()),
        "benchmark_rows": len(benchmark),
        "experiment_population_rows": len(population),
        "encounter_rows": len(ledgers["encounters"]),
        "starter_facing_pa": int(ledgers["encounters"]["role_classification"].eq("STARTER_FACING_PA").sum()) if not ledgers["encounters"].empty else 0,
        "reliever_facing_pa": int(ledgers["encounters"]["role_classification"].eq("RELIEVER_FACING_PA").sum()) if not ledgers["encounters"].empty else 0,
        "hitter_game_rows": len(summary),
        "boxscore_pa_reconciliation_rate": pa_rate,
        "boxscore_hits_reconciliation_rate": hit_rate,
        "second_hit_source_counts": second_counts,
        "temporal_split_ready": split_ok,
        "decisions": decisions,
    }
    write_json(metrics, out_dir / "machine_readable_full_benchmark_encounter_expansion_2026-07-17.json")

    md = f"""# MLB Full-Benchmark Official Encounter-Feed Acquisition and Canonical Ledger Expansion

Generated: `{metrics['generated_at_utc']}`

## Executive Summary

The frozen request manifest contains **{metrics['request_count']}** missing official MLB StatsAPI feed/live requests for the 618-game frozen benchmark. Acquisition success: **{request_success}**; failures: **{request_fail}**. Certified benchmark feeds: **{cert_count} / {metrics['benchmark_games']}**.

Expanded encounter rows: **{metrics['encounter_rows']}**. Starter-facing PA: **{metrics['starter_facing_pa']}**. Reliever-facing PA: **{metrics['reliever_facing_pa']}**.

Boxscore PA reconciliation: **{pa_rate:.2%}**. Boxscore hit reconciliation: **{hit_rate:.2%}**.

## Second-Hit Source Distribution

{pilot.markdown_table(pd.DataFrame([{'second_hit_source_class': k, 'two_plus_hitter_games': v} for k, v in second_counts.items()]))}

## Temporal Coverage

{pilot.markdown_table(temporal)}

## Experiment Population Summary

{pilot.markdown_table(pop_summary)}

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## No Behavior Changed

No OddsAPI, DB write, model fitting, threshold optimization, production model, selector, candidate, upload, Quick Card, workspace, or LaunchAgent behavior changed. Network access was limited to the frozen official MLB feed/live manifest.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")
    write_validation(out_dir)
    write_manifest(out_dir)
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT))
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    parser.add_argument("--acquire", action="store_true")
    parser.add_argument("--retry", action="store_true")
    args = parser.parse_args()
    metrics = build(Path(args.output_dir), acquire=args.acquire, retry=args.retry)
    print(json.dumps(metrics, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
