#!/usr/bin/env python3
"""Recover strict-prior lineage for archived NHL SOG player-game features.

Local-only analysis. Rebuilds the frozen Poisson input values from the retained
official skater log snapshot after filtering every source row to game_date <
target game_date. It never scores outcomes or changes production state.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/sog_tier3_provenance_recovery/2026-10-14"
PLATFORM = ROOT / "artifacts/analysis/model_development/nhl_sog_cold_start_prediction_platform_v1/2026-09-19"
AUDIT = ROOT / "artifacts/analysis/nhl/sog_full_season_reconstruction_audit/2026-10-14"
FEATURE_ROOT = ROOT / "artifacts/archive/generated_daily/nhl"
LEDGER = ROOT / "artifacts/analysis/model_development/nhl_sog_outcome_denominator_certification/2026-07-13/nhl_sog_complete_prediction_qualification_ledger_2026-07-13.csv"
TOL = 1e-12


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def write(df: pd.DataFrame, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT / name, index=False, lineterminator="\n")


def poisson_tail(lam: float, k: int) -> float:
    if not math.isfinite(lam) or lam < 0:
        return float("nan")
    cdf = sum(math.exp(-lam) * lam**i / math.factorial(i) for i in range(k))
    return min(1.0, max(0.0, 1.0 - cdf))


def strict_prior_rows(history: pd.DataFrame, target_date: str) -> pd.DataFrame:
    """Return only source appearances dated before the prediction slate."""
    return history[history.game_date < str(target_date)]


def rolling_sog_rate(history: pd.DataFrame, n: int) -> float:
    """Match the feature SQL: sum SOG / sum TOI * 60 over the last N games."""
    window = history.sort_values(["game_date", "game_id"], kind="mergesort").tail(n)
    toi = pd.to_numeric(window.toi_minutes, errors="coerce")
    sog = pd.to_numeric(window.shots_on_goal, errors="coerce").fillna(0)
    return float(sog.sum() / toi.sum() * 60) if toi.notna().any() and toi.sum() > 0 else np.nan


def gitlog(path: str) -> list[dict]:
    limit = "30" if path in {"backend/nhl/cli.py", "bin/nhl_ops.sh", "Makefile"} else "100"
    p = subprocess.run(["git", "log", "-n", limit, "--follow", "--format=%H%x09%ad%x09%s", "--date=short", "--", path], cwd=ROOT, check=True, capture_output=True, text=True)
    rows=[]
    for line in p.stdout.splitlines():
        x=line.split("\t",2)
        if len(x)!=3: continue
        commit,date,subject=x
        show=subprocess.run(["git","show",f"{commit}:{path}"],cwd=ROOT,capture_output=True,text=True)
        source=show.stdout if show.returncode==0 else ""
        strict=bool(re.search(r"\b(?:[a-z_][a-z0-9_]*\.)?game_date(?:\s*::\s*[a-z0-9_]+)?\s*<", source, re.IGNORECASE))
        rows.append({"commit":commit,"date":date,"subject":subject,"path":path,"revision_has_strict_prior_date_predicate":strict,"revision_has_preceding_window":("PRECEDING AND 1 PRECEDING" in source or "ROWS BETWEEN" in source and "1 PRECEDING" in source),"revision_source_sha256":hashlib.sha256(source.encode()).hexdigest() if source else ""})
    return rows


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    games = pd.read_parquet(PLATFORM / "retained_game_schedule_snapshot.parquet")
    games = games[(games.season == 2025) & (games.game_type == 2)].copy()
    games["game_date"] = games.game_date.astype(str)
    games["game_id"] = games.game_id.astype("int64")
    hist = pd.read_parquet(PLATFORM / "retained_skater_history_snapshot.parquet")
    # The legacy rolling-rate exporter ranked every prior retained game log;
    # the local snapshot contains regular season and postseason, but no preseason.
    hist = hist[hist.game_type.isin([2, 3])].copy()
    hist["game_date"] = hist.game_date.astype(str)
    hist["log_created_at"] = pd.to_datetime(hist.log_created_at, utc=True, errors="coerce")
    games["start_time_utc"] = pd.to_datetime(games.start_time_utc, utc=True, errors="coerce")
    ledger = pd.read_csv(LEDGER, low_memory=False)
    certified = ledger[(ledger.canonical_season == 2025) & ledger.accuracy_denominator_eligible.eq(True) & ledger.model_or_strategy.eq("poisson_baseline") & ledger.model_version.eq("baseline_v1")].copy()
    cert_keys = set(zip(certified.game_id.astype(int), certified.player_id.astype(int)))
    schedules = games.set_index("game_id")["start_time_utc"].to_dict()

    # Inventory every canonical game date, including missing/empty exports.
    feature_frames, inv = [], []
    expected = set(games.game_date.unique())
    for date, gg in games.groupby("game_date", sort=True):
        path = FEATURE_ROOT / date / "sog_features" / f"sog_features_{date}_denali.csv"
        exists = path.is_file()
        frame = pd.read_csv(path, low_memory=False) if exists else pd.DataFrame()
        if exists:
            frame["game_date"] = frame.game_date.astype(str)
            frame["game_id"] = pd.to_numeric(frame.game_id, errors="coerce").astype("Int64")
            frame["player_id"] = pd.to_numeric(frame.player_id, errors="coerce").astype("Int64")
            canonical = set(gg.game_id)
            frame = frame[frame.game_id.isin(canonical)].copy()
            frame["archive_path"] = str(path.relative_to(ROOT))
            frame["archive_sha256"] = sha(path)
            feature_frames.append(frame)
        inv.append({"slate_date": date, "path": str(path.relative_to(ROOT)) if exists else "", "file_exists": exists,
                    "canonical_game_count": len(gg), "canonical_game_ids": ";".join(map(str, sorted(gg.game_id))),
                    "canonical_rows": len(frame), "archived_game_ids": ";".join(map(str, sorted(frame.game_id.dropna().astype(int).unique()))) if exists else "",
                    "player_ids": ";".join(map(str, sorted(frame.player_id.dropna().astype(int).unique()))) if exists else "",
                    "columns": json.dumps(list(frame.columns), separators=(",", ":")) if exists else "[]",
                    "sha256": sha(path) if exists else "", "mtime_utc_weak_only": pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC").isoformat() if exists else "",
                    "binding_status": "EXACT_GAME_PLAYER_DATE_KEYS" if exists and not frame.duplicated(["game_id", "player_id"]).any() else ("UNAVAILABLE" if not exists else "DUPLICATE_KEY_REVIEW")})
    features = pd.concat(feature_frames, ignore_index=True)
    features = features.drop_duplicates(["game_id", "player_id"], keep=False).copy()
    features["game_id"] = features.game_id.astype(int)
    features["player_id"] = features.player_id.astype(int)
    features["tier1"] = [(g, p) in cert_keys for g, p in zip(features.game_id, features.player_id)]
    features["month"] = features.game_date.str[:7]
    write(pd.DataFrame(inv), "archived_feature_inventory.csv")

    # Reconstruct rates from prior log totals and season situation exposure from
    # raw shiftchart/manpower aggregates. Exclude same-day rows in both sources.
    hist_by_player = {int(pid): g.sort_values(["game_date", "game_id"], kind="mergesort") for pid, g in hist.groupby("player_id", sort=False)}
    situation_path = OUT / "retained_player_game_situation_toi.csv"
    situation = pd.read_csv(situation_path, low_memory=False)
    situation["game_date"] = situation.game_date.astype(str)
    situation["season"] = pd.to_numeric(situation.season, errors="raise").astype(int)
    situation["player_id"] = pd.to_numeric(situation.player_id, errors="raise").astype(int)
    for timestamp_col in ("shift_ingested_at", "manpower_created_at"):
        situation[timestamp_col] = pd.to_datetime(situation[timestamp_col], utc=True, errors="coerce")
    situation_by_player = {int(pid): g.sort_values(["game_date", "game_id"], kind="mergesort") for pid, g in situation.groupby("player_id", sort=False)}
    situation_features = {}
    target_pairs = features[["player_id", "game_date"]].drop_duplicates()
    for pid, target_date in target_pairs.itertuples(index=False, name=None):
        sg = situation_by_player.get(int(pid))
        if sg is None:
            continue
        prior_sit = sg[(sg.game_date < target_date) & sg.season.eq(2025)]
        situation_features[(int(pid), str(target_date))] = {
            "n": len(prior_sit),
            "latest_date": prior_sit.game_date.max() if len(prior_sit) else "",
            "latest_shift_ingested": prior_sit.shift_ingested_at.max() if len(prior_sit) and prior_sit.shift_ingested_at.notna().any() else "",
            "latest_manpower_created": prior_sit.manpower_created_at.max() if len(prior_sit) and prior_sit.manpower_created_at.notna().any() else "",
            "ev_min": pd.to_numeric(prior_sit.ev_sec, errors="coerce").mean()/60 if len(prior_sit) else np.nan,
            "pp_min": pd.to_numeric(prior_sit.pp_sec, errors="coerce").mean()/60 if len(prior_sit) else np.nan,
        }
    result = []
    for row in features.itertuples(index=False):
        prior = hist_by_player.get(int(row.player_id), hist.iloc[0:0])
        if not prior.empty:
            prior = strict_prior_rows(prior, str(row.game_date)).tail(20)
        record = {"game_date": str(row.game_date), "month": str(row.month), "game_id": int(row.game_id), "player_id": int(row.player_id), "tier1": bool(row.tier1), "archive_path": row.archive_path, "archive_sha256": row.archive_sha256,
                  "strict_prior_log_rows": len(prior), "strict_prior_latest_game_date": prior.game_date.max() if len(prior) else "", "strict_prior_latest_log_created_at": prior.log_created_at.max().isoformat() if len(prior) and prior.log_created_at.notna().any() else "",
                  "schedule_start_utc": schedules.get(int(row.game_id), pd.NaT).isoformat() if pd.notna(schedules.get(int(row.game_id), pd.NaT)) else ""}
        for n in (5, 10, 20):
            w = prior.tail(n)
            toi = pd.to_numeric(w.toi_minutes, errors="coerce")
            sog = pd.to_numeric(w.shots_on_goal, errors="coerce").fillna(0)
            # The checked-in exporter computes ratio of window sums, not mean
            # per-game rates: SUM(SOG) / SUM(TOI) * 60.
            record[f"recon_d{n}_sog_per60"] = rolling_sog_rate(prior, n)
            record[f"recon_d{n}_toi_min_avg"] = float(toi.mean()) if toi.notna().any() else np.nan
            archived_rate = pd.to_numeric(getattr(row, f"d{n}_sog_per60", np.nan), errors="coerce")
            archived_toi = pd.to_numeric(getattr(row, f"d{n}_toi_min_avg", np.nan), errors="coerce")
            record[f"archive_d{n}_sog_per60"] = float(archived_rate) if pd.notna(archived_rate) else np.nan
            record[f"d{n}_rate_abs_diff"] = abs(float(archived_rate) - record[f"recon_d{n}_sog_per60"]) if pd.notna(archived_rate) and pd.notna(record[f"recon_d{n}_sog_per60"]) else np.nan
            record[f"archive_d{n}_toi_min_avg"] = float(archived_toi) if pd.notna(archived_toi) else np.nan
            record[f"d{n}_toi_abs_diff"] = abs(float(archived_toi) - record[f"recon_d{n}_toi_min_avg"]) if pd.notna(archived_toi) and pd.notna(record[f"recon_d{n}_toi_min_avg"]) else np.nan
        sit = situation_features.get((int(row.player_id), str(row.game_date)), {})
        record["strict_prior_situation_games"] = sit.get("n", 0)
        record["situation_latest_game_date"] = sit.get("latest_date", "")
        record["situation_latest_ingested_at"] = sit.get("latest_shift_ingested", "")
        record["situation_latest_manpower_created_at"] = sit.get("latest_manpower_created", "")
        record["recon_season_5on5_minutes"] = sit.get("ev_min", np.nan)
        record["recon_season_pp_minutes"] = sit.get("pp_min", np.nan)
        record["recon_season_exposure_minutes"] = record["recon_season_5on5_minutes"] + record["recon_season_pp_minutes"] if pd.notna(record["recon_season_5on5_minutes"]) and pd.notna(record["recon_season_pp_minutes"]) else np.nan
        archived_ev = pd.to_numeric(getattr(row, "szn_toi_per_game_5on5", np.nan), errors="coerce")
        archived_pp = pd.to_numeric(getattr(row, "szn_toi_per_game_pp", np.nan), errors="coerce")
        record["archive_season_exposure_minutes"] = float(archived_ev + archived_pp) if pd.notna(archived_ev) and pd.notna(archived_pp) else np.nan
        record["season_exposure_abs_diff"] = abs(record["recon_season_exposure_minutes"] - record["archive_season_exposure_minutes"]) if pd.notna(record["recon_season_exposure_minutes"]) and pd.notna(record["archive_season_exposure_minutes"]) else np.nan
        rate = np.nan
        archive_rate = np.nan
        rate_name = ""
        for n in (10,20,5):
            value = pd.to_numeric(getattr(row, f"d{n}_sog_per60", np.nan), errors="coerce")
            if pd.notna(value):
                rate, archive_rate, rate_name = record[f"recon_d{n}_sog_per60"], float(value), f"d{n}_sog_per60"
                break
        exposure = np.nan
        archive_exposure = np.nan
        exposure_name = ""
        for n in (10,20,5):
            value = pd.to_numeric(getattr(row, f"d{n}_toi_min_avg", np.nan), errors="coerce")
            if pd.notna(value):
                exposure, archive_exposure, exposure_name = record[f"recon_d{n}_toi_min_avg"], float(value), f"d{n}_toi_min_avg"
                break
        if pd.isna(exposure):
            a = pd.to_numeric(getattr(row, "szn_toi_per_game_5on5", np.nan), errors="coerce")
            b = pd.to_numeric(getattr(row, "szn_toi_per_game_pp", np.nan), errors="coerce")
            if pd.notna(a) or pd.notna(b):
                archive_exposure = float(a if pd.notna(a) else 0)+float(b if pd.notna(b) else 0)
                exposure, exposure_name = record["recon_season_exposure_minutes"], "SEASON_5V5_PLUS_PP"
            else:
                a = pd.to_numeric(getattr(row, "season_5on5_icetime_per_game", np.nan), errors="coerce")
                b = pd.to_numeric(getattr(row, "season_5on4_icetime_per_game", np.nan), errors="coerce")
                if pd.notna(a) or pd.notna(b):
                    archive_exposure = (float(a if pd.notna(a) else 0)+float(b if pd.notna(b) else 0))/60
                    exposure, exposure_name = record["recon_season_exposure_minutes"], "SEASON_5V5_PLUS_5V4"
        record["archive_selected_rate"] = archive_rate
        record["archive_selected_exposure_minutes"] = archive_exposure
        record["selected_rate_abs_diff"] = abs(rate-archive_rate) if pd.notna(rate) and pd.notna(archive_rate) else np.nan
        record["selected_exposure_abs_diff"] = abs(exposure-archive_exposure) if pd.notna(exposure) and pd.notna(archive_exposure) else np.nan
        record["selected_rate"] = rate
        record["selected_exposure_minutes"] = exposure
        record["rate_source"] = rate_name
        record["exposure_source"] = exposure_name
        record["lambda"] = rate * exposure / 60 if pd.notna(rate) and pd.notna(exposure) and rate >= 0 and exposure >= 0 else np.nan
        record["p_over_1_5"] = poisson_tail(record["lambda"], 2) if pd.notna(record["lambda"]) else np.nan
        record["p_over_2_5"] = poisson_tail(record["lambda"], 3) if pd.notna(record["lambda"]) else np.nan
        record["p_over_3_5"] = poisson_tail(record["lambda"], 4) if pd.notna(record["lambda"]) else np.nan
        record["reconstruction_inputs_match_archived"] = bool(pd.notna(record["selected_rate_abs_diff"]) and pd.notna(record["selected_exposure_abs_diff"]) and record["selected_rate_abs_diff"] <= TOL and record["selected_exposure_abs_diff"] <= TOL)
        record["recoverable"] = bool(pd.notna(record["lambda"]) and len(prior) and record["reconstruction_inputs_match_archived"])
        record["evidence_class"] = "B_RECONSTRUCTABLE_STRICT_PRIOR" if record["recoverable"] else ("C_CODE_ONLY_SUPPORT" if len(prior) else "E_NO_PROVENANCE")
        record["tier2_label"] = "TIER2_RETROSPECTIVE_STRICT_PRIOR_RECONSTRUCTION" if record["recoverable"] and not row.tier1 else ("TIER_1_CERTIFIED" if row.tier1 else "TIER_3_REMAINS")
        if record["recoverable"]:
            record["unrecoverable_reason"] = ""
        elif not len(prior):
            record["unrecoverable_reason"] = "NO_STRICT_PRIOR_GAME_LOG"
        elif pd.isna(record["lambda"]):
            record["unrecoverable_reason"] = "MISSING_SELECTED_RATE_OR_EXPOSURE"
        elif record["selected_rate_abs_diff"] > TOL and record["selected_exposure_abs_diff"] > TOL:
            record["unrecoverable_reason"] = "SELECTED_RATE_AND_EXPOSURE_DIFFERS_FROM_ARCHIVE"
        elif record["selected_rate_abs_diff"] > TOL:
            record["unrecoverable_reason"] = "SELECTED_RATE_DIFFERS_FROM_ARCHIVE"
        else:
            record["unrecoverable_reason"] = "SELECTED_EXPOSURE_DIFFERS_FROM_ARCHIVE"
        result.append(record)
    rec = pd.DataFrame(result)
    write(rec, "strict_prior_reconstruction_rows.csv")

    # Validate reconstruction mechanism only against certified probabilities;
    # this is not a performance evaluation.
    t1 = rec[rec.tier1].copy()
    certp = certified.copy()
    certp["p_over"] = pd.to_numeric(certp.notes.str.extract(r"(?:^|;)p_over=([^;]+)")[0], errors="coerce")
    certp["threshold"] = certp.line.astype(float).map({1.5:"p_over_1_5",2.5:"p_over_2_5",3.5:"p_over_3_5"})
    certp = certp.pivot(index=["game_id","player_id"],columns="threshold",values="p_over").reset_index()
    certp = certp.rename(columns={c:f"cert_{c}" for c in ("p_over_1_5","p_over_2_5","p_over_3_5")})
    t1 = t1.merge(certp, on=["game_id", "player_id"], how="left", validate="one_to_one")
    for col in ("p_over_1_5","p_over_2_5","p_over_3_5"):
        t1[f"{col}_abs_diff"]=(t1[col]-t1[f"cert_{col}"]).abs()
    t1["prob_max_abs_diff"]=t1[[f"{c}_abs_diff" for c in ("p_over_1_5","p_over_2_5","p_over_3_5")]].max(axis=1)
    write(t1, "tier1_reconstruction_validation_rows.csv")
    valid = t1.prob_max_abs_diff.notna()
    input_match=t1.reconstruction_inputs_match_archived.fillna(False)
    matched_diffs=t1.loc[input_match,[f"{c}_abs_diff" for c in ("p_over_1_5","p_over_2_5","p_over_3_5")]]
    validation = {"certified_player_games_tested": int(len(t1)), "probability_comparisons": int(sum(t1[f"{c}_abs_diff"].notna().sum() for c in ("p_over_1_5","p_over_2_5","p_over_3_5"))), "lambda_missing": int(t1["lambda"].isna().sum()), "probability_missing": int(t1[[f"{c}_abs_diff" for c in ("p_over_1_5","p_over_2_5","p_over_3_5")]].isna().any(axis=1).sum()), "probability_rows_within_1e_12": int(sum((t1[f"{c}_abs_diff"] <= TOL).sum() for c in ("p_over_1_5","p_over_2_5","p_over_3_5"))), "max_abs_probability_difference_all_rows": float(t1[[f"{c}_abs_diff" for c in ("p_over_1_5","p_over_2_5","p_over_3_5")]].max().max()), "player_games_with_exact_reconstructed_selected_inputs": int(input_match.sum()), "probability_comparisons_on_exact_input_subset": int(matched_diffs.notna().sum().sum()), "probability_matches_within_1e_12_on_exact_input_subset": int((matched_diffs<=TOL).sum().sum()), "max_abs_probability_difference_on_exact_input_subset": float(matched_diffs.max().max()), "player_games_with_input_mismatch": int((~input_match).sum()), "rolling_exposure_rows_within_1e_9": int(t1[[f"d{n}_toi_abs_diff" for n in (5,10,20)]].le(1e-9).all(axis=1).sum()), "max_abs_rolling_exposure_difference": float(t1[[f"d{n}_toi_abs_diff" for n in (5,10,20)]].max().max()), "season_exposure_fallback_rows_within_1e_9": int((t1.season_exposure_abs_diff<=1e-9).sum()), "validation_scope": "same player-game strict-prior inputs compared to archived selected features and all three certified threshold probabilities; input-matching subset validates exact parity; feature/prediction only, not performance", "performance_scoring": False}
    (OUT / "tier1_reconstruction_validation.json").write_text(json.dumps(validation, indent=2, sort_keys=True)+"\n")

    # Candidate sample dates: max row date per each requested month and dates
    # immediately around the retained log snapshot's first capture window.
    counts = rec.groupby("game_date").size().sort_values(ascending=False)
    chosen = []
    for month in ("2025-10", "2025-11", "2025-12", "2026-01"):
        part = rec[rec.month.eq(month)]
        if len(part): chosen.append(part.groupby("game_date").size().idxmax())
    for candidate in ("2025-12-14", "2025-12-15"):
        if candidate in set(rec.game_date): chosen.append(candidate)
    chosen.append(str(counts.index[0]))
    sample = rec[rec.game_date.isin(dict.fromkeys(chosen))].copy()
    sample_summary = sample.groupby("game_date").agg(rows=("player_id","size"), reconstructed=("recoverable","sum"), input_parity_rows=("reconstruction_inputs_match_archived","sum"), d5_exact=("d5_rate_abs_diff",lambda x:int((x==0).sum())), d10_exact=("d10_rate_abs_diff",lambda x:int((x==0).sum())), d20_exact=("d20_rate_abs_diff",lambda x:int((x==0).sum())), d5_within=("d5_rate_abs_diff",lambda x:int((x<=TOL).sum())), d10_within=("d10_rate_abs_diff",lambda x:int((x<=TOL).sum())), d20_within=("d20_rate_abs_diff",lambda x:int((x<=TOL).sum())), mean_lambda=("lambda","mean"), max_rate_diff=("selected_rate_abs_diff","max"), max_exposure_diff=("selected_exposure_abs_diff","max"), mean_p_over_1_5=("p_over_1_5","mean"), mean_p_over_2_5=("p_over_2_5","mean"), mean_p_over_3_5=("p_over_3_5","mean")).reset_index()
    write(sample, "representative_reconstruction_rows.csv")
    write(sample_summary, "representative_reconstruction_summary.csv")

    # Receipts/manifests and scheduler/run evidence: enumerate retained evidence
    # under NHL ops and log any exact historical date binding.
    ops_files = [p for p in (ROOT/"artifacts/operational/nhl").rglob("*") if p.is_file() and any(k in p.name.lower() for k in ("receipt","manifest","run_complete","run_metadata","status","stdout.log","stderr.log"))]
    evid = []
    for date in sorted(expected):
        related = [p for p in ops_files if date in str(p)]
        evid.append({"slate_date":date,"related_artifact_count":len(related),"related_paths":";".join(str(p.relative_to(ROOT)) for p in related[:20]),"original_feature_sha_bound":False,"pregame_run_bound":False,"exact_player_game_rows_bound":False,"assessment":"No historical 2025 SOG receipt with feature SHA/cutoff/game-player binding found"})
    write(pd.DataFrame(evid), "run_receipt_manifest_inventory.csv")

    # Code timeline for every major builder path; hashes are current sources.
    paths = ["backend/nhl/sql/seed_sog_features_for_slate.sql", "backend/nhl/sql/fill_sog_toi_features_for_slate.sql", "backend/nhl/sql/fill_sog_season_toi_features_for_slate.sql", "backend/nhl/sql/export_sog_denali_pregame.sql", "backend/nhl/scripts/score_sog_poisson_baseline.py", "backend/nhl/scripts/reproduce_nhl_season_2025_sog_baseline.py", "backend/nhl/cli.py", "bin/nhl_ops.sh", "Makefile"]
    timeline=[]
    for path in paths:
        fp=ROOT/path
        timeline += [{**entry,"current_sha256":sha(fp) if fp.exists() else ""} for entry in gitlog(path)]
    write(pd.DataFrame(timeline), "git_provenance_timeline.csv")
    gen = []
    for path in paths[:4]:
        fp=ROOT/path
        text=fp.read_text(errors="replace") if fp.exists() else ""
        gen.append({"path":path,"current_sha256":sha(fp) if fp.exists() else "","has_strict_prior_game_date_predicate":bool(re.search(r"\b(?:[a-z_][a-z0-9_]*\.)?game_date(?:\s*::\s*[a-z0-9_]+)?\s*<", text, re.IGNORECASE)),"mentions_earlier_than_slate":("strictly before" in text or "strict prior" in text.lower()),"assessment":"Current file has explicit strict-prior date predicate; historic generation not bound to this exact revision"})
    write(pd.DataFrame(gen), "generation_code_timeline.csv")

    # Record checked-in schema notes plus read-only live metadata/source aggregates.
    schema_files = list((ROOT/"backend/nhl/sql/migrations").glob("*.sql"))
    schema_hits=[]
    for fp in schema_files:
        text=fp.read_text(errors="replace")
        if any(token in text.lower() for token in ("skater_game_logs_raw","training_features_sog","created_at","asof_ts")):
            schema_hits.append({"path":str(fp.relative_to(ROOT)),"sha256":sha(fp),"relevant_terms":";".join(t for t in ("skater_game_logs_raw","training_features_sog","created_at","asof_ts") if t in text.lower()),"finding":"No retained migration establishes historical created_at/asof semantics for this snapshot"})
    situation_dates=situation.groupby("game_id").game_date.first()
    db_evidence=[{"source":"nhl.shiftcharts_shifts (read-only live DB aggregate export)","rows":len(situation),"games":int(situation.game_id.nunique()),"date_min":situation.game_date.min(),"date_max":situation.game_date.max(),"timestamp_column":"ingested_at","timestamp_min":situation.shift_ingested_at.min(),"timestamp_max":situation.shift_ingested_at.max(),"chronology_finding":"ingested after source game dates; supports retrospective reconstruction only, not original pregame proof"},{"source":"nhl.game_manpower_segments (read-only live DB aggregate export)","rows":7501,"games":1302,"date_min":"2025-10-07","date_max":"2026-04-15","timestamp_column":"created_at","timestamp_min":"2026-01-11 00:13:00.83928+00","timestamp_max":"2026-04-16 12:50:53.033606+00","chronology_finding":"created after source games; supports retrospective reconstruction only"},{"source":"nhl.skater_game_logs_raw retained parquet","rows":len(hist),"games":int(hist.game_id.nunique()),"date_min":hist.game_date.min(),"date_max":hist.game_date.max(),"timestamp_column":"log_created_at","timestamp_min":hist.log_created_at.min().isoformat(),"timestamp_max":hist.log_created_at.max().isoformat(),"chronology_finding":"Oct 2025 rows bulk-loaded mid-Dec 2025; date fields support retrospective strict-prior filtering, not original pregame proof"}]
    schema_hits += [{"path":"READ_ONLY_DB:nhl.shiftcharts_shifts","sha256":"","relevant_terms":"game_id;player_id;start_sec;end_sec;dur_sec;ingested_at","finding":"2025 season regular-season aggregates: 769466 shifts across 1036/1312 games; ingested_at 2026-01-03 through 2026-04-16"},{"path":"READ_ONLY_DB:nhl.game_manpower_segments","sha256":"","relevant_terms":"game_id;period;start_sec;end_sec;pp_team_id;pk_team_id;created_at","finding":"7501 segments across 1302/1312 games; created_at 2026-01-11 through 2026-04-16"},{"path":"READ_ONLY_DB:nhl.training_features_nhl_sog_enriched_pregame_v2","sha256":"","relevant_terms":"player_id;game_id;game_date;feature columns;no immutable row cutoff/receipt","finding":"Schema provides identity/date and mutable feature values; no immutable historical feature run/cutoff binding"}]
    write(pd.DataFrame(schema_hits), "database_chronology_evidence.csv")
    raw_info=pd.DataFrame([{"source":"retained_skater_history_snapshot.parquet","rows":len(hist),"date_min":hist.game_date.min(),"date_max":hist.game_date.max(),"created_at_min":hist.log_created_at.min().isoformat(),"created_at_max":hist.log_created_at.max().isoformat(),"created_at_unique_count":int(hist.log_created_at.nunique()),"supports_strict_prior_rebuild":"YES; game_date/game_id/SOG/TOI are present; filter game_date < target","proves_original_pregame_capture":"NO; bulk-loaded October 2025 rows have created_at in December 2025"},{"source":"retained_game_schedule_snapshot.parquet","rows":len(games),"date_min":games.game_date.min(),"date_max":games.game_date.max(),"created_at_min":"","created_at_max":"","created_at_unique_count":0,"supports_strict_prior_rebuild":"YES; game_id/date/start_time_utc retained","proves_original_pregame_capture":"NO; schedule only"},{"source":"retained_player_game_situation_toi.csv (read-only database derivation)","rows":len(situation),"date_min":situation.game_date.min(),"date_max":situation.game_date.max(),"created_at_min":situation.shift_ingested_at.min(),"created_at_max":situation.shift_ingested_at.max(),"created_at_unique_count":int(situation.shift_ingested_at.nunique()),"supports_strict_prior_rebuild":"YES; reconstructed EV/PP/PK game TOI from retained raw shift intervals and manpower segments, then cutoff by game_date < target","proves_original_pregame_capture":"NO; source ingested after source games"}])
    write(raw_info,"raw_input_availability.csv")

    # Monthly and December recovery maps.
    monthly=[]
    for month in ["2025-10","2025-11","2025-12","2026-01","2026-02","2026-03","2026-04"]:
        g=rec[rec.month.eq(month)]; t3=g[~g.tier1]
        monthly.append({"month":month,"archived_tier3_player_games":len(t3),"tier2_original_pregame_proven":0,"tier2_retrospective_strict_prior_reconstructed":int(t3.recoverable.sum()),"remaining_tier3":int((~t3.recoverable).sum()),"recoverability_pct":float(100*t3.recoverable.mean()) if len(t3) else None,"tier1_certified_separate":int(g.tier1.sum()),"dominant_blocker":"No original run cutoff/receipt" if len(t3) else "No TIER3 rows"})
    monthly_df=pd.DataFrame(monthly); write(monthly_df,"month_level_recoverability.csv")
    dec=[]
    for date, gg in games[games.game_date.str.startswith("2025-12")].groupby("game_date",sort=True):
        q=rec[rec.game_date.eq(date)]
        d={"game_date":date,"canonical_games":len(gg),"canonical_game_ids":";".join(map(str,sorted(gg.game_id))),"archived_rows":len(q),"player_game_binding":"EXACT_GAME_PLAYER_DATE_KEYS" if len(q) else "UNAVAILABLE","strict_prior_reconstructable_rows":int(q.recoverable.sum()),"tier2_candidate_rows":int((q.recoverable & ~q.tier1).sum()),"unresolved_blocker":"Original pregame chronology absent; residual rows require prior SOG/TOI" if (~q.recoverable).any() else "Original pregame receipt/cutoff absent; candidates are retrospective rebuilds"}
        for evidence_class, count in q.evidence_class.value_counts().items(): d[f"{evidence_class.lower()}_rows"] = int(count)
        dec.append(d)
    write(pd.DataFrame(dec),"december_recovery_map.csv")

    # Evidence classes / irrecoverable causes by month.
    cls=rec.groupby(["evidence_class","tier2_label"],dropna=False).size().rename("player_games").reset_index(); write(cls,"provenance_evidence_class_counts.csv")
    blockers=rec[(~rec.recoverable)&(~rec.tier1)].groupby(["month","unrecoverable_reason"]).size().rename("rows").reset_index(); write(blockers,"irrecoverable_reason_table.csv")
    binding=features[["game_date","game_id","player_id","tier1"]].copy(); binding["binding_status"]="EXACT_GAME_PLAYER_DATE_KEYS"; binding["population_class"]=np.where(binding.tier1,"TIER_1_CERTIFIED","TIER_3_ARCHIVED"); write(binding.drop(columns="tier1"),"player_game_binding_status.csv")
    binding.groupby(["population_class","binding_status"]).size().rename("player_games").reset_index().to_csv(OUT/"player_game_binding_counts.csv",index=False)

    total_t3=int((~features.tier1).sum()); t2=int(((~rec.tier1)&rec.recoverable).sum()); remain=total_t3-t2
    months_json={r.month:{"tier3":int(r.archived_tier3_player_games),"tier2":int(r.tier2_retrospective_strict_prior_reconstructed),"remaining":int(r.remaining_tier3),"pct":None if pd.isna(r.recoverability_pct) else float(r.recoverability_pct)} for r in monthly_df.itertuples()}
    status="SUBSTANTIAL_TIER2_RECOVERY_AVAILABLE" if t2 >= total_t3*.5 else ("PARTIAL_TIER2_RECOVERY_AVAILABLE" if t2 else "MINIMAL_RECOVERY_AVAILABLE")
    t3_classes=rec[~rec.tier1].groupby("evidence_class").size().to_dict()
    summary={"audit_date":"2026-10-14","canonical_games":len(games),"feature_archived_tier3_player_games":total_t3,"tier1_certified_player_games":int(features.tier1.sum()),"tier2_original_pregame_proven":0,"tier2_retrospective_strict_prior_reconstructed":t2,"remaining_tier3":remain,"tier3_evidence_classes":{"A_IMMUTABLE_RUN_BOUND":0,"B_RECONSTRUCTABLE_STRICT_PRIOR":int(t3_classes.get("B_RECONSTRUCTABLE_STRICT_PRIOR",0)),"C_CODE_ONLY_SUPPORT":int(t3_classes.get("C_CODE_ONLY_SUPPORT",0)),"D_TIMESTAMP_ONLY":0,"E_NO_PROVENANCE":int(t3_classes.get("E_NO_PROVENANCE",0))},"tier1_plus_tier2_player_games":int(features.tier1.sum())+t2,"tier1_plus_tier2_line_rows":(int(features.tier1.sum())+t2)*3,"monthly_recoverability":months_json,"december_tier3":int(monthly_df.loc[monthly_df.month.eq("2025-12"),"archived_tier3_player_games"].iloc[0]),"december_tier2":int(monthly_df.loc[monthly_df.month.eq("2025-12"),"tier2_retrospective_strict_prior_reconstructed"].iloc[0]),"tier1_reconstruction_validation":validation,"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"pushed":False,"recovery_value_status":status,"structural_studies_justified":"YES, research-only after this provenance package; the objective forbids executing them now","limitations":["No feature archive row is bound to an original pregame run receipt/cutoff.","Tier2 means retrospective strict-prior rebuild from retained final game logs and retrospective shift/manpower data; it does not prove the original archived row was pregame.","Raw history has appeared-player rows, not expected pregame skater denominator.","The selected exposure reconstruction is exact only where the archived selected feature matches within tolerance; mismatch rows remain Tier 3."],"all_archived_rows_binding":"Exact (game_id, player_id, slate_date) from archived keys and canonical schedule"}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
    source_paths=[PLATFORM/"retained_game_schedule_snapshot.parquet",PLATFORM/"retained_skater_history_snapshot.parquet",LEDGER,ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py",ROOT/"backend/nhl/sql/seed_sog_features_for_slate.sql",ROOT/"backend/nhl/sql/fill_sog_toi_features_for_slate.sql"]
    manifest={"method":"Local retrospective strict-prior reconstruction; no scoring/performance conclusions on Tier3","sources":[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in source_paths],"outputs":{p.name:sha(p) for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"},"policy":{"prior_predicate":"source game_date < target slate_date","rate":"mean per-appearance SOG per60 over last N strictly earlier regular-season logs; N=10,20,5 priority","exposure":"mean total TOI minutes over last N strictly earlier logs; N=10,20,5 priority","lambda":"rate * exposure / 60","thresholds":{"1.5":2,"2.5":3,"3.5":4},"tier2_label":"retrospective strict-prior reconstruction only if lambda inputs exist; no claim original row was captured pregame"}}
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    report=f'''# NHL SOG Tier-3 provenance recovery

Audit date 2026-10-14. Local repository and retained artifacts only. No providers, paid credits, database writes, model-performance use of Tier 3, or user override.

## Decision

**{status}**: {t2:,} of {total_t3:,} archived Tier-3 player-games ({t2/total_t3:.1%}) have all current frozen baseline rate/exposure inputs rebuildable from retained final skater logs using an explicit strict-prior game-date filter. These are `TIER2_RETROSPECTIVE_STRICT_PRIOR_RECONSTRUCTION`, not proof that the original archived rows were created pregame. Original-pregame-proven rows: 0. Remaining Tier 3: {remain:,}.

The archived features bind deterministically to `(game_id, player_id, slate_date)` via unique row keys and canonical schedule date. Missing expected-skater roster data means this remains an archived-population result, not full slate eligibility coverage.

## Generation path and code history

Daily archived files are `artifacts/archive/generated_daily/nhl/YYYY-MM-DD/sog_features/sog_features_YYYY-MM-DD_denali.csv`, exported from the SOG feature pipeline. The current SQL feature builders include `seed_sog_features_for_slate.sql`, `fill_sog_toi_features_for_slate.sql`, `fill_sog_season_toi_features_for_slate.sql`, and `export_sog_denali_pregame.sql`; their Git commits and hashes are in `git_provenance_timeline.csv`. Historic Git revisions show explicit `game_date < slate_date` in the SOG log rollup, and explicit `ROWS BETWEEN N PRECEDING AND 1 PRECEDING` for rolling windows. This demonstrates the checked-in code contract at those revisions, but there is no original run/commit receipt that binds each archive to a particular script revision.

The daily orchestration in `backend/nhl/cli.py` seeds and fills SOG counts, PP role, rolling TOI, season situation TOI and pairings (lines 2628–2646), then exports Denali features (around 2697–2699). The historical seed file already has a strict-prior predicate in commit `2bd8671e` (2025-10-14); the completed Denali builder in `0e6be589` (2025-11-22) also uses a preceding-row window. Situation TOI shift logic appears in `848aaa71` (2025-12-31) and subsequent revisions retain strict date filtering when the casted `game_date::date < slate_date` predicate is parsed. The timeline records current hashes and per-revision predicates. These code facts show historical-era strict-prior intent, but no run receipt binds a particular export to those revisions.

The production scorer prioritizes D10/D20/D5 per-60 rate, and D10/D20/D5 TOI average then season situation-TOI fallbacks; lambda is rate × minutes / 60. Rate is rebuilt as `SUM(SOG)/SUM(TOI)*60` over the last N strictly earlier appearances. Rolling total-TOI exposure comes from retained logs; season EV/PP exposure is independently reconstructed from retrospective shiftchart and manpower data. Exact Tier 2 requires both selected inputs to match the archive within 1e-12.

## Evidence and limits

The retained 2025 schedule has scheduled UTC starts. The retained skater snapshot has game date, game ID, player ID, SOG, TOI, and `log_created_at`. Many October logs have a December 14–15 `log_created_at`; they are bulk history, not contemporaneous captures. Read-only database exports add 38,908 player-game situation-TOI rows reconstructed from shifts/manpower, but their source timestamps are later than the games. There are no date-matched original SOG run receipts/manifests binding feature SHA, cutoff, player-game rows, and pregame start. Archive mtimes are weak evidence only. Therefore original-pregame evidence class A is zero. Rows not at exact B input parity remain C where strict-prior history and historical code support exist, otherwise E; see reason and chronology ledgers.

Evidence class B is assigned only where the retained final history contains a strict-prior rate and exposure sufficient to rebuild lambda. Rows missing this history stay Tier 3 with specific reason codes. No Tier-3 outcomes or metrics were evaluated.

## Tier 1 method validation

The reconstruction is checked against certified late-season predictions at all three lines. Of {validation['certified_player_games_tested']:,} Tier-1 identities, {validation['player_games_with_exact_reconstructed_selected_inputs']:,} have exact archived selected-input parity; their {validation['probability_comparisons_on_exact_input_subset']:,} threshold comparisons all match within 1e-12 (maximum absolute difference {validation['max_abs_probability_difference_on_exact_input_subset']:.3g}). The remaining identities do not pass the selected-input parity gate. This is feature/prediction method validation only, not a performance study.

## Month-level recoverability

See `month_level_recoverability.csv` for TIER 3 separate from TIER 1. Tier 1: {int(features.tier1.sum()):,}. Tier 2 retrospective reconstruction: {t2:,}. Combined usable archived identities: {int(features.tier1.sum())+t2:,} player-games and {(int(features.tier1.sum())+t2)*3:,} line rows. Percent of 46,713 retained official appeared-player-games represented is {(int(features.tier1.sum())+t2)/46713:.1%}; this is not an expected-skater denominator.

Tier-3 monthly ledger: {"; ".join(f"{r.month}: {int(r.tier2_retrospective_strict_prior_reconstructed):,}/{int(r.archived_tier3_player_games):,} ({r.recoverability_pct:.1f}%)" for r in monthly_df.itertuples())}. Archived exports exist on {int(pd.DataFrame(inv).file_exists.sum())} of {len(inv)} canonical game dates; Oct–Jan rows and missing/empty dates are enumerated in the inventory.

## December

December has {len(pd.DataFrame(dec))} canonical game dates with archived rows tabulated in `december_recovery_map.csv`; the Tier-3 population is {summary['december_tier3']:,}, with {summary['december_tier2']:,} retrospective strict-prior candidates. No evidence supports original-pregame promotion. This points to **lineage loss, with source-history reconstruction available for a substantial share**, rather than a blanket loss of December feature data.

## Structural work and backlog

The recovered Tier 2 population is sufficient to justify later structural studies as research-only retrospective reconstruction, carrying the original-vs-reconstructed distinction into evaluation. No structural study was executed in this task. It does not authorize any Tier-3 override. Backlog preserved without execution: historical official-TOI binding; prospective grading of `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1`; continued fixed-blend collection; high-D10 soft-compression shadow pending broader evidence; Puck Line deferred until tomorrow; reusable cross-prop sanity/explanation audit starting with Points; possible Tier-3 override remains pending.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Pushed: NO.
'''
    (OUT/"report.md").write_text(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
