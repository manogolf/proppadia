"""Build Oct 4 game-specific SOG coverage from retained packages only."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, load_canonical_slate, sha256_file, verify_package
from backend.nhl.performance_summary import _write_immutable_package, render_markdown

ROOT = Path(__file__).resolve().parents[3]
SLATE = "2026-10-04"
RUNS = (
    "nhldaily_20261004T184302135295Z_43eadb1e",
    "nhldaily_20261004T221508884047Z_8897eadf",
)


def _key(row: object) -> tuple[int, int, str]:
    return int(row.game_id), int(row.player_id), str(float(row.line)).rstrip("0").rstrip(".")


def reconstruct() -> Path:
    slate_dir = ROOT / "artifacts/operational/nhl/slates" / SLATE
    games = load_canonical_slate(
        slate_date=SLATE, raw_schedule_path=slate_dir / "raw_schedule_response.json",
        slate_health_path=slate_dir / "slate_health.json")
    starts = {int(game.game_id): datetime.fromisoformat(
        game.start_time_utc.replace("Z", "+00:00")) for game in games}
    source_records = []
    predictions: pd.DataFrame | None = None
    prediction_sha: str | None = None
    for run_id in RUNS:
        package = ROOT / "artifacts/operational/nhl/sog_market_attachments/season=2026" / f"slate_date={SLATE}" / f"run_id={run_id}"
        report_path = package / "sog_attachment_integrity.json"
        package_sha = verify_package(package)
        report = json.loads(report_path.read_text())
        if report.get("slate_date") != SLATE or report.get("parent_daily_run_id") != run_id:
            raise ValueError("SOURCE_ATTACHMENT_SLATE_OR_RUN_MISMATCH")
        pred_path = Path(report["prediction_artifact_path"])
        if sha256_file(pred_path) != report["prediction_artifact_sha256"]:
            raise ValueError("SOURCE_PREDICTION_HASH_MISMATCH")
        if prediction_sha and prediction_sha != report["prediction_artifact_sha256"]:
            raise ValueError("SOURCE_PREDICTION_SHA_MISMATCH")
        prediction_sha = report["prediction_artifact_sha256"]
        current_predictions = pd.read_csv(pred_path)
        if predictions is not None and current_predictions.to_csv(index=False) != predictions.to_csv(index=False):
            raise ValueError("SOURCE_PREDICTION_POPULATION_MISMATCH")
        predictions = current_predictions
        odds_dir = Path(report["odds_observation_path"])
        odds_sha = verify_package(odds_dir)
        if odds_sha != report["odds_observation_manifest_sha256"]:
            raise ValueError("SOURCE_ODDS_HASH_MISMATCH")
        odds_summary = json.loads((odds_dir / "observation_summary.json").read_text())
        if (odds_summary.get("slate_date") != SLATE
                or odds_summary.get("parent_daily_run_id") != run_id
                or report.get("canonical_game_set_hash") != canonical_game_set_hash(starts)):
            raise ValueError("SOURCE_ODDS_OR_CANONICAL_GAME_IDENTITY_MISMATCH")
        observed = datetime.fromisoformat(odds_summary["observation_timestamp_utc"].replace("Z", "+00:00"))
        attach = pd.read_csv(package / "sog_with_market.csv")
        source_records.append({
            "run_id": run_id, "package_path": str(package), "package_manifest_sha256": package_sha,
            "prediction_sha256": prediction_sha, "odds_manifest_sha256": odds_sha,
            "odds_observation_path": str(odds_dir),
            "observation_timestamp_utc": observed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "attachment": attach, "report": report,
        })
    assert predictions is not None
    pred = predictions.copy()
    pred["game_id"] = pd.to_numeric(pred.game_id, errors="raise").astype(int)
    if set(pred.game_id) != set(starts):
        raise ValueError("PREDICTION_CANONICAL_GAME_SET_MISMATCH")
    line_count = sum(bool(re.fullmatch(r"p_over_(\d+(?:[._]\d+)?)", str(col)))
                     for col in pred.columns)
    prediction_keys = {
        (int(row.game_id), int(row.player_id), str(float(match.group(1).replace("_", "."))).rstrip("0").rstrip("."))
        for row in pred.itertuples(index=False)
        for column in pred.columns
        if (match := re.fullmatch(r"p_over_(\d+(?:[._]\d+)?)", str(column)))
    }
    selected_by_game = {}
    selected_rows = []
    game_reports = {}
    for game_id in sorted(starts):
        eligible = [item for item in source_records if datetime.fromisoformat(
            item["observation_timestamp_utc"].replace("Z", "+00:00")) < starts[game_id]]
        game_preds = pred[pred.game_id.eq(game_id)]
        if eligible:
            chosen = max(eligible, key=lambda item: item["observation_timestamp_utc"])
            selected_by_game[game_id] = chosen
            attached = chosen["attachment"]
            attached = attached[pd.to_numeric(attached.game_id, errors="raise").astype(int).eq(game_id)]
            selected_rows.extend(attached.to_dict(orient="records"))
            statuses = {_key(row): pd.notna(pd.to_numeric(getattr(row, "p_over_mkt", None), errors="coerce"))
                        for row in attached.itertuples(index=False)}
            arm_counts = {"prediction_keys": len(game_preds),
                          "matched": sum(statuses.values()), "unmatched": len(statuses)-sum(statuses.values())}
            game_reports[str(game_id)] = {
                "canonical_start_time_utc": starts[game_id].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "prediction_keys": len(game_preds) * line_count, "matched": arm_counts["matched"],
                "unmatched": arm_counts["unmatched"], "ambiguous": 0,
                "poststart_excluded": 0, "selected_source_run_id": chosen["run_id"],
                "selected_observation_timestamp_utc": chosen["observation_timestamp_utc"],
                "selected_odds_manifest_sha256": chosen["odds_manifest_sha256"],
            }
        else:
            game_reports[str(game_id)] = {
                "canonical_start_time_utc": starts[game_id].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "prediction_keys": len(game_preds) * line_count, "matched": 0, "unmatched": 0,
                "ambiguous": 0, "poststart_excluded": len(game_preds) * line_count,
                "selected_source_run_id": None, "selected_observation_timestamp_utc": None,
                "selected_odds_manifest_sha256": None,
            }
    selected = pd.DataFrame(selected_rows)
    selected["game_id"] = pd.to_numeric(selected.game_id, errors="raise").astype(int)
    key_status = {_key(row): pd.notna(pd.to_numeric(getattr(row, "p_over_mkt", None), errors="coerce"))
                  for row in selected.itertuples(index=False)}
    operational = pred
    arms_frame = pd.read_csv(ROOT / "artifacts/operational/nhl/postgame_reconciliation/2026-10-04/reconciliation=8c7575230ceb6c7625da/graded_sog.csv")
    per_arm = {}
    for arm, frame in arms_frame.groupby("contract_arm", sort=True):
        keys = [(int(row.game_id), int(row.player_id), str(float(row.line)).rstrip("0").rstrip("."))
                for row in frame.itertuples(index=False)]
        eligible_keys = [key for key in keys if key in set(key_status)]
        poststart = sum(key[0] not in selected_by_game and key in prediction_keys
                        for key in keys)
        matched = sum(bool(key_status[key]) for key in eligible_keys)
        unbound = len(keys) - len(eligible_keys) - poststart
        per_arm[str(arm)] = {"prediction_rows": len(keys), "eligible_prestart": len(eligible_keys),
                             "matched": matched, "unmatched": len(eligible_keys)-matched,
                             "ambiguous": 0, "poststart_excluded": poststart,
                             "not_in_operational_population": unbound}
    counts = {
        "prestart_eligible_prediction_keys": sum(x["prediction_keys"] for x in game_reports.values() if not x["poststart_excluded"]),
        "prestart_matched": sum(x["matched"] for x in game_reports.values()),
        "prestart_unmatched": sum(x["unmatched"] for x in game_reports.values()),
        "ambiguous": 0,
        "poststart_excluded_prediction_keys": sum(x["poststart_excluded"] for x in game_reports.values()),
        "eligible_game_count": sum(game_id in selected_by_game for game_id in starts),
        "ineligible_game_count": sum(game_id not in selected_by_game for game_id in starts),
    }
    identity = {
        "contract": "NHL_SOG_GAME_SPECIFIC_PRESTART_RECONSTRUCTION_V1", "slate_date": SLATE,
        "canonical_schedule_raw_sha256": sha256_file(slate_dir / "raw_schedule_response.json"),
        "canonical_schedule_health_sha256": sha256_file(slate_dir / "slate_health.json"),
        "prediction_sha256": prediction_sha, "algorithm_sha256": sha256_file(Path(__file__).resolve()),
        "sources": [{k: v for k, v in item.items() if k not in {"attachment", "report"}} for item in source_records],
    }
    reconstruction_id = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    root = ROOT / "artifacts/operational/nhl/sog_market_coverage_reconstructions/season=2026" / f"slate_date={SLATE}" / f"reconstruction={reconstruction_id[:20]}"
    if root.exists():
        verify_package(root)
        _restate_summary(root)
        return root
    root.mkdir(parents=True)
    selected.to_csv(root / "sog_with_market.csv", index=False)
    report = {
        "schema_version": "NHL_SOG_GAME_SPECIFIC_PRESTART_RECONSTRUCTION_V1",
        "classification": "GAME_SPECIFIC_PRESTART_RECONSTRUCTION", "status": "PASS",
        "slate_date": SLATE, "reconstruction_identity": reconstruction_id,
        "identity_inputs": identity, "canonical_game_set": sorted(starts),
        "source_snapshots": [{k: v for k, v in item.items() if k not in {"attachment", "report"}} for item in source_records],
        "game_coverage": game_reports, "counts": counts, "per_arm": per_arm,
        "exact_key": ["game_id", "player_id", "prop", "line"],
        "market_join": "exact game/player/prop/line; no adjacent-line inference",
    }
    (root / "coverage.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (root / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE", "classification": report["classification"], "reconstruction_identity": reconstruction_id}, indent=2) + "\n")
    files = sorted(p for p in root.iterdir() if p.is_file())
    (root / "SHA256SUMS").write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))
    _restate_summary(root)
    return root


def _restate_summary(reconstruction: Path) -> Path:
    prior_dir = ROOT / "artifacts/operational/nhl/postgame_reconciliation/learning_restatements" / SLATE / "performance_summary=6abb2a78ddc1303dd348"
    prior = json.loads((prior_dir / "performance_summary.json").read_text())
    coverage = json.loads((reconstruction / "coverage.json").read_text())
    source_manifest = verify_package(reconstruction)
    before_grades = json.dumps({k: v for k, v in prior["models"]["sog"].items()
                                if k not in {"market_coverage", "arm_market_coverage"}},
                               sort_keys=True, separators=(",", ":"))
    summary = json.loads(json.dumps(prior))
    per_arm = coverage["per_arm"]
    selected_count = int(coverage["counts"]["prestart_eligible_prediction_keys"])
    summary["models"]["sog"]["market_coverage"] = {
        "status": "AVAILABLE_PARTIAL", "prediction_rows": selected_count,
        "eligible_prestart": selected_count,
        "matched": coverage["counts"]["prestart_matched"],
        "unmatched": coverage["counts"]["prestart_unmatched"], "ambiguous": 0,
        "poststart_excluded": coverage["counts"]["poststart_excluded_prediction_keys"],
        "eligible_game_count": coverage["counts"]["eligible_game_count"],
        "ineligible_game_count": coverage["counts"]["ineligible_game_count"],
        "per_arm": per_arm, "source_artifact": str((reconstruction / "coverage.json").resolve()),
        "source_artifact_sha256": sha256_file(reconstruction / "coverage.json"),
        "integrity_package_manifest_sha256": source_manifest,
        "prediction_artifact_sha256": coverage["identity_inputs"]["prediction_sha256"],
        "odds_observation_manifest_sha256": hashlib.sha256(json.dumps(
            sorted(x["odds_manifest_sha256"] for x in coverage["source_snapshots"]),
            separators=(",", ":")).encode()).hexdigest(),
        "population_binding": "EXACT_GAME_PLAYER_PROP_LINE_KEYS_PER_ARM",
        "reconstructed_from_retained_evidence": True, "affects_grading_denominator": False,
    }
    summary["models"]["sog"]["arm_market_coverage"] = per_arm
    if before_grades != json.dumps({k: v for k, v in summary["models"]["sog"].items()
                                    if k not in {"market_coverage", "arm_market_coverage"}},
                                   sort_keys=True, separators=(",", ":")):
        raise AssertionError("SOG_GRADING_SUMMARY_CHANGED")
    payload = {"prior_summary_identity": prior["summary_identity"],
               "reconstruction_identity": coverage["reconstruction_identity"],
               "reconstruction_manifest_sha256": source_manifest,
               "coverage_sha256": sha256_file(reconstruction / "coverage.json"),
               "summary_contract_sha256": sha256_file(Path(__file__).resolve().parents[1] / "performance_summary.py")}
    identity = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    summary["summary_identity"] = identity
    dest = prior_dir.parent / f"performance_summary={identity[:20]}"
    summary["summary_package"] = str(dest.resolve())
    summary["source_artifacts"]["sog_game_specific_reconstruction_sha256"] = payload["coverage_sha256"]
    summary["source_artifacts"]["sog_game_specific_reconstruction_manifest_sha256"] = source_manifest
    summary["generated_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    _write_immutable_package(dest, summary)
    return dest


if __name__ == "__main__":
    print(reconstruct())
