#!/usr/bin/env python3
"""Prepare, validate, activate, or roll back one governed postseason authority child.

Each child is an append-only descendant of the active snapshot, declared below as
a ``ChildSpec``. Preparation never touches the active selector; activation is a
separate, operator-run compare-and-swap from the exact parent selector bytes.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from backend.mlb.scripts.build_mlb_versioned_file_phase_authority_v1 import (
    CANDIDATE_READY,
    build_candidate_snapshot,
)
from backend.mlb.season_transition.game_phase_authority_v1 import VersionedFileAuthority
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    ACTIVE_SELECTION_PATH,
    REPO_ROOT,
    canonical_json_bytes,
    sha256_file,
    verify_descriptor_chain,
)


SELECTION_CONTRACT = "MLB_2026_ACTIVE_FILE_PHASE_AUTHORITY_SELECTION_V1"
POSTSEASON_GAME_TYPES = {"F": "WILD_CARD", "D": "DIVISION_SERIES",
                         "L": "LEAGUE_CHAMPIONSHIP_SERIES", "W": "WORLD_SERIES"}
SNAPSHOT_FILES = ("canonical_game_phase_full_snapshot.jsonl", "retained_source_manifest.jsonl",
                  "descriptor.json", "validation_report.json")


@dataclass(frozen=True)
class ChildSpec:
    version: str
    package: str
    parent_descriptor_sha256: str
    parent_selection_sha256: str
    # gamePk -> (gameType, seriesDescription, gameDate) exactly as retained.
    additions: Mapping[int, tuple[str, str, str]]

    @property
    def package_dir(self) -> Path:
        return REPO_ROOT / "docs/contracts" / self.package

    @property
    def snapshot_dir(self) -> Path:
        return REPO_ROOT / "backend/mlb/season_transition/authority_snapshots" / self.version

    @property
    def descriptor_relpath(self) -> str:
        return f"backend/mlb/season_transition/authority_snapshots/{self.version}/descriptor.json"


CHILDREN: dict[str, ChildSpec] = {
    "v6": ChildSpec(
        version="v6",
        package="mlb_2026_postseason_authority_v6",
        parent_descriptor_sha256="fa639c543ba77bab4ca4a5a853ec78cb10eaf2163b7cebddad6156eeed4d941d",
        parent_selection_sha256="1690f8be895918ca0dab962ed252f6df8b7e93cfccb84c870f8209be16d6e04a",
        additions={
            849809: ("L", "NL Championship Series", "2026-10-12T00:00:00Z"),
            849810: ("L", "NL Championship Series", "2026-10-16T00:00:00Z"),
            849811: ("L", "NL Championship Series", "2026-10-15T00:00:00Z"),
            849812: ("L", "NL Championship Series", "2026-10-12T21:00:00Z"),
            849813: ("L", "AL Championship Series", "2026-10-14T00:00:00Z"),
            849815: ("L", "AL Championship Series", "2026-10-20T00:08:00Z"),
            849818: ("L", "AL Championship Series", "2026-10-15T17:00:00Z"),
            849819: ("D", "NL Division Series", "2026-10-06T22:00:00Z"),
            849820: ("L", "AL Championship Series", "2026-10-13T00:00:00Z"),
            849822: ("D", "NL Division Series", "2026-10-07T22:00:00Z"),
            849826: ("D", "NL Division Series", "2026-10-07T01:30:00Z"),
            849827: ("D", "NL Division Series", "2026-10-08T02:00:00Z"),
            849831: ("D", "AL Division Series", "2026-10-11T00:00:00Z"),
            849832: ("D", "AL Division Series", "2026-10-09T00:00:00Z"),
            849833: ("D", "AL Division Series", "2026-10-07T20:00:00Z"),
            849834: ("D", "AL Division Series", "2026-10-05T21:00:00Z"),
            849838: ("D", "AL Division Series", "2026-10-08T00:00:00Z"),
            849839: ("D", "AL Division Series", "2026-10-06T00:00:00Z"),
        },
    ),
}


class ChildValidationError(RuntimeError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ChildValidationError(f"JSON_OBJECT_REQUIRED:{path.name}")
    return value


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _verified_bytes(row: Mapping[str, Any]) -> bytes:
    relative = str(row.get("source_path") or "")
    path = (REPO_ROOT / relative).resolve()
    try:
        path.relative_to(REPO_ROOT.resolve())
    except ValueError:
        raise ChildValidationError(f"EVIDENCE_PATH_OUTSIDE_REPOSITORY:{relative}") from None
    try:
        raw = path.read_bytes()
    except OSError:
        raise ChildValidationError(f"RETAINED_EVIDENCE_MISSING:{relative}") from None
    if len(raw) != int(row["source_bytes"]) or hashlib.sha256(raw).hexdigest() != row["source_sha256"]:
        raise ChildValidationError(f"EVIDENCE_HASH_MISMATCH:{relative}")
    return raw


def _games_by_pk(raw: bytes) -> dict[int, list[dict[str, Any]]]:
    payload = json.loads(raw)
    games: dict[int, list[dict[str, Any]]] = {}
    for day in payload.get("dates") or []:
        for game in day.get("games") or []:
            games.setdefault(int(game.get("gamePk") or 0), []).append(game)
    return games


def _unique_game(games: Mapping[int, list[dict[str, Any]]], pk: int, code: str) -> dict[str, Any]:
    matches = games.get(pk) or []
    if len(matches) != 1:
        raise ChildValidationError(f"{code}:{pk}")
    return matches[0]


def _identity(game: Mapping[str, Any]) -> tuple[str, str, str]:
    return (str(game.get("gameType") or ""), str(game.get("seriesDescription") or ""),
            str(game.get("gameDate") or ""))


def _selection_bytes(spec: ChildSpec, descriptor_sha: str) -> bytes:
    return canonical_json_bytes({
        "contract_name": SELECTION_CONTRACT,
        "descriptor_path": spec.descriptor_relpath,
        "descriptor_sha256": descriptor_sha,
        "schema_version": 1,
        "selection_status": "GOVERNED_ACTIVE",
    }) + b"\n"


def _descends_from(selection: bytes, descriptor_sha: str) -> bool:
    """True when the selected descriptor's committed hash chain contains ``descriptor_sha``."""
    data = json.loads(selection)
    path = (REPO_ROOT / str(data.get("descriptor_path") or "")).resolve()
    expected = str(data.get("descriptor_sha256") or "")
    while path.is_file() and sha256_file(path) == expected:
        if expected == descriptor_sha:
            return True
        descriptor = _read_json(path)
        if not descriptor.get("parent_descriptor_path"):
            return False
        expected = str(descriptor.get("parent_descriptor_sha256") or "")
        path = (path.parent / str(descriptor["parent_descriptor_path"])).resolve()
    return False


def _atomic_replace(path: Path, body: bytes) -> None:
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def prepare(spec: ChildSpec) -> dict[str, Any]:
    """Build once, mark the new descriptor governed, and leave the parent selected."""
    package, snapshot = spec.package_dir, spec.snapshot_dir
    if sha256_file(ACTIVE_SELECTION_PATH) != spec.parent_selection_sha256:
        raise ChildValidationError("ACTIVE_SELECTION_NOT_EXPECTED_PARENT")
    if snapshot.exists() or (package / "inactive_promotion_record.json").exists():
        raise ChildValidationError("CHILD_OUTPUT_ALREADY_EXISTS")
    build = build_candidate_snapshot(source_set_path=package / "source_set.json", output_dir=snapshot)
    if build.get("decision") != CANDIDATE_READY or build.get("new_game_pks") != sorted(spec.additions):
        raise ChildValidationError("CHILD_BUILD_ADDITIONS_MISMATCH")
    descriptor_path = snapshot / "descriptor.json"
    candidate_sha = sha256_file(descriptor_path)
    descriptor = _read_json(descriptor_path)
    if descriptor.get("snapshot_status") != "CANDIDATE":
        raise ChildValidationError("CHILD_BUILD_NOT_CANDIDATE")
    descriptor["snapshot_status"] = "GOVERNED"
    descriptor_bytes = canonical_json_bytes(descriptor) + b"\n"
    descriptor_path.write_bytes(descriptor_bytes)
    (snapshot / "sha256_manifest.txt").write_bytes(b"".join(
        f"{sha256_file(snapshot / name)}  {name}\n".encode() for name in SNAPSHOT_FILES))
    governed_sha = hashlib.sha256(descriptor_bytes).hexdigest()
    selection = _selection_bytes(spec, governed_sha)
    promotion = {
        "contract_name": "MLB_2026_INACTIVE_GOVERNED_PHASE_AUTHORITY_PROMOTION_V1",
        "active_selection_sha256_before_and_after": spec.parent_selection_sha256,
        "candidate_descriptor_sha256": candidate_sha,
        "governed_descriptor_sha256": governed_sha,
        "parent_descriptor_sha256": spec.parent_descriptor_sha256,
        "proposal_sha256": sha256_file(snapshot / SNAPSHOT_FILES[0]),
        "source_manifest_sha256": sha256_file(snapshot / SNAPSHOT_FILES[1]),
        "source_set_sha256": sha256_file(package / "source_set.json"),
        "new_game_pks": sorted(spec.additions),
        "selection_changed": False,
        "network_requests": 0,
        "database_requests": 0,
    }
    (package / "inactive_promotion_record.json").write_bytes(canonical_json_bytes(promotion) + b"\n")
    (package / "proposed_active_selection.json").write_bytes(selection)
    (package / "proposed_active_selection.sha256").write_text(
        hashlib.sha256(selection).hexdigest() + "  proposed_active_selection.json\n", encoding="utf-8")
    return {"decision": "GOVERNED_CHILD_PREPARED_INACTIVE", **promotion}


def validate(spec: ChildSpec, *, selection_path: Path = ACTIVE_SELECTION_PATH) -> dict[str, Any]:
    """Read-only, full raw-byte verification of the child and all pinned evidence."""
    package, snapshot = spec.package_dir, spec.snapshot_dir
    descriptor_path = snapshot / "descriptor.json"
    source_set = _read_json(package / "source_set.json")
    promotion = _read_json(package / "inactive_promotion_record.json")
    if promotion.get("source_set_sha256") != sha256_file(package / "source_set.json"):
        raise ChildValidationError("SOURCE_SET_HASH_MISMATCH")
    if source_set.get("parent_descriptor_sha256") != spec.parent_descriptor_sha256:
        raise ChildValidationError("SOURCE_SET_PARENT_MISMATCH")

    descriptor_sha = sha256_file(descriptor_path)
    verified = verify_descriptor_chain(
        descriptor_path, expected_sha256=descriptor_sha, root=REPO_ROOT, allow_candidate=False)
    if verified.parent is None or verified.parent.sha256 != spec.parent_descriptor_sha256:
        raise ChildValidationError("CHILD_PARENT_MISMATCH")
    if verified.data.get("snapshot_status") != "GOVERNED":
        raise ChildValidationError("CHILD_NOT_GOVERNED")
    if promotion.get("governed_descriptor_sha256") != descriptor_sha:
        raise ChildValidationError("PROMOTION_DESCRIPTOR_HASH_MISMATCH")
    candidate = dict(verified.data, snapshot_status="CANDIDATE")
    if hashlib.sha256(canonical_json_bytes(candidate) + b"\n").hexdigest() != promotion.get(
            "candidate_descriptor_sha256"):
        raise ChildValidationError("CANDIDATE_TO_GOVERNED_TRANSITION_MISMATCH")

    proposal_path = snapshot / SNAPSHOT_FILES[0]
    if not proposal_path.read_bytes().startswith(verified.parent.proposal_path.read_bytes()):
        raise ChildValidationError("PARENT_PROPOSAL_PREFIX_CHANGED")
    rows = _jsonl(proposal_path)
    additions = rows[len(_jsonl(verified.parent.proposal_path)):]
    if [int(row["game_pk"]) for row in additions] != sorted(spec.additions):
        raise ChildValidationError("CHILD_EXACT_ADDITIONS_MISMATCH")
    by_pk = {int(row["game_pk"]): row for row in additions}

    sources = source_set.get("sources") or []
    selected: dict[int, set[str]] = {pk: set() for pk in spec.additions}
    for source in sources:
        games = _games_by_pk(_verified_bytes(source))
        for pk in source.get("selected_game_pks") or []:
            pk = int(pk)
            if pk not in spec.additions:
                raise ChildValidationError(f"SELECTED_GAME_NOT_DECLARED:{pk}")
            game = _unique_game(games, pk, "SELECTED_GAME_NOT_UNIQUE_OR_MISSING")
            if _identity(game) != spec.additions[pk] or int(game.get("season")) != 2026:
                raise ChildValidationError(f"SELECTED_SOURCE_IDENTITY_MISMATCH:{pk}")
            selected[pk].add(str(source["source_sha256"]))
    for pk, (game_type, round_label, start) in spec.additions.items():
        row = by_pk[pk]
        if (row.get("source_season") != 2026 or row.get("source_game_type") != game_type
                or row.get("season_phase") != "POSTSEASON"
                or row.get("postseason_round") != POSTSEASON_GAME_TYPES.get(game_type)
                or row.get("source_round") != round_label
                or row.get("scheduled_start_utc") != start):
            raise ChildValidationError(f"CHILD_CLASSIFICATION_MISMATCH:{pk}")
        if not selected[pk] or set(row.get("source_hashes") or []) != selected[pk]:
            raise ChildValidationError(f"CHILD_PROPOSAL_SOURCE_HASH_MISMATCH:{pk}")

    # Later retained observations must repeat the same identity and start.
    corroborating = source_set.get("corroborating_schedule_evidence") or []
    for evidence in corroborating:
        games = _games_by_pk(_verified_bytes(evidence))
        for pk in evidence.get("game_pks") or []:
            pk = int(pk)
            if pk not in spec.additions:
                raise ChildValidationError(f"CORROBORATING_GAME_NOT_DECLARED:{pk}")
            if _identity(_unique_game(games, pk, "CORROBORATING_GAME_NOT_UNIQUE_OR_MISSING")) != spec.additions[pk]:
                raise ChildValidationError(f"CORROBORATING_IDENTITY_CONFLICT:{pk}")

    # The run receipt shows the fully verified local lifecycle already accepted
    # each addition with the same classification on top of the parent.
    pinned = {str(row["source_sha256"]): str(row["source_path"]) for row in [*sources, *corroborating]}
    receipts = source_set.get("selection_evidence") or []
    receipt_pks: set[int] = set()
    for evidence in receipts:
        receipt = json.loads(_verified_bytes(evidence))
        schedule = receipt.get("history_schedule") or {}
        if (schedule.get("source_sha256") != evidence.get("schedule_sha256")
                or pinned.get(str(evidence.get("schedule_sha256"))) != schedule.get("source_path")):
            raise ChildValidationError("SELECTION_SCHEDULE_BINDING_MISMATCH")
        binding = receipt.get("phase_authority_binding") or {}
        if (binding.get("active_authority_descriptor_sha256") != spec.parent_descriptor_sha256
                or binding.get("schedule_source_sha256") != evidence.get("schedule_sha256")):
            raise ChildValidationError("SELECTION_AUTHORITY_BINDING_MISMATCH")
        decisions = {int(item["game_pk"]): item for item in binding.get("decisions") or []}
        for pk in evidence.get("game_pks") or []:
            pk = int(pk)
            decision, row = decisions.get(pk) or {}, by_pk.get(pk) or {}
            if (decision.get("authority_decision") != "EXACT_GAME_AUTHORITY_ACCEPTED"
                    or decision.get("normalized_phase") != row.get("season_phase")
                    or decision.get("postseason_round") != row.get("postseason_round")
                    or decision.get("source_game_type") != row.get("source_game_type")
                    or decision.get("source_round") != row.get("source_round")
                    or decision.get("source_season") != row.get("source_season")):
                raise ChildValidationError(f"SELECTION_DECISION_MISMATCH:{pk}")
            receipt_pks.add(pk)
    if receipt_pks != set(spec.additions):
        raise ChildValidationError("SELECTION_EVIDENCE_COVERAGE_MISMATCH")

    proposed = (package / "proposed_active_selection.json").read_bytes()
    proposed_sha = hashlib.sha256(proposed).hexdigest()
    if proposed != _selection_bytes(spec, descriptor_sha):
        raise ChildValidationError("PROPOSED_SELECTION_DESCRIPTOR_MISMATCH")
    if (package / "proposed_active_selection.sha256").read_text().split()[0] != proposed_sha:
        raise ChildValidationError("PROPOSED_SELECTION_FILE_HASH_MISMATCH")
    if promotion.get("active_selection_sha256_before_and_after") != spec.parent_selection_sha256:
        raise ChildValidationError("ACTIVE_SELECTION_COMMITMENT_MISMATCH")

    expected_manifest = [f"{sha256_file(snapshot / name)}  {name}" for name in SNAPSHOT_FILES]
    if (snapshot / "sha256_manifest.txt").read_text(encoding="utf-8").splitlines() != expected_manifest:
        raise ChildValidationError("CHILD_SNAPSHOT_MANIFEST_MISMATCH")
    report = _read_json(snapshot / "validation_report.json")
    if (report.get("decision") != CANDIDATE_READY or report.get("active_selection_modified") is not False
            or report.get("candidate_descriptor_sha256") != promotion.get("candidate_descriptor_sha256")):
        raise ChildValidationError("CHILD_BUILD_REPORT_MISMATCH")
    if VersionedFileAuthority(
        descriptor_path=descriptor_path, expected_descriptor_sha256=descriptor_sha, root=REPO_ROOT
    ).metadata.proposal_count != len(rows):
        raise ChildValidationError("CHILD_LOADER_COUNT_MISMATCH")

    active = selection_path.read_bytes()
    active_sha = hashlib.sha256(active).hexdigest()
    if active_sha == spec.parent_selection_sha256:
        decision = "GOVERNED_CHILD_VALID_INACTIVE"
    elif active_sha == proposed_sha:
        decision = "GOVERNED_CHILD_VALID_ACTIVE"
    elif _descends_from(active, descriptor_sha):
        decision = "GOVERNED_CHILD_VALID_SUPERSEDED"
    else:
        raise ChildValidationError("ACTIVE_SELECTION_NOT_REVIEWED_LINEAGE")
    return {
        "decision": decision,
        "active_selection_sha256": active_sha,
        "proposed_selection_sha256": proposed_sha,
        "parent_descriptor_sha256": spec.parent_descriptor_sha256,
        "descriptor_sha256": descriptor_sha,
        "proposal_sha256": sha256_file(proposal_path),
        "source_manifest_sha256": sha256_file(snapshot / SNAPSHOT_FILES[1]),
        "new_game_pks": sorted(spec.additions),
        "row_count": len(rows),
        "postseason_count": verified.data["normalized_phase_counts"]["POSTSEASON"],
        "scheduled_date_through": verified.data["scheduled_date_through"],
        "network_requests": 0,
        "database_requests": 0,
    }


def activate(spec: ChildSpec, *, selection_path: Path = ACTIVE_SELECTION_PATH,
             rollback_path: Path | None = None) -> dict[str, Any]:
    """Operator-only compare-and-swap from the exact parent selector bytes."""
    rollback_path = rollback_path or spec.package_dir / "activation_rollback.json"
    child = validate(spec, selection_path=selection_path)
    if child["decision"] != "GOVERNED_CHILD_VALID_INACTIVE":
        raise ChildValidationError("ACTIVE_SELECTION_NOT_EXPECTED_PARENT")
    prior = selection_path.read_bytes()
    selected = (spec.package_dir / "proposed_active_selection.json").read_bytes()
    record = {
        "contract_name": "MLB_2026_PHASE_AUTHORITY_SELECTION_ROLLBACK_V1",
        "prior_selection_base64": base64.b64encode(prior).decode(),
        "prior_selection_sha256": hashlib.sha256(prior).hexdigest(),
        "child_selection_base64": base64.b64encode(selected).decode(),
        "child_selection_sha256": hashlib.sha256(selected).hexdigest(),
        "child_descriptor_sha256": child["descriptor_sha256"],
        "rollback_mode": "COMPARE_AND_SWAP_ONLY",
    }
    fd = os.open(rollback_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "wb") as stream:
        stream.write(canonical_json_bytes(record) + b"\n")
    if selection_path.read_bytes() != prior:
        raise ChildValidationError("ACTIVE_SELECTION_COMPARE_AND_SWAP_MISMATCH")
    _atomic_replace(selection_path, selected)
    return {"decision": "ACTIVATED", "child_selection_sha256": record["child_selection_sha256"],
            "prior_selection_sha256": record["prior_selection_sha256"]}


def rollback(spec: ChildSpec, *, selection_path: Path = ACTIVE_SELECTION_PATH,
             rollback_path: Path | None = None) -> dict[str, Any]:
    record = _read_json(rollback_path or spec.package_dir / "activation_rollback.json")
    prior = base64.b64decode(record["prior_selection_base64"])
    child = base64.b64decode(record["child_selection_base64"])
    current = selection_path.read_bytes()
    if current != child or hashlib.sha256(current).hexdigest() != record["child_selection_sha256"]:
        raise ChildValidationError("ROLLBACK_COMPARE_AND_SWAP_MISMATCH")
    _atomic_replace(selection_path, prior)
    return {"decision": "ROLLED_BACK", "restored_sha256": hashlib.sha256(prior).hexdigest()}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", required=True, choices=sorted(CHILDREN))
    actions = parser.add_mutually_exclusive_group(required=True)
    for name in ("prepare", "validate", "activate", "rollback"):
        actions.add_argument(f"--{name}", action="store_true")
    args = parser.parse_args()
    spec = CHILDREN[args.child]
    try:
        result = (prepare(spec) if args.prepare else activate(spec) if args.activate
                  else rollback(spec) if args.rollback else validate(spec))
    except Exception as exc:
        print(json.dumps({"decision": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
