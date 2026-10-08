"""Deterministic, scoring-time identities for NHL fitted scoring artifacts."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def identity_hash_from_components(components: Iterable[dict]) -> str:
    material = [{"role": str(item["role"]),
                 "canonical_artifact_path": str(item["canonical_artifact_path"]),
                 "sha256": str(item["sha256"])} for item in components]
    material.sort(key=lambda item: (item["role"], item["canonical_artifact_path"]))
    manifest = json.dumps(material, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(manifest.encode("utf-8")).hexdigest()


def validate_fitted_model_evidence(evidence: dict, *, prediction_path: str,
                                  prediction_sha256: str) -> bool:
    """Validate the receipt-bound prediction and self-contained identity manifest."""
    try:
        components = evidence["component_artifacts"]
        if not isinstance(components, list) or not components:
            return False
        config_component = next((item for item in components
                                 if item["role"] == "scoring_configuration"), None)
        if config_component is not None:
            config_bytes = json.dumps(evidence.get("scoring_configuration") or {}, sort_keys=True,
                                      separators=(",", ":"), allow_nan=False).encode("utf-8")
            if hashlib.sha256(config_bytes).hexdigest() != config_component["sha256"]:
                return False
        return (
            evidence.get("prediction_artifact_path") == prediction_path
            and evidence.get("prediction_artifact_sha256") == prediction_sha256
            and evidence.get("fitted_model_identity_sha256") == identity_hash_from_components(components)
            and all(len(item["sha256"]) == 64 for item in components)
        )
    except (KeyError, TypeError, ValueError):
        return False


def fitted_model_identity(*, model_family: str, model_version: str,
                          components: Iterable[tuple[str, Path]],
                          prediction_path: Path, scoring_run_id: str,
                          scoring_configuration: dict | None = None) -> dict:
    """Hash role/path/content tuples; paths are repository relative and stable."""
    material = []
    for role, path in components:
        path = Path(path).resolve()
        if not path.is_file():
            raise RuntimeError(f"FITTED_MODEL_COMPONENT_MISSING:{path}")
        try:
            canonical_path = path.relative_to(ROOT).as_posix()
        except ValueError as exc:
            raise RuntimeError(f"FITTED_MODEL_COMPONENT_OUTSIDE_REPO:{path}") from exc
        material.append({"role": str(role), "canonical_artifact_path": canonical_path,
                         "sha256": sha256_file(path)})
    if scoring_configuration is not None:
        config_bytes = json.dumps(scoring_configuration, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        material.append({"role": "scoring_configuration", "canonical_artifact_path": "<scoring-configuration>",
                         "sha256": hashlib.sha256(config_bytes).hexdigest()})
    material.sort(key=lambda item: (item["role"], item["canonical_artifact_path"]))
    if not material:
        raise ValueError("FITTED_MODEL_COMPONENTS_EMPTY")
    model_hash = identity_hash_from_components(material)
    prediction_path = Path(prediction_path).resolve()
    try:
        prediction_identity = prediction_path.relative_to(ROOT).as_posix()
    except ValueError as exc:
        raise RuntimeError("PREDICTION_ARTIFACT_OUTSIDE_REPO") from exc
    return {
        "model_family": model_family,
        "model_version": model_version,
        "fitted_model_identity_sha256": model_hash,
        "component_artifacts": material,
        "scoring_configuration": scoring_configuration or {},
        "prediction_artifact_path": prediction_identity,
        "prediction_artifact_sha256": sha256_file(prediction_path),
        "scoring_run_id": scoring_run_id,
    }
