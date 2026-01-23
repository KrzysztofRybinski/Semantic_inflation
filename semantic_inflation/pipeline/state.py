from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Any

from semantic_inflation.pipeline.io import read_json, write_json


@dataclass(frozen=True)
class StageResult:
    name: str
    status: str
    outputs: list[str]
    qc_path: str | None = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)
    inputs_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def stage_manifest_path(outputs_dir: Path, stage: str) -> Path:
    return outputs_dir / "qc" / f"stage_{stage}.json"


def compute_inputs_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def load_stage_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return read_json(path)
    except json.JSONDecodeError:
        return None


def should_skip_stage(
    manifest_path: Path,
    outputs: list[Path],
    inputs_hash: str,
    force: bool,
) -> bool:
    if force:
        return False
    manifest = load_stage_manifest(manifest_path)
    if not manifest:
        return False
    if manifest.get("status") != "completed":
        return False
    if manifest.get("inputs_hash") != inputs_hash:
        return False
    return all(p.exists() for p in outputs)


def write_stage_manifest(path: Path, result: StageResult) -> None:
    payload = result.to_dict()
    write_json(path, payload)
