from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def is_complete(manifest_path: Path, outputs: list[Path]) -> bool:
    if not manifest_path.exists():
        return False
    try:
        manifest = read_json(manifest_path)
    except json.JSONDecodeError:
        return False
    if manifest.get("status") != "completed":
        return False
    return all(p.exists() for p in outputs)
