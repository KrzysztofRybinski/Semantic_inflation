from __future__ import annotations

from pathlib import Path

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io_utils import write_json


def run_doctor(context: PipelineContext) -> dict[str, object]:
    settings = context.settings
    created_dirs: list[str] = []
    deleted_files: list[str] = []
    warnings: list[str] = []

    required_dirs = [
        settings.paths.data_dir,
        settings.paths.raw_dir,
        settings.paths.interim_dir,
        settings.paths.processed_dir,
        settings.paths.outputs_dir,
        settings.paths.raw_dir / "sec",
        settings.paths.raw_dir / "epa" / "ghgrp",
        settings.paths.raw_dir / "epa" / "echo",
        settings.paths.outputs_dir / "manifests",
        settings.paths.outputs_dir / "qc",
    ]

    for path in required_dirs:
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            created_dirs.append(str(path))

    if settings.sec.requests_per_second > 10:
        warnings.append(
            "SEC requests_per_second exceeds 10; will be clamped in pipeline stages."
        )

    raw_dir = settings.paths.raw_dir
    for candidate in raw_dir.rglob("*"):
        if candidate.is_file() and candidate.stat().st_size == 0:
            candidate.unlink()
            deleted_files.append(str(candidate))

    qc_payload = {
        "created_dirs": created_dirs,
        "deleted_files": deleted_files,
        "warnings": warnings,
    }

    manifest = {
        "stage": "doctor",
        "status": "completed",
        "timestamp": context.now_iso(),
        "qc_path": str(settings.paths.outputs_dir / "qc" / "doctor_qc.json"),
    }

    write_json(settings.paths.outputs_dir / "qc" / "doctor_qc.json", qc_payload)
    write_json(settings.paths.outputs_dir / "manifests" / "doctor.json", manifest)
    return qc_payload
