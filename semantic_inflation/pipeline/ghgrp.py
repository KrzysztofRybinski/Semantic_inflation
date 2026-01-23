from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import httpx

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io import write_json
from semantic_inflation.pipeline.state import (
    StageResult,
    compute_inputs_hash,
    should_skip_stage,
    stage_manifest_path,
    write_stage_manifest,
)


def _resolve_path(path: str | Path, repo_root: Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else repo_root / p


def download_ghgrp(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = settings.paths.processed_dir / "ghgrp.parquet"
    inputs_hash = compute_inputs_hash(
        {"stage": "ghgrp_download", "config": settings.model_dump(mode="json")}
    )
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "ghgrp_download")
    if should_skip_stage(manifest_path, [output_path], inputs_hash, force):
        return StageResult(
            name="ghgrp_download",
            status="skipped",
            outputs=[str(output_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    source_path = _resolve_path(settings.pipeline.ghgrp.fixture_path, context.repo_root)
    if source_path.exists():
        df = pd.read_csv(source_path)
    elif settings.pipeline.ghgrp.parent_companies_url or settings.pipeline.ghgrp.source_url:
        source_url = settings.pipeline.ghgrp.parent_companies_url or settings.pipeline.ghgrp.source_url
        response = httpx.get(source_url, timeout=60.0)
        response.raise_for_status()
        output_raw = settings.paths.raw_dir / "epa" / "ghgrp" / "ghgrp.csv"
        output_raw.parent.mkdir(parents=True, exist_ok=True)
        output_raw.write_bytes(response.content)
        df = pd.read_csv(output_raw)
    else:
        raise FileNotFoundError("Missing GHGRP fixture and no source_url configured")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    qc_payload = {
        "rows": len(df),
        "columns": list(df.columns),
        "output": str(output_path),
    }
    qc_path = settings.paths.outputs_dir / "qc" / "ghgrp_download.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="ghgrp_download",
        status="completed",
        outputs=[str(output_path)],
        qc_path=str(qc_path),
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
