from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import httpx

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io_utils import is_complete, write_json


def _resolve_path(path: str | Path, repo_root: Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else repo_root / p


def download_ghgrp(context: PipelineContext) -> dict[str, Any]:
    settings = context.settings
    manifest_path = settings.paths.outputs_dir / "manifests" / "ghgrp_download.json"
    output_path = settings.paths.processed_dir / "ghgrp.parquet"

    if is_complete(manifest_path, [output_path]):
        return {"skipped": True, "output": str(output_path)}

    source_path = _resolve_path(settings.pipeline.ghgrp.fixture_path, context.repo_root)
    if source_path.exists():
        df = pd.read_csv(source_path)
    elif settings.pipeline.ghgrp.source_url:
        response = httpx.get(settings.pipeline.ghgrp.source_url, timeout=60.0)
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
    write_json(settings.paths.outputs_dir / "qc" / "ghgrp_qc.json", qc_payload)

    manifest = {
        "stage": "ghgrp_download",
        "status": "completed",
        "timestamp": context.now_iso(),
        "output": str(output_path),
    }
    write_json(manifest_path, manifest)
    return qc_payload
