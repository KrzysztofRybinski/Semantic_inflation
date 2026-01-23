from __future__ import annotations

from pathlib import Path
from typing import Any
import zipfile

import pandas as pd
from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.downloads import download_with_cache, sha256_file
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


def download_echo(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = settings.paths.processed_dir / "echo.parquet"
    inputs_hash = compute_inputs_hash(
        {"stage": "echo_download", "config": settings.model_dump(mode="json")}
    )
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "echo_download")
    if should_skip_stage(manifest_path, [output_path], inputs_hash, force):
        return StageResult(
            name="echo_download",
            status="skipped",
            outputs=[str(output_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    source_path = _resolve_path(settings.pipeline.echo.fixture_path, context.repo_root)
    if source_path.exists():
        df = pd.read_csv(source_path)
    else:
        if settings.runtime.offline:
            raise FileNotFoundError("ECHO fixture missing while runtime.offline is true.")
        case_url = settings.pipeline.echo.case_downloads_url
        frs_url = settings.pipeline.echo.frs_downloads_url
        if not case_url or not frs_url:
            raise FileNotFoundError("Missing ECHO case/frs download URLs.")

        headers = {"User-Agent": settings.sec.resolved_user_agent()}
        rps = min(settings.sec.max_requests_per_second, 10.0)
        log_path = settings.paths.outputs_dir / "qc" / "download_log.jsonl"

        raw_dir = settings.paths.raw_dir / "epa" / "echo"
        case_zip = raw_dir / "case_downloads.zip"
        frs_zip = raw_dir / "frs_downloads.zip"
        download_with_cache(case_url, case_zip, headers, rps, log_path)
        download_with_cache(frs_url, frs_zip, headers, rps, log_path)

        with zipfile.ZipFile(case_zip) as archive:
            candidates = [
                name
                for name in archive.namelist()
                if name.lower().endswith(".csv")
            ]
            if not candidates:
                raise FileNotFoundError("No CSV files found in ICIS case_downloads.zip")
            chosen = candidates[0]
            extracted_path = raw_dir / chosen
            extracted_path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(chosen) as handle:
                extracted_path.write_bytes(handle.read())
            df = pd.read_csv(extracted_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    qc_payload = {
        "rows": len(df),
        "columns": list(df.columns),
        "output": str(output_path),
        "output_sha256": sha256_file(output_path),
    }
    qc_path = settings.paths.outputs_dir / "qc" / "echo_download.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="echo_download",
        status="completed",
        outputs=[str(output_path)],
        qc_path=str(qc_path),
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
