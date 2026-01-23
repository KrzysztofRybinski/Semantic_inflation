from __future__ import annotations

from pathlib import Path
from typing import Any
import zipfile
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import pandas as pd
import httpx

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


def download_ghgrp(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = settings.paths.processed_dir / "ghgrp.parquet"
    parent_output_path = settings.paths.processed_dir / "ghgrp_parent_companies.parquet"
    inputs_hash = compute_inputs_hash(
        {"stage": "ghgrp_download", "config": settings.model_dump(mode="json")}
    )
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "ghgrp_download")
    if should_skip_stage(
        manifest_path, [output_path, parent_output_path], inputs_hash, force
    ):
        return StageResult(
            name="ghgrp_download",
            status="skipped",
            outputs=[str(output_path), str(parent_output_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    source_path = _resolve_path(settings.pipeline.ghgrp.fixture_path, context.repo_root)
    if source_path.exists():
        df = pd.read_csv(source_path)
        parent_df = pd.DataFrame()
    else:
        if settings.runtime.offline:
            raise FileNotFoundError("GHGRP fixture missing while runtime.offline is true.")
        data_sets_url = settings.pipeline.ghgrp.data_sets_url
        data_summary_url = settings.pipeline.ghgrp.data_summary_url
        parent_url = settings.pipeline.ghgrp.parent_companies_url
        if not data_summary_url or not parent_url:
            response = httpx.get(data_sets_url, timeout=60.0)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            for link in soup.find_all("a"):
                text = (link.get_text() or "").strip().lower()
                href = link.get("href")
                if not href:
                    continue
                if "data summary" in text and href.endswith(".zip"):
                    data_summary_url = data_summary_url or urljoin(data_sets_url, href)
                if "reported parent companies" in text and href.endswith(".xlsb"):
                    parent_url = parent_url or urljoin(data_sets_url, href)
        if not data_summary_url or not parent_url:
            raise FileNotFoundError("Missing GHGRP data summary or parent companies URL.")

        headers = {"User-Agent": settings.sec.resolved_user_agent()}
        rps = min(settings.sec.max_requests_per_second, 10.0)
        log_path = settings.paths.outputs_dir / "qc" / "download_log.jsonl"

        raw_dir = settings.paths.raw_dir / "epa" / "ghgrp"
        data_summary_zip = raw_dir / "ghgrp_data_summary.zip"
        parent_companies_path = raw_dir / "ghgrp_parent_companies.xlsb"

        download_with_cache(data_summary_url, data_summary_zip, headers, rps, log_path)
        download_with_cache(parent_url, parent_companies_path, headers, rps, log_path)

        parent_df = pd.read_excel(parent_companies_path, engine="pyxlsb")

        with zipfile.ZipFile(data_summary_zip) as archive:
            candidates = [
                name
                for name in archive.namelist()
                if name.lower().endswith((".csv", ".xlsx", ".xls"))
            ]
            if not candidates:
                raise FileNotFoundError("No readable tables found in GHGRP data summary zip.")
            preferred = [name for name in candidates if "summary" in name.lower()]
            chosen = preferred[0] if preferred else candidates[0]
            extracted_path = raw_dir / chosen
            extracted_path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(chosen) as handle:
                extracted_path.write_bytes(handle.read())
            if extracted_path.suffix.lower() == ".csv":
                df = pd.read_csv(extracted_path)
            else:
                df = pd.read_excel(extracted_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    parent_output_path.parent.mkdir(parents=True, exist_ok=True)
    parent_df.to_parquet(parent_output_path, index=False)

    qc_payload = {
        "rows": len(df),
        "columns": list(df.columns),
        "output": str(output_path),
        "parent_companies_output": str(parent_output_path),
        "parent_companies_rows": len(parent_df),
        "output_sha256": sha256_file(output_path),
        "parent_companies_sha256": sha256_file(parent_output_path)
        if parent_output_path.exists()
        else None,
    }
    qc_path = settings.paths.outputs_dir / "qc" / "ghgrp_download.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="ghgrp_download",
        status="completed",
        outputs=[str(output_path), str(parent_output_path)],
        qc_path=str(qc_path),
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
