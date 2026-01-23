from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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


@dataclass(frozen=True)
class SecFilingRecord:
    cik: str
    filing_year: int
    source_path: Path | None
    source_url: str | None
    primary_document: str | None


def _resolve_path(path: str | Path, repo_root: Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else repo_root / p


def _load_filings_index(context: PipelineContext) -> list[SecFilingRecord]:
    settings = context.settings
    index_path = _resolve_path(settings.pipeline.sec.filings_index_path, context.repo_root)
    if not index_path.exists():
        raise FileNotFoundError(f"Missing filings index: {index_path}")

    records: list[SecFilingRecord] = []
    with index_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            cik = (row.get("cik") or "").strip()
            if not cik:
                continue
            filing_year = int(row.get("filing_year") or 0)
            source_path = row.get("file_path")
            source_url = row.get("source_url")
            primary_document = row.get("primary_document")
            records.append(
                SecFilingRecord(
                    cik=cik,
                    filing_year=filing_year,
                    source_path=_resolve_path(source_path, context.repo_root)
                    if source_path
                    else None,
                    source_url=source_url or None,
                    primary_document=primary_document or None,
                )
            )

    if settings.pipeline.sec.max_filings:
        records = records[: settings.pipeline.sec.max_filings]
    return records


def download_sec_filings(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    raw_dir = settings.paths.raw_dir / "sec" / "filings"
    outputs: list[Path] = []
    filings = _load_filings_index(context)
    outputs = [
        raw_dir
        / rec.cik
        / str(rec.filing_year)
        / (rec.primary_document or f"{rec.cik}-{rec.filing_year}.html")
        for rec in filings
    ]
    inputs_hash = compute_inputs_hash(
        {
            "stage": "sec_download",
            "filings": [rec.__dict__ for rec in filings],
            "config": settings.model_dump(mode="json"),
        }
    )
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "sec_download")
    if should_skip_stage(manifest_path, outputs, inputs_hash, force):
        return StageResult(
            name="sec_download",
            status="skipped",
            outputs=[str(p) for p in outputs],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    headers = {"User-Agent": settings.sec.resolved_user_agent()}
    rps = min(settings.sec.max_requests_per_second, 10.0)
    log_path = settings.paths.raw_dir / "_manifests" / "sec_filings.jsonl"

    downloaded: list[str] = []
    manifest_rows: list[dict[str, Any]] = []
    for record, dest in zip(filings, outputs):
        if dest.exists():
            continue
        if record.source_path and record.source_path.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(record.source_path.read_bytes())
            downloaded.append(str(dest))
            manifest_rows.append(
                {
                    "cik": record.cik,
                    "filing_year": record.filing_year,
                    "url": None,
                    "local_path": str(dest),
                    "sha256": sha256_file(dest),
                    "bytes": dest.stat().st_size,
                    "status": "copied",
                }
            )
            continue
        if record.source_url:
            result = download_with_cache(record.source_url, dest, headers, rps, log_path)
            downloaded.append(str(dest))
            manifest_rows.append(
                {
                    "cik": record.cik,
                    "filing_year": record.filing_year,
                    "url": record.source_url,
                    "local_path": str(result.path),
                    "sha256": result.sha256,
                    "bytes": result.bytes_written,
                    "status": "cached" if result.cached else "downloaded",
                }
            )
            continue
        raise FileNotFoundError(
            f"No source path or URL for filing {record.cik} {record.filing_year}"
        )

    manifest_path = settings.paths.raw_dir / "sec" / "filings_manifest.parquet"
    if manifest_rows:
        manifest_df = pd.DataFrame(manifest_rows)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_df.to_parquet(manifest_path, index=False)
        success_rate = (manifest_df["status"] != "failed").mean()
    else:
        success_rate = 1.0

    failures_path = None
    if success_rate < 0.95:
        failures = [row for row in manifest_rows if row["status"] == "failed"]
        failures_path = settings.paths.outputs_dir / "qc" / "sec_download_failures.json"
        failures_path.parent.mkdir(parents=True, exist_ok=True)
        write_json(failures_path, {"failures": failures})
        raise ValueError("SEC download success rate below 95%.")

    qc_payload: dict[str, Any] = {
        "filings": len(filings),
        "downloaded": downloaded,
        "user_agent": settings.sec.resolved_user_agent(),
        "requests_per_second": rps,
        "manifest": str(manifest_path),
        "success_rate": success_rate,
        "failures_path": str(failures_path) if failures_path else None,
    }
    qc_path = settings.paths.outputs_dir / "qc" / "sec_download.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="sec_download",
        status="completed",
        outputs=[str(p) for p in outputs],
        qc_path=str(qc_path),
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
