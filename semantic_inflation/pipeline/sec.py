from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import time

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io_utils import is_complete, write_json


@dataclass(frozen=True)
class SecFilingRecord:
    cik: str
    filing_year: int
    source_path: Path | None
    source_url: str | None


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
            records.append(
                SecFilingRecord(
                    cik=cik,
                    filing_year=filing_year,
                    source_path=_resolve_path(source_path, context.repo_root)
                    if source_path
                    else None,
                    source_url=source_url or None,
                )
            )

    if settings.pipeline.sec.max_filings:
        records = records[: settings.pipeline.sec.max_filings]
    return records


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 500, 502, 503, 504}
    return isinstance(exc, httpx.TransportError)


@retry(
    retry=retry_if_exception(_should_retry),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=16),
    reraise=True,
)
def _download_with_throttle(url: str, destination: Path, headers: dict[str, str], rps: float) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    timeout = httpx.Timeout(60.0)
    with httpx.Client(headers=headers, timeout=timeout) as client:
        response = client.get(url)
        response.raise_for_status()
        destination.write_bytes(response.content)
        time.sleep(max(0.1, 1.0 / max(rps, 0.1)))


def download_sec_filings(context: PipelineContext) -> dict[str, Any]:
    settings = context.settings
    raw_dir = settings.paths.raw_dir / "sec"
    manifest_path = settings.paths.outputs_dir / "manifests" / "sec_download.json"

    filings = _load_filings_index(context)
    outputs = [raw_dir / f"{rec.cik}-{rec.filing_year}.html" for rec in filings]

    if is_complete(manifest_path, outputs):
        return {"skipped": True, "outputs": [str(p) for p in outputs]}

    headers = {"User-Agent": settings.sec.user_agent}
    rps = min(settings.sec.requests_per_second, 10.0)

    downloaded: list[str] = []
    for record, dest in zip(filings, outputs):
        if dest.exists():
            continue
        if record.source_path and record.source_path.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(record.source_path.read_bytes())
            downloaded.append(str(dest))
            continue
        if record.source_url:
            _download_with_throttle(record.source_url, dest, headers, rps)
            downloaded.append(str(dest))
            continue
        raise FileNotFoundError(
            f"No source path or URL for filing {record.cik} {record.filing_year}"
        )

    qc_payload = {
        "filings": len(filings),
        "downloaded": downloaded,
        "user_agent": settings.sec.user_agent,
        "requests_per_second": rps,
    }
    write_json(settings.paths.outputs_dir / "qc" / "sec_download_qc.json", qc_payload)

    manifest = {
        "stage": "sec_download",
        "status": "completed",
        "timestamp": context.now_iso(),
        "outputs": [str(p) for p in outputs],
    }
    write_json(manifest_path, manifest)
    return qc_payload
