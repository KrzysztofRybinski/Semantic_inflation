from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io import write_json
from semantic_inflation.pipeline.state import (
    StageResult,
    compute_inputs_hash,
    should_skip_stage,
    stage_manifest_path,
    write_stage_manifest,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PageFetchResult:
    payload: dict[str, Any]
    cached: bool
    api_url: str


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
def _post_json(url: str, payload: dict[str, Any], headers: dict[str, str], timeout: float) -> dict[str, Any]:
    with httpx.Client(headers=headers, timeout=timeout) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        return response.json()


def _resolve_page_dir(context: PipelineContext) -> Path:
    settings = context.settings
    configured = settings.pipeline.usaspending.pages_dir
    if configured:
        return configured if configured.is_absolute() else context.repo_root / configured
    return settings.paths.raw_dir / "usaspending" / "pages"


def _resolve_output_path(context: PipelineContext) -> Path:
    settings = context.settings
    configured = settings.pipeline.usaspending.output_path
    if configured:
        return configured if configured.is_absolute() else context.repo_root / configured
    return settings.paths.processed_dir / "usaspending_awards.parquet"


def _resolve_manifest_path(context: PipelineContext) -> Path:
    settings = context.settings
    configured = settings.pipeline.usaspending.manifest_path
    if configured:
        return configured if configured.is_absolute() else context.repo_root / configured
    return settings.paths.outputs_dir / "logs" / "usaspending_manifest.jsonl"


def _load_cached_page(path: Path, warnings: list[str]) -> dict[str, Any] | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        warnings.append(f"Cached USAspending page {path} was corrupt and will be re-fetched.")
        path.unlink(missing_ok=True)
        return None
    if not isinstance(payload, dict) or "results" not in payload:
        warnings.append(f"Cached USAspending page {path} missing results and will be re-fetched.")
        path.unlink(missing_ok=True)
        return None
    return payload


def _infer_total_pages(payload: dict[str, Any], page_size: int) -> int | None:
    meta = payload.get("page_metadata")
    if isinstance(meta, dict):
        total_pages = meta.get("total_pages")
        if isinstance(total_pages, int) and total_pages > 0:
            return total_pages
        total = meta.get("total")
        limit = meta.get("limit") or page_size
        if isinstance(total, int) and total >= 0 and isinstance(limit, int) and limit > 0:
            return int(math.ceil(total / limit))
    return None


def _has_next(payload: dict[str, Any], page: int, total_pages: int | None) -> bool:
    meta = payload.get("page_metadata")
    if isinstance(meta, dict) and "hasNext" in meta:
        return bool(meta.get("hasNext"))
    if total_pages is not None:
        return page < total_pages
    results = payload.get("results")
    return bool(results)


def _fetch_page(
    url: str,
    fallback_url: str | None,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: float,
    cache_path: Path,
    cache_pages: bool,
    warnings: list[str],
) -> PageFetchResult:
    if cache_pages:
        cached = _load_cached_page(cache_path, warnings)
        if cached is not None:
            return PageFetchResult(payload=cached, cached=True, api_url=url)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        response_payload = _post_json(url, payload, headers, timeout)
        used_url = url
    except httpx.HTTPStatusError as exc:
        if (
            exc.response.status_code == 404
            and fallback_url
            and fallback_url != url
        ):
            warnings.append(
                f"USAspending endpoint {url} returned 404; retrying with {fallback_url}."
            )
            response_payload = _post_json(fallback_url, payload, headers, timeout)
            used_url = fallback_url
        else:
            raise
    if cache_pages:
        cache_path.write_text(json.dumps(response_payload, sort_keys=True), encoding="utf-8")
    return PageFetchResult(payload=response_payload, cached=False, api_url=used_url)


def download_usaspending_awards(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = _resolve_output_path(context)
    inputs_hash = compute_inputs_hash(
        {"stage": "usaspending", "config": settings.model_dump(mode="json")}
    )
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "usaspending")
    if should_skip_stage(manifest_path, [output_path], inputs_hash, force):
        return StageResult(
            name="usaspending",
            status="skipped",
            outputs=[str(output_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    api_url = settings.pipeline.usaspending.api_url
    fallback_url = api_url.rstrip("/") if api_url.endswith("/") else f"{api_url}/"
    base_payload = settings.pipeline.usaspending.request_payload
    page_size = settings.pipeline.usaspending.page_size
    max_pages = settings.pipeline.usaspending.max_pages
    start_page = settings.pipeline.usaspending.start_page
    cache_pages = settings.pipeline.usaspending.cache_pages
    timeout = settings.runtime.request_timeout_seconds
    page_dir = _resolve_page_dir(context)
    log_path = _resolve_manifest_path(context)
    user_agent = settings.pipeline.usaspending.resolved_user_agent()
    headers = {"Accept": "application/json"}
    if user_agent:
        headers["User-Agent"] = user_agent

    warnings: list[str] = []
    records: list[dict[str, Any]] = []
    total_pages: int | None = None
    page = start_page
    page_count = 0
    while True:
        if max_pages is not None and page_count >= max_pages:
            warnings.append(
                f"USAspending download stopped after max_pages={max_pages} at page {page}."
            )
            break
        payload = dict(base_payload)
        payload["page"] = page
        payload["limit"] = page_size
        cache_path = page_dir / f"page_{page:04d}.json"
        page_result = _fetch_page(
            api_url,
            fallback_url,
            payload,
            headers,
            timeout,
            cache_path,
            cache_pages,
            warnings,
        )
        if page_result.api_url != api_url:
            api_url = page_result.api_url
            fallback_url = api_url.rstrip("/") if api_url.endswith("/") else f"{api_url}/"
        page_payload = page_result.payload
        page_results = page_payload.get("results", [])
        if not isinstance(page_results, list):
            raise ValueError(f"USAspending page {page} missing results list.")
        records.extend(page_results)
        page_count += 1
        total_pages = total_pages or _infer_total_pages(page_payload, page_size)
        status = "cached" if page_result.cached else "downloaded"
        LOGGER.info(
            "USAspending page %s %s with %s rows (cumulative %s)",
            page,
            status,
            len(page_results),
            len(records),
        )
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "page": page,
                        "rows": len(page_results),
                        "cached": page_result.cached,
                        "total_pages": total_pages,
                    },
                    sort_keys=True,
                )
                + "\n"
            )
        if not _has_next(page_payload, page, total_pages):
            break
        page += 1

    df = pd.json_normalize(records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    qc_payload = {
        "rows": len(df),
        "columns": list(df.columns),
        "pages": page_count,
        "output": str(output_path),
        "page_dir": str(page_dir),
        "total_pages": total_pages,
    }
    qc_path = settings.paths.outputs_dir / "qc" / "usaspending.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="usaspending",
        status="completed",
        outputs=[str(output_path)],
        qc_path=str(qc_path),
        warnings=warnings,
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
