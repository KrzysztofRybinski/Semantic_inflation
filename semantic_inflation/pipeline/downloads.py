from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential


@dataclass(frozen=True)
class DownloadResult:
    url: str
    path: Path
    status_code: int
    sha256: str
    bytes_written: int
    content_type: str | None
    cached: bool


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def append_download_log(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


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
def _fetch_bytes(url: str, headers: dict[str, str], timeout: float) -> httpx.Response:
    with httpx.Client(headers=headers, timeout=timeout) as client:
        response = client.get(url)
        response.raise_for_status()
        return response


def _throttle_sleep(max_rps: float) -> None:
    time.sleep(max(0.1, 1.0 / max(max_rps, 0.1)))


def download_with_cache(
    url: str,
    destination: Path,
    headers: dict[str, str],
    max_rps: float,
    log_path: Path,
    timeout: float = 60.0,
) -> DownloadResult:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.stat().st_size > 0:
        sha = sha256_file(destination)
        result = DownloadResult(
            url=url,
            path=destination,
            status_code=200,
            sha256=sha,
            bytes_written=destination.stat().st_size,
            content_type=None,
            cached=True,
        )
        append_download_log(
            log_path,
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "url": url,
                "path": str(destination),
                "sha256": sha,
                "bytes": destination.stat().st_size,
                "cached": True,
            },
        )
        return result

    response = _fetch_bytes(url, headers=headers, timeout=timeout)
    destination.write_bytes(response.content)
    _throttle_sleep(max_rps)
    sha = sha256_bytes(response.content)
    result = DownloadResult(
        url=url,
        path=destination,
        status_code=response.status_code,
        sha256=sha,
        bytes_written=len(response.content),
        content_type=response.headers.get("content-type"),
        cached=False,
    )
    append_download_log(
        log_path,
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "url": url,
            "path": str(destination),
            "sha256": sha,
            "bytes": len(response.content),
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
            "cached": False,
        },
    )
    return result
