from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Iterable

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


class RateLimiter:
    def __init__(self, max_rps: float) -> None:
        self._max_rps = max(max_rps, 0.1)
        self._min_interval = 1.0 / self._max_rps
        self._last_time = 0.0

    def acquire(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_time = time.monotonic()


_LIMITERS: dict[float, RateLimiter] = {}


def append_manifest(path: Path, payload: dict[str, Any]) -> None:
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


def download_file(
    url: str,
    destination: Path,
    *,
    headers: dict[str, str] | None = None,
    max_rps: float | None = None,
    manifest_path: Path | None = None,
    timeout: float = 60.0,
    expected_sha256: str | None = None,
    extra_manifest: dict[str, Any] | None = None,
) -> DownloadResult:
    destination.parent.mkdir(parents=True, exist_ok=True)
    headers = headers or {}
    limiter = None
    if max_rps:
        limiter = _LIMITERS.setdefault(max_rps, RateLimiter(max_rps))

    def _log_result(
        status_code: int,
        sha: str,
        bytes_written: int,
        content_type: str | None,
        cached: bool,
    ) -> None:
        if not manifest_path:
            return
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "url": url,
            "path": str(destination),
            "sha256": sha,
            "bytes": bytes_written,
            "status_code": status_code,
            "content_type": content_type,
            "cached": cached,
        }
        if extra_manifest:
            payload.update(extra_manifest)
        append_manifest(manifest_path, payload)

    if destination.exists() and destination.stat().st_size > 0:
        sha = sha256_file(destination)
        if expected_sha256 and expected_sha256 != sha:
            destination.unlink()
        else:
            _log_result(200, sha, destination.stat().st_size, None, True)
            return DownloadResult(
                url=url,
                path=destination,
                status_code=200,
                sha256=sha,
                bytes_written=destination.stat().st_size,
                content_type=None,
                cached=True,
            )

    if limiter:
        limiter.acquire()
    response = _fetch_bytes(url, headers=headers, timeout=timeout)
    destination.write_bytes(response.content)
    print(f"Downloaded: {destination}")
    sha = sha256_bytes(response.content)
    _log_result(
        response.status_code,
        sha,
        len(response.content),
        response.headers.get("content-type"),
        False,
    )
    return DownloadResult(
        url=url,
        path=destination,
        status_code=response.status_code,
        sha256=sha,
        bytes_written=len(response.content),
        content_type=response.headers.get("content-type"),
        cached=False,
    )


def ensure_directories(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
