from __future__ import annotations

from pathlib import Path

from semantic_inflation.net.download import DownloadResult, download_file, sha256_bytes, sha256_file


def download_with_cache(
    url: str,
    destination: Path,
    headers: dict[str, str],
    max_rps: float,
    log_path: Path,
    timeout: float = 60.0,
) -> DownloadResult:
    return download_file(
        url,
        destination,
        headers=headers,
        max_rps=max_rps,
        manifest_path=log_path,
        timeout=timeout,
    )
