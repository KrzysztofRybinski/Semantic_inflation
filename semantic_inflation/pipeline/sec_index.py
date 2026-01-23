from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import re
import time
from typing import Any, Iterable

import httpx
import pandas as pd
from rapidfuzz import fuzz, process

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.downloads import download_with_cache, sha256_bytes, sha256_file
from semantic_inflation.pipeline.io import write_json
from semantic_inflation.pipeline.state import (
    StageResult,
    compute_inputs_hash,
    should_skip_stage,
    stage_manifest_path,
    write_stage_manifest,
)


@dataclass(frozen=True)
class FilingCandidate:
    cik: str
    form: str
    filing_date: str | None
    report_date: str | None
    accession_number: str | None
    primary_document: str | None
    company_name: str | None

    def archive_dir(self) -> str | None:
        if not self.accession_number:
            return None
        return self.accession_number.replace("-", "")

    def source_url(self) -> str | None:
        if not self.archive_dir() or not self.primary_document:
            return None
        try:
            cik_int = int(self.cik)
        except ValueError:
            return None
        return (
            f"https://www.sec.gov/Archives/edgar/data/{cik_int}/"
            f"{self.archive_dir()}/{self.primary_document}"
        )

    def filing_year(self) -> int | None:
        for date_value in [self.report_date, self.filing_date]:
            if date_value:
                try:
                    return int(date_value[:4])
                except ValueError:
                    continue
        return None

    def is_amendment(self) -> bool:
        return self.form.upper().endswith("/A")


def _resolve_path(path: str | Path, repo_root: Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else repo_root / p


def _load_suffixes(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip().upper()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def _normalize_name(name: str, suffixes: set[str]) -> str:
    cleaned = re.sub(r"[^\w\s]", " ", name.upper())
    tokens = [tok for tok in cleaned.split() if tok and tok not in suffixes]
    return " ".join(tokens)


def _find_column(columns: Iterable[str], keywords: list[str]) -> str | None:
    lower_cols = {col.lower(): col for col in columns}
    for keyword in keywords:
        for col_lower, original in lower_cols.items():
            if keyword in col_lower:
                return original
    return None


def _load_parent_companies(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing GHGRP parent companies file: {path}")
    if path.suffix.lower() in {".xlsb", ".xls", ".xlsx"}:
        return pd.read_excel(path, engine="pyxlsb" if path.suffix.lower() == ".xlsb" else None)
    return pd.read_csv(path)


def _load_sec_company_tickers(
    url: str,
    destination: Path,
    headers: dict[str, str],
    max_rps: float,
    log_path: Path,
) -> pd.DataFrame:
    download_with_cache(url, destination, headers, max_rps, log_path)
    payload = json.loads(destination.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        rows = list(payload.values())
    else:
        rows = payload
    return pd.DataFrame(rows)


def _extract_filings(payload: dict[str, Any], cik: str) -> list[FilingCandidate]:
    filings = payload.get("filings", {})
    recent = filings.get("recent", {})
    candidates: list[FilingCandidate] = []
    if recent:
        count = len(recent.get("accessionNumber", []))
        for idx in range(count):
            candidates.append(
                FilingCandidate(
                    cik=cik,
                    form=recent.get("form", [None])[idx],
                    filing_date=recent.get("filingDate", [None])[idx],
                    report_date=recent.get("reportDate", [None])[idx],
                    accession_number=recent.get("accessionNumber", [None])[idx],
                    primary_document=recent.get("primaryDocument", [None])[idx],
                    company_name=payload.get("name"),
                )
            )
    return candidates


def _fetch_submissions(
    cik: str,
    cache_dir: Path,
    headers: dict[str, str],
    max_rps: float,
    log_path: Path,
) -> list[FilingCandidate]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    cache_path = cache_dir / f"CIK{cik}.json"
    download_with_cache(submissions_url, cache_path, headers, max_rps, log_path)
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    candidates = _extract_filings(payload, cik)
    for extra in payload.get("filings", {}).get("files", []):
        name = extra.get("name")
        if not name:
            continue
        extra_url = f"https://data.sec.gov/submissions/{name}"
        extra_path = cache_dir / name
        download_with_cache(extra_url, extra_path, headers, max_rps, log_path)
        extra_payload = json.loads(extra_path.read_text(encoding="utf-8"))
        candidates.extend(_extract_filings(extra_payload, cik))
    return candidates


def _select_filings(
    candidates: list[FilingCandidate],
    start_year: int,
    end_year: int,
    forms: list[str],
) -> list[FilingCandidate]:
    allowed_forms = {form.upper() for form in forms}
    allowed_forms.update({"10-K", "10-K/A", "10-K405"})
    filtered = [
        c
        for c in candidates
        if c.form and c.form.upper() in allowed_forms and c.filing_year() is not None
    ]
    grouped: dict[tuple[str, int], list[FilingCandidate]] = {}
    for cand in filtered:
        year = cand.filing_year()
        if year is None or year < start_year or year > end_year:
            continue
        grouped.setdefault((cand.cik, year), []).append(cand)

    selected: list[FilingCandidate] = []
    for _, group in grouped.items():
        non_amendments = [c for c in group if not c.is_amendment()]
        pool = non_amendments or group
        chosen = max(
            pool,
            key=lambda c: (
                c.filing_date or "",
                c.accession_number or "",
            ),
        )
        selected.append(chosen)
    return selected


def _assert_unique(records: list[dict[str, Any]]) -> None:
    seen: set[tuple[str, int]] = set()
    for row in records:
        key = (row["cik"], int(row["filing_year"]))
        if key in seen:
            raise ValueError(f"Duplicate filing index key: {key}")
        seen.add(key)


def _validate_urls(records: list[dict[str, Any]]) -> None:
    pattern = re.compile(r"^https://www\.sec\.gov/Archives/edgar/data/\d+/\d+/.+")
    for row in records:
        url = row.get("source_url") or ""
        if url and not pattern.match(url):
            raise ValueError(f"Invalid SEC Archives URL: {url}")


def _sample_urls(
    urls: list[str],
    headers: dict[str, str],
    max_rps: float,
    sample_size: int,
) -> list[dict[str, Any]]:
    sampled = random.sample(urls, k=min(sample_size, len(urls)))
    results: list[dict[str, Any]] = []
    with httpx.Client(headers=headers, timeout=30.0) as client:
        for url in sampled:
            response = client.get(url)
            results.append(
                {
                    "url": url,
                    "status_code": response.status_code,
                    "content_type": response.headers.get("content-type"),
                    "sha256": sha256_bytes(response.content)
                    if response.status_code < 400
                    else None,
                }
            )
            time.sleep(max(0.1, 1.0 / max(max_rps, 0.1)))
    return results


def build_sec_filings_index(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    if not settings.pipeline.sec.build_index:
        return StageResult(
            name="sec_index",
            status="skipped",
            outputs=[],
            warnings=["sec.build_index is disabled"],
        )
    if settings.runtime.offline:
        raise ValueError("SEC filings index build requires network access (runtime.offline=true).")

    output_path = _resolve_path(settings.pipeline.sec.filings_index_path, context.repo_root)
    crosswalk_path = settings.paths.processed_dir / "crosswalk_parent_to_cik.csv"
    log_path = settings.paths.outputs_dir / "qc" / "download_log.jsonl"
    inputs_hash = compute_inputs_hash(
        {
            "stage": "sec_index",
            "config": settings.model_dump(mode="json"),
            "output_path": str(output_path),
        }
    )
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "sec_index")
    if should_skip_stage(manifest_path, [output_path, crosswalk_path], inputs_hash, force):
        return StageResult(
            name="sec_index",
            status="skipped",
            outputs=[str(output_path), str(crosswalk_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    suffixes = _load_suffixes(_resolve_path(settings.dictionaries.corp_suffixes_path, context.repo_root))
    parent_path = _resolve_path(settings.pipeline.ghgrp.parent_companies_path, context.repo_root)
    parent_df = _load_parent_companies(parent_path)
    parent_name_col = _find_column(parent_df.columns, ["parent company", "parent_company"])
    if not parent_name_col:
        raise ValueError("Could not identify parent company name column in GHGRP file.")
    parent_df = parent_df.rename(columns={parent_name_col: "parent_company_name_raw"})
    parent_df["parent_company_name_norm"] = parent_df["parent_company_name_raw"].astype(str).map(
        lambda value: _normalize_name(value, suffixes)
    )

    headers = {"User-Agent": settings.sec.resolved_user_agent()}
    rps = min(settings.sec.max_requests_per_second, 10.0)
    sec_tickers_path = settings.paths.raw_dir / "sec" / "company_tickers.json"
    sec_df = _load_sec_company_tickers(
        settings.pipeline.sec.company_tickers_url,
        sec_tickers_path,
        headers,
        rps,
        log_path,
    )
    sec_name_col = _find_column(sec_df.columns, ["title", "name"])
    if not sec_name_col:
        raise ValueError("Could not find SEC company name column in company_tickers.json.")
    sec_df["sec_name_norm"] = sec_df[sec_name_col].astype(str).map(
        lambda value: _normalize_name(value, suffixes)
    )

    sec_choices = sec_df["sec_name_norm"].tolist()
    matches: list[dict[str, Any]] = []
    for raw, norm in (
        parent_df[["parent_company_name_raw", "parent_company_name_norm"]]
        .drop_duplicates()
        .itertuples(index=False)
    ):
        match = process.extractOne(norm, sec_choices, scorer=fuzz.token_sort_ratio)
        if match:
            match_name, score, idx = match
            sec_row = sec_df.iloc[idx]
            matches.append(
                {
                    "parent_company_name_raw": raw,
                    "parent_company_name_norm": norm,
                    "matched_cik": str(sec_row.get("cik_str") or "").zfill(10),
                    "matched_sec_name": sec_row.get(sec_name_col),
                    "match_score": int(score),
                    "match_method": "fuzzy",
                    "match_tier": "high"
                    if score >= settings.linkage.fuzzy_threshold_high
                    else "medium"
                    if score >= settings.linkage.fuzzy_threshold_medium
                    else "low",
                    "manual_override": False,
                }
            )
        else:
            matches.append(
                {
                    "parent_company_name_raw": raw,
                    "parent_company_name_norm": norm,
                    "matched_cik": "",
                    "matched_sec_name": "",
                    "match_score": 0,
                    "match_method": "unmatched",
                    "match_tier": "low",
                    "manual_override": False,
                }
            )

    overrides_path = _resolve_path(settings.linkage.manual_overrides_path, context.repo_root)
    if overrides_path.exists():
        overrides = pd.read_csv(overrides_path)
        rename_map: dict[str, str] = {}
        parent_col = _find_column(overrides.columns, ["parent_company_name_raw", "parent company"])
        cik_col = _find_column(overrides.columns, ["matched_cik", "cik"])
        if parent_col:
            rename_map[parent_col] = "parent_company_name_raw"
        if cik_col:
            rename_map[cik_col] = "matched_cik"
        if rename_map:
            overrides = overrides.rename(columns=rename_map)
        if "parent_company_name_raw" in overrides.columns and "matched_cik" in overrides.columns:
            override_map = overrides.set_index("parent_company_name_raw")["matched_cik"].to_dict()
            for row in matches:
                override = override_map.get(row["parent_company_name_raw"])
                if override:
                    row["matched_cik"] = str(override).zfill(10)
                    row["match_method"] = "manual"
                    row["manual_override"] = True
                    row["match_tier"] = "high"

    crosswalk_df = pd.DataFrame(matches)
    crosswalk_path.parent.mkdir(parents=True, exist_ok=True)
    crosswalk_df.to_csv(crosswalk_path, index=False)

    matched_ciks = sorted({row["matched_cik"] for row in matches if row["matched_cik"]})

    submissions_cache = settings.paths.raw_dir / "sec" / "submissions"
    candidates: list[FilingCandidate] = []
    for cik in matched_ciks:
        candidates.extend(
            _fetch_submissions(
                cik,
                submissions_cache,
                headers,
                rps,
                log_path,
            )
        )

    selected = _select_filings(
        candidates,
        settings.project.start_year,
        settings.project.end_year,
        settings.project.filing_forms,
    )

    rows: list[dict[str, Any]] = []
    for cand in selected:
        archive_dir = cand.archive_dir()
        source_url = cand.source_url()
        rows.append(
            {
                "cik": cand.cik,
                "filing_year": cand.filing_year(),
                "source_url": source_url or "",
                "file_path": "",
                "form": cand.form,
                "filing_date": cand.filing_date or "",
                "report_date": cand.report_date or "",
                "accession_number": cand.accession_number or "",
                "primary_document": cand.primary_document or "",
                "archive_dir": archive_dir or "",
                "company_name_sec": cand.company_name or "",
            }
        )

    _assert_unique(rows)
    _validate_urls(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "cik",
        "filing_year",
        "source_url",
        "file_path",
        "form",
        "filing_date",
        "report_date",
        "accession_number",
        "primary_document",
        "archive_dir",
        "company_name_sec",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)

    qc_payload: dict[str, Any] = {
        "rows": len(rows),
        "output": str(output_path),
        "crosswalk": str(crosswalk_path),
        "index_sha256": sha256_file(output_path) if output_path.exists() else None,
        "crosswalk_sha256": sha256_file(crosswalk_path) if crosswalk_path.exists() else None,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "dictionary_sha256": sha256_file(
            _resolve_path(settings.dictionaries.dict_path, context.repo_root)
        ),
    }

    warnings: list[str] = []
    if rows:
        sampled = _sample_urls(
            [row["source_url"] for row in rows if row.get("source_url")],
            headers,
            rps,
            sample_size=20,
        )
        qc_payload["sampled_urls"] = sampled
        for entry in sampled:
            status = entry.get("status_code")
            content_type = (entry.get("content_type") or "").lower()
            if status != 200:
                warnings.append(f"Non-200 status for sample URL: {entry.get('url')}")
            if status == 200 and not ("html" in content_type or "text" in content_type):
                warnings.append(f"Unexpected content-type for sample URL: {entry.get('url')}")

    qc_path = settings.paths.outputs_dir / "qc" / "sec_index.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="sec_index",
        status="completed",
        outputs=[str(output_path), str(crosswalk_path)],
        qc_path=str(qc_path),
        stats=qc_payload,
        warnings=warnings,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
