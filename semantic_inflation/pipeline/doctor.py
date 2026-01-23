from __future__ import annotations

from pathlib import Path
import zipfile

import httpx

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io import write_json
from semantic_inflation.pipeline.state import (
    StageResult,
    compute_inputs_hash,
    stage_manifest_path,
    write_stage_manifest,
)
from semantic_inflation.text.clean_html import html_to_text
from semantic_inflation.text.features import compute_features_from_file


SEC_SAMPLE_CIKS = ["0000320193", "0000051143", "0000789019"]


def _fetch_sample(url: str, dest: Path, *, max_bytes: int = 50_000) -> dict[str, str]:
    headers = {"Range": f"bytes=0-{max_bytes - 1}"}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(response.content[:max_bytes])
    preview = response.content[:max_bytes].decode("utf-8", errors="replace")
    return {"url": url, "bytes": str(len(response.content)), "preview": preview.splitlines()[:5]}


def _sec_sample(context: PipelineContext, sample_dir: Path) -> dict[str, object]:
    settings = context.settings
    user_agent = settings.sec.resolved_user_agent()
    headers = {"User-Agent": user_agent}
    samples: list[dict[str, object]] = []
    with httpx.Client(headers=headers, timeout=30.0) as client:
        for cik in SEC_SAMPLE_CIKS:
            submissions_url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
            submissions = client.get(submissions_url)
            submissions.raise_for_status()
            payload = submissions.json()
            recent = payload.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            accession_numbers = recent.get("accessionNumber", [])
            primary_docs = recent.get("primaryDocument", [])
            accession = None
            primary_doc = None
            for form, accession_number, doc in zip(forms, accession_numbers, primary_docs):
                if form == "10-K":
                    accession = accession_number
                    primary_doc = doc
                    break
            if not accession or not primary_doc:
                samples.append({"cik": cik, "status": "no_10k_found"})
                continue
            accession_slug = accession.replace("-", "")
            filing_url = (
                "https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{accession_slug}/{primary_doc}"
            )
            dest = sample_dir / "sec" / f"{cik}_{accession_slug}.html"
            response = client.get(filing_url)
            response.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(response.content)
            text = html_to_text(
                response.text,
                extractor=settings.text.html.extractor,
                drop_hidden=settings.text.html.drop_hidden,
                drop_ix_hidden=settings.text.html.drop_ix_hidden,
                unwrap_ix_tags=settings.text.html.unwrap_ix_tags,
                keep_tables=settings.text.html.keep_tables,
                table_cell_sep=settings.text.html.table_cell_sep,
                table_row_sep=settings.text.html.table_row_sep,
            )
            text_path = dest.with_suffix(".txt")
            text_path.write_text(text, encoding="utf-8")
            features = compute_features_from_file(
                dest,
                dictionary_version=settings.text.dictionary_version,
                min_sentence_chars=settings.text.min_sentence_chars,
                html_extractor=settings.text.html.extractor,
                drop_hidden=settings.text.html.drop_hidden,
                drop_ix_hidden=settings.text.html.drop_ix_hidden,
                unwrap_ix_tags=settings.text.html.unwrap_ix_tags,
                keep_tables=settings.text.html.keep_tables,
                table_cell_sep=settings.text.html.table_cell_sep,
                table_row_sep=settings.text.html.table_row_sep,
            )
            samples.append(
                {
                    "cik": cik,
                    "filing_url": filing_url,
                    "text_path": str(text_path),
                    "features": {
                        "env_sentences": features.get("env_sentence_count"),
                        "aspiration_share": features.get("A_share"),
                        "kpi_share": features.get("Q_share"),
                    },
                }
            )
    return {"sec_samples": samples}


def run_doctor(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    created_dirs: list[str] = []
    deleted_files: list[str] = []
    warnings: list[str] = []
    errors: list[str] = []
    fixes: list[str] = []

    required_dirs = [
        settings.paths.data_dir,
        settings.paths.raw_dir,
        settings.paths.interim_dir,
        settings.paths.processed_dir,
        settings.paths.outputs_dir,
        settings.paths.cache_dir,
        settings.paths.raw_dir / "sec",
        settings.paths.raw_dir / "epa" / "ghgrp",
        settings.paths.raw_dir / "epa" / "echo",
        settings.paths.outputs_dir / "qc",
        settings.paths.outputs_dir / "tables",
        settings.paths.outputs_dir / "figures",
    ]

    for path in required_dirs:
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            created_dirs.append(str(path))

    try:
        _ = settings.sec.resolved_user_agent()
    except ValueError as exc:
        errors.append(str(exc))

    raw_dir = settings.paths.raw_dir
    for candidate in raw_dir.rglob("*"):
        if candidate.is_file() and candidate.stat().st_size == 0:
            candidate.unlink()
            deleted_files.append(str(candidate))
            fixes.append(f"Removed zero-byte file: {candidate}")
        if candidate.is_file() and candidate.suffix.lower() == ".zip":
            if not zipfile.is_zipfile(candidate):
                candidate.unlink()
                deleted_files.append(str(candidate))
                fixes.append(f"Removed corrupted zip: {candidate}")

    network_checks: dict[str, object] = {}
    sample_dir = settings.paths.outputs_dir / "qc" / "doctor_samples"
    if settings.runtime.offline:
        warnings.append("Doctor running in offline mode; network checks skipped.")
    else:
        try:
            network_checks["sec"] = _sec_sample(context, sample_dir)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"SEC preflight failed: {exc}")

        if settings.pipeline.ghgrp.parent_companies_url:
            try:
                network_checks["ghgrp_parent"] = _fetch_sample(
                    settings.pipeline.ghgrp.parent_companies_url,
                    sample_dir / "ghgrp_parent_companies.sample",
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"GHGRP parent companies fetch failed: {exc}")
        if settings.pipeline.ghgrp.emissions_url:
            try:
                network_checks["ghgrp_emissions"] = _fetch_sample(
                    settings.pipeline.ghgrp.emissions_url,
                    sample_dir / "ghgrp_emissions.sample",
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"GHGRP emissions fetch failed: {exc}")
        if settings.pipeline.echo.exporter_url:
            try:
                network_checks["echo_exporter"] = _fetch_sample(
                    settings.pipeline.echo.exporter_url,
                    sample_dir / "echo_exporter.sample",
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"ECHO exporter fetch failed: {exc}")

    qc_payload = {
        "created_dirs": created_dirs,
        "deleted_files": deleted_files,
        "warnings": warnings,
        "fixes": fixes,
        "network_checks": network_checks,
        "status": "pass" if not errors else "fail",
        "errors": errors,
    }

    inputs_hash = compute_inputs_hash({"stage": "doctor", "config": settings.model_dump(mode="json")})
    result = StageResult(
        name="doctor",
        status="completed" if not errors else "failed",
        outputs=[str(settings.paths.outputs_dir / "qc" / "doctor.json")],
        qc_path=str(settings.paths.outputs_dir / "qc" / "doctor.json"),
        warnings=warnings,
        errors=errors,
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_json(settings.paths.outputs_dir / "qc" / "doctor.json", qc_payload)
    write_stage_manifest(stage_manifest_path(settings.paths.outputs_dir, "doctor"), result)
    if errors:
        raise RuntimeError("Doctor preflight failed; see outputs/qc/doctor.json for details.")
    return result
