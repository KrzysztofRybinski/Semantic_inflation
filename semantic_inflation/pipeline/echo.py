from __future__ import annotations

from pathlib import Path
from typing import Any
import zipfile

import pandas as pd

from semantic_inflation.epa.frs import build_ghgrp_to_frs, parse_frs_program_links
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


def _normalize_column(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _find_column(columns: list[str], keywords: list[str]) -> str | None:
    normalized_cols = {_normalize_column(col): col for col in columns}
    for keyword in keywords:
        normalized_keyword = _normalize_column(keyword)
        for normalized_col, original in normalized_cols.items():
            if normalized_keyword and normalized_keyword in normalized_col:
                return original
    return None


def _find_column_with_tokens(columns: list[str], tokens: list[str]) -> str | None:
    normalized_cols = {_normalize_column(col): col for col in columns}
    normalized_tokens = [_normalize_column(token) for token in tokens if token]
    if not normalized_tokens:
        return None
    for normalized_col, original in normalized_cols.items():
        if all(token in normalized_col for token in normalized_tokens):
            return original
    return None


def _find_date_column(columns: list[str]) -> str | None:
    date_keywords = [
        "action_date",
        "actiondate",
        "enf_action_date",
        "enfactiondate",
        "enforcement_action_date",
        "final_action_date",
        "finalactiondate",
        "final action date",
        "case_status_date",
        "activity_status_date",
        "case_date",
        "case_action_date",
        "caseactiondate",
        "case action date",
        "action date",
    ]
    date_col = _find_column(columns, date_keywords)
    if date_col:
        return date_col
    date_col = _find_column_with_tokens(columns, ["action", "date"])
    if date_col:
        return date_col
    date_col = _find_column_with_tokens(columns, ["case", "date"])
    if date_col:
        return date_col
    normalized_cols = {_normalize_column(col): col for col in columns}
    for normalized_col, original in normalized_cols.items():
        if "date" in normalized_col and (
            "action" in normalized_col
            or "final" in normalized_col
            or "case" in normalized_col
        ):
            return original
    return None


def _select_case_csv(archive: zipfile.ZipFile) -> str:
    candidates = [name for name in archive.namelist() if name.lower().endswith(".csv")]
    if not candidates:
        raise FileNotFoundError("No CSV files found in case_downloads.zip")
    preferred = [
        name for name in candidates if "case" in name.lower() or "enf" in name.lower()
    ]
    return preferred[0] if preferred else candidates[0]


def _parse_case_downloads(
    case_zip: Path, start_year: int, end_year: int
) -> tuple[pd.DataFrame, str]:
    with zipfile.ZipFile(case_zip) as archive:
        chosen = _select_case_csv(archive)
        with archive.open(chosen) as handle:
            df = pd.read_csv(handle, low_memory=False)

    frs_col = _find_column(
        df.columns.tolist(),
        [
            "registry_id",
            "registryid",
            "frs_id",
            "frsid",
            "frs_registry_id",
            "frs registry id",
            "epa_registry_id",
            "registry identifier",
        ],
    )
    frs_source = "registry_id"
    date_col = _find_date_column(df.columns.tolist())
    penalty_col = _find_column(
        df.columns.tolist(),
        [
            "penalty",
            "civil_penalty",
            "civilpenalty",
            "penalty_amount",
        ],
    )
    if not frs_col:
        frs_col = _find_column_with_tokens(df.columns.tolist(), ["registry", "id"])
    if not frs_col:
        frs_col = _find_column_with_tokens(df.columns.tolist(), ["frs", "id"])
    if not frs_col:
        frs_col = _find_column(
            df.columns.tolist(),
            [
                "case_number",
                "casenumber",
                "activity_id",
                "activityid",
            ],
        )
        if frs_col:
            frs_source = "case_identifier"
    if not date_col:
        date_col = _find_date_column(df.columns.tolist())
    if not frs_col or not date_col:
        column_list = ", ".join(df.columns.astype(str).tolist())
        raise ValueError(
            "Unable to identify registry ID or action date columns in case data. "
            f"Columns observed: {column_list}"
        )

    df = df.rename(columns={frs_col: "frs_id", date_col: "action_date"})
    if penalty_col:
        df = df.rename(columns={penalty_col: "penalty_amount"})
    else:
        df["penalty_amount"] = 0.0

    df["frs_id"] = df["frs_id"].astype(str)
    df["action_date"] = pd.to_datetime(df["action_date"], errors="coerce")
    df["reporting_year"] = df["action_date"].dt.year
    df = df[df["reporting_year"].between(start_year, end_year, inclusive="both")]
    df["penalty_amount"] = pd.to_numeric(df["penalty_amount"], errors="coerce").fillna(0.0)

    grouped = (
        df.groupby(["frs_id", "reporting_year"], as_index=False)
        .agg(enforcement_action_count=("frs_id", "size"), penalty_amount=("penalty_amount", "sum"))
        .copy()
    )
    return grouped, frs_source


def download_echo(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = settings.paths.processed_dir / "echo_facility_year.parquet"
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
    fixture_mode = settings.pipeline.echo.use_fixture and source_path.exists()
    if fixture_mode:
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
        log_path = settings.paths.raw_dir / "_manifests" / "echo_downloads.jsonl"

        raw_dir = settings.paths.raw_dir / "epa" / "echo"
        case_zip = raw_dir / "case_downloads.zip"
        frs_zip = raw_dir / "frs_downloads.zip"
        download_with_cache(case_url, case_zip, headers, rps, log_path)
        download_with_cache(frs_url, frs_zip, headers, rps, log_path)

        df, frs_id_source = _parse_case_downloads(
            case_zip,
            settings.project.start_year,
            settings.project.end_year,
        )

        program_links = parse_frs_program_links(frs_zip)
        program_links_path = settings.paths.processed_dir / "frs_program_links.parquet"
        program_links_path.parent.mkdir(parents=True, exist_ok=True)
        program_links.to_parquet(program_links_path, index=False)

        ghgrp_facilities = settings.paths.processed_dir / "ghgrp_facility_year.parquet"
        if ghgrp_facilities.exists():
            ghgrp_df = pd.read_parquet(ghgrp_facilities)
            ghgrp_to_frs = build_ghgrp_to_frs(program_links, ghgrp_df["ghgrp_facility_id"])
            ghgrp_to_frs_path = settings.paths.processed_dir / "ghgrp_to_frs.parquet"
            ghgrp_to_frs.to_parquet(ghgrp_to_frs_path, index=False)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    if not fixture_mode:
        if len(df) < 2:
            raise ValueError("ECHO enforcement table is unexpectedly small.")
        frs_share = df["frs_id"].notna().mean()
        if frs_id_source == "registry_id" and frs_share < 0.95:
            raise ValueError("FRS ID coverage below expected threshold in ECHO enforcement data.")
        years = sorted(df["reporting_year"].dropna().unique().tolist())
        if len(years) < 2:
            raise ValueError("ECHO enforcement table does not span multiple years.")
    else:
        years = sorted(df.get("reporting_year", pd.Series(dtype=int)).dropna().unique().tolist())
        frs_id_source = "fixture"

    qc_payload = {
        "rows": len(df),
        "columns": list(df.columns),
        "output": str(output_path),
        "output_sha256": sha256_file(output_path),
        "year_range": {"min": min(years) if years else None, "max": max(years) if years else None},
        "fixture_mode": fixture_mode,
        "frs_id_source": frs_id_source,
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
