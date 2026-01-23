from __future__ import annotations

from pathlib import Path
from typing import Any
import zipfile
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import httpx
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
from semantic_inflation.sec.universe import load_corp_suffixes, normalize_company_name


def _resolve_path(path: str | Path, repo_root: Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else repo_root / p


def _find_column(columns: list[str], keywords: list[str]) -> str | None:
    lower_cols = {col.lower(): col for col in columns}
    for keyword in keywords:
        for col_lower, original in lower_cols.items():
            if keyword in col_lower:
                return original
    return None


def resolve_ghgrp_urls(
    data_sets_page: str, data_summary_label: str, parent_label: str
) -> dict[str, str]:
    response = httpx.get(data_sets_page, timeout=60.0)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    summary_url = None
    parent_url = None
    for link in soup.find_all("a"):
        text = (link.get_text() or "").strip()
        href = link.get("href")
        if not href:
            continue
        if data_summary_label.lower() in text.lower() and href.lower().endswith(".zip"):
            summary_url = summary_url or urljoin(data_sets_page, href)
        if parent_label.lower() in text.lower() and href.lower().endswith(".xlsb"):
            parent_url = parent_url or urljoin(data_sets_page, href)
    if not summary_url or not parent_url:
        raise FileNotFoundError("Unable to resolve GHGRP data summary or parent companies URL.")
    return {"data_summary": summary_url, "parent_companies": parent_url}


def parse_ghgrp_parent_companies(xlsb_path: Path, suffixes: set[str]) -> pd.DataFrame:
    parent_df = pd.read_excel(xlsb_path, engine="pyxlsb")
    id_col = _find_column(parent_df.columns.tolist(), ["facility id", "ghgrp facility id"])
    name_col = _find_column(parent_df.columns.tolist(), ["parent company", "reported parent"])
    ownership_col = _find_column(parent_df.columns.tolist(), ["ownership", "percent", "pct"])
    if not id_col or not name_col:
        raise ValueError("Could not identify GHGRP parent companies columns.")
    parent_df = parent_df.rename(
        columns={
            id_col: "ghgrp_facility_id",
            name_col: "parent_company_name_raw",
        }
    )
    if ownership_col:
        parent_df = parent_df.rename(columns={ownership_col: "ownership_pct"})
    else:
        parent_df["ownership_pct"] = pd.NA
    parent_df["ghgrp_facility_id"] = parent_df["ghgrp_facility_id"].astype(str)
    parent_df["parent_company_name_norm"] = parent_df["parent_company_name_raw"].astype(str).map(
        lambda value: normalize_company_name(value, suffixes)
    )
    return parent_df


def _extract_summary_table(zip_path: Path, extract_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as archive:
        candidates = [
            name
            for name in archive.namelist()
            if name.lower().endswith((".csv", ".xlsx", ".xls"))
        ]
        if not candidates:
            raise FileNotFoundError("No readable tables found in GHGRP data summary zip.")
        preferred = [name for name in candidates if "facility" in name.lower()]
        chosen = preferred[0] if preferred else candidates[0]
        extract_dir.mkdir(parents=True, exist_ok=True)
        extracted_path = extract_dir / Path(chosen).name
        with archive.open(chosen) as handle:
            extracted_path.write_bytes(handle.read())
    return extracted_path


def parse_ghgrp_facility_year(
    zip_path: Path, start_year: int, end_year: int, extract_dir: Path
) -> pd.DataFrame:
    extracted_path = _extract_summary_table(zip_path, extract_dir)
    if extracted_path.suffix.lower() == ".csv":
        df = pd.read_csv(extracted_path, low_memory=False)
    else:
        df = pd.read_excel(extracted_path)

    id_col = _find_column(df.columns.tolist(), ["facility id", "ghgrp facility id"])
    name_col = _find_column(df.columns.tolist(), ["facility name", "plant name"])
    frs_col = _find_column(df.columns.tolist(), ["frs", "registry"])
    reporting_col = _find_column(df.columns.tolist(), ["reporting year", "reporting_year"])
    emissions_col = _find_column(df.columns.tolist(), ["co2e", "mtco2e", "emissions"])

    if not id_col:
        raise ValueError("Missing GHGRP facility ID column in data summary.")
    if not name_col:
        raise ValueError("Missing GHGRP facility name column in data summary.")

    df = df.rename(
        columns={
            id_col: "ghgrp_facility_id",
            name_col: "facility_name",
        }
    )
    if frs_col:
        df = df.rename(columns={frs_col: "frs_id"})
    if reporting_col:
        df = df.rename(columns={reporting_col: "reporting_year"})
    if emissions_col:
        df = df.rename(columns={emissions_col: "emissions_mtco2e"})

    year_cols = [
        col
        for col in df.columns
        if str(col).strip().isdigit()
        and start_year <= int(str(col).strip()) <= end_year
    ]

    if year_cols:
        id_vars = ["ghgrp_facility_id", "facility_name"]
        if "frs_id" in df.columns:
            id_vars.append("frs_id")
        long_df = df.melt(
            id_vars=id_vars,
            value_vars=year_cols,
            var_name="reporting_year",
            value_name="emissions_mtco2e",
        )
    elif "reporting_year" in df.columns:
        long_df = df.copy()
    else:
        raise ValueError("Unable to identify reporting year columns in GHGRP data summary.")

    long_df["reporting_year"] = pd.to_numeric(long_df["reporting_year"], errors="coerce")
    long_df = long_df[
        long_df["reporting_year"].between(start_year, end_year, inclusive="both")
    ].copy()
    long_df["ghgrp_facility_id"] = long_df["ghgrp_facility_id"].astype(str)
    long_df["facility_name"] = long_df["facility_name"].astype(str)
    long_df["emissions_mtco2e"] = pd.to_numeric(
        long_df.get("emissions_mtco2e"), errors="coerce"
    )
    if "frs_id" in long_df.columns:
        long_df["frs_id"] = long_df["frs_id"].astype(str)
    else:
        long_df["frs_id"] = pd.NA
    return long_df[
        ["ghgrp_facility_id", "reporting_year", "facility_name", "emissions_mtco2e", "frs_id"]
    ]


def _merge_frs_ids(
    facility_year: pd.DataFrame, frs_zip: Path
) -> tuple[pd.DataFrame, pd.DataFrame]:
    program_links = parse_frs_program_links(frs_zip)
    mapping = build_ghgrp_to_frs(program_links, facility_year["ghgrp_facility_id"])
    merged = facility_year.merge(mapping, on="ghgrp_facility_id", how="left", suffixes=("", "_map"))
    merged["frs_id"] = merged["frs_id"].fillna(merged["frs_id_map"])
    merged = merged.drop(columns=["frs_id_map"])
    return merged, mapping


def download_ghgrp(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    facility_output_path = settings.paths.processed_dir / "ghgrp_facility_year.parquet"
    parent_output_path = settings.paths.processed_dir / "ghgrp_parent_companies.parquet"
    combined_output_path = (
        settings.paths.processed_dir / "ghgrp_facility_year_with_parent.parquet"
    )
    inputs_hash = compute_inputs_hash(
        {"stage": "ghgrp_download", "config": settings.model_dump(mode="json")}
    )
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "ghgrp_download")
    if should_skip_stage(
        manifest_path,
        [facility_output_path, parent_output_path, combined_output_path],
        inputs_hash,
        force,
    ):
        return StageResult(
            name="ghgrp_download",
            status="skipped",
            outputs=[
                str(facility_output_path),
                str(parent_output_path),
                str(combined_output_path),
            ],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    source_path = _resolve_path(settings.pipeline.ghgrp.fixture_path, context.repo_root)
    fixture_mode = settings.pipeline.ghgrp.use_fixture and source_path.exists()
    if fixture_mode:
        facility_df = pd.read_csv(source_path)
        parent_df = pd.DataFrame()
    else:
        if settings.runtime.offline:
            raise FileNotFoundError("GHGRP fixture missing while runtime.offline is true.")
        data_summary_url = settings.pipeline.ghgrp.data_summary_url
        parent_url = settings.pipeline.ghgrp.parent_companies_url
        if not data_summary_url or not parent_url:
            resolved = resolve_ghgrp_urls(
                settings.pipeline.ghgrp.data_sets_page,
                settings.pipeline.ghgrp.data_summary_label,
                settings.pipeline.ghgrp.parent_companies_label,
            )
            data_summary_url = data_summary_url or resolved["data_summary"]
            parent_url = parent_url or resolved["parent_companies"]

        headers = {"User-Agent": settings.sec.resolved_user_agent()}
        rps = min(settings.sec.max_requests_per_second, 10.0)
        log_path = settings.paths.raw_dir / "_manifests" / "ghgrp_downloads.jsonl"

        raw_dir = settings.paths.raw_dir / "epa" / "ghgrp"
        data_summary_zip = raw_dir / "data_summary_spreadsheets.zip"
        parent_companies_path = raw_dir / "reported_parent_companies.xlsb"

        download_with_cache(data_summary_url, data_summary_zip, headers, rps, log_path)
        download_with_cache(parent_url, parent_companies_path, headers, rps, log_path)

        suffixes = load_corp_suffixes(
            _resolve_path(settings.dictionaries.corp_suffixes_path, context.repo_root)
        )
        parent_df = parse_ghgrp_parent_companies(parent_companies_path, suffixes)
        facility_df = parse_ghgrp_facility_year(
            data_summary_zip,
            settings.project.start_year,
            settings.project.end_year,
            raw_dir / "unzipped",
        )

        frs_share = facility_df["frs_id"].notna().mean()
        mapping_df = pd.DataFrame()
        if frs_share < 0.8:
            frs_zip = settings.paths.raw_dir / "epa" / "echo" / "frs_downloads.zip"
            if not frs_zip.exists():
                echo_headers = {"User-Agent": settings.sec.resolved_user_agent()}
                download_with_cache(
                    settings.pipeline.echo.frs_downloads_url, frs_zip, echo_headers, rps, log_path
                )
            facility_df, mapping_df = _merge_frs_ids(facility_df, frs_zip)
            mapping_path = settings.paths.processed_dir / "ghgrp_to_frs.parquet"
            mapping_path.parent.mkdir(parents=True, exist_ok=True)
            if not mapping_df.empty:
                mapping_df.to_parquet(mapping_path, index=False)

    facility_output_path.parent.mkdir(parents=True, exist_ok=True)
    facility_df.to_parquet(facility_output_path, index=False)
    parent_output_path.parent.mkdir(parents=True, exist_ok=True)
    parent_df.to_parquet(parent_output_path, index=False)

    if not parent_df.empty:
        combined_df = facility_df.merge(parent_df, on="ghgrp_facility_id", how="left")
    else:
        combined_df = facility_df.copy()
    combined_output_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(combined_output_path, index=False)

    warnings: list[str] = []
    years = sorted(facility_df.get("reporting_year", pd.Series(dtype=int)).dropna().unique().tolist())
    missing_frs_path = None
    frs_share = facility_df.get("frs_id", pd.Series(dtype=object)).notna().mean()
    if not fixture_mode:
        if settings.project.start_year not in years:
            warnings.append(
                f"Reporting year {settings.project.start_year} not present in GHGRP data."
            )
        if settings.project.end_year not in years:
            raise ValueError("Reporting year end_year not present in GHGRP data.")
        if len(facility_df) < 50_000:
            raise ValueError("GHGRP facility-year table is unexpectedly small.")

        if frs_share < 0.8:
            missing_frs = facility_df[facility_df["frs_id"].isna()][
                ["ghgrp_facility_id", "facility_name"]
            ].drop_duplicates()
            missing_frs_path = settings.paths.outputs_dir / "qc" / "ghgrp_missing_frs.csv"
            missing_frs_path.parent.mkdir(parents=True, exist_ok=True)
            missing_frs.to_csv(missing_frs_path, index=False)
            raise ValueError("FRS ID coverage below 80% for GHGRP facility-year data.")

    qc_payload = {
        "rows": len(facility_df),
        "columns": list(facility_df.columns),
        "output": str(facility_output_path),
        "parent_companies_output": str(parent_output_path),
        "parent_companies_rows": len(parent_df),
        "combined_output": str(combined_output_path),
        "output_sha256": sha256_file(facility_output_path),
        "parent_companies_sha256": sha256_file(parent_output_path)
        if parent_output_path.exists()
        else None,
        "year_range": {"min": min(years) if years else None, "max": max(years) if years else None},
        "frs_id_share": frs_share,
        "missing_frs_path": str(missing_frs_path) if missing_frs_path else None,
        "fixture_mode": fixture_mode,
    }
    qc_path = settings.paths.outputs_dir / "qc" / "ghgrp_download.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="ghgrp_download",
        status="completed",
        outputs=[
            str(facility_output_path),
            str(parent_output_path),
            str(combined_output_path),
        ],
        qc_path=str(qc_path),
        stats=qc_payload,
        warnings=warnings,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
