from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.downloads import sha256_file
from semantic_inflation.pipeline.io import write_json
from semantic_inflation.pipeline.state import (
    StageResult,
    compute_inputs_hash,
    should_skip_stage,
    stage_manifest_path,
    write_stage_manifest,
)
from semantic_inflation.sec.universe import (
    build_cik_universe,
    build_parent_to_cik_crosswalk,
    download_company_tickers,
    load_corp_suffixes,
)


def _select_parent(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ownership_pct"] = pd.to_numeric(df.get("ownership_pct"), errors="coerce")
    df["ownership_rank"] = df["ownership_pct"].fillna(-1)
    df = df.sort_values(["ghgrp_facility_id", "ownership_rank"], ascending=[True, False])
    return df.drop_duplicates(subset=["ghgrp_facility_id"])


def _build_ghgrp_matched(
    facility_df: pd.DataFrame,
    parent_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    output_path: Path,
    diagnostics_path: Path,
) -> pd.DataFrame:
    parent_selected = _select_parent(parent_df)
    merged = facility_df.merge(parent_selected, on="ghgrp_facility_id", how="left")
    merged = merged.merge(
        crosswalk_df[["parent_company_name_norm", "matched_cik", "match_tier"]],
        on="parent_company_name_norm",
        how="left",
    )
    merged = merged.rename(columns={"matched_cik": "cik"})
    unmatched = merged[merged["cik"].isna()][
        ["ghgrp_facility_id", "facility_name", "parent_company_name_raw"]
    ].drop_duplicates()
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    unmatched.to_csv(diagnostics_path, index=False)
    matched = merged[merged["cik"].notna()].copy()
    matched = matched[
        ["frs_id", "cik", "reporting_year", "facility_name", "emissions_mtco2e"]
    ].copy()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    matched.to_parquet(output_path, index=False)
    return matched


def build_parent_to_cik(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = settings.paths.processed_dir / "parent_to_cik.parquet"
    universe_path = settings.paths.processed_dir / "cik_universe_ghgrp.csv"
    ghgrp_matched_path = settings.paths.processed_dir / "ghgrp.parquet"
    inputs_hash = compute_inputs_hash(
        {"stage": "parent_to_cik", "config": settings.model_dump(mode="json")}
    )
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "parent_to_cik")
    if should_skip_stage(
        manifest_path, [output_path, universe_path, ghgrp_matched_path], inputs_hash, force
    ):
        return StageResult(
            name="parent_to_cik",
            status="skipped",
            outputs=[str(output_path), str(universe_path), str(ghgrp_matched_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    if settings.runtime.offline or settings.pipeline.ghgrp.use_fixture:
        fixture_path = (
            settings.pipeline.ghgrp.fixture_path
            if settings.pipeline.ghgrp.fixture_path.is_absolute()
            else context.repo_root / settings.pipeline.ghgrp.fixture_path
        )
        if not ghgrp_matched_path.exists() and fixture_path.exists():
            fixture_df = pd.read_csv(fixture_path)
            ghgrp_matched_path.parent.mkdir(parents=True, exist_ok=True)
            fixture_df.to_parquet(ghgrp_matched_path, index=False)
        if ghgrp_matched_path.exists():
            ghgrp_df = pd.read_parquet(ghgrp_matched_path)
            ciks = sorted({str(cik).zfill(10) for cik in ghgrp_df.get("cik", [])})
        else:
            ciks = []
        universe_df = pd.DataFrame(
            {
                "cik": ciks,
                "match_tier": ["high"] * len(ciks),
                "parent_company_name_norm": ["FIXTURE"] * len(ciks),
            }
        )
        universe_path.parent.mkdir(parents=True, exist_ok=True)
        universe_df.to_csv(universe_path, index=False)

        crosswalk_df = pd.DataFrame(
            {
                "parent_company_name_raw": ["FIXTURE"] * len(ciks),
                "parent_company_name_norm": ["FIXTURE"] * len(ciks),
                "matched_cik": ciks,
                "matched_sec_name": ["FIXTURE"] * len(ciks),
                "match_score": [100] * len(ciks),
                "match_method": ["fixture"] * len(ciks),
                "match_tier": ["high"] * len(ciks),
                "manual_override": [False] * len(ciks),
            }
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        crosswalk_df.to_parquet(output_path, index=False)

        qc_payload: dict[str, Any] = {
            "rows": len(crosswalk_df),
            "output": str(output_path),
            "output_sha256": sha256_file(output_path) if output_path.exists() else None,
            "match_counts": {"high": len(ciks)},
            "match_rate": 1.0 if ciks else 0.0,
            "score_distribution": {},
            "unmatched_top_emissions": [],
            "universe_rows": len(universe_df),
            "ghgrp_matched_rows": len(ciks),
            "fixture_mode": True,
        }
        qc_path = settings.paths.outputs_dir / "qc" / "parent_to_cik_qc.json"
        write_json(qc_path, qc_payload)
        result = StageResult(
            name="parent_to_cik",
            status="completed",
            outputs=[str(output_path), str(universe_path), str(ghgrp_matched_path)],
            qc_path=str(qc_path),
            stats=qc_payload,
            inputs_hash=inputs_hash,
        )
        write_stage_manifest(manifest_path, result)
        return result

    parent_path = settings.paths.processed_dir / "ghgrp_parent_companies.parquet"
    facility_path = settings.paths.processed_dir / "ghgrp_facility_year.parquet"
    if not parent_path.exists():
        raise FileNotFoundError("Missing ghgrp_parent_companies.parquet for parent-to-CIK.")
    parent_df = pd.read_parquet(parent_path)

    headers = {"User-Agent": settings.sec.resolved_user_agent()}
    rps = min(settings.sec.max_requests_per_second, 10.0)
    manifest_log = settings.paths.raw_dir / "_manifests" / "sec_downloads.jsonl"
    sec_tickers_path = settings.paths.raw_dir / "sec" / "company_tickers.json"
    sec_df = download_company_tickers(
        settings.pipeline.sec.company_tickers_url,
        sec_tickers_path,
        headers,
        rps,
        manifest_log,
    )
    sec_df = sec_df.rename(columns={"title": "sec_name_raw"})

    suffixes = load_corp_suffixes(
        settings.dictionaries.corp_suffixes_path
        if settings.dictionaries.corp_suffixes_path.is_absolute()
        else context.repo_root / settings.dictionaries.corp_suffixes_path
    )
    crosswalk_df = build_parent_to_cik_crosswalk(
        parent_df,
        sec_df,
        suffixes=suffixes,
        fuzzy_high=settings.linkage.fuzzy_threshold_high,
        fuzzy_medium=settings.linkage.fuzzy_threshold_medium,
        overrides_path=(
            settings.linkage.manual_overrides_path
            if settings.linkage.manual_overrides_path.is_absolute()
            else context.repo_root / settings.linkage.manual_overrides_path
        ),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    crosswalk_df.to_parquet(output_path, index=False)

    universe_df = build_cik_universe(crosswalk_df, {"high", "medium"})
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    universe_df.to_csv(universe_path, index=False)

    ghgrp_matched = None
    diagnostics_path = settings.paths.outputs_dir / "qc" / "ghgrp_unmatched_parents.csv"
    if facility_path.exists():
        facility_df = pd.read_parquet(facility_path)
        ghgrp_matched = _build_ghgrp_matched(
            facility_df,
            parent_df,
            crosswalk_df,
            ghgrp_matched_path,
            diagnostics_path,
        )
        if (ghgrp_matched["emissions_mtco2e"] < 0).any():
            raise ValueError("Negative emissions detected in GHGRP matched data.")

    emissions_by_parent = None
    if facility_path.exists():
        facility_df = pd.read_parquet(facility_path)
        if "emissions_mtco2e" in facility_df.columns:
            emissions_by_parent = (
                facility_df.merge(
                    parent_df[["ghgrp_facility_id", "parent_company_name_norm"]],
                    on="ghgrp_facility_id",
                    how="left",
                )
                .groupby("parent_company_name_norm", as_index=False)
                .agg(emissions_mtco2e=("emissions_mtco2e", "sum"))
            )

    match_counts = crosswalk_df["match_tier"].value_counts().to_dict()
    matched = crosswalk_df[crosswalk_df["match_tier"].isin(["high", "medium"])]
    match_rate = len(matched) / len(crosswalk_df) if len(crosswalk_df) else 0

    unmatched_top = []
    if emissions_by_parent is not None:
        unmatched = crosswalk_df[crosswalk_df["match_tier"] == "low"]
        merged = unmatched.merge(
            emissions_by_parent,
            left_on="parent_company_name_norm",
            right_on="parent_company_name_norm",
            how="left",
        )
        merged = merged.sort_values("emissions_mtco2e", ascending=False).head(50)
        unmatched_top = merged[
            ["parent_company_name_raw", "parent_company_name_norm", "emissions_mtco2e"]
        ].to_dict(orient="records")

    qc_payload: dict[str, Any] = {
        "rows": len(crosswalk_df),
        "output": str(output_path),
        "output_sha256": sha256_file(output_path) if output_path.exists() else None,
        "match_counts": match_counts,
        "match_rate": match_rate,
        "score_distribution": crosswalk_df["match_score"].describe().to_dict()
        if "match_score" in crosswalk_df.columns
        else {},
        "unmatched_top_emissions": unmatched_top,
        "universe_rows": len(universe_df),
        "ghgrp_matched_rows": len(ghgrp_matched) if ghgrp_matched is not None else 0,
        "ghgrp_unmatched_path": str(diagnostics_path),
    }
    qc_path = settings.paths.outputs_dir / "qc" / "parent_to_cik_qc.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="parent_to_cik",
        status="completed",
        outputs=[str(output_path), str(universe_path), str(ghgrp_matched_path)],
        qc_path=str(qc_path),
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
