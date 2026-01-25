from __future__ import annotations

from typing import Any

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.doctor import run_doctor
from semantic_inflation.pipeline.echo import download_echo
from semantic_inflation.pipeline.features import compute_sec_features
from semantic_inflation.pipeline.ghgrp import download_ghgrp
from semantic_inflation.pipeline.linkage import build_linkage
from semantic_inflation.pipeline.models import run_models
from semantic_inflation.pipeline.parent_to_cik import build_parent_to_cik
from semantic_inflation.pipeline.panel import build_panel
from semantic_inflation.pipeline.sec import download_sec_filings
from semantic_inflation.pipeline.sec_index import build_sec_filings_index
from semantic_inflation.pipeline.state import StageResult
from semantic_inflation.pipeline.usaspending import download_usaspending_awards


def run_pipeline(context: PipelineContext, *, force: bool = False) -> dict[str, Any]:
    results: dict[str, Any] = {}
    if context.settings.pipeline.mode == "full" and (
        context.settings.pipeline.ghgrp.use_fixture or context.settings.pipeline.echo.use_fixture
    ):
        raise ValueError("Full pipeline mode cannot run with fixture-based inputs enabled.")

    stages: list[tuple[str, callable[[PipelineContext, bool], StageResult]]] = [
        ("doctor", run_doctor),
        ("usaspending", download_usaspending_awards),
        ("ghgrp_download", download_ghgrp),
        ("echo_download", download_echo),
        ("parent_to_cik", build_parent_to_cik),
        ("sec_index", build_sec_filings_index),
        ("sec_download", download_sec_filings),
        ("sec_features", compute_sec_features),
        ("linkage", build_linkage),
        ("panel", build_panel),
        ("models", run_models),
    ]

    for name, func in stages:
        results[name] = func(context, force)

    return {
        "status": "completed",
        "stages": {name: result.to_dict() for name, result in results.items()},
    }
