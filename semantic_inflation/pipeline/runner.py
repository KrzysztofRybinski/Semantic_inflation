from __future__ import annotations

from typing import Any

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.doctor import run_doctor
from semantic_inflation.pipeline.echo import download_echo
from semantic_inflation.pipeline.features import compute_sec_features
from semantic_inflation.pipeline.ghgrp import download_ghgrp
from semantic_inflation.pipeline.linkage import build_linkage
from semantic_inflation.pipeline.models import run_models
from semantic_inflation.pipeline.panel import build_panel
from semantic_inflation.pipeline.sec import download_sec_filings
from semantic_inflation.pipeline.state import StageResult


def run_pipeline(context: PipelineContext, *, force: bool = False) -> dict[str, Any]:
    results: dict[str, Any] = {}
    stages: list[tuple[str, callable[[PipelineContext, bool], StageResult]]] = [
        ("doctor", run_doctor),
        ("sec_download", download_sec_filings),
        ("sec_features", compute_sec_features),
        ("ghgrp_download", download_ghgrp),
        ("echo_download", download_echo),
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
