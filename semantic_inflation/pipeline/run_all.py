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


def run_all(context: PipelineContext) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "doctor": run_doctor(context),
        "sec_download": download_sec_filings(context),
        "sec_features": compute_sec_features(context),
        "ghgrp": download_ghgrp(context),
        "echo": download_echo(context),
        "linkage": build_linkage(context),
        "panel": build_panel(context),
        "models": run_models(context),
    }
    return summary
