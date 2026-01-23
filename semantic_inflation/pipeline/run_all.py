from __future__ import annotations

from typing import Any

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.runner import run_pipeline


def run_all(context: PipelineContext, force: bool = False) -> dict[str, Any]:
    return run_pipeline(context, force=force)
