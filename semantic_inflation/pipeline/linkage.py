from __future__ import annotations

from typing import Any

import pandas as pd

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io import write_json
from semantic_inflation.pipeline.state import (
    StageResult,
    compute_inputs_hash,
    should_skip_stage,
    stage_manifest_path,
    write_stage_manifest,
)


def build_linkage(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = settings.paths.processed_dir / "linkage.parquet"
    inputs_hash = compute_inputs_hash({"stage": "linkage", "config": settings.model_dump(mode="json")})
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "linkage")
    if should_skip_stage(manifest_path, [output_path], inputs_hash, force):
        return StageResult(
            name="linkage",
            status="skipped",
            outputs=[str(output_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    ghgrp_path = settings.paths.processed_dir / "ghgrp.parquet"
    echo_path = settings.paths.processed_dir / "echo.parquet"

    ghgrp = pd.read_parquet(ghgrp_path)
    echo = pd.read_parquet(echo_path)

    for col in ["frs_id"]:
        if col in ghgrp.columns:
            ghgrp[col] = ghgrp[col].astype(str)
        if col in echo.columns:
            echo[col] = echo[col].astype(str)

    merged = ghgrp.merge(
        echo,
        on=[c for c in ["frs_id", "reporting_year"] if c in ghgrp.columns and c in echo.columns],
        how="left",
        suffixes=("_ghgrp", "_echo"),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(output_path, index=False)

    qc_payload = {
        "rows": len(merged),
        "columns": list(merged.columns),
        "join_key": "frs_id",
        "output": str(output_path),
    }
    qc_path = settings.paths.outputs_dir / "qc" / "linkage.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="linkage",
        status="completed",
        outputs=[str(output_path)],
        qc_path=str(qc_path),
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
