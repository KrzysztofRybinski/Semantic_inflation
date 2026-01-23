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


def build_panel(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = settings.paths.processed_dir / "panel.parquet"
    inputs_hash = compute_inputs_hash({"stage": "panel", "config": settings.model_dump(mode="json")})
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "panel")
    if should_skip_stage(manifest_path, [output_path], inputs_hash, force):
        return StageResult(
            name="panel",
            status="skipped",
            outputs=[str(output_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    features = pd.read_parquet(settings.paths.processed_dir / "sec_features.parquet")
    linkage = pd.read_parquet(settings.paths.processed_dir / "linkage.parquet")

    features["cik"] = features["cik"].astype(str)
    if "cik" in linkage.columns:
        linkage["cik"] = linkage["cik"].astype(str)

    panel = features.merge(
        linkage,
        left_on=["cik", "filing_year"],
        right_on=["cik", "reporting_year"],
        how="left",
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(output_path, index=False)

    qc_payload = {
        "rows": len(panel),
        "columns": list(panel.columns),
        "output": str(output_path),
    }
    qc_path = settings.paths.outputs_dir / "qc" / "panel.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="panel",
        status="completed",
        outputs=[str(output_path)],
        qc_path=str(qc_path),
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
