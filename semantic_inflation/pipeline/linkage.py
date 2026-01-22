from __future__ import annotations

from typing import Any

import pandas as pd

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io_utils import is_complete, write_json


def build_linkage(context: PipelineContext) -> dict[str, Any]:
    settings = context.settings
    manifest_path = settings.paths.outputs_dir / "manifests" / "linkage.json"
    output_path = settings.paths.processed_dir / "linkage.parquet"

    if is_complete(manifest_path, [output_path]):
        return {"skipped": True, "output": str(output_path)}

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
    write_json(settings.paths.outputs_dir / "qc" / "linkage_qc.json", qc_payload)

    manifest = {
        "stage": "linkage",
        "status": "completed",
        "timestamp": context.now_iso(),
        "output": str(output_path),
    }
    write_json(manifest_path, manifest)
    return qc_payload
