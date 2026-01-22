from __future__ import annotations

from typing import Any

import pandas as pd

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io_utils import is_complete, write_json


def build_panel(context: PipelineContext) -> dict[str, Any]:
    settings = context.settings
    manifest_path = settings.paths.outputs_dir / "manifests" / "panel.json"
    output_path = settings.paths.processed_dir / "panel.parquet"

    if is_complete(manifest_path, [output_path]):
        return {"skipped": True, "output": str(output_path)}

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
    write_json(settings.paths.outputs_dir / "qc" / "panel_qc.json", qc_payload)

    manifest = {
        "stage": "panel",
        "status": "completed",
        "timestamp": context.now_iso(),
        "output": str(output_path),
    }
    write_json(manifest_path, manifest)
    return qc_payload
