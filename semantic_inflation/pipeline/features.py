from __future__ import annotations

import csv
from pathlib import Path
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
from semantic_inflation.text.features import compute_features_from_file


def _resolve_path(path: str | Path, repo_root: Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else repo_root / p


def compute_sec_features(context: PipelineContext, force: bool = False) -> StageResult:
    settings = context.settings
    output_path = settings.paths.processed_dir / "sec_features.parquet"
    inputs_hash = compute_inputs_hash(
        {"stage": "sec_features", "config": settings.model_dump(mode="json")}
    )
    manifest_path = stage_manifest_path(settings.paths.outputs_dir, "sec_features")
    if should_skip_stage(manifest_path, [output_path], inputs_hash, force):
        return StageResult(
            name="sec_features",
            status="skipped",
            outputs=[str(output_path)],
            inputs_hash=inputs_hash,
            stats={"skipped": True},
        )

    index_path = _resolve_path(settings.pipeline.sec.filings_index_path, context.repo_root)
    rows: list[dict[str, Any]] = []

    with index_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            cik = (row.get("cik") or "").strip()
            if not cik:
                continue
            filing_year = int(row.get("filing_year") or 0)
            source_path = row.get("file_path")
            if source_path:
                file_path = _resolve_path(source_path, context.repo_root)
            else:
                file_path = settings.paths.raw_dir / "sec" / f"{cik}-{filing_year}.html"
            if not file_path.exists():
                raise FileNotFoundError(f"Missing SEC filing: {file_path}")

            result = compute_features_from_file(
                file_path,
                dictionary_version=settings.text.dictionary_version,
                min_sentence_chars=settings.text.min_sentence_chars,
                html_extractor=settings.text.html.extractor,
                drop_hidden=settings.text.html.drop_hidden,
                drop_ix_hidden=settings.text.html.drop_ix_hidden,
                unwrap_ix_tags=settings.text.html.unwrap_ix_tags,
                keep_tables=settings.text.html.keep_tables,
                table_cell_sep=settings.text.html.table_cell_sep,
                table_row_sep=settings.text.html.table_row_sep,
            )
            result["cik"] = cik
            result["filing_year"] = filing_year
            result["si_simple"] = float(result.get("A_share") or 0) - float(
                result.get("Q_share") or 0
            )
            rows.append(result)

    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    qc_payload = {
        "rows": len(df),
        "columns": list(df.columns),
        "output": str(output_path),
    }
    qc_path = settings.paths.outputs_dir / "qc" / "sec_features.json"
    write_json(qc_path, qc_payload)

    result = StageResult(
        name="sec_features",
        status="completed",
        outputs=[str(output_path)],
        qc_path=str(qc_path),
        stats=qc_payload,
        inputs_hash=inputs_hash,
    )
    write_stage_manifest(manifest_path, result)
    return result
