from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pandas as pd

from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.io_utils import is_complete, write_json
from semantic_inflation.text.features import compute_features_from_file


def _resolve_path(path: str | Path, repo_root: Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else repo_root / p


def compute_sec_features(context: PipelineContext) -> dict[str, Any]:
    settings = context.settings
    manifest_path = settings.paths.outputs_dir / "manifests" / "sec_features.json"
    output_path = settings.paths.processed_dir / "sec_features.parquet"

    if is_complete(manifest_path, [output_path]):
        return {"skipped": True, "output": str(output_path)}

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
    write_json(settings.paths.outputs_dir / "qc" / "sec_features_qc.json", qc_payload)

    manifest = {
        "stage": "sec_features",
        "status": "completed",
        "timestamp": context.now_iso(),
        "output": str(output_path),
    }
    write_json(manifest_path, manifest)
    return qc_payload
