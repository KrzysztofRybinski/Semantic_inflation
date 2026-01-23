from __future__ import annotations

from pathlib import Path

import pandas as pd

from semantic_inflation.config import load_settings
from semantic_inflation.pipeline import PipelineContext
from semantic_inflation.pipeline.run_all import run_all


def _write_config(tmp_path: Path, repo_root: Path) -> Path:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
[sec]
user_agent = "Test Researcher (test@example.com)"
max_requests_per_second = 5

[paths]
data_dir = "{data_dir}"
outputs_dir = "{outputs_dir}"

[pipeline]
mode = "sample"
sample_frame = "ghgrp_matched"

[pipeline.sec]
filings_index_path = "{filings_index}"

[pipeline.ghgrp]
fixture_path = "{ghgrp_fixture}"

[pipeline.echo]
fixture_path = "{echo_fixture}"

[runtime]
offline = true
""".format(
            data_dir=tmp_path / "data",
            outputs_dir=tmp_path / "outputs",
            filings_index=repo_root / "data" / "fixtures" / "filings_index.csv",
            ghgrp_fixture=repo_root / "data" / "fixtures" / "ghgrp_sample.csv",
            echo_fixture=repo_root / "data" / "fixtures" / "echo_sample.csv",
        ),
        encoding="utf-8",
    )
    return config_path


def test_run_all_pipeline(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = _write_config(tmp_path, repo_root)
    settings = load_settings(config_path)
    context = PipelineContext(settings)

    result = run_all(context, force=True)
    assert "stages" in result
    assert "models" in result["stages"]

    panel_path = settings.paths.processed_dir / "panel.parquet"
    linkage_path = settings.paths.processed_dir / "linkage.parquet"

    assert panel_path.exists()
    assert linkage_path.exists()

    linkage = pd.read_parquet(linkage_path)
    assert "frs_id" in linkage.columns
    assert "enforcement_action_count" in linkage.columns
