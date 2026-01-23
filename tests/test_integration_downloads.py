import os
from pathlib import Path

import pytest

from semantic_inflation.config import (
    DictionarySettings,
    LinkageSettings,
    PathsSettings,
    PipelineEchoSettings,
    PipelineGhgrpSettings,
    PipelineSecSettings,
    PipelineSettings,
    ProjectSettings,
    RuntimeSettings,
    SecSettings,
    Settings,
)
from semantic_inflation.pipeline.context import PipelineContext
from semantic_inflation.pipeline.echo import download_echo
from semantic_inflation.pipeline.ghgrp import download_ghgrp
from semantic_inflation.pipeline.sec_index import build_sec_filings_index


def _integration_enabled() -> bool:
    return os.getenv("RUN_INTEGRATION") == "1" and bool(os.getenv("SEC_USER_AGENT"))


def _build_settings(tmp_path: Path) -> Settings:
    data_dir = tmp_path / "data"
    outputs_dir = tmp_path / "outputs"
    paths = PathsSettings(
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        processed_dir=data_dir / "processed",
        outputs_dir=outputs_dir,
        cache_dir=data_dir / "cache",
    )
    return Settings(
        project=ProjectSettings(start_year=2010, end_year=2023, filing_forms=["10-K"]),
        paths=paths,
        sec=SecSettings(user_agent=os.environ.get("SEC_USER_AGENT")),
        runtime=RuntimeSettings(offline=False),
        pipeline=PipelineSettings(
            sec=PipelineSecSettings(
                filings_index_path=data_dir / "raw" / "sec" / "filings_index.csv",
                build_index=True,
            ),
            ghgrp=PipelineGhgrpSettings(use_fixture=False),
            echo=PipelineEchoSettings(use_fixture=False),
        ),
        linkage=LinkageSettings(),
        dictionaries=DictionarySettings(),
    )


@pytest.mark.skipif(not _integration_enabled(), reason="Integration test disabled.")
def test_sec_index_integration(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    universe_path = settings.paths.processed_dir / "cik_universe_ghgrp.csv"
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    universe_path.write_text("cik,match_tier,parent_company_name_norm\n0000320193,high,APPLE\n")

    context = PipelineContext(settings)
    result = build_sec_filings_index(context, force=True)
    assert result.status == "completed"
    assert (settings.paths.raw_dir / "sec" / "filings_index.csv").exists()


@pytest.mark.skipif(not _integration_enabled(), reason="Integration test disabled.")
def test_ghgrp_echo_integration(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    context = PipelineContext(settings)
    ghgrp_result = download_ghgrp(context, force=True)
    echo_result = download_echo(context, force=True)

    assert ghgrp_result.status == "completed"
    assert echo_result.status == "completed"
