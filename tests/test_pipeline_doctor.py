from __future__ import annotations

from pathlib import Path

from semantic_inflation.config import load_settings
from semantic_inflation.pipeline import PipelineContext
from semantic_inflation.pipeline.doctor import run_doctor


def test_doctor_creates_dirs(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[sec]
user_agent = "Test Researcher (test@example.com)"
max_requests_per_second = 5

[paths]
data_dir = "{data_dir}"
outputs_dir = "{outputs_dir}"

[runtime]
offline = true
""".format(
            data_dir=tmp_path / "data",
            outputs_dir=tmp_path / "outputs",
        ),
        encoding="utf-8",
    )
    settings = load_settings(config_path)
    context = PipelineContext(settings)

    qc = run_doctor(context)
    assert Path(settings.paths.raw_dir).exists()
    assert Path(settings.paths.processed_dir).exists()
    assert any("offline mode" in warning for warning in qc.warnings)
