from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from semantic_inflation.config import Settings
from semantic_inflation.paths import repo_root


@dataclass(frozen=True)
class PipelineContext:
    settings: Settings

    @property
    def repo_root(self) -> Path:
        return repo_root()

    @property
    def data_dir(self) -> Path:
        return self.settings.paths.data_dir

    @property
    def raw_dir(self) -> Path:
        return self.settings.paths.raw_dir

    @property
    def processed_dir(self) -> Path:
        return self.settings.paths.processed_dir

    @property
    def cache_dir(self) -> Path:
        return self.settings.paths.cache_dir

    @property
    def outputs_dir(self) -> Path:
        return self.settings.paths.outputs_dir

    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()
