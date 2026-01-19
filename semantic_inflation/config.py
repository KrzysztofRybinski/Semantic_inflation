from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomllib
import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from semantic_inflation.paths import repo_root


class PathsSettings(BaseModel):
    data_dir: Path = Path("data")
    outputs_dir: Path = Path("outputs")
    raw_dir: Path | None = None
    interim_dir: Path | None = None
    processed_dir: Path | None = None

    @model_validator(mode="after")
    def _populate_paths(self) -> "PathsSettings":
        if self.raw_dir is None:
            self.raw_dir = self.data_dir / "raw"
        if self.interim_dir is None:
            self.interim_dir = self.data_dir / "interim"
        if self.processed_dir is None:
            self.processed_dir = self.data_dir / "processed"
        return self


class ProjectSettings(BaseModel):
    start_year: int = 2009
    end_year: int = 2024
    filing_forms: list[str] = Field(default_factory=lambda: ["10-K"])


class SecSettings(BaseModel):
    user_agent: str = Field(..., description="SEC User-Agent with contact info.")
    requests_per_second: float = 10.0
    use_bulk_submissions: bool = True
    use_bulk_companyfacts: bool = True

    @field_validator("user_agent")
    @classmethod
    def _require_user_agent(cls, value: str) -> str:
        if not value or value.strip().lower() in {"required", "changeme", "todo"}:
            raise ValueError("SEC user_agent must be set with contact info.")
        return value


class DictionarySettings(BaseModel):
    env_tokens_path: Path = Path("assets/dicts/environment_tokens.txt")
    aspiration_markers_path: Path = Path("assets/dicts/aspiration_markers.txt")
    kpi_units_path: Path = Path("assets/dicts/kpi_units.txt")
    kpi_labels_path: Path = Path("assets/dicts/kpi_labels.txt")
    number_words_path: Path = Path("assets/dicts/number_words.txt")
    corp_suffixes_path: Path = Path("assets/dicts/corp_suffixes.txt")


class TextSettings(BaseModel):
    dictionary_version: str = "v1"
    min_sentence_chars: int = 10
    sentence_splitter: str = "regex"
    store_sentence_samples: bool = False


class RuntimeSettings(BaseModel):
    chunk_size: int = 100_000
    max_workers: int = 4
    request_timeout_seconds: int = 60


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SEMANTIC_INFLATION_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    project: ProjectSettings = Field(default_factory=ProjectSettings)
    paths: PathsSettings = Field(default_factory=PathsSettings)
    sec: SecSettings
    dictionaries: DictionarySettings = Field(default_factory=DictionarySettings)
    text: TextSettings = Field(default_factory=TextSettings)
    runtime: RuntimeSettings = Field(default_factory=RuntimeSettings)

    def resolved_paths(self) -> dict[str, str]:
        return {
            "data_dir": str(self.paths.data_dir),
            "raw_dir": str(self.paths.raw_dir),
            "interim_dir": str(self.paths.interim_dir),
            "processed_dir": str(self.paths.processed_dir),
            "outputs_dir": str(self.paths.outputs_dir),
        }


@dataclass(frozen=True)
class ConfigSource:
    path: Path | None
    data: dict[str, Any]


def _parse_config(path: Path) -> dict[str, Any]:
    if path.suffix.lower() in {".yml", ".yaml"}:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _resolve_config_path(path: str | Path | None) -> Path | None:
    if path:
        return Path(path)
    repo = repo_root()
    for name in ("config.toml", "config.yaml", "config.yml"):
        candidate = repo / name
        if candidate.exists():
            return candidate
    fallback = repo / "configs" / "default.toml"
    return fallback if fallback.exists() else None


def load_settings(path: str | Path | None = None) -> Settings:
    cfg_path = _resolve_config_path(path)
    data = _parse_config(cfg_path) if cfg_path else {}
    env_file = repo_root() / ".env"
    return Settings(
        **(data or {}),
        _env_file=str(env_file) if env_file.exists() else None,
        _env_file_encoding="utf-8",
    )
