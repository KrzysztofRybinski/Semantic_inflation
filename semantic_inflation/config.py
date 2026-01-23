from __future__ import annotations

from dataclasses import dataclass
import os
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
    cache_dir: Path | None = None
    raw_dir: Path | None = None
    interim_dir: Path | None = None
    processed_dir: Path | None = None

    @model_validator(mode="after")
    def _populate_paths(self) -> "PathsSettings":
        if self.cache_dir is None:
            self.cache_dir = self.data_dir / "cache"
        if self.raw_dir is None:
            self.raw_dir = self.data_dir / "raw"
        if self.interim_dir is None:
            self.interim_dir = self.data_dir / "interim"
        if self.processed_dir is None:
            self.processed_dir = self.data_dir / "processed"
        return self


class ProjectSettings(BaseModel):
    start_year: int = 2010
    end_year: int = 2023
    filing_forms: list[str] = Field(default_factory=lambda: ["10-K"])


class SecSettings(BaseModel):
    user_agent_env: str = "SEC_USER_AGENT"
    user_agent: str | None = Field(
        default=None, description="SEC User-Agent with contact info."
    )
    max_requests_per_second: float = 8.0
    concurrent_downloads: int = 4
    use_submissions_api: bool = True
    download_primary_html_only: bool = True

    @field_validator("user_agent")
    @classmethod
    def _normalize_user_agent(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip() or value.strip().lower() in {"required", "changeme", "todo"}:
            raise ValueError("SEC user_agent must be set with contact info.")
        return value

    def resolved_user_agent(self) -> str:
        env_value = os.getenv(self.user_agent_env, "")
        resolved = self.user_agent or env_value
        if not resolved or resolved.strip().lower() in {"required", "changeme", "todo"}:
            raise ValueError(
                f"SEC user agent not set. Provide {self.user_agent_env} env var or "
                "set sec.user_agent in config."
            )
        return resolved


class DictionarySettings(BaseModel):
    dict_path: Path = Path("semantic_inflation/resources/dictionaries_v1.toml")
    env_tokens_path: Path = Path("assets/dicts/environment_tokens.txt")
    aspiration_markers_path: Path = Path("assets/dicts/aspiration_markers.txt")
    kpi_units_path: Path = Path("assets/dicts/kpi_units.txt")
    kpi_labels_path: Path = Path("assets/dicts/kpi_labels.txt")
    number_words_path: Path = Path("assets/dicts/number_words.txt")
    corp_suffixes_path: Path = Path("assets/dicts/corp_suffixes.txt")


class TextHtmlSettings(BaseModel):
    extractor: str = "bs4"
    drop_hidden: bool = True
    drop_ix_hidden: bool = True
    unwrap_ix_tags: bool = True
    keep_tables: bool = True
    table_cell_sep: str = " | "
    table_row_sep: str = "\n"


class TextSettings(BaseModel):
    dictionary_version: str = "v1"
    min_sentence_chars: int = 10
    sentence_splitter: str = "regex"
    store_sentence_samples: bool = False
    html: TextHtmlSettings = Field(default_factory=TextHtmlSettings)


class RuntimeSettings(BaseModel):
    chunk_size: int = 100_000
    max_workers: int = 4
    request_timeout_seconds: int = 60
    offline: bool = False


class PipelineSecSettings(BaseModel):
    filings_index_path: Path = Path("data/fixtures/filings_index.csv")
    max_filings: int | None = None
    build_index: bool = False
    company_tickers_url: str = "https://www.sec.gov/files/company_tickers.json"


class PipelineGhgrpSettings(BaseModel):
    fixture_path: Path = Path("data/fixtures/ghgrp_sample.csv")
    data_sets_url: str = "https://www.epa.gov/ghgreporting/ghgrp-data-sets"
    data_summary_url: str | None = None
    parent_companies_url: str | None = None
    parent_companies_path: Path = Path("data/raw/epa/ghgrp/ghgrp_parent_companies.xlsb")
    years: list[int] | None = None


class PipelineEchoSettings(BaseModel):
    fixture_path: Path = Path("data/fixtures/echo_sample.csv")
    case_downloads_url: str = "https://echo.epa.gov/files/echodownloads/case_downloads.zip"
    frs_downloads_url: str = "https://echo.epa.gov/files/echodownloads/frs_downloads.zip"
    schema_fields: list[str] = Field(default_factory=list)


class LinkageSettings(BaseModel):
    fuzzy_threshold_high: int = 95
    fuzzy_threshold_medium: int = 90
    manual_overrides_path: Path = Path("assets/manual_overrides/parent_to_cik.csv")


class AnalysisSettings(BaseModel):
    horizons: list[int] = Field(default_factory=lambda: [1, 2])
    classifier_seed: int = 42
    classifier_test_fraction: float = 0.2
    cluster_by: str = "firm"
    firm_fixed_effects: bool = True
    year_fixed_effects: bool = True


class PipelineSettings(BaseModel):
    mode: str = "full"
    sample_frame: str = "ghgrp_matched"
    sec: PipelineSecSettings = Field(default_factory=PipelineSecSettings)
    ghgrp: PipelineGhgrpSettings = Field(default_factory=PipelineGhgrpSettings)
    echo: PipelineEchoSettings = Field(default_factory=PipelineEchoSettings)


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
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    linkage: LinkageSettings = Field(default_factory=LinkageSettings)
    analysis: AnalysisSettings = Field(default_factory=AnalysisSettings)

    def resolved_paths(self) -> dict[str, str]:
        return {
            "data_dir": str(self.paths.data_dir),
            "raw_dir": str(self.paths.raw_dir),
            "interim_dir": str(self.paths.interim_dir),
            "processed_dir": str(self.paths.processed_dir),
            "outputs_dir": str(self.paths.outputs_dir),
            "cache_dir": str(self.paths.cache_dir),
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
