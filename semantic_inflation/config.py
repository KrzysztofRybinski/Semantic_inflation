from __future__ import annotations

from pathlib import Path
import tomllib


def load_config(path: str | Path) -> dict:
    cfg_path = Path(path)
    data = tomllib.loads(cfg_path.read_text(encoding="utf-8"))

    if "text" not in data or "dictionary_version" not in data["text"]:
        raise ValueError("Config missing [text].dictionary_version")

    return data

