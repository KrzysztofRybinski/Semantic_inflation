from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from semantic_inflation.text.clean_html import html_to_text
from semantic_inflation.text.dictionaries import load_dictionaries
from semantic_inflation.text.sentence_split import split_sentences


_NUMBER_RE = re.compile(
    r"(?<!\w)(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?!\w)"
)


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_filing_text(path: Path) -> str:
    raw = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix.lower() in {".html", ".htm"}:
        return html_to_text(raw)
    return raw


def _is_kpi_sentence(sentence: str, dicts) -> bool:
    if not _NUMBER_RE.search(sentence):
        return False
    if dicts.kpi_unit_pattern.search(sentence):
        return True
    if dicts.kpi_label_pattern.search(sentence):
        return True
    return False


def compute_features_from_text(
    text: str,
    *,
    dictionary_version: str = "v1",
    min_sentence_chars: int = 10,
) -> dict:
    dicts = load_dictionaries(dictionary_version)
    sentences = [s for s in split_sentences(text) if len(s) >= min_sentence_chars]

    env = [s for s in sentences if dicts.env_pattern.search(s)]
    kpi = [s for s in env if _is_kpi_sentence(s, dicts)]

    aspirational = []
    for s in env:
        if dicts.aspirational_pattern.search(s):
            aspirational.append(s)
            continue
        if dicts.net_zero_pattern.search(s) and not _is_kpi_sentence(s, dicts):
            aspirational.append(s)

    env_count = len(env)
    asp_count = len(aspirational)
    kpi_count = len(kpi)

    a_share = (asp_count / env_count) if env_count else 0.0
    q_share = (kpi_count / env_count) if env_count else 0.0

    env_words = sum(len(s.split()) for s in env)

    return {
        "dictionary_version": dicts.version,
        "dictionary_sha256": dicts.sha256,
        "sentences_total": len(sentences),
        "sentences_env": env_count,
        "sentences_aspirational": asp_count,
        "sentences_kpi": kpi_count,
        "A_share": a_share,
        "Q_share": q_share,
        "env_word_count": env_words,
    }


def compute_features_from_file(
    path: str | Path,
    *,
    dictionary_version: str = "v1",
    min_sentence_chars: int = 10,
) -> dict:
    p = Path(path)
    text = _read_filing_text(p)
    feats = compute_features_from_text(
        text,
        dictionary_version=dictionary_version,
        min_sentence_chars=min_sentence_chars,
    )
    feats["input_path"] = str(p)
    feats["input_sha256"] = _file_sha256(p)
    return feats

