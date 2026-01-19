from __future__ import annotations

import re


_ABBREVIATIONS = {
    "u.s.",
    "u.k.",
    "inc.",
    "ltd.",
    "corp.",
    "co.",
    "no.",
    "dr.",
    "mr.",
    "ms.",
    "mrs.",
    "st.",
    "e.g.",
    "i.e.",
}


_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _protect_abbreviations(text: str) -> tuple[str, dict[str, str]]:
    replacements: dict[str, str] = {}
    protected = text
    for i, abbr in enumerate(sorted(_ABBREVIATIONS)):
        token = f"__ABBR{i}__"
        if abbr in protected.lower():
            replacements[token] = abbr
            protected = re.sub(
                re.escape(abbr),
                token,
                protected,
                flags=re.IGNORECASE,
            )
    return protected, replacements


def _restore_abbreviations(text: str, replacements: dict[str, str]) -> str:
    restored = text
    for token, abbr in replacements.items():
        restored = restored.replace(token, abbr)
    return restored


def split_sentences(text: str) -> list[str]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)

    protected, repl = _protect_abbreviations(normalized)

    # First split on newlines (SEC filings contain many bullet/list boundaries).
    rough_parts: list[str] = []
    for para in protected.split("\n"):
        para = para.strip()
        if not para:
            continue
        rough_parts.extend(_SENTENCE_SPLIT_RE.split(para))

    sentences = []
    for s in rough_parts:
        s = _restore_abbreviations(s.strip(), repl)
        if s:
            sentences.append(s)
    return sentences

