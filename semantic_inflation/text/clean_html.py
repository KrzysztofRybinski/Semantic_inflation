from __future__ import annotations

from html.parser import HTMLParser
from html import unescape


_BLOCK_TAGS = {
    "p",
    "div",
    "br",
    "hr",
    "li",
    "ul",
    "ol",
    "table",
    "tr",
    "td",
    "th",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
}


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self._chunks: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        tag = tag.lower()
        if tag in {"script", "style"}:
            self._skip_depth += 1
            return
        if self._skip_depth == 0 and tag in _BLOCK_TAGS:
            self._chunks.append("\n")

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        tag = tag.lower()
        if tag in {"script", "style"} and self._skip_depth > 0:
            self._skip_depth -= 1
            return
        if self._skip_depth == 0 and tag in _BLOCK_TAGS:
            self._chunks.append("\n")

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        if self._skip_depth > 0:
            return
        if not data:
            return
        self._chunks.append(data)

    def get_text(self) -> str:
        return unescape("".join(self._chunks))


def html_to_text(html: str) -> str:
    parser = _TextExtractor()
    parser.feed(html)
    text = parser.get_text()
    parser.close()
    return text

