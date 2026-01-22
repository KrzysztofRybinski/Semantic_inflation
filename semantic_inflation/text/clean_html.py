from __future__ import annotations

from html import unescape
from html.parser import HTMLParser
from pathlib import Path
import re
import warnings

from bs4 import BeautifulSoup, NavigableString, Tag, XMLParsedAsHTMLWarning


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


def _normalize_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"[ \t]*\n[ \t]*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _html_to_text_htmlparser(html: str) -> str:
    parser = _TextExtractor()
    parser.feed(html)
    text = _normalize_text(parser.get_text())
    parser.close()
    return text


def _html_to_text_bs4(
    html: str,
    *,
    drop_hidden: bool,
    drop_ix_hidden: bool,
    unwrap_ix_tags: bool,
    keep_tables: bool,
    table_cell_sep: str,
    table_row_sep: str,
) -> str:
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    for tag in soup.find_all(True):
        if not isinstance(tag, Tag):
            continue
        name = (tag.name or "").lower()
        if name == "ix:hidden" and drop_ix_hidden:
            tag.decompose()
            continue
        if name.startswith("ix:") and unwrap_ix_tags:
            tag.unwrap()

    if drop_hidden:
        for tag in soup.find_all(True):
            if not isinstance(tag, Tag) or tag.attrs is None:
                continue
            style = (tag.get("style") or "").lower()
            normalized_style = re.sub(r"\s+", "", style)
            if (
                tag.has_attr("hidden")
                or "display:none" in normalized_style
                or "visibility:hidden" in normalized_style
            ):
                tag.decompose()

    if keep_tables:
        for table in soup.find_all("table"):
            rows: list[str] = []
            for row in table.find_all("tr"):
                cells = row.find_all(["th", "td"])
                cell_text = [cell.get_text(" ", strip=True) for cell in cells]
                row_text = table_cell_sep.join(t for t in cell_text if t)
                if row_text:
                    rows.append(row_text)
            if rows:
                table.replace_with(NavigableString(table_row_sep.join(rows)))
            else:
                table.decompose()

    text = soup.get_text(separator="\n")
    return _normalize_text(text)


def html_to_text(
    html: str,
    *,
    extractor: str = "bs4",
    drop_hidden: bool = True,
    drop_ix_hidden: bool = True,
    unwrap_ix_tags: bool = True,
    keep_tables: bool = True,
    table_cell_sep: str = " | ",
    table_row_sep: str = "\n",
    output_path: str | Path | None = None,
) -> str:
    extractor_key = extractor.lower()
    if extractor_key == "htmlparser":
        text = _html_to_text_htmlparser(html)
    elif extractor_key == "bs4":
        text = _html_to_text_bs4(
            html,
            drop_hidden=drop_hidden,
            drop_ix_hidden=drop_ix_hidden,
            unwrap_ix_tags=unwrap_ix_tags,
            keep_tables=keep_tables,
            table_cell_sep=table_cell_sep,
            table_row_sep=table_row_sep,
        )
    else:
        raise ValueError(f"Unsupported HTML extractor: {extractor}")

    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return text
