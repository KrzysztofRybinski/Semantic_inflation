from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
import tomllib
from importlib import resources


def _term_to_regex(term: str) -> str:
    """
    Converts a dictionary term into a regex fragment.
    Supported: trailing '*' wildcard (word-prefix match).
    """
    if "*" in term and not term.endswith("*"):
        raise ValueError(f"Only trailing '*' wildcards are supported: {term!r}")

    is_wildcard = term.endswith("*")
    core = term[:-1] if is_wildcard else term

    # Collapse whitespace in phrases.
    parts = [re.escape(p) for p in core.strip().split()]
    core_re = r"\s+".join(parts)

    if is_wildcard:
        core_re = core_re + r"\w*"

    # Word-ish boundaries (works for phrases too).
    return r"(?<!\w)" + core_re + r"(?!\w)"


def _compile_terms(terms: list[str]) -> re.Pattern[str]:
    if not terms:
        raise ValueError("Empty term list")
    joined = "|".join(_term_to_regex(t) for t in terms)
    return re.compile(joined, flags=re.IGNORECASE)


@dataclass(frozen=True)
class Dictionaries:
    version: str
    sha256: str
    env_pattern: re.Pattern[str]
    aspirational_pattern: re.Pattern[str]
    net_zero_pattern: re.Pattern[str]
    kpi_unit_pattern: re.Pattern[str]
    kpi_label_pattern: re.Pattern[str]


def load_dictionaries(version: str = "v1") -> Dictionaries:
    resource_name = f"dictionaries_{version}.toml"
    data_bytes = resources.files("semantic_inflation.resources").joinpath(resource_name).read_bytes()
    sha256 = hashlib.sha256(data_bytes).hexdigest()
    data = tomllib.loads(data_bytes.decode("utf-8"))

    env_terms = list(data["environment"]["terms"])
    asp_terms = list(data["aspirational"]["terms"])
    net_zero_terms = list(data["aspirational"]["net_zero_terms"])
    unit_terms = list(data["kpi"]["unit_terms"])
    label_terms = list(data["kpi"]["label_terms"])

    return Dictionaries(
        version=version,
        sha256=sha256,
        env_pattern=_compile_terms(env_terms),
        aspirational_pattern=_compile_terms(asp_terms),
        net_zero_pattern=_compile_terms(net_zero_terms),
        kpi_unit_pattern=_compile_terms(unit_terms),
        kpi_label_pattern=_compile_terms(label_terms),
    )

