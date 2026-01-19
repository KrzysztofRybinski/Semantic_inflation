# Semantic Inflation (Climate Disclosure "Words vs Numbers")

This repository implements a fully reproducible pipeline to measure **semantic inflation** in corporate climate/environment disclosures: when **aspirational language** grows faster than **verifiable quantitative KPI disclosure** within filings.

## What exists today

- A dependency-light Python package (`semantic_inflation/`) that:
  - Converts filing HTML → text
  - Splits text into sentences (baseline heuristic splitter)
  - Classifies environmental sentences via a **frozen dictionary**
  - Classifies aspirational vs KPI sentences within environmental sentences
  - Outputs auditable counts/shares (`A_share`, `Q_share`) plus provenance hashes

## Quickstart (no external dependencies)

Run the toy example:

```bash
python3 -m semantic_inflation toy
```

Extract features from a local file:

```bash
python3 -m semantic_inflation features --input path/to/filing.html
```

## Reproducibility standards

- Dictionaries/regex rules are stored in `semantic_inflation/resources/dictionaries_v1.toml` and should be treated as **frozen** once pre-registered.
- All outputs include a SHA-256 of the dictionary file used.
- Later steps (SEC/EPA ingestion, entity matching, panel assembly, econometrics) will be implemented as deterministic, scripted transforms from `data/raw/` → `data/processed/` → `outputs/`.

