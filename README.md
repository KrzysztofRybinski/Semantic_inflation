# Semantic Inflation (Climate Disclosure "Words vs Numbers")

This repository implements a fully reproducible pipeline to measure **semantic inflation** in corporate climate/environment disclosures: when **aspirational language** grows faster than **verifiable quantitative KPI disclosure** within filings.

## What exists today

- A dependency-light Python package (`semantic_inflation/`) that:
  - Converts filing HTML → text using bs4 + lxml (tables flattened row-wise)
  - Splits text into sentences (baseline heuristic splitter)
  - Classifies environmental sentences via a **frozen dictionary**
  - Classifies aspirational vs KPI sentences within environmental sentences
  - Outputs auditable counts/shares (`A_share`, `Q_share`) plus provenance hashes
  - Provides `semantic-inflation extract-text` for debugging HTML extraction

## Quickstart (no external dependencies)

Run the toy example:

```bash
python3 -m semantic_inflation toy
```

> **Note (Windows PowerShell):** Redirecting output with `>` writes UTF-16 by default, which will fail UTF-8 reads.
> Use `| Set-Content -Encoding utf8 outputs/toy.json` or `| Out-File -Encoding utf8 outputs/toy.json` instead.

Extract features from a local file:

```bash
python -m semantic_inflation features --input path/to/filing.html
```

## End-to-end research pipeline

Run the full pipeline (preflight checks → SEC → GHGRP → ECHO → linkage → panel → models):

```bash
python -m semantic_inflation run-all --config configs/pipeline.toml
```

> **Note:** `configs/pipeline.toml` points `pipeline.sec.filings_index_path` at the
> sample fixture in `data/fixtures/filings_index.csv`. If you want to run against
> real SEC filings, generate a filings index CSV and update the config to point at
> your `data/raw/sec/filings_index.csv` (or another path).

Run the preflight doctor checks (creates missing directories, cleans zero-byte files):

```bash
python -m semantic_inflation doctor --config configs/pipeline.toml
```

Pipeline outputs are written to `data/processed/` (parquet intermediates) and `outputs/` (QC + model results).

## Reproducibility standards

- Dictionaries/regex rules are stored in `semantic_inflation/resources/dictionaries_v1.toml` and should be treated as **frozen** once pre-registered.
- All outputs include a SHA-256 of the dictionary file used.
- Later steps (SEC/EPA ingestion, entity matching, panel assembly, econometrics) will be implemented as deterministic, scripted transforms from `data/raw/` → `data/processed/` → `outputs/`.
