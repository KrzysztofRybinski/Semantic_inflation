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
uv run semantic-inflation toy
```

> **Note (Windows PowerShell):** Redirecting output with `>` writes UTF-16 by default, which will fail UTF-8 reads.
> Use `| Set-Content -Encoding utf8 outputs/toy.json` or `| Out-File -Encoding utf8 outputs/toy.json` instead.

Extract features from a local file:

```bash
uv run semantic-inflation features --input path/to/filing.html
```

## End-to-end research pipeline

### Required environment

Set the SEC User-Agent with contact info before running any SEC steps:

```bash
export SEC_USER_AGENT="Krzysztof Rybinski (k.rybinski@vistula.edu.pl)"
```

### Full pipeline

Run the full pipeline (preflight checks → SEC → GHGRP → ECHO → linkage → panel → models):

```bash
uv run semantic-inflation run-all --config configs/pipeline.toml
```

> **Note:** `configs/pipeline.toml` points `pipeline.sec.filings_index_path` at the
> sample fixture in `data/fixtures/filings_index.csv`. If you want to run against
> real SEC filings, generate a filings index CSV and update the config to point at
> your `data/raw/sec/filings_index.csv` (or another path). When using the SEC API
> directly, the pipeline enforces conservative throttling (`sec.max_requests_per_second = 8`).

Run the preflight doctor checks (creates missing directories, cleans zero-byte files):

```bash
uv run semantic-inflation doctor --config configs/pipeline.toml
```

### Stage-by-stage commands

```bash
uv run semantic-inflation sec download --config configs/pipeline.toml
uv run semantic-inflation sec features --config configs/pipeline.toml
uv run semantic-inflation epa ghgrp download --config configs/pipeline.toml
uv run semantic-inflation epa echo download --config configs/pipeline.toml
uv run semantic-inflation link build --config configs/pipeline.toml
uv run semantic-inflation panel build --config configs/pipeline.toml
uv run semantic-inflation analyze regressions --config configs/pipeline.toml
uv run semantic-inflation analyze classifier --config configs/pipeline.toml
```

### Resuming or rebuilding stages

Every stage writes a manifest under `outputs/qc/stage_<name>.json`. If inputs and outputs
haven't changed, the stage is skipped. To rebuild, add `--force`:

```bash
uv run semantic-inflation sec download --config configs/pipeline.toml --force
```

### Output locations

- `data/raw/...` raw downloads (zips/html/json)
- `data/processed/...` parquet tables
- `outputs/qc/*.json` QC summaries per stage
- `outputs/tables/*.csv` regression tables
- `outputs/figures/*.png` plots
- `outputs/provenance.json` run provenance

### Disk usage

The full dataset mode (`pipeline.sample_frame = "ghgrp_matched"`) can still be large.
To relocate storage, update `paths.data_dir` in `configs/pipeline.toml`.

Pipeline outputs are written to `data/processed/` (parquet intermediates) and `outputs/` (QC + model results).

## Reproducibility standards

- Dictionaries/regex rules are stored in `semantic_inflation/resources/dictionaries_v1.toml` and should be treated as **frozen** once pre-registered.
- All outputs include a SHA-256 of the dictionary file used.
- Later steps (SEC/EPA ingestion, entity matching, panel assembly, econometrics) will be implemented as deterministic, scripted transforms from `data/raw/` → `data/processed/` → `outputs/`.

> **Note (Windows PowerShell):** Redirecting output with `>` writes UTF-16 by default, which will fail UTF-8 reads.
> The pipeline itself always writes UTF-8 to avoid this trap.
