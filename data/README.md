# Data layout

This repo follows a strict "raw → processed → outputs" workflow.

- `data/raw/`: downloaded source files (SEC/EPA/FRS), immutable after download.
- `data/interim/`: intermediate artifacts (parsed filings, sentence tables, match tables).
- `data/processed/`: analysis-ready panels (firm-year, facility-year, link tables).
- `data/fixtures/`: tiny, versioned test fixtures used by unit tests and toy runs.

Large datasets should not be committed to git.

