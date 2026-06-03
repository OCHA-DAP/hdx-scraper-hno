# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-hno** retrieves Humanitarian Needs Overview (HNO) data from [HPC tools](https://api.hpc.tools/) and publishes it to HDX. It processes caseload data, monitor and progress JSON, and generates both standard and HAPI-compatible datasets for humanitarian planning purposes.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.hno
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_hno.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline in `__main__.py`:

1. **`main`** — Calls `facade()` to set up HDX configuration, then orchestrates fetching HNO plan data and generating datasets.

Key modules:
- **`plan.py`** — Fetches HNO plan data from the HPC API using `Read`.
- **`dataset_generator.py`** — Generates standard HDX datasets from caseload/monitor/progress data.
- **`hapi_dataset_generator.py`** — Generates HAPI-compatible datasets.
- **`hapi_output.py`** — Produces HAPI output using admin lookups, sector mappings, and time period helpers.
- **`caseload_json.py`**, **`monitor_json.py`**, **`progress_json.py`** — Parse the respective JSON structures from the HPC API.
- **`timeperiod_helper.py`** — Handles time period parsing and formatting for HNO data.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-hno` entry.

Additional env vars used at runtime: `HPC_BASIC_AUTH`, `HPC_BEARER_TOKEN`, `YEAR`, `ERR_TO_HDX`.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
