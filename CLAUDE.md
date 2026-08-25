# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-hno** retrieves Humanitarian Needs Overview (HNO) data from the
Humanitarian Action Fabric GraphQL API (`hpc-apims.azure-api.net`, per
`docs/decisions/0001-migrate-to-fabric-graphql-api.md`) and publishes it to
HDX. It processes disaggregated caseload/needs data and generates both
standard and HAPI-compatible datasets for humanitarian planning purposes.

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
- **`plan.py`** — Fetches plan discovery and disaggregated Caseload data from the Fabric GraphQL API via `graphql_reader`/`queries`, and builds the per-row `rows`/`global_rows` dicts consumed by the rest of the pipeline.
- **`graphql_reader.py`** — GraphQL transport: reader/auth construction (see `docs/decisions/0002-interim-subscription-key-auth.md`), `execute_query()` (checks the top-level GraphQL `errors` array — an HTTP 200 doesn't imply success), and `paginate()` (cursor-based pagination).
- **`queries.py`** — The GraphQL query documents (plan discovery, Caseload attachments list, paginated Caseload facts).
- **`dataset_generator.py`** — Generates standard HDX datasets from the `rows`/`global_rows` produced by `plan.py`.
- **`hapi_dataset_generator.py`** — Generates HAPI-compatible datasets.
- **`hapi_output.py`** — Produces HAPI output using admin lookups, sector mappings, and time period helpers.
- **`timeperiod_helper.py`** — Handles time period parsing and formatting for HNO data.

See `docs/decisions/` for the record of why the source API changed and the key data-shape decisions (publication-readiness gate, cluster/sector code derivation) made during that migration.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-hno` entry.

Additional env vars used at runtime: `HPC_SUBSCRIPTION_KEY` (interim Fabric GraphQL API auth — see `docs/decisions/0002-interim-subscription-key-auth.md`), `YEAR`, `ERR_TO_HDX`.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
