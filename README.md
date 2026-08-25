# Collector for HNO Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-hno/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-hno/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-hno/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-hno?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This pipeline retrieves Humanitarian Needs Overview (HNO) data from the
[Humanitarian Action Fabric GraphQL API](https://hpc-apims.azure-api.net/)
and publishes it to HDX first as per-country and global HNO datasets, and
then as a HAPI dataset derived from the same data (see `docs/decisions/` for
the migration history from the previous HPC Tools REST API). It makes reads
to the Fabric GraphQL API (one plan-discovery query, plus a Caseload
attachments-list query and one or more paginated Caseload-facts queries per
plan for the configured year) and HDX writes (one per HRP country dataset
plus a global dataset and a HAPI dataset). Temporary per-country CSV files of
a few hundred KB each are created during processing. The pipeline fetches
plan IDs and associated countries via plan discovery, fetches each plan's
Caseload attachments and disaggregated facts, maps locations to P-codes,
disaggregates population figures (Population, In Need, Targeted, Affected,
Reached) by sector and admin level (0–2), and writes the results to the
per-country and global HNO datasets; the HAPI dataset is then generated from
the global output.

## Data Pipeline

### API reads

- **Plan discovery** (1 query): fetches all HNO plan IDs and associated
  countries for the configured year from the Fabric GraphQL API.
- **Per-plan queries** (2+ queries per plan): a Caseload attachments-list
  query, plus one or more paginated Caseload-facts queries (cursor-based;
  a plan with many locations/categories needs multiple pages).

### API writes

- **Per-country HNO datasets** (~one write per HRP country): each dataset contains
  disaggregated population figures by sector and admin level.
- **Global HNO dataset** (1 write): aggregates data across all countries.
- **HAPI dataset** (1 write): derived from the global HNO output.

### Temporary files

- Per-country CSV files of a few hundred KB each, created during processing.

### Uploaded files

- Per-country HNO datasets with population figures disaggregated by sector and
  admin level (0–2).
- Global HNO dataset.
- HAPI dataset derived from the global output.

### Transformations

1. **Plan resolution**: plan IDs and associated countries are fetched via
   plan discovery.
2. **Caseload fact grouping**: per-metric Caseload fact rows returned by the
   API are grouped by location/demographic combination into one row per
   category (see `plan.py`).
3. **P-code mapping**: locations in the source data are matched to admin P-codes.
4. **Population disaggregation**: Population, In Need, Targeted, Affected, and
   Reached figures are disaggregated by sector and admin level (0–2).

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-hno** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python -m hdx.scraper.hno
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
