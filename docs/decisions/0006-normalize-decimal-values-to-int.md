# 0006: Cast whole-number GraphQL Decimal values to int

## Status

Accepted — 2026-08-25

## Context

The old REST API returned population/caseload figures as plain JSON
integers (confirmed directly against an old saved fixture: `<class 'int'>
5296124`). The new API's `ValueNum` field is a GraphQL `Decimal`, which
serializes as a float even for whole numbers (e.g. `53557.0`). `Plan.process()`
passed this straight through into `totals`/`metrics` dicts and from there into
published CSVs.

This was caught by live-diffing a real production dataset (Central African
Republic's `caf_hpc_needs_api_2025.csv`) against a freshly-generated stage
version: every numeric column showed spurious `.0` suffixes
(`5296124.0,2440338.0,1772631.0,...`) that don't appear in the current
production file, and wouldn't have appeared in this pipeline's historical
output either.

## Decision

In `Plan.process()`, cast `fact["ValueNum"]` to `int` whenever it's a
whole-number float (`isinstance(value, float) and value.is_integer()`),
before storing it. Genuinely fractional values (none expected in practice for
these metrics, but not assumed impossible) are left untouched.

## Consequences

- Published CSVs are numerically formatted the same way as the old
  REST-sourced output — verified by re-running against live data and
  confirming zero remaining `.0`-suffixed numbers in the regenerated file.
- Golden CSV test fixtures (`tests/fixtures/*.csv`) needed regenerating to
  match the corrected formatting.
- No behavior change for the underlying values themselves, only their string
  representation in output files. Fixing this once at the source in
  `Plan.process()` (rather than in `dataset_generator.py` and/or
  `hapi_output.py` separately) covers both the per-country/global CSV path
  and the HAPI output path in one place: `hapi_output.py`'s
  `get_numeric_if_possible()` only transforms *string* input back to a
  number, so a value that arrives already as a Python `float` (as these did,
  being read from an in-memory dict rather than parsed CSV text) passes
  through it unchanged - it would not have caught this on its own.
