# 0001: Migrate HNO caseload ingestion from the HPC REST API to the Humanitarian Action Fabric GraphQL API

## Status

Accepted — 2026-08-25

## Context

`hdx-scraper-hno` fetched plan discovery from `fts/flow/plan/overview/progress/{year}`
and disaggregated caseload data from
`plan/{plan_id}/responseMonitoring?includeCaseloadDisaggregation=true` on the old
HPC REST API (`api.hpc.tools`, v1). That API is being retired (HDXPIPE-152,
HDXDSYS-2743). OCHA HPC's replacement is a GraphQL API on Microsoft Fabric
(`hpc-apims.azure-api.net`).

Before committing to a migration, feasibility of the *data* — not just the
schema — was verified live against the new API: `Attachment`
(`AttachmentType: "Caseload"`) and `AttachmentFact` expose the same
admin-location × gender × age-group × population-status × metric-type
disaggregation the old API's `disaggregatedAttachments`/`dataMatrix` provided,
confirmed against real current-cycle plans (Afghanistan HNO 2025 `PlanId 1263`,
plus ~15 other 2025 HNOs). All 5 metrics used by this pipeline
(`totalPopulation`, `inNeed`, `target`, `affected`, `expectedReach`) exist 1:1
as `MetricType` rows. Two candidate 2026 plans initially showed zero
disaggregation and looked like a feasibility blocker, until cross-plan
comparison showed this was simply publication lag (new plans haven't reached
that point in their cycle yet) — the same caveat the old API had via
`lastPublishedVersion`.

The alternative to migrating was to keep depending on the retiring v1 REST
API, which is not viable, or to wait for a hypothetical alternative source —
no such alternative was identified or requested.

## Decision

Replace the two old REST calls with equivalent Fabric GraphQL queries against
`plans`, `attachments`, and `attachmentFacts`, while keeping every downstream
module (`dataset_generator.py`, `hapi_dataset_generator.py`, `hapi_output.py`,
`timeperiod_helper.py`) and the published HDX/HAPI dataset shapes unchanged.
Only `Plan.get_plan_ids_and_countries()` and `Plan.process()` (and their
direct helpers in `caseload_json.py`/`monitor_json.py`/`progress_json.py`)
change; the `rows`/`global_rows` dict contract they produce stays the same.

## Consequences

- This pipeline's data need is fully met by the new API's Caseload-typed
  `Attachment`/`AttachmentFact` types; no fallback source is required.
- `monitor_json.py`/`progress_json.py` are named after the old REST
  *endpoints* (`responseMonitoring`, `overview/progress`), not a distinct
  monitoring-report data domain — they will be renamed/adapted to reflect the
  new query shape rather than preserved as endpoint-named wrappers.
- Ingestion shape shifts from "one call per plan" to cursor-paginated GraphQL
  (Fabric's documented 100-item default page size, 100,000-item pagination
  ceiling), meaningfully increasing the number of round-trips per pipeline
  run — see 0004 for how sector/cluster identification is kept simple despite
  this, and note the runtime/rate-limiting risk this introduces (Fabric
  returned a live `429` during ad hoc testing at a much lower request volume
  than a full run will generate).
- The production auth mechanism is tracked separately (0002) and is not yet
  finalized as of this record's date.
