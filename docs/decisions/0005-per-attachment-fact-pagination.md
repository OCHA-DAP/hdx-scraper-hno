# 0005: Paginate Caseload facts per attachment, not per plan

## Status

Accepted — 2026-08-25

## Context

The initial design (0001) queried `attachmentFacts` filtered by
`PlanId`+`AttachmentType` in a single cross-sector paginated stream per plan,
at the default `first: 100` page size.

Live production testing against Afghanistan's 2025 HNO (the largest HRP, by
old-fixture file size roughly 3x the next-largest plan) showed this stream
needed 2,700+ pages and had not finished after 45+ minutes, still climbing.
Cross-checking against the old REST API's saved fixture for the same plan
confirmed why: summing `dataMatrix` metric rows across all 16 of the plan's
caseload attachments gives ~265,000 disaggregated fact rows for this one
plan — comfortably past Fabric's documented 100,000-item pagination ceiling
per query, and the underlying cause of the pathological page count.

## Decision

Changed `CASELOAD_FACTS_QUERY` to filter by `AttachmentId` instead of
`PlanId`+`AttachmentType`, and `Plan.process()` now loops over each
attachment (sector) and paginates its facts independently, rather than one
combined cross-plan-sector stream. Also raised the page size to `first: 1000`
(Fabric's documented maximum without prior agreement with the API owner) to
cut the number of round trips roughly 10x.

## Consequences

- Each individual paginated stream now stays safely under Fabric's per-query
  pagination ceiling — the largest single attachment observed (Afghanistan's
  national-level "Final HRP caseload") needed ~59 pages at `first: 1000`
  (~58,556 rows), far below the 100,000-item limit.
- Matches Fabric's own "split complex operations: first obtain stable IDs,
  then request related rows in a second filtered query" guidance
  (`docs/querying/performance-and-limits.md` in the vendor's docs repo).
- Total row volume per plan is unchanged, but split across many smaller,
  independently-retryable streams instead of one large one; this also makes
  fixture capture/replay for tests more manageable (one small file per
  attachment rather than one huge file per plan).
- Afghanistan (confirmed worst-case plan) now completes in ~22 minutes,
  instead of not completing at all in a reasonable time window.
- Added country- and attachment-level progress logging
  (`### Country i/N: XXX ###`, `  XXX: attachment j/n (...)`) so a long run's
  position is visible without needing to infer it from page-download log
  lines alone.
