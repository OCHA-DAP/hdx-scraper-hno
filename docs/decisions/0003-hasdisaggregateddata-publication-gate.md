# 0003: Use Attachment.HasDisaggregatedData as the per-attachment publication gate; skip RevisionStateId fallback for Caseload facts

## Status

Accepted — 2026-08-25

## Context

The old REST API gated whether to process `disaggregatedAttachments` on
`data["lastPublishedVersion"] >= 1` — a plan-level signal for "has
disaggregation been published for this plan yet." The new API needed an
equivalent, since two 2026 plans (Cameroon, Afghanistan) had zero
disaggregated `AttachmentFact` rows while otherwise looking fully "released."

Live queries ruled out several plan-level candidates: `Plan.IsReleased`,
`Plan.ReleasedDate`, `Plan.RevisionState`, and `Plan.DocumentPublishDate` were
all identical/populated ("released", `"Active"`) across both a plan with real
disaggregated data (Afghanistan 2025, `PlanId 1263`) and the two plans without
any. None of them distinguish the cases this pipeline needs to distinguish.

`Attachment.HasDisaggregatedData`, checked at the individual attachment level
rather than the plan level, matched exactly: `true` for the Afghanistan 2025
Health attachment (which has real per-province facts) and `false` for every
attachment on the two 2026 plans (which have none).

Separately, the HDXPIPE-152 ticket flagged a `RevisionStateId` fallback rule
(1=original, 2=current; use 1 if 2 is absent) that applies to `Cost`/
requirement `AttachmentFact` rows. A live check of 200 Caseload facts on the
Afghanistan 2025 Health attachment found `RevisionStateId` is `null` on all of
them — the field is simply unused for Caseload data.

## Decision

Gate disaggregated-fact processing per `Attachment` (not per `Plan`) on
`HasDisaggregatedData == true`, mirroring the old API's plan-level
`lastPublishedVersion >= 1` check but at the correct granularity for the new
schema. Do not implement the `RevisionStateId` original/current fallback logic
for Caseload `AttachmentFact` rows — it does not apply to this data.

## Consequences

- If `HasDisaggregatedData` is `false` for a Caseload attachment, only its
  `IsTotal: true` national/aggregate rows should be used, matching the old
  API's behavior when a plan hadn't published disaggregation yet.
- If a future API change begins populating `RevisionStateId` on Caseload
  facts, this decision should be revisited (superseded, not silently
  reversed) rather than assumed still correct.
