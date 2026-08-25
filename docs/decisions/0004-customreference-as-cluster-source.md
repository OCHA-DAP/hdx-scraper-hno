# 0004: Use Attachment.CustomReference as the authoritative cluster/sector code for Caseload attachments

## Status

Accepted — 2026-08-25

## Context

The HDXPIPE-152 ticket's documentation-review notes recommend joining sector
costs through `Attachment.EntityId → CoordinationEntity →
SectorCoordinationEntityRel → Sector`, not `AttachmentFact.SectorId` (which is
null for historical records) — this rule is stated for `Cost`-type
attachments. Before assuming the same join applies to `Caseload`-type
attachments, both approaches were checked live against Afghanistan HNO 2025
(`PlanId 1263`).

For most sectors (Education, Health, Nutrition, WASH, Shelter, etc.), the two
approaches agree: `Attachment.CustomReference` (e.g. `"1-HEA"`) matches the
single `Sector.SectorCode` (`"HEA"`) reachable via the entity join.

For Protection, they diverge in a way that makes the entity join unusable:
all 6 protection-related Caseload attachments (Protection overall, General
Protection, Child Protection, GBV, HLP, Mine Action) share **one**
`CoordinationEntity` (`Id 7910`), which itself links to 5 different
`Sector` codes (`PRO`, `PRO-CPN`, `PRO-GBV`, `PRO-HLP`, `PRO-MIN`). The
`CoordinationEntity → Sector` join cannot disambiguate which of the 6
attachments corresponds to which sector — it returns the same 5-sector list
for all of them. `Attachment.CustomReference` (`"2-PRO"`, `"3-PRO-CPN"`,
`"5-PRO-GBV"`, `"6-PRO-HLP"`, `"4-PRO-MIN"`, `"1-PRO-OVE"`) disambiguates them
correctly and directly.

The old pipeline (`plan.py`) worked around exactly this ambiguity in the old
REST API with a block of description-substring matching ("HACKY CODE TO DEAL
WITH DIFFERENT AORS UNDER PROTECTION" — matching on words like "child",
"gender", "housing", "mine" in the caseload description) after its own
cluster-mapping lookup failed to disambiguate protection sub-clusters.

## Decision

Derive the cluster/sector code for Caseload attachments directly from
`Attachment.CustomReference` (stripping the leading `"<order>-"` prefix, e.g.
`"3-PRO-CPN"` → `"PRO-CPN"`), not from the `CoordinationEntity → Sector` join.

## Consequences

- The old pipeline's protection-AOR description-substring-matching workaround
  is no longer needed and should be removed rather than ported — the new API
  disambiguates AORs correctly at the attachment level.
- This decision is specific to `Caseload`-type attachments; it does not
  contradict or apply to the ticket's stated rule for `Cost`-type attachments,
  which was not re-verified here and may still need the entity/sector join
  with its own allocation-rule caveat.
- If a future plan's `CustomReference` values don't follow the observed
  `"<order>-<CODE>"` pattern, this should surface as a data-quality warning
  (matching the old pipeline's `error_handler.add_message` pattern for unknown
  clusters), not a silent failure.
