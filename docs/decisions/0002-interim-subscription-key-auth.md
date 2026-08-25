# 0002: Use interim Ocp-Apim-Subscription-Key auth pending production Entra credentials

## Status

Accepted — 2026-08-25

## Context

The Fabric GraphQL API's own documentation (`hpc-fabric-graphql-api-docs`,
`docs/getting-started/authentication.md`, `docs/clients/applications.md`)
specifies that an unattended ETL/scheduled job should authenticate via a
dedicated Microsoft Entra service principal using the client-credentials flow
(`https://api.fabric.microsoft.com/.default` scope), sending
`Authorization: Bearer <token>` to a direct Fabric endpoint. As of
2026-08-24 (per HDXPIPE-152 comments), this production path was not yet
working for the team — a colleague testing the "direct route" got
authentication errors, and developer-portal API permissions had not yet been
granted to her account. A meeting with the HPC team was scheduled for
2026-08-25 to resolve this, but no outcome was available at decision time.

Separately, an Ocp-Apim-Subscription-Key issued via the developer portal
against the `hpc-apims.azure-api.net/v1` gateway was confirmed working this
session, returning real data for multiple current-cycle HNO plans.

Blocking implementation entirely on the Entra path was rejected: the data-side
feasibility work (0001) is time-sensitive, and the explicit product decision
(from the repo owner) was to proceed with what currently works and swap the
auth layer later rather than wait.

## Decision

Build the pipeline's GraphQL transport layer against the currently-working
Ocp-Apim-Subscription-Key header, but keep the auth mechanism isolated to the
reader/downloader construction step (`__main__.py`'s equivalent of
`Read.create_readers(...)`) so it can be swapped for an Entra
service-principal bearer token later without touching any query-building or
transformation code. Concretely: the subscription-key path needs a small
direct call to `Download.generate_downloaders`/`Read.generate_retrievers`
(bypassing `Read.create_readers`'s `header_auths` convenience, which hardcodes
the header name to `Authorization`); the Entra path, if/when adopted, fits the
existing `bearer_tokens={"hpc_graphql": token}` mechanism natively, matching
the pattern already used for the old API's `HPC_BEARER_TOKEN`.

## Consequences

- No token-refresh logic is needed under the current interim scheme (a static
  subscription key doesn't expire the way a short-lived Entra token would);
  this will need revisiting if/when the Entra path is adopted, since a
  full-year multi-country run's duration versus token TTL is unconfirmed.
- The subscription key must be stored the same way `HPC_BASIC_AUTH`/
  `HPC_BEARER_TOKEN` are today (env var / secret store, never committed).
- This record should be superseded once production Entra credentials are
  confirmed and adopted, rather than edited in place.
