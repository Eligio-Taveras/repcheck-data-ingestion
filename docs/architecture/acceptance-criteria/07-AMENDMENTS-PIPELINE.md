# Acceptance Criteria: Component 7 — Amendments Pipeline

> Single SBT project within `repcheck-data-ingestion` that ingests amendments from the Congress.gov JSON API.
> Single source, simple upsert, no history archival — the simplest ingestion pipeline.
> **Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2), `ingestion-common` (Component 3).

---

## System Context

### One Project, One Source

Component 7 is a single SBT project (`amendments-pipeline`) deployed as a Cloud Run Job:

| Project | Trigger | Responsibility | Publishes |
|---------|---------|---------------|-----------|
| `amendments-pipeline` | Scheduled (e.g., every 6 hours) | Fetch amendments from Congress.gov API, detect changes, upsert to AlloyDB, create placeholder members/bills for unknown sponsors and amended bills | Nothing |

Unlike votes (Component 6) which has two data sources and history archival, or bills (Component 4) which has three separate projects, the amendments pipeline is the simplest ingestion pipeline:
- **Single data source** — Congress.gov JSON API only
- **No history archival** — amendments rarely change after initial recording; `updateDate` comparison + upsert is sufficient
- **Single DO output** — `AmendmentDetailDTO.toDO` produces a single `AmendmentDO` (no fan-out to child tables)
- **Standard `ChangeDetector`** — uses the generic `ChangeDetector` from Component 3 §3.3 (no custom detector like votes)

### End-to-End Data Flow

```
Cloud Scheduler
    |
    +-- amendments-pipeline (scheduled)
          |
          +-- Fetch amendment list from Congress.gov API (paginated)
          +-- For each amendment: detect change via updateDate comparison
          +-- For changed amendments: fetch detail endpoint
          +-- Convert AmendmentDetailDTO.toDO -> AmendmentDO
          +-- Create placeholder member for unknown sponsor (if sponsorBioguideId present)
          +-- Create placeholder bill for amended bill (if billId present)
          +-- Upsert AmendmentDO to AlloyDB
```

### No Events Emitted

The amendments pipeline is a **pure data recorder** — it persists amendment data but does not emit events. Bill re-analysis is triggered by **new bill text**, not by amendment recording:

1. An amendment is adopted by Congress
2. Congress.gov publishes a new text version of the amended bill
3. `bill-text-availability-checker` (Component 4, Project B) detects the new text via `updateDateIncludingText` change
4. Emits `bill.text.available` → `bill-text-pipeline` downloads text → emits `bill.text.ingested`
5. Bill Analysis Pipeline (Component 10) re-analyzes the bill with the new text

This means the amendment can be *recorded* before its effect is *analyzed* — the analysis waits for the actual amended bill text, which is the definitive source. The amendments table serves as a reference for sponsor attribution, amendment descriptions, and roll call vote context.

> **`AmendmentRecordedEvent` removed from Components 2 and 3:** This event type was originally defined in Component 2 §2.1 and `IngestionEventPublisher` (Component 3 §3.4) during initial system design. It has been removed — the amendments pipeline emits no events, and bill re-analysis is triggered by the bill text path instead.

### Amendment Votes

Roll call votes on amendments flow through the votes pipeline (Component 6), not the amendments pipeline. `VoteDO` has `legislationType` and `legislationNumber` fields that reference the amendment being voted on. The amendments pipeline only records amendment *metadata* (sponsor, description, purpose, amended bill) — vote data comes from Component 6.

### No History Archival

Unlike bills (Component 4) and votes (Component 6), amendments do **not** use the archive-before-overwrite pattern:

- Amendments rarely change substantively after initial recording — the typical "change" is a metadata correction (e.g., updated `latestActionDate`)
- The `updateDate` comparison in `ChangeDetector` already prevents redundant writes
- No downstream consumer depends on amendment history (the re-analysis pipeline reads the *current* amendment, not its change history)
- If amendment history becomes needed in the future, adding it follows the same `HistoryArchiver` pattern as bills/votes/members — no architectural change required

### Amendment Types

Per Component 1 §1.8, `AmendmentType` is an enum with values:
- `HAMDT` — House amendment
- `SAMDT` — Senate amendment
- `SUAMDT` — Senate unprinted amendment (included in Congress.gov API, parsed identically to `SAMDT`)

### Placeholder Entity Pattern

When an amendment references entities not yet in the database:

| Reference | Placeholder Type | Condition |
|-----------|-----------------|-----------|
| `sponsorBioguideId` | `MemberDO` placeholder | Only when `sponsorBioguideId` is `Some` — some amendments have no identified sponsor |
| Amended bill | `BillDO` placeholder | Only when the amendment references a specific bill via `amendedBill` |

Placeholders use `INSERT ... ON CONFLICT DO NOTHING` (Component 3 §3.6) — safe against concurrent ingestion by the bills or members pipeline.

### Congress.gov API Endpoints

The amendments API (`/v3/amendment`) follows the standard Congress.gov paginated pattern:

| Endpoint | Returns | Used By |
|----------|---------|---------|
| `GET /amendment?congress={N}&...` | List of `AmendmentListItemDTO` | `fetchAll` (pagination) |
| `GET /amendment/{congress}/{type}/{number}` | `AmendmentDetailDTO` with sponsors, amended bill, latest action | `fetchDetail` |
| `GET /amendment/{congress}/{type}/{number}/actions` | Amendment actions timeline | Not used in initial implementation |
| `GET /amendment/{congress}/{type}/{number}/cosponsors` | Amendment cosponsors | Not used in initial implementation |
| `GET /amendment/{congress}/{type}/{number}/amendments` | Sub-amendments | Not used in initial implementation |
| `GET /amendment/{congress}/{type}/{number}/text` | Text versions (117th Congress+) | Not used in initial implementation |

> **Minimal initial scope:** The amendments pipeline fetches only the list and detail endpoints. Actions, cosponsors, sub-amendments, and text are available in the API but not consumed in the initial implementation. If downstream analysis needs them, the API client can be extended without changing the pipeline architecture.

---

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 7.1 Amendments API Client | New | Extends `CongressGovPaginatedClient` for amendment list and detail endpoints |
| 7.2 Amendment Repository | New | Doobie repository for amendments table — upsert, queries |
| 7.3 Amendment Processing Pipeline | New | FS2 streaming pipeline: fetch → detect → placeholders → upsert |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Congress.gov amendment list/detail API integration | [7.1 Amendments API Client](07-amendments-pipeline/07.1-amendments-api-client.md) |
| AlloyDB persistence for amendments | [7.2 Amendment Repository](07-amendments-pipeline/07.2-amendment-repository.md) |
| Streaming pipeline: fetch → detect → placeholders → upsert | [7.3 Amendment Processing Pipeline](07-amendments-pipeline/07.3-amendment-processing-pipeline.md) |

---

## Cross-Cutting Concerns

### SBT Module Structure

```
repcheck-data-ingestion/
└── amendments-pipeline/             (Cloud Run Job)
    └── repcheck.ingestion.amendments
        ├── api
        │   └── AmendmentsApiClient          (7.1)
        ├── repository
        │   └── AmendmentRepository          (7.2)
        ├── pipeline
        │   └── AmendmentProcessor           (7.3)
        ├── app
        │   └── AmendmentPipelineApp         (IOApp entry point — pure wiring)
        └── errors
            ├── AmendmentFetchFailed         (7.1)
            └── AmendmentUpsertFailed        (7.2)
```

> **No shared module needed** — unlike bills (which have three projects sharing `bills-common`), the amendments pipeline is a single project. All classes live in one SBT module.

Application entry point (`AmendmentPipelineApp`) follows the standard IOApp + PureConfig + `PipelineBootstrap` pattern from Component 3 §3.7. No area file needed — it is pure wiring.

### Dependencies

```
amendments-pipeline
├── ingestion-common                 (internal SBT dependency — Component 3)
│   ├── CongressGovPaginatedClient   (API base)
│   ├── ChangeDetector               (change detection)
│   ├── PlaceholderCreator           (cross-entity refs)
│   ├── TransactorResource           (DB connection)
│   ├── UpsertHelper                 (SQL generation)
│   ├── PipelineBootstrap            (config, runId)
│   └── WorkflowStateUpdater         (step tracking)
├── repcheck-shared-models           (published artifact — Component 1)
│   ├── AmendmentListItemDTO, AmendmentDetailDTO
│   ├── AmendmentDO
│   ├── AmendmentType (HAMDT, SAMDT, SUAMDT)
│   └── HasPlaceholder[MemberDO], HasPlaceholder[BillDO]
└── repcheck-pipeline-models         (published artifact — Component 2)
    ├── ProcessingResult, PipelineRunSummary
    └── Tables (Amendments)
```

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | Processor logic, change detection integration | MockitoScala |
| WireMock tests | `AmendmentsApiClient` pagination, detail fetching, error classification | WireMock |
| Integration tests | `AmendmentRepository` (CRUD, upsert, conflict handling) | `DockerPostgresSpec` |
| Pipeline integration | Full pipeline flow: API → detect → placeholders → upsert | WireMock + DockerPostgresSpec |
