<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/07-AMENDMENTS-PIPELINE.md -->

# Acceptance Criteria: Component 7 — Amendments Pipeline

Single SBT project within `repcheck-data-ingestion` that ingests amendments from Congress.gov JSON API. Single source, simple upsert, no history archival — simplest ingestion pipeline.
**Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2), `ingestion-common` (Component 3).

## System Context

### One Project, One Source

| Project | Trigger | Responsibility | Publishes |
|---------|---------|---------------|-----------|
| `amendments-pipeline` | Scheduled (e.g., every 6 hours) | Fetch amendments from Congress.gov API, detect changes, upsert to AlloyDB, create placeholder members/bills for unknown sponsors and amended bills | Nothing |

**Differences from other pipelines:** Single Congress.gov API source only; no history archival (updateDate comparison + upsert sufficient); single DO output; uses standard `ChangeDetector` from Component 3 §3.3.

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

Amendments pipeline is a **pure data recorder** — no events emitted. Bill re-analysis triggered by **new bill text** (Congress.gov `updateDateIncludingText` change) detected by `bill-text-availability-checker` (Component 4, Project B), not by amendment recording. Amendment effect is analyzed only after the actual amended bill text is available.

> **`AmendmentRecordedEvent` removed:** Originally in Components 2 and 3, removed from system design — amendments pipeline emits no events.

### Amendment Votes

Roll call votes on amendments flow through votes pipeline (Component 6), not amendments pipeline. `VoteDO.legislationType` and `legislationNumber` reference amendments. Amendments pipeline records only metadata (sponsor, description, amended bill) — vote data from Component 6.

### No History Archival

Amendments do **not** use archive-before-overwrite pattern: rarely change substantively after initial recording; `updateDate` comparison prevents redundant writes; no downstream consumer depends on history; future history support follows standard `HistoryArchiver` pattern (no architectural change needed).

### Amendment Types

Per Component 1 §1.8, `AmendmentType` enum values:
- `HAMDT` — House amendment
- `SAMDT` — Senate amendment
- `SUAMDT` — Senate unprinted amendment

### Placeholder Entity Pattern

| Reference | Placeholder Type | Condition |
|-----------|-----------------|-----------|
| `sponsorBioguideId` | `MemberDO` placeholder | Only when `sponsorBioguideId` is `Some` |
| Amended bill | `BillDO` placeholder | Only when amendment references specific bill |

Uses `INSERT ... ON CONFLICT DO NOTHING` (Component 3 §3.6).

### Congress.gov API Endpoints

| Endpoint | Returns | Used |
|----------|---------|------|
| `GET /amendment?congress={N}&...` | `AmendmentListItemDTO` list | `fetchAll` (pagination) |
| `GET /amendment/{congress}/{type}/{number}` | `AmendmentDetailDTO` | `fetchDetail` |
| `GET /amendment/{congress}/{type}/{number}/actions` | Amendment actions | Not in initial scope |
| `GET /amendment/{congress}/{type}/{number}/cosponsors` | Cosponsors | Not in initial scope |
| `GET /amendment/{congress}/{type}/{number}/amendments` | Sub-amendments | Not in initial scope |
| `GET /amendment/{congress}/{type}/{number}/text` | Text versions | Not in initial scope |

**Minimal initial scope:** List and detail endpoints only. Actions, cosponsors, sub-amendments, text available but not consumed. API client extensible without architectural change.

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

No shared module needed — single project. Application entry point (`AmendmentPipelineApp`) follows IOApp + PureConfig + `PipelineBootstrap` pattern (Component 3 §3.7). Pure wiring, no area file needed.

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
| Integration tests | `AmendmentRepository` (CRUD, upsert, conflict handling) | DockerPostgresSpec |
| Pipeline integration | Full pipeline flow: API → detect → placeholders → upsert | WireMock + DockerPostgresSpec |