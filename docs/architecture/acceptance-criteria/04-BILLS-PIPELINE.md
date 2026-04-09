# Acceptance Criteria: Component 4 — Bills Pipeline Projects

> Three SBT projects within `repcheck-data-ingestion` that handle the full bill data lifecycle:
> bill metadata ingestion, bill text availability checking, and bill text downloading/storage.
> **Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2), `ingestion-common` (Component 3).

---

## System Context

### Three Projects, Three Responsibilities

Component 4 contains three SBT projects. Each is a separate Cloud Run Job with its own entry point, config, and schedule:

| Project | Trigger | Responsibility | Publishes |
|---------|---------|---------------|-----------|
| `bill-metadata-pipeline` | Scheduled (e.g., every 6 hours) | Fetch bill metadata from Congress.gov, detect changes, archive history, persist to AlloyDB | Nothing |
| `bill-text-availability-checker` | Scheduled (e.g., every 2 hours) | Scan tracked bills for new/changed text versions | `bill.text.available` |
| `bill-text-pipeline` | Event-driven (`bill.text.available` via Launcher) | Download actual bill text content, store in AlloyDB | `bill.text.ingested` |

### End-to-End Data Flow

```
Cloud Scheduler
    │
    ├── bill-metadata-pipeline (scheduled)
    │     ├── Fetch bill list from Congress.gov API (paginated)
    │     ├── For each bill: detect change via updateDate comparison
    │     ├── For changed bills: fetch detail (sponsors, cosponsors, subjects)
    │     ├── Archive old bill/cosponsors/subjects to history tables
    │     ├── Upsert bill metadata + cosponsors + subjects to AlloyDB
    │     └── Create placeholder members for unknown sponsors
    │
    └── bill-text-availability-checker (scheduled)
          ├── Query bills needing text check (no text yet, or non-final text)
          ├── For each: call Congress.gov text endpoint
          ├── Detect new/changed text versions
          └── Emit bill.text.available event per bill with new text
                │
                ▼
          Pub/Sub → Launcher
                │
                ▼
          bill-text-pipeline (event-driven)
                ├── Receive bill.text.available event
                ├── Download text content from Congress.gov URL
                ├── Store in bill_text_versions table
                ├── Update bills row (latest text fields + latestTextVersionId)
                └── Emit bill.text.ingested event
                      │
                      ▼
                Bill Analysis Pipeline (downstream)
```

### History & Versioning Strategy

**Bill metadata history** follows the same archive-before-overwrite pattern as votes (BEHAVIORAL_SPECS §1):

| Table | History Table | Archive Trigger |
|-------|--------------|-----------------|
| `bills` | `bill_history` | Before every upsert of a changed bill |
| `bill_cosponsors` | `bill_cosponsor_history` | Before replacing cosponsors for a changed bill |
| `bill_subjects` | `bill_subject_history` | Before replacing subjects for a changed bill |

Each history row gets a `history_id` (UUID PK) and `archived_at` timestamp, linking back to the original `bill_id`. This preserves the full audit trail of how a bill's metadata, sponsors, and subjects evolved over time.

**Bill text versioning** uses a separate `bill_text_versions` table (per Component 1 §1.4) — each legislative stage produces a new immutable text version rather than overwriting.

**Schema note:** All table definitions, DOs, and history DOs are owned by Component 1 (`repcheck-shared-models`). Component 4 uses those tables but does not define them.

### Bill Text Lifecycle

Text-related fields on `BillDO` are `Option` — `None` on initial metadata ingestion. The `bill-text-availability-checker` uses these to decide which bills need checking:

| State | Condition | Action |
|-------|-----------|--------|
| No text yet | `textUrl` is `None` | Check text endpoint on every run |
| Has text, not final | `textUrl` set, `textVersionType` ≠ `"ENR"` | Re-check if `updateDateIncludingText` changed |
| Has enrolled text | `textVersionType` = `"ENR"` | Text is final, skip |

### Event Payloads

Per Component 2 §2.1 (authoritative for payload shapes):

**`BillTextAvailableEvent`** (emitted by `bill-text-availability-checker`):
```
billId: String, congress: Int, textUrl: String, textFormat: String,
versionCode: String, previousVersionCode: Option[String]
```

**`BillTextIngestedEvent`** (emitted by `bill-text-pipeline`):
```
billId: String, congress: Int, versionCode: String,
previousVersionCode: Option[String], committeeCode: Option[String]
```

### Placeholder Entity Pattern

When a bill references a sponsor or cosponsor not yet in the `members` table, `bill-metadata-pipeline` creates placeholder member rows via `PlaceholderCreator` (Component 3 §3.6). The members-pipeline (Component 5) fills in full data later.

---

## Implementation Areas

### Project A: `bill-metadata-pipeline`

| Area | Status | Description |
|------|--------|-------------|
| 4.1 Bill Metadata API Client | Migrate + Extend | Extends `CongressGovPaginatedClient` for bill list and detail endpoints |
| 4.2 Bill Repository & History | Migrate + Extend | Doobie repositories for bills, cosponsors, subjects — with archive-before-overwrite |
| 4.3 Bill Metadata Processing | Migrate + Extend | FS2 streaming pipeline: fetch → detect → archive → upsert → placeholders |

### Project B: `bill-text-availability-checker`

| Area | Status | Description |
|------|--------|-------------|
| 4.4 Bill Text Availability API Client | New | Fetches text version links from Congress.gov text endpoint |
| 4.5 Text Availability Checking Logic | New | Scans tracked bills, detects new/changed text, emits events |

### Project C: `bill-text-pipeline`

| Area | Status | Description |
|------|--------|-------------|
| 4.6 Bill Text Downloader | New | Downloads bill text content from Congress.gov URL |
| 4.7 Bill Text Version Repository | New | Doobie repository for `bill_text_versions` table + `bills` text field updates |
| 4.8 Text Pipeline Processing | New | Event-driven pipeline: receive event → download → store → emit |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Congress.gov bill list/detail API integration | [4.1 Bill Metadata API Client](04-bills-pipeline/04.1-bill-metadata-api-client.md) |
| AlloyDB persistence for bills, cosponsors, subjects + history archival | [4.2 Bill Repository & History](04-bills-pipeline/04.2-bill-repository-history.md) |
| Metadata streaming pipeline: fetch → detect → archive → upsert | [4.3 Bill Metadata Processing](04-bills-pipeline/04.3-bill-metadata-processing.md) |
| Congress.gov text endpoint integration | [4.4 Bill Text Availability API Client](04-bills-pipeline/04.4-bill-text-availability-api-client.md) |
| Text availability scanning and event emission | [4.5 Text Availability Checking Logic](04-bills-pipeline/04.5-text-availability-checking.md) |
| Bill text content downloading from Congress.gov | [4.6 Bill Text Downloader](04-bills-pipeline/04.6-bill-text-downloader.md) |
| AlloyDB persistence for `bill_text_versions` + `bills` text updates | [4.7 Bill Text Version Repository](04-bills-pipeline/04.7-bill-text-version-repository.md) |
| Event-driven text pipeline: receive → download → store → emit | [4.8 Text Pipeline Processing](04-bills-pipeline/04.8-text-pipeline-processing.md) |

---

## Cross-Cutting Concerns

### SBT Module Structure

The three pipeline projects share repositories via a `bills-common` internal SBT module:

```
repcheck-data-ingestion/
├── bills-common/                   (internal SBT module — NOT a standalone app)
│   └── repcheck.ingestion.bills.common
│       ├── repository
│       │   ├── BillRepository              (4.2)
│       │   ├── BillHistoryArchiver         (4.2)
│       │   ├── BillCosponsorRepository     (4.2)
│       │   ├── BillSubjectRepository       (4.2)
│       │   └── BillTextVersionRepository   (4.7)
│       └── errors
│           ├── BillUpsertFailed            (4.2)
│           ├── BillArchiveFailed           (4.2)
│           └── TextStoreFailed             (4.7)
│
├── bill-metadata-pipeline/         (Cloud Run Job)
│   └── repcheck.ingestion.bills.metadata
│       ├── api
│       │   └── BillsApiClient              (4.1)
│       ├── pipeline
│       │   └── BillMetadataProcessor       (4.3)
│       └── errors
│           └── BillFetchFailed             (4.1)
│
├── bill-text-availability-checker/ (Cloud Run Job)
│   └── repcheck.ingestion.bills.textcheck
│       ├── api
│       │   └── BillTextApiClient           (4.4)
│       ├── checker
│       │   └── BillTextAvailabilityChecker (4.5)
│       └── errors
│           └── BillTextCheckFailed         (4.4)
│
└── bill-text-pipeline/             (Cloud Run Job)
    └── repcheck.ingestion.bills.textpipeline
        ├── download
        │   └── BillTextDownloader          (4.6)
        ├── pipeline
        │   └── BillTextProcessor           (4.8)
        └── errors
            └── TextDownloadFailed          (4.6)
```

> **`bills-common`** is an internal SBT module (not published, not a standalone app). It holds all bill-related repositories and error types shared by the three pipeline projects. Each pipeline project depends on `bills-common` via SBT `dependsOn`. This avoids duplicating `BillRepository` across three projects.

Application entry points (`BillMetadataPipelineApp`, `TextAvailabilityApp`, `BillTextPipelineApp`) follow the standard IOApp + PureConfig + `PipelineBootstrap` pattern from Component 3 §3.7. No area files needed — each app is pure wiring.

### Dependencies

```
bill-metadata-pipeline / bill-text-availability-checker / bill-text-pipeline
├── bills-common                     (internal SBT dependsOn — shared repositories)
│   ├── BillRepository, BillHistoryArchiver
│   ├── BillCosponsorRepository, BillSubjectRepository
│   └── BillTextVersionRepository
├── ingestion-common                 (internal SBT dependency — Component 3)
│   ├── CongressGovPaginatedClient   (API base)
│   ├── ChangeDetector               (change detection)
│   ├── IngestionEventPublisher      (event emission)
│   ├── PlaceholderCreator           (cross-entity refs)
│   ├── TransactorResource           (DB connection)
│   ├── UpsertHelper                 (SQL generation)
│   ├── PipelineBootstrap            (config, runId)
│   └── WorkflowStateUpdater         (step tracking)
├── repcheck-shared-models           (published artifact — Component 1)
│   ├── BillListItemDTO, BillDetailDTO, BillTextLinksDTO
│   ├── BillDO, BillHistoryDO, BillCosponsorDO, BillSubjectDO, BillTextVersionDO
│   └── BillTypes, FormatType, TextVersionCode, LatestActionDTO
└── repcheck-pipeline-models         (published artifact — Component 2)
    ├── BillTextAvailableEvent, BillTextIngestedEvent
    ├── ProcessingResult, PipelineRunSummary
    └── Tables (Bills, BillCosponsors, BillSubjects, BillHistory, BillTextVersions)
```

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | Processor logic, change detection, text checking, download logic | MockitoScala |
| WireMock tests | `BillsApiClient`, `BillTextApiClient`, `BillTextDownloader` | WireMock |
| Integration tests | All repositories (CRUD, upsert, history archival, text versions) | `DockerPostgresSpec` |
| Pipeline integration | Full pipeline flows per project | WireMock + DockerPostgresSpec + mock Pub/Sub |

### Migration Checklist

After Component 4 is implemented:
1. `bill-identifier` module: remove `BillIdentifierApp`, `BillProcessor`, `DoobieBillRepository`. Replaced by `bill-metadata-pipeline`.
2. `gov-apis` module: remove `LegislativeBillsApi`, `BillTextLinksApi`. Replaced by `BillsApiClient` and `BillTextApiClient`.
3. Entity-specific DTOs/DOs already migrated to `shared-models` in Component 1.
4. Shared infrastructure already migrated to `ingestion-common` in Component 3.
