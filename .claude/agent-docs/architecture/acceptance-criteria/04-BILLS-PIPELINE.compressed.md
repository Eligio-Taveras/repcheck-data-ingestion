<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/04-BILLS-PIPELINE.md -->

# Acceptance Criteria: Component 4 — Bills Pipeline Projects

Three SBT projects within `repcheck-data-ingestion` handling full bill data lifecycle: metadata ingestion, text availability checking, text downloading/storage. **Depends on**: Component 1, Component 2, Component 3.

---

## System Context

### Three Projects, Three Responsibilities

| Project | Trigger | Responsibility | Publishes |
|---------|---------|---------------|-----------|
| `bill-metadata-pipeline` | Scheduled (e.g., 6h) | Fetch bill metadata from Congress.gov, detect changes, archive history, persist to AlloyDB | Nothing |
| `bill-text-availability-checker` | Scheduled (e.g., 2h) | Scan tracked bills for new/changed text versions | `bill.text.available` |
| `bill-text-pipeline` | Event-driven (`bill.text.available`) | Download bill text content, store in AlloyDB | `bill.text.ingested` |

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

| Table | History Table | Archive Trigger |
|-------|--------------|-----------------|
| `bills` | `bill_history` | Before every upsert of a changed bill |
| `bill_cosponsors` | `bill_cosponsor_history` | Before replacing cosponsors for a changed bill |
| `bill_subjects` | `bill_subject_history` | Before replacing subjects for a changed bill |

Each history row gets `history_id` (UUID PK) and `archived_at` timestamp. **Bill text versioning** uses separate `bill_text_versions` table — each legislative stage produces immutable version, no overwrites. **Schema note:** All table definitions, DOs, history DOs owned by Component 1. Component 4 uses but does not define.

### Bill Text Lifecycle

Text-related fields on `BillDO` are `Option` — `None` on initial metadata ingestion.

| State | Condition | Action |
|-------|-----------|--------|
| No text yet | `textUrl` is `None` | Check text endpoint on every run |
| Has text, not final | `textUrl` set, `textVersionType` ≠ `"ENR"` | Re-check if `updateDateIncludingText` changed |
| Has enrolled text | `textVersionType` = `"ENR"` | Text is final, skip |

### Event Payloads

**`BillTextAvailableEvent`**:
```
billId: String, congress: Int, textUrl: String, textFormat: String,
versionCode: String, previousVersionCode: Option[String]
```

**`BillTextIngestedEvent`**:
```
billId: String, congress: Int, versionCode: String,
previousVersionCode: Option[String], committeeCode: Option[String]
```

### Placeholder Entity Pattern

When bill references sponsor/cosponsor not yet in `members` table, `bill-metadata-pipeline` creates placeholder rows via `PlaceholderCreator` (Component 3 §3.6). Members-pipeline (Component 5) fills full data later.

---

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 4.1 Bill Metadata API Client | Migrate + Extend | Extends `CongressGovPaginatedClient` for bill list and detail endpoints |
| 4.2 Bill Repository & History | Migrate + Extend | Doobie repositories for bills, cosponsors, subjects — with archive-before-overwrite |
| 4.3 Bill Metadata Processing | Migrate + Extend | FS2 streaming pipeline: fetch → detect → archive → upsert → placeholders |
| 4.4 Bill Text Availability API Client | New | Fetches text version links from Congress.gov text endpoint |
| 4.5 Text Availability Checking Logic | New | Scans tracked bills, detects new/changed text, emits events |
| 4.6 Bill Text Downloader | New | Downloads bill text content from Congress.gov URL |
| 4.7 Bill Text Version Repository | New | Doobie repository for `bill_text_versions` table + `bills` text field updates |
| 4.8 Text Pipeline Processing | New | Event-driven pipeline: receive event → download → store → emit |

---

## SBT Module Structure

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

`bills-common` is internal SBT module (not published, not app). Holds all bill-related repositories and error types shared by three pipeline projects. Application entry points follow standard IOApp + PureConfig + `PipelineBootstrap` pattern from Component 3 §3.7.

### Dependencies

```
bill-metadata-pipeline / bill-text-availability-checker / bill-text-pipeline
├── bills-common                     (internal SBT dependsOn)
├── ingestion-common                 (Component 3)
│   ├── CongressGovPaginatedClient, ChangeDetector, IngestionEventPublisher
│   ├── PlaceholderCreator, TransactorResource, UpsertHelper
│   ├── PipelineBootstrap, WorkflowStateUpdater
├── repcheck-shared-models           (Component 1)
│   ├── BillListItemDTO, BillDetailDTO, BillTextLinksDTO
│   ├── BillDO, BillHistoryDO, BillCosponsorDO, BillSubjectDO, BillTextVersionDO
├── repcheck-pipeline-models         (Component 2)
│   ├── BillTextAvailableEvent, BillTextIngestedEvent
│   ├── ProcessingResult, PipelineRunSummary, Tables
```

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | Processor logic, change detection, text checking, download logic | MockitoScala |
| WireMock tests | API clients (bills, text, downloader) | WireMock |
| Integration tests | All repositories (CRUD, upsert, history archival, text versions) | `DockerPostgresSpec` |
| Pipeline integration | Full pipeline flows per project | WireMock + DockerPostgresSpec + mock Pub/Sub |

### Migration Checklist

1. `bill-identifier`: remove `BillIdentifierApp`, `BillProcessor`, `DoobieBillRepository` → replaced by `bill-metadata-pipeline`
2. `gov-apis`: remove `LegislativeBillsApi`, `BillTextLinksApi` → replaced by `BillsApiClient`, `BillTextApiClient`
3. Entity DTOs/DOs already migrated to `shared-models` (Component 1)
4. Shared infrastructure already migrated to `ingestion-common` (Component 3)