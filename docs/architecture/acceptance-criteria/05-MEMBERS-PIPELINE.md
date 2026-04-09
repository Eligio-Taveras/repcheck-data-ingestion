# Acceptance Criteria: Component 5 — Members Pipeline Projects

> Two SBT projects within `repcheck-data-ingestion` that handle member data ingestion:
> Congress.gov member profile sync and Senate LIS-to-bioguide mapping refresh.
> **Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2), `ingestion-common` (Component 3).

---

## System Context

### Two Projects, Two Data Sources

Component 5 contains two SBT projects. Each is a separate Cloud Run Job with its own entry point, config, and schedule:

| Project | Trigger | Responsibility | Publishes |
|---------|---------|---------------|-----------|
| `member-profile-pipeline` | Scheduled (e.g., every 6 hours) | Fetch member profiles from Congress.gov, detect changes, archive history, persist to AlloyDB | `member.updated` (House: always on new data; Senate: only if LIS mapping exists) |
| `lis-mapping-refresher` | Scheduled (e.g., daily) | Refresh `lis_member_mapping` table from `senator-lookup.xml` | `member.updated` (for senators who already exist in `members` table and just gained a mapping) |

> **Committee membership ingestion** (chamber XML feeds for `CommitteeMemberDO`) is a separate component — not part of the members pipeline.

### End-to-End Data Flow

```
Cloud Scheduler
    |
    +-- member-profile-pipeline (scheduled)
    |     +-- Fetch member list from Congress.gov API for configurable congress (paginated)
    |     +-- For each member: detect change via updateDate comparison
    |     +-- For changed members: fetch detail (terms, party history)
    |     +-- Archive old member/terms to history tables
    |     +-- Upsert member profile + terms to AlloyDB
    |     +-- Append new party history rows (append-only, no archive)
    |     +-- Fill in placeholder members created by bills pipeline
    |     +-- House members: emit member.updated immediately
    |     +-- Senate members: emit member.updated only if LIS mapping exists
    |
    +-- lis-mapping-refresher (scheduled, daily)
          +-- Fetch senator-lookup.xml from senate.gov
          +-- Parse <senator> entries within configurable congress lookback window
          +-- Upsert LIS-to-bioguide mappings to lis_member_mapping table
          +-- For newly mapped senators already in members table:
          |     emit member.updated (senator just became "complete")
          +-- Update lastVerified timestamp on all refreshed mappings
```

### Downstream Consumers

```
member.updated event
    |
    v
Scoring Engine (downstream)
    +-- Re-score alignment for updated member
    +-- Update score cache
```

### History & Archival Strategy

**Member profile history** follows the same archive-before-overwrite pattern as bills (BEHAVIORAL_SPECS §1):

| Table | History Table | Archive Trigger |
|-------|--------------|-----------------|
| `members` | `member_history` | Member profile changed (updateDate differs) |
| `member_terms` | `member_term_history` | Member terms replaced during archive |
| `member_party_history` | *(none — append-only)* | New party affiliations appended, never overwritten |

A `MemberHistoryArchiver` copies current `members` and `member_terms` state to history tables with a shared `history_id` UUID before the upsert overwrites it — same pattern as `BillHistoryArchiver` in Component 4. `member_party_history` is append-only and does not participate in the archive cycle.

> **Note**: History table DOs (`MemberHistoryDO`, `MemberTermHistoryDO`) are defined in Component 1 §1.2. Per project convention, the specification lives here (where the need originates) but the code lives in `repcheck-shared-models`.

### Placeholder Member Resolution

The bills pipeline (Component 4) creates placeholder member rows for sponsors/cosponsors not yet in the `members` table. The member-profile-pipeline fills these in naturally during its normal upsert cycle:

1. Fetch member detail from Congress.gov
2. Detect that the row exists (placeholder) but `updateDate` is null or stale
3. Archive the placeholder state to history (preserving the record)
4. Upsert full member data over the placeholder

No special placeholder-detection logic is needed — the normal change detection + archive + upsert flow handles it.

### Event Payload

**`MemberUpdatedEvent`** (emitted by both projects):
```
memberId: String (bioguideId)
```

Minimal payload — consumers look up member details from AlloyDB as needed.

### LIS Mapping Data Source

**Primary source**: `https://www.senate.gov/about/senator-lookup.xml`
- Contains `<lisid>` and `<bioguide>` for all senators (current and historical)
- Not limited to committee members
- Includes `current="yes"` attribute and congress service dates for filtering
- Refresher filters to senators within a configurable congress lookback window (e.g., last 5 congresses)

See Component 1 §1.1 (`SenatorLookupXmlDTO`) for the DTO definition.

---

## Implementation Areas

### Project A: `member-profile-pipeline`

| Area | Status | Description |
|------|--------|-------------|
| 5.1 Member API Client | Migrate + Extend | Extends `CongressGovPaginatedClient` for member list and detail endpoints |
| 5.2 Member Repository & History | New | Doobie repositories for members, terms, party history — with archive-before-overwrite |
| 5.3 Member Profile Processing | New | FS2 streaming pipeline: fetch -> detect -> archive -> upsert -> emit events |

### Project B: `lis-mapping-refresher`

| Area | Status | Description |
|------|--------|-------------|
| 5.4 Senator Lookup XML Client | New | Fetches and parses senator-lookup.xml from senate.gov |
| 5.5 LIS Mapping Repository | New | Doobie repository for lis_member_mapping table |
| 5.6 LIS Mapping Processing | New | Refresh logic: parse -> upsert mappings -> emit events for newly complete senators |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Congress.gov member list/detail API integration | [5.1 Member API Client](05-members-pipeline/05.1-member-api-client.md) |
| AlloyDB persistence for members, terms, party history + history archival | [5.2 Member Repository & History](05-members-pipeline/05.2-member-repository-history.md) |
| Member profile streaming pipeline: fetch -> detect -> archive -> upsert -> events | [5.3 Member Profile Processing](05-members-pipeline/05.3-member-profile-processing.md) |
| Senate.gov senator-lookup.xml fetch and parsing | [5.4 Senator Lookup XML Client](05-members-pipeline/05.4-senator-lookup-xml-client.md) |
| AlloyDB persistence for LIS-to-bioguide mappings | [5.5 LIS Mapping Repository](05-members-pipeline/05.5-lis-mapping-repository.md) |
| LIS mapping refresh logic and event emission | [5.6 LIS Mapping Processing](05-members-pipeline/05.6-lis-mapping-processing.md) |

---

## Cross-Cutting Concerns

### Package Structure

```
repcheck.ingestion.members
+-- profile
|   +-- api
|   |   +-- MembersApiClient              (5.1)
|   +-- repository
|   |   +-- MemberRepository              (5.2)
|   |   +-- MemberHistoryArchiver         (5.2)
|   |   +-- MemberTermRepository          (5.2)
|   |   +-- MemberPartyHistoryRepository  (5.2)
|   +-- pipeline
|   |   +-- MemberProfileProcessor        (5.3)
|   +-- errors
|       +-- MemberFetchFailed             (5.1)
|       +-- MemberUpsertFailed            (5.2)
|       +-- MemberArchiveFailed           (5.2)
|
+-- lismapping
    +-- client
    |   +-- SenatorLookupXmlClient        (5.4)
    +-- repository
    |   +-- LisMappingRepository          (5.5)
    +-- pipeline
    |   +-- LisMappingProcessor           (5.6)
    +-- errors
        +-- LisMappingFetchFailed         (5.4)
        +-- LisMappingUpsertFailed        (5.5)
```

Application entry points (`MemberProfilePipelineApp`, `LisMappingRefresherApp`) follow the standard IOApp + PureConfig + `PipelineBootstrap` pattern from Component 3 §3.7. No area files needed — each app is pure wiring.

### Dependencies

Both projects share the same dependency set:

```
member-profile-pipeline / lis-mapping-refresher
+-- ingestion-common                 (internal SBT dependency -- Component 3)
|   +-- CongressGovPaginatedClient   (API base, member-profile-pipeline only)
|   +-- XmlFeedClient               (XML parsing, lis-mapping-refresher only)
|   +-- ChangeDetector               (change detection)
|   +-- IngestionEventPublisher      (event emission)
|   +-- PlaceholderCreator           (cross-entity refs, not needed here but available)
|   +-- TransactorResource           (DB connection)
|   +-- UpsertHelper                 (SQL generation)
|   +-- PipelineBootstrap            (config, runId)
|   +-- WorkflowStateUpdater         (step tracking)
+-- repcheck-shared-models           (published artifact -- Component 1)
|   +-- MemberListItemDTO, MemberDetailDTO, SenatorLookupXmlDTO
|   +-- MemberDO, MemberTermDO, MemberPartyHistoryDO, LisMemberMappingDO
|   +-- MemberHistoryDO, MemberTermHistoryDO
+-- repcheck-pipeline-models         (published artifact -- Component 2)
    +-- MemberUpdatedEvent
    +-- ProcessingResult, PipelineRunSummary
    +-- Tables (Members, MemberTerms, MemberPartyHistory, LisMemberMapping,
               MemberHistory, MemberTermHistory)
```

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | Processor logic, change detection, event emission conditions | MockitoScala |
| WireMock tests | `MembersApiClient`, `SenatorLookupXmlClient` | WireMock |
| Integration tests | All repositories (CRUD, upsert, history archival, LIS mapping) | `DockerPostgresSpec` |
| Pipeline integration | Full pipeline flows per project | WireMock + DockerPostgresSpec + mock Pub/Sub |
