<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/05-MEMBERS-PIPELINE.md -->

# Acceptance Criteria: Component 5 — Members Pipeline Projects

Two SBT projects within `repcheck-data-ingestion` handling member data ingestion: Congress.gov member profile sync and Senate LIS-to-bioguide mapping refresh. **Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2), `ingestion-common` (Component 3).

## System Context

### Two Projects, Two Data Sources

| Project | Trigger | Responsibility | Publishes |
|---------|---------|---------------|-----------|
| `member-profile-pipeline` | Scheduled (e.g., every 6 hours) | Fetch member profiles from Congress.gov, detect changes, archive history, persist to AlloyDB | `member.updated` (House: always on new data; Senate: only if LIS mapping exists) |
| `lis-mapping-refresher` | Scheduled (e.g., daily) | Refresh `lis_member_mapping` table from `senator-lookup.xml` | `member.updated` (for senators who already exist in `members` table and just gained a mapping) |

**Committee membership ingestion** is a separate component — not part of the members pipeline.

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
member.updated event → Scoring Engine
    +-- Re-score alignment for updated member
    +-- Update score cache
```

### History & Archival Strategy

Member profile history follows archive-before-overwrite pattern:

| Table | History Table | Archive Trigger |
|-------|--------------|-----------------|
| `members` | `member_history` | Member profile changed (updateDate differs) |
| `member_terms` | `member_term_history` | Member terms replaced during archive |
| `member_party_history` | *(none — append-only)* | New party affiliations appended, never overwritten |

`MemberHistoryArchiver` copies current `members` and `member_terms` state to history tables with shared `history_id` UUID before upsert overwrites it. `member_party_history` is append-only, no archive cycle.

History table DOs (`MemberHistoryDO`, `MemberTermHistoryDO`) defined in Component 1 §1.2; code lives in `repcheck-shared-models`.

### Placeholder Member Resolution

Bills pipeline creates placeholder member rows for sponsors/cosponsors. Member-profile-pipeline fills them naturally:
1. Fetch member detail from Congress.gov
2. Detect placeholder row exists but `updateDate` is null or stale
3. Archive placeholder state to history
4. Upsert full member data over placeholder

No special placeholder-detection logic — normal change detection + archive + upsert handles it.

### Event Payload

**`MemberUpdatedEvent`**:
```
memberId: String (bioguideId)
```

Minimal payload — consumers look up member details from AlloyDB.

### LIS Mapping Data Source

**Primary source**: `https://www.senate.gov/about/senator-lookup.xml`
- Contains `<lisid>` and `<bioguide>` for all senators (current and historical)
- Includes `current="yes"` attribute and congress service dates
- Refresher filters to senators within configurable congress lookback window (e.g., last 5 congresses)

See Component 1 §1.1 (`SenatorLookupXmlDTO`) for DTO definition.

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 5.1 Member API Client | Migrate + Extend | Extends `CongressGovPaginatedClient` for member list and detail endpoints |
| 5.2 Member Repository & History | New | Doobie repositories for members, terms, party history — with archive-before-overwrite |
| 5.3 Member Profile Processing | New | FS2 streaming pipeline: fetch → detect → archive → upsert → emit events |
| 5.4 Senator Lookup XML Client | New | Fetches and parses senator-lookup.xml from senate.gov |
| 5.5 LIS Mapping Repository | New | Doobie repository for lis_member_mapping table |
| 5.6 LIS Mapping Processing | New | Refresh logic: parse → upsert mappings → emit events for newly complete senators |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Congress.gov member list/detail API integration | [5.1 Member API Client](05-members-pipeline/05.1-member-api-client.md) |
| AlloyDB persistence for members, terms, party history + history archival | [5.2 Member Repository & History](05-members-pipeline/05.2-member-repository-history.md) |
| Member profile streaming pipeline: fetch → detect → archive → upsert → events | [5.3 Member Profile Processing](05-members-pipeline/05.3-member-profile-processing.md) |
| Senate.gov senator-lookup.xml fetch and parsing | [5.4 Senator Lookup XML Client](05-members-pipeline/05.4-senator-lookup-xml-client.md) |
| AlloyDB persistence for LIS-to-bioguide mappings | [5.5 LIS Mapping Repository](05-members-pipeline/05.5-lis-mapping-repository.md) |
| LIS mapping refresh logic and event emission | [5.6 LIS Mapping Processing](05-members-pipeline/05.6-lis-mapping-processing.md) |

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

Application entry points (`MemberProfilePipelineApp`, `LisMappingRefresherApp`) follow standard IOApp + PureConfig + `PipelineBootstrap` pattern from Component 3 §3.7.

### Dependencies

```
member-profile-pipeline / lis-mapping-refresher
+-- ingestion-common (Component 3)
|   +-- CongressGovPaginatedClient (member-profile-pipeline only)
|   +-- XmlFeedClient (lis-mapping-refresher only)
|   +-- ChangeDetector, IngestionEventPublisher, TransactorResource
|   +-- UpsertHelper, PipelineBootstrap, WorkflowStateUpdater
+-- repcheck-shared-models (Component 1)
|   +-- MemberListItemDTO, MemberDetailDTO, SenatorLookupXmlDTO
|   +-- MemberDO, MemberTermDO, MemberPartyHistoryDO, LisMemberMappingDO
|   +-- MemberHistoryDO, MemberTermHistoryDO
+-- repcheck-pipeline-models (Component 2)
    +-- MemberUpdatedEvent, ProcessingResult, PipelineRunSummary
    +-- Tables: Members, MemberTerms, MemberPartyHistory, LisMemberMapping, MemberHistory, MemberTermHistory
```

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | Processor logic, change detection, event emission conditions | MockitoScala |
| WireMock tests | `MembersApiClient`, `SenatorLookupXmlClient` | WireMock |
| Integration tests | All repositories (CRUD, upsert, history archival, LIS mapping) | `DockerPostgresSpec` |
| Pipeline integration | Full pipeline flows per project | WireMock + DockerPostgresSpec + mock Pub/Sub |