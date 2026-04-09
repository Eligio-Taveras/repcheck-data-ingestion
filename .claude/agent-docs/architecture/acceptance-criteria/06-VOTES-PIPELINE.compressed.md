<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/06-VOTES-PIPELINE.md -->

# Acceptance Criteria: Component 6 — Votes Pipeline

Single SBT project (`votes-pipeline`) within `repcheck-data-ingestion` ingesting roll call votes from Congress.gov (House) and senate.gov (Senate) to unified DTOs with shared storage, history, and event emission.
**Depends on**: Component 1, Component 2, Component 3.

## System Context

### One Project, Two Sources

| Project | Trigger | Responsibility | Publishes |
|---------|---------|---------------|-----------|
| `votes-pipeline` | Scheduled (e.g., every 2 hours) | Fetch roll call votes from Congress.gov (House) and senate.gov (Senate), detect changes, archive history, persist to AlloyDB, create placeholder members/bills | `vote.recorded` |

Both data sources converge on unified DTO types; change detection, history archival, storage, and event emission are shared.

### End-to-End Data Flow

```
Cloud Scheduler
    |
    +-- votes-pipeline (scheduled)
          |
          +-- House votes path:
          |     +-- Fetch vote list from Congress.gov API (paginated)
          |     +-- For each vote: detect change via updateDate comparison
          |     +-- For changed votes: fetch members endpoint (positions)
          |     +-- Convert HouseVoteMembers JSON to unified VoteMembersDTO
          |
          +-- Senate votes path:
          |     +-- Fetch roll call vote XML from senate.gov
          |     +-- Parse SenateVoteXmlDTO
          |     +-- Resolve LIS member IDs to bioguide IDs via lis_member_mapping table
          |     +-- Convert to unified VoteMembersDTO
          |
          +-- Shared path (both chambers):
                +-- Convert VoteMembersDTO.toDO -> VoteConversionResult (VoteDO + positions)
                +-- Detect position changes via VoteChangeDetector
                +-- Archive old vote + positions to history tables
                +-- Upsert vote metadata + positions to AlloyDB
                +-- Create placeholder members for unknown voters
                +-- Emit vote.recorded event (new votes, or updates with position changes)
```

### Downstream Consumers

```
vote.recorded event → Scoring Engine
  +-- Extract all memberIds from vote positions
  +-- Find users whose representatives include those members
  +-- Re-score affected (userId, memberId) pairs
  +-- If bill has no analysis yet, requeue with backoff
```

### History & Archival Strategy

Vote history follows archive-before-overwrite pattern (BEHAVIORAL_SPECS §1):

| Table | History Table | Archive Trigger |
|-------|--------------|-----------------|
| `votes` | `vote_history` | Vote metadata or positions changed |
| `vote_positions` | `vote_history_positions` | Positions archived alongside vote metadata (shared `history_id`) |

`VoteHistoryArchiver` copies current state to history tables with shared `history_id` UUID before upsert overwrites it.

### Vote Change Detection

- **Generic `ChangeDetector`** (bills/members): Compares `updateDate`, performs field-by-field diffing.
- **`VoteChangeDetector`** (votes-only): Compares `updateDate` (fast path skip), then checks if **positions** actually changed by comparing incoming position set against stored positions. Position changes drive event emission; metadata-only changes do not emit.

`VoteChangeDetector` lives in votes-pipeline project (not `ingestion-common`) due to vote-specific logic.

### Event Payload

**`VoteRecordedEvent`** (Component 2 §2.1):
```
voteId: String, billId: Option[String], chamber: String,
date: Instant, congress: Int, isUpdate: Boolean
```

**Emission conditions** (BEHAVIORAL_SPECS §4):
- **New vote**: emit with `isUpdate = false`
- **Updated vote (positions changed)**: emit with `isUpdate = true`
- **Updated vote (metadata only)**: upsert metadata, archive history, do NOT emit

### Vote Type Classification

| Vote Type | `question` Pattern | Example |
|-----------|-------------------|---------|
| `Passage` | "On Passage" / "On Motion to Suspend the Rules and Pass" | Floor vote to pass a bill |
| `ConferenceReport` | "On Agreeing to the Conference Report" | Final reconciled version |
| `Cloture` | "On Cloture" / "On the Cloture Motion" | Senate debate ending (60 votes) |
| `VetoOverride` | "On Overriding the Veto" | Override presidential veto (2/3) |
| `Amendment` | "On Agreeing to the Amendment" | Modify bill text |
| `Committee` | "Reported favorably" / "Ordered to be reported" | Committee advancement |
| `Recommit` | "On Motion to Recommit" | Send bill back to committee |
| `Other` | *(no pattern match)* | Fallback for unrecognized questions |

`VoteType` enum and `fromQuestion` parser are in Component 1 §1.8. Unknown patterns default to `Other`, logged at warn level.

### Placeholder Member Resolution

When vote references member (via `bioguideId` in House positions, or resolved `lisMemberId` in Senate positions) not yet in `members` table, `PlaceholderCreator` (Component 3 §3.6) creates placeholder rows. Members-pipeline (Component 5) fills in full data later.

### Senate LIS Resolution

Senate vote XML identifies members by `lisMemberId` (e.g., "S428"), not `bioguideId`. Resolution flow:
1. Query `lis_member_mapping` table for each `lisMemberId`
2. If mapping exists: use `bioguideId` from mapping
3. If mapping does not exist: skip position, log warn with unresolved `lisMemberId`, continue processing. Vote persisted with resolved positions; unresolved positions tracked in `ProcessingResult` for visibility.

Skip-on-missing pattern: single unmapped senator should not block entire vote ingestion. LIS mapping refresher (Component 5) will eventually populate; next scheduled run picks up missing positions.

### Bill Linkage

- Congress.gov House vote responses include `bill` object with `billId`, `congress`, `type`, `number`
- Senate vote XML includes legislation references mapping to same fields
- Stored as `VoteDO.billId` (FK to `bills` table) — `Option[String]` (procedural votes have no bill)
- Votes-pipeline creates placeholder bill rows via `PlaceholderCreator` if referenced bill not yet in `bills` table
- Procedural votes (null `billId`) stored but excluded from alignment scoring by downstream consumers

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 6.1 House Votes API Client | Migrate + Extend | Extends `CongressGovPaginatedClient` for House vote list and members endpoints |
| 6.2 Senate Vote XML Client | New | Fetches and parses roll call vote XML from senate.gov |
| 6.3 Vote Repository & History | New | Doobie repositories for votes, positions — with archive-before-overwrite |
| 6.4 Vote Change Detection | New | Position-aware change detection: `VoteChangeDetector` |
| 6.5 Vote Processing Pipeline | New | FS2 streaming pipeline: fetch both chambers -> detect -> archive -> upsert -> placeholders -> events |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Congress.gov House vote list/members API integration | [6.1 House Votes API Client](06-votes-pipeline/06.1-house-votes-api-client.md) |
| Senate.gov roll call vote XML fetch and parsing | [6.2 Senate Vote XML Client](06-votes-pipeline/06.2-senate-vote-xml-client.md) |
| AlloyDB persistence for votes, positions + history archival | [6.3 Vote Repository & History](06-votes-pipeline/06.3-vote-repository-history.md) |
| Position-aware vote change detection | [6.4 Vote Change Detection](06-votes-pipeline/06.4-vote-change-detection.md) |
| Full vote processing pipeline: fetch -> detect -> archive -> upsert -> events | [6.5 Vote Processing Pipeline](06-votes-pipeline/06.5-vote-processing-pipeline.md) |

## Cross-Cutting Concerns

### Package Structure

```
repcheck.ingestion.votes
+-- api
|   +-- HouseVotesApiClient               (6.1)
|   +-- SenateVoteXmlClient               (6.2)
+-- repository
|   +-- VoteRepository                    (6.3)
|   +-- VotePositionRepository            (6.3)
|   +-- VoteHistoryArchiver               (6.3)
+-- detection
|   +-- VoteChangeDetector                (6.4)
+-- pipeline
|   +-- VoteProcessor                     (6.5)
+-- errors
    +-- HouseVoteFetchFailed              (6.1)
    +-- SenateVoteFetchFailed             (6.2)
    +-- VoteUpsertFailed                  (6.3)
    +-- VoteArchiveFailed                 (6.3)
    +-- LisResolutionFailed              (6.2)
```

Application entry point (`VotesPipelineApp`) follows standard IOApp + PureConfig + `PipelineBootstrap` pattern (Component 3 §3.7).

### Dependencies

```
votes-pipeline
+-- ingestion-common                     (internal SBT dependency -- Component 3)
|   +-- CongressGovPaginatedClient, XmlFeedClient, IngestionEventPublisher
|   +-- PlaceholderCreator, TransactorResource, UpsertHelper
|   +-- PipelineBootstrap, WorkflowStateUpdater, RetryWrapper
+-- repcheck-shared-models               (published artifact -- Component 1)
|   +-- VoteListItemDTO, VoteDetailDTO, VoteMembersDTO, VoteResultDTO
|   +-- SenateVoteXmlDTO, SenateVoteMemberXmlDTO
|   +-- VoteDO, VotePositionDO, VoteHistoryDO, VoteHistoryPositionDO
|   +-- VoteType (enum), VotePartyTotalDTO
|   +-- HasPlaceholder[MemberDO], HasPlaceholder[BillDO]
+-- repcheck-pipeline-models             (published artifact -- Component 2)
    +-- VoteRecordedEvent, ProcessingResult, PipelineRunSummary
    +-- Tables (Votes, VotePositions, VoteHistory, VoteHistoryPositions)
```

Votes-pipeline does NOT depend on bills-common (Component 4) or members repositories (Component 5). References members/bills only through `PlaceholderCreator` + `EntityRepository`.

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|----------------|
| Unit tests | VoteChangeDetector, vote type parsing, LIS resolution logic, processor flow | MockitoScala |
| WireMock tests | `HouseVotesApiClient` (JSON), `SenateVoteXmlClient` (XML) | WireMock |
| Integration tests | All repositories (CRUD, upsert, history archival, position replacement) | `DockerPostgresSpec` |
| Pipeline integration | Full pipeline flow (both chambers) | WireMock + DockerPostgresSpec + mock Pub/Sub |

### Migration Checklist

After Component 6 implemented:
1. Remove vote fetching/storage from `vote-ingestion` module (legacy code)
2. Remove vote-related API clients from `gov-apis` module
3. Entity-specific DTOs/DOs already in `shared-models` (Component 1)
4. Shared infrastructure already in `ingestion-common` (Component 3)