# Acceptance Criteria: Component 6 — Votes Pipeline

> Single SBT project within `repcheck-data-ingestion` that ingests roll call votes from both chambers.
> House votes come from the Congress.gov JSON API; Senate votes come from senate.gov XML feeds.
> Both sources map to unified DTOs and share the same storage, history, and event emission logic.
> **Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2), `ingestion-common` (Component 3).

---

## System Context

### One Project, Two Sources

Component 6 is a single SBT project (`votes-pipeline`) deployed as a Cloud Run Job:

| Project | Trigger | Responsibility | Publishes |
|---------|---------|---------------|-----------|
| `votes-pipeline` | Scheduled (e.g., every 2 hours) | Fetch roll call votes from Congress.gov (House) and senate.gov (Senate), detect changes, archive history, persist to AlloyDB, create placeholder members for unknown voters | `vote.recorded` |

Unlike Components 4 and 5 (which split responsibilities across multiple projects), the votes pipeline is a single project that handles both chambers. The two data sources converge on the same unified DTO types, so change detection, history archival, storage, and event emission are shared.

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
vote.recorded event
    |
    v
Scoring Engine (downstream)
    +-- Extract all memberIds from vote positions
    +-- Find users whose representatives include those members
    +-- Re-score affected (userId, memberId) pairs
    +-- If bill has no analysis yet, requeue with backoff
```

### History & Archival Strategy

**Vote history** follows the same archive-before-overwrite pattern as bills and members (BEHAVIORAL_SPECS §1):

| Table | History Table | Archive Trigger |
|-------|--------------|-----------------|
| `votes` | `vote_history` | Vote metadata or positions changed |
| `vote_positions` | `vote_history_positions` | Positions archived alongside vote metadata (shared `history_id`) |

A `VoteHistoryArchiver` copies current `votes` and `vote_positions` state to history tables with a shared `history_id` UUID before the upsert overwrites it — same pattern as `BillHistoryArchiver` (Component 4) and `MemberHistoryArchiver` (Component 5).

> **Note**: History table DOs (`VoteHistoryDO`, `VoteHistoryPositionDO`) are defined in Component 1 §1.2. Per project convention, the specification lives here (where the need originates) but the code lives in `repcheck-shared-models`.

### Vote Change Detection

Vote change detection differs from the generic `ChangeDetector` (Component 3 §3.3) used by bills and members:

- **Generic `ChangeDetector`**: Compares `updateDate` timestamps, then performs field-by-field diffing via `Product` reflection. Used for bills, members, amendments.
- **`VoteChangeDetector`**: First compares `updateDate` (fast path skip if unchanged). If dates differ, checks whether vote **positions** actually changed by comparing the incoming position set against the stored positions. This distinction matters because event emission depends on position changes, not just metadata changes.

The `VoteChangeDetector` lives in the votes-pipeline project (not in `ingestion-common`) because it requires vote-specific logic.

### Event Payload

**`VoteRecordedEvent`** (per Component 2 §2.1):
```
voteId: String, billId: Option[String], chamber: String,
date: Instant, congress: Int, isUpdate: Boolean
```

**Emission conditions** (per BEHAVIORAL_SPECS §4):
- **New vote** (first insert): always emit with `isUpdate = false`
- **Updated vote** (positions changed): emit with `isUpdate = true`
- **Updated vote** (metadata changed but positions identical): upsert metadata, archive history, but do **not** emit event (scoring is position-driven)

### Vote Type Classification

Each vote has a `voteType` determined from the `question` field in the API/XML response:

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

Vote type is stored in `VoteDO.voteType` and passed through in the event payload for downstream scoring weight application. Unknown patterns default to `Other` and are logged at warn level for manual review.

> **Note**: The `VoteType` enum and its `fromQuestion` parser are defined in Component 1 §1.8 (`repcheck-shared-models`). The votes-pipeline uses them but does not define them.

### Placeholder Member Resolution

When a vote references a member (via `bioguideId` in House positions, or via resolved `lisMemberId` in Senate positions) not yet in the `members` table, the votes-pipeline creates placeholder member rows via `PlaceholderCreator` (Component 3 §3.6). The members-pipeline (Component 5) fills in full data later.

### Senate LIS Resolution

Senate vote XML identifies members by `lisMemberId` (e.g., "S428"), not `bioguideId`. Resolution flow:

1. Votes-pipeline queries `lis_member_mapping` table for each `lisMemberId`
2. If mapping exists: use the `bioguideId` from the mapping
3. If mapping does not exist: skip that position, log at warn level with the unresolved `lisMemberId`, and continue processing the remaining positions. The vote is still persisted with the positions that could be resolved. The unresolved positions are tracked in `ProcessingResult` for operator visibility.

> **Why skip instead of fail?** A single unmapped senator should not block the entire vote from being ingested. The LIS mapping refresher (Component 5) will eventually populate the mapping, and the next scheduled run of the votes-pipeline will pick up the missing positions.

### Bill Linkage

- Congress.gov House vote responses include a `bill` object with `billId`, `congress`, `type`, and `number`
- Senate vote XML includes legislation references that map to the same fields
- Stored as `VoteDO.billId` (FK to `bills` table) — `Option[String]` because procedural votes have no bill
- The votes-pipeline creates placeholder bill rows via `PlaceholderCreator` if a referenced bill is not yet in the `bills` table
- Procedural votes (null `billId`) are stored but excluded from alignment scoring by downstream consumers

---

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

---

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

Application entry point (`VotesPipelineApp`) follows the standard IOApp + PureConfig + `PipelineBootstrap` pattern from Component 3 §3.7. No area file needed — the app is pure wiring.

### Dependencies

```
votes-pipeline
+-- ingestion-common                     (internal SBT dependency -- Component 3)
|   +-- CongressGovPaginatedClient       (API base, House votes)
|   +-- XmlFeedClient                    (XML parsing, Senate votes)
|   +-- IngestionEventPublisher          (event emission)
|   +-- PlaceholderCreator               (cross-entity refs for unknown members/bills)
|   +-- TransactorResource               (DB connection)
|   +-- UpsertHelper                     (SQL generation)
|   +-- PipelineBootstrap                (config, runId)
|   +-- WorkflowStateUpdater             (step tracking)
|   +-- RetryWrapper                     (operation-level retry)
+-- repcheck-shared-models               (published artifact -- Component 1)
|   +-- VoteListItemDTO, VoteDetailDTO, VoteMembersDTO, VoteResultDTO
|   +-- SenateVoteXmlDTO, SenateVoteMemberXmlDTO
|   +-- VoteDO, VotePositionDO, VoteHistoryDO, VoteHistoryPositionDO
|   +-- VoteType (enum), VotePartyTotalDTO
|   +-- HasPlaceholder[MemberDO], HasPlaceholder[BillDO]
+-- repcheck-pipeline-models             (published artifact -- Component 2)
    +-- VoteRecordedEvent
    +-- ProcessingResult, PipelineRunSummary
    +-- Tables (Votes, VotePositions, VoteHistory, VoteHistoryPositions)
```

> **Note**: The votes-pipeline does NOT depend on `bills-common` (Component 4) or the members repositories (Component 5). It references members and bills only through `PlaceholderCreator` + `EntityRepository` — no direct repository dependency.

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | VoteChangeDetector, vote type parsing, LIS resolution logic, processor flow | MockitoScala |
| WireMock tests | `HouseVotesApiClient` (JSON), `SenateVoteXmlClient` (XML) | WireMock |
| Integration tests | All repositories (CRUD, upsert, history archival, position replacement) | `DockerPostgresSpec` |
| Pipeline integration | Full pipeline flow (both chambers) | WireMock + DockerPostgresSpec + mock Pub/Sub |

### Migration Checklist

After Component 6 is implemented:
1. `vote-ingestion` module (if any existing legacy code): remove vote fetching and storage logic. Replaced by `votes-pipeline`.
2. `gov-apis` module: remove vote-related API clients. Replaced by `HouseVotesApiClient` and `SenateVoteXmlClient`.
3. Entity-specific DTOs/DOs already migrated to `shared-models` in Component 1.
4. Shared infrastructure already migrated to `ingestion-common` in Component 3.
