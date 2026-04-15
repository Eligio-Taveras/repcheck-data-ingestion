# Component 5 Execution Plan — Members Pipeline Projects

**Author**: Staff-level engineer / tech lead  
**Date**: 2026-04-14  
**Scope**: Three SBT projects (`members-common`, `member-profile-pipeline`, `lis-mapping-refresher`) within `repcheck-data-ingestion`, plus cross-repo prerequisite work in `repcheck-shared-models`, `repcheck-db-migrations`, and `repcheck-ingestion-common`.  
**Acceptance Criteria**: `repcheck-pipeline-models/docs/architecture/acceptance-criteria/05-MEMBERS-PIPELINE.md`

---

## Pre-Implementation Inventory

### Already Built (DO NOT rebuild)

| Artifact | Location | Status |
|----------|----------|--------|
| MemberDO (memberId: Long PK, naturalKey: String = bioguideId) | repcheck-shared-models | Complete |
| MemberTermDO, MemberPartyHistoryDO | repcheck-shared-models | Complete |
| LisMemberDO (id: Long PK, naturalKey: String = e.g. "S428") | repcheck-shared-models | **Needs expansion** — add firstName, lastName, party, state, lastVerified |
| MemberLisMappingDO (id: Long, memberId: Long FK→members, lisMemberId: Long FK→lis_members) | repcheck-shared-models | Complete |
| MemberHistoryDO (id: Long PK), MemberTermHistoryDO (historyId: Long) | repcheck-shared-models | Complete |
| MemberListItemDTO (bioguideId: String), MemberDetailDTO | repcheck-shared-models | Complete |
| MemberConversions (toDO with MemberConversionResult) | repcheck-shared-models | Complete |
| SenatorLookupXmlDTO (lisId: String, bioguideId: String, firstName, lastName, party, state, senateClass, serviceDates, isCurrent) | repcheck-shared-models | Complete |
| MemberUpdatedEvent (memberId: String) | repcheck-pipeline-models | Complete |
| Tables: Members, MemberTerms, MemberPartyHistory, MemberHistory, MemberTermHistory, LisMembers, MemberLisMapping | repcheck-pipeline-models | Complete |
| CongressGovPaginatedClient, XmlFeedClient, ChangeDetector, IngestionEventPublisher, PipelineBootstrap, WorkflowStateUpdater, TransactorResource, UpsertHelper, PipelineLogger, DockerPostgres | repcheck-ingestion-common | Complete |
| HasPlaceholder[MemberDO] typeclass | repcheck-shared-models | Complete |
| PlaceholderCreator, DefaultPlaceholderCreator | repcheck-ingestion-common | Complete |
| DB migrations (all member tables including lis_members and member_lis_mapping) | repcheck-db-migrations | Complete |
| DefaultIngestionEventPublisher | repcheck-ingestion-common | **Needs retry** — currently no retry on publish failure |

### Key Data Model: Two-Table LIS Structure

```
lis_members                          member_lis_mapping
+----+-------------+----------+--+  +----+-----------+---------------+---------------+
| id | natural_key | fname |...  |  | id | member_id | lis_member_id | last_verified |
+----+-------------+----------+--+  +----+-----------+---------------+---------------+
|  1 | S428        | John  |...  |  |  1 |        42 |             1 | 2024-06-15    |
+----+-------------+----------+--+  +----+-----------+---------------+---------------+
```

- `lis_members`: LIS member records. `natural_key` = LIS ID string (e.g., "S428"). `id` = BIGSERIAL PK. After expansion: also stores firstName, lastName, party, state, lastVerified.
- `member_lis_mapping`: maps `lis_members.id` → `members.id`. Unique on `(member_id, lis_member_id)`.
- The LIS refresher must: parse XML → upsert `lis_members` → look up member by bioguideId → upsert `member_lis_mapping` with both Long FKs.

### Placeholder Context

The bills pipeline creates placeholder member rows via `PlaceholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)`. The member-profile-pipeline fills these in during its normal upsert cycle. The LIS mapping refresher ALSO creates member placeholders when it encounters a senator whose bioguideId is not yet in the `members` table (the XML provides the bioguideId as a natural key).

**PlaceholderCreator contract** (from `repcheck-ingestion-common`):
- `ensureExists[T](naturalKey: String, repository: EntityRepository[F, T])(using HasPlaceholder[T]): F[Unit]`
- Calls `HasPlaceholder[T].placeholder(naturalKey)` to create a minimal entity (only `naturalKey` populated, all other fields `None`/zero)
- Calls `EntityRepository.insertIfNotExists(entity): F[Unit]` which executes `INSERT ... ON CONFLICT (natural_key) DO NOTHING`
- **Returns `F[Unit]`** — callers MUST query the repository again to get the generated `memberId: Long` after placeholder creation
- The owning pipeline (member-profile-pipeline) fills in the full data later via normal upsert + ChangeDetector diff

### Change Detection Strategy

All pipelines use `ChangeDetector` from ingestion-common with `Differ[T]` typeclasses:
1. Compare `updateDate` as a fast pre-filter
2. If dates differ (or new entity), fetch existing from DB and run full field-by-field diff
3. Upsert only the changed fields

**Note**: The bills pipeline currently does NOT use `ChangeDetector` — it only compares dates and does full upserts. This is a bug. The bills pipeline correction is out of scope for this plan but should be addressed as a follow-up task. The member pipeline will use `ChangeDetector` correctly from the start.

### Repository Pattern: ConnectionIO Composition

**All repositories expose `ConnectionIO` methods, NOT `F[_]` methods.** This enables composing multiple repository calls into a single atomic transaction:

```scala
// Repositories return bare ConnectionIO:
trait MemberRepository {
  def upsert(member: MemberDO): ConnectionIO[Long]
  def findByBioguideId(bioguideId: String): ConnectionIO[Option[MemberDO]]
}

// Processors compose in for-comprehension, then lift once:
val writeProgram: ConnectionIO[Unit] = for {
  _        <- historyArchiver.archiveMember(bioguideId)
  memberId <- memberRepo.upsert(memberDO)
  _        <- termRepo.replaceAll(memberId, terms)
  _        <- partyHistoryRepo.appendNew(memberId, partyHistory)
} yield ()
TransactionRunner.run(xa)(writeProgram)  // single atomic transaction
```

This follows the existing pattern in `BillPersister` and `DoobieBillHistoryArchiver`. Individual repos never call `.transact(xa)` — that responsibility belongs to the caller composing the transaction.

**Exception**: `EntityRepository[F, T].insertIfNotExists` uses `F[_]` (not `ConnectionIO`) because it owns its own transactor — it's called by `PlaceholderCreator` which is framework code outside the repository composition layer.

### What This Plan Builds

| Area | SBT Project / Repo | Classes |
|------|------------|---------|
| Pre-req | repcheck-shared-models | LisMemberDO expansion (add fields) |
| Pre-req | repcheck-db-migrations | Migration to expand lis_members table |
| Pre-req | repcheck-ingestion-common | Add retry to DefaultIngestionEventPublisher |
| Migration | repcheck-data-ingestion (all bill projects) | Package rename `com.repcheck.bills` → `repcheck.ingestion.bills` + migrate to shared MemberRepository |
| Shared | members-common | MemberRepository (trait+Doobie impl), MemberHistoryArchiver, MemberTermRepository, MemberPartyHistoryRepository, MemberInsertSql constant, Differ[MemberDO], error types |
| 5.1 | member-profile-pipeline | MembersApiClient |
| 5.3 | member-profile-pipeline | MemberProfileProcessor |
| 5.4 | lis-mapping-refresher | SenatorLookupXmlClient |
| 5.5 | lis-mapping-refresher | LisMemberRepository, LisMappingRepository (+ UpsertResult) |
| 5.6 | lis-mapping-refresher | LisMappingProcessor |
| Entry | member-profile-pipeline | MemberProfilePipelineApp, PipelineExecutor |
| Entry | lis-mapping-refresher | LisMappingRefresherApp |
| Local Dev | docker-compose | Dockerfiles, docker-compose.local.yml services, ofelia jobs, pubsub topics |

---

## Naming Conventions

### Package Prefix: `repcheck` (not `com.repcheck`)

| Repository | Current Prefix | File Count |
|------------|---------------|------------|
| repcheck-shared-models | `repcheck.shared.models` | 500+ files |
| repcheck-pipeline-models | `repcheck.pipeline.models` | 56 files (100%) |
| repcheck-ingestion-common | `repcheck.ingestion.common` | 128+ files |
| repcheck-data-ingestion | `com.repcheck.bills` | 115+ files (OUTLIER → fixed in Phase 0) |

### Package Structure

- **members-common**: `repcheck.members.common`
- **member-profile-pipeline**: `repcheck.ingestion.members.profile`
- **lis-mapping-refresher**: `repcheck.members.lismapping` (no `ingestion` — enriches, not ingests)
- **bills code (after Phase 0)**: `repcheck.ingestion.bills`

### Field Naming

- `memberId: Long` — BIGSERIAL PK in `members` table
- `bioguideId: String` — Congress.gov bioguide ID (stored as `naturalKey` in MemberDO)
- `lisId: String` — Senate LIS ID (stored as `naturalKey` in LisMemberDO, e.g. "S428")
- Repositories: `findById(memberId: Long)` and `findByBioguideId(bioguideId: String)`

### Correlation IDs

Per-item, not per-pipeline-run:
- Each member processed gets its own UUID in all log messages and ProcessingResult
- Each LIS mapping gets its own UUID
- Pipeline run tracked separately by `runId` from WorkflowStateUpdater

### Logging Levels

| Level | When | Example |
|-------|------|---------|
| DEBUG | Per-item progress | `"Fetching detail for A000001"`, `"Change detected: 3 fields"` |
| INFO | Milestones/summaries | `"Pipeline started for congress 118"`, `"Complete: 540 processed, 12 updated"` |
| WARN | Noteworthy skips | `"Senate member A000001 — no LIS mapping, skipping event"` |
| ERROR | Failures (individual item detail) | `"Failed to fetch A000001: HTTP 500"` (with correlationId, stack trace) |

---

## Phase -1: Cross-Repo Prerequisites

**Agent count**: 3 (one per repo, code in parallel, PRs sequential for version publishing)  
**Creates**: 3 PRs (one per repo, published in order)

### Agent -1A: Expand LisMemberDO + DB Migration

**Repos**: `repcheck-shared-models` + `repcheck-db-migrations`

The senate XML provides these fields per senator that we should store in `lis_members`:

| Field | Source (SenatorLookupXmlDTO) | DB Column | Scala Type |
|-------|----------------------------|-----------|------------|
| lisId | `lisId` | `natural_key` | String (already exists) |
| firstName | `firstName` | `first_name` | Option[String] |
| lastName | `lastName` | `last_name` | Option[String] |
| party | `party` | `party` | Option[String] |
| state | `state` | `state` | Option[String] |
| lastVerified | (set at refresh time) | `last_verified` | Option[Instant] |

**Deliverables:**

1. **DB Migration** (`repcheck-db-migrations`): Add columns to `lis_members`:
   ```sql
   ALTER TABLE lis_members ADD COLUMN first_name TEXT;
   ALTER TABLE lis_members ADD COLUMN last_name TEXT;
   ALTER TABLE lis_members ADD COLUMN party TEXT;
   ALTER TABLE lis_members ADD COLUMN state TEXT;
   ALTER TABLE lis_members ADD COLUMN last_verified TIMESTAMPTZ;
   ```

2. **LisMemberDO expansion** (`repcheck-shared-models`):
   ```scala
   final case class LisMemberDO(
     id: Long,
     naturalKey: String,       // "S428"
     firstName: Option[String],
     lastName: Option[String],
     party: Option[String],
     state: Option[String],
     lastVerified: Option[Instant],
     createdAt: Option[Instant],
   )
   ```

3. Publish new versions: db-migrations first, then shared-models.

**Testing:**
- Migration: verify columns added, existing data preserved
- LisMemberDO: Circe encoder/decoder round-trip, field accessors

### Agent -1B: Add Retry to Event Publisher

**Repo**: `repcheck-ingestion-common`

The `DefaultIngestionEventPublisher` currently has NO retry. All pipelines that publish events are vulnerable to transient Pub/Sub failures.

**Deliverables:**

1. Wrap event publish calls in `RetryWrapper` with configurable `RetryConfig`:
   - Default: 3 retries, exponential backoff (10ms initial, 2x multiplier, 60s cap)
   - Per the project convention from `pipeline-models`
2. If all retries fail, the error propagates to the caller (the processor decides what to do)
3. Add `RetryConfig` to the publisher's constructor or config

**Testing:**
- Unit: mock PubSubEventPublisher that fails twice then succeeds → verify 3 attempts
- Unit: mock that fails all retries → verify error propagates
- Unit: verify backoff timing

**Publish**: New version of ingestion-common after merge.

### Phase -1 Sequencing

```
Agent -1A: shared-models PR → review → merge → publish
Agent -1A: db-migrations PR → review → merge → publish (depends on shared-models version)
Agent -1B: ingestion-common PR → review → merge → publish (independent of -1A)
```

Then update `repcheck-data-ingestion/build.sbt` dependency versions in Phase 0.

---

## Phase 0: Package Migration + Bills Repository Consolidation

**Agent count**: 1  
**Depends on**: Phase -1 merged and published  
**Creates**: 1 PR for review

### Agent 0: Rename + Consolidate

**Scope**: Rename `com.repcheck.bills` → `repcheck.ingestion.bills` AND migrate bills pipeline from `DoobieMemberLookupRepository` to the new shared `MemberRepository`.

**Deliverables:**

1. **Update `build.sbt`** dependency versions for shared-models, pipeline-models, ingestion-common, db-migrations-runner to pick up Phase -1 changes.

2. **Package rename** across all bill projects:
   - `bills-common`: `com.repcheck.bills.common` → `repcheck.ingestion.bills.common`
   - `bill-metadata-pipeline`: `com.repcheck.bills.metadata` → `repcheck.ingestion.bills.metadata`
   - `bill-text-availability-checker`: `com.repcheck.bills.textcheck` → `repcheck.ingestion.bills.textcheck`
   - `bill-text-pipeline`: `com.repcheck.bills.text` → `repcheck.ingestion.bills.text`
   - Update `assembly / mainClass` paths in build.sbt
   - Move source files to match new directory structure
   - Update all imports

3. **Create `members-common` project** (SBT subproject in build.sbt):

   ```scala
   lazy val membersCommon = (project in file("members-common"))
     .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
     .settings(commonSettings)
     .settings(
       name := "members-common",
       libraryDependencies ++= doobie ++ catsEffect ++ logging ++ testDeps,
       libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test,
       // Docker integration tests share a single AlloyDB Omni container
       Test / parallelExecution := false,
     )
   ```

   - Add to root aggregate: `.aggregate(..., membersCommon, ...)`
   - `bill-metadata-pipeline` must `.dependsOn(membersCommon)` (for MemberRepository)

   **Classes in `repcheck.members.common.persistence`:**
   - `MemberRepository` — trait with `ConnectionIO` methods: `findById`, `findByBioguideId`, `upsert`, `findPlaceholders`, `existsWithLisMapping`
   - `DoobieMemberRepository extends MemberRepository` — concrete Doobie implementation (follows trait+impl pattern from `BillRepository`/`DoobieBillRepository`)
   - `existsWithLisMapping(memberId: Long): ConnectionIO[Boolean]` lives on `MemberRepository` because the member-profile-pipeline needs it to decide whether to emit events for Senate members. It queries `member_lis_mapping` — this is a simple cross-table query and acceptable in `members-common`.

   **Constants in `repcheck.members.common`:**
   - `MemberInsertSql` — the INSERT SQL constant used by `DoobieEntityRepository[F, MemberDO]` for placeholder creation. Lives here so both bills and members pipelines reference the same statement.

   **Placeholder creation is NOT part of MemberRepository.** Instead, callers wire a `DoobieEntityRepository[F, MemberDO](xa, MemberInsertSql)` in the entry point (matching the existing `BillMetadataPipeline` pattern). This operates in `F[_]` with its own transactor.

   **Error types in `repcheck.members.common.errors`:**
   - `MemberUpsertFailed`, `MemberArchiveFailed` — flat exceptions per project convention

4. **Migrate bills pipeline** to use `MemberRepository` from `members-common`:
   - `bill-metadata-pipeline` depends on `members-common`
   - Remove `DoobieMemberLookupRepository` from `bills-common`
   - Remove `MemberLookupRepository` trait from `bills-common`
   - Update `BillMetadataPipeline` wiring: create `DoobieEntityRepository[F, MemberDO](xa, MemberInsertSql)` for placeholder creation (same pattern, new shared SQL constant from members-common)
   - Update `MemberResolver` to use `MemberRepository.findByBioguideId` (ConnectionIO, lifted via `TransactionRunner.run`) instead of `MemberLookupRepository.findIdByNaturalKey`
   - The `MemberInsertSql` constant should live in `members-common` alongside `MemberRepository` so both bills and members pipelines reference the same INSERT statement

5. Leave `exceptionUniquenessRootPackages` with both `"com.repcheck"` and `"repcheck"`.

**Testing:**
- `sbt compile test scalafmtCheckAll scalafixAll --check` must pass
- All existing bill pipeline tests must still pass
- MemberRepository gets its own unit + integration tests in members-common

**Risk**: This is a large PR touching ~115+ files for the rename plus the repository consolidation. Consider splitting into two commits within one PR: (1) package rename, (2) repository consolidation.

---

## Phase 1: SBT Scaffolding + Remaining Infrastructure

**Agent count**: 1  
**Depends on**: Phase 0 merged  
**Creates**: 1 PR for review

### Agent 1: Project Scaffolding

**Deliverables:**

#### 1. Add remaining SBT subprojects

`members-common` was created in Phase 0. Now add:

**`member-profile-pipeline`** (mirrors `billMetadataPipeline`):
```scala
lazy val memberProfilePipeline = (project in file("member-profile-pipeline"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .dependsOn(membersCommon % "compile->compile;test->test")
  .settings(pipelineSettings)
  .settings(
    name := "member-profile-pipeline",
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig
      ++ catsEffect ++ doobie ++ diff ++ pubSub ++ fs2 ++ logging ++ testDeps,
    libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test,
    coverageExcludedFiles := ".*MemberProfilePipelineApp",
    assembly / mainClass := Some("repcheck.ingestion.members.profile.app.MemberProfilePipelineApp"),
    assembly / assemblyJarName := "member-profile-pipeline.jar",
  )
```

**`lis-mapping-refresher`** (mirrors `billTextAvailabilityChecker`):
```scala
lazy val lisMappingRefresher = (project in file("lis-mapping-refresher"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .dependsOn(membersCommon % "compile->compile;test->test")
  .settings(pipelineSettings)
  .settings(
    name := "lis-mapping-refresher",
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig
      ++ catsEffect ++ doobie ++ xml ++ pubSub ++ fs2 ++ logging ++ testDeps,
    coverageExcludedFiles := ".*LisMappingRefresherApp",
    assembly / mainClass := Some("repcheck.members.lismapping.app.LisMappingRefresherApp"),
    assembly / assemblyJarName := "lis-mapping-refresher.jar",
  )
```

- Add both to root aggregate
- Add `dockerTest` aliases for both + members-common

#### 2. Create package directories

```
member-profile-pipeline/src/main/scala/repcheck/ingestion/members/profile/
  api/, pipeline/, config/, errors/, app/
member-profile-pipeline/src/test/scala/repcheck/ingestion/members/profile/
  api/, pipeline/, config/, errors/, app/
member-profile-pipeline/src/main/resources/
member-profile-pipeline/src/test/resources/

lis-mapping-refresher/src/main/scala/repcheck/members/lismapping/
  client/, repository/, pipeline/, config/, errors/, app/
lis-mapping-refresher/src/test/scala/repcheck/members/lismapping/
  client/, repository/, pipeline/, config/, errors/, app/
lis-mapping-refresher/src/main/resources/
lis-mapping-refresher/src/test/resources/
```

#### 3. Error types

**In `member-profile-pipeline`** (`repcheck.ingestion.members.profile.errors`):
```scala
final case class MemberFetchFailed(
  bioguideId: Option[String],
  detail: String,
  cause: Option[Throwable] = None
) extends Exception(...)
```

**In `lis-mapping-refresher`** (`repcheck.members.lismapping.errors`):
```scala
final case class LisMappingFetchFailed(detail: String, cause: Option[Throwable] = None) extends Exception(...)
final case class LisMappingUpsertFailed(lisId: String, detail: String, cause: Option[Throwable] = None) extends Exception(...)
final case class MemberPlaceholderFailed(bioguideId: String) extends Exception(...)
```

Note: `MemberUpsertFailed` and `MemberArchiveFailed` already live in `members-common` (created in Phase 0).

#### 4. Config case classes

**`member-profile-pipeline`**: `MemberProfileConfig(congresses: List[Int] = List(118), parallelism: Int = 4, pageDelay: FiniteDuration = 500.millis)`

**`lis-mapping-refresher`**: `LisMappingConfig(currentCongress: Int = 118, congressLookbackWindow: Int = 5, parallelism: Int = 1, requestTimeout: FiniteDuration = 30.seconds)`

#### 5. application.conf / application-test.conf for both projects

#### 6. UpsertResult sealed trait in lis-mapping-refresher

#### 7. Differ[MemberDO] instance in members-common

Create a `Differ[MemberDO]` instance using difflicious auto-derivation for use with `ChangeDetector`. Lives in `members-common` because `ChangeDetector` is called from the member profile processor (in member-profile-pipeline) which depends on members-common.

**Requires adding `diff` to `membersCommon` libraryDependencies in `build.sbt`** (Phase 0 didn't include it because Differ wasn't needed yet).

**Testing:**
- Error type tests: message formatting, cause chaining, field accessors
- Config tests: PureConfig loading, defaults, missing fields
- Differ[MemberDO]: verify diff detection on changed vs unchanged instances
- Validation: `sbt compile` passes

---

## Phase 2: Data Access Layer (Repositories)

**Agent count**: 2 (code in parallel, SBT tests serialized)  
**Depends on**: Phase 1 merged  
**Creates**: 2 PRs for review

### Agent 2A: Member Repositories (AC 5.2)

**Project**: `members-common`

Note: `MemberRepository` already exists from Phase 0 (with `upsert`, `findById`, `findByBioguideId`, `findPlaceholders`, `existsWithLisMapping`). Placeholder insertion is handled separately by `DoobieEntityRepository[F, MemberDO]` (wired in the entry point, not part of MemberRepository). This agent adds the remaining repositories.

**Deliverables:**

1. **MemberHistoryArchiver** — `repcheck.members.common.persistence`
   - `archiveMember(bioguideId: String): ConnectionIO[Unit]`
   - Returns `ConnectionIO` so it can be composed into the caller's transaction (archive + upsert + terms + party history = one atomic transaction)
   - The archiver performs its own member lookup by `natural_key = bioguideId` internally:
     1. Look up member by `natural_key = bioguideId`
     2. If not found → no-op (return `ConnectionIO.unit`)
     3. INSERT into `member_history` (BIGSERIAL id). Uses `RETURNING id` for auto-generated PK.
     4. INSERT all `member_terms` into `member_term_history` with `history_id` = returned id
   - Uses `Tables.MemberHistory`, `Tables.MemberTermHistory`
   - Follows the same pattern as `DoobieBillHistoryArchiver.archiveBill(naturalKey)` in bills-common

2. **MemberTermRepository** — `repcheck.members.common.persistence`
   - `replaceAll(memberId: Long, terms: List[MemberTermDO]): ConnectionIO[Unit]` — delete + insert
   - `findByMemberId(memberId: Long): ConnectionIO[List[MemberTermDO]]`

3. **MemberPartyHistoryRepository** — `repcheck.members.common.persistence`
   - `appendNew(memberId: Long, entries: List[MemberPartyHistoryDO]): ConnectionIO[Unit]` — insert non-duplicates (match on `member_id` + `party_name` + `start_year`)
   - `findByMemberId(memberId: Long): ConnectionIO[List[MemberPartyHistoryDO]]`

**Logging:** DEBUG for SQL/row counts. WARN for archive no-op. ERROR for failures with bioguideId/memberId.

**Testing:**

| Layer | What | How |
|-------|------|-----|
| Unit | Doobie Read/Write compile verification | Auto-derived instances resolve |
| Unit | SQL fragment construction | ConnectionIO values compile |
| Class-level | Archiver: insert member+terms, archive, verify history tables | DockerPostgresSpec |
| Class-level | TermRepo: replaceAll, findByMemberId | DockerPostgresSpec |
| Class-level | PartyHistoryRepo: appendNew, findByMemberId | DockerPostgresSpec |
| Component | Archiver + MemberRepo + TermRepo: shared history_id | Auto-generated Long id links tables |
| Functional | Archive-before-overwrite cycle | v1→archive→v2: history has v1, live has v2 |
| Functional | appendNew deduplication | 3, then 2 overlap + 1 new → 4 total |
| Functional | replaceAll transactional | Replace 2 with 3 → exactly 3 |
| Integration | Full CRUD with AlloyDB Omni | DockerPostgresSpec + migration runner |
| Negative | Archive non-existent → no-op | No error, no rows |
| Negative | replaceAll rollback | Failure after delete → old preserved |

### Agent 2B: LIS Repositories (AC 5.5)

**Project**: `lis-mapping-refresher`

**Deliverables:**

1. **LisMemberRepository** — `repcheck.members.lismapping.repository`
   - `upsertByNaturalKey(lisMemberDO: LisMemberDO): ConnectionIO[Long]` — INSERT ON CONFLICT (natural_key) DO UPDATE SET first_name=, last_name=, party=, state=, last_verified=. Returns `id` via RETURNING.
   - `findByNaturalKey(lisId: String): ConnectionIO[Option[LisMemberDO]]`
   - `findById(id: Long): ConnectionIO[Option[LisMemberDO]]`
   - Uses `Tables.LisMembers`

2. **LisMappingRepository** — `repcheck.members.lismapping.repository`
   - `upsert(mapping: MemberLisMappingDO): ConnectionIO[UpsertResult]` — INSERT ON CONFLICT (member_id, lis_member_id) DO UPDATE SET last_verified=. Uses `xmax`: 0→Inserted, >0→Updated.
   - `upsertBatch(mappings: List[MemberLisMappingDO]): ConnectionIO[List[UpsertResult]]` — preserve order
   - `findByLisMemberId(lisMemberId: Long): ConnectionIO[Option[MemberLisMappingDO]]`
   - `findByMemberId(memberId: Long): ConnectionIO[Option[MemberLisMappingDO]]`
   - Uses `Tables.MemberLisMapping`

**Logging:** DEBUG for SQL/xmax values. ERROR for constraint violations with lisId.

**Testing:**

| Layer | What | How |
|-------|------|-----|
| Unit | Doobie Read/Write for LisMemberDO, MemberLisMappingDO | Codec verification |
| Class-level | LisMemberRepo: upsert new → returns id | DockerPostgresSpec |
| Class-level | LisMemberRepo: upsert existing → same id, fields updated | DockerPostgresSpec |
| Functional | xmax: new → Inserted, existing → Updated | DockerPostgresSpec |
| Functional | Batch: 3 mappings → 3 ordered results | DockerPostgresSpec |
| Functional | Batch transactional | Mid-failure → no partial rows |
| Functional | Two-table flow | upsertByNaturalKey → get id → create mapping → both tables populated |
| Functional | lastVerified updated on re-upsert | t1 → t2 |
| Integration | Full CRUD with AlloyDB Omni | DockerPostgresSpec + migration runner |
| Negative | Unknown FK → LisMappingUpsertFailed | FK violation |

**SBT Test Coordination**: One agent runs `sbt membersCommon/test lisMappingRefresher/test` after both agents finish coding.

---

## Phase 3: API Clients

**Agent count**: 2 (code in parallel, SBT tests serialized)  
**Depends on**: Phase 1 merged  
**Creates**: 2 PRs for review

### Agent 3A: Members API Client (AC 5.1)

**Project**: `member-profile-pipeline`

**Deliverables:**

1. **MembersApiClient** — `repcheck.ingestion.members.profile.api`
   - Extends `CongressGovPaginatedClient[F, MemberListItemDTO]`
   - `fetchPage(params: FetchParams): F[PagedResponse[MemberListItemDTO]]` — `/v3/member/congress/{congress}`
   - `fetchDetail(detailUrl: String): F[MemberDetailDTO]`
   - Inherits `fetchAll(params): Stream[F, MemberListItemDTO]` — uses `Stream.unfoldEval` for lazy pagination, emits items one by one as pages are fetched. NOT `F[List[T]]`.
   - Rate limiting via `pageDelay`, retry via `CongressGovErrorClassifier`
   - Multi-congress: processor calls `fetchAll` once per congress, both streams composed

**Logging:** DEBUG per-page/detail fetch. INFO total count per congress. ERROR with bioguideId on failure.

**Testing:** WireMock (127.0.0.1 + dynamicPort): single page, multi-page, detail, rate limiting, empty page, retry on 500, 404→error, timeout→error, malformed JSON→error.

### Agent 3B: Senator Lookup XML Client (AC 5.4)

**Project**: `lis-mapping-refresher`

**Deliverables:**

1. **SenatorLookupXmlClient** — `repcheck.members.lismapping.client`
   - Constructor: `XmlFeedClient[F]`, `LisMappingConfig`, `PipelineLogger[F]`
   - `fetchMappings(): Stream[F, SenatorLookupXmlDTO]`
   - Fetches senate XML via `XmlFeedClient.fetchXml()` (returns `F[Elem]`), parses into DTOs, filters by congress lookback window, emits as stream
   - Uses `Stream.eval(fetchXml).flatMap(elem => Stream.emits(parse(elem)))` pattern — single fetch, streamed output
   - Although the data set is small (~100 senators), we use `Stream` consistently for best-practice reference code

**Logging:** DEBUG per-senator parse. INFO total/filtered counts. ERROR parse failures.

**Testing:** WireMock (127.0.0.1 + dynamicPort): happy path, congress filter, isCurrent, 404, 500+retry, malformed XML, empty XML.

**SBT Test Coordination**: One agent runs tests for both after coding.

---

## Phase 4: Pipeline Processors

**Agent count**: 2 (code in parallel, SBT tests serialized)  
**Depends on**: Phase 2 AND Phase 3 merged  
**Creates**: 2 PRs for review

### Agent 4A: Member Profile Processor (AC 5.3)

**Project**: `member-profile-pipeline`

**Deliverables:**

1. **MemberProfileProcessor** — `repcheck.ingestion.members.profile.pipeline`
   - Constructor: MembersApiClient, MemberRepository, MemberTermRepository, MemberPartyHistoryRepository, MemberHistoryArchiver, ChangeDetector (with Differ[MemberDO]), IngestionEventPublisher, Transactor[F], MemberProfileConfig, PipelineLogger

**`streamAll(): Stream[F, ProcessingResult]`**
1. For each congress in `config.congresses`: `Stream.emits(congresses).flatMap(c => apiClient.fetchAll(FetchParams(congress = Some(c))))`
2. Per member: generate correlationId, call `processMember` via `parEvalMap(config.parallelism)`

**`processMember(listItem, correlationId): F[ProcessingResult]`**
1. Fetch detail via `apiClient.fetchDetail(listItem.url)`
2. Convert via `MemberDetailDTO.toDO` → `Either[String, MemberConversionResult]`
3. Left → `ProcessingResult.Failure`
4. Lookup existing via `memberRepo.findByBioguideId(bioguideId)`
5. Change detection via `ChangeDetector.detect`:
   - `Unchanged` → `ProcessingResult.Skipped`
   - `Updated` or `New` → proceed
6. **Single Doobie transaction** — compose `ConnectionIO` in a for-comprehension, lift once via `TransactionRunner.run(xa)`:
   ```scala
   val writeProgram: ConnectionIO[Long] = for {
     _        <- if (!isNew) historyArchiver.archiveMember(bioguideId) else ConnectionIO.unit
     memberId <- memberRepo.upsert(memberDO)
     _        <- termRepo.replaceAll(memberId, terms)
     _        <- partyHistoryRepo.appendNew(memberId, partyHistory)
   } yield memberId
   TransactionRunner.run(xa)(writeProgram)
   ```
7. **Event (outside transaction)**: determine chamber from most recent term (highest congress). House → emit. Senate → check `existsWithLisMapping` (separate `TransactionRunner.run` call) → emit if true, WARN if false.
8. Event publish failure → ERROR logged, Success still returned (data was written, retry in publisher handles transient failures)
9. Return `ProcessingResult.Success` with correlationId

**Logging:** DEBUG per-member. WARN for Senate no-mapping. ERROR for failures with correlationId+bioguideId.

**Testing:**

| Layer | What | How |
|-------|------|-----|
| Unit | Chamber from most recent term | Highest congress → that chamber |
| Unit | House → emit, Senate+mapping → emit, Senate-no-mapping → skip | Mock verification |
| Unit | Per-member correlationId | Unique UUIDs |
| Class-level | New, changed, unchanged members | Mock deps |
| Component | streamAll: 3 members, correct result mix | Mocks |
| Component | Processor + real ChangeDetector + Differ[MemberDO] | Correct diff decisions |
| Component | Multi-congress: List(117, 118) | Both fetched |
| Functional | Placeholder fill | Placeholder has updateDate=None → ChangeDetector sees New → upsert+event |
| Functional | toDO failure | Empty bioguideId → Failure |
| Functional | Archive-before-upsert order | InOrder mock |
| Negative | Detail fetch fails one member | Others still processed |
| Negative | Upsert fails | MemberUpsertFailed |
| Negative | Event publish fails all retries | ERROR logged, Success returned |

### Agent 4B: LIS Mapping Processor (AC 5.6)

**Project**: `lis-mapping-refresher`

**Deliverables:**

1. **LisMappingProcessor** — `repcheck.members.lismapping.pipeline`
   - Constructor: SenatorLookupXmlClient, LisMemberRepository, LisMappingRepository, MemberRepository (from members-common), PlaceholderCreator (from ingestion-common), EntityRepository[F, MemberDO] (for placeholder wiring), IngestionEventPublisher, Transactor[F], LisMappingConfig, PipelineLogger

**`refreshAll(): Stream[F, ProcessingResult]`**
1. Fetch mappings via `xmlClient.fetchMappings()` (returns `Stream[F, SenatorLookupXmlDTO]`)
2. Per DTO via `parEvalMap(config.parallelism)`, generate per-item correlationId, call `processMapping`:

**`processMapping(dto: SenatorLookupXmlDTO, correlationId: UUID): F[ProcessingResult]`**

All steps operate in `F[_]`, each DB call lifted via `TransactionRunner.run(xa)(...)`:

```scala
def processMapping(dto: SenatorLookupXmlDTO, correlationId: UUID): F[ProcessingResult] = {
  for {
    lisMemberDO  <- buildLisMemberDO(dto)                                           // pure
    lisMemberId  <- TransactionRunner.run(xa)(lisMemberRepo.upsertByNaturalKey(lisMemberDO))  // txn 1
    memberOpt    <- TransactionRunner.run(xa)(memberRepo.findByBioguideId(dto.bioguideId))    // txn 2
    memberId     <- memberOpt match {
      case Some(m) => Async[F].pure(m.memberId)
      case None    =>                                                               // placeholder path
        placeholderCreator.ensureExists[MemberDO](dto.bioguideId, memberEntityRepo) // txn 3 (F[Unit])
        *> TransactionRunner.run(xa)(memberRepo.findByBioguideId(dto.bioguideId))   // txn 4
            .flatMap(_.liftTo[F](MemberPlaceholderFailed(dto.bioguideId)))
            .map(_.memberId)
    }
    mappingDO     = MemberLisMappingDO(0L, memberId, lisMemberId, Instant.now())
    upsertResult <- TransactionRunner.run(xa)(mappingRepo.upsert(mappingDO))         // txn 5 (or 3 happy)
    _            <- upsertResult match {
      case UpsertResult.Inserted => publisher.publish(MemberUpdatedEvent(dto.bioguideId))
      case UpsertResult.Updated  => Async[F].unit
    }
  } yield ProcessingResult.Success(correlationId)
}
```

**Transaction structure:**
- Happy path (member exists): 3 separate transactions (lis upsert, member lookup, mapping upsert)
- Placeholder path (member not found): 5 transactions (lis upsert, member lookup, placeholder insert, member re-query, mapping upsert)
- These are intentionally separate transactions — there's no atomicity requirement across them. Each step is independently idempotent (upserts use ON CONFLICT, placeholder uses DO NOTHING).

The entry point consumes this stream via `PipelineExecutor` (same pattern as member-profile-pipeline). Counts (inserted, updated, events emitted) are derived from the `ProcessingResult` stream at the executor level — no need for a separate accumulator.

**Logging:** DEBUG per-mapping. INFO start/complete with counts. WARN placeholder created. ERROR failures with correlationId+lisId.

**Testing:**

| Layer | What | How |
|-------|------|-----|
| Unit | DTO→LisMemberDO: fields mapped, lastVerified within 1s | Time assertion |
| Unit | Inserted + member exists → emit event → Success | Publisher called |
| Unit | Inserted + member not found → placeholder + mapping + emit → Success | PlaceholderCreator + findByBioguideId called |
| Unit | Updated → no event → Success | Publisher NOT called |
| Class-level | refreshAll stream: 10 DTOs → 10 ProcessingResults | Mocks, `.compile.toList` in test |
| Component | Two-table flow: lis_members + member_lis_mapping | Correct FKs |
| Functional | Placeholder creation flow | Member not in DB → placeholder → mapping → event |
| Functional | Result mix: 3 Inserted, 7 Updated → 3 events | ProcessingResult stream |
| Negative | XML failure → stream error | Error propagates |
| Negative | Event publish fails all retries → Failure result | ProcessingResult.Failure |
| Negative | Empty XML → empty stream | Zero results |

**SBT Test Coordination**: One agent runs tests for both.

---

## Phase 5: Entry Points

**Agent count**: 2 (code in parallel, SBT tests serialized)  
**Depends on**: Phase 4 merged  
**Creates**: 2 PRs for review

### Agent 5A: Member Profile Pipeline Entry Point

**Project**: `member-profile-pipeline`

1. **MemberProfilePipelineApp** — IOApp, pure wiring, delegates to executor
2. **PipelineExecutor** — testable logic:
   - Load config → logger → Resource(transactor, httpClient) → instantiate repos + archiver + API client + processor
   - `processor.streamAll()` → compile → `PipelineRunSummary` → exit code
   - WorkflowStateUpdater if `WORKFLOW_RUN_ID` set

**Testing:** Unit (stubbed factories), functional (DockerPostgres + WireMock end-to-end)

### Agent 5B: LIS Mapping Refresher Entry Point

**Project**: `lis-mapping-refresher`

1. **LisMappingRefresherApp** — IOApp, pure wiring
   - Load config → logger → Resource(transactor, httpClient) → instantiate XmlFeedClient + SenatorLookupXmlClient + LisMemberRepo + LisMappingRepo + MemberRepo + PlaceholderCreator + processor
   - `processor.refreshAll()` returns `Stream[F, ProcessingResult]` → delegates to `PipelineExecutor.execute()` (same pattern as member-profile-pipeline)
   - WorkflowStateUpdater if configured

**Testing:** Unit (stubbed), functional (DockerPostgres + WireMock)

---

## Phase 6: Integration + Local Dev Environment

**Agent count**: 1  
**Depends on**: Phase 5 merged  
**Creates**: 1 PR for review

### Agent 6: Integration Tests + Docker Compose

#### 1. Cross-project integration tests (DockerRequired)
- Senator lifecycle: profile pipeline → no LIS mapping → skip event → LIS refresher → mapping created → event emitted
- Placeholder fill by profile pipeline: placeholder → full data → history → event
- Placeholder created by LIS refresher for unknown senator → profile pipeline fills it later
- Both pipelines sharing AlloyDB without deadlocks

#### 2. E2E test stubs (E2ETest tagged)
Full pipeline: WireMock Congress.gov + WireMock senate.gov + DockerPostgres + mock Pub/Sub

#### 3. Docker Compose
- Dockerfiles (distroless java21)
- docker-compose.local.yml services for both pipelines
- ofelia cron jobs (offset from bill pipelines)
- pubsub-init.sh: `member-updated` topic
- Build + smoke test

---

## Agent Execution Order

```
Phase -1: [Agent -1A: LisMemberDO + migration] → PR → merge → publish
          [Agent -1B: Publisher retry]          → PR → merge → publish
              │
Phase 0:  [Agent 0: Package rename + MemberRepo consolidation] → PR → merge
              │
Phase 1:  [Agent 1: Scaffolding] → PR → merge
              │
Phase 2:  [Agent 2A: Member Repos] ─┐ parallel code
          [Agent 2B: LIS Repos]     ─┤ serial test → 2 PRs → merge
              │
Phase 3:  [Agent 3A: Members API]  ─┐ parallel code
          [Agent 3B: Senator XML]  ─┤ serial test → 2 PRs → merge
              │
Phase 4:  [Agent 4A: Member Proc]  ─┐ parallel code
          [Agent 4B: LIS Proc]     ─┤ serial test → 2 PRs → merge
              │
Phase 5:  [Agent 5A: Profile App]  ─┐ parallel code
          [Agent 5B: Refresher App]─┤ serial test → 2 PRs → merge
              │
Phase 6:  [Agent 6: Integration + Docker] → PR → merge
```

**Total**: 14 agent assignments, 8 phases, 13 PRs  
**SBT coordination**: parallel code writing, serial test runs

---

## Critical Design Decisions

1. **Two-table LIS**: `lis_members` (stores LIS senator data) + `member_lis_mapping` (FK links)
2. **Package prefix**: `repcheck.*` everywhere after Phase 0 migration
3. **`members-common`**: shared by both pipelines AND bills pipeline (replaces DoobieMemberLookupRepository)
4. **Placeholder creation**: `PlaceholderCreator.ensureExists` takes `naturalKey: String`, creates minimal entity via `HasPlaceholder[T].placeholder(naturalKey)`, calls `EntityRepository.insertIfNotExists` (`ON CONFLICT DO NOTHING`), returns `F[Unit]` — caller must re-query to get generated ID
5. **`memberId: Long` for PK, `bioguideId: String` for natural key**: both lookup paths on repositories
6. **History archiver**: BIGSERIAL auto-increment IDs, `RETURNING id` to link tables. Archiver does its own member lookup by bioguideId internally.
7. **All repositories expose `ConnectionIO`**: never `F[_]`. Composed in for-comprehensions, lifted once via `TransactionRunner.run(xa)`. Exception: `EntityRepository[F, T]` (framework code) uses `F[_]` with its own transactor.
8. **Transaction boundary**: archive+upsert+terms+party in single `ConnectionIO` for-comprehension; events outside transaction in `F[_]`
9. **xmax**: `RETURNING xmax` for insert-vs-update detection on LIS mappings
10. **ChangeDetector + Differ[MemberDO]**: full field diff after updateDate pre-filter
11. **Event publisher retry**: built into DefaultIngestionEventPublisher (Phase -1B), all retries fail → error propagates
12. **LIS refresher creates member placeholders**: when bioguideId not in members table, uses PlaceholderCreator (returns `F[Unit]`), then re-queries to get `memberId`, then creates mapping
13. **All events carry bioguideId**: LIS refresher gets bioguideId directly from `SenatorLookupXmlDTO.bioguideId` (XML provides it), no extra lookup needed
14. **Correlation IDs per item**: each member/mapping gets unique UUID
15. **Multi-congress**: `congresses: List[Int]`, defaults to `List(118)`
16. **Logging**: DEBUG per-item, INFO milestones, WARN notable skips, ERROR failures only
17. **All clients return `Stream[F, T]`**: MembersApiClient inherits `Stream` from `CongressGovPaginatedClient.fetchAll`; SenatorLookupXmlClient wraps single XML fetch in `Stream.eval(...).flatMap(Stream.emits(parse(...)))` for consistency
