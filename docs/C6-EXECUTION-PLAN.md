# Votes Pipeline — Master Execution Plan

## Context

RepCheck needs roll-call vote ingestion to unblock Phase 3 (LLM analysis) and Phase 4 (scoring). Bills and members pipelines are already shipping data; without votes, the scoring engine has no signal for member-vs-user alignment. This plan delivers Component 6 (`votes-pipeline`) end-to-end — Scala code, tests across six layers, CI wiring, local Docker, terraform, and a staged release to prod.

Scope: **code + CI + local Docker + terraform + staging deploy** (confirmed with user).

Spec authority: `C:\Users\elita\source\repos2024\votr\docs\architecture\acceptance-criteria\06-VOTES-PIPELINE.md` and the five area files under `06-votes-pipeline/`. Agents implementing should read the **compressed** versions in `.claude/agent-docs/architecture/acceptance-criteria/06-votes-pipeline/*.compressed.md` per CLAUDE.md.

Total acceptance criteria: **99 rows** across 5 areas.

## Current State (2026-04-21)

### Progress snapshot

**Phase 0: COMPLETE** — all prerequisites merged.

| PR | Status | Notes |
|---|---|---|
| P0.1 shared-models VoteType PG enum + DTO corrections | Merged (PR #28, v0.1.26) | Folded in House DTO field-name fixes; published via release.yml |
| P0.2 LisMappingRepository → members-common + batch | Merged (data-ingestion #37) | Added `findByLisMemberIds` batch method; consolidated LIS cleanup in members-common TransactorFixture |
| P0.3a shared-models plugin bump | Merged (PR #29, v0.1.27) | 0 violations |
| P0.3b pipeline-models plugin bump | Merged (PR #20, v0.1.18) | 2 RetryWrapper fixes via re-raise pattern |
| P0.3c ingestion-common plugin bump | Merged (PR #20, v0.1.18) | 0 violations, 25 Docker tests green |
| P0.3d db-migrations plugin bump | Merged (PR #20) | 0 violations |
| P0.3e data-ingestion plugin bump | Merged (data-ingestion #38) | 3 violations fixed via local `*HttpError` + per-API classifiers; later collapsed to one-liners off a shared base (see bonus PRs below) |
| P0.4 GlobalRequestThrottle | **DELETED** (2026-04-21) | Wrong abstraction — replaced by the existing per-client `rateLimitedClient` pattern (Semaphore(1) + pageDelay) that every pipeline already uses |
| P0.5 votr docs revisions | Merged (votr #112) | 4 source files + 4 compressed outputs regenerated via `scripts/generate-agent-docs.ps1` |
| P0.6 PubSubEmulatorFixture | **SKIPPED** (conditional gate resolved 2026-04-21) | Audit confirmed `PubSubEmulatorFixture` already exists in `bills-common/src/test/scala/.../testing/`; votes-pipeline imports via `billsCommon % "test->test"` |
| P0.7 VoteRecordedEvent refactor | Merged (PR #21, v0.2.0) | Drop `voteId`; rename `naturalKey` → `billNaturalKey`; add required `voteNaturalKey` |

**Bonus (surfaced during review cycles):**

| PR | Status | Notes |
|---|---|---|
| ingestion-common #21: `HttpStatusErrorClassifier` abstract base | Merged (v0.1.19) | Extract `classify` loop so per-API classifiers become pure wiring |
| ingestion-common #22: generic `HttpStatusErrorClassifier[E <: HttpStatusError]` + `HttpStatusError` marker trait | Merged (v0.1.20) | Subclasses collapse to one-liners (`Set(...)` + type param); `extractStatusCode` no longer needed |

**Phase 1: COMPLETE** — scaffold shipped.

| PR | Status | Notes |
|---|---|---|
| P1.1 votes-pipeline scaffold | Merged (data-ingestion #39) | New subproject directory tree + App/Pipeline/PipelineExecutor + `rateLimitedClient` helper + VotesPipelineConfig + Dockerfile + application.conf(s) + smoke test + PipelineExecutor spec (100% coverage). Launcher contract: `args(0)` = config JSON, `args(1)` = runId, `args(2)` = stepRunId. `StepProgress` streaming fold retains full `ProcessingResult.Failed` context for debug logging. |

### Next focus: Phase 2 — 5 parallel-ready PRs

All of P2.1–P2.5 depend only on P1.1 (merged) and P0.2 (merged, for P2.3). They can all open concurrently on separate branches within `repcheck-data-ingestion`, touching different packages under `votes-pipeline/`:

| PR | Scope | Package |
|---|---|---|
| P2.1 HouseVotesApiClient | Paginated Congress.gov `/house-vote` client + client-side lookback filter + WireMock tests | `votes-pipeline/.../api/` |
| P2.2 SenateVoteXmlClient + Decoder | senate.gov XML client + scala-xml parser + fixture-based tests | `votes-pipeline/.../xml/` |
| P2.3 LisResolver | Batch LIS → bioguide resolution + placeholder creation for unknown LIS ids + lis-mapping-refresher merge step | `votes-pipeline/.../lis/` |
| P2.4 Vote repositories | VoteRepository, VotePositionRepository, VoteHistoryArchiver, StanceMaterializationStatusRepository (all Doobie) | `votes-pipeline/.../repo/` |
| P2.5 VoteChangeDetector | New / Updated / Unchanged diffing against stored state + position ADT diffs | `votes-pipeline/.../pipeline/` |

After all 5 merge → P3.1 VoteProcessor (wires them together).

### Already exists (verified via audit)

| Area | Location | Notes |
|---|---|---|
| All vote DTOs (House JSON, Senate XML) | `repcheck-shared-models/.../dto/vote/` | VoteListItemDTO, VoteDetailDTO, VoteMembersDTO, VoteResultDTO, SenateVoteXmlDTO, SenateVoteMemberXmlDTO — Circe codecs included |
| All vote DOs | `repcheck-shared-models/.../dos/vote/` | VoteDO, VotePositionDO, VoteHistoryDO, VoteHistoryPositionDO |
| `VoteType` enum + `apiValue` + `fromString` + `fromQuestion` | `repcheck-shared-models/.../vote/VoteType.scala` | 8 variants. `apiValue` matches DB enum strings ("Conference Report", "Veto Override" etc). `fromString` for DB↔enum, `fromQuestion` for DTO→DO. |
| `vote_type_enum` PG ENUM | `repcheck-db-migrations/.../013-enum-type-constraints.sql:46` | Values: 'Passage', 'Conference Report', 'Cloture', 'Veto Override', 'Amendment', 'Committee', 'Recommit', 'Other'. Column `votes.vote_type` already ALTERed to this type. |
| `HasPlaceholder[MemberDO \| BillDO]` | shared-models DO files | Implicit instances present |
| DB schema — all tables | `repcheck-db-migrations/.../001-initial-schema.sql`, `009-scoring-architecture.sql`, `011-*.sql`, `018-expand-lis-members.sql` | votes, vote_positions, vote_history, vote_history_positions, stance_materialization_status, lis_member_mapping |
| `VoteRecordedEvent` | `repcheck-pipeline-models/.../events/EventPayloads.scala:48` | Uses `naturalKey: Option[String]` per the DO PK refactor (correct; spec is stale) |
| `Tables.*` constants (Votes, VotePositions, VoteHistory, VoteHistoryPositions, StanceMaterializationStatus, LisMemberMapping) | `repcheck-pipeline-models/.../Tables.scala` | |
| `CongressGovPaginatedClient`, `XmlFeedClient`, `IngestionEventPublisher.voteRecorded`, `PlaceholderCreator`, `TransactorResource`, `PipelineBootstrap`, `RetryWrapper`, `CongressGovErrorClassifier`, `PipelineLogger`, `EntityRepository`, `LogContext`, `CongressGovClientConfig` | `repcheck-ingestion-common` + `repcheck-pipeline-models` | All plumbing already exists |
| `DockerPostgres` / `SharedDockerPostgres` / `DockerPostgresSpec` | `repcheck-ingestion-common/.../testing/DockerPostgres.scala` | AlloyDB Omni fixture |
| `DockerRequired` tag | same file | `E2ETest` tag lives in bills-common |
| `LisMappingRepository` (writer side) | `repcheck-data-ingestion/lis-mapping-refresher/.../LisMappingRepository.scala` | Has `findByLisMemberId` (singular). Needs promotion + batch method. |
| g8 template | `repcheck-g8/src/main/g8/` | Use archetype=pipeline, has_pubsub + has_alloydb |
| `ExceptionUniquenessPlugin` v0.5.0 | `repcheck-sbt-plugins` | v0.5.0 adds `ProjectExceptionsOnlyCheck` — fails `sbt test` if production code (non-test) constructs non-project exceptions at `throw new X(...)`, `throw X(...)`, or `*.raiseError(new X(...))` sites. Re-raises (`throw e`, `raiseError(err)`) are allowed. **data-ingestion currently pinned to 0.4.0** — needs bump to 0.5.0 + fix any surfaced violations. |

### Missing / needs work

| Gap | Fix | Phase |
|---|---|---|
| `VoteType` missing from `DoobieEnumInstances.scala` (shared-models) | Add `voteTypeMeta = pgEnumStringOpt("vote_type_enum", VoteType.fromString(_).toOption, _.apiValue)` + Get/Put — same pattern as `voteMethodMeta` at lines 100-107. Wires the existing `vote_type_enum` PG ENUM to Scala `VoteType` enum. | P0.1 |
| `LisMappingRepository` is private to `lis-mapping-refresher`; no batch read | Move to `members-common`; add `findByLisMemberIds(ids: List[Long]): ConnectionIO[Map[Long, Long]]`; update lis-mapping-refresher import path | P0.2 |
| **All 5 repos** pinned to `sbt-exception-uniqueness` 0.4.0 | Bump to 0.5.0 (enables `ProjectExceptionsOnlyCheck`) in every repo + fix any pre-existing violations. Audit confirmed: shared-models (0.4.0), pipeline-models (0.4.0), ingestion-common (0.4.0), db-migrations (0.4.0), data-ingestion (0.4.0). | P0.3a–P0.3e (parallel) |
| Per-client HTTP pacing for votes-pipeline | Copy the existing `rateLimitedClient` helper pattern (private `Resource[F, Client[F]]` wrapping `Semaphore(1)` + `pageDelay` between releases — canonical copies live in `bill-metadata-pipeline`, `bill-text-availability-checker`, `bill-text-pipeline`) into the votes-pipeline `app/` package. Each HTTP client (House, Senate) gets its own wrapped `Client` with configurable `pageDelay` / `requestDelay`. No global/shared throttle; naturally bounded by per-client `parEvalMap(parallelism)` upstream. | P1.1 / P2.1 / P2.2 |
| `ProcessingResult` / `PipelineRunSummary` location | Check bills-common + members-common; if present, import. If local-only, leave as votes-local case until a broader cleanup. | P1.1 (verify during scaffold) |
| `PubSubEmulatorFixture` | Check members-common; import if present, otherwise add to votes-pipeline test sources | P1.1 (scaffold verify) |
| `E2ETest` tag promotion | Keep in bills-common or promote to ingestion-common — out of scope; votes imports from bills-common | — |
| Senate XML → DTO parser | `XmlFeedClient` returns `Elem`. Build a `SenateVoteXmlDecoder` inside votes-pipeline that reads the scala-xml Elem into `SenateVoteXmlDTO` | P2.2 |
| tf-repcheck-infra: vote-events topic, Cloud Run Job, Scheduler, IAM | Model after bills-events pattern | Phase 5 |

## Target Architecture (brief)

```
repcheck-data-ingestion/
  votes-pipeline/                          [NEW sbt subproject]
    src/main/scala/repcheck/ingestion/votes/
      app/      VotesPipelineApp.scala, VotesPipeline.scala (companion with factory fns)
      api/      HouseVotesApiClient.scala
      xml/      SenateVoteXmlClient.scala, SenateVoteXmlDecoder.scala
      lis/      LisResolver.scala           (votes-local; reads members-common repo)
      repo/     VoteRepository.scala, DoobieVoteRepository.scala,
                VotePositionRepository.scala, DoobieVotePositionRepository.scala,
                VoteHistoryArchiver.scala, DoobieVoteHistoryArchiver.scala,
                StanceMaterializationStatusRepository.scala
      pipeline/ VoteChangeDetector.scala, VoteProcessor.scala, ProcessingResult.scala (if local)
      config/   VotePipelineConfig.scala, HouseVotesConfig.scala, SenateVoteXmlConfig.scala
      errors/   HouseVoteFetchFailed, SenateVoteFetchFailed, LisResolutionFailed,
                VoteUpsertFailed, VoteArchiveFailed, VoteProcessingFailed
    src/test/scala/...
  members-common/
    persistence/LisMappingRepository.scala  [MOVED IN P0.2 + batch method]
```

Dependencies (build.sbt): `votes-pipeline` depends on `members-common % "compile->compile;test->test"` (for LisMappingRepository + shared test fixtures) and pulls `repcheck-ingestion-common`, `repcheck-shared-models`, `repcheck-pipeline-models`, `repcheck-db-migrations-runner % Test` via GitHub Packages.

### Unknown-member handling (uniform placeholder strategy, both chambers)

Per user direction, both chambers handle unknown members via placeholder creation (no skip-and-hope). The next scheduled run of the relevant enrichment pipeline replaces the placeholder's stub fields with real data.

**House (Congress.gov JSON) — unknown bioguide:**
1. API response includes `bioguideID: String` (e.g., `"A000055"`).
2. Before upserting `VoteDO` / `VotePositionDO`, call `PlaceholderCreator.ensureExists[MemberDO]` keyed by `bioguideID`.
3. `HasPlaceholder[MemberDO]` typeclass supplies the placeholder row: `natural_key = bioguide`, `first_name = "Unknown"`, `last_name = "Member (pending enrichment)"`, `current_party = "I"` (placeholder default), `state = "??"`, etc. All placeholder fields are OVERWRITTEN by members-pipeline's next `/member/{bioguide}` fetch.
4. Vote + positions persist fully. FK constraint on `vote_positions.member_id → members.id` is satisfied via the placeholder's BIGSERIAL id.

**Senate (senate.gov XML) — unknown LIS member:**
1. XML response includes `lis_member_id: Long` but NO bioguide.
2. `LisResolver` looks up `lis_member_mapping` table. If mapping exists → resolve to `members.id`, done. If not → PROCEED with placeholder creation (instead of skip).
3. Create placeholder `MemberDO` with synthetic natural key `s"lis:$lisMemberId"` (e.g., `"lis:412"`). Distinguishable from real bioguides (always letter + 6 digits, e.g., `"A000055"`) by the `:` separator.
4. Upsert a row into `lis_member_mapping` linking `lis_member_id → members.id` (the placeholder's Long PK).
5. Vote + positions persist with references to the placeholder's `members.id`.
6. **Resolution path** (out of scope of votes-pipeline but documented here): when `lis-mapping-refresher` (Component 5) next runs senator lookup XML and discovers the real bioguide for that LIS ID, it updates the placeholder in place:
   - Load the member row whose `lis_member_mapping.lis_member_id` matches.
   - If `natural_key` still matches the synthetic `lis:*` pattern → UPDATE `natural_key` to real bioguide + populate `first_name`, `last_name`, `current_party`, `state`, etc. from senators.gov data. Placeholder's `members.id` remains stable → all `vote_positions` FKs stay valid.
   - If a different real member with that bioguide already exists (rare edge case — shouldn't happen because bioguide→member is 1:1, but handle defensively): merge — migrate `vote_positions.member_id` from placeholder → real, delete placeholder, update `lis_member_mapping`.

**This means P2.3 scope expands to include a small lis-mapping-refresher change** for the placeholder-merge behavior; see P2.3 details below.

**Why synthetic natural_key, not nullable:**
- `members.natural_key` is `UNIQUE NOT NULL` (migration 011:547-548). Making it nullable would require a schema migration with broad impact and breaks the invariant that every member is queryable by a natural key.
- Synthetic key with `"lis:"` prefix preserves NOT NULL, preserves uniqueness (LIS IDs are unique within senate.gov), and is self-documenting (a human looking at `natural_key = "lis:412"` knows it's a pending-resolution placeholder).

**Divergence from original §6.2 spec**: spec §6.2.8–12 describes skip-and-warn for unresolved LIS IDs. This plan REPLACES that with placeholder creation per user direction. Acceptance criteria §6.5.24–25 (partial LIS resolution persisted / unresolved count logged) are REPLACED with:
- **§6.5.24 (revised)**: Senate vote with N unknown LIS IDs → N placeholder members created (with synthetic `lis:*` natural keys), full position list persisted.
- **§6.5.25 (revised)**: placeholder creation events logged at info level with count + IDs (not warn); operator visibility without alarming.

### Enum conversion flow — DTO → DO → DB

Vote type stays a typed enum through the entire stack; string conversion happens in the DO layer, NOT in the repository or DTO codec.

```
Congress.gov API JSON           DTO (string)           DO (typed)              AlloyDB (PG enum)
"question":"On Passage"   →   question: String   →   voteType: VoteType   →   vote_type_enum
                              (VoteMembersDTO)        (VoteDO)                 'Passage'
                                  │                       ▲                       ▲
                                  │                       │                       │
                                  │  VoteMembersDTO.toDO  │     Meta[VoteType] via pgEnumStringOpt
                                  │  uses                 │     (maps apiValue ↔ enum variant)
                                  └── VoteType.fromQuestion(question) ──┘
```

- **DTO layer**: `VoteMembersDTO` carries the raw API string `voteQuestion: String`. No parsing.
- **DO layer (conversion site)**: `VoteMembersDTO.toDO` calls `VoteType.fromQuestion(voteQuestion)` to produce `VoteDO(voteType: VoteType, ...)`. `VoteDO.voteType` is the typed enum.
- **DB layer**: `Meta[VoteType]` (defined in shared-models `DoobieEnumInstances.scala`, added in P0.1) serializes the enum to/from the `vote_type_enum` PG ENUM column via `apiValue`.
- **Event layer**: `VoteRecordedEvent` does not carry vote type. No Circe leak needed for DB-facing concerns.

Rationale (user direction): keep DB as an enum (not TEXT) for integrity + query performance. Put the lossy question→type parsing in the DO — that's the semantic boundary where "API data" becomes "our domain". The repository stays dumb (just persists the typed DO). Add a test at the DO-conversion boundary (see P0.1 and P2.4 test matrices).

## Roadmap Overview

| Phase | PR count | Critical path? | Can overlap with |
|---|---|---|---|
| Phase 0 — Prerequisites | 9 PRs (P0.1, P0.2, P0.3a–e, P0.5, P0.7) + **P0.6 CONDITIONAL** (opens only if P1.1 audit finds no existing PubSubEmulatorFixture) | Yes — P0.1, P0.2, P0.5, P0.7 all block P1.1; P0.3a-e parallel. All 9 can open concurrently. P0.6 is gated on P1.1 audit outcome. | — |
| Phase 1 — Scaffold | 1 PR (P1.1) | Yes | — |
| Phase 2 — Implementation (parallel) | 5 PRs (P2.1–P2.5) | Yes, but internal branches parallelize | Phase 5 (infra) |
| Phase 3 — Processor + App | 3 PRs (P3.1–P3.3) | Yes | Phase 5 |
| Phase 4 — CI + Local Docker | 2 PRs (P4.1, P4.2) | No — gates final merge, not each impl PR | Phase 2/3 |
| Phase 5 — Infrastructure (zero-cost GCP only) | 2 PRs (P5.1, P5.4) | No — independent | Phase 2+ |
| Phase 6 — Local validation | Ops steps (docker-compose E2E) | Yes | — |
| Phase 7 — **DEFERRED** GCP compute deploy | Not in scope now | Deferred until system is truly ready (user approval gate) | — |

**Total: 23 PRs guaranteed + 1 conditional (P0.6)** → **23–24 PRs** + local-validation ops. **GCP compute deploy (Cloud Run Job, Cloud Scheduler) explicitly deferred** to Phase 7 (separate future plan, not in this plan's scope).

**Deployment philosophy (user direction)**: validate the full system locally via `docker-compose.local.yml` + `docker-compose.e2e.yml` first. Avoid paying for GCP compute (Cloud Run, Cloud Scheduler) until end-to-end correctness is proven locally. Create GCP resources that DO NOT cost money to hold — Pub/Sub topics/subscriptions, IAM bindings, Secret Manager bindings — so the deploy step (when it comes) is plug-and-play.

**Critical path**: (P0.1 ∥ P0.2 ∥ P0.3a ∥ P0.3b ∥ P0.3c ∥ P0.3d ∥ P0.3e ∥ P0.5 ∥ P0.7) → P1.1 → Phase 2 (parallel merge) → P3.1 → P3.2 → P3.3 → P4 → P5.1 ∥ P5.4 → P6. All 9 Phase 0 PRs can open concurrently (P0.7 rebases on P0.3b at merge time — same repo). Max parallelism = 9 agents (one per Phase 0 PR).

**Sequencing notes**:
- P0.5 lands in `votr` repo (only one touching votr for this plan). No conflict risk with other PRs.
- P0.2 and P0.3e both land in data-ingestion. Different files (LisMappingRepository in members-common vs plugins.sbt) — low conflict risk. Second-to-merge rebases.

**Parallel opportunities**:
- Within Phase 2: all 5 PRs can open on separate branches concurrently (different files).
- Phase 5 terraform work can proceed alongside Phase 2 implementation — independent repo.
- Phase 4 Docker Compose entry can piggyback on P3.2.

---

## Phase 0 — Prerequisites

### P0.1 — `repcheck-shared-models`: wire `VoteType` to the `vote_type_enum` PG ENUM + verify DTO→DO conversion

**Goal**: complete the VoteType stack — DTO string is parsed to the typed `VoteType` enum in the DO layer, and the DO is persisted natively as the existing `vote_type_enum` PG enum. User direction: the DB column IS an enum, conversion happens in the DO, not in SQL or the repository.

**Branch**: `feat/votetype-pg-enum-meta`

**Pre-existing** (no work needed, verified during audit):
- `vote_type_enum` PG ENUM is defined in migration `013-enum-type-constraints.sql:46-49` with values `'Passage', 'Conference Report', 'Cloture', 'Veto Override', 'Amendment', 'Committee', 'Recommit', 'Other'`.
- `votes.vote_type` column is already `ALTER`ed to this type in migration 013 (lines 223, 248).
- `VoteType` enum has `apiValue: String` matching each DB enum value exactly (including spaced variants).
- `VoteType.fromString(s: String): Either[UnrecognizedVoteType, VoteType]` and `VoteType.fromQuestion(question: String): VoteType` already exist.

**Files to modify**:

1. `src/main/scala/repcheck/shared/models/congress/common/DoobieEnumInstances.scala` — add the `pgEnumStringOpt` meta for `VoteType` following the **`voteMethodMeta` pattern at lines 100-107**:
    ```scala
    private val voteTypeMeta = doobie.postgres.implicits.pgEnumStringOpt(
      "vote_type_enum",
      s => VoteType.fromString(s).toOption,
      _.apiValue,
    )
    implicit val voteTypeGet: Get[VoteType] = voteTypeMeta.get
    implicit val voteTypePut: Put[VoteType] = voteTypeMeta.put
    ```

2. `src/main/scala/repcheck/shared/models/congress/dos/vote/VoteDO.scala` — **CHANGE** (verified during plan review — the field IS currently `voteType: Option[String]`, line 21): change to `voteType: Option[VoteType]`. Doobie auto-derived Write will then use the `Meta[VoteType]` added above and correctly write to the `vote_type_enum` PG ENUM column. Also update `HasPlaceholder[VoteDO]` at line 41-64: `voteType = None` stays fine (Option, defaults None). No other VoteDO field change needed — the existing `question: Option[String]` field is where the raw API question string lives (for auditability) and is the source `VoteType.fromQuestion` reads. The API's procedural `voteType` field (e.g., `"Yea-and-Nay"`) is NOT persisted — per YAGNI, only the classified domain type goes to DB.

3. `src/main/scala/repcheck/shared/models/congress/dto/vote/VoteDTOs.scala` — **two tasks**:

   **(a)** Fix field-name mismatches vs. Congress.gov OpenAPI 3.0.3 spec (per `docs/reference/congress-gov-api.yaml` lines 3823–3984). Current DTOs diverge from the API:

   | File location | Current field | Actual API field | Change |
   |---|---|---|---|
   | `VoteResultDTO.memberId: Option[String]` | `houseVoteResults.bioguideID: string` (required) | Rename to `bioguideID: String` (non-optional, capital ID matches API casing for Circe semi-auto decode). |
   | `VoteResultDTO.party: Option[String]` | `houseVoteResults.voteParty: string` (required) | Rename to `voteParty: String` (non-optional). |
   | `VoteResultDTO.state: Option[String]` | `houseVoteResults.voteState: string` (required) | Rename to `voteState: String` (non-optional). |
   | `VoteListItemDTO.chamber: String` | not in API `HouseVote` schema | Drop from DTO; let the House client inject `chamber = "House"` when mapping to DO. (Senate XML DTO likewise injects `chamber = "Senate"`.) |
   | `VoteListItemDTO.voteType: Option[String]` | `HouseVote.voteType: string` ("Yea-and-Nay", "Recorded Vote", etc. — procedural, NOT our `VoteType` enum) | Rename to `procedureType: Option[String]` with scaladoc explaining this is Congress.gov's procedural label, not the domain classification. Domain `VoteType` derived from `voteQuestion`. |
   | `VoteListItemDTO.identifier: Option[String]` | `HouseVote.identifier: integer` (e.g., 1191202517) | Change to `Option[Long]`. |
   | `VoteMembersDTO.results: Option[List[VoteResultDTO]]` | `HouseVoteMembers.results: array` (required, can be empty) | Change to `results: List[VoteResultDTO]` with default `Nil`. |

   **(b)** **Verify** `VoteMembersDTO.toDO` (or equivalent DTO→DO method) calls `VoteType.fromQuestion(voteQuestion)` to populate the DO's enum field. If not present, add it. This is the single conversion site for question-string→VoteType per the user's architectural direction. The bioguideID → memberId translation (for `VotePositionDO`) also happens here — apply any bioguide → internal member PK lookup.

   Update Circe semi-auto derivations to re-derive against the corrected case-class shapes (no extra work — `deriveDecoder` picks up the new fields automatically once recompiled).

**Tests**:

| Layer | File | What it verifies |
|---|---|---|
| Unit (enum) | `test/.../DoobieEnumInstancesSpec.scala` | Round-trip all 8 `VoteType` variants through `.get`/`.put`. Assert `VoteType.Passage.apiValue == "Passage"`, `VoteType.ConferenceReport.apiValue == "Conference Report"` (spaces matter), etc. Unknown DB value (e.g., "Bogus") → `fromString` returns `None`, Doobie surfaces as decode error. |
| Unit (DTO→DO) | `test/.../VoteMembersDTOSpec.scala` | Table-driven: for each of the 8 `fromQuestion` patterns, convert a representative DTO and assert the resulting DO has the correct `VoteType` variant. Cover: "On Passage of HR 1234" → `Passage`, "On Agreeing to the Conference Report" → `ConferenceReport`, "On Cloture on the Motion to Proceed" → `Cloture`, "On Overriding the Veto" → `VetoOverride`, "On Agreeing to the Amendment" → `Amendment`, committee phrases → `Committee`, "On Motion to Recommit" → `Recommit`, unrecognized → `Other` (plus a warn log if applicable). |
| Integration (later, P2.4) | `DoobieVoteRepositorySpec` | Writes a VoteDO with each of 8 variants, reads it back, asserts round-trip identity. Proves end-to-end PG enum serialization. |

**Acceptance criteria covered**: enables §6.3 integration tests (persistence must round-trip VoteType). Also enables §6.5.6 (DTO→DO conversion including vote type classification) and §6.1.14 / §6.2.15 (implicit decoder derivation).

**Infrastructure**: Unit tests only in this PR. No Docker. The integration round-trip test runs in P2.4 against DockerPostgres.

**Release**: bump shared-models to `0.1.26` (patch or minor depending on whether VoteDO.voteType field type changes qualifies as breaking; if changing field type, treat as minor). Tag + publish via `release.yml` to GitHub Packages. Data-ingestion bumps the dep version during P1.1.

**Guardrails**: `pushToPR` before push. Branch hygiene. 95% file coverage. Custom exceptions — `UnrecognizedVoteType` already exists. No `@nowarn`. No `SELECT *` (N/A here). Reply to review comments.

**Blocks**: P0.2, P1.1, and especially P2.4 (repository integration tests cannot round-trip VoteType without this).

---

### P0.2 — `repcheck-data-ingestion`: promote `LisMappingRepository` to `members-common` + add batch read

**Goal**: unblock votes-pipeline to resolve Senate LIS IDs → bioguide IDs in a single batch query (AC §6.2.10). One writer (lis-mapping-refresher), one reader (votes-pipeline), one shared home (members-common).

**Branch**: `refactor/lis-mapping-repo-to-members-common`

**Files**:
- Move `lis-mapping-refresher/src/main/scala/repcheck/members/lismapping/repository/LisMappingRepository.scala` → `members-common/src/main/scala/repcheck/members/common/persistence/LisMappingRepository.scala`
- Same for `DoobieLisMappingRepository.scala`
- Update package declarations
- Add method to `LisMappingRepository`: `def findByLisMemberIds(lisMemberIds: List[Long]): ConnectionIO[Map[Long, Long]]` — returns mapping `lis_member_id → member_id`. SQL: `SELECT lis_member_id, member_id FROM lis_member_mapping WHERE lis_member_id = ANY(?)`. Must list columns explicitly (no `SELECT *`).
- Update `lis-mapping-refresher/build.sbt` or root `build.sbt`: `lisMappingRefresher.dependsOn(membersCommon % "compile->compile;test->test")` (verify current wiring — may already depend).
- Update all imports in `lis-mapping-refresher` to reference new location.
- Move existing unit + integration tests for LisMappingRepository to `members-common/src/test/...`.

**Tests**:
- **Unit**: existing tests continue to pass after move (no behavioral change for existing methods).
- **Class-level**: new unit test for `findByLisMemberIds` using MockitoScala — stub transactor, verify single `WHERE ... = ANY(?)` query, verify empty list short-circuits to `F.pure(Map.empty)` (no DB call).
- **Integration**: extend existing `DoobieLisMappingRepositorySpec` with `findByLisMemberIds` round-trip using `DockerPostgresSpec` — insert 5 mappings, fetch 3 by batch, verify map correctness, verify unmapped IDs are absent from result map, verify an empty input list returns an empty map.

**Acceptance criteria covered**: supports §6.2.8 (all-mapped resolution), §6.2.9 (partial-mapping resolution), §6.2.10 (single batch query — proven by integration spec observing one SQL execution).

**Infrastructure**: AlloyDB Omni via DockerPostgres. `Test / parallelExecution := false` already set on the subproject.

**Release**: none (internal to data-ingestion monorepo).

**Guardrails**: `pushToPR`. Branch hygiene. Integration tests tagged `DockerRequired`. `dockerTestParallel` must pass. 95% file coverage. Custom exceptions. No `SELECT *`.

**Blocks**: P1.1 (votes scaffold needs import path stable), P2.3 (LisResolver uses batch method).

---

### P0.3 — Bump `sbt-exception-uniqueness` 0.4.0 → 0.5.0 in ALL repos

**Goal**: adopt the v0.5.0 `ProjectExceptionsOnlyCheck` across every RepCheck repo so production code is build-time-enforced to use only project-owned custom exceptions. This is a project-wide hygiene upgrade, not specific to votes-pipeline — but votes-pipeline implementation depends on it being active in data-ingestion, and publishing artifacts from other repos cleanly requires their own checks to be uniform.

**Why this matters**: v0.5.0 fails `sbt test` if production (non-test) code constructs non-project exceptions at `throw new X(...)`, `throw X(...)`, `*.raiseError(new X(...))`, or `*.raiseError(X(...))` sites. Re-raises (`throw e`, `raiseError(err)` where `err` is a variable) remain allowed. Test code is exempt — tests still use `IllegalArgumentException` etc. for mock failure simulation.

**Scope — 5 parallel per-repo PRs** (one agent per repo, fully independent):

| Sub-PR | Repo | Current plugin version | Plugin line location | Branch |
|---|---|---|---|---|
| P0.3a | `repcheck-shared-models` | 0.4.0 | `project/plugins.sbt:16` | `chore/bump-exception-uniqueness-plugin-0.5.0` |
| P0.3b | `repcheck-pipeline-models` | 0.4.0 | `project/plugins.sbt:16` | `chore/bump-exception-uniqueness-plugin-0.5.0` |
| P0.3c | `repcheck-ingestion-common` | 0.4.0 | `project/plugins.sbt:16` | `chore/bump-exception-uniqueness-plugin-0.5.0` |
| P0.3d | `repcheck-db-migrations` | 0.4.0 | `project/plugins.sbt:17` | `chore/bump-exception-uniqueness-plugin-0.5.0` |
| P0.3e | `repcheck-data-ingestion` | 0.4.0 | `project/plugins.sbt:17` | `chore/bump-exception-uniqueness-plugin-0.5.0` |

**Per-sub-PR procedure** (identical for each repo):

**Files changed**:
- `project/plugins.sbt`: change version string to `"0.5.0"`.
- `build.sbt`: verify `exceptionUniquenessRootPackages := Seq("com.repcheck", "repcheck")` covers production code — this is already the case in all 5 repos (audited). No change expected.
- Fix any production-code violations surfaced by the first `sbt test` run after the bump.

**Tests**:
- **Build-gate**: `sbt test` after the bump must pass. The plugin's `ProjectExceptionsOnlyCheck` runs as part of `(Test / test)`, so existing CI catches it.
- **Unit** (if new exception classes added): each new exception class gets a simple spec asserting `.getMessage` includes key context fields (per existing pattern in error-class specs throughout the repos).

**Procedure per agent**:
1. Bump plugin version in `project/plugins.sbt`.
2. Run `sbt clean compile test` locally — note every violation.
3. For each violation, decide: (a) reuse an existing custom exception if semantically correct, (b) declare a new unique one (must have a unique simple name across the whole repo's production code, enforced by the plugin's declaration-uniqueness check) in the appropriate `*.errors.*` package.
4. Commit the bump + fixes as a single PR so the atomic state transitions cleanly.
5. `pushToPR` — verify CI green.
6. **If the repo publishes to GitHub Packages** (applies to P0.3a, P0.3b, P0.3c, P0.3d — all except data-ingestion): merge triggers `release.yml`; label the PR appropriately (`release:patch` since this is a non-source-level plugin upgrade with no API change). The new version gets published.
7. **P0.3e (data-ingestion)**: does not publish; merge completes the chain.

**Likely violation hotspots to audit**:
- In `shared-models`/`pipeline-models`: any production `throw new IllegalArgumentException(...)` in DTO/DO validation. Often there's a custom exception already (like `UnrecognizedVoteType`); use it.
- In `ingestion-common`: `raiseError` calls in retry logic, HTTP client error paths, XML parsing.
- In `db-migrations`: Liquibase migration runner's Scala wrapper (smaller surface area).
- In `data-ingestion`: bill/member pipeline processor error paths, placeholder creation, event publishing.

**Acceptance criteria covered**: enables enforcement for every subsequent PR. No votes-pipeline ACs directly, but this ensures the standards described in `feedback_custom_exceptions_only` (memory) are automatic rather than manual.

**Infrastructure**: sbt only per repo. No Docker.

**Release coordination**:
- P0.3a–d publish new versions to GitHub Packages (patch bump).
- data-ingestion (P0.3e) does NOT need to bump its consumed artifact versions (e.g., `repchecksharedmodels` stays at 0.1.26 from P0.1) — the plugin bump is purely a build-tooling change.
- If any of P0.3a–d surface a violation that requires an API change to a published DTO/DO/event (unlikely; these libraries already use custom exceptions heavily), flag to the user before merging — that becomes a coordinated minor-bump scenario.

**Guardrails**: `pushToPR`. Branch hygiene per repo. Every newly created exception class must have a unique simple name (the plugin's declaration-uniqueness check enforces this). Reply to PR review comments.

**Parallelism rules**:
- 5 agents can own one sub-PR each. They work in 5 different repos; no conflict.
- Each agent must serialize its own `sbt test` runs internally; across agents, since the repos are disjoint, no cross-agent coordination needed.
- Merges can happen independently in any order.

**Blocks**: P1.1 (data-ingestion scaffold needs 0.5.0 active across the toolchain).

---

### P0.4 — **DELETED** (was: `GlobalRequestThrottle` primitive)

**Decision (2026-04-21, per user correction)**: the original P0.4 (process-wide semaphore syncing multiple HTTP clients through a single `GlobalRequestThrottle[F]`) was the wrong abstraction. Per-client `parEvalMap(parallelism)` combined with the existing per-client `rateLimitedClient` wrapper pattern already gives each HTTP client its own configurable pacing; the sum of those is naturally bounded without a shared gate.

**Canonical pattern each pipeline already uses** (bill-metadata-pipeline, bill-text-availability-checker, bill-text-pipeline — each has its own private copy):

```scala
private def rateLimitedClient[F[_]: Async](
  underlying: Resource[F, Client[F]],
  pageDelay: FiniteDuration,
): Resource[F, Client[F]] =
  underlying.flatMap { raw =>
    Resource.eval(Semaphore[F](1)).map { sem =>
      Client[F] { request =>
        Resource.make(sem.acquire)(_ => Temporal[F].sleep(pageDelay) >> sem.release) >>
          raw.run(request)
      }
    }
  }
```

Each HTTP client in each pipeline gets its own wrapped `Client[F]` with a configurable `pageDelay` from PureConfig. No shared state across clients, no breaking change to ingestion-common.

**Votes-pipeline adoption** (handled during P1.1 / P2.1 / P2.2): copy this helper into `votes-pipeline/src/main/scala/repcheck/ingestion/votes/app/VotesPipeline.scala` as a `private def`. Wire it into `buildResources` so both the House JSON client and the Senate XML client get their own wrapped `Client[F]` with configurable `pageDelay` / `requestDelay` from `HouseVotesConfig` / `SenateVoteXmlConfig`.

No work in ingestion-common. No retrofit of existing pipelines. No new primitive. No `HttpClientResource.make` signature change.

---

### P0.6 — `repcheck-ingestion-common`: promote `PubSubEmulatorFixture` (CONDITIONAL — only if missing)

**Trigger**: P1.1 scaffold audit discovers neither `bills-common` nor `members-common` contains a usable `PubSubEmulatorFixture`. Open P0.6 ONLY in that case. If either common project already has a fixture that works for votes-pipeline, skip P0.6 entirely and import the existing one.

**Goal**: establish a reusable Pub/Sub emulator fixture in ingestion-common so votes-pipeline (and any future pipeline) can spin up a local Pub/Sub emulator for integration + functional tests.

**Branch**: `feat/pubsub-emulator-fixture`

**Files** (mirrors `DockerPostgres.scala` pattern):
- `src/test/scala/repcheck/ingestion/common/testing/PubSubEmulator.scala` — `Resource[F, PubSubEmulatorInfo]` with project ID, host/port, and a cleanup step.
- `src/test/scala/repcheck/ingestion/common/testing/SharedPubSubEmulator.scala` — JVM-wide singleton similar to `SharedDockerPostgres`.
- `src/test/scala/repcheck/ingestion/common/testing/PubSubEmulatorSpec.scala` — trait mixin for ScalaTest suites.
- Docker image: `gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators` (matches `docker-compose.local.yml`'s version).

**Tests**:
- **Unit** (REQUIRED): verify Resource acquires and releases the container cleanly; no leaked containers across test runs.
- **Integration**: publish a message via the emulator, consume it, assert payload round-trip.

**Release**: bump ingestion-common to next patch version (API-additive — new test-scope helper).

**Guardrails**: `pushToPR`. Branch hygiene. Don't break the existing `DockerPostgres` pattern — parallel semantics.

**Blocks**: P3.2 integration test, P3.3 E2E test (both need the emulator fixture).

---

### P0.5 — `votr`: update acceptance-criteria docs to reflect plan revisions

**Goal**: bring the acceptance-criteria markdown in `votr/docs/architecture/acceptance-criteria/06-votes-pipeline/` into alignment with this plan's revisions so agents reading the spec get consistent guidance. Per user direction: spec and plan must not conflict while votes-pipeline work is underway.

**Branch (in `votr` repo)**: `docs/votes-pipeline-spec-revisions`

**Files to modify** (in `C:\Users\elita\source\repos2024\votr\docs\architecture\acceptance-criteria\`):

1. `06-VOTES-PIPELINE.md` (index): update the short description of the pipeline to include "unified placeholder member strategy for unknown LIS IDs" instead of the skip-and-retry language.
2. `06-votes-pipeline/06.1-house-votes-api-client.md`:
   - Replace §6.1.6 ("fromDateTime filter applied to list requests") with the reframed client-side lookback behavior (fetch all pages → sort DESC by updateDate → filter by cutoff).
   - Correct the example URL to remove `fromDateTime` / `toDateTime` / `sort`.
   - Update DTO field name notes: `bioguideID` (capital ID), `voteParty`, `voteState` — match actual Congress.gov OpenAPI.
3. `06-votes-pipeline/06.2-senate-vote-xml-client.md`:
   - Replace §6.2.8–12 (skip-and-log for unresolved LIS) with placeholder-creation behavior. Cite synthetic `lis:$id` natural_key pattern.
   - Add a note: the `lis-mapping-refresher` merge step promotes placeholders in-place when real bioguides arrive.
4. `06-votes-pipeline/06.5-vote-processing-pipeline.md`:
   - Replace AC#24 and AC#25 (partial LIS persisted / unresolved logged) with revised placeholder-creation-equivalent ACs.
   - Update the event-emission decision matrix description to reference `naturalKey: Option[String]` (current `VoteRecordedEvent` shape, per PR#15), not `billId: Option[String]`.
5. Area files that don't diverge (§6.3, §6.4) stay untouched beyond nitpick edits.

6. **Regenerate compressed docs** per memory `feedback_doc_compression`: run `scripts/generate-agent-docs.sh` in the votr repo to refresh `.claude/agent-docs/architecture/acceptance-criteria/06-votes-pipeline/*.compressed.md`. DO NOT hand-edit the compressed files.

**Tests**: docs-only PR; no code tests. CI still runs markdown lint / link-check if configured.

**Guardrails**: `pushToPR` in votr (uses same CI script pattern if present). Never edit compressed docs manually. No scope creep — this PR modifies ONLY the §6 area files, not other components.

**Blocks**: Phase 2 opens. Phase 0 PRs (P0.1–P0.3, P0.7) can proceed in parallel with P0.5 since they don't depend on doc state.

---

## Phase 1 — Scaffold

### P1.1 — `repcheck-data-ingestion`: create `votes-pipeline` subproject directly

**Goal**: create an empty-but-wired votes-pipeline subproject that compiles and has a passing trivial test. All subsequent Phase 2 PRs add real classes on top. The subproject must be covered by the v0.5.0 exception uniqueness + project-exceptions-only checks from day 1 (no retrofit later).

**Branch**: `feat/votes-pipeline-scaffold`

**Why direct creation (not g8)**: the `repcheck-g8` template generates entirely new repositories, not new subprojects inside an existing monorepo. For a new subproject within `repcheck-data-ingestion`, the correct pattern is to model after the nearest sibling — **`bill-metadata-pipeline`** is the best template since it's a paginated Congress.gov pipeline with Pub/Sub publishing, which matches votes-pipeline's shape most closely. For the Senate XML side, `lis-mapping-refresher` is a useful secondary reference (XML + Doobie). (Note: the `feedback_g8_scaffolding` memory is about new-repo creation; it does not apply here.)

**Exception-uniqueness wiring**: the root `build.sbt` already sets `exceptionUniquenessRootPackages := Seq("com.repcheck", "repcheck")` which covers `repcheck.ingestion.votes.*`. The subproject only needs `.enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)` on its `lazy val` definition. Verify the check runs against votes-pipeline via `sbt votesPipeline/test` after scaffold.

**Steps**:

1. **Create directory tree** under `votes-pipeline/` matching the bill-metadata-pipeline layout:
   ```
   votes-pipeline/
   ├── src/main/scala/repcheck/ingestion/votes/
   │   ├── app/             (VotesPipelineApp, VotesPipeline companion)
   │   ├── api/             (HouseVotesApiClient — added in P2.1)
   │   ├── xml/             (SenateVoteXmlClient, SenateVoteXmlDecoder — P2.2)
   │   ├── lis/             (LisResolver — P2.3)
   │   ├── repo/            (VoteRepository, VotePositionRepository, VoteHistoryArchiver — P2.4)
   │   ├── pipeline/        (VoteProcessor, VoteChangeDetector — P2.5, P3.1)
   │   ├── config/          (VotePipelineConfig, HouseVotesConfig, SenateVoteXmlConfig)
   │   └── errors/          (HouseVoteFetchFailed, SenateVoteFetchFailed, etc.)
   ├── src/main/resources/
   │   ├── application.conf
   │   └── application-test.conf
   ├── src/test/scala/repcheck/ingestion/votes/
   │   └── app/VotesPipelineScaffoldSpec.scala   (smoke test)
   └── Dockerfile
   ```

2. **Add `lazy val` to root `build.sbt`** (append near the existing pipeline subproject definitions, ~line 151 where `billMetadataPipeline` is declared):
   ```scala
   lazy val votesPipeline = (project in file("votes-pipeline"))
     .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
     .dependsOn(membersCommon % "compile->compile;test->test")
     .settings(pipelineSettings)
     .settings(
       name := "votes-pipeline",
       libraryDependencies ++= http4sEmber ++ circe ++ pureConfig
         ++ catsEffect ++ doobie ++ pubSub ++ fs2 ++ logging ++ testDeps,
       libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "2.4.0",
       libraryDependencies += "org.http4s" %% "http4s-scala-xml" % "0.23.14",
       Test / parallelExecution := false,
       coverageExcludedFiles := ".*VotesPipelineApp",
       assembly / mainClass := Some("repcheck.ingestion.votes.app.VotesPipelineApp"),
       assembly / assemblyJarName := "votes-pipeline.jar",
     )
   ```

3. **Add `votesPipeline` to the root aggregate list** (the existing `.aggregate(...)` call at ~line 110).

4. **Add `scala-xml` + `http4s-scala-xml` to `project/Dependencies.scala`** if a centralized `val scalaXml = Seq(...)` pattern is in use — otherwise inline as shown in step 2.

5. **Bump `repchecksharedmodels` dep to `0.1.26`** (from P0.1). Confirm db-migrations-runner version matches whatever P0.3d published.

6. **Create skeleton source files** (copy the pattern from `bill-metadata-pipeline/src/main/scala/repcheck/ingestion/bills/metadata/app/*.scala`):

   - `app/VotesPipelineApp.scala` — thin `IOApp` delegating to `VotesPipeline.runWithFactories[IO](...)`. No logic. Excluded from coverage.
   - `app/VotesPipeline.scala` — companion object with `private[app]` methods: `runWithFactories[F]`, `buildResources[F]`, `buildProcessor[F]`, `buildStream[F]`. Each accepts factory functions per CLAUDE.md testability refactor. Body stays stubbed (`???` or minimal pass-through) in this PR; P3.2 fills it out. **`buildResources` MUST define a `private def rateLimitedClient[F[_]: Async](underlying: Resource[F, Client[F]], pageDelay: FiniteDuration): Resource[F, Client[F]]` helper (canonical pattern copied from sibling pipelines — `Semaphore(1)` + `pageDelay` between releases)** and wrap the raw EmberClient once per HTTP client (House, Senate) before handing to the respective API client constructor. No shared throttle.
   - `config/VotePipelineConfig.scala` — top-level case class, PureConfig-derived. Start minimal — just `database: DatabaseConfig`, `congressApi: CongressGovClientConfig`, `eventPublisher: EventPublisherConfig`. P3.2 adds the votes-specific substructures (`HouseVotesConfig` + `SenateVoteXmlConfig`, each with their own `pageDelay` / `requestDelay` + `parallelism` fields).
   - `application.conf` + `application-test.conf` — stubs pointing at Congress.gov + AlloyDB with env-var overrides. Each HTTP client's pacing is configured via its own `pageDelay` / `requestDelay` (e.g., `pipeline.house.page-delay = 2s`, `pipeline.senate.request-delay = 3s`). No cross-client throttle.
   - `Dockerfile` — multi-stage, copied from `bill-metadata-pipeline/Dockerfile`. Adjust JAR name to `votes-pipeline.jar` and main class to `repcheck.ingestion.votes.app.VotesPipelineApp`. Distroless Java 21 base (`gcr.io/distroless/java21-debian12`).

7. **Add skeleton test**: `src/test/scala/.../app/VotesPipelineScaffoldSpec.scala` — one test asserting the package compiles and `VotesPipelineApp.getClass.getName` resolves to the expected FQN. Purely a compile-gate; no behavior tested yet.

8. **Verify `ProcessingResult` / `PipelineRunSummary` location** (resolves an open audit question): grep bills-common + members-common. If present, record the import path here in this plan file. If not, keep them as votes-local case classes under `pipeline/` and flag an out-of-scope cleanup to potentially promote to ingestion-common later.

9. **Verify `PubSubEmulatorFixture` location** (check bills-common AND members-common first). If neither has it, OPEN **P0.6** (conditional prereq — see Phase 0) to add the fixture to ingestion-common. Do NOT create a votes-local one. If a fixture exists in one of the common projects, import it directly; document the import path in this plan file.

10. **Confirm `E2ETest` tag import path**: should be `com.repcheck.tags.E2ETest` (agent 3 located it in bills-common). Document the exact `import` line in this plan.

11. **Run `pushToPR`** — should pass with only the smoke test.

**Tests**:
- **Unit** only at this phase: smoke spec in `VotesPipelineScaffoldSpec.scala` verifying compile + class resolution.

**Acceptance criteria covered**: none directly.

**Infrastructure**: none beyond sbt.

**Release**: none.

**Guardrails**: no g8 step (g8 is for repo creation, not subproject creation). `pushToPR`. Branch hygiene. `coverageExcludedFiles` set for App only. Subproject MUST enable `ExceptionUniquenessPlugin` — missing this would silently skip enforcement. Verify post-scaffold via `sbt "show votesPipeline/checkExceptionUniqueness"`.

**Blocks**: all of Phase 2, Phase 3.

---

## Phase 2 — Implementation (parallelizable after P1.1 merges)

All five PRs open on separate branches. Different files, no conflicts. Agents working in parallel **must serialize `sbt test` runs** (per memory `feedback_serialize_ci_checks`) — agree on a lockfile or sequence.

### P2.1 — `HouseVotesApiClient` (Area 6.1)

**Branch**: `feat/votes-house-api-client`

**Authoritative API reference**: `C:\Users\elita\source\repos2024\votr\docs\reference\congress-gov-api.yaml` lines 1016–1134 (house-vote paths) and 3823–3984 (schemas). The votes-pipeline acceptance criteria spec (06.1) diverges from the actual API on three points, resolved below per user direction:

#### API specifics verified from OpenAPI 3.0.3 spec

**Endpoints** (all BETA):
| Path | Purpose | Response schema |
|---|---|---|
| `GET /house-vote` | List all votes | `HouseVote` (with outer pagination wrapper) |
| `GET /house-vote/{congress}` | List filtered by congress | same |
| `GET /house-vote/{congress}/{session}` | List filtered by congress+session | same |
| `GET /house-vote/{congress}/{session}/{voteNumber}` | Single vote detail | `HouseVoteNumber` |
| `GET /house-vote/{congress}/{session}/{voteNumber}/members` | Vote + member positions | `HouseVoteMembers` |

**Query parameters supported** by house-vote endpoints (ONLY these three):
- `format` — `json` or `xml` (default json)
- `offset` — integer
- `limit` — integer

**NOT supported on house-vote**: `fromDateTime`, `toDateTime`, `sort`. (These exist on other endpoints like `/bill` but not `/house-vote` per spec lines 1022–1025, 1044–1048, 1067–1072, 1091–1097, 1116–1122.) The votes-pipeline acceptance-criteria spec's §6.1.6 ("fromDateTime filter applied to list requests") is **factually wrong for this beta API**.

**Authentication**: `api_key` query parameter (spec line 6194–6198 defines `securitySchemes.ApiKeyAuth` as `type: apiKey, in: query, name: api_key`). Global `security: [ApiKeyAuth: []]` at line 10–11 applies to all paths.

**URL construction** (correct form):
- List: `{baseUrl}/house-vote/{congress}/{session}?format=json&offset={offset}&limit={pageSize}&api_key={apiKey}`
- Members: `{baseUrl}/house-vote/{congress}/{session}/{voteNumber}/members?format=json&api_key={apiKey}`

**Lookback window — implement client-side** (NOT via `fromDateTime`):
- Paginate from offset=0 through all pages for the given congress/session (the API doesn't declare ordering, so do NOT short-circuit via `takeWhile`).
- Accumulate all `VoteListItemDTO`s into an in-memory buffer.
- **Sort DESC by `updateDate`** on the buffer (newest-first accumulator pattern, per user direction).
- Apply the lookback filter: keep items where `updateDate >= Instant.now().minus(config.lookbackDays, ChronoUnit.DAYS)`.
- Emit the filtered list downstream.

Memory note: a full session's vote list is ≤ ~600 votes (House typically 500–600 roll calls per year). Holding all in memory is trivial (< 1MB). No streaming needed for the list phase — streaming kicks in at the per-vote `fetchMembers` fan-out.

Test impact (AC#6 reframed): stub 3 pages (all within lookback + random shuffle); verify client fetches ALL pages, sorts DESC by updateDate, returns filtered subset. Do NOT assert early pagination termination — that assumption is rejected.

#### Response schema mappings — verify existing DTOs in shared-models

The audit found `VoteListItemDTO`, `VoteDetailDTO`, `VoteMembersDTO`, `VoteResultDTO` in `repcheck-shared-models/.../dto/vote/VoteDTOs.scala`. **Three field-name mismatches must be resolved before this PR opens** — either fix the DTOs (prereq to bump shared-models again) or implement a custom Circe `Decoder` that renames fields during decoding. User direction is "Make sure types are treated carefully and matched" — so the DTO should match the API exactly.

| DTO field (current) | API field (OpenAPI) | Action |
|---|---|---|
| `VoteResultDTO.memberId: Option[String]` | `houseVoteResults.bioguideID: string` (required, uppercase ID) | **Rename** DTO field to `bioguideID` (match API casing exactly — Circe semi-auto derivation is case-sensitive). Make non-optional per spec. |
| `VoteResultDTO.party: Option[String]` | `houseVoteResults.voteParty: string` | **Rename** to `voteParty`. Non-optional. |
| `VoteResultDTO.state: Option[String]` | `houseVoteResults.voteState: string` | **Rename** to `voteState`. Non-optional. |
| `VoteListItemDTO.chamber: String` (current, required) | not in API `HouseVote` schema | The API doesn't return `chamber`. Options: (a) drop field and derive `chamber = "House"` when constructing DOs in the House client, or (b) add a custom decoder default. Recommend (a) — chamber is a client-level concept here. |
| `VoteListItemDTO.voteType: Option[String]` | `HouseVote.voteType: string` e.g. `"Yea-and-Nay"` | **Keep as string** — the API's `voteType` is procedural ("Yea-and-Nay", "Recorded Vote", "Quorum Call"), NOT the domain `VoteType` enum (Passage/Amendment/Cloture/etc.). Consider **renaming to `procedureType: Option[String]`** to disambiguate, or document the semantic distinction in a scaladoc on the DTO. Do NOT auto-parse this into `VoteType`. Domain `VoteType` is derived from `voteQuestion` via `VoteType.fromQuestion` (per P0.1). |
| `VoteMembersDTO.results: Option[List[VoteResultDTO]]` | `HouseVoteMembers.results: array` (required per allOf composition; array can be empty) | Make non-optional (empty list rather than None). |
| `VoteListItemDTO.identifier: Option[String]` | `HouseVote.identifier: integer` (e.g., 1191202517) | Change to `Option[Long]` — it's an integer ID. |

**Decision for P2.1**: resolve these DTO mismatches as a **mini-bump to shared-models** folded into P0.1 before it merges. Already handled — P0.1 shipped with the vote DTO corrections.

**Files to create in P2.1** (votes-pipeline only):
- `api/HouseVotesApiClient.scala` — extends `CongressGovPaginatedClient[F, VoteListItemDTO]`, adds `fetchMembers(congress: Int, session: Int, voteNumber: Int): F[VoteMembersDTO]`. Must build URLs per the correct patterns above. Must NOT include `fromDateTime` / `toDateTime` / `sort` query params — those produce 400 errors on the beta endpoint. **Constructor receives a pre-wrapped `Client[F]` (already wrapped by the P1.1 `rateLimitedClient` helper with the House-specific `pageDelay`)** — this client does NOT construct its own http4s client and does NOT own its own rate limiter; the wrapping is an app-level concern wired in P1.1's `buildResources`.
- `config/HouseVotesConfig.scala` — PureConfig-derived: `congress`, `session`, `parallelism=1`, `pageDelay=2.seconds`, `lookbackDays=7`. `parallelism` controls the fs2 `parEvalMap` over vote numbers (how many detail-endpoint calls happen in parallel **from this one client**); the per-request pacing is enforced by the House client's `rateLimitedClient` wrapper (`Semaphore(1)` + `pageDelay`). `lookbackDays` drives client-side filtering, not a query param.
- `errors/HouseVoteFetchFailed.scala` — unique project exception (subclass of `Throwable`, unique simple name across the whole repo per plugin rule) with congress/session/voteNumber/detail/cause. NO standard exceptions like `RuntimeException` or `IOException` may be `throw`n or `raiseError`-ed from production paths in this PR (plugin v0.5.0 enforces).

**Test layers**:
| Layer | File | Infrastructure | AC rows (reframed to match API) |
|---|---|---|---|
| Unit | `api/HouseVotesApiClientSpec.scala` | MockitoScala | 5 (api_key query param present), 13 (error ctx includes all three identifiers) |
| Component (WireMock) | same file | WireMock (127.0.0.1 + dynamicPort per memory), stub Congress.gov JSON with recorded real-shape responses (use `voteQuestion: "On Passage"`, `bioguideID: "A000055"`, etc.) | 1 (paginated list decoded), 2 (pagination stops when `items.size < pageSize`), 3 (members endpoint decoded with correct field names), 4 (`voteQuestion` present in members response), 6 **reframed**: lookback window applied client-side — stub 2 pages where page 1 has all `updateDate` within window and page 2 has all outside; assert only page 1 items collected and pagination short-circuits, 7/8 (transient classification/retry), 9 (systemic no-retry), 10 (404 on specific vote → `HouseVoteFetchFailed`), 11 (pageDelay respected), 12 (timeout), 14 (decoders work on recorded fixtures) |

Total AC rows covered: 14/14 of §6.1 (AC#6 reinterpreted per actual API behavior; note discrepancy in PR description so spec can be updated in a follow-up).

Test must verify FUNCTIONAL correctness:
- **AC#1** (pagination): 3-page mocked response (250+250+50); assert exactly 550 items collected AND client stops paginating once `items.size < pageSize`.
- **AC#4** (voteQuestion): fetch members for a WireMock-stubbed vote whose JSON body has `"voteQuestion": "On Motion to Suspend the Rules and Pass"`; assert DTO decodes correctly, question string preserved verbatim (don't lossy-normalize — `VoteType.fromQuestion` handles the classification downstream).
- **AC#5** (api_key): use WireMock `verify()` — assert every outgoing request has `api_key={configured}` as a query parameter. Failure mode: if a future implementer puts the key in a header, this test catches it.
- **AC#6 reframed** (lookback): stub page 1 with 5 items all within lookback, page 2 with 5 items all outside lookback. `fetchAll` with `lookbackDays=7` collects exactly 5 items AND makes exactly 1 API call (no request for page 2). Use WireMock `verify(exactly(1), getRequestedFor(...))`.
- **AC#11** (pageDelay): stub 100ms inter-page delay, 3 pages, elapsed ≥ 200ms using a real clock (not time-mocked).

**Blocks**: P3.1 (processor needs this client).

---

### P2.2 — `SenateVoteXmlClient` + `SenateVoteXmlDecoder` (Area 6.2 client half)

**Branch**: `feat/votes-senate-xml-client`

**Files**:
- `xml/SenateVoteXmlDecoder.scala` — pure function `decode(elem: Elem): Either[XmlParseFailed, SenateVoteXmlDTO]` and index decoder. Uses scala-xml.
- `xml/SenateVoteXmlClient.scala` — wraps `XmlFeedClient[F]`, adds `fetchVote(congress, session, voteNumber): F[SenateVoteXmlDTO]` (URL constructs 5-digit zero-padded vote number) and `fetchVoteIndex(congress, session): F[List[SenateVoteIndexEntry]]`.
- `config/SenateVoteXmlConfig.scala` — `parallelism=2`, `requestDelay=1.second`.
- `errors/SenateVoteFetchFailed.scala`.

**Test layers**:
| Layer | File | Infrastructure | AC rows |
|---|---|---|---|
| Unit | `xml/SenateVoteXmlDecoderSpec.scala` | scala-xml fixtures in `src/test/resources/senate-xml/` — one well-formed, one malformed, one index | 3, 5 |
| Component (WireMock) | `xml/SenateVoteXmlClientSpec.scala` | WireMock serving XML Content-Type responses | 1, 2, 4, 6, 7, 13 |

Total AC rows covered from §6.2: 1-7, 13 (8 of 15). Remaining 8 rows (8, 9, 10, 11, 12, 14) belong to P2.3.

AC#2 (5-digit zero-padding) — test a vote number 7, assert URL suffix `vote_{congress}_{session}_00007.xml`. FUNCTIONAL verification, not just "url contains 7".

**Blocks**: P3.1.

---

### P2.3 — `LisResolver` + placeholder creation for unknown LIS + lis-mapping-refresher merge step

**Branch**: `feat/votes-lis-resolver`

**Scope expanded** per "Unknown-member handling" in Target Architecture: resolver now CREATES PLACEHOLDER MEMBERS for unknown LIS IDs instead of skipping. Additionally, `lis-mapping-refresher` is updated to merge placeholders when it later discovers real bioguides. One PR — end-to-end self-healing lands atomically.

**Files to CREATE** (votes-pipeline):
- `lis/LisResolver.scala` — constructor takes `LisMappingRepository` (from P0.2), `MemberRepository` (from members-common), `PlaceholderCreator[F]` (ingestion-common), `Transactor[F]`, `PipelineLogger[F]`.
  - `resolve(senateVote: SenateVoteXmlDTO): F[LisResolutionResult]` — flow:
    1. Collect distinct `lisMemberId`s from `senateVote.members`.
    2. `lisMappingRepo.findByLisMemberIds(ids)` → `Map[Long, Long]` (lis_member_id → members.id).
    3. For each senator position:
       - If mapping present → produce `VoteResultDTO` with resolved `members.id`.
       - If mapping absent → **create placeholder member** with synthetic natural key `s"lis:$lisMemberId"` via `PlaceholderCreator.ensureExists`, then insert `lis_member_mapping(lis_member_id, members.id)`, then produce `VoteResultDTO` with the placeholder's `members.id`. ALL inside the same `ConnectionIO` that the processor later commits.
    4. Return `LisResolutionResult(resolvedMembers: List[VoteResultDTO], placeholdersCreated: List[Long])` — note: no more `unresolvedLisIds` field; every senator resolves to some members.id now.
  - Log at info level: "Created N LIS placeholders for vote X: [lisIds...]" — operator visibility.
- `lis/LisResolutionResult.scala` — case class with `resolvedMembers` + `placeholdersCreated`.
- `errors/LisPlaceholderCreationFailed.scala` — unique project exception for the rare case where placeholder insertion fails (e.g., synthetic key collision with an existing row).

**Files to CREATE** (members-common, since `HasPlaceholder[MemberDO]` placeholder-row factory may need an LIS variant):
- Update `HasPlaceholder[MemberDO]` in `repcheck-shared-models` (if it doesn't already accept a synthetic key) to support `placeholderFor(naturalKey: String, lisMemberId: Option[Long])` pattern — or overload `PlaceholderCreator.ensureExistsForLis(lisMemberId: Long)` as a specialized entry point. Decide during implementation; either way, keep the typeclass cohesive with the existing House-bioguide placeholder.

**Files to MODIFY** (lis-mapping-refresher) — placeholder-merge logic:
- `lis-mapping-refresher/src/main/scala/.../pipeline/LisMappingProcessor.scala` (or wherever the refresher upserts members):
  - Before the normal upsert-by-bioguide flow, check if a placeholder row already exists for this `lisMemberId` via `lis_member_mapping` join.
  - If found AND its `natural_key` matches the synthetic `lis:$id` pattern:
    - Attempt to promote the placeholder to the real bioguide: UPDATE `natural_key = <real bioguide>`, populate `first_name`, `last_name`, `current_party`, `state`, etc. from the senator lookup XML data.
    - If a row with the real bioguide already exists (rare): migrate `vote_positions.member_id` from placeholder → real, UPDATE `lis_member_mapping.member_id` to real, DELETE the placeholder.
  - If found AND `natural_key` is already a real bioguide: no-op (already merged).
  - If not found: normal upsert flow (no placeholder to merge).
- `lis-mapping-refresher/src/test/scala/.../pipeline/LisMappingProcessorSpec.scala`: add cases for the three merge scenarios above.

**Test layers**:
| Layer | File | Infrastructure | AC rows (revised) |
|---|---|---|---|
| Unit | `lis/LisResolverSpec.scala` | MockitoScala — stub `LisMappingRepository`, `PlaceholderCreator`, `MemberRepository` | §6.2.8 revised (all-mapped still resolves via mapping), §6.2.9 revised (partial mapping → placeholder creation for unknown), §6.2.11 revised (placeholders logged at info with counts) |
| Unit | `lis/LisPlaceholderNamingSpec.scala` | pure function | Verify `s"lis:$id"` formation, verify it never collides with real bioguide shape (pattern `/^[A-Z]\d{6}$/`) |
| Integration | `lis/LisResolverIntegrationSpec.scala` | DockerPostgres — real `DoobieLisMappingRepository` + `DoobieMemberRepository` | §6.2.10 (single batch query verified — `findByLisMemberIds` issues exactly one SQL call regardless of senator count), §6.5.24 revised (Senate vote with N unknown LIS → N placeholder members created, full position list persisted), §6.5.25 revised (placeholder creations logged with count + IDs) |
| Integration (cross-pipeline) | `lis-mapping-refresher/.../integration/PlaceholderMergeIntegrationSpec.scala` | DockerPostgres — simulate: votes-pipeline creates placeholder, then refresher runs with real senator lookup data | Three merge scenarios: (a) promote placeholder to real bioguide (in-place UPDATE, id preserved), (b) merge into existing real bioguide row (vote_positions FK migration + placeholder deletion), (c) no-op when no placeholder exists |

AC#10 verification: enable Doobie SQL logging via `logConfig` OR use a pg stats query — assert exactly one `SELECT ... FROM lis_member_mapping WHERE lis_member_id = ANY(?)` executed per `resolve` call regardless of senator count. FUNCTIONAL verification, not coverage-gaming.

Total AC rows from §6.2: 8 revised, 9 revised, 10, 11 revised (12, 14, 15 folded into 9 and 10 as placeholder paths absorb the "unresolved" cases). Combined with P2.2's 8 rows = 15/15 of §6.2.

**Depends on**: P0.2 (LisMappingRepository in members-common with batch method). P2.3 introduces placeholder-merge behavior that ALSO changes lis-mapping-refresher — coordinate the refresher test updates in the same PR so refresher CI stays green.

**Blocks**: P3.1 (processor wires the resolver).

---

### P2.4 — Vote repositories + history archiver (Area 6.3)

**Branch**: `feat/votes-repositories`

**Files**:
- `repo/VoteRepository.scala` (trait) + `DoobieVoteRepository.scala` — `findByVoteId`, `upsert` (ON CONFLICT), `findByBillId`, `findByCongress`.
- `repo/VotePositionRepository.scala` + `DoobieVotePositionRepository.scala` — `findByVoteId`, `replaceAll` (delete-then-batch-insert in one `ConnectionIO`), `findByMemberAndBill`.
- `repo/VoteHistoryArchiver.scala` + `DoobieVoteHistoryArchiver.scala` — `archiveVote(voteId): ConnectionIO[UUID]` — generates UUID, copies votes+positions rows into history tables with shared `history_id`.
- `repo/StanceMaterializationStatusRepository.scala` + impl — `markHasVotes(billId: String): ConnectionIO[Unit]` — `INSERT ... ON CONFLICT (bill_id) DO UPDATE SET has_votes=true, votes_updated_at=NOW()`.
- `errors/VoteUpsertFailed.scala`, `VoteArchiveFailed.scala`.

**CRITICAL**: every Doobie query lists columns explicitly (no `SELECT *` per memory). Column order must match case class constructor order.

**Test layers**:
| Layer | File | Infrastructure | AC rows |
|---|---|---|---|
| **Unit (REQUIRED)** | `repo/DoobieVoteRepositoryUnitSpec.scala` | MockitoScala + Doobie fragment rendering | SQL shape tests: columns listed explicitly (no SELECT *), ON CONFLICT clause correct, column count matches VoteDO arity, UPDATE SET list complete, updated_at refreshed |
| Unit | `repo/DoobieVotePositionRepositoryUnitSpec.scala` | MockitoScala + Doobie fragment | delete-then-insert sequence, batch insert columns explicit, composite PK clause |
| Unit | `repo/DoobieVoteHistoryArchiverUnitSpec.scala` | MockitoScala | UUID generation deterministic via test clock, archive-then-delete order, ConnectionIO composition |
| Unit | `repo/StanceMaterializationStatusRepositoryUnitSpec.scala` | MockitoScala + Doobie fragment | UPSERT clause, conditional `has_votes=true, votes_updated_at=NOW()` gating |
| Class-level (justified: PG-specific behavior) | `repo/DoobieVoteRepositorySpec.scala` | DockerPostgres | §6.3: 1, 2, 3, 4, 5, 6, 7 |
| Class-level | `repo/DoobieVotePositionRepositorySpec.scala` | DockerPostgres | §6.3: 8, 9, 10 |
| Class-level | `repo/DoobieVoteHistoryArchiverSpec.scala` | DockerPostgres | §6.3: 11, 12, 13, 14, 15 |
| Integration | `repo/StanceMaterializationStatusRepositorySpec.scala` | DockerPostgres | supports §6.5: 29, 30, 31, 32, 33, 34 |

FUNCTIONAL verification:
- AC#3 (ON CONFLICT DO UPDATE): upsert A, then upsert B with different title — verify DB has B's title AND `updated_at` advanced.
- AC#13 (shared history_id): archive once, query both `vote_history` AND `vote_history_positions` — assert same UUID present in both.
- AC#15 (`ConnectionIO` composable): compose `archiveVote` + `upsert` + `replaceAll` into a single transaction, inject a failure mid-chain, assert DB rolled back (no partial write).
- AC#9 (replaceAll empty list): vote with 100 positions, call with `List.empty` → verify 0 positions in DB.
- **VoteType PG ENUM round-trip** (depends on P0.1): for each of the 8 variants, insert a VoteDO with that vote type and read it back — assert identity. Additionally, manually insert a row with a bogus `vote_type_enum` value via raw SQL (should fail at the DB level because PG rejects unknown enum values — asserts we get DB-level protection, not just app-level).

Total §6.3 AC rows: 15/15.

**Blocks**: P3.1.

---

### P2.5 — `VoteChangeDetector` (Area 6.4)

**Branch**: `feat/votes-change-detector`

**Files**:
- `pipeline/VoteChangeDetector.scala` — constructor takes `VoteRepository[F]` + `VotePositionRepository[F]` + `PipelineLogger[F]`.
  - `detect(incoming: VoteConversionResult, correlationId: UUID): F[VoteChangeReport]`.
- `pipeline/VoteChangeReport.scala` — enum ADT: `New | Updated(positionsChanged, diffs) | Unchanged`.
- `pipeline/VotePositionDiff.scala` — enum ADT: `Added | Removed | Changed`.

**Test layers**:
| Layer | File | Infrastructure | AC rows |
|---|---|---|---|
| Unit | `pipeline/VoteChangeDetectorSpec.scala` | MockitoScala — stub both repos | §6.4 all 17 rows |
| Class-level (property) | `pipeline/VotePositionDiffPropSpec.scala` | ScalaCheck | §6.4: 9 (order independence), 6-8 (diff detection generators) |

FUNCTIONAL verification:
- AC#2 (same updateDate → Unchanged): identical stored and incoming → `VoteChangeReport.Unchanged`, NO position fetch attempted (verify via Mockito `never()` on `positionRepo.findByVoteId`).
- AC#3 (regression — incoming older): assert warn logged, `Unchanged` returned, positions NOT fetched.
- AC#9 (order independence): use ScalaCheck to generate position lists with random orderings; assert `detect` returns same report regardless of order.
- AC#11 (correlationId in logs): capture log context in a test appender, verify `correlationId` field on every emitted log message.

Total §6.4 AC rows: 17/17.

**Blocks**: P3.1.

---

## Phase 3 — Processor + App

Each PR blocks the next. Must run sequentially.

### P3.1 — `VoteProcessor` (Area 6.5 streams + processing)

**Branch**: `feat/votes-processor`

**Depends on**: P2.1, P2.2, P2.3, P2.4, P2.5 all merged.

**Files**:
- `pipeline/VoteProcessor.scala` — constructor takes all clients, repos, resolver, detector, placeholder creator, event publisher, member/bill entity repos, config, logger. Methods: `streamAll`, `processHouseVotes`, `processSenateVotes`, `processVote`, `summarize`.
- `pipeline/ProcessingResult.scala` — **verify location first** (bills-common? members-common?). If present, import. Else define locally as enum `Succeeded(voteId, eventEmitted) | Skipped(voteId, reason) | Failed(voteId, reason)`.
- `pipeline/PipelineRunSummary.scala` — same verification approach.
- `errors/VoteProcessingFailed.scala`.

**Test layers**:
| Layer | File | Infrastructure | AC rows |
|---|---|---|---|
| **Unit (REQUIRED)** | `pipeline/VoteProcessorUnitSpec.scala` | MockitoScala — ALL deps stubbed (clients, repos, resolver, detector, publisher, placeholders) | Flow correctness: New → archive-skip+upsert+positions+event, Updated(positionsChanged=true) → archive+upsert+positions+event, Updated(positionsChanged=false) → archive+upsert+no-event, Unchanged → skip. Each ProcessingResult branch (Succeeded/Skipped/Failed). Correlation-ID propagation into every `F` action. |
| Component (multi-class) | `pipeline/VoteProcessorComponentSpec.scala` | MockitoScala — stub clients, real repos over H2 OR stubbed repos | §6.5: 6, 10-13, 21-23, 26-28 |
| Functional (end-to-end subsystem) | `pipeline/VoteProcessorFunctionalSpec.scala` | WireMock (Congress.gov + senate.gov) + DockerPostgres + in-memory event publisher capturing emissions | §6.5: 1-5, 7-9, 14-20, 24-25, 29-34 |

Total §6.5 AC rows: 37/37 (AC 35-37 are implicit, covered by passing other AC).

FUNCTIONAL verification:
- AC#1 (concurrent chambers): Stream.merge with WireMock stubs that yield 10 House votes + 10 Senate votes; assert all 20 ProcessingResults received, no ordering expected; time the total — must be less than sequential run (proves concurrency).
- AC#2/3 (failure isolation): inject a House-side failure; assert Senate results still arrive.
- AC#17 (atomic transaction): inject `positionRepo.replaceAll` failure; assert `votes` upsert rolled back (DB row gone/unchanged).
- AC#19 (retry via RetryWrapper): configure retry=3, 10ms backoff; mock publisher failing twice then succeeding; assert exactly 3 publish attempts, success, no error log.
- AC#31 (positionsChanged=false → no stance update): construct a metadata-only update scenario; assert `stance_materialization_status` NOT written.

**Blocks**: P3.2.

---

### P3.2 — `VotesPipelineApp` + wiring + integration test

**Branch**: `feat/votes-pipeline-app`

**Files**:
- Fill out `app/VotesPipelineApp.scala` and `app/VotesPipeline.scala` per CLAUDE.md testability refactor pattern (IOApp thin; all logic in companion `private[app]` methods accepting factory fns).
- Finalize `config/VotePipelineConfig.scala` with nested `HouseVotesConfig` + `SenateVoteXmlConfig` + `EventPublisherConfig` + `DatabaseConfig` + `CongressGovClientConfig`.
- `application.conf` — complete production config with env-var overrides.
- `application-test.conf` — H2-backed config for unit-runs where possible.

**Test layers**:
| Layer | File | Infrastructure | AC rows |
|---|---|---|---|
| **Unit (REQUIRED)** | `app/VotesPipelineUnitSpec.scala` | MockitoScala — stub all factory fns (configLoader, resourceBuilder, processorFactory, streamFactory) | `runWithFactories` calls each factory exactly once in the correct order, threads config + logger through, threads resources through to processor, returns the stream factory's output. All factories stubbed — no real construction. Verify: failure in configLoader short-circuits rest; failure in resourceBuilder does NOT leak acquired resources (Resource safety). |
| Integration | `app/VotesPipelineIntegrationSpec.scala` | DockerPostgres + Pub/Sub emulator + WireMock | full flow with real DB writes and real event emission on emulator |

FUNCTIONAL verification:
- Integration: stage 2 House votes + 2 Senate votes in WireMock; spin up emulator; run `VotesPipeline.runWithFactories` with real factories (except Congress.gov base URL pointed at WireMock); assert 4 rows in `votes` table, correct position counts, 4 events in emulator topic, all with `isUpdate=false`.
- Re-run with 1 position changed in one vote; assert history row created, event emitted with `isUpdate=true`.

**Blocks**: P3.3.

---

### P3.3 — E2E test (tagged)

**Branch**: `feat/votes-pipeline-e2e`

**Files**:
- `src/test/scala/.../e2e/VotesPipelineE2ESpec.scala` — tagged `taggedAs E2ETest` (imported from bills-common).
- `src/test/resources/wiremock/votes/` — Congress.gov + senate.gov recorded responses.

**Scope** (E2E, excluded from default `sbt test`, run via `sbt "testOnly -- -n com.repcheck.tags.E2ETest"`):
- Brings up DockerPostgres, Pub/Sub emulator, WireMock with recorded fixtures.
- Runs full `VotesPipelineApp.run` with real config (env vars mocked).
- Asserts end-to-end invariants: DB state, event emission, stance_materialization_status updates, placeholder creations, history archival, unresolved LIS logging.

**AC rows covered**: integrates everything — especially AC#28 (correlationId flows through everything — E2E is the true test).

**Blocks**: P4.1 (CI coverage upload), P6.

---

## Phase 4 — CI + Local Docker

Can overlap Phase 3.

### P4.1 — CI `.github/workflows/ci.yml`: add votes-pipeline coverage path + E2E hook

**Branch**: `ci/votes-pipeline-coverage`

**Changes**:
- Add `votes-pipeline/target/scala-3.7.3/scoverage-report/scoverage.xml` to Codecov upload list (ci.yml lines 63-69 pattern).
- (optional) Add `votes-pipeline` to the e2e-gcp job if running E2E against real GCP is desired in CI; otherwise E2E runs in docker-compose.e2e locally.

**Tests**: existing CI runs; new subproject auto-discovered via `sbt test` and `dockerTestParallel` (no explicit per-project listing needed).

**Guardrails**: verify all CI jobs green before merge.

---

### P4.2 — Docker Compose: add `votes-pipeline` to local + E2E compose

**Branch**: `feat/votes-pipeline-docker-compose`

**Changes**:
- `docker-compose.local.yml` — add `votes-pipeline` service (build context, env vars for DB/Pub/Sub host), `depends_on: [alloydb, pubsub-init, db-migrations]`, `restart: "no"`.
- `ofelia-config.ini` — register cron job (schedule: every 2 hours, or config-driven).
- `docker-compose.e2e.yml` — add `votes-pipeline` for E2E runs with WireMock configured to stub both Congress.gov and senate.gov.
- `e2e/wiremock/mappings/` — add votes-pipeline stub mappings if P3.3 hasn't already checked them in.

**Tests**: manually spin up `docker compose -f docker-compose.e2e.yml up` and run a smoke flow. Document the flow in the PR description.

---

## Phase 5 — Infrastructure (`tf-repcheck-infra`) — zero-cost GCP resources only

**User direction**: during this plan we validate the full system locally via docker-compose. We do NOT deploy Cloud Run Jobs or Cloud Scheduler yet — those incur recurring cost. However, we DO create GCP resources that cost nothing to hold (Pub/Sub topics/subscriptions, IAM bindings, Secret Manager bindings), so the future deploy is plug-and-play.

**What's in scope for Phase 5**: P5.1 (Pub/Sub), P5.4 (IAM). Can proceed in parallel with Phase 2.

**What's deferred to Phase 7 (not in this plan)**:
- Cloud Run Job terraform (compute cost)
- Cloud Scheduler (scheduler + invocation cost)
- Any `google_cloud_run_v2_job.*` or `google_cloud_scheduler_job.*` resources

The Dockerfile (from P1.1), image build path, and env-var schema all get validated via `docker-compose.local.yml` + Ofelia cron (P4.2). When the user greenlights cloud deploy (separate future plan), adding the Cloud Run Job + Scheduler is a small follow-up because the image + config are already proven.

### P5.1 — `modules/messaging`: Pub/Sub topics + subscriptions (zero-cost to hold)

**Branch**: `infra/votes-pubsub`

**Cost check**: GCP Pub/Sub pricing is per-message-published + per-byte-delivered. Idle topics and subscriptions with no traffic cost nothing. Dead-letter topics same. Safe to create ahead of deploy.

**Canonical naming** (per Tech-lead decision #12 — do not deviate):
- Topic: `vote-events`
- Dead-letter topic: `vote-events-dead-letter`
- Subscription: `vote-recorded-sub`

**Changes**:
- Add `google_pubsub_topic.vote_events` (resource ID `vote_events`; display name `vote-events`).
- Add `google_pubsub_topic.vote_events_dead_letter` (resource ID `vote_events_dead_letter`; display name `vote-events-dead-letter`).
- Add `google_pubsub_subscription.vote_recorded_sub` (subscribes to `vote-events` topic, with dead-letter policy pointing at `vote-events-dead-letter`, ack deadlines matching bills pattern).
- Placeholder publisher IAM binding — grant votes-pipeline SA `roles/pubsub.publisher` scoped to `vote-events` topic (this specific binding lives here or in P5.4 — agent decides; either is fine).

**Tests**: `terraform plan` in CI for the repo's existing `plan` job; apply to dev; verify topic/sub exist via `gcloud pubsub topics list`. No dollars spent — topics are idle.

---

### P5.4 — IAM: votes-pipeline service account + bindings (zero-cost to hold)

**Branch**: `infra/votes-iam`

**Cost check**: IAM service accounts are free. Role bindings are free. Secret Manager access bindings are free (though secret storage itself has a tiny per-secret-per-month cost; existing `congress-api-key` already paid).

**Changes**:
- `google_service_account.votes_pipeline_sa` — new SA `votes-pipeline-sa@<project>.iam.gserviceaccount.com`.
- Role bindings:
  - `roles/cloudsql.client` (if Cloud SQL) OR `roles/alloydb.client` (if AlloyDB) — chosen per env (dev uses Cloud SQL PostgreSQL per CLAUDE.md cost strategy; staging/prod use AlloyDB).
  - `roles/pubsub.publisher` scoped to `vote_events` topic.
  - `roles/secretmanager.secretAccessor` scoped to existing `congress-api-key` secret.
  - `roles/logging.logWriter`, `roles/monitoring.metricWriter` (free — only pay on log/metric volume).
- Workload Identity Federation binding for CI (if applicable for future deploy).

**Tests**: `terraform plan` → apply to dev → `gcloud iam service-accounts describe votes-pipeline-sa@...` returns the account with expected bindings. Verify `gcloud secrets versions access --impersonate-service-account=votes-pipeline-sa@...` can read `congress-api-key` (validates the binding without incurring compute).

---

## Phase 6 — Local validation (docker-compose) — iterative debug-and-verify loop

**User direction**: Phase 6 is **NOT** a one-shot smoke test. It's a **circular/iterative process**: launch → observe logs + DB → find an issue → fix (via hotfix PR or branch patch) → relaunch → re-verify. The phase is DONE only when every behavioral expectation holds, proven by SQL queries returning expected values.

Expect to do multiple cycles. Each cycle may yield new hotfix PRs (call them P6.H1, P6.H2, ...). The plan's Status Tracker records them as they appear.

### P6.1 — Tag + build local Docker image

Label the merged-to-main PR with `release:minor` to trigger the release workflow. Build the Docker image locally via `docker build -t repcheck/votes-pipeline:<tag> .`. No GCP push yet; this is just for local-compose consumption.

### P6.2 — Run the E2E loop (iterate until stable)

**The loop**:

```
┌─→ Launch stack ──→ Observe logs + DB ──→ Issue found? ───┐
│                                                 │        │
│                                                 ▼        │
│                                              Fix + patch │
│                                                 │        │
└───────────────────────────────────────────────  ←─────────┘
                          │
                          ▼
                  All expectations met
                          │
                          ▼
                       Done
```

**Launch**: `docker-compose -f docker-compose.e2e.yml up --build` with:
- WireMock seeded with recorded Congress.gov + senate.gov fixtures (from P3.3 `src/test/resources/wiremock/votes/`).
- AlloyDB Omni container (same one used in integration tests).
- Pub/Sub emulator container (topic `vote-events` created by `pubsub-init.sh`).
- Votes-pipeline container from the image built in P6.1.
- A debug subscriber container (or just `gcloud pubsub subscriptions pull` polling) to capture emitted events.

**Per-run observation checklist**:

1. **Votes-pipeline container logs**:
   - Log lines tagged with correlation IDs flow through the entire request → processing → upsert → publish chain (grep a correlation ID, expect ≥ 4 log lines: ingestion start, change detected, DB tx committed, event published).
   - No ERROR-level logs unless test explicitly injected failure.
   - WARN logs expected ONLY for: API retry recoveries, placeholder member creation for unknown LIS IDs.
   - Exception classes seen in logs are all project-owned (`HouseVoteFetchFailed`, `SenateVoteFetchFailed`, etc.) — no `RuntimeException`, `IOException`. If a standard exception appears, that's a plugin v0.5.0 bug — open hotfix PR.

2. **AlloyDB state — run these SQL queries against the live docker-compose AlloyDB**:

   **Q1 — Vote volume matches fixture**:
   ```sql
   SELECT chamber, congress, COUNT(*) AS total_votes
   FROM votes
   GROUP BY chamber, congress
   ORDER BY chamber, congress;
   ```
   Expected: row count matching the fixture's vote count per chamber/congress. If short, some fixture failed to process — check logs for correlation IDs with Failed results.

   **Q2 — Position counts plausible**:
   ```sql
   SELECT v.id AS vote_id, v.chamber, COUNT(vp.member_id) AS positions
   FROM votes v
   LEFT JOIN vote_positions vp ON vp.vote_id = v.id
   GROUP BY v.id, v.chamber
   ORDER BY positions DESC;
   ```
   Expected: House votes have ≈435 positions (some voters miss); Senate votes have ≤100. Any vote with 0 positions → bug (either fetch failed silently OR DTO→DO conversion dropped all positions). Investigate via correlation ID.

   **Q3 — VoteType enum correctly classified**:
   ```sql
   SELECT vote_type, COUNT(*) FROM votes GROUP BY vote_type ORDER BY 2 DESC;
   ```
   Expected: enum values distributed across `Passage`, `Amendment`, `Cloture`, etc. If everything is `Other`, `fromQuestion` isn't classifying — likely a regex/pattern bug. If you see a TEXT value that is NOT one of the 8 enum literals, migration 013's `vote_type_enum` constraint would have rejected the insert → look for `SenateVoteXmlDecoder` or DTO→DO conversion bug.

   **Q4 — Change detection works on re-run**:
   Reset: note current `max(updated_at)` on `votes`. Re-run the pipeline with the SAME fixture → verify NO row has a newer `updated_at` (Unchanged path worked).
   Modify one fixture's `updateDate` and one member's `voteCast` → re-run → verify EXACTLY ONE row gets new `updated_at` AND corresponds to the modified vote AND `vote_history` has a new row for that vote.
   ```sql
   SELECT vote_id, COUNT(*) AS archive_count FROM vote_history GROUP BY vote_id HAVING COUNT(*) > 0;
   ```
   Expected: one entry per modified vote across the run sequence. Zero if no modifications were made.

   **Q5 — Position history uses shared `history_id`**:
   ```sql
   SELECT vh.history_id,
          COUNT(DISTINCT vh.vote_id) AS vote_copies,
          COUNT(vhp.member_id) AS position_copies
   FROM vote_history vh
   LEFT JOIN vote_history_positions vhp ON vhp.history_id = vh.history_id
   GROUP BY vh.history_id
   HAVING COUNT(DISTINCT vh.vote_id) <> 1 OR COUNT(vhp.member_id) = 0;
   ```
   Expected: zero rows. Any output means history_id linkage is broken (either archiver didn't propagate history_id or the FK is wrong).

   **Q6 — Stance materialization flagged correctly**:
   ```sql
   SELECT sms.bill_id, sms.has_votes, sms.votes_updated_at,
          (SELECT COUNT(*) FROM votes v WHERE v.bill_id = sms.bill_id) AS vote_count
   FROM stance_materialization_status sms
   WHERE sms.has_votes = true
   ORDER BY sms.votes_updated_at DESC
   LIMIT 20;
   ```
   Expected: every bill with votes in `votes` table has `has_votes = true` AND a recent `votes_updated_at`. Any `has_votes = true` row with `vote_count = 0` → bug (flag set without actual votes). Any bill with `vote_count > 0` but no row in `stance_materialization_status` → bug (flag not set).

   **Q7 — Placeholder LIS members exist with correct naming**:
   ```sql
   SELECT m.id, m.natural_key, m.first_name, m.last_name, lmm.lis_member_id
   FROM members m
   LEFT JOIN lis_member_mapping lmm ON lmm.member_id = m.id
   WHERE m.natural_key LIKE 'lis:%'
   LIMIT 20;
   ```
   Expected: placeholders with `natural_key = 'lis:<digits>'`, `first_name`/`last_name` showing stub values, AND a matching `lis_member_mapping` row. Any placeholder missing from `lis_member_mapping` → LisResolver bug. Any `natural_key` matching the placeholder pattern but whose senator is known (bioguide exists) → a leaked placeholder that should have been merged.

   **Q8 — Placeholder merge works**:
   Given fixture with a known LIS mapping, run the pipeline. Confirm a placeholder is created. Then run lis-mapping-refresher against a fixture containing the real bioguide for that LIS ID. Then re-query Q7: the placeholder should no longer exist (or its `natural_key` should now be the real bioguide). Verify vote positions still reference the same `members.id` (FKs intact):
   ```sql
   SELECT vp.vote_id, vp.member_id, m.natural_key
   FROM vote_positions vp
   JOIN members m ON m.id = vp.member_id
   WHERE vp.vote_id IN (/* vote IDs that had placeholders before merge */);
   ```

   **Q9 — Foreign keys intact (no orphans)**:
   ```sql
   SELECT 'vote_positions with missing vote' AS check_name, COUNT(*)
   FROM vote_positions vp LEFT JOIN votes v ON v.id = vp.vote_id WHERE v.id IS NULL
   UNION ALL
   SELECT 'vote_positions with missing member', COUNT(*)
   FROM vote_positions vp LEFT JOIN members m ON m.id = vp.member_id WHERE m.id IS NULL
   UNION ALL
   SELECT 'vote_history with missing archive_id parent', COUNT(*)
   FROM vote_history_positions vhp LEFT JOIN vote_history vh ON vh.history_id = vhp.history_id WHERE vh.history_id IS NULL;
   ```
   Expected: all counts zero. Any non-zero → FK integrity violation; likely a Doobie transaction boundary bug.

3. **Pub/Sub emulator state**:
   Query the emulator via `gcloud` (point at `PUBSUB_EMULATOR_HOST=localhost:8085`):
   ```
   gcloud pubsub subscriptions pull --project=repcheck-local --auto-ack debug-vote-recorded-sub --limit=100
   ```
   Expected: one event per `ProcessingResult.Succeeded(voteId, eventEmitted=true)` observed in the logs. Count must match `SELECT COUNT(*) FROM votes WHERE updated_at >= <pipeline_start>` minus skipped/failed.

**When something doesn't match expectations**:
- Capture the failing state (correlation ID, query output, log excerpt).
- Open a hotfix PR (P6.H<N>) against `main` OR against the offending feature branch. Track in the Status Tracker.
- Re-build, re-launch, re-verify.

**Loop exit criteria**: all 9 queries above return the expected shape for the seeded fixtures. No unexpected WARN/ERROR log patterns. All ProcessingResults account for in Pub/Sub events.

### P6.3 — Ofelia cron long-running validation

After P6.2 loop stabilizes, switch to `docker-compose -f docker-compose.local.yml up` (Ofelia-scheduled). Let it run for ≥30 minutes with a shortened cadence (e.g., every 2 minutes for validation, not every 2 hours).

Per-tick checks (run after each scheduled firing):
- Ofelia logs show trigger firing at expected cadence.
- Votes-pipeline container exits cleanly (exit code 0) within timeout.
- `Q1` count doesn't inflate unexpectedly (Unchanged path suppresses re-writes).
- `Q4` re-confirms: no spurious archive rows unless fixtures changed.

Do this observation over ≥15 scheduled ticks to smoke out timing issues (e.g., overlapping runs, container restart storms, connection-pool exhaustion).

**Done after P6.3 loop stabilizes**: the system is "truly ready" per user direction. GCP compute deploy (Phase 7) unlocks as a separate future plan.

### P6.H? — Hotfix PRs discovered during P6.2 / P6.3

Not predictable upfront — list them here as they appear. Expected categories:
- Circe decoder field mismatches (JSON shape drifted from our DTO)
- SQL shape issues the unit tests missed (e.g., column ordering with PG type coercion)
- Docker Compose wiring (env var naming, network topology, container startup order)
- Pub/Sub emulator config (topic name, project ID)
- Exception classes missed by plugin v0.5.0 scan (something squeaked through)

Each hotfix follows the normal PR flow: branch, pushToPR, merge, re-launch P6.2.

---

## Phase 7 — GCP compute deploy (DEFERRED, NOT IN THIS PLAN)

Out of scope for this plan. When the user is satisfied the local validation proves correctness, a separate future plan covers:
- Cloud Run Job terraform (compute cost)
- Cloud Scheduler job (cron in GCP, cost)
- Dev → Staging → Prod promotion playbook
- Live Congress.gov / senate.gov runs against dev AlloyDB

Nothing in the current plan should add Cloud Run Job or Cloud Scheduler terraform resources. If an agent catches itself writing `google_cloud_run_v2_job.*` or `google_cloud_scheduler_job.*` for votes, STOP and surface to the user.

---

## Universal Guardrails (every PR, every agent)

| Guardrail | Rule | Source |
|---|---|---|
| Branch hygiene | Before pushing, run `gh pr list --state merged --head <branch>` — if merged, create new branch | memory `feedback_branch_hygiene` |
| CI checks | Use `pushToPR` / `CreatePR` — runs compile, test, scalafmtCheckAll, scalafixAll --check. Never push direct. | CLAUDE.md + memory `feedback_use_ci_scripts` |
| Serial CI in parallel agents | Parallel agents must serialize `sbt test` — agree on a lockfile or sequence | memory `feedback_serialize_ci_checks` |
| Coverage | 95% per-file (local gate), 90% patch (Codecov). No new entries in `codecov.yml` ignore list. | CLAUDE.md |
| Testability refactor | IOApp thin; all logic in companion object accepting factory fns. `coverageExcludedFiles` only for pure-wiring App. | CLAUDE.md |
| No `@nowarn` | Fix underlying deprecation; use non-deprecated API | CLAUDE.md |
| No `SELECT *` | List columns explicitly, order matching case class | CLAUDE.md + memory `feedback_no_select_star` |
| No `null` in tests | WartRemover applies to test code too | memory `feedback_no_null_in_tests` |
| Custom exceptions only — production code | Plugin v0.5.0 `ProjectExceptionsOnlyCheck` fails build if production code constructs non-project exceptions at `throw new X(...)`, `throw X(...)`, or `*.raiseError(new X(...))` / `*.raiseError(X(...))` sites. **Re-raises allowed** (`throw e`, `raiseError(err)` where arg is a variable). **Test code exempt** — tests may still throw standard exceptions to simulate external failures in mocks. Every new exception in production code must be a unique `Throwable` subclass declared under `repcheck.*` (enforced alongside existing simple-name uniqueness rule). | plugin v0.5.0 + CLAUDE.md + memory `feedback_custom_exceptions_only` |
| Import style | No FQN in signatures; always import short names | memory `feedback_import_style` |
| Table names | Constants from `pipeline-models.Tables`, never hardcoded strings | CLAUDE.md |
| WireMock binding | `127.0.0.1` + `dynamicPort()` — avoid Windows firewall prompts | memory `feedback_wiremock_localhost` |
| Doc compression | Never manually copy compressed docs — run `generate-agent-docs.sh` | memory `feedback_doc_compression` |
| G8 scaffolding scope | g8 template creates **new repos only**, not new subprojects inside an existing monorepo. For in-repo subprojects, model directly after the nearest sibling (e.g., `bill-metadata-pipeline` for paginated JSON + Pub/Sub pipelines). | memory `feedback_g8_scaffolding` (applies to new repos) + this PR (votes-pipeline subproject) |
| Per-client HTTP pacing | Every outbound HTTP client in every pipeline wraps its raw http4s `Client[F]` in the private `rateLimitedClient` helper (canonical copies live in `bill-metadata-pipeline`, `bill-text-availability-checker`, `bill-text-pipeline` — each pipeline has its own). The helper uses `Semaphore(1)` to serialize requests and a configurable `pageDelay` between releases. Per-client `parEvalMap(parallelism)` upstream naturally bounds the total fan-out. No shared/global throttle across clients. | existing pattern across all pipelines |
| PR review replies | Always reply to review comments on GitHub after fixing them | memory `feedback_reply_to_pr_comments` |
| No preemptive merge | Check for conflicts with `git merge-tree` first; only merge main when conflicts exist | memory `feedback_no_preemptive_merge` |
| Allow PR review time | Don't auto-merge. Wait for user. | memory `feedback_allow_pr_review` |
| Never delete plan files | This file and any EXECUTION_PLAN.md must persist | memory `feedback_never_delete_plan_files` |
| Stop on uncertainty | Don't make autonomous decisions when uncertain — stop and ask | memory `feedback_stop_on_uncertainty` |

## Parallel Agent Coordination Rules

1. **One agent per worktree, one branch per PR**. Different worktrees isolate JVM state, SBT sessions, and Docker containers (UUID-named in DockerPostgres). Agents never share a worktree.
2. **Serialize sbt validation**. Even across worktrees, running `sbt test` simultaneously against Docker-backed suites shares the ephemeral Docker daemon and can OOM a 16-32GB dev box. Agents must queue test runs.
3. **Regular rebase** on `main` to surface conflicts early — especially Phase 2 PRs all touching `build.sbt`.
4. **Status tracker updates on own branch** (per user direction). Each agent updates their row(s) in the Status Tracker on their own feature branch as status changes (Not Started → In Progress → In Review → Merged). Conflicts in the plan file resolve at merge via standard git conflict resolution — last merged wins. Agents rebase their branch on `main` before pushing to pull in other agents' tracker updates.
5. **Claim an unclaimed PR**: "first agent to commit to the designated branch name wins". If two agents want the same PR, the one with an open branch wins; the second picks an unclaimed PR. The branch name is pre-assigned in the Status Tracker (the "Branch" column).
6. **No cross-agent code review blockers**. Each agent ships their PR through the normal review process; agents don't review each other's PRs (the human user reviews).
7. **Shared fixtures**. If a Phase 2 agent needs a shared XML / JSON fixture, place it in `src/test/resources/shared/` and note it in the PR description so other Phase 2 agents can reuse.

## Status Tracker

(Agents update as work progresses. Status values: `Not Started` / `In Progress` / `In Review` / `Merged` / `Blocked`. **Updates land on each agent's own feature branch**; conflicts resolve at merge — last-merged-in wins.)

| PR ID | Title | Branch | Status | Deps | Notes |
|---|---|---|---|---|---|
| P0.1 | shared-models: VoteType PG enum meta + DTO→DO verify | feat/votetype-pg-enum-meta | **Merged** (PR #28, v0.1.26) | — | Wired existing vote_type_enum from migration 013 to Scala VoteType; DTO→DO billLookup via Applicative[F]. Also bumped vote_method_type PG enum via db-migrations #19. |
| P0.2 | data-ingestion: LisMappingRepository → members-common + batch | refactor/lis-mapping-repo-to-members-common | **Merged** (data-ingestion #37) | — | Moved trait+impl+UpsertResult; added findByLisMemberIds batch method; consolidated LIS cleanup into members-common TransactorFixture. |
| P0.3a | shared-models: bump exception-uniqueness plugin 0.4.0 → 0.5.0 | chore/bump-exception-uniqueness-plugin-0.5.0 | **Merged** (PR #29) | — | 0 violations surfaced. Published patch. |
| P0.3b | pipeline-models: bump exception-uniqueness plugin 0.4.0 → 0.5.0 | chore/bump-exception-uniqueness-plugin-0.5.0 | **Merged** (PR #20) | — | 2 RetryWrapper violations fixed (bound errorFactory result to val before raiseError). |
| P0.3c | ingestion-common: bump exception-uniqueness plugin 0.4.0 → 0.5.0 | chore/bump-exception-uniqueness-plugin-0.5.0 | **Merged** (PR #20) | — | 0 violations. 25 Docker-tagged tests green. Published patch. |
| P0.3d | db-migrations: bump exception-uniqueness plugin 0.4.0 → 0.5.0 | chore/bump-exception-uniqueness-plugin-0.5.0 | **Merged** (PR #20) | — | 0 violations. Published patch of db-migrations-runner. |
| P0.3e | data-ingestion: bump exception-uniqueness plugin 0.4.0 → 0.5.0 | chore/bump-exception-uniqueness-plugin-0.5.0 | **In Progress** (agent running) | — | Final of the 5; includes local dockerTestParallel per user direction. |
| P0.4 | ~~ingestion-common: `GlobalRequestThrottle`~~ | — | **DELETED** (2026-04-21) | — | Wrong abstraction — votes-pipeline will adopt the existing per-client `rateLimitedClient` pattern from sibling pipelines (Semaphore(1) + pageDelay) during P1.1 scaffold. No ingestion-common changes, no retrofit. |
| P0.5 | votr: update acceptance-criteria docs to match plan revisions | docs/votes-pipeline-spec-revisions | **Merged** (votr #112) | — | 4 §6 source files + 4 compressed outputs; regenerated via scripts/generate-agent-docs.ps1. |
| P0.6 | **CONDITIONAL** ingestion-common: promote PubSubEmulatorFixture | feat/pubsub-emulator-fixture | Conditional on P1.1 audit | — | Only open if P1.1 audit finds no existing fixture in bills-common / members-common. Otherwise skip and import existing. |
| P0.7 | pipeline-models: refactor VoteRecordedEvent (voteNaturalKey + billNaturalKey + drop voteId) | feat/vote-recorded-event-schema | **Merged** (PR #21, v0.2.0) | — | Breaking event shape change. 321/321 tests pass. Bumped to minor. |
| P1.1 | data-ingestion: votes-pipeline scaffold (direct; modeled after bill-metadata-pipeline) | feat/votes-pipeline-scaffold | **Merged** (data-ingestion #39) | — | Scaffold directory tree, IOApp, VotesPipeline companion w/ `rateLimitedClient`, PipelineExecutor (streaming Monoid fold retaining full failure context), VotesPipelineConfig, Dockerfile, application.conf(s), smoke test + PipelineExecutor spec (100% coverage). Launcher contract: args(0)=config, args(1)=runId, args(2)=stepRunId. |
| P2.1 | HouseVotesApiClient + WireMock tests | feat/votes-house-api-client | **In Review** (on feat/votes-house-api-client) | P1.1 merged, P0.2 merged | 14/14 §6.1 AC rows covered via WireMock + unit tests. 65 tests, per-file coverage ≥97.92%. DTO decoder adapters map real API field names (bioguideID/voteParty/voteState, houseRollCallVotes envelope, integer identifier, sourceDataURL) into v0.1.24 shared-models DTOs — P0.1 intended field renames shipped in shared-models but were NOT in v0.1.24 (verified: deferred to future shared-models bump per PR #28 commit message). |
| P2.2 | SenateVoteXmlClient + decoder + WireMock | feat/votes-senate-xml-client | Not Started — **next up** | P1.1 merged | |
| P2.3 | LisResolver + integration tests | feat/votes-lis-resolver | Not Started — **next up** | P1.1 merged, P0.2 merged | |
| P2.4 | Vote repositories + history archiver | feat/votes-repositories | Not Started — **next up** | P1.1 merged | |
| P2.5 | VoteChangeDetector | feat/votes-change-detector | Not Started — **next up** | P1.1 merged | |
| P3.1 | VoteProcessor + component/functional tests | feat/votes-processor | Not Started | P2.1–P2.5 | |
| P3.2 | VotesPipelineApp + integration | feat/votes-pipeline-app | Not Started | P3.1 | |
| P3.3 | E2E test | feat/votes-pipeline-e2e | Not Started | P3.2 | |
| P4.1 | CI coverage path | ci/votes-pipeline-coverage | Not Started | P3.3 | |
| P4.2 | Docker Compose entries | feat/votes-pipeline-docker-compose | Not Started | P3.2 | |
| P5.1 | tf: Pub/Sub vote-events (zero-cost to hold) | infra/votes-pubsub | Not Started | — | Can start during Phase 2 |
| P5.2 | ~~tf: Cloud Run Job~~ | — | **DEFERRED (Phase 7)** | — | Costs money; deferred until local validation proves ready |
| P5.3 | ~~tf: Cloud Scheduler~~ | — | **DEFERRED (Phase 7)** | — | Costs money; deferred |
| P5.4 | tf: IAM bindings + SA (zero-cost) | infra/votes-iam | Not Started | P5.1 (for topic-scoped pub bindings) | |
| P6.1 | Tag release + build local Docker image | — | Not Started | P4.*, P5.1, P5.4 | Builds image; no GCP deploy yet |
| P6.2 | Docker-compose E2E **iterative loop** | — | Not Started (iterative) | P6.1 | Launch → observe → fix → relaunch until 9 DB validation queries (Q1–Q9) all return expected shapes |
| P6.3 | Ofelia cron long-run validation (≥15 ticks) | — | Not Started (iterative) | P6.2 stable | Idempotency, scheduling, no overlap, no connection leaks |
| P6.H? | Hotfix PRs surfaced during P6.2 / P6.3 | — | Unknown until iteration begins | — | Add rows here as issues are found and fixed. Expect 0–10 hotfixes. Each follows normal PR flow. |
| P7.* | **DEFERRED** GCP compute deploy | — | **Not in this plan** | — | Separate future plan when user greenlights |

## Test Layer Matrix (by class)

**Rule (user direction)**: **Unit tests are REQUIRED for every class**. Class-level tests are optional and added only when a real collaborator provides meaningful additional signal (e.g., verifying a Doobie repository against an in-memory or DockerPostgres DB to catch SQL shape issues that mocks can't).

| Class | Unit (required) | Class-level (optional) | Component | Functional | Integration | E2E | Infrastructure | AC rows |
|---|---|---|---|---|---|---|---|---|
| HouseVotesApiClient | ✓ | — | ✓ | — | — | ✓ (via P3.3) | MockitoScala, WireMock (127.0.0.1 + dynPort) | §6.1 1-14 |
| SenateVoteXmlDecoder | ✓ | — | — | — | — | — | scala-xml fixtures | §6.2 3, 5 |
| SenateVoteXmlClient | ✓ | — | ✓ | — | — | ✓ | MockitoScala, WireMock | §6.2 1-7, 13 |
| LisResolver | ✓ | — | — | — | ✓ | ✓ | MockitoScala + DockerPostgres | §6.2 8-12, 14 |
| LisMappingRepository (extended in P0.2) | ✓ | ✓ (justified: SQL-shape verification) | — | — | ✓ | — | MockitoScala + DockerPostgres | supports §6.2 10 |
| VoteRepository | ✓ (SQL construction, column ordering, UPSERT clauses) | ✓ (optional, SQL shape via DockerPostgres) | — | — | ✓ | ✓ | MockitoScala + DockerPostgres | §6.3 1-7 |
| VotePositionRepository | ✓ | ✓ (optional) | — | — | ✓ | ✓ | MockitoScala + DockerPostgres | §6.3 8-10 |
| VoteHistoryArchiver | ✓ (UUID generation, ConnectionIO composition, archive-then-delete logic) | ✓ (optional) | — | — | ✓ | ✓ | MockitoScala + DockerPostgres | §6.3 11-15 |
| StanceMaterializationStatusRepository | ✓ (UPSERT SQL, conditional gating) | ✓ (optional) | — | — | ✓ | ✓ | MockitoScala + DockerPostgres | §6.5 29-34 |
| VoteChangeDetector | ✓ | — | ✓ | — | — | ✓ | MockitoScala + ScalaCheck | §6.4 1-17 |
| VoteProcessor | ✓ (flow orchestration, result aggregation, error classification branches — all deps mocked) | — | ✓ | ✓ | ✓ | ✓ | MockitoScala, WireMock, DockerPostgres, Pub/Sub emulator | §6.5 1-37 |
| VotesPipelineApp | ✓ (verify companion `runWithFactories` wires factories in the right order; all factory fns stubbed) | — | — | — | ✓ | ✓ | MockitoScala + DockerPostgres (integration) | wiring correctness |
| `rateLimitedClient` helper in VotesPipeline | ✓ (Semaphore(1) acquires, pageDelay spacing, permit released after each request) | — | — | — | — | — | cats-effect-testkit TestControl | pacing ACs |
| Every `*.errors.*` exception class | ✓ (message format, context inclusion) | — | — | — | — | — | pure | N/A |

Test layer definitions:
- **Unit** — REQUIRED FOR EVERY CLASS. Pure function or single class with all deps mocked. `sbt test` runs these with no external infra. Purpose: verify the class's logic in isolation; catch regressions in input-handling, error-branch selection, state transitions, and algorithmic correctness. MockitoScala stubs for traits/abstract collaborators; `cats-effect-testkit` TestControl for time + concurrency; ScalaCheck for property-based coverage where the domain benefits (diffing, parsing).
- **Class-level (optional)** — single class exercised with lightly-real collaborators: e.g., a real Doobie `ConnectionIO` against DockerPostgres for repository tests where mocked SQL behavior wouldn't catch column-order bugs or PG-specific ON CONFLICT semantics. Add a class-level test ONLY when unit-level coverage leaves a meaningful gap. Tagged `DockerRequired` when DB-backed.
- **Component** — multiple classes collaborating, external systems stubbed (WireMock for HTTP, in-memory for DB, in-memory event sink). Catches integration seams.
- **Functional** — full subsystem flow, real DB, real event publishing, external HTTP stubbed — but run in a single JVM test process. Tagged `DockerRequired`.
- **Integration** — full app wiring, real infrastructure (DockerPostgres + Pub/Sub emulator), tagged `DockerRequired`.
- **E2E** — full app runnable via docker-compose.e2e — tagged `com.repcheck.tags.E2ETest`, excluded from default `sbt test`.

**Unit-test guidance per class category** (so agents know what to actually test, not just add for coverage):
- **Doobie repositories**: unit test SQL construction (e.g., using `doobie.util.fragment.Fragment.internals` or matching the rendered SQL string against a regex); assert columns listed explicitly (catches SELECT *); assert UPSERT uses ON CONFLICT correctly; verify `Read[T]` column count matches case class arity. Don't test actual DB behavior at this layer — that's the integration test's job.
- **API clients**: unit test URL construction, query param presence (especially `api_key`), header setting, error-classification branch choices given mocked HTTP responses.
- **Processors / orchestrators**: unit test the DAG of operations — given a specific mock outcome for each dependency, verify which branches fire, what order, what the aggregated result looks like. Every `ProcessingResult.Succeeded | Skipped | Failed` path should have at least one unit test.
- **Exception classes**: unit test `.getMessage` format; assert all constructor fields appear in the message; assert `getCause` exposes the wrapped cause when provided.
- **Companion objects / factory wiring** (App.scala companions): unit test `runWithFactories` with all factory fns stubbed; assert each stub is called exactly once, in order, with the expected inputs threaded through.

## Tech-lead decisions locked (not re-open without cause)

These were flagged as ambiguities during plan review. Listed here so agents don't re-litigate.

1. **API pagination ordering** (P2.1): fetch ALL pages, sort client-side DESC by updateDate, then filter by lookback cutoff. Per user direction. Do NOT use `takeWhile` short-circuit.
2. **Spec docs** (P0.5): votr's acceptance-criteria markdown gets updated ahead of Phase 2 via its own PR. Plan file and spec stay in sync.
3. **P0.4 DELETED** (2026-04-21): no retrofit of existing pipelines. Votes-pipeline copies the existing `rateLimitedClient` helper pattern from sibling pipelines (per-client `Semaphore(1)` + `pageDelay`). No shared throttle.
4. **Status tracker updates**: each agent updates on own branch; conflicts resolve at merge; no external tracker.
5. **Correlation ID generation site**: generated at the `processVote` entry point in `VoteProcessor` (one UUID per vote item as it enters the pipeline). Stream factory generates a separate run-level UUID (`runId`) available via `LogContext.runId`. Both flow through every log line and every event.
6. **ProcessingResult / PipelineRunSummary fallback**: if not found in bills-common or members-common during P1.1 audit, define locally under `repcheck.ingestion.votes.pipeline` package. Out-of-scope cleanup (promote to ingestion-common) flagged as follow-up in "Out of Scope" section.
7. **Senate XML fixtures**: agent captures a minimum 3 live senate.gov XML samples (one well-formed vote, one malformed, one index) during P2.2 development. Anonymize only if they contain PII (shouldn't — legislative data is public). Commit to `votes-pipeline/src/test/resources/senate-xml/`. If WireMock can't serve XML with the correct `application/xml` Content-Type, file a P6.H hotfix — likely a docker-compose.e2e WireMock config tweak.
8. **Plugin v0.5.0 violations per repo**: agent opens each P0.3* PR with an empty commit first, runs `sbt test`, captures the full violation list from `ProjectExceptionsOnlyCheck` output, pastes it into the PR description. Then fixes each violation and commits. Reviewer sees the before-after delta explicitly.
9. **`HasPlaceholder[MemberDO]` extension for LIS** (P2.3): DO NOT extend the typeclass. Instead, add a specialized `PlaceholderCreator.ensureExistsForLis(lisMemberId: Long, naturalKey: String): F[Long]` overload in ingestion-common (or votes-local equivalent). The typeclass stays simple (one method, bioguide-keyed); the LIS path is explicit at the call site. Keeps the abstraction honest.
10. **Placeholder field defaults** (per user direction): reuse the existing `HasPlaceholder[MemberDO]` instance defined in `repcheck-shared-models/.../dos/member/MemberDO.scala`. P1.1 agent audits: if it sets sensible defaults for all NOT NULL columns (`first_name`, `last_name`, `current_party`, `state`, `update_date`), reuse as-is for both House and Senate placeholder paths. If it depends on bioguide being present in a way that breaks for LIS-only inputs, the agent either (a) refactors the instance to accept a natural_key parameter directly, or (b) introduces an `ensureExistsForLis` overload that passes through to the same field factories with the synthetic natural_key. Either path keeps field defaults consolidated in one location.
11. **Position comparison identity** (P2.5 VoteChangeDetector, per user direction): compare positions by `memberId: Long` (DO-level). Placeholder→real merge via lis-mapping-refresher will show as Removed(placeholder_id) + Added(real_id) on the next votes-pipeline run for that vote. This is semantically accurate — the FK identity really did change, and downstream consumers (scoring engine) must reprocess. Do NOT add a natural_key column to vote_positions. Keep the schema lean.
12. **Canonical Pub/Sub naming** (per user direction): use these exact strings across terraform, IngestionEventPublisher config, docker-compose files, and tests:
    - Topic: `vote-events` (hyphenated; matches `bill-events` pattern)
    - Dead-letter topic: `vote-events-dead-letter`
    - Subscription: `vote-recorded-sub`
    - Event class: `VoteRecordedEvent` (already exists in pipeline-models)
    - Environment-scoped prefix: none at the topic name level; GCP project boundary provides isolation.
    No variation. Any agent encountering a different name in the codebase / config should assume bug and fix.
13. **Senate XML voteNumber zero-padding** (P2.2): URL format is `vote_<congress>_<session>_<voteNumber padded to 5 digits>.xml`. Example for vote 7, congress 119, session 1: `vote_119_1_00007.xml`. Use `f"$voteNumber%05d"` in Scala. Unit test with boundary cases: 1, 9, 10, 99, 100, 999, 1000, 9999, 10000 (and verify error if voteNumber ≥ 100000 — shouldn't happen, but assert).
14. **WireMock XML Content-Type** (P2.2 tests): `.willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/xml").withBody(xmlString))`. The `http4s-scala-xml` library expects this Content-Type header to trigger XML decoding. If you serve XML with `text/plain` or no Content-Type, decoding silently falls back to string and fails downstream.
15. **E2ETest tag import path** (all PRs that tag E2E tests): `import com.repcheck.tags.E2ETest` — the tag is defined in `bills-common/src/test/scala/.../tags/E2ETest.scala` (verified via grep). Votes-pipeline can import it transitively via `membersCommon % "compile->compile;test->test"` IF members-common itself depends on bills-common's test sources. If the import fails during P1.1 scaffold, add a direct `billsCommon % "test->test"` dependency on votes-pipeline's SBT config — or flag as a promotion-candidate for follow-up (move tag to ingestion-common).
16. **Doobie repository SQL unit test technique**: use Doobie's `Query0.sql` / `Update0.sql` accessors to obtain the rendered SQL string, then assert with regex matches or exact string comparison for the critical parts (column lists, ON CONFLICT clauses). Example:
    ```scala
    val (sqlString, _) = fr"INSERT INTO votes (vote_id, ...) VALUES ($voteId, ...)".update.sql
    sqlString should include("INSERT INTO votes")
    sqlString should not include("*")  // ensure no SELECT *
    ```
    For SELECT queries, `.query[T].sql` exposes the string. If the Doobie API surface shifts, fall back to passing a mock `Transactor` via MockitoScala that captures the `ConnectionIO` and inspects it. The goal is FUNCTIONAL verification of SQL shape, not syntactic validation — PG handles that.

17. **VoteDO schema change** (P0.1 expanded, per plan review): `VoteDO.voteType: Option[String]` → `VoteDO.voteType: Option[VoteType]`. The API's procedural `voteType` (e.g., "Yea-and-Nay") is NOT persisted — only the domain-classified `VoteType` (derived via `VoteType.fromQuestion`) goes to DB. Existing `question: Option[String]` field stays as the source of truth for the raw API question text. HasPlaceholder instance (line 41-64) needs no change since `voteType = None` remains valid.

18. **Stream.merge failure isolation** (P3.1 VoteProcessor, per user direction): each chamber's stream MUST be wrapped in `.handleErrorWith(e => Stream.eval(logError(e)) >> Stream.emit(ProcessingResult.Failed("chamber-level", e.getMessage)))` at the OUTER boundary before `Stream.merge`. A systemic error (401/403/DB-down) materializes as a single `ProcessingResult.Failed` for that chamber, and the other chamber continues to completion. `PipelineRunSummary.failed` counts those chamber-level failures. Do NOT let exceptions propagate through the merge — that would kill both branches and violate AC#2/#3.

19. **Mid-plan DB migrations** (any Phase 2 / Phase 3 PR, per user direction): if an agent discovers a need for a new migration, they **STOP**, surface the need to the user with column/table/constraint + rationale, and await user confirmation before opening a migration PR. No migrations land silently. When approved, the flow is: migration PR in `repcheck-db-migrations` → bump + publish db-migrations-runner → data-ingestion rebases onto the new runner version → then feature PR. This applies even for "small" migrations.

20. **VoteDO natural key format** (per user direction): `s"${chamber.toString.toLowerCase}:$congress:$session:$rollNumber"` — example: `"house:119:1:17"`, `"senate:119:2:437"`. Format locked; every writer and every ON CONFLICT clause uses this exact construction. `VoteDTOs.toDO` produces it; `VoteRepository.upsert` keys on `ON CONFLICT (natural_key) DO UPDATE`.

**Session source of truth**: session is ALWAYS known from the fetch URL, not from the response body. House: `/house-vote/{congress}/{session}` — session in path. Senate: `vote_{congress}_{session}_N.xml` and `vote_menu_{congress}_{session}.xml` — session in path. The DTO's `sessionNumber: Option[Int]` field is just API-shape defense; when constructing the DO, the session is threaded through from the fetch-call parameter (not from XML body parsing). Never "default to 1" — session is an input, not a derived value.

**Bill natural_key format** (locked per user direction): `s"${legislationType.toLowerCase}-$legislationNumber-$congress"` — example: `"hr-30-119"`, `"sres-12-119"`. Always lowercase legislationType (matches bills-pipeline pattern). Used for bill placeholder creation AND for the `billNaturalKey` field in `VoteRecordedEvent`. Unit tests verify exact format.

21a. **HTTP 429 Retry-After handling** (P2.1 + P2.2, per user direction): add a votes-local retry wrapper (or reuse existing `RetryWrapper` if it already accepts a per-error delay function) that inspects the failed response for a `Retry-After` header when the error is 429. If present, parse as either `<delta-seconds>` (integer) or `<HTTP-date>` (RFC 7231 date); compute the delay to next retry from that value. If absent or unparseable, fall back to the existing exponential backoff (10ms initial × 2 multiplier, 60s cap, max 3 attempts). Do NOT override `Retry-After` with the exponential schedule — the header is the server's explicit signal. Unit test: stub a response with `Retry-After: 120` → verify next attempt fires at ≥ 120s; stub with no header → verify next attempt fires at 20ms (first backoff). Do NOT burn quota by retrying faster than `Retry-After` says.

21b. **Senate XML date parsing** (P2.2 `SenateVoteXmlDecoder`, per user direction): parse `voteDate` by trying formats in this order:
    1. ISO-8601 (`"yyyy-MM-dd'T'HH:mm:ss"` + offset variants).
    2. `"EEEE, MMMM d, yyyy, hh:mm a"` (e.g., `"Thursday, April 3, 2025, 02:42 PM"`).
    If both fail → raise `SenateVoteFetchFailed(..., detail = "Unparseable voteDate: <raw>", cause = <date parse exception>)`. Vote does NOT persist (fail-fast). Unit test: parseable ISO → success; parseable long-form → success; unparseable → exception with context. DO NOT silently default to None — per user direction, parse failure is a hard error that must surface.

21c. **Post-merge issue policy** (per user direction): once a Phase 0/1/2/3 PR has merged to `main`, any fix goes through a new branch + new PR, not reopen. Treat as a P6.H<N> hotfix even if Phase 6 hasn't started yet. Status tracker gets a new row. Never force-push onto the original PR's history after merge.

21d. **CI validation command** (all agents before `pushToPR`, per user direction): `sbt coverage test dockerTestParallel coverageReport` — matches CI exactly. Sequential `dockerTest` is slower and doesn't exercise test-isolation under concurrent load, so it's insufficient. If the parallel command fails locally but `dockerTest` passes, that's a real test-isolation bug to fix (not a test to skip).

21e. **HikariCP + pipeline concurrency tuned low enough to allow ALL pipelines to run concurrently** (per user direction, revised): the docker-compose.local stack runs 7+ pipeline containers concurrently. AlloyDB Omni default `max_connections ≈ 100`. To avoid pool exhaustion under simultaneous runs, reduce BOTH the per-pipeline HikariCP pool size AND the in-app parallelism so each pipeline uses very few connections at once. No scheduling stagger — apps run concurrently.

**Pool budget**: target 7 pipelines × 5 connections = 35 peak, well under 100. Headroom for migrations + ad-hoc queries.

Revised votes-pipeline profile:
- `database.hikari.maximum-pool-size = 5` (was default 10)
- House `parallelism = 1` (was 2)
- Senate `parallelism = 1` (unchanged)
- House `page-delay = 2s` (between requests, enforced by House client's `rateLimitedClient` wrapper)
- Senate `request-delay = 3s` (between requests, enforced by Senate client's `rateLimitedClient` wrapper)

Peak in-flight Doobie connections for votes-pipeline under this profile: House (1) + Senate (1) + event publisher (1) + migration runner startup (1) = 4 → pool of 5 has minimal but adequate headroom for stuck queries.

**Existing pipelines stay as-is** — each already uses its own `rateLimitedClient` (`Semaphore(1)` + per-pipeline `pageDelay`) and has its own HikariCP pool tuning. No retrofit needed; no shared throttle introduced.

**Pool starvation signal**: `HikariPool-1 - Connection is not available, request timed out after Xms` in logs. If seen during Phase 6 validation, investigate the stuck query first — don't reflexively expand the pool.

21f. **Pub/Sub event payload format — Circe JSON, all pipelines, all events** (per user direction): every RepCheck pipeline's Pub/Sub events serialize to Circe JSON as the message `data` bytes (UTF-8). `VoteRecordedEvent` is no exception. P1.1 agent audits the existing `DefaultIngestionEventPublisher` / `GooglePubSubEventPublisher` serialization code during scaffold: if it already emits Circe JSON, votes-pipeline inherits. If ANY existing event uses a different format (ProtoBuf, Kryo, etc.), flag it as **out-of-scope follow-up** in this plan's "Out of Scope / Follow-ups" section — do not block votes-pipeline work to fix a pre-existing divergence. Test pattern: integration/E2E tests pull messages from the Pub/Sub emulator and assert `io.circe.parser.decode[VoteRecordedEvent](new String(msg.data, UTF_8))` succeeds with all fields populated.

21g. **VoteDO.billId resolution flow** (P3.1 `VoteProcessor.processVote`, per user direction): DTO→DO conversion is a two-phase handoff.

Phase 1 (pure, in `VoteMembersDTO.toDO`): produce an intermediate `VoteConversionResult` that contains:
- A partially-populated `VoteDO` with `billId: Option[Long] = None`.
- A `billNaturalKey: Option[String]` derived from DTO's `legislationType` + `legislationNumber` + `congress` fields. Format must match existing bills natural_key pattern — agent audits bills-common repository code to confirm (likely `s"${legislationType.toLowerCase}-${legislationNumber}-${congress}"` or similar).
- The list of `VotePositionDO` stubs with `memberId: Long` unresolved at this phase.

Phase 2 (impure, in `VoteProcessor.processVote`): given the `VoteConversionResult`, resolve IDs:
- For each positional `bioguideID` (House) or synthesized from placeholder (Senate, via LisResolver): look up via `MemberRepository.findByNaturalKey(bioguide)`; if `None`, call `PlaceholderCreator.ensureExists[MemberDO]` to create and return the Long. Thread the resulting Long into `VotePositionDO.memberId`.
- For `billNaturalKey`: if `Some(nk)`, call `BillRepository.findByNaturalKey(nk)`; if `None`, call `PlaceholderCreator.ensureExists[BillDO]` with that natural key — which creates a bill placeholder that the bills pipeline will enrich on its next run. Use returned Long as `VoteDO.billId = Some(id)`. If `billNaturalKey` is `None` (procedural vote), leave `VoteDO.billId = None`.
- Final `VoteDO` + `List[VotePositionDO]` go into the upsert + replaceAll transaction (see decision 21h).

21i. **Change detection runs FIRST** (per user direction, P3.1 VoteProcessor): order of operations inside `processVote`:
1. Convert DTO → VoteConversionResult (pure).
2. `VoteChangeDetector.detect` against stored state (reads only — no writes).
3. If `Unchanged` → return `ProcessingResult.Skipped`. **No placeholders created, no upserts, no event**. This is the fast path for most votes on repeat runs.
4. If `New` or `Updated` → proceed with T1 (placeholder creation) then T2 (archive + upsert + positions).
5. If `New` or `Updated(positionsChanged=true)` AND billId present → upsert `stance_materialization_status` (outside transactions).
6. If `New` or `Updated(positionsChanged=true)` → publish `VoteRecordedEvent` (outside transactions, with retries).

Performance win: Unchanged votes skip the entire placeholder + write path, avoiding orphan placeholder creation AND redundant DB work. The change-detection read is cheap (index hit on votes.natural_key).

21j. **`VoteRecordedEvent` schema change** (pipeline-models, per user direction): event must carry BOTH the vote's primary identity AND the bill's natural key for downstream correlation. New event shape:
```scala
final case class VoteRecordedEvent(
  voteNaturalKey: String,               // "house:119:1:17" — every vote has this
  billNaturalKey: Option[String],       // "hr-30-119" when linked; None for procedural
  chamber: Chamber,
  date: Instant,
  congress: Int,
  isUpdate: Boolean,
)
```
Requires a **new prereq PR (P0.7) in `repcheck-pipeline-models`** to refactor the existing event:
- Drop the legacy `voteId: String` field (replaced by `voteNaturalKey`).
- Rename `naturalKey: Option[String]` → `billNaturalKey: Option[String]` (semantic clarity — PR #15's rename is refined).
- Add `voteNaturalKey: String` (required).
- Bump pipeline-models minor version (breaking change to event shape).
- Consumer migration: no existing consumers of this event yet (scoring engine is Phase 4), so zero breakage today. Document the migration pattern for future consumers.

P0.7 must merge + publish before P3.1 (VoteProcessor) consumes the new shape. Sequencing: P0.3b (plugin bump) merges first (same repo), then P0.7 rebases + merges. Agents keep concurrent branches open but serialize merges.

21k. **`VoteMembersDTO.toDO` Left cases** (P0.1 + P2.1, per user direction — all four conditions trigger `Left(reason)`):
1. Unparseable date strings (`voteDate`, `updateDate`, `startDate`) → `Left("Unparseable <fieldName> date: '<raw>'")`.
2. Missing or empty `voteQuestion` → `Left("Missing voteQuestion")`. Without it, we can't derive domain VoteType.
3. Empty `results` list (zero positions) → `Left("Zero positions in vote — suspect data fetch issue")`. Surfaces to operator.
4. Invalid `legislationType` (not parseable to `BillType` enum) → `Left("Unrecognized legislationType: '<raw>'")`. Alternative: tolerate by treating the vote as procedural (`billId = None`) — NOT chosen per user direction; fail instead so bad API data surfaces quickly.

Each Left reason is captured in `ProcessingResult.Failed(voteNaturalKey, reason)` by the processor; vote does not persist. PipelineRunSummary counts Failed.

21m. **Docker-compose migration ordering** (P4.2): both `docker-compose.local.yml` and `docker-compose.e2e.yml` must include a `db-migrations` service that runs Liquibase migrations before any pipeline container starts. Votes-pipeline container declares `depends_on: { db-migrations: { condition: service_completed_successfully } }`. Pattern already present for bills/members — votes mirrors exactly.

21n. **Ofelia no-overlap for votes-pipeline cron** (P4.2): the votes-pipeline job entry in `ofelia-config.ini` must include `no-overlap: true`. Without it, two concurrent runs can race on the same votes (UPSERT is idempotent, but change-detection reads + subsequent writes can interleave producing unnecessary churn). Standard Ofelia option.

21o. **`VoteConversionResult` type shape** (P0.1 / P2.1 / P3.1):
```scala
final case class VoteConversionResult(
  voteDo: VoteDO,                            // billId starts as None; processor resolves in Phase 2
  billNaturalKey: Option[String],            // derived from DTO legislationType + legislationNumber + congress; None for procedural
  positions: List[UnresolvedVotePosition],   // carries source identifier pre-resolution
)

final case class UnresolvedVotePosition(
  positionSource: Either[String, Long],      // Left(bioguideID) for House, Right(lisMemberId) for Senate
  voteCast: String,
  voteParty: String,
  voteState: String,
)
```
Lives in `repcheck.ingestion.votes.pipeline` package (votes-local, not promoted to shared-models — votes-specific intermediate type).

21p. **`docker-compose.e2e.yml` is LOCAL-ONLY** (not run in CI): CI continues to use `sbt test dockerTestParallel` which exercises `SharedDockerPostgres` + test-scoped emulators. The `docker-compose.e2e.yml` stack is invoked ONLY during Phase 6 local-iteration loop. Do NOT add a GitHub Actions job that stands up docker-compose.e2e — it would be costly, redundant with `dockerTestParallel`, and hard to observe. If an agent is tempted to add it, STOP.

21q. **Position deduplication in `VoteMembersDTO.toDO`** (per user direction): the DTO→DO conversion dedupes positions on the way in. For House: dedupe by `bioguideID`. For Senate: dedupe by `lisMemberId`. Keep the first occurrence (deterministic), discard duplicates, log a warn with count + duplicate IDs. Prevents UPSERT from hitting the `(vote_id, member_id)` composite PK constraint. Unit test: stub a DTO with two identical bioguides → result has one position, one warn log.

21r. **HTTP 400 from Congress.gov classified Systemic** (per user direction): 400 (Bad Request) means the request shape is wrong — retrying won't help. `CongressGovErrorClassifier` returns `Systemic` for 400 just like for 401/403 (already existing behavior for 4xx per audit). Vote fails fast; operator investigates. Do NOT retry on 400.

21s. **Unknown `legislationType` fails fast** (per user direction, reinforcement of decision 21k): if the API returns a `legislationType` value that doesn't parse to the existing `BillType` enum, `VoteMembersDTO.toDO` returns `Left("Unrecognized legislationType: '<raw>'")`. The vote does not persist. The operator adds the new variant to `BillType` (in shared-models, as a bump PR) and retries. No tolerant fallback — unknown types are a signal that the API has evolved and our domain model needs to evolve with it.

21u. **Secret injection via env vars** (per user direction): `CONGRESS_GOV_API_KEY` is passed into every pipeline container via docker-compose `environment` blocks, sourced from the host process environment. In `docker-compose.local.yml` / `docker-compose.e2e.yml`, the votes-pipeline service includes:
```yaml
environment:
  CONGRESS_GOV_API_KEY: ${CONGRESS_GOV_API_KEY}
  GOOGLE_APPLICATION_CREDENTIALS: /dev/null   # unused locally; real GCP auth deferred to Phase 7
  DATABASE_URL: jdbc:postgresql://alloydb:5432/repcheck
```
Agent setting up local dev exports `CONGRESS_GOV_API_KEY` in their shell (PowerShell for Windows host per memory `reference_anthropic_key` pattern) before `docker-compose up`. `.env` files in the repo are NOT committed (security). Missing key → container fails fast on first API call with a Systemic 401.

21v. **Phase 3 waits for ALL Phase 2 merges** (tech-lead decision, no stubs): P3.1 VoteProcessor wires real implementations of every Phase 2 class (HouseVotesApiClient, SenateVoteXmlClient, LisResolver, VoteRepository et al., VoteChangeDetector). Starting P3.1 with stubs for unmerged Phase 2 dependencies would require abstraction work (mock factories) that gets thrown away when real deps land. Not worth it. Phase 3 agent waits for the last Phase 2 PR to merge, rebases onto main, opens P3.1.

21w. **No PR size cap** (per user direction): PRs ship at whatever size the coherent change requires. No artificial line limit. Expected-larger PRs (P3.1 VoteProcessor + full test suite) land as single reviewable units without splitting. Agents focus on clarity of PR description and commit structure, not on hitting a line cap. Reviewer sets their own pace.

21x. **Historic vote ingestion scope — configurable range via tuple list** (per user direction): `VotePipelineConfig` includes `congressSessions: List[CongressSession]` where `CongressSession(congress: Int, session: Int)`. Default config specifies the current session: `[{ congress = 119, session = 1 }]`. For backfill, the operator overrides config with additional entries, e.g.:
```hocon
pipeline.congress-sessions = [
  { congress = 117, session = 1 }, { congress = 117, session = 2 },
  { congress = 118, session = 1 }, { congress = 118, session = 2 },
  { congress = 119, session = 1 }
]
```
Pipeline iterates the tuple list sequentially (one (congress, session) at a time — keep the concurrency model simple). For each tuple, runs both House and Senate streams (which themselves merge as per the Stream.merge pattern). `PipelineRunSummary` aggregates across all tuples.

**Completeness verification** (Phase 6 Q1 extension): for each (congress, session) tuple, assert ingested-vote-count == source-reported-count. Source count:
- House: API response's `pagination.count` field from the first `/house-vote/{congress}/{session}` page.
- Senate: count of entries in `vote_menu_{congress}_{session}.xml` index.
Any mismatch → warn log with both numbers. Operator investigates.

**Scope implication**: no separate backfill pipeline needed. Same votes-pipeline code handles both current session (one tuple) and historical backfill (many tuples). Operational convention: production config sets current sessions only; ad-hoc backfill runs override config.

21t. **Ofelia job ordering — lis-mapping-refresher BEFORE votes-pipeline** (per user direction, updates decision 21n): in `ofelia-config.ini` (used by `docker-compose.local.yml`), schedule the two pipelines as sequential, not concurrent:
```ini
[job-exec "lis-mapping-refresher"]
schedule = @every 2h
container = lis-mapping-refresher
no-overlap = true

[job-exec "votes-pipeline"]
schedule = @every 2h, offset 10m        # 10-minute offset ensures refresher finishes first
container = votes-pipeline
no-overlap = true
```
Alternatively use Ofelia's `job-exec-after` dependency (if available in the pinned Ofelia version) to explicitly chain: votes-pipeline runs ONLY after lis-mapping-refresher completes successfully. Time-offset is simpler and robust enough.

Rationale: by running refresher first, the `lis_member_mapping` table is as fresh as senate.gov allows before any Senate vote ingestion. The only remaining LIS placeholders are for truly-new-to-senate-gov senators (rare). On the next tick, refresher picks them up and merges. Steady state: no placeholder accumulation.

For docker-compose.e2e.yml (Phase 6 validation): the same ordering — launch lis-mapping-refresher container first, await completion, then launch votes-pipeline. Doable via explicit script in the Phase 6 smoke: `docker-compose run --rm lis-mapping-refresher && docker-compose run --rm votes-pipeline`.

21l. **Bills natural_key format audit** (P0.1 / P1.1 agent, per user direction): before locking the bill natural_key construction, the agent reads:
- `C:\Users\elita\source\repos2024\repcheck-data-ingestion\bills-common\src\main\scala\repcheck\bills\common\persistence\DoobieBillRepository.scala` — find the `ON CONFLICT (natural_key) DO UPDATE` clause; find the natural_key construction site.
- `C:\Users\elita\source\repos2024\repcheck-shared-models\repcheck-shared-models\src\main\scala\repcheck\shared\models\congress\dos\bill\BillDO.scala` — find the `naturalKey: String` field + any factory method.
- `C:\Users\elita\source\repos2024\repcheck-shared-models\repcheck-shared-models\src\main\scala\repcheck\shared\models\congress\dto\bill\BillDTOs.scala` — find the `.toDO` implementation; note the exact format string.

Lock the discovered format into this plan file (edit Tech-lead decision 20 to add the bill natural_key format alongside the vote natural_key format). Votes-pipeline must use the IDENTICAL format when constructing `billNaturalKey` for placeholder creation and event emission — any mismatch causes duplicate bill placeholders (natural_key uniqueness enforced at the DB level, so collisions fail visibly at least).

21h. **Transaction scope — two transactions** (per user direction): placeholder creation and vote write happen in SEPARATE Doobie transactions.

**Transaction 1 (placeholders — commits independently)**:
- `LisResolver` creates any missing placeholder members (Senate only, synthetic natural_key).
- `MemberRepository` placeholder creation for unknown House bioguides.
- `BillRepository` placeholder creation for unknown billNaturalKey.
- All three combined into a single `ConnectionIO`, `.transact(xa)` once.

**Transaction 2 (vote + positions + history — commits independently after T1 succeeds)**:
- If `VoteChangeReport = Updated`, `VoteHistoryArchiver.archiveVote(voteId)` first.
- `VoteRepository.upsert(voteDO)`.
- `VotePositionRepository.replaceAll(voteId, positions)`.
- Combined into a single `ConnectionIO`, `.transact(xa)` once.

**Orphan risk acknowledged**: if T2 fails after T1 commits, the just-created placeholders become orphans (never referenced by any vote_position or bill foreign key). Bills-pipeline and members-pipeline's next scheduled runs naturally enrich those orphans (overwriting the stub fields with real data); the natural_key uniqueness constraint prevents duplicate placeholder creation on retry. No explicit sweeper needed for this plan. If orphan accumulation becomes a problem during Phase 6 validation, file a P6.H hotfix or a follow-up PR to add a periodic cleanup.

**Outside both transactions**: `stance_materialization_status` upsert AND `VoteRecordedEvent` publish. Per spec, these only fire if `New` or `Updated(positionsChanged = true)`. Event publish uses `RetryWrapper` for transient failures; exhausted retries log error (DB state is correct).

22. **Config defaults locked** (`application.conf` in P1.1; same values also become the PureConfig-derived defaults in the case classes). Values reflect the revised low-concurrency tuning per decision 21e. Per-client pacing is configured on the pipeline's `house` / `senate` sections; there is no `http.global-concurrency` knob — each HTTP client owns its own `rateLimitedClient` wrapper with its own `pageDelay` / `requestDelay`.
    ```hocon
    http {
      request-timeout = 30s
    }
    congress-api {
      api-key = ${?CONGRESS_GOV_API_KEY}
      base-url = "https://api.congress.gov/v3"
      page-size = 250
    }
    pipeline {
      congress-sessions = [
        { congress = 119, session = 1 }
      ]  # List of (congress, session) tuples; historic backfill adds more entries
      house {
        parallelism = 1
        page-delay = 2s
        lookback-days = 7
      }
      senate {
        parallelism = 1
        request-delay = 3s
      }
    }
    event-publisher {
      topic-name = "vote-events"
      source = "votes-pipeline"
      retry {
        max-attempts = 3
        initial-delay = 10ms
        multiplier = 2
        max-delay = 60s
      }
    }
    database {
      hikari {
        maximum-pool-size = 5  # tight pool so 7+ pipelines can coexist under AlloyDB max_connections
      }
    }
    ```
    `application-test.conf` overrides for deterministic tests: all parallelism = 1, all delays = 1ms, timeouts = 5s, `database.hikari.maximum-pool-size = 3`.

## Out of scope / Follow-ups

Explicitly not in this plan. Track in memory / backlog.

- **GCP compute deploy (Phase 7)**: Cloud Run Job + Cloud Scheduler terraform for votes-pipeline. Requires user greenlight after local validation stable.
- **`ProcessingResult` / `PipelineRunSummary` promotion** to ingestion-common if duplicated across pipelines post-audit.
- **`E2ETest` tag promotion** from bills-common to ingestion-common (currently cross-pipeline import).
- **HasPlaceholder typeclass unification** if a third placeholder scheme emerges (currently just bioguide + lis; extending now would be premature).
- **Per-client request observability**: optional Grafana gauge for in-flight counts of each wrapped http4s `Client[F]` via the existing `rateLimitedClient` semaphore — nice-to-have post-launch.
- **Votes-pipeline → user-facing API**: out of scope here; lives in Phase 4 of the master implementation plan (Component 11+).

## Verification (Done Criteria)

The votes pipeline is DONE (for THIS plan — GCP compute deploy deferred to Phase 7) when:

1. All 21 core PRs + all hotfix PRs (P6.H*) from the iteration loop merged, all CI green.
2. `sbt test` passes (unit for every class — REQUIRED, plus any class-level chosen).
3. `sbt dockerTestParallel` passes (includes component + functional + integration).
4. `sbt "testOnly -- -n com.repcheck.tags.E2ETest"` passes.
5. Codecov patch coverage ≥ 90% on every Phase 2 + Phase 3 + hotfix PR.
6. tf-repcheck-infra: P5.1 Pub/Sub topics + P5.4 IAM + SA applied to dev (zero-cost resources verified via gcloud).
7. **P6.2 loop exits**: all 9 DB validation queries (Q1–Q9) return expected shapes against seeded fixtures. Expected log patterns only (no standard exceptions, no unexplained ERROR entries). Pub/Sub event count matches Succeeded-with-emit count.
8. **P6.3 loop exits**: Ofelia fires ≥15 scheduled runs; Unchanged path suppresses re-writes; Updated path archives correctly; no overlapping runs or connection-pool exhaustion.
9. MEMORY.md updated: add a project memory noting votes-pipeline local-validation complete, pointing to this plan as reference (similar to `project_c3_implementation_plan.md`). Also note that GCP compute deploy (Phase 7) is the next plan to schedule.

**Note on iteration**: P6.2 and P6.3 are NOT single-pass milestones. They are loops. "Done" means the loop has exited because every expectation holds, not because the container started.

**Explicitly NOT required for this plan to be DONE**:
- GCP Cloud Run Job execution.
- GCP Cloud Scheduler configuration.
- Staging / prod promotion to GCP compute.
- Real live hits against Congress.gov / senate.gov from GCP IPs.

These are all deferred to Phase 7 per user direction.
