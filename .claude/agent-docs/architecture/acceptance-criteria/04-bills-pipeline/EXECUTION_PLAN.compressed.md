<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/04-bills-pipeline/EXECUTION_PLAN.md -->

# Component 4 Execution Plan — Rev 6 (APPROVED)

**Last updated:** 2026-04-11
**Status:** Phase 0 complete. Phase 1 complete. Phase 2 complete. Phase 3 next.

## Status Tracker

| Step | Description | Status | PR(s) | Notes |
|------|-------------|--------|-------|-------|
| 0.A | Fix g8 template gaps | MERGED | repcheck-g8 PRs | Build.sbt baseline, CI, scripts, .gitignore |
| 0.B.1 | shared-models: exception uniqueness CI check | MERGED | shared-models PR | check-exception-uniqueness.sh + GHA step |
| 0.B.2 | pipeline-models: exception uniqueness plugin | MERGED | pipeline-models PR #9 | 334/334 tests, 8 decls/0 throws |
| 0.B.3 | ingestion-common: exception uniqueness plugin | MERGED | ingestion-common PR #5 | 180/180 tests, 6 decls/0 throws |
| 0.C | Audit shared-models migrations | DONE | Multiple PRs (see sub-steps) | All 10 bill tables present, 5 discrepancies fixed |
| 0.C.1 | Scaffold repcheck-db-migrations repo | MERGED | db-migrations PR #1 | 3-subproject: changesets/runner/app |
| 0.C.1+ | Fix scoverage in published runner JAR | MERGED | db-migrations PR #2 | coverageEnabled removed from runner |
| 0.C.2 | Remove db-migrations from votr, use published JAR | MERGED | votr PR #99 | 40/40 tests pass, runner 0.1.2 |
| 0.C.2+ | Add GitHub Packages auth to votr CI | MERGED | votr PR #99 (commit 403675e) | GITHUB_TOKEN + packages:read |
| 0.C.3 | Verify C4 bill tables exist in migrations | DONE | N/A | All tables present in migrations 001-003 |
| 0.C.3+ | Column-level schema audit | MERGED | votr PR #100, db-migrations PR #4 | 5 fixes: PK refactor docs, history exclusions, Option fields, embedding migration |
| 0.C.3++ | Docker distroless fix for db-migrations | MERGED | db-migrations PR #3 | DockerPermissionStrategy.None + nonroot user |
| 0.D | Terraform: free-tier C4 resources | MERGED | tf-repcheck-infra PR #3 | $0/mo, 39 resources each, all 3 envs applied |
| 0.D+ | ~~AlloyDB Omni on GKE~~ | SKIPPED | — | GKE free tier per billing account, not per project (~$158/mo for 3 envs). Using local Docker AlloyDB Omni instead. |
| 0.E | Manual: CONGRESS_GOV_API_KEY repo secret | DONE | — | Secret set 2026-04-09; CODECOV_TOKEN also added 2026-04-10 |
| 1.0 | Scaffold repcheck-data-ingestion | MERGED | data-ingestion initial commits | 4 sub-projects + doc-generator, CI, scripts |
| 1a | Bill metadata repos (bills-common) | MERGED | data-ingestion PR #3 | 5 Doobie repos + archiver, AlloyDB Omni tests |
| 1b | Bill text version repo (bills-common) | MERGED | data-ingestion PR #3 | BillTextVersionRepository included in 1a PR |
| 1.2 | Congress.gov bill DTOs + client (bills-common) | MERGED | data-ingestion PR #2 | BillsApiClient + BillTextApiClient, WireMock tests |
| 2.1 | Change-detection + processor | MERGED | shared-models PR #14, data-ingestion PR #6 | ConnectionIO refactor (all 5 repos), BillMetadataProcessor, fetchCosponsors, MemberLookupRepository, CosponsorListResponseDTO |
| 2.2 | IOApp entry point | MERGED | data-ingestion PR #6 | BillMetadataPipelineApp + BillMetadataPipeline testable wiring, 130 tests pass |
| 2.3 | DO type audit alignment | MERGED | shared-models PRs #15–#18, pipeline-models PR #13, ingestion-common PRs #9–#10, data-ingestion PR #7 | String → BillType/Chamber/Party/UsState/FormatType enums, String → LocalDate/Instant dates, ConnectionIO merge conflict resolution |
| 3.1 | Scanner + publisher | NOT STARTED | — | BillTextAvailabilityChecker, Pub/Sub emitter |
| 4.1 | Subscriber + text fetch + persistence | NOT STARTED | — | Pub/Sub → fetch → persist → emit downstream |
| 5.1 | Ofelia local cron + docker-compose | NOT STARTED | — | AlloyDB Omni + Pub/Sub emulator + 3 Cloud Run images |
| 5.2 | End-to-end test + dev GCP smoke | NOT STARTED | — | E2ETest tag, dev Congress.gov key, WIF |

## Scope

Build `repcheck-data-ingestion` containing three Cloud Run Jobs plus internal `bills-common` SBT module:
- `bill-metadata-pipeline` — pulls bill metadata, cosponsors, subjects from Congress.gov → AlloyDB
- `bill-text-availability-checker` — scans bills missing text, emits Pub/Sub events
- `bill-text-pipeline` — Pub/Sub-triggered, fetches bill text versions → AlloyDB
- `bills-common` — shared Doobie repos, Circe codecs, Congress.gov DTOs, config loaders

## Guardrails (apply to every PR)

- Scala 3.4.1, curly-brace syntax, tagless final `F[_]`
- One flat unique exception per failure point; CI uniqueness check runs per repo
- `parEvalMap(parallelism)` streaming; `RetryWrapper[F]` for transient failures
- PureConfig auto-derivation; Congress.gov key from `application.conf` via `${?CONGRESS_GOV_API_KEY}`
- Codecov ≥ 90% patch coverage; testability refactor pattern for IOApps
- `Test / javaOptions += "-Dconfig.resource=application-test.conf"` **per pipeline project** (not `bills-common`)
- **Strict serial PR cadence** — every PR, including Phase 0 prerequisites, waits for explicit "go" before next agent dispatch
- g8 scaffolding via **coursier**, never `sbt new`; g8 stays single-project
- Every agent PR via `CreatePR` shell function

## Phase 0 — Foundation (prerequisites, strictly serial, wait-for-go between each)

### PR #0.A — Fix g8 template gaps (`repcheck-g8`) — MERGED
Audit template against C4 needs: build.sbt baseline (WartRemover + tpolecat + scalafix + scalafmt + Codecov + pre-push hook), CI workflow, `scripts/ci-functions.sh`, license/readme, `.gitignore`. Does not add multi-project support — that follows in C4 repo scaffold.

### PR #0.B.1 — Backfill exception-uniqueness CI check to `repcheck-shared-models` — MERGED
New script `scripts/check-exception-uniqueness.sh` + GitHub Actions step. Fails if two `class … extends Throwable` share a name across module. Wired into main CI workflow.

### PR #0.B.2 — Backfill exception-uniqueness CI check to `repcheck-pipeline-models` — MERGED
Same script, same wiring. 334/334 tests, 8 decls/0 throws.

### PR #0.B.3 — Backfill exception-uniqueness CI check to `repcheck-ingestion-common` — MERGED
Same script, same wiring. 180/180 tests, 6 decls/0 throws.

### PR #0.C — Audit shared-models migrations — DONE
Cross-reference migrations for every C4 table: `bills`, `bill_cosponsors`, `bill_subjects`, `bill_text_versions`, history archives. All 10 tables exist in migrations 001-003.

**Sub-steps:**
- **0.C.1**: Extract `db-migrations/` from votr → `repcheck-db-migrations` standalone repo — MERGED (db-migrations PR #1)
  - 3-subproject: `changesets/` (resources JAR), `runner/` (library + DockerPostgresSpec), `app/` (IOApp + Docker image)
  - Published to GitHub Packages: `com.repcheck %% repcheck-db-migrations-runner`, `com.repcheck % repcheck-db-migrations-changesets`
  - Docker: `ghcr.io/eligio-taveras/repcheck-db-migrations-runner`
- **0.C.1+**: Fix scoverage contamination in runner JAR — MERGED (db-migrations PR #2)
  - Removed `coverageEnabled := true` from runner; prepended `coverageOff clean` to release workflow
- **0.C.2**: Remove `db-migrations/` from votr, rewire onto published JAR — MERGED (votr PR #99)
- **0.C.2+**: Add GitHub Packages auth to votr CI — MERGED (votr PR #99, commit 403675e)
  - `permissions: packages: read` + `env: GITHUB_TOKEN`
- **0.C.3**: Verify C4 bill tables exist + column-level schema audit — DONE
  - All 10 tables in migrations 001-003
  - 5 discrepancies fixed (votr PR #100, db-migrations PR #4):
    1. BillDO PK: `billId` → `id: Long` surrogate + `naturalKey: String`
    2. BillHistoryDO: text_content, text_embedding, latest_text_version_id excluded (documented)
    3. BillTextVersionDO: `versionType`, `versionDate`, `formatType`, `url` → `Option[T]` (single-phase creation)
    4. BillSubjectHistoryDO: added `embedding: Option[Array[Float]]` (migration 010)
    5. Docs updated
- **0.C.3++**: Docker distroless fix for db-migrations — MERGED (db-migrations PR #3)
  - `DockerPermissionStrategy.None` + `daemonUser := "nonroot"`

### PR #0.D — Terraform: C4 GCP resources (`tf-repcheck-infra`) — MERGED
PR #3: trim to free-tier resources, $0/mo, all 3 envs identical.
- **Removed**: AlloyDB cluster, Cloud SQL, all Cloud Run Jobs, Atlantis, scheduling, non-bill Pub/Sub topics, LLM secrets, VPC Access Connector
- **Kept**: API enablements, VPC/Subnet/PSA, deployer+pipeline+scheduler SAs, WIF, GCS buckets, bill-events topic+dead-letter+subscription, congress-api-key secret, Artifact Registry, observability

### ~~PR #0.D+ — AlloyDB Omni on GKE~~ — SKIPPED
GKE free tier per billing account (1 zonal cluster + e2-micro), not per project. 3 envs cost ~$158/mo. Use local Docker AlloyDB Omni instead. Revisit at production deployment.

### Manual step #0.E — GitHub Actions secret propagation — DONE
`CONGRESS_GOV_API_KEY` repo secret (2026-04-09). `CODECOV_TOKEN` also added (2026-04-10).

## Phase 1 — Scaffold + persistence

### PR #1.0 — Scaffold `repcheck-data-ingestion` — MERGED
Coursier + g8; convert to multi-project:
```
repcheck-data-ingestion/
  bills-common/
  bill-metadata-pipeline/
  bill-text-availability-checker/
  bill-text-pipeline/
```
Root `build.sbt` wires deps on shared-models, pipeline-models, ingestion-common from GitHub Packages. Each pipeline project: `Test / javaOptions += "-Dconfig.resource=application-test.conf"`. Bills-common does not. Pre-push hook + ci-functions sourced. CI includes exception-uniqueness + Codecov (unit/integration/e2e tags). `SharedDockerPostgres` trait in testkit folder under bills-common (Test scope).

### PR #1a — Bill metadata repos (in `bills-common`) — MERGED (data-ingestion PR #3)
`BillRepository`, `BillCosponsorRepository`, `BillSubjectRepository` + `BillHistoryArchiver` (archive-before-overwrite). Doobie auto-derived `Read`/`Write`. Unique exceptions per failure. Integration tests vs published db-migrations (AlloyDB Omni). ≥ 90% patch coverage.

### PR #1b — Bill text version repo (in `bills-common`) — MERGED (data-ingestion PR #3)
`BillTextVersionRepository` + `DoobieBillTextVersionRepository` with `storeAndUpdateBill` compound op. Included in 1a PR.

### PR #1.2 — Congress.gov bill DTOs and client (in `bills-common`) — MERGED (data-ingestion PR #2)
`BillsApiClient` + `BillTextApiClient` on http4s Ember with retry and pagination. WireMock unit tests (pagination, 4xx/5xx, retries). Unique error types (`BillFetchFailed`, `BillTextCheckFailed`).

## Phase 2 — bill-metadata-pipeline

### PR #2.1 — Change-detection + processor — MERGED (shared-models PR #14, data-ingestion PR #6)
**Prerequisite:** `CosponsorListResponseDTO` + `PaginationInfoDTO` `next` field fallback fix added to shared-models (PR #14). 803 tests pass. Published locally as snapshot.

**ConnectionIO refactoring:** All 5 Doobie repos refactored from `F[_]` with `.transact(xa)` baked in to returning `ConnectionIO`. Callers control boundaries. `upsert` returns `ConnectionIO[Long]` via `RETURNING id`.

**New components:**
- `MemberLookupRepository` + `DoobieMemberLookupRepository` — `bioguideId → Long` member ID resolution
- `BillsApiClient.fetchCosponsors` — paginated via `/bill/{congress}/{type}/{number}/cosponsors`
- `BillMetadataProcessor[F]` — pipeline: date-comparison change detection, bill detail fetch, DTO→DO conversion, cosponsor fetching with PlaceholderCreator for member forward refs, atomic `ConnectionIO` transaction (archive + upsert + cosponsor/subject replace)
- `BillMetadataConfig`, `BillProcessingFailed` error

Unit tests with MockitoScala + `StubPlaceholderCreator` (Scala 3 `using` workaround). 58 tests pass.

### PR #2.2 — IOApp entry point — MERGED (data-ingestion PR #6)
`BillMetadataPipelineApp` extends `IOApp.Simple` (thin wrapper, coverage-excluded). `BillMetadataPipeline` as `private[app]` testable wiring:
- `AppConfig` with PureConfig derivation (database, congressApi, pipeline sections)
- Custom `Write[MemberDO]` omits auto-increment PK
- `run[F]`: loads config, builds Resources (Ember client, DB transactor), wires repos + processor
- `execute[F]`: takes `Stream[F, ProcessingResult]` + logger, collects results, returns ExitCode

9 tests covering ExitCode.Success/Error, log messages, correlation ID, error propagation. 130 total tests pass.

### PR #2.3 — DO type audit alignment — MERGED (shared-models PRs #15–#18, pipeline-models PR #13, ingestion-common PRs #9–#10, data-ingestion PR #7)
Cross-repo audit replacing raw strings with proper types:
- **shared-models PRs #15–#18:** `MemberDO` typed fields (Party, UsState, Int birthYear, Instant dates); all bill DOs (BillType, Chamber, FormatType, TextVersionCode enums; LocalDate/Instant); `DoobieEnumInstances` for Doobie `Get`/`Put`; `UsState.Put` writes `.code`; `Chamber.Joint` added
- **pipeline-models PR #13:** bump shared-models to 0.1.15
- **ingestion-common PRs #9–#10:** bump to 0.1.15, fix DO mismatches, bind WireMock to 127.0.0.1
- **data-ingestion PR #7:** merge ConnectionIO repo refactor + DO type changes; `text_date::date` / `version_date::date` casts for TIMESTAMPTZ→LocalDate; `Write[MemberDO]` updated for Party/UsState/Int; unit specs rewritten for parameterless ConnectionIO repos; test fixtures updated for enum types. 78 unit + 43 docker tests pass.

## Phase 3 — bill-text-availability-checker

### PR #3.1 — Scanner + publisher
`BillTextAvailabilityChecker[F]` streams bills where no `BillTextVersion` exists for latest congress/bill. Emits `BillTextAvailabilityRequested` events (defined in pipeline-models). IOApp with testability refactor. Integration test vs AlloyDB Omni + Pub/Sub emulator.

## Phase 4 — bill-text-pipeline

### PR #4.1 — Subscriber + text fetch + persistence
Subscribes to `bill-text-availability-requested`, fetches bill text via `CongressBillApiClient` (text endpoints), persists `BillTextVersion` rows through `BillTextVersionRepository`. Archive-before-overwrite on update. Emits `BillTextReadyForIngest` downstream. IOApp testability pattern; integration test with Pub/Sub emulator + Omni + WireMock.

## Phase 5 — Local cron + end-to-end wiring

### PR #5.1 — Ofelia local cron + docker-compose
`docker-compose.local.yml`:
- AlloyDB Omni (Postgres wire)
- `mcuadros/ofelia:v0.3` (pinned) cron container with labels running three Cloud Run Job images
- Pub/Sub emulator
- Volume mount for `gcloud` ADC

Schedules overridable by env vars (defaults):
- `BILL_METADATA_SCHEDULE` (`@every 6h`)
- `BILL_TEXT_AVAILABILITY_SCHEDULE` (`@every 3h`)
- `BILL_TEXT_PIPELINE_SCHEDULE` (`@every 1h`)

Distroless Dockerfiles per app. README: "Run the full pipeline locally."

### PR #5.2 — End-to-end test + dev GCP smoke
Tagged `E2ETest` module running three apps vs dev Pub/Sub + Omni, real (dev) Congress.gov key from Secret Manager, impersonate dev SAs via WIF. Excluded from `sbt test`; run via `sbt "testOnly -- -n com.repcheck.tags.E2ETest"`. Documented in PR body: executed or deferred to manual run (Docker/WIF dependent).

## Cross-Component Dependencies

| From | What | Used by |
|------|------|---------|
| Component 1 (shared-models) | BillDO, BillCosponsorDO, BillSubjectDO, BillTextVersionDO, TextVersionCode | All C4 projects |
| Component 2 (pipeline-models) | Tables constants, EventTypes, RetryConfig, ErrorClassifier, PipelineEvent | All C4 projects |
| Component 3 (ingestion-common) | CongressGovPaginatedClient, ChangeDetector, PlaceholderCreator, EventPublisher, WorkflowStateUpdater, PipelineBootstrap | All C4 projects |
| repcheck-db-migrations | DockerPostgresSpec (test infra) | All C4 test suites |

## Dependency Graph

```
Phase 0: Foundation
  0.A (g8 template) ─────────────────────── MERGED
  0.B.1 (shared-models uniqueness) ────────── MERGED
  0.B.2 (pipeline-models uniqueness) ──────── MERGED
  0.B.3 (ingestion-common uniqueness) ─────── MERGED
  0.C (db-migrations audit) ───────────────── DONE (all sub-steps merged)
  0.D (Terraform free-tier) ───────────────── MERGED (all 3 envs applied)
  0.D+ (AlloyDB Omni on GKE) ─────────────── SKIPPED (cost ~$158/mo)
  0.E (API key secret) ───────────────────── DONE (2026-04-09)

Phase 1: Scaffold + persistence
  1.0 (scaffold) ────────────────────────── MERGED
  1a (bill repos) ────────────────────────── MERGED (PR #3)
  1b (text repo) ─────────────────────────── MERGED (PR #3)
  1.2 (DTOs + client) ────────────────────── MERGED (PR #2)

Phase 2: bill-metadata-pipeline       ← COMPLETE
  2.1 (processor) ─────────────────────── MERGED (shared-models #14, data-ingestion #6)
  2.2 (IOApp) ─────────────────────────── MERGED (data-ingestion #6)
  2.3 (DO type audit) ───────────────── MERGED (shared-models #15–#18, data-ingestion #7)

Phase 3: bill-text-availability-checker  ← NEXT
  3.1 (scanner + publisher)

Phase 4: bill-text-pipeline
  4.1 (subscriber + fetch + persist)

Phase 5: Local cron + E2E
  5.1 (docker-compose) → 5.2 (E2E test)
```

## Rev 6 changes vs Rev 5
1. Strict serial cadence **explicit for Phase 0** — every PR waits for "go" before next agent dispatch
2. Manual step #0.E **blocks Phase 1 entirely**
3. Plan approved once; proceed sequentially, wait for "go" between PRs