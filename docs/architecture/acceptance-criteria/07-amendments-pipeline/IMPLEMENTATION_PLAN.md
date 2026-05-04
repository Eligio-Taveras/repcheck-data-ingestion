# Amendments Pipeline — Implementation Plan

> Step-by-step execution order, runtime flow, and cross-application interactions for **amendment ingestion only** (§7.1–§7.6). Amendment analysis (§10.11), bill-side analysis refactor (§10.6/§10.7), and amendment scoring (§11.12) are **deferred to separate plans** that follow Component 10 + 11 deliverables — they're not in this scope.

This file is the **execution-ordered companion** to the area files. The area files are the spec; this file is the order-of-operations + interaction map.

**Scope boundary:** Everything below ends at "amendment data ingested into the DB." Whether a downstream pipeline reads `amendment_text_versions WHERE fetched_at IS NOT NULL` to begin analysis is the next plan's concern.

---

## Part 1 — Implementation phases (sequenced)

Each phase produces a shippable, testable deliverable. Don't start phase N+1 until N is published / merged unless explicitly noted as parallel.

### Phase 0 — Upstream artifact bumps (PRECONDITION, sequential within this phase)

| Step | Artifact | Source repo | What | Tracking |
|---|---|---|---|---|
| ~~0.1~~ | ~~`members-common`~~ | — | **REMOVED** — §7.3 reuses the existing shared `PlaceholderCreator.ensureExists[MemberDO]` pattern + `MemberRepository.findByBioguideId`. No new method on `MemberRepository`. See [P7.8](PRODUCTION_TASKS.md#p78--removed). | — |
| 0.2 | `repcheck-shared-models 0.1.39 → 0.1.40` | repcheck-shared-models repo | Field additions: `AmendmentDO` (+5 fields per S2 simplification — dropped `textVersionsCount`): `parentAmendmentId`, `effectiveBillId`, `proposedDate`, `latestActionTime`, `lastTextCheckAt`. **`chamber` tightened to `chamber_enum NOT NULL`** (per L9 — always derivable from `amendmentType`). New `LegislationRef` sealed type. New DTOs: `AmendmentTextItemDTO`, `AmendmentFormatDTO`, `AmendmentTextResponseDTO` (`date: Option[String]`, parsed downstream). New `AmendmentDetailDTO.toDO(billId, sponsorMemberId, parentAmendmentId)` overload. **No analysis types** — `AnalysisComplexity` / `FindingImpact` / `*ComplexityScoreDO` / `*FindingDO` field additions belong to the deferred analysis plan, not this one. | [P7.0](PRODUCTION_TASKS.md#p70) |
| 0.3 | `repcheck-pipeline-models 0.1.21 → 0.1.22` | repcheck-pipeline-models repo | New event: `AmendmentTextAvailableEvent` (output of availability checker — same pattern as `BillTextAvailableEvent`). **No `AmendmentTextIngestedEvent`** — completion is signaled by `amendment_text_versions.fetched_at IS NOT NULL`, mirroring the bill-side pattern. New `EventTypes.*` constants for the one new event. New `Tables.*` constants for `amendments`, `amendment_text_versions`, `amendment_text_chunks`. New `Constants.MinAmendmentCongress = 102`. | [P7.5](PRODUCTION_TASKS.md#p75) |
| 0.4 | `repcheck-db-migrations 0.1.x → 0.1.x+N` | repcheck-db-migrations repo | Schema additions for ingestion only: `amendments` table (with `chamber chamber_enum NOT NULL` per L9), `amendment_text_versions` (with two partial indexes for `fetched_at IS NULL` / `IS NOT NULL`), `amendment_text_chunks` (embedding `vector(1024)` to match bill-side qwen3-embedding:0.6b output; **HNSW index** per P4). New enums: `amendment_format_type`, **`amendment_text_version_code_type`** (per L3 — dedicated enum for amendment text versions, NOT a co-mingled extension of bill-side `text_version_code_type`). Enum extensions: `legislation_type_enum` (+HAMDT/SAMDT/SUAMDT — needed for §7.4 votes integration), `vote_weight_type` (+AMENDMENT_SUBSTANTIVE/AMENDMENT_PROCEDURAL — written by §7.4 even though scoring won't use them yet). **Analysis/scoring tables (`amendment_findings`, `amendment_complexity_scores`, `bill_complexity_scores`, `member_amendment_stances*`, `finding_impact_weights`, etc.) are deferred to the analysis/scoring plans.** | [P7.6](PRODUCTION_TASKS.md#p76) |
| 0.5 | `ingestion-common` | this repo's `ingestion-common` subproject | Add `transientNetworkAware[E <: HttpStatusError](base: HttpStatusErrorClassifier[E])` helper that wraps a status-code classifier with the cause-chain walk (per S7 — replaces the four near-identical copies of the walk that would otherwise land in `BillSummariesApi`/`AmendmentsApi`/`AmendmentTextCheck`/`AmendmentTextDownload` classifiers). | [P7.11](PRODUCTION_TASKS.md#p711) |

**Done when**: all artifacts published; `build.sbt` pins updated in this repo; `sbt compile` succeeds.

### Phase 1 — `text-extraction-common` refactor (parallel — pure refactor in this repo)

Lift the bills-pipeline text infrastructure into a shared module so amendment-text-pipeline can reuse it byte-for-byte (per directive: "Chunking and text pipeline should follow implementation strategy used for bills text pipeline").

| Step | Action | Tracking |
|---|---|---|
| 1.1 | Create new SBT subproject `text-extraction-common`. | [P7.4](PRODUCTION_TASKS.md#p74) |
| 1.2 | Move from `bill-text-pipeline` into the new module: `HtmlStreamExtractor`, `PdfStreamExtractor`, `XmlStreamExtractor`, `PlainTextStreamExtractor`, `BillTextChunker` (rename to `TextChunker` — generic), `OllamaEmbeddingService`, `CrossBillEmbedder` (rename to `CrossEntityEmbedder` or keep generic-typed via `[F[_], A]`). | |
| 1.3 | Add common base trait `HtmlStreamExtractorBase[F]` with `shouldKeepNode` + `transformText` overrides — bill-side passes through, amendment-side overrides for CREC running headers/footers (per §7.6 Q18). | |
| 1.4 | Generalize chunk-row persistence shape: chunker emits `Stream[F, String]`; downstream subproject wraps each chunk into its own row DO. No DB-table-aware code in this module. | |
| 1.5 | Update `bill-text-pipeline` to depend on `text-extraction-common`. Existing tests pass unchanged. | |

**Done when**: separate PR merged. Bill text pipeline behavior unchanged. Module ready for §7.6 to consume.

### Phase 2 — Amendments-pipeline core (§7.1, §7.2, §7.3)

Sequence: API client → repository → processor → IOApp wiring.

| Step | Class / File | Notes |
|---|---|---|
| 2.1 | `AmendmentsConfig` (PureConfig case class) | `congressesMin: Int = 102, congressesMax: Int = 119` (per S3 — bounded range, not arbitrary list); `congresses: Range = congressesMin to congressesMax` derived. `require(congressesMin >= 102)` + `require(congressesMax >= congressesMin)`. `lookbackDays: Int = 7`. `parallelism: Int = 4` (steady-state) / `1` (backfill, per P7). `maxRecursionDepth: Int = 10`. |
| 2.2 | `AmendmentsApiErrorClassifier` | Wraps the shared `transientNetworkAware` helper from `ingestion-common` (per S7 — no copy-pasted cause-chain walk). New `AmendmentFetchFailed` + `AmendmentsApiHttpError`. |
| 2.3 | `AmendmentsApiClient[F]` extends `CongressGovPaginatedClient[F, AmendmentListItemDTO]` | Per-congress iteration via `/amendment/{c}?fromDateTime=now-{config.lookbackDays}d` (Q8). `fetchDetail(url): F[AmendmentDetailDTO]`. URL casing rules: path lowercase, query uppercase. |
| 2.4 | `AmendmentRepository[F]` trait + `DoobieAmendmentRepository` | Methods: `upsert`, **`upsertPlaceholder(naturalKey): F[Unit]` — `ON CONFLICT (natural_key) DO NOTHING` mirroring [`BillRepository.upsertPlaceholder`](../../../../bills-common/src/main/scala/repcheck/ingestion/bills/common/persistence/BillRepository.scala). Caller does `findByNaturalKey` separately to get the surrogate id (2-step pattern matches votes-pipeline's [`BillLookup`](../../../../votes-pipeline/src/main/scala/repcheck/ingestion/votes/pipeline/BillLookup.scala))**, `findById`, `findByNaturalKey`, **`findByNaturalKeys(keys): F[Map[String, AmendmentDO]]` (per P2 — page-batch helper)**, `findByBillId`, `findByEffectiveBillId`, `findByCongress`, `findByParentAmendmentId`, `updateEffectiveBillId`, `findCandidatesForTextCheck` (parameterized `staleAfter`), `updateLastTextCheckAt` (per L1). **No `computeEffectiveBillId`** (per S8 — processor computes inline). **No `failureCount` / `lastFailureReason` / `resolveUnresolvedSubAmendments`** — there's no "stuck amendment" concept and no end-of-run sweep. New errors: `AmendmentUpsertFailed`, `InvalidAmendmentNaturalKey`. |
| 2.5 | `AmendmentProcessor[F]` | `streamAll(runId): Stream[F, ProcessingResult]` iterates `config.congresses`. **Per-page batch** (per P2): `findByNaturalKeys(allKeysOnPage)` → 1 SELECT per page. Per-amendment: inline parent recursion (depth-bounded, **no cycle guard** per S1) → resolve sponsor + bill placeholders → recurse parents (correlationId propagated through frames) → DTO→DO via overload → upsert with `effective_bill_id` already set inline. **No end-of-run sweep.** NO event emission. |
| 2.6 | `AmendmentPipelineApp` (IOApp.Simple) | Pure wiring. **`maximumPoolSize = parallelism × maxRecursionDepth + 5 = 45`** in HikariCP config (per P1 — recursion can hold deep connection counts during cold-chain hydration). `PipelineBootstrap`, transactor, HTTP client with `pageDelay` rate-limited semaphore, processor, run, summarize, `WorkflowStateUpdater.recordStepCompleted`. |
| 2.7 | Tests | Unit (per-class) + WireMock (HTTP simulation) + DockerRequired integration (AlloyDB Omni). Acceptance criteria from §7.1, §7.2, §7.3 — including the new test rows for: pagination boundaries, congresses validation, page-batch SELECT (P2), single-roundtrip upsertPlaceholder (S5), parallel-recursion under shared parent (L2), connection-pool sizing negative test (P1), correlationId propagation through recursion frames. |

**Inline parent recursion contract (§7.3 detail):**

Given `processAmendment(naturalKey: String, listItemOpt: Option[AmendmentListItemDTO], storedOpt: Option[AmendmentDO], depth: Int, correlationId: UUID)`:

1. **Depth guard (per S1 — only safety net needed):** if `depth > config.maxRecursionDepth` (default 10) → log + raise `AmendmentRecursionTooDeep`. **No cycle guard** — Congress.gov can't structurally produce parent cycles; depth bound covers any corrupt-data edge cases.
2. **Idempotency check:** Using `storedOpt` (passed in by caller, batch-fetched per P2): if `storedOpt.exists(_.updateDate.isDefined)` AND incoming `listItemOpt`'s `updateDate <= stored.updateDate` → `Skipped("unchanged")`.
3. **Fetch detail:** `apiClient.fetchDetail(naturalKey) → AmendmentDetailDTO`.
4. **Resolve sponsor placeholder** (mirrors `MemberResolver.ensureSponsorPlaceholder` in bill-metadata-pipeline):
   - `placeholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)` — idempotent INSERT via `MemberInsertSql` (`ON CONFLICT (natural_key) DO NOTHING`).
   - `memberRepo.findByBioguideId(bioguideId).map(_.map(_.memberId))` — read surrogate id.
   - Yields `sponsorMemberId: Option[Long]` — `None` if the bioguide is missing from the DTO, otherwise `Some(id)`.
5. **Resolve bill placeholder** (mirrors `BillLookup.forContext` in votes-pipeline) when `detail.amendedBill.isDefined`:
   - `billRepo.upsertPlaceholder(billNaturalKey).transact(xa)` — `F[Unit]`, idempotent (`ON CONFLICT (congress, bill_type, number) DO NOTHING`). Already exists in [bills-common](../../../../bills-common/src/main/scala/repcheck/ingestion/bills/common/persistence/BillRepository.scala).
   - `billRepo.findByBillId(billNaturalKey).map(_.map(_.billId)).transact(xa)` — read surrogate id.
   - Yields `billId: Option[Long]`. May be `None` for amendment-amends-amendment chains where the leaf amendment refs no bill directly.
6. **Resolve parent — RECURSIVE:** if `detail.amendedAmendment.isDefined`:
   - Compute `parentNaturalKey` from the amended-amendment ref.
   - `parentExisting ← amendmentRepo.findByNaturalKey(parentNaturalKey)`.
   - **If `parentExisting.exists(_.updateDate.isDefined)`** → parent is fully hydrated; use its surrogate id as `parentAmendmentId` and read its `effective_bill_id` cache.
   - **Else** → call `processAmendment(parentNaturalKey, listItemOpt = None, storedOpt = parentExisting, depth + 1, correlationId)` recursively (**same `correlationId`** — child + parent share log context). Recursion drains the entire parent chain to the bill before returning. After it returns, re-`findByNaturalKey(parentNaturalKey)` to read the now-hydrated parent's `id` and `effective_bill_id`.
7. **Compute `effectiveBillId` inline (per S8 — no `computeEffectiveBillId` repo round-trip):** `billId.orElse(parentEffectiveBillId)`. Else `None` (orphan amendment — legitimate for some procedural / treaty amendments; per L8, downstream scoring/analysis pipelines querying via `findByEffectiveBillId` will skip these).
8. **DTO→DO** via `detail.toDO(billId, sponsorMemberId, parentAmendmentId)`.
9. **Upsert** (single transaction): write the row with `effective_bill_id` already set. Return surrogate id.

The recursion guarantees that by the time any row gets persisted, its full parent chain is also persisted with `effective_bill_id` populated. **No separate `resolveUnresolvedSubAmendments` end-of-run sweep is needed** — the data is consistent on every commit.

> **Per L2 — known wasted-call under parallel processing.** When two amendments A and B share parent C and are processed in parallel, both can independently fetch C's detail before either commits. `ON CONFLICT` upsert prevents duplicate rows but C is fetched from Congress.gov twice. Document the wasted-call rate via the `congress_gov_detail_fetches_total{cause="recursion_redundant"}` counter (see §7.3 acceptance). Mitigation by per-page parent pre-resolution (per P6) is available as future work if backfill cost ever requires it.

**Done when**: `sbt amendmentsPipeline/test` green. Manual local run against Congress.gov populates `amendments` table for one congress.

### Phase 3 — Votes-pipeline amendment integration (§7.4)

Depends on Phase 2 (votes-pipeline needs `AmendmentRepository`).

| Step | Action |
|---|---|
| 3.1 | Add `votesPipeline.dependsOn(amendmentsPipeline)` in `build.sbt`. |
| 3.2 | Extend `SenateVoteXmlDecoder` + `SenateVoteDocumentDTO`: new top-level fields `amendmentNumber`, `amendmentToDocumentNumber`, `amendmentToDocumentShortTitle` (per Q20). |
| 3.3 | Update `SenateVoteConverter.normalizeDocumentType` to return `Either[NonBillOrUnknown, LegislationRef]`. Add cases for `"S.Amdt."` → `Amendment(SAMDT)` and `"H.Amdt."` → `Amendment(HAMDT)`. NOT `S.U.Amdt.` (out of scope per Q9). |
| 3.4 | Update `SenateVoteConverter.classifyDocument` to dispatch on `LegislationRef.Bill` vs `Amendment`. For amendment branch: extract amendment number via regex from `<amendment_number>`, build natural key `{congress}-{type}-{number}`, **skip placeholder if congress < 102** (Q9), call `amendmentRepo.upsertPlaceholder(nk).transact(xa)` directly (no wrapper), populate `legislation_type` + `legislation_number` on VoteDO with `bill_id = None`. Construct parent-bill placeholder from `<amendment_to_document_number>` if needed. |
| 3.5 | Update `VoteProcessor.processHouseVote` for House dispatch on amendment-typed `legislationType` (HAMDT/SAMDT/SUAMDT). Skip placeholder if congress < 102. |
| 3.6 | Wiring: `VotesPipelineResources` constructs `DoobieAmendmentRepository`. `VotesProcessorFactory` injects `amendmentRepository` into `VoteProcessor` and `SenateVoteConverter`. |
| 3.7 | Tests covering Senate XML amendment-vote decoding, House dispatch, pre-102 skip, placeholder creation, all per §7.4 acceptance criteria. |

**Done when**: `sbt votesPipeline/test` green. Senate amendment-vote fixture decodes correctly; placeholder amendment row appears in DB after vote ingestion.

### Phase 4 — Amendment text availability checker (§7.5)

Depends on Phase 0 + Phase 2.

Same pattern as `bill-text-availability-checker`: scheduled job polls DB for candidate amendments needing text, fetches text-version metadata from Congress.gov, **emits `AmendmentTextAvailableEvent` to Pub/Sub** for each new (versionTypeCode, formatType) it finds. The checker writes only the audit fields (`last_text_check_at`, `last_text_check_count`) on the `amendments` row — it does NOT write to `amendment_text_versions`.

| Step | Action |
|---|---|
| 4.1 | New SBT subproject `amendment-text-availability-checker`. Depends on `amendments-pipeline` (for `AmendmentRepository`). |
| 4.2 | `AmendmentTextApiClient` — extends `CongressGovPaginatedClient[F, AmendmentTextItemDTO]`. `fetchTextVersions(congress, type, number)`. |
| 4.3 | `AmendmentTextVersionSelector.selectAllNewVersions(versions, existing)` per Q8 — emits one event per new (versionTypeCode, formatType) tuple. |
| 4.4 | `AmendmentTextAvailabilityChecker[F]` — cron-driven (per Q10). `findCandidatesForTextCheck(minCongress=117, staleAfter)` then per-amendment processing. Per-amendment correlation IDs. |
| 4.5 | `AmendmentTextCheckerApp` (IOApp) — one-shot Cloud Run Job. Loads config, builds resources, runs stream, exits. |
| 4.6 | New errors: `AmendmentTextCheckFailed`, `AmendmentTextCheckHttpError`, `AmendmentTextCheckErrorClassifier` (per Q10 — distinct from §7.1's). |
| 4.7 | Pub/Sub publisher emits `AmendmentTextAvailableEvent` on `amendment.text.available` topic. |
| 4.8 | Tests + WireMock fixtures for `/amendment/.../text` endpoint. |

**Done when**: container builds. Live run discovers amendments with text and emits events to Pub/Sub emulator.

### Phase 5 — Amendment text pipeline (§7.6)

Depends on Phase 0 (GOVINFO_API_KEY) + Phase 1 (text-extraction-common) + Phase 4 (events to consume).

**Implementation strategy mirrors bill-text-pipeline byte-for-byte.** The processor uses the same Ollama HTTP embedder (`qwen3-embedding:0.6b`, 1024-dim output) and the same chunking object (`TextChunker` — character-based, `maxChunkChars=12000`, no overlap, format-agnostic) extracted to `text-extraction-common` in Phase 1. **No event is emitted on completion** — readiness is advertised by `amendment_text_versions.fetched_at IS NOT NULL`, paralleling the bill-side pattern.

| Step | Action |
|---|---|
| 5.1 | New SBT subproject `amendment-text-pipeline`. Depends on `amendments-pipeline` + `text-extraction-common`. |
| 5.2 | `CrecGovInfoUrlRewriter` — verified rewriter from www.congress.gov CREC URL → api.govinfo.gov. Pure object. |
| 5.3 | `CrecHtmlExtractor` extends `HtmlStreamExtractorBase` (per Q18, Q45, Q46) — UTF-8 only, decode HTML entities, drop CREC running headers/footers/page markers, keep speaker tags + section markers. |
| 5.4 | `AmendmentTextDownloader[F]` — uses rewriter, GOVINFO_API_KEY in query param, retry-wrapped with `AmendmentTextDownloadErrorClassifier`. NACK on 503 (Pub/Sub redelivers per Q32). |
| 5.5 | `AmendmentTextProcessor[F]` — Pub/Sub subscriber. **Single-statement upsert per S4** combines isAlreadyProcessed + INSERT into one statement (`INSERT...ON CONFLICT (amendment_id, version_type_code, format_type) DO UPDATE...WHERE EXCLUDED.published_date > stored.published_date OR stored.fetched_at IS NULL RETURNING id, alreadyComplete`). **Per L6 re-submission semantics**: when upstream republishes with newer `published_date`, the ON CONFLICT clause refreshes the row (resets `fetched_at=NULL`); orphan chunks deleted; re-streamed; new `text_length` and `fetched_at` written. Then: stream download via rewriter → `CrecHtmlExtractor` or `PdfStreamExtractor` → `TextChunker.chunkPipe(maxChunkChars=12000)` → `OllamaEmbeddingService` (model `bill-text-embedding`, 1024-dim) **with bounded queue per P8 (`OLLAMA_EMBED_QUEUE_BYTES = 1.2MB`)** → batched per-chunk INSERT into `amendment_text_chunks` → UPDATE `amendment_text_versions SET fetched_at = NOW(), text_length = ?`. **No event emission on completion** — `fetched_at IS NOT NULL` is the readiness signal for downstream pipelines. |
| 5.6 | `AmendmentTextVersionRepository` (`markFetched(versionId, ts)` analogous to bill side) + `AmendmentTextChunkRepository` (Doobie). Schema includes **HNSW index per P4** on `amendment_text_chunks.embedding`. **Two partial indexes** on `amendment_text_versions(amendment_id) WHERE fetched_at IS NULL` and `WHERE fetched_at IS NOT NULL` — supports both the in-flight diagnostics view and downstream "ready to consume" polling. |
| 5.7 | `AmendmentTextPipelineApp` — Cloud Run Service (long-running subscriber). |
| 5.8 | Tests + WireMock fixtures for api.govinfo.gov CREC granule HEAD + GET. |

**Done when**: container builds. Subscribed event triggers download → chunks land in `amendment_text_chunks` with 1024-dim embeddings → `amendment_text_versions.fetched_at` populated.

### Phase 6 — Cross-pipeline E2E (§7.7)

Depends on Phases 1–5.

| Step | Action |
|---|---|
| 6.1 | `docker-compose.local.yml` additions for `amendments-pipeline`, `amendment-text-availability-checker`, `amendment-text-pipeline`. |
| 6.2 | `pubsub-init.sh` additions: 1 new topic (`amendment.text.available`) + its subscription. |
| 6.3 | `ofelia-config.ini` cron entries on **common 4h cadence** (per L10): amendments-pipeline at minute 30 (`30 */4 * * *`) and amendment-text-availability-checker at minute 50 (`50 */4 * * *`). 20-minute offset gives amendments-pipeline time to complete before the checker scans. |
| 6.4 | `docker-compose.e2e.yml` additions with WireMock URL overrides. |
| 6.5 | WireMock fixtures + `AmendmentChainFixtures` programmatic helper (Q12). |
| 6.6 | E2E spec `AmendmentsCrossPipelineSpec` in `docker-compose-e2e/src/test`. Ingestion-only scenarios: S1 chain happy path (bill → amendment → sub-amendment, all hydrate via inline recursion); S2 placeholder upgrade when votes-pipeline writes a placeholder before amendments-pipeline runs; S3 fan-out (one bill, multiple amendments); S4 text ingestion (event consumed → chunks persist → `fetched_at` set); S5 failure modes (API 5xx retries, malformed amendment skipped); S6 backwards-compat regression (bill-text-pipeline unchanged after `text-extraction-common` refactor). |
| 6.7 | `dockerComposeE2e/test` builds all relevant fat jars, runs full stack. |

**Done when**: `sbt 'set dockerComposeE2e/Test/testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, "-n", "DockerRequired"))' 'dockerComposeE2e/test'` passes.

### Phase 7 — Local backfill + production cutover

Depends on all prior. **Local-first per Q22, sequential per-congress per Q17.**

| Step | Action |
|---|---|
| 7.1 | Locally deploy via `docker-compose.local.yml`. Pause other Congress.gov consumers. |
| 7.2 | Run amendments-pipeline once per congress (102, 103, ..., 119) sequentially with `LOOKBACK_DAYS=999999, PAGE_DELAY=750ms`. ~24-30h wall-clock. |
| 7.3 | Run amendment-text-availability-checker once. Run amendment-text-pipeline locally to drain text events. |
| 7.4 | Sanity-check counts: `amendments`, `amendment_text_versions WHERE fetched_at IS NOT NULL`, `amendment_text_chunks` (chunks-per-version distribution). |
| 7.5 | `pg_dump` local AlloyDB Omni. Validate schema parity with production. |
| 7.6 | `pg_restore` into production AlloyDB. |
| 7.7 | Deploy production amendments-pipeline + amendment-text-availability-checker (Cloud Run Jobs) + amendment-text-pipeline (Cloud Run Service) in steady-state config. |
| 7.8 | Verify production logs show clean steady-state operation. |

**Done when**: production pipelines running on cron / subscriber as expected. New amendments since the dump appear in production DB on next cron tick.

---

## Part 2 — Runtime flow (cradle to grave for one amendment, ingestion only)

End-to-end ingestion when a brand new amendment appears on Congress.gov. The flow ends at "data is in the DB and ready" — what reads it (analysis / scoring) is a separate plan.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  T0:  Sponsor introduces S.Amdt. 5000 to H.R. 1234                       │
│       Congress.gov starts returning the amendment in API responses       │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
[CRON +0:45] amendments-pipeline (Cloud Run Job)
   1. PipelineBootstrap loads AmendmentsConfig
        congresses    = [102..119]
        lookbackDays  = config value, default 7  (operator-tunable; backfill uses 999999)
   2. For each congress in config.congresses (sequential):
        AmendmentsApiClient.fetchAll(/amendment/{c}?fromDateTime=now-{lookbackDays}d)
   3. For each AmendmentListItemDTO via parEvalMap(parallelism=4):
        a. Generate per-amendment correlationId UUID
        b. naturalKey = "119-SAMDT-5000"
        c. processAmendment(naturalKey, inFlight=Set.empty, depth=0):
             - Cycle/depth guards
             - amendmentRepo.findByNaturalKey(nk) → existing or None
             - Idempotency: skip if existing.updateDate already current
             - apiClient.fetchDetail(listItem.url) → AmendmentDetailDTO
             - SPONSOR RESOLVE (mirrors MemberResolver.ensureSponsorPlaceholder in bill-metadata-pipeline):
                 bioguide ← detail.sponsors[0].bioguideId
                 placeholderCreator.ensureExists[MemberDO](bioguide, memberEntityRepo)  # F[Unit], ON CONFLICT DO NOTHING
                 resolvedSponsorMemberId = memberRepo.findByBioguideId(bioguide).map(_.map(_.memberId))
             - BILL RESOLVE (mirrors BillLookup in votes-pipeline) when detail.amendedBill.isDefined:
                 billNk = "119-HR-1234"
                 billRepo.upsertPlaceholder(billNk).transact(xa)             # F[Unit], DO NOTHING
                 resolvedBillId = billRepo.findByBillId(billNk).map(_.map(_.billId))
             - PARENT RESOLVE — INLINE RECURSION (when detail.amendedAmendment.isDefined):
                 parentNk = "119-SAMDT-4999"
                 parentExisting = amendmentRepo.findByNaturalKey(parentNk)
                 IF parentExisting.exists(_.updateDate.isDefined):
                     # Parent already fully hydrated — use as-is
                     resolvedParentAmendmentId = parentExisting.id
                     parentEffectiveBillId    = parentExisting.effectiveBillId
                 ELSE:
                     # Recurse: drain entire parent chain to the bill before continuing
                     processAmendment(parentNk, inFlight + naturalKey, depth + 1)
                     # After recursion returns, re-read parent
                     hydrated = amendmentRepo.findByNaturalKey(parentNk)
                     resolvedParentAmendmentId = hydrated.id
                     parentEffectiveBillId    = hydrated.effectiveBillId
             - effectiveBillId = resolvedBillId
                                  .orElse(parentEffectiveBillId)
                                  .orElse(None)  # legitimate for some procedural amendments
             - DTO→DO:
                 detail.toDO(resolvedBillId, resolvedSponsorMemberId, resolvedParentAmendmentId)
                 → Either[String, AmendmentDO]
             - UPSERT (single transaction, effectiveBillId already populated):
                 amendmentRepo.upsert(amendmentDO).transact(xa) → AmendmentDO with surrogate id
             - LOG diff at info level
             - Return Succeeded
   4. summarize → PipelineRunSummary (eventsEmitted = 0)
   5. WorkflowStateUpdater.recordStepCompleted(runId, "amendments-ingestion")
   6. Exit 0
                                    │
                                    │  amendments table now has S.Amdt. 5000
                                    │  with parent + effectiveBillId already cached
                                    ▼
[CRON +0:50] amendment-text-availability-checker (Cloud Run Job)
   1. amendmentRepo.findCandidatesForTextCheck(minCongress=117, staleAfter=4h)
        Filters: congress >= 117 AND amendment_type != SUAMDT
                 AND (last_text_check_at IS NULL OR < NOW()-4h)
   2. For each candidate via parEvalMap(parallelism=4):
        a. correlationId fresh UUID
        b. apiClient.fetchTextVersions(amendment.congress, type, number)
              → AmendmentTextResponseDTO with textVersions: List[AmendmentTextItemDTO]
        c. existing ← amendmentTextVersionRepo.findByAmendmentId(amendment.amendmentId)
        d. selectAllNewVersions(versions, existing) → List[(item, format)]
        e. For each new (version, format) tuple:
             eventPublisher.publish(AmendmentTextAvailableEvent(
               amendmentId, naturalKey, congress, type, number,
               versionTypeCode, formatType, url, publishedDate, correlationId))
             → Pub/Sub topic "amendment.text.available"
        f. ON SUCCESS ONLY (per L1): amendmentRepo.updateLastTextCheckAt(amendmentId)
           (no text_versions_count anywhere — dropped per S2)
   3. Exit 0
                                    │
                                    │  Pub/Sub queue holds AmendmentTextAvailableEvent
                                    ▼
[SUBSCRIBER, ALWAYS RUNNING] amendment-text-pipeline (Cloud Run Service)
   Mirrors bill-text-pipeline implementation strategy byte-for-byte.
   1. PubSubSubscriber pulls event from "amendment.text.available"
   2. processEvent(event):
        a. isAlreadyProcessed(amendmentId, versionTypeCode, formatType)
             → SELECT EXISTS (...) WHERE fetched_at IS NOT NULL
             IF true → ACK, return Skipped
        b. INSERT amendment_text_versions row with fetched_at=NULL,
             source_url=event.url,
             download_url=CrecGovInfoUrlRewriter.rewrite(event.url) → api.govinfo.gov URL
        c. DELETE FROM amendment_text_chunks WHERE version_id = $newVersionId  (orphan cleanup)
        d. STREAMING DOWNLOAD from download_url (with GOVINFO_API_KEY query param)
        e. extractor.extract(bytes, formatType):
             "HTML" → CrecHtmlExtractor.extract (UTF-8, decode entities, strip CREC noise)
             "PDF"  → PdfStreamExtractor.extract (spool-to-temp + parse)
        f. TextChunker.chunkPipe(maxChunkChars=12000) — character-based, no overlap,
             format-agnostic (same impl bills uses)
        g. OllamaEmbeddingService — HTTP POST to ollama /api/embed,
             model = "bill-text-embedding" (qwen3-embedding:0.6b tuned),
             output dim = 1024 → vector(1024)
        h. Batched per-chunk INSERT into amendment_text_chunks
        i. UPDATE amendment_text_versions SET fetched_at = NOW(), text_length = $bytes
             WHERE id = $newVersionId
        j. ACK message  ←  NO EVENT EMITTED. Readiness signal is fetched_at IS NOT NULL
                          (same pattern bills uses post-event-removal).
                                    │
                                    │  amendment_text_versions.fetched_at populated;
                                    │  chunks ready for downstream pipelines to query.
                                    ▼
              Downstream analysis / scoring pipelines (separate plan)
              poll `amendment_text_versions WHERE fetched_at IS NOT NULL`
              and pick up amendments whose text is ready. That work is
              out of scope for this plan.
```

**Independently, in parallel with the above:**

```
[CRON +5:00] votes-pipeline (Cloud Run Job)
   1. Fetch House votes from /house-vote/.../members and Senate XML rolls
   2. For Senate amendment vote XML:
        a. SenateVoteXmlDecoder reads <document_type>S.Amdt.</document_type>,
           <amendment_number>S.Amdt. 5000</amendment_number>,
           <amendment_to_document_number>H.R. 1234</...>
        b. SenateVoteConverter.classifyDocument:
             - normalizeDocumentType → LegislationRef.Amendment(SAMDT)
             - Parse "5000" from amendment_number
             - Build amendment natural key "119-SAMDT-5000"
             - IF congress < 102: skip placeholder, increment pre_102_amendment counter,
               persist vote with legislation_type/number set anyway
             - ELSE: amendmentRepo.upsertPlaceholder("119-SAMDT-5000").transact(xa).void
                     (returns Long, voted, but not used — votes don't FK to amendments)
        c. buildVoteDO with legislation_type=SAMDT, legislation_number=5000, bill_id=None
        d. Persist vote + vote_positions
                                    │
                                    │  votes table has roll-call rows for the amendment.
                                    │  Whatever scoring pipeline ships next reads them.
                                    ▼
```

---

## Part 3 — Cross-application interactions (ingestion-only)

External systems and data flows the ingestion work touches. **Each row enumerates one interaction; review-checkable.**

### External APIs / data sources

| Caller | Endpoint | Auth | Format | Rate limit | Used for |
|---|---|---|---|---|---|
| amendments-pipeline (§7.1) | `https://api.congress.gov/v3/amendment/{c}` (list) | `api_key` query param (CONGRESS_GOV_API_KEY) | JSON | 5K/hr shared | Per-congress amendment metadata |
| amendments-pipeline (§7.1) | `https://api.congress.gov/v3/amendment/{c}/{type}/{n}` (detail) | Same | JSON | Same | Amendment detail with sponsors, amendedBill, amendedAmendment |
| amendment-text-availability-checker (§7.5) | `https://api.congress.gov/v3/amendment/{c}/{type}/{n}/text` | Same | JSON | Same | Text version metadata + format URLs |
| amendment-text-pipeline (§7.6) | `https://api.govinfo.gov/packages/CREC-{date}/granules/{granule-id}/{ext}` | `api_key` query param (GOVINFO_API_KEY) | HTML / PDF bytes | 36K/hr | Amendment text body (CREC mirror) |
| amendment-text-pipeline (§7.6) | `http://localhost:11434/api/embed` (in-cluster Ollama) | None | JSON | Self-hosted | qwen3-embedding:0.6b inference, 1024-dim vectors |
| votes-pipeline (existing + §7.4) | `https://www.senate.gov/legislative/LIS/roll_call_votes/...` | None | XML | None published; tight `permits=1, requestDelay=3s` | Senate amendment-vote roll calls |
| votes-pipeline (existing) | `https://api.congress.gov/v3/house-vote/...` | api_key | JSON | 5K/hr | House amendment-vote roll calls |

### Database tables — read interactions (ingestion only)

| Reader | Reads from table | Owned by |
|---|---|---|
| amendments-pipeline §7.3 | `members` (via `findByBioguideId`), `bills` (via `findByNaturalKey`), `amendments` (via `findByNaturalKey` for parent + idempotency) | members-common, bills-common, amendments-pipeline (self) |
| amendment-text-availability-checker §7.5 | `amendments`, `amendment_text_versions` | amendments-pipeline, amendment-text-pipeline |
| amendment-text-pipeline §7.6 | `amendment_text_versions` (existence check on `fetched_at IS NOT NULL`) | amendment-text-pipeline (self) |

### Database tables — write interactions (ingestion only)

| Writer | Writes to table | Notes |
|---|---|---|
| amendments-pipeline §7.3 | `amendments` (full upsert with `effective_bill_id` already populated), `members` (placeholder), `bills` (placeholder) | Cross-pipeline writes to members + bills tables — only placeholders, never overwrites |
| votes-pipeline §7.4 | `amendments` (placeholder via `upsertPlaceholder`), `votes` (full) | Cross-pipeline placeholder write to amendments — never overwrites real data |
| amendment-text-availability-checker §7.5 | `amendments` (only `last_text_check_at` via `updateLastTextCheckAt`, on success path per L1) | Targeted UPDATE; doesn't touch amendment metadata fields |
| amendment-text-pipeline §7.6 | `amendment_text_versions` (INSERT with `fetched_at=NULL`, then `markFetched(versionId, NOW())` on completion), `amendment_text_chunks` | Owned tables. **No event emitted** — `fetched_at IS NOT NULL` is the readiness signal. |

### Pub/Sub topics (ingestion only)

| Topic | Producer | Consumer(s) | Payload |
|---|---|---|---|
| `amendment.text.available` | amendment-text-availability-checker §7.5 | amendment-text-pipeline §7.6 | `AmendmentTextAvailableEvent` |
| `bill.text.available` (existing — unchanged) | bill-text-availability-checker | bill-text-pipeline | Unchanged |

**No `amendment.text.ingested` topic** — completion is signaled by `amendment_text_versions.fetched_at IS NOT NULL`, mirroring the bill-side pattern after its own event removal.

### Inter-module Scala dependencies (build.sbt)

| Subproject | Depends on | Why |
|---|---|---|
| amendments-pipeline | shared-models 0.1.40, pipeline-models 0.1.22, ingestion-common, members-common, bills-common, db-migrations-runner (test) | DOs, events, common helpers, sponsor + bill placeholder repos |
| amendment-text-availability-checker | amendments-pipeline | `AmendmentRepository`, `AmendmentDO` |
| amendment-text-pipeline | amendments-pipeline, text-extraction-common (NEW from P7.4) | `AmendmentRepository`, format extractors, chunker, embedder |
| votes-pipeline | amendments-pipeline (NEW per §7.4) | `AmendmentRepository.upsertPlaceholder` |
| bill-text-pipeline | text-extraction-common (NEW after P7.4 refactor) | Extractors moved out of bill-text-pipeline into shared module |
| docker-compose-e2e | All ingestion pipelines (test-time `assembly` deps) | Brings up full stack for E2E |

### External processes / human gates

| Trigger | Action | Owner |
|---|---|---|
| Production deploy approval | Manual cutover from local-built DB to production AlloyDB (Phase 7.6) | Ops |
| Schema migration sequencing | Run db-migrations against production AFTER pg_restore but BEFORE deploying new pipeline images | Ops |

---

## Part 4 — Pre-implementation checklist

Confirm before starting Phase 2:

- [ ] Phase 0 artifacts published with the trimmed scope (no analysis types). `sbt compile` clean against pinned versions.
- [ ] Phase 1 `text-extraction-common` PR merged. `bill-text-pipeline` regression tests still pass.
- [ ] `MIN_AMENDMENT_CONGRESS=102` confirmed in pipeline-models constants.
- [ ] GOVINFO_API_KEY provisioned in dev + prod secret manager.
- [ ] Pub/Sub topic `amendment.text.available` (and its subscription) created in dev + prod.

Confirm before starting Phase 6 (E2E):

- [ ] All four ingestion pipelines build green.
- [ ] WireMock fixtures cover Senate amendment-vote XML, Congress.gov amendment list/detail/text, govinfo CREC granules.
- [ ] `AmendmentChainFixtures` produces deterministic chains (bill ← amendment ← sub-amendment) for recursion testing.

Confirm before starting Phase 7 (production cutover):

- [ ] Local backfill produced amendment counts within an order of magnitude of estimate (~50K–150K rows for 102–119).
- [ ] `amendment_text_versions WHERE fetched_at IS NOT NULL` count is non-zero for ≥117.
- [ ] Pre-102 votes do NOT create amendment placeholders (counter visible in logs).
- [ ] `effective_bill_id` populated for ≥99% of amendments where parent + bill chain is fully discoverable from API data (orphan rate documented).

---

## Out of scope (explicit deferrals)

These are **NOT in this plan**. They have their own forthcoming plans:

| Deferred work | Belongs to |
|---|---|
| Amendment LLM analysis (themes, findings, complexity scoring, multi-pass routing) — §10.11 | Amendment-analysis plan (depends on Component 10 shipping for bills first) |
| Bill-side analysis refactor (`bill_complexity_scores`, `AnalysisComplexity` / `FindingImpact` enums, `*FindingDO.impact` fields, UUID→Long migrations) — §10.6, §10.7 | Component 10 plan (bill-analysis pipeline, separate effort) |
| Amendment scoring (member stances on amendments, alignment-score contributions) — §11.12 | Amendment-scoring plan (depends on Component 11 shipping for bills first + amendment-analysis plan above) |
| `finding_impact_weights` table + retuning | Amendment-scoring plan |
| `member_amendment_stances*`, `user_amendment_alignments` | Amendment-scoring plan |
| `amendment_findings`, `amendment_complexity_scores`, `amendment_themes`, `amendment_theme_chunk_members` schema | Amendment-analysis plan |
| Component 8 amendment prompts (`amendment-analysis-{tier}.md`) | Amendment-analysis plan |
| Component 9 score-explainer extension for amendments | Amendment-scoring plan |
| Production threshold tuning (`pass2Threshold`, `pass3Threshold`) | Amendment-analysis plan post-deploy |

When those plans start, the relevant area files (§10.11, §11.12, §10.6, §10.7) already specify the contracts — implementers pick up from there. The enum-rename done in this planning effort (`MagnitudeLevel` → `AnalysisComplexity` + `FindingImpact`) is captured in those files even though the implementation is deferred.

---

## Part 5 — Test Strategy

Validation coverage spans five layers — each layer protects against a different class of bug. Implementers must satisfy the acceptance criteria at every layer; no layer is optional.

### Layer 1 — Class-level (unit)

Each class in §7.1–§7.6 has its own ScalaTest spec under its subproject's `src/test`. Specs use ScalaTest `AnyFlatSpec + Matchers` plus MockitoScala for trait collaborators. Coverage gates: 90%+ patch coverage on Codecov.

Concrete unit-spec coverage required:

| Class | Boundary cases that must have tests |
|---|---|
| `AmendmentsApiClient` | pagination boundaries (exactly-pageSize, pageSize-1, empty, single-item); `lookbackDays` 0 / 999999; URL casing (path lowercase, query uppercase); 429/5xx retry; 404 skip-and-warn; cause-chain walk via `transientNetworkAware` |
| `AmendmentsConfig` | `congressesMin < 102` rejected; `congressesMax < congressesMin` rejected; defaults applied when HOCON minimal; `congresses` Range derivation |
| `AmendmentRepository` | `upsertPlaceholder` single-roundtrip (spy DB asserts 1 SQL stmt); `findByNaturalKeys` empty → empty map; concurrent `upsertPlaceholder` → exactly 1 row; `chamber` NOT NULL constraint enforced |
| `AmendmentProcessor` | depth-bound trips at `maxRecursionDepth + 1`; correlationId propagated through every recursion frame; recursion converges on shared parent under `parEvalMap` (L2); per-page batch issues 1 SELECT |
| `AmendmentTextVersionSelector` | `selectAllNewVersions` returns BOTH SUB and MOD when both new; filters already-ingested tuples; HTML preferred over PDF; skips XML format |
| `CrecGovInfoUrlRewriter` | adversarial URLs (path traversal, double-encoded, query-string strip); long-tail fallback returns `None` |
| `CrecHtmlExtractor` | drops running headers / page numbers; keeps speaker tags + section markers; UTF-8 entity decoding |
| `TextChunker` | UTF-16 surrogate-pair safety at chunk boundaries; `maxChunkChars` enforced; bounded heap on streaming input |
| `OllamaEmbeddingService` | dimension invariant (1024); 429 retry; queue back-pressure under producer overrun (P8) |
| `AmendmentTextProcessor` | re-submission update path (L6); duplicate Pub/Sub delivery handled idempotently; crash mid-stream leaves orphan-cleanup-detectable state |
| `SenateVoteConverter` | amendment-number regex matches NBSP + ASCII space (L5); pre-102 skip increments counter; H.Amdt./S.Amdt./S.U.Amdt. all dispatch correctly |

### Layer 2 — Inter-class within subproject

Per-subproject "integration" test that wires real classes together (still no Docker) to catch contract mismatches between collaborators:

| Subproject | Inter-class test |
|---|---|
| amendments-pipeline | `AmendmentProcessor` ↔ `AmendmentRepository` ↔ recursion: end-to-end flow from `streamAll` to persisted row, using AlloyDB Omni Docker singleton + WireMock for API. Verifies recursion's effective_bill_id resolution against a real DB. |
| amendment-text-availability-checker | `AmendmentTextAvailabilityChecker` ↔ `AmendmentRepository` ↔ Pub/Sub publisher: cron-tick simulation that produces `AmendmentTextAvailableEvent` against the emulator. |
| amendment-text-pipeline | `AmendmentTextProcessor` ↔ embedder ↔ chunker ↔ extractor: byte-level fixture flows through the full pipeline; chunks land with valid 1024-dim embeddings; HNSW index satisfies a sample cosine query. |

### Layer 3 — Inter-component (within this repo)

Cross-subproject regression tests verify the `text-extraction-common` refactor (Phase 1) didn't break bills:

| Test | Coverage |
|---|---|
| Phase 1 byte-identical regression | Hash-of-output regression: extract a fixture HTML through `bill-text-pipeline`'s old extractor (pre-refactor binary), then through `text-extraction-common`'s extracted version. Hashes must match. |
| Bill-text-pipeline E2E unchanged | Existing `BillTextPipelineE2ESpec` runs unchanged after `text-extraction-common` lands. No new failures. |
| `AmendmentRepository` and `BillRepository` use the same `transientNetworkAware` helper | Inspect class hierarchy / source — both classifiers wrap the shared helper; no copy-pasted cause-chain walk in either. |

### Layer 4 — Inter-application (cross-pipeline)

The `docker-compose-e2e` harness brings up the full stack and exercises wire-level interactions. Already covered in §7.7 — but the audit pass added these new scenarios:

| Scenario | Layer 4 coverage |
|---|---|
| S5e — re-submission update (per L6) | New event with newer `publishedDate` for already-fetched (versionType, format) → text refreshed in place |
| S5f — Pub/Sub redelivery idempotency | Event published twice → second processed as `Skipped` via ON CONFLICT short-circuit |
| S5g — Ollama unreachable | Stop ollama container → events stay un-ACKed, redelivered after recovery |
| S5h — connection-pool exhaustion (P1 negative test) | Undersized pool → recursion deadlocks; verifies sizing guidance is required |

### Layer 5 — Inter-system (with external APIs / GCP)

Tests that exercise actual external boundaries. Tagged `RequiresInternet` — excluded from default `sbt test`, run via `sbt e2e-gcp/test` against a dev project.

| Test | Layer 5 coverage |
|---|---|
| Dev-GCP contract: `/v3/amendment/{c}` shape unchanged | Fetches one page from real `api.congress.gov` against dev API key; asserts the response decodes into `AmendmentListItemDTO` without `Decoder` errors. **Surfaces upstream schema drift.** |
| Dev-GCP contract: `/amendment/.../text` returns the format URLs we expect | Fetches text versions for a known recent SAMDT in 119th congress; asserts URLs match `CrecGovInfoUrlRewriter` regex. |
| Dev-GCP contract: api.govinfo.gov CREC granule HEAD returns 200 | Verifies the rewriter targets a live granule. |
| Dev-GCP contract: senate.gov XML for a known amendment vote parses into `SenateVoteDocumentDTO` | Verifies the L5 NBSP regex against actual XML. **Surfaces senate.gov format drift.** |
| Schema-migration parity: AlloyDB Omni vs AlloyDB | CI step runs all migrations against `postgres:16` Docker AND AlloyDB Omni Docker; `\d+` outputs diffed. Catches "works on Omni, breaks on prod" surprises. |
| Pub/Sub at-least-once: real Pub/Sub redelivery | Send + delay ACK past the 60s deadline → message redelivers; assert idempotent processing per S5f. |

### Test execution model

| Tag | Runs by default? | Trigger |
|---|---|---|
| (none) | Yes (every PR) | `sbt test` |
| `DockerRequired` | No | `sbt 'set Test/testOptions := ...' dockerComposeE2e/test` (existing convention) |
| `RequiresInternet` | No | `sbt e2e-gcp/test` (manual / scheduled, against dev GCP) |

---

## Part 6 — Performance Tuning Knobs

Configuration values that affect throughput, latency, or operational cost. Documented here so operators can tune without rereading every area file.

| Knob | Default (steady-state) | Backfill override | Rationale | Where set |
|---|---|---|---|---|
| `AmendmentsConfig.parallelism` | 4 | **1** (per P7) | Backfill prefers tight rate control; steady-state prefers throughput | HOCON / env |
| `AmendmentsConfig.pageDelay` | 0ms | **750ms** (per P7) | Throttle during backfill to ~4800 req/hour, headroom under 5K/hour Congress.gov budget | HOCON / env |
| `AmendmentsConfig.lookbackDays` | 7 | **999999** | Steady-state: 4h cron with 7d lookback gives 42× redundancy buffer for crash recovery; backfill: full history | HOCON / env |
| `AmendmentsConfig.congressesMin` / `Max` | 102 / 119 | Same | S3 — bounded range; bumped manually each new congress | HOCON / env |
| `AmendmentsConfig.maxRecursionDepth` | 10 | Same | Per S1 — depth bound is the only safety net for runaway recursion | HOCON / env |
| `DATABASE_POOL_MAX_SIZE` | **45** | Same | Per P1 — `parallelism × maxRecursionDepth + 5`; **MUST** be at least 45 to avoid deadlock during deep-chain hydration | env / HikariCP config |
| `OLLAMA_EMBED_BATCH_SIZE` | 50 | Same | Matches bills-pipeline; do NOT lower for amendments per directive | HOCON / env |
| `OLLAMA_EMBED_QUEUE_BYTES` | 1,200,000 (≈1.2 MB) | Same | Per P8 — `maxChunkChars × 100` worth of in-flight chunks; producer blocks when bound reached. Prevents heap blowup on 50MB amendment substitutes. | HOCON / env |
| HNSW index parameters (`amendment_text_chunks.embedding`) | `m=16, ef_construction=64` | Same | Per P4 — pgvector recommended starting defaults; `ef_search` tuned at query time per recall/latency target | DB migration |
| Cron cadence (amendments-pipeline + checker) | **4h** (per L10) | Manual one-shot | Common cadence so checker reliably picks up new amendments within same cycle | ofelia-config.ini |
| `OLLAMA_BASE_URL` | `http://localhost:11434` (in-cluster ollama) | Same | Self-hosted, no external dependency | env |

### Observability counters / gauges (operators tune from these)

| Metric | Type | Tuned by |
|---|---|---|
| `congress_gov_detail_fetches_total{cause="recursion_redundant"}` | Counter | Watch during backfill — if rate spikes, consider P6 (parent pre-resolution) |
| `amendment_text_download_attempts_total{outcome=...}` | Counter | govinfo 429 rate; investigate if sustained > 5% |
| `amendment_text_download_govinfo_remaining` | Gauge | Sampled from `X-Ratelimit-Remaining`; alert at <10% of 36000 |
| `amendment_placeholder_skipped_total{reason="pre_102_amendment"}` | Counter | Confirms pre-102 filter is firing; investigate if zero (possible regression) |
| `amendment_recursion_depth_histogram` | Histogram | Distribution of recursion depths reached during backfill / steady-state. p99 informs `maxRecursionDepth` tuning. |

---

## Part 7 — Terraform / IaC (`tf-repcheck-infra`)

Cloud resources for the amendments ingestion work live in the separate repo `tf-repcheck-infra` (path: `C:\Users\elita\source\repos2024\tf-repcheck-infra`). Per directive, **this section enumerates only the elements that can land now without later rework**. Resources that depend on the not-yet-built `compute` Terraform module (Cloud Run Jobs/Services, Cloud Scheduler) are deferred to a follow-up landing — they would otherwise need to ship together with code that doesn't exist yet (image refs, deployer wiring).

### Repo conventions (mirrored from existing pipelines)

- **Layout**: `environments/{dev,staging,prod}/main.tf` instantiates shared `modules/{iam,messaging,secrets,compute,...}`. Same module set for every environment; only `terraform.tfvars` differs.
- **Promotion**: dev → staging → prod, identical to the rest of the platform. Atlantis auto-plans on PR; apply requires manual approval per environment.
- **Naming**: Pub/Sub topics use kebab-case with no env suffix (env is implicit by project). SAs use `{purpose}-{env}` suffix. Deadletter topics suffix `-dead-letter`. Subscriptions suffix `-{consumer}-sub`.

### What lands NOW (stable across code changes)

These resources have no dependency on the as-yet-unbuilt Cloud Run images or the deployer wiring — they're pure infrastructure that the eventual application code will plug into.

#### 7A — Pub/Sub: `amendment-text-available` (in `modules/messaging/main.tf`)

Mirrors the existing `bill-events` pattern (60s ack deadline + 5-nack dead-letter):

```hcl
# Dead-letter topic — receives messages after 5 nack attempts on the main subscription
resource "google_pubsub_topic" "amendment_text_available_dead_letter" {
  project                    = var.project_id
  name                       = "amendment-text-available-dead-letter"
  message_retention_duration = "604800s"  # 7 days
}

# Main topic — produced by amendment-text-availability-checker (§7.5)
resource "google_pubsub_topic" "amendment_text_available" {
  project                    = var.project_id
  name                       = "amendment-text-available"
  message_retention_duration = "${var.message_retention_seconds}s"
}

# Subscription — consumed by amendment-text-pipeline (§7.6)
resource "google_pubsub_subscription" "amendment_text_available_pipeline_sub" {
  project = var.project_id
  name    = "amendment-text-available-pipeline-sub"
  topic   = google_pubsub_topic.amendment_text_available.id

  ack_deadline_seconds       = 60      # tighter than module default (300s) — §7.6 short-circuits already-processed events fast
  message_retention_duration = "${var.message_retention_seconds}s"
  retain_acked_messages      = false

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.amendment_text_available_dead_letter.id
    max_delivery_attempts = 5          # mirrors bill-events convention
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# Pub/Sub service agent → permission to publish to the dead-letter topic
resource "google_pubsub_topic_iam_member" "pubsub_sa_amendment_dlq_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.amendment_text_available_dead_letter.name
  role    = "roles/pubsub.publisher"
  member  = local.pubsub_service_agent  # already defined in module
}

# Pub/Sub service agent → permission to subscribe to the main subscription (for DLQ routing)
resource "google_pubsub_subscription_iam_member" "pubsub_sa_amendment_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.amendment_text_available_pipeline_sub.name
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_service_agent
}
```

**No `amendment-metadata-ingested` topic.** §7.3 emits no events — DB scan is the discovery mechanism.

**No `amendment-text-ingested` topic.** §7.6 emits no completion event — `amendment_text_versions.fetched_at IS NOT NULL` is the readiness signal.

#### 7B — Secret Manager: `govinfo-api-key` (in `modules/secrets/main.tf`)

Currently only `congress-api-key` exists in Terraform. The amendment-text-pipeline (§7.6) uses `api.govinfo.gov` for CREC downloads — that key needs its own secret container and IAM bindings:

```hcl
resource "google_secret_manager_secret" "govinfo_api_key" {
  project   = var.project_id
  secret_id = "govinfo-api-key"
  labels = {
    environment = var.environment
    managed-by  = "terraform"
  }
  replication { auto {} }
}

# SA-scoped accessor — uses the shared repcheck-pipeline-{env} SA (consistent with congress-api-key binding)
resource "google_secret_manager_secret_iam_member" "pipeline_govinfo_accessor" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.govinfo_api_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.pipeline_sa_email}"
}
```

The **secret value** is added separately (out-of-band, not in Terraform) per the existing convention:
```bash
gcloud secrets versions add govinfo-api-key --project=$PROJECT --data-file=- <<< "$GOVINFO_API_KEY"
```

#### 7C — Service Accounts (decision: REUSE `repcheck-pipeline-{env}`)

Two patterns precedent in `modules/iam/main.tf`:
- **Shared SA** (`repcheck-pipeline-{env}`): used by bills + members + amendments pipelines. Acceptable shared blast radius.
- **Per-pipeline SA** (`votes-pipeline-{env}`): used only by votes for stricter least-privilege.

**Choice: reuse `repcheck-pipeline-{env}` for all three new amendment pipelines.** Rationale:
- Three pipelines (§7.3, §7.5, §7.6) all read/write the same set of tables (`amendments`, `amendment_text_versions`, `amendment_text_chunks`, `members`, `bills`).
- All three need the same set of secrets (`congress-api-key`, `govinfo-api-key`).
- All three publish to / subscribe from the same Pub/Sub topic (`amendment-text-available`).
- Splitting them into three SAs would triple the IAM-binding surface for zero practical isolation — they're already in the same trust boundary as bills.

**No change to `modules/iam/main.tf` is needed for SAs.** The existing `repcheck-pipeline-{env}` SA's project-wide bindings (`pubsub.publisher`, `pubsub.subscriber`, `secretmanager.secretAccessor`, etc.) already cover the new pipelines once 7A and 7B land.

#### 7D — Workload Identity Federation (no change)

The amendments code ships from the existing `repcheck-data-ingestion` repo. That repo is already in `var.github_repos` per `modules/iam/variables.tf`. **No WIF change needed.**

### What is DEFERRED (depends on compute module not yet built)

These resources reference Cloud Run images that don't exist yet — they can't land in Terraform until the deploy pipeline is wired and ready to push images. They are **NOT** in scope for the Terraform changes attached to this plan; a follow-up Terraform PR adds them when image references are real.

| Resource | Type | Why deferred |
|---|---|---|
| `amendments-pipeline` Cloud Run Job | `google_cloud_run_v2_job` | Image not yet built; needs CI deploy wiring |
| `amendment-text-availability-checker` Cloud Run Job | `google_cloud_run_v2_job` | Same |
| `amendment-text-pipeline` Cloud Run Service | `google_cloud_run_v2_service` | Same; long-running subscriber |
| Cloud Scheduler trigger for amendments-pipeline (4h cron) | `google_cloud_scheduler_job` | Needs Cloud Run Job URI to target |
| Cloud Scheduler trigger for amendment-text-availability-checker (4h cron, +20m offset) | `google_cloud_scheduler_job` | Same |
| Monitoring alert policies for `amendment_text_download_govinfo_remaining`, govinfo 429 rate | `google_monitoring_alert_policy` | Tune from real-traffic baseline (per P7.7); ship after first deploy |
| AlloyDB IAM grants for the amendments pipelines | `google_alloydb_user` / IAM bindings | Depends on which SA the deferred Cloud Run resources actually run as |

### Order of operations

1. **Now (with this plan):** P7.12 Terraform PR lands sections 7A + 7B. Pub/Sub topic + dead-letter + subscription + IAM, plus `govinfo-api-key` secret container + accessor binding. Apply through dev → staging → prod.
2. **Out-of-band:** secret values for `govinfo-api-key` populated in each env's Secret Manager via `gcloud secrets versions add`.
3. **Local development:** unchanged. `pubsub-init.sh` and `ofelia-config.ini` (per §7.7) provide local equivalents that don't depend on Terraform-provisioned resources.
4. **Follow-up Terraform PR (post-code-build):** Cloud Run Jobs / Service + Cloud Scheduler triggers + monitoring alerts. Scope of that PR mirrors the deferred-table above.
