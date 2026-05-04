# Acceptance Criteria: Component 7 — Amendments Pipeline

> Three SBT projects within `repcheck-data-ingestion` that together ingest amendment metadata, detect new amendment text versions, and persist amendment text bodies as embedded chunks for analysis.
> **Why amendment text is in scope:** amendments can substantively alter the function of a bill or law — a substitute amendment can replace the entire body of a bill while leaving the bill number intact. Analysis that reads only the finalized bill text loses the per-amendment contribution and the ability to score amendment-specific roll calls accurately.
> **Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2), `ingestion-common` (Component 3), and a new `text-extraction-common` module shared with `bill-text-pipeline` (see §7.6).

---

## System Context

### Three Projects

Component 7 is **three** SBT projects deployed as Cloud Run Jobs and Subscribers:

| Project | Trigger | Responsibility | Publishes |
|---------|---------|---------------|-----------|
| `amendments-pipeline` (§7.1–§7.3) | Scheduled (e.g., every 6 hours) | Fetch amendment metadata from Congress.gov API, detect changes, upsert to AlloyDB, create placeholder members/bills | Nothing (metadata pipeline) |
| `amendment-text-availability-checker` (§7.5) | Scheduled (e.g., every 4 hours) | Poll `/amendment/.../text` for amendments where `congress >= 117`, pick canonical version, emit event when new text is available | `amendment.text.available` |
| `amendment-text-pipeline` (§7.6) | Pub/Sub subscriber | Consume `amendment.text.available`, download from `www.congress.gov` (no govinfo mirror), extract HTML/PDF, chunk, embed, persist | `amendment.text.ingested` |

Plus the **votes-pipeline cross-pipeline integration** specified in §7.4 — `votes-pipeline` creates amendment placeholders for amendment-typed roll calls.

The metadata side (§7.1–§7.3) remains "the simplest ingestion pipeline":
- **Single data source for metadata** — Congress.gov JSON API only
- **No history archival** — amendments rarely change after initial recording
- **Single DO output** — `AmendmentDetailDTO.toDO` produces one `AmendmentDO`, no fan-out
- **Standard `ChangeDetector`** — generic from Component 3 §3.3

The text side (§7.5–§7.6) inherits the bill-text architecture but with one critical infrastructure difference: **no api.govinfo.gov mirror exists for amendments** (verified — govinfo collections are BILLS, BILLSTATUS, CREC, FR, etc.; no AMENDMENTS). Downloads must hit `www.congress.gov` directly with Cloudflare bot-challenge handling. See §7.6.

### End-to-End Data Flow

```
Cloud Scheduler ──▶ amendments-pipeline (§7.1–§7.3, every 6h)
                       │
                       ▼
                    Fetch /amendment list (paginated)
                    Per amendment: change-detect, fetch detail,
                    resolve sponsor + bill placeholders via
                    memberRepo.upsertPlaceholder + billRepo.upsertPlaceholder
                    (returns surrogate ids), upsert AmendmentDO.
                    NO events emitted.

Cloud Scheduler ──▶ amendment-text-availability-checker (§7.5, every 4h)
                       │
                       ▼
                    amendmentRepo.findCandidatesForTextCheck(minCongress=117, staleAfter=4h)
                    Per candidate: GET /amendment/.../text via api.congress.gov
                    AmendmentTextVersionSelector.selectAllNewVersions: HTML > PDF
                    Compare against amendment_text_versions; emit one event per new (version, format)
                    On success ONLY: updateLastTextCheckAt (per L1)
                    │
                    ▼ (new version found)
                    emit amendment.text.available
                                                  │
                                              Pub/Sub
                                                  │
                                                  ▼
                          amendment-text-pipeline (§7.6, long-running subscriber)
                                                  │
                                                  ▼
                          CrecGovInfoUrlRewriter:
                            www.congress.gov/.../crec/.../CREC-...{ext}
                          → api.govinfo.gov/packages/.../granules/.../{ext}
                          Download (with GOVINFO_API_KEY) — NO Cloudflare path
                          Extract HTML or PDF → chunks → embeddings
                          Persist amendment_text_versions + amendment_text_chunks
                                                  │
                                                  ▼
                          emit amendment.text.ingested
                                                  │
                                              Pub/Sub
                                                  │
                                                  ▼
                          bill-analysis-pipeline (§10.11 single-pass amendment branch)
                          Direct LLM analysis → amendment_findings rows
                          emit analysis.completed (entityType="amendment")

Cloud Scheduler ──▶ votes-pipeline (Component 6 + §7.4 integration)
                       │
                       ▼
                    On amendment-typed roll call:
                    amendmentRepo.upsertPlaceholder(naturalKey).void
                    (surrogate id returned but discarded —
                     votes don't FK to amendments today)

Cloud Scheduler ──▶ stance-materializer (Component 11 §11.9, scheduled)
                       │
                       ▼
                    For each ready bill in stance_materialization_status:
                      materializeBill (existing path)
                      THEN amendmentRepo.findByBillId(billId):
                        for each amendment with both votes AND findings:
                          materializeAmendmentStances (§11.12 new)
                    NO separate amendment_materialization_status table
                    (per Q8 — bill-driven scan)
```

### Events

Two new events on the project-wide Pub/Sub topology, plus an extension to an existing event:

| Event | Emitter | Consumer(s) | Payload |
|-------|---------|-------------|---------|
| `amendment.text.available` (NEW) | §7.5 | §7.6 | amendmentId (surrogate Long), naturalKey, congress, type, number, versionTypeCode, formatType, url, publishedDate, correlationId |
| `amendment.text.ingested` (NEW) | §7.6 | Component 10 §10.11 (single-pass amendment analysis) | amendmentId, versionId, versionTypeCode, formatType, chunkCount, textLength, correlationId |
| `analysis.completed` (EXTENDED) | Existing for bills, extended for amendments per §10.11 | Existing consumers + Component 11 §11.12 | Existing fields + new `entityType: String` (`"bill" | "amendment"`) + `entityId: Long` discriminator |

> **Trigger model — cron + Pub/Sub mix mirroring bill-text architecture (per Q10).**
> - **§7.3 (amendments-pipeline)** is cron-scheduled and emits **no** events. It's a pure data recorder.
> - **§7.5 (amendment-text-availability-checker)** is cron-scheduled too — mirrors `bill-text-availability-checker` exactly. Scans `amendments` for candidates needing a text check; emits `amendment.text.available` for new versions found.
> - **§7.6 (amendment-text-pipeline)** is Pub/Sub-subscribed (long-running Cloud Run Service). Emits `amendment.text.ingested`.
> - **Component 10 §10.11** subscribes to `amendment.text.ingested` and emits the extended `analysis.completed` event with `entityType="amendment"`.
> - **Component 11 §11.12** does NOT subscribe to events — it scans for ready amendments via the bill-driven scan inside the existing stance materializer cron job (per Q8).
>
> **`AmendmentRecordedEvent` and `amendment.metadata.ingested` remain removed.** Earlier drafts attempted an event-driven §7.5 subscribed to a metadata event; reverted to cron-driven per Q10.

### Amendment Votes

Roll call votes on amendments flow through the votes pipeline (Component 6), not the amendments pipeline. `VoteDO` has `legislationType` and `legislationNumber` fields that reference the amendment being voted on. The amendments pipeline only records amendment *metadata* (sponsor, description, purpose, amended bill) — vote data comes from Component 6.

> **Today this is aspirational** — the votes pipeline currently drops the linkage. The `votes-pipeline` integration work specified in §7.4 closes this gap: extends the chamber converters to recognize `S.Amdt.` / `H.Amdt.` / `S.U.Amdt.` document types, populates the `legislation_*` columns, and creates amendment placeholders via `AmendmentRepository.upsertPlaceholder` when the referenced amendment is not yet ingested. See [§7.4](07-amendments-pipeline/07.4-cross-pipeline-integration.md) for the full spec.

### No History Archival

Unlike bills (Component 4) and votes (Component 6), amendments do **not** use the archive-before-overwrite pattern:

- Amendments rarely change substantively after initial recording — the typical "change" is a metadata correction (e.g., updated `latestActionDate`)
- The `updateDate` comparison in `ChangeDetector` already prevents redundant writes
- No downstream consumer depends on amendment history (the re-analysis pipeline reads the *current* amendment, not its change history)
- If amendment history becomes needed in the future, adding it follows the same `HistoryArchiver` pattern as bills/votes/members — no architectural change required

### Amendment Types and the 102nd-Congress Cutoff

Per Component 1 §1.8, `AmendmentType` is an enum with values:
- `HAMDT` — House amendment
- `SAMDT` — Senate amendment
- `SUAMDT` — Senate unprinted amendment (97th-98th Congresses only; **out of scope** for ingestion per the cutoff below)

**`MIN_AMENDMENT_CONGRESS = 102`** (per Q9 of planning — see [PRODUCTION_TASKS.md global constant](07-amendments-pipeline/PRODUCTION_TASKS.md)). Amendments older than the 102nd Congress (1991-1993) are out of scope: pre-102 senate.gov XML and Congress.gov data have inconsistent shape, SUAMDT only existed in the 97th-98th Congresses (well below cutoff), and historical analysis of pre-1991 amendments is not part of the product. Enforced at:
- amendments-pipeline `congresses` config (PureConfig validates list contains no values < 102)
- votes-pipeline placeholder creation (skips with `pre_102_amendment` counter)
- §7.4 `SenateVoteConverter.normalizeDocumentType` does not map `S.U.Amdt.` (dead code given the cutoff)

### Placeholder Entity Pattern

Placeholders flow in **both directions** through this pipeline:

**Outbound — placeholders created BY the amendments pipeline** (when an amendment references entities not yet in the database):

| Reference | Placeholder Type | Condition |
|-----------|-----------------|-----------|
| `sponsorBioguideId` | `MemberDO` placeholder | Only when `sponsorBioguideId` is `Some` — some amendments have no identified sponsor |
| Amended bill | `BillDO` placeholder | Only when the amendment references a specific bill via `amendedBill` |

**Inbound — placeholders created FOR the amendments pipeline** (when other pipelines reference an amendment not yet ingested):

| Source pipeline | When | How |
|-----------------|------|-----|
| `votes-pipeline` | Roll call references S.Amdt./H.Amdt./S.U.Amdt. on either chamber | Depends on `amendments-pipeline` directly; calls `AmendmentRepository.upsertPlaceholder(naturalKey)` inline (returns `F[Long]` surrogate id, votes-pipeline `.void`s it). See [§7.4](07-amendments-pipeline/07.4-cross-pipeline-integration.md) |
| `bill-summary-pipeline` | Never | The `/summaries` endpoint returns no amendment references — verified via OpenAPI spec |

Placeholders use `INSERT ... ON CONFLICT DO NOTHING` (Component 3 §3.6) — safe against concurrent ingestion. An inbound placeholder leaves all DTO-sourced fields (including `update_date`) NULL, so the next amendments-pipeline run treats it as `New` and hydrates from the Congress.gov detail endpoint. See [§7.2 Date Semantics](07-amendments-pipeline/07.2-amendment-repository.md) for why this matters for change detection.

### Congress.gov API Endpoints

The amendments API (`/v3/amendment`) follows the standard Congress.gov paginated pattern:

| Endpoint | Returns | Used By |
|----------|---------|---------|
| `GET /amendment?congress={N}&...` | List of `AmendmentListItemDTO` | `fetchAll` (pagination) |
| `GET /amendment/{congress}/{type}/{number}` | `AmendmentDetailDTO` with sponsors, amended bill, latest action | `fetchDetail` |
| `GET /amendment/{congress}/{type}/{number}/actions` | Amendment actions timeline | Not used in initial implementation |
| `GET /amendment/{congress}/{type}/{number}/cosponsors` | Amendment cosponsors | Not used in initial implementation |
| `GET /amendment/{congress}/{type}/{number}/amendments` | Sub-amendments | Not used in initial implementation |
| `GET /amendment/{congress}/{type}/{number}/text` | Text versions (117th Congress+) | Not used in initial implementation |

> **Endpoint consumption — by area:**
> - **§7.1 metadata client** consumes `/amendment` (list) and `/amendment/{c}/{type}/{n}` (detail) only.
> - **§7.5 availability checker** consumes `/amendment/{c}/{type}/{n}/text`.
> - **Actions, cosponsors, sub-amendments** remain unconsumed in initial scope. If downstream analysis needs them, add per-area without changing the existing architecture (each is a candidate for a separate child-table pipeline following the same shape).

---

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 7.1 Amendments API Client | New | Extends `CongressGovPaginatedClient` for amendment list and detail endpoints. **Per-congress iteration** via `congressesMin/Max: Int` bounded range (per S3; default 102..119). Stream EOF / `EmberException` / `IOException` classified as Transient via shared `transientNetworkAware` helper (per S7) |
| 7.2 Amendment Repository | New | Doobie repository — `upsert`, `upsertPlaceholder`, queries. Strict separation between DTO-sourced dates and RepCheck-internal audit timestamps. Schema additions for text-version metadata in §7.5 |
| 7.3 Amendment Processing Pipeline | New | FS2 streaming pipeline: fetch → detect → placeholders → upsert. Per-amendment correlation ID. Two-stage change detection (fast `updateDate` filter + observability diff) |
| 7.4 Cross-Pipeline Integration | New | Votes-pipeline depends on `amendments-pipeline` directly and calls `AmendmentRepository.upsertPlaceholder` inline. Schema migration to extend `legislation_type_enum`. Summary-pipeline confirmed not applicable. E2E placeholder-and-hydration scenarios |
| 7.5 Amendment Text Availability Checker | New | **Cron-scheduled** Cloud Run Job (mirrors `bill-text-availability-checker`). Scans `amendments` for candidates with `congress >= 117` AND non-SUAMDT AND stale `last_text_check_at`; queries `/amendment/.../text` for each; emits `amendment.text.available` for ALL new versions (per Q8 — `selectAllNewVersions`). Tracks state via `last_text_check_at` only (per S2 — `text_versions_count` dropped); per L1, updates `last_text_check_at` only on successful API + publish path. |
| 7.6 Amendment Text Pipeline | New | Pub/Sub-subscribed worker. **Downloads via `api.govinfo.gov` CREC mirror** (verified 2026-05-03 — `CrecGovInfoUrlRewriter` rewrites `www.congress.gov/.../crec/.../CREC-...{ext}` URLs to `api.govinfo.gov/packages/.../granules/.../{ext}` with API key). Extracts HTML/PDF via shared `text-extraction-common` module, chunks, embeds, persists. Emits `amendment.text.ingested`. **No Cloudflare strategy needed** — Tier 1 govinfo path is verified working |
| 7.7 Docker-Compose + Holistic E2E | New | Service additions to `docker-compose.local.yml` and `docker-compose.e2e.yml` for the three new pipelines, ofelia cron entry for `amendments-pipeline` (45-min offset), Pub/Sub topic + subscription init script additions, WireMock fixtures, and six cross-pipeline E2E test scenarios validating amendments↔votes↔members↔bills wiring end-to-end |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Congress.gov amendment list/detail API integration | [7.1 Amendments API Client](07-amendments-pipeline/07.1-amendments-api-client.md) |
| AlloyDB persistence for amendments (incl. text-version metadata columns) | [7.2 Amendment Repository](07-amendments-pipeline/07.2-amendment-repository.md) |
| Streaming pipeline: fetch → detect → placeholders → upsert | [7.3 Amendment Processing Pipeline](07-amendments-pipeline/07.3-amendment-processing-pipeline.md) |
| Votes-pipeline integration, schema migration, E2E placeholder/hydration tests | [7.4 Cross-Pipeline Integration](07-amendments-pipeline/07.4-cross-pipeline-integration.md) |
| Amendment text availability polling, version selection, event emission | [7.5 Amendment Text Availability Checker](07-amendments-pipeline/07.5-amendment-text-availability-checker.md) |
| Amendment text download, extraction, chunking, embedding, persistence | [7.6 Amendment Text Pipeline](07-amendments-pipeline/07.6-amendment-text-pipeline.md) |
| Docker-compose service additions + cross-pipeline E2E test scenarios | [7.7 Docker-Compose + Holistic E2E](07-amendments-pipeline/07.7-docker-compose-and-e2e-integration.md) |
| **Production-environment tasks** (allowlist outreach, monitoring tuning, deferred sidecar work) | [Production Tasks tracker](07-amendments-pipeline/PRODUCTION_TASKS.md) |

---

## Cross-Cutting Concerns

### SBT Module Structure

```
repcheck-data-ingestion/
├── amendments-pipeline/              (Cloud Run Job — §7.1–§7.3)
│   └── repcheck.ingestion.amendments
│       ├── api                       AmendmentsApiClient                (7.1)
│       ├── repository                AmendmentRepository                (7.2)
│       ├── pipeline                  AmendmentProcessor                 (7.3)
│       ├── app                       AmendmentPipelineApp               (IOApp wiring)
│       └── errors                    AmendmentFetchFailed (7.1),
│                                     AmendmentUpsertFailed (7.2),
│                                     InvalidAmendmentNaturalKey (7.2)
│
├── amendment-text-availability-checker/  (Cloud Run Job, cron-scheduled — §7.5)
│   └── repcheck.ingestion.amendments.textcheck
│       ├── api                       AmendmentTextApiClient
│       ├── selection                 AmendmentTextVersionSelector
│       ├── pipeline                  AmendmentTextAvailabilityChecker
│       ├── app                       AmendmentTextCheckerApp            (IOApp wiring)
│       └── errors                    AmendmentTextCheckFailed,
│                                     AmendmentTextApiHttpError
│
├── amendment-text-pipeline/          (Cloud Run Service — §7.6, Pub/Sub subscriber)
│   └── repcheck.ingestion.amendments.text
│       ├── download                  AmendmentTextDownloader,
│       │                             CrecGovInfoUrlRewriter             (NEW — Tier 1 govinfo mirror)
│       ├── extraction                AmendmentTextExtractor
│       │                             (dispatches to shared HTML/PDF extractors)
│       ├── persistence               AmendmentTextVersionRepository,
│       │                             AmendmentTextChunkRepository
│       ├── pipeline                  AmendmentTextProcessor
│       ├── app                       AmendmentTextPipelineApp           (IOApp wiring)
│       └── errors                    AmendmentTextDownloadFailed,
│                                     AmendmentTextExtractionFailed
│
└── text-extraction-common/           (shared module, new — extracted as separate PR per P7.4)
    └── repcheck.ingestion.text.extraction
        ├── HtmlStreamExtractor       (extracted from bill-text-pipeline)
        ├── PdfStreamExtractor        (extracted from bill-text-pipeline)
        ├── StreamingChunker          (extracted, content-agnostic)
        └── ChunkEmbedder             (extracted, generic over chunk type)
```

Cross-cutting dependencies:

| Project | Depends on |
|---------|------------|
| `votes-pipeline` | `amendments-pipeline` (per §7.4 — for `AmendmentRepository` trait + Doobie impl, no `amendments-common` extracted) |
| `bill-text-pipeline` | `text-extraction-common` (refactor to extract format-agnostic primitives) |
| `amendment-text-pipeline` | `text-extraction-common`, `amendments-pipeline` (for `AmendmentRepository`) |
| `amendment-text-availability-checker` | `amendments-pipeline` (for `AmendmentRepository`) |

> **Why `text-extraction-common` exists but `amendments-common` does not:** the format extractors are pure infrastructure with two real consumers today (`bill-text-pipeline`, `amendment-text-pipeline`) and zero domain coupling — clean candidate for extraction. The amendments repository has only two consumers (amendments-pipeline itself + votes-pipeline) and is tightly coupled to the amendment domain — direct dependency is cheaper than a third-party module.

Application entry points (the `*App` IOApps) follow the standard PureConfig + `PipelineBootstrap` pattern. All three are pure wiring — coverage-excluded per the build.sbt convention.

### Dependencies

```
amendments-pipeline
├── ingestion-common                 (internal SBT dependency — Component 3)
│   ├── CongressGovPaginatedClient   (API base)
│   ├── ChangeDetector               (change detection)
│   ├── PlaceholderCreator           (cross-entity refs)
│   ├── TransactorResource           (DB connection)
│   ├── UpsertHelper                 (SQL generation)
│   ├── PipelineBootstrap            (config, runId)
│   └── WorkflowStateUpdater         (step tracking)
├── repcheck-shared-models           (published artifact — Component 1)
│   ├── AmendmentListItemDTO, AmendmentDetailDTO
│   ├── AmendmentDO
│   ├── AmendmentType (HAMDT, SAMDT, SUAMDT)
│   └── HasPlaceholder[MemberDO], HasPlaceholder[BillDO]
└── repcheck-pipeline-models         (published artifact — Component 2)
    ├── ProcessingResult, PipelineRunSummary
    └── Tables (Amendments)
```

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | Processor logic, change detection integration | MockitoScala |
| WireMock tests | `AmendmentsApiClient` pagination, detail fetching, error classification | WireMock |
| Integration tests | `AmendmentRepository` (CRUD, upsert, conflict handling) | `DockerPostgresSpec` |
| Pipeline integration | Full pipeline flow: API → detect → placeholders → upsert | WireMock + DockerPostgresSpec |
